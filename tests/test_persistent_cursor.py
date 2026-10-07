from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from engines import persistent_cursor
from engines.persistent_cursor import PersistentCursorWorker


class FakeStdin:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    def write(self, data: bytes) -> None:
        self.sent.append(json.loads(data))

    async def drain(self) -> None:
        return None


def make_worker() -> tuple[PersistentCursorWorker, FakeStdin, list[str]]:
    stdin = FakeStdin()
    proc = SimpleNamespace(returncode=None, stdin=stdin)
    worker = PersistentCursorWorker((1, 2), proc, "sid", "/tmp", None)
    published: list[str] = []

    async def collect(text: str) -> None:
        published.append(text)

    worker.on_intermediate = collect
    return worker, stdin, published


def chunk(text: str) -> dict:
    return {"update": {"sessionUpdate": "agent_message_chunk",
                       "content": {"type": "text", "text": text}}}


class PersistentCursorTest(unittest.IsolatedAsyncioTestCase):
    async def test_turn_collects_answer_after_last_tool(self) -> None:
        worker, stdin, published = make_worker()
        worker._prefix = "SYS\n\n"
        is_new, fut = await worker.submit("привет")
        self.assertTrue(is_new)
        prompt = stdin.sent[-1]
        self.assertEqual(prompt["method"], "session/prompt")
        self.assertEqual(prompt["params"]["prompt"][0]["text"], "SYS\n\nпривет")

        await worker._handle_update(chunk("Смотрю "))
        await worker._handle_update(chunk("файлы."))
        await worker._handle_update({"update": {
            "sessionUpdate": "tool_call", "toolCallId": "t1", "kind": "execute",
            "title": "`ls`", "status": "pending", "rawInput": {}}})
        await worker._handle_update({"update": {
            "sessionUpdate": "tool_call_update", "toolCallId": "t1", "rawInput": {"command": "ls"}}})
        await worker._handle_update(chunk("Готово."))
        await worker._handle_response({"id": prompt["id"], "result": {"stopReason": "end_turn"}})

        self.assertEqual(await fut, (True, "Готово."))
        self.assertFalse(worker.busy)
        journal = "\n".join(published)
        self.assertIn("Смотрю файлы.", journal)
        self.assertIn("ls", journal)
        # Второй ход префикс уже не несёт.
        await worker.submit("ещё")
        self.assertEqual(stdin.sent[-1]["params"]["prompt"][0]["text"], "ещё")

    async def test_steer_ignores_cancelled_reply_of_previous_prompt(self) -> None:
        worker, stdin, _ = make_worker()
        _, fut = await worker.submit("первое")
        first = stdin.sent[-1]["id"]
        await worker._handle_update(chunk("недосказанное"))
        is_new, steer_fut = await worker.submit("второе")
        second = stdin.sent[-1]["id"]
        self.assertFalse(is_new)
        self.assertIs(steer_fut, fut)

        await worker._handle_response({"id": first, "result": {"stopReason": "cancelled"}})
        self.assertFalse(fut.done())
        await worker._handle_update(chunk("Ответ на оба."))
        await worker._handle_response({"id": second, "result": {"stopReason": "end_turn"}})
        self.assertEqual(await fut, (True, "Ответ на оба."))

    async def test_exclusive_turn_refuses_steering(self) -> None:
        worker, stdin, _ = make_worker()
        await worker.submit("триггер", exclusive=True)
        sent = len(stdin.sent)
        self.assertEqual(await worker.submit("пользователь"), (False, None))
        self.assertEqual(len(stdin.sent), sent)

    async def test_permission_request_allows_once(self) -> None:
        worker, stdin, _ = make_worker()
        await worker._answer_request({"id": 0, "method": "session/request_permission", "params": {
            "options": [{"optionId": "allow-always", "kind": "allow_always"},
                        {"optionId": "allow-once", "kind": "allow_once"}]}})
        self.assertEqual(stdin.sent[-1], {"jsonrpc": "2.0", "id": 0, "result": {
            "outcome": {"outcome": "selected", "optionId": "allow-once"}}})
        await worker._answer_request({"id": 1, "method": "fs/read_text_file", "params": {}})
        self.assertEqual(stdin.sent[-1]["error"]["code"], -32601)

    async def test_prompt_error_fails_turn(self) -> None:
        worker, stdin, _ = make_worker()
        _, fut = await worker.submit("x")
        await worker._handle_response({"id": stdin.sent[-1]["id"], "error": {
            "message": "Internal error", "data": {"message": "usage limit"}}})
        ok, text = await fut
        self.assertFalse(ok)
        self.assertIn("usage limit", text)

    async def test_retriable_error_repeats_prompt_then_fails(self) -> None:
        worker, stdin, _ = make_worker()
        _, fut = await worker.submit("релизь")
        with patch.object(persistent_cursor, "_RETRY_DELAY", 0):
            for _ in range(persistent_cursor._RETRY_LIMIT + 1):
                await worker._handle_update(chunk("Error: RetriableError: [resource_exhausted] Error"))
                await worker._handle_response({"id": stdin.sent[-1]["id"], "result": {}})
                if worker._retry_task:
                    await worker._retry_task
                    worker._retry_task = None
        prompts = [m for m in stdin.sent if m.get("method") == "session/prompt"]
        self.assertEqual(len(prompts), persistent_cursor._RETRY_LIMIT + 1)
        self.assertTrue(prompts[-1]["params"]["prompt"][0]["text"].endswith("релизь"))
        ok, text = await fut
        self.assertFalse(ok)
        self.assertIn("resource_exhausted", text)

    async def test_retry_recovers_turn(self) -> None:
        worker, stdin, _ = make_worker()
        _, fut = await worker.submit("x")
        with patch.object(persistent_cursor, "_RETRY_DELAY", 0):
            await worker._handle_update(chunk("Error: RetriableError: [resource_exhausted] Error"))
            await worker._handle_response({"id": stdin.sent[-1]["id"], "result": {}})
            await worker._retry_task
        await worker._handle_update(chunk("готово"))
        await worker._handle_response({"id": stdin.sent[-1]["id"], "result": {}})
        self.assertEqual(await fut, (True, "готово"))

    async def test_open_session_loads_known_or_creates_new(self) -> None:
        for exists, method in ((True, "session/load"), (False, "session/new")):
            worker, stdin, _ = make_worker()

            async def fake_call(m, params, timeout=0, _sent=stdin.sent):
                _sent.append({"method": m, "params": params})
                return {"sessionId": "acp-1"} if m == "session/new" else {}

            with patch.object(persistent_cursor, "acp_session_exists", return_value=exists), \
                 patch.object(worker, "_call", side_effect=fake_call):
                sid = await worker.open_session("sid", "SYS")
            self.assertEqual(stdin.sent[-1]["method"], method)
            self.assertEqual(sid, "sid" if exists else "acp-1")
            self.assertEqual(bool(worker._prefix), not exists)

    async def test_open_session_passes_topic_mcp_servers(self) -> None:
        worker, stdin, _ = make_worker()
        servers = [{"type": "http", "name": "qw", "url": "https://qw.test/mcp",
                    "headers": [{"name": "Authorization", "value": "Bearer t"}]}]

        async def fake_call(m, params, timeout=0):
            stdin.sent.append({"method": m, "params": params})
            return {"sessionId": "acp-1"}

        with patch.object(persistent_cursor, "acp_session_exists", return_value=False), \
             patch.object(persistent_cursor, "acp_mcp_servers", return_value=servers) as role_servers, \
             patch.object(worker, "_call", side_effect=fake_call):
            await worker.open_session("sid", None, "teamlead")
        role_servers.assert_called_once_with("teamlead")
        self.assertEqual(stdin.sent[-1]["params"]["mcpServers"], servers)

    async def test_history_replay_is_not_journaled(self) -> None:
        worker, _, published = make_worker()
        worker._loading = True
        await worker._handle_update(chunk("старое"))
        self.assertEqual(worker._text, "")
        self.assertEqual(published, [])


if __name__ == "__main__":
    unittest.main()
