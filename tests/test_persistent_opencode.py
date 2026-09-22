from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace

from engines.persistent_opencode import PersistentOpenCodeWorker, _model_ref

SID = "ses_test"


def _worker() -> PersistentOpenCodeWorker:
    worker = PersistentOpenCodeWorker(
        (1, 2), SimpleNamespace(returncode=None), "http://127.0.0.1:1",
        SID, "/tmp", None, "system",
    )
    sent: list[str] = []

    async def fake_prompt(text: str) -> None:
        sent.append(text)

    worker._prompt = fake_prompt  # type: ignore[method-assign]
    worker.sent = sent  # type: ignore[attr-defined]
    return worker


def _message(mid: str, role: str) -> dict:
    return {"type": "message.updated",
            "properties": {"sessionID": SID, "info": {"id": mid, "role": role}}}


def _text(pid: str, mid: str, text: str, ended: bool = True) -> dict:
    part = {"id": pid, "messageID": mid, "sessionID": SID, "type": "text", "text": text}
    if ended:
        part["time"] = {"start": 1, "end": 2}
    return {"type": "message.part.updated", "properties": {"sessionID": SID, "part": part}}


IDLE = {"type": "session.idle", "properties": {"sessionID": SID}}
BUSY = {"type": "session.status", "properties": {"sessionID": SID, "status": {"type": "busy"}}}


class PersistentOpenCodeTest(unittest.TestCase):
    def test_turn_resolves_with_last_assistant_text_only(self) -> None:
        async def run() -> None:
            worker = _worker()
            is_new, fut = await worker.submit("задача")
            self.assertTrue(is_new)
            for ev in (
                _message("msg_u1", "user"), _text("prt_u1", "msg_u1", "задача"),
                BUSY,
                _message("msg_a1", "assistant"), _text("prt_a1", "msg_a1", "промежуточный"),
                _message("msg_u2", "user"), _text("prt_u2", "msg_u2", "дополнение"),
                _message("msg_a2", "assistant"), _text("prt_a2", "msg_a2", "итог"),
                IDLE,
            ):
                await worker._handle_event(ev)
            self.assertEqual(fut.result(), (True, "итог"))
            self.assertFalse(worker.busy)

        asyncio.run(run())

    def test_message_during_turn_joins_it(self) -> None:
        async def run() -> None:
            worker = _worker()
            _new, fut = await worker.submit("первое")
            is_new, fut2 = await worker.submit("второе")
            self.assertFalse(is_new)
            self.assertIs(fut, fut2)
            self.assertEqual(worker.sent, ["первое", "второе"])

        asyncio.run(run())

    def test_idle_before_turn_started_is_ignored(self) -> None:
        async def run() -> None:
            worker = _worker()
            _new, fut = await worker.submit("задача")
            await worker._handle_event(IDLE)
            self.assertFalse(fut.done())
            self.assertTrue(worker.busy)

        asyncio.run(run())

    def test_other_session_events_are_ignored(self) -> None:
        async def run() -> None:
            worker = _worker()
            _new, fut = await worker.submit("задача")
            await worker._handle_event(BUSY)
            await worker._handle_event(
                {"type": "session.idle", "properties": {"sessionID": "ses_other"}}
            )
            self.assertFalse(fut.done())

        asyncio.run(run())

    def test_session_error_fails_turn(self) -> None:
        async def run() -> None:
            worker = _worker()
            _new, fut = await worker.submit("задача")
            await worker._handle_event({
                "type": "session.error",
                "properties": {"sessionID": SID,
                               "error": {"name": "X", "data": {"message": "Model not found"}}},
            })
            await worker._handle_event(IDLE)
            self.assertEqual(fut.result(), (False, "Model not found"))

        asyncio.run(run())

    def test_journal_gets_finished_parts_not_user_text(self) -> None:
        async def run() -> None:
            worker = _worker()
            published: list[str] = []

            async def collect(text: str) -> None:
                published.append(text)

            worker.on_intermediate = collect
            await worker.submit("задача")
            for ev in (
                _message("msg_u1", "user"), _text("prt_u1", "msg_u1", "задача"),
                _message("msg_a1", "assistant"),
                _text("prt_a1", "msg_a1", "пи", ended=False),
                _text("prt_a1", "msg_a1", "пишу ответ"),
            ):
                await worker._handle_event(ev)
            await worker._flush(force=True)
            self.assertEqual(published, ["пишу ответ"])

        asyncio.run(run())

    def test_model_ref(self) -> None:
        self.assertEqual(
            _model_ref("deepseek/deepseek-v4-pro"),
            {"providerID": "deepseek", "modelID": "deepseek-v4-pro"},
        )
        self.assertEqual(
            _model_ref("openrouter/anthropic/claude"),
            {"providerID": "openrouter", "modelID": "anthropic/claude"},
        )


if __name__ == "__main__":
    unittest.main()
