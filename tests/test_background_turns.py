from __future__ import annotations

import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from bot import background_turns
from engines.persistent_claude import PersistentClaudeWorker


def _assistant(text: str, parent: str | None = None) -> dict:
    return {
        "type": "assistant", "parent_tool_use_id": parent,
        "message": {"content": [{"type": "text", "text": text}]},
    }


def _result(text: str) -> dict:
    return {"type": "result", "subtype": "success", "result": text}


class _Stdin:
    def __init__(self) -> None:
        self.lines: list[bytes] = []

    def write(self, data: bytes) -> None:
        self.lines.append(data)

    async def drain(self) -> None:
        return None


def _worker() -> tuple[PersistentClaudeWorker, asyncio.StreamReader]:
    stdout = asyncio.StreamReader()
    proc = SimpleNamespace(stdout=stdout, stdin=_Stdin(), returncode=None)
    return PersistentClaudeWorker((1, 2), proc, "sid", "/tmp"), stdout


def _feed(stdout: asyncio.StreamReader, *events: dict) -> None:
    for ev in events:
        stdout.feed_data((json.dumps(ev) + "\n").encode())


async def _settle() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


class BackgroundTurnWorkerTest(unittest.TestCase):
    """Отчёт фонового субагента приходит отдельным ходом после ``result``
    (проверено на живом claude 2026-10-12): его нужно отдать боту, а не выбросить."""

    def test_turn_started_by_cli_reaches_callback(self) -> None:
        async def run() -> None:
            worker, stdout = _worker()
            started: list[asyncio.Future] = []
            worker.on_background_turn = started.append
            worker.reader_task = asyncio.create_task(worker._read_loop())

            _is_new, fut = await worker.submit("запусти субагента")
            _feed(stdout, _assistant("STARTED"), _result("STARTED"))
            self.assertEqual(await fut, (True, "STARTED"))

            # Шаги субагента после конца хода — не новый ход.
            _feed(stdout, _assistant("PONG", parent="toolu_1"))
            await _settle()
            self.assertEqual(started, [])
            self.assertFalse(worker.busy)

            _feed(stdout, _assistant("REPORT"), _result("REPORT"))
            await _settle()
            self.assertEqual(len(started), 1)
            self.assertEqual(await started[0], (True, "REPORT"))
            self.assertFalse(worker.busy)

            stdout.feed_eof()
            await worker.reader_task

        asyncio.run(run())

    def test_user_message_during_background_turn_is_appended(self) -> None:
        async def run() -> None:
            worker, stdout = _worker()
            started: list[asyncio.Future] = []
            worker.on_background_turn = started.append
            worker.reader_task = asyncio.create_task(worker._read_loop())

            _feed(stdout, _assistant("REPORT"))
            await _settle()
            self.assertTrue(worker.busy)

            is_new, fut = await worker.submit("а что там?")
            self.assertFalse(is_new)
            self.assertIs(fut, started[0])

            _feed(stdout, _result("REPORT + ответ"))
            self.assertEqual(await started[0], (True, "REPORT + ответ"))

            stdout.feed_eof()
            await worker.reader_task

        asyncio.run(run())


class _Journal:
    def __init__(self, chat, thread_id) -> None:
        self.lines: list[str] = []

    async def start(self) -> None:
        return None

    async def append(self, line: str) -> None:
        self.lines.append(line)


class AttachBackgroundTurnsTest(unittest.TestCase):
    def test_background_turn_is_delivered_to_topic(self) -> None:
        async def run() -> None:
            worker = SimpleNamespace(on_background_turn=None, on_intermediate=None)
            delivered: list[tuple] = []

            async def finish(chat, thread_id, journal, ok, text, engine, key) -> None:
                delivered.append((thread_id, ok, text, engine, key))

            with patch.object(background_turns, "ProgressJournal", _Journal):
                background_turns.attach_background_turns(
                    worker, "chat", 7, (1, 7), "claude", 5, finish,
                )
                fut = asyncio.get_running_loop().create_future()
                worker.on_background_turn(fut)
                await _settle()
                self.assertIsNotNone(worker.on_intermediate)
                fut.set_result((True, "REPORT"))
                await _settle()

            self.assertEqual(delivered, [(7, True, "REPORT", "claude", (1, 7))])
            self.assertIsNone(worker.on_intermediate)

        asyncio.run(run())

    def test_worker_without_support_is_left_alone(self) -> None:
        worker = SimpleNamespace()
        background_turns.attach_background_turns(worker, "chat", 7, (1, 7), "codex", 5, None)
        self.assertFalse(hasattr(worker, "on_background_turn"))


if __name__ == "__main__":
    unittest.main()
