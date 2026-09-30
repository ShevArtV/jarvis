"""Склейка внешних триггеров (coalesce) и молчаливый ответ ([[SILENT]]).

БД — временный файл, Telegram мокается: живую bot_state.db тесты не трогают.
"""

from __future__ import annotations

import asyncio
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

from bot import db as bot_db
from bot import queues
from bot.handlers import messages

TOPIC = (-1001, 77)
OTHER = (-1001, 88)
QW = {"queuewarden": 60}


class ClaimCoalesceTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self._tmp.name) / "bot_state.db")
        self._db_patch = patch.object(bot_db, "DB_PATH", self.db_path)
        self._db_patch.start()
        bot_db.init_db()

    def tearDown(self) -> None:
        self._db_patch.stop()
        self._tmp.cleanup()

    def add(self, text: str, source: str = "queuewarden", topic=TOPIC,
            age: float = 120) -> int:
        tid = queues.enqueue_agent_trigger(*topic, text, source)
        created = (datetime.utcnow() - timedelta(seconds=age)).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE agent_triggers SET created_at = ? WHERE id = ?",
                         (created, tid))
        return tid

    def status(self, tid: int) -> str:
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute("SELECT status FROM agent_triggers WHERE id = ?",
                                (tid,)).fetchone()[0]

    def test_young_series_waits(self) -> None:
        self.add("a", age=10)
        self.assertIsNone(queues.claim_next_agent_trigger(coalesce=QW))

    def test_aged_series_claimed_together_in_order(self) -> None:
        a = self.add("a", age=120)
        b = self.add("b", age=30)
        c = self.add("c", age=1)
        other = self.add("x", topic=OTHER, age=100)
        mxb = self.add("m", source="mxboard", age=110)
        trig = queues.claim_next_agent_trigger(coalesce=QW)
        self.assertEqual(trig["ids"], [a, b, c])
        self.assertEqual(trig["texts"], ["a", "b", "c"])
        self.assertEqual(trig["id"], a)
        for tid in (a, b, c):
            self.assertEqual(self.status(tid), "in_progress")
        for tid in (other, mxb):
            self.assertEqual(self.status(tid), "pending")

    def test_other_sources_not_delayed_or_merged(self) -> None:
        a = self.add("m1", source="mxboard", age=1)
        self.add("m2", source="mxboard", age=0)
        trig = queues.claim_next_agent_trigger(coalesce=QW)
        self.assertEqual(trig["ids"], [a])

    def test_without_coalesce_one_row(self) -> None:
        a = self.add("a", age=1)
        self.add("b", age=0)
        self.assertEqual(queues.claim_next_agent_trigger()["ids"], [a])


class _Journal:
    total_steps = 3

    def __init__(self) -> None:
        self.finished: list[tuple] = []

    async def finish(self, final_text=None, discard=False) -> None:
        self.finished.append((final_text, discard))


class SilentReplyTest(unittest.TestCase):
    def run_finish(self, text: str, allow_silent: bool, ok: bool = True):
        journal = _Journal()
        send = AsyncMock()
        with patch.object(messages, "send_claude_reply", send), \
                patch.object(messages, "get_session", return_value=("s", "/", "claude")), \
                patch.object(messages, "_warn_large_context_if_needed", AsyncMock()), \
                patch.object(messages, "_ask_done_confirmation_if_needed", AsyncMock()):
            asyncio.run(messages._finish_turn_reply(
                object(), 1, journal, ok, text, "claude", TOPIC,
                allow_silent=allow_silent,
            ))
        return journal, send

    def test_silent_marker_sends_nothing_and_drops_journal(self) -> None:
        journal, send = self.run_finish("  [[SILENT]]\n", allow_silent=True)
        send.assert_not_awaited()
        self.assertEqual(journal.finished, [(None, True)])

    def test_marker_with_text_sends_text_only(self) -> None:
        journal, send = self.run_finish("Доклад\n[[SILENT]]", allow_silent=True)
        self.assertEqual(send.await_args.args[2], "Доклад")

    def test_marker_ignored_without_permission(self) -> None:
        _journal, send = self.run_finish("[[SILENT]]", allow_silent=False)
        self.assertEqual(send.await_args.args[2], "[[SILENT]]")


if __name__ == "__main__":
    unittest.main()
