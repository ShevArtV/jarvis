"""Reaper не убивает процесс с фоновой работой; правка инструкций не рвёт сеанс."""

from __future__ import annotations

import asyncio
import contextlib
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bot import db as bot_db
from bot import sessions
from bot import workers as bot_workers
from engines import background_work
from engines.persistent_claude import PersistentClaudeWorker


def _idle_worker(**kw) -> SimpleNamespace:
    return SimpleNamespace(busy=False, last_activity=time.monotonic() - 3600, **kw)


def _run_reaper_once(workers: dict) -> AsyncMock:
    """Один проход reaper'а: второй sleep обрывает цикл."""
    kill = AsyncMock()
    sleeps = iter([None])

    async def fake_sleep(_s):
        try:
            next(sleeps)
        except StopIteration:
            raise asyncio.CancelledError from None

    with patch.object(bot_workers, "persistent_workers", workers), \
            patch.object(bot_workers, "_kill_persistent_worker", kill), \
            patch.object(bot_workers, "touch_session"), \
            patch.object(bot_workers.asyncio, "sleep", fake_sleep):
        with contextlib.suppress(asyncio.CancelledError):
            asyncio.run(bot_workers.persistent_reaper(None))
    return kill


class ReaperBackgroundTest(unittest.TestCase):
    def test_kills_plain_idle_worker(self) -> None:
        w = _idle_worker(has_background_work=lambda: False)
        kill = _run_reaper_once({(1, 2): w})
        kill.assert_awaited_once_with((1, 2), "")

    def test_keeps_worker_with_background_work(self) -> None:
        w = _idle_worker(has_background_work=lambda: True)
        kill = _run_reaper_once({(1, 2): w})
        kill.assert_not_awaited()

    def test_background_hold_is_capped(self) -> None:
        w = _idle_worker(has_background_work=lambda: True)
        w.last_activity = time.monotonic() - (bot_workers.BACKGROUND_MAX_HOURS * 3600 + 60)
        kill = _run_reaper_once({(1, 2): w})
        kill.assert_awaited_once()


class ClaudeBackgroundTasksTest(unittest.TestCase):
    def test_tracks_background_tasks_changed(self) -> None:
        events = [
            b'{"type":"system","subtype":"background_tasks_changed",'
            b'"tasks":[{"task_id":"t1","task_type":"local_agent"}]}\n',
        ]

        async def run() -> PersistentClaudeWorker:
            reader = asyncio.StreamReader()
            for e in events:
                reader.feed_data(e)
            proc = SimpleNamespace(stdout=reader, stdin=None, stderr=None)
            w = PersistentClaudeWorker((1, 2), proc, "sid", "/tmp")
            task = asyncio.create_task(w._read_loop())
            await asyncio.sleep(0.05)
            self.assertTrue(w.has_background_work())
            reader.feed_data(b'{"type":"system","subtype":"background_tasks_changed","tasks":[]}\n')
            await asyncio.sleep(0.05)
            self.assertFalse(w.has_background_work())
            reader.feed_eof()
            await task
            return w

        asyncio.run(run())


@unittest.skipUnless(sys.platform.startswith("linux"), "/proc only")
class ShellChildrenTest(unittest.TestCase):
    def test_new_shell_child_counts_as_background_work(self) -> None:
        # Сам тест — «процесс движка»: его дети видны в /proc.
        w = SimpleNamespace(proc=SimpleNamespace(pid=os.getpid()))
        background_work.capture_baseline(w)
        self.assertFalse(background_work.has_background_work(w))
        child = subprocess.Popen(["sh", "-c", "sleep 5"])
        try:
            time.sleep(0.1)
            self.assertTrue(background_work.has_background_work(w))
        finally:
            child.kill()
            child.wait()
        self.assertFalse(background_work.has_background_work(w))

    def test_non_shell_child_is_ignored(self) -> None:
        w = SimpleNamespace(proc=SimpleNamespace(pid=os.getpid()))
        background_work.capture_baseline(w)
        child = subprocess.Popen(["sleep", "5"])
        try:
            time.sleep(0.1)
            self.assertFalse(background_work.has_background_work(w))
        finally:
            child.kill()
            child.wait()


class InstructionsChangeKeepsSessionTest(unittest.TestCase):
    def test_session_kept_and_note_queued(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "bot_state.db")
            cwd = Path(tmp) / "topic"
            cwd.mkdir()
            with patch.object(bot_db, "DB_PATH", db_path):
                bot_db.init_db()
                sid, *_ = sessions.ensure_active_session(-1, 7)
                with bot_db._db() as conn:
                    conn.execute(
                        "UPDATE sessions SET cwd = ?, session_started_at = ? "
                        "WHERE chat_id = -1 AND thread_id = 7",
                        (str(cwd), "2000-01-01T00:00:00"),
                    )
                (cwd / "AGENTS.md").write_text("new rules")

                sid2, _cwd, _eng, opened_new = sessions.ensure_active_session(-1, 7)
                self.assertEqual(sid2, sid)
                self.assertFalse(opened_new)
                self.assertEqual(sessions.pop_instructions_note(-1, 7),
                                 sessions.INSTRUCTIONS_NOTE)
                self.assertIsNone(sessions.pop_instructions_note(-1, 7))

                # Метка старта сдвинута — повторно напоминание не ставится.
                sessions.ensure_active_session(-1, 7)
                self.assertIsNone(sessions.pop_instructions_note(-1, 7))


if __name__ == "__main__":
    unittest.main()
