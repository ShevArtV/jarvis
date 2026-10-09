"""Строго один живой процесс на топик.

Раньше вторая сессия появлялась двумя путями: job Менеджера в persistent-топике
запускал разовый `--resume` рядом с живым процессом, а два почти одновременных
хода успевали поднять по процессу каждый (запуск — await на секунды).
"""

from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from bot import jobs, topics
from bot.handlers import messages

KEY = (-100, 7)


class FakeWorker:
    def __init__(self) -> None:
        self.dead = False
        self.busy = False
        self.steerable = True
        self.pending_future = None
        self.on_intermediate = None
        self.session_id = "sid"
        self.submitted: list[str] = []
        self.proc = type("P", (), {"returncode": None})()

    async def submit(self, text: str, *, exclusive: bool = False):
        if self.busy and (exclusive or not self.steerable):
            return False, None
        is_new = not self.busy
        if is_new:
            self.steerable = not exclusive
        self.busy = True
        self.submitted.append(text)
        fut = asyncio.get_running_loop().create_future()
        fut.set_result((True, "done: " + text))
        return is_new, fut


class SingleWorkerTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        topics.persistent_workers.clear()
        topics.persistent_spawn_locks.clear()
        self.addCleanup(topics.persistent_workers.clear)
        self.addCleanup(topics.persistent_spawn_locks.clear)

    async def test_concurrent_turns_spawn_one_worker(self) -> None:
        spawned: list[FakeWorker] = []

        async def slow_start(_engine, **_kwargs):
            await asyncio.sleep(0.05)
            worker = FakeWorker()
            spawned.append(worker)
            return worker

        with patch.object(messages, "ensure_active_session",
                          return_value=("sid", "/tmp", "claude", False)), \
             patch.object(messages, "get_persistent_for_engine", return_value=True), \
             patch.object(messages, "get_model", return_value=None), \
             patch.object(messages, "get_account", return_value="main"), \
             patch.object(messages, "get_mcp_playwright", return_value=False), \
             patch.object(messages, "resolve_topic_role", return_value="agent"), \
             patch.object(messages, "build_system_prefix", return_value=""), \
             patch.object(messages, "start_persistent", side_effect=slow_start):
            results = await asyncio.gather(*(
                messages._get_or_start_persistent_worker(None, KEY[1], KEY)
                for _ in range(3)
            ))

        self.assertEqual(len(spawned), 1)
        self.assertTrue(all(worker is spawned[0] for worker, _ in results))
        self.assertEqual([s for _, s in results].count(True), 1)
        self.assertIs(topics.persistent_workers[KEY], spawned[0])

    async def test_job_waits_for_running_turn_and_uses_live_worker(self) -> None:
        worker = FakeWorker()
        worker.busy = True
        topics.persistent_workers[KEY] = worker

        async def finish_running_turn() -> None:
            await asyncio.sleep(0.05)
            self.assertEqual(worker.submitted, [])  # job не влез довеском
            worker.busy = False

        with patch.object(jobs, "get_session", return_value=("sid", "/tmp", "claude")), \
             patch.object(jobs, "call_llm_stream", new=AsyncMock()) as one_shot:
            _, (ok, text) = await asyncio.gather(
                finish_running_turn(),
                jobs._run_job_turn_persistent(None, KEY[1], KEY, "job prompt", None),
            )

        self.assertTrue(ok)
        self.assertEqual(text, "done: job prompt")
        self.assertEqual(worker.submitted, ["job prompt"])
        one_shot.assert_not_called()


class TurnWaitTest(unittest.IsolatedAsyncioTestCase):
    """QW-триггер (allow_silent) и сообщение пользователя не смешиваются в
    одном ходе живого процесса: второй ждёт конца первого."""

    def setUp(self) -> None:
        self.sent: list[str] = []
        self.replies: list[str] = []

        async def fake_send(_chat, _thread, text, *a, **k):
            self.sent.append(text)

        async def fake_finish(_chat, _thread, _journal, _ok, text, *a, **k):
            self.replies.append(text)

        journal = AsyncMock()
        self.patches = [
            patch.object(messages, "send_to_topic", side_effect=fake_send),
            patch.object(messages, "_finish_turn_reply", side_effect=fake_finish),
            patch.object(messages, "ProgressJournal", return_value=journal),
            patch.object(messages, "get_session", return_value=("sid", "/tmp", "codex")),
        ]
        for p in self.patches:
            p.start()
            self.addCleanup(p.stop)

    async def _run_with_busy_worker(self, steerable: bool, allow_silent: bool):
        worker = FakeWorker()
        worker.busy, worker.steerable = True, steerable
        running = asyncio.get_running_loop().create_future()
        worker.pending_future = running

        async def finish_running_turn() -> None:
            await asyncio.sleep(0.05)
            self.assertEqual(worker.submitted, [])  # не влез довеском
            worker.busy = False
            running.set_result((True, "первый ход"))

        with patch.object(messages, "_get_or_start_persistent_worker",
                          new=AsyncMock(return_value=(worker, False))):
            await asyncio.gather(
                finish_running_turn(),
                messages._handle_persistent_message(
                    None, KEY[1], KEY, "второе", "", allow_silent=allow_silent),
            )
        return worker

    async def test_qw_trigger_waits_for_user_turn(self) -> None:
        worker = await self._run_with_busy_worker(steerable=True, allow_silent=True)
        self.assertEqual(len(worker.submitted), 1)
        self.assertFalse(worker.steerable)  # в ход триггера не допишут
        self.assertEqual(self.sent, [])
        self.assertEqual(len(self.replies), 1)

    async def test_user_message_waits_for_qw_turn(self) -> None:
        worker = await self._run_with_busy_worker(steerable=False, allow_silent=False)
        self.assertEqual(len(worker.submitted), 1)
        self.assertTrue(worker.steerable)
        self.assertEqual(len(self.sent), 1)
        self.assertIn("отвечу следующим ходом", self.sent[0])
        self.assertEqual(len(self.replies), 1)

    async def test_user_message_still_joins_user_turn(self) -> None:
        worker = FakeWorker()
        worker.busy = True
        with patch.object(messages, "_get_or_start_persistent_worker",
                          new=AsyncMock(return_value=(worker, False))):
            await messages._handle_persistent_message(None, KEY[1], KEY, "ещё", "")
        self.assertEqual(len(worker.submitted), 1)
        self.assertIn("Добавил", self.sent[0])
        self.assertEqual(self.replies, [])


if __name__ == "__main__":
    unittest.main()
