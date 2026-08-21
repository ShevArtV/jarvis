from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from bot.handlers import messages
from bot.topics import persistent_start_locks, persistent_workers
from engines.persistent_codex import PersistentCodexWorker


class PersistentCodexJournalTest(unittest.TestCase):
    def test_delta_fragments_are_not_published_to_journal(self) -> None:
        async def run() -> None:
            published: list[str] = []

            async def collect(text: str) -> None:
                published.append(text)

            worker = PersistentCodexWorker(
                (1, 2),
                SimpleNamespace(returncode=None),
                "thread-1",
                "/tmp",
                None,
            )
            worker.on_intermediate = collect

            await worker._handle_notification({
                "method": "item/agentMessage/delta",
                "params": {"delta": "Од"},
            })
            await worker._handle_notification({
                "method": "item/reasoning/textDelta",
                "params": {"delta": "обр"},
            })

            self.assertEqual(published, [])

            await worker._handle_notification({
                "method": "item/completed",
                "params": {
                    "item": {
                        "type": "agentMessage",
                        "text": "Одобрение получил.",
                    },
                },
            })

            self.assertEqual(published, ["Одобрение получил."])

        asyncio.run(run())

    def test_concurrent_persistent_entries_start_one_codex_writer(self) -> None:
        """A second entry waits for thread/resume and then steers that worker."""
        async def run() -> None:
            key = (1, 2)
            persistent_workers.pop(key, None)
            persistent_start_locks.pop(key, None)
            started = asyncio.Event()
            release_start = asyncio.Event()
            starts = 0

            class FakeWorker:
                dead = False
                proc = SimpleNamespace(returncode=None)
                session_id = "thread-1"

                async def submit(self, prompt: str):
                    return False, asyncio.get_running_loop().create_future()

            async def start_worker(**kwargs):
                nonlocal starts
                starts += 1
                started.set()
                await release_start.wait()
                return FakeWorker()

            with (
                patch.object(messages, "ensure_active_session", return_value=("thread-1", "/tmp", "codex", False)),
                patch.object(messages, "get_persistent_for_engine", return_value=True),
                patch.object(messages, "_persistent_column_for_engine", return_value="persistent_codex"),
                patch.object(messages, "get_model", return_value=None),
                patch.object(messages, "get_pending_summary", return_value=None),
                patch.object(messages, "get_mcp_playwright", return_value=False),
                patch.object(messages, "resolve_topic_role", return_value="teamlead"),
                patch.object(messages, "build_system_prefix", return_value="system"),
                patch.object(messages, "start_persistent_codex", side_effect=start_worker),
                patch.object(messages, "send_to_topic", new=AsyncMock()),
            ):
                first = asyncio.create_task(
                    messages._handle_persistent_message(SimpleNamespace(), 2, key, "first", ""),
                )
                await started.wait()
                second = asyncio.create_task(
                    messages._handle_persistent_message(SimpleNamespace(), 2, key, "second", ""),
                )
                await asyncio.sleep(0)
                release_start.set()
                await asyncio.gather(first, second)

            self.assertEqual(starts, 1)
            self.assertIn(key, persistent_workers)
            persistent_workers.pop(key, None)
            persistent_start_locks.pop(key, None)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
