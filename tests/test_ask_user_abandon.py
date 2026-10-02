from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bot import db as bot_db
from mcp_server import common
from mcp_server.tools import asks


class AskUserAbandonTest(unittest.TestCase):
    """Клиент бросил ask_user (codex по tool_timeout) — вопрос закрывается,
    а не висит pending и не съедает следующее сообщение в топик."""

    def setUp(self) -> None:
        # Модуль сервера общий на все тесты — состояние возвращаем на место.
        for name, value in (("_DB_PATH", None), ("_JOBS_ORIGIN_COLS", None)):
            patcher = patch.object(common, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_cancelled_ask_is_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "bot_state.db")
            with patch.object(bot_db, "DB_PATH", db_path):
                bot_db.init_db()
            common._DB_PATH = Path(db_path)

            async def run() -> None:
                task = asyncio.create_task(asks.ask_user(
                    question="Выкладывать?", thread_id=77, chat_id=-100,
                ))
                await asyncio.sleep(0.2)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task

            with patch.object(common, "_telegram_api", return_value={"message_id": 5}):
                asyncio.run(run())

            with bot_db.connect(db_path) as conn:
                status, polled_at = conn.execute(
                    "SELECT status, polled_at FROM ask_requests"
                ).fetchone()
            self.assertEqual(status, "timed_out")
            self.assertIsNotNone(polled_at)


if __name__ == "__main__":
    unittest.main()
