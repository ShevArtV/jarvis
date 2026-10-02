from __future__ import annotations

import asyncio
import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bot import db as bot_db


def _load_mcp_server():
    """scripts/ не пакет — грузим MCP-сервер по пути."""
    path = Path(__file__).resolve().parent.parent / "scripts" / "jarvis_mcp_server.py"
    spec = importlib.util.spec_from_file_location("jarvis_mcp_server_abandon_test", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class AskUserAbandonTest(unittest.TestCase):
    """Клиент бросил ask_user (codex по tool_timeout) — вопрос закрывается,
    а не висит pending и не съедает следующее сообщение в топик."""

    def test_cancelled_ask_is_closed(self) -> None:
        mcp_server = _load_mcp_server()
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "bot_state.db")
            with patch.object(bot_db, "DB_PATH", db_path):
                bot_db.init_db()
            mcp_server._DB_PATH = Path(db_path)

            async def run() -> None:
                task = asyncio.create_task(mcp_server.ask_user(
                    question="Выкладывать?", thread_id=77, chat_id=-100,
                ))
                await asyncio.sleep(0.2)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task

            with patch.object(mcp_server, "_telegram_api", return_value={"message_id": 5}):
                asyncio.run(run())

            with bot_db.connect(db_path) as conn:
                status, polled_at = conn.execute(
                    "SELECT status, polled_at FROM ask_requests"
                ).fetchone()
            self.assertEqual(status, "timed_out")
            self.assertIsNotNone(polled_at)


if __name__ == "__main__":
    unittest.main()
