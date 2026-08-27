from __future__ import annotations

import asyncio
import importlib.util
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

from bot import db as bot_db
from bot import delivery as bot_delivery
from bot.queues import claim_next_job
from bot.topics import resolve_job_notice_target


OLD_JOBS_SCHEMA = """
CREATE TABLE jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER NOT NULL,
    thread_id INTEGER NOT NULL,
    text TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'manager',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    claimed_at TEXT,
    finished_at TEXT,
    error TEXT,
    result_message_id INTEGER
)
"""

SERVICE_ENV = {
    "JARVIS_MANAGER_CHAT_ID": "-100",
    "JARVIS_MANAGER_THREAD_ID": "2338",
    "JARVIS_SECRETARY_CHAT_ID": "-100",
    "JARVIS_SECRETARY_THREAD_ID": "2338",
    "JARVIS_TEAMLEAD_CHAT_ID": "-100",
    "JARVIS_TEAMLEAD_THREAD_ID": "16376",
}


def _load_mcp_server():
    """scripts/ не пакет — грузим MCP-сервер по пути."""
    path = Path(__file__).resolve().parent.parent / "scripts" / "jarvis_mcp_server.py"
    spec = importlib.util.spec_from_file_location("jarvis_mcp_server_origin_test", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class JobOriginNoticeTest(unittest.TestCase):
    """Нотис об ответе на job уходит топику, который job делегировал.

    До 27.08.2026 адресат был зашит константой (роль teamlead), поэтому job,
    делегированный Менеджеру, будил ещё и Тимлида: тот вклинивался в чужую
    задачу и ронял сессию исполнителя (codex thread-store conflict).
    """

    def setUp(self) -> None:
        self.env_patcher = patch.dict("os.environ", SERVICE_ENV, clear=False)
        self.env_patcher.start()

    def tearDown(self) -> None:
        self.env_patcher.stop()

    def _fresh_db(self, tmp: str) -> str:
        db_path = str(Path(tmp) / "bot_state.db")
        with patch.object(bot_db, "DB_PATH", db_path):
            bot_db.init_db()
        return db_path

    def test_init_db_migrates_old_jobs_table(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "old.db")
            with sqlite3.connect(db_path) as conn:
                conn.execute(OLD_JOBS_SCHEMA)
            with patch.object(bot_db, "DB_PATH", db_path):
                bot_db.init_db()
            with sqlite3.connect(db_path) as conn:
                cols = [r[1] for r in conn.execute(
                    "PRAGMA table_info(jobs)"
                ).fetchall()]
        self.assertIn("origin_chat_id", cols)
        self.assertIn("origin_thread_id", cols)

    def test_target_falls_back_to_teamlead_without_origin(self) -> None:
        self.assertEqual(resolve_job_notice_target(None, None), (-100, 16376))
        self.assertEqual(resolve_job_notice_target(-100, None), (-100, 16376))

    def test_target_is_origin_topic_when_known(self) -> None:
        self.assertEqual(resolve_job_notice_target(-100, 453), (-100, 453))

    def test_manager_send_records_origin(self) -> None:
        mcp_server = _load_mcp_server()
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._fresh_db(tmp)
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, "
                    "engine, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (-100, 2338, "sid", "/tmp", "codex",
                     datetime.utcnow().isoformat()),
                )
            mcp_server._DB_PATH = Path(db_path)
            result = mcp_server.manager_send(
                thread_id=2338,
                text="Заведи карточку",
                chat_id=-100,
                origin_thread_id=16376,
                origin_chat_id=-100,
            )
            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    "SELECT origin_chat_id, origin_thread_id FROM jobs WHERE id = ?",
                    (result["job_id"],),
                ).fetchone()
        self.assertEqual(row, (-100, 16376))
        self.assertEqual(result["origin_thread_id"], 16376)

    def test_manager_send_without_origin_keeps_nulls(self) -> None:
        mcp_server = _load_mcp_server()
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._fresh_db(tmp)
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, "
                    "engine, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (-100, 2338, "sid", "/tmp", "codex",
                     datetime.utcnow().isoformat()),
                )
            mcp_server._DB_PATH = Path(db_path)
            result = mcp_server.manager_send(
                thread_id=2338, text="Без инициатора", chat_id=-100,
            )
            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    "SELECT origin_chat_id, origin_thread_id FROM jobs WHERE id = ?",
                    (result["job_id"],),
                ).fetchone()
        self.assertEqual(row, (None, None))

    def test_claim_next_job_returns_origin(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._fresh_db(tmp)
            with patch.object(bot_db, "DB_PATH", db_path):
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        "INSERT INTO jobs(chat_id, thread_id, text, source, "
                        "status, created_at, origin_chat_id, origin_thread_id) "
                        "VALUES (?, ?, ?, 'manager', 'pending', ?, ?, ?)",
                        (-100, 2338, "текст", datetime.utcnow().isoformat(),
                         -100, 16376),
                    )
                job = claim_next_job()
        self.assertIsNotNone(job)
        self.assertEqual(job["origin_chat_id"], -100)
        self.assertEqual(job["origin_thread_id"], 16376)

    def _notice_to(self, db_path: str, target):
        """Отправить нотис и вернуть (thread_id доставки, thread_id auto-kick)."""
        chat = AsyncMock()
        sent = AsyncMock()
        sent.message_id = 777
        app = AsyncMock()
        app.bot.get_chat.return_value = chat
        with patch.object(bot_db, "DB_PATH", db_path), \
                patch.object(bot_delivery, "send_to_topic",
                             AsyncMock(return_value=sent)) as send_mock:
            asyncio.run(bot_delivery._send_manager_notice(
                app, "📨 job #1: новый ответ", kind="job_notification",
                target_role="teamlead", target=target,
            ))
        delivered_thread = send_mock.await_args.args[1]
        with sqlite3.connect(db_path) as conn:
            kick = conn.execute(
                "SELECT chat_id, thread_id FROM jobs WHERE source='self_notice'"
            ).fetchall()
        return delivered_thread, kick

    def test_notice_goes_to_origin_not_teamlead(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._fresh_db(tmp)
            delivered, kick = self._notice_to(db_path, (-100, 2338))
        self.assertEqual(delivered, 2338)
        self.assertEqual(kick, [(-100, 2338)])

    def test_notice_without_target_uses_role(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._fresh_db(tmp)
            delivered, kick = self._notice_to(db_path, None)
        self.assertEqual(delivered, 16376)
        self.assertEqual(kick, [(-100, 16376)])


if __name__ == "__main__":
    unittest.main()
