from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from bot import db as bot_db
from bot.asks import get_recent_timed_out_ask, mark_ask_late_answered


class AsksGraceTest(unittest.TestCase):
    """Grace-окно: ответ текстом на вопрос, истёкший по таймауту, не должен
    теряться бесследно (см. ask_user в scripts/jarvis_mcp_server.py)."""

    def _fresh_db(self, tmp: str) -> str:
        db_path = str(Path(tmp) / "bot_state.db")
        with patch.object(bot_db, "DB_PATH", db_path):
            bot_db.init_db()
        return db_path

    def _add_ask(
        self, db_path: str, status: str, created_at: datetime,
        chat_id: int = -100, thread_id: int = 77, question: str = "Сносить таблицу?",
        answered_at: datetime | None = None,
    ) -> int:
        with sqlite3.connect(db_path) as conn:
            cur = conn.execute(
                "INSERT INTO ask_requests(chat_id, thread_id, question, status, "
                "created_at, answered_at) VALUES (?, ?, ?, ?, ?, ?)",
                (chat_id, thread_id, question, status, created_at.isoformat(),
                 answered_at.isoformat() if answered_at else None),
            )
            return cur.lastrowid

    def test_window_counts_from_expiry_not_creation(self) -> None:
        """Вопрос ждал ответа полчаса и истёк только что — grace действует.

        По created_at такой вопрос выпадал бы из окна всегда: сам таймаут
        ожидания (до 1800 с) длиннее grace-окна.
        """
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._fresh_db(tmp)
            ask_id = self._add_ask(
                db_path, "timed_out",
                created_at=datetime.utcnow() - timedelta(minutes=35),
                answered_at=datetime.utcnow() - timedelta(minutes=2),
            )
            with patch.object(bot_db, "DB_PATH", db_path):
                ask = get_recent_timed_out_ask(-100, 77)
            self.assertIsNotNone(ask)
            self.assertEqual(ask["id"], ask_id)

    def test_finds_recent_timed_out_ask(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._fresh_db(tmp)
            ask_id = self._add_ask(
                db_path, "timed_out", datetime.utcnow() - timedelta(minutes=5),
            )
            with patch.object(bot_db, "DB_PATH", db_path):
                ask = get_recent_timed_out_ask(-100, 77)
            self.assertIsNotNone(ask)
            self.assertEqual(ask["id"], ask_id)

    def test_ignores_ask_older_than_grace_window(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._fresh_db(tmp)
            self._add_ask(
                db_path, "timed_out", datetime.utcnow() - timedelta(minutes=40),
            )
            with patch.object(bot_db, "DB_PATH", db_path):
                ask = get_recent_timed_out_ask(-100, 77)
            self.assertIsNone(ask)

    def test_ignores_answered_ask(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._fresh_db(tmp)
            self._add_ask(
                db_path, "answered", datetime.utcnow() - timedelta(minutes=1),
            )
            with patch.object(bot_db, "DB_PATH", db_path):
                ask = get_recent_timed_out_ask(-100, 77)
            self.assertIsNone(ask)

    def test_mark_ask_late_answered_transitions_status_and_returns_true(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._fresh_db(tmp)
            ask_id = self._add_ask(
                db_path, "timed_out", datetime.utcnow() - timedelta(minutes=5),
            )
            with patch.object(bot_db, "DB_PATH", db_path):
                self.assertTrue(mark_ask_late_answered(ask_id, "да, сносить"))
            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    "SELECT status, answer, via FROM ask_requests WHERE id = ?",
                    (ask_id,),
                ).fetchone()
            self.assertEqual(row[0], "answered_late")
            self.assertEqual(row[1], "да, сносить")
            self.assertEqual(row[2], "text_late")

    def test_mark_ask_late_answered_is_not_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._fresh_db(tmp)
            ask_id = self._add_ask(
                db_path, "timed_out", datetime.utcnow() - timedelta(minutes=5),
            )
            with patch.object(bot_db, "DB_PATH", db_path):
                self.assertTrue(mark_ask_late_answered(ask_id, "первый ответ"))
                self.assertFalse(mark_ask_late_answered(ask_id, "второй ответ"))


if __name__ == "__main__":
    unittest.main()
