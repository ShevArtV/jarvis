from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bot import db as bot_db
from bot import sessions as bot_sessions


# Схема "до миграции" — persistent_claude/persistent_codex без NOT NULL,
# чтобы можно было вставить NULL и проверить трактовку значения.
LEGACY_SESSIONS_SCHEMA = """
CREATE TABLE sessions (
    chat_id INTEGER NOT NULL,
    thread_id INTEGER NOT NULL DEFAULT 0,
    session_id TEXT NOT NULL,
    cwd TEXT,
    engine TEXT NOT NULL DEFAULT 'claude',
    updated_at TEXT NOT NULL,
    persistent_claude INTEGER,
    persistent_codex INTEGER,
    PRIMARY KEY (chat_id, thread_id)
)
"""


class PersistentDefaultOnTest(unittest.TestCase):
    """Persistent по умолчанию включён (решение оператора 2026-09-05) для
    движков, которые его поддерживают; выключается только явным /persistent
    off. См. bot/sessions.py:get_persistent_for_engine."""

    def _fresh_db(self, tmp: str) -> str:
        db_path = str(Path(tmp) / "bot_state.db")
        with patch.object(bot_db, "DB_PATH", db_path):
            bot_db.init_db()
        return db_path

    def test_missing_topic_row_is_persistent_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._fresh_db(tmp)
            with patch.object(bot_db, "DB_PATH", db_path):
                self.assertTrue(bot_sessions.get_persistent_for_engine(1, 1, "claude"))
                self.assertTrue(bot_sessions.get_persistent_for_engine(1, 1, "codex"))

    def test_null_column_value_is_persistent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "legacy.db")
            with sqlite3.connect(db_path) as conn:
                conn.execute(LEGACY_SESSIONS_SCHEMA)
                conn.execute(
                    "INSERT INTO sessions(chat_id, thread_id, session_id, engine, "
                    "updated_at, persistent_claude, persistent_codex) "
                    "VALUES (1, 1, 'sid', 'claude', '2026-01-01', NULL, NULL)"
                )
            with patch.object(bot_db, "DB_PATH", db_path):
                self.assertTrue(bot_sessions.get_persistent_for_engine(1, 1, "claude"))
                self.assertTrue(bot_sessions.get_persistent_for_engine(1, 1, "codex"))

    def test_explicit_zero_is_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._fresh_db(tmp)
            with patch.object(bot_db, "DB_PATH", db_path):
                bot_sessions.set_persistent_for_engine(1, 1, "claude", False)
                self.assertFalse(bot_sessions.get_persistent_for_engine(1, 1, "claude"))
                # codex-колонку не трогали — остаётся включённой по умолчанию.
                self.assertTrue(bot_sessions.get_persistent_for_engine(1, 1, "codex"))

    def test_unsupported_engine_is_never_persistent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._fresh_db(tmp)
            with patch.object(bot_db, "DB_PATH", db_path):
                self.assertFalse(bot_sessions.get_persistent_for_engine(1, 1, "opencode"))

    def test_schema_columns_default_to_one(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._fresh_db(tmp)
            with sqlite3.connect(db_path) as conn:
                defaults = {
                    row[1]: row[4]
                    for row in conn.execute("PRAGMA table_info(sessions)").fetchall()
                }
            self.assertEqual(defaults["persistent_claude"], "1")
            self.assertEqual(defaults["persistent_codex"], "1")

    def test_init_db_backfills_existing_rows_to_enabled(self) -> None:
        """До миграции часть боевых топиков стояла на persistent=0 — оператор
        решил включить всем, кто поддерживает persistent."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "legacy.db")
            with sqlite3.connect(db_path) as conn:
                conn.execute(LEGACY_SESSIONS_SCHEMA)
                conn.execute(
                    "INSERT INTO sessions(chat_id, thread_id, session_id, engine, "
                    "updated_at, persistent_claude, persistent_codex) "
                    "VALUES (1, 1, 'sid', 'claude', '2026-01-01', 0, 0)"
                )
            with patch.object(bot_db, "DB_PATH", db_path):
                bot_db.init_db()
            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    "SELECT persistent_claude, persistent_codex FROM sessions "
                    "WHERE chat_id = 1 AND thread_id = 1"
                ).fetchone()
            self.assertEqual(row, (1, 1))

    def test_backfill_migration_does_not_repeat_on_restart(self) -> None:
        """Повторный init_db (рестарт бота) не должен возвращать в 1 топик,
        который оператор явно выключил ПОСЛЕ одноразового бэкфилла."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._fresh_db(tmp)
            with patch.object(bot_db, "DB_PATH", db_path):
                bot_sessions.set_persistent_for_engine(1, 1, "claude", False)
                bot_db.init_db()
                self.assertFalse(bot_sessions.get_persistent_for_engine(1, 1, "claude"))


if __name__ == "__main__":
    unittest.main()
