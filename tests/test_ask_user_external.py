from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bot import db as bot_db
from bot.timeutil import utcnow
from mcp_server import common
from mcp_server.tools import asks

OLD_TRIGGERS_SCHEMA = """
CREATE TABLE agent_triggers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER NOT NULL,
    thread_id INTEGER NOT NULL,
    text TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'tracker',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    claimed_at TEXT,
    finished_at TEXT,
    error TEXT,
    result_message_id INTEGER
)
"""

TRIGGER_TEXT = (
    "Задача #482 «Починить корзину» переведена в стадию «В работе» (operator) — "
    "ты исполнитель, следующий ход твой."
)


class AskUserExternalGuardTest(unittest.TestCase):
    """Исполнителю, работающему по задаче внешнего трекера, чат как канал
    закрыт: ответ в нём осел бы мимо задачи. Менеджера это не касается.

    Гард смотрит на role, а НЕ на source: контракт общий для любой
    интеграции, не только для одного трекера (обобщено 2026-07-25)."""

    def setUp(self) -> None:
        # Модуль сервера общий на все тесты — состояние возвращаем на место.
        for name, value in (("_DB_PATH", None), ("_JOBS_ORIGIN_COLS", None)):
            patcher = patch.object(common, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def _fresh_db(self, tmp: str) -> str:
        db_path = str(Path(tmp) / "bot_state.db")
        with patch.object(bot_db, "DB_PATH", db_path):
            bot_db.init_db()
        return db_path

    def _add_trigger(
        self, db_path: str, role: str | None, status: str = "in_progress",
        source: str = "tracker", thread_id: int = 77,
    ) -> None:
        with bot_db.connect(db_path) as conn:
            conn.execute(
                "INSERT INTO agent_triggers(chat_id, thread_id, text, source, role, "
                "status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (-100, thread_id, TRIGGER_TEXT, source, role, status,
                 utcnow().isoformat()),
            )

    def test_init_db_migrates_old_agent_triggers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "old.db")
            with bot_db.connect(db_path) as conn:
                conn.execute(OLD_TRIGGERS_SCHEMA)
            with patch.object(bot_db, "DB_PATH", db_path):
                bot_db.init_db()
            with bot_db.connect(db_path) as conn:
                cols = [r[1] for r in conn.execute(
                    "PRAGMA table_info(agent_triggers)"
                ).fetchall()]
        self.assertIn("role", cols)

    def test_executor_trigger_blocks_ask_user(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._fresh_db(tmp)
            self._add_trigger(db_path, "executor")
            common._DB_PATH = Path(db_path)
            with patch.object(common, "_telegram_api") as api:
                result = asyncio.run(asks.ask_user(
                    question="Сносить таблицу?",
                    thread_id=77,
                    chat_id=-100,
                    options=["да", "нет"],
                ))
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["task"], "#482")
        self.assertEqual(result["source"], "tracker")
        self.assertIn("#482", result["error"])
        api.assert_not_called()

    def test_manager_and_finished_triggers_do_not_block(self) -> None:
        cases = [
            ("manager", "in_progress"),   # Менеджеру спрашивать в чате можно
            ("executor", "done"),         # ход по задаче уже закончен
            (None, "in_progress"),        # поллер ещё не обновлён — не блокируем
        ]
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._fresh_db(tmp)
            common._DB_PATH = Path(db_path)
            for idx, (role, status) in enumerate(cases):
                thread_id = 100 + idx
                self._add_trigger(db_path, role, status=status, thread_id=thread_id)
                with self.subTest(role=role, status=status):
                    self.assertIsNone(
                        asks._external_executor_task(-100, thread_id)
                    )

    def test_guard_is_not_limited_to_one_integration(self) -> None:
        """Любой source с role=executor блокирует — до 2026-07-25 в SQL был
        зашит source='tracker', и чужая интеграция гард не получала."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._fresh_db(tmp)
            common._DB_PATH = Path(db_path)
            self._add_trigger(db_path, "executor", source="jira", thread_id=201)
            self.assertEqual(
                asks._external_executor_task(-100, 201), ("#482", "jira"),
            )

    def test_missing_role_column_fails_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "old.db")
            with bot_db.connect(db_path) as conn:
                conn.execute(OLD_TRIGGERS_SCHEMA)
                conn.execute(
                    "INSERT INTO agent_triggers(chat_id, thread_id, text, source, "
                    "status, created_at) VALUES (?, ?, ?, 'tracker', 'in_progress', ?)",
                    (-100, 77, TRIGGER_TEXT, utcnow().isoformat()),
                )
            common._DB_PATH = Path(db_path)
            self.assertIsNone(asks._external_executor_task(-100, 77))

    def test_trigger_without_task_tag_still_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = self._fresh_db(tmp)
            with bot_db.connect(db_path) as conn:
                conn.execute(
                    "INSERT INTO agent_triggers(chat_id, thread_id, text, source, role, "
                    "status, created_at) VALUES (?, ?, ?, 'tracker', 'executor', "
                    "'in_progress', ?)",
                    (-100, 88, "Событие по задаче без тега", utcnow().isoformat()),
                )
            common._DB_PATH = Path(db_path)
            self.assertEqual(
                asks._external_executor_task(-100, 88), ("#?", "tracker"),
            )


if __name__ == "__main__":
    unittest.main()
