from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from engines.session_usage import aggregate_claude_usage
from bot import db as bot_db
from bot import workers as bot_workers
from bot.handlers.toggles import (
    _done_confirm_keyboard,
    _looks_like_task_done,
    _looks_like_waiting_for_user,
    _session_confirm_token,
)


class DoneDetectorTest(unittest.TestCase):
    def test_done_phrases(self) -> None:
        samples = [
            "Готово. Изменения закоммитил, проверки прошли.",
            "Итог: фикс задеплоен и проверен.",
            "Done: implemented session control.",
        ]
        for text in samples:
            with self.subTest(text=text):
                self.assertTrue(_looks_like_task_done(text))

    def test_waiting_phrases_block_autoclose(self) -> None:
        samples = [
            "План готов, жду подтверждение.",
            "Можно продолжать?",
            "Нужно согласовать деплой.",
            "Готово ли отправлять?",
            "#ask_563 Нужно уточнение по окружению.",
        ]
        for text in samples:
            with self.subTest(text=text):
                self.assertTrue(_looks_like_waiting_for_user(text))
                self.assertFalse(_looks_like_task_done(text))

    def test_negative_done_phrases_block_autoclose(self) -> None:
        samples = [
            "Не готово: тесты упали.",
            "Not done: waiting for CI.",
        ]
        for text in samples:
            with self.subTest(text=text):
                self.assertFalse(_looks_like_task_done(text))

    def test_done_confirm_keyboard_uses_short_session_token(self) -> None:
        session_id = "placeholder-36e972de-0504-4d32-9c19-6eb67879d72c"
        token = _session_confirm_token(session_id)
        self.assertEqual(len(token), 12)

        keyboard = _done_confirm_keyboard(session_id)
        buttons = keyboard.inline_keyboard[0]
        callback_data = [button.callback_data for button in buttons]

        self.assertEqual(callback_data[0], f"done_confirm:{token}:yes")
        self.assertEqual(callback_data[1], f"done_confirm:{token}:no")
        self.assertLessEqual(max(len(item) for item in callback_data), 64)


class ClaudeUsageAggregationTest(unittest.TestCase):
    def test_deduplicates_repeated_request_id_rows(self) -> None:
        session_id = "session-1"
        cwd = "/tmp/jarvis-session-control-test"
        encoded_cwd = "-tmp-jarvis-session-control-test"
        ts = datetime(2026, 7, 19, 12, 0, tzinfo=timezone.utc).isoformat()

        duplicate_a = {
            "timestamp": ts,
            "sessionId": session_id,
            "requestId": "req_same",
            "uuid": "uuid-a",
            "message": {
                "id": "msg_same",
                "role": "assistant",
                "model": "claude-sonnet-5",
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 20,
                    "cache_creation_input_tokens": 30,
                    "cache_read_input_tokens": 40,
                },
            },
        }
        duplicate_b = {
            **duplicate_a,
            "uuid": "uuid-b",
        }
        unique = {
            "timestamp": ts,
            "sessionId": session_id,
            "requestId": "req_unique",
            "uuid": "uuid-c",
            "message": {
                "id": "msg_unique",
                "role": "assistant",
                "model": "claude-sonnet-5",
                "usage": {
                    "input_tokens": 1,
                    "output_tokens": 2,
                    "cache_creation_input_tokens": 3,
                    "cache_read_input_tokens": 4,
                },
            },
        }

        with tempfile.TemporaryDirectory() as tmp:
            transcript_dir = Path(tmp) / ".claude" / "projects" / encoded_cwd
            transcript_dir.mkdir(parents=True)
            transcript = transcript_dir / f"{session_id}.jsonl"
            transcript.write_text(
                "\n".join(json.dumps(row) for row in (duplicate_a, duplicate_b, unique)),
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"HOME": tmp}):
                usage = aggregate_claude_usage(session_id, cwd)

        totals = usage.by_model["claude-sonnet-5"]
        self.assertEqual(totals.n_messages, 2)
        self.assertEqual(totals.input_tokens, 11)
        self.assertEqual(totals.output_tokens, 22)
        self.assertEqual(totals.cache_write_tokens, 33)
        self.assertEqual(totals.cache_read_tokens, 44)
        self.assertIn("deduplicated 1", usage.note or "")


def _load_mcp_server():
    """scripts/ не пакет — грузим MCP-сервер по пути."""
    path = Path(__file__).resolve().parent.parent / "scripts" / "jarvis_mcp_server.py"
    spec = importlib.util.spec_from_file_location("jarvis_mcp_server_under_test", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class _FakeChat:
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict]] = []

    async def send_message(self, text: str, **kwargs):
        self.sent.append((text, kwargs))
        return SimpleNamespace(message_id=4242)


class _FakeApp:
    def __init__(self, chat: _FakeChat) -> None:
        self.chat = chat
        self.bot = SimpleNamespace(get_chat=self._get_chat)

    async def _get_chat(self, chat_id: int):
        return self.chat


class ManagerCloseSessionTest(unittest.TestCase):
    """manager_close_session (MCP) → close_requests_worker (бот): закрытие
    сеанса чужого топика через БД, потому что процессы топика видит только бот."""

    CHAT_ID = -100500
    THREAD_ID = 77

    def _seed(self, db_path: str) -> None:
        now = datetime.utcnow().isoformat()
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, "
                "model, updated_at, last_activity_at, session_started_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (self.CHAT_ID, self.THREAD_ID, "sid-1", "/tmp/topic", "claude",
                 None, now, now, now),
            )
            conn.execute(
                "INSERT INTO jobs(chat_id, thread_id, text, status, created_at) "
                "VALUES (?, ?, ?, 'in_progress', ?)",
                (self.CHAT_ID, self.THREAD_ID, "долгая задача", now),
            )

    def test_close_request_flows_from_mcp_to_bot(self) -> None:
        mcp_server = _load_mcp_server()
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "bot_state.db")
            with patch.object(bot_db, "DB_PATH", db_path):
                bot_db.init_db()
                self._seed(db_path)

                mcp_server._DB_PATH = Path(db_path)
                result = mcp_server.manager_close_session(
                    thread_id=self.THREAD_ID, chat_id=self.CHAT_ID,
                )

                # MCP: сеанс закрыт сразу, активный job помечен на прерывание,
                # для бота выставлен close_requested.
                self.assertTrue(result["was_open"])
                self.assertEqual(len(result["interrupted_jobs"]), 1)
                self.assertEqual(result["engine"], "claude")
                with sqlite3.connect(db_path) as conn:
                    row = conn.execute(
                        "SELECT last_activity_at, close_requested FROM sessions "
                        "WHERE chat_id = ? AND thread_id = ?",
                        (self.CHAT_ID, self.THREAD_ID),
                    ).fetchone()
                    cancel = conn.execute(
                        "SELECT cancel_requested FROM jobs"
                    ).fetchone()[0]
                self.assertIsNone(row[0])
                self.assertIsNotNone(row[1])
                self.assertIsNotNone(cancel)

                # Бот: добивает процессы топика, гасит флаг, пишет в топик.
                chat = _FakeChat()
                app = _FakeApp(chat)
                key = (self.CHAT_ID, self.THREAD_ID)
                with patch.object(
                    bot_workers, "_kill_persistent_worker", new=AsyncMock(return_value=True)
                ) as kill:
                    asyncio.run(bot_workers._apply_close_request(app, key))
                kill.assert_awaited_once()
                self.assertEqual(kill.await_args.args[0], key)

                with sqlite3.connect(db_path) as conn:
                    flag = conn.execute(
                        "SELECT close_requested FROM sessions "
                        "WHERE chat_id = ? AND thread_id = ?",
                        (self.CHAT_ID, self.THREAD_ID),
                    ).fetchone()[0]
                    logged = conn.execute(
                        "SELECT kind, telegram_message_id FROM messages_log"
                    ).fetchone()
                self.assertIsNone(flag)
                self.assertEqual(len(chat.sent), 1)
                self.assertIn("Сеанс закрыт Менеджером", chat.sent[0][0])
                self.assertEqual(chat.sent[0][1]["message_thread_id"], self.THREAD_ID)
                self.assertEqual(logged, ("session_closed", 4242))

                # Повтор на закрытом сеансе идемпотентен (job к этому моменту
                # уже завершён — прерывать нечего).
                with sqlite3.connect(db_path) as conn:
                    conn.execute("UPDATE jobs SET status = 'done'")
                repeat = mcp_server.manager_close_session(
                    thread_id=self.THREAD_ID, chat_id=self.CHAT_ID,
                )
                self.assertFalse(repeat["was_open"])
                self.assertEqual(repeat["interrupted_jobs"], [])

    def test_unknown_topic_is_rejected(self) -> None:
        mcp_server = _load_mcp_server()
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "bot_state.db")
            with patch.object(bot_db, "DB_PATH", db_path):
                bot_db.init_db()
            mcp_server._DB_PATH = Path(db_path)
            with self.assertRaises(RuntimeError):
                mcp_server.manager_close_session(thread_id=1, chat_id=self.CHAT_ID)

    def test_mcp_adds_close_requested_column_on_old_db(self) -> None:
        """MCP-сервер может подняться раньше бота новой версии — колонку
        заводит сам, иначе инструмент падал бы на 'no such column'."""
        mcp_server = _load_mcp_server()
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "old.db")
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "CREATE TABLE sessions (chat_id INTEGER NOT NULL, "
                    "thread_id INTEGER NOT NULL, session_id TEXT NOT NULL)"
                )
                mcp_server._ensure_close_requested_column(conn)
                cols = [r[1] for r in conn.execute(
                    "PRAGMA table_info(sessions)"
                ).fetchall()]
        self.assertIn("close_requested", cols)


class TopicAdminTest(unittest.TestCase):
    """manager_archive_topic / manager_delete_topic: жизненный цикл временного
    топика. Telegram мокается — тесты не ходят в сеть и не трогают форум."""

    CHAT_ID = -100500
    THREAD_ID = 88

    def _seed(self, db_path: str, *, job_status: str = "pending") -> None:
        now = datetime.utcnow().isoformat()
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, "
                "model, topic_title, updated_at, last_activity_at, session_started_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (self.CHAT_ID, self.THREAD_ID, "sid-1", "/tmp/tmp-topic", "claude",
                 None, "tmp-2607-37", now, now, now),
            )
            conn.execute(
                "INSERT INTO jobs(chat_id, thread_id, text, status, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (self.CHAT_ID, self.THREAD_ID, "отложенная задача", job_status, now),
            )
            conn.execute(
                "INSERT INTO ask_requests(chat_id, thread_id, question, status, "
                "created_at) VALUES (?, ?, ?, 'pending', ?)",
                (self.CHAT_ID, self.THREAD_ID, "продолжать?", now),
            )
            conn.execute(
                "INSERT INTO reminders(chat_id, thread_id, text, schedule, "
                "next_fire_at, enabled, created_at) "
                "VALUES (?, ?, ?, 'daily 10:00', ?, 1, ?)",
                (self.CHAT_ID, self.THREAD_ID, "напомнить", now, now),
            )
            conn.execute(
                "INSERT INTO messages_log(chat_id, thread_id, direction, kind, "
                "text, ts) VALUES (?, ?, 'in', 'user_text', 'привет', ?)",
                (self.CHAT_ID, self.THREAD_ID, now),
            )

    def _prepared(self, tmp: str, **seed_kwargs):
        """Готовая БД + MCP-модуль, нацеленный на неё."""
        db_path = str(Path(tmp) / "bot_state.db")
        with patch.object(bot_db, "DB_PATH", db_path):
            bot_db.init_db()
        self._seed(db_path, **seed_kwargs)
        mcp_server = _load_mcp_server()
        mcp_server._DB_PATH = Path(db_path)
        return mcp_server, db_path

    @staticmethod
    def _session_row(db_path: str, chat_id: int, thread_id: int):
        with sqlite3.connect(db_path) as conn:
            return conn.execute(
                "SELECT last_activity_at FROM sessions WHERE chat_id = ? AND thread_id = ?",
                (chat_id, thread_id),
            ).fetchone()

    def test_await_bot_close_waits_for_the_bot(self) -> None:
        """Хелпер возвращает True только когда бот погасил close_requested —
        до этого момента живые процессы топика ещё не убиты."""
        with tempfile.TemporaryDirectory() as tmp:
            mcp_server, db_path = self._prepared(tmp)
            key = (self.CHAT_ID, self.THREAD_ID)

            async def fake_bot() -> None:
                """close_requests_worker в миниатюре: дожидается флага и
                отрабатывает закрытие, как это делает живой бот."""
                for _ in range(200):
                    with sqlite3.connect(db_path) as conn:
                        flag = conn.execute(
                            "SELECT close_requested FROM sessions "
                            "WHERE chat_id = ? AND thread_id = ?", key,
                        ).fetchone()[0]
                    if flag is not None:
                        break
                    await asyncio.sleep(0.01)
                with patch.object(bot_db, "DB_PATH", db_path), patch.object(
                    bot_workers, "_kill_persistent_worker",
                    new=AsyncMock(return_value=False),
                ):
                    await bot_workers._apply_close_request(
                        _FakeApp(_FakeChat()), key,
                    )

            async def scenario() -> tuple[bool, bool]:
                # Бот молчит: флаг стоит, гасить его некому — False.
                timed_out = await mcp_server._await_bot_close(
                    *key, timeout=0.0, poll=0.01,
                )
                # Бот жив и гасит флаг — дожидаемся подтверждения.
                confirmed, _ = await asyncio.gather(
                    mcp_server._await_bot_close(*key, timeout=2.0, poll=0.01),
                    fake_bot(),
                )
                return timed_out, confirmed

            timed_out, confirmed = asyncio.run(scenario())
        self.assertFalse(timed_out)
        self.assertTrue(confirmed)

    def test_archive_folds_topic_and_keeps_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mcp_server, db_path = self._prepared(tmp)
            with patch.object(mcp_server, "_telegram_api") as api, patch.object(
                mcp_server, "_await_bot_close", new=AsyncMock(return_value=True)
            ):
                result = asyncio.run(mcp_server.manager_archive_topic(
                    thread_id=self.THREAD_ID, chat_id=self.CHAT_ID,
                ))
            self.assertEqual(api.call_args.args[0], "closeForumTopic")
            self.assertEqual(
                api.call_args.args[1]["message_thread_id"], self.THREAD_ID,
            )
            self.assertEqual(result["state"], "closed")
            self.assertTrue(result["bot_confirmed"])
            # Топик остаётся в БД — архивация обратима.
            self.assertIsNotNone(
                self._session_row(db_path, self.CHAT_ID, self.THREAD_ID)
            )

    def test_archive_reopen_unfolds_and_keeps_session_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mcp_server, _ = self._prepared(tmp)
            with patch.object(mcp_server, "_telegram_api") as api, patch.object(
                mcp_server, "_await_bot_close", new=AsyncMock(return_value=True)
            ) as await_close:
                result = asyncio.run(mcp_server.manager_archive_topic(
                    thread_id=self.THREAD_ID, chat_id=self.CHAT_ID, reopen=True,
                ))
            self.assertEqual(api.call_args.args[0], "reopenForumTopic")
            self.assertEqual(result["state"], "open")
            # Разворачивание — не повод трогать сеанс.
            await_close.assert_not_awaited()

    def test_delete_removes_state_and_defuses_pending_work(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mcp_server, db_path = self._prepared(tmp)
            with patch.object(mcp_server, "_telegram_api") as api, patch.object(
                mcp_server, "_await_bot_close", new=AsyncMock(return_value=True)
            ):
                result = asyncio.run(mcp_server.manager_delete_topic(
                    thread_id=self.THREAD_ID, chat_id=self.CHAT_ID,
                ))
            self.assertEqual(api.call_args.args[0], "deleteForumTopic")
            self.assertTrue(result["telegram_deleted"])

            with sqlite3.connect(db_path) as conn:
                self.assertIsNone(conn.execute(
                    "SELECT 1 FROM sessions WHERE chat_id=? AND thread_id=?",
                    (self.CHAT_ID, self.THREAD_ID),
                ).fetchone())
                job_status = conn.execute("SELECT status FROM jobs").fetchone()[0]
                ask_status = conn.execute(
                    "SELECT status FROM ask_requests"
                ).fetchone()[0]
                reminders = conn.execute(
                    "SELECT COUNT(*) FROM reminders"
                ).fetchone()[0]
                log_rows = conn.execute(
                    "SELECT COUNT(*) FROM messages_log"
                ).fetchone()[0]
            # Подвешенная работа обезврежена: она стреляла бы в удалённый тред.
            self.assertEqual(job_status, "cancelled")
            self.assertEqual(ask_status, "cancelled")
            self.assertEqual(reminders, 0)
            # Переписка — ценность, её сносит только purge_log.
            self.assertEqual(log_rows, 1)
            self.assertTrue(result["log_kept"])

    def test_delete_with_purge_log_drops_the_history_too(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mcp_server, db_path = self._prepared(tmp)
            with patch.object(mcp_server, "_telegram_api"), patch.object(
                mcp_server, "_await_bot_close", new=AsyncMock(return_value=True)
            ):
                result = asyncio.run(mcp_server.manager_delete_topic(
                    thread_id=self.THREAD_ID, chat_id=self.CHAT_ID, purge_log=True,
                ))
            with sqlite3.connect(db_path) as conn:
                log_rows = conn.execute(
                    "SELECT COUNT(*) FROM messages_log"
                ).fetchone()[0]
        self.assertEqual(log_rows, 0)
        self.assertEqual(result["deleted_log_rows"], 1)
        self.assertFalse(result["log_kept"])

    def test_delete_cleans_orphaned_state_when_topic_is_already_gone(self) -> None:
        """Топик удалили руками в Telegram — строка в sessions осиротела;
        инструмент обязан её убрать, а не упасть."""
        with tempfile.TemporaryDirectory() as tmp:
            mcp_server, db_path = self._prepared(tmp)
            gone = RuntimeError(
                "Telegram deleteForumTopic failed: Bad Request: message thread not found"
            )
            with patch.object(
                mcp_server, "_telegram_api", side_effect=gone
            ), patch.object(
                mcp_server, "_await_bot_close", new=AsyncMock(return_value=True)
            ):
                result = asyncio.run(mcp_server.manager_delete_topic(
                    thread_id=self.THREAD_ID, chat_id=self.CHAT_ID,
                ))
            with sqlite3.connect(db_path) as conn:
                left = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        self.assertFalse(result["telegram_deleted"])
        self.assertIn("already gone", result["warning"])
        self.assertEqual(left, 0)

    def test_delete_propagates_other_telegram_errors(self) -> None:
        """Нет прав — не наш случай «топика уже нет»: состояние трогать нельзя."""
        with tempfile.TemporaryDirectory() as tmp:
            mcp_server, db_path = self._prepared(tmp)
            denied = RuntimeError(
                "Telegram deleteForumTopic failed: Bad Request: not enough rights"
            )
            with patch.object(
                mcp_server, "_telegram_api", side_effect=denied
            ), patch.object(
                mcp_server, "_await_bot_close", new=AsyncMock(return_value=True)
            ):
                with self.assertRaises(RuntimeError):
                    asyncio.run(mcp_server.manager_delete_topic(
                        thread_id=self.THREAD_ID, chat_id=self.CHAT_ID,
                    ))
            with sqlite3.connect(db_path) as conn:
                left = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        self.assertEqual(left, 1)

    def test_delete_refuses_general_and_manager_topics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mcp_server, _ = self._prepared(tmp)
            with self.assertRaises(RuntimeError) as general:
                asyncio.run(mcp_server.manager_delete_topic(
                    thread_id=1, chat_id=self.CHAT_ID,
                ))
            self.assertIn("General", str(general.exception))

            with patch.dict(
                os.environ, {"JARVIS_MANAGER_THREAD_ID": str(self.THREAD_ID)}
            ):
                with self.assertRaises(RuntimeError) as manager:
                    asyncio.run(mcp_server.manager_delete_topic(
                        thread_id=self.THREAD_ID, chat_id=self.CHAT_ID,
                    ))
        self.assertIn("Manager", str(manager.exception))

    def test_delete_refuses_unknown_topic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mcp_server, _ = self._prepared(tmp)
            with self.assertRaises(RuntimeError):
                asyncio.run(mcp_server.manager_delete_topic(
                    thread_id=999, chat_id=self.CHAT_ID,
                ))

    def test_delete_refuses_busy_topic_unless_forced(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            mcp_server, db_path = self._prepared(tmp, job_status="in_progress")
            with patch.object(mcp_server, "_telegram_api"), patch.object(
                mcp_server, "_await_bot_close", new=AsyncMock(return_value=True)
            ):
                with self.assertRaises(RuntimeError) as busy:
                    asyncio.run(mcp_server.manager_delete_topic(
                        thread_id=self.THREAD_ID, chat_id=self.CHAT_ID,
                    ))
                self.assertIn("in_progress", str(busy.exception))

                forced = asyncio.run(mcp_server.manager_delete_topic(
                    thread_id=self.THREAD_ID, chat_id=self.CHAT_ID, force=True,
                ))
            with sqlite3.connect(db_path) as conn:
                left = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        self.assertEqual(len(forced["interrupted_jobs"]), 1)
        self.assertEqual(left, 0)

    def test_delete_refuses_when_bot_did_not_confirm(self) -> None:
        """Бот не отозвался — его процессы пережили бы топик. Только force."""
        with tempfile.TemporaryDirectory() as tmp:
            mcp_server, db_path = self._prepared(tmp)
            with patch.object(mcp_server, "_telegram_api"), patch.object(
                mcp_server, "_await_bot_close", new=AsyncMock(return_value=False)
            ):
                with self.assertRaises(RuntimeError) as silent:
                    asyncio.run(mcp_server.manager_delete_topic(
                        thread_id=self.THREAD_ID, chat_id=self.CHAT_ID,
                        wait_seconds=0.0,
                    ))
                self.assertIn("force=true", str(silent.exception))

                forced = asyncio.run(mcp_server.manager_delete_topic(
                    thread_id=self.THREAD_ID, chat_id=self.CHAT_ID, force=True,
                ))
            with sqlite3.connect(db_path) as conn:
                left = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        self.assertFalse(forced["bot_confirmed"])
        self.assertEqual(left, 0)


if __name__ == "__main__":
    unittest.main()
