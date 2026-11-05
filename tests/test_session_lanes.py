"""Ротация по размеру, seed диалога, полоса notify."""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from bot import db as bot_db
from bot.db import init_db, log_message
from bot.session_lanes import (
    ROTATE_ENGINES,
    build_dialog_seed,
    build_notify_reply_seed,
    close_notify_session,
    context_over_rotate,
    dialog_busy_reasons,
    ensure_notify_session,
    get_rotate_pending,
    notify_session_is_open,
    plan_dialog_rotate,
    set_rotate_pending,
)
from bot.sessions import ensure_active_session, get_session, touch_session


class SessionLanesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "bot.db")
        self.db_patch = patch.object(bot_db, "DB_PATH", self.db_path)
        self.db_patch.start()
        init_db()
        self.chat_id = -100
        self.thread_id = 42
        get_session(self.chat_id, self.thread_id)

    def tearDown(self) -> None:
        self.db_patch.stop()
        self.tmp.cleanup()

    def test_dialog_seed_skips_queuewarden(self) -> None:
        log_message(self.chat_id, self.thread_id, "in", "user_text", "привет", 1)
        log_message(self.chat_id, self.thread_id, "out", "bot_reply", "ответ на привет", 2)
        log_message(
            self.chat_id, self.thread_id, "in", "queuewarden_inject",
            "Пришли уведомления QueueWarden " + ("x" * 3000), None,
        )
        log_message(
            self.chat_id, self.thread_id, "out", "bot_reply",
            "доклад по уведомлению длинный " + ("y" * 2000), 3,
        )
        log_message(self.chat_id, self.thread_id, "in", "user_text", "ещё вопрос", 4)
        log_message(self.chat_id, self.thread_id, "out", "bot_reply", "ещё ответ", 5)

        seed = build_dialog_seed(self.chat_id, self.thread_id, limit=8)
        self.assertIsNotNone(seed)
        assert seed is not None
        self.assertIn("привет", seed)
        self.assertIn("ещё вопрос", seed)
        self.assertNotIn("Пришли уведомления", seed)
        self.assertNotIn("доклад по уведомлению", seed)

    def test_dialog_seed_strips_fences_and_skips_dump_only(self) -> None:
        log_message(self.chat_id, self.thread_id, "in", "user_text", "покажи код", 1)
        log_message(
            self.chat_id, self.thread_id, "out", "bot_reply",
            "Кратко: вот файл, смотри основной путь.\n\n```python\n"
            + ("print(1)\n" * 80) + "```\n",
            2,
        )
        log_message(self.chat_id, self.thread_id, "in", "user_text", "а лог?", 3)
        log_message(
            self.chat_id, self.thread_id, "out", "bot_reply",
            "```\n" + ("ERROR line\n" * 40) + "```\n",
            4,
        )
        seed = build_dialog_seed(self.chat_id, self.thread_id, limit=8)
        self.assertIsNotNone(seed)
        assert seed is not None
        self.assertIn("покажи код", seed)
        self.assertIn("Кратко: вот файл", seed)
        self.assertNotIn("print(1)", seed)
        self.assertIn("а лог?", seed)
        self.assertNotIn("ERROR line", seed)

    def test_dialog_seed_pages_past_inject_flood(self) -> None:
        # Много inject'ов новее диалога: фиксированное окно 40 их бы съело.
        log_message(self.chat_id, self.thread_id, "in", "user_text", "старый вопрос", 1)
        log_message(self.chat_id, self.thread_id, "out", "bot_reply", "старый ответ диалога", 2)
        for i in range(60):
            log_message(
                self.chat_id, self.thread_id, "in", "queuewarden_inject",
                f"inject {i} " + ("z" * 100), None,
            )
        seed = build_dialog_seed(self.chat_id, self.thread_id, limit=4)
        self.assertIsNotNone(seed)
        assert seed is not None
        self.assertIn("старый вопрос", seed)
        self.assertIn("старый ответ диалога", seed)

    def test_notify_session_idle_and_reuse(self) -> None:
        sid1, _cwd, _eng, opened = ensure_notify_session(self.chat_id, self.thread_id)
        self.assertTrue(opened)
        self.assertTrue(notify_session_is_open(self.chat_id, self.thread_id))
        sid2, _, _, opened2 = ensure_notify_session(self.chat_id, self.thread_id)
        self.assertFalse(opened2)
        self.assertEqual(sid1, sid2)
        self.assertTrue(close_notify_session(self.chat_id, self.thread_id))
        self.assertFalse(notify_session_is_open(self.chat_id, self.thread_id))
        sid3, _, _, opened3 = ensure_notify_session(self.chat_id, self.thread_id)
        self.assertTrue(opened3)
        self.assertNotEqual(sid1, sid3)

    def test_rotate_pending_opens_new_session(self) -> None:
        sid_before, _, _, opened = ensure_active_session(self.chat_id, self.thread_id)
        self.assertTrue(opened)
        set_rotate_pending(self.chat_id, self.thread_id, True)
        sid_after, _, _, opened2 = ensure_active_session(self.chat_id, self.thread_id)
        self.assertTrue(opened2)
        self.assertNotEqual(sid_before, sid_after)

    def test_plan_rotate_defers_when_busy(self) -> None:
        worker = MagicMock()
        worker.busy = True
        with patch("bot.session_lanes.persistent_workers", {(self.chat_id, self.thread_id): worker}):
            with patch("bot.session_lanes.context_over_rotate", return_value=True):
                with patch("bot.session_lanes.has_background_work", return_value=False):
                    with patch("bot.session_lanes.get_pending_ask", return_value=None):
                        action = plan_dialog_rotate(
                            (self.chat_id, self.thread_id), "claude", "sid", None,
                        )
        self.assertEqual(action, "defer")

    def test_plan_rotate_closes_when_idle(self) -> None:
        with patch("bot.session_lanes.persistent_workers", {}):
            with patch("bot.session_lanes.active_procs", {}):
                with patch("bot.session_lanes.context_over_rotate", return_value=True):
                    with patch("bot.session_lanes.get_pending_ask", return_value=None):
                        action = plan_dialog_rotate(
                            (self.chat_id, self.thread_id), "claude", "sid", None,
                        )
        self.assertEqual(action, "close")

    def test_rotate_engines_include_cursor_kimi(self) -> None:
        self.assertIn("cursor", ROTATE_ENGINES)
        self.assertIn("kimi", ROTATE_ENGINES)

    def test_notify_reply_seed_keeps_task_markers(self) -> None:
        report = "**[tako] 2610-235 · Beautysleep · title** #qw_tako-2610-235\nтело"
        seed = build_notify_reply_seed(report, "го")
        self.assertIn("2610-235", seed)
        self.assertIn("го", seed)
        self.assertIn("Доклад", seed)

    def test_dialog_busy_reasons_ask(self) -> None:
        with patch("bot.session_lanes.get_pending_ask", return_value={"id": 1}):
            with patch("bot.session_lanes.persistent_workers", {}):
                with patch("bot.session_lanes.active_procs", {}):
                    reasons = dialog_busy_reasons((self.chat_id, self.thread_id))
        self.assertIn("ask_user", reasons)

    def test_rotate_pending_worker_closes_session_before_ensure(self) -> None:
        """Баг: kill без close_session оставлял тот же раздутый session_id."""
        from contextlib import nullcontext

        from bot.handlers import messages as messages_mod
        from bot import topics as topics_mod

        key = (self.chat_id, self.thread_id)
        touch_session(*key)
        sid_before, _, _ = get_session(*key)
        set_rotate_pending(*key, True)
        log_message(self.chat_id, self.thread_id, "in", "user_text", "диалог до ротации", 10)

        old_worker = MagicMock()
        old_worker.dead = False
        old_worker.proc = SimpleNamespace(returncode=None)
        old_worker.busy = False
        topics_mod.persistent_workers[key] = old_worker
        topics_mod.persistent_spawn_locks.clear()

        new_worker = MagicMock()
        new_worker.session_id = "new-after-rotate"
        new_worker.fresh = False
        new_worker.proc = SimpleNamespace(pid=1234, returncode=None)

        chat = SimpleNamespace(id=self.chat_id)

        async def _run() -> None:
            with patch.object(messages_mod, "dialog_busy_reasons", return_value=[]), \
                 patch.object(messages_mod, "_kill_persistent_worker", new_callable=AsyncMock) as kill, \
                 patch.object(messages_mod, "start_persistent", new_callable=AsyncMock, return_value=new_worker), \
                 patch.object(messages_mod, "send_to_topic", new_callable=AsyncMock), \
                 patch.object(messages_mod, "get_persistent_for_engine", return_value=True), \
                 patch.object(messages_mod, "_persistent_column_for_engine", return_value="persistent_claude"), \
                 patch.object(messages_mod, "attach_background_turns"), \
                 patch.object(messages_mod, "capture_baseline"), \
                 patch.object(messages_mod, "get_model", return_value=None), \
                 patch.object(messages_mod, "get_account", return_value=None), \
                 patch.object(messages_mod, "get_mcp_playwright", return_value=False), \
                 patch.object(messages_mod, "resolve_topic_role", return_value="agent"), \
                 patch.object(messages_mod, "build_system_prefix", return_value=""), \
                 patch.object(messages_mod, "engine_account_scope", return_value=nullcontext()), \
                 patch.object(messages_mod, "update_session_id"):
                worker, spawned = await messages_mod._get_or_start_persistent_worker(
                    chat, self.thread_id, key,
                )
            self.assertTrue(spawned)
            self.assertIs(worker, new_worker)
            kill.assert_awaited()
            self.assertFalse(get_rotate_pending(*key))
            sid_now, _, _ = get_session(*key)
            self.assertNotEqual(sid_before, sid_now)
            self.assertIn(key, messages_mod._pending_dialog_seed)

        try:
            asyncio.run(_run())
        finally:
            topics_mod.persistent_workers.pop(key, None)
            messages_mod._pending_dialog_seed.pop(key, None)


class KimiCursorUsageTest(unittest.TestCase):
    def test_inspect_kimi_reads_token_counting(self) -> None:
        from engines.session_usage import inspect_session_usage

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        root = Path(tmp.name)
        wire = root / "agents" / "main" / "wire.jsonl"
        wire.parent.mkdir(parents=True)
        wire.write_text(
            '{"type":"other"}\n'
            '{"type":"token_counting.measured","tokens":111}\n'
            '{"type":"token_counting.measured","tokens":222}\n',
            encoding="utf-8",
        )
        with patch("engines.kimi_engine._session_dir", return_value=root):
            usage = inspect_session_usage("kimi", "sess-1", "/tmp")
        self.assertEqual(usage.context_tokens, 222)
        self.assertEqual(usage.threshold_tokens, 222)

    def test_inspect_cursor_estimates_from_store(self) -> None:
        import json
        import sqlite3

        from engines.session_usage import inspect_session_usage

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        sid = "cursor-sess-test"
        db_path = Path(tmp.name) / ".cursor" / "acp-sessions" / sid / "store.db"
        db_path.parent.mkdir(parents=True)
        con = sqlite3.connect(db_path)
        con.execute("CREATE TABLE blobs (id TEXT PRIMARY KEY, data BLOB)")
        con.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
        payload = json.dumps({
            "role": "user",
            "content": "x" * 400,
        }).encode()
        con.execute("INSERT INTO blobs(id, data) VALUES (?, ?)", ("1", payload))
        con.commit()
        con.close()

        with patch("engines.session_usage.Path.home", return_value=Path(tmp.name)):
            usage = inspect_session_usage("cursor", sid, None)
            over = context_over_rotate("cursor", sid, None)
        self.assertEqual(usage.estimated_tokens, 100)
        self.assertTrue(usage.is_estimate)
        # 100 << 120k порога
        self.assertIs(over, False)

    def test_cursor_store_skips_binary_blobs(self) -> None:
        """Бинарные узлы дерева ACP не должны входить в оценку токенов."""
        import json
        import sqlite3

        from engines.session_usage import _cursor_store_text_chars

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        db_path = Path(tmp.name) / "store.db"
        con = sqlite3.connect(db_path)
        con.execute("CREATE TABLE blobs (id TEXT PRIMARY KEY, data BLOB)")
        text_blob = json.dumps({
            "role": "assistant",
            "content": "hello world from json",
        }).encode()
        binary_blob = bytes(range(256)) * 1000  # ~256k, как узлы дерева
        con.execute("INSERT INTO blobs(id, data) VALUES (?, ?)", ("json", text_blob))
        con.execute("INSERT INTO blobs(id, data) VALUES (?, ?)", ("bin", binary_blob))
        con.commit()
        con.close()

        chars = _cursor_store_text_chars(db_path)
        self.assertEqual(chars, len("hello world from json"))
        # Если бы считали binary — было бы ~256k+
        self.assertLess(chars, 100)


if __name__ == "__main__":
    unittest.main()
