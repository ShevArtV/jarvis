"""Ротация по размеру, seed диалога, полоса notify."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

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
    notify_session_is_open,
    plan_dialog_rotate,
    set_rotate_pending,
)
from bot.sessions import ensure_active_session, get_session


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

    def test_context_over_rotate_unknown_for_cursor(self) -> None:
        self.assertNotIn("cursor", ROTATE_ENGINES)
        self.assertNotIn("kimi", ROTATE_ENGINES)
        self.assertIsNone(context_over_rotate("cursor", "sid", None))
        self.assertIsNone(context_over_rotate("kimi", "sid", None))

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


if __name__ == "__main__":
    unittest.main()
