"""Аккаунты внутри движка: реестр из env, env процесса CLI, хранение per-topic."""

from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from bot import db as bot_db
from bot.sessions import get_session, set_engine
from bot.topic_account import get_account, set_account
from engines import accounts, limits, process_control

ENV = {"JARVIS_ACCOUNTS": "claude:work=/tmp/claude-work, codex:alt=/tmp/codex-alt, bogus:x=/y, claude:main=/z"}


@patch.dict(os.environ, ENV)
class RegistryTest(unittest.TestCase):
    def test_names_main_first_and_bad_items_skipped(self) -> None:
        self.assertEqual(accounts.account_names("claude"), ["main", "work"])
        self.assertEqual(accounts.account_names("codex"), ["main", "alt"])
        self.assertEqual(accounts.account_names("cursor"), ["main"])

    def test_env_only_inside_scope_of_non_main_account(self) -> None:
        self.assertEqual(accounts.account_env(), {})
        with accounts.engine_account_scope("claude", "main"):
            self.assertEqual(accounts.account_env(), {})
        with accounts.engine_account_scope("claude", "work"):
            self.assertEqual(accounts.account_env(), {"CLAUDE_CONFIG_DIR": str(Path("/tmp/claude-work"))})
        with accounts.engine_account_scope("codex", "alt"):
            self.assertEqual(accounts.account_env(), {"CODEX_HOME": str(Path("/tmp/codex-alt"))})
        self.assertEqual(accounts.account_env(), {})

    def test_limits_read_credentials_of_topic_account(self) -> None:
        with accounts.engine_account_scope("claude", "work"):
            self.assertEqual(limits._claude_credentials_path(),
                             Path("/tmp/claude-work/.credentials.json"))
            self.assertEqual(limits._claude_config_path(), Path("/tmp/claude-work/.claude.json"))
        self.assertNotIn("claude-work", str(limits._claude_credentials_path()))

    def test_spawn_passes_account_dir_to_cli(self) -> None:
        create = AsyncMock()
        with patch.object(process_control.asyncio, "create_subprocess_exec", create), \
             patch.object(process_control, "resolve_command", lambda b: [b]):
            with accounts.engine_account_scope("claude", "work"):
                asyncio.run(process_control.spawn(["claude", "-p"]))
            env = create.call_args.kwargs["env"]
            self.assertEqual(env["CLAUDE_CONFIG_DIR"], str(Path("/tmp/claude-work")))
            self.assertEqual(env["PATH"], os.environ["PATH"])

            asyncio.run(process_control.spawn(["claude", "-p"]))
            self.assertNotIn("env", create.call_args.kwargs)


@patch.dict(os.environ, ENV)
class TopicAccountTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        patcher = patch.object(bot_db, "DB_PATH", str(Path(tmp.name) / "bot_state.db"))
        patcher.start()
        self.addCleanup(patcher.stop)
        bot_db.init_db()

    def test_switch_keeps_session_engine_switch_resets_account(self) -> None:
        sid, _ = set_engine(-100, 5, "claude")
        self.assertEqual(get_account(-100, 5), "main")

        set_account(-100, 5, "work")
        self.assertEqual(get_account(-100, 5), "work")
        self.assertEqual(get_session(-100, 5)[0], sid)

        set_engine(-100, 5, "codex")
        self.assertEqual(get_account(-100, 5), "main")

    def test_account_removed_from_env_falls_back_to_main(self) -> None:
        set_engine(-100, 6, "claude")
        set_account(-100, 6, "work")
        with patch.dict(os.environ, {"JARVIS_ACCOUNTS": ""}):
            self.assertEqual(get_account(-100, 6), "main")

    def test_switch_account_handler(self) -> None:
        from bot.handlers import account as handler

        set_engine(-100, 7, "claude")
        kill = AsyncMock()
        with patch.object(handler, "_kill_persistent_worker", kill):
            text = asyncio.run(handler.switch_account((-100, 7), "claude", "nope"))
            self.assertIn("нет аккаунта", text)
            text = asyncio.run(handler.switch_account((-100, 7), "codex", "alt"))
            self.assertIn("уже на движке", text)
            text = asyncio.run(handler.switch_account((-100, 7), "claude", "work"))
            self.assertIn("main → work", text)
        kill.assert_awaited_once()
        self.assertEqual(get_account(-100, 7), "work")
        rows = handler.account_rows((-100, 7))
        self.assertEqual([b.text for b in rows[0]], ["👤 main", "👤 ✓ work"])


if __name__ == "__main__":
    unittest.main()
