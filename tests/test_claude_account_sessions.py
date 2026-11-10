"""Транскрипт claude ищется в каталоге аккаунта текущего хода."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from engines import accounts
from engines.claude_engine import ClaudeEngine, _sessions_dir_for

CWD = "/home/u/proj"
SID = "c0089227-1aff-4c9e-9c18-86b79dd5fea4"


class AccountSessionsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.home = root / "home"
        self.alt = root / "claude-alt"
        env = {"JARVIS_ACCOUNTS": f"claude:alt={self.alt}"}
        for p in (patch.dict(os.environ, env), patch.object(Path, "home", lambda: self.home)):
            p.start()
            self.addCleanup(p.stop)
        transcript = _sessions_dir_for(CWD, self.home / ".claude") / f"{SID}.jsonl"
        transcript.parent.mkdir(parents=True)
        transcript.write_text("{}\n")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_main_account_resumes(self) -> None:
        engine = ClaudeEngine()
        self.assertTrue(engine.session_exists(SID, CWD))
        self.assertFalse(engine.session_in_other_account(SID, CWD))
        self.assertEqual(engine._session_flags(SID, CWD)[1], ["--resume", SID])

    def test_other_account_starts_new_and_flags_lost_transcript(self) -> None:
        engine = ClaudeEngine()
        with accounts.engine_account_scope("claude", "alt"):
            self.assertFalse(engine.session_exists(SID, CWD))
            self.assertTrue(engine.session_in_other_account(SID, CWD))
            self.assertEqual(engine._session_flags(SID, CWD)[1], ["--session-id", SID])

    def test_unknown_session_is_not_lost(self) -> None:
        with accounts.engine_account_scope("claude", "alt"):
            self.assertFalse(ClaudeEngine().session_in_other_account("other", CWD))


if __name__ == "__main__":
    unittest.main()
