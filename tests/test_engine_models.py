from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bot import db as bot_db
from bot import sessions as bot_sessions
from engines import claude_engine, cursor_engine


def _init_response(values: list[str]) -> str:
    ev = {
        "type": "control_response",
        "response": {
            "subtype": "success",
            "request_id": "jarvis-models",
            "response": {"models": [{"value": v} for v in values]},
        },
    }
    return json.dumps(ev) + "\n"


class ClaudeModelDiscoveryTest(unittest.TestCase):
    """Список моделей claude берётся из ответа CLI на control-запрос initialize."""

    def _run(self, stdout: str, returncode: int = 0):
        return subprocess.CompletedProcess([], returncode, stdout=stdout, stderr="")

    def test_models_from_initialize_without_default(self) -> None:
        out = '{"type":"system"}\n' + _init_response(
            ["default", "opus", "claude-opus-5", "claude-sonnet-4-6"],
        )
        with patch.object(claude_engine.subprocess, "run", return_value=self._run(out)), \
                patch.dict("os.environ", {}, clear=False) as env:
            env.pop("CLAUDE_MODELS", None)
            self.assertEqual(
                claude_engine._discover_claude_models(),
                ["opus", "claude-opus-5", "claude-sonnet-4-6"],
            )

    def test_display_names_become_button_labels(self) -> None:
        from bot.handlers.engine import _model_label

        ev = json.loads(_init_response([]))
        ev["response"]["response"]["models"] = [
            {"value": "opus", "displayName": "Opus 5.5"},
            {"value": "claude-fable-5-1", "displayName": "Fable 5.1"},
        ]
        with patch.object(
            claude_engine.subprocess, "run", return_value=self._run(json.dumps(ev) + "\n"),
        ):
            self.assertEqual(
                claude_engine._models_from_claude_init(), ["opus", "claude-fable-5-1"],
            )
        self.assertEqual(_model_label("opus"), "Opus 5.5")
        self.assertEqual(_model_label("claude-fable-5-1"), "Fable 5.1")
        self.assertEqual(_model_label("deepseek/deepseek-chat"), "deepseek-chat")

    def test_env_override_wins(self) -> None:
        with patch.object(claude_engine.subprocess, "run") as run, \
                patch.dict("os.environ", {"CLAUDE_MODELS": "opus,haiku"}):
            self.assertEqual(claude_engine._discover_claude_models(), ["opus", "haiku"])
            run.assert_not_called()

    def test_fallback_when_cli_fails(self) -> None:
        with patch.object(
            claude_engine.subprocess, "run",
            side_effect=subprocess.TimeoutExpired("claude", 30),
        ), patch.object(claude_engine, "_models_from_claude_config", return_value=["x"]), \
                patch.dict("os.environ", {}, clear=False) as env:
            env.pop("CLAUDE_MODELS", None)
            self.assertEqual(
                claude_engine._discover_claude_models(),
                claude_engine.DEFAULT_CLAUDE_MODELS + ["x"],
            )


class SetEngineResetsActualModelTest(unittest.TestCase):
    """После /engine в статусе не должна висеть модель прежнего движка."""

    def test_actual_model_cleared_on_engine_switch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "bot_state.db")
            with patch.object(bot_db, "DB_PATH", db_path):
                bot_db.init_db()
                bot_sessions.set_engine(1, 2, "claude", model="opus")
                bot_sessions.update_actual_model(1, 2, "claude", "claude-opus-5")
                self.assertEqual(bot_sessions.get_actual_model(1, 2), "claude-opus-5")

                bot_sessions.set_engine(1, 2, "codex", model="gpt-6.1-sol")
                self.assertIsNone(bot_sessions.get_actual_model(1, 2))
                self.assertEqual(bot_sessions.get_model(1, 2), "gpt-6.1-sol")



class CursorModelDiscoveryTest(unittest.TestCase):
    """`cursor-agent --list-models`: строки «id - название», список урезан."""

    OUT = (
        "Available models\n\n"
        "auto - Auto (default)\n"
        "gpt-5.2 - GPT-5.2\n"
        "grok-4.7-low-fast - Grok 4.7  Low Fast\u200b\u200b\n"
        "glm-5.2-max - GLM 5.2 Max\n\n"
        "Tip: use --model <id> to switch.\n"
    )

    def test_parse_labels_and_limit(self) -> None:
        from bot.handlers.engine import _model_label

        run = subprocess.CompletedProcess([], 0, stdout=self.OUT, stderr="")
        with patch("engines.model_cache.run_cli", return_value=run), \
                patch.object(cursor_engine, "CURSOR_MODELS_LIMIT", 3), \
                patch.dict("os.environ", {}, clear=False) as env:
            env.pop("CURSOR_MODELS", None)
            self.assertEqual(cursor_engine._discover_cursor_models(),
                             ["auto", "gpt-5.2", "grok-4.7-low-fast"])
        self.assertEqual(_model_label("gpt-5.2"), "GPT-5.2")
        self.assertEqual(_model_label("grok-4.7-low-fast"), "Grok 4.7  Low Fast")

    def test_env_override_wins(self) -> None:
        with patch("engines.model_cache.run_cli") as run, \
                patch.dict("os.environ", {"CURSOR_MODELS": "auto,gpt-5.2"}):
            self.assertEqual(cursor_engine._discover_cursor_models(), ["auto", "gpt-5.2"])
            run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
