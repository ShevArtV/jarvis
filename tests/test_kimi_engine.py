"""Unit tests for kimi print adapter and ACP worker hooks."""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from engines import kimi_engine, persistent_kimi
from engines.kimi_engine import KimiEngine, _kimi_command
from engines.kimi_events import KimiStream
from engines.persistent_kimi import PersistentKimiWorker


class KimiCommandTest(unittest.TestCase):
    def test_command_new_and_resume(self) -> None:
        cmd = _kimi_command(None, "kimi-code/k3", "hi", resume_mode=False)
        self.assertEqual(cmd[:1], [kimi_engine.KIMI_BIN])
        self.assertIn("-p", cmd)
        self.assertIn("hi", cmd)
        self.assertNotIn("--session", cmd)
        self.assertEqual(
            _kimi_command("session_1", None, "x", resume_mode=True)[:3],
            [kimi_engine.KIMI_BIN, "--session", "session_1"],
        )


class KimiModelsTest(unittest.TestCase):
    def test_discover_from_config_toml(self) -> None:
        with TemporaryDirectory() as tmp:
            home = Path(tmp)
            (home / "config.toml").write_text(
                'default_model = "kimi-code/k3"\n'
                '[models."kimi-code/k3"]\n'
                'display_name = "K3"\n'
                '[models."kimi-code/kimi-for-coding"]\n'
                'display_name = "K2.8"\n',
                encoding="utf-8",
            )
            with patch.object(kimi_engine, "_home", return_value=home), \
                 patch.dict("os.environ", {}, clear=False):
                env = dict(**{k: v for k, v in __import__("os").environ.items()
                              if k != "KIMI_MODELS"})
                with patch.dict("os.environ", env, clear=True):
                    models = kimi_engine._discover_kimi_models()
            self.assertEqual(models[0], "kimi-code/k3")
            self.assertIn("kimi-code/kimi-for-coding", models)


class KimiStreamTest(unittest.IsolatedAsyncioTestCase):
    async def test_assistant_and_tools(self) -> None:
        published: list[str] = []

        async def collect(text: str) -> None:
            published.append(text)

        stream = KimiStream("/tmp", "kimi-code/k3", collect)
        await stream.handle_event({
            "role": "assistant",
            "content": "Смотрю",
            "tool_calls": [{
                "type": "function",
                "function": {"name": "Bash", "arguments": '{"command":"ls"}'},
            }],
        })
        await stream.handle_event({"role": "tool", "content": "a"})
        await stream.handle_event({"role": "assistant", "content": "Готово."})
        self.assertEqual(stream.answer(), "Готово.")
        self.assertTrue(any("ls" in x for x in published))


class KimiCallStreamTest(unittest.IsolatedAsyncioTestCase):
    async def test_captures_session_and_parses_answer(self) -> None:
        events = [
            {"role": "meta", "type": "system.version", "version": "2.4.1"},
            {"role": "assistant", "content": "пинг"},
        ]

        class FakeProc:
            returncode = 0
            stdout = object()  # non-None; reader uses patched iter_json_events
            stderr = None

            async def wait(self) -> int:
                return 0

        async def fake_spawn(cmd, **kwargs):
            self.cmd = cmd
            return FakeProc()

        async def fake_wait_stream(proc, coro, timeout):
            # Drive the stream reader manually via iter replacement below.
            await coro
            return True

        async def fake_iter(stdout, label):
            for e in events:
                yield e

        async def fake_read_stderr(proc):
            return ""

        with TemporaryDirectory() as tmp:
            eng = KimiEngine()
            with patch.object(kimi_engine, "spawn", side_effect=fake_spawn), \
                 patch.object(kimi_engine, "wait_stream", side_effect=fake_wait_stream), \
                 patch("engines.kimi_events.iter_json_events", side_effect=fake_iter), \
                 patch.object(kimi_engine, "read_stderr", side_effect=fake_read_stderr), \
                 patch.object(kimi_engine, "register_proc"), \
                 patch.object(kimi_engine, "unregister_proc"), \
                 patch.object(kimi_engine, "_list_sessions", side_effect=[
                     [],
                     [{"id": "session_new", "updatedAt": 2}],
                 ]), \
                 patch.object(kimi_engine, "resolve_cwd", return_value=tmp):
                ok, text, sid, model = await eng.call_stream(
                    "placeholder-1", "hi", (1, 2), tmp,
                    AsyncMock(), {}, {},
                )
        self.assertTrue(ok)
        self.assertEqual(text, "пинг")
        self.assertEqual(sid, "session_new")
        self.assertIn("-p", self.cmd)


class PersistentKimiTest(unittest.IsolatedAsyncioTestCase):
    async def test_mcp_servers_include_playwright_stdio(self) -> None:
        stdin = SimpleNamespace(write=lambda b: None, drain=AsyncMock())
        proc = SimpleNamespace(returncode=None, stdin=stdin)
        worker = PersistentKimiWorker((1, 2), proc, "sid", "/tmp", None)
        with patch.object(
            persistent_kimi, "_playwright_stdio_server",
            return_value={"type": "stdio", "name": "playwright", "command": "npx", "args": []},
        ), patch.object(
            persistent_kimi, "acp_mcp_servers",
            return_value=[{"type": "http", "name": "qw", "url": "https://x"}],
        ):
            servers = worker.mcp_servers("teamlead", True)
        self.assertEqual(servers[0]["name"], "qw")
        self.assertEqual(servers[1]["type"], "stdio")


if __name__ == "__main__":
    unittest.main()
