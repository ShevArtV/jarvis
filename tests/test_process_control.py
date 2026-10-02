"""Запуск CLI на любой ОС: развёртка npm-обёрток Windows, stdin, кодировка cwd."""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from engines import process_control
from engines.claude_engine import _sessions_dir_for
from engines.process_control import feed_stdin, pid_alive, spawn, unwrap_cmd_shim

NPM_CMD_SHIM = r"""@ECHO off
GOTO start
:find_dp0
SET dp0=%~dp0
EXIT /b
:start
SETLOCAL
CALL :find_dp0

IF EXIST "%dp0%\node.exe" (
  SET "_prog=%dp0%\node.exe"
) ELSE (
  SET "_prog=node"
  SET PATHEXT=%PATHEXT:;.JS;=;%
)

endLocal & goto #_undefined_# 2>NUL || title %COMSPEC% & "%_prog%"  "%dp0%\node_modules\@openai\codex\bin\codex.js" %*
"""


class CmdShimTest(unittest.TestCase):
    def _shim(self, tmp: str, with_node: bool) -> str:
        root = Path(tmp)
        script = root / "node_modules" / "@openai" / "codex" / "bin" / "codex.js"
        script.parent.mkdir(parents=True)
        script.write_text("", encoding="utf-8")
        if with_node:
            (root / "node.exe").write_text("", encoding="utf-8")
        shim = root / "codex.cmd"
        shim.write_text(NPM_CMD_SHIM, encoding="utf-8")
        return str(shim)

    @unittest.skipIf(os.sep != "\\", "пути обёртки npm собираются через os.path.join Windows")
    def test_unwraps_npm_shim_to_node_and_script(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            argv = unwrap_cmd_shim(self._shim(tmp, with_node=True))
            self.assertEqual(argv[0], os.path.join(tmp, "node.exe"))
            self.assertTrue(argv[1].endswith(os.path.join("@openai", "codex", "bin", "codex.js")))

    def test_regex_finds_script_in_npm_shim(self) -> None:
        match = process_control._CMD_SHIM_SCRIPT_RE.search(NPM_CMD_SHIM)
        self.assertEqual(match.group(1), r"node_modules\@openai\codex\bin\codex.js")

    def test_unknown_shim_format_is_left_as_is(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            shim = Path(tmp) / "tool.cmd"
            shim.write_text("@echo off\r\nsome.exe %*\r\n", encoding="utf-8")
            self.assertIsNone(unwrap_cmd_shim(str(shim)))
            with patch.object(process_control, "IS_WINDOWS", True), \
                    patch("shutil.which", return_value=str(shim)):
                self.assertEqual(process_control.resolve_command("tool"), [str(shim)])

    def test_missing_binary_keeps_bare_name(self) -> None:
        with patch("shutil.which", return_value=None):
            self.assertEqual(process_control.resolve_command("nope"), ["nope"])


class SpawnTest(unittest.IsolatedAsyncioTestCase):
    async def test_prompt_goes_through_stdin_unchanged(self) -> None:
        prompt = "строка 1\n%PATH% & \"кавычки\"\n" + "x" * 200_000
        script = "import sys; data = sys.stdin.buffer.read(); sys.stdout.write(str(len(data)))"
        proc = await spawn([sys.executable, "-c", script], stdin=asyncio.subprocess.PIPE)
        await feed_stdin(proc, prompt)
        out, _ = await proc.communicate()
        self.assertEqual(int(out), len(prompt.encode("utf-8")))

    async def test_terminate_process_tree_stops_child(self) -> None:
        proc = await spawn([sys.executable, "-c", "import time; time.sleep(60)"])
        await process_control.terminate_process_tree(proc, terminate_timeout=5)
        self.assertIsNotNone(proc.returncode)

    async def test_pid_alive(self) -> None:
        self.assertTrue(pid_alive(os.getpid()))
        proc = await spawn([sys.executable, "-c", "pass"])
        await proc.wait()
        self.assertFalse(pid_alive(proc.pid))


class ClaudeSessionDirTest(unittest.TestCase):
    def test_encodes_like_claude_cli(self) -> None:
        self.assertEqual(
            _sessions_dir_for("/home/user/visa-center.ru").name, "-home-user-visa-center-ru",
        )
        self.assertEqual(_sessions_dir_for(r"C:\Users\me\my_proj").name, "C--Users-me-my-proj")


if __name__ == "__main__":
    unittest.main()
