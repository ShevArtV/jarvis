"""Поведение call_stream движков на подставном процессе.

CLI не запускается: spawn/feed_stdin/terminate_process_tree подменены, stdout —
заранее заданные JSONL-строки. Проверяются исходы (успех, ошибка, пустой ответ,
таймаут, отмена, нет папки, нет бинаря), выбор new/resume и снятие процесса
из active_procs/spawn_procs.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import tempfile
import unittest
from unittest.mock import patch

from engines import claude_engine, codex_engine, common, cursor_engine, cursor_events, opencode_engine


class _FakeStdout:
    def __init__(self, lines: list[str], hang: bool) -> None:
        self._lines = [line.encode() + b"\n" for line in lines]
        self._hang = hang

    async def readline(self) -> bytes:
        if self._lines:
            return self._lines.pop(0)
        if self._hang:
            await asyncio.sleep(3600)
        return b""


class _FakeStderr:
    def __init__(self, data: str) -> None:
        self._data = data.encode()

    async def read(self) -> bytes:
        return self._data


class _FakeProc:
    pid = 999999

    def __init__(self, lines: list[str], rc: int, stderr: str, hang: bool) -> None:
        self.stdout = _FakeStdout(lines, hang)
        self.stderr = _FakeStderr(stderr)
        self.returncode: int | None = None
        self._rc = rc

    async def wait(self) -> int:
        self.returncode = self._rc
        return self._rc


class _Harness:
    """Подмена процесса и сбор всего, что call_stream отдал наружу."""

    def __init__(self, module, lines=(), rc=0, stderr="", hang=False, spawn_error=None):
        self.module = module
        self.proc = _FakeProc([json.dumps(x) if not isinstance(x, str) else x for x in lines],
                              rc, stderr, hang)
        self.spawn_error = spawn_error
        self.cmd: list[str] | None = None
        self.spawn_kwargs: dict = {}
        self.stdin: str | None = None
        self.terminated = 0
        self.published: list[str] = []
        self.active_procs: dict = {}
        self.spawn_procs: dict = {}
        self.registered: list = []

    async def _spawn(self, cmd, **kwargs):
        self.cmd = list(cmd)
        self.spawn_kwargs = kwargs
        if self.spawn_error is not None:
            raise self.spawn_error
        return self.proc

    async def _feed(self, proc, text):
        self.stdin = text

    async def _terminate(self, proc, **kwargs):
        self.terminated += 1
        proc.returncode = -15

    async def on_intermediate(self, text: str) -> None:
        self.published.append(text)
        self.registered.append((dict(self.active_procs), dict(self.spawn_procs)))

    @contextlib.contextmanager
    def patched(self):
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(self.module, "spawn", self._spawn))
            stack.enter_context(patch.object(self.module, "feed_stdin", self._feed))
            for mod in (self.module, common):
                if hasattr(mod, "terminate_process_tree"):
                    stack.enter_context(patch.object(mod, "terminate_process_tree", self._terminate))
            yield

    def run(self, engine, session_id, cwd, **kwargs):
        async def go():
            return await engine.call_stream(
                session_id, "hello", (1, 2), cwd, self.on_intermediate,
                self.active_procs, self.spawn_procs, **kwargs,
            )

        with self.patched():
            return asyncio.run(go())


class _Base(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.cwd = self._tmp.name
        env = patch.dict("os.environ", {}, clear=False)
        env.start()
        self.addCleanup(env.stop)
        # Предупреждения движков о rc != 0 — ожидаемы, в выводе тестов шумят.
        logging.disable(logging.WARNING)
        self.addCleanup(logging.disable, logging.NOTSET)
        for name in ("CLAUDE_MODEL", "CODEX_MODEL", "OPENCODE_MODEL",
                     "OPENCODE_AGENT", "OPENCODE_VARIANT", "CURSOR_MODEL"):
            os.environ.pop(name, None)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def assert_unregistered(self, h: _Harness) -> None:
        self.assertEqual(h.active_procs, {})
        self.assertEqual(h.spawn_procs, {})


def _cancel_case(test: unittest.TestCase, h: _Harness, engine, session_id: str, cwd: str) -> None:
    async def go():
        task = asyncio.create_task(engine.call_stream(
            session_id, "hello", (1, 2), cwd, h.on_intermediate, h.active_procs, h.spawn_procs,
        ))
        await asyncio.sleep(0.05)
        test.assertEqual(h.active_procs, {(1, 2): h.proc})
        task.cancel()
        with test.assertRaises(asyncio.CancelledError):
            await task

    with h.patched():
        asyncio.run(go())
    test.assertEqual(h.terminated, 1)
    test.assertEqual(h.active_procs, {})


class ClaudeCallStreamTest(_Base):
    def engine(self, exists=False):
        cleared: list[str] = []
        self.cleared = cleared
        stack = contextlib.ExitStack()
        stack.enter_context(patch.object(claude_engine.ClaudeEngine, "session_exists",
                                         lambda _self, sid, cwd: exists))
        stack.enter_context(patch.object(claude_engine.ClaudeEngine, "clear_stale_session_pidfile",
                                         lambda _self, sid: cleared.append(sid)))
        self.addCleanup(stack.close)
        return claude_engine.ClaudeEngine()

    def test_success_new_session(self) -> None:
        h = _Harness(claude_engine, [
            {"type": "system", "subtype": "init", "model": "claude-x"},
            {"type": "assistant", "message": {"model": "other", "content": [
                {"type": "text", "text": "шаг 1"},
                {"type": "tool_use", "name": "Bash", "input": {"command": "ls"}},
            ]}},
            "not json",
            {"type": "assistant", "message": {"content": [{"type": "text", "text": "шаг 2"}]}},
            {"type": "result", "result": "готово"},
        ])
        res = h.run(self.engine(), "sid-1", self.cwd)
        self.assertEqual(res, (True, "готово", "sid-1", "claude-x"))
        self.assertIn("--session-id", h.cmd)
        self.assertNotIn("--resume", h.cmd)
        self.assertEqual(h.stdin, "hello")
        self.assertEqual(h.published, ["шаг 1\n💻 ls", "шаг 2"])
        self.assertEqual(h.registered[0][0], {(1, 2): h.proc})
        self.assert_unregistered(h)

    def test_resume_clears_pidfile(self) -> None:
        h = _Harness(claude_engine, [{"type": "result", "result": "ok"}])
        res = h.run(self.engine(exists=True), "sid-1", self.cwd)
        self.assertEqual(res, (True, "ok", "sid-1", None))
        self.assertEqual(h.cmd[-2:], ["--resume", "sid-1"])
        self.assertEqual(self.cleared, ["sid-1"])

    def test_spawn_never_resumes(self) -> None:
        h = _Harness(claude_engine, [
            {"type": "assistant", "message": {"model": "m2", "content": [{"type": "text", "text": "x"}]}},
            {"type": "result", "result": "ok"},
        ])
        res = h.run(self.engine(exists=True), "sid-1", self.cwd, spawn_id="s1")
        self.assertEqual(res, (True, "ok", "sid-1", "m2"))
        self.assertEqual(h.cmd[-2:], ["--session-id", "sid-1"])
        self.assertEqual(self.cleared, [])
        self.assertEqual(h.registered[0][1], {(1, 2, "s1"): h.proc})
        self.assert_unregistered(h)

    def test_error_rc(self) -> None:
        h = _Harness(claude_engine, [], rc=1, stderr="boom")
        self.assertEqual(h.run(self.engine(), "sid", self.cwd),
                         (False, "Ошибка claude (rc=1): boom", "sid", None))
        h = _Harness(claude_engine, [], rc=1)
        self.assertEqual(h.run(self.engine(), "sid", self.cwd)[1], "Ошибка claude (rc=1): (пусто)")

    def test_error_rc_with_text_is_ok(self) -> None:
        h = _Harness(claude_engine, [{"type": "result", "result": "частично"}], rc=1)
        self.assertEqual(h.run(self.engine(), "sid", self.cwd), (True, "частично", "sid", None))

    def test_killed(self) -> None:
        h = _Harness(claude_engine, [{"type": "result", "result": "x"}], rc=-9)
        self.assertEqual(h.run(self.engine(), "sid", self.cwd), (False, "", "sid", None))

    def test_empty(self) -> None:
        h = _Harness(claude_engine, [{"type": "result", "result": "  "}], stderr="warn")
        self.assertEqual(h.run(self.engine(), "sid", self.cwd),
                         (False, "claude вернул пустой ответ.\nwarn", "sid", None))

    def test_timeout(self) -> None:
        h = _Harness(claude_engine, [
            {"type": "system", "subtype": "init", "model": "m"},
            {"type": "assistant", "message": {"content": [{"type": "text", "text": "t"}]}},
        ], hang=True)
        with patch.object(claude_engine, "CLAUDE_TIMEOUT", 0.05):
            res = h.run(self.engine(), "sid", self.cwd)
        self.assertEqual(res, (False, "Timeout: claude не ответил за 0.05с.", "sid", "m"))
        self.assertEqual(h.terminated, 1)
        self.assertEqual(h.published, ["t"])
        self.assert_unregistered(h)

    def test_cancel(self) -> None:
        h = _Harness(claude_engine, [], hang=True)
        _cancel_case(self, h, self.engine(), "sid", self.cwd)

    def test_missing_cwd_and_bin(self) -> None:
        h = _Harness(claude_engine)
        res = h.run(self.engine(), "sid", self.cwd + "/nope")
        self.assertFalse(res[0])
        self.assertIn("не существует", res[1])
        self.assertIsNone(h.cmd)
        h = _Harness(claude_engine, spawn_error=FileNotFoundError())
        self.assertEqual(h.run(self.engine(), "sid", self.cwd),
                         (False, f"`{claude_engine.CLAUDE_BIN}` не найден в PATH.", "sid", None))


class CodexCallStreamTest(_Base):
    def engine(self, exists=False):
        p = patch.object(codex_engine.CodexEngine, "session_exists", lambda self, sid, cwd: exists)
        p.start()
        self.addCleanup(p.stop)
        return codex_engine.CodexEngine()

    def test_success_new_placeholder(self) -> None:
        h = _Harness(codex_engine, [
            {"type": "thread.started", "thread_id": "th-1"},
            {"type": "turn.started", "turn": {"model": "gpt-x"}},
            {"type": "item.started", "item": {"type": "command_execution", "command": "ls -la"}},
            {"type": "item.completed", "item": {"type": "reasoning", "text": "думаю"}},
            {"type": "item.started", "item": {"type": "agent_message", "text": "skip"}},
            {"type": "item.completed", "item": {"type": "agent_message", "text": "ответ"}},
            {"type": "turn.completed"},
        ])
        eng = self.engine(exists=True)
        sid = eng.new_session_id()
        self.assertTrue(sid.startswith("placeholder-"))
        res = h.run(eng, sid, self.cwd, system_prefix="[SYSTEM: x]")
        self.assertEqual(res, (True, "ответ", "th-1", "gpt-x"))
        self.assertEqual(h.cmd[:2], [codex_engine.CODEX_BIN, "exec"])
        self.assertNotIn("resume", h.cmd)
        self.assertIn("--sandbox", h.cmd)
        self.assertTrue(h.stdin.startswith("[SYSTEM: x]\n\n[SYSTEM NOTE FOR CODEX]"))
        self.assertTrue(h.stdin.endswith("\n\nhello"))
        self.assertEqual(h.published, ["🔧 exec ls -la", "💭 думаю\nответ"])
        self.assert_unregistered(h)

    def test_resume(self) -> None:
        h = _Harness(codex_engine, [
            {"type": "item.completed", "item": {"type": "agent_message", "text": "ок"}},
        ])
        with patch.dict("os.environ", {"CODEX_MODEL": "gpt-env"}):
            res = h.run(self.engine(exists=True), "th-old", self.cwd, system_prefix="[SYSTEM: x]")
        self.assertEqual(res, (True, "ок", "th-old", "gpt-env"))
        self.assertEqual(h.cmd[:3], [codex_engine.CODEX_BIN, "exec", "resume"])
        self.assertEqual(h.cmd[-2:], ["th-old", "-"])
        self.assertIn("--model", h.cmd)
        self.assertFalse(h.stdin.startswith("[SYSTEM: x]"))

    def test_spawn_keeps_session_id(self) -> None:
        h = _Harness(codex_engine, [
            {"type": "thread.started", "thread_id": "th-new"},
            {"type": "item.completed", "item": {"type": "agent_message", "text": "ок"}},
        ])
        res = h.run(self.engine(exists=True), "th-old", self.cwd, spawn_id="s1")
        self.assertEqual(res, (True, "ок", "th-old", None))
        self.assertIn("--ephemeral", h.cmd)
        self.assertNotIn("resume", h.cmd)
        self.assert_unregistered(h)

    def test_stream_errors(self) -> None:
        h = _Harness(codex_engine, [
            {"type": "thread.started", "thread_id": "th-1"},
            {"type": "error", "message": "usage limit"},
            {"type": "turn.failed", "error": {"message": "usage limit"}},
            {"type": "turn.failed", "error": {"message": "second"}},
        ], rc=1, stderr="stderr text")
        self.assertEqual(h.run(self.engine(), "placeholder-1", self.cwd),
                         (False, "Ошибка codex (rc=1): usage limit\nsecond", "placeholder-1", None))
        h = _Harness(codex_engine, [], rc=2, stderr="stderr text")
        self.assertEqual(h.run(self.engine(), "p", self.cwd)[1], "Ошибка codex (rc=2): stderr text")
        h = _Harness(codex_engine, [], rc=2)
        self.assertEqual(h.run(self.engine(), "p", self.cwd)[1], "Ошибка codex (rc=2): (пусто)")

    def test_killed_and_empty(self) -> None:
        h = _Harness(codex_engine, [{"type": "thread.started", "thread_id": "th-1"}], rc=-9)
        self.assertEqual(h.run(self.engine(), "placeholder-1", self.cwd),
                         (False, "", "th-1", None))
        h = _Harness(codex_engine, [{"type": "thread.started", "thread_id": "th-1"}], stderr="e")
        self.assertEqual(h.run(self.engine(), "placeholder-1", self.cwd),
                         (False, "codex вернул пустой ответ.\ne", "th-1", None))

    def test_timeout_and_cancel(self) -> None:
        h = _Harness(codex_engine, [{"type": "turn.started", "turn": {"model": "m"}}], hang=True)
        with patch.object(codex_engine, "CODEX_TIMEOUT", 0.05):
            res = h.run(self.engine(), "p", self.cwd)
        self.assertEqual(res, (False, "Timeout: codex не ответил за 0.05с.", "p", "m"))
        self.assertEqual(h.terminated, 1)
        self.assert_unregistered(h)
        _cancel_case(self, _Harness(codex_engine, [], hang=True), self.engine(), "p", self.cwd)

    def test_missing_cwd_bin_and_profile_cleanup(self) -> None:
        cleaned: list = []
        overrides = (["--profile", "jarvis-x"], ["/tmp/x.toml"])
        with patch.object(codex_engine, "_mcp_config_overrides", lambda *a, **k: overrides), \
                patch("engines.topic_mcp.cleanup_codex_profile", cleaned.append):
            h = _Harness(codex_engine)
            res = h.run(self.engine(), "p", self.cwd + "/nope", mcp_topic_role="agent")
            self.assertIn("не существует", res[1])
            h = _Harness(codex_engine, spawn_error=FileNotFoundError())
            self.assertEqual(h.run(self.engine(), "p", self.cwd, mcp_topic_role="agent"),
                             (False, f"`{codex_engine.CODEX_BIN}` не найден в PATH.", "p", None))
            h = _Harness(codex_engine, spawn_error=PermissionError())
            with self.assertRaises(PermissionError):
                h.run(self.engine(), "p", self.cwd, mcp_topic_role="agent")
            h = _Harness(codex_engine, [
                {"type": "item.completed", "item": {"type": "agent_message", "text": "ок"}},
            ])
            h.run(self.engine(), "p", self.cwd, mcp_topic_role="agent")
            self.assertEqual(h.cmd[:3], [codex_engine.CODEX_BIN, "--profile", "jarvis-x"])
        self.assertEqual(cleaned, ["/tmp/x.toml"] * 4)


class OpenCodeCallStreamTest(_Base):
    def engine(self):
        return opencode_engine.OpenCodeEngine()

    def test_success_new_session(self) -> None:
        h = _Harness(opencode_engine, [
            {"type": "step_start", "sessionID": "ses_1", "part": {"modelID": "ds-v4"}},
            {"type": "tool_use", "sessionID": "ses_1",
             "part": {"tool": "bash", "state": {"input": {"command": "ls"}}}},
            {"type": "text", "sessionID": "ses_1", "part": {"id": "p1", "text": "часть 1"}},
            {"type": "text", "sessionID": "ses_1", "part": {"id": "p2", "text": "часть 2"}},
            {"type": "step_finish", "sessionID": "ses_1", "part": {"reason": "stop"}},
        ])
        res = h.run(self.engine(), "placeholder-1", self.cwd, system_prefix="[SYSTEM: x]")
        self.assertEqual(res, (True, "часть 1\nчасть 2", "ses_1", "ds-v4"))
        self.assertNotIn("--session", h.cmd)
        self.assertTrue(h.stdin.startswith("[SYSTEM: x]\n\n[SYSTEM NOTE FOR OPENCODE]"))
        self.assertEqual(h.published, ["🔧 bash command=ls", "часть 1\nчасть 2"])
        self.assertNotIn("OPENCODE_CONFIG", h.spawn_kwargs["env"])
        self.assertEqual(h.spawn_kwargs["env"]["OPENCODE_CLIENT"], "jarvis")
        self.assert_unregistered(h)

    def test_resume_deltas_and_message_updated(self) -> None:
        h = _Harness(opencode_engine, [
            {"type": "message.part.delta", "delta": "при"},
            {"type": "message.part.delta", "delta": "вет"},
            {"type": "message.updated", "message": {"text": "игнор: есть чанки"}},
        ])
        with patch.dict("os.environ", {"OPENCODE_MODEL": "a/b", "OPENCODE_AGENT": "ag",
                                       "OPENCODE_VARIANT": "v"}):
            res = h.run(self.engine(), "ses_old", self.cwd, system_prefix="[SYSTEM: x]")
        self.assertEqual(res, (True, "привет", "ses_old", "a/b"))
        self.assertEqual(h.cmd[-6:], ["--agent", "ag", "--variant", "v", "--session", "ses_old"])
        self.assertFalse(h.stdin.startswith("[SYSTEM: x]"))

    def test_part_updated_and_message_updated(self) -> None:
        h = _Harness(opencode_engine, [
            {"type": "message.updated", "properties": {"message": {"text": "итог"}}},
        ])
        self.assertEqual(h.run(self.engine(), "ses_old", self.cwd), (True, "итог", "ses_old", None))
        h = _Harness(opencode_engine, [
            {"type": "message.part.updated", "properties": {"part": {"id": "p", "text": "a"}}},
            {"type": "message.part.updated", "properties": {"part": {"id": "p", "text": "ab"}}},
        ])
        self.assertEqual(h.run(self.engine(), "ses_old", self.cwd), (True, "ab", "ses_old", None))

    def test_spawn(self) -> None:
        h = _Harness(opencode_engine, [
            {"type": "text", "sessionID": "ses_new", "text": "ок"},
        ])
        res = h.run(self.engine(), "ses_old", self.cwd, spawn_id="s1")
        self.assertEqual(res, (True, "ок", "ses_old", None))
        self.assertNotIn("--session", h.cmd)
        self.assert_unregistered(h)

    def test_errors(self) -> None:
        h = _Harness(opencode_engine, [
            {"type": "error", "sessionID": "ses_1", "error": {"data": {"message": "квота"}}},
            {"type": "session.error", "properties": {"error": "квота"}},
        ], rc=1, stderr="se")
        self.assertEqual(h.run(self.engine(), "placeholder-1", self.cwd),
                         (False, "Ошибка opencode (rc=1): квота", "placeholder-1", None))
        h = _Harness(opencode_engine, [], rc=1, stderr="se")
        self.assertEqual(h.run(self.engine(), "p", self.cwd)[1], "Ошибка opencode (rc=1): se")
        h = _Harness(opencode_engine, [], rc=1)
        self.assertEqual(h.run(self.engine(), "p", self.cwd)[1], "Ошибка opencode (rc=1): (пусто)")
        h = _Harness(opencode_engine, [{"type": "x", "sessionID": "ses_1"}], rc=-9)
        self.assertEqual(h.run(self.engine(), "placeholder-1", self.cwd),
                         (False, "", "ses_1", None))
        h = _Harness(opencode_engine, [{"type": "x", "sessionID": "ses_1"}], stderr="e")
        self.assertEqual(h.run(self.engine(), "placeholder-1", self.cwd),
                         (False, "opencode вернул пустой ответ.\ne", "ses_1", None))

    def test_timeout_cancel_missing(self) -> None:
        h = _Harness(opencode_engine, [{"type": "x", "part": {"modelID": "m"}}], hang=True)
        with patch.object(opencode_engine, "OPENCODE_TIMEOUT", 0.05):
            res = h.run(self.engine(), "p", self.cwd)
        self.assertEqual(res, (False, "Timeout: opencode не ответил за 0.05с.", "p", "m"))
        self.assertEqual(h.terminated, 1)
        self.assert_unregistered(h)
        _cancel_case(self, _Harness(opencode_engine, [], hang=True), self.engine(), "p", self.cwd)
        h = _Harness(opencode_engine)
        self.assertIn("не существует", h.run(self.engine(), "p", self.cwd + "/nope")[1])
        h = _Harness(opencode_engine, spawn_error=FileNotFoundError())
        self.assertEqual(h.run(self.engine(), "p", self.cwd),
                         (False, f"`{opencode_engine.OPENCODE_BIN}` не найден в PATH.", "p", None))

    def test_mcp_config_tempfile_removed(self) -> None:
        fd, path = tempfile.mkstemp()
        os.close(fd)
        with patch.object(opencode_engine, "_opencode_mcp_config", lambda *a: path):
            h = _Harness(opencode_engine, [{"type": "text", "text": "ок"}])
            h.run(self.engine(), "p", self.cwd, mcp_playwright=True)
        self.assertEqual(h.spawn_kwargs["env"]["OPENCODE_CONFIG"], path)
        self.assertFalse(os.path.exists(path))


class CursorCallStreamTest(_Base):
    def engine(self):
        return cursor_engine.CursorEngine()

    def _init(self, sid="s1"):
        return {"type": "system", "subtype": "init", "session_id": sid, "model": "Auto"}

    def test_success_new_session_answer_after_last_tool(self) -> None:
        h = _Harness(cursor_engine, [
            self._init(),
            {"type": "thinking", "subtype": "delta", "text": "думаю"},
            {"type": "assistant", "message": {"content": [{"type": "text", "text": "привет"}]}},
            {"type": "tool_call", "subtype": "started", "tool_call": {"shellToolCall": {
                "args": {"command": "ls"}, "description": "Список файлов"}}},
            {"type": "tool_call", "subtype": "completed", "tool_call": {"shellToolCall": {}}},
            {"type": "assistant", "message": {"content": [{"type": "text", "text": "Каталог пуст."}]}},
            {"type": "result", "subtype": "success", "is_error": False,
             "result": "приветКаталог пуст."},
        ])
        res = h.run(self.engine(), "s1", self.cwd, system_prefix="[SYSTEM: x]")
        self.assertEqual(res, (True, "Каталог пуст.", "s1", "Auto"))
        self.assertEqual(h.cmd[:2], [cursor_engine.CURSOR_BIN, "--print"])
        self.assertEqual(h.cmd[-2:], ["--resume", "s1"])
        self.assertIn("--force", h.cmd)
        self.assertNotIn("--model", h.cmd)
        self.assertTrue(h.stdin.startswith("[SYSTEM: x]\n\n[SYSTEM NOTE FOR CURSOR]"))
        self.assertEqual("\n".join(h.published), "привет\n💻 Список файлов\nКаталог пуст.")
        self.assert_unregistered(h)

    def test_resume_with_model(self) -> None:
        h = _Harness(cursor_engine, [{"type": "result", "result": "ок"}])
        with patch.object(cursor_engine.CursorEngine, "session_exists", lambda *a: True), \
                patch.dict("os.environ", {"CURSOR_MODEL": "gpt-5.2"}):
            res = h.run(self.engine(), "s1", self.cwd, system_prefix="[SYSTEM: x]")
        self.assertEqual(res, (True, "ок", "s1", "gpt-5.2"))
        self.assertEqual(h.cmd[-4:], ["--resume", "s1", "--model", "gpt-5.2"])
        self.assertTrue(h.stdin.startswith("[SYSTEM NOTE FOR CURSOR]"))

    def test_spawn_is_always_new(self) -> None:
        h = _Harness(cursor_engine, [{"type": "result", "result": "ок"}])
        with patch.object(cursor_engine.CursorEngine, "session_exists", lambda *a: True):
            h.run(self.engine(), "s2", self.cwd, spawn_id="sp", system_prefix="[SYSTEM: x]")
        self.assertTrue(h.stdin.startswith("[SYSTEM: x]"))
        self.assert_unregistered(h)

    def test_errors(self) -> None:
        h = _Harness(cursor_engine, [], rc=1, stderr="Cannot use this model: x")
        self.assertEqual(h.run(self.engine(), "s1", self.cwd),
                         (False, "Ошибка cursor (rc=1): Cannot use this model: x", "s1", None))
        h = _Harness(cursor_engine, [{"type": "result", "is_error": True, "result": "квота"}])
        self.assertEqual(h.run(self.engine(), "s1", self.cwd)[1], "Ошибка cursor (rc=0): квота")
        h = _Harness(cursor_engine, [self._init()], rc=-9)
        self.assertEqual(h.run(self.engine(), "s1", self.cwd), (False, "", "s1", "Auto"))
        h = _Harness(cursor_engine, [self._init()], stderr="e")
        self.assertEqual(h.run(self.engine(), "s1", self.cwd),
                         (False, "cursor вернул пустой ответ.\ne", "s1", "Auto"))

    def test_timeout_cancel_missing(self) -> None:
        h = _Harness(cursor_engine, [self._init()], hang=True)
        with patch.object(cursor_engine, "CURSOR_TIMEOUT", 0.05):
            res = h.run(self.engine(), "s1", self.cwd)
        self.assertEqual(res, (False, "Timeout: cursor не ответил за 0.05с.", "s1", "Auto"))
        self.assertEqual(h.terminated, 1)
        self.assert_unregistered(h)
        _cancel_case(self, _Harness(cursor_engine, [], hang=True), self.engine(), "s1", self.cwd)
        h = _Harness(cursor_engine)
        self.assertIn("не существует", h.run(self.engine(), "s1", self.cwd + "/nope")[1])
        h = _Harness(cursor_engine, spawn_error=FileNotFoundError())
        self.assertEqual(h.run(self.engine(), "s1", self.cwd),
                         (False, f"`{cursor_engine.CURSOR_BIN}` не найден в PATH.", "s1", None))

    def test_session_exists_by_chat_dir(self) -> None:
        import hashlib
        from pathlib import Path

        with patch.dict("os.environ", {"CURSOR_CONFIG_DIR": self.cwd}):
            self.assertFalse(self.engine().session_exists("s1", "/proj"))
            chat = Path(self.cwd, "chats", hashlib.md5(b"/proj").hexdigest(), "s1")
            chat.mkdir(parents=True)
            (chat / "store.db").write_bytes(b"")
            self.assertTrue(self.engine().session_exists("s1", "/proj"))

    def test_mcp_tool_step(self) -> None:
        step = cursor_events._cursor_tool_step({"mcpToolCall": {"args": {
            "providerIdentifier": "jarvis", "toolName": "manager_inbox",
            "args": {"thread_id": 5}}}}, self.cwd)
        self.assertEqual(step, "📨 jarvis · manager_inbox (thread 5)")
        step = cursor_events._cursor_tool_step(
            {"readToolCall": {"args": {"path": self.cwd + "/a.py"}}}, self.cwd)
        self.assertEqual(step, "📖 a.py")


if __name__ == "__main__":
    unittest.main()
