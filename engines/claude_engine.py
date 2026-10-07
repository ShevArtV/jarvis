"""Claude CLI adapter.

Инкапсулирует текущее поведение (из старого telegram_bot.py):
- stream-json через `claude --print --output-format stream-json --verbose`;
- `--resume` если jsonl сессии уже существует, иначе `--session-id`;
- чистка stale pidfile в `~/.claude/sessions/`;
- `--append-system-prompt` для инструкции про маркер [[FILE:]];
- bypassPermissions.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import uuid
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from pathlib import Path
from typing import Any

from engines.base import BaseEngine
from engines.claude_cli import (
    CLAUDE_BIN,
    CLAUDE_TIMEOUT,
    _accumulate_assistant_event,
    _claude_command,
)
from engines.claude_models import DEFAULT_CLAUDE_MODELS, _discover_claude_models
from engines.common import (
    IntermediateBuffer,
    iter_json_events,
    missing_cwd_result,
    read_stderr,
    register_proc,
    resolve_cwd,
    unregister_proc,
    wait_stream,
)
from engines.process_control import feed_stdin, pid_alive, spawn

logger = logging.getLogger(__name__)

# Per-call модель. Выставляется через engines.engine_model_scope() из
# telegram_bot.py перед call_stream. Если None — фолбэк на CLAUDE_MODEL env,
# иначе --model не передаётся (CLI берёт свою дефолтную).
CURRENT_MODEL: ContextVar[str | None] = ContextVar("claude_model", default=None)


def _sessions_dir_for(cwd: str) -> Path:
    """~/.claude/projects/<encoded-cwd>/. Claude CLI заменяет каждый символ, кроме
    латинских букв и цифр, на '-': /home/user/visa-center.ru → -home-user-visa-center-ru,
    C:\\Users\\me\\proj → C--Users-me-proj."""
    encoded = re.sub(r"[^A-Za-z0-9]", "-", cwd)
    return Path.home() / ".claude" / "projects" / encoded


class _ClaudeStream:
    """Состояние разбора stream-json одного `claude --print`."""

    def __init__(self, cwd: str, on_intermediate: Callable[[str], Awaitable[None]]) -> None:
        self.cwd = cwd
        self.on_intermediate = on_intermediate
        self.journal = IntermediateBuffer()
        self.final_text = ""
        self.actual_model: str | None = None

    async def flush(self, force: bool = False) -> None:
        await self.journal.flush(self.on_intermediate, force)

    async def read(self, proc: asyncio.subprocess.Process) -> None:
        assert proc.stdout is not None
        async for ev in iter_json_events(proc.stdout, "claude"):
            await self.handle_event(ev)

    async def handle_event(self, ev: Any) -> None:
        etype = ev.get("type")
        # 'system' с subtype='init' — первый event, содержит model.
        # Также fallback: message.model в каждом assistant event.
        if etype == "system" and ev.get("subtype") == "init":
            m = ev.get("model")
            if isinstance(m, str) and m:
                self.actual_model = m
        if etype == "assistant":
            msg = ev.get("message", {}) or {}
            if not self.actual_model:
                m = msg.get("model")
                if isinstance(m, str) and m:
                    self.actual_model = m
            _accumulate_assistant_event(ev, self.journal.lines, self.cwd)
            await self.flush()
        elif etype == "result":
            r = ev.get("result")
            if isinstance(r, str):
                self.final_text = r


class ClaudeEngine(BaseEngine):
    name = "claude"
    bin_path = CLAUDE_BIN
    default_models = DEFAULT_CLAUDE_MODELS

    def _discover_models(self) -> list[str]:
        return _discover_claude_models()

    # --- Session filesystem helpers ---

    def new_session_id(self) -> str:
        return str(uuid.uuid4())

    def session_exists(self, session_id: str, cwd: str) -> bool:
        return (_sessions_dir_for(cwd) / f"{session_id}.jsonl").exists()

    def clear_stale_session_pidfile(self, session_id: str) -> None:
        """Claude CLI хранит лок-файлы в ~/.claude/sessions/<pid>.json с полем sessionId.
        Если процесс мёртв — удаляем файл, чтобы --resume не упёрся в 'session in use'."""
        sessions_dir = Path.home() / ".claude" / "sessions"
        if not sessions_dir.is_dir():
            return
        for p in sessions_dir.glob("*.json"):
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                # Чужой/битый/удалённый на ходу файл — не наш лок, пропускаем.
                logger.debug("cannot read session lock %s", p, exc_info=True)
                continue
            if data.get("sessionId") != session_id:
                continue
            pid = data.get("pid")
            if not (isinstance(pid, int) and pid_alive(pid)):
                try:
                    p.unlink()
                    logger.info(
                        "removed stale session lock %s (pid=%s sid=%s)",
                        p, pid, session_id,
                    )
                except OSError:
                    logger.warning("cannot remove stale session lock %s", p, exc_info=True)

    def _session_flags(self, session_id: str, cwd: str, is_spawn: bool) -> tuple[bool, list[str]]:
        """(resume_mode, флаги сессии). Spawn — всегда новая сессия; перед
        --resume снимаем stale pidfile."""
        if is_spawn:
            return False, ["--session-id", session_id]
        if self.session_exists(session_id, cwd):
            self.clear_stale_session_pidfile(session_id)
            return True, ["--resume", session_id]
        return False, ["--session-id", session_id]

    # --- Stream ---

    async def call_stream(
        self,
        session_id: str,
        prompt: str,
        key: tuple[int, int],
        cwd: str | None,
        on_intermediate: Callable[[str], Awaitable[None]],
        active_procs: dict,
        spawn_procs: dict,
        spawn_id: str | None = None,
        system_prefix: str | None = None,
        mcp_playwright: bool = False,
        mcp_topic_role: str | None = None,
    ) -> tuple[bool, str, str | None, str | None]:
        # cwd берётся из аргумента (если None — caller должен подставить default).
        effective_cwd = resolve_cwd(cwd)

        resume_mode, session_flags = self._session_flags(
            session_id, effective_cwd, spawn_id is not None,
        )
        model = CURRENT_MODEL.get() or os.environ.get("CLAUDE_MODEL")
        cmd = _claude_command(session_flags, model, system_prefix, mcp_playwright, mcp_topic_role)

        logger.info(
            "claude start: key=%s session=%s mode=%s cwd=%s prompt_len=%d spawn_id=%s",
            key, session_id, "resume" if resume_mode else "new",
            effective_cwd, len(prompt), spawn_id,
        )

        if effective_cwd and not os.path.isdir(effective_cwd):
            return missing_cwd_result(effective_cwd, session_id)

        try:
            proc = await spawn(cmd, cwd=effective_cwd, stdin=asyncio.subprocess.PIPE)
        except FileNotFoundError:
            return False, f"`{CLAUDE_BIN}` не найден в PATH.", session_id, None

        await feed_stdin(proc, prompt)
        register_proc(proc, key, spawn_id, active_procs, spawn_procs)

        stream = _ClaudeStream(effective_cwd, on_intermediate)
        try:
            finished = await wait_stream(proc, stream.read(proc), CLAUDE_TIMEOUT)
        finally:
            await stream.flush(force=True)
            unregister_proc(proc, key, spawn_id, active_procs, spawn_procs)
        if not finished:
            return False, f"Timeout: claude не ответил за {CLAUDE_TIMEOUT}с.", session_id, stream.actual_model

        stderr_text = await read_stderr(proc)
        return self._finish(proc, stream, stderr_text, key=key, session_id=session_id)

    def _finish(
        self,
        proc: asyncio.subprocess.Process,
        stream: _ClaudeStream,
        stderr_text: str,
        *,
        key: tuple[int, int],
        session_id: str,
    ) -> tuple[bool, str, str | None, str | None]:
        """Итог вызова по коду выхода и тексту из события result."""
        final_text = stream.final_text
        actual_model = stream.actual_model

        if proc.returncode != 0:
            logger.warning("claude rc=%s stderr=%s", proc.returncode, stderr_text[:500])
            if proc.returncode and proc.returncode < 0:
                return False, "", session_id, actual_model
            if not final_text:
                return False, (
                    f"Ошибка claude (rc={proc.returncode}): "
                    f"{stderr_text[:1500] or '(пусто)'}"
                ), session_id, actual_model

        logger.info(
            "claude done: key=%s rc=%s final_len=%d model=%s",
            key, proc.returncode, len(final_text), actual_model,
        )
        if not final_text.strip():
            return False, (
                "claude вернул пустой ответ."
                + (f"\n{stderr_text[:500]}" if stderr_text else "")
            ), session_id, actual_model
        return True, final_text, session_id, actual_model
