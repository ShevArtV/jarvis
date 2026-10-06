"""Cursor CLI (``cursor-agent``) adapter.

- stream-json через ``cursor-agent -p --output-format stream-json``; prompt — в stdin;
- id сессии назначает Jarvis: ``--resume <uuid>`` на несуществующий чат создаёт
  чат с этим id, поэтому placeholder, как у codex/opencode, не нужен;
- чаты лежат в ``<config>/chats/<md5(cwd)>/<id>/`` — по ним и проверяется resume;
- системного канала нет: [SYSTEM:]-блок идёт префиксом prompt на новой сессии;
- MCP cursor берёт только из ``~/.cursor/mcp.json`` и ``<cwd>/.cursor/mcp.json``,
  per-invocation подключения нет. Manager MCP регистрируется глобально
  (engines/jarvis_mcp.py), Playwright и topic-MCP для cursor не поддержаны.

Живой процесс (/persistent) — engines/persistent_cursor.py поверх ``cursor-agent acp``.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import uuid
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from pathlib import Path
from typing import Any

from engines.base import BaseEngine
from engines.claude_engine import _tool_step
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
from engines.model_cache import cli_models, remember_labels, split_models
from engines.process_control import feed_stdin, spawn

logger = logging.getLogger(__name__)

CURSOR_BIN = os.environ.get("CURSOR_BIN", "cursor-agent")
CURSOR_TIMEOUT = int(os.environ.get("CURSOR_TIMEOUT", "3600"))
# `--list-models` отдаёт ~250 моделей, а у inline-клавиатуры Telegram потолок
# 100 кнопок. CLI ставит свои основные модели первыми — берём начало списка.
CURSOR_MODELS_LIMIT = int(os.environ.get("CURSOR_MODELS_LIMIT", "30"))

DEFAULT_CURSOR_MODELS = ["auto"]

FILE_MARKER_SYSTEM = (
    "[SYSTEM NOTE FOR CURSOR] Если нужно отправить пользователю файл "
    "(скриншот, собранный пакет, сгенерированный документ и т.п.) — выведи "
    "отдельной строкой маркер [[FILE: /абсолютный/путь]] (опционально с "
    "подписью через '|': [[FILE: /путь | подпись]]). Бот парсит это и отправит "
    "файл в Telegram. Используй только для файлов в пределах cwd сессии или "
    "явно указанных пользователем."
)

# Per-call модель. Выставляется через engines.engine_model_scope() перед
# call_stream. Если None — фолбэк на CURSOR_MODEL env, иначе --model не
# передаётся (CLI берёт свою дефолтную).
CURRENT_MODEL: ContextVar[str | None] = ContextVar("cursor_model", default=None)

# Строка `--list-models`: "<id> - <название>".
_MODEL_LINE = re.compile(r"^(\S+) - (.+)$")

# Вид tool_call cursor → имя инструмента claude: журнал шагов общий с claude.
_TOOL_NAMES = {
    "shell": "Bash",
    "read": "Read",
    "edit": "Edit",
    "write": "Write",
    "delete": "Delete",
    "grep": "Grep",
    "glob": "Glob",
    "semSearch": "Grep",
    "webFetch": "WebFetch",
    "fetch": "WebFetch",
    "webSearch": "WebSearch",
    "task": "Task",
}


def _config_dir() -> Path:
    """Каталог данных CLI — тот же порядок, что у самого cursor-agent."""
    explicit = os.environ.get("CURSOR_CONFIG_DIR", "").strip()
    if explicit:
        return Path(explicit)
    xdg = os.environ.get("XDG_CONFIG_HOME", "").strip()
    if xdg:
        return Path(xdg) / "cursor"
    return Path.home() / ".cursor"


def _chat_dir(session_id: str, cwd: str) -> Path:
    digest = hashlib.md5(cwd.encode("utf-8")).hexdigest()  # noqa: S324 — имя каталога CLI, не криптография
    return _config_dir() / "chats" / digest / session_id


def _discover_cursor_models() -> list[str]:
    override = split_models(os.environ.get("CURSOR_MODELS"))
    if override:
        return override
    models: list[str] = []
    labels: dict[str, str] = {}
    for line in cli_models([CURSOR_BIN, "--list-models"]):
        match = _MODEL_LINE.match(line)
        if not match:
            continue
        model, label = match.groups()
        models.append(model)
        # В названиях встречаются хвостовые zero-width символы.
        labels[model] = label.strip().strip("​")
    models = models[:CURSOR_MODELS_LIMIT]
    remember_labels({m: labels[m] for m in models})
    return models


def _cursor_prompt(prompt: str, system_prefix: str | None, resume_mode: bool) -> str:
    """Нет канала system-prompt: FILE-маркер клеим каждый ход, общий
    [SYSTEM:]-блок — только на НОВОЙ сессии (на resume он уже в транскрипте)."""
    prefix_parts: list[str] = []
    if system_prefix and not resume_mode:
        prefix_parts.append(system_prefix)
    prefix_parts.append(FILE_MARKER_SYSTEM)
    return "\n\n".join(prefix_parts) + "\n\n" + prompt


def _cursor_command(session_id: str, cwd: str, model: str | None) -> list[str]:
    cmd = [
        CURSOR_BIN, "--print",
        "--output-format", "stream-json",
        "--force", "--trust", "--approve-mcps",
        "--workspace", cwd,
        "--resume", session_id,
    ]
    if model:
        cmd.extend(["--model", model])
    return cmd


def _cursor_tool_step(tool_call: Any, cwd: str) -> str | None:
    """Строка журнала для ``tool_call``: ``{"shellToolCall": {"args": {...}}}``."""
    if not isinstance(tool_call, dict):
        return None
    for key, body in tool_call.items():
        if not key.endswith("ToolCall") or not isinstance(body, dict):
            continue
        kind = key[: -len("ToolCall")]
        args = body.get("args") if isinstance(body.get("args"), dict) else {}
        if kind == "mcp":
            server = args.get("providerIdentifier") or args.get("serverName") or "mcp"
            tool = args.get("toolName") or args.get("name") or ""
            return _tool_step(f"mcp__{server}__{tool}", args.get("args"), cwd)
        if kind == "shell" and body.get("description") and not args.get("description"):
            args = {**args, "description": body["description"]}
        # Файловые инструменты cursor называют путь `path`, у claude — `file_path`.
        if args.get("path") and not args.get("file_path"):
            args = {**args, "file_path": args["path"]}
        return _tool_step(_TOOL_NAMES.get(kind, kind), args, cwd)
    return None


class _CursorStream:
    """Состояние разбора stream-json одного ``cursor-agent --print``."""

    def __init__(self, cwd: str, model: str | None,
                 on_intermediate: Callable[[str], Awaitable[None]]) -> None:
        self.cwd = cwd
        self.on_intermediate = on_intermediate
        self.journal = IntermediateBuffer()
        self.final_text = ""
        # Тексты после последнего tool_call — это и есть ответ. `result` у
        # cursor склеивает ВСЕ реплики хода без разделителей.
        self.tail_texts: list[str] = []
        # init сообщает название модели («Auto», «Claude Opus 5»); до него —
        # то, что просили сами.
        self.actual_model = model
        self.error = ""

    async def flush(self, force: bool = False) -> None:
        await self.journal.flush(self.on_intermediate, force)

    async def read(self, proc: asyncio.subprocess.Process) -> None:
        assert proc.stdout is not None
        async for ev in iter_json_events(proc.stdout, "cursor"):
            await self.handle_event(ev)

    async def handle_event(self, ev: Any) -> None:
        etype = ev.get("type")
        if etype == "system" and ev.get("subtype") == "init":
            m = ev.get("model")
            if isinstance(m, str) and m:
                self.actual_model = m
        elif etype == "assistant":
            for block in (ev.get("message") or {}).get("content") or []:
                if isinstance(block, dict) and block.get("type") == "text":
                    txt = (block.get("text") or "").strip()
                    if txt:
                        self.tail_texts.append(txt)
                        self.journal.append(txt[:800])
            await self.flush()
        elif etype == "tool_call" and ev.get("subtype") == "started":
            self.tail_texts.clear()
            step = _cursor_tool_step(ev.get("tool_call"), self.cwd)
            if step:
                self.journal.append(step)
                await self.flush()
        elif etype == "result":
            r = ev.get("result")
            if isinstance(r, str):
                self.final_text = r
            if ev.get("is_error"):
                self.error = self.final_text or str(ev.get("subtype") or "error")

    def answer(self) -> str:
        return "\n\n".join(self.tail_texts) or self.final_text


class CursorEngine(BaseEngine):
    name = "cursor"
    bin_path = CURSOR_BIN
    default_models = DEFAULT_CURSOR_MODELS

    def _discover_models(self) -> list[str]:
        return _discover_cursor_models()

    def new_session_id(self) -> str:
        return str(uuid.uuid4())

    def session_exists(self, session_id: str, cwd: str) -> bool:
        return (_chat_dir(session_id, cwd) / "store.db").exists()

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
        effective_cwd = resolve_cwd(cwd)
        resume_mode = spawn_id is None and self.session_exists(session_id, effective_cwd)
        full_prompt = _cursor_prompt(prompt, system_prefix, resume_mode)
        model = CURRENT_MODEL.get() or os.environ.get("CURSOR_MODEL")
        cmd = _cursor_command(session_id, effective_cwd, model)

        if mcp_playwright:
            logger.warning("cursor: Playwright MCP per-invocation не поддержан, key=%s", key)

        logger.info(
            "cursor start: key=%s session=%s mode=%s cwd=%s prompt_len=%d spawn_id=%s",
            key, session_id, "resume" if resume_mode else "new",
            effective_cwd, len(prompt), spawn_id,
        )

        if effective_cwd and not os.path.isdir(effective_cwd):
            return missing_cwd_result(effective_cwd, session_id)

        try:
            proc = await spawn(cmd, cwd=effective_cwd, stdin=asyncio.subprocess.PIPE)
        except FileNotFoundError:
            return False, f"`{CURSOR_BIN}` не найден в PATH.", session_id, None

        await feed_stdin(proc, full_prompt)
        register_proc(proc, key, spawn_id, active_procs, spawn_procs)

        stream = _CursorStream(effective_cwd, model, on_intermediate)
        try:
            finished = await wait_stream(proc, stream.read(proc), CURSOR_TIMEOUT)
        finally:
            await stream.flush(force=True)
            unregister_proc(proc, key, spawn_id, active_procs, spawn_procs)
        if not finished:
            return False, f"Timeout: cursor не ответил за {CURSOR_TIMEOUT}с.", session_id, stream.actual_model

        stderr_text = await read_stderr(proc)
        return self._finish(proc, stream, stderr_text, key=key, session_id=session_id)

    def _finish(
        self,
        proc: asyncio.subprocess.Process,
        stream: _CursorStream,
        stderr_text: str,
        *,
        key: tuple[int, int],
        session_id: str,
    ) -> tuple[bool, str, str | None, str | None]:
        """Итог вызова по коду выхода и событию result."""
        final_text = stream.answer()
        actual_model = stream.actual_model

        if proc.returncode != 0 or stream.error:
            logger.warning(
                "cursor rc=%s error=%s stderr=%s",
                proc.returncode, stream.error[:300], stderr_text[:500],
            )
            if proc.returncode and proc.returncode < 0:
                return False, "", session_id, actual_model
            if stream.error or not final_text:
                return False, (
                    f"Ошибка cursor (rc={proc.returncode}): "
                    f"{(stream.error or stderr_text)[:1500] or '(пусто)'}"
                ), session_id, actual_model

        logger.info(
            "cursor done: key=%s rc=%s final_len=%d model=%s",
            key, proc.returncode, len(final_text), actual_model,
        )
        if not final_text.strip():
            return False, (
                "cursor вернул пустой ответ."
                + (f"\n{stderr_text[:500]}" if stderr_text else "")
            ), session_id, actual_model
        return True, final_text, session_id, actual_model
