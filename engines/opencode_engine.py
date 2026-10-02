"""OpenCode CLI adapter.

OpenCode generates its own session id on first run. We store a placeholder in
Jarvis DB, catch the real ``ses_...`` id from JSONL output, then persist it.
Resume goes through ``opencode run --session <id>``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
import uuid
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from typing import Any

from engines.base import BaseEngine
from engines.common import (
    PLACEHOLDER_PREFIX,
    IntermediateBuffer,
    cleanup_tempfile,
    is_placeholder,
    iter_json_events,
    missing_cwd_result,
    read_stderr,
    register_proc,
    resolve_cwd,
    unregister_proc,
    wait_stream,
)
from engines.model_cache import cli_models, split_models
from engines.process_control import feed_stdin, spawn

logger = logging.getLogger(__name__)

OPENCODE_BIN = os.environ.get("OPENCODE_BIN", "opencode")
OPENCODE_TIMEOUT = int(os.environ.get("OPENCODE_TIMEOUT", "3600"))

# Фолбэк, если `opencode models` недоступен (CLI не установлен, нет сети).
DEFAULT_OPENCODE_MODELS = [
    "deepseek/deepseek-v4-flash",
    "deepseek/deepseek-v4-pro",
]

FILE_MARKER_SYSTEM = (
    "[SYSTEM NOTE FOR OPENCODE] Если нужно отправить пользователю файл "
    "(скриншот, собранный пакет, сгенерированный документ и т.п.) — выведи "
    "отдельной строкой маркер [[FILE: /абсолютный/путь]] (опционально с "
    "подписью через '|': [[FILE: /путь | подпись]]). Бот парсит это и отправит "
    "файл в Telegram. Используй только для файлов в пределах cwd сессии или "
    "явно указанных пользователем."
)

# Per-call модель. Выставляется через engines.engine_model_scope() из
# telegram_bot.py перед call_stream. Если None — фолбэк на OPENCODE_MODEL env.
CURRENT_MODEL: ContextVar[str | None] = ContextVar("opencode_model", default=None)


def _opencode_mcp_config(mcp_playwright: bool, mcp_topic_role: str | None) -> str | None:
    """Временный opencode-конфиг с per-topic MCP-серверами.

    Клонирует глобальный opencode.json (Manager MCP и прочие настройки
    сохраняются), добавляет нужные MCP в `mcp` и пишет во временный файл.
    Путь подставляется в OPENCODE_CONFIG для конкретного запуска.
    Вызывающий обязан удалить файл через cleanup_tempfile.

    ``None`` — добавлять нечего, запускаем opencode с его штатным конфигом.
    Проверять надо именно СПИСОК серверов, а не роль: роль есть у каждого
    топика всегда, поэтому ветка «роль задана → пишем temp-файл» подменяла
    глобальный конфиг клоном на каждом ходу даже там, где ни одного сервера не
    объявлено (найдено тестом 2026-07-25).
    """
    additions: dict[str, dict[str, Any]] = {}

    if mcp_topic_role:
        from engines.topic_mcp import opencode_mcp_servers

        additions.update(opencode_mcp_servers(mcp_topic_role))

    if mcp_playwright:
        from engines.playwright_mcp import playwright_command_args, playwright_server_name

        spec = playwright_command_args()
        if spec is None:
            logger.warning("mcp_playwright requested but Playwright globally disabled")
        else:
            npx, args = spec
            additions[playwright_server_name()] = {
                "type": "local",
                "command": [npx, *args],
                "enabled": True,
            }

    if not additions:
        return None

    from engines.playwright_mcp import OPENCODE_CONFIG as BASE_CONFIG

    base: Any = {}
    if BASE_CONFIG.exists():
        try:
            base = json.loads(BASE_CONFIG.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            logger.warning("cannot parse %s; starting opencode config from scratch", BASE_CONFIG)
            base = {}
    if not isinstance(base, dict):
        base = {}
    base.setdefault("$schema", "https://opencode.ai/config.json")
    mcp = base.setdefault("mcp", {})
    if not isinstance(mcp, dict):
        mcp = {}
        base["mcp"] = mcp

    mcp.update(additions)

    fd, path = tempfile.mkstemp(prefix="jarvis-opencode-", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(base, fh, ensure_ascii=False, indent=2)
    except Exception:
        cleanup_tempfile(path)
        raise
    return path


def _opencode_env() -> dict[str, str]:
    env = os.environ.copy()
    # The bot is a long-running service; autoupdate checks add noise and can
    # unexpectedly change CLI behavior between restarts.
    env.setdefault("OPENCODE_DISABLE_AUTOUPDATE", "true")
    env.setdefault("OPENCODE_CLIENT", "jarvis")
    return env


def _extract_session_id(ev: dict[str, Any]) -> str | None:
    for key in ("sessionID", "sessionId", "session_id"):
        val = ev.get(key)
        if isinstance(val, str) and val:
            return val
    for key in ("session", "part", "message", "info"):
        obj = ev.get(key)
        if not isinstance(obj, dict):
            continue
        for nested_key in ("id", "sessionID", "sessionId", "session_id"):
            val = obj.get(nested_key)
            if isinstance(val, str) and val.startswith("ses_"):
                return val
    props = ev.get("properties")
    if isinstance(props, dict):
        return _extract_session_id(props)
    return None


def _string_from_any(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        chunks: list[str] = []
        for item in value:
            if isinstance(item, str):
                chunks.append(item)
            elif isinstance(item, dict):
                txt = item.get("text") or item.get("content")
                if isinstance(txt, str):
                    chunks.append(txt)
        return "".join(chunks)
    return ""


def _text_from_part(part: Any) -> str:
    if not isinstance(part, dict):
        return ""
    for key in ("text", "delta", "content", "value", "message", "output"):
        txt = _string_from_any(part.get(key))
        if txt:
            return txt
    state = part.get("state")
    if isinstance(state, dict):
        for key in ("text", "content", "output"):
            txt = _string_from_any(state.get(key))
            if txt:
                return txt
    return ""


def _error_message(ev: dict[str, Any]) -> str:
    err = ev.get("error")
    if isinstance(err, str):
        return err.strip()
    if isinstance(err, dict):
        data = err.get("data")
        for obj in (err, data):
            if not isinstance(obj, dict):
                continue
            msg = obj.get("message") or obj.get("error")
            if isinstance(msg, str) and msg.strip():
                return msg.strip()
        return json.dumps(err, ensure_ascii=False)[:1500]
    msg = ev.get("message")
    if isinstance(msg, str):
        return msg.strip()
    return ""


def _tool_summary(part: dict[str, Any]) -> str:
    name = part.get("tool") or part.get("name") or "tool"
    state = part.get("state") if isinstance(part.get("state"), dict) else {}
    title = state.get("title") if isinstance(state, dict) else None
    if isinstance(title, str) and title.strip():
        return f"{name} {title.strip()[:140]}"
    inp = state.get("input") if isinstance(state, dict) else part.get("input")
    if isinstance(inp, dict):
        for key in ("command", "file_path", "path", "pattern", "query", "description"):
            val = inp.get(key)
            if val:
                return f"{name} {key}={str(val)[:140]}"
    return str(name)


def _discover_opencode_models() -> list[str]:
    """`opencode models` печатает по строке на модель в виде provider/model —
    берём то, что реально сконфигурировано у CLI (провайдеры, ключи, free-пул)."""
    override = split_models(os.environ.get("OPENCODE_MODELS"))
    if override:
        return override
    return [line for line in cli_models([OPENCODE_BIN, "models"]) if "/" in line]


def _opencode_prompt(prompt: str, system_prefix: str | None, resume_mode: bool) -> str:
    """У opencode нет канала system-prompt. FILE-маркер клеим каждый ход;
    общий [SYSTEM:]-блок — только на НОВОЙ сессии (на resume уже в
    транскрипте). См. codex-адаптер — логика идентична."""
    prefix_parts: list[str] = []
    if system_prefix and not resume_mode:
        prefix_parts.append(system_prefix)
    prefix_parts.append(FILE_MARKER_SYSTEM)
    return "\n\n".join(prefix_parts) + "\n\n" + prompt


def _opencode_command(
    session_id: str, cwd: str, *, resume_mode: bool, model: str | None,
) -> list[str]:
    cmd = [
        OPENCODE_BIN, "run",
        "--format", "json",
        "--dangerously-skip-permissions",
        "--dir", cwd,
    ]
    if model:
        cmd.extend(["--model", model])
    agent = os.environ.get("OPENCODE_AGENT")
    if agent:
        cmd.extend(["--agent", agent])
    variant = os.environ.get("OPENCODE_VARIANT")
    if variant:
        cmd.extend(["--variant", variant])
    if resume_mode:
        cmd.extend(["--session", session_id])
    return cmd


class _OpenCodeStream:
    """Состояние разбора JSONL одного `opencode run --format json`."""

    def __init__(
        self,
        model: str | None,
        on_intermediate: Callable[[str], Awaitable[None]],
    ) -> None:
        self.on_intermediate = on_intermediate
        self.journal = IntermediateBuffer()
        self.requested_model = model
        self.real_session_id: str | None = None
        # actual_model: fallback на то, что мы сами просили (--model);
        # stream-парсер ниже перезапишет, если CLI сообщит точное.
        self.actual_model = model
        self.text_chunks: list[str] = []
        self.part_texts: dict[str, str] = {}
        self.final_text = ""
        self.stream_errors: list[str] = []

    async def flush(self, force: bool = False) -> None:
        await self.journal.flush(self.on_intermediate, force)

    async def read(self, proc: asyncio.subprocess.Process) -> None:
        assert proc.stdout is not None
        async for ev in iter_json_events(proc.stdout, "opencode"):
            await self.handle_event(ev)

    def remember_text(self, part: Any, fallback: str = "") -> str:
        txt = _text_from_part(part) or fallback
        if not txt:
            return ""
        if isinstance(part, dict):
            pid = part.get("id")
            if isinstance(pid, str) and pid:
                self.part_texts[pid] = txt
        return txt

    def _note_model(self, ev: dict[str, Any]) -> None:
        # Best-effort парсинг модели: opencode кладёт её в part.modelID
        # или message.info.modelID. Берём первое попадание.
        for obj in (
            ev, ev.get("part"), ev.get("info"), ev.get("message"),
            ev.get("properties"),
        ):
            if isinstance(obj, dict):
                for key_name in ("modelID", "model_id", "model"):
                    m = obj.get(key_name)
                    if isinstance(m, str) and m:
                        self.actual_model = m
                        break
                if self.actual_model and self.actual_model != self.requested_model:
                    break

    async def _show(self, text: str) -> None:
        """Последний кусок текста — в final_text и хвостом в журнал."""
        self.final_text = text
        self.journal.append(text[-800:])
        await self.flush()

    async def handle_event(self, ev: Any) -> None:
        sid = _extract_session_id(ev)
        if sid:
            self.real_session_id = sid

        self._note_model(ev)

        etype = ev.get("type")
        props = ev.get("properties") if isinstance(ev.get("properties"), dict) else {}
        part = ev.get("part") if isinstance(ev.get("part"), dict) else props.get("part")

        if etype in ("error", "session.error", "turn.failed"):
            msg = _error_message(ev) or _error_message(props)
            if msg and msg not in self.stream_errors:
                self.stream_errors.append(msg)
            return

        if etype == "tool_use":
            if isinstance(part, dict):
                self.journal.append(f"🔧 {_tool_summary(part)}")
                await self.flush()
            return

        if etype == "message.part.delta":
            delta = _string_from_any(ev.get("delta") or props.get("delta")) or _text_from_part(part)
            if delta:
                if isinstance(part, dict) and isinstance(part.get("id"), str):
                    pid = part["id"]
                    self.part_texts[pid] = self.part_texts.get(pid, "") + delta
                else:
                    self.text_chunks.append(delta)
                await self._show(delta)
            return

        if etype == "message.part.updated":
            txt = self.remember_text(part)
            if txt:
                await self._show(txt)
            return

        if etype == "text":
            txt = self.remember_text(part, _string_from_any(ev.get("text")))
            if txt:
                if not isinstance(part, dict) or not part.get("id"):
                    self.text_chunks.append(txt)
                await self._show(txt)
            return

        if etype == "step_finish":
            if isinstance(part, dict) and part.get("reason") == "stop":
                await self.flush(force=True)
            return

        if etype == "message.updated":
            msg = ev.get("message") if isinstance(ev.get("message"), dict) else props.get("message")
            txt = _text_from_part(msg)
            if txt:
                self.final_text = txt

    def collected_text(self) -> str:
        """Итоговый текст: части по id, иначе склейка чанков, иначе последний текст."""
        if self.part_texts:
            return "\n".join(self.part_texts.values())
        if self.text_chunks:
            return "".join(self.text_chunks)
        return self.final_text


class OpenCodeEngine(BaseEngine):
    name = "opencode"
    bin_path = OPENCODE_BIN
    default_models = DEFAULT_OPENCODE_MODELS

    def _discover_models(self) -> list[str]:
        return _discover_opencode_models()

    def new_session_id(self) -> str:
        return f"{PLACEHOLDER_PREFIX}{uuid.uuid4()}"

    def session_exists(self, session_id: str, cwd: str) -> bool:
        """OpenCode stores sessions in SQLite and validates ids itself.

        Avoid probing the DB from the bot process: it can be locked by another
        opencode instance. For real ``ses_...`` ids we optimistically resume and
        let the CLI return a clear error if the session was removed.
        """
        return (not is_placeholder(session_id)) and session_id.startswith("ses_")

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
        is_spawn = spawn_id is not None

        resume_mode = (not is_spawn) and self.session_exists(session_id, effective_cwd)
        full_prompt = _opencode_prompt(prompt, system_prefix, resume_mode)

        # opencode run не умеет per-invocation MCP, поэтому клонируем глобальный
        # конфиг (Manager MCP сохраняется) + добавляем per-topic MCP во временный
        # файл и указываем на него через OPENCODE_CONFIG.
        pw_config_path = _opencode_mcp_config(mcp_playwright, mcp_topic_role)

        model = CURRENT_MODEL.get() or os.environ.get("OPENCODE_MODEL")
        cmd = _opencode_command(session_id, effective_cwd, resume_mode=resume_mode, model=model)

        logger.info(
            "opencode start: key=%s session=%s mode=%s cwd=%s prompt_len=%d spawn_id=%s",
            key, session_id, "resume" if resume_mode else "new",
            effective_cwd, len(prompt), spawn_id,
        )

        if effective_cwd and not os.path.isdir(effective_cwd):
            cleanup_tempfile(pw_config_path)
            return missing_cwd_result(effective_cwd, session_id)

        env = _opencode_env()
        if pw_config_path:
            env["OPENCODE_CONFIG"] = pw_config_path

        try:
            proc = await spawn(cmd, cwd=effective_cwd, stdin=asyncio.subprocess.PIPE, env=env)
        except FileNotFoundError:
            cleanup_tempfile(pw_config_path)
            return False, f"`{OPENCODE_BIN}` не найден в PATH.", session_id, None

        await feed_stdin(proc, full_prompt)
        register_proc(proc, key, spawn_id, active_procs, spawn_procs)

        stream = _OpenCodeStream(model, on_intermediate)
        try:
            finished = await wait_stream(proc, stream.read(proc), OPENCODE_TIMEOUT)
        finally:
            await stream.flush(force=True)
            cleanup_tempfile(pw_config_path)
            unregister_proc(proc, key, spawn_id, active_procs, spawn_procs)
        if not finished:
            return False, f"Timeout: opencode не ответил за {OPENCODE_TIMEOUT}с.", session_id, stream.actual_model

        stderr_text = await read_stderr(proc)
        return self._finish(
            proc, stream, stderr_text,
            cmd=cmd, cwd=effective_cwd, key=key, session_id=session_id, is_spawn=is_spawn,
        )

    def _finish(
        self,
        proc: asyncio.subprocess.Process,
        stream: _OpenCodeStream,
        stderr_text: str,
        *,
        cmd: list[str],
        cwd: str,
        key: tuple[int, int],
        session_id: str,
        is_spawn: bool,
    ) -> tuple[bool, str, str | None, str | None]:
        """Итог вызова по коду выхода, собранному тексту и ошибкам stream."""
        final_text = stream.collected_text()
        real_session_id = stream.real_session_id
        actual_model = stream.actual_model
        stream_errors = stream.stream_errors

        if proc.returncode != 0:
            logger.warning(
                "opencode rc=%s cmd=%s cwd=%s stderr=%s stream_errors=%s",
                proc.returncode, cmd, cwd,
                stderr_text[:500], stream_errors[:3],
            )
            if proc.returncode and proc.returncode < 0:
                return False, "", (real_session_id or session_id), actual_model
            if not final_text:
                err_body = "\n".join(stream_errors)[:1500] if stream_errors else stderr_text[:1500]
                return False, (
                    f"Ошибка opencode (rc={proc.returncode}): {err_body or '(пусто)'}"
                ), session_id, actual_model

        logger.info(
            "opencode done: key=%s rc=%s final_len=%d real_session_id=%s model=%s",
            key, proc.returncode, len(final_text), real_session_id, actual_model,
        )
        if not final_text.strip():
            return False, (
                "opencode вернул пустой ответ."
                + (f"\n{stderr_text[:500]}" if stderr_text else "")
            ), (real_session_id or session_id), actual_model

        effective_out_id = session_id
        if not is_spawn and real_session_id and real_session_id != session_id:
            effective_out_id = real_session_id

        return True, final_text, effective_out_id, actual_model
