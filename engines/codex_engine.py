"""OpenAI Codex CLI adapter (@openai/codex).

Ключевые отличия от claude:
- session_id (в терминах codex — `thread_id`) генерирует сам codex при первом запуске,
  задать его заранее нельзя. Поэтому мы при первом запуске передаём `None` и
  ловим реальный id из события `{"type":"thread.started","thread_id":"..."}`.
  Адаптер возвращает его из call_stream; вызывающий код обязан обновить БД.
- Resume: `codex exec resume --json <thread_id> [PROMPT]`.
- Stream: `--json` даёт JSONL с типами thread.started / turn.started / item.started /
  item.completed / turn.completed. У item.type бывают: agent_message (с .text),
  command_execution (с .command), reasoning (с .text).
- --append-system-prompt у codex нет; инструкции уходят либо через AGENTS.md,
  либо префиксом в prompt. Мы префиксуем: это работает и для новой сессии, и для resume.
- Нет pidfile-локов, которые нужно чистить.
- Сессии хранятся в ~/.codex/sessions/<YYYY>/<MM>/<DD>/rollout-<ts>-<thread_id>.jsonl.
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
from engines.common import (
    PLACEHOLDER_PREFIX,
    IntermediateBuffer,
    cleanup_codex_profiles,
    is_placeholder,
    iter_json_events,
    missing_cwd_result,
    read_stderr,
    register_proc,
    resolve_cwd,
    unregister_proc,
    wait_stream,
)
from engines.model_cache import split_models
from engines.process_control import feed_stdin, spawn

logger = logging.getLogger(__name__)

CODEX_BIN = os.environ.get("CODEX_BIN", "codex")
CODEX_TIMEOUT = int(os.environ.get("CODEX_TIMEOUT", "3600"))
DEFAULT_CODEX_MODELS = [
    "gpt-5.5",
    "gpt-5.4",
    "gpt-5.4-mini",
    "gpt-5.3-codex",
    "gpt-5.2",
]

# Per-call модель. Выставляется через engines.engine_model_scope() из
# telegram_bot.py перед call_stream. Если None — фолбэк на CODEX_MODEL env,
# иначе CLI берёт свою дефолтную модель.
CURRENT_MODEL: ContextVar[str | None] = ContextVar("codex_model", default=None)

# Инструкция про маркер [[FILE: ...]]. Клеится префиксом к пользовательскому
# prompt'у на каждом вызове (чтобы не зависеть от AGENTS.md — тот может быть
# переопределён под проект).
FILE_MARKER_SYSTEM = (
    "[SYSTEM NOTE FOR CODEX] Если нужно отправить пользователю файл "
    "(скриншот, собранный пакет, сгенерированный документ и т.п.) — выведи "
    "отдельной строкой маркер [[FILE: /абсолютный/путь]] (опционально с "
    "подписью через '|': [[FILE: /путь | подпись]]). Бот парсит это и отправит "
    "файл в Telegram. Используй только для файлов в пределах cwd сессии или "
    "явно указанных пользователем."
)


def _mcp_config_overrides(
    mcp_playwright: bool,
    mcp_topic_role: str | None,
    *,
    inline_topic: bool = False,
) -> tuple[list[str], list[Path]]:
    """Per-invocation MCP flags and temporary files.

    Значения сериализуем через json.dumps — codex парсит value как TOML/JSON,
    так что строки получают кавычки, args — валидный массив, headers — объект.
    ПРИМЕЧАНИЕ: парсинг -c с массивом стоит проверить на конкретной версии
    codex (см. README — фолбэк через ручную регистрацию в config.toml).

    Topic-MCP по умолчанию уходит временным профилем (``--profile``, файл
    вернётся в cleanup-списке). ``inline_topic`` — живому app-server: он
    профилей не принимает, ему те же серверы передаются ``-c``-флагами.
    """
    flags: list[str] = []
    cleanup_paths: list[Path] = []

    if mcp_playwright:
        from engines.playwright_mcp import playwright_command_args, playwright_server_name

        spec = playwright_command_args()
        if spec is None:
            logger.warning("mcp_playwright requested but Playwright globally disabled")
        else:
            npx, args = spec
            table = f"mcp_servers.{playwright_server_name()}"
            flags.extend([
                "-c", f"{table}.command={json.dumps(npx, ensure_ascii=False)}",
                "-c", f"{table}.args={json.dumps(args, ensure_ascii=False)}",
                "-c", f"{table}.enabled=true",
            ])

    if mcp_topic_role and inline_topic:
        from engines.topic_mcp import codex_inline_config_flags

        flags.extend(codex_inline_config_flags(mcp_topic_role))
    elif mcp_topic_role:
        from engines.topic_mcp import create_codex_profile

        profile = create_codex_profile(mcp_topic_role)
        if profile is not None:
            profile_name, profile_path = profile
            flags.extend(["--profile", profile_name])
            cleanup_paths.append(profile_path)

    return flags, cleanup_paths


def _split_codex_global_flags(flags: list[str]) -> tuple[list[str], list[str]]:
    """Move Codex global-only flags before the subcommand."""
    global_flags: list[str] = []
    command_flags: list[str] = []
    i = 0
    while i < len(flags):
        flag = flags[i]
        value = flags[i + 1] if i + 1 < len(flags) else None
        if flag == "--profile" and value is not None:
            global_flags.extend([flag, value])
            i += 2
            continue
        command_flags.append(flag)
        i += 1
    return global_flags, command_flags


def _redact_cmd(cmd: list[str]) -> list[str]:
    return [re.sub(r"Bearer [^\"'}\s]+", "Bearer ***", part) for part in cmd]


def _codex_sessions_root() -> Path:
    return Path.home() / ".codex" / "sessions"


def _models_from_codex_cache() -> list[str]:
    cache_path = Path(
        os.environ.get("CODEX_MODELS_CACHE", Path.home() / ".codex" / "models_cache.json")
    )
    if not cache_path.is_file():
        return []
    try:
        data = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logger.warning("cannot read codex models cache: %s", cache_path, exc_info=True)
        return []
    if not isinstance(data, dict):
        return []

    models: list[str] = []
    for item in data.get("models", []):
        if not isinstance(item, dict) or item.get("visibility") != "list":
            continue
        slug = item.get("slug")
        if isinstance(slug, str) and slug and slug not in models:
            models.append(slug)
    return models


def _discover_codex_models() -> list[str]:
    return (
        split_models(os.environ.get("CODEX_MODELS"))
        or _models_from_codex_cache()
    )


def _codex_prompt(prompt: str, system_prefix: str | None, resume_mode: bool) -> str:
    """У codex нет канала system-prompt. FILE-маркер клеим каждый ход
    (он нужен и в середине сессии). Общий [SYSTEM:]-блок — только на
    НОВОЙ сессии: на resume он уже в транскрипте, повтор = лишние токены."""
    prefix_parts: list[str] = []
    if system_prefix and not resume_mode:
        prefix_parts.append(system_prefix)
    prefix_parts.append(FILE_MARKER_SYSTEM)
    return "\n\n".join(prefix_parts) + "\n\n" + prompt


def _codex_command(
    session_id: str,
    cwd: str,
    *,
    resume_mode: bool,
    is_spawn: bool,
    model: str | None,
    mcp_playwright: bool,
    mcp_topic_role: str | None,
) -> tuple[list[str], list[Path]]:
    """argv для `codex exec` / `codex exec resume` и временные файлы MCP-профиля,
    которые вызывающий обязан удалить (cleanup_codex_profiles)."""
    # ВАЖНО: у `codex exec` и `codex exec resume` разный набор флагов.
    #   exec принимает: --sandbox, --dangerously-bypass-approvals-and-sandbox,
    #                   --skip-git-repo-check, -C/--cd, --ephemeral, --json
    #   resume принимает: --dangerously-bypass-approvals-and-sandbox,
    #                   --skip-git-repo-check, --ephemeral, --json
    #                   (НЕТ --sandbox, НЕТ -C/--cd)
    # Для resume --dangerously-bypass-approvals-and-sandbox уже включает
    # "без сэндбокса" + approval=never, так что --sandbox не нужен.
    # cwd передаём через `cwd=` в subprocess для обоих путей.
    shared_flags = [
        "--json",
        "--skip-git-repo-check",
        "--dangerously-bypass-approvals-and-sandbox",
    ]
    # Per-topic MCP overrides поверх config.toml. Manager MCP остаётся
    # глобальным, topic-MCP и Playwright выбираются на конкретный запуск.
    mcp_flags, mcp_cleanup_paths = _mcp_config_overrides(mcp_playwright, mcp_topic_role)
    global_flags, command_mcp_flags = _split_codex_global_flags(mcp_flags)
    shared_flags.extend(command_mcp_flags)
    if is_spawn:
        # Одноразовая параллельная сессия: не хотим, чтобы codex её сохранял
        # и потом мешался в списке recent-sessions.
        shared_flags.append("--ephemeral")

    model_flags = ["--model", model] if model else []

    if resume_mode:
        # У resume нет --sandbox и -C/--cd. Sandbox-режим уже покрыт
        # --dangerously-bypass-approvals-and-sandbox, а cwd — через subprocess cwd=.
        cmd = [
            CODEX_BIN, *global_flags, "exec", "resume",
            *shared_flags,
            *model_flags,
            session_id, "-",
        ]
    else:
        # Новая сессия: можем (и хотим) явно задать sandbox и cwd.
        cmd = [
            CODEX_BIN, *global_flags, "exec",
            *shared_flags,
            *model_flags,
            "--sandbox", "danger-full-access",
            "-C", cwd,
            "-",
        ]
    return cmd, mcp_cleanup_paths


class _CodexStream:
    """Состояние разбора JSONL одного `codex exec --json`."""

    def __init__(
        self,
        model: str | None,
        on_intermediate: Callable[[str], Awaitable[None]],
    ) -> None:
        self.on_intermediate = on_intermediate
        self.journal = IntermediateBuffer()
        self.final_text = ""
        self.real_thread_id: str | None = None
        # actual_model: то, что мы сами попросили (через --model) — пока
        # CLI не сообщит точное. Stream-парсер ниже может перезаписать.
        self.actual_model = model
        # Ошибки из stream (type=error и type=turn.failed). Codex пишет их
        # в stdout-JSON, НЕ в stderr, и затем exit=1. Если их не ловить —
        # бот отдаёт пользователю «Ошибка codex (rc=1): (пусто)».
        self.stream_errors: list[str] = []

    async def flush(self, force: bool = False) -> None:
        await self.journal.flush(self.on_intermediate, force)

    async def read(self, proc: asyncio.subprocess.Process) -> None:
        assert proc.stdout is not None
        async for ev in iter_json_events(proc.stdout, "codex"):
            await self.handle_event(ev)

    async def handle_event(self, ev: Any) -> None:
        # Best-effort парсинг модели из любого top-level event'а
        # с полем `model` или nested в `item`/`turn`/`thread`.
        for obj in (ev, ev.get("item"), ev.get("turn"), ev.get("thread")):
            if isinstance(obj, dict):
                m = obj.get("model")
                if isinstance(m, str) and m:
                    self.actual_model = m
                    break
        etype = ev.get("type")
        if etype == "thread.started":
            tid = ev.get("thread_id")
            if isinstance(tid, str) and tid:
                self.real_thread_id = tid
            return
        if etype == "error":
            # Верхнеуровневая ошибка codex — например, usage limit,
            # сетевой сбой до старта turn. Сохраняем, чтобы отдать
            # пользователю после rc != 0.
            msg = (ev.get("message") or "").strip()
            if msg:
                self.stream_errors.append(msg)
            return
        if etype == "turn.failed":
            err = ev.get("error") or {}
            msg = (err.get("message") or "").strip()
            if msg and msg not in self.stream_errors:
                self.stream_errors.append(msg)
            return
        if etype in ("item.started", "item.completed"):
            await self._handle_item(etype, ev.get("item") or {})
        # turn.started / turn.completed — игнорируем, нам достаточно item.*.

    async def _handle_item(self, etype: str, item: dict) -> None:
        itype = item.get("type")
        if itype == "agent_message":
            # Финальный текст ответа — обновляем final_text (перезапись:
            # последнее agent_message — финальный) и показываем как
            # промежуточный для пользователя.
            txt = (item.get("text") or "").strip()
            if txt and etype == "item.completed":
                self.final_text = txt
                # Промежуточно тоже показываем — но коротко.
                self.journal.append(txt[:800])
                await self.flush()
        elif itype == "command_execution":
            # Аналог tool_use в claude.
            if etype != "item.started":
                return
            cmd_str = item.get("command") or ""
            if cmd_str:
                s = cmd_str[:150]
                self.journal.append(f"🔧 exec {s}")
                await self.flush()
        elif itype == "reasoning":
            if etype != "item.completed":
                return
            txt = (item.get("text") or "").strip()
            if txt:
                # В отличие от claude, codex отдаёт рассуждения текстом.
                # Раньше брали одну первую строку — остальное терялось
                # в затираемом индикаторе. Теперь есть журнал хода, так
                # что показываем рассуждение целиком (в разумных рамках).
                self.journal.append(f"💭 {txt[:800]}")
                await self.flush()


class CodexEngine(BaseEngine):
    name = "codex"
    bin_path = CODEX_BIN
    default_models = DEFAULT_CODEX_MODELS

    def _discover_models(self) -> list[str]:
        return _discover_codex_models()

    # --- Session helpers ---

    def new_session_id(self) -> str:
        """Генерируем placeholder — пометку, что настоящий id нам отдаст codex
        в первом же stream-событии. До тех пор в БД лежит именно placeholder."""
        return f"{PLACEHOLDER_PREFIX}{uuid.uuid4()}"

    def session_exists(self, session_id: str, cwd: str) -> bool:
        """Ищем файл сессии в ~/.codex/sessions/**/rollout-*-<session_id>.jsonl.

        Для placeholder'ов возвращаем False (реальной сессии ещё нет).
        """
        if is_placeholder(session_id):
            return False
        root = _codex_sessions_root()
        if not root.is_dir():
            return False
        try:
            # Быстрее, чем rglob, так как структура строго YYYY/MM/DD/.
            for match in root.rglob(f"rollout-*-{session_id}.jsonl"):
                return match.is_file()
        except OSError:
            logger.debug("codex sessions scan failed: %s", root, exc_info=True)
            return False
        return False

    # Codex не ведёт pidfile-локов (или мы их не знаем): clear_stale_session_pidfile
    # из BaseEngine — no-op, при next call не будет 'already in use'.

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
        effective_cwd = resolve_cwd(cwd)

        is_spawn = spawn_id is not None

        # Решаем: новая сессия или resume.
        resume_mode = (
            not is_spawn
            and not is_placeholder(session_id)
            and self.session_exists(session_id, effective_cwd)
        )
        full_prompt = _codex_prompt(prompt, system_prefix, resume_mode)
        model = CURRENT_MODEL.get() or os.environ.get("CODEX_MODEL")
        cmd, mcp_cleanup_paths = _codex_command(
            session_id, effective_cwd,
            resume_mode=resume_mode, is_spawn=is_spawn, model=model,
            mcp_playwright=mcp_playwright, mcp_topic_role=mcp_topic_role,
        )

        logger.info(
            "codex start: key=%s session=%s mode=%s cwd=%s prompt_len=%d spawn_id=%s",
            key, session_id, "resume" if resume_mode else "new",
            effective_cwd, len(prompt), spawn_id,
        )

        if effective_cwd and not os.path.isdir(effective_cwd):
            cleanup_codex_profiles(mcp_cleanup_paths)
            return missing_cwd_result(effective_cwd, session_id)

        try:
            proc = await spawn(cmd, cwd=effective_cwd, stdin=asyncio.subprocess.PIPE)
        except FileNotFoundError:
            cleanup_codex_profiles(mcp_cleanup_paths)
            return False, f"`{CODEX_BIN}` не найден в PATH.", session_id, None
        except Exception:
            cleanup_codex_profiles(mcp_cleanup_paths)
            raise

        await feed_stdin(proc, full_prompt)
        register_proc(proc, key, spawn_id, active_procs, spawn_procs)

        stream = _CodexStream(model, on_intermediate)
        try:
            finished = await wait_stream(proc, stream.read(proc), CODEX_TIMEOUT)
        finally:
            cleanup_codex_profiles(mcp_cleanup_paths)
            await stream.flush(force=True)
            unregister_proc(proc, key, spawn_id, active_procs, spawn_procs)
        if not finished:
            return False, f"Timeout: codex не ответил за {CODEX_TIMEOUT}с.", session_id, stream.actual_model

        stderr_text = await read_stderr(proc)
        return self._finish(
            proc, stream, stderr_text,
            cmd=cmd, cwd=effective_cwd, key=key, session_id=session_id, is_spawn=is_spawn,
        )

    def _finish(
        self,
        proc: asyncio.subprocess.Process,
        stream: _CodexStream,
        stderr_text: str,
        *,
        cmd: list[str],
        cwd: str,
        key: tuple[int, int],
        session_id: str,
        is_spawn: bool,
    ) -> tuple[bool, str, str | None, str | None]:
        """Итог вызова по коду выхода, собранному тексту и ошибкам stream."""
        final_text = stream.final_text
        real_thread_id = stream.real_thread_id
        actual_model = stream.actual_model
        stream_errors = stream.stream_errors

        if proc.returncode != 0:
            # Подробный лог, чтобы в следующий раз не гадать, что именно
            # запускали: сама команда, cwd и собранные stream-ошибки.
            # Команду логируем только после санитизации: это страховка на случай
            # будущих secret-bearing флагов.
            logger.warning(
                "codex rc=%s cmd=%s cwd=%s stderr=%s stream_errors=%s",
                proc.returncode, _redact_cmd(cmd), cwd,
                stderr_text[:500], stream_errors[:3],
            )
            if proc.returncode and proc.returncode < 0:
                return False, "", (real_thread_id or session_id), actual_model
            if not final_text:
                # Приоритет: сообщения из stdout-stream (codex пишет сюда
                # usage-limit и пр.), затем stderr, затем «(пусто)».
                err_body = ""
                if stream_errors:
                    err_body = "\n".join(stream_errors)[:1500]
                elif stderr_text:
                    err_body = stderr_text[:1500]
                else:
                    err_body = "(пусто)"
                return False, (
                    f"Ошибка codex (rc={proc.returncode}): {err_body}"
                ), session_id, actual_model

        logger.info(
            "codex done: key=%s rc=%s final_len=%d real_thread_id=%s model=%s",
            key, proc.returncode, len(final_text), real_thread_id, actual_model,
        )
        if not final_text.strip():
            return False, (
                "codex вернул пустой ответ."
                + (f"\n{stderr_text[:500]}" if stderr_text else "")
            ), (real_thread_id or session_id), actual_model

        # Если это была новая постоянная сессия — отдаём новый id наверх.
        # Для spawn'а (ephemeral) — не возвращаем; даже если codex его выдал,
        # мы не хотим его резюмировать (он --ephemeral всё равно не сохранён).
        effective_out_id = session_id
        if not is_spawn and real_thread_id and real_thread_id != session_id:
            effective_out_id = real_thread_id

        return True, final_text, effective_out_id, actual_model
