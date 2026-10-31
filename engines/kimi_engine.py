"""Kimi Code CLI (``kimi``) adapter.

- print: ``kimi -p <prompt> --output-format stream-json``; prompt в argv
  (stdin-режима у CLI 2.4.x нет);
- id сессии назначает CLI (``session_<uuid>``); до первого ответа в БД —
  placeholder, как у codex/opencode. ``--session`` на несуществующий id
  падает — поэтому на новой сессии флаг не передаём, id ловим из
  ``kimi session list --json`` (и meta-событий stream, если есть);
- системного канала нет: [SYSTEM:]-блок — префикс prompt на новой сессии;
- MCP: ``~/.kimi-code/mcp.json`` (Manager MCP глобально). Per-invocation
  Playwright в print нет — только в ``kimi acp`` (см. persistent_kimi).

Живой процесс (/persistent) — engines/persistent_kimi.py поверх ``kimi acp``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import subprocess
import tomllib
import uuid
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from pathlib import Path
from typing import Any

from engines.base import BaseEngine
from engines.common import (
    PLACEHOLDER_PREFIX,
    is_placeholder,
    missing_cwd_result,
    read_stderr,
    register_proc,
    resolve_cwd,
    unregister_proc,
    wait_stream,
)
from engines.kimi_events import KimiStream
from engines.model_cache import remember_labels, split_models
from engines.process_control import run_cli, spawn

logger = logging.getLogger(__name__)

KIMI_BIN = os.environ.get("KIMI_BIN", "kimi")
KIMI_TIMEOUT = int(os.environ.get("KIMI_TIMEOUT", "3600"))

DEFAULT_KIMI_MODELS = ["kimi-code/k3"]

FILE_MARKER_SYSTEM = (
    "[SYSTEM NOTE FOR CURSOR] Если нужно отправить пользователю файл "
    "(скриншот, собранный пакет, сгенерированный документ и т.п.) — выведи "
    "отдельной строкой маркер [[FILE: /абсолютный/путь]] (опционально с "
    "подписью через '|': [[FILE: /путь | подпись]]). Бот парсит это и отправит "
    "файл в Telegram. Используй только для файлов в пределах cwd сессии или "
    "явно указанных пользователем."
)

CURRENT_MODEL: ContextVar[str | None] = ContextVar("kimi_model", default=None)

_RESUME_HINT = re.compile(
    r"(?:To resume this session:\s*)?kimi\s+(?:-r|--session|-S)\s+(\S+)",
    re.IGNORECASE,
)


def _home() -> Path:
    from engines.accounts import current_dir

    account = current_dir("kimi")
    if account is not None:
        return account
    explicit = os.environ.get("KIMI_CODE_HOME", "").strip()
    if explicit:
        return Path(explicit).expanduser()
    return Path.home() / ".kimi-code"


def _work_dir_key(cwd: str) -> str | None:
    """Ключ каталога сессий для cwd из workspaces.json, если есть."""
    path = _home() / "workspaces.json"
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    workspaces = data.get("workspaces") if isinstance(data, dict) else None
    if not isinstance(workspaces, dict):
        return None
    cwd_resolved = str(Path(cwd).resolve()) if cwd else cwd
    for key, meta in workspaces.items():
        if not isinstance(meta, dict):
            continue
        root = meta.get("root")
        if root == cwd or root == cwd_resolved:
            return key
    return None


def _session_dir(session_id: str, cwd: str) -> Path | None:
    """Путь к каталогу сессии, если он есть на диске."""
    if not session_id or is_placeholder(session_id):
        return None
    root = _home() / "sessions"
    if not root.is_dir():
        return None
    wd = _work_dir_key(cwd)
    if wd:
        candidate = root / wd / session_id
        if (candidate / "state.json").is_file():
            return candidate
    # Фоллбек: любой workDir с этой сессией.
    try:
        for match in root.glob(f"*/{session_id}/state.json"):
            return match.parent
    except OSError:
        logger.debug("kimi sessions scan failed", exc_info=True)
    return None


def _discover_kimi_models() -> list[str]:
    override = split_models(os.environ.get("KIMI_MODELS"))
    if override:
        return override
    cfg_path = _home() / "config.toml"
    if not cfg_path.is_file():
        return []
    try:
        cfg = tomllib.loads(cfg_path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError):
        logger.warning("kimi model discovery failed reading %s", cfg_path, exc_info=True)
        return []
    models_tbl = cfg.get("models")
    if not isinstance(models_tbl, dict):
        return []
    models: list[str] = []
    labels: dict[str, str] = {}
    default = cfg.get("default_model")
    if isinstance(default, str) and default in models_tbl:
        models.append(default)
    for name, meta in models_tbl.items():
        if not isinstance(name, str) or name in models:
            continue
        models.append(name)
        if isinstance(meta, dict):
            label = meta.get("display_name")
            if isinstance(label, str) and label.strip():
                labels[name] = label.strip()
    if labels:
        remember_labels(labels)
    return models


def _kimi_prompt(prompt: str, system_prefix: str | None, resume_mode: bool) -> str:
    prefix_parts: list[str] = []
    if system_prefix and not resume_mode:
        prefix_parts.append(system_prefix)
    prefix_parts.append(FILE_MARKER_SYSTEM)
    return "\n\n".join(prefix_parts) + "\n\n" + prompt


def _kimi_command(
    session_id: str | None,
    model: str | None,
    prompt: str,
    *,
    resume_mode: bool,
) -> list[str]:
    cmd = [KIMI_BIN]
    if resume_mode and session_id and not is_placeholder(session_id):
        cmd.extend(["--session", session_id])
    if model:
        cmd.extend(["-m", model])
    cmd.extend(["-p", prompt, "--output-format", "stream-json"])
    return cmd


def _list_sessions(cwd: str) -> list[dict[str, Any]]:
    try:
        proc = run_cli(
            [KIMI_BIN, "session", "list", "--cwd", cwd, "--json", "--limit", "50"],
            capture_output=True, text=True, timeout=30, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        logger.debug("kimi session list failed", exc_info=True)
        return []
    if proc.returncode != 0 or not (proc.stdout or "").strip():
        return []
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return []
    return data if isinstance(data, list) else []


def _capture_new_session_id(cwd: str, before_ids: set[str]) -> str | None:
    sessions = _list_sessions(cwd)
    new = [
        s for s in sessions
        if isinstance(s, dict) and isinstance(s.get("id"), str) and s["id"] not in before_ids
    ]
    if not new:
        return None
    new.sort(key=lambda s: s.get("updatedAt") or 0, reverse=True)
    sid = new[0].get("id")
    return sid if isinstance(sid, str) else None


class KimiEngine(BaseEngine):
    name = "kimi"
    bin_path = KIMI_BIN
    default_models = DEFAULT_KIMI_MODELS

    def _discover_models(self) -> list[str]:
        return _discover_kimi_models()

    def new_session_id(self) -> str:
        return f"{PLACEHOLDER_PREFIX}{uuid.uuid4()}"

    def session_exists(self, session_id: str, cwd: str) -> bool:
        return _session_dir(session_id, cwd) is not None

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
        resume_mode = (
            spawn_id is None
            and not is_placeholder(session_id)
            and self.session_exists(session_id, effective_cwd)
        )
        full_prompt = _kimi_prompt(prompt, system_prefix, resume_mode)
        model = CURRENT_MODEL.get() or os.environ.get("KIMI_MODEL")

        if mcp_playwright:
            logger.warning(
                "kimi print: Playwright per-invocation не поддержан "
                "(нужен /persistent + ACP), key=%s", key,
            )
        if mcp_topic_role:
            logger.debug("kimi print: topic-MCP только в ACP, role=%s", mcp_topic_role)

        before_ids = {
            s.get("id") for s in _list_sessions(effective_cwd)
            if isinstance(s, dict) and isinstance(s.get("id"), str)
        } if not resume_mode else set()

        cmd = _kimi_command(
            session_id, model, full_prompt, resume_mode=resume_mode,
        )
        logger.info(
            "kimi start: key=%s session=%s mode=%s cwd=%s prompt_len=%d spawn_id=%s",
            key, session_id, "resume" if resume_mode else "new",
            effective_cwd, len(prompt), spawn_id,
        )

        if effective_cwd and not os.path.isdir(effective_cwd):
            return missing_cwd_result(effective_cwd, session_id)

        try:
            proc = await spawn(cmd, cwd=effective_cwd)
        except FileNotFoundError:
            return False, f"`{KIMI_BIN}` не найден в PATH.", session_id, None

        register_proc(proc, key, spawn_id, active_procs, spawn_procs)
        stream = KimiStream(effective_cwd, model, on_intermediate)
        try:
            finished = await wait_stream(proc, stream.read(proc), KIMI_TIMEOUT)
        finally:
            await stream.flush(force=True)
            unregister_proc(proc, key, spawn_id, active_procs, spawn_procs)
        if not finished:
            return (
                False,
                f"Timeout: kimi не ответил за {KIMI_TIMEOUT}с.",
                session_id, stream.actual_model,
            )

        stderr_text = await read_stderr(proc)
        sid_after = session_id
        if not resume_mode:
            captured = stream.session_id or _capture_new_session_id(effective_cwd, before_ids)
            if not captured:
                match = _RESUME_HINT.search(stderr_text or "")
                if match:
                    captured = match.group(1)
            if captured:
                sid_after = captured
        return self._finish(
            proc, stream, stderr_text, key=key, session_id=sid_after,
        )

    def _finish(
        self,
        proc: asyncio.subprocess.Process,
        stream: KimiStream,
        stderr_text: str,
        *,
        key: tuple[int, int],
        session_id: str,
    ) -> tuple[bool, str, str | None, str | None]:
        final_text = stream.answer()
        actual_model = stream.actual_model
        if proc.returncode != 0 or stream.error:
            logger.warning(
                "kimi rc=%s error=%s stderr=%s",
                proc.returncode, stream.error[:300], stderr_text[:500],
            )
            if proc.returncode and proc.returncode < 0:
                return False, "", session_id, actual_model
            err = stream.error or final_text or stderr_text.strip() or "(пусто)"
            return (
                False,
                f"Ошибка kimi (rc={proc.returncode}): {err[:500]}",
                session_id, actual_model,
            )
        logger.info(
            "kimi done: key=%s rc=%s final_len=%d model=%s session=%s",
            key, proc.returncode, len(final_text), actual_model, session_id,
        )
        if not final_text.strip():
            extra = f"\n{stderr_text.strip()}" if stderr_text.strip() else ""
            return False, f"kimi вернул пустой ответ.{extra}", session_id, actual_model
        return True, final_text, session_id, actual_model
