"""Общие куски одноразовых адаптеров (claude / codex / opencode) и их живых
воркеров: placeholder-сессии, временные файлы, cwd по умолчанию, журнал
промежуточных шагов, чтение JSONL из stdout CLI и учёт запущенных процессов.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any

from engines.process_control import terminate_process_tree

logger = logging.getLogger(__name__)

# Не чаще раза в столько секунд шлём промежуточный журнал в Telegram.
INTERMEDIATE_MIN_INTERVAL = 2.0

# codex и opencode сами выдают id сессии при первом запуске. До тех пор в БД
# лежит наш placeholder, а реальный id ловится из stream и заменяет его.
PLACEHOLDER_PREFIX = "placeholder-"


def is_placeholder(session_id: str) -> bool:
    return session_id.startswith(PLACEHOLDER_PREFIX)


def resolve_cwd(cwd: str | None) -> str:
    """Рабочая папка вызова: аргумент, иначе CLAUDE_CWD, иначе домашняя."""
    return cwd or os.environ.get("CLAUDE_CWD", str(Path.home()))


def missing_cwd_result(cwd: str, session_id: str) -> tuple[bool, str, str | None, str | None]:
    return (
        False,
        f"⚠️ Рабочая папка `{cwd}` не существует. "
        "Создай её или переназначь через /bind.",
        session_id, None,
    )


def cleanup_tempfile(path: str | None) -> None:
    if not path:
        return
    try:
        os.unlink(path)
    except OSError:
        logger.debug("cannot remove temp file %s", path, exc_info=True)


def cleanup_codex_profiles(paths: list[Path]) -> None:
    from engines.topic_mcp import cleanup_codex_profile

    for path in paths:
        cleanup_codex_profile(path)


class IntermediateBuffer:
    """Журнал промежуточных шагов хода: строки копятся и уходят колбэку одним
    сообщением не чаще INTERMEDIATE_MIN_INTERVAL (force — без оглядки на него)."""

    def __init__(self, error_message: str = "on_intermediate failed", *error_args: Any) -> None:
        self.lines: list[str] = []
        self._last_push = 0.0
        self._error = (error_message, *error_args)

    def append(self, line: str) -> None:
        self.lines.append(line)

    async def flush(
        self,
        callback: Callable[[str], Awaitable[None]] | None,
        force: bool = False,
    ) -> None:
        if not self.lines:
            return
        now = time.monotonic()
        if not force and (now - self._last_push) < INTERMEDIATE_MIN_INTERVAL:
            return
        text = "\n".join(self.lines)
        self.lines.clear()
        self._last_push = now
        if callback is None:
            return
        try:
            await callback(text)
        except Exception:
            logger.exception(*self._error)


async def iter_json_events(stream: asyncio.StreamReader, label: str) -> AsyncIterator[Any]:
    """JSON-значения построчно из stdout CLI; пустые и не-JSON строки пропускаются."""
    while True:
        line = await stream.readline()
        if not line:
            break
        raw = line.decode("utf-8", errors="replace").strip()
        if not raw:
            continue
        try:
            ev = json.loads(raw)
        except json.JSONDecodeError:
            logger.debug("%s non-json stdout: %r", label, raw[:300])
            continue
        yield ev


def register_proc(
    proc: asyncio.subprocess.Process,
    key: tuple[int, int],
    spawn_id: str | None,
    active_procs: dict,
    spawn_procs: dict,
) -> None:
    """Запомнить процесс, чтобы /stop и отмена задачи могли его погасить."""
    if spawn_id is not None:
        spawn_procs[(key[0], key[1], spawn_id)] = proc
    else:
        active_procs[key] = proc


def unregister_proc(
    proc: asyncio.subprocess.Process,
    key: tuple[int, int],
    spawn_id: str | None,
    active_procs: dict,
    spawn_procs: dict,
) -> None:
    """Снять процесс с учёта, если на его месте ещё не стоит более новый."""
    if spawn_id is not None:
        skey = (key[0], key[1], spawn_id)
        if spawn_procs.get(skey) is proc:
            spawn_procs.pop(skey, None)
    else:
        if active_procs.get(key) is proc:
            active_procs.pop(key, None)


async def wait_stream(
    proc: asyncio.subprocess.Process,
    reader: Awaitable[None],
    timeout: float,
) -> bool:
    """Дочитать stdout и дождаться выхода CLI.

    False — таймаут: процесс уже погашен. Отмена задачи гасит процесс и
    пробрасывается дальше.
    """
    try:
        try:
            await asyncio.wait_for(reader, timeout=timeout)
        except TimeoutError:
            await terminate_process_tree(proc)
            return False
        await proc.wait()
    except asyncio.CancelledError:
        await terminate_process_tree(proc)
        raise
    return True


async def read_stderr(proc: asyncio.subprocess.Process) -> str:
    if proc.stderr is None:
        return ""
    try:
        stderr_b = await proc.stderr.read()
    except Exception:
        logger.debug("cannot read CLI stderr", exc_info=True)
        return ""
    return stderr_b.decode("utf-8", errors="replace")
