"""Живой процесс движка на топик (/persistent): запуск и таймаут хода по имени движка."""

from __future__ import annotations

import asyncio
import logging

from engines.claude_cli import CLAUDE_TIMEOUT
from engines.codex_engine import CODEX_TIMEOUT
from engines.cursor_engine import CURSOR_TIMEOUT
from engines.kimi_engine import KIMI_TIMEOUT
from engines.opencode_engine import OPENCODE_TIMEOUT
from engines.persistent_claude import start_persistent as _start_claude
from engines.persistent_codex import start_persistent as _start_codex
from engines.persistent_cursor import start_persistent as _start_cursor
from engines.persistent_kimi import start_persistent as _start_kimi
from engines.persistent_opencode import start_persistent as _start_opencode

logger = logging.getLogger(__name__)

# Патчится тестами.
_RETRY_DELAY = 5.0

_STARTERS = {
    "claude": _start_claude,
    "codex": _start_codex,
    "opencode": _start_opencode,
    "cursor": _start_cursor,
    "kimi": _start_kimi,
}

_TIMEOUTS = {
    "codex": CODEX_TIMEOUT,
    "opencode": OPENCODE_TIMEOUT,
    "cursor": CURSOR_TIMEOUT,
    "kimi": KIMI_TIMEOUT,
}


async def start_persistent(engine_name: str, **kwargs):
    """Поднять живой процесс движка. ``worker.session_id`` может отличаться от
    запрошенного (codex, opencode, cursor, kimi назначают id сами) — сохраняет вызывающий."""
    starter = _STARTERS.get(engine_name)
    if starter is None:
        raise RuntimeError(f"persistent is not supported for {engine_name}")
    try:
        return await starter(**kwargs)
    except Exception as exc:  # noqa: BLE001 — любой сбой подъёма повторяем один раз
        # Сбои подъёма CLI (таймаут initialize/session, Internal error сервера)
        # обычно разовые — одна повторная попытка снимает большую их часть.
        logger.warning("persistent %s start failed, retrying: %s", engine_name, describe_error(exc))
    await asyncio.sleep(_RETRY_DELAY)
    return await starter(**kwargs)


def describe_error(exc: BaseException) -> str:
    """Текст ошибки для пользователя: у TimeoutError и подобных ``str(exc)`` пуст."""
    text = str(exc).strip()
    name = type(exc).__name__
    if isinstance(exc, TimeoutError) and not text:
        return "CLI не ответил вовремя (TimeoutError)"
    return f"{text} ({name})" if text else name


def persistent_timeout(engine_name: str) -> int:
    return _TIMEOUTS.get(engine_name, CLAUDE_TIMEOUT)
