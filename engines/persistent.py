"""Живой процесс движка на топик (/persistent): запуск и таймаут хода по имени движка."""

from __future__ import annotations

from engines.claude_engine import CLAUDE_TIMEOUT
from engines.claude_engine import start_persistent as _start_claude
from engines.codex_engine import CODEX_TIMEOUT
from engines.cursor_engine import CURSOR_TIMEOUT
from engines.opencode_engine import OPENCODE_TIMEOUT
from engines.persistent_codex import start_persistent as _start_codex
from engines.persistent_cursor import start_persistent as _start_cursor
from engines.persistent_opencode import start_persistent as _start_opencode

_STARTERS = {
    "claude": _start_claude,
    "codex": _start_codex,
    "opencode": _start_opencode,
    "cursor": _start_cursor,
}

_TIMEOUTS = {
    "codex": CODEX_TIMEOUT,
    "opencode": OPENCODE_TIMEOUT,
    "cursor": CURSOR_TIMEOUT,
}


async def start_persistent(engine_name: str, **kwargs):
    """Поднять живой процесс движка. ``worker.session_id`` может отличаться от
    запрошенного (codex, opencode, cursor назначают id сами) — сохраняет вызывающий."""
    starter = _STARTERS.get(engine_name)
    if starter is None:
        raise RuntimeError(f"persistent is not supported for {engine_name}")
    return await starter(**kwargs)


def persistent_timeout(engine_name: str) -> int:
    return _TIMEOUTS.get(engine_name, CLAUDE_TIMEOUT)
