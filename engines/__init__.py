"""Engine adapters — абстракция поверх различных LLM CLI (claude / codex / opencode).

Модель одинаковая: «топик = постоянная сессия», у каждой сессии есть id (UUID).
Адаптер знает, как:
- вызвать CLI неинтерактивно с заданным cwd и system-prompt, получить stream событий;
- понять, существует ли сессия (стоит ли --resume, или создавать новую);
- подчистить stale lock'и своего CLI (для claude — pidfile, для codex — нет смысла);
- сгенерировать новый session_id (UUID).

Движок per-topic: `default_engine_name()` читает env JARVIS_ENGINE и используется
как дефолт для новых топиков. `get_engine_by_name(name)` возвращает singleton
адаптера; `get_engine()` — adapter дефолта.
"""

from __future__ import annotations

import os
from typing import Protocol, Awaitable, Callable

# Набор возможных движков. Добавляется при расширении.
ENGINE_CLAUDE = "claude"
ENGINE_CODEX = "codex"
ENGINE_OPENCODE = "opencode"
SUPPORTED_ENGINES = (ENGINE_CLAUDE, ENGINE_CODEX, ENGINE_OPENCODE)


class Engine(Protocol):
    """Интерфейс адаптера движка. Все методы строго asyncio-safe, где применимо."""

    name: str       # "claude" | "codex" | "opencode"
    bin_path: str   # путь/имя бинаря CLI (для shutil.which)

    def new_session_id(self) -> str: ...
    def session_exists(self, session_id: str, cwd: str) -> bool: ...
    def clear_stale_session_pidfile(self, session_id: str) -> None: ...

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
    ) -> tuple[bool, str, str | None]: ...


_CACHE: dict[str, Engine] = {}


def default_engine_name() -> str:
    """Имя дефолтного движка из env (для новых топиков). Дефолт — claude."""
    raw = (os.environ.get("JARVIS_ENGINE") or ENGINE_CLAUDE).strip().lower()
    if raw not in SUPPORTED_ENGINES:
        raise RuntimeError(
            f"JARVIS_ENGINE={raw!r} не поддерживается. "
            f"Допустимо: {list(SUPPORTED_ENGINES)}"
        )
    return raw


def get_engine_by_name(name: str) -> Engine:
    """Singleton-кэш адаптера по имени. Бросает RuntimeError для неизвестного имени."""
    name = name.strip().lower()
    if name not in SUPPORTED_ENGINES:
        raise RuntimeError(
            f"Engine {name!r} не поддерживается. Допустимо: {list(SUPPORTED_ENGINES)}"
        )
    cached = _CACHE.get(name)
    if cached is not None:
        return cached
    if name == ENGINE_CLAUDE:
        from engines.claude_engine import ClaudeEngine
        eng: Engine = ClaudeEngine()
    elif name == ENGINE_CODEX:
        from engines.codex_engine import CodexEngine
        eng = CodexEngine()
    else:
        from engines.opencode_engine import OpenCodeEngine
        eng = OpenCodeEngine()
    _CACHE[name] = eng
    return eng


def get_engine() -> Engine:
    """Адаптер дефолтного движка (для новых топиков)."""
    return get_engine_by_name(default_engine_name())
