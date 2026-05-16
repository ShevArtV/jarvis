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
from contextlib import contextmanager
from typing import Protocol, Awaitable, Callable, Iterator

# Набор возможных движков. Добавляется при расширении.
ENGINE_CLAUDE = "claude"
ENGINE_CODEX = "codex"
ENGINE_OPENCODE = "opencode"
SUPPORTED_ENGINES = (ENGINE_CLAUDE, ENGINE_CODEX, ENGINE_OPENCODE)


class Engine(Protocol):
    """Интерфейс адаптера движка. Все методы строго asyncio-safe, где применимо."""

    name: str            # "claude" | "codex" | "opencode"
    bin_path: str        # путь/имя бинаря CLI (для shutil.which)
    models: list[str]    # доступные модели; пустой список = «модель не выбирается»

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
    ) -> tuple[bool, str, str | None, str | None]: ...
    # Возвращает (ok, final_text, session_id_after, actual_model).
    # actual_model — модель, которой CLI ответил последний раз (из stream
    # event'ов: 'system.init' у claude, 'turn.started' у codex, 'message'
    # у opencode). None если не удалось определить.


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


def ensure_engine_tools(engine: Engine) -> tuple[bool, str]:
    """Runtime tool setup for an engine when Jarvis activates or uses it.

    Registers all stdio MCP servers Jarvis exposes (Playwright + Manager).
    Returns combined (ok, status); ok=False if any one server fails so the
    caller can log it, but individual failures don't block the others.
    """
    from engines.jarvis_mcp import ensure_jarvis_mcp
    from engines.playwright_mcp import ensure_playwright_mcp

    pw_ok, pw_status = ensure_playwright_mcp(engine.name, engine.bin_path)
    mgr_ok, mgr_status = ensure_jarvis_mcp(engine.name, engine.bin_path)
    return pw_ok and mgr_ok, f"{pw_status}; {mgr_status}"


@contextmanager
def engine_model_scope(engine_name: str, model: str | None) -> Iterator[None]:
    """Выставляет текущую модель для движка через ContextVar на время блока.

    Поддерживаются claude, codex и opencode.
    Для движков без поддержки или при model=None — no-op.
    ContextVar.set/reset синхронны, но значение видно через `await` внутри
    той же таски — этого достаточно для call_stream.
    """
    if not model:
        yield
        return
    if engine_name == ENGINE_OPENCODE:
        from engines.opencode_engine import CURRENT_MODEL as OC_MODEL

        token = OC_MODEL.set(model)
        try:
            yield
        finally:
            OC_MODEL.reset(token)
    elif engine_name == ENGINE_CLAUDE:
        from engines.claude_engine import CURRENT_MODEL as CL_MODEL

        token = CL_MODEL.set(model)
        try:
            yield
        finally:
            CL_MODEL.reset(token)
    elif engine_name == ENGINE_CODEX:
        from engines.codex_engine import CURRENT_MODEL as CX_MODEL

        token = CX_MODEL.set(model)
        try:
            yield
        finally:
            CX_MODEL.reset(token)
    else:
        yield
