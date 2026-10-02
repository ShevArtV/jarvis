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

import logging
import os
import shutil
from collections.abc import Iterator
from contextlib import contextmanager

# Интерфейс адаптера описан в engines/base.py; здесь — реэкспорт для
# `from engines import Engine`.
from engines.base import Engine as Engine

logger = logging.getLogger(__name__)

# Набор возможных движков. Добавляется при расширении.
ENGINE_CLAUDE = "claude"
ENGINE_CODEX = "codex"
ENGINE_OPENCODE = "opencode"
SUPPORTED_ENGINES = (ENGINE_CLAUDE, ENGINE_CODEX, ENGINE_OPENCODE)


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

    Manager MCP is registered globally (always-on — cheap, every topic may
    orchestrate). Playwright is NOT: it is on-demand and injected per
    invocation by the adapter, so here we only strip any stale always-on
    Playwright registration left by older Jarvis versions.

    Returns combined (ok, status); ok=False if any step fails so the caller can
    log it, but failures don't block the others.
    """
    from engines.jarvis_mcp import ensure_jarvis_mcp
    from engines.playwright_mcp import disable_global_playwright_mcp

    mgr_ok, mgr_status = ensure_jarvis_mcp(engine.name, engine.bin_path)
    pw_ok, pw_status = disable_global_playwright_mcp(engine.name, engine.bin_path)
    return mgr_ok and pw_ok, f"{mgr_status}; {pw_status}"


def prewarm_models() -> None:
    """Прогреть списки моделей всех установленных движков.

    Опрос CLI занимает секунды (`opencode models` — ~1.5с), а `engine.models`
    читают async-хендлеры Telegram. Зовётся из потока на старте бота, чтобы
    первый /engine не ждал холодный кэш. Дальше кэш освежается фоном по TTL.
    """
    for name in SUPPORTED_ENGINES:
        engine = get_engine_by_name(name)
        if shutil.which(engine.bin_path) is None:
            continue
        try:
            engine.prewarm_models()
        except Exception:
            logger.warning("prewarm models failed: engine=%s", name, exc_info=True)


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
