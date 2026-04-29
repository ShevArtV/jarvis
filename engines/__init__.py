"""Engine adapters — абстракция поверх различных LLM CLI (claude / codex / opencode).

Модель одинаковая: «топик = постоянная сессия», у каждой сессии есть id (UUID).
Адаптер знает, как:
- вызвать CLI неинтерактивно с заданным cwd и system-prompt, получить stream событий;
- понять, существует ли сессия (стоит ли --resume, или создавать новую);
- подчистить stale lock'и своего CLI (для claude — pidfile, для codex — нет смысла);
- сгенерировать новый session_id (UUID).

`get_engine()` — фабрика: читает env JARVIS_ENGINE (claude|codex|opencode),
возвращает singleton.
"""

from __future__ import annotations

import os
from typing import Protocol, Awaitable, Callable

# Набор возможных движков. Добавляется при расширении.
ENGINE_CLAUDE = "claude"
ENGINE_CODEX = "codex"
ENGINE_OPENCODE = "opencode"
SUPPORTED_ENGINES = {ENGINE_CLAUDE, ENGINE_CODEX, ENGINE_OPENCODE}


class Engine(Protocol):
    """Интерфейс адаптера движка. Все методы строго asyncio-safe, где применимо."""

    name: str  # "claude" | "codex" | "opencode"

    def new_session_id(self) -> str:
        """Вернуть новый id для сессии.

        claude — случайный UUID; codex/opencode — placeholder, настоящий id
        вернётся из stream при первом запуске.
        """
        ...

    def session_exists(self, session_id: str, cwd: str) -> bool:
        """True — если на диске есть follow-up файл этой сессии, можно резюмировать."""
        ...

    def clear_stale_session_pidfile(self, session_id: str) -> None:
        """Убрать stale-lock своего CLI (если актуально). Для codex — no-op."""
        ...

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
    ) -> tuple[bool, str, str | None]:
        """Запустить CLI, стримить события, собрать финальный текст.

        Возвращает (ok, final_text, session_id_after).
        `session_id_after`: для codex/opencode это реальный id, полученный из
        stream (если сессия только что создана — отличается от переданного
        placeholder); для claude равен входному `session_id`.
        """
        ...


def get_engine() -> Engine:
    """Возвращает адаптер согласно JARVIS_ENGINE. Дефолт — claude.

    Кэшируется в модуле (singleton): повторный вызов вернёт тот же объект.
    """
    global _CACHED
    try:
        return _CACHED
    except NameError:
        pass
    raw = (os.environ.get("JARVIS_ENGINE") or ENGINE_CLAUDE).strip().lower()
    if raw not in SUPPORTED_ENGINES:
        raise RuntimeError(
            f"JARVIS_ENGINE={raw!r} не поддерживается. "
            f"Допустимо: {sorted(SUPPORTED_ENGINES)}"
        )
    if raw == ENGINE_CLAUDE:
        from engines.claude_engine import ClaudeEngine
        _CACHED = ClaudeEngine()
    elif raw == ENGINE_CODEX:
        from engines.codex_engine import CodexEngine
        _CACHED = CodexEngine()
    else:
        from engines.opencode_engine import OpenCodeEngine
        _CACHED = OpenCodeEngine()
    return _CACHED
