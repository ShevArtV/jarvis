"""Интерфейс адаптера движка и его общая часть.

``Engine`` — контракт, на который опирается бот (структурный тип). ``BaseEngine``
— то, что у адаптеров совпадает: список моделей поверх TTL-кэша и no-op для
pidfile'ов. Наследовать его не обязательно, но все встроенные адаптеры так и
делают.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol

from engines.model_cache import cached_models, prewarm


class Engine(Protocol):
    """Интерфейс адаптера движка. Все методы строго asyncio-safe, где применимо."""

    name: str            # "claude" | "codex" | "opencode"
    bin_path: str        # путь/имя бинаря CLI (для shutil.which)

    # Реально доступные модели — property поверх TTL-кэша (engines/model_cache.py):
    # адаптер спрашивает свой CLI (env-override → конфиг/кэш CLI → `<cli> models`),
    # при осечке отдаёт свой дефолт. Пустой список = «модель не выбирается».
    models: list[str]

    def prewarm_models(self) -> None: ...   # заполнить кэш моделей; блокирует

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
        system_prefix: str | None = None,
        mcp_playwright: bool = False,
        mcp_topic_role: str | None = None,
    ) -> tuple[bool, str, str | None, str | None]: ...
    # Возвращает (ok, final_text, session_id_after, actual_model).
    # actual_model — модель, которой CLI ответил последний раз (из stream
    # event'ов: 'system.init' у claude, 'turn.started' у codex, 'message'
    # у opencode). None если не удалось определить.
    #
    # system_prefix — постоянный [SYSTEM:]-блок (cwd + правила). Адаптер
    #   размещает его в системном канале своего CLI: у claude —
    #   --append-system-prompt (каждый ход, как system → кешируется, не копится
    #   в транскрипте); у codex/opencode (нет system-канала) — префиксом к
    #   prompt ТОЛЬКО на новой сессии (на resume он уже в транскрипте).
    # mcp_playwright — если True, адаптер инъектит Playwright MCP per-invocation
    #   (claude: --mcp-config; codex: -c оверрайды; opencode: OPENCODE_CONFIG
    #   temp-файл). По умолчанию браузер НЕ грузится — это on-demand.
    # mcp_topic_role — роль топика ('manager' | 'agent'). Адаптер инъектит
    #   remote MCP-серверы, объявленные для этой роли в JARVIS_TOPIC_MCP_CONFIG
    #   (см. engines/topic_mcp.py): один форум может работать с внешним сервисом
    #   под двумя личностями, не перемешивая их между топиками. Нет конфига —
    #   серверов просто нет, это НЕ ошибка.
    # Будущие движки ОБЯЗАНЫ реализовать оба контракта (см. README, раздел
    # «Как подключить новый движок»).


class BaseEngine:
    """Общая реализация ``Engine``: модели и pidfile. Сессии и call_stream —
    у каждого адаптера свои."""

    name: str
    bin_path: str
    # Фолбэк, если CLI не сообщил ни одной модели.
    default_models: list[str] = []

    def _discover_models(self) -> list[str]:
        """Опросить CLI/конфиг о доступных моделях (блокирует)."""
        return []

    @property
    def models(self) -> list[str]:
        return cached_models(self.name, self._discover_models, self.default_models)

    def prewarm_models(self) -> None:
        prewarm(self.name, self._discover_models, self.default_models)

    def clear_stale_session_pidfile(self, session_id: str) -> None:
        """По умолчанию CLI не держит pidfile-локов — чистить нечего."""
        return None
