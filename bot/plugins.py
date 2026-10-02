"""Подключаемые интеграции: пакеты ``plugins/<name>``, список — ``JARVIS_PLUGINS``.

Пакет плагина объявляет в ``plugin.py`` объект ``PLUGIN = Plugin(...)``. Ядро
берёт из него фоновые задачи, команды Telegram, источники триггеров и DDL своих
таблиц. MCP-тулы плагина лежат в ``plugins/<name>/mcp_tools.py`` — их подключает
MCP-сервер по тому же списку.

Без ``JARVIS_PLUGINS`` ядро работает без интеграций: чистая установка не тянет
чужих зависимостей и не стучится в чужие сервисы.
"""

from __future__ import annotations

import importlib
import logging
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from functools import cache

from telegram.ext import Application

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TriggerSource:
    """Как ядро исполняет ``agent_triggers`` с этим ``source``.

    ``coalesce_seconds`` — склеивать серию: ждать, пока самому старому
    триггеру исполнится столько секунд (0 — не ждать), и забрать все
    ожидающие триггеры топика одним ходом; None — каждый триггер отдельно.
    ``build_prompt`` получает тексты серии и собирает промпт хода.
    ``allow_silent`` разрешает агенту ответить ``[[SILENT]]`` — тогда в топик
    ничего не уходит."""

    build_prompt: Callable[[list[str]], str]
    coalesce_seconds: float | None = None
    allow_silent: bool = False


@dataclass(frozen=True)
class Command:
    name: str
    callback: Callable[..., Awaitable[None]]
    description: str


@dataclass(frozen=True)
class Plugin:
    name: str
    # Фоновые задачи: корутина от Application, живёт всё время работы бота.
    workers: tuple[Callable[[Application], Awaitable[None]], ...] = ()
    # Команды Telegram: регистрируются с фильтром разрешённых пользователей и
    # попадают в меню бота.
    commands: tuple[Command, ...] = ()
    # Произвольная настройка Application (например, хендлер в своей группе).
    setup: Callable[[Application], None] | None = None
    trigger_sources: Mapping[str, TriggerSource] = field(default_factory=dict)
    # Идемпотентный DDL своих таблиц (CREATE ... IF NOT EXISTS).
    schema: tuple[str, ...] = ()


def enabled_names(raw: str | None = None) -> list[str]:
    raw = os.environ.get("JARVIS_PLUGINS", "") if raw is None else raw
    return list(dict.fromkeys(n.strip().lower() for n in raw.split(",") if n.strip()))


@cache
def load_plugins() -> tuple[Plugin, ...]:
    """Включённые плагины. Плагин, который не импортируется, пропускается с
    ошибкой в журнале: бот без одной интеграции полезнее упавшего бота."""
    plugins: list[Plugin] = []
    for name in enabled_names():
        try:
            module = importlib.import_module(f"plugins.{name}.plugin")
            plugin = module.PLUGIN
        except Exception:
            logger.exception("plugin %r failed to load — skipped", name)
            continue
        plugins.append(plugin)
    if plugins:
        logger.info("plugins loaded: %s", ", ".join(p.name for p in plugins))
    return tuple(plugins)


def trigger_source(source: str | None) -> TriggerSource | None:
    for plugin in load_plugins():
        if source in plugin.trigger_sources:
            return plugin.trigger_sources[source]
    return None


def coalesce_map() -> dict[str, float]:
    return {
        source: spec.coalesce_seconds
        for plugin in load_plugins()
        for source, spec in plugin.trigger_sources.items()
        if spec.coalesce_seconds is not None
    }
