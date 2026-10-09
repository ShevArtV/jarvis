"""Аккаунты внутри движка: несколько логинов одного CLI на одной машине.

CLI держит логин в своём каталоге конфигов, и каталог переназначается
переменной окружения: у claude — ``CLAUDE_CONFIG_DIR``, у codex — ``CODEX_HOME``.
Аккаунт = имя + такой каталог. ``main`` есть всегда — это каталог по умолчанию,
переменная не ставится.

Остальные аккаунты задаются в ``.env``::

    JARVIS_ACCOUNTS=claude:work=~/.claude-work,codex:alt=~/.codex-alt

Аккаунт выбирается per-topic (``/engine``) и на время хода выставляется в
ContextVar — так же, как модель. ``spawn`` подмешивает переменную в окружение
процесса, поэтому до CLI она доходит из любого места запуска.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path

logger = logging.getLogger(__name__)

MAIN_ACCOUNT = "main"

# Переменная, которой CLI движка переназначает каталог конфигов.
ACCOUNT_ENV_VARS = {
    "claude": "CLAUDE_CONFIG_DIR",
    "codex": "CODEX_HOME",
}

# (engine, account) текущего хода; None — main.
CURRENT_ACCOUNT: ContextVar[tuple[str, str] | None] = ContextVar("engine_account", default=None)


def _parse(raw: str) -> dict[str, dict[str, Path]]:
    accounts: dict[str, dict[str, Path]] = {}
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        engine, _, rest = item.partition(":")
        name, _, path = rest.partition("=")
        engine, name, path = engine.strip().lower(), name.strip(), path.strip()
        if engine not in ACCOUNT_ENV_VARS or not name or not path or name == MAIN_ACCOUNT:
            logger.warning("JARVIS_ACCOUNTS: пропускаю %r", item)
            continue
        accounts.setdefault(engine, {})[name] = Path(path).expanduser()
    return accounts


def configured() -> dict[str, dict[str, Path]]:
    """Дополнительные аккаунты из env: {engine: {name: каталог}}."""
    return _parse(os.environ.get("JARVIS_ACCOUNTS", ""))


def account_names(engine_name: str) -> list[str]:
    """Аккаунты движка, ``main`` первым. Один элемент — выбирать нечего."""
    return [MAIN_ACCOUNT, *configured().get(engine_name, {})]


def account_dir(engine_name: str, account: str | None) -> Path | None:
    """Каталог конфигов аккаунта; None — каталог CLI по умолчанию."""
    if not account or account == MAIN_ACCOUNT:
        return None
    return configured().get(engine_name, {}).get(account)


def current_dir(engine_name: str) -> Path | None:
    """Каталог аккаунта текущего хода, если он выставлен для этого движка."""
    current = CURRENT_ACCOUNT.get()
    if current is None or current[0] != engine_name:
        return None
    return account_dir(*current)


def account_env() -> dict[str, str]:
    """Переменная окружения для процесса CLI текущего хода (или пусто)."""
    current = CURRENT_ACCOUNT.get()
    if current is None:
        return {}
    path = account_dir(*current)
    if path is None:
        return {}
    return {ACCOUNT_ENV_VARS[current[0]]: str(path)}


@contextmanager
def engine_account_scope(engine_name: str, account: str | None) -> Iterator[None]:
    """Выставляет аккаунт движка на время блока. main/None — no-op."""
    if not account or account == MAIN_ACCOUNT:
        yield
        return
    token = CURRENT_ACCOUNT.set((engine_name, account))
    try:
        yield
    finally:
        CURRENT_ACCOUNT.reset(token)
