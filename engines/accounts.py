"""Аккаунты внутри движка: несколько логинов одного CLI на одной машине.

CLI держит логин в своём каталоге конфигов; каталог переназначается
переменными окружения. Аккаунт = имя + корневой каталог. ``main`` есть
всегда — это каталог CLI по умолчанию, переменные не ставятся.

``.env``::

    JARVIS_ACCOUNTS=claude:work=~/.claude-work,codex:alt=~/.codex-alt,kimi:job=~/.kimi-job,cursor:job=~/.cursor-job,opencode:job=~/.opencode-job

Переменные на ход (через ``spawn``):

| движок | env |
|---|---|
| claude | ``CLAUDE_CONFIG_DIR`` = каталог |
| codex | ``CODEX_HOME`` = каталог |
| kimi | ``KIMI_CODE_HOME`` = каталог |
| cursor | ``CURSOR_CONFIG_DIR`` = каталог, ``XDG_CONFIG_HOME`` = ``<каталог>/xdg-config`` (auth) |
| opencode | ``XDG_CONFIG_HOME`` / ``XDG_DATA_HOME`` / ``XDG_STATE_HOME`` = ``config``/``data``/``state`` под каталогом; ``OPENCODE_CONFIG`` = ``…/config/opencode/opencode.json`` |

У cursor/opencode XDG_* наследуются дочерними процессами CLI — это цена
изоляции auth. Каталог аккаунта лучше держать отдельным.

Аккаунт выбирается per-topic (``/engine``) и на время хода выставляется в
ContextVar. ``spawn`` подмешивает переменные в окружение процесса CLI.
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

# Движки, для которых в JARVIS_ACCOUNTS допускаются доп. аккаунты.
ACCOUNT_ENGINES = frozenset({"claude", "codex", "kimi", "cursor", "opencode"})

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
        if engine not in ACCOUNT_ENGINES or not name or not path or name == MAIN_ACCOUNT:
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


def env_for_account(engine_name: str, path: Path) -> dict[str, str]:
    """Env-переменные CLI для корневого каталога аккаунта."""
    if engine_name == "claude":
        return {"CLAUDE_CONFIG_DIR": str(path)}
    if engine_name == "codex":
        return {"CODEX_HOME": str(path)}
    if engine_name == "kimi":
        return {"KIMI_CODE_HOME": str(path)}
    if engine_name == "cursor":
        # Данные CLI — в корне; auth.json — в xdg-config/cursor/ (как ~/.config/cursor).
        return {
            "CURSOR_CONFIG_DIR": str(path),
            "XDG_CONFIG_HOME": str(path / "xdg-config"),
        }
    if engine_name == "opencode":
        config = path / "config"
        data = path / "data"
        state = path / "state"
        return {
            "XDG_CONFIG_HOME": str(config),
            "XDG_DATA_HOME": str(data),
            "XDG_STATE_HOME": str(state),
            "OPENCODE_CONFIG": str(config / "opencode" / "opencode.json"),
        }
    return {}


def account_env() -> dict[str, str]:
    """Переменные окружения для процесса CLI текущего хода (или пусто)."""
    current = CURRENT_ACCOUNT.get()
    if current is None:
        return {}
    path = account_dir(*current)
    if path is None:
        return {}
    return env_for_account(current[0], path)


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
