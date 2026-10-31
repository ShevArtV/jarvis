"""Persistent Cursor worker over ``cursor-agent acp``.

Транспорт — :class:`engines.acp_worker.AcpWorker`. Здесь: auth, путь ACP-сессий,
http-only topic MCP, retry на RetriableError.

ACP хранит сессии в ``<config>/acp-sessions/<id>/``, отдельно от чатов
``cursor-agent --print`` — сессии живого и разового режимов не пересекаются.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import Any

from engines.acp_worker import AcpWorker
from engines.common import resolve_cwd
from engines.cursor_engine import (
    CURSOR_BIN,
    FILE_MARKER_SYSTEM,
    CursorEngine,
    _config_dir,
)
from engines.process_control import spawn, terminate_process_tree
from engines.topic_mcp import acp_mcp_servers

logger = logging.getLogger(__name__)

# Патчатся тестами (test_persistent_cursor).
_RETRY_LIMIT = 2
_RETRY_DELAY = 5.0

_RETRIABLE = re.compile(r"^(Error: )?RetriableError\b")


def acp_session_exists(session_id: str) -> bool:
    return bool(session_id) and (_config_dir() / "acp-sessions" / session_id / "store.db").exists()


class PersistentCursorWorker(AcpWorker):
    """Live ``cursor-agent acp`` process for one Telegram topic."""

    label = "cursor"
    file_marker = FILE_MARKER_SYSTEM

    @property
    def retry_limit(self) -> int:
        return _RETRY_LIMIT

    @property
    def retry_delay(self) -> float:
        return _RETRY_DELAY

    def is_retriable(self, answer: str) -> bool:
        return bool(_RETRIABLE.match(answer.strip()))

    def retry_system_prefix(self) -> str:
        return (
            "[SYSTEM: прошлый ход оборвался сбоем связи с сервером Cursor "
            "(RetriableError) — выполни сообщение заново.]\n\n"
        )

    async def authenticate(self) -> None:
        # Ключ API CLI берёт из env сам, а cursor_login при ключе уводит во вход через браузер.
        if not os.environ.get("CURSOR_API_KEY"):
            await self._call("authenticate", {"methodId": "cursor_login"})

    def session_on_disk(self, session_id: str) -> bool:
        return acp_session_exists(session_id)

    def mcp_servers(
        self, mcp_topic_role: str | None, mcp_playwright: bool,
    ) -> list[dict[str, Any]]:
        # Manager MCP — из ~/.cursor/mcp.json; stdio в ACP cursor не принимает.
        if mcp_playwright:
            logger.warning("persistent cursor: Playwright MCP не поддержан, key=%s", self.key)
        return acp_mcp_servers(mcp_topic_role) if mcp_topic_role else []

    def had_print_session(self, session_id: str) -> bool:
        return CursorEngine().session_exists(session_id, self.cwd)


async def start_persistent(
    key: tuple[int, int],
    session_id: str,
    cwd: str,
    model: str | None,
    system_prefix: str | None,
    mcp_playwright: bool,
    mcp_topic_role: str | None = None,
) -> PersistentCursorWorker:
    """Поднять ``cursor-agent acp`` и открыть/загрузить сессию топика."""
    effective_cwd = resolve_cwd(cwd)
    if not os.path.isdir(effective_cwd):
        raise RuntimeError(f"Рабочая папка `{effective_cwd}` не существует.")
    model = model or os.environ.get("CURSOR_MODEL")
    cmd = [CURSOR_BIN, *(["--model", model] if model else []), "acp"]
    logger.info(
        "persistent cursor start: key=%s session=%s cwd=%s model=%s",
        key, session_id, effective_cwd, model,
    )
    proc = await spawn(cmd, cwd=effective_cwd, stdin=asyncio.subprocess.PIPE)
    worker = PersistentCursorWorker(key, proc, session_id, effective_cwd, model)
    worker.reader_task = asyncio.create_task(worker._read_loop())
    worker.stderr_task = asyncio.create_task(worker._read_stderr_loop())
    try:
        await worker.open_session(
            session_id, system_prefix, mcp_topic_role, mcp_playwright=mcp_playwright,
        )
    except BaseException:
        worker.dead = True
        worker.reader_task.cancel()
        worker.stderr_task.cancel()
        await terminate_process_tree(proc)
        raise
    return worker
