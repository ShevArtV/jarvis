"""Persistent Kimi worker over ``kimi acp``.

Транспорт — :class:`engines.acp_worker.AcpWorker`. Сессии print и ACP общие
(``~/.kimi-code/sessions/``) — ``session/load`` подхватывает разовый сеанс.
ACP принимает stdio MCP → Playwright и http topic-MCP уходят в ``mcpServers``.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from engines.acp_worker import AcpWorker
from engines.common import is_placeholder, resolve_cwd
from engines.kimi_engine import FILE_MARKER_SYSTEM, KIMI_BIN, KimiEngine
from engines.playwright_mcp import playwright_command_args, playwright_server_name
from engines.process_control import spawn, terminate_process_tree
from engines.topic_mcp import acp_mcp_servers

logger = logging.getLogger(__name__)


def _playwright_stdio_server() -> dict[str, Any] | None:
    spec = playwright_command_args()
    if spec is None:
        return None
    command, args = spec
    return {
        "type": "stdio",
        "name": playwright_server_name(),
        "command": command,
        "args": args,
    }


class PersistentKimiWorker(AcpWorker):
    """Live ``kimi acp`` process for one Telegram topic."""

    label = "kimi"
    file_marker = FILE_MARKER_SYSTEM
    # kimi отвечает «another turn is already in progress» на второй prompt.
    supports_steer = False

    async def authenticate(self) -> None:
        # Device-code login; при уже валидном oauth CLI отвечает без ошибок.
        try:
            await self._call("authenticate", {"methodId": "login"})
        except RuntimeError as exc:
            # Уже залогинен / method не нужен — не валим старт сессии.
            logger.info("kimi acp authenticate skipped: %s", exc)

    def session_on_disk(self, session_id: str) -> bool:
        if is_placeholder(session_id):
            return False
        return KimiEngine().session_exists(session_id, self.cwd)

    def mcp_servers(
        self, mcp_topic_role: str | None, mcp_playwright: bool,
    ) -> list[dict[str, Any]]:
        servers: list[dict[str, Any]] = []
        if mcp_topic_role:
            servers.extend(acp_mcp_servers(mcp_topic_role))
        if mcp_playwright:
            pw = _playwright_stdio_server()
            if pw is not None:
                servers.append(pw)
            else:
                logger.warning(
                    "persistent kimi: Playwright запрошен, но недоступен, key=%s",
                    self.key,
                )
        return servers

    def had_print_session(self, session_id: str) -> bool:
        # Print и ACP делят каталог — fresh только если id был, но load не нашёл
        # (сюда не попадаем). Оставляем False.
        return False


async def start_persistent(
    key: tuple[int, int],
    session_id: str,
    cwd: str,
    model: str | None,
    system_prefix: str | None,
    mcp_playwright: bool,
    mcp_topic_role: str | None = None,
) -> PersistentKimiWorker:
    """Поднять ``kimi acp`` и открыть/загрузить сессию топика."""
    effective_cwd = resolve_cwd(cwd)
    if not os.path.isdir(effective_cwd):
        raise RuntimeError(f"Рабочая папка `{effective_cwd}` не существует.")
    model = model or os.environ.get("KIMI_MODEL")
    cmd = [KIMI_BIN, *(["--model", model] if model else []), "acp"]
    logger.info(
        "persistent kimi start: key=%s session=%s cwd=%s model=%s",
        key, session_id, effective_cwd, model,
    )
    proc = await spawn(cmd, cwd=effective_cwd, stdin=asyncio.subprocess.PIPE)
    worker = PersistentKimiWorker(key, proc, session_id, effective_cwd, model)
    worker.reader_task = asyncio.create_task(worker._read_loop())
    worker.stderr_task = asyncio.create_task(worker._read_stderr_loop())
    try:
        await worker.open_session(
            session_id, system_prefix, mcp_topic_role, mcp_playwright=mcp_playwright,
        )
        if model:
            # set_config_option / set_model — best-effort, модель могла уйти в argv.
            try:
                await worker._call(
                    "session/set_config_option",
                    {"sessionId": worker.session_id, "configId": "model", "value": model},
                )
            except RuntimeError:
                try:
                    await worker._call(
                        "session/set_model",
                        {"sessionId": worker.session_id, "modelId": model},
                    )
                except RuntimeError:
                    logger.debug("kimi acp set model failed", exc_info=True)
    except BaseException:
        worker.dead = True
        worker.reader_task.cancel()
        worker.stderr_task.cancel()
        await terminate_process_tree(proc)
        raise
    return worker
