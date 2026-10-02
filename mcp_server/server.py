"""Точка входа MCP-сервера: сборка тулов и запуск stdio-транспорта."""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import logging
import sys
from pathlib import Path

from mcp_server import common
from mcp_server.common import logger, mcp

BUILTIN_TOOL_MODULES = (
    "mcp_server.tools.topics",
    "mcp_server.tools.jobs",
    "mcp_server.tools.asks",
)


def _plugin_names() -> list[str]:
    raw = common._env_or_dotenv("JARVIS_PLUGINS") or ""
    return [name.strip() for name in raw.split(",") if name.strip()]


def load_tools() -> None:
    """Зарегистрировать встроенные тулы и тулы плагинов из JARVIS_PLUGINS.

    Плагин без модуля mcp_tools пропускается молча — MCP-тулов у него может
    не быть. Ошибка импорта или регистрации — warning, остальные грузятся.
    """
    # Встроенные тулы регистрируются декораторами при импорте модуля.
    for module_name in BUILTIN_TOOL_MODULES:
        importlib.import_module(module_name)

    for name in _plugin_names():
        module_name = f"plugins.{name}.mcp_tools"
        try:
            if importlib.util.find_spec(module_name) is None:
                continue
            module = importlib.import_module(module_name)
            module.register(common.mcp)
        except Exception as exc:
            logger.warning("plugin %s: MCP tools not loaded: %s", name, exc)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Jarvis Manager MCP server")
    parser.add_argument(
        "--db",
        required=True,
        type=Path,
        help="Absolute path to Jarvis bot_state.db (shared with the running bot).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="[jarvis-mcp] %(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    # httpx пишет на INFO каждый запрос с полным URL, а в URL Bot API — токен.
    logging.getLogger("httpx").setLevel(logging.WARNING)

    db_path = args.db.expanduser().resolve()
    if not db_path.exists():
        logger.error("DB not found: %s", db_path)
        return 2

    common._DB_PATH = db_path
    logger.info("Jarvis MCP server starting (db=%s)", common._DB_PATH)
    load_tools()
    mcp.run(transport="stdio")
    return 0
