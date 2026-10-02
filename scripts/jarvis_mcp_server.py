#!/usr/bin/env python3
"""Jarvis Manager MCP server — точка входа.

Путь к этому файлу прописан в конфигах движков (`engines/jarvis_mcp.py`),
поэтому он остаётся, а сам сервер живёт в пакете `mcp_server/`.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from mcp_server.server import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
