"""Runtime registration of the Jarvis Manager MCP server for engines.

Like ``playwright_mcp``, this module idempotently writes the MCP config for
claude/codex/opencode so the Manager agent gets ``manager_topics`` /
``manager_inbox`` tools natively. The actual server lives in
``scripts/jarvis_mcp_server.py`` and is launched via the bot's venv Python.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path


logger = logging.getLogger(__name__)

HOME = Path.home()
SERVER_NAME = os.environ.get("JARVIS_MCP_NAME", "jarvis")

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SCRIPT = REPO_ROOT / "scripts" / "jarvis_mcp_server.py"
DEFAULT_DB = REPO_ROOT / "bot_state.db"
DEFAULT_PYTHON = REPO_ROOT / "venv" / "bin" / "python"

CODEX_CONFIG = Path(
    os.environ.get("CODEX_CONFIG", HOME / ".codex" / "config.toml")
).expanduser()
OPENCODE_CONFIG = Path(
    os.environ.get("OPENCODE_CONFIG", HOME / ".config" / "opencode" / "opencode.json")
).expanduser()

BEGIN_MARKER = "# BEGIN JARVIS MANAGER MCP"
END_MARKER = "# END JARVIS MANAGER MCP"

_STATUS: dict[str, tuple[bool, str]] = {}


def _validate_server_name() -> None:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", SERVER_NAME):
        raise RuntimeError(
            "JARVIS_MCP_NAME may contain only letters, digits, '_' and '-'"
        )


def _resolve_python() -> str:
    """Pick a Python interpreter that has the mcp SDK available.

    Prefer the bot's venv (always installed by `pip install -r requirements.txt`),
    then JARVIS_MCP_PYTHON env, then the interpreter running the bot.
    """
    configured = os.environ.get("JARVIS_MCP_PYTHON")
    if configured:
        path = Path(configured).expanduser()
        if not path.exists():
            raise RuntimeError(f"JARVIS_MCP_PYTHON missing: {path}")
        return str(path)
    if DEFAULT_PYTHON.exists():
        return str(DEFAULT_PYTHON)
    return sys.executable


def _resolve_script() -> str:
    configured = os.environ.get("JARVIS_MCP_SCRIPT")
    if configured:
        path = Path(configured).expanduser()
        if not path.exists():
            raise RuntimeError(f"JARVIS_MCP_SCRIPT missing: {path}")
        return str(path)
    if not DEFAULT_SCRIPT.exists():
        raise RuntimeError(f"MCP server script not found: {DEFAULT_SCRIPT}")
    return str(DEFAULT_SCRIPT)


def _resolve_db() -> str:
    configured = os.environ.get("JARVIS_MCP_DB") or os.environ.get("JARVIS_DB_PATH")
    if configured:
        return str(Path(configured).expanduser())
    return str(DEFAULT_DB)


def _command_args() -> tuple[str, list[str]]:
    """(python_path, [script, --db, db_path]) used by every engine config."""
    python = _resolve_python()
    script = _resolve_script()
    db = _resolve_db()
    return python, [script, "--db", db]


def _toml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _toml_array(values: list[str]) -> str:
    return "[" + ", ".join(_toml_string(v) for v in values) + "]"


def _configure_codex(python: str, args: list[str]) -> None:
    table = f"mcp_servers.{SERVER_NAME}"
    block = "\n".join(
        [
            BEGIN_MARKER,
            f"[{table}]",
            f"command = {_toml_string(python)}",
            f"args = {_toml_array(args)}",
            "enabled = true",
            "startup_timeout_sec = 30",
            "tool_timeout_sec = 60",
            END_MARKER,
            "",
        ]
    )
    old = CODEX_CONFIG.read_text(encoding="utf-8") if CODEX_CONFIG.exists() else ""
    managed_re = re.compile(
        rf"(?ms)^{re.escape(BEGIN_MARKER)}\n.*?^{re.escape(END_MARKER)}\n?"
    )
    if managed_re.search(old):
        new = managed_re.sub(block, old)
    else:
        table_re = re.compile(rf"(?ms)^\[{re.escape(table)}\]\n.*?(?=^\[|\Z)")
        if table_re.search(old):
            new = table_re.sub(block, old)
        else:
            new = old.rstrip() + ("\n\n" if old.strip() else "") + block
    if new != old:
        CODEX_CONFIG.parent.mkdir(parents=True, exist_ok=True)
        CODEX_CONFIG.write_text(new, encoding="utf-8")


def _configure_opencode(python: str, args: list[str]) -> None:
    if OPENCODE_CONFIG.exists():
        try:
            data = json.loads(OPENCODE_CONFIG.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Cannot parse {OPENCODE_CONFIG}: {exc}") from exc
    else:
        data = {"$schema": "https://opencode.ai/config.json"}
    if not isinstance(data, dict):
        raise RuntimeError(f"{OPENCODE_CONFIG} must contain a JSON object")
    mcp = data.setdefault("mcp", {})
    if not isinstance(mcp, dict):
        raise RuntimeError(f"{OPENCODE_CONFIG}: `mcp` must be an object")
    mcp[SERVER_NAME] = {
        "type": "local",
        "command": [python, *args],
        "enabled": True,
    }
    new = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    old = OPENCODE_CONFIG.read_text(encoding="utf-8") if OPENCODE_CONFIG.exists() else ""
    if new != old:
        OPENCODE_CONFIG.parent.mkdir(parents=True, exist_ok=True)
        OPENCODE_CONFIG.write_text(new, encoding="utf-8")


def _configure_claude(claude_bin: str, python: str, args: list[str]) -> None:
    claude = shutil.which(claude_bin) or claude_bin
    if shutil.which(claude) is None and not Path(claude).exists():
        raise RuntimeError(f"claude binary not found: {claude_bin}")
    server_json = json.dumps(
        {"type": "stdio", "command": python, "args": args},
        ensure_ascii=False,
    )
    subprocess.run(
        [claude, "mcp", "remove", "--scope", "user", SERVER_NAME],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    result = subprocess.run(
        [claude, "mcp", "add-json", "--scope", "user", SERVER_NAME, server_json],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(stderr or f"claude mcp add-json failed with rc={result.returncode}")


def ensure_jarvis_mcp(engine_name: str, engine_bin: str) -> tuple[bool, str]:
    """Idempotently register Jarvis Manager MCP for one engine.

    Returns (ok, human-readable status). Result is cached per-process.
    """
    engine_name = engine_name.strip().lower()
    cached = _STATUS.get(engine_name)
    if cached is not None:
        return cached

    raw_enabled = os.environ.get("JARVIS_MANAGER_MCP", "1").strip().lower()
    if raw_enabled in {"0", "false", "no", "off"}:
        status = (True, "Jarvis Manager MCP отключён через JARVIS_MANAGER_MCP")
        _STATUS[engine_name] = status
        return status

    try:
        _validate_server_name()
        python, args = _command_args()
        if engine_name == "claude":
            _configure_claude(engine_bin, python, args)
        elif engine_name == "codex":
            _configure_codex(python, args)
        elif engine_name == "opencode":
            _configure_opencode(python, args)
        else:
            raise RuntimeError(f"unknown engine: {engine_name}")
    except Exception as exc:
        logger.warning("Jarvis MCP setup failed for %s: %s", engine_name, exc)
        status = (False, f"Jarvis Manager MCP не подключён: {exc}")
        _STATUS[engine_name] = status
        return status

    status = (True, "Jarvis Manager MCP подключён")
    _STATUS[engine_name] = status
    logger.info("Jarvis Manager MCP configured for engine=%s", engine_name)
    return status
