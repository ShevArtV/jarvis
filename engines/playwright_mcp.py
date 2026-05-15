"""Runtime Playwright MCP configuration for Jarvis engines.

Jarvis is not an MCP client itself. Browser tools are exposed by the selected
CLI engine, so each engine must have the same Playwright MCP server configured.
This module is called when an engine is activated or used; no manual setup step
is required.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shlex
import shutil
import subprocess
from pathlib import Path


logger = logging.getLogger(__name__)

HOME = Path.home()
SERVER_NAME = os.environ.get("PLAYWRIGHT_MCP_NAME", "playwright")
PLAYWRIGHT_PACKAGE = os.environ.get("PLAYWRIGHT_MCP_PACKAGE", "@playwright/mcp@latest")
PLAYWRIGHT_MCP_MODE = os.environ.get("PLAYWRIGHT_MCP_MODE", "cdp").strip().lower()
PLAYWRIGHT_MCP_CDP_ENDPOINT = os.environ.get("PLAYWRIGHT_MCP_CDP_ENDPOINT", "chrome").strip()
CODEX_CONFIG = Path(
    os.environ.get("CODEX_CONFIG", HOME / ".codex" / "config.toml")
).expanduser()
OPENCODE_CONFIG = Path(
    os.environ.get("OPENCODE_CONFIG", HOME / ".config" / "opencode" / "opencode.json")
).expanduser()

BEGIN_MARKER = "# BEGIN JARVIS PLAYWRIGHT MCP"
END_MARKER = "# END JARVIS PLAYWRIGHT MCP"

_STATUS: dict[str, tuple[bool, str]] = {}


def _version_key(path: Path) -> tuple[int, ...]:
    match = re.search(r"/node/v([0-9.]+)/bin/npx$", str(path))
    if not match:
        return ()
    return tuple(int(part) for part in match.group(1).split(".") if part.isdigit())


def _find_npx() -> str:
    configured = os.environ.get("PLAYWRIGHT_MCP_NPX") or os.environ.get("NPX_BIN")
    if configured:
        path = Path(configured).expanduser()
        if not path.exists():
            raise RuntimeError(f"PLAYWRIGHT_MCP_NPX points to a missing file: {path}")
        return str(path)

    found = shutil.which("npx")
    if found:
        return found

    nvm_matches = sorted(
        HOME.glob(".nvm/versions/node/v*/bin/npx"),
        key=_version_key,
        reverse=True,
    )
    if nvm_matches:
        return str(nvm_matches[0])

    raise RuntimeError("npx not found; install Node.js 20+ or set PLAYWRIGHT_MCP_NPX")


def _playwright_args() -> list[str]:
    default_extra = "" if PLAYWRIGHT_MCP_MODE == "cdp" else "--headless"
    raw_extra = os.environ.get("PLAYWRIGHT_MCP_ARGS", default_extra)
    try:
        extra = shlex.split(raw_extra)
    except ValueError as exc:
        raise RuntimeError(f"PLAYWRIGHT_MCP_ARGS is not valid shell-like syntax: {exc}") from exc
    if PLAYWRIGHT_MCP_MODE == "cdp":
        endpoint = PLAYWRIGHT_MCP_CDP_ENDPOINT
        if not endpoint:
            raise RuntimeError("PLAYWRIGHT_MCP_CDP_ENDPOINT is empty")
        return ["-y", PLAYWRIGHT_PACKAGE, f"--cdp-endpoint={endpoint}", *extra]
    if PLAYWRIGHT_MCP_MODE == "launch":
        return ["-y", PLAYWRIGHT_PACKAGE, *extra]
    raise RuntimeError("PLAYWRIGHT_MCP_MODE must be 'cdp' or 'launch'")


def _validate_server_name() -> None:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", SERVER_NAME):
        raise RuntimeError("PLAYWRIGHT_MCP_NAME may contain only letters, digits, '_' and '-'")


def _toml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _toml_array(values: list[str]) -> str:
    return "[" + ", ".join(_toml_string(v) for v in values) + "]"


def _configure_codex(npx: str, args: list[str]) -> None:
    table = f"mcp_servers.{SERVER_NAME}"
    block = "\n".join(
        [
            BEGIN_MARKER,
            f"[{table}]",
            f"command = {_toml_string(npx)}",
            f"args = {_toml_array(args)}",
            "enabled = true",
            "startup_timeout_sec = 30",
            "tool_timeout_sec = 120",
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


def _configure_opencode(npx: str, args: list[str]) -> None:
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
        "command": [npx, *args],
        "enabled": True,
    }

    new = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    old = OPENCODE_CONFIG.read_text(encoding="utf-8") if OPENCODE_CONFIG.exists() else ""
    if new != old:
        OPENCODE_CONFIG.parent.mkdir(parents=True, exist_ok=True)
        OPENCODE_CONFIG.write_text(new, encoding="utf-8")


def _configure_claude(claude_bin: str, npx: str, args: list[str]) -> None:
    claude = shutil.which(claude_bin) or claude_bin
    if shutil.which(claude) is None and not Path(claude).exists():
        raise RuntimeError(f"claude binary not found: {claude_bin}")

    server_json = json.dumps(
        {"type": "stdio", "command": npx, "args": args},
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


def ensure_playwright_mcp(engine_name: str, engine_bin: str) -> tuple[bool, str]:
    """Ensure Playwright MCP is configured for a single engine.

    Returns (ok, human-readable status). The result is cached for the current
    Jarvis process to avoid touching configs on every prompt.
    """
    engine_name = engine_name.strip().lower()
    cached = _STATUS.get(engine_name)
    if cached is not None:
        return cached

    raw_enabled = os.environ.get("JARVIS_PLAYWRIGHT_MCP", "1").strip().lower()
    if raw_enabled in {"0", "false", "no", "off"}:
        status = (True, "Playwright MCP отключён через JARVIS_PLAYWRIGHT_MCP")
        _STATUS[engine_name] = status
        return status

    try:
        _validate_server_name()
        npx = _find_npx()
        args = _playwright_args()
        if engine_name == "claude":
            _configure_claude(engine_bin, npx, args)
        elif engine_name == "codex":
            _configure_codex(npx, args)
        elif engine_name == "opencode":
            _configure_opencode(npx, args)
        else:
            raise RuntimeError(f"unknown engine: {engine_name}")
    except Exception as exc:
        logger.warning("Playwright MCP setup failed for %s: %s", engine_name, exc)
        status = (False, f"Playwright MCP не подключён: {exc}")
        _STATUS[engine_name] = status
        return status

    status = (True, "Playwright MCP подключён")
    _STATUS[engine_name] = status
    logger.info("Playwright MCP configured for engine=%s", engine_name)
    return status
