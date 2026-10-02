"""Playwright MCP wiring for Jarvis engines.

Jarvis is not an MCP client itself — browser tools are exposed by the selected
CLI engine. Unlike the Manager MCP (which every topic always gets), Playwright
is **on-demand**: it is loaded only for topics that explicitly enabled browser
mode (the ``mcp_playwright`` flag / ``/browser`` command). Loading ~30 browser
tools into every prompt of every topic is pure token overhead otherwise.

So this module no longer registers Playwright in the engines' global config.
Instead it exposes :func:`playwright_command_args` — the single source of truth
for the stdio server spec — which each engine adapter injects **per
invocation** in its own CLI dialect (claude ``--mcp-config``, codex ``-c``
overrides, opencode ``OPENCODE_CONFIG`` temp file).

:func:`disable_global_playwright_mcp` cleans up any stale always-on Playwright
registration left by older Jarvis versions, so the on-demand model is honoured
even after an upgrade.
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

from engines.process_control import run_cli

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

# Cache the cleanup result per engine for the current process (config touch once).
_CLEANUP_STATUS: dict[str, tuple[bool, str]] = {}


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


def playwright_enabled() -> bool:
    """False if Playwright is globally disabled via JARVIS_PLAYWRIGHT_MCP."""
    raw = os.environ.get("JARVIS_PLAYWRIGHT_MCP", "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def playwright_server_name() -> str:
    return SERVER_NAME


def playwright_command_args() -> tuple[str, list[str]] | None:
    """``(npx_path, args)`` for the Playwright MCP stdio server.

    Single source of truth used by every engine adapter to inject Playwright
    per invocation. Returns ``None`` when browser support is globally disabled
    (``JARVIS_PLAYWRIGHT_MCP=0``). Raises if enabled but misconfigured (no npx,
    bad args) so the caller can surface a clear error to the user.
    """
    if not playwright_enabled():
        return None
    _validate_server_name()
    npx = _find_npx()
    args = _playwright_args()
    return npx, args


# ---------------------------------------------------------------------------
# Cleanup of stale always-on registration left by older Jarvis versions.
# ---------------------------------------------------------------------------

def _strip_managed_block(text: str) -> str:
    managed_re = re.compile(
        rf"(?ms)\n*^{re.escape(BEGIN_MARKER)}\n.*?^{re.escape(END_MARKER)}\n?"
    )
    return managed_re.sub("\n", text)


def _disable_codex(npx: str | None = None) -> None:
    if not CODEX_CONFIG.exists():
        return
    old = CODEX_CONFIG.read_text(encoding="utf-8")
    new = _strip_managed_block(old)
    # Also drop a plain [mcp_servers.playwright] table if it lingers.
    table_re = re.compile(
        rf"(?ms)^\[mcp_servers\.{re.escape(SERVER_NAME)}\]\n.*?(?=^\[|\Z)"
    )
    new = table_re.sub("", new)
    if new != old:
        CODEX_CONFIG.write_text(new, encoding="utf-8")


def _disable_opencode() -> None:
    if not OPENCODE_CONFIG.exists():
        return
    try:
        data = json.loads(OPENCODE_CONFIG.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Cannot parse {OPENCODE_CONFIG}: {exc}") from exc
    if not isinstance(data, dict):
        return
    mcp = data.get("mcp")
    if isinstance(mcp, dict) and SERVER_NAME in mcp:
        mcp.pop(SERVER_NAME, None)
        new = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
        OPENCODE_CONFIG.write_text(new, encoding="utf-8")


def _disable_claude(claude_bin: str) -> None:
    claude = shutil.which(claude_bin) or claude_bin
    if shutil.which(claude) is None and not Path(claude).exists():
        # No claude binary — nothing global to clean.
        return
    run_cli(
        [claude, "mcp", "remove", "--scope", "user", SERVER_NAME],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )


def disable_global_playwright_mcp(engine_name: str, engine_bin: str) -> tuple[bool, str]:
    """Remove any always-on Playwright registration for one engine.

    Playwright is injected per invocation now; a leftover global entry from an
    older Jarvis would put browser tools into every prompt. Idempotent and
    cached per process. Returns ``(ok, status)``.
    """
    engine_name = engine_name.strip().lower()
    cached = _CLEANUP_STATUS.get(engine_name)
    if cached is not None:
        return cached

    try:
        _validate_server_name()
        if engine_name == "claude":
            _disable_claude(engine_bin)
        elif engine_name == "codex":
            _disable_codex()
        elif engine_name == "opencode":
            _disable_opencode()
        else:
            raise RuntimeError(f"unknown engine: {engine_name}")
    except Exception as exc:
        logger.warning("Playwright MCP cleanup failed for %s: %s", engine_name, exc, exc_info=True)
        status = (False, f"Playwright MCP cleanup не выполнен: {exc}")
        _CLEANUP_STATUS[engine_name] = status
        return status

    status = (True, "Playwright MCP — on-demand (глобальная регистрация снята)")
    _CLEANUP_STATUS[engine_name] = status
    return status
