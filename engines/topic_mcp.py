"""Remote MCP servers attached per topic role.

Jarvis knows service topic roles — ``secretary`` / ``teamlead`` — plus
``agent`` (every project/execution topic). The old ``manager`` role remains
a compatibility alias for manager-level credentials. This module lets an external
integration declare remote MCP servers whose credentials differ per role, so
one Telegram forum can talk to the same service under two identities without
either identity leaking into the other's topics.

Jarvis itself knows nothing about any particular service: the whole
integration is a JSON file pointed at by ``JARVIS_TOPIC_MCP_CONFIG``. A live
example is the ``jarvis-mxboard-poller`` project, which bridges an mxBoard
kanban into Jarvis and ships its own template for this file.

Config format (``roles`` is optional — without it the server is attached to
every role using the top-level ``headers``)::

    {
      "servers": [
        {
          "name": "mxboard",
          "url": "https://example.org/rest-mcp.php",
          "roles": {
            "manager":   {"headers": {"Authorization": "Bearer <manager-token>"}},
            "secretary": {"headers": {"Authorization": "Bearer <manager-token>"}},
            "teamlead":  {"headers": {"Authorization": "Bearer <manager-token>"}},
            "agent":     {"headers": {"Authorization": "Bearer <agent-token>"}}
          }
        }
      ]
    }

**Absence of the config is not an error.** No path, missing file, broken JSON,
one bad server entry — Jarvis logs it and runs without those servers. A topic
losing a tool is an inconvenience; a bot that cannot answer at all is an
outage, and this used to be exactly that (2026-07-25: a missing config raised
``RuntimeError`` on every single message).
"""

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)

HOME = Path.home()

ROLE_MANAGER = "manager"
ROLE_SECRETARY = "secretary"
ROLE_TEAMLEAD = "teamlead"
ROLE_AGENT = "agent"
ROLES = (ROLE_MANAGER, ROLE_SECRETARY, ROLE_TEAMLEAD, ROLE_AGENT)
ROLE_COMPAT_ALIASES = {
    ROLE_SECRETARY: (ROLE_MANAGER,),
    ROLE_TEAMLEAD: (ROLE_MANAGER,),
}

_NAME_RE = re.compile(r"[A-Za-z0-9_-]+")
# Only remote HTTP servers: a per-role identity is a credential, and stdio
# servers carry credentials in argv/env, where they leak into the process list.
_HTTP_TYPES = {"http", "remote", "sse", "streamable-http"}

# Parsed config cached by (path, mtime, size) — the bot reads this on every
# turn, and re-parsing (plus re-emitting the same warning) each time is waste.
_CACHE: dict[str, Any] = {"stat": None, "servers": None}


def topic_mcp_enabled() -> bool:
    raw = os.environ.get("JARVIS_TOPIC_MCP", "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def config_path() -> Path | None:
    """Configured path, or ``None`` when no integration is set up."""
    raw = (os.environ.get("JARVIS_TOPIC_MCP_CONFIG") or "").strip()
    if not raw:
        return None
    return Path(raw).expanduser()


def _load_servers() -> list[dict[str, Any]]:
    """Raw ``servers`` list from the config file. Never raises."""
    path = config_path()
    if path is None:
        return []

    try:
        stat = path.stat()
        key = (str(path), stat.st_mtime_ns, stat.st_size)
    except OSError as exc:
        # The path was set explicitly, so silence here would hide a typo until
        # someone noticed the missing tools mid-task.
        logger.warning("JARVIS_TOPIC_MCP_CONFIG unreadable (%s): %s", path, exc)
        _CACHE["stat"], _CACHE["servers"] = None, None
        return []

    if _CACHE["stat"] == key and _CACHE["servers"] is not None:
        return _CACHE["servers"]

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("cannot parse topic MCP config %s: %s", path, exc)
        _CACHE["stat"], _CACHE["servers"] = key, []
        return []

    if isinstance(data, dict):
        servers = data.get("servers")
    else:
        servers = data
    if not isinstance(servers, list):
        logger.warning("%s: expected a 'servers' list, got %s", path, type(servers).__name__)
        servers = []

    _CACHE["stat"], _CACHE["servers"] = key, servers
    return servers


def _headers(entry: Any) -> dict[str, str]:
    if not isinstance(entry, dict):
        return {}
    raw = entry.get("headers")
    if not isinstance(raw, dict):
        return {}
    return {str(k): str(v) for k, v in raw.items() if str(k)}


def _server_for_role(raw: Any, role: str, index: int) -> dict[str, Any] | None:
    """Validate one config entry against ``role``. ``None`` = skip it."""
    if not isinstance(raw, dict):
        logger.warning("topic MCP server #%d is not an object — skipped", index)
        return None

    name = str(raw.get("name") or "").strip()
    if not _NAME_RE.fullmatch(name):
        logger.warning(
            "topic MCP server #%d has an invalid name %r (allowed: letters, "
            "digits, '_', '-') — skipped", index, name,
        )
        return None

    if raw.get("enabled") is False:
        return None

    kind = str(raw.get("type") or "http").strip().lower()
    if kind not in _HTTP_TYPES:
        logger.warning(
            "topic MCP server %r has unsupported type %r — only remote HTTP is "
            "supported, skipped", name, kind,
        )
        return None

    roles = raw.get("roles")
    role_entry: Any = None
    if roles is None:
        role_entry = {}
    elif isinstance(roles, dict):
        role_key = role
        if role_key not in roles:
            for alias in ROLE_COMPAT_ALIASES.get(role, ()):
                if alias in roles:
                    role_key = alias
                    break
        if role_key not in roles:
            return None
        role_entry = roles[role_key]
        if role_entry is None or role_entry is False:
            return None
    else:
        logger.warning("topic MCP server %r: 'roles' must be an object — skipped", name)
        return None

    url = str(
        (role_entry.get("url") if isinstance(role_entry, dict) else None)
        or raw.get("url")
        or ""
    ).strip()
    if not url:
        logger.warning("topic MCP server %r has no url for role %r — skipped", name, role)
        return None

    headers = {**_headers(raw), **_headers(role_entry)}
    return {"name": name, "role": role, "url": url, "headers": headers}


def servers_for_role(role: str) -> list[dict[str, Any]]:
    """``[{name, role, url, headers}]`` for the given topic role.

    Empty list means "nothing to attach" — no configuration, disabled, or every
    entry invalid. Callers must treat all three the same way.
    """
    if not topic_mcp_enabled():
        return []
    role = (role or "").strip().lower()
    if role not in ROLES:
        logger.warning("unknown topic role %r — no MCP servers attached", role)
        return []

    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(_load_servers()):
        spec = _server_for_role(raw, role, index)
        if spec is None:
            continue
        if spec["name"] in seen:
            logger.warning("duplicate topic MCP server %r — later one ignored", spec["name"])
            continue
        seen.add(spec["name"])
        result.append(spec)
    return result


def log_role(role: str) -> list[dict[str, Any]]:
    """Resolve servers for ``role`` and log the result (never the headers)."""
    specs = servers_for_role(role)
    if specs:
        logger.info(
            "topic MCP role=%s servers=%s", role, ",".join(s["name"] for s in specs),
        )
    return specs


# ---------- Engine-specific rendering ----------

def claude_mcp_servers(role: str) -> dict[str, dict[str, Any]]:
    """``mcpServers`` fragment for ``claude --mcp-config``."""
    return {
        spec["name"]: {"type": "http", "url": spec["url"], "headers": spec["headers"]}
        for spec in servers_for_role(role)
    }


def opencode_mcp_servers(role: str) -> dict[str, dict[str, Any]]:
    """``mcp`` fragment for an opencode config file."""
    return {
        spec["name"]: {
            "type": "remote",
            "url": spec["url"],
            "headers": spec["headers"],
            "enabled": True,
        }
        for spec in servers_for_role(role)
    }


def create_codex_profile(role: str) -> tuple[str, Path] | None:
    """Write a temporary Codex profile-v2 config; ``None`` if nothing to attach.

    Codex ``-c`` overrides would put credentials into process argv. A named
    profile keeps argv clean: Jarvis passes only ``--profile-v2 <name>`` and
    deletes the generated file once Codex has loaded it.
    """
    specs = servers_for_role(role)
    if not specs:
        return None

    codex_home = Path(os.environ.get("CODEX_HOME", HOME / ".codex")).expanduser()
    try:
        codex_home.mkdir(mode=0o700, parents=True, exist_ok=True)
    except OSError as exc:
        logger.warning("cannot prepare CODEX_HOME %s: %s", codex_home, exc)
        return None

    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=codex_home,
            prefix="jarvis-topic-mcp-",
            suffix=".config.toml",
            delete=False,
        ) as fh:
            path = Path(fh.name)
            try:
                path.chmod(0o600)
            except OSError:
                logger.debug("cannot chmod Codex profile %s", path, exc_info=True)

            for spec in specs:
                header_items = ", ".join(
                    f"{key} = {json.dumps(str(value), ensure_ascii=False)}"
                    for key, value in spec["headers"].items()
                )
                fh.write(f"[mcp_servers.{spec['name']}]\n")
                fh.write(f"url = {json.dumps(spec['url'], ensure_ascii=False)}\n")
                fh.write("enabled = true\n")
                if header_items:
                    fh.write(f"http_headers = {{ {header_items} }}\n")
                fh.write("\n")
    except OSError as exc:
        logger.warning("cannot write Codex topic MCP profile: %s", exc)
        return None

    profile_name = path.name.removesuffix(".config.toml")
    logger.info(
        "topic MCP Codex profile role=%s servers=%s profile=%s",
        role, ",".join(s["name"] for s in specs), profile_name,
    )
    return profile_name, path


def codex_inline_config_flags(role: str) -> list[str]:
    """``-c`` flags for Codex entry points that cannot use profile-v2.

    Only the persistent app-server needs this; it puts credentials into argv,
    which is why ``create_codex_profile`` is preferred everywhere else.
    """
    flags: list[str] = []
    for spec in servers_for_role(role):
        table = f"mcp_servers.{spec['name']}"
        flags.extend([
            "-c", f"{table}.url={json.dumps(spec['url'], ensure_ascii=False)}",
            "-c", f"{table}.enabled=true",
        ])
        if spec["headers"]:
            header_items = ", ".join(
                f"{key} = {json.dumps(str(value), ensure_ascii=False)}"
                for key, value in spec["headers"].items()
            )
            flags.extend(["-c", f"{table}.http_headers={{ {header_items} }}"])
    return flags


def cleanup_codex_profile(path: Path | None) -> None:
    if path is None:
        return
    try:
        path.unlink(missing_ok=True)
    except OSError:
        logger.warning("cannot remove Codex topic MCP profile %s", path, exc_info=True)
