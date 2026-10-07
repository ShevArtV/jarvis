"""Перевод событий cursor-agent (stream-json разового режима и ACP живого) в
шаги журнала хода и текст ошибок."""

from __future__ import annotations

import json
from typing import Any

from engines.tool_steps import _tool_step

# Вид tool_call cursor → имя инструмента claude: журнал шагов общий с claude.
_TOOL_NAMES = {
    "shell": "Bash",
    "read": "Read",
    "edit": "Edit",
    "write": "Write",
    "delete": "Delete",
    "grep": "Grep",
    "glob": "Glob",
    "semSearch": "Grep",
    "webFetch": "WebFetch",
    "fetch": "WebFetch",
    "webSearch": "WebSearch",
    "task": "Task",
}


def _cursor_tool_step(tool_call: Any, cwd: str) -> str | None:
    """Строка журнала для ``tool_call``: ``{"shellToolCall": {"args": {...}}}``."""
    if not isinstance(tool_call, dict):
        return None
    for key, body in tool_call.items():
        if not key.endswith("ToolCall") or not isinstance(body, dict):
            continue
        kind = key[: -len("ToolCall")]
        args = body.get("args") if isinstance(body.get("args"), dict) else {}
        if kind == "mcp":
            server = args.get("providerIdentifier") or args.get("serverName") or "mcp"
            tool = args.get("toolName") or args.get("name") or ""
            return _tool_step(f"mcp__{server}__{tool}", args.get("args"), cwd)
        if kind == "shell" and body.get("description") and not args.get("description"):
            args = {**args, "description": body["description"]}
        # Файловые инструменты cursor называют путь `path`, у claude — `file_path`.
        if args.get("path") and not args.get("file_path"):
            args = {**args, "file_path": args["path"]}
        return _tool_step(_TOOL_NAMES.get(kind, kind), args, cwd)
    return None


# Вид tool_call в ACP → вид cursor stream-json (а он уже → имя инструмента claude).
_ACP_KINDS = {"execute": "shell", "read": "read", "edit": "edit", "delete": "delete",
              "search": "grep", "fetch": "webFetch"}


def _acp_tool_step(kind: str, title: str, raw: dict, cwd: str) -> str:
    """Строка журнала для tool_call ACP; без rawInput — заголовок от CLI."""
    if raw.get("providerIdentifier"):
        return _tool_step(f"mcp__{raw['providerIdentifier']}__{raw.get('toolName') or ''}",
                          raw.get("args"), cwd)
    mapped = _ACP_KINDS.get(kind)
    if mapped and raw:
        if raw.get("path") and not raw.get("file_path"):
            raw = {**raw, "file_path": raw["path"]}
        return _tool_step(_TOOL_NAMES.get(mapped, mapped), raw, cwd)
    return f"🔧 {title or kind or 'tool'}"


def _format_error(error: object) -> str:
    if not isinstance(error, dict):
        return str(error)
    text = str(error.get("message") or "")
    data = error.get("data")
    if isinstance(data, dict) and data.get("message"):
        text = f"{text}: {data['message']}" if text else str(data["message"])
    return text or json.dumps(error, ensure_ascii=False)[:1500]
