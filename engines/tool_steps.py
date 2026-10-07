"""Строка журнала хода по вызову инструмента: значок + поле input, которое
объясняет шаг. Общая для движков claude и cursor."""

from __future__ import annotations

# Встроенные инструменты: значок + поле input, которое объясняет шаг.
_TOOL_LABELS = {
    "Bash": ("💻", ("description", "command")),
    "Read": ("📖", ("file_path",)),
    "Write": ("📝", ("file_path",)),
    "Edit": ("✏️", ("file_path",)),
    "MultiEdit": ("✏️", ("file_path",)),
    "NotebookEdit": ("✏️", ("notebook_path",)),
    "Grep": ("🔎", ("pattern",)),
    "Glob": ("🔎", ("pattern",)),
    "WebFetch": ("🌐", ("url",)),
    "WebSearch": ("🌐", ("query",)),
    "Agent": ("🤖", ("description",)),
    "Task": ("🤖", ("description",)),
    "Skill": ("📚", ("skill",)),
}
_PATH_KEYS = {"file_path", "notebook_path", "path"}
# MCP: первое непустое поле из списка — то, что отличает один вызов от другого.
_MCP_KEYS = ("question", "query", "text", "message", "url", "file_path", "path",
             "task_id", "thread_id")
_MCP_ICONS = {"ask_user": "❓", "codegraph_explore": "🔎", "kb_explore": "🔎",
              "manager_inbox": "📨", "manager_send": "📨"}


def _short_value(key: str, value: object, cwd: str | None) -> str:
    text = " ".join(str(value).split())
    root = (cwd or "").rstrip("/\\")
    if key in _PATH_KEYS and root and text.startswith(root) and text[len(root):len(root) + 1] in ("/", "\\"):
        text = text[len(root) + 1:]
    return text[:120]


def _tool_step(name: str, inp: object, cwd: str | None = None) -> str:
    """Строка журнала для tool_use: что делается, а не техническое имя."""
    inp = inp if isinstance(inp, dict) else {}
    if name.startswith("mcp__"):
        _, _, rest = name.partition("mcp__")
        server, _, tool = rest.partition("__")
        if tool == "ask_user" and inp.get("question"):
            return f"❓ Спрашиваю: «{_short_value('question', inp['question'], cwd)}»"
        icon = _MCP_ICONS.get(tool, "🔌")
        label = f"{icon} {server} · {tool}" if tool else f"{icon} {server}"
        for key in _MCP_KEYS:
            if inp.get(key) not in (None, ""):
                value = _short_value(key, inp[key], cwd)
                return f"{label} (thread {value})" if key == "thread_id" else f"{label}: {value}"
        return label
    icon, keys = _TOOL_LABELS.get(name, ("🔧", ("command", "file_path", "path", "pattern", "url", "description")))
    for key in keys:
        if inp.get(key):
            value = _short_value(key, inp[key], cwd)
            return f"{icon} {value}" if name in _TOOL_LABELS else f"{icon} {name} {value}"
    return f"{icon} {name}"
