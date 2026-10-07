"""Сборка вызова ``claude`` CLI и разбор его assistant-событий — общее для
разового и живого режима."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
from pathlib import Path

from engines.tool_steps import _tool_step

logger = logging.getLogger(__name__)


CLAUDE_BIN = os.environ.get("CLAUDE_BIN", "claude")
CLAUDE_TIMEOUT = int(os.environ.get("CLAUDE_TIMEOUT", "3600"))
# Claude Code implements its native Grep tool with bundled ugrep.  A pathological
# regex against minified assets previously exhausted host memory, so Jarvis uses
# Bash/rg for searches instead.
CLAUDE_DISALLOWED_TOOLS = ["Grep"]

APPEND_SYSTEM_PROMPT = (
    "Если нужно отправить пользователю файл (скриншот, собранный пакет, "
    "сгенерированный документ и т.п.) — выведи отдельной строкой маркер "
    "[[FILE: /абсолютный/путь]] (опционально с подписью через '|': "
    "[[FILE: /путь | подпись]]). Бот автоматически отправит файл в Telegram. "
    "Используй только для файлов в пределах cwd сессии или явно указанных пользователем."
)

def _system_prompt_file(text: str) -> str:
    """Файл для --append-system-prompt-file. Многострочный текст в argv ломается
    на Windows (cmd.exe) и упирается в лимит длины аргумента. Имя — хеш
    содержимого: одинаковый промпт пишется один раз."""
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
    path = Path(tempfile.gettempdir()) / "jarvis-prompts" / f"{digest}.txt"
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(f".{os.getpid()}.tmp")
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, path)
    return str(path)


def _mcp_config_flags(mcp_playwright: bool, mcp_topic_role: str | None) -> list[str]:
    """``--mcp-config <inline-json>`` для per-invocation MCP-серверов.

    Без ``--strict-mcp-config`` — конфиг аддитивен к глобальному Manager MCP.
    Пустой список, если дополнительных серверов нет.
    """
    servers: dict[str, dict] = {}

    if mcp_playwright:
        from engines.playwright_mcp import playwright_command_args, playwright_server_name

        spec = playwright_command_args()
        if spec is None:
            logger.warning("mcp_playwright requested but Playwright globally disabled")
        else:
            npx, args = spec
            servers[playwright_server_name()] = {
                "type": "stdio",
                "command": npx,
                "args": args,
            }

    if mcp_topic_role:
        from engines.topic_mcp import claude_mcp_servers

        servers.update(claude_mcp_servers(mcp_topic_role))

    if not servers:
        return []
    config = {"mcpServers": servers}
    return ["--mcp-config", json.dumps(config, ensure_ascii=False)]

def _accumulate_assistant_event(
    ev: dict, buffer_intermediate: list[str], cwd: str | None = None,
) -> None:
    """Извлечь шаги журнала (текст/tool_use) из assistant-события
    stream-json. Общая логика для одноразового ``call_stream`` и живого
    ``PersistentClaudeWorker`` — событие одно и то же в обоих режимах.

    Блоки thinking пропускаются: Claude Code отдаёт их с пустым текстом,
    строка «размышляет…» в журнале ничего не сообщала."""
    msg = ev.get("message", {}) or {}
    for block in msg.get("content", []) or []:
        btype = block.get("type")
        if btype == "text":
            txt = (block.get("text") or "").strip()
            if txt:
                buffer_intermediate.append(txt[:800])
        elif btype == "tool_use":
            buffer_intermediate.append(
                _tool_step(block.get("name", "?"), block.get("input"), cwd)
            )

def _claude_command(
    session_flags: list[str],
    model: str | None,
    system_prefix: str | None,
    mcp_playwright: bool,
    mcp_topic_role: str | None,
    input_format: str = "text",
) -> list[str]:
    """argv claude. ``input_format`` — "text" для разового вызова (prompt в
    stdin целиком), "stream-json" для живого процесса /persistent."""
    model_flags = ["--model", model] if model else []

    # Системный канал claude: FILE-маркер + (опц.) общий [SYSTEM:]-блок.
    # Уходит как system-сообщение каждый ход — кешируется и НЕ копится в
    # транскрипте (в отличие от старой схемы, где блок вшивался в prompt).
    append_system = APPEND_SYSTEM_PROMPT
    if system_prefix:
        append_system = f"{system_prefix}\n\n{APPEND_SYSTEM_PROMPT}"

    # Per-topic MCP injection via --mcp-config (аддитивно к глобальному
    # Manager MCP, без --strict-mcp-config).
    mcp_flags = _mcp_config_flags(mcp_playwright, mcp_topic_role)

    return [
        CLAUDE_BIN, "--print",
        "--permission-mode", "bypassPermissions",
        "--disallowedTools", *CLAUDE_DISALLOWED_TOOLS,
        "--input-format", input_format,
        "--output-format", "stream-json",
        "--verbose",
        "--append-system-prompt-file", _system_prompt_file(append_system),
        *mcp_flags,
        *model_flags,
        *session_flags,
    ]
