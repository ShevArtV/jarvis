"""Общее состояние и хелперы MCP-сервера Jarvis.

Модули tools/ и plugins/*/mcp_tools.py обращаются к состоянию и патчимым
функциям через модуль (``common._DB_PATH``, ``common._telegram_api``), а не
через ``from ... import``: иначе подмена в тестах не подействовала бы.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

logger = logging.getLogger("jarvis-mcp")

# Filled in main(); module-level so tool functions can close over it without
# FastMCP context plumbing.
_DB_PATH: Path | None = None

# Telegram's fixed forum-icon palette (Bot API docs).
_ICON_COLORS = [7322096, 16766590, 13338331, 9367192, 16749490, 16478047]
_ICON_NAMES = {
    7322096: "light blue",
    16766590: "yellow",
    13338331: "purple",
    9367192: "green",
    16749490: "rose",
    16478047: "red",
}
_PLACEHOLDER_ENGINES = {"codex", "opencode"}


class _ClosingConnection(sqlite3.Connection):
    """Закрывается на выходе из ``with`` (как bot.db.connect): на Windows
    незакрытое соединение держит файл БД."""

    def __exit__(self, *exc):
        try:
            return super().__exit__(*exc)
        finally:
            self.close()


def _connect() -> sqlite3.Connection:
    if _DB_PATH is None:
        raise RuntimeError("MCP server is not initialised (no --db)")
    conn = sqlite3.connect(_DB_PATH, isolation_level=None, factory=_ClosingConnection)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _env_or_dotenv(name: str) -> str | None:
    """Настройка из окружения, иначе из .env бота.

    Бот читает .env через python-dotenv; MCP-сервер запускается отдельно и
    окружение бота наследует не всегда, поэтому смотрим в файл сами. Путь к
    .env — рядом с БД, так что нестандартный --db тоже работает.
    """
    value = os.environ.get(name)
    if value:
        return value
    if _DB_PATH is None:
        return None
    env_path = _DB_PATH.parent / ".env"
    if not env_path.exists():
        return None
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, raw = line.partition("=")
        if key.strip() == name:
            return raw.strip().strip('"').strip("'") or None
    return None


def _telegram_token() -> str:
    """Read TELEGRAM_TOKEN from env or the bot's .env file."""
    token = _env_or_dotenv("TELEGRAM_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_TOKEN not found (env / .env)")
    return token


def _default_chat_id() -> int:
    """Resolve chat_id for create-topic when caller omits it.

    Priority: explicit service-topic env → single most-frequent chat_id in
    sessions. Errors if the bot serves multiple chats and no env was set.
    """
    for name in (
        "JARVIS_SECRETARY_CHAT_ID",
        "JARVIS_MANAGER_CHAT_ID",
        "JARVIS_TEAMLEAD_CHAT_ID",
    ):
        raw = _env_or_dotenv(name)
        if raw:
            try:
                return int(raw)
            except ValueError as exc:
                raise RuntimeError(f"{name} is not int: {raw!r}") from exc
    with _connect() as conn:
        # Forum chats only — private DMs always have thread_id=0 and can't host
        # topics. If the bot serves a single forum chat, that's the answer.
        rows = conn.execute(
            "SELECT chat_id, COUNT(*) AS n FROM sessions WHERE thread_id > 0 "
            "GROUP BY chat_id ORDER BY n DESC"
        ).fetchall()
    if not rows:
        raise RuntimeError(
            "no forum topics in sessions yet — pass chat_id explicitly or "
            "set JARVIS_SECRETARY_CHAT_ID/JARVIS_MANAGER_CHAT_ID"
        )
    if len(rows) > 1:
        raise RuntimeError(
            "multiple forum chats present; pass chat_id explicitly or set "
            "JARVIS_SECRETARY_CHAT_ID/JARVIS_MANAGER_CHAT_ID"
        )
    return rows[0]["chat_id"]


def _thread_id_from_env(names: tuple[str, ...], label: str) -> int:
    for name in names:
        raw = _env_or_dotenv(name)
        if not raw:
            continue
        try:
            return int(raw)
        except ValueError as exc:
            raise RuntimeError(f"{name} is not int: {raw!r}") from exc
    joined = " or ".join(names)
    raise RuntimeError(
        f"{label} topic is not configured; pass thread_id explicitly or set {joined}"
    )


def _secretary_thread_id() -> int:
    """Secretary topic thread_id; old JARVIS_MANAGER_THREAD_ID is its alias."""
    return _thread_id_from_env(
        ("JARVIS_SECRETARY_THREAD_ID", "JARVIS_MANAGER_THREAD_ID"),
        "Secretary",
    )


def _manager_thread_id() -> int:
    """Compatibility alias: old Manager defaults now point to Secretary.

    Until 2026-07-25 this was a SQL lookup for a cwd naming convention private
    to the author, which raised «Manager topic not found» for everyone else.
    """
    return _secretary_thread_id()


def _telegram_api(method: str, params: dict[str, Any]) -> dict[str, Any]:
    """Call Telegram Bot API. Raises with the description on `ok=false`."""
    token = _telegram_token()
    url = f"https://api.telegram.org/bot{token}/{method}"
    payload = json.dumps(params, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url, data=payload,
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        raise RuntimeError(f"Telegram {method} HTTP {exc.code}: {body[:500]}") from exc
    data = json.loads(body)
    if not data.get("ok"):
        raise RuntimeError(
            f"Telegram {method} failed: {data.get('description', body[:500])}"
        )
    return data["result"]


def _ensure_close_requested_column(conn: sqlite3.Connection) -> None:
    """Idempotent-миграция sessions.close_requested на стороне MCP.

    Ту же колонку заводит init_db бота, но порядок запуска не гарантирован:
    MCP-сервер может подняться на БД, которую бот ещё не открывал новой
    версией. ALTER здесь — nullable-колонка, для бота безвредна.
    """
    cols = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
    if cols and "close_requested" not in cols:
        logger.info("adding 'close_requested' column to sessions (mcp side)")
        conn.execute("ALTER TABLE sessions ADD COLUMN close_requested TEXT")


def _new_session_id(engine: str) -> str:
    """Match engine adapters: claude/cursor use raw UUID, codex/opencode placeholders."""
    if engine in _PLACEHOLDER_ENGINES:
        return f"placeholder-{uuid.uuid4()}"
    return str(uuid.uuid4())


def _used_icon_colors(conn: sqlite3.Connection, chat_id: int) -> set[int]:
    rows = conn.execute(
        "SELECT DISTINCT topic_icon_color FROM sessions "
        "WHERE chat_id = ? AND topic_icon_color IS NOT NULL",
        (chat_id,),
    ).fetchall()
    return {r["topic_icon_color"] for r in rows}


def _pick_icon_color(conn: sqlite3.Connection, chat_id: int) -> int:
    """Pick the first palette colour not yet used in this chat; if all are
    used, fall back to the one used in the oldest topic."""
    used = _used_icon_colors(conn, chat_id)
    for color in _ICON_COLORS:
        if color not in used:
            return color
    row = conn.execute(
        "SELECT topic_icon_color FROM sessions "
        "WHERE chat_id = ? AND topic_icon_color IS NOT NULL "
        "ORDER BY updated_at ASC LIMIT 1",
        (chat_id,),
    ).fetchone()
    return row["topic_icon_color"] if row else _ICON_COLORS[0]


def _find_topic_by_cwd(
    conn: sqlite3.Connection, chat_id: int, cwd: str,
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT chat_id, thread_id, topic_title, cwd, engine, model, "
        "session_id, topic_icon_color, updated_at FROM sessions "
        "WHERE chat_id = ? AND cwd = ?",
        (chat_id, cwd),
    ).fetchone()


def _row_to_topic(row: sqlite3.Row, last: dict[tuple[int, int], str]) -> dict[str, Any]:
    key = (row["chat_id"], row["thread_id"])
    return {
        "chat_id": row["chat_id"],
        "thread_id": row["thread_id"],
        "title": row["topic_title"],
        "cwd": row["cwd"],
        "engine": row["engine"],
        "model": row["model"],
        "actual_model": row["actual_model"] if "actual_model" in row.keys() else None,
        "session_id": row["session_id"],
        "updated_at": row["updated_at"],
        "last_message_at": last.get(key),
    }

_JOBS_ORIGIN_COLS: bool | None = None


def _jobs_has_origin_columns(conn) -> bool:
    """Есть ли в jobs колонки origin_* (миграция bot/db.py).

    Сервер и бот — разные процессы: если MCP поднялся на БД, где миграция ещё
    не отработала, INSERT с origin_* уронил бы manager_send целиком. Тогда
    пишем по-старому, а нотис уйдёт по роли.
    """
    global _JOBS_ORIGIN_COLS
    if _JOBS_ORIGIN_COLS is None:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()]
        _JOBS_ORIGIN_COLS = "origin_chat_id" in cols and "origin_thread_id" in cols
    return _JOBS_ORIGIN_COLS


mcp = FastMCP(
    name="jarvis-manager",
    instructions=(
        "Access to Jarvis bot state and controlled Manager actions. "
        "Use manager_topics to see every active topic and its cwd/engine. "
        "Use manager_inbox to read recent user/bot messages for a topic."
    ),
)
