#!/usr/bin/env python3
"""Jarvis Manager MCP server — cross-topic and controlled external tools.

The server exposes Jarvis state, reminders and selected integrations to the
Secretary/Manager. Tool descriptions explicitly mark external write actions;
the role instructions require an explicit operator request before using them.

Wired into each engine (claude/codex/opencode) by `engines/jarvis_mcp.py`.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta
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


def _connect() -> sqlite3.Connection:
    if _DB_PATH is None:
        raise RuntimeError("MCP server is not initialised (no --db)")
    conn = sqlite3.connect(_DB_PATH, isolation_level=None)
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


def _activecollab_client():
    """Build an ActiveCollab client from the bot environment without logging secrets."""
    url = _env_or_dotenv("ACTIVE_COLLAB_URL")
    token = _env_or_dotenv("ACTIVE_COLLAB_TOKEN")
    if not url or not token:
        raise RuntimeError(
            "ActiveCollab is not configured: set ACTIVE_COLLAB_URL and "
            "ACTIVE_COLLAB_TOKEN in Jarvis .env"
        )
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from integrations.activecollab import ActiveCollabClient  # type: ignore
    return ActiveCollabClient(url, token)


def _activecollab_check_updates() -> dict[str, Any]:
    """Fetch the user's ActiveCollab delta and persist the observed IDs."""
    client = _activecollab_client()
    all_tasks = client.my_tasks(include_completed=True)
    open_tasks = [task for task in all_tasks if not task.get("is_completed")]
    task_ids = {
        task["id"] for task in all_tasks if isinstance(task.get("id"), int)
    }
    task_by_id = {
        task["id"]: task for task in all_tasks if isinstance(task.get("id"), int)
    }
    comment_notifications = client.comment_notifications_for(task_ids)
    for notification in comment_notifications:
        task = task_by_id.get(notification.get("task_id"))
        if task is not None:
            notification["project_id"] = task.get("project_id")
            notification["project_name"] = task.get("project_name")
            notification["task_name"] = task.get("name")
    now = datetime.utcnow().isoformat()

    with _connect() as conn:
        initialized = conn.execute(
            "SELECT 1 FROM integration_sync_state "
            "WHERE integration='activecollab' AND state_key='updates_initialized'"
        ).fetchone() is not None

        new_tasks: list[dict[str, Any]] = []
        for task in open_tasks:
            task_id = task.get("id")
            if not isinstance(task_id, int):
                continue
            cur = conn.execute(
                "INSERT OR IGNORE INTO integration_seen_items "
                "(integration, kind, item_id, seen_at) VALUES "
                "('activecollab', 'task', ?, ?)",
                (task_id, now),
            )
            if initialized and cur.rowcount == 1:
                new_tasks.append(task)

        new_comments: list[dict[str, Any]] = []
        for notification in comment_notifications:
            notification_id = notification.get("id")
            if not isinstance(notification_id, int):
                continue
            cur = conn.execute(
                "INSERT OR IGNORE INTO integration_seen_items "
                "(integration, kind, item_id, seen_at) VALUES "
                "('activecollab', 'comment_notification', ?, ?)",
                (notification_id, now),
            )
            if initialized and cur.rowcount == 1:
                new_comments.append(notification)

        conn.execute(
            "INSERT INTO integration_sync_state "
            "(integration, state_key, state_value, updated_at) VALUES "
            "('activecollab', 'updates_initialized', '1', ?) "
            "ON CONFLICT(integration, state_key) DO UPDATE SET "
            "state_value=excluded.state_value, updated_at=excluded.updated_at",
            (now,),
        )

    return {
        "initial_check": not initialized,
        "open_tasks": open_tasks if not initialized else [],
        "new_tasks": new_tasks,
        "new_comment_notifications": new_comments,
    }


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


def _teamlead_thread_id() -> int:
    """Teamlead topic thread_id; falls back to Secretary until configured."""
    try:
        return _thread_id_from_env(("JARVIS_TEAMLEAD_THREAD_ID",), "Teamlead")
    except RuntimeError:
        return _secretary_thread_id()


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
    """Match engine adapters: claude uses raw UUID, codex/opencode placeholders."""
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


mcp = FastMCP(
    name="jarvis-manager",
    instructions=(
        "Access to Jarvis bot state and controlled Manager actions. "
        "Use manager_topics to see every active topic and its cwd/engine. "
        "Use manager_inbox to read recent user/bot messages for a topic. "
        "ActiveCollab tools marked as external writes require an explicit "
        "operator request for the exact task and action."
    ),
)


@mcp.tool(
    name="manager_topics",
    description=(
        "List Jarvis topics (one row per Telegram thread). Returns chat_id, "
        "thread_id, title, cwd, engine, model, session_id, updated_at, "
        "last_message_at. Filter by cwd substring (case-insensitive) or engine "
        "to narrow the result. Use this before manager_inbox to discover "
        "thread_id."
    ),
)
def manager_topics(
    cwd_contains: str | None = None,
    engine: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Return all topics the bot knows about."""
    with _connect() as conn:
        rows = conn.execute(
            "SELECT chat_id, thread_id, topic_title, cwd, engine, model, "
            "actual_model, session_id, updated_at FROM sessions "
            "ORDER BY updated_at DESC"
        ).fetchall()
        last_rows = conn.execute(
            "SELECT chat_id, thread_id, MAX(ts) AS last_ts FROM messages_log "
            "GROUP BY chat_id, thread_id"
        ).fetchall()
    last: dict[tuple[int, int], str] = {
        (r["chat_id"], r["thread_id"]): r["last_ts"] for r in last_rows
    }
    topics = [_row_to_topic(r, last) for r in rows]
    if cwd_contains:
        needle = cwd_contains.lower()
        topics = [t for t in topics if t["cwd"] and needle in t["cwd"].lower()]
    if engine:
        topics = [t for t in topics if (t["engine"] or "").lower() == engine.lower()]
    return {"count": len(topics), "topics": topics[:limit]}


@mcp.tool(
    name="manager_engines",
    description=(
        "List available LLM engines and their selectable models. For each "
        "engine returns: name, bin path, available (CLI in PATH), models. "
        "Use this before manager_set_engine to know what to pick. Codex "
        "models are dynamic (cached from `codex models list`); claude and "
        "opencode models are fixed in the engine adapter."
    ),
)
def manager_engines() -> dict[str, Any]:
    """Snapshot engines + models the bot knows about."""
    import shutil
    # Lazy import — avoids pulling Telegram/asyncio deps at MCP startup if
    # tools never get called.
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from engines import SUPPORTED_ENGINES, get_engine_by_name  # type: ignore

    out: list[dict[str, Any]] = []
    for name in SUPPORTED_ENGINES:
        try:
            eng = get_engine_by_name(name)
            bin_path = eng.bin_path
            resolved = shutil.which(bin_path) or bin_path
            available = shutil.which(bin_path) is not None
            models = list(eng.models)
        except Exception as exc:
            out.append({
                "name": name, "available": False,
                "error": str(exc)[:300], "models": [],
            })
            continue
        out.append({
            "name": name,
            "bin": bin_path,
            "resolved_bin": resolved,
            "available": available,
            "models": models,
        })
    return {"engines": out}


@mcp.tool(
    name="manager_set_engine",
    description=(
        "Switch a topic's engine (and optionally model). Same semantics as the "
        "bot's /engine command: NEW session_id under the new engine, cwd is "
        "kept, context of the previous engine is NOT transferred by default. "
        "Use this BEFORE manager_send to delegate the next task under a "
        "different engine/model. `model` must be one of the engine's "
        "selectable models (see manager_engines); pass null to clear and use "
        "the engine's default.\n\n"
        "Pass transfer_context=True to request a summary-based handoff: a "
        "JSON marker is stored in pending_summary; the bot resolves it "
        "on the next message/job by asking the old engine for a dialogue "
        "summary. This adds latency to the first response after the switch."
    ),
)
def manager_set_engine(
    thread_id: int,
    engine: str,
    model: str | None = None,
    chat_id: int | None = None,
    transfer_context: bool = False,
) -> dict[str, Any]:
    """Persistently switch the topic to a different engine/model."""
    engine = engine.strip().lower()
    if engine not in {"claude", "codex", "opencode"}:
        raise ValueError(f"engine must be claude|codex|opencode, got {engine!r}")
    target_chat_id = chat_id if chat_id is not None else _default_chat_id()

    # Lazy import — same reason as manager_engines.
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from engines import get_engine_by_name  # type: ignore
    import shutil

    try:
        eng = get_engine_by_name(engine)
    except Exception as exc:
        raise RuntimeError(f"engine {engine!r} unavailable: {exc}") from exc

    if shutil.which(eng.bin_path) is None:
        raise RuntimeError(
            f"engine {engine!r} binary {eng.bin_path!r} not found in PATH"
        )
    if model is not None and eng.models and model not in eng.models:
        raise ValueError(
            f"model {model!r} is not in {engine}.models={eng.models}; "
            "pass null to clear or pick one from manager_engines"
        )

    with _connect() as conn:
        row = conn.execute(
            "SELECT cwd, engine, model, session_id, topic_title FROM sessions "
            "WHERE chat_id = ? AND thread_id = ?",
            (target_chat_id, thread_id),
        ).fetchone()
    if row is None:
        raise RuntimeError(
            f"topic chat_id={target_chat_id} thread_id={thread_id} is not "
            "tracked — call manager_create_topic first."
        )

    prev = {
        "engine": row["engine"],
        "model": row["model"],
        "session_id": row["session_id"],
    }
    new_session_id = _new_session_id(engine)
    now = datetime.utcnow().isoformat()

    # Если переключаемся на тот же движок (только модель) — контекст сохраняется
    # автоматически (тот же session_id). transfer_context игнорируем.
    if engine == prev["engine"]:
        with _connect() as conn:
            conn.execute(
                "UPDATE sessions SET model = ?, updated_at = ? "
                "WHERE chat_id = ? AND thread_id = ?",
                (model, now, target_chat_id, thread_id),
            )
        logger.info(
            "switched model in-place topic chat=%s thread=%s: %s -> %s",
            target_chat_id, thread_id, prev["model"], model,
        )
        return {
            "chat_id": target_chat_id,
            "thread_id": thread_id,
            "title": row["topic_title"],
            "cwd": row["cwd"],
            "previous": prev,
            "current": {
                "engine": engine,
                "model": model,
                "session_id": prev["session_id"],
            },
            "warning": None,
        }

    with _connect() as conn:
        conn.execute(
            "UPDATE sessions SET engine = ?, model = ?, session_id = ?, "
            "updated_at = ? WHERE chat_id = ? AND thread_id = ?",
            (engine, model, new_session_id, now, target_chat_id, thread_id),
        )
        if transfer_context:
            marker = json.dumps({
                "transfer_requested": True,
                "old_engine": prev["engine"],
                "old_session_id": prev["session_id"],
                "old_model": prev["model"],
            })
            conn.execute(
                "UPDATE sessions SET pending_summary = ? "
                "WHERE chat_id = ? AND thread_id = ?",
                (marker, target_chat_id, thread_id),
            )

    logger.info(
        "switched topic chat=%s thread=%s: %s/%s -> %s/%s (new sid=%s, transfer=%s)",
        target_chat_id, thread_id, prev["engine"], prev["model"],
        engine, model, new_session_id, transfer_context,
    )
    result: dict[str, Any] = {
        "chat_id": target_chat_id,
        "thread_id": thread_id,
        "title": row["topic_title"],
        "cwd": row["cwd"],
        "previous": prev,
        "current": {
            "engine": engine,
            "model": model,
            "session_id": new_session_id,
        },
    }
    if transfer_context:
        result["warning"] = (
            "transfer_context=True: a summary marker was stored. The bot will "
            "ask the old engine for a dialogue summary on the next message/job "
            "in this topic (adds latency to the first response)."
        )
    else:
        result["warning"] = (
            "Context of the previous engine is NOT transferred. The next "
            "manager_send / user message starts a fresh session under the "
            "new engine."
        )
    return result


@mcp.tool(
    name="manager_set_browser",
    description=(
        "Toggle browser support (Playwright MCP) for a topic. Same semantics "
        "as the bot's /browser command. Playwright is OFF by default and "
        "on-demand: ~30 browser_* tools are loaded into the engine's context "
        "ONLY for topics where it's enabled, so leave it off unless the next "
        "task actually needs a browser. Takes effect on the next message/job "
        "in that topic; the session and its context are kept (no reset). Call "
        "this BEFORE manager_send when delegating a browser task."
    ),
)
def manager_set_browser(
    thread_id: int,
    enabled: bool,
    chat_id: int | None = None,
) -> dict[str, Any]:
    """Persistently set the topic's mcp_playwright flag."""
    target_chat_id = chat_id if chat_id is not None else _default_chat_id()
    now = datetime.utcnow().isoformat()
    with _connect() as conn:
        row = conn.execute(
            "SELECT cwd, engine, topic_title, mcp_playwright FROM sessions "
            "WHERE chat_id = ? AND thread_id = ?",
            (target_chat_id, thread_id),
        ).fetchone()
        if row is None:
            raise RuntimeError(
                f"topic chat_id={target_chat_id} thread_id={thread_id} is not "
                "tracked — call manager_create_topic first."
            )
        previous = bool(row["mcp_playwright"])
        conn.execute(
            "UPDATE sessions SET mcp_playwright = ?, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ?",
            (1 if enabled else 0, now, target_chat_id, thread_id),
        )
    logger.info(
        "browser toggled topic chat=%s thread=%s: %s -> %s",
        target_chat_id, thread_id, previous, enabled,
    )
    return {
        "chat_id": target_chat_id,
        "thread_id": thread_id,
        "title": row["topic_title"],
        "cwd": row["cwd"],
        "engine": row["engine"],
        "browser": "on" if enabled else "off",
        "previous": "on" if previous else "off",
        "note": (
            "Takes effect on the next message/job; session context is kept."
        ),
    }


@mcp.tool(
    name="manager_create_topic",
    description=(
        "Create a new Telegram forum topic and bind it to a project. Picks a "
        "free icon color from Telegram's 6-color palette automatically. If a "
        "topic already exists for the same cwd, returns it (existed=true) "
        "instead of duplicating — pass force=true to override and create "
        "anyway. Engine must be one of: claude, codex, opencode. The bot "
        "ignores topics that don't appear in the sessions table until the "
        "first user message — this tool seeds the row so subsequent "
        "manager_send and on-topic messages bind to the right cwd/engine "
        "from the start."
    ),
)
def manager_create_topic(
    title: str,
    cwd: str,
    engine: str = "claude",
    model: str | None = None,
    chat_id: int | None = None,
    icon_color: int | None = None,
    icon_custom_emoji_id: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Create a forum topic and seed the sessions row."""
    title = title.strip()
    if not title:
        raise ValueError("title is required")
    if not cwd or not cwd.startswith("/"):
        raise ValueError("cwd must be an absolute path")
    engine = engine.strip().lower()
    if engine not in {"claude", "codex", "opencode"}:
        raise ValueError(f"engine must be claude|codex|opencode, got {engine!r}")
    if icon_color is not None and icon_color not in _ICON_COLORS:
        raise ValueError(
            f"icon_color must be one of {_ICON_COLORS} (Telegram palette)"
        )

    target_chat_id = chat_id if chat_id is not None else _default_chat_id()

    with _connect() as conn:
        if not force:
            existing = _find_topic_by_cwd(conn, target_chat_id, cwd)
            if existing is not None:
                return {
                    "existed": True,
                    "chat_id": existing["chat_id"],
                    "thread_id": existing["thread_id"],
                    "title": existing["topic_title"],
                    "cwd": existing["cwd"],
                    "engine": existing["engine"],
                    "model": existing["model"],
                    "icon_color": existing["topic_icon_color"],
                    "icon_color_name": _ICON_NAMES.get(
                        existing["topic_icon_color"] or 0
                    ),
                    "message": (
                        "Topic already bound to this cwd; not creating a "
                        "duplicate. Pass force=true to override."
                    ),
                }
        color = icon_color if icon_color is not None else _pick_icon_color(
            conn, target_chat_id,
        )

    params: dict[str, Any] = {
        "chat_id": target_chat_id,
        "name": title[:128],
        "icon_color": color,
    }
    if icon_custom_emoji_id:
        params["icon_custom_emoji_id"] = icon_custom_emoji_id
    result = _telegram_api("createForumTopic", params)
    thread_id = int(result["message_thread_id"])
    color_actual = int(result.get("icon_color", color))

    session_id = _new_session_id(engine)
    now = datetime.utcnow().isoformat()
    with _connect() as conn:
        conn.execute(
            "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, "
            "model, topic_title, topic_icon_color, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(chat_id, thread_id) DO UPDATE SET "
            "session_id=excluded.session_id, cwd=excluded.cwd, "
            "engine=excluded.engine, model=excluded.model, "
            "topic_title=excluded.topic_title, "
            "topic_icon_color=excluded.topic_icon_color, "
            "updated_at=excluded.updated_at",
            (
                target_chat_id, thread_id, session_id, cwd, engine, model,
                title, color_actual, now,
            ),
        )

    logger.info(
        "created topic chat=%s thread=%s title=%r engine=%s color=%s",
        target_chat_id, thread_id, title, engine, color_actual,
    )
    return {
        "existed": False,
        "chat_id": target_chat_id,
        "thread_id": thread_id,
        "title": result.get("name", title),
        "cwd": cwd,
        "engine": engine,
        "model": model,
        "session_id": session_id,
        "icon_color": color_actual,
        "icon_color_name": _ICON_NAMES.get(color_actual),
        "icon_custom_emoji_id": result.get("icon_custom_emoji_id"),
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


@mcp.tool(
    name="manager_send",
    description=(
        "Send a message to a topic.\n"
        "as_user=True (default): enqueues a job — the bot runs it through "
        "the LLM pipeline. With delay_seconds>0 the job is SCHEDULED to fire "
        "later (auto-go pattern: «делай по плану через 10 мин если оператор "
        "не вмешался»). Any subsequent immediate as_user=True send to the "
        "SAME thread_id automatically cancels still-pending scheduled jobs "
        "in that topic — so a fresh correction supersedes the timer "
        "without manual cleanup.\n"
        "as_user=False: plain sendMessage (notice / FYI), no LLM cycle. Use "
        "this for short notifications to other topics (e.g. «вопрос #ask_N "
        "в топике X — загляни»).\n"
        "parse_mode='HTML' renders <b>, <i> and <a href=\"URL\">text</a> — use it "
        "when publishing formatted content (news digest and the like), so links "
        "hide behind words instead of showing raw URLs. Only with as_user=False. "
        "If Telegram rejects the markup, the message is re-sent as plain text "
        "rather than lost.\n"
        "ALWAYS pass origin_thread_id=<your own thread_id> (it is stated in your "
        "[SYSTEM:] block as «Твой топик: chat_id=…, thread_id=…») when "
        "as_user=True. The bot reports the answer, the heartbeat warnings and "
        "the interrupt notice for that job back to this topic. Omit it and the "
        "notice falls back to the Teamlead topic, which then wakes up and "
        "interferes with a task that is not his."
    ),
)
def manager_send(
    thread_id: int,
    text: str,
    chat_id: int | None = None,
    as_user: bool = True,
    delay_seconds: int = 0,
    parse_mode: str | None = None,
    origin_thread_id: int | None = None,
    origin_chat_id: int | None = None,
) -> dict[str, Any]:
    """Queue or deliver a message into the topic."""
    text = text.strip()
    if not text:
        raise ValueError("text is required")
    if delay_seconds < 0:
        raise ValueError("delay_seconds must be >= 0")
    if delay_seconds > 0 and not as_user:
        raise ValueError("delay_seconds only makes sense with as_user=True")
    target_chat_id = chat_id if chat_id is not None else _default_chat_id()
    # Топик-инициатор: сюда бот вернёт нотис об ответе на этот job. Сервер
    # общий на все топики и вызывающего сам не знает — его передаёт агент.
    if origin_thread_id is not None and origin_chat_id is None:
        origin_chat_id = _default_chat_id()

    with _connect() as conn:
        row = conn.execute(
            "SELECT thread_id, cwd, engine FROM sessions "
            "WHERE chat_id = ? AND thread_id = ?",
            (target_chat_id, thread_id),
        ).fetchone()
    if row is None:
        raise RuntimeError(
            f"topic chat_id={target_chat_id} thread_id={thread_id} is not "
            "tracked — call manager_create_topic first or pass an existing "
            "thread_id (use manager_topics to list)."
        )

    now_dt = datetime.utcnow()
    now = now_dt.isoformat()
    if as_user:
        not_before: str | None = None
        if delay_seconds > 0:
            not_before = (now_dt + timedelta(seconds=delay_seconds)).isoformat()
        cancelled_ids: list[int] = []
        with _connect() as conn:
            if delay_seconds == 0:
                # Immediate send → auto-cancel still-pending scheduled jobs
                # for the same topic. Fresh decision supersedes the timer.
                cancelled = conn.execute(
                    "UPDATE jobs SET status='cancelled', finished_at=?, "
                    "error='superseded by new manager_send' "
                    "WHERE chat_id=? AND thread_id=? AND status='pending' "
                    "AND not_before IS NOT NULL AND not_before > ? "
                    "RETURNING id",
                    (now, target_chat_id, thread_id, now),
                ).fetchall()
                cancelled_ids = [r[0] for r in cancelled]
            if _jobs_has_origin_columns(conn):
                cur = conn.execute(
                    "INSERT INTO jobs(chat_id, thread_id, text, source, status, "
                    "created_at, not_before, origin_chat_id, origin_thread_id) "
                    "VALUES (?, ?, ?, 'manager', 'pending', ?, ?, ?, ?)",
                    (
                        target_chat_id, thread_id, text, now, not_before,
                        origin_chat_id if origin_thread_id is not None else None,
                        origin_thread_id,
                    ),
                )
            else:
                logger.warning(
                    "jobs.origin_* missing — restart the bot to migrate; "
                    "notice for this job falls back to the service role"
                )
                cur = conn.execute(
                    "INSERT INTO jobs(chat_id, thread_id, text, source, status, "
                    "created_at, not_before) "
                    "VALUES (?, ?, ?, 'manager', 'pending', ?, ?)",
                    (target_chat_id, thread_id, text, now, not_before),
                )
            job_id = cur.lastrowid
            conn.execute(
                "INSERT INTO messages_log(chat_id, thread_id, direction, kind, "
                "text, telegram_message_id, ts) VALUES "
                "(?, ?, 'in', 'manager_inject', ?, NULL, ?)",
                (target_chat_id, thread_id, text, now),
            )
        logger.info(
            "queued job %s for chat=%s thread=%s delay=%ss cancelled=%s",
            job_id, target_chat_id, thread_id, delay_seconds, cancelled_ids,
        )
        return {
            "mode": "scheduled" if delay_seconds > 0 else "as_user",
            "job_id": job_id,
            "chat_id": target_chat_id,
            "thread_id": thread_id,
            "queued_at": now,
            "not_before": not_before,
            "delay_seconds": delay_seconds,
            "cancelled_scheduled": cancelled_ids,
            "engine": row["engine"],
            "cwd": row["cwd"],
            "origin_chat_id": origin_chat_id if origin_thread_id is not None else None,
            "origin_thread_id": origin_thread_id,
        }

    # as_user=False — just deliver a bot message, no LLM trigger.
    params: dict[str, Any] = {
        "chat_id": target_chat_id,
        "text": text,
        "message_thread_id": thread_id,
    }
    if parse_mode:
        params["parse_mode"] = parse_mode
    try:
        result = _telegram_api("sendMessage", params)
    except RuntimeError:
        # Кривая разметка не должна съедать сообщение целиком: у агента может
        # не сойтись тег, и тогда Telegram отвергает весь текст. Лучше отдать
        # содержимое как есть, чем потерять его.
        if not parse_mode:
            raise
        logger.warning("manager_send: %s markup rejected, retrying as plain text",
                       parse_mode)
        params.pop("parse_mode")
        result = _telegram_api("sendMessage", params)
    msg_id = int(result["message_id"])
    with _connect() as conn:
        conn.execute(
            "INSERT INTO messages_log(chat_id, thread_id, direction, kind, "
            "text, telegram_message_id, ts) VALUES "
            "(?, ?, 'out', 'manager_notice', ?, ?, ?)",
            (target_chat_id, thread_id, text, msg_id, now),
        )
    logger.info(
        "delivered notice chat=%s thread=%s message_id=%s",
        target_chat_id, thread_id, msg_id,
    )
    return {
        "mode": "notice",
        "chat_id": target_chat_id,
        "thread_id": thread_id,
        "telegram_message_id": msg_id,
        "sent_at": now,
    }


@mcp.tool(
    name="manager_cancel_job",
    description=(
        "Cancel a still-pending job (typically a scheduled auto-go that the "
        "operator wants to abort entirely without sending a replacement). "
        "Returns cancelled=true if the job was actually flipped, false if "
        "it was already in_progress / done / cancelled. Note: for the "
        "common case «оператор передумал, шлёт корректировку», just call "
        "manager_send again — it auto-cancels pending scheduled jobs for "
        "the same topic."
    ),
)
def manager_cancel_job(job_id: int) -> dict[str, Any]:
    """Cancel a single pending job by id."""
    now = datetime.utcnow().isoformat()
    with _connect() as conn:
        row = conn.execute(
            "SELECT status, chat_id, thread_id, not_before FROM jobs WHERE id = ?",
            (job_id,),
        ).fetchone()
        if row is None:
            raise RuntimeError(f"job {job_id} not found")
        prev_status = row["status"]
        cur = conn.execute(
            "UPDATE jobs SET status='cancelled', finished_at=?, "
            "error='cancelled via manager_cancel_job' "
            "WHERE id=? AND status='pending'",
            (now, job_id),
        )
    cancelled = cur.rowcount == 1
    return {
        "job_id": job_id,
        "cancelled": cancelled,
        "previous_status": prev_status,
        "chat_id": row["chat_id"],
        "thread_id": row["thread_id"],
        "not_before": row["not_before"],
    }


@mcp.tool(
    name="manager_remind_add",
    description=(
        "Создаёт напоминание для Секретаря (старое имя manager_* сохранено "
        "для совместимости). Формат schedule (простой текст):\n"
        "  daily HH:MM              - каждый день\n"
        "  weekday HH:MM            - Пн-Пт\n"
        "  weekend HH:MM            - Сб-Вс\n"
        "  weekly DAY[,DAY,...] HH:MM   (DAY: mon|tue|wed|thu|fri|sat|sun)\n"
        "  monthly D HH:MM          - конкретное число (1..28)\n"
        "  once YYYY-MM-DD HH:MM    - one-time\n"
        "Все времена в Europe/Moscow (можно переопределить через "
        "JARVIS_REMINDERS_TZ). В назначенное время бот шлёт в топик "
        "Секретаря '🔔 Напоминание #N: <text>' и активирует Секретаря "
        "через auto-kick. Возвращает id, next_fire_at."
    ),
)
def manager_remind_add(
    text: str,
    schedule: str,
    thread_id: int | None = None,
    chat_id: int | None = None,
) -> dict[str, Any]:
    """Add a reminder."""
    text = text.strip()
    if not text:
        raise ValueError("text is required")
    if not schedule.strip():
        raise ValueError("schedule is required")
    target_chat_id = chat_id if chat_id is not None else _default_chat_id()
    if thread_id is None:
        thread_id = _manager_thread_id()

    # Парсим schedule и считаем next_fire_at через python из bot-модуля
    # (там же логика TZ). Импортируем lazy.
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from bot.reminders import compute_next_fire, parse_reminder_schedule  # type: ignore

    parsed = parse_reminder_schedule(schedule)
    next_fire = compute_next_fire(parsed)
    if next_fire is None:
        raise ValueError(
            f"schedule {schedule!r} resolves to a past moment "
            "(once-reminder with past date?)"
        )

    now = datetime.utcnow().isoformat()
    with _connect() as conn:
        cur = conn.execute(
            "INSERT INTO reminders(chat_id, thread_id, text, schedule, "
            "next_fire_at, enabled, created_at) VALUES (?, ?, ?, ?, ?, 1, ?)",
            (
                target_chat_id, thread_id, text, schedule,
                next_fire.isoformat(), now,
            ),
        )
        rid = cur.lastrowid
    logger.info(
        "reminder created id=%s schedule=%r next=%s",
        rid, schedule, next_fire.isoformat(),
    )
    return {
        "id": rid,
        "chat_id": target_chat_id,
        "thread_id": thread_id,
        "text": text,
        "schedule": schedule,
        "next_fire_at": next_fire.isoformat(),
        "enabled": True,
    }


@mcp.tool(
    name="manager_remind_list",
    description=(
        "Список напоминаний. По умолчанию только enabled=true; задай "
        "only_enabled=false чтобы увидеть и отключённые (выполненные once, "
        "или те, которые ты отключил через manager_remind_toggle)."
    ),
)
def manager_remind_list(
    only_enabled: bool = True,
    chat_id: int | None = None,
) -> dict[str, Any]:
    """List reminders."""
    target_chat_id = chat_id if chat_id is not None else _default_chat_id()
    sql = (
        "SELECT id, chat_id, thread_id, text, schedule, next_fire_at, "
        "last_fired_at, enabled, created_at FROM reminders "
        "WHERE chat_id = ?"
    )
    args: list[Any] = [target_chat_id]
    if only_enabled:
        sql += " AND enabled = 1"
    sql += " ORDER BY next_fire_at ASC"
    with _connect() as conn:
        rows = conn.execute(sql, args).fetchall()
    reminders = []
    for r in rows:
        reminders.append({
            "id": r["id"],
            "chat_id": r["chat_id"],
            "thread_id": r["thread_id"],
            "text": r["text"],
            "schedule": r["schedule"],
            "next_fire_at": r["next_fire_at"],
            "last_fired_at": r["last_fired_at"],
            "enabled": bool(r["enabled"]),
            "created_at": r["created_at"],
        })
    return {"count": len(reminders), "reminders": reminders}


@mcp.tool(
    name="manager_activecollab_my_tasks",
    description=(
        "Возвращает задачи, назначенные пользователю токена ActiveCollab. "
        "По умолчанию только незавершённые; каждая запись содержит проект и "
        "текущую стадию (task list). Только чтение."
    ),
)
def manager_activecollab_my_tasks(include_completed: bool = False) -> dict[str, Any]:
    """List tasks assigned to the authenticated ActiveCollab user."""
    tasks = _activecollab_client().my_tasks(include_completed=include_completed)
    return {"count": len(tasks), "tasks": tasks}


@mcp.tool(
    name="manager_activecollab_task",
    description=(
        "Читает задачу ActiveCollab и её комментарии. Нужны project_id и task_id; "
        "бери их из manager_activecollab_my_tasks или ссылки на задачу. Только чтение."
    ),
)
def manager_activecollab_task(project_id: int, task_id: int) -> dict[str, Any]:
    """Return one task, including its comments and subscribers."""
    return _activecollab_client().task(project_id, task_id)


@mcp.tool(
    name="manager_activecollab_stages",
    description=(
        "Возвращает допустимые стадии задачи (task lists) проекта ActiveCollab. "
        "Используй перед переносом, если целевая стадия не названа точно. Только чтение."
    ),
)
def manager_activecollab_stages(project_id: int) -> dict[str, Any]:
    """List the project task lists used as workflow stages."""
    stages = _activecollab_client().stages(project_id)
    return {"count": len(stages), "stages": stages}


@mcp.tool(
    name="manager_activecollab_job_types",
    description=(
        "Возвращает доступные типы работ ActiveCollab для трекинга времени. "
        "Только чтение."
    ),
)
def manager_activecollab_job_types() -> dict[str, Any]:
    """List ActiveCollab job types available to the current user."""
    job_types = _activecollab_client().job_types()
    return {"count": len(job_types), "job_types": job_types}


@mcp.tool(
    name="manager_activecollab_check_updates",
    description=(
        "Проверяет новые незавершённые задачи пользователя и новые уведомления "
        "о комментариях к его задачам. Запоминает уже увиденные ID в локальной "
        "SQLite БД: первый вызов возвращает стартовую сводку, следующие — только "
        "дельту. Этот tool предназначен для reminder-чеклиста Секретаря."
    ),
)
def manager_activecollab_check_updates() -> dict[str, Any]:
    """Return new ActiveCollab tasks/comment notifications since the last check."""
    return _activecollab_check_updates()


@mcp.tool(
    name="manager_activecollab_add_comment",
    description=(
        "Создаёт комментарий к задаче ActiveCollab. ВНЕШНЕЕ WRITE-ДЕЙСТВИЕ: "
        "вызывай только после явного одобрения оператора с конкретной задачей "
        "и текстом комментария."
    ),
)
def manager_activecollab_add_comment(task_id: int, body: str) -> dict[str, Any]:
    """Post an explicitly approved comment to an ActiveCollab task."""
    return _activecollab_client().add_comment(task_id, body)


@mcp.tool(
    name="manager_activecollab_track_time",
    description=(
        "Добавляет учёт времени к задаче ActiveCollab. ВНЕШНЕЕ WRITE-ДЕЙСТВИЕ: "
        "вызывай только после явного одобрения оператора с задачей, длительностью, "
        "датой и типом работы. value: например '1:30' или '1.5'; record_date: YYYY-MM-DD."
    ),
)
def manager_activecollab_track_time(
    project_id: int,
    task_id: int,
    value: str,
    record_date: str,
    job_type_id: int,
    summary: str | None = None,
    billable_status: int | None = None,
) -> dict[str, Any]:
    """Create an explicitly approved ActiveCollab time record."""
    return _activecollab_client().track_time(
        project_id, task_id, value, record_date, job_type_id, summary, billable_status,
    )


@mcp.tool(
    name="manager_activecollab_move_task",
    description=(
        "Переносит задачу ActiveCollab на стадию по её точному названию, например "
        "'В работе' или 'Можно тестировать'. ВНЕШНЕЕ WRITE-ДЕЙСТВИЕ: вызывай "
        "только после явного одобрения оператора с конкретной задачей и стадией."
    ),
)
def manager_activecollab_move_task(
    project_id: int,
    task_id: int,
    stage_name: str,
) -> dict[str, Any]:
    """Move a task to an explicitly approved workflow stage."""
    return _activecollab_client().move_to_stage_name(project_id, task_id, stage_name)


@mcp.tool(
    name="manager_remind_delete",
    description="Удаляет напоминание по id. Возвращает deleted=true/false.",
)
def manager_remind_delete(reminder_id: int) -> dict[str, Any]:
    """Delete a reminder by id."""
    with _connect() as conn:
        cur = conn.execute(
            "DELETE FROM reminders WHERE id = ?", (reminder_id,),
        )
    return {"reminder_id": reminder_id, "deleted": cur.rowcount == 1}


@mcp.tool(
    name="manager_remind_toggle",
    description=(
        "Включает/выключает напоминание. enabled=false — не сработает, "
        "но запись остаётся (полезно для временной паузы). enabled=true — "
        "пересчитывает next_fire_at от текущего момента."
    ),
)
def manager_remind_toggle(reminder_id: int, enabled: bool) -> dict[str, Any]:
    """Toggle a reminder on/off."""
    with _connect() as conn:
        row = conn.execute(
            "SELECT schedule FROM reminders WHERE id = ?", (reminder_id,),
        ).fetchone()
    if row is None:
        raise RuntimeError(f"reminder {reminder_id} not found")
    if enabled:
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from bot.reminders import compute_next_fire, parse_reminder_schedule  # type: ignore
        try:
            parsed = parse_reminder_schedule(row["schedule"])
            next_fire = compute_next_fire(parsed)
        except Exception as exc:
            raise RuntimeError(
                f"cannot re-enable: schedule unparseable / past: {exc}"
            ) from exc
        if next_fire is None:
            raise RuntimeError(
                "cannot re-enable: schedule resolves to past (once-reminder?)"
            )
        with _connect() as conn:
            conn.execute(
                "UPDATE reminders SET enabled = 1, next_fire_at = ? WHERE id = ?",
                (next_fire.isoformat(), reminder_id),
            )
        return {
            "reminder_id": reminder_id, "enabled": True,
            "next_fire_at": next_fire.isoformat(),
        }
    else:
        with _connect() as conn:
            conn.execute(
                "UPDATE reminders SET enabled = 0 WHERE id = ?", (reminder_id,),
            )
        return {"reminder_id": reminder_id, "enabled": False}


@mcp.tool(
    name="manager_interrupt",
    description=(
        "Останавливает активный исполнитель LLM в указанном топике. "
        "Используется, когда агент работает подозрительно долго / явно "
        "пошёл не туда. Под капотом: выставляет cancel_requested в jobs "
        "для всех in_progress job'ов этого thread_id; bot-watcher через "
        "~2с увидит флаг и убьёт subprocess. После прерывания можно "
        "сразу прислать новый manager_send(as_user=True) с уточняющим "
        "вопросом — агент resume в той же сессии и увидит контекст до "
        "прерывания. Возвращает interrupted_jobs — id'ы задач, "
        "которым выставлен флаг (пусто если в топике сейчас нет "
        "активных)."
    ),
)
def manager_interrupt(
    thread_id: int,
    chat_id: int | None = None,
) -> dict[str, Any]:
    """Set cancel_requested for active jobs in the given topic."""
    target_chat_id = chat_id if chat_id is not None else _default_chat_id()
    now = datetime.utcnow().isoformat()
    with _connect() as conn:
        rows = conn.execute(
            "UPDATE jobs SET cancel_requested = ? "
            "WHERE chat_id = ? AND thread_id = ? AND status = 'in_progress' "
            "RETURNING id",
            (now, target_chat_id, thread_id),
        ).fetchall()
    interrupted_jobs = [r[0] for r in rows]
    logger.info(
        "manager_interrupt chat=%s thread=%s -> jobs=%s",
        target_chat_id, thread_id, interrupted_jobs,
    )
    return {
        "chat_id": target_chat_id,
        "thread_id": thread_id,
        "interrupted_jobs": interrupted_jobs,
        "requested_at": now,
        "message": (
            f"Cancel-flag set for {len(interrupted_jobs)} job(s). "
            "Bot watcher will terminate the subprocess within ~2s."
            if interrupted_jobs
            else "No in_progress jobs in this topic right now."
        ),
    }


@mcp.tool(
    name="manager_close_session",
    description=(
        "Закрывает сеанс в топике — программный аналог команды /close. "
        "Сеанс = окно терминала: закрытие сбрасывает контекст движка, но "
        "сам топик (cwd, engine, model, флаги браузера/persistent) "
        "сохраняется, и следующее сообщение / manager_send откроет новый "
        "сеанс с чистой историей. Используй, когда контекст топика "
        "распух или протух: агент тянет старую задачу, путается в "
        "отменённых договорённостях, ест токены на ненужной истории. "
        "По умолчанию (interrupt_active=True) сначала прерывает активные "
        "job'ы топика, как manager_interrupt — иначе идущая задача "
        "продолжала бы писать в закрытый сеанс. Живой процесс "
        "/persistent и запущенный subprocess убивает бот: он видит "
        "флаг close_requested в течение ~2с, после чего пишет в топик, "
        "что сеанс закрыт. Возвращает was_open (был ли сеанс открыт) и "
        "interrupted_jobs."
    ),
)
def manager_close_session(
    thread_id: int,
    chat_id: int | None = None,
    interrupt_active: bool = True,
) -> dict[str, Any]:
    """Close a topic's session: reset engine context, keep the topic."""
    target_chat_id = chat_id if chat_id is not None else _default_chat_id()
    now = datetime.utcnow().isoformat()

    with _connect() as conn:
        _ensure_close_requested_column(conn)
        row = conn.execute(
            "SELECT cwd, engine, model, topic_title, last_activity_at "
            "FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (target_chat_id, thread_id),
        ).fetchone()
        if row is None:
            raise RuntimeError(
                f"topic chat_id={target_chat_id} thread_id={thread_id} is not "
                "tracked — nothing to close (use manager_topics to list)."
            )

        interrupted_jobs: list[int] = []
        if interrupt_active:
            jobs = conn.execute(
                "UPDATE jobs SET cancel_requested = ? "
                "WHERE chat_id = ? AND thread_id = ? AND status = 'in_progress' "
                "RETURNING id",
                (now, target_chat_id, thread_id),
            ).fetchall()
            interrupted_jobs = [r[0] for r in jobs]

        # last_activity_at = NULL — это и есть «сеанс закрыт» (session_id
        # NOT NULL, его обнулить нельзя; новый создастся лениво при следующем
        # сообщении). close_requested — сигнал боту добить процессы топика.
        conn.execute(
            "UPDATE sessions SET last_activity_at = NULL, close_requested = ?, "
            "updated_at = ? WHERE chat_id = ? AND thread_id = ?",
            (now, now, target_chat_id, thread_id),
        )

    was_open = row["last_activity_at"] is not None
    logger.info(
        "manager_close_session chat=%s thread=%s was_open=%s jobs=%s",
        target_chat_id, thread_id, was_open, interrupted_jobs,
    )
    return {
        "chat_id": target_chat_id,
        "thread_id": thread_id,
        "title": row["topic_title"],
        "cwd": row["cwd"],
        "engine": row["engine"],
        "model": row["model"],
        "was_open": was_open,
        "interrupted_jobs": interrupted_jobs,
        "requested_at": now,
        "message": (
            (
                "Session closed. "
                if was_open
                else "Session was already closed; request registered anyway. "
            )
            + (
                f"Interrupted {len(interrupted_jobs)} in_progress job(s). "
                if interrupted_jobs
                else ""
            )
            + "The bot kills the topic's live processes within ~2s and posts a "
              "notice there. Topic settings (cwd/engine/model) are kept; the "
              "next message starts a fresh session."
        ),
    }


# Топики, которые нельзя ни свернуть, ни удалить: General (у него нет своего
# message_thread_id — 0/1 адресуют корень форума) и топик Менеджера, иначе
# оркестратор заглушил бы сам себя, и вернуть его было бы некому.
def _guard_topic_admin(chat_id: int, thread_id: int, action: str) -> None:
    if thread_id <= 1:
        raise RuntimeError(
            f"thread_id={thread_id} is the forum's General topic — {action} "
            "would hit the whole chat, not a topic. Refusing."
        )
    # Через _env_or_dotenv, а не os.environ: MCP-сервер поднимается отдельным
    # процессом и окружение бота наследует не всегда. Читай guard только из
    # переменных — он бы молча не сработал там, где .env есть, а env пуст,
    # и топик Менеджера удалялся бы как обычный.
    raw_manager = _env_or_dotenv("JARVIS_MANAGER_THREAD_ID")
    if raw_manager and raw_manager.strip().isdigit():
        if int(raw_manager) == thread_id:
            raise RuntimeError(
                f"thread_id={thread_id} is the Manager's own topic — {action} "
                "would cut off the orchestrator. Refusing."
            )


async def _await_bot_close(
    chat_id: int, thread_id: int, timeout: float = 10.0, poll: float = 1.0,
) -> bool:
    """Закрыть сеанс топика и дождаться, пока бот это исполнит.

    MCP-процесс не видит active_procs / persistent_workers — их знает только
    бот, поэтому единственный канал — close_requested в sessions. Ждём, пока
    close_requests_worker погасит флаг: до этого момента процессы топика ещё
    живы, и удалять топик в Telegram рано — осиротевший процесс пошёл бы
    писать в несуществующий тред.

    True — бот подтвердил; False — не дождались (бот не запущен / занят).
    """
    manager_close_session(
        thread_id=thread_id, chat_id=chat_id, interrupt_active=True,
    )
    deadline = time.monotonic() + max(timeout, 0.0)
    while True:
        with _connect() as conn:
            row = conn.execute(
                "SELECT close_requested FROM sessions "
                "WHERE chat_id = ? AND thread_id = ?",
                (chat_id, thread_id),
            ).fetchone()
        if row is None or row["close_requested"] is None:
            return True
        if time.monotonic() >= deadline:
            return False
        await asyncio.sleep(poll)


# Топик уже мог быть удалён руками в Telegram — тогда строка в sessions
# осиротела, и чистка БД как раз то, что нужно. Отличаем этот случай от
# «нет прав» / «бот не админ»: те обязаны прерывать операцию.
_GONE_MARKERS = (
    "thread not found",
    "topic_id_invalid",
    "message thread not found",
    "topic_deleted",
    "chat not found",
)


def _telegram_topic_gone(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(marker in text for marker in _GONE_MARKERS)


@mcp.tool(
    name="manager_archive_topic",
    description=(
        "Свернуть топик в Telegram (closeForumTopic) — мягкая архивация: "
        "история и настройки топика остаются, писать в него нельзя, в списке "
        "он уходит в свёрнутые. Это ДЕФОЛТНЫЙ способ убрать временный топик "
        "после финальной стадии задачи: в отличие от manager_delete_topic "
        "операция обратима — reopen=True разворачивает топик обратно "
        "(reopenForumTopic).\n\n"
        "Перед сворачиванием сеанс топика закрывается, как manager_close_"
        "session, и инструмент ждёт (до wait_seconds), пока бот добьёт живые "
        "процессы — иначе идущая задача продолжала бы писать в закрытый "
        "топик. Строка в sessions сохраняется: cwd/engine/model на месте, "
        "после reopen топик работает как раньше.\n\n"
        "Отказ на топике Менеджера и на General."
    ),
)
async def manager_archive_topic(
    thread_id: int,
    chat_id: int | None = None,
    reopen: bool = False,
    wait_seconds: float = 10.0,
) -> dict[str, Any]:
    """Свернуть (или развернуть обратно) форум-топик, сохранив его состояние."""
    target_chat_id = chat_id if chat_id is not None else _default_chat_id()
    _guard_topic_admin(target_chat_id, thread_id, "reopen" if reopen else "archive")

    with _connect() as conn:
        row = conn.execute(
            "SELECT cwd, engine, model, topic_title FROM sessions "
            "WHERE chat_id = ? AND thread_id = ?",
            (target_chat_id, thread_id),
        ).fetchone()
    if row is None:
        raise RuntimeError(
            f"topic chat_id={target_chat_id} thread_id={thread_id} is not "
            "tracked — nothing to archive (use manager_topics to list)."
        )

    if reopen:
        _telegram_api(
            "reopenForumTopic",
            {"chat_id": target_chat_id, "message_thread_id": thread_id},
        )
        logger.info(
            "manager_archive_topic reopened chat=%s thread=%s",
            target_chat_id, thread_id,
        )
        return {
            "chat_id": target_chat_id,
            "thread_id": thread_id,
            "title": row["topic_title"],
            "cwd": row["cwd"],
            "engine": row["engine"],
            "state": "open",
            "session_closed": False,
            "bot_confirmed": None,
            "message": (
                "Topic reopened. Its session stays closed — the next message "
                "or manager_send starts a fresh one."
            ),
        }

    bot_confirmed = await _await_bot_close(
        target_chat_id, thread_id, timeout=wait_seconds,
    )
    _telegram_api(
        "closeForumTopic",
        {"chat_id": target_chat_id, "message_thread_id": thread_id},
    )
    logger.info(
        "manager_archive_topic archived chat=%s thread=%s bot_confirmed=%s",
        target_chat_id, thread_id, bot_confirmed,
    )
    return {
        "chat_id": target_chat_id,
        "thread_id": thread_id,
        "title": row["topic_title"],
        "cwd": row["cwd"],
        "engine": row["engine"],
        "model": row["model"],
        "state": "closed",
        "session_closed": True,
        "bot_confirmed": bot_confirmed,
        "message": (
            "Topic archived (folded in Telegram); history and settings kept. "
            + (
                ""
                if bot_confirmed
                else "WARNING: the bot did not confirm the session close in "
                     "time — it may be down, and the topic's live processes "
                     "may still be running. "
            )
            + "Call again with reopen=true to unfold it."
        ),
    }


@mcp.tool(
    name="manager_delete_topic",
    description=(
        "УДАЛИТЬ форум-топик вместе со всеми его сообщениями "
        "(deleteForumTopic) и убрать его состояние из БД бота. "
        "НЕОБРАТИМО: Telegram не умеет восстанавливать удалённый топик. "
        "Для штатного завершения временного топика предпочитай "
        "manager_archive_topic — он обратим; удаляй только когда топик "
        "действительно больше не нужен и оператор этого хочет.\n\n"
        "Порядок: сеанс закрывается, инструмент ждёт (до wait_seconds), пока "
        "бот добьёт живые процессы топика, и только потом удаляет тред — "
        "иначе процесс пережил бы топик и писал в пустоту. Затем чистит БД: "
        "строку sessions, напоминания топика, а pending job'ы, триггеры и "
        "вопросы ask_user переводит в cancelled, чтобы они не выстрелили в "
        "несуществующий тред. messages_log по умолчанию СОХРАНЯЕТСЯ (это "
        "переписка, её подчистит cleanup_worker по TTL); purge_log=true "
        "удаляет и его.\n\n"
        "Отказ на топике Менеджера и на General, а также если в топике есть "
        "in_progress job или бот не подтвердил закрытие — обойти можно "
        "force=true, но тогда убедись, что бот запущен и топик не в работе. "
        "Если топик уже удалён руками в Telegram, инструмент всё равно "
        "почистит осиротевшее состояние в БД."
    ),
)
async def manager_delete_topic(
    thread_id: int,
    chat_id: int | None = None,
    purge_log: bool = False,
    force: bool = False,
    wait_seconds: float = 10.0,
) -> dict[str, Any]:
    """Delete a forum topic and drop the bot state bound to it."""
    target_chat_id = chat_id if chat_id is not None else _default_chat_id()
    _guard_topic_admin(target_chat_id, thread_id, "deletion")

    with _connect() as conn:
        row = conn.execute(
            "SELECT cwd, engine, model, topic_title FROM sessions "
            "WHERE chat_id = ? AND thread_id = ?",
            (target_chat_id, thread_id),
        ).fetchone()
        if row is None:
            raise RuntimeError(
                f"topic chat_id={target_chat_id} thread_id={thread_id} is not "
                "tracked — nothing to delete (use manager_topics to list)."
            )
        busy = [
            r["id"] for r in conn.execute(
                "SELECT id FROM jobs WHERE chat_id = ? AND thread_id = ? "
                "AND status = 'in_progress'",
                (target_chat_id, thread_id),
            ).fetchall()
        ]
    if busy and not force:
        raise RuntimeError(
            f"topic thread_id={thread_id} has in_progress job(s) {busy} — "
            "the task would die mid-run. Wait for it, or pass force=true if "
            "the topic is meant to go away anyway."
        )

    bot_confirmed = await _await_bot_close(
        target_chat_id, thread_id, timeout=wait_seconds,
    )
    if not bot_confirmed and not force:
        raise RuntimeError(
            f"the bot did not confirm the session close within {wait_seconds}s "
            "— it may be down, and this topic's live processes would outlive "
            "the topic. Start the bot and retry, or pass force=true to delete "
            "anyway."
        )

    telegram_deleted = True
    warning: str | None = None
    try:
        _telegram_api(
            "deleteForumTopic",
            {"chat_id": target_chat_id, "message_thread_id": thread_id},
        )
    except RuntimeError as exc:
        if not _telegram_topic_gone(exc):
            raise
        telegram_deleted = False
        warning = (
            f"Telegram says the topic is already gone ({exc}); cleaned up the "
            "orphaned state in the DB anyway."
        )
        logger.info("manager_delete_topic: topic already gone (%s)", exc)

    now = datetime.utcnow().isoformat()
    with _connect() as conn:
        cancelled_jobs = [
            r[0] for r in conn.execute(
                "UPDATE jobs SET status='cancelled', finished_at=?, "
                "error='topic deleted' "
                "WHERE chat_id=? AND thread_id=? AND status='pending' "
                "RETURNING id",
                (now, target_chat_id, thread_id),
            ).fetchall()
        ]
        try:
            cancelled_triggers = [
                r[0] for r in conn.execute(
                    "UPDATE agent_triggers SET status='cancelled', finished_at=?, "
                    "error='topic deleted' "
                    "WHERE chat_id=? AND thread_id=? AND status='pending' "
                    "RETURNING id",
                    (now, target_chat_id, thread_id),
                ).fetchall()
            ]
        except sqlite3.OperationalError:
            # Старая БД без agent_triggers — интеграций с трекером тут просто нет.
            cancelled_triggers = []
        cancelled_asks = [
            r[0] for r in conn.execute(
                "UPDATE ask_requests SET status='cancelled', answered_at=? "
                "WHERE chat_id=? AND thread_id=? AND status='pending' "
                "RETURNING id",
                (now, target_chat_id, thread_id),
            ).fetchall()
        ]
        deleted_reminders = conn.execute(
            "DELETE FROM reminders WHERE chat_id=? AND thread_id=?",
            (target_chat_id, thread_id),
        ).rowcount
        deleted_log = 0
        if purge_log:
            deleted_log = conn.execute(
                "DELETE FROM messages_log WHERE chat_id=? AND thread_id=?",
                (target_chat_id, thread_id),
            ).rowcount
        conn.execute(
            "DELETE FROM sessions WHERE chat_id=? AND thread_id=?",
            (target_chat_id, thread_id),
        )

    logger.info(
        "manager_delete_topic chat=%s thread=%s title=%r telegram_deleted=%s "
        "jobs=%s triggers=%s asks=%s reminders=%s log=%s",
        target_chat_id, thread_id, row["topic_title"], telegram_deleted,
        cancelled_jobs, cancelled_triggers, cancelled_asks,
        deleted_reminders, deleted_log,
    )
    return {
        "chat_id": target_chat_id,
        "thread_id": thread_id,
        "title": row["topic_title"],
        "cwd": row["cwd"],
        "engine": row["engine"],
        "telegram_deleted": telegram_deleted,
        "bot_confirmed": bot_confirmed,
        "forced": bool(force),
        "interrupted_jobs": busy,
        "cancelled_jobs": cancelled_jobs,
        "cancelled_triggers": cancelled_triggers,
        "cancelled_asks": cancelled_asks,
        "deleted_reminders": deleted_reminders,
        "deleted_log_rows": deleted_log,
        "log_kept": not purge_log,
        "warning": warning,
        "message": (
            "Topic deleted and its bot state removed. "
            + ("Message log kept (cleanup_worker prunes it by TTL). "
               if not purge_log else "Message log purged too. ")
            + "This cannot be undone — recreate with manager_create_topic if "
              "the project needs a topic again."
        ),
    }


@mcp.tool(
    name="manager_dismiss_notice",
    description=(
        "Delete a notice/message from Telegram by its message_id. Use after "
        "you've read and processed a notification (e.g. «#ask_N» or "
        "«#job_N ✅») to keep the Manager topic tidy. Bot API restricts "
        "deleting messages older than 48h or from a different bot — failures "
        "are returned as ok=false with the API error, not raised."
    ),
)
def manager_dismiss_notice(
    message_id: int,
    chat_id: int | None = None,
) -> dict[str, Any]:
    """Delete one message via Telegram Bot API."""
    target_chat_id = chat_id if chat_id is not None else _default_chat_id()
    params = {"chat_id": target_chat_id, "message_id": message_id}
    try:
        _telegram_api("deleteMessage", params)
        ok = True
        err = None
    except Exception as exc:
        ok = False
        err = str(exc)[:300]
    return {
        "ok": ok,
        "chat_id": target_chat_id,
        "message_id": message_id,
        "error": err,
    }


@mcp.tool(
    name="manager_wait_reply",
    description=(
        "Wait until the bot posts a reply into the topic after a given "
        "timestamp. Use this right after manager_send(as_user=True): pass "
        "the `queued_at` value as `since`. Returns the bot reply text when it "
        "appears, or {timed_out: true} after `timeout_seconds`. If `job_id` "
        "is given, also short-circuits on job failure. Polls every "
        "`poll_interval` seconds (default 3)."
    ),
)
async def manager_wait_reply(
    thread_id: int,
    since: str,
    chat_id: int | None = None,
    timeout_seconds: int = 300,
    poll_interval: float = 3.0,
    job_id: int | None = None,
    text_limit: int = 4000,
) -> dict[str, Any]:
    """Block until a bot reply newer than `since` appears (or timeout)."""
    if timeout_seconds <= 0 or timeout_seconds > 1800:
        timeout_seconds = 300
    if poll_interval < 1.0:
        poll_interval = 1.0
    if poll_interval > 30.0:
        poll_interval = 30.0
    if text_limit <= 0 or text_limit > 20000:
        text_limit = 4000
    target_chat_id = chat_id if chat_id is not None else _default_chat_id()

    deadline = time.monotonic() + timeout_seconds
    last_seen_ts: str | None = None
    while True:
        with _connect() as conn:
            row = conn.execute(
                "SELECT id, kind, direction, text, telegram_message_id, ts "
                "FROM messages_log WHERE chat_id = ? AND thread_id = ? "
                "AND ts > ? AND direction = 'out' "
                "AND kind IN ('bot_reply', 'spawn_reply') "
                "ORDER BY ts ASC, id ASC LIMIT 1",
                (target_chat_id, thread_id, since),
            ).fetchone()
            if row is not None:
                last_seen_ts = row["ts"]
                body = row["text"] or ""
                return {
                    "status": "reply",
                    "chat_id": target_chat_id,
                    "thread_id": thread_id,
                    "message_id": row["id"],
                    "kind": row["kind"],
                    "ts": row["ts"],
                    "telegram_message_id": row["telegram_message_id"],
                    "text": body[:text_limit],
                    "truncated": len(body) > text_limit,
                }
            if job_id is not None:
                job_row = conn.execute(
                    "SELECT status, error, result_message_id FROM jobs WHERE id = ?",
                    (job_id,),
                ).fetchone()
                if job_row is not None and job_row["status"] == "failed":
                    return {
                        "status": "job_failed",
                        "chat_id": target_chat_id,
                        "thread_id": thread_id,
                        "job_id": job_id,
                        "error": job_row["error"],
                    }
        if time.monotonic() >= deadline:
            return {
                "status": "timed_out",
                "chat_id": target_chat_id,
                "thread_id": thread_id,
                "since": since,
                "timeout_seconds": timeout_seconds,
                "last_seen_ts": last_seen_ts,
            }
        await asyncio.sleep(poll_interval)


_TASK_TAG_RE = re.compile(r"#[A-Za-z0-9][A-Za-z0-9._-]*")


def _external_executor_task(chat_id: int, thread_id: int) -> tuple[str, str] | None:
    """Внешняя задача, по которой топик прямо сейчас работает как ИСПОЛНИТЕЛЬ.

    Возвращает ``(тег, source)`` — тег задачи ('#123', или '#?' если тег не
    распознан) и метку интеграции, поднявшей ход. None — обычный ход
    (сообщение человека, триггер Менеджеру, роль неизвестна).

    Роль пишет интегратор в agent_triggers.role; на любой source, а не только
    на доску — контракт «исполнителю вопросы в трекер, не в чат» общий.
    NULL/нет колонки → None: гард не должен включаться вслепую на
    неперезапущенном стеке.
    """
    try:
        with _connect() as conn:
            row = conn.execute(
                "SELECT text, source FROM agent_triggers "
                "WHERE chat_id = ? AND thread_id = ? "
                "AND status = 'in_progress' AND role = 'executor' "
                "ORDER BY id DESC LIMIT 1",
                (chat_id, thread_id),
            ).fetchone()
    except sqlite3.OperationalError:
        logger.warning("agent_triggers.role is missing — external ask_user guard is off")
        return None
    if row is None:
        return None
    match = _TASK_TAG_RE.search(row["text"] or "")
    return (match.group(0) if match else "#?"), (row["source"] or "external")


def _ask_keyboard(ask_id: int, options: list[str]) -> dict[str, Any]:
    """Inline-клавиатура вариантов. В callback_data идёт ИНДЕКС, а не текст:
    Telegram ограничивает callback_data 64 байтами."""
    return {
        "inline_keyboard": [
            [{"text": opt[:64], "callback_data": f"ask:{ask_id}:{idx}"}]
            for idx, opt in enumerate(options)
        ]
    }


@mcp.tool(
    name="ask_user",
    description=(
        "Ask the human a question in his Telegram topic and BLOCK until he "
        "answers. Use this whenever you need a decision only he can make: "
        "before destructive or irreversible actions (deleting files, DROP/"
        "DELETE, force-push, anything on production), when the task is "
        "ambiguous and guessing would waste work, or when you must choose "
        "between options with real trade-offs.\n\n"
        "Pass `options` to render tappable buttons — prefer this over free-form "
        "questions, it is much faster for him to answer. He can always reply "
        "with text instead, so keep questions answerable either way.\n\n"
        "Returns {status: 'answered', answer, option_index, via} once he "
        "responds, or {status: 'timed_out', answer: <default>} if he does not "
        "answer within `timeout_seconds`. On timeout do NOT assume consent: "
        "treat it as 'no answer' and stop, unless you passed an explicit "
        "`default`.\n\n"
        "Do not use this for questions you can answer yourself by reading the "
        "code, running a command, or checking git — he is not a lookup service.\n\n"
        "NOT available while you work on an external tracker's task as its "
        "assignee: such a call is rejected with {status: 'blocked'}. There the "
        "whole dialogue belongs in the task itself — post your question as a "
        "task comment and end your turn; his reply wakes you again."
    ),
)
async def ask_user(
    question: str,
    thread_id: int,
    options: list[str] | None = None,
    chat_id: int | None = None,
    timeout_seconds: int = 1800,
    default: str | None = None,
    poll_interval: float = 2.0,
) -> dict[str, Any]:
    """Post a question into the topic and wait for the human's answer."""
    question = (question or "").strip()
    if not question:
        return {"status": "error", "error": "question is empty"}
    if timeout_seconds <= 0:
        timeout_seconds = 1800
    elif timeout_seconds > 3600:
        timeout_seconds = 3600
    poll_interval = min(max(poll_interval, 1.0), 30.0)
    options = [str(o).strip() for o in (options or []) if str(o).strip()][:8]
    target_chat_id = chat_id if chat_id is not None else _default_chat_id()

    # Работаешь по внешней задаче как исполнитель — чат не твой канал: ответ в
    # нём осел бы мимо трекера. Отказ вместо вопроса, ничего не отправляем.
    external = _external_executor_task(target_chat_id, thread_id)
    if external is not None:
        task_tag, source = external
        logger.info(
            "ask_user blocked for %s task %s (thread=%s)", source, task_tag, thread_id,
        )
        return {
            "status": "blocked",
            "task": task_tag,
            "source": source,
            "error": (
                f"Ты работаешь над задачей {task_tag} из внешнего трекера "
                f"({source}) — вопросы в чат отключены. Напиши комментарий в "
                "задаче и заверши ход: ответ придёт следующим триггером. "
                "ОБЯЗАТЕЛЬНО упомяни адресата по логину — «@<логин>» в тексте "
                "комментария: комментарий без упоминания никого не будит, и "
                "вопрос повиснет. Логин постановщика виден в карточке. Перед "
                "опасным действием так же остановись и спроси комментарием, не "
                "делай его на своё усмотрение."
            ),
        }

    now = datetime.utcnow().isoformat()

    with _connect() as conn:
        cur = conn.execute(
            "INSERT INTO ask_requests(chat_id, thread_id, question, options_json, "
            "status, created_at) VALUES (?, ?, ?, ?, 'pending', ?)",
            (
                target_chat_id, thread_id, question,
                json.dumps(options, ensure_ascii=False) if options else None,
                now,
            ),
        )
        ask_id = cur.lastrowid

    body = f"❓ {question}\n\n"
    body += (
        "Выбери вариант или ответь сообщением."
        if options else "Ответь сообщением в этот топик."
    )
    params: dict[str, Any] = {
        "chat_id": target_chat_id,
        "text": body,
        "message_thread_id": thread_id,
    }
    if options:
        params["reply_markup"] = _ask_keyboard(ask_id, options)

    try:
        sent = _telegram_api("sendMessage", params)
    except RuntimeError as exc:
        with _connect() as conn:
            conn.execute(
                "UPDATE ask_requests SET status = 'failed' WHERE id = ?", (ask_id,),
            )
        return {"status": "error", "error": f"failed to post question: {exc}"}

    tg_msg_id = sent.get("message_id")
    with _connect() as conn:
        conn.execute(
            "UPDATE ask_requests SET telegram_message_id = ? WHERE id = ?",
            (tg_msg_id, ask_id),
        )
    # Бот ищет только status='pending', а истёкший вопрос всё равно остаётся
    # последним сообщением топика — messages_log даёт боту его message_id,
    # чтобы отличить «ответ на протухший вопрос» от обычного сообщения.
    try:
        with _connect() as conn:
            conn.execute(
                "INSERT INTO messages_log(chat_id, thread_id, direction, kind, "
                "text, telegram_message_id, ts) VALUES (?, ?, 'out', 'ask_user', ?, ?, ?)",
                (target_chat_id, thread_id, question, tg_msg_id, now),
            )
    except Exception as exc:
        logger.warning("ask_user #%s: failed to log question to messages_log: %s",
                       ask_id, exc)
    logger.info("ask_user #%s posted to thread=%s (options=%d)",
                ask_id, thread_id, len(options))

    deadline = time.monotonic() + timeout_seconds
    while True:
        with _connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT status, answer, option_index, via FROM ask_requests WHERE id = ?",
                (ask_id,),
            ).fetchone()
        if row is not None and row["status"] == "answered":
            logger.info("ask_user #%s answered via %s", ask_id, row["via"])
            return {
                "status": "answered",
                "ask_id": ask_id,
                "answer": row["answer"],
                "option_index": row["option_index"],
                "via": row["via"],
            }
        if time.monotonic() >= deadline:
            break
        await asyncio.sleep(poll_interval)

    # Таймаут: закрываем вопрос, гасим кнопки — чтобы по протухшему нельзя было
    # кликнуть и чтобы следующее сообщение в топик не съелось как «ответ».
    with _connect() as conn:
        conn.execute(
            "UPDATE ask_requests SET status = 'timed_out', answered_at = ? "
            "WHERE id = ? AND status = 'pending'",
            (datetime.utcnow().isoformat(), ask_id),
        )
    if tg_msg_id:
        try:
            _telegram_api("editMessageText", {
                "chat_id": target_chat_id,
                "message_id": tg_msg_id,
                "text": (
                    f"❓ {question}\n\n⌛ Вопрос истёк — ответа не было. "
                    "Можешь всё равно ответить сообщением: передам агенту "
                    "в ближайшие 15 минут."
                ),
                "parse_mode": "HTML",
                "reply_markup": {"inline_keyboard": []},
            })
        except RuntimeError:
            pass
    logger.info("ask_user #%s timed out after %ss", ask_id, timeout_seconds)
    return {
        "status": "timed_out",
        "ask_id": ask_id,
        "answer": default,
        "timeout_seconds": timeout_seconds,
    }


@mcp.tool(
    name="manager_inbox",
    description=(
        "Read the message log for a single topic. Returns user inputs and bot "
        "replies in chronological order (oldest first). Use `since` (ISO 8601 "
        "UTC) to fetch only newer entries; use `limit` to cap the number of "
        "rows. Bodies are truncated to `text_limit` chars per entry to keep "
        "responses small — bump it if you need full text."
    ),
)
def manager_inbox(
    chat_id: int,
    thread_id: int,
    since: str | None = None,
    limit: int = 50,
    text_limit: int = 4000,
    direction: str | None = None,
) -> dict[str, Any]:
    """Read messages for the given topic."""
    if limit <= 0 or limit > 500:
        limit = 50
    if text_limit <= 0 or text_limit > 20000:
        text_limit = 4000
    where = ["chat_id = ?", "thread_id = ?"]
    args: list[Any] = [chat_id, thread_id]
    if since:
        where.append("ts > ?")
        args.append(since)
    if direction in ("in", "out"):
        where.append("direction = ?")
        args.append(direction)
    sql = (
        "SELECT id, direction, kind, text, telegram_message_id, ts "
        "FROM messages_log WHERE " + " AND ".join(where)
        + " ORDER BY ts ASC, id ASC LIMIT ?"
    )
    args.append(limit)
    with _connect() as conn:
        rows = conn.execute(sql, args).fetchall()
    messages = []
    for r in rows:
        body = r["text"] or ""
        truncated = len(body) > text_limit
        messages.append({
            "id": r["id"],
            "direction": r["direction"],
            "kind": r["kind"],
            "ts": r["ts"],
            "telegram_message_id": r["telegram_message_id"],
            "text": body[:text_limit],
            "truncated": truncated,
        })
    return {
        "chat_id": chat_id,
        "thread_id": thread_id,
        "count": len(messages),
        "since": since,
        "messages": messages,
    }


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

    db_path = args.db.expanduser().resolve()
    if not db_path.exists():
        logger.error("DB not found: %s", db_path)
        return 2

    global _DB_PATH
    _DB_PATH = db_path
    logger.info("Jarvis MCP server starting (db=%s)", _DB_PATH)
    mcp.run(transport="stdio")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
