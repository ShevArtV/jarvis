#!/usr/bin/env python3
"""Jarvis Manager MCP server — read-only tools for the Manager agent.

Stage 1 ships two tools:
- `manager_topics`: list every topic Jarvis knows about (sessions table).
- `manager_inbox`: read the messages_log for one topic.

The server is intentionally read-only at this stage. Create/send tools land in
later stages once the bot grows the matching ingest plumbing.

Wired into each engine (claude/codex/opencode) by `engines/jarvis_mcp.py`.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime
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


def _telegram_token() -> str:
    """Read TELEGRAM_TOKEN from env or the bot's .env file.

    The bot already loads .env via python-dotenv; the MCP server runs
    standalone, so we duplicate that lookup here. .env path is resolved
    relative to JARVIS_DB_PATH's parent so a non-default DB also works.
    """
    token = os.environ.get("TELEGRAM_TOKEN")
    if token:
        return token
    if _DB_PATH is not None:
        env_path = _DB_PATH.parent / ".env"
        if env_path.exists():
            for line in env_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                if k.strip() == "TELEGRAM_TOKEN":
                    return v.strip().strip('"').strip("'")
    raise RuntimeError("TELEGRAM_TOKEN not found (env / .env)")


def _default_chat_id() -> int:
    """Resolve chat_id for create-topic when caller omits it.

    Priority: JARVIS_MANAGER_CHAT_ID env → single most-frequent chat_id in
    sessions. Errors if the bot serves multiple chats and no env was set.
    """
    raw = os.environ.get("JARVIS_MANAGER_CHAT_ID")
    if raw:
        try:
            return int(raw)
        except ValueError as exc:
            raise RuntimeError(f"JARVIS_MANAGER_CHAT_ID is not int: {raw!r}") from exc
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
            "set JARVIS_MANAGER_CHAT_ID"
        )
    if len(rows) > 1:
        raise RuntimeError(
            "multiple forum chats present; pass chat_id explicitly or set "
            "JARVIS_MANAGER_CHAT_ID"
        )
    return rows[0]["chat_id"]


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
        "session_id": row["session_id"],
        "updated_at": row["updated_at"],
        "last_message_at": last.get(key),
    }


mcp = FastMCP(
    name="jarvis-manager",
    instructions=(
        "Read-only access to Jarvis bot state for the Manager agent. "
        "Use manager_topics to see every active topic and its cwd/engine. "
        "Use manager_inbox to read recent user/bot messages for a topic."
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
            "session_id, updated_at FROM sessions ORDER BY updated_at DESC"
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
        "kept, context of the previous engine is NOT transferred. Use this "
        "BEFORE manager_send to delegate the next task under a different "
        "engine/model. `model` must be one of the engine's selectable models "
        "(see manager_engines); pass null to clear and use the engine's "
        "default."
    ),
)
def manager_set_engine(
    thread_id: int,
    engine: str,
    model: str | None = None,
    chat_id: int | None = None,
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
    with _connect() as conn:
        conn.execute(
            "UPDATE sessions SET engine = ?, model = ?, session_id = ?, "
            "updated_at = ? WHERE chat_id = ? AND thread_id = ?",
            (engine, model, new_session_id, now, target_chat_id, thread_id),
        )

    logger.info(
        "switched topic chat=%s thread=%s: %s/%s -> %s/%s (new sid=%s)",
        target_chat_id, thread_id, prev["engine"], prev["model"],
        engine, model, new_session_id,
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
            "session_id": new_session_id,
        },
        "warning": (
            "Context of the previous engine is NOT transferred. The next "
            "manager_send / user message starts a fresh session under the "
            "new engine."
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


@mcp.tool(
    name="manager_send",
    description=(
        "Send a message to a topic. With as_user=True (default) the message is "
        "enqueued as a job — the bot picks it up, runs it through the LLM "
        "pipeline (as if a Telegram user wrote it), and posts the reply back "
        "into the topic. With as_user=False the text is delivered as a plain "
        "bot message (notice / FYI), no LLM cycle. Use manager_inbox after a "
        "few seconds (or longer for non-trivial work) to read the reply."
    ),
)
def manager_send(
    thread_id: int,
    text: str,
    chat_id: int | None = None,
    as_user: bool = True,
) -> dict[str, Any]:
    """Queue or deliver a message into the topic."""
    text = text.strip()
    if not text:
        raise ValueError("text is required")
    target_chat_id = chat_id if chat_id is not None else _default_chat_id()

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

    now = datetime.utcnow().isoformat()
    if as_user:
        with _connect() as conn:
            cur = conn.execute(
                "INSERT INTO jobs(chat_id, thread_id, text, source, status, "
                "created_at) VALUES (?, ?, ?, 'manager', 'pending', ?)",
                (target_chat_id, thread_id, text, now),
            )
            job_id = cur.lastrowid
            conn.execute(
                "INSERT INTO messages_log(chat_id, thread_id, direction, kind, "
                "text, telegram_message_id, ts) VALUES "
                "(?, ?, 'in', 'manager_inject', ?, NULL, ?)",
                (target_chat_id, thread_id, text, now),
            )
        logger.info(
            "queued job %s for chat=%s thread=%s (%d chars)",
            job_id, target_chat_id, thread_id, len(text),
        )
        return {
            "mode": "as_user",
            "job_id": job_id,
            "chat_id": target_chat_id,
            "thread_id": thread_id,
            "queued_at": now,
            "engine": row["engine"],
            "cwd": row["cwd"],
            "message": (
                "Job queued. Poll manager_inbox (since=queued_at, "
                "direction='out') to see the bot's reply."
            ),
        }

    # as_user=False — just deliver a bot message, no LLM trigger.
    params: dict[str, Any] = {
        "chat_id": target_chat_id,
        "text": text,
        "message_thread_id": thread_id,
    }
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
