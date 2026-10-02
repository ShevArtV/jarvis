"""MCP-тулы управления топиками: список, движок, браузер, создание,
закрытие сеанса, архивация и удаление."""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
from datetime import datetime
from typing import Any

from mcp_server import common
from mcp_server.common import logger, mcp


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
    with common._connect() as conn:
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
    topics = [common._row_to_topic(r, last) for r in rows]
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
    target_chat_id = chat_id if chat_id is not None else common._default_chat_id()

    # Lazy import — same reason as manager_engines.
    import shutil

    from engines import get_engine_by_name  # type: ignore

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

    with common._connect() as conn:
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
    new_session_id = common._new_session_id(engine)
    now = datetime.utcnow().isoformat()

    # Если переключаемся на тот же движок (только модель) — контекст сохраняется
    # автоматически (тот же session_id). transfer_context игнорируем.
    if engine == prev["engine"]:
        with common._connect() as conn:
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

    with common._connect() as conn:
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
    target_chat_id = chat_id if chat_id is not None else common._default_chat_id()
    now = datetime.utcnow().isoformat()
    with common._connect() as conn:
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
    if not cwd or not os.path.isabs(cwd):
        raise ValueError("cwd must be an absolute path")
    engine = engine.strip().lower()
    if engine not in {"claude", "codex", "opencode"}:
        raise ValueError(f"engine must be claude|codex|opencode, got {engine!r}")
    if icon_color is not None and icon_color not in common._ICON_COLORS:
        raise ValueError(
            f"icon_color must be one of {common._ICON_COLORS} (Telegram palette)"
        )

    target_chat_id = chat_id if chat_id is not None else common._default_chat_id()

    with common._connect() as conn:
        if not force:
            existing = common._find_topic_by_cwd(conn, target_chat_id, cwd)
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
                    "icon_color_name": common._ICON_NAMES.get(
                        existing["topic_icon_color"] or 0
                    ),
                    "message": (
                        "Topic already bound to this cwd; not creating a "
                        "duplicate. Pass force=true to override."
                    ),
                }
        color = icon_color if icon_color is not None else common._pick_icon_color(
            conn, target_chat_id,
        )

    params: dict[str, Any] = {
        "chat_id": target_chat_id,
        "name": title[:128],
        "icon_color": color,
    }
    if icon_custom_emoji_id:
        params["icon_custom_emoji_id"] = icon_custom_emoji_id
    result = common._telegram_api("createForumTopic", params)
    thread_id = int(result["message_thread_id"])
    color_actual = int(result.get("icon_color", color))

    session_id = common._new_session_id(engine)
    now = datetime.utcnow().isoformat()
    with common._connect() as conn:
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
        "icon_color_name": common._ICON_NAMES.get(color_actual),
        "icon_custom_emoji_id": result.get("icon_custom_emoji_id"),
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
    target_chat_id = chat_id if chat_id is not None else common._default_chat_id()
    now = datetime.utcnow().isoformat()

    with common._connect() as conn:
        common._ensure_close_requested_column(conn)
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
    # Через common._env_or_dotenv, а не os.environ: MCP-сервер поднимается отдельным
    # процессом и окружение бота наследует не всегда. Читай guard только из
    # переменных — он бы молча не сработал там, где .env есть, а env пуст,
    # и топик Менеджера удалялся бы как обычный.
    raw_manager = common._env_or_dotenv("JARVIS_MANAGER_THREAD_ID")
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
        with common._connect() as conn:
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
    target_chat_id = chat_id if chat_id is not None else common._default_chat_id()
    _guard_topic_admin(target_chat_id, thread_id, "reopen" if reopen else "archive")

    with common._connect() as conn:
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
        common._telegram_api(
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
    common._telegram_api(
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
    target_chat_id = chat_id if chat_id is not None else common._default_chat_id()
    _guard_topic_admin(target_chat_id, thread_id, "deletion")

    with common._connect() as conn:
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
        common._telegram_api(
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
    with common._connect() as conn:
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
        # reminders — таблица плагина: без него её может не быть.
        deleted_reminders = 0
        if conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='reminders'"
        ).fetchone():
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
