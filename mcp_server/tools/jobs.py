"""MCP-тулы очереди задач и сообщений: отправка, отмена, прерывание,
ожидание ответа, чтение лога топика."""

from __future__ import annotations

import asyncio
import time
from datetime import timedelta
from typing import Any

from bot.timeutil import utcnow
from mcp_server import common
from mcp_server.common import logger, mcp


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
    target_chat_id = chat_id if chat_id is not None else common._default_chat_id()
    # Топик-инициатор: сюда бот вернёт нотис об ответе на этот job. Сервер
    # общий на все топики и вызывающего сам не знает — его передаёт агент.
    if origin_thread_id is not None and origin_chat_id is None:
        origin_chat_id = common._default_chat_id()

    with common._connect() as conn:
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

    now_dt = utcnow()
    now = now_dt.isoformat()
    if as_user:
        not_before: str | None = None
        if delay_seconds > 0:
            not_before = (now_dt + timedelta(seconds=delay_seconds)).isoformat()
        cancelled_ids: list[int] = []
        with common._connect() as conn:
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
            if common._jobs_has_origin_columns(conn):
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
        result = common._telegram_api("sendMessage", params)
    except RuntimeError:
        # Кривая разметка не должна съедать сообщение целиком: у агента может
        # не сойтись тег, и тогда Telegram отвергает весь текст. Лучше отдать
        # содержимое как есть, чем потерять его.
        if not parse_mode:
            raise
        logger.warning("manager_send: %s markup rejected, retrying as plain text",
                       parse_mode)
        params.pop("parse_mode")
        result = common._telegram_api("sendMessage", params)
    msg_id = int(result["message_id"])
    with common._connect() as conn:
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
    now = utcnow().isoformat()
    with common._connect() as conn:
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
    target_chat_id = chat_id if chat_id is not None else common._default_chat_id()
    now = utcnow().isoformat()
    with common._connect() as conn:
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
    target_chat_id = chat_id if chat_id is not None else common._default_chat_id()
    params = {"chat_id": target_chat_id, "message_id": message_id}
    try:
        common._telegram_api("deleteMessage", params)
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
    target_chat_id = chat_id if chat_id is not None else common._default_chat_id()

    deadline = time.monotonic() + timeout_seconds
    last_seen_ts: str | None = None
    while True:
        with common._connect() as conn:
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
    with common._connect() as conn:
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
