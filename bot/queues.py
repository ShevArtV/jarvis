"""Очереди работ: делегированные задачи (``jobs``), внешние триггеры
(``agent_triggers``) и уборка старых записей.

Обе очереди забираются атомарно: ``claim_next_*`` в одной транзакции выбирает
строку и переводит её в ``in_progress``, поэтому несколько воркеров не могут
взять одну и ту же работу. Сами воркеры — в ``bot.workers``, здесь только SQL.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

from bot.db import _db
from bot.timeutil import utcnow

logger = logging.getLogger(__name__)

def claim_next_job(
    exclude_keys: frozenset[tuple[int, int]] | None = None,
) -> dict | None:
    """Atomically claim one pending job that's due now. Returns its row as
    a dict or None.

    Scheduled jobs (`not_before > NOW()`) are skipped; immediate jobs
    (not_before IS NULL) and overdue scheduled ones are eligible. Ordering
    is by "effective firing time" (`COALESCE(not_before, created_at)`) so
    that a live live job created during a 10-min wait gets handled before
    the wait expires.

    `exclude_keys` — топики (chat_id, thread_id), у которых уже есть задача в
    работе. Пропускаются, чтобы (а) сохранить порядок внутри топика и (б) не
    занимать слот пула ожиданием per-topic лока. Параллельный диспетчер
    обрабатывает задачи РАЗНЫХ топиков одновременно; claim атомарен и
    multi-worker-safe (см. rowcount-проверку ниже).
    """
    now = utcnow().isoformat()
    sql = (
        "SELECT id, chat_id, thread_id, text, source, origin_chat_id, "
        "origin_thread_id FROM jobs "
        "WHERE status = 'pending' AND (not_before IS NULL OR not_before <= ?)"
    )
    params = [now]
    for chat_id, thread_id in (exclude_keys or ()):
        sql += " AND NOT (chat_id = ? AND thread_id = ?)"
        params.extend([chat_id, thread_id])
    sql += " ORDER BY COALESCE(not_before, created_at) ASC, id ASC LIMIT 1"
    with _db() as conn:
        row = conn.execute(sql, params).fetchone()
        if not row:
            return None
        cur = conn.execute(
            "UPDATE jobs SET status = 'in_progress', claimed_at = ? "
            "WHERE id = ? AND status = 'pending'",
            (now, row[0]),
        )
        if cur.rowcount != 1:
            # Раса с другим worker'ом — пусть следующий цикл подберёт.
            return None
    return {
        "id": row[0],
        "chat_id": row[1],
        "thread_id": row[2],
        "text": row[3],
        "source": row[4],
        # Топик-инициатор делегирования: нотис об ответе идёт ему, а не
        # константной служебной роли. NULL у старых job и self_notice.
        "origin_chat_id": row[5],
        "origin_thread_id": row[6],
    }


def finish_job(
    job_id: int,
    status: str,
    error: str | None = None,
    result_message_id: int | None = None,
) -> None:
    """status: 'done' | 'failed'."""
    with _db() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, error = ?, result_message_id = ?, "
            "finished_at = ? WHERE id = ?",
            (status, error, result_message_id, utcnow().isoformat(), job_id),
        )


def claim_next_agent_trigger(
    exclude_keys: frozenset[tuple[int, int]] | None = None,
    coalesce: dict[str, float] | None = None,
) -> dict | None:
    """Atomically claim one pending non-job trigger.

    This queue is for external-integration handoff (see agent_triggers in
    init_db): run a normal LLM turn in the topic, but do not create/finish a
    Jarvis job and do not emit job safety notices.

    ``coalesce`` — ``{source: seconds}``: триггер такого источника ждёт, пока
    самому старому из них не исполнится ``seconds``, и забирается вместе со
    всеми ожидающими триггерами того же источника в этом топике — серия
    событий уходит агенту одним ходом. ``ids``/``texts`` в ответе — все
    забранные строки по порядку.
    """
    coalesce = coalesce or {}
    now_dt = utcnow()
    now = now_dt.isoformat()
    sql = (
        "SELECT id, chat_id, thread_id, text, source FROM agent_triggers "
        "WHERE status = 'pending'"
    )
    params: list[int | str] = []
    for chat_id, thread_id in (exclude_keys or ()):
        sql += " AND NOT (chat_id = ? AND thread_id = ?)"
        params.extend([chat_id, thread_id])
    for source, seconds in coalesce.items():
        sql += " AND NOT (source = ? AND created_at > ?)"
        params.extend([source, (now_dt - timedelta(seconds=seconds)).isoformat()])
    sql += " ORDER BY created_at ASC, id ASC LIMIT 1"
    with _db() as conn:
        row = conn.execute(sql, params).fetchone()
        if not row:
            return None
        rows = [row]
        if row[4] in coalesce:
            rows = conn.execute(
                "SELECT id, chat_id, thread_id, text, source FROM agent_triggers "
                "WHERE status = 'pending' AND chat_id = ? AND thread_id = ? "
                "AND source = ? ORDER BY created_at ASC, id ASC",
                (row[1], row[2], row[4]),
            ).fetchall()
        claimed = []
        for r in rows:
            cur = conn.execute(
                "UPDATE agent_triggers SET status = 'in_progress', claimed_at = ? "
                "WHERE id = ? AND status = 'pending'",
                (now, r[0]),
            )
            if cur.rowcount == 1:
                claimed.append(r)
        if not claimed:
            return None
    return {
        "id": claimed[0][0],
        "ids": [r[0] for r in claimed],
        "chat_id": row[1],
        "thread_id": row[2],
        "text": "\n\n".join(r[3] for r in claimed),
        "texts": [r[3] for r in claimed],
        "source": row[4],
    }


def enqueue_agent_trigger(
    chat_id: int,
    thread_id: int,
    text: str,
    source: str,
    role: str | None = None,
    seen_key: tuple[str, str, int | str] | None = None,
) -> int | None:
    """Поставить внешний триггер в очередь. Возвращает id триггера.

    ``seen_key`` — ``(integration, kind, item_id)`` для источников с доставкой
    at-least-once: отметка в ``integration_seen_items`` и сам триггер пишутся
    одной транзакцией. Если отметка уже есть — это повторная доставка, триггер
    не ставится и возвращается None. Ошибка записи откатывает обе строки, так
    что «отметили, но не поставили» не бывает.
    """
    now = utcnow().isoformat()
    with _db() as conn:
        if seen_key is not None:
            cur = conn.execute(
                "INSERT OR IGNORE INTO integration_seen_items"
                "(integration, kind, item_id, seen_at) VALUES (?, ?, ?, ?)",
                (*seen_key, now),
            )
            if cur.rowcount != 1:
                return None
        cur = conn.execute(
            "INSERT INTO agent_triggers(chat_id, thread_id, text, source, role, "
            "status, created_at) VALUES (?, ?, ?, ?, ?, 'pending', ?)",
            (chat_id, thread_id, text, source, role, now),
        )
        return int(cur.lastrowid)


def finish_agent_trigger(
    trigger_id: int,
    status: str,
    error: str | None = None,
    result_message_id: int | None = None,
) -> None:
    """status: 'done' | 'failed'."""
    with _db() as conn:
        conn.execute(
            "UPDATE agent_triggers SET status = ?, error = ?, result_message_id = ?, "
            "finished_at = ? WHERE id = ?",
            (status, error, result_message_id, utcnow().isoformat(), trigger_id),
        )


def _log_ttl_days() -> int:
    """How long to retain logs + completed queues. 0/'none'/'off' disables."""
    raw = (os.environ.get("JARVIS_LOG_TTL_DAYS") or "30").strip().lower()
    if raw in {"0", "none", "off", "false", "no"}:
        return 0
    try:
        n = int(raw)
        return max(0, n)
    except ValueError:
        logger.warning("JARVIS_LOG_TTL_DAYS=%r is not an int, defaulting to 30", raw)
        return 30


def cleanup_old_log_entries(ttl_days: int) -> dict[str, int]:
    """Delete messages_log entries and terminal-status queues older than ttl_days.

    Pending jobs/triggers are NEVER deleted: they may be valid queued work.
    Returns counts of deleted rows.
    """
    if ttl_days <= 0:
        return {"messages_log": 0, "jobs": 0, "agent_triggers": 0}
    threshold = (utcnow() - timedelta(days=ttl_days)).isoformat()
    with _db() as conn:
        log_n = conn.execute(
            "DELETE FROM messages_log WHERE ts < ?", (threshold,),
        ).rowcount
        jobs_n = conn.execute(
            "DELETE FROM jobs WHERE status IN ('done', 'failed', 'cancelled') "
            "AND COALESCE(finished_at, created_at) < ?",
            (threshold,),
        ).rowcount
        triggers_n = conn.execute(
            "DELETE FROM agent_triggers WHERE status IN ('done', 'failed') "
            "AND COALESCE(finished_at, created_at) < ?",
            (threshold,),
        ).rowcount
    return {"messages_log": log_n, "jobs": jobs_n, "agent_triggers": triggers_n}
