"""Диалоговый сеанс vs сеанс уведомлений QW; ротация по размеру; seed.

Диалог живёт в ``sessions.session_id`` / persistent-worker топика.
Уведомления QueueWarden — в ``notify_session_id`` и разовом CLI-процессе
(``call_stream`` с ``spawn_id``), параллельно диалогу: диалоговый worker
не убиваем и не переключаем.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta

from bot.asks import get_pending_ask
from bot.db import _db
from bot.settings import (
    CONTEXT_ROTATE_TOKENS,
    NOTIFY_IDLE_MINUTES,
    SESSION_SEED_MESSAGES,
)
from bot.timeutil import utcnow
from bot.topics import active_procs, load_message_context, persistent_workers
from engines import get_engine_by_name
from engines.background_work import has_background_work
from engines.session_usage import inspect_session_usage

logger = logging.getLogger(__name__)

# Движки, у которых inspect_session_usage умеет отдать размер контекста.
# cursor/kimi → source=unknown, threshold_tokens=None — ротация на них молчит.
ROTATE_ENGINES = frozenset({"claude", "codex", "opencode"})

_TASK_KEY_RE = re.compile(r"\b(\d{4}-\d+)\b")
_HASHTAG_RE = re.compile(r"#(?:qw[_-])?[a-z0-9_-]+-\d{4}-\d+", re.I)


def dialog_busy_reasons(key: tuple[int, int]) -> list[str]:
    """Почему нельзя закрыть/ротировать диалоговый сеанс прямо сейчас."""
    reasons: list[str] = []
    if get_pending_ask(*key) is not None:
        reasons.append("ask_user")
    worker = persistent_workers.get(key)
    if worker is not None:
        if getattr(worker, "busy", False):
            reasons.append("turn")
        if has_background_work(worker):
            reasons.append("background")
    if active_procs.get(key) is not None:
        reasons.append("oneshot")
    return reasons


def set_rotate_pending(chat_id: int, thread_id: int, pending: bool = True) -> None:
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET rotate_pending = ? WHERE chat_id = ? AND thread_id = ?",
            (1 if pending else 0, chat_id, thread_id),
        )


def get_rotate_pending(chat_id: int, thread_id: int) -> bool:
    with _db() as conn:
        row = conn.execute(
            "SELECT rotate_pending FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
    return bool(row and row[0])


def _notify_is_stale(last_activity_at: str | None) -> bool:
    if not last_activity_at:
        return True
    if NOTIFY_IDLE_MINUTES <= 0:
        return False
    try:
        last = datetime.fromisoformat(last_activity_at)
    except ValueError:
        return True
    return utcnow() - last > timedelta(minutes=NOTIFY_IDLE_MINUTES)


def ensure_notify_session(
    chat_id: int, thread_id: int,
) -> tuple[str, str | None, str, bool]:
    """Сеанс полосы notify: (session_id, cwd, engine, opened_new)."""
    now = utcnow().isoformat()
    with _db() as conn:
        row = conn.execute(
            "SELECT notify_session_id, notify_last_activity_at, cwd, engine "
            "FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
    if row is None:
        from bot.sessions import get_session

        get_session(chat_id, thread_id)
        with _db() as conn:
            row = conn.execute(
                "SELECT notify_session_id, notify_last_activity_at, cwd, engine "
                "FROM sessions WHERE chat_id = ? AND thread_id = ?",
                (chat_id, thread_id),
            ).fetchone()
    assert row is not None
    notify_id, last_at, cwd, engine_name = row
    engine = get_engine_by_name(engine_name)
    if notify_id and not _notify_is_stale(last_at):
        touch_notify_session(chat_id, thread_id)
        return notify_id, cwd, engine_name, False
    new_id = engine.new_session_id()
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET notify_session_id = ?, notify_last_activity_at = ?, "
            "notify_started_at = ?, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ?",
            (new_id, now, now, now, chat_id, thread_id),
        )
    logger.info(
        "notify session opened: key=%s session=%s reason=%s",
        (chat_id, thread_id), new_id, "stale" if notify_id else "new",
    )
    return new_id, cwd, engine_name, True


def touch_notify_session(chat_id: int, thread_id: int) -> None:
    now = utcnow().isoformat()
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET notify_last_activity_at = ?, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ?",
            (now, now, chat_id, thread_id),
        )


def close_notify_session(chat_id: int, thread_id: int) -> bool:
    """Закрыть полосу notify (idle). Диалог не трогает."""
    with _db() as conn:
        row = conn.execute(
            "SELECT notify_last_activity_at FROM sessions "
            "WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        if row is None or row[0] is None:
            return False
        conn.execute(
            "UPDATE sessions SET notify_last_activity_at = NULL, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ?",
            (utcnow().isoformat(), chat_id, thread_id),
        )
    return True


def update_notify_session_id(
    chat_id: int, thread_id: int, expected_engine: str, new_session_id: str,
) -> None:
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET notify_session_id = ?, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ? AND engine = ?",
            (new_session_id, utcnow().isoformat(),
             chat_id, thread_id, expected_engine),
        )


def notify_session_is_open(chat_id: int, thread_id: int) -> bool:
    with _db() as conn:
        row = conn.execute(
            "SELECT notify_last_activity_at FROM sessions "
            "WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
    return bool(row and row[0] and not _notify_is_stale(row[0]))


def _clip_seed_text(text: str, limit: int = 4000) -> str:
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def build_dialog_seed(chat_id: int, thread_id: int, limit: int | None = None) -> str | None:
    """Последние диалоговые реплики: user_text и ответы на них.

    ``queuewarden_inject`` и доклады по ним (bot_reply/ask_user сразу после
    inject или с meta lane=notify) в seed не входят.
    """
    n = SESSION_SEED_MESSAGES if limit is None else limit
    if n <= 0:
        return None
    with _db() as conn:
        rows = conn.execute(
            "SELECT direction, kind, text, telegram_message_id FROM messages_log "
            "WHERE chat_id = ? AND thread_id = ? ORDER BY id DESC LIMIT ?",
            (chat_id, thread_id, max(n * 8, 40)),
        ).fetchall()
    if not rows:
        return None

    # Идём от старых к новым в окне кандидатов.
    chronological = list(reversed(rows))
    picked: list[tuple[str, str]] = []
    last_inbound_kind: str | None = None
    for direction, kind, text, tg_id in chronological:
        if kind == "queuewarden_inject":
            last_inbound_kind = kind
            continue
        if kind in {"job_notification", "reminder", "manager_inject",
                    "manager_notice", "session_closed"}:
            continue
        if kind == "user_text":
            picked.append(("user", _clip_seed_text(text)))
            last_inbound_kind = kind
            continue
        if kind in {"bot_reply", "ask_user"}:
            lane = None
            if tg_id is not None:
                ctx = load_message_context(chat_id, tg_id)
                if ctx:
                    lane = ctx.get("lane") or (
                        "notify" if ctx.get("source") == "queuewarden" else None
                    )
            if lane == "notify" or last_inbound_kind == "queuewarden_inject":
                continue
            if last_inbound_kind != "user_text" and not picked:
                continue
            # Ответ на user_text (последний inbound был user или уже есть диалог).
            if last_inbound_kind == "user_text" or (
                picked and picked[-1][0] == "user"
            ):
                role = "ask" if kind == "ask_user" else "assistant"
                picked.append((role, _clip_seed_text(text)))
            continue
        if direction == "in":
            last_inbound_kind = kind

    if not picked:
        return None
    # Берём хвост из n реплик.
    picked = picked[-n:]
    lines = [
        "[Контекст: последние сообщения диалога этого топика "
        "(без уведомлений QueueWarden), от старых к новым:]",
    ]
    for role, body in picked:
        label = {"user": "Оператор", "assistant": "Ассистент", "ask": "Вопрос ассистента"}[role]
        lines.append(f"--- {label} ---\n{body}")
    return "\n\n".join(lines)


def extract_task_markers(text: str) -> dict:
    """taskId/тег из текста доклада для late-reply seed."""
    keys = _TASK_KEY_RE.findall(text or "")
    tags = _HASHTAG_RE.findall(text or "")
    out: dict = {}
    if keys:
        out["task_keys"] = list(dict.fromkeys(keys))
    if tags:
        out["tags"] = list(dict.fromkeys(tags))
    return out


def build_notify_reply_seed(
    report_text: str, operator_text: str, markers: dict | None = None,
) -> str:
    markers = markers or extract_task_markers(report_text)
    parts = [
        "[Контекст: оператор отвечает reply на доклад по уведомлению QueueWarden "
        "после закрытия сеанса уведомлений. Новый короткий сеанс.]",
        "--- Доклад ---\n" + _clip_seed_text(report_text, 8000),
    ]
    if markers.get("task_keys"):
        parts.append("taskId/номера: " + ", ".join(markers["task_keys"]))
    if markers.get("tags"):
        parts.append("Теги: " + ", ".join(markers["tags"]))
    parts.append("--- Ответ оператора ---\n" + operator_text)
    return "\n\n".join(parts)


def context_over_rotate(engine_name: str, session_id: str, cwd: str | None) -> bool | None:
    """True — пора ротировать; False — ещё нет; None — размер неизвестен (cursor/kimi)."""
    if CONTEXT_ROTATE_TOKENS <= 0:
        return False
    engine = (engine_name or "").strip().lower()
    if engine not in ROTATE_ENGINES:
        return None
    try:
        usage = inspect_session_usage(engine, session_id, cwd)
    except Exception:
        logger.exception("inspect_session_usage failed engine=%s session=%s",
                         engine, session_id)
        return None
    tokens = usage.threshold_tokens
    if tokens is None:
        return None
    return tokens >= CONTEXT_ROTATE_TOKENS


def plan_dialog_rotate(
    key: tuple[int, int], engine_name: str, session_id: str, cwd: str | None,
) -> str:
    """``close`` | ``defer`` | ``noop``.

    ``close`` — безопасно закрыть сейчас (следующее сообщение откроет новый).
    ``defer`` — поставить ``rotate_pending`` (ход/ask/фон ещё живы).
    """
    over = context_over_rotate(engine_name, session_id, cwd)
    pending = get_rotate_pending(*key)
    if over is None:
        return "noop"
    if not over and not pending:
        return "noop"
    reasons = dialog_busy_reasons(key)
    if reasons:
        logger.info("dialog rotate deferred key=%s reasons=%s", key, reasons)
        return "defer"
    return "close"
