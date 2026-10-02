"""Воркер напоминаний: раз в N секунд шлёт сработавшие напоминания в их топик."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from telegram.ext import Application

from bot.db import _db, log_message
from bot.delivery import send_to_topic
from bot.topics import resolve_secretary_topic, resolve_teamlead_topic
from bot.workers import _env_int
from plugins.reminders.schedule import compute_next_fire, parse_reminder_schedule

logger = logging.getLogger(__name__)


async def reminders_worker(app: Application) -> None:
    """Раз в N секунд сканирует reminders и шлёт сработавшие в их топик."""
    interval = _env_int("JARVIS_REMINDERS_INTERVAL", 60, 10)
    logger.info("reminders_worker started (interval=%ds)", interval)
    while True:
        try:
            now = datetime.utcnow()
            now_iso = now.isoformat()
            with _db() as conn:
                due_rows = conn.execute(
                    "SELECT id, chat_id, thread_id, text, schedule, next_fire_at "
                    "FROM reminders WHERE enabled = 1 AND next_fire_at <= ? "
                    "ORDER BY next_fire_at ASC",
                    (now_iso,),
                ).fetchall()
            for r in due_rows:
                rid, rchat, rthread, rtext, rschedule, _ = r
                notice = f"🔔 Напоминание #{rid}: {rtext}"
                service_targets = {
                    t for t in (resolve_secretary_topic(), resolve_teamlead_topic())
                    if t is not None
                }
                try:
                    chat = await app.bot.get_chat(rchat)
                    sent = await send_to_topic(chat, rthread, notice)
                    msg_id = sent.message_id if sent is not None else None
                    log_message(rchat, rthread, "out", "reminder", notice, msg_id)
                except Exception:
                    logger.exception("reminders_worker: send failed id=%s", rid)
                    # Не пересчитываем next_fire_at — попробуем в следующем цикле.
                    continue

                # Auto-kick если напоминание в служебный топик. Существующие
                # reminders старого Менеджера остаются в Секретаре, а если
                # когда-нибудь появятся инженерные reminders в Тимлиде, они тоже
                # будут будить свой топик.
                if (rchat, rthread) in service_targets:
                    try:
                        with _db() as conn:
                            existing = conn.execute(
                                "SELECT COUNT(*) FROM jobs "
                                "WHERE chat_id=? AND thread_id=? "
                                "AND status IN ('pending','in_progress')",
                                (rchat, rthread),
                            ).fetchone()[0]
                            if existing == 0:
                                conn.execute(
                                    "INSERT INTO jobs(chat_id, thread_id, text, "
                                    "source, status, created_at) VALUES "
                                    "(?, ?, ?, 'self_notice', 'pending', ?)",
                                    (
                                        rchat, rthread,
                                        f"[REMINDER] 🔔 Сработало напоминание "
                                        f"#{rid}: {rtext}",
                                        now_iso,
                                    ),
                                )
                    except Exception:
                        logger.exception(
                            "reminders_worker: auto-kick failed id=%s", rid,
                        )

                # Пересчитать next_fire_at.
                try:
                    parsed = parse_reminder_schedule(rschedule)
                    next_fire = compute_next_fire(parsed, now)
                except Exception:
                    logger.exception(
                        "reminders_worker: failed to recompute next_fire id=%s "
                        "schedule=%r → disabling", rid, rschedule,
                    )
                    next_fire = None
                with _db() as conn:
                    if next_fire is None:
                        # once отработал, либо schedule сломался → выключаем.
                        conn.execute(
                            "UPDATE reminders SET enabled=0, last_fired_at=? "
                            "WHERE id=?",
                            (now_iso, rid),
                        )
                    else:
                        conn.execute(
                            "UPDATE reminders SET next_fire_at=?, last_fired_at=? "
                            "WHERE id=?",
                            (next_fire.isoformat(), now_iso, rid),
                        )
                logger.info(
                    "reminders_worker: fired id=%s next=%s",
                    rid, next_fire.isoformat() if next_fire else "DISABLED",
                )
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info("reminders_worker cancelled")
            raise
        except Exception:
            logger.exception("reminders_worker loop crashed; sleeping 60s")
            await asyncio.sleep(60.0)
