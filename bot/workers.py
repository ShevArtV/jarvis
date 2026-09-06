"""Фоновые воркеры: единственные бесконечные циклы в боте.

Каждый — отдельная asyncio-задача, поднимаемая в ``bot.app._post_init``. Общее
правило: воркер обязан переживать любое исключение внутри итерации, иначе
одна ошибка тихо убивает всю подсистему до рестарта службы.

Обе очереди работ (``jobs``, ``agent_triggers``) забираются со слотами: разные
топики идут параллельно, внутри одного топика — строго по очереди.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timedelta

from telegram.ext import Application

from engines.process_control import terminate_process_tree

from bot.db import _db, log_message
from bot.delivery import _send_manager_notice, send_to_topic
from bot.jobs import _process_agent_trigger, _run_manager_job
from bot.queues import (
    _log_ttl_days,
    finish_agent_trigger,
    claim_next_agent_trigger,
    claim_next_job,
    cleanup_old_log_entries,
    finish_job,
)
from bot.reminders import compute_next_fire, parse_reminder_schedule
from bot.sessions import clear_close_request, close_session, get_session
from bot.settings import CLAUDE_CWD
from bot.topics import (
    PERSISTENT_IDLE_MINUTES,
    _kill_persistent_worker,
    active_procs,
    persistent_workers,
    resolve_job_notice_target,
    resolve_secretary_topic,
    resolve_teamlead_topic,
)

logger = logging.getLogger(__name__)

# Закрытия сеансов, запрошенные Менеджером через MCP. Опрос частый (как у
# interrupt-вотчера): между запросом и смертью живого процесса топик ещё
# отвечает старым контекстом, поэтому окно держим коротким.
CLOSE_REQUEST_POLL_SECONDS = 2.0

# Топики, у которых сейчас выполняется делегированная задача. Диспетчер не
# claim'ит новую задачу для топика из этого множества — это сохраняет порядок
# задач внутри топика и не даёт слоту пула висеть на per-topic локе.
_inflight_job_keys: set[tuple[int, int]] = set()

# То же для внешних триггеров: свой набор, чтобы job и trigger одного
# топика не блокировали друг другу слот пула.
_inflight_trigger_keys: set[tuple[int, int]] = set()

async def cleanup_worker(app: Application) -> None:
    """Long-running background task: hourly sweep of stale log/jobs rows.

    Honors JARVIS_LOG_TTL_DAYS env (defaults to 30). Set to 0/none/off
    to disable cleanup entirely.
    """
    ttl = _log_ttl_days()
    if ttl <= 0:
        logger.info("cleanup_worker: TTL disabled (JARVIS_LOG_TTL_DAYS=%s)",
                    os.environ.get("JARVIS_LOG_TTL_DAYS"))
        return
    logger.info("cleanup_worker started (TTL=%dd)", ttl)
    while True:
        try:
            stats = cleanup_old_log_entries(ttl)
            if stats["messages_log"] or stats["jobs"] or stats["agent_triggers"]:
                logger.info(
                    "cleanup_worker: pruned messages_log=%d jobs=%d "
                    "agent_triggers=%d (TTL=%dd)",
                    stats["messages_log"], stats["jobs"], stats["agent_triggers"], ttl,
                )
            await asyncio.sleep(3600.0)
        except asyncio.CancelledError:
            logger.info("cleanup_worker cancelled")
            raise
        except Exception:
            logger.exception("cleanup_worker loop crashed; sleeping 5min")
            await asyncio.sleep(300.0)


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


def _env_int(name: str, default: int, min_val: int = 1) -> int:
    """Parse positive int from env with fallback. Used for heartbeat thresholds."""
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        n = int(raw)
        return max(min_val, n)
    except ValueError:
        logger.warning("%s=%r is not int, defaulting to %d", name, raw, default)
        return default


async def persistent_reaper(app: Application) -> None:
    """Убивает простаивающие живые процессы (свободные между ходами)
    после PERSISTENT_IDLE_MINUTES простоя. Живой процесс не экономит токены
    (сессия и так резюмируется с диска), только задержку на старте — вечно
    держать subprocess смысла нет. Активные (busy) воркеры не трогает."""
    if PERSISTENT_IDLE_MINUTES <= 0:
        logger.info("persistent_reaper: disabled (JARVIS_PERSISTENT_IDLE_MINUTES<=0)")
        return
    logger.info("persistent_reaper started (idle=%dmin)", PERSISTENT_IDLE_MINUTES)
    while True:
        try:
            await asyncio.sleep(60.0)
            now = time.monotonic()
            stale = [
                key for key, w in list(persistent_workers.items())
                if not w.busy and (now - w.last_activity) > PERSISTENT_IDLE_MINUTES * 60
            ]
            for key in stale:
                await _kill_persistent_worker(key, "")
                logger.info("persistent_reaper: killed idle worker key=%s", key)
        except asyncio.CancelledError:
            logger.info("persistent_reaper cancelled")
            raise
        except Exception:
            logger.exception("persistent_reaper loop crashed; continuing")


async def _apply_close_request(app: Application, key: tuple[int, int]) -> None:
    """Доделать закрытие сеанса, помеченное Менеджером: убить процессы топика
    (их видит только бот), закрыть сеанс, погасить флаг и сказать об этом в
    топик. Порядок и эффект — те же, что у /close."""
    proc = active_procs.get(key)
    if proc is not None:
        await terminate_process_tree(proc)
        active_procs.pop(key, None)
        logger.info("manager close: killed active proc for key=%s", key)
    await _kill_persistent_worker(key, "сеанс закрыт Менеджером")

    close_session(*key)
    clear_close_request(*key)

    _sid, cwd, engine_name = get_session(*key)
    logger.info("session closed by manager: key=%s engine=%s", key, engine_name)

    notice = (
        "🚪 Сеанс закрыт Менеджером. Контекст сброшен — следующее сообщение "
        "начнёт новый.\n"
        f"Топик сохранён: {engine_name}, {cwd or CLAUDE_CWD}"
    )
    try:
        chat = await app.bot.get_chat(key[0])
        sent = await send_to_topic(chat, key[1], notice)
        log_message(key[0], key[1], "out", "session_closed", notice,
                    sent.message_id if sent is not None else None)
    except Exception:
        # Сеанс уже закрыт — нотис вторичен, повторять цикл из-за него нельзя.
        logger.exception("manager close: notice failed key=%s", key)


async def close_requests_worker(app: Application) -> None:
    """Исполняет закрытия сеансов, запрошенные Менеджером через MCP
    (manager_close_session).

    MCP-сервер — отдельный процесс: он умеет только пометить строку в
    sessions. Убить активный subprocess и живой процесс /persistent может
    лишь бот — они лежат в его памяти (active_procs / persistent_workers), а
    persistent-путь берёт воркера ДО ensure_active_session, т.е. без этого
    добивания топик продолжил бы отвечать из старого контекста."""
    logger.info("close_requests_worker started (poll=%.1fs)",
                CLOSE_REQUEST_POLL_SECONDS)
    while True:
        try:
            await asyncio.sleep(CLOSE_REQUEST_POLL_SECONDS)
            with _db() as conn:
                rows = conn.execute(
                    "SELECT chat_id, thread_id FROM sessions "
                    "WHERE close_requested IS NOT NULL"
                ).fetchall()
            for chat_id, thread_id in rows:
                await _apply_close_request(app, (chat_id, thread_id))
        except asyncio.CancelledError:
            logger.info("close_requests_worker cancelled")
            raise
        except Exception:
            logger.exception("close_requests_worker loop crashed; continuing")


async def health_worker(app: Application) -> None:
    """Watcher для долгоиграющих job'ов.

    Сканирует in_progress job'ы каждые HEARTBEAT_INTERVAL секунд:
    - claimed_at < now - WARN и heartbeat_notified_at IS NULL → шлём
      нотис «job работает долго, проверь» (один раз за job).
    - claimed_at < now - FAIL → принудительно помечаем failed и шлём
      «job отменён по таймауту». Subprocess сам не убиваем — ответ
      возможно уже почти готов; если хочешь принудительно прервать —
      пользуйся manager_interrupt.
    """
    interval = _env_int("JARVIS_HEARTBEAT_INTERVAL", 300, 30)
    warn_s = _env_int("JARVIS_HEARTBEAT_WARN", 900, 60)
    fail_s = _env_int("JARVIS_HEARTBEAT_FAIL", 3600, 120)
    logger.info(
        "health_worker started (interval=%ds warn=%ds fail=%ds)",
        interval, warn_s, fail_s,
    )
    while True:
        try:
            now_dt = datetime.utcnow()
            warn_thr = (now_dt - timedelta(seconds=warn_s)).isoformat()
            fail_thr = (now_dt - timedelta(seconds=fail_s)).isoformat()
            now_iso = now_dt.isoformat()

            # WARN: in_progress + claimed_at < warn_thr + ещё не уведомляли.
            with _db() as conn:
                warn_rows = conn.execute(
                    "SELECT id, chat_id, thread_id, claimed_at, origin_chat_id, "
                    "origin_thread_id FROM jobs "
                    "WHERE status='in_progress' AND claimed_at IS NOT NULL "
                    "AND claimed_at < ? AND heartbeat_notified_at IS NULL",
                    (warn_thr,),
                ).fetchall()
            for r in warn_rows:
                jid, jchat, jthread, jclaimed = r[0], r[1], r[2], r[3]
                jtarget = resolve_job_notice_target(r[4], r[5])
                try:
                    claimed_dt = datetime.fromisoformat(jclaimed)
                    mins = int((now_dt - claimed_dt).total_seconds() / 60)
                except Exception:
                    mins = warn_s // 60
                with _db() as conn:
                    title_row = conn.execute(
                        "SELECT topic_title FROM sessions WHERE chat_id=? AND thread_id=?",
                        (jchat, jthread),
                    ).fetchone()
                title = (
                    (title_row[0] if title_row and title_row[0] else None)
                    or f"thread_id={jthread}"
                )
                text = (
                    f"⏳ job #{jid} работает {mins} мин в топике «{title}» "
                    f"(thread_id={jthread}).\n"
                    f"Возможно ушло не туда. Если что — останови через "
                    f"manager_interrupt(thread_id={jthread}) и спроси, "
                    f"что происходит."
                )
                await _send_manager_notice(
                    app, text, kind="job_heartbeat_warn", target_role="teamlead",
                    target=jtarget,
                )
                with _db() as conn:
                    conn.execute(
                        "UPDATE jobs SET heartbeat_notified_at = ? WHERE id = ?",
                        (now_iso, jid),
                    )

            # FAIL: in_progress + claimed_at < fail_thr → помечаем failed.
            with _db() as conn:
                fail_rows = conn.execute(
                    "SELECT id, chat_id, thread_id, origin_chat_id, "
                    "origin_thread_id FROM jobs "
                    "WHERE status='in_progress' AND claimed_at IS NOT NULL "
                    "AND claimed_at < ?",
                    (fail_thr,),
                ).fetchall()
            for r in fail_rows:
                jid, jchat, jthread = r[0], r[1], r[2]
                jtarget = resolve_job_notice_target(r[3], r[4])
                with _db() as conn:
                    conn.execute(
                        "UPDATE jobs SET status='failed', "
                        "error='timeout > heartbeat_fail', finished_at=? "
                        "WHERE id=? AND status='in_progress'",
                        (now_iso, jid),
                    )
                text = (
                    f"❌ job #{jid}: принудительно помечен failed "
                    f"(работал >{fail_s // 60} мин в thread_id={jthread}). "
                    f"Subprocess не убит — если ответ всё-таки придёт, "
                    f"он окажется в проектном топике, но job уже закрыт. "
                    f"Чтобы реально прервать — используй "
                    f"manager_interrupt(thread_id={jthread})."
                )
                await _send_manager_notice(
                    app, text, kind="job_heartbeat_fail", target_role="teamlead",
                    target=jtarget,
                )

            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info("health_worker cancelled")
            raise
        except Exception:
            logger.exception("health_worker loop crashed; sleeping 60s")
            await asyncio.sleep(60.0)


async def _run_job_slot(
    app: Application,
    job: dict,
    key: tuple[int, int],
    sem: asyncio.Semaphore,
) -> None:
    """Выполнить одну делегированную задачу в слоте пула и освободить слот.
    Сериализация внутри топика — через per-topic _lock_for в _run_manager_job."""
    job_id = job["id"]
    try:
        ok, msg_id, err_text = await _run_manager_job(app, job)
        finish_job(
            job_id, "done" if ok else "failed",
            None if ok else (err_text or "engine returned no usable reply"),
            msg_id,
        )
    except asyncio.CancelledError:
        finish_job(job_id, "failed", "worker cancelled", None)
        raise
    except Exception as exc:
        logger.exception("manager job %s crashed", job_id)
        finish_job(job_id, "failed", str(exc)[:1000], None)
    finally:
        _inflight_job_keys.discard(key)
        sem.release()


async def jobs_worker(app: Application) -> None:
    """Диспетчер делегированных задач: claim'ит pending-задачи и запускает их
    параллельными тасками, ограниченными семафором (JARVIS_JOBS_CONCURRENCY,
    дефолт 5). Задачи РАЗНЫХ топиков идут одновременно; задачи ОДНОГО топика
    сериализуются (исключение busy-топиков при claim + per-topic лок).

    Раньше воркер был один и await'ил каждую задачу до конца — длинный ресёрч в
    одном топике блокировал делегирование во все остальные. Теперь не блокирует.
    """
    concurrency = _env_int("JARVIS_JOBS_CONCURRENCY", 5, 1)
    sem = asyncio.Semaphore(concurrency)
    tasks: set[asyncio.Task] = set()
    logger.info("jobs_worker started (concurrency=%d)", concurrency)
    while True:
        # acquired: держим ли мы слот, который ещё не передан таске. Гарантирует
        # освобождение слота на любом пути ошибки (иначе пул бы протекал).
        acquired = False
        try:
            # Ждём свободный слот ДО claim'а — задача не помечается in_progress,
            # пока её некому выполнять (нет осиротевших claimed-but-idle задач).
            await sem.acquire()
            acquired = True
            job = claim_next_job(exclude_keys=frozenset(_inflight_job_keys))
            if job is None:
                sem.release()
                acquired = False
                await asyncio.sleep(2.0)
                continue
            key = (job["chat_id"], job["thread_id"])
            _inflight_job_keys.add(key)
            t = asyncio.create_task(_run_job_slot(app, job, key, sem))
            acquired = False  # владение слотом перешло к таске (release в её finally)
            tasks.add(t)
            t.add_done_callback(tasks.discard)
        except asyncio.CancelledError:
            if acquired:
                sem.release()
            logger.info("jobs_worker cancelled; cancelling %d in-flight job(s)", len(tasks))
            for t in list(tasks):
                t.cancel()
            raise
        except Exception:
            if acquired:
                sem.release()
            logger.exception("jobs_worker dispatcher crashed; sleeping 5s")
            await asyncio.sleep(5.0)


async def _run_agent_trigger_slot(
    app: Application,
    trigger: dict,
    key: tuple[int, int],
    sem: asyncio.Semaphore,
) -> None:
    trigger_id = trigger["id"]
    try:
        ok, err_text = await _process_agent_trigger(app, trigger)
        finish_agent_trigger(
            trigger_id,
            "done" if ok else "failed",
            None if ok else (err_text or "engine returned no usable reply"),
            None,
        )
    except asyncio.CancelledError:
        finish_agent_trigger(trigger_id, "failed", "worker cancelled", None)
        raise
    except Exception as exc:
        logger.exception("agent trigger %s crashed", trigger_id)
        finish_agent_trigger(trigger_id, "failed", str(exc)[:1000], None)
    finally:
        _inflight_trigger_keys.discard(key)
        sem.release()


async def agent_triggers_worker(app: Application) -> None:
    """Dispatcher for non-job external triggers.

    Unlike jobs_worker, these turns have no job_id and are invisible to
    manager_interrupt/health_worker. They are still serialized per topic by the
    normal topic lock inside _process_prompt_locked/_handle_persistent_message.
    """
    concurrency = _env_int("JARVIS_AGENT_TRIGGERS_CONCURRENCY", 5, 1)
    sem = asyncio.Semaphore(concurrency)
    tasks: set[asyncio.Task] = set()
    logger.info("agent_triggers_worker started (concurrency=%d)", concurrency)
    while True:
        acquired = False
        try:
            await sem.acquire()
            acquired = True
            exclude = frozenset(_inflight_trigger_keys | _inflight_job_keys)
            trigger = claim_next_agent_trigger(exclude_keys=exclude)
            if trigger is None:
                sem.release()
                acquired = False
                await asyncio.sleep(2.0)
                continue
            key = (trigger["chat_id"], trigger["thread_id"])
            _inflight_trigger_keys.add(key)
            t = asyncio.create_task(_run_agent_trigger_slot(app, trigger, key, sem))
            acquired = False
            tasks.add(t)
            t.add_done_callback(tasks.discard)
        except asyncio.CancelledError:
            if acquired:
                sem.release()
            logger.info(
                "agent_triggers_worker cancelled; cancelling %d in-flight trigger(s)",
                len(tasks),
            )
            for t in list(tasks):
                t.cancel()
            raise
        except Exception:
            if acquired:
                sem.release()
            logger.exception("agent_triggers_worker dispatcher crashed; sleeping 5s")
            await asyncio.sleep(5.0)
