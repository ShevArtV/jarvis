#!/usr/bin/env python3
"""Jarvis — тонкая Telegram-обёртка над LLM-CLI (claude, codex или opencode).

Модель: «один топик Telegram = один проект = одна постоянная LLM-сессия».
- Ключ сессии — (chat_id, message_thread_id). В не-форумных чатах thread_id=0.
- Каждый топик может быть привязан к своей рабочей директории (cwd) командой /bind.
- Внутри ключа вызовы сериализуются через asyncio.Lock; разные ключи работают параллельно.
- Используется stream-json: промежуточные сообщения (tool_use/exec, рассуждения)
  показываются пользователю.
- Движок выбирается per-topic: env JARVIS_ENGINE задаёт дефолт для новых топиков,
  команда /engine — переключает движок текущего топика (новый session_id, cwd
  сохраняется; контекст прежнего движка не переносится).
"""

import os
import re
import json
import shutil
import uuid
import secrets
import sqlite3
import asyncio
import logging
import tempfile
from datetime import datetime, timedelta

from telegram import (
    BotCommand,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    MenuButtonCommands,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

from config import TELEGRAM_TOKEN, ALLOWED_USER_IDS, BASE_DIR
from engines import (
    SUPPORTED_ENGINES,
    Engine,
    default_engine_name,
    engine_model_scope,
    ensure_engine_tools,
    get_engine_by_name,
)
from engines.process_control import terminate_process_tree

# ---------- Константы ----------

# Движок per-topic: env JARVIS_ENGINE — дефолт для новых топиков; существующие
# топики хранят свой engine в БД и переключаются командой /engine.
DEFAULT_ENGINE_NAME = default_engine_name()
DEFAULT_ENGINE = get_engine_by_name(DEFAULT_ENGINE_NAME)

# Дефолтный cwd для топиков без явного /bind. Имя переменной историческое (CLAUDE_CWD),
# для обратной совместимости: задаёт дефолт для любого движка.
CLAUDE_CWD = os.environ.get("CLAUDE_CWD", "/home/shevartv")

MSG_LIMIT = 3500           # порог отправки ответа как документ
TG_HARD_LIMIT = 4096       # жёсткий лимит Telegram
TG_FILE_LIMIT_MB = 50      # Telegram Bot API лимит на sendDocument

# Маркер для отправки файлов из LLM-сессии: [[FILE: /abs/path]] или [[FILE: /path | подпись]].
# Должен стоять на отдельной строке (но допускаются пробелы вокруг).
FILE_MARKER_RE = re.compile(
    r"^[ \t]*\[\[FILE:\s*(?P<path>[^|\]\n]+?)(?:\s*\|\s*(?P<caption>[^\]\n]+?))?\s*\]\][ \t]*$",
    re.MULTILINE,
)

DB_PATH = os.path.join(BASE_DIR, "bot_state.db")
MEDIA_DIR = os.path.join(BASE_DIR, "temp", "media")
os.makedirs(MEDIA_DIR, exist_ok=True)

logging.basicConfig(
    format="[bot] %(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def _system_prefix(effective_cwd: str) -> str:
    return (
        f"[SYSTEM: Сообщение пришло от пользователя через Telegram-бота Jarvis.\n"
        f"Ты работаешь в проекте {effective_cwd}. Используй memory-правила из "
        f"~/.claude/projects/-home-shevartv/memory/.\n"
        f"Если нужно работать с браузером, используй Playwright MCP browser_* tools, "
        f"когда они доступны; если MCP недоступен, скажи об этом и выбери рабочий fallback.\n"
        f"Опасные действия (удаления, DELETE/DROP, действия на проде, sudo, push --force) — "
        f"переспрашивай.]"
    )


# ---------- SQLite ----------

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _backup_db_once() -> None:
    """Перед первой миграцией схемы — однократный бэкап bot_state.db.
    Имя содержит дату, поэтому «одна копия в сутки» защищает и от повторных перезаписей.
    """
    if not os.path.exists(DB_PATH):
        return
    stamp = datetime.utcnow().strftime("%Y%m%d")
    bak_path = f"{DB_PATH}.bak-{stamp}"
    if os.path.exists(bak_path):
        return
    try:
        shutil.copy2(DB_PATH, bak_path)
        logger.info("bot_state.db backed up to %s", bak_path)
    except OSError as exc:
        logger.warning("failed to backup bot_state.db: %s", exc)


def init_db() -> None:
    with _db() as conn:
        # Миграция: если sessions существует со старым PK (chat_id) без колонок thread_id/cwd —
        # пересоздаём таблицу и переносим данные (thread_id=0).
        cols = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
        if cols and "thread_id" not in cols:
            _backup_db_once()
            logger.info("migrating sessions table: adding thread_id/cwd, new PK (chat_id, thread_id)")
            conn.execute("ALTER TABLE sessions RENAME TO sessions_old")
            conn.execute(
                """
                CREATE TABLE sessions (
                    chat_id INTEGER NOT NULL,
                    thread_id INTEGER NOT NULL DEFAULT 0,
                    session_id TEXT NOT NULL,
                    cwd TEXT,
                    engine TEXT NOT NULL DEFAULT 'claude',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (chat_id, thread_id)
                )
                """
            )
            conn.execute(
                "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, updated_at) "
                "SELECT chat_id, 0, session_id, NULL, 'claude', updated_at FROM sessions_old"
            )
            conn.execute("DROP TABLE sessions_old")
            logger.info("sessions migration done")
        else:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sessions (
                    chat_id INTEGER NOT NULL,
                    thread_id INTEGER NOT NULL DEFAULT 0,
                    session_id TEXT NOT NULL,
                    cwd TEXT,
                    engine TEXT NOT NULL DEFAULT 'claude',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (chat_id, thread_id)
                )
                """
            )
            # Idempotent миграция: добавляем engine в существующую таблицу, если её нет.
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "engine" not in cols_now:
                _backup_db_once()
                logger.info("adding 'engine' column to sessions (default='claude')")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN engine TEXT NOT NULL DEFAULT 'claude'"
                )
            # Idempotent миграция: pending_summary — резюме предыдущей сессии другого
            # движка, ждущее доставки в первый prompt после /engine с переносом.
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "pending_summary" not in cols_now:
                _backup_db_once()
                logger.info("adding 'pending_summary' column to sessions")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN pending_summary TEXT"
                )
            # Idempotent миграция: model — выбранная для топика модель движка.
            # NULL = «дефолт движка» (фолбэк на env / встроенный дефолт CLI).
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "model" not in cols_now:
                _backup_db_once()
                logger.info("adding 'model' column to sessions")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN model TEXT"
                )
            # Idempotent миграция: topic_title — название топика в Telegram.
            # Заполняется при `manager_create_topic`; Telegram API не отдаёт
            # имя топика обратно через getChat, поэтому нужен свой реестр.
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "topic_title" not in cols_now:
                _backup_db_once()
                logger.info("adding 'topic_title' column to sessions")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN topic_title TEXT"
                )
            # Idempotent миграция: topic_icon_color — цвет иконки топика
            # (один из 6 валидных RGB-кодов Telegram). Хранится, чтобы
            # manager_create_topic не дублировал цвета между топиками.
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "topic_icon_color" not in cols_now:
                _backup_db_once()
                logger.info("adding 'topic_icon_color' column to sessions")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN topic_icon_color INTEGER"
                )
            # Idempotent миграция: actual_model — реальная модель, которой
            # CLI ответил последний раз (парсится из stream-events каждого
            # адаптера). Отличается от sessions.model — там «что выбрали
            # руками», а actual_model — «что фактически работает».
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "actual_model" not in cols_now:
                _backup_db_once()
                logger.info("adding 'actual_model' column to sessions")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN actual_model TEXT"
                )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                context_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, message_id)
            )
            """
        )
        # Полный лог сообщений по топикам — нужен для Менеджера (MCP tool
        # manager_inbox). Пишем входящие пользовательские реплики и финальные
        # ответы бота. Промежуточные tool_use не логируем — слишком шумно.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS messages_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                thread_id INTEGER NOT NULL DEFAULT 0,
                direction TEXT NOT NULL,
                kind TEXT NOT NULL,
                text TEXT NOT NULL,
                telegram_message_id INTEGER,
                ts TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_messages_log_topic_ts "
            "ON messages_log(chat_id, thread_id, ts)"
        )
        # Очередь задач от Менеджера (MCP tool manager_send as_user=True).
        # Worker внутри бота забирает pending и прокручивает их через
        # обычный LLM-pipeline в указанном топике.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                thread_id INTEGER NOT NULL,
                text TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'manager',
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                claimed_at TEXT,
                finished_at TEXT,
                error TEXT,
                result_message_id INTEGER
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, created_at)"
        )
        # Idempotent миграция: not_before — момент времени, когда job
        # становится доступным для worker'а. NULL = доступен немедленно.
        # Используется для «авто-go через 10 мин» сценария Менеджера:
        # план готов → ставится scheduled job с delay=600s; если оператор
        # одобрил/корректирует раньше — новый manager_send в этот же
        # thread_id автоматически переводит pending scheduled в cancelled.
        cols_now = [r[1] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()]
        if cols_now and "not_before" not in cols_now:
            _backup_db_once()
            logger.info("adding 'not_before' column to jobs")
            conn.execute("ALTER TABLE jobs ADD COLUMN not_before TEXT")
            # Поправляем индекс под новый ORDER BY.
            conn.execute("DROP INDEX IF EXISTS idx_jobs_status")
        # Idempotent миграция: heartbeat_notified_at — когда health_worker
        # уже шлёт warn-нотис по этому job'у. NULL = ещё не уведомлял.
        # Это защита от спама — нотис идёт один раз за job.
        cols_now = [r[1] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()]
        if cols_now and "heartbeat_notified_at" not in cols_now:
            logger.info("adding 'heartbeat_notified_at' column to jobs")
            conn.execute(
                "ALTER TABLE jobs ADD COLUMN heartbeat_notified_at TEXT"
            )
        # Idempotent миграция: cancel_requested — флаг для manager_interrupt.
        # MCP-сервер ставит timestamp, watcher в _run_manager_job его читает
        # раз в N секунд и убивает subprocess. NULL = не запрошено.
        cols_now = [r[1] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()]
        if cols_now and "cancel_requested" not in cols_now:
            logger.info("adding 'cancel_requested' column to jobs")
            conn.execute(
                "ALTER TABLE jobs ADD COLUMN cancel_requested TEXT"
            )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_pending "
            "ON jobs(status, not_before, created_at)"
        )
        # Напоминания Менеджеру (cron-light). schedule — простой текст,
        # парсится в _parse_schedule(): daily HH:MM, weekday HH:MM,
        # weekend HH:MM, weekly DAY[,DAY] HH:MM, monthly D HH:MM,
        # once YYYY-MM-DD HH:MM (все времена в JARVIS_REMINDERS_TZ).
        # next_fire_at хранится в UTC ISO.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reminders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                thread_id INTEGER NOT NULL,
                text TEXT NOT NULL,
                schedule TEXT NOT NULL,
                next_fire_at TEXT NOT NULL,
                last_fired_at TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_reminders_due "
            "ON reminders(enabled, next_fire_at)"
        )


def log_message(
    chat_id: int,
    thread_id: int,
    direction: str,
    kind: str,
    text: str,
    telegram_message_id: int | None = None,
) -> None:
    """Пишет одну запись в messages_log. direction: 'in' | 'out'.

    Поглощает ошибки записи: логирование — вторичная функция, не должна
    блокировать бот при проблемах с БД.
    """
    if not text:
        return
    try:
        with _db() as conn:
            conn.execute(
                "INSERT INTO messages_log(chat_id, thread_id, direction, kind, "
                "text, telegram_message_id, ts) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    chat_id, thread_id, direction, kind, text,
                    telegram_message_id, datetime.utcnow().isoformat(),
                ),
            )
    except Exception:
        logger.exception("log_message failed (chat=%s thread=%s kind=%s)",
                         chat_id, thread_id, kind)


def claim_next_job() -> dict | None:
    """Atomically claim one pending job that's due now. Returns its row as
    a dict or None.

    Scheduled jobs (`not_before > NOW()`) are skipped; immediate jobs
    (not_before IS NULL) and overdue scheduled ones are eligible. Ordering
    is by "effective firing time" (`COALESCE(not_before, created_at)`) so
    that a live live job created during a 10-min wait gets handled before
    the wait expires.
    """
    now = datetime.utcnow().isoformat()
    with _db() as conn:
        row = conn.execute(
            "SELECT id, chat_id, thread_id, text, source FROM jobs "
            "WHERE status = 'pending' AND (not_before IS NULL OR not_before <= ?) "
            "ORDER BY COALESCE(not_before, created_at) ASC, id ASC LIMIT 1",
            (now,),
        ).fetchone()
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
            (status, error, result_message_id, datetime.utcnow().isoformat(), job_id),
        )


def _log_ttl_days() -> int:
    """How long to retain messages_log + completed jobs. 0/'none'/'off' disables."""
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
    """Delete messages_log entries and terminal-status jobs older than ttl_days.

    Pending jobs (incl. scheduled with future not_before) are NEVER deleted —
    they may be valid auto-go timers. Returns counts of deleted rows.
    """
    if ttl_days <= 0:
        return {"messages_log": 0, "jobs": 0}
    threshold = (datetime.utcnow() - timedelta(days=ttl_days)).isoformat()
    with _db() as conn:
        log_n = conn.execute(
            "DELETE FROM messages_log WHERE ts < ?", (threshold,),
        ).rowcount
        jobs_n = conn.execute(
            "DELETE FROM jobs WHERE status IN ('done', 'failed', 'cancelled') "
            "AND COALESCE(finished_at, created_at) < ?",
            (threshold,),
        ).rowcount
    return {"messages_log": log_n, "jobs": jobs_n}


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
            if stats["messages_log"] or stats["jobs"]:
                logger.info(
                    "cleanup_worker: pruned messages_log=%d jobs=%d (TTL=%dd)",
                    stats["messages_log"], stats["jobs"], ttl,
                )
            await asyncio.sleep(3600.0)
        except asyncio.CancelledError:
            logger.info("cleanup_worker cancelled")
            raise
        except Exception:
            logger.exception("cleanup_worker loop crashed; sleeping 5min")
            await asyncio.sleep(300.0)


def _reminders_tz():
    """Local timezone для парсера schedule. Default Europe/Moscow."""
    from zoneinfo import ZoneInfo
    name = os.environ.get("JARVIS_REMINDERS_TZ", "Europe/Moscow")
    try:
        return ZoneInfo(name)
    except Exception:
        logger.warning("JARVIS_REMINDERS_TZ=%r invalid, using Europe/Moscow", name)
        return ZoneInfo("Europe/Moscow")


_DAY_NAMES = {
    "mon": 0, "tue": 1, "wed": 2, "thu": 3,
    "fri": 4, "sat": 5, "sun": 6,
}


def parse_reminder_schedule(schedule: str) -> dict:
    """Парсит человекочитаемое расписание в структуру.

    Поддерживаемые форматы:
      daily HH:MM
      weekday HH:MM        (Пн-Пт)
      weekend HH:MM        (Сб-Вс)
      weekly DAY[,DAY,...] HH:MM   (DAY: mon|tue|wed|thu|fri|sat|sun)
      monthly D HH:MM      (D: 1..28)
      once YYYY-MM-DD HH:MM

    Возвращает dict с ключами:
      type: 'daily'|'weekday'|'weekend'|'weekly'|'monthly'|'once'
      hour, minute: int
      days: list[int] — для 'weekly', индексы 0=mon..6=sun
      day: int — для 'monthly' (1..28)
      date: 'YYYY-MM-DD' — для 'once'
    """
    raw = " ".join((schedule or "").split()).strip().lower()
    if not raw:
        raise ValueError("schedule is empty")

    def parse_hm(token: str) -> tuple[int, int]:
        try:
            h, m = token.split(":")
            h, m = int(h), int(m)
            if not (0 <= h <= 23 and 0 <= m <= 59):
                raise ValueError
            return h, m
        except (ValueError, AttributeError):
            raise ValueError(f"invalid HH:MM: {token!r}")

    parts = raw.split()
    kind = parts[0]

    if kind in ("daily", "weekday", "weekend") and len(parts) == 2:
        h, m = parse_hm(parts[1])
        return {"type": kind, "hour": h, "minute": m}

    if kind == "weekly" and len(parts) == 3:
        days_token = parts[1]
        days_idx: list[int] = []
        for d in days_token.split(","):
            d = d.strip()
            if d not in _DAY_NAMES:
                raise ValueError(f"unknown day: {d!r}; expected one of {list(_DAY_NAMES)}")
            if _DAY_NAMES[d] not in days_idx:
                days_idx.append(_DAY_NAMES[d])
        if not days_idx:
            raise ValueError("weekly: at least one day required")
        h, m = parse_hm(parts[2])
        return {"type": "weekly", "days": sorted(days_idx), "hour": h, "minute": m}

    if kind == "monthly" and len(parts) == 3:
        try:
            day = int(parts[1])
        except ValueError:
            raise ValueError(f"monthly: day must be int, got {parts[1]!r}")
        if not (1 <= day <= 28):
            raise ValueError("monthly: day must be 1..28 (защита от февраля)")
        h, m = parse_hm(parts[2])
        return {"type": "monthly", "day": day, "hour": h, "minute": m}

    if kind == "once" and len(parts) == 3:
        date_str = parts[1]
        h, m = parse_hm(parts[2])
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"once: date must be YYYY-MM-DD, got {date_str!r}")
        return {"type": "once", "date": date_str, "hour": h, "minute": m}

    raise ValueError(
        f"can't parse schedule: {schedule!r}. Examples: "
        "'daily 09:30', 'weekday 09:30', 'weekly mon,wed 14:00', "
        "'monthly 1 10:00', 'once 2026-06-01 09:00'."
    )


def compute_next_fire(parsed: dict, after_utc: datetime | None = None) -> datetime | None:
    """Возвращает следующий момент срабатывания (datetime, UTC, naive ISO-able).

    Возвращает None для 'once' если дата уже в прошлом — такой reminder
    в БД пометится disabled при INSERT/обновлении.
    """
    from zoneinfo import ZoneInfo
    tz = _reminders_tz()
    if after_utc is None:
        after_utc = datetime.utcnow()
    # UTC naive → aware
    now_aware = after_utc.replace(tzinfo=ZoneInfo("UTC")).astimezone(tz)
    hh = parsed["hour"]
    mm = parsed["minute"]

    def at_local(year: int, month: int, day: int) -> datetime:
        local = datetime(year, month, day, hh, mm, tzinfo=tz)
        return local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)

    ptype = parsed["type"]
    if ptype == "once":
        y, mo, d = map(int, parsed["date"].split("-"))
        fire = at_local(y, mo, d)
        return fire if fire > after_utc else None

    today_local = now_aware.replace(hour=hh, minute=mm, second=0, microsecond=0)

    def in_set(weekday: int) -> bool:
        if ptype == "daily":
            return True
        if ptype == "weekday":
            return weekday < 5
        if ptype == "weekend":
            return weekday >= 5
        if ptype == "weekly":
            return weekday in parsed["days"]
        if ptype == "monthly":
            return False  # для monthly другой механизм ниже
        return False

    if ptype == "monthly":
        target_day = parsed["day"]
        candidate_local = now_aware.replace(day=target_day, hour=hh, minute=mm, second=0, microsecond=0)
        if candidate_local <= now_aware:
            # следующий месяц
            year = candidate_local.year
            month = candidate_local.month + 1
            if month > 12:
                month = 1
                year += 1
            candidate_local = candidate_local.replace(year=year, month=month)
        return candidate_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)

    # daily/weekday/weekend/weekly — итеративный поиск ближайшего дня.
    for delta in range(0, 8):
        cand_local = today_local + timedelta(days=delta)
        if delta == 0 and cand_local <= now_aware:
            continue
        if in_set(cand_local.weekday()):
            return cand_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    return None  # не должно случаться для рекуррентных


async def reminders_worker(app: Application) -> None:
    """Раз в N секунд сканирует reminders и шлёт сработавшие в Менеджера."""
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
                # Используем _send_manager_notice не получится — нотис для
                # конкретного thread_id, а helper жёстко идёт в Менеджера.
                # Но reminders сейчас работают только в Менеджеров топик
                # (по умолчанию), так что helper подходит, если thread_id
                # совпадает с manager-target. Универсально — отправим
                # напрямую как plain notice, плюс auto-kick если это
                # Менеджеров топик.
                mgr_target = resolve_manager_topic()
                try:
                    chat = await app.bot.get_chat(rchat)
                    sent = await send_to_topic(chat, rthread, notice)
                    msg_id = sent.message_id if sent is not None else None
                    log_message(rchat, rthread, "out", "reminder", notice, msg_id)
                except Exception:
                    logger.exception("reminders_worker: send failed id=%s", rid)
                    # Не пересчитываем next_fire_at — попробуем в следующем цикле.
                    continue

                # Auto-kick если напоминание в топик Менеджера.
                if mgr_target and (rchat, rthread) == mgr_target:
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


async def _send_manager_notice(
    app: Application, text: str, kind: str = "job_notification",
) -> int | None:
    """Шлёт plain-сообщение в топик Менеджера, логирует и будит Менеджера.

    Помимо доставки plain-сообщения через Telegram, ставит auto-job
    source='self_notice' в очередь Менеджера, чтобы worker запустил его
    LLM-сессию и тот прочитал свой inbox. Дедуп: если у Менеджера уже
    есть pending/in_progress job — не создаём, он обработает все
    свежие нотисы вместе при текущем запуске.

    Используется safety-нотисом из _run_manager_job, health_worker'ом,
    и manager_interrupt event'ом. Возвращает telegram_message_id или None
    при сбое (например, бот не админ в группе).
    """
    mgr_target = resolve_manager_topic()
    if not mgr_target:
        return None
    chat_id, thread_id = mgr_target
    try:
        chat = await app.bot.get_chat(chat_id)
        sent = await send_to_topic(chat, thread_id, text)
        msg_id = sent.message_id if sent is not None else None
        log_message(chat_id, thread_id, "out", kind, text, msg_id)
    except Exception:
        logger.exception("_send_manager_notice failed (kind=%s)", kind)
        return None

    # Auto-kick: создать job для Менеджера, чтобы он сам активировался и
    # обработал свежие нотисы. С дедупом — один job на всю серию нотисов
    # пока Менеджер их не разгребёт.
    try:
        now = datetime.utcnow().isoformat()
        with _db() as conn:
            existing = conn.execute(
                "SELECT COUNT(*) FROM jobs "
                "WHERE chat_id = ? AND thread_id = ? "
                "AND status IN ('pending', 'in_progress')",
                (chat_id, thread_id),
            ).fetchone()
            if existing and existing[0] > 0:
                return msg_id
            conn.execute(
                "INSERT INTO jobs(chat_id, thread_id, text, source, status, "
                "created_at) VALUES (?, ?, ?, 'self_notice', 'pending', ?)",
                (
                    chat_id, thread_id,
                    "[AUTO-KICK] В твой топик пришли новые нотисы от бота. "
                    "Прочитай свой manager_inbox(thread_id=<свой>) и решай "
                    "что с ними делать.",
                    now,
                ),
            )
        logger.info(
            "manager auto-kick job created chat=%s thread=%s", chat_id, thread_id,
        )
    except Exception:
        logger.exception("auto-kick INSERT failed (kind=%s)", kind)
    return msg_id


def resolve_manager_topic() -> tuple[int, int] | None:
    """Return (chat_id, thread_id) of the Manager's topic, or None.

    Used to inject the «report back to Manager» instruction into the SYSTEM
    NOTE of delegated jobs. Falls back to a SQL lookup so the bot works out
    of the box for the default Shevartv setup; explicit env vars override
    for unusual deployments.
    """
    raw_chat = os.environ.get("JARVIS_MANAGER_CHAT_ID")
    raw_thread = os.environ.get("JARVIS_MANAGER_THREAD_ID")
    if raw_chat and raw_thread:
        try:
            return int(raw_chat), int(raw_thread)
        except ValueError:
            logger.warning(
                "JARVIS_MANAGER_{CHAT,THREAD}_ID not int: %r/%r",
                raw_chat, raw_thread,
            )
    try:
        with _db() as conn:
            row = conn.execute(
                "SELECT chat_id, thread_id FROM sessions "
                "WHERE thread_id > 0 AND cwd LIKE '%/knowledge-base/manager' "
                "ORDER BY updated_at DESC LIMIT 1"
            ).fetchone()
        if row:
            return row[0], row[1]
    except Exception:
        logger.exception("resolve_manager_topic failed")
    return None


def save_message_context(chat_id: int, message_id: int, ctx: dict) -> None:
    with _db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO messages(chat_id, message_id, context_json, created_at) "
            "VALUES (?, ?, ?, ?)",
            (chat_id, message_id, json.dumps(ctx, ensure_ascii=False),
             datetime.utcnow().isoformat()),
        )


def load_message_context(chat_id: int, message_id: int) -> dict | None:
    with _db() as conn:
        row = conn.execute(
            "SELECT context_json, created_at FROM messages WHERE chat_id = ? AND message_id = ?",
            (chat_id, message_id),
        ).fetchone()
        if not row:
            return None
        try:
            ctx = json.loads(row[0])
        except json.JSONDecodeError:
            return None
        ctx["_created_at"] = row[1]
        return ctx


# ---------- Сессии ----------

def get_session(chat_id: int, thread_id: int) -> tuple[str, str | None, str]:
    """Возвращает (session_id, cwd, engine_name) для топика.

    Если записи нет — создаёт новую под дефолтный движок (DEFAULT_ENGINE).
    Engine хранится per-topic; при смене JARVIS_ENGINE существующие топики
    продолжают работать со своим движком, переключение — через /engine.
    """
    now = datetime.utcnow().isoformat()
    with _db() as conn:
        row = conn.execute(
            "SELECT session_id, cwd, engine FROM sessions "
            "WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        if row:
            session_id, cwd, engine_in_db = row
            return session_id, cwd, engine_in_db
        new_id = DEFAULT_ENGINE.new_session_id()
        conn.execute(
            "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, updated_at) "
            "VALUES (?, ?, ?, NULL, ?, ?)",
            (chat_id, thread_id, new_id, DEFAULT_ENGINE_NAME, now),
        )
        return new_id, None, DEFAULT_ENGINE_NAME


def update_session_id(
    chat_id: int, thread_id: int, expected_engine: str, new_session_id: str,
) -> None:
    """Обновляет session_id, только если в БД для топика всё ещё лежит
    `expected_engine`. Используется codex/opencode-движком: при первом запуске
    они отдают реальный id в stream'е, мы подменяем placeholder. Условие
    `engine = expected_engine` защищает от race с командой /engine: если
    пользователь успел переключиться на другой движок, старый стрим не должен
    перезаписывать новый id."""
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET session_id = ?, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ? AND engine = ?",
            (new_session_id, datetime.utcnow().isoformat(),
             chat_id, thread_id, expected_engine),
        )


def reset_session(chat_id: int, thread_id: int) -> tuple[str, str | None, str]:
    """Новый id для движка топика; cwd и engine сохраняются. Если записи
    нет — создаётся под DEFAULT_ENGINE."""
    now = datetime.utcnow().isoformat()
    with _db() as conn:
        row = conn.execute(
            "SELECT cwd, engine FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        if row:
            cwd, engine_name = row
        else:
            cwd, engine_name = None, DEFAULT_ENGINE_NAME
        engine = get_engine_by_name(engine_name)
        new_id = engine.new_session_id()
        conn.execute(
            "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(chat_id, thread_id) DO UPDATE SET "
            "session_id=excluded.session_id, updated_at=excluded.updated_at",
            (chat_id, thread_id, new_id, cwd, engine_name, now),
        )
    return new_id, cwd, engine_name


def set_engine(
    chat_id: int, thread_id: int, new_engine_name: str, model: str | None = None,
) -> tuple[str, str | None]:
    """Меняет движок топика: создаёт новый session_id под новый движок, cwd
    сохраняется, model записывается явно (NULL допустим). Если записи не было —
    создаётся. Возвращает (session_id, cwd).

    Engine-проверка (поддерживается ли имя) — на стороне get_engine_by_name."""
    new_engine = get_engine_by_name(new_engine_name)
    new_id = new_engine.new_session_id()
    now = datetime.utcnow().isoformat()
    with _db() as conn:
        row = conn.execute(
            "SELECT cwd FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        cwd = row[0] if row else None
        conn.execute(
            "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, model, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(chat_id, thread_id) DO UPDATE SET "
            "session_id=excluded.session_id, engine=excluded.engine, "
            "model=excluded.model, updated_at=excluded.updated_at",
            (chat_id, thread_id, new_id, cwd, new_engine.name, model, now),
        )
    return new_id, cwd


def get_model(chat_id: int, thread_id: int) -> str | None:
    """Возвращает выбранную для топика модель (или None — дефолт движка)."""
    with _db() as conn:
        row = conn.execute(
            "SELECT model FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
    if not row:
        return None
    return row[0] or None


def update_actual_model(
    chat_id: int, thread_id: int, engine_name: str, model: str | None,
) -> None:
    """Сохранить реальную модель, которой ответил CLI в последнем запуске.

    Engine-проверка как у update_session_id — защита от race с /engine: если
    оператор успел переключиться, прошлый стрим не должен перезаписать
    actual_model нового движка.
    """
    if not model:
        return
    try:
        with _db() as conn:
            conn.execute(
                "UPDATE sessions SET actual_model = ? "
                "WHERE chat_id = ? AND thread_id = ? AND engine = ?",
                (model, chat_id, thread_id, engine_name),
            )
    except Exception:
        logger.exception(
            "update_actual_model failed chat=%s thread=%s engine=%s",
            chat_id, thread_id, engine_name,
        )


def get_actual_model(chat_id: int, thread_id: int) -> str | None:
    """Возвращает последнюю реально использованную моделью или None."""
    with _db() as conn:
        row = conn.execute(
            "SELECT actual_model FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
    if not row:
        return None
    return row[0] or None


def update_model_only(chat_id: int, thread_id: int, model: str | None) -> bool:
    """Меняет только модель текущего движка топика, не трогая session_id.

    Используется, когда оператор тыкает в свой же активный движок и
    выбирает другую модель. Контекст сессии сохраняется (тот же jsonl).
    Возвращает True если запись существовала.
    """
    with _db() as conn:
        cur = conn.execute(
            "UPDATE sessions SET model = ?, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ?",
            (model, datetime.utcnow().isoformat(), chat_id, thread_id),
        )
    return cur.rowcount == 1


def set_pending_summary(chat_id: int, thread_id: int, summary: str) -> None:
    """Сохраняет резюме предыдущей сессии — будет доставлено в первый prompt
    после переключения движка."""
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET pending_summary = ?, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ?",
            (summary, datetime.utcnow().isoformat(), chat_id, thread_id),
        )


def pop_pending_summary(chat_id: int, thread_id: int) -> str | None:
    """Atomically: вернуть pending_summary и очистить его. Возвращает None,
    если резюме нет."""
    with _db() as conn:
        row = conn.execute(
            "SELECT pending_summary FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        if not row or not row[0]:
            return None
        summary = row[0]
        conn.execute(
            "UPDATE sessions SET pending_summary = NULL, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ?",
            (datetime.utcnow().isoformat(), chat_id, thread_id),
        )
        return summary


_TRANSFER_MARKER_PREFIX = '{"transfer_requested":true'


def _is_transfer_marker(pending: str) -> bool:
    """True если pending_summary — JSON-маркер запроса на генерацию summary."""
    return pending.startswith(_TRANSFER_MARKER_PREFIX)


def _parse_transfer_marker(pending: str) -> dict | None:
    """Распарсить JSON-маркер {transfer_requested, old_engine, old_session_id}
    или вернуть None, если не маркер / невалидный JSON."""
    try:
        data = json.loads(pending)
    except (json.JSONDecodeError, TypeError):
        return None
    if data.get("transfer_requested") and data.get("old_engine"):
        return data
    return None


async def _resolve_pending_summary(
    key: tuple[int, int], pending: str,
) -> str | None:
    """Если pending — маркер transfer_requested: вызывает старый движок для
    генерации summary, сохраняет его в БД и возвращает текст summary.
    Иначе возвращает pending как есть (готовое summary)."""
    marker = _parse_transfer_marker(pending)
    if marker is None:
        return pending  # обычное summary, уже готово

    chat_id, thread_id = key
    old_engine_name = marker["old_engine"]
    old_session_id = marker.get("old_session_id")
    _, cwd, _ = get_session(*key)
    old_model = marker.get("old_model")

    logger.info(
        "resolving transfer marker: key=%s old_engine=%s old_sid=%s",
        key, old_engine_name, old_session_id,
    )

    # Попросим старый движок выдать summary. Если старый движок недоступен —
    # просто вернём None (контекст не перенесётся, но сессия уже переключена).
    try:
        get_engine_by_name(old_engine_name)
    except Exception:
        logger.warning(
            "old engine %s unavailable for transfer marker resolution key=%s",
            old_engine_name, key,
        )
        return None

    summary = await _generate_handoff_summary(key, old_engine_name, cwd, old_model)
    if summary:
        set_pending_summary(chat_id, thread_id, summary)
        logger.info(
            "transfer marker resolved: stored real summary for key=%s (%d chars)",
            key, len(summary),
        )
        return summary
    return None


def set_cwd(chat_id: int, thread_id: int, cwd: str) -> None:
    """Создаёт запись, если её нет (session_id — новый id для дефолтного движка),
    либо обновляет cwd."""
    now = datetime.utcnow().isoformat()
    with _db() as conn:
        row = conn.execute(
            "SELECT session_id FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        if row:
            conn.execute(
                "UPDATE sessions SET cwd = ?, updated_at = ? WHERE chat_id = ? AND thread_id = ?",
                (cwd, now, chat_id, thread_id),
            )
        else:
            conn.execute(
                "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (chat_id, thread_id, DEFAULT_ENGINE.new_session_id(), cwd,
                 DEFAULT_ENGINE_NAME, now),
            )


def clear_cwd(chat_id: int, thread_id: int) -> None:
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET cwd = NULL, updated_at = ? WHERE chat_id = ? AND thread_id = ?",
            (datetime.utcnow().isoformat(), chat_id, thread_id),
        )


# ---------- Ключ per-topic ----------

def _key(update: Update) -> tuple[int, int]:
    chat_id = update.effective_chat.id
    msg = update.message or update.effective_message
    thread_id = 0
    if msg is not None and getattr(msg, "is_topic_message", False):
        thread_id = msg.message_thread_id or 0
    return chat_id, thread_id


chat_locks: dict[tuple[int, int], asyncio.Lock] = {}
active_procs: dict[tuple[int, int], asyncio.subprocess.Process] = {}
# Отдельный реестр для /spawn: key=(chat_id, thread_id, spawn_id_hex).
# Основной /stop не трогает эти процессы; снять spawn можно через /stop <spawn_id>.
spawn_procs: dict[tuple[int, int, str], asyncio.subprocess.Process] = {}


def _lock_for(key: tuple[int, int]) -> asyncio.Lock:
    lock = chat_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        chat_locks[key] = lock
    return lock


# Реестр отменяемых ожидающих запросов: queue_id -> asyncio.Event.
# Когда запрос ждёт освобождения lock'а топика, в реестре лежит его event.
# Callback "cancel_queue:<queue_id>" выставляет event, ожидающая корутина видит
# это и выходит, не захватывая lock и не вызывая claude.
# Когда запрос уже начал выполняться (lock захвачен) — его id удаляется из реестра;
# попытка отменить в этот момент отвечает пользователю «используй /stop».
pending_queue: dict[str, asyncio.Event] = {}


# ---------- Markdown → HTML для Telegram ----------

def _html_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ```lang\n...\n```  (multiline) или ```...```
_FENCE_RE = re.compile(r"```([A-Za-z0-9_+\-]*)\n?(.*?)```", re.DOTALL)
_INLINE_CODE_RE = re.compile(r"`([^`\n]+)`")
_BOLD_RE = re.compile(r"\*\*(.+?)\*\*", re.DOTALL)
_ITALIC_RE = re.compile(r"(?<![\*A-Za-z0-9])\*(?!\s)(.+?)(?<!\s)\*(?![\*A-Za-z0-9])", re.DOTALL)


def md_to_html(text: str) -> str:
    """Конвертирует упрощённый markdown от claude в HTML, понятный Telegram.
    Поддерживает: ```code blocks``` (с языком), `inline`, **bold**, *italic*.
    Всё, что вне кода, экранируется (<, >, &); внутри кода — тоже."""
    placeholders: list[str] = []

    def _stash(html: str) -> str:
        placeholders.append(html)
        return f"\x00PH{len(placeholders) - 1}\x00"

    def _fence(m: re.Match) -> str:
        lang = m.group(1) or ""
        body = m.group(2)
        body_esc = _html_escape(body)
        if lang:
            return _stash(f'<pre><code class="language-{_html_escape(lang)}">{body_esc}</code></pre>')
        return _stash(f"<pre><code>{body_esc}</code></pre>")

    def _inline(m: re.Match) -> str:
        return _stash(f"<code>{_html_escape(m.group(1))}</code>")

    text = _FENCE_RE.sub(_fence, text)
    text = _INLINE_CODE_RE.sub(_inline, text)
    text = _html_escape(text)
    text = _BOLD_RE.sub(lambda m: f"<b>{m.group(1)}</b>", text)
    text = _ITALIC_RE.sub(lambda m: f"<i>{m.group(1)}</i>", text)

    def _restore(m: re.Match) -> str:
        return placeholders[int(m.group(1))]

    return re.sub(r"\x00PH(\d+)\x00", _restore, text)


def split_html_for_telegram(html: str, limit: int = TG_HARD_LIMIT) -> list[str]:
    """Бьёт HTML на куски ≤ limit, не разрывая открытые <pre>/<code>.
    Стратегия: режем по \\n, если внутри куска остался незакрытый <pre><code> —
    закрываем в конце куска и переоткрываем в начале следующего."""
    if len(html) <= limit:
        return [html]
    # Делим по строкам.
    lines = html.split("\n")
    chunks: list[str] = []
    cur = ""
    for line in lines:
        candidate = (cur + "\n" + line) if cur else line
        if len(candidate) <= limit:
            cur = candidate
            continue
        if cur:
            chunks.append(cur)
        # Если сама строка длиннее лимита — режем грубо по символам.
        while len(line) > limit:
            chunks.append(line[:limit])
            line = line[limit:]
        cur = line
    if cur:
        chunks.append(cur)
    # Балансируем <pre><code> между чанками.
    balanced: list[str] = []
    open_pre = False
    for ch in chunks:
        prefix = "<pre><code>" if open_pre else ""
        body = prefix + ch
        # Простой подсчёт: count open vs close <pre>.
        opens = body.count("<pre>")
        closes = body.count("</pre>")
        if opens > closes:
            body += "</code></pre>"
            open_pre = True
        else:
            open_pre = False
        balanced.append(body)
    return balanced


# ---------- Отправка в топик ----------

async def _send_with_html_fallback(send_func, text: str, **kwargs):
    """Шлёт text как HTML; при ошибке парсинга — повторяет без parse_mode (plain)."""
    try:
        return await send_func(text=text, parse_mode=ParseMode.HTML, **kwargs)
    except BadRequest as exc:
        if "parse" in str(exc).lower() or "entit" in str(exc).lower():
            logger.warning("HTML parse failed, falling back to plain: %s", exc)
            kwargs.pop("parse_mode", None)
            return await send_func(text=text, **kwargs)
        raise


async def send_to_topic(chat, thread_id: int, text: str, **kwargs):
    if thread_id:
        kwargs.setdefault("message_thread_id", thread_id)
    return await chat.send_message(text=text, **kwargs)


async def send_document_to_topic(chat, thread_id: int, document, **kwargs):
    if thread_id:
        kwargs.setdefault("message_thread_id", thread_id)
    return await chat.send_document(document=document, **kwargs)


def extract_file_markers(text: str) -> tuple[str, list[tuple[str, str | None]]]:
    """Парсит маркеры [[FILE: /path]] / [[FILE: /path | caption]] на отдельных строках.
    Возвращает (текст без маркеров, список (path, caption|None))."""
    markers: list[tuple[str, str | None]] = []

    def _collect(m: re.Match) -> str:
        path = m.group("path").strip()
        cap = m.group("caption")
        cap = cap.strip() if cap else None
        markers.append((path, cap))
        return ""

    cleaned = FILE_MARKER_RE.sub(_collect, text)
    # Подчищаем пустые строки, оставшиеся после вырезания маркеров.
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned, markers


async def deliver_file_markers(
    chat,
    thread_id: int,
    markers: list[tuple[str, str | None]],
    notice_prefix: str = "",
) -> None:
    """Отправляет файлы по списку маркеров. Ошибки сообщает текстом в тот же топик
    (с notice_prefix, например '[#xxxx] '). Сами файлы шлёт без префикса."""
    for path, caption in markers:
        try:
            if not os.path.isabs(path):
                await send_to_topic(
                    chat, thread_id,
                    f"{notice_prefix}⚠️ путь не абсолютный: {path}",
                )
                continue
            if not os.path.exists(path):
                await send_to_topic(
                    chat, thread_id,
                    f"{notice_prefix}⚠️ файл {path} не найден",
                )
                continue
            if not os.path.isfile(path):
                await send_to_topic(
                    chat, thread_id,
                    f"{notice_prefix}⚠️ {path} не является обычным файлом",
                )
                continue
            size = os.path.getsize(path)
            size_mb = size / (1024 * 1024)
            if size_mb > TG_FILE_LIMIT_MB:
                await send_to_topic(
                    chat, thread_id,
                    f"{notice_prefix}⚠️ файл {path} слишком большой "
                    f"({size_mb:.1f} MB), лимит Telegram {TG_FILE_LIMIT_MB} MB",
                )
                continue
            kwargs = {"filename": os.path.basename(path)}
            if caption:
                kwargs["caption"] = caption[:1024]
            with open(path, "rb") as fh:
                await send_document_to_topic(chat, thread_id, document=fh, **kwargs)
            logger.info("delivered file marker: %s (%.1f MB)", path, size_mb)
        except Exception as exc:
            logger.exception("deliver_file_markers failed for %s", path)
            try:
                await send_to_topic(
                    chat, thread_id,
                    f"{notice_prefix}⚠️ не удалось отправить {path}: {exc}",
                )
            except Exception:
                pass


async def send_claude_reply(
    chat, thread_id: int, text: str, meta: dict, filename_prefix: str = "reply",
    html_prefix: str = "",
):
    """Короткий текст — send_message с HTML-форматированием; длинный — .md вложение.
    `html_prefix` (например '[#xxxx] ') добавляется как уже готовый HTML-фрагмент
    перед сконвертированным телом."""
    log_kind = "spawn_reply" if meta.get("spawn_id") else "bot_reply"

    if len(text) <= MSG_LIMIT:
        html_body = html_prefix + md_to_html(text)
        chunks = split_html_for_telegram(html_body, TG_HARD_LIMIT)
        send_kwargs = {}
        if thread_id:
            send_kwargs["message_thread_id"] = thread_id
        sent = None
        for chunk in chunks:
            sent = await _send_with_html_fallback(chat.send_message, chunk, **send_kwargs)
        try:
            if sent is not None:
                save_message_context(chat.id, sent.message_id, meta)
                log_message(
                    chat.id, thread_id, "out", log_kind, text, sent.message_id,
                )
        except Exception:
            logger.exception("save_message_context failed")
        return sent

    plain_prefix = re.sub(r"<[^>]+>", "", html_prefix) if html_prefix else ""
    preview = plain_prefix + text[:200].rstrip() + "...\n\nполный ответ во вложении"
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".md", delete=False, encoding="utf-8", dir=MEDIA_DIR,
    ) as f:
        f.write(text)
        path = f.name
    try:
        with open(path, "rb") as fh:
            sent = await send_document_to_topic(
                chat, thread_id,
                document=fh,
                filename=f"{filename_prefix}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.md",
                caption=preview[:1024],
            )
        try:
            save_message_context(chat.id, sent.message_id, meta)
            log_message(
                chat.id, thread_id, "out", log_kind, text,
                sent.message_id if sent is not None else None,
            )
        except Exception:
            logger.exception("save_message_context failed")
        return sent
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


# ---------- Вызов LLM CLI (stream) ----------

async def call_llm_stream(
    engine: Engine,
    session_id: str,
    prompt: str,
    key: tuple[int, int],
    cwd: str | None,
    on_intermediate,
    spawn_id: str | None = None,
) -> tuple[bool, str, str | None]:
    """Обёртка над engine.call_stream. Обновляет session_id в БД, если движок
    вернул изменённый id (актуально для codex/opencode — они сами назначают
    реальный id при первом запуске). Для spawn'а id в БД не сохраняется.

    Также сохраняет actual_model в sessions — реальное имя модели,
    которым CLI ответил (из stream-events). Это позволяет /session
    показать точную модель, не догадки.

    Возвращает (ok, final_text, session_id_after).
    """
    mcp_ok, mcp_status = ensure_engine_tools(engine)
    if not mcp_ok:
        logger.warning("engine=%s activated without Playwright MCP: %s", engine.name, mcp_status)
    ok, final_text, sid_after, actual_model = await engine.call_stream(
        session_id=session_id,
        prompt=prompt,
        key=key,
        cwd=cwd,
        on_intermediate=on_intermediate,
        active_procs=active_procs,
        spawn_procs=spawn_procs,
        spawn_id=spawn_id,
    )
    # Для постоянной сессии (не spawn) — если движок отдал новый id, сохраняем.
    if spawn_id is None and sid_after and sid_after != session_id:
        try:
            update_session_id(key[0], key[1], engine.name, sid_after)
            logger.info(
                "session_id updated by engine=%s for key=%s: %s -> %s",
                engine.name, key, session_id, sid_after,
            )
        except Exception:
            logger.exception("failed to persist new session_id from engine")
    # Реальная модель — сохраняем в sessions для /session и manager_topics.
    if spawn_id is None and actual_model:
        update_actual_model(key[0], key[1], engine.name, actual_model)
    return ok, final_text, sid_after


# ---------- Reply-to контекст ----------

def _build_reply_context_prefix(ctx: dict) -> str:
    parts = []
    t = ctx.get("type")
    created = ctx.get("_created_at", "")
    if t == "claude_response":
        parts.append(f"пользователь отвечает на твой предыдущий ответ (время {created})")
    else:
        parts.append(f"пользователь отвечает на твоё сообщение типа {t!r} (время {created})")
        extras = {k: v for k, v in ctx.items() if k not in ("type", "_created_at")}
        if extras:
            parts.append(f"метаданные: {json.dumps(extras, ensure_ascii=False)}")
    return "[Пользователь отвечает на:] " + "; ".join(parts)


# ---------- Handlers: команды ----------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    key = _key(update)
    session_id, cwd, engine_name = get_session(*key)
    model = get_model(*key)
    effective = cwd or CLAUDE_CWD
    model_line = f"Модель: `{model}`\n" if model else ""
    text = (
        f"Привет! Я Jarvis — Telegram-обёртка над LLM CLI.\n\n"
        f"Движок этого топика: `{engine_name}` (дефолт: `{DEFAULT_ENGINE_NAME}`)\n"
        f"{model_line}"
        f"session-id: `{session_id}`\n"
        f"Рабочая директория: `{effective}`" + (" (дефолт)" if not cwd else "") + "\n\n"
        "Команды:\n"
        "/engine [name] — показать/переключить движок (claude|codex|opencode)\n"
        "/new, /reset — новая сессия (cwd сохраняется)\n"
        "/stop — прервать текущий запрос (сессия сохраняется)\n"
        "/session — показать session-id, cwd и движок\n"
        "/bind <path> — привязать топик к директории\n"
        "/unbind — сбросить привязку к дефолту\n"
        "/where — эффективный cwd"
    )
    await update.message.reply_text(text)


async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    key = _key(update)
    # Прерываем активный процесс
    proc = active_procs.get(key)
    if proc is not None:
        await terminate_process_tree(proc)
        logger.info("reset: killed active proc for key=%s", key)
    new_id, cwd, engine_name = reset_session(*key)
    effective = cwd or CLAUDE_CWD
    logger.info("reset: key=%s engine=%s new_session=%s cwd=%s",
                key, engine_name, new_id, effective)
    await update.message.reply_text(
        f"Сессия сброшена ({engine_name}), новый id: {new_id}\nCwd: {effective}"
    )


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    key = _key(update)
    args = context.args or []
    # /stop <spawn_id> — прибить конкретный spawn в этом топике.
    if args:
        spawn_id = args[0].strip().lower().lstrip("#")
        skey = (key[0], key[1], spawn_id)
        sproc = spawn_procs.get(skey)
        if sproc is None or sproc.returncode is not None:
            await update.message.reply_text(f"Spawn [#{spawn_id}] не найден или уже завершён.")
            return
        await terminate_process_tree(sproc)
        spawn_procs.pop(skey, None)
        await update.message.reply_text(f"⛔ Spawn [#{spawn_id}] прерван.")
        return

    proc = active_procs.get(key)
    if proc is None or proc.returncode is not None:
        await update.message.reply_text(
            "Нет активного запроса в этом топике."
            + (f"\nАктивные spawn'ы: {', '.join('#' + s[2] for s in spawn_procs if s[:2] == key)}"
               if any(s[:2] == key for s in spawn_procs) else "")
        )
        return
    pid = proc.pid
    await terminate_process_tree(proc)
    # Достаём session_id и engine из БД, чистим pidfile, чтобы следующий запрос
    # не упёрся в "session in use".
    session_id, _, engine_name = get_session(*key)
    engine = get_engine_by_name(engine_name)
    engine.clear_stale_session_pidfile(session_id)
    active_procs.pop(key, None)
    logger.info("stop: killed proc pid=%s for key=%s, cleaned pidfile for %s (engine=%s)",
                pid, key, session_id, engine_name)
    await update.message.reply_text("⛔ Текущий запрос прерван. Сессия сохранена.")


def _topic_status_block(key: tuple[int, int]) -> str:
    """HTML-блок со статусом топика для /session и /engine.

    Pre-форматированный, моноширинный. Показывает engine, выбранную модель
    (что хотим) и реально использованную (что CLI сообщил последний раз),
    session-id, cwd.
    """
    session_id, cwd, engine_name = get_session(*key)
    model = get_model(*key)
    actual = get_actual_model(*key)
    effective_cwd = cwd or CLAUDE_CWD
    cwd_suffix = "" if cwd else " (дефолт)"
    if model and actual and model.lower() == actual.lower():
        model_line = actual
    elif model and actual:
        model_line = f"{actual}  (выбрано: {model})"
    elif actual:
        model_line = f"{actual}  (дефолт движка)"
    elif model:
        model_line = f"{model}  (ожидается, ещё не отвечал)"
    else:
        model_line = "(дефолт движка; ещё ни разу не отвечал)"
    body = (
        f"engine     : {engine_name}\n"
        f"model      : {model_line}\n"
        f"session-id : {session_id}\n"
        f"cwd        : {effective_cwd}{cwd_suffix}"
    )
    return "<pre>" + _html_escape(body) + "</pre>"


async def cmd_session(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    key = _key(update)
    await update.message.reply_text(
        _topic_status_block(key), parse_mode=ParseMode.HTML,
    )


def _engine_keyboard(current_engine: str) -> InlineKeyboardMarkup:
    """Inline-клавиатура с кнопками выбора движка. Текущий помечается ✓."""
    row = []
    for name in SUPPORTED_ENGINES:
        label = f"✓ {name}" if name == current_engine else name
        row.append(InlineKeyboardButton(label, callback_data=f"engine_select:{name}"))
    return InlineKeyboardMarkup([row])


def _model_label(model: str) -> str:
    """Сокращение для отображения: 'deepseek/deepseek-v4-flash' → 'deepseek-v4-flash'."""
    return model.split("/", 1)[-1] if "/" in model else model


def _model_keyboard(engine_name: str, models: list[str]) -> InlineKeyboardMarkup:
    """Список моделей движка — по одной в строке, callback_data использует
    индекс модели в списке (не имя), чтобы не упереться в 64-байтный лимит
    callback_data при длинных идентификаторах."""
    rows = []
    for idx, model in enumerate(models):
        rows.append([
            InlineKeyboardButton(
                _model_label(model),
                callback_data=f"model_select:{engine_name}:{idx}",
            )
        ])
    return InlineKeyboardMarkup(rows)


def _carry_keyboard(
    old_engine: str, new_engine: str, model_idx: int | None = None,
) -> InlineKeyboardMarkup:
    """Inline-клавиатура «перенести контекст?». В callback_data зашивается
    выбранная модель целевого движка (индексом) — чтобы переключение и выбор
    модели атомарно прилетели в `on_engine_carry`. Для движков без моделей —
    `-` вместо индекса."""
    mtoken = "-" if model_idx is None else str(model_idx)
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "✅ Да, с резюме",
            callback_data=f"engine_carry:{old_engine}:{new_engine}:{mtoken}:y",
        ),
        InlineKeyboardButton(
            "🚫 Нет, чисто",
            callback_data=f"engine_carry:{old_engine}:{new_engine}:{mtoken}:n",
        ),
    ]])


def _engine_precheck(key: tuple[int, int], target: str) -> tuple[bool, str, str | None]:
    """Проверяет переключение ДО действий. Возвращает (ok, message, current_engine).
    current_engine != None даже при ok=False (если запись в БД есть)."""
    available = ", ".join(SUPPORTED_ENGINES)
    if target not in SUPPORTED_ENGINES:
        return False, f"Неизвестный движок: {target!r}. Доступны: {available}.", None

    _, _, current_engine = get_session(*key)
    if target == current_engine:
        return False, (
            f"Этот топик уже на движке `{target}`. /new — если нужна свежая сессия."
        ), current_engine

    target_engine = get_engine_by_name(target)
    if shutil.which(target_engine.bin_path) is None:
        return False, (
            f"⚠️ Бинарь `{target_engine.bin_path}` не найден в PATH. "
            f"Установи {target!r} CLI или задай путь через "
            f"{target.upper()}_BIN, перезапусти бота."
        ), current_engine

    return True, "", current_engine


def _resolve_target_model(target: str, model_idx: int | None) -> str | None:
    """По индексу из callback_data выдаёт реальное имя модели целевого движка.
    Контракт: если у движка нет моделей — None; если одна — она; иначе — по idx."""
    target_engine = get_engine_by_name(target)
    models = list(target_engine.models)
    if not models:
        return None
    if len(models) == 1:
        return models[0]
    if model_idx is None or model_idx < 0 or model_idx >= len(models):
        return None
    return models[model_idx]


async def _do_engine_switch(
    key: tuple[int, int], target: str, model: str | None = None,
) -> str:
    """Финальное действие переключения (без pre-check, который уже сделан вызывающим).
    Прерывает активный процесс, создаёт новый session_id, сохраняет model (или
    NULL для движков без моделей), возвращает текст ответа."""
    _, _, current_engine = get_session(*key)
    target_engine = get_engine_by_name(target)
    mcp_ok, mcp_status = ensure_engine_tools(target_engine)

    proc = active_procs.get(key)
    if proc is not None:
        await terminate_process_tree(proc)
        active_procs.pop(key, None)
        logger.info("engine switch: killed active proc for key=%s", key)

    new_id, cwd = set_engine(key[0], key[1], target, model=model)
    effective = cwd or CLAUDE_CWD
    logger.info("engine switched for key=%s: %s -> %s (new sid=%s, model=%s)",
                key, current_engine, target, new_id, model)
    mcp_line = f"\n{mcp_status}" if mcp_ok else f"\n⚠️ {mcp_status}"
    model_line = f"\nМодель: {model}" if model else ""
    return (
        f"🔁 Движок переключён: {current_engine} → {target}"
        f"{model_line}\n"
        f"Новая сессия: {new_id}\n"
        f"Cwd сохранён: {effective}"
        f"{mcp_line}"
    )


async def _generate_handoff_summary(
    key: tuple[int, int], old_engine_name: str, cwd: str | None, old_model: str | None,
) -> str | None:
    """Попросить старый движок сгенерировать summary диалога.
    Возвращает очищенный текст (до 4000 символов) или None при ошибке."""
    old_engine = get_engine_by_name(old_engine_name)
    old_session_id, _, _ = get_session(*key)
    effective_cwd = cwd or CLAUDE_CWD

    summary_prompt = (
        _system_prefix(effective_cwd)
        + "\n\n---\n\n"
        "Сделай краткое резюме нашего диалога для передачи другому LLM-агенту, "
        "который продолжит разговор вместо тебя. Включи: 1) цель/задачу, "
        "2) текущий статус и ключевые решения, 3) что уже выяснено или сделано, "
        "4) открытые вопросы и следующий шаг. До 2000 символов. Без преамбул "
        "и заключений — только сам summary, чтобы агент сразу понял контекст."
    )

    async def on_intermediate(_text: str) -> None:
        pass

    try:
        with engine_model_scope(old_engine.name, old_model):
            ok, summary_text, _sid_after = await call_llm_stream(
                old_engine, old_session_id, summary_prompt, key, cwd, on_intermediate,
            )
    except Exception as exc:
        logger.exception("handoff summary call crashed: key=%s engine=%s",
                         key, old_engine_name)
        return None

    if not ok or not summary_text.strip():
        logger.warning("handoff summary empty: key=%s engine=%s", key, old_engine_name)
        return None

    cleaned, _ = extract_file_markers(summary_text)
    cleaned = cleaned.strip() or summary_text.strip()
    cleaned = cleaned[:4000]
    return cleaned


async def _do_engine_handoff(
    key: tuple[int, int],
    old_engine_name: str,
    new_engine_name: str,
    progress_edit,
    model: str | None = None,
) -> str:
    """Сценарий «с переносом»: 1) lock топика, 2) попросить старый движок
    выдать summary, 3) сохранить в pending_summary, 4) переключить движок.
    `progress_edit(text)` — async-функция для обновления карточки в чате."""
    chat_id, thread_id = key
    old_model = get_model(*key)
    _, cwd, _ = get_session(*key)

    lock = _lock_for(key)
    if lock.locked():
        return (
            "⚠️ Топик занят активным запросом. Дождись завершения или /stop, "
            "потом повтори переключение."
        )

    await lock.acquire()
    try:
        await progress_edit(f"🧠 Снимаю резюме сессии у {old_engine_name}...")

        cleaned = await _generate_handoff_summary(key, old_engine_name, cwd, old_model)
        if cleaned is None:
            return (
                f"❌ {old_engine_name} не вернул резюме. Переключение отменено. "
                "Можешь попробовать без переноса контекста."
            )

        await progress_edit("🔁 Переключаю движок...")
        switch_text = await _do_engine_switch(key, new_engine_name, model=model)
        set_pending_summary(chat_id, thread_id, cleaned)
        logger.info("handoff: stored pending_summary for key=%s (%d chars)",
                    key, len(cleaned))

        preview = cleaned[:200].rstrip()
        if len(cleaned) > 200:
            preview += "..."
        return (
            f"{switch_text}\n\n"
            f"📝 Резюме от {old_engine_name} сохранено и будет передано в первое "
            f"твоё сообщение новому движку.\n\n"
            f"<i>preview:</i>\n<code>{_html_escape(preview)}</code>"
        )
    finally:
        try:
            lock.release()
        except RuntimeError:
            pass


async def cmd_engine(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/engine — показать движок топика с кнопками переключения;
    /engine <name> [model-substring] [--keep-context] — переключить движок.
    С флагом --keep-context: summary-based handoff (старый движок пишет резюме,
    новый получает его в первый prompt). Без флага: чистый старт новой сессии."""
    key = _key(update)
    args = list(context.args or [])

    # Вытащим --keep-context из аргументов
    keep_context = False
    filtered: list[str] = []
    for a in args:
        if a == "--keep-context":
            keep_context = True
        else:
            filtered.append(a)
    args = filtered

    if not args:
        _, _, engine_name = get_session(*key)
        footer = _html_escape(
            f"\n\nДефолт (для новых топиков): {DEFAULT_ENGINE_NAME}\n"
            "Выбери новый движок ниже или введи /engine <name> [--keep-context]."
        )
        await update.message.reply_text(
            _topic_status_block(key) + footer,
            parse_mode=ParseMode.HTML,
            reply_markup=_engine_keyboard(engine_name),
        )
        return

    target = args[0].strip().lower()

    # Same-engine: текстовое /engine <current> <model> меняет только модель,
    # не пересоздаёт сессию. Контекст сохраняется. --keep-context не нужен.
    _, _, current_engine = get_session(*key)
    if target in SUPPORTED_ENGINES and target == current_engine and len(args) >= 2:
        target_engine = get_engine_by_name(target)
        models = list(target_engine.models)
        substr = args[1].strip().lower()
        exact = [m for m in models if m.lower() == substr]
        if exact:
            chosen = exact[0]
        else:
            matches = [m for m in models if substr in m.lower()]
            if len(matches) != 1:
                await update.message.reply_text(
                    f"Подстрока {substr!r} матчит {len(matches)} модель(и) у `{target}`. "
                    f"Доступны: {', '.join(_model_label(m) for m in models)}."
                )
                return
            chosen = matches[0]
        update_model_only(key[0], key[1], chosen)
        await update.message.reply_text(
            f"Модель движка `{target}` изменена: → {_model_label(chosen)}.\n"
            f"Контекст сессии сохранён.",
        )
        logger.info(
            "model changed in-place via /engine for key=%s engine=%s: -> %s",
            key, target, chosen,
        )
        return

    ok, msg, _ = _engine_precheck(key, target)
    if not ok:
        await update.message.reply_text(msg)
        return

    target_engine = get_engine_by_name(target)
    models = list(target_engine.models)
    chosen_model: str | None = None
    if len(models) == 1:
        chosen_model = models[0]
    elif len(models) > 1:
        if len(args) < 2:
            await update.message.reply_text(
                f"У движка `{target}` несколько моделей: "
                + ", ".join(_model_label(m) for m in models)
                + ".\nИспользуй /engine без аргументов и выбери в UI, "
                "или передай подстроку модели: /engine "
                f"{target} {_model_label(models[0])}."
            )
            return
        substr = args[1].strip().lower()
        exact = [m for m in models if m.lower() == substr]
        if len(exact) == 1:
            chosen_model = exact[0]
        else:
            matches = [m for m in models if substr in m.lower()]
            if len(matches) != 1:
                await update.message.reply_text(
                    f"Подстрока {substr!r} матчит {len(matches)} модель(и) у `{target}`. "
                    f"Доступны: {', '.join(_model_label(m) for m in models)}."
                )
                return
            chosen_model = matches[0]

    if keep_context:
        async def _progress_edit(text: str) -> None:
            pass  # из текстовой команды не можем обновлять карточку
        text = await _do_engine_handoff(
            key, current_engine, target, _progress_edit, model=chosen_model,
        )
        text = re.sub(r"<[^>]+>", "", text)
        await update.message.reply_text(text)
    else:
        text = await _do_engine_switch(key, target, model=chosen_model)
        await update.message.reply_text(text + "\nКонтекст прежнего диалога не переносится.")


async def on_engine_select(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Callback от inline-кнопки выбора движка. Не переключает сразу — после
    pre-check'а спрашивает: переносить контекст?"""
    query = update.callback_query
    if query is None:
        return
    data = query.data or ""
    if not data.startswith("engine_select:"):
        return
    target = data.split(":", 1)[1].strip().lower()
    key = _key(update)

    # Same-engine click: предлагаем смену модели вместо отказа.
    _, _, current_engine = get_session(*key)
    if target == current_engine:
        try:
            await query.answer()
        except Exception:
            pass
        target_engine = get_engine_by_name(target)
        models = list(target_engine.models)
        current_model = get_model(*key)
        if len(models) > 1:
            prompt_text = (
                f"Движок `{target}` уже активен.\n"
                f"Текущая модель: {current_model or '(дефолт движка)'}.\n"
                f"Выбери другую модель — контекст сессии сохранится:"
            )
            try:
                await query.edit_message_text(
                    prompt_text,
                    reply_markup=_model_keyboard(target, models),
                )
            except BadRequest:
                await send_to_topic(
                    update.effective_chat, key[1],
                    prompt_text,
                    reply_markup=_model_keyboard(target, models),
                )
        else:
            msg = (
                f"Движок `{target}` уже активен. "
                + (
                    f"У него только одна модель ({models[0]}), сменить не на что."
                    if models
                    else "Выбор модели для этого движка недоступен."
                )
            )
            try:
                await query.edit_message_text(
                    msg, reply_markup=_engine_keyboard(current_engine),
                )
            except BadRequest:
                await send_to_topic(update.effective_chat, key[1], msg)
        return

    ok, msg, current = _engine_precheck(key, target)
    if not ok:
        try:
            await query.answer("Не могу переключить", show_alert=False)
        except Exception:
            pass
        try:
            await query.edit_message_text(
                msg + (f"\n\n(текущий движок: {current})" if current else ""),
                reply_markup=_engine_keyboard(current) if current else None,
            )
        except BadRequest:
            await send_to_topic(update.effective_chat, key[1], msg)
        return

    try:
        await query.answer()
    except Exception:
        pass

    # Шаг выбора модели: только если у целевого движка их >1.
    target_engine = get_engine_by_name(target)
    models = list(target_engine.models)
    if len(models) > 1:
        prompt_text = (
            f"Движок: {current} → {target}.\n"
            f"Выбери модель {target}:"
        )
        try:
            await query.edit_message_text(
                prompt_text,
                reply_markup=_model_keyboard(target, models),
            )
        except BadRequest:
            await send_to_topic(
                update.effective_chat, key[1],
                prompt_text,
                reply_markup=_model_keyboard(target, models),
            )
        return

    # 0 или 1 модель — сразу к шагу carry. Для одной модели сохраняем её индекс,
    # чтобы on_engine_carry знал, что записать в БД.
    model_idx = 0 if len(models) == 1 else None
    try:
        await query.edit_message_text(
            f"Переключаюсь {current} → {target}.\n"
            "Перенести контекст текущего диалога в новый движок?\n"
            "(резюме старого движка будет добавлено к первому твоему сообщению)",
            reply_markup=_carry_keyboard(current, target, model_idx),
        )
    except BadRequest:
        await send_to_topic(
            update.effective_chat, key[1],
            f"Переключаюсь {current} → {target}. Перенести контекст?",
            reply_markup=_carry_keyboard(current, target, model_idx),
        )


async def on_model_select(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Callback от кнопки выбора модели. После выбора — обычный шаг про
    перенос контекста."""
    query = update.callback_query
    if query is None:
        return
    data = query.data or ""
    if not data.startswith("model_select:"):
        return
    parts = data.split(":")
    if len(parts) != 3:
        return
    _, target, idx_str = parts
    target = target.strip().lower()
    try:
        model_idx = int(idx_str)
    except ValueError:
        return
    key = _key(update)

    target_engine = get_engine_by_name(target)
    models = list(target_engine.models)
    if model_idx < 0 or model_idx >= len(models):
        try:
            await query.answer("Модель не найдена", show_alert=True)
        except Exception:
            pass
        return
    chosen = models[model_idx]

    # Same-engine: меняем только модель в БД, session_id и контекст
    # сохраняются. Carry-этап не нужен.
    _, _, current_engine = get_session(*key)
    if target == current_engine:
        update_model_only(key[0], key[1], chosen)
        try:
            await query.answer()
        except Exception:
            pass
        new_text = (
            f"Модель движка `{target}` изменена: → {_model_label(chosen)}.\n"
            f"Контекст сессии сохранён."
        )
        try:
            await query.edit_message_text(new_text)
        except BadRequest:
            await send_to_topic(update.effective_chat, key[1], new_text)
        logger.info(
            "model changed in-place for key=%s engine=%s: -> %s",
            key, target, chosen,
        )
        return

    ok, msg, current = _engine_precheck(key, target)
    if not ok:
        try:
            await query.answer("Не могу переключить", show_alert=False)
        except Exception:
            pass
        try:
            await query.edit_message_text(
                msg + (f"\n\n(текущий движок: {current})" if current else ""),
                reply_markup=_engine_keyboard(current) if current else None,
            )
        except BadRequest:
            await send_to_topic(update.effective_chat, key[1], msg)
        return

    try:
        await query.answer()
    except Exception:
        pass
    try:
        await query.edit_message_text(
            f"Переключаюсь {current} → {target} ({_model_label(chosen)}).\n"
            "Перенести контекст текущего диалога в новый движок?\n"
            "(резюме старого движка будет добавлено к первому твоему сообщению)",
            reply_markup=_carry_keyboard(current, target, model_idx),
        )
    except BadRequest:
        await send_to_topic(
            update.effective_chat, key[1],
            f"Переключаюсь {current} → {target} ({_model_label(chosen)}). "
            "Перенести контекст?",
            reply_markup=_carry_keyboard(current, target, model_idx),
        )


async def on_engine_carry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Callback после ответа «Да/Нет» на вопрос о переносе контекста.
    Формат callback_data: engine_carry:<old>:<new>:<model_idx_or_dash>:<y|n>."""
    query = update.callback_query
    if query is None:
        return
    data = query.data or ""
    if not data.startswith("engine_carry:"):
        return
    parts = data.split(":")
    if len(parts) != 5:
        return
    _, old_engine, new_engine, model_token, choice = parts
    key = _key(update)

    # model_token: "-" → без модели, иначе индекс в target_engine.models.
    model_idx: int | None = None
    if model_token != "-":
        try:
            model_idx = int(model_token)
        except ValueError:
            return

    # Проверим, что состояние с момента предыдущего шага не изменилось.
    ok, msg, current = _engine_precheck(key, new_engine)
    if not ok or current != old_engine:
        try:
            await query.answer("Состояние изменилось", show_alert=False)
        except Exception:
            pass
        try:
            await query.edit_message_text(
                (msg or f"Состояние изменилось: текущий движок — {current}.")
                + ("\n\nВыбери движок заново." if current else ""),
                reply_markup=_engine_keyboard(current) if current else None,
            )
        except BadRequest:
            await send_to_topic(update.effective_chat, key[1],
                                msg or "Состояние изменилось.")
        return

    chosen_model = _resolve_target_model(new_engine, model_idx)

    try:
        await query.answer()
    except Exception:
        pass

    if choice == "n":
        text = await _do_engine_switch(key, new_engine, model=chosen_model)
        text += "\nКонтекст прежнего диалога не переносится."
        try:
            await query.edit_message_text(text)
        except BadRequest:
            await send_to_topic(update.effective_chat, key[1], text)
        return

    # choice == 'y' — handoff с резюме. Может занять десятки секунд.
    async def progress_edit(t: str) -> None:
        try:
            await query.edit_message_text(t)
        except BadRequest:
            pass

    text = await _do_engine_handoff(
        key, old_engine, new_engine, progress_edit, model=chosen_model,
    )
    try:
        await query.edit_message_text(text, parse_mode=ParseMode.HTML)
    except BadRequest:
        # Возможно HTML невалиден — fallback на plain.
        plain = re.sub(r"<[^>]+>", "", text)
        try:
            await query.edit_message_text(plain)
        except BadRequest:
            await send_to_topic(update.effective_chat, key[1], plain)


async def cmd_bind(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    key = _key(update)
    args = context.args or []
    if not args:
        await update.message.reply_text("Использование: /bind <абсолютный путь>")
        return
    raw = " ".join(args).strip()
    if raw.startswith("~"):
        raw = os.path.expanduser(raw)
    if not os.path.isabs(raw):
        await update.message.reply_text("Путь должен быть абсолютным.")
        return
    raw = os.path.normpath(raw)  # убираем trailing slash и т.п. — важно для slug'а сессий claude
    if not os.path.isdir(raw):
        await update.message.reply_text(f"Директория не существует: {raw}")
        return
    set_cwd(key[0], key[1], raw)
    logger.info("bind: key=%s cwd=%s", key, raw)
    await update.message.reply_text(
        f"Топик привязан к {raw}. Новые запросы будут исполняться оттуда."
    )


async def cmd_unbind(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    key = _key(update)
    clear_cwd(*key)
    logger.info("unbind: key=%s", key)
    await update.message.reply_text(
        f"Привязка снята. Эффективный cwd теперь — дефолт: {CLAUDE_CWD}"
    )


async def cmd_where(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    key = _key(update)
    _, cwd, _ = get_session(*key)
    effective = cwd or CLAUDE_CWD
    await update.message.reply_text(
        f"cwd: {effective}" + (" (дефолт)" if not cwd else " (bound)")
    )


# ---------- Handlers: обработка сообщений ----------

async def _process_prompt(
    update: Update,
    user_text: str,
    attachments: list[str] | None = None,
) -> None:
    key = _key(update)
    chat = update.effective_chat
    thread_id = key[1]

    # Логируем входящее в messages_log сразу — Менеджер через MCP должен видеть
    # запросы, прилетающие в проектные топики, даже если бот ещё в очереди.
    in_text = user_text
    if attachments:
        in_text = in_text + "\n" + "\n".join(f"[Прикреплён файл: {p}]" for p in attachments)
    tg_msg_id = update.message.message_id if update.message else None
    log_message(chat.id, thread_id, "in", "user_text", in_text, tg_msg_id)

    # Reply-to контекст и вложения → meta_block
    extra_lines: list[str] = []
    reply = update.message.reply_to_message if update.message else None
    if reply is not None:
        ctx = load_message_context(chat.id, reply.message_id)
        if ctx:
            extra_lines.append(_build_reply_context_prefix(ctx))
    if attachments:
        for p in attachments:
            extra_lines.append(f"[Прикреплён файл: {p}]")
    meta_block = "\n".join(extra_lines)

    # Проверяем, занят ли lock. Если занят — сообщаем «в очереди» с кнопкой «Отменить».
    lock = _lock_for(key)
    queue_msg = None
    queue_id: str | None = None
    cancel_event: asyncio.Event | None = None
    if lock.locked():
        queue_id = uuid.uuid4().hex[:12]
        cancel_event = asyncio.Event()
        pending_queue[queue_id] = cancel_event
        try:
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_queue:{queue_id}")
            ]])
            queue_msg = await send_to_topic(
                chat, thread_id,
                "⏳ В очереди — текущий запрос ещё выполняется.",
                reply_markup=kb,
            )
        except Exception:
            queue_msg = None

    # Ожидание lock'а с возможностью отмены.
    if cancel_event is not None:
        acquire_task = asyncio.create_task(lock.acquire())
        cancel_task = asyncio.create_task(cancel_event.wait())
        try:
            await asyncio.wait(
                {acquire_task, cancel_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            cancel_task.cancel()
        if cancel_event.is_set():
            # Отменено. Если lock успел захватиться — освободим.
            if acquire_task.done() and not acquire_task.cancelled():
                try:
                    lock.release()
                except RuntimeError:
                    pass
            else:
                acquire_task.cancel()
            pending_queue.pop(queue_id, None)
            if queue_msg is not None:
                try:
                    await queue_msg.edit_text("❌ Отменено пользователем")
                except Exception:
                    pass
            logger.info("queued request cancelled: key=%s queue_id=%s", key, queue_id)
            return
        # Дождались lock'а.
        pending_queue.pop(queue_id, None)
        # Снимем кнопку «Отменить» — запрос стартует.
        if queue_msg is not None:
            try:
                await queue_msg.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
    else:
        await lock.acquire()

    try:
        logger.info("lock acquired: key=%s", key)
        # Получаем актуальные session/cwd/engine уже под локом (вдруг /reset
        # или /engine сработал, пока мы стояли в очереди).
        session_id, cwd, engine_name = get_session(*key)
        engine = get_engine_by_name(engine_name)
        model = get_model(*key)
        effective_cwd = cwd or CLAUDE_CWD

        # Pending handoff summary: pop'аем атомарно — доставляется ровно один раз,
        # в первый prompt после переключения движка с переносом контекста.
        pending_raw = pop_pending_summary(*key)
        pending_summary = None
        if pending_raw:
            pending_summary = await _resolve_pending_summary(key, pending_raw)
            if pending_summary:
                logger.info("delivering pending_summary to engine=%s key=%s (%d chars)",
                            engine_name, key, len(pending_summary))

        prompt_parts = [_system_prefix(effective_cwd)]
        if pending_summary:
            prompt_parts.append(
                "[Контекст от предыдущего движка — резюме прошлого диалога, "
                "продолжай с этой точки:]\n" + pending_summary
            )
        if meta_block:
            prompt_parts.append(meta_block)
        prompt_parts.append("---\n\nСообщение пользователя:\n" + user_text)
        prompt = "\n\n".join(prompt_parts)

        # Индикатор + промежуточные апдейты
        indicator = None
        try:
            indicator = await update.message.reply_text("⏳ Думаю...")
        except Exception:
            indicator = None

        async def on_intermediate(text: str) -> None:
            nonlocal indicator
            preview = text[-TG_HARD_LIMIT + 20:]
            html_preview = md_to_html(preview)
            if indicator is not None:
                try:
                    await indicator.edit_text(html_preview, parse_mode=ParseMode.HTML)
                    return
                except BadRequest as exc:
                    if "parse" in str(exc).lower() or "entit" in str(exc).lower():
                        try:
                            await indicator.edit_text(preview)
                            return
                        except Exception:
                            indicator = None
                    else:
                        indicator = None
                except Exception:
                    indicator = None
            try:
                indicator = await _send_with_html_fallback(
                    chat.send_message, html_preview,
                    **({"message_thread_id": thread_id} if thread_id else {}),
                )
            except Exception:
                pass

        try:
            with engine_model_scope(engine.name, model):
                ok, final_text, _sid_after = await call_llm_stream(
                    engine, session_id, prompt, key, cwd, on_intermediate,
                )
        except Exception as exc:
            logger.exception("llm call crashed: key=%s engine=%s", key, engine.name)
            ok, final_text = False, f"Внутренняя ошибка: {exc}"

        # Удалить индикатор (если можем) и отправить финал
        if indicator is not None:
            try:
                await indicator.delete()
            except Exception:
                pass

        if not ok and not final_text.strip():
            logger.info("llm call stopped without final reply: key=%s engine=%s", key, engine.name)
            return

        # Извлекаем маркеры файлов и отправляем их отдельными документами.
        cleaned_text, file_markers = extract_file_markers(final_text)
        if not cleaned_text.strip():
            cleaned_text = "(пустой ответ)" if not file_markers else "(см. вложения)"
        meta = {"type": "claude_response", "engine": engine.name}
        try:
            await send_claude_reply(chat, thread_id, cleaned_text, meta)
        except Exception:
            logger.exception("failed to send llm reply: key=%s", key)
        if file_markers:
            await deliver_file_markers(chat, thread_id, file_markers)

        logger.info("lock released: key=%s ok=%s files=%d engine=%s",
                    key, ok, len(file_markers), engine.name)
    finally:
        try:
            lock.release()
        except RuntimeError:
            pass


async def _run_spawn(update: Update, user_text: str) -> None:
    """Одноразовая параллельная сессия. Не использует lock топика,
    не сохраняет session_id в БД. Движок наследуется от топика. Все
    сообщения помечаются префиксом [#xxxx]."""
    key = _key(update)
    chat = update.effective_chat
    thread_id = key[1]

    spawn_id = secrets.token_hex(2)  # 4 hex-символа
    prefix = f"[#{spawn_id}] "

    # cwd, engine, model наследуются из топика; session_id новый и в БД не сохраняется.
    _, cwd, engine_name = get_session(*key)
    engine = get_engine_by_name(engine_name)
    model = get_model(*key)
    effective_cwd = cwd or CLAUDE_CWD
    session_id = engine.new_session_id()

    prompt = (
        _system_prefix(effective_cwd)
        + "\n\n---\n\nСообщение пользователя (одноразовый spawn):\n"
        + user_text
    )

    try:
        indicator = await send_to_topic(chat, thread_id, f"{prefix}⏳ Spawn запущен...")
    except Exception:
        indicator = None

    async def on_intermediate(text: str) -> None:
        nonlocal indicator
        body = text[-(TG_HARD_LIMIT - len(prefix) - 20):]
        html_msg = _html_escape(prefix) + md_to_html(body)
        plain_msg = prefix + body
        if indicator is not None:
            try:
                await indicator.edit_text(html_msg, parse_mode=ParseMode.HTML)
                return
            except BadRequest as exc:
                if "parse" in str(exc).lower() or "entit" in str(exc).lower():
                    try:
                        await indicator.edit_text(plain_msg)
                        return
                    except Exception:
                        indicator = None
                else:
                    indicator = None
            except Exception:
                indicator = None
        try:
            indicator = await _send_with_html_fallback(
                chat.send_message, html_msg,
                **({"message_thread_id": thread_id} if thread_id else {}),
            )
        except Exception:
            pass

    try:
        with engine_model_scope(engine.name, model):
            ok, final_text, _sid_after = await call_llm_stream(
                engine, session_id, prompt, key, cwd, on_intermediate, spawn_id=spawn_id,
            )
    except Exception as exc:
        logger.exception("spawn crashed: key=%s spawn=%s engine=%s",
                         key, spawn_id, engine.name)
        ok, final_text = False, f"Внутренняя ошибка: {exc}"

    if indicator is not None:
        try:
            await indicator.delete()
        except Exception:
            pass

    if not ok and not final_text.strip():
        logger.info("spawn stopped without final reply: key=%s spawn=%s engine=%s",
                    key, spawn_id, engine.name)
        return

    cleaned_text, file_markers = extract_file_markers(final_text)
    if not cleaned_text.strip():
        cleaned_text = "(пустой ответ)" if not file_markers else "(см. вложения)"
    meta = {"type": "claude_response", "spawn_id": spawn_id}
    try:
        await send_claude_reply(
            chat, thread_id, cleaned_text, meta,
            filename_prefix=f"spawn_{spawn_id}",
            html_prefix=_html_escape(prefix),
        )
    except Exception:
        logger.exception("failed to send spawn reply: key=%s spawn=%s", key, spawn_id)
    if file_markers:
        await deliver_file_markers(chat, thread_id, file_markers, notice_prefix=prefix)
    logger.info("spawn done: key=%s spawn=%s ok=%s files=%d",
                key, spawn_id, ok, len(file_markers))


async def _run_manager_job(app: Application, job: dict) -> tuple[bool, int | None]:
    """Drive a single queued job through the normal LLM pipeline.

    No Update object: the prompt is treated as user input but sourced from the
    Manager, so we ack it in the topic and post the bot's reply there. Per-key
    lock is respected — if a Telegram user is mid-conversation in the same
    topic, this waits its turn.
    """
    chat_id = job["chat_id"]
    thread_id = job["thread_id"]
    user_text = job["text"]
    job_id = job["id"]
    source = job.get("source") or "manager"
    is_self_kick = source == "self_notice"
    key = (chat_id, thread_id)
    bot = app.bot
    try:
        chat = await bot.get_chat(chat_id)
    except Exception:
        logger.exception("manager job %s: bot.get_chat(%s) failed", job_id, chat_id)
        return False, None

    lock = _lock_for(key)
    await lock.acquire()
    try:
        logger.info("manager job %s: lock acquired key=%s", job_id, key)
        session_id, cwd, engine_name = get_session(*key)
        engine = get_engine_by_name(engine_name)
        model = get_model(*key)
        effective_cwd = cwd or CLAUDE_CWD

        pending_raw = pop_pending_summary(*key)
        pending_summary = await _resolve_pending_summary(key, pending_raw) if pending_raw else None

        prompt_parts = [_system_prefix(effective_cwd)]
        if pending_summary:
            prompt_parts.append(
                "[Контекст от предыдущего движка — резюме прошлого диалога, "
                "продолжай с этой точки:]\n" + pending_summary
            )
        mgr_target = resolve_manager_topic()
        if is_self_kick:
            prompt_parts.append(
                f"[SYSTEM NOTE: это AUTO-KICK для Менеджера (job_id={job_id}, "
                f"source=self_notice). Тебя разбудил бот, потому что в твой "
                f"топик пришли новые нотисы от исполнителей — обычно "
                f"safety-нотисы «📨 ✅ job #N: новый ответ» или health-нотисы "
                f"«⏳ работает долго».\n\n"
                f"Что делать:\n"
                f"1. Прочитай свежие нотисы через "
                f"mcp__jarvis__manager_inbox(chat_id={chat_id}, "
                f"thread_id={thread_id}, limit=10). Фильтруй по kind="
                f"'job_notification' / 'job_heartbeat_warn' / 'job_heartbeat_fail' / "
                f"'job_interrupted'.\n"
                f"2. Для каждого нотиса по необходимости — заходи в источник "
                f"через manager_inbox(thread_id=<src>) и читай развёрнутый "
                f"ответ агента.\n"
                f"3. Решай: либо ждать оператора (просто ничего не делай в "
                f"ответ), либо отвечать оператору в свой топик, либо двигать "
                f"задачу через manager_send(thread_id=<src>, as_user=True).\n"
                f"4. После обработки нотиса — manager_dismiss_notice "
                f"(message_id=...) чтобы топик не зарастал.\n"
                f"5. Если по содержанию нотисов делать нечего — кратко "
                f"подытожь («просмотрел нотисы #N..#M, оператор не нужен») "
                f"и заверши turn. Ничего substantive без одобрения.]"
            )
        elif mgr_target and mgr_target != (chat_id, thread_id):
            mgr_chat_id, mgr_thread_id = mgr_target
            prompt_parts.append(
                f"[SYSTEM NOTE: задача делегирована Менеджером через MCP "
                f"(job_id={job_id}). Финальный ответ этого turn'а идёт в "
                f"этот топик (thread_id={thread_id}, cwd={effective_cwd}). "
                f"Бот сам пришлёт Менеджеру короткий нотис «есть ответ» "
                f"после твоего bot_reply — отдельно слать manager_send не "
                f"нужно.\n\n"
                f"Правила:\n"
                f"1. Нетривиальная задача (любая правка кода / архитектурное "
                f"решение / >1 файла) — сначала предложи план, не начинай "
                f"реализацию. Финальный ответ = план. Менеджер либо одобрит, "
                f"либо корректирует следующим сообщением.\n"
                f"2. Если нужно уточнение по ToR — финальный ответ = вопрос "
                f"с тегом #ask_{job_id}, ничего не реализуй. Жди следующего "
                f"сообщения.\n"
                f"3. При правках на ПРОДЕ — обязательный smoke-check после: "
                f"ищи команду в knowledge-base/projects/<имя>/production_smoke_check.md "
                f"(или подобном файле). Если smoke упал — откатить через "
                f"git revert и сообщить ❌ в финальном ответе. Если файла "
                f"smoke-check нет — задай уточняющий вопрос через #ask.\n"
                f"4. (опционально) По завершении CODE-задачи можешь "
                f"дополнительно прислать богатый отчёт в Менеджеров топик: "
                f"mcp__jarvis__manager_send(thread_id={mgr_thread_id}, "
                f"as_user=false, text='#job_{job_id} ✅ <одна строка> — "
                f"src: thread_id={thread_id}, cwd={effective_cwd}'). Это "
                f"улучшает поиск по хэштегу, но НЕ обязательно — бот пришлёт "
                f"свой нотис сам.]"
            )
        else:
            prompt_parts.append(
                "[SYSTEM NOTE: задача делегирована Менеджером через MCP "
                f"(job_id={job_id}). Отвечай как обычно.]"
            )
        prompt_parts.append("---\n\nСообщение пользователя:\n" + user_text)
        prompt = "\n\n".join(prompt_parts)

        if is_self_kick:
            indicator = None
        else:
            try:
                indicator = await send_to_topic(
                    chat, thread_id,
                    f"⏳ Manager делегировал задачу (job #{job_id})...",
                )
            except Exception:
                indicator = None

        async def on_intermediate(text: str) -> None:
            nonlocal indicator
            preview = text[-TG_HARD_LIMIT + 20:]
            html_preview = md_to_html(preview)
            if indicator is not None:
                try:
                    await indicator.edit_text(html_preview, parse_mode=ParseMode.HTML)
                    return
                except BadRequest as exc:
                    if "parse" in str(exc).lower() or "entit" in str(exc).lower():
                        try:
                            await indicator.edit_text(preview)
                            return
                        except Exception:
                            indicator = None
                    else:
                        indicator = None
                except Exception:
                    indicator = None
            try:
                indicator = await _send_with_html_fallback(
                    chat.send_message, html_preview,
                    **({"message_thread_id": thread_id} if thread_id else {}),
                )
            except Exception:
                pass

        # Watcher для interrupt: раз в 2с смотрит cancel_requested.
        # Если выставлен — терминирует subprocess; основной stream выйдет
        # с ошибкой, мы это поймаем по interrupted=True ниже.
        watcher_stop = asyncio.Event()
        interrupted = False

        async def _interrupt_watcher() -> None:
            nonlocal interrupted
            while not watcher_stop.is_set():
                try:
                    await asyncio.wait_for(watcher_stop.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pass
                if watcher_stop.is_set():
                    return
                try:
                    with _db() as conn_:
                        row = conn_.execute(
                            "SELECT cancel_requested FROM jobs WHERE id = ?",
                            (job_id,),
                        ).fetchone()
                except Exception:
                    logger.exception("interrupt watcher poll failed job=%s", job_id)
                    continue
                if row and row[0]:
                    interrupted = True
                    proc = active_procs.get(key)
                    if proc is not None:
                        logger.info(
                            "manager job %s: cancel_requested, terminating proc",
                            job_id,
                        )
                        try:
                            await terminate_process_tree(proc)
                        except Exception:
                            logger.exception(
                                "terminate_process_tree failed job=%s", job_id,
                            )
                    return

        watcher_task = asyncio.create_task(_interrupt_watcher())

        try:
            with engine_model_scope(engine.name, model):
                ok, final_text, _sid_after = await call_llm_stream(
                    engine, session_id, prompt, key, cwd, on_intermediate,
                )
        except Exception as exc:
            logger.exception("manager job %s: llm crashed engine=%s", job_id, engine.name)
            ok, final_text = False, f"Внутренняя ошибка: {exc}"
        finally:
            watcher_stop.set()
            try:
                await asyncio.wait_for(watcher_task, timeout=3.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                watcher_task.cancel()

        if indicator is not None:
            try:
                await indicator.delete()
            except Exception:
                pass

        if interrupted:
            # Менеджер прервал. Доделаем то немногое что успели увидеть в
            # final_text (если что-то пришло), но job помечаем как cancelled
            # и шлём отдельный нотис.
            try:
                await send_to_topic(
                    chat, thread_id,
                    f"⏹ job #{job_id} остановлен Менеджером. "
                    "Жду уточняющий вопрос или новые инструкции.",
                )
            except Exception:
                logger.exception("manager job %s: failed to post interrupt notice", job_id)
            await _send_manager_notice(
                app,
                f"⏹ job #{job_id}: subprocess остановлен по твоему запросу "
                f"(thread_id={thread_id}). Теперь можно прислать уточняющий "
                f"вопрос обычным manager_send(as_user=True) — агент resume "
                f"той же сессии и увидит контекст до прерывания.",
                kind="job_interrupted",
            )
            logger.info("manager job %s: interrupted by manager", job_id)
            return False, None

        if not ok and not final_text.strip():
            logger.info("manager job %s stopped without final reply", job_id)
            return False, None

        cleaned_text, file_markers = extract_file_markers(final_text)
        if not cleaned_text.strip():
            cleaned_text = "(пустой ответ)" if not file_markers else "(см. вложения)"
        meta = {"type": "claude_response", "engine": engine.name, "job_id": job_id}
        sent_msg_id = None
        try:
            sent = await send_claude_reply(chat, thread_id, cleaned_text, meta)
            if sent is not None:
                sent_msg_id = sent.message_id
        except Exception:
            logger.exception("manager job %s: failed to send reply", job_id)
        if file_markers:
            await deliver_file_markers(chat, thread_id, file_markers)

        # Safety notice в топик Менеджера — гарантированно даём знать что
        # есть ответ. Не зависит от того, прислал ли агент сам что-то
        # через mcp__jarvis__manager_send. Не шлём для self_kick — Менеджер
        # сам себе нотис не нужен, он уже разбирает свой inbox.
        if not is_self_kick and resolve_manager_topic() != (chat_id, thread_id):
            with _db() as conn_:
                row = conn_.execute(
                    "SELECT topic_title, cwd FROM sessions "
                    "WHERE chat_id = ? AND thread_id = ?",
                    (chat_id, thread_id),
                ).fetchone()
            title = (row[0] if row and row[0] else None) or f"thread_id={thread_id}"
            cwd_disp = (row[1] if row and row[1] else None) or f"{CLAUDE_CWD} (дефолт)"
            status_emoji = "✅" if ok else "⚠️"
            notice_text = (
                f"📨 {status_emoji} job #{job_id}: новый ответ от агента\n"
                f"Топик: «{title}» (thread_id={thread_id})\n"
                f"cwd: {cwd_disp}\n"
                f"Действие: прочитай через "
                f"manager_inbox(thread_id={thread_id}) или зайди в сам топик."
            )
            await _send_manager_notice(app, notice_text, kind="job_notification")

        logger.info("manager job %s done: ok=%s files=%d engine=%s",
                    job_id, ok, len(file_markers), engine.name)
        return ok, sent_msg_id
    finally:
        try:
            lock.release()
        except RuntimeError:
            pass


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
                    "SELECT id, chat_id, thread_id, claimed_at FROM jobs "
                    "WHERE status='in_progress' AND claimed_at IS NOT NULL "
                    "AND claimed_at < ? AND heartbeat_notified_at IS NULL",
                    (warn_thr,),
                ).fetchall()
            for r in warn_rows:
                jid, jchat, jthread, jclaimed = r[0], r[1], r[2], r[3]
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
                await _send_manager_notice(app, text, kind="job_heartbeat_warn")
                with _db() as conn:
                    conn.execute(
                        "UPDATE jobs SET heartbeat_notified_at = ? WHERE id = ?",
                        (now_iso, jid),
                    )

            # FAIL: in_progress + claimed_at < fail_thr → помечаем failed.
            with _db() as conn:
                fail_rows = conn.execute(
                    "SELECT id, chat_id, thread_id FROM jobs "
                    "WHERE status='in_progress' AND claimed_at IS NOT NULL "
                    "AND claimed_at < ?",
                    (fail_thr,),
                ).fetchall()
            for r in fail_rows:
                jid, jchat, jthread = r[0], r[1], r[2]
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
                await _send_manager_notice(app, text, kind="job_heartbeat_fail")

            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info("health_worker cancelled")
            raise
        except Exception:
            logger.exception("health_worker loop crashed; sleeping 60s")
            await asyncio.sleep(60.0)


async def jobs_worker(app: Application) -> None:
    """Long-running background task: pull pending jobs and run them.

    One worker is enough for now — jobs serialise per-topic anyway via the
    same key-lock the user-side pipeline uses. The poll interval is intentionally
    short (~2s) because the queue is meant for interactive delegation.
    """
    logger.info("jobs_worker started")
    while True:
        try:
            job = claim_next_job()
            if job is None:
                await asyncio.sleep(2.0)
                continue
            try:
                ok, msg_id = await _run_manager_job(app, job)
                finish_job(
                    job["id"], "done" if ok else "failed",
                    None if ok else "engine returned no usable reply",
                    msg_id,
                )
            except Exception as exc:
                logger.exception("manager job %s crashed", job["id"])
                finish_job(job["id"], "failed", str(exc)[:1000], None)
        except asyncio.CancelledError:
            logger.info("jobs_worker cancelled")
            raise
        except Exception:
            logger.exception("jobs_worker loop crashed; sleeping 5s")
            await asyncio.sleep(5.0)


async def cmd_spawn(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/spawn <prompt> — запустить одноразовую параллельную claude-сессию.
    Не блокируется lock'ом топика, не трогает основную сессию."""
    if not update.message:
        return
    raw = (update.message.text or "")
    # Убираем саму команду "/spawn" (с возможным @botname).
    parts = raw.split(None, 1)
    prompt = parts[1].strip() if len(parts) > 1 else ""
    if not prompt:
        await update.message.reply_text(
            "Использование: /spawn <prompt>\n"
            "Запускает параллельную одноразовую claude-сессию в cwd этого топика.\n"
            "Остановить конкретный spawn: /stop <id> (4 hex, напр. /stop a1b2)."
        )
        return
    # Фоновая задача, чтобы handler вернулся сразу и не блокировал polling.
    asyncio.create_task(_run_spawn(update, prompt))


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text:
        return
    await _process_prompt(update, update.message.text.strip())


async def _download_tg_file(update: Update, file_id: str, suggested_name: str) -> str:
    tg_file = await update.get_bot().get_file(file_id)
    safe_name = "".join(c for c in suggested_name if c.isalnum() or c in "._-") or "file"
    dest_dir = os.path.join(MEDIA_DIR, str(update.effective_chat.id))
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(
        dest_dir,
        f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{safe_name}",
    )
    await tg_file.download_to_drive(dest)
    return dest


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.photo:
        return
    photo = update.message.photo[-1]
    path = await _download_tg_file(update, photo.file_id, "photo.jpg")
    caption = (update.message.caption or "").strip() or "(опиши изображение)"
    await _process_prompt(update, caption, attachments=[path])


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.document:
        return
    doc = update.message.document
    path = await _download_tg_file(update, doc.file_id, doc.file_name or "document")
    caption = (update.message.caption or "").strip() or f"(прикреплён файл {doc.file_name or ''})"
    await _process_prompt(update, caption, attachments=[path])


async def on_cancel_queue(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка нажатия кнопки «❌ Отменить» на сообщении «в очереди»."""
    query = update.callback_query
    if query is None:
        return
    data = query.data or ""
    if not data.startswith("cancel_queue:"):
        return
    queue_id = data.split(":", 1)[1]
    event = pending_queue.get(queue_id)
    if event is None:
        # Уже не в очереди: либо стартовал, либо уже отменён ранее.
        try:
            await query.answer("Уже выполняется — используй /stop", show_alert=True)
        except Exception:
            pass
        # На всякий случай снимем кнопку, чтобы не нажималась повторно.
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        return
    event.set()
    try:
        await query.answer("Отменено")
    except Exception:
        pass
    # Само сообщение редактируется в ожидающей корутине (в «❌ Отменено пользователем»).


async def unauthorized_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if user is not None:
        logger.warning(
            "Unauthorized: user_id=%s username=%r full_name=%r",
            user.id, user.username, user.full_name,
        )
    if update.message is not None:
        try:
            await update.message.reply_text("Доступ запрещён.")
        except Exception:
            pass


# ---------- main ----------

# Команды, выводимые в нативное меню Telegram (синяя кнопка слева от поля ввода).
# Описания короткие — Telegram обрезает длинные.
BOT_COMMANDS: list[BotCommand] = [
    BotCommand("engine", "движок: показать/переключить (claude|codex|opencode)"),
    BotCommand("new", "новая сессия (cwd и движок сохраняются)"),
    BotCommand("session", "session-id, cwd и движок"),
    BotCommand("stop", "прервать текущий запрос"),
    BotCommand("spawn", "одноразовая параллельная сессия — /spawn <prompt>"),
    BotCommand("bind", "привязать топик к каталогу — /bind <abs path>"),
    BotCommand("unbind", "снять привязку cwd, вернуть дефолт"),
    BotCommand("where", "показать эффективный cwd"),
    BotCommand("start", "приветствие и состояние топика"),
]


async def _post_init(application: Application) -> None:
    """Регистрируем команды для всех контекстов (default + private + group)
    и явно ставим MenuButtonCommands — иначе в форум-группах нативная кнопка
    меню часто не появляется без явной настройки."""
    bot = application.bot
    scopes = [
        ("default", None),
        ("all_private_chats", BotCommandScopeAllPrivateChats()),
        ("all_group_chats", BotCommandScopeAllGroupChats()),
    ]
    for label, scope in scopes:
        try:
            if scope is None:
                await bot.set_my_commands(BOT_COMMANDS)
            else:
                await bot.set_my_commands(BOT_COMMANDS, scope=scope)
            logger.info("bot commands registered for scope=%s (%d entries)",
                        label, len(BOT_COMMANDS))
        except Exception:
            logger.exception("set_my_commands failed for scope=%s", label)
    try:
        await bot.set_chat_menu_button(menu_button=MenuButtonCommands())
        logger.info("default menu button set to MenuButtonCommands")
    except Exception:
        logger.exception("set_chat_menu_button failed (меню не критично)")

    # Запускаем worker для очереди jobs (delegations from Manager via MCP).
    # Хранить ссылку в bot_data на случай нужды в shutdown'е/тестах.
    task = asyncio.create_task(jobs_worker(application))
    application.bot_data["jobs_worker_task"] = task

    # Гигиена: hourly cleanup старых записей messages_log + завершённых jobs.
    # TTL — env JARVIS_LOG_TTL_DAYS (дефолт 30, 0/none/off отключает).
    cleanup_task = asyncio.create_task(cleanup_worker(application))
    application.bot_data["cleanup_worker_task"] = cleanup_task

    # Health: следит за долгими in_progress jobs, шлёт Менеджеру нотисы.
    # Параметры в env JARVIS_HEARTBEAT_INTERVAL/WARN/FAIL (300/900/3600с).
    health_task = asyncio.create_task(health_worker(application))
    application.bot_data["health_worker_task"] = health_task

    # Reminders: cron-light напоминания для Менеджера.
    reminders_task = asyncio.create_task(reminders_worker(application))
    application.bot_data["reminders_worker_task"] = reminders_task


def main() -> None:
    print(f"=== Jarvis Telegram Bot (per-topic engine, default={DEFAULT_ENGINE_NAME}) ===")

    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN не задан. См. .env.example.")
    if not ALLOWED_USER_IDS:
        logger.warning("ALLOWED_USER_IDS пуст — никто не сможет писать боту.")

    init_db()
    mcp_ok, mcp_status = ensure_engine_tools(DEFAULT_ENGINE)
    if mcp_ok:
        logger.info("Default engine tools ready: %s", mcp_status)
    else:
        logger.warning("Default engine tools are not fully ready: %s", mcp_status)

    # concurrent_updates=True: без этого PTB обрабатывает апдейты последовательно,
    # и per-key asyncio.Lock не даёт параллельности между разными топиками —
    # второй топик ждёт, пока освободится воркер PTB, а не сам lock.
    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .concurrent_updates(True)
        .post_init(_post_init)
        .build()
    )

    allowed = filters.User(user_id=ALLOWED_USER_IDS)

    app.add_handler(CommandHandler("start", cmd_start, filters=allowed))
    app.add_handler(CommandHandler("new", cmd_reset, filters=allowed))
    app.add_handler(CommandHandler("reset", cmd_reset, filters=allowed))
    app.add_handler(CommandHandler("stop", cmd_stop, filters=allowed))
    app.add_handler(CommandHandler("spawn", cmd_spawn, filters=allowed))
    app.add_handler(CommandHandler("session", cmd_session, filters=allowed))
    app.add_handler(CommandHandler("engine", cmd_engine, filters=allowed))
    app.add_handler(CommandHandler("bind", cmd_bind, filters=allowed))
    app.add_handler(CommandHandler("unbind", cmd_unbind, filters=allowed))
    app.add_handler(CommandHandler("where", cmd_where, filters=allowed))

    app.add_handler(MessageHandler(allowed & filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(allowed & filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(allowed & filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(on_cancel_queue, pattern=r"^cancel_queue:"))
    app.add_handler(CallbackQueryHandler(on_engine_select, pattern=r"^engine_select:"))
    app.add_handler(CallbackQueryHandler(on_model_select, pattern=r"^model_select:"))
    app.add_handler(CallbackQueryHandler(on_engine_carry, pattern=r"^engine_carry:"))

    app.add_handler(MessageHandler(~allowed, unauthorized_handler))

    logger.info("Whitelisted user_ids: %s", sorted(ALLOWED_USER_IDS))
    logger.info("Default engine: %s  default cwd=%s", DEFAULT_ENGINE_NAME, CLAUDE_CWD)
    print(f"Бот запущен (default engine={DEFAULT_ENGINE_NAME}). Жду сообщения в Telegram...")
    app.run_polling()


if __name__ == "__main__":
    main()
