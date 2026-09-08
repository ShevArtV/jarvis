"""Схема SQLite и низкоуровневый доступ к ней.

Здесь живёт вся эволюция схемы: таблицы создаются идемпотентно, а недостающие
колонки добавляются на месте, поэтому апгрейд бота не требует ручных миграций.
Перед первой правкой схемы делается однократный бэкап файла БД.

``DB_PATH`` — единственная точка, определяющая, с каким файлом работает бот.
Тесты подменяют именно её (``patch.object(bot.db, "DB_PATH", ...)``): одноимённое
имя, реэкспортированное в ``telegram_bot``, — только для обратной совместимости
и на выбор файла не влияет.
"""

from __future__ import annotations

import logging
import os
import shutil
import sqlite3
from datetime import datetime

from bot.settings import DB_PATH

logger = logging.getLogger(__name__)

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
            # Idempotent миграция: mcp_playwright — per-topic флаг «браузер
            # подключён». 0 = off (дефолт): Playwright не грузится в контекст.
            # 1 = on: адаптер инъектит Playwright MCP per-invocation. Команда
            # /browser тоглит флаг (с пересозданием session_id — набор тулов).
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "mcp_playwright" not in cols_now:
                _backup_db_once()
                logger.info("adding 'mcp_playwright' column to sessions (default=0)")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN mcp_playwright INTEGER NOT NULL DEFAULT 0"
                )
            # Idempotent миграция: persistent_claude — per-topic флаг «живой
            # процесс claude». 1 = on (дефолт): один subprocess на сеанс
            # (--input-format stream-json), сообщение во время активного хода
            # дописывается в его stdin вместо ожидания очереди. 0 = off,
            # выставляется только явным /persistent off. Команда /persistent
            # тоглит флаг.
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "persistent_claude" not in cols_now:
                _backup_db_once()
                logger.info("adding 'persistent_claude' column to sessions (default=1)")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN persistent_claude INTEGER NOT NULL DEFAULT 1"
                )
            # Idempotent migration: persistent_codex — per-topic flag for a live
            # Codex app-server. Kept separate from persistent_claude to avoid a
            # risky state refactor and preserve existing Claude behavior.
            # Default is on (1); explicit /persistent off sets 0.
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "persistent_codex" not in cols_now:
                _backup_db_once()
                logger.info("adding 'persistent_codex' column to sessions (default=1)")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN persistent_codex INTEGER NOT NULL DEFAULT 1"
                )
            # Idempotent миграция: persistent_default_migrated — одноразовый
            # бэкфилл persistent_claude/persistent_codex в 1 для СУЩЕСТВУЮЩИХ
            # строк (решение оператора 2026-09-05: включить persistent всем
            # топикам, кто его поддерживает). Маркер-колонка нужна, чтобы
            # бэкфилл не повторялся на каждом рестарте бота и не сбрасывал
            # топики, которые оператор явно выключил командой /persistent off
            # уже ПОСЛЕ этой миграции.
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "persistent_default_migrated" not in cols_now:
                _backup_db_once()
                logger.info(
                    "backfilling persistent_claude/persistent_codex=1 for existing sessions rows"
                )
                conn.execute(
                    "UPDATE sessions SET persistent_claude = 1, persistent_codex = 1"
                )
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN persistent_default_migrated "
                    "INTEGER NOT NULL DEFAULT 1"
                )
            # Idempotent миграция: autocompact_enabled — легаси, автокомпакт
            # убран вместе с переходом на сеансы. Колонку не используем и не
            # удаляем (чтобы не терять данные на откате).
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "autocompact_enabled" not in cols_now:
                _backup_db_once()
                logger.info("adding 'autocompact_enabled' column to sessions")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN autocompact_enabled INTEGER"
                )
            # Idempotent миграция: last_activity_at — время последнего сообщения
            # в топике. По нему закрывается протухший сеанс (idle > порога).
            # NULL у старых строк = сеанс считается протухшим при первом
            # обращении, т.е. откроется новый — это и нужно.
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "last_activity_at" not in cols_now:
                _backup_db_once()
                logger.info("adding 'last_activity_at' column to sessions")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN last_activity_at TEXT"
                )
            # Idempotent миграция: session_started_at — когда открыт текущий сеанс.
            # Нужен, чтобы понять, не изменились ли с тех пор инструкции проекта
            # (AGENTS.md / CLAUDE.md): движки читают их при СТАРТЕ сессии, и без
            # этой проверки правка инструкций молча не действует — агент до конца
            # сеанса работает по версии, прочитанной когда-то давно.
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "session_started_at" not in cols_now:
                _backup_db_once()
                logger.info("adding 'session_started_at' column to sessions")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN session_started_at TEXT"
                )
            # Idempotent миграция: close_requested — флаг для manager_close_session.
            # MCP-сервер живёт отдельным процессом и не видит active_procs /
            # persistent_workers бота, поэтому «убей живые процессы топика»
            # передаётся через БД: MCP ставит timestamp, close_requests_worker
            # раз в 2с его читает и доделывает то, что умеет только бот.
            # NULL = не запрошено (тот же приём, что cancel_requested у jobs).
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "close_requested" not in cols_now:
                logger.info("adding 'close_requested' column to sessions")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN close_requested TEXT"
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
        # Вопросы агента пользователю (MCP tool ask_user). Канал между двумя
        # процессами: MCP-сервер пишет вопрос и поллит ответ, бот принимает
        # ответ (нажатие кнопки или обычное сообщение в топик) и кладёт сюда.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ask_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                thread_id INTEGER NOT NULL DEFAULT 0,
                question TEXT NOT NULL,
                options_json TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                answer TEXT,
                option_index INTEGER,
                via TEXT,
                telegram_message_id INTEGER,
                created_at TEXT NOT NULL,
                answered_at TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ask_requests_pending "
            "ON ask_requests(chat_id, thread_id, status, id)"
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
        # Idempotent миграция: origin_* — топик, который делегировал задачу
        # (manager_send его передаёт). Нужен, чтобы safety/heartbeat-нотис об
        # ответе уходил инициатору, а не константному Тимлиду: до этого любой
        # job будил топик Тимлида, и тот вклинивался в чужую задачу.
        # NULL = инициатор неизвестен → fallback на служебную роль teamlead.
        cols_now = [r[1] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()]
        if cols_now and "origin_chat_id" not in cols_now:
            logger.info("adding 'origin_chat_id'/'origin_thread_id' columns to jobs")
            conn.execute("ALTER TABLE jobs ADD COLUMN origin_chat_id INTEGER")
            conn.execute("ALTER TABLE jobs ADD COLUMN origin_thread_id INTEGER")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_pending "
            "ON jobs(status, not_before, created_at)"
        )
        # Очередь внешних триггеров без job-семантики — публичный контракт для
        # любого интегратора (issue tracker, CI, cron): вставь строку, и бот
        # проведёт обычный LLM turn в топике, но без job_id, health_worker,
        # manager_interrupt и safety-notice Менеджеру на ответ или interrupt.
        # source — свободная метка интеграции ('mxboard' у поллера доски),
        # нужна только для логов и гарда ask_user ниже.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_triggers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                thread_id INTEGER NOT NULL,
                text TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'external',
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                claimed_at TEXT,
                finished_at TEXT,
                error TEXT,
                result_message_id INTEGER,
                role TEXT,
                not_before TEXT,
                throttle_reason TEXT,
                throttle_notice_message_id INTEGER,
                throttle_force INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        # Idempotent миграция: role — кому адресован триггер ('executor' |
        # 'manager'). Пишет интегратор; читает ask_user в MCP-сервере, чтобы
        # запретить вопросы в чат исполнителю, работающему по внешней задаче:
        # там весь диалог принадлежит трекеру, а не Telegram.
        # NULL = роль неизвестна (старая запись / интегратор её не пишет) —
        # такие не блокируем.
        cols_now = [
            r[1] for r in conn.execute("PRAGMA table_info(agent_triggers)").fetchall()
        ]
        if cols_now and "role" not in cols_now:
            logger.info("adding 'role' column to agent_triggers")
            conn.execute("ALTER TABLE agent_triggers ADD COLUMN role TEXT")
        for column, sql_type in (("not_before", "TEXT"), ("throttle_reason", "TEXT"),
                                 ("throttle_notice_message_id", "INTEGER"),
                                 ("throttle_force", "INTEGER NOT NULL DEFAULT 0")):
            if cols_now and column not in cols_now:
                conn.execute(f"ALTER TABLE agent_triggers ADD COLUMN {column} {sql_type}")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_agent_triggers_pending "
            "ON agent_triggers(status, not_before, created_at)"
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
        # Состояние polling-интеграций. Сейчас его использует ActiveCollab MCP,
        # чтобы не повторять уже доложенные Секретарю задачи и уведомления.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS integration_seen_items (
                integration TEXT NOT NULL,
                kind TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                seen_at TEXT NOT NULL,
                PRIMARY KEY (integration, kind, item_id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_integration_seen_items "
            "ON integration_seen_items(integration, kind, item_id)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS integration_sync_state (
                integration TEXT NOT NULL,
                state_key TEXT NOT NULL,
                state_value TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (integration, state_key)
            )
            """
        )
        # imap_state: UIDs уже отправленных нотисов, чтобы не дублировать.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS imap_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account TEXT NOT NULL,
                uid INTEGER NOT NULL,
                seen_at TEXT NOT NULL,
                UNIQUE(account, uid)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_imap_state_account "
            "ON imap_state(account, uid)"
        )
        # webhook_log: входящие события от Битрикс24 и других вебхуков.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS webhook_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                event TEXT NOT NULL,
                payload TEXT NOT NULL,
                received_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_webhook_log_received "
            "ON webhook_log(source, received_at)"
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
