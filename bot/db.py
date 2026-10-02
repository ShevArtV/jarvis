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

from bot.plugins import load_plugins
from bot.settings import DB_PATH

logger = logging.getLogger(__name__)

class _ClosingConnection(sqlite3.Connection):
    """``with`` у sqlite3 только фиксирует транзакцию, а соединение оставляет
    открытым до сборщика мусора. На Windows открытый файл нельзя удалить или
    переименовать, поэтому закрываем соединение на выходе из блока."""

    def __exit__(self, *exc):
        try:
            return super().__exit__(*exc)
        finally:
            self.close()


def connect(path: str, **kwargs) -> sqlite3.Connection:
    """``sqlite3.connect``, соединение которого закрывается на выходе из ``with``."""
    return sqlite3.connect(path, factory=_ClosingConnection, **kwargs)


def _db() -> sqlite3.Connection:
    conn = connect(DB_PATH)
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


_SESSIONS_DDL = """
    CREATE TABLE {if_not_exists}sessions (
        chat_id INTEGER NOT NULL,
        thread_id INTEGER NOT NULL DEFAULT 0,
        session_id TEXT NOT NULL,
        cwd TEXT,
        engine TEXT NOT NULL DEFAULT 'claude',
        updated_at TEXT NOT NULL,
        PRIMARY KEY (chat_id, thread_id)
    )
"""

# Колонки, добавленные к таблицам после их появления. Порядок — порядок
# появления: ALTER дописывает колонку в конец, и схема старой и новой БД
# должна совпадать. Новую колонку — только в конец списка своей таблицы.
# (таблица, колонка, тип и дефолт)
_ADDED_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("sessions", "engine", "TEXT NOT NULL DEFAULT 'claude'"),
    # Резюме предыдущей сессии другого движка, ждущее первого prompt после
    # /engine с переносом.
    ("sessions", "pending_summary", "TEXT"),
    # Выбранная для топика модель; NULL — дефолт движка.
    ("sessions", "model", "TEXT"),
    # Название топика: Telegram не отдаёт его через getChat, нужен свой реестр.
    ("sessions", "topic_title", "TEXT"),
    # Цвет иконки (один из 6 кодов Telegram), чтобы manager_create_topic не
    # повторял цвета.
    ("sessions", "topic_icon_color", "INTEGER"),
    # Модель, которой CLI ответил последний раз (а model — что выбрали руками).
    ("sessions", "actual_model", "TEXT"),
    # /browser: 1 — адаптер подключает Playwright MCP.
    ("sessions", "mcp_playwright", "INTEGER NOT NULL DEFAULT 0"),
    # /persistent: живой процесс движка на сеанс; 0 — только явным выключением.
    ("sessions", "persistent_claude", "INTEGER NOT NULL DEFAULT 1"),
    ("sessions", "persistent_codex", "INTEGER NOT NULL DEFAULT 1"),
    ("sessions", "persistent_opencode", "INTEGER NOT NULL DEFAULT 1"),
    # Маркер одноразового бэкфилла persistent_*=1 (см. _before_add).
    ("sessions", "persistent_default_migrated", "INTEGER NOT NULL DEFAULT 1"),
    # Легаси автокомпакта: не используется, не удаляется ради отката.
    ("sessions", "autocompact_enabled", "INTEGER"),
    # Время последнего сообщения: по нему закрывается протухший сеанс. NULL у
    # старых строк — сеанс протух, откроется новый.
    ("sessions", "last_activity_at", "TEXT"),
    # Когда открыт сеанс: движки читают AGENTS.md/CLAUDE.md при старте, и по
    # этой метке видно, что инструкции с тех пор поменялись.
    ("sessions", "session_started_at", "TEXT"),
    # Флаг manager_close_session: MCP-сервер не видит процессов бота и просит
    # закрыть сеанс через БД, close_requests_worker доделывает.
    ("sessions", "close_requested", "TEXT"),
    # Пульс ожидающего ask_user: протух — вопрос никто не ждёт (процесс убит),
    # и сообщение в топик в него не уходит.
    ("ask_requests", "polled_at", "TEXT"),
    # Отложенный job (авто-go Менеджера); NULL — доступен сразу.
    ("jobs", "not_before", "TEXT"),
    # Когда health_worker уже предупредил о долгом job'е — нотис один раз.
    ("jobs", "heartbeat_notified_at", "TEXT"),
    # Флаг manager_interrupt: watcher job'а читает его и убивает процесс.
    ("jobs", "cancel_requested", "TEXT"),
    # Топик-инициатор job'а: нотис об ответе уходит ему, а не Тимлиду.
    ("jobs", "origin_chat_id", "INTEGER"),
    ("jobs", "origin_thread_id", "INTEGER"),
    # Кому адресован триггер ('executor' | 'manager'): ask_user не задаёт
    # вопросов в чат исполнителю внешней задачи. NULL — не блокируем.
    ("agent_triggers", "role", "TEXT"),
)


def _before_add(conn: sqlite3.Connection, table: str, column: str) -> None:
    if (table, column) == ("sessions", "persistent_default_migrated"):
        # Решение оператора 2026-09-05: включить persistent всем существующим
        # топикам. Маркер-колонка не даёт бэкфиллу повториться и сбросить
        # топики, выключенные /persistent off уже после миграции.
        logger.info("backfilling persistent_claude/persistent_codex=1 for existing sessions rows")
        conn.execute("UPDATE sessions SET persistent_claude = 1, persistent_codex = 1")


def _after_add(conn: sqlite3.Connection, table: str, column: str) -> None:
    if (table, column) == ("jobs", "not_before"):
        # Старый индекс не под новый ORDER BY; idx_jobs_pending создаётся ниже.
        conn.execute("DROP INDEX IF EXISTS idx_jobs_status")


def _add_missing_columns(conn: sqlite3.Connection, table: str) -> None:
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for tbl, column, decl in _ADDED_COLUMNS:
        if tbl != table or column in cols:
            continue
        _backup_db_once()
        logger.info("adding '%s' column to %s", column, table)
        _before_add(conn, table, column)
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")
        _after_add(conn, table, column)


def init_db() -> None:
    with _db() as conn:
        # Миграция: если sessions существует со старым PK (chat_id) без колонок thread_id/cwd —
        # пересоздаём таблицу и переносим данные (thread_id=0).
        cols = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
        if cols and "thread_id" not in cols:
            _backup_db_once()
            logger.info("migrating sessions table: adding thread_id/cwd, new PK (chat_id, thread_id)")
            conn.execute("ALTER TABLE sessions RENAME TO sessions_old")
            conn.execute(_SESSIONS_DDL.format(if_not_exists=""))
            conn.execute(
                "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, updated_at) "
                "SELECT chat_id, 0, session_id, NULL, 'claude', updated_at FROM sessions_old"
            )
            conn.execute("DROP TABLE sessions_old")
            logger.info("sessions migration done")
        else:
            conn.execute(_SESSIONS_DDL.format(if_not_exists="IF NOT EXISTS "))
            _add_missing_columns(conn, "sessions")
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
        _add_missing_columns(conn, "ask_requests")
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
        _add_missing_columns(conn, "jobs")
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
                role TEXT
            )
            """
        )
        _add_missing_columns(conn, "agent_triggers")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_agent_triggers_pending "
            "ON agent_triggers(status, created_at)"
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
        # Таблицы включённых плагинов (JARVIS_PLUGINS).
        for plugin in load_plugins():
            for ddl in plugin.schema:
                conn.execute(ddl)


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
