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
from datetime import datetime

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
    """Atomically claim one pending job. Returns its row as a dict or None.

    Uses UPDATE ... WHERE id = (SELECT ... LIMIT 1) to grab a single row
    without holding the connection between SELECT and UPDATE.
    """
    now = datetime.utcnow().isoformat()
    with _db() as conn:
        row = conn.execute(
            "SELECT id, chat_id, thread_id, text, source FROM jobs "
            "WHERE status = 'pending' ORDER BY created_at ASC, id ASC LIMIT 1"
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

    Возвращает (ok, final_text, session_id_after).
    """
    mcp_ok, mcp_status = ensure_engine_tools(engine)
    if not mcp_ok:
        logger.warning("engine=%s activated without Playwright MCP: %s", engine.name, mcp_status)
    ok, final_text, sid_after = await engine.call_stream(
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


async def cmd_session(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    key = _key(update)
    session_id, cwd, engine_name = get_session(*key)
    model = get_model(*key)
    effective = cwd or CLAUDE_CWD
    model_line = f"\nmodel: {model}" if model else ""
    await update.message.reply_text(
        f"engine: {engine_name}{model_line}\n"
        f"session-id: {session_id}\ncwd: {effective}"
        + (" (дефолт)" if not cwd else "")
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
    old_engine = get_engine_by_name(old_engine_name)
    old_session_id, cwd, _ = get_session(*key)
    old_model = get_model(*key)
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

    lock = _lock_for(key)
    if lock.locked():
        return (
            "⚠️ Топик занят активным запросом. Дождись завершения или /stop, "
            "потом повтори переключение."
        )

    await lock.acquire()
    try:
        await progress_edit(f"🧠 Снимаю резюме сессии у {old_engine_name}...")

        async def on_intermediate(_text: str) -> None:
            # промежуточные сообщения от старого движка не показываем —
            # пользователь и так знает, что мы снимаем резюме.
            pass

        try:
            with engine_model_scope(old_engine.name, old_model):
                ok, summary_text, _sid_after = await call_llm_stream(
                    old_engine, old_session_id, summary_prompt, key, cwd, on_intermediate,
                )
        except Exception as exc:
            logger.exception("handoff summary call crashed: key=%s engine=%s",
                             key, old_engine_name)
            return f"❌ Ошибка при снятии резюме: {exc}\nПереключение отменено."

        if not ok or not summary_text.strip():
            return (
                f"❌ {old_engine_name} не вернул резюме. Переключение отменено. "
                "Можешь попробовать без переноса контекста."
            )

        # cleaned: вырежем file-маркеры (если случайно попали), не нужны в саммари.
        cleaned, _ = extract_file_markers(summary_text)
        cleaned = cleaned.strip() or summary_text.strip()
        # Безопасный лимит — Telegram-сообщения тут не ограничивают, но сильно
        # длинный summary раздует первый prompt. 4000 символов с запасом.
        cleaned = cleaned[:4000]

        # Переключаем движок и сохраняем pending_summary.
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
    /engine <name> [model-substring] — переключить без переноса контекста.
    При >1 моделях и без явного аргумента model — отказ с подсказкой использовать UI."""
    key = _key(update)
    args = context.args or []

    if not args:
        session_id, _, engine_name = get_session(*key)
        model = get_model(*key)
        model_line = f"\nМодель: {model}" if model else ""
        await update.message.reply_text(
            f"Движок этого топика: {engine_name}{model_line}\n"
            f"session-id: {session_id}\n"
            f"Дефолт (для новых топиков): {DEFAULT_ENGINE_NAME}\n\n"
            "Выбери новый движок ниже или введи /engine <name>.",
            reply_markup=_engine_keyboard(engine_name),
        )
        return

    target = args[0].strip().lower()
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

    target_engine = get_engine_by_name(target)
    models = list(target_engine.models)
    if model_idx < 0 or model_idx >= len(models):
        try:
            await query.answer("Модель не найдена", show_alert=True)
        except Exception:
            pass
        return
    chosen = models[model_idx]

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
        pending_summary = pop_pending_summary(*key)
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

        pending_summary = pop_pending_summary(*key)

        prompt_parts = [_system_prefix(effective_cwd)]
        if pending_summary:
            prompt_parts.append(
                "[Контекст от предыдущего движка — резюме прошлого диалога, "
                "продолжай с этой точки:]\n" + pending_summary
            )
        mgr_target = resolve_manager_topic()
        if mgr_target and mgr_target != (chat_id, thread_id):
            mgr_chat_id, mgr_thread_id = mgr_target
            prompt_parts.append(
                f"[SYSTEM NOTE: задача делегирована Менеджером через MCP "
                f"(job_id={job_id}). Отвечай как обычно — финальный ответ "
                f"пойдёт в этот топик (thread_id={thread_id}, cwd={effective_cwd}).\n"
                f"ПОСЛЕ финального ответа ОБЯЗАТЕЛЬНО отправь короткий "
                f"отчёт в топик Менеджера через "
                f"mcp__jarvis__manager_send с as_user=false:\n"
                f"  thread_id={mgr_thread_id}\n"
                f"  text:\n"
                f"    #job_{job_id} ✅ <одна строка: что сделано>\n"
                f"    src: thread_id={thread_id}, cwd={effective_cwd}\n"
                f"либо при провале — #job_{job_id} ❌ <что не получилось>.\n"
                f"Одно сообщение, без дублей и промежуточных статусов.]"
            )
        else:
            prompt_parts.append(
                "[SYSTEM NOTE: задача делегирована Менеджером через MCP "
                f"(job_id={job_id}). Отвечай как обычно.]"
            )
        prompt_parts.append("---\n\nСообщение пользователя:\n" + user_text)
        prompt = "\n\n".join(prompt_parts)

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

        try:
            with engine_model_scope(engine.name, model):
                ok, final_text, _sid_after = await call_llm_stream(
                    engine, session_id, prompt, key, cwd, on_intermediate,
                )
        except Exception as exc:
            logger.exception("manager job %s: llm crashed engine=%s", job_id, engine.name)
            ok, final_text = False, f"Внутренняя ошибка: {exc}"

        if indicator is not None:
            try:
                await indicator.delete()
            except Exception:
                pass

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

        logger.info("manager job %s done: ok=%s files=%d engine=%s",
                    job_id, ok, len(file_markers), engine.name)
        return ok, sent_msg_id
    finally:
        try:
            lock.release()
        except RuntimeError:
            pass


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
