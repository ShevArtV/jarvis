#!/usr/bin/env python3
"""Jarvis — тонкая Telegram-обёртка над `claude` CLI.

Модель: «один топик Telegram = один проект = одна постоянная claude-сессия».
- Ключ сессии — (chat_id, message_thread_id). В не-форумных чатах thread_id=0.
- Каждый топик может быть привязан к своей рабочей директории (cwd) командой /bind.
- Внутри ключа вызовы сериализуются через asyncio.Lock; разные ключи работают параллельно.
- Используется stream-json: промежуточные сообщения (tool_use, рассуждения) показываются пользователю.
"""

import os
import re
import json
import uuid
import secrets
import sqlite3
import asyncio
import logging
import tempfile
import subprocess
import time
from datetime import datetime
from pathlib import Path

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

from config import TELEGRAM_TOKEN, ALLOWED_USER_IDS, BASE_DIR

# ---------- Константы ----------

CLAUDE_BIN = os.environ.get("CLAUDE_BIN", "claude")
# Дефолтный cwd для топиков без явного /bind.
CLAUDE_CWD = os.environ.get("CLAUDE_CWD", "/home/shevartv")
CLAUDE_TIMEOUT = int(os.environ.get("CLAUDE_TIMEOUT", "3600"))

MSG_LIMIT = 3500           # порог отправки ответа как документ
TG_HARD_LIMIT = 4096       # жёсткий лимит Telegram
INTERMEDIATE_MIN_INTERVAL = 2.0  # сек между апдейтами промежуточного сообщения
TG_FILE_LIMIT_MB = 50      # Telegram Bot API лимит на sendDocument

# Маркер для отправки файлов из claude-сессии: [[FILE: /abs/path]] или [[FILE: /path | подпись]].
# Должен стоять на отдельной строке (но допускаются пробелы вокруг).
FILE_MARKER_RE = re.compile(
    r"^[ \t]*\[\[FILE:\s*(?P<path>[^|\]\n]+?)(?:\s*\|\s*(?P<caption>[^\]\n]+?))?\s*\]\][ \t]*$",
    re.MULTILINE,
)

APPEND_SYSTEM_PROMPT = (
    "Если нужно отправить пользователю файл (скриншот, собранный пакет, "
    "сгенерированный документ и т.п.) — выведи отдельной строкой маркер "
    "[[FILE: /абсолютный/путь]] (опционально с подписью через '|': "
    "[[FILE: /путь | подпись]]). Бот автоматически отправит файл в Telegram. "
    "Используй только для файлов в пределах cwd сессии или явно указанных пользователем."
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
        f"Опасные действия (удаления, DELETE/DROP, действия на проде, sudo, push --force) — "
        f"переспрашивай.]"
    )


# ---------- SQLite ----------

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    with _db() as conn:
        # Миграция: если sessions существует со старым PK (chat_id) без колонок thread_id/cwd —
        # пересоздаём таблицу и переносим данные (thread_id=0).
        cols = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
        if cols and "thread_id" not in cols:
            logger.info("migrating sessions table: adding thread_id/cwd, new PK (chat_id, thread_id)")
            conn.execute("ALTER TABLE sessions RENAME TO sessions_old")
            conn.execute(
                """
                CREATE TABLE sessions (
                    chat_id INTEGER NOT NULL,
                    thread_id INTEGER NOT NULL DEFAULT 0,
                    session_id TEXT NOT NULL,
                    cwd TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (chat_id, thread_id)
                )
                """
            )
            conn.execute(
                "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, updated_at) "
                "SELECT chat_id, 0, session_id, NULL, updated_at FROM sessions_old"
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
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (chat_id, thread_id)
                )
                """
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

def get_session(chat_id: int, thread_id: int) -> tuple[str, str | None]:
    """Возвращает (session_id, cwd). Если записи нет — создаёт новую с UUID и cwd=NULL."""
    with _db() as conn:
        row = conn.execute(
            "SELECT session_id, cwd FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        if row:
            return row[0], row[1]
        new_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, updated_at) "
            "VALUES (?, ?, ?, NULL, ?)",
            (chat_id, thread_id, new_id, datetime.utcnow().isoformat()),
        )
        return new_id, None


def reset_session(chat_id: int, thread_id: int) -> tuple[str, str | None]:
    """Новый UUID; cwd сохраняется. Если записи нет — создаётся."""
    new_id = str(uuid.uuid4())
    with _db() as conn:
        row = conn.execute(
            "SELECT cwd FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        cwd = row[0] if row else None
        conn.execute(
            "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, updated_at) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(chat_id, thread_id) DO UPDATE SET "
            "session_id=excluded.session_id, updated_at=excluded.updated_at",
            (chat_id, thread_id, new_id, cwd, datetime.utcnow().isoformat()),
        )
    return new_id, cwd


def set_cwd(chat_id: int, thread_id: int, cwd: str) -> None:
    """Создаёт запись, если её нет (session_id — новый UUID), либо обновляет cwd."""
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
                "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (chat_id, thread_id, str(uuid.uuid4()), cwd, now),
            )


def clear_cwd(chat_id: int, thread_id: int) -> None:
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET cwd = NULL, updated_at = ? WHERE chat_id = ? AND thread_id = ?",
            (datetime.utcnow().isoformat(), chat_id, thread_id),
        )


# ---------- Claude sessions dir / pidfile / resume ----------

def _sessions_dir_for(cwd: str) -> Path:
    """~/.claude/projects/<encoded-cwd>/  (Claude CLI кодирует путь заменой / на -)."""
    encoded = cwd.replace("/", "-")
    return Path.home() / ".claude" / "projects" / encoded


def _session_exists(session_id: str, cwd: str) -> bool:
    return (_sessions_dir_for(cwd) / f"{session_id}.jsonl").exists()


def _clear_stale_session_pidfile(session_id: str) -> None:
    """Claude CLI хранит лок-файлы в ~/.claude/sessions/<pid>.json с полем sessionId.
    Если процесс мёртв — удаляем файл, чтобы --resume не упёрся в 'session in use'."""
    sessions_dir = Path.home() / ".claude" / "sessions"
    if not sessions_dir.is_dir():
        return
    for p in sessions_dir.glob("*.json"):
        try:
            data = json.loads(p.read_text())
        except Exception:
            continue
        if data.get("sessionId") != session_id:
            continue
        pid = data.get("pid")
        alive = False
        if isinstance(pid, int):
            try:
                os.kill(pid, 0)
                alive = True
            except ProcessLookupError:
                alive = False
            except PermissionError:
                alive = True  # чужой, не трогаем
        if not alive:
            try:
                p.unlink()
                logger.info("removed stale session lock %s (pid=%s sid=%s)",
                            p, pid, session_id)
            except OSError:
                pass


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


# ---------- Отправка в топик ----------

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
):
    """Короткий текст — send_message; длинный — как .md вложение в тот же топик."""
    if len(text) <= MSG_LIMIT:
        sent = await send_to_topic(chat, thread_id, text[:TG_HARD_LIMIT])
        try:
            save_message_context(chat.id, sent.message_id, meta)
        except Exception:
            logger.exception("save_message_context failed")
        return sent

    preview = text[:200].rstrip() + "...\n\nполный ответ во вложении"
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
        except Exception:
            logger.exception("save_message_context failed")
        return sent
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


# ---------- Вызов claude (stream-json) ----------

async def call_claude_stream(
    session_id: str,
    prompt: str,
    key: tuple[int, int],
    cwd: str | None,
    on_intermediate,
    spawn_id: str | None = None,
) -> tuple[bool, str]:
    """Запускает claude с stream-json, парсит события.
    Возвращает (ok, final_text). Промежуточные события отдаёт в on_intermediate(text).

    Если spawn_id задан — это одноразовая параллельная сессия (всегда new session),
    процесс регистрируется в spawn_procs[(chat, thread, spawn_id)], а не в active_procs."""
    effective_cwd = cwd or CLAUDE_CWD

    is_spawn = spawn_id is not None
    if is_spawn:
        # Одноразовая сессия: всегда new, без resume.
        session_flags = ["--session-id", session_id]
        resume_mode = False
    else:
        resume_mode = _session_exists(session_id, effective_cwd)
        if resume_mode:
            _clear_stale_session_pidfile(session_id)
            session_flags = ["--resume", session_id]
        else:
            session_flags = ["--session-id", session_id]

    cmd = [
        CLAUDE_BIN, "--print",
        "--permission-mode", "bypassPermissions",
        "--input-format", "text",
        "--output-format", "stream-json",
        "--verbose",
        "--append-system-prompt", APPEND_SYSTEM_PROMPT,
        *session_flags,
        prompt,
    ]

    logger.info(
        "claude start: key=%s session=%s mode=%s cwd=%s prompt_len=%d spawn_id=%s",
        key, session_id, "resume" if resume_mode else "new", effective_cwd, len(prompt),
        spawn_id,
    )

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=effective_cwd,
            limit=10 * 1024 * 1024,
        )
    except FileNotFoundError:
        return False, f"`{CLAUDE_BIN}` не найден в PATH."

    if is_spawn:
        spawn_procs[(key[0], key[1], spawn_id)] = proc
    else:
        active_procs[key] = proc

    final_text = ""
    buffer_intermediate: list[str] = []
    last_push = 0.0

    async def flush_intermediate(force: bool = False) -> None:
        nonlocal last_push
        if not buffer_intermediate:
            return
        now = time.monotonic()
        if not force and (now - last_push) < INTERMEDIATE_MIN_INTERVAL:
            return
        text = "\n".join(buffer_intermediate)
        buffer_intermediate.clear()
        last_push = now
        try:
            await on_intermediate(text)
        except Exception:
            logger.exception("on_intermediate failed")

    async def read_stream():
        nonlocal final_text
        assert proc.stdout is not None
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            raw = line.decode("utf-8", errors="replace").strip()
            if not raw:
                continue
            try:
                ev = json.loads(raw)
            except json.JSONDecodeError:
                continue
            etype = ev.get("type")
            if etype == "assistant":
                msg = ev.get("message", {}) or {}
                for block in msg.get("content", []) or []:
                    btype = block.get("type")
                    if btype == "text":
                        txt = (block.get("text") or "").strip()
                        if txt:
                            buffer_intermediate.append(txt[:800])
                    elif btype == "tool_use":
                        name = block.get("name", "?")
                        inp = block.get("input") or {}
                        summary = ""
                        if isinstance(inp, dict):
                            for k in ("command", "file_path", "path", "pattern", "url", "description"):
                                if k in inp and inp[k]:
                                    s = str(inp[k])
                                    summary = f" {k}={s[:120]}"
                                    break
                        buffer_intermediate.append(f"🔧 {name}{summary}")
                await flush_intermediate()
            elif etype == "result":
                r = ev.get("result")
                if isinstance(r, str):
                    final_text = r

    try:
        try:
            await asyncio.wait_for(read_stream(), timeout=CLAUDE_TIMEOUT)
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            await proc.wait()
            return False, f"Timeout: claude не ответил за {CLAUDE_TIMEOUT}с."
        await proc.wait()
    except asyncio.CancelledError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        raise
    finally:
        await flush_intermediate(force=True)
        if is_spawn:
            skey = (key[0], key[1], spawn_id)
            if spawn_procs.get(skey) is proc:
                spawn_procs.pop(skey, None)
        else:
            if active_procs.get(key) is proc:
                active_procs.pop(key, None)

    stderr_text = ""
    if proc.stderr is not None:
        try:
            stderr_b = await proc.stderr.read()
            stderr_text = stderr_b.decode("utf-8", errors="replace")
        except Exception:
            pass

    if proc.returncode != 0:
        logger.warning("claude rc=%s stderr=%s", proc.returncode, stderr_text[:500])
        if not final_text:
            return False, f"Ошибка claude (rc={proc.returncode}): {stderr_text[:1500] or '(пусто)'}"

    logger.info("claude done: key=%s rc=%s final_len=%d", key, proc.returncode, len(final_text))
    if not final_text.strip():
        return False, "claude вернул пустой ответ." + (f"\n{stderr_text[:500]}" if stderr_text else "")
    return True, final_text


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
    session_id, cwd = get_session(*key)
    effective = cwd or CLAUDE_CWD
    text = (
        "Привет! Я Jarvis — Telegram-обёртка над claude CLI.\n\n"
        f"Этот топик привязан к сессии `{session_id}`\n"
        f"Рабочая директория: `{effective}`" + (" (дефолт)" if not cwd else "") + "\n\n"
        "Команды:\n"
        "/new, /reset — новая сессия (cwd сохраняется)\n"
        "/stop — прервать текущий запрос (сессия сохраняется)\n"
        "/session — показать session-id и cwd\n"
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
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        logger.info("reset: killed active proc for key=%s", key)
    new_id, cwd = reset_session(*key)
    effective = cwd or CLAUDE_CWD
    logger.info("reset: key=%s new_session=%s cwd=%s", key, new_id, effective)
    await update.message.reply_text(
        f"Сессия сброшена, новый id: {new_id}\nCwd: {effective}"
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
        try:
            sproc.terminate()
            try:
                await asyncio.wait_for(sproc.wait(), timeout=2)
            except asyncio.TimeoutError:
                sproc.kill()
                try:
                    await asyncio.wait_for(sproc.wait(), timeout=2)
                except asyncio.TimeoutError:
                    pass
        except ProcessLookupError:
            pass
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
    try:
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=2)
        except asyncio.TimeoutError:
            proc.kill()
            try:
                await asyncio.wait_for(proc.wait(), timeout=2)
            except asyncio.TimeoutError:
                pass
    except ProcessLookupError:
        pass
    # Достаём session_id из БД и чистим pidfile, чтобы следующий запрос не упёрся в "session in use".
    session_id, _ = get_session(*key)
    _clear_stale_session_pidfile(session_id)
    active_procs.pop(key, None)
    logger.info("stop: killed proc pid=%s for key=%s, cleaned pidfile for %s",
                pid, key, session_id)
    await update.message.reply_text("⛔ Текущий запрос прерван. Сессия сохранена.")


async def cmd_session(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    key = _key(update)
    session_id, cwd = get_session(*key)
    effective = cwd or CLAUDE_CWD
    await update.message.reply_text(
        f"session-id: {session_id}\ncwd: {effective}" + (" (дефолт)" if not cwd else "")
    )


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
    _, cwd = get_session(*key)
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
        # Получаем актуальные session/cwd уже под локом (вдруг /reset сработал)
        session_id, cwd = get_session(*key)
        effective_cwd = cwd or CLAUDE_CWD

        prompt_parts = [_system_prefix(effective_cwd)]
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
            if indicator is not None:
                try:
                    await indicator.edit_text(preview)
                    return
                except Exception:
                    indicator = None
            try:
                indicator = await send_to_topic(chat, thread_id, preview)
            except Exception:
                pass

        try:
            ok, final_text = await call_claude_stream(
                session_id, prompt, key, cwd, on_intermediate,
            )
        except Exception as exc:
            logger.exception("claude call crashed: key=%s", key)
            ok, final_text = False, f"Внутренняя ошибка: {exc}"

        # Удалить индикатор (если можем) и отправить финал
        if indicator is not None:
            try:
                await indicator.delete()
            except Exception:
                pass

        # Извлекаем маркеры файлов и отправляем их отдельными документами.
        cleaned_text, file_markers = extract_file_markers(final_text)
        if not cleaned_text.strip():
            cleaned_text = "(пустой ответ)" if not file_markers else "(см. вложения)"
        meta = {"type": "claude_response"}
        try:
            await send_claude_reply(chat, thread_id, cleaned_text, meta)
        except Exception:
            logger.exception("failed to send claude reply: key=%s", key)
        if file_markers:
            await deliver_file_markers(chat, thread_id, file_markers)

        logger.info("lock released: key=%s ok=%s files=%d",
                    key, ok, len(file_markers))
    finally:
        try:
            lock.release()
        except RuntimeError:
            pass


async def _run_spawn(update: Update, user_text: str) -> None:
    """Одноразовая параллельная claude-сессия. Не использует lock топика,
    не сохраняет session_id в БД. Все сообщения помечаются префиксом [#xxxx]."""
    key = _key(update)
    chat = update.effective_chat
    thread_id = key[1]

    spawn_id = secrets.token_hex(2)  # 4 hex-символа
    prefix = f"[#{spawn_id}] "

    # cwd наследуется из топика (но session_id новый, в БД не сохраняется).
    _, cwd = get_session(*key)
    effective_cwd = cwd or CLAUDE_CWD
    session_id = str(uuid.uuid4())

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
        msg = prefix + body
        if indicator is not None:
            try:
                await indicator.edit_text(msg)
                return
            except Exception:
                indicator = None
        try:
            indicator = await send_to_topic(chat, thread_id, msg)
        except Exception:
            pass

    try:
        ok, final_text = await call_claude_stream(
            session_id, prompt, key, cwd, on_intermediate, spawn_id=spawn_id,
        )
    except Exception as exc:
        logger.exception("spawn crashed: key=%s spawn=%s", key, spawn_id)
        ok, final_text = False, f"Внутренняя ошибка: {exc}"

    if indicator is not None:
        try:
            await indicator.delete()
        except Exception:
            pass

    cleaned_text, file_markers = extract_file_markers(final_text)
    if not cleaned_text.strip():
        cleaned_text = "(пустой ответ)" if not file_markers else "(см. вложения)"
    # Префиксуем финальный ответ
    final_with_prefix = prefix + cleaned_text
    meta = {"type": "claude_response", "spawn_id": spawn_id}
    try:
        await send_claude_reply(
            chat, thread_id, final_with_prefix, meta,
            filename_prefix=f"spawn_{spawn_id}",
        )
    except Exception:
        logger.exception("failed to send spawn reply: key=%s spawn=%s", key, spawn_id)
    if file_markers:
        await deliver_file_markers(chat, thread_id, file_markers, notice_prefix=prefix)
    logger.info("spawn done: key=%s spawn=%s ok=%s files=%d",
                key, spawn_id, ok, len(file_markers))


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

def main() -> None:
    print("=== Jarvis Telegram Bot (per-topic claude session) ===")

    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN не задан. См. .env.example.")
    if not ALLOWED_USER_IDS:
        logger.warning("ALLOWED_USER_IDS пуст — никто не сможет писать боту.")

    init_db()

    # concurrent_updates=True: без этого PTB обрабатывает апдейты последовательно,
    # и per-key asyncio.Lock не даёт параллельности между разными топиками —
    # второй топик ждёт, пока освободится воркер PTB, а не сам lock.
    app = Application.builder().token(TELEGRAM_TOKEN).concurrent_updates(True).build()

    allowed = filters.User(user_id=ALLOWED_USER_IDS)

    app.add_handler(CommandHandler("start", cmd_start, filters=allowed))
    app.add_handler(CommandHandler("new", cmd_reset, filters=allowed))
    app.add_handler(CommandHandler("reset", cmd_reset, filters=allowed))
    app.add_handler(CommandHandler("stop", cmd_stop, filters=allowed))
    app.add_handler(CommandHandler("spawn", cmd_spawn, filters=allowed))
    app.add_handler(CommandHandler("session", cmd_session, filters=allowed))
    app.add_handler(CommandHandler("bind", cmd_bind, filters=allowed))
    app.add_handler(CommandHandler("unbind", cmd_unbind, filters=allowed))
    app.add_handler(CommandHandler("where", cmd_where, filters=allowed))

    app.add_handler(MessageHandler(allowed & filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(allowed & filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(allowed & filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(on_cancel_queue, pattern=r"^cancel_queue:"))

    app.add_handler(MessageHandler(~allowed, unauthorized_handler))

    logger.info("Whitelisted user_ids: %s", sorted(ALLOWED_USER_IDS))
    logger.info("Claude binary: %s  default cwd=%s", CLAUDE_BIN, CLAUDE_CWD)
    print("Бот запущен. Жду сообщения в Telegram...")
    app.run_polling()


if __name__ == "__main__":
    main()
