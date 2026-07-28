"""Топик как рабочее место: ключ, роль, разделяемое состояние.

Топик Telegram — единица работы Jarvis: у него свой cwd, движок и своя очередь.
Здесь то, что относится к топику как таковому, до всякой бизнес-логики.

**Про разделяемое состояние.** Словари ниже — не кэш, а рабочие реестры живых
объектов: локов, процессов, живых воркеров. Они делятся ПО ССЫЛКЕ, поэтому
модули, которым они нужны, импортируют их отсюда и работают с тем же объектом.
Переприсваивать их нельзя (``chat_locks = {}`` в другом модуле создаст второй
реестр, и per-topic лок перестанет что-либо защищать) — только мутировать.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime

from telegram import Update

from engines.process_control import terminate_process_tree

from bot.db import _db
from bot.settings import int_env

logger = logging.getLogger(__name__)

TopicKey = tuple[int, int]


def _resolve_topic_from_env(prefix: str) -> TopicKey | None:
    raw_chat = os.environ.get(f"JARVIS_{prefix}_CHAT_ID")
    raw_thread = os.environ.get(f"JARVIS_{prefix}_THREAD_ID")
    if not (raw_chat and raw_thread):
        return None
    try:
        return int(raw_chat), int(raw_thread)
    except ValueError:
        logger.warning(
            "JARVIS_%s_{CHAT,THREAD}_ID not int: %r/%r",
            prefix, raw_chat, raw_thread,
        )
        return None


def resolve_secretary_topic() -> TopicKey | None:
    """Return the Secretary topic, or None.

    The old Manager topic becomes Secretary. New installs may set explicit
    JARVIS_SECRETARY_* variables; existing installs keep working through
    JARVIS_MANAGER_* as the compatibility alias.
    """
    return _resolve_topic_from_env("SECRETARY") or _resolve_topic_from_env("MANAGER")


def resolve_teamlead_topic() -> TopicKey | None:
    """Return the Teamlead topic, or fall back to Secretary/legacy Manager.

    Engineering notices must not disappear while the new topic is being wired.
    Once JARVIS_TEAMLEAD_* is configured, they stop waking Secretary.
    """
    return _resolve_topic_from_env("TEAMLEAD") or resolve_secretary_topic()


def resolve_manager_topic() -> TopicKey | None:
    """Compatibility alias for the old Manager topic, now Secretary.

    Existing MCP tools, docs and tests still use the manager naming. Keep the
    API stable while the human-facing role is split into Secretary + Teamlead.
    """
    return resolve_secretary_topic()


def resolve_service_topic(role: str) -> TopicKey | None:
    """Resolve a service role target used for notices/auto-kick."""
    role = (role or "").strip().lower()
    if role in {"teamlead", "lead", "engineering"}:
        return resolve_teamlead_topic()
    if role in {"secretary", "manager", "communications", "communication"}:
        return resolve_secretary_topic()
    logger.warning("unknown service topic role %r", role)
    return None


def resolve_topic_role(key: TopicKey) -> str:
    """Role of a Jarvis topic for external per-topic MCP credentials.

    The selected LLM engine is irrelevant here — the role belongs to the topic.
    External MCP servers use it to pick credentials, so that one forum can act
    under two identities without either leaking into the other's topics.
    """
    if _resolve_topic_from_env("TEAMLEAD") == key:
        return "teamlead"
    if resolve_secretary_topic() == key:
        return "secretary"
    return "agent"


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


# Живые процессы для топиков с /persistent on. Отдельно от active_procs:
# эти сообщения НЕ идут через chat_locks — сообщение, пришедшее пока живой
# процесс занят ходом, дописывается в него, а не ждёт очереди.
persistent_workers: dict[tuple[int, int], object] = {}


# Простаивающий живой процесс не экономит токены (сессия и так резюмируется
# с диска) — только задержку на старте. Держать его вечно смысла нет.
PERSISTENT_IDLE_MINUTES = int_env("JARVIS_PERSISTENT_IDLE_MINUTES", 20)


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


async def _kill_persistent_worker(key: tuple[int, int], reason: str) -> bool:
    """Убить живой процесс топика, если есть. Будит того, кто ждёт
    результата текущего хода (не вешает его до CLAUDE_TIMEOUT). Возвращает
    True, если воркер был и его убили."""
    worker = persistent_workers.pop(key, None)
    if worker is None:
        return False
    logger.info("killing persistent worker key=%s reason=%s", key, reason)
    worker.dead = True
    if worker.pending_future is not None and not worker.pending_future.done():
        worker.pending_future.set_result((False, reason))
    if worker.reader_task is not None:
        worker.reader_task.cancel()
    stderr_task = getattr(worker, "stderr_task", None)
    if stderr_task is not None:
        stderr_task.cancel()
    await terminate_process_tree(worker.proc)
    return True
