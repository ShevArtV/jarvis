"""Входящие сообщения: текст, фото, документы и ход по ним.

Два пути хода. Обычный — под per-topic локом: сообщение ждёт, пока закончится
предыдущее. ``/persistent`` — сообщение дописывается в живой процесс и
подхватывается моделью между шагами, не ожидая лока.
"""

from __future__ import annotations

import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

import asyncio
import os
import uuid
from datetime import datetime
from bot.asks import _mark_ask_answered, answer_ask, get_pending_ask
from bot.db import log_message
from bot.delivery import ProgressJournal, deliver_file_markers, extract_file_markers, send_claude_reply, send_to_topic
from bot.handlers.toggles import _ask_done_confirmation_if_needed, _warn_large_context_if_needed
from bot.llm import _build_reply_context_prefix, build_system_prefix, call_llm_stream
from bot.sessions import _parse_transfer_marker, _persistent_column_for_engine, build_context_handoff, clear_pending_summary, ensure_active_session, get_mcp_playwright, get_model, get_pending_summary, get_persistent_for_engine, get_session, update_session_id
from bot.settings import CLAUDE_CWD, MEDIA_DIR
from bot.topics import _key, _kill_persistent_worker, _lock_for, _persistent_start_lock_for, load_message_context, pending_queue, persistent_workers, resolve_topic_role
from engines import engine_model_scope, get_engine_by_name
from engines.claude_engine import CLAUDE_TIMEOUT, start_persistent as start_persistent_claude
from engines.codex_engine import CODEX_TIMEOUT
from engines.persistent_codex import start_persistent as start_persistent_codex











logger = logging.getLogger(__name__)

async def _resolve_pending_summary(
    key: tuple[int, int], pending: str,
) -> str | None:
    """Маркер transfer_requested → указание новому движку прочитать историю
    топика. Легаси-строки (готовое summary из старых версий) отдаём как есть."""
    marker = _parse_transfer_marker(pending)
    if marker is None:
        return pending  # легаси: реальное summary, уже лежит в БД

    old_engine_name = marker["old_engine"]
    logger.info("resolving transfer marker: key=%s old_engine=%s",
                key, old_engine_name)
    return build_context_handoff(key, old_engine_name)


async def _finish_turn_reply(
    chat, thread_id: int, journal: "ProgressJournal", ok: bool, final_text: str,
    engine_name: str, key: tuple[int, int],
) -> None:
    """Общий хвост хода: закрыть журнал, отправить финальный ответ и файлы.
    Общий для разового вызова движка и живого процесса claude (/persistent)."""
    await journal.finish(final_text)

    if not ok and not final_text.strip():
        logger.info("llm call stopped without final reply: key=%s engine=%s", key, engine_name)
        final_text = (
            f"{engine_name} не прислал финальный ответ "
            "(процесс завершился без текста). "
            "Попробуй /new для новой сессии или переключи движок через /engine."
        )

    cleaned_text, file_markers = extract_file_markers(final_text)
    if not cleaned_text.strip():
        cleaned_text = "(пустой ответ)" if not file_markers else "(см. вложения)"
    meta = {"type": "claude_response", "engine": engine_name}
    try:
        await send_claude_reply(chat, thread_id, cleaned_text, meta)
    except Exception:
        logger.exception("failed to send llm reply: key=%s", key)
    if file_markers:
        await deliver_file_markers(chat, thread_id, file_markers)

    session_id, cwd, current_engine_name = get_session(*key)
    await _warn_large_context_if_needed(
        chat, thread_id, current_engine_name or engine_name, session_id, cwd, key,
    )
    await _ask_done_confirmation_if_needed(chat, thread_id, ok, final_text, session_id, key)


async def _handle_persistent_message(
    chat, thread_id: int, key: tuple[int, int], user_text: str, meta_block: str,
) -> None:
    """Путь для топиков с /persistent on: без topic-lock и без «в очереди».

    Сообщение уходит живому процессу движка — новым ходом, если он свободен
    между ходами, или довеском к уже идущему ходу, если он ещё работает (без
    ожидания и без нового subprocess). Для claude довесок пишется в stdin
    stream-json; для codex — отправляется JSON-RPC `turn/steer` в app-server."""
    worker = persistent_workers.get(key)
    if worker is not None and (worker.dead or worker.proc.returncode is not None):
        persistent_workers.pop(key, None)
        worker = None

    pending_summary_delivered = False
    created_worker = False
    if worker is None:
        # /persistent bypasses the normal topic lock to allow steering an
        # active turn. Serialize only worker creation, then re-check the
        # registry: another caller may have finished thread/resume while this
        # one was waiting.
        async with _persistent_start_lock_for(key):
            worker = persistent_workers.get(key)
            if worker is not None and (worker.dead or worker.proc.returncode is not None):
                persistent_workers.pop(key, None)
                worker = None

            if worker is None:
                session_id, cwd, engine_name, opened_new = ensure_active_session(*key)
                if not get_persistent_for_engine(*key, engine_name):
                    # /engine сменили мимо /persistent — тихий fallback на обычный путь.
                    await _process_prompt_locked(chat, thread_id, key, user_text, meta_block)
                    return
                if _persistent_column_for_engine(engine_name) is None:
                    await _process_prompt_locked(chat, thread_id, key, user_text, meta_block)
                    return
                model = get_model(*key)

                if opened_new:
                    try:
                        await send_to_topic(
                            chat, thread_id,
                            f"🆕 Новый сеанс ({engine_name}). Контекст прошлых разговоров "
                            "не загружен — попроси поднять историю, если нужно.",
                        )
                    except Exception:
                        logger.exception("failed to send new-session notice key=%s", key)

                pending_raw = get_pending_summary(*key)
                pending_summary = None
                if pending_raw:
                    pending_summary = await _resolve_pending_summary(key, pending_raw)

                mcp_playwright = get_mcp_playwright(*key)
                mcp_topic_role = resolve_topic_role(key)
                effective_cwd = cwd or CLAUDE_CWD
                system_prefix = build_system_prefix(effective_cwd, mcp_playwright, key=key)

                try:
                    if engine_name == "claude":
                        worker = await start_persistent_claude(
                            key=key, session_id=session_id, cwd=effective_cwd, model=model,
                            system_prefix=system_prefix, mcp_playwright=mcp_playwright,
                            mcp_topic_role=mcp_topic_role,
                        )
                    elif engine_name == "codex":
                        worker = await start_persistent_codex(
                            key=key, session_id=session_id, cwd=effective_cwd, model=model,
                            system_prefix=system_prefix, mcp_playwright=mcp_playwright,
                            mcp_topic_role=mcp_topic_role,
                        )
                        if worker.session_id and worker.session_id != session_id:
                            update_session_id(key[0], key[1], "codex", worker.session_id)
                    else:
                        raise RuntimeError(f"persistent is not supported for {engine_name}")
                except Exception as exc:
                    logger.exception("persistent worker spawn failed key=%s", key)
                    await send_to_topic(
                        chat, thread_id,
                        f"⚠️ Не удалось поднять живой процесс {engine_name}: {exc}",
                    )
                    return
                persistent_workers[key] = worker
                created_worker = True

                prompt_parts: list[str] = []
                if pending_summary:
                    prompt_parts.append("[Контекст:]\n" + pending_summary)
                    pending_summary_delivered = True
                if meta_block:
                    prompt_parts.append(meta_block)
                prompt_parts.append("---\n\nСообщение пользователя:\n" + user_text)
                prompt = "\n\n".join(prompt_parts)

        if not created_worker:
            prompt_parts = [meta_block] if meta_block else []
            prompt_parts.append("---\n\nСообщение пользователя:\n" + user_text)
            prompt = "\n\n".join(prompt_parts)
    else:
        prompt_parts = [meta_block] if meta_block else []
        prompt_parts.append("---\n\nСообщение пользователя:\n" + user_text)
        prompt = "\n\n".join(prompt_parts)

    is_new, fut = await worker.submit(prompt)

    if not is_new:
        try:
            await send_to_topic(
                chat, thread_id,
                "✅ Добавил к тому, над чем сейчас работаю — учту сразу после текущего шага.",
            )
        except Exception:
            logger.exception("failed to send persistent-append ack key=%s", key)
        return

    journal = ProgressJournal(chat, thread_id)
    await journal.start()
    worker.on_intermediate = journal.append
    timeout = CODEX_TIMEOUT if get_session(*key)[2] == "codex" else CLAUDE_TIMEOUT
    try:
        ok, final_text = await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning("persistent worker timeout key=%s", key)
        await _kill_persistent_worker(key, "")
        engine_name = get_session(*key)[2]
        ok, final_text = False, f"Timeout: живой {engine_name} не ответил за {timeout}с."
    except Exception as exc:
        logger.exception("persistent worker await failed key=%s", key)
        ok, final_text = False, f"Внутренняя ошибка: {exc}"
    finally:
        if worker is not None:
            worker.on_intermediate = None

    if ok and pending_summary_delivered:
        clear_pending_summary(*key)

    engine_name = get_session(*key)[2]
    await _finish_turn_reply(chat, thread_id, journal, ok, final_text, engine_name, key)
    logger.info("persistent turn done: key=%s engine=%s ok=%s", key, engine_name, ok)


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

    # Агент ждёт ответа на ask_user? Тогда это сообщение — ОТВЕТ ему, а не новый
    # запрос. Перехватываем ДО очереди и лока (или живого процесса) — агент
    # стоит в ask_user, и без перехвата ответ ушёл бы к нему вторым, отдельным ходом.
    pending_ask = get_pending_ask(chat.id, thread_id)
    if pending_ask is not None and user_text.strip():
        if answer_ask(pending_ask["id"], user_text.strip(), via="text"):
            logger.info("ask #%s answered by text: key=%s", pending_ask["id"], key)
            await _mark_ask_answered(chat, pending_ask, user_text.strip())
            return
        # Не смогли записать — вопрос уже закрыт (кнопка/таймаут). Обрабатываем
        # сообщение как обычный запрос.
        logger.info("ask #%s already closed, treating as normal message",
                    pending_ask["id"])

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

    # Живой процесс (/persistent on) — своя ветка, без topic-lock:
    # сообщение, пришедшее пока агент работает, дописывается ему на лету.
    _peek_sid, _peek_cwd, peek_engine_name = get_session(*key)
    if get_persistent_for_engine(*key, peek_engine_name):
        await _handle_persistent_message(chat, thread_id, key, user_text, meta_block)
        return

    await _process_prompt_locked(chat, thread_id, key, user_text, meta_block)


async def _process_prompt_locked(
    chat, thread_id: int, key: tuple[int, int], user_text: str, meta_block: str,
) -> None:
    """Обычный путь: topic-lock, «в очереди», процесс движка на сообщение."""
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
        # Сеанс под локом (вдруг /close, /reset или /engine сработали, пока мы
        # стояли в очереди). Протухший или закрытый сеанс здесь же открывается
        # заново — это аналог «открыть проект в терминале».
        session_id, cwd, engine_name, opened_new = ensure_active_session(*key)
        engine = get_engine_by_name(engine_name)
        model = get_model(*key)

        if opened_new:
            try:
                await send_to_topic(
                    chat, thread_id,
                    f"🆕 Новый сеанс ({engine_name}). Контекст прошлых разговоров "
                    "не загружен — попроси поднять историю, если нужно.",
                )
            except Exception:
                logger.exception("failed to send new-session notice key=%s", key)

        # Pending handoff: pop'аем атомарно — доставляется ровно один раз,
        # в первый prompt после переключения движка с переносом контекста.
        pending_raw = get_pending_summary(*key)
        pending_summary = None
        if pending_raw:
            pending_summary = await _resolve_pending_summary(key, pending_raw)
            if pending_summary:
                logger.info("delivering pending context to engine=%s key=%s (%d chars)",
                            engine_name, key, len(pending_summary))

        prompt_parts: list[str] = []  # [SYSTEM:]-блок уходит через системный канал движка
        if pending_summary:
            prompt_parts.append("[Контекст:]\n" + pending_summary)
        if meta_block:
            prompt_parts.append(meta_block)
        prompt_parts.append("---\n\nСообщение пользователя:\n" + user_text)
        prompt = "\n\n".join(prompt_parts)

        # Журнал хода: шаги агента копятся в одном сообщении и остаются в топике.
        journal = ProgressJournal(chat, thread_id)
        await journal.start()

        try:
            with engine_model_scope(engine.name, model):
                ok, final_text, _sid_after = await call_llm_stream(
                    engine, session_id, prompt, key, cwd, journal.append,
                )
            if ok and pending_summary:
                clear_pending_summary(*key)
        except Exception as exc:
            logger.exception("llm call crashed: key=%s engine=%s", key, engine.name)
            ok, final_text = False, f"Внутренняя ошибка: {exc}"

        await _finish_turn_reply(chat, thread_id, journal, ok, final_text, engine.name, key)

        logger.info("lock released: key=%s ok=%s engine=%s", key, ok, engine.name)
    finally:
        try:
            lock.release()
        except RuntimeError:
            pass


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
