"""Выполнение делегированных работ: задачи Менеджера, /spawn, внешние триггеры.

Три источника хода, отличающиеся обвязкой, но не сутью:

* **job** — задача, поставленная Менеджером: с heartbeat, с нотисом об ответе и
  возможностью прерывания;
* **/spawn** — разовый параллельный ход в том же топике, вне per-topic лока;
* **trigger** — ход, поднятый внешней интеграцией: без job_id, heartbeat и
  служебных нотисов, чтобы не будить Менеджера на каждый чужой шаг.
"""

from __future__ import annotations

import asyncio
import logging
import secrets

from telegram import Update
from telegram.ext import Application

from engines import engine_model_scope, get_engine_by_name
from engines.process_control import terminate_process_tree

from bot.db import _db, log_message
from bot.formatting import _html_escape
from bot.delivery import (
    ProgressJournal,
    _send_manager_notice,
    deliver_file_markers,
    extract_file_markers,
    send_claude_reply,
    send_to_topic,
)
from bot.llm import call_llm_stream
from bot.sessions import (
    clear_pending_summary,
    ensure_active_session,
    get_model,
    get_pending_summary,
    get_persistent_for_engine,
    get_session,
)
from bot.settings import CLAUDE_CWD
from bot.topics import (
    _key,
    _lock_for,
    active_procs,
    resolve_teamlead_topic,
)

logger = logging.getLogger(__name__)


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
    # Дефолт cwd подставит call_llm_stream — сюда передаём сырой cwd.
    _, cwd, engine_name = get_session(*key)
    engine = get_engine_by_name(engine_name)
    model = get_model(*key)
    session_id = engine.new_session_id()

    # [SYSTEM:]-блок уходит через системный канал движка (call_llm_stream).
    prompt = (
        "---\n\nСообщение пользователя (одноразовый spawn):\n"
        + user_text
    )

    journal = ProgressJournal(
        chat, thread_id, prefix=prefix, header="⏳ Spawn запущен...",
    )
    await journal.start()

    try:
        with engine_model_scope(engine.name, model):
            ok, final_text, _sid_after = await call_llm_stream(
                engine, session_id, prompt, key, cwd, journal.append, spawn_id=spawn_id,
            )
    except Exception as exc:
        logger.exception("spawn crashed: key=%s spawn=%s engine=%s",
                         key, spawn_id, engine.name)
        ok, final_text = False, f"Внутренняя ошибка: {exc}"

    await journal.finish(final_text)

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


async def _run_manager_job(app: Application, job: dict) -> tuple[bool, int | None, str | None]:
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
        return False, None, "failed to resolve target chat via Telegram API"

    lock = _lock_for(key)
    await lock.acquire()
    try:
        logger.info("manager job %s: lock acquired key=%s", job_id, key)
        # Задача исполняется в сеансе топика; протухший сеанс здесь тоже
        # переоткрывается. Job самодостаточен — полный текст задачи внутри.
        session_id, cwd, engine_name, opened_new = ensure_active_session(*key)
        engine = get_engine_by_name(engine_name)
        model = get_model(*key)
        effective_cwd = cwd or CLAUDE_CWD
        if opened_new:
            logger.info("manager job %s: opened new session %s for key=%s",
                        job_id, session_id, key)

        pending_raw = get_pending_summary(*key)
        if pending_raw:
            from bot.handlers.messages import _resolve_pending_summary

            pending_summary = await _resolve_pending_summary(key, pending_raw)
        else:
            pending_summary = None

        prompt_parts: list[str] = []  # [SYSTEM:]-блок уходит через системный канал движка
        if pending_summary:
            prompt_parts.append("[Контекст:]\n" + pending_summary)
        mgr_target = resolve_teamlead_topic()
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
                f"ищи команду в документации проекта (например "
                f"production_smoke_check.md). Если smoke упал — откатить через "
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

        # Self-kick — служебный ход Менеджера, топик им не засоряем: журнала нет.
        journal: ProgressJournal | None = None
        if not is_self_kick:
            journal = ProgressJournal(
                chat, thread_id,
                header=f"⏳ Manager делегировал задачу (job #{job_id})...",
            )
            await journal.start()

        async def on_intermediate(text: str) -> None:
            if journal is not None:
                await journal.append(text)

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
            if ok and pending_summary:
                clear_pending_summary(*key)
        except Exception as exc:
            logger.exception("manager job %s: llm crashed engine=%s", job_id, engine.name)
            ok, final_text = False, f"Внутренняя ошибка: {exc}"
        finally:
            watcher_stop.set()
            try:
                await asyncio.wait_for(watcher_task, timeout=3.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                watcher_task.cancel()

        if journal is not None:
            await journal.finish(final_text)

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
                target_role="teamlead",
            )
            logger.info("manager job %s: interrupted by manager", job_id)
            return False, None, "interrupted by manager request"

        if not ok and not final_text.strip():
            logger.info("manager job %s stopped without final reply", job_id)
            final_text = (
                f"{engine.name} не прислал финальный ответ "
                "(процесс завершился без текста). "
                "Рекомендуется запустить /new в этом топике и повторить задачу."
            )

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
        if not is_self_kick and resolve_teamlead_topic() != (chat_id, thread_id):
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
            await _send_manager_notice(
                app, notice_text, kind="job_notification", target_role="teamlead",
            )

        logger.info("manager job %s done: ok=%s files=%d engine=%s",
                    job_id, ok, len(file_markers), engine.name)
        err_text = None if ok else (cleaned_text[:1000] if cleaned_text else "engine returned no usable reply")
        return ok, sent_msg_id, err_text
    finally:
        try:
            lock.release()
        except RuntimeError:
            pass


async def _process_agent_trigger(app: Application, trigger: dict) -> tuple[bool, str | None]:
    """Run one non-job trigger through the normal topic LLM pipeline."""
    chat_id = trigger["chat_id"]
    thread_id = trigger["thread_id"]
    key = (chat_id, thread_id)
    text = trigger["text"]
    source = trigger.get("source") or "external"
    try:
        chat = await app.bot.get_chat(chat_id)
    except Exception:
        logger.exception("agent trigger %s: bot.get_chat(%s) failed", trigger["id"], chat_id)
        return False, "failed to resolve target chat via Telegram API"

    log_message(chat_id, thread_id, "in", f"{source}_inject", text, None)

    # Ленивый импорт: обработчики сообщений сами зовут _run_spawn отсюда,
    # и импорт на уровне модуля дал бы цикл.
    from bot.handlers.messages import (
        _handle_persistent_message,
        _process_prompt_locked,
    )

    try:
        _sid, _cwd, engine_name = get_session(*key)
        if get_persistent_for_engine(*key, engine_name):
            await _handle_persistent_message(chat, thread_id, key, text, "")
        else:
            await _process_prompt_locked(chat, thread_id, key, text, "")
    except Exception as exc:
        logger.exception("agent trigger %s crashed key=%s", trigger["id"], key)
        return False, str(exc)[:1000]
    return True, None
