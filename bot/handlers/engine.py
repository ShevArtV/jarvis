"""Переключение движка и модели: ``/engine`` и его инлайн-диалог.

Смена движка — не просто запись в БД: у каждого CLI свой формат транскрипта,
поэтому контекст не переносится, и пользователя надо спросить, переносить ли
текстовое резюме прежнего диалога (``_carry_keyboard`` → handoff).
"""

from __future__ import annotations

import logging
import re
import shutil

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import ContextTypes

from bot.delivery import send_to_topic
from bot.formatting import _html_escape
from bot.handlers.commands import _topic_status_block
from bot.sessions import _transfer_marker, get_model, get_session, set_engine, set_pending_summary, update_model_only
from bot.settings import CLAUDE_CWD, DEFAULT_ENGINE_NAME
from bot.topics import _key, _kill_persistent_worker, _lock_for, active_procs
from engines import SUPPORTED_ENGINES, ensure_engine_tools, get_engine_by_name
from engines.model_cache import label_for
from engines.process_control import terminate_process_tree

logger = logging.getLogger(__name__)

def _engine_keyboard(current_engine: str) -> InlineKeyboardMarkup:
    """Inline-клавиатура с кнопками выбора движка. Текущий помечается ✓."""
    row = []
    for name in SUPPORTED_ENGINES:
        label = f"✓ {name}" if name == current_engine else name
        row.append(InlineKeyboardButton(label, callback_data=f"engine_select:{name}"))
    return InlineKeyboardMarkup([row])


def _model_label(model: str) -> str:
    """Сокращение для отображения: 'deepseek/deepseek-v4-flash' → 'deepseek-v4-flash'.

    Провайдера прячем, только если он и так дублируется в имени модели: в списке
    opencode рядом живут 'deepseek/deepseek-chat' и 'opencode/hy3-free', и у
    второго провайдер — единственное, что говорит, чья это модель.
    Если CLI сообщил имя модели (claude: 'opus' → 'Opus 5.5') — показываем его."""
    label = label_for(model)
    if label:
        return label
    provider, _, short = model.partition("/")
    if short and short.startswith(provider):
        return short
    return model


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
    await _kill_persistent_worker(key, "движок переключён через /engine")

    new_id, cwd = set_engine(key[0], key[1], target, model=model)
    effective = cwd or CLAUDE_CWD
    logger.info("engine switched for key=%s: %s -> %s (new sid=%s, model=%s)",
                key, current_engine, target, new_id, model)
    mcp_line = f"\n{mcp_status}" if mcp_ok else f"\n⚠️ {mcp_status}"
    model_line = f"\nМодель: {_model_label(model)}" if model else ""
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
    """Сценарий «с переносом контекста»: переключить движок и велеть новому
    поднять историю топика самому (через manager_inbox).

    Раньше здесь старый движок гонялся за резюме — полный проход по всей его
    истории, самый дорогой вызов из возможных, да ещё и до переключения. Теперь
    переключение мгновенное и бесплатное: новый движок читает ровно столько,
    сколько ему нужно, и только когда ему нужно.
    """
    chat_id, thread_id = key

    lock = _lock_for(key)
    if lock.locked():
        return (
            "⚠️ Топик занят активным запросом. Дождись завершения или /stop, "
            "потом повтори переключение."
        )

    await lock.acquire()
    try:
        await progress_edit("🔁 Переключаю движок...")
        switch_text = await _do_engine_switch(key, new_engine_name, model=model)
        set_pending_summary(
            chat_id, thread_id, _transfer_marker(old_engine_name),
        )
        logger.info("handoff: stored transfer marker for key=%s (old=%s)",
                    key, old_engine_name)
        return (
            f"{switch_text}\n\n"
            f"📖 Новый движок сам поднимет историю топика через manager_inbox "
            f"при первом сообщении — резюме у {old_engine_name} не запрашиваем."
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
        substr = " ".join(args[1:]).strip().lower()  # «Fable 5.1» — с пробелом
        exact = [m for m in models if substr in (m.lower(), _model_label(m).lower())]
        if exact:
            chosen = exact[0]
        else:
            matches = [m for m in models if substr in m.lower() or substr in _model_label(m).lower()]
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
        substr = " ".join(args[1:]).strip().lower()  # «Fable 5.1» — с пробелом
        exact = [m for m in models if substr in (m.lower(), _model_label(m).lower())]
        if len(exact) == 1:
            chosen_model = exact[0]
        else:
            matches = [m for m in models if substr in m.lower() or substr in _model_label(m).lower()]
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
                f"Текущая модель: {_model_label(current_model) if current_model else '(дефолт движка)'}.\n"
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
