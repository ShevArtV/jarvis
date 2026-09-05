"""Простые команды топика: старт, статус, сброс, привязка каталога.

Здесь всё, что не требует своего диалога с кнопками: ответ формируется сразу.
Переключение движка и тумблеры вынесены отдельно — у них многошаговый UI.
"""

from __future__ import annotations

import logging

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

import asyncio
import os
from bot.formatting import _html_escape
from bot.sessions import _persistent_column_for_engine, _session_state_line, clear_cwd, close_session, get_actual_model, get_mcp_playwright, get_model, get_persistent_for_engine, get_session, reset_session, set_cwd, touch_session
from bot.settings import CLAUDE_CWD, DEFAULT_ENGINE_NAME, SESSION_IDLE_MINUTES
from bot.topics import _key, _kill_persistent_worker, active_procs, persistent_workers, spawn_procs
from engines import get_engine_by_name
from engines.limits import all_limits, format_limits_block
from engines.process_control import terminate_process_tree
from engines.session_usage import SessionUsage, inspect_session_usage











from bot.jobs import _run_spawn

logger = logging.getLogger(__name__)

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
        "/browser [on|off] — браузер (Playwright MCP) для топика, on-demand\n"
        "/persistent [on|off] — живой процесс claude/codex: сообщения на лету, без очереди\n"
        "/tokens — оценка размера текущей сессии\n"
        f"/close — закрыть сеанс (сам закроется после {SESSION_IDLE_MINUTES} мин простоя)\n"
        "/new, /reset — закрыть сеанс и сразу открыть новый\n"
        "/stop — прервать текущий запрос (сеанс сохраняется)\n"
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
    await _kill_persistent_worker(key, "сеанс сброшен через /new или /reset")
    new_id, cwd, engine_name = reset_session(*key)
    touch_session(*key)  # сеанс сразу открыт — /new это «закрыть и открыть»
    effective = cwd or CLAUDE_CWD
    logger.info("reset: key=%s engine=%s new_session=%s cwd=%s",
                key, engine_name, new_id, effective)
    await update.message.reply_text(
        f"🆕 Новый сеанс ({engine_name}), id: {new_id}\nCwd: {effective}"
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

    worker = persistent_workers.get(key)
    if worker is not None and worker.busy and not worker.dead:
        _sid, _, engine_name = get_session(*key)
        await _kill_persistent_worker(key, "прервано через /stop")
        await update.message.reply_text(
            f"⛔ Живой процесс {engine_name} прерван. Сессия сохранена — следующее "
            "сообщение поднимет новый живой процесс."
        )
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
    browser_state = "on" if get_mcp_playwright(*key) else "off"
    persistent_state = "on" if get_persistent_for_engine(*key, engine_name) else "off"
    persistent_desc = (
        f"живой процесс {engine_name}"
        if _persistent_column_for_engine(engine_name)
        else "не поддерживается для этого движка"
    )
    body = (
        f"engine     : {engine_name}\n"
        f"model      : {model_line}\n"
        f"session-id : {session_id}\n"
        f"cwd        : {effective_cwd}{cwd_suffix}\n"
        f"browser    : {browser_state}  (Playwright MCP, on-demand)\n"
        f"persistent : {persistent_state}  ({persistent_desc})\n"
        f"сеанс      : {_session_state_line(key)}"
    )
    return "<pre>" + _html_escape(body) + "</pre>"


async def cmd_session(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    key = _key(update)
    await update.message.reply_text(
        _topic_status_block(key), parse_mode=ParseMode.HTML,
    )


async def cmd_tokens(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    key = _key(update)
    session_id, cwd, engine_name = get_session(*key)
    usage = inspect_session_usage(engine_name, session_id, cwd or CLAUDE_CWD)
    body = (
        f"engine      : {engine_name}\n"
        f"session-id  : {session_id}\n"
        f"сеанс       : {_session_state_line(key)}\n"
        f"usage       : {_usage_line(usage)}"
    )
    if usage.path:
        body += f"\npath        : {usage.path}"
    await update.message.reply_text("<pre>" + _html_escape(body) + "</pre>", parse_mode=ParseMode.HTML)


async def cmd_usage(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    body = format_limits_block(all_limits())
    body += "\n\nКонтекст текущей сессии: /tokens"
    await update.message.reply_text("<pre>" + _html_escape(body) + "</pre>", parse_mode=ParseMode.HTML)


async def cmd_close(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Закрыть сеанс — как закрыть окно терминала. Топик (cwd, движок, модель)
    остаётся, контекст сессии — нет. Следующее сообщение откроет новый сеанс."""
    key = _key(update)

    proc = active_procs.get(key)
    if proc is not None:
        await terminate_process_tree(proc)
        active_procs.pop(key, None)
        logger.info("session close: killed active proc for key=%s", key)
    await _kill_persistent_worker(key, "сеанс закрыт через /close")

    was_open = close_session(*key)
    if not was_open:
        await update.message.reply_text(
            "Сеанс и так закрыт. Следующее сообщение откроет новый."
        )
        return

    _sid, cwd, engine_name = get_session(*key)
    logger.info("session closed: key=%s engine=%s", key, engine_name)
    await update.message.reply_text(
        "🚪 Сеанс закрыт. Контекст сброшен — следующее сообщение начнёт новый.\n"
        f"Топик сохранён: {engine_name}, {cwd or CLAUDE_CWD}"
    )


def _usage_line(usage: SessionUsage) -> str:
    token_value = usage.threshold_tokens
    token_part = "unknown" if token_value is None else f"{token_value:,}".replace(",", " ")
    suffix = " (estimate)" if usage.is_estimate else ""
    bits = [f"context: {token_part}{suffix}", f"source: {usage.source}"]
    if usage.cache_read_tokens is not None:
        bits.append(f"cache_read: {usage.cache_read_tokens:,}".replace(",", " "))
    if usage.cache_write_tokens is not None:
        bits.append(f"cache_write: {usage.cache_write_tokens:,}".replace(",", " "))
    if usage.output_tokens is not None:
        bits.append(f"last_output: {usage.output_tokens:,}".replace(",", " "))
    if usage.bytes_size is not None:
        bits.append(f"file: {usage.bytes_size / 1024 / 1024:.1f} MB")
    if usage.note:
        bits.append(f"note: {usage.note}")
    return "; ".join(bits)


def _inspect_topic_usage(key: tuple[int, int]) -> SessionUsage:
    session_id, cwd, engine_name = get_session(*key)
    return inspect_session_usage(engine_name, session_id, cwd or CLAUDE_CWD)


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
