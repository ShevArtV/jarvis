"""Тумблеры топика: ``/browser``, ``/persistent`` и вопрос «задача завершена?».

Оба тумблера НЕ сбрасывают сеанс: флаг применяется со следующего сообщения,
контекст цел. Цена — однократная инвалидация prompt-cache на первом ходу после
переключения.
"""

from __future__ import annotations

import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

import hashlib
import re
from bot.delivery import send_to_topic
from bot.handlers.commands import _usage_line
from bot.sessions import _persistent_column_for_engine, close_session, get_mcp_playwright, get_persistent_for_engine, get_session, set_mcp_playwright, set_persistent_for_engine
from bot.settings import CLAUDE_CWD, CONTEXT_WARN_TOKENS, DONE_CONFIRM_ON_DONE
from bot.topics import _key, _kill_persistent_worker
from engines.session_usage import inspect_session_usage
from telegram.error import BadRequest











logger = logging.getLogger(__name__)

_DONE_RE = re.compile(
    r"\b("
    r"готово|итог|выполнено|закрыто|задеплоено|задеплоил|деплой\s+выполнен|"
    r"проверено|проверил|закоммитил|коммит|commit|deploy(?:ed|ment)?|"
    r"implemented|done|fixed"
    r")\b",
    re.IGNORECASE,
)
_WAIT_RE = re.compile(
    r"("
    r"жду|подтверди|подтвердите|можно(?:\s+[^?\n]{1,40})?\?|что\s+дальше\?|отправлять\?|"
    r"согласовать|согласуй|нужно\s+подтверждение|нужен\s+ответ|"
    r"уточни|уточните|нужно\s+уточнить|ожидаю|#ask_\d+|waiting|confirm|approve"
    r")",
    re.IGNORECASE,
)
_NOT_DONE_RE = re.compile(r"\b(не\s+готово|не\s+выполнено|не\s+закрыто|not\s+done)\b", re.IGNORECASE)

def _browser_keyboard(enabled: bool) -> InlineKeyboardMarkup:
    """Кнопка-тоггл браузера для /browser."""
    target = "off" if enabled else "on"
    label = "🚫 Выключить браузер" if enabled else "🌐 Включить браузер"
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(label, callback_data=f"browser_toggle:{target}")]]
    )


def _browser_precheck(enable: bool) -> tuple[bool, str]:
    """Можно ли включить браузер: npx есть и Playwright не выключен глобально."""
    if not enable:
        return True, ""
    from engines.playwright_mcp import playwright_command_args

    try:
        spec = playwright_command_args()
    except Exception as exc:
        return False, f"⚠️ Playwright недоступен: {exc}"
    if spec is None:
        return False, "⚠️ Playwright выключен глобально (JARVIS_PLAYWRIGHT_MCP=0)."
    return True, ""


async def _apply_browser(key: tuple[int, int], enable: bool) -> str:
    """Применить флаг браузера (с pre-check) и вернуть текст ответа."""
    ok, msg = _browser_precheck(enable)
    if not ok:
        return msg
    set_mcp_playwright(key[0], key[1], enable)
    logger.info("browser toggled for key=%s: %s", key, "on" if enable else "off")
    if enable:
        return (
            "🌐 Браузер включён для топика. Playwright MCP подключится со "
            "СЛЕДУЮЩЕГО сообщения (≈30 browser_* тулов в контексте). Контекст "
            "сессии сохраняется. Выключай через /browser off, когда закончишь "
            "— это экономит токены."
        )
    return (
        "🚫 Браузер выключен. Playwright больше не грузится в контекст этого "
        "топика (со следующего сообщения). Контекст сессии сохранён."
    )


async def cmd_browser(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/browser — статус + кнопка; /browser on|off — включить/выключить браузер
    (Playwright MCP) для текущего топика. On-demand: дефолт off."""
    key = _key(update)
    args = [a.strip().lower() for a in (context.args or [])]
    current = get_mcp_playwright(*key)

    if not args:
        state = "включён" if current else "выключен"
        await update.message.reply_text(
            f"Браузер (Playwright MCP) сейчас {state} для этого топика.\n\n"
            "On-demand: по умолчанию выключен, чтобы не держать ~30 browser_* "
            "тулов в каждом запросе. Включай только под браузерные задачи.",
            reply_markup=_browser_keyboard(current),
        )
        return

    arg = args[0]
    if arg in {"on", "вкл", "1", "true", "yes"}:
        enable = True
    elif arg in {"off", "выкл", "0", "false", "no"}:
        enable = False
    else:
        await update.message.reply_text("Использование: /browser [on|off]")
        return

    if enable == current:
        await update.message.reply_text(
            f"Браузер уже {'включён' if current else 'выключен'}."
        )
        return
    await update.message.reply_text(await _apply_browser(key, enable))


async def on_browser_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Колбэк кнопки browser_toggle:<on|off>."""
    query = update.callback_query
    data = query.data or ""
    if not data.startswith("browser_toggle:"):
        return
    try:
        await query.answer()
    except Exception:
        pass
    enable = data.split(":", 1)[1] == "on"
    key = _key(update)
    text = await _apply_browser(key, enable)
    try:
        await query.edit_message_text(
            text, reply_markup=_browser_keyboard(get_mcp_playwright(*key)),
        )
    except BadRequest:
        await send_to_topic(update.effective_chat, key[1], text)


def _persistent_keyboard(enabled: bool) -> InlineKeyboardMarkup:
    target = "off" if enabled else "on"
    label = "🚫 Выключить живой процесс" if enabled else "⚡ Включить живой процесс"
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(label, callback_data=f"persistent_toggle:{target}")]]
    )


async def _apply_persistent(key: tuple[int, int], enable: bool) -> str:
    _session_id, _cwd, engine_name = get_session(*key)
    if enable and _persistent_column_for_engine(engine_name) is None:
        return (
            f"⚠️ Живой процесс поддержан для claude, codex и opencode, а у "
            f"топика движок `{engine_name}`. Переключи `/engine` и включай после."
        )
    set_persistent_for_engine(key[0], key[1], engine_name, enable)
    logger.info(
        "persistent toggled for key=%s engine=%s: %s",
        key, engine_name, "on" if enable else "off",
    )
    if enable:
        if engine_name == "codex":
            transport = "codex app-server"
            append = "через turn/steer"
        elif engine_name == "opencode":
            transport = "opencode serve"
            append = "через prompt_async"
        else:
            transport = engine_name
            append = "через stdin stream-json"
        return (
            f"⚡ Живой процесс {engine_name} включён для топика. Со следующего "
            f"сообщения {transport} поднимается один раз на весь сеанс: то, что "
            "прилетит, пока он ещё работает над предыдущим, допишется ему "
            f"прямо во время работы ({append}), а не будет ждать своей очереди. "
            "Уже начатую команду это не остановит — только подхватится, как "
            "только он освободится от неё. Выключай через /persistent off, "
            "когда не нужно — простаивающий процесс просто занимает память."
        )
    await _kill_persistent_worker(key, "выключено через /persistent off")
    return "🚫 Живой процесс выключен. Дальше — как обычно, процесс на сообщение."


async def cmd_persistent(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/persistent — статус + кнопка; /persistent on|off — включить/выключить
    живой процесс claude/codex/opencode для топика (сообщения во время работы агента
    подхватываются на лету, а не ждут своей очереди)."""
    key = _key(update)
    args = [a.strip().lower() for a in (context.args or [])]
    _sid, _cwd, engine_name = get_session(*key)
    current = get_persistent_for_engine(*key, engine_name)

    if not args:
        state = "включён" if current else "выключен"
        if _persistent_column_for_engine(engine_name) is None:
            await update.message.reply_text(
                f"Живой процесс не поддержан для `{engine_name}`. "
                "Доступно для `claude`, `codex` и `opencode`."
            )
            return
        await update.message.reply_text(
            f"Живой процесс {engine_name} сейчас {state} для этого топика.\n\n"
            "Пока выключен (дефолт) — на каждое сообщение новый процесс, а "
            "то, что прилетает во время работы, ждёт своей очереди. Включи, "
            "если хочешь на лету дописывать задачу агенту, пока он работает.",
            reply_markup=_persistent_keyboard(current),
        )
        return

    arg = args[0]
    if arg in {"on", "вкл", "1", "true", "yes"}:
        enable = True
    elif arg in {"off", "выкл", "0", "false", "no"}:
        enable = False
    else:
        await update.message.reply_text("Использование: /persistent [on|off]")
        return

    if enable == current:
        await update.message.reply_text(
            f"Живой процесс уже {'включён' if current else 'выключен'}."
        )
        return
    await update.message.reply_text(await _apply_persistent(key, enable))


async def on_persistent_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Колбэк кнопки persistent_toggle:<on|off>."""
    query = update.callback_query
    data = query.data or ""
    if not data.startswith("persistent_toggle:"):
        return
    try:
        await query.answer()
    except Exception:
        pass
    enable = data.split(":", 1)[1] == "on"
    key = _key(update)
    text = await _apply_persistent(key, enable)
    try:
        await query.edit_message_text(
            text,
            reply_markup=_persistent_keyboard(
                get_persistent_for_engine(*key, get_session(*key)[2])
            ),
        )
    except BadRequest:
        await send_to_topic(update.effective_chat, key[1], text)


async def on_done_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Колбэк кнопки done_confirm:<session_token>:<yes|no>."""
    query = update.callback_query
    data = query.data or ""
    if not data.startswith("done_confirm:"):
        return
    try:
        _prefix, token, action = data.split(":", 2)
    except ValueError:
        try:
            await query.answer("Некорректная кнопка", show_alert=True)
        except Exception:
            pass
        return

    key = _key(update)
    session_id, cwd, engine_name = get_session(*key)
    if token != _session_confirm_token(session_id):
        text = "Эта кнопка относится к старой сессии. Текущую сессию не трогаю."
        try:
            await query.answer(text, show_alert=True)
        except Exception:
            pass
        try:
            await query.edit_message_text(text)
        except BadRequest:
            await send_to_topic(update.effective_chat, key[1], text)
        return

    if action != "yes":
        text = "Ок, продолжаем в текущей сессии."
        try:
            await query.answer("Продолжаем")
        except Exception:
            pass
        try:
            await query.edit_message_text(text)
        except BadRequest:
            await send_to_topic(update.effective_chat, key[1], text)
        return

    try:
        await query.answer("Закрываю сессию")
    except Exception:
        pass
    await _kill_persistent_worker(key, "сессия закрыта по подтверждению завершения задачи")
    was_open = close_session(key[0], key[1])
    if was_open:
        text = (
            "✅ Сессия закрыта после подтверждения завершения задачи. "
            "Следующее сообщение откроет новую."
        )
    else:
        text = "Сессия уже закрыта. Следующее сообщение откроет новую."
    logger.info(
        "done confirmation: key=%s engine=%s cwd=%s action=yes closed=%s",
        key, engine_name, cwd or CLAUDE_CWD, was_open,
    )
    try:
        await query.edit_message_text(text)
    except BadRequest:
        await send_to_topic(update.effective_chat, key[1], text)


def _looks_like_waiting_for_user(text: str) -> bool:
    return bool(_WAIT_RE.search(text or ""))


def _looks_like_task_done(text: str) -> bool:
    if _looks_like_waiting_for_user(text):
        return False
    if _NOT_DONE_RE.search(text or ""):
        return False
    return bool(_DONE_RE.search(text or ""))


def _session_confirm_token(session_id: str) -> str:
    return hashlib.sha256(session_id.encode("utf-8", errors="replace")).hexdigest()[:12]


def _done_confirm_keyboard(session_id: str) -> InlineKeyboardMarkup:
    token = _session_confirm_token(session_id)
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("Да, закрыть", callback_data=f"done_confirm:{token}:yes"),
        InlineKeyboardButton("Нет", callback_data=f"done_confirm:{token}:no"),
    ]])


async def _warn_large_context_if_needed(
    chat, thread_id: int, engine_name: str, session_id: str, cwd: str | None, key: tuple[int, int],
) -> None:
    if CONTEXT_WARN_TOKENS <= 0:
        return
    try:
        usage = inspect_session_usage(engine_name, session_id, cwd or CLAUDE_CWD)
    except Exception:
        logger.exception("session usage inspection failed: key=%s engine=%s session=%s",
                         key, engine_name, session_id)
        return
    tokens = usage.threshold_tokens
    if tokens is None or tokens < CONTEXT_WARN_TOKENS:
        return
    try:
        await send_to_topic(
            chat, thread_id,
            "⚠️ Большой контекст: "
            f"{_usage_line(usage)}. "
            "Если задача завершена, используй /new или /close, чтобы следующий ход не тянул старую историю.",
        )
    except Exception:
        logger.exception("failed to send context warning: key=%s", key)


async def _ask_done_confirmation_if_needed(
    chat, thread_id: int, ok: bool, final_text: str, session_id: str, key: tuple[int, int],
) -> None:
    if not (DONE_CONFIRM_ON_DONE and ok and _looks_like_task_done(final_text)):
        return
    try:
        await send_to_topic(
            chat, thread_id,
            "Задача завершена? Закрыть сессию, чтобы следующий ход начал новый контекст?",
            reply_markup=_done_confirm_keyboard(session_id),
        )
    except Exception:
        logger.exception("failed to send done confirmation: key=%s", key)
