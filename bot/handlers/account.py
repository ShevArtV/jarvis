"""Выбор аккаунта движка в ``/engine``: кнопки ``👤 <имя>`` и ``/engine @<имя>``.

Кнопки показываются, только если у движка топика настроено больше одного
аккаунта (engines/accounts.py). Смена аккаунта сохраняет сессию; живой процесс
топика гасится — он залогинен под прежним аккаунтом.
"""

from __future__ import annotations

import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest, TelegramError
from telegram.ext import ContextTypes

from bot.delivery import send_to_topic
from bot.sessions import get_session
from bot.topic_account import get_account, set_account
from bot.topics import _key, _kill_persistent_worker, _lock_for
from engines.accounts import account_names

logger = logging.getLogger(__name__)


def account_rows(key: tuple[int, int]) -> list[list[InlineKeyboardButton]]:
    """Ряд кнопок аккаунтов движка топика; пусто, если выбирать не из чего."""
    _, _, engine_name = get_session(*key)
    names = account_names(engine_name)
    if len(names) < 2:
        return []
    current = get_account(*key)
    return [[
        InlineKeyboardButton(
            f"👤 ✓ {name}" if name == current else f"👤 {name}",
            callback_data=f"account_select:{engine_name}:{name}",
        )
        for name in names
    ]]


def with_account_rows(
    key: tuple[int, int], markup: InlineKeyboardMarkup | None = None,
) -> InlineKeyboardMarkup | None:
    """Клавиатура /engine с рядом аккаунтов снизу (или только он)."""
    rows = [list(r) for r in markup.inline_keyboard] if markup else []
    rows += account_rows(key)
    return InlineKeyboardMarkup(rows) if rows else None


async def switch_account(key: tuple[int, int], engine_name: str, account: str) -> str:
    """Переключает аккаунт топика; возвращает текст ответа."""
    _, _, current_engine = get_session(*key)
    if engine_name != current_engine:
        return f"Топик уже на движке `{current_engine}`, аккаунт не сменён. Открой /engine заново."
    names = account_names(engine_name)
    if account not in names:
        return f"У движка `{engine_name}` нет аккаунта {account!r}. Доступны: {', '.join(names)}."
    current = get_account(*key)
    if account == current:
        return f"Топик уже на аккаунте {account} ({engine_name})."
    if _lock_for(key).locked():
        return "⚠️ Топик занят активным запросом. Дождись завершения или /stop, потом повтори."
    await _kill_persistent_worker(key, "аккаунт сменён через /engine")
    set_account(key[0], key[1], account)
    logger.info("account switched for key=%s engine=%s: %s -> %s",
                key, engine_name, current, account)
    return (
        f"👤 Аккаунт {engine_name}: {current} → {account}.\n"
        "Сессия сохранена, следующий ход пойдёт под новым аккаунтом."
    )


async def on_account_select(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Callback ``account_select:<engine>:<account>``."""
    query = update.callback_query
    if query is None:
        return
    parts = (query.data or "").split(":", 2)
    if len(parts) != 3:
        return
    _, engine_name, account = parts
    key = _key(update)
    text = await switch_account(key, engine_name, account)
    try:
        await query.answer()
    except TelegramError:
        logger.debug("failed to answer callback query", exc_info=True)
    try:
        await query.edit_message_text(text)
    except BadRequest:
        await send_to_topic(update.effective_chat, key[1], text)
