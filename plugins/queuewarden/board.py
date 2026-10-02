"""/board: кнопки миниаппов досок QueueWarden."""

from __future__ import annotations

import logging
import os

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, WebAppInfo
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

# Миниаппы досок QueueWarden: URL нужен для web_app-кнопки в личке (там разрешён
# только https-адрес), а короткое имя (direct link из BotFather) — для ссылки t.me
# в группах/форумах, где web_app-кнопки запрещены. Досок сколько угодно:
# BOARD_MINIAPPS=alpha|https://…/miniapp|qwalpha,beta|https://…/miniapp|qwbeta.
# Без списка — одна доска из BOARD_MINIAPP_URL/BOARD_MINIAPP_SHORT_NAME.
BOARD_MINIAPP_URL = os.environ.get("BOARD_MINIAPP_URL", "")
BOARD_MINIAPP_SHORT_NAME = os.environ.get("BOARD_MINIAPP_SHORT_NAME", "qwboard")


def parse_board_miniapps(raw: str | None) -> list[tuple[str, str, str]]:
    """``[(подпись, url, short_name)]`` из BOARD_MINIAPPS. Запись без https-адреса
    пропускается; short_name может быть пустым — тогда доска только в личке."""
    result: list[tuple[str, str, str]] = []
    for entry in (raw or "").split(","):
        parts = [p.strip() for p in entry.split("|")]
        if len(parts) < 2 or not parts[0]:
            continue
        label, url = parts[0], parts[1]
        short_name = parts[2] if len(parts) > 2 else ""
        if not url.startswith("https://"):
            logger.warning(
                "BOARD_MINIAPPS: %r has no https url — skipped", label,
            )
            continue
        result.append((label, url, short_name))
    return result


BOARD_MINIAPPS = parse_board_miniapps(os.environ.get("BOARD_MINIAPPS")) or (
    [("", BOARD_MINIAPP_URL, BOARD_MINIAPP_SHORT_NAME)] if BOARD_MINIAPP_URL else []
)


def board_keyboard(
    boards: list[tuple[str, str, str]], is_private: bool, bot_username: str,
) -> InlineKeyboardMarkup | None:
    """Кнопка на каждую доску. В личке — web_app-кнопка (миниапп прямо в
    Telegram); в группах/форумах web_app-кнопки запрещены Telegram — там ссылка
    на именованное приложение t.me/<bot>/<short_name>, доска без short_name
    не показывается. ``None`` — показать нечего."""
    rows = []
    for label, url, short_name in boards:
        text = f"Доска {label}" if label else "Открыть доску"
        if is_private:
            rows.append([InlineKeyboardButton(text, web_app=WebAppInfo(url=url))])
        elif short_name:
            rows.append([InlineKeyboardButton(
                text, url=f"https://t.me/{bot_username}/{short_name}",
            )])
    return InlineKeyboardMarkup(rows) if rows else None


async def cmd_board(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/board — открыть миниаппы досок QueueWarden (BOARD_MINIAPPS)."""
    if not update.message:
        return
    is_private = update.effective_chat is not None and update.effective_chat.type == "private"
    keyboard = board_keyboard(BOARD_MINIAPPS, is_private, context.bot.username)
    if keyboard is None:
        await update.message.reply_text(
            "Миниапп доски не настроен: задайте BOARD_MINIAPPS в .env "
            "(в группе нужен short_name доски)."
        )
        return
    await update.message.reply_text("Доски QueueWarden", reply_markup=keyboard)
