"""Вопрос агента пользователю (``ask_user``) и приём ответа.

Агент работает неинтерактивно, перебить его нельзя — но он может сам спросить и
заблокироваться до ответа. Вопрос уходит в топик кнопками, ответ ловится либо
callback-ом кнопки, либо обычным текстовым сообщением; MCP-сторона (в
``scripts/jarvis_mcp_server.py``) в это время опрашивает таблицу.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timedelta

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

from bot.db import _db
from bot.formatting import _html_escape

logger = logging.getLogger(__name__)

GRACE_MINUTES = 15


def get_pending_ask(chat_id: int, thread_id: int) -> dict | None:
    """Незакрытый вопрос топика (последний, если их вдруг несколько)."""
    with _db() as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM ask_requests WHERE chat_id = ? AND thread_id = ? "
            "AND status = 'pending' ORDER BY id DESC LIMIT 1",
            (chat_id, thread_id),
        ).fetchone()
    return dict(row) if row is not None else None


def get_recent_timed_out_ask(
    chat_id: int, thread_id: int, within_minutes: int = GRACE_MINUTES,
) -> dict | None:
    """Вопрос, истёкший по таймауту не более within_minutes назад.

    Окно считается от МОМЕНТА ИСТЕЧЕНИЯ (``answered_at`` — его проставляет
    MCP-сервер, закрывая вопрос по таймауту), а не от создания: ждать ответа
    вопрос мог полчаса, и по ``created_at`` grace-окно не наступало бы вовсе.
    """
    with _db() as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM ask_requests WHERE chat_id = ? AND thread_id = ? "
            "AND status = 'timed_out' ORDER BY id DESC LIMIT 1",
            (chat_id, thread_id),
        ).fetchone()
    if row is None:
        return None
    ask = dict(row)
    try:
        expired_at = datetime.fromisoformat(ask["answered_at"] or ask["created_at"])
    except (TypeError, ValueError):
        return None
    if datetime.utcnow() - expired_at > timedelta(minutes=within_minutes):
        return None
    return ask


def answer_ask(
    ask_id: int, answer: str, via: str, option_index: int | None = None,
) -> bool:
    """Записать ответ. False, если вопрос уже закрыт (ответили дважды/таймаут)."""
    with _db() as conn:
        cur = conn.execute(
            "UPDATE ask_requests SET status = 'answered', answer = ?, "
            "option_index = ?, via = ?, answered_at = ? "
            "WHERE id = ? AND status = 'pending'",
            (answer, option_index, via, datetime.utcnow().isoformat(), ask_id),
        )
    return cur.rowcount == 1


def mark_ask_late_answered(ask_id: int, answer: str) -> bool:
    """Пометить истёкший вопрос как отвеченный задним числом."""
    with _db() as conn:
        cur = conn.execute(
            "UPDATE ask_requests SET status = 'answered_late', answer = ?, "
            "via = 'text_late', answered_at = ? WHERE id = ? AND status = 'timed_out'",
            (answer, datetime.utcnow().isoformat(), ask_id),
        )
    return cur.rowcount == 1


def ask_question_text(question: str, options: list[str] | None) -> str:
    """Как вопрос выглядит в топике. Используется и ботом, и MCP-сервером."""
    body = f"❓ {question}"
    if not options:
        body += "\n\n<i>Ответь сообщением в этот топик.</i>"
    else:
        body += "\n\n<i>Выбери вариант или ответь сообщением.</i>"
    return body


async def _mark_ask_answered(chat, ask: dict, answer: str) -> None:
    """Погасить кнопки у заданного вопроса и показать выбранный ответ."""
    tg_msg_id = ask.get("telegram_message_id")
    if not tg_msg_id:
        return
    body = (
        f"❓ {_html_escape(ask['question'])}\n\n"
        f"✅ <b>Ответ:</b> {_html_escape(answer)}"
    )
    try:
        await chat.get_bot().edit_message_text(
            chat_id=ask["chat_id"], message_id=tg_msg_id, text=body,
            parse_mode=ParseMode.HTML, reply_markup=None,
        )
    except Exception:
        # Сообщение могли удалить/изменить — ответ уже в БД, агент его получит.
        logger.warning("ask #%s: failed to update question message", ask["id"])


async def on_ask_answer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Нажата кнопка варианта. callback_data: ask:<ask_id>:<option_index>."""
    query = update.callback_query
    if query is None or not (query.data or "").startswith("ask:"):
        return
    parts = (query.data or "").split(":")
    if len(parts) != 3:
        return
    try:
        ask_id = int(parts[1])
        option_index = int(parts[2])
    except ValueError:
        return

    with _db() as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM ask_requests WHERE id = ?", (ask_id,)).fetchone()
    if row is None:
        await query.answer("Вопрос не найден", show_alert=False)
        return
    ask = dict(row)

    options = json.loads(ask["options_json"] or "[]")
    if not 0 <= option_index < len(options):
        await query.answer("Неизвестный вариант", show_alert=False)
        return
    answer = options[option_index]

    if not answer_ask(ask_id, answer, via="button", option_index=option_index):
        # Уже отвечено текстом или истёк таймаут.
        await query.answer("Вопрос уже закрыт", show_alert=False)
        return

    logger.info("ask #%s answered by button: %r", ask_id, answer)
    try:
        await query.answer(f"Принято: {answer}"[:200])
    except Exception:
        pass
    try:
        await query.edit_message_text(
            f"❓ {_html_escape(ask['question'])}\n\n"
            f"✅ <b>Ответ:</b> {_html_escape(answer)}",
            parse_mode=ParseMode.HTML, reply_markup=None,
        )
    except Exception:
        logger.warning("ask #%s: failed to edit question message", ask_id)
