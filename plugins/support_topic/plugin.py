"""Топик отдельного бота поддержки: сообщения в нём не запускают Jarvis.

``JARVIS_SUPPORT_CHAT_ID`` и ``JARVIS_SUPPORT_THREAD_ID`` — чат и топик; без
обоих плагин ничего не регистрирует."""

from __future__ import annotations

from telegram import Update
from telegram.ext import Application, ApplicationHandlerStop, TypeHandler

from bot.plugins import Plugin
from bot.settings import int_env

SUPPORT_CHAT_ID = int_env("JARVIS_SUPPORT_CHAT_ID", 0)
SUPPORT_THREAD_ID = int_env("JARVIS_SUPPORT_THREAD_ID", 0)


async def skip_support_topic(update: Update, context) -> None:
    message = update.effective_message
    if (message is not None and message.chat_id == SUPPORT_CHAT_ID
            and message.message_thread_id == SUPPORT_THREAD_ID):
        raise ApplicationHandlerStop


def setup(app: Application) -> None:
    # Группа -2 — раньше журнала входящих (-1) и всех обработчиков.
    if SUPPORT_CHAT_ID and SUPPORT_THREAD_ID:
        app.add_handler(TypeHandler(Update, skip_support_topic), group=-2)


PLUGIN = Plugin(name="support_topic", setup=setup)
