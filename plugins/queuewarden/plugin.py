"""QueueWarden: уведомления досок → Тимлиду, вложения задач, команда /board."""

from __future__ import annotations

import asyncio
import logging

from telegram.ext import Application

from bot.plugins import Command, Plugin, TriggerSource
from bot.queues import _log_ttl_days
from bot.workers import _env_int
from plugins.queuewarden.board import cmd_board
from plugins.queuewarden.notifications import (
    prune_attachments,
    queuewarden_notifications_worker,
)
from plugins.queuewarden.round import build_round_prompt

logger = logging.getLogger(__name__)


async def attachments_cleanup_worker(app: Application) -> None:
    """Раз в час удаляет папки вложений задач старше JARVIS_LOG_TTL_DAYS."""
    ttl = _log_ttl_days()
    if ttl <= 0:
        return
    while True:
        try:
            dirs = prune_attachments(ttl)
            if dirs:
                logger.info("qw attachments: pruned %d dirs (TTL=%dd)", dirs, ttl)
        except Exception:
            logger.exception("qw attachments cleanup failed")
        await asyncio.sleep(3600.0)


PLUGIN = Plugin(
    name="queuewarden",
    workers=(queuewarden_notifications_worker, attachments_cleanup_worker),
    commands=(Command("board", cmd_board, "открыть доску QueueWarden (миниапп)"),),
    # Слот JARVIS_QW_COALESCE_SECONDS (15 минут) по часам: :00, :15, :30, :45.
    # К границе уходит всё, что пришло раньше. Пустой слот молчит. Пачка —
    # отдельным ходом основного диалога Тимлида: gate по одному, потом дайджест.
    trigger_sources={
        "queuewarden": TriggerSource(
            build_prompt=build_round_prompt,
            coalesce_seconds=_env_int("JARVIS_QW_COALESCE_SECONDS", 900, 0),
            allow_silent=False,
            exclusive=True,
        ),
    },
)
