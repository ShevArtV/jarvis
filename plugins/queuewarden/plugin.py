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
    build_batch_prompt,
    prune_attachments,
    queuewarden_notifications_worker,
)

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
    # Уведомления идут сериями: ждём затишья и отдаём серию Тимлиду одним
    # ходом, а не разбираем устаревшие события по одному. Советнику можно
    # промолчать, если оператору писать не о чем.
    trigger_sources={
        "queuewarden": TriggerSource(
            build_prompt=build_batch_prompt,
            coalesce_seconds=_env_int("JARVIS_QW_COALESCE_SECONDS", 60, 0),
            allow_silent=True,
        ),
    },
)
