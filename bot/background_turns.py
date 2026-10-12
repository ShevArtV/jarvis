"""Ходы, которые живой процесс начал сам, — без реплики пользователя.

Фоновый субагент дорабатывает после конца хода, CLI будит агента его
уведомлением, и тот отвечает новым ходом. Ответа никто не ждёт — без этого
модуля отчёт о работе субагента до топика не доходит."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from bot.delivery import ProgressJournal

logger = logging.getLogger(__name__)

# (chat, thread_id, journal, ok, final_text, engine_name, key) — _finish_turn_reply.
FinishReply = Callable[..., Awaitable[None]]


def attach_background_turns(
    worker, chat, thread_id: int, key: tuple[int, int],
    engine_name: str, timeout: int, finish: FinishReply,
) -> None:
    """Доставлять в топик ходы ``worker``, начатые без реплики пользователя."""
    if not hasattr(worker, "on_background_turn"):
        return

    async def _deliver(fut: asyncio.Future) -> None:
        journal = ProgressJournal(chat, thread_id)
        await journal.start()
        worker.on_intermediate = journal.append
        try:
            ok, final_text = await asyncio.wait_for(fut, timeout=timeout)
        except TimeoutError:
            logger.warning("background turn timeout key=%s", key)
            ok, final_text = False, ""
        finally:
            worker.on_intermediate = None
        await finish(chat, thread_id, journal, ok, final_text, engine_name, key)
        logger.info("background turn done: key=%s engine=%s ok=%s", key, engine_name, ok)

    def _on_turn(fut: asyncio.Future) -> None:
        logger.info("background turn started: key=%s engine=%s", key, engine_name)
        asyncio.create_task(_deliver(fut))

    worker.on_background_turn = _on_turn
