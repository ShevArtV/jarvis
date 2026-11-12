"""Перезапуск живого процесса после сетевой ошибки API.

Ход оборвался «API Error: Unable to connect…» — убиваем процесс, ждём,
поднимаем заново (``--resume``) и просим продолжить ход. Исходное сообщение
уже в транскрипте, повторно его не шлём."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from engines.api_errors import is_network_api_error

logger = logging.getLogger(__name__)

RETRY_DELAYS = (30, 60, 120, 240)

CONTINUE_PROMPT = (
    "[SYSTEM: предыдущая попытка хода оборвалась сетевой ошибкой API, "
    "процесс перезапущен. Продолжи прерванный ход с места обрыва; уже "
    "сделанные шаги не повторяй.]"
)


async def retry_network_errors(
    chat, thread_id: int, key: tuple[int, int], ok: bool, text: str,
    notify: Callable[[str], Awaitable[None]] | None,
) -> tuple[bool, str]:
    """Повторять ход, пока он падает сетевой ошибкой API и есть попытки."""
    from bot.jobs import _run_job_turn_persistent
    from bot.topics import _kill_persistent_worker

    total = len(RETRY_DELAYS)
    for attempt, delay in enumerate(RETRY_DELAYS, 1):
        if ok or not is_network_api_error(text):
            break
        logger.warning(
            "network API error key=%s, restart %s/%s in %ss: %s",
            key, attempt, total, delay, text[:200],
        )
        if notify is not None:
            try:
                await notify(
                    f"⚠️ Сеть до API оборвалась — перезапускаю агента через "
                    f"{delay} с (попытка {attempt}/{total})."
                )
            except Exception:
                logger.exception("turn retry notify failed key=%s", key)
        await _kill_persistent_worker(key, "network API error")
        await asyncio.sleep(delay)
        ok, text = await _run_job_turn_persistent(
            chat, thread_id, key, CONTINUE_PROMPT, notify,
        )
    return ok, text
