"""Канал уведомлений QueueWarden «Бот»: long-poll → внешний триггер Тимлиду.

Бот заходит токеном учётки-моста (``QUEUEWARDEN_MCP_TOKEN``) и сам забирает
свои уведомления: ``GET /api/bot/notifications?wait=25`` держит запрос, пока
нечего отдать. Каждое уведомление становится отдельным ``agent_triggers``
(source='queuewarden') в топике Тимлида; агент разбирает его через MCP
``queuewarden`` и присылает оператору короткое резюме.

Доставка at-least-once: неподтверждённое приходит снова. Поэтому ack уходит
только после коммита триггера, а повторы гасятся отметкой по ``notificationId``
в ``integration_seen_items`` (пишется в той же транзакции, что и триггер).
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

import httpx

from bot.queues import enqueue_agent_trigger
from bot.topics import TopicKey, _resolve_topic_from_env, resolve_teamlead_topic

logger = logging.getLogger(__name__)

DEFAULT_URL = "https://queuewarden.ru"
SOURCE = "queuewarden"
SEEN_INTEGRATION = "queuewarden"
SEEN_KIND = "bot_notification"
# Тимлид разбирает уведомление и может спросить оператора — роль не
# 'executor', иначе гард ask_user закроет ему чат.
TRIGGER_ROLE = "manager"

POLL_WAIT_SECONDS = 25
POLL_LIMIT = 50
# HTTP-таймаут с запасом над wait: сервер держит запрос до 25 с.
HTTP_TIMEOUT_SECONDS = 35.0
BACKOFF_MIN_SECONDS = 5.0
BACKOFF_MAX_SECONDS = 60.0
# Маршрута нет (404) — канал на стороне QW ещё не выложен: ждём, не шумим.
NOT_DEPLOYED_SLEEP_SECONDS = 60.0
QUIET_LOG_INTERVAL_SECONDS = 600.0

_OFF_VALUES = {"0", "off", "false", "no", "none"}


def _enabled() -> bool:
    raw = (os.environ.get("JARVIS_QW_NOTIFICATIONS") or "1").strip().lower()
    return raw not in _OFF_VALUES


def resolve_notice_topic() -> TopicKey | None:
    """Топик для уведомлений: env JARVIS_QW_NOTICE_* или Тимлид (фолбэк — Секретарь)."""
    return _resolve_topic_from_env("QW_NOTICE") or resolve_teamlead_topic()


def parse_notifications(payload: Any) -> list[dict]:
    """Достать уведомления из ответа GET. Битые элементы (без id или
    notificationId) отбрасываются: ни поставить, ни подтвердить их нельзя."""
    if not isinstance(payload, dict):
        raise ValueError(f"unexpected payload type: {type(payload).__name__}")
    items = payload.get("items")
    if not isinstance(items, list):
        raise ValueError("payload has no 'items' list")
    result = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("id") in (None, "") or item.get("notificationId") in (None, ""):
            logger.warning("queuewarden: skipping malformed notification %r", item)
            continue
        result.append(item)
    return result


def build_trigger_text(item: dict) -> str:
    """Инструкция агенту по одному уведомлению."""
    lines = [
        "Пришло уведомление QueueWarden (канал «Бот»).",
        f"Тип: {item.get('type') or '—'}",
        f"Заголовок: {item.get('title') or '—'}",
    ]
    body = (item.get("body") or "").strip()
    if body:
        lines.append(f"Текст: {body}")
    if item.get("url"):
        lines.append(f"Ссылка: {item['url']}")
    lines.append(
        f"taskId: {item.get('taskId') or '—'}, projectId: {item.get('projectId') or '—'}"
    )
    lines.append("")
    lines.append(
        "Разбери уведомление через MCP-сервер queuewarden (queuewarden_task_get "
        "и другие инструменты), при необходимости действуй по правилам проекта. "
        "Затем пришли оператору КОРОТКОЕ резюме: что произошло и нужно ли его участие."
    )
    return "\n".join(lines)


def enqueue_notification(item: dict, topic: TopicKey) -> int | None:
    """Поставить триггер по уведомлению. Возвращает id триггера или None у
    повторной доставки. Вернулся без исключения — уведомление можно
    подтверждать: триггер записан сейчас или был записан раньше."""
    chat_id, thread_id = topic
    trigger_id = enqueue_agent_trigger(
        chat_id, thread_id, build_trigger_text(item), SOURCE,
        role=TRIGGER_ROLE,
        seen_key=(SEEN_INTEGRATION, SEEN_KIND, item["notificationId"]),
    )
    if trigger_id is None:
        logger.info("queuewarden: duplicate notification %s — ack only",
                    item["notificationId"])
    else:
        logger.info("queuewarden: notification %s → agent_trigger #%d",
                    item["notificationId"], trigger_id)
    return trigger_id


class _QuietLog:
    """Повторяющееся состояние (канал не выложен, токен отвергнут) пишем в лог
    не чаще раза в QUIET_LOG_INTERVAL_SECONDS, чтобы не забивать журнал."""

    def __init__(self) -> None:
        self._last: dict[str, float] = {}

    def __call__(self, key: str, level: int, msg: str, *args: Any) -> None:
        now = time.monotonic()
        last = self._last.get(key)
        if last is not None and now - last < QUIET_LOG_INTERVAL_SECONDS:
            return
        self._last[key] = now
        logger.log(level, msg, *args)


async def poll_once(
    client: httpx.AsyncClient, base: str, topic: TopicKey, quiet: _QuietLog,
) -> float | None:
    """Один цикл GET → триггеры → ack.

    Возвращает паузу перед следующим циклом: 0 — сразу снова, число — ждать
    столько, None — ошибка, пауза по backoff. Сетевые исключения летят наверх.
    """
    resp = await client.get(
        f"{base}/api/bot/notifications",
        params={"wait": POLL_WAIT_SECONDS, "limit": POLL_LIMIT},
    )
    if resp.status_code == 404:
        quiet("404", logging.INFO,
              "queuewarden: /api/bot/notifications → 404, канал ещё не выложен; жду")
        return NOT_DEPLOYED_SLEEP_SECONDS
    if resp.status_code in (401, 403):
        quiet("auth", logging.ERROR,
              "queuewarden: токен отвергнут (%d): %s", resp.status_code, resp.text[:200])
        return NOT_DEPLOYED_SLEEP_SECONDS
    if resp.status_code != 200:
        logger.warning("queuewarden: GET notifications → HTTP %d", resp.status_code)
        return None
    items = parse_notifications(resp.json())
    if not items:
        return 0.0

    ack_ids = []
    for item in items:
        try:
            enqueue_notification(item, topic)
            ack_ids.append(item["id"])
        except Exception:
            # Не подтверждаем — QW доставит повторно.
            logger.exception("queuewarden: failed to enqueue notification %s",
                             item.get("notificationId"))
    if not ack_ids:
        return None
    ack = await client.post(f"{base}/api/bot/notifications/ack", json={"ids": ack_ids})
    if ack.status_code != 200:
        # Триггеры уже записаны — повторная доставка погасится дедупом.
        logger.warning("queuewarden: ack → HTTP %d", ack.status_code)
        return None
    return 0.0


def _make_client(token: str) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers={"Authorization": f"Bearer {token}"},
        timeout=HTTP_TIMEOUT_SECONDS,
    )


async def queuewarden_notifications_worker(app: Any) -> None:
    """Фоновая задача бота: забирает уведомления QueueWarden и ставит триггеры.

    Никогда не падает (кроме отмены): сеть/5xx — backoff 5→60 с.
    """
    if not _enabled():
        logger.info("queuewarden_notifications_worker: disabled (JARVIS_QW_NOTIFICATIONS=%s)",
                    os.environ.get("JARVIS_QW_NOTIFICATIONS"))
        return
    token = (os.environ.get("QUEUEWARDEN_MCP_TOKEN") or "").strip()
    if not token:
        logger.info("queuewarden_notifications_worker: QUEUEWARDEN_MCP_TOKEN not set — off")
        return
    topic = resolve_notice_topic()
    if topic is None:
        logger.warning("queuewarden_notifications_worker: no Teamlead/Secretary topic "
                       "(JARVIS_TEAMLEAD_*/JARVIS_SECRETARY_*/JARVIS_QW_NOTICE_*) — off")
        return
    base = (os.environ.get("QUEUEWARDEN_URL") or DEFAULT_URL).strip().rstrip("/")
    logger.info("queuewarden_notifications_worker started (%s → topic %s)", base, topic)

    quiet = _QuietLog()
    backoff = BACKOFF_MIN_SECONDS
    async with _make_client(token) as client:
        while True:
            try:
                delay = await poll_once(client, base, topic, quiet)
            except asyncio.CancelledError:
                logger.info("queuewarden_notifications_worker cancelled")
                raise
            except Exception as exc:
                logger.warning("queuewarden: poll failed: %s: %s",
                               type(exc).__name__, exc)
                delay = None
            if delay is None:
                delay = backoff
                backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)
            else:
                backoff = BACKOFF_MIN_SECONDS
            if delay > 0:
                await asyncio.sleep(delay)
