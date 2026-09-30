"""Канал уведомлений QueueWarden «Бот»: long-poll → внешний триггер Тимлиду.

Бот заходит токеном учётки-моста и сам забирает свои уведомления:
``GET /api/bot/notifications?wait=25`` держит запрос, пока нечего отдать.
Каждое уведомление становится отдельным ``agent_triggers``
(source='queuewarden') в топике Тимлида; агент разбирает его через MCP
установки и присылает оператору короткое резюме.

Установок QW может быть несколько (``QUEUEWARDEN_INSTALLATIONS=artsites,tako``,
у каждой ``QUEUEWARDEN_<SLUG>_URL`` и ``QUEUEWARDEN_<SLUG>_TOKEN``) — каждая
опрашивается своей задачей, недоступность одной не мешает остальным. Агент
разбирает уведомление через MCP ``queuewarden_<slug>`` своей установки. Без
списка — одна установка из ``QUEUEWARDEN_URL``/``QUEUEWARDEN_MCP_TOKEN`` и MCP
``queuewarden``, как было до мультиустановки.

Доставка at-least-once: неподтверждённое приходит снова. Поэтому ack уходит
только после коммита триггера, а повторы гасятся отметкой по ``notificationId``
в ``integration_seen_items`` (пишется в той же транзакции, что и триггер; kind
включает slug установки — id разных установок пересекаются).
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any

import httpx

from bot.queues import enqueue_agent_trigger
from bot.topics import TopicKey, _resolve_topic_from_env, resolve_teamlead_topic

logger = logging.getLogger(__name__)

DEFAULT_URL = "https://stage.queuewarden.ru"
SOURCE = "queuewarden"
SEEN_INTEGRATION = "queuewarden"
SEEN_KIND = "bot_notification"
LEGACY_SLUG = "default"
LEGACY_MCP = "queuewarden"
_SLUG_RE = re.compile(r"[a-z0-9_-]+")
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


@dataclass(frozen=True)
class Installation:
    slug: str
    url: str
    token: str
    mcp: str


def load_installations() -> list[Installation]:
    """Установки из env. Установка без адреса/токена или с кривым slug
    пропускается с предупреждением — остальные работают."""
    raw = (os.environ.get("QUEUEWARDEN_INSTALLATIONS") or "").strip()
    if not raw:
        token = (os.environ.get("QUEUEWARDEN_MCP_TOKEN") or "").strip()
        if not token:
            return []
        url = (os.environ.get("QUEUEWARDEN_URL") or DEFAULT_URL).strip().rstrip("/")
        return [Installation(LEGACY_SLUG, url, token, LEGACY_MCP)]

    result: list[Installation] = []
    for slug in dict.fromkeys(s.strip().lower() for s in raw.split(",") if s.strip()):
        if not _SLUG_RE.fullmatch(slug):
            logger.warning("queuewarden: bad installation slug %r — skipped", slug)
            continue
        env = slug.upper().replace("-", "_")
        url = (os.environ.get(f"QUEUEWARDEN_{env}_URL") or "").strip().rstrip("/")
        token = (os.environ.get(f"QUEUEWARDEN_{env}_TOKEN") or "").strip()
        if not (url and token):
            logger.warning("queuewarden: installation %r has no QUEUEWARDEN_%s_URL/"
                           "_TOKEN — skipped", slug, env)
            continue
        result.append(Installation(slug, url, token, f"queuewarden_{slug}"))
    return result


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


def build_trigger_text(item: dict, inst: Installation) -> str:
    """Инструкция агенту по одному уведомлению."""
    lines = [
        "Пришло уведомление QueueWarden (канал «Бот»).",
        f"Установка: {inst.slug} ({inst.url}), MCP-сервер: {inst.mcp}",
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
        f"Разбери уведомление через MCP-сервер {inst.mcp} этой установки "
        "(queuewarden_task_get и другие инструменты; сервер другой установки "
        "задачу не найдёт), при необходимости действуй по правилам проекта. "
        "Затем пришли оператору КОРОТКОЕ резюме: что произошло и нужно ли его участие."
    )
    return "\n".join(lines)


def enqueue_notification(item: dict, topic: TopicKey, inst: Installation) -> int | None:
    """Поставить триггер по уведомлению. Возвращает id триггера или None у
    повторной доставки. Вернулся без исключения — уведомление можно
    подтверждать: триггер записан сейчас или был записан раньше."""
    chat_id, thread_id = topic
    trigger_id = enqueue_agent_trigger(
        chat_id, thread_id, build_trigger_text(item, inst), SOURCE,
        role=TRIGGER_ROLE,
        seen_key=(SEEN_INTEGRATION, f"{SEEN_KIND}:{inst.slug}", item["notificationId"]),
    )
    if trigger_id is None:
        logger.info("queuewarden[%s]: duplicate notification %s — ack only",
                    inst.slug, item["notificationId"])
    else:
        logger.info("queuewarden[%s]: notification %s → agent_trigger #%d",
                    inst.slug, item["notificationId"], trigger_id)
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
    client: httpx.AsyncClient, inst: Installation, topic: TopicKey, quiet: _QuietLog,
) -> float | None:
    """Один цикл GET → триггеры → ack.

    Возвращает паузу перед следующим циклом: 0 — сразу снова, число — ждать
    столько, None — ошибка, пауза по backoff. Сетевые исключения летят наверх.
    """
    resp = await client.get(
        f"{inst.url}/api/bot/notifications",
        params={"wait": POLL_WAIT_SECONDS, "limit": POLL_LIMIT},
    )
    if resp.status_code == 404:
        quiet("404", logging.INFO,
              "queuewarden[%s]: /api/bot/notifications → 404, канал ещё не выложен; жду",
              inst.slug)
        return NOT_DEPLOYED_SLEEP_SECONDS
    if resp.status_code in (401, 403):
        quiet("auth", logging.ERROR,
              "queuewarden[%s]: токен отвергнут (%d): %s",
              inst.slug, resp.status_code, resp.text[:200])
        return NOT_DEPLOYED_SLEEP_SECONDS
    if resp.status_code != 200:
        logger.warning("queuewarden[%s]: GET notifications → HTTP %d",
                       inst.slug, resp.status_code)
        return None
    items = parse_notifications(resp.json())
    if not items:
        return 0.0

    ack_ids = []
    for item in items:
        try:
            enqueue_notification(item, topic, inst)
            ack_ids.append(item["id"])
        except Exception:
            # Не подтверждаем — QW доставит повторно.
            logger.exception("queuewarden[%s]: failed to enqueue notification %s",
                             inst.slug, item.get("notificationId"))
    if not ack_ids:
        return None
    ack = await client.post(f"{inst.url}/api/bot/notifications/ack", json={"ids": ack_ids})
    if ack.status_code != 200:
        # Триггеры уже записаны — повторная доставка погасится дедупом.
        logger.warning("queuewarden[%s]: ack → HTTP %d", inst.slug, ack.status_code)
        return None
    return 0.0


def _make_client(token: str) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers={"Authorization": f"Bearer {token}"},
        timeout=HTTP_TIMEOUT_SECONDS,
    )


async def _poll_installation(inst: Installation, topic: TopicKey) -> None:
    """Бесконечный опрос одной установки. Сеть/5xx — backoff 5→60 с."""
    quiet = _QuietLog()
    backoff = BACKOFF_MIN_SECONDS
    async with _make_client(inst.token) as client:
        while True:
            try:
                delay = await poll_once(client, inst, topic, quiet)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("queuewarden[%s]: poll failed: %s: %s",
                               inst.slug, type(exc).__name__, exc)
                delay = None
            if delay is None:
                delay = backoff
                backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)
            else:
                backoff = BACKOFF_MIN_SECONDS
            if delay > 0:
                await asyncio.sleep(delay)


async def queuewarden_notifications_worker(app: Any) -> None:
    """Фоновая задача бота: забирает уведомления всех установок QueueWarden и
    ставит триггеры. Никогда не падает (кроме отмены)."""
    if not _enabled():
        logger.info("queuewarden_notifications_worker: disabled (JARVIS_QW_NOTIFICATIONS=%s)",
                    os.environ.get("JARVIS_QW_NOTIFICATIONS"))
        return
    installations = load_installations()
    if not installations:
        logger.info("queuewarden_notifications_worker: no installations "
                    "(QUEUEWARDEN_INSTALLATIONS or QUEUEWARDEN_MCP_TOKEN) — off")
        return
    topic = resolve_notice_topic()
    if topic is None:
        logger.warning("queuewarden_notifications_worker: no Teamlead/Secretary topic "
                       "(JARVIS_TEAMLEAD_*/JARVIS_SECRETARY_*/JARVIS_QW_NOTICE_*) — off")
        return
    logger.info("queuewarden_notifications_worker started (%s → topic %s)",
                ", ".join(f"{i.slug}={i.url}" for i in installations), topic)
    try:
        await asyncio.gather(*(_poll_installation(i, topic) for i in installations))
    except asyncio.CancelledError:
        logger.info("queuewarden_notifications_worker cancelled")
        raise
