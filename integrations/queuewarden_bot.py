"""Канал уведомлений QueueWarden «Бот»: long-poll → внешний триггер Тимлиду.

Бот заходит токеном учётки-моста и сам забирает свои уведомления:
``GET /api/bot/notifications?wait=25`` держит запрос, пока нечего отдать.
Каждое уведомление становится отдельным ``agent_triggers``
(source='queuewarden') в топике Тимлида. Воркер триггеров склеивает серию
уведомлений в один ход (``JARVIS_QW_COALESCE_SECONDS``); Тимлид-советник
сверяет её с задачей через MCP установки и пишет оператору только по делу,
иначе отвечает ``[[SILENT]]`` и в топик ничего не уходит.

Установок QW может быть несколько (``QUEUEWARDEN_INSTALLATIONS=artsites,tako``,
у каждой ``QUEUEWARDEN_<SLUG>_URL`` и ``QUEUEWARDEN_<SLUG>_TOKEN``) — каждая
опрашивается своей задачей, недоступность одной не мешает остальным. Агент
разбирает уведомление через MCP ``queuewarden_<slug>`` своей установки. Без
списка — одна установка из ``QUEUEWARDEN_URL``/``QUEUEWARDEN_MCP_TOKEN`` и MCP
``queuewarden``, как было до мультиустановки.

Новая задача (``task.created``) перед постановкой обогащается из MCP
установки: вложения скачиваются в ``temp/media/qw/<slug>/<номер>/`` и попадают
в триггер готовыми маркерами ``[[FILE:]]`` (Jarvis встроит их в ответ
слайдером), а роль оператора вычисляется по участникам: агент в слоте — его
владелец. Сбой обогащения не мешает триггеру.

Доставка at-least-once: неподтверждённое приходит снова. Поэтому ack уходит
только после коммита триггера, а повторы гасятся отметкой по ``notificationId``
в ``integration_seen_items`` (пишется в той же транзакции, что и триггер; kind
включает slug установки — id разных установок пересекаются).
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
import shutil
import time
from dataclasses import dataclass
from typing import Any

import httpx

from bot.delivery import SILENT_MARKER
from bot.queues import enqueue_agent_trigger
from bot.settings import MEDIA_DIR
from bot.topics import TopicKey, _resolve_topic_from_env, resolve_teamlead_topic

logger = logging.getLogger(__name__)

DEFAULT_URL = "https://stage.queuewarden.ru"
SOURCE = "queuewarden"
SEEN_INTEGRATION = "queuewarden"
SEEN_KIND = "bot_notification"
LEGACY_SLUG = "default"
LEGACY_MCP = "queuewarden"
_SLUG_RE = re.compile(r"[a-z0-9_-]+")
# Номер задачи QW (task_key вида 2609-2) — в payload его нет, он в заголовке.
_TASK_KEY_RE = re.compile(r"\b(\d{4}-\d+)\b")
# Тимлид разбирает уведомление и может спросить оператора — роль не
# 'executor', иначе гард ask_user закроет ему чат.
TRIGGER_ROLE = "manager"

ATTACH_DIR = os.path.join(MEDIA_DIR, "qw")
# MCP artifact_get отдаёт файл base64 не больше 5 МиБ (MCP_ARTIFACT_LIMIT в QW).
ARTIFACT_LIMIT_BYTES = 5 * 1024 * 1024
MAX_ATTACHMENTS = 20
# Слот задачи: поле, роль для department_users (оператор — всегда человек),
# роль оператора, если в слоте он сам, и если его агент (🤖 — роль через агента).
_ROLE_SLOTS = (
    ("reviewer_id", "reviewer", "ревизор", "ревизор (🤖)"),
    ("assignee_id", "assignee", "исполнитель", "исполнитель (🤖)"),
    ("operator_id", None, "оператор", "оператор (🤖)"),
)

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


def task_hashtag(item: dict, inst: Installation) -> str:
    """Хэштег задачи для поиска в Telegram: #qwtako2609_2. Дефис хэштег рвёт,
    поэтому номер — через подчёркивание. Номера в заголовке нет — пусто."""
    match = _TASK_KEY_RE.search(item.get("title") or "")
    if not match:
        return ""
    slug = "" if inst.slug == LEGACY_SLUG else inst.slug.replace("-", "")
    return f"#qw{slug}{match.group(1).replace('-', '_')}"


def operator_roles(task: dict, people: dict[str, dict]) -> list[str]:
    """Роли оператора в задаче. QW шлёт task.created участникам, кроме
    создателя, — значит, оператор занимает слот, не принадлежащий создателю:
    сам (человек в слоте) или через своего агента (владелец агента)."""
    creator = task.get("created_by")
    roles = []
    for field, _role, human, owner in _ROLE_SLOTS:
        uid = task.get(field)
        if not uid or uid == creator:
            continue
        person = people.get(uid)
        if person and person.get("kind") == "agent":
            if person.get("ownerId") != creator:
                roles.append(owner)
        else:
            roles.append(human)
    return roles


async def _mcp_call(client: httpx.AsyncClient, inst: Installation, tool: str,
                    args: dict) -> Any:
    """Вызов инструмента MCP установки тем же токеном моста."""
    resp = await client.post(
        f"{inst.url}/mcp",
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/call",
              "params": {"name": tool, "arguments": args}},
        headers={"Accept": "application/json, text/event-stream"},
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(f"{tool}: {data['error'].get('message')}")
    result = data.get("result") or {}
    if result.get("isError"):
        raise RuntimeError(f"{tool}: {result.get('content')}")
    if result.get("structuredContent") is not None:
        return result["structuredContent"]
    return json.loads(result["content"][0]["text"])


def _safe_name(name: str | None) -> str:
    name = os.path.basename(name or "").strip() or "file"
    return re.sub(r"[^\w.\- ]", "_", name)[:120]


async def enrich_created(client: httpx.AsyncClient, inst: Installation,
                         item: dict) -> dict:
    """Для task.created: роль оператора и скачанные вложения.
    Возвращает {"roles": [...], "files": [путь, ...], "skipped": [имя, ...]}."""
    detail = await _mcp_call(client, inst, "queuewarden_task_get",
                             {"taskId": item["taskId"]})
    task = detail.get("task") or {}
    people: dict[str, dict] = {}
    for field, role, _human, _owner in _ROLE_SLOTS:
        if role and task.get(field) and task.get(field) != task.get("created_by"):
            found = await _mcp_call(client, inst, "queuewarden_department_users",
                                    {"projectId": task["project_id"], "role": role})
            people.update({row["id"]: row for row in found.get("rows") or []})

    folder = os.path.join(ATTACH_DIR, inst.slug,
                          _safe_name(task.get("task_key") or str(item["taskId"])))
    attachments = [a for a in detail.get("artifacts") or []
                   if a.get("kind") == "attachment" and not a.get("run_id")]
    files: list[str] = []
    skipped = [_safe_name(a.get("filename")) for a in attachments[MAX_ATTACHMENTS:]]
    for art in attachments[:MAX_ATTACHMENTS]:
        name = _safe_name(art.get("filename"))
        if int(art.get("size_bytes") or 0) > ARTIFACT_LIMIT_BYTES:
            skipped.append(name)
            continue
        got = await _mcp_call(client, inst, "queuewarden_artifact_get",
                              {"artifactId": art["id"]})
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, f"{str(art['id'])[:8]}-{name}")
        with open(path, "wb") as fh:
            fh.write(base64.b64decode(got.get("content") or ""))
        files.append(path)
    return {"roles": operator_roles(task, people), "files": files, "skipped": skipped}


def prune_attachments(ttl_days: int) -> int:
    """Удалить папки вложений задач старше ttl_days. Возвращает их число."""
    if not os.path.isdir(ATTACH_DIR):
        return 0
    cutoff = time.time() - ttl_days * 86400
    removed = 0
    for slug in os.listdir(ATTACH_DIR):
        base = os.path.join(ATTACH_DIR, slug)
        if not os.path.isdir(base):
            continue
        for key in os.listdir(base):
            path = os.path.join(base, key)
            if os.path.isdir(path) and os.path.getmtime(path) < cutoff:
                shutil.rmtree(path, ignore_errors=True)
                removed += 1
    return removed


def build_trigger_text(item: dict, inst: Installation,
                       extra: dict | None = None) -> str:
    """Одно уведомление — текст строки триггера. Инструкцию агенту к серии
    таких строк добавляет build_batch_prompt при запуске хода. `extra` —
    результат enrich_created."""
    lines = [
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
    tag = task_hashtag(item, inst)
    if tag:
        lines.append(f"Тег: {tag}")
    if extra:
        if extra.get("roles"):
            lines.append("Роль оператора: " + " / ".join(extra["roles"]))
        if extra.get("files"):
            lines.append("Вложения (скопируй маркеры в ответ как есть, каждый "
                         "на своей строке):")
            lines += [f"[[FILE: {path}]]" for path in extra["files"]]
        if extra.get("skipped"):
            lines.append("Не скачаны (больше 5 МиБ или сверх лимита), есть только "
                         "в задаче: " + ", ".join(extra["skipped"]))
    return "\n".join(lines)


def build_batch_prompt(events: list[str]) -> str:
    """Ход Тимлида-советника по серии уведомлений (см. coalesce в
    claim_next_agent_trigger). Правила докладов — teamlead/AGENTS.md."""
    parts = [
        f"Пришли уведомления QueueWarden (канал «Бот»), {len(events)} шт., "
        "от старых к новым:",
    ]
    parts += [f"--- {n} ---\n{text}" for n, text in enumerate(events, 1)]
    parts.append(
        "Ты — технический советник оператора по задачам QueueWarden; правила — "
        "раздел «QueueWarden» в teamlead/AGENTS.md. В QueueWarden сам ничего не "
        "меняй: не подтверждай gate, не двигай, не возвращай и не перезапускай задачи.\n"
        "Сверь уведомления с актуальным состоянием задачи через MCP-сервер её "
        "установки (queuewarden_task_get, комментарии; сервер другой установки "
        "задачу не найдёт). Уже устаревшие события не пересказывай.\n"
        "Пиши оператору только в этих случаях:\n"
        "1) задача не пошла в работу из бэклога — почему ревизор отклонил постановку;\n"
        "2) проблема или вопрос, которые не разрешились сами;\n"
        "3) план — суть, риски, твоё мнение;\n"
        "4) перевод в «Готово» — что сделано, куда выложено, как проверено, твоё мнение;\n"
        "5) human gate — спроси оператора через ask_user (вместе с докладом 3 или 4, "
        "если gate на плане или на финальной проверке) и исполни его решение;\n"
        "6) новая задача (task.created) — докладывай ВСЕГДА: QW шлёт его в этот канал, "
        "только если оператор участник задачи, а не создатель, — участие уже проверено, "
        "сам не перепроверяй и не молчи из-за того, что исполнитель и ревизор — агенты. "
        "Роль оператора — из строки «Роль оператора:» уведомления, сам не выводи. "
        "Оформи карточкой:\n"
        "  маркеры [[FILE: …]] из уведомления — в самом начале, каждый на своей строке;\n"
        "  `## <название задачи>`;\n"
        "  список `- **Проект:** <название проекта>`, `- **Тип:** <тип задачи>`, "
        "`- **Моя роль:** <роль оператора как есть>`, `- **Дедлайн:** <дата>` "
        "(нет значения — пункт пропусти);\n"
        "  по каждому непустому полю задачи — `### <название поля>` и его значение;\n"
        "  `### Мнение Тимлида` — твоё мнение о постановке;\n"
        "  в конце две отдельные строки: `[Открыть задачу](<ссылка>)`, затем тег.\n"
        "Остальные поводы — блоком на задачу; первая строка блока: "
        "`**[<установка>] <номер задачи> · <проект> · <название>** <тег>` (тег из "
        "строки «Тег:» уведомления) и ссылка на задачу; тот же заголовок с тегом — в начале "
        "вопроса ask_user. "
        f"Если писать не о чем — ответь ровно {SILENT_MARKER}"
    )
    return "\n\n".join(parts)


def enqueue_notification(item: dict, topic: TopicKey, inst: Installation,
                         extra: dict | None = None) -> int | None:
    """Поставить триггер по уведомлению. Возвращает id триггера или None у
    повторной доставки. Вернулся без исключения — уведомление можно
    подтверждать: триггер записан сейчас или был записан раньше."""
    chat_id, thread_id = topic
    trigger_id = enqueue_agent_trigger(
        chat_id, thread_id, build_trigger_text(item, inst, extra), SOURCE,
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
        extra = None
        if item.get("type") == "task.created" and item.get("taskId"):
            try:
                extra = await enrich_created(client, inst, item)
            except Exception as exc:
                logger.warning("queuewarden[%s]: enrich %s failed: %s: %s", inst.slug,
                               item.get("notificationId"), type(exc).__name__, exc)
        try:
            enqueue_notification(item, topic, inst, extra)
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
