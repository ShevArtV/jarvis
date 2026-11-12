"""Ссылка на задачу QW в миниаппе Telegram.

QW открывает задачу по start_param ``t_<taskId>``: ссылка
``https://t.me/<бот>/<short_name доски>?startapp=t_<taskId>`` ведёт прямо в
миниапп, а не в браузер на /miniapp/. Миниаппы досок висят на боте Jarvis
(BOARD_MINIAPPS), имя бота воркер уведомлений берёт при старте."""

from __future__ import annotations

import re

from plugins.queuewarden.board import BOARD_MINIAPPS

_TASK_ID_RE = re.compile(r"^[0-9a-fA-F-]{8,64}$")

_bot_username = ""


def set_bot_username(username: str | None) -> None:
    global _bot_username
    _bot_username = (username or "").lstrip("@")


def _short_name(inst_url: str) -> str:
    base = inst_url.rstrip("/")
    for _label, url, short_name in BOARD_MINIAPPS:
        if url.startswith(base + "/") and short_name:
            return short_name
    return ""


def miniapp_task_url(url: str, task_id: str | None = None, inst_url: str = "") -> str | None:
    """Ссылка на задачу в миниаппе. Есть бот, short_name доски и id задачи —
    t.me-ссылка со startapp; иначе сайт ``https://host/tasks/<id>`` →
    ``https://host/miniapp/tasks/<id>``. Другой путь не переписываем:
    выдуманная ссылка хуже её отсутствия."""
    short = _short_name(inst_url) if inst_url else ""
    if _bot_username and short and task_id and _TASK_ID_RE.match(task_id):
        return f"https://t.me/{_bot_username}/{short}?startapp=t_{task_id}"
    if not url.startswith("https://") or "/miniapp/" in url:
        return None
    marker = "/tasks/"
    at = url.find(marker)
    if at < 0:
        return None
    return url[:at] + "/miniapp" + url[at:]
