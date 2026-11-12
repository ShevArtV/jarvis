"""Распознавание сетевых ошибок API в ответе CLI движка.

Сеть периодически подменяет TLS (самоподписанный сертификат), обрывает
соединения. CLI отдаёт это текстом «API Error: Unable to connect…» — такой
ход стоит перезапустить, а не отдавать оператору как ответ. Лимиты,
авторизация и ошибки модели сюда не относятся."""

from __future__ import annotations

import re

_NETWORK_RE = re.compile(
    r"Unable to connect to API|SELF_SIGNED|self[- ]signed certificate"
    r"|ECONNRESET|ECONNREFUSED|ETIMEDOUT|ENOTFOUND|EAI_AGAIN"
    r"|Connection error|fetch failed|socket hang up",
    re.IGNORECASE,
)


def is_network_api_error(text: str | None) -> bool:
    """Текст хода — сетевая ошибка API (а не ответ модели)."""
    head = (text or "").strip()[:500]
    return head.startswith("API Error") and bool(_NETWORK_RE.search(head))
