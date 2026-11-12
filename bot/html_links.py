"""Текст → HTML Telegram с markdown-ссылками ``[слово](https://…)`` в виде ``<a>``.

Без зависимостей бота: модуль импортирует и MCP-сервер (ask_user)."""

from __future__ import annotations

import re

_MD_LINK_RE = re.compile(r"\[([^\]\n]+)\]\((https://[^\s)]+)\)")


def _escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def escape_with_links(text: str) -> str:
    """Экранирует текст, а markdown-ссылки превращает в ``<a>`` — длинный URL
    прячется под слово. Ссылки не на https остаются текстом."""
    out, pos = [], 0
    for m in _MD_LINK_RE.finditer(text):
        out.append(_escape(text[pos:m.start()]))
        href = _escape(m.group(2)).replace('"', "&quot;")
        out.append(f'<a href="{href}">{_escape(m.group(1))}</a>')
        pos = m.end()
    out.append(_escape(text[pos:]))
    return "".join(out)
