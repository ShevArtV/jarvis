"""Rich Messages (Bot API 10.1+) → markdown-текст для агента.

PTB 22.x про ``Message.rich_message`` не знает: поле оседает в
``message.api_kwargs`` сырым dict'ом, а ``message.text`` пуст — поэтому
``filters.TEXT`` такие сообщения не ловил, и бот молчал. Здесь — перевод
блоков в markdown. Схема: https://core.telegram.org/bots/api#richmessage.
Когда PTB научится Bot API 10.x, модуль можно заменить штатным полем.
"""
from __future__ import annotations

from typing import Any

from telegram import Message
from telegram.ext import filters

# Инлайн-типы RichText → обёртка markdown. Остальные типы — просто текст.
_INLINE_WRAP = {
    "bold": "**",
    "italic": "_",
    "strikethrough": "~~",
    "code": "`",
}

# Медиа-блоки: агенту хватает пометки, что там было вложение.
_MEDIA_BLOCKS = {
    "photo": "фото",
    "video": "видео",
    "animation": "анимация",
    "audio": "аудио",
    "voice_note": "голосовое",
    "document": "файл",
    "collage": "коллаж",
    "slideshow": "слайдшоу",
    "map": "карта",
}


def get_rich_message(message: Message | None) -> dict | None:
    if message is None:
        return None
    rich = message.api_kwargs.get("rich_message")
    return rich if isinstance(rich, dict) else None


class _HasRichMessage(filters.MessageFilter):
    def filter(self, message: Message) -> bool:
        return get_rich_message(message) is not None


RICH_MESSAGE = _HasRichMessage(name="RICH_MESSAGE")


def rich_text(node: Any) -> str:
    """RichText: строка, массив или объект с ``type``; вложенность любая."""
    if node is None:
        return ""
    if isinstance(node, str):
        return node
    if isinstance(node, list):
        return "".join(rich_text(x) for x in node)
    if not isinstance(node, dict):
        return ""
    kind = node.get("type")
    inner = rich_text(node.get("text"))
    if kind in _INLINE_WRAP and inner:
        mark = _INLINE_WRAP[kind]
        return f"{mark}{inner}{mark}"
    if kind == "url" and node.get("url"):
        return f"[{inner}]({node['url']})" if inner else node["url"]
    if kind == "mathematical_expression":
        return f"${inner}$"
    if kind == "date_time" and not inner:
        return str(node.get("unix_time", ""))
    return inner


def _blocks(blocks: Any, indent: str = "") -> list[str]:
    out: list[str] = []
    for block in blocks if isinstance(blocks, list) else []:
        text = _block(block, indent)
        if text:
            out.append(text)
    return out


def _quote(text: str) -> str:
    return "\n".join(f"> {line}" if line else ">" for line in text.splitlines())


def _block(block: Any, indent: str = "") -> str:
    if not isinstance(block, dict):
        return ""
    kind = block.get("type")
    text = rich_text(block.get("text"))
    children = "\n\n".join(_blocks(block.get("blocks"), indent))

    if kind == "section_heading":
        return f"## {text}"
    if kind == "preformatted":
        return f"```{block.get('language') or ''}\n{text}\n```"
    if kind == "divider":
        return "---"
    if kind == "mathematical_expression":
        return f"$$\n{block.get('expression', '')}\n$$"
    if kind == "list":
        lines = []
        for i, item in enumerate(block.get("items") or [], start=1):
            if not isinstance(item, dict):
                continue
            # Живой Telegram (22.09.2026) шлёт не text, а label + blocks:
            # {"label": "1.", "blocks": [{"type": "paragraph", ...}]}.
            bullet = item.get("label") or (f"{i}." if block.get("ordered") else "-")
            head = rich_text(item.get("text"))
            children = _blocks(item.get("blocks"), indent + "  ")
            if not head and children and "\n" not in children[0]:
                head = children.pop(0)
            lines.append(f"{indent}{bullet} {head}".rstrip())
            lines.extend(children)
        return "\n".join(lines)
    if kind in ("block_quotation", "pull_quotation", "expandable_block_quotation"):
        return _quote("\n\n".join(x for x in (text, children) if x))
    if kind == "details":
        header = rich_text(block.get("header"))
        return "\n\n".join(x for x in (f"**{header}**" if header else "", children) if x)
    if kind == "table":
        rows = []
        for row in block.get("rows") or []:
            cells = [
                rich_text(c.get("text")) or " ".join(_blocks(c.get("blocks")))
                for c in (row.get("cells") or []) if isinstance(c, dict)
            ]
            rows.append("| " + " | ".join(cells) + " |")
        if rows:
            cols = rows[0].count("|") - 1
            rows.insert(1, "|" + " --- |" * cols)
        return "\n".join(rows)
    if kind in _MEDIA_BLOCKS:
        caption = rich_text((block.get("caption") or {}).get("text"))
        label = f"[{_MEDIA_BLOCKS[kind]}]"
        return f"{label} {caption}".strip()
    if kind in ("buttons", "anchor"):
        return ""
    # paragraph, footer, thinking и типы, которых ещё нет в схеме.
    return "\n\n".join(x for x in (text, children) if x)


def rich_message_to_markdown(rich: dict) -> str:
    return "\n\n".join(_blocks(rich.get("blocks"))).strip()


def rich_message_files(rich: dict) -> list[tuple[str, str]]:
    """(file_id, имя) фото и документов из блоков — чтобы агент увидел
    вложения, а не только пометку ``[фото]``. Фото — самый крупный размер."""
    found: list[tuple[str, str]] = []

    def walk(node: Any) -> None:
        if isinstance(node, list):
            for x in node:
                walk(x)
            return
        if not isinstance(node, dict):
            return
        kind = node.get("type")
        if kind == "photo" and isinstance(node.get("photo"), list) and node["photo"]:
            biggest = node["photo"][-1]
            if isinstance(biggest, dict) and biggest.get("file_id"):
                found.append((biggest["file_id"], "photo.jpg"))
        elif kind == "document" and isinstance(node.get("document"), dict):
            doc = node["document"]
            if doc.get("file_id"):
                found.append((doc["file_id"], doc.get("file_name") or "document"))
        for key in ("blocks", "items", "rows", "cells"):
            walk(node.get(key))

    walk(rich.get("blocks"))
    return found
