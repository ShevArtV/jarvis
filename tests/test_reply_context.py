"""Reply/quote prefix, который уходит агенту в meta_block."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from telegram import Chat, Message

from bot.llm import _build_reply_context_prefix, _clip_reply_text, reply_visible_text


def _msg(*, text=None, caption=None, api_kwargs=None) -> Message:
    return Message(
        message_id=1,
        date=datetime.now(timezone.utc),
        chat=Chat(id=1, type=Chat.SUPERGROUP),
        text=text,
        caption=caption,
        api_kwargs=api_kwargs or {},
    )


class ReplyVisibleTextTest(unittest.TestCase):
    def test_plain_text(self) -> None:
        self.assertEqual(reply_visible_text(_msg(text="привет")), "привет")

    def test_caption(self) -> None:
        self.assertEqual(reply_visible_text(_msg(caption="подпись")), "подпись")

    def test_rich_message_blocks(self) -> None:
        msg = _msg(api_kwargs={
            "rich_message": {"blocks": [
                {"type": "heading", "size": 2, "text": "Титул"},
                {"type": "paragraph", "text": "текст ответа"},
            ]},
        })
        self.assertEqual(reply_visible_text(msg), "## Титул\n\nтекст ответа")

    def test_no_text_at_all(self) -> None:
        self.assertIsNone(reply_visible_text(_msg()))


class ReplyContextPrefixTest(unittest.TestCase):
    def test_always_includes_message_id(self) -> None:
        text = _build_reply_context_prefix(message_id=58980)
        self.assertIn("message_id=58980", text)
        self.assertTrue(text.startswith("[Пользователь отвечает на:]"))

    def test_quote_and_replied_text(self) -> None:
        text = _build_reply_context_prefix(
            message_id=1,
            quote_text="актуализировал",
            replied_text="Ты актуализировал описание в репозитории?",
        )
        self.assertIn("цитата: актуализировал", text)
        self.assertIn("текст: Ты актуализировал описание в репозитории?", text)

    def test_bot_response_ctx_keeps_hint(self) -> None:
        text = _build_reply_context_prefix(
            message_id=42,
            ctx={"type": "claude_response", "_created_at": "2026-10-31T15:08:33"},
            quote_text="описание",
        )
        self.assertIn("message_id=42", text)
        self.assertIn("это твой предыдущий ответ", text)
        self.assertIn("цитата: описание", text)

    def test_quote_equals_text_not_duplicated(self) -> None:
        text = _build_reply_context_prefix(
            message_id=3,
            quote_text="коротко",
            replied_text="коротко",
        )
        self.assertIn("цитата: коротко", text)
        self.assertNotIn("текст:", text)

    def test_clip_long_text(self) -> None:
        long = "x" * 600
        clipped = _clip_reply_text(long, limit=50)
        self.assertEqual(len(clipped), 50)
        self.assertTrue(clipped.endswith("…"))


if __name__ == "__main__":
    unittest.main()
