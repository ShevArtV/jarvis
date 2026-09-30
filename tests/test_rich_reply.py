"""Ответ агента Rich Message'ем: картинки коллажем, откат на HTML.

Telegram мокается на уровне do_api_request / _send_claude_reply_legacy —
сеть и боевая bot_state.db не трогаются.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from bot import delivery


def _chat(api):
    chat = MagicMock()
    chat.id = -1001
    chat.get_bot.return_value.do_api_request = api
    return chat


class SplitImagesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _file(self, name: str) -> str:
        path = os.path.join(self.tmp.name, name)
        with open(path, "wb") as fh:
            fh.write(b"x")
        return path

    def test_images_split_from_other_files(self) -> None:
        png, pdf = self._file("a.PNG"), self._file("b.pdf")
        images, rest = delivery.split_image_markers(
            [(png, "cap"), (pdf, None), ("rel.png", None), ("/nope.jpg", None)])
        self.assertEqual(images, [(png, "cap")])
        self.assertEqual(rest, [(pdf, None), ("rel.png", None), ("/nope.jpg", None)])

    def test_image_block_single_and_collage(self) -> None:
        self.assertEqual(delivery._rich_image_block([("/a.png", None)]),
                         "![](tg://photo?id=img0)")
        block = delivery._rich_image_block([("/a.png", 'с "кавычкой"'), ("/b.png", None)])
        self.assertEqual(block.splitlines(), [
            "<tg-collage>",
            "![](tg://photo?id=img0 \"с 'кавычкой'\")",
            "![](tg://photo?id=img1)",
            "</tg-collage>",
        ])


class SendClaudeReplyTest(unittest.TestCase):
    def setUp(self) -> None:
        for name in ("note_topic_message", "save_message_context", "log_message"):
            p = patch.object(delivery, name)
            p.start()
            self.addCleanup(p.stop)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.png = os.path.join(self.tmp.name, "shot.png")
        with open(self.png, "wb") as fh:
            fh.write(b"\x89PNG")

    def test_rich_message_with_image(self) -> None:
        sent = MagicMock(message_id=42)
        api = AsyncMock(return_value=sent)
        legacy = AsyncMock()
        with patch.object(delivery, "_send_claude_reply_legacy", legacy):
            got = asyncio.run(delivery.send_claude_reply(
                _chat(api), 7, "## Итог\n\n| a | b |\n|--|--|\n| 1 | 2 |", {},
                html_prefix="<b>[#ab12]</b> ", images=[(self.png, "скрин")]))
        self.assertIs(got, sent)
        legacy.assert_not_awaited()
        method = api.await_args.args[0]
        kwargs = api.await_args.kwargs["api_kwargs"]
        self.assertEqual(method, "sendRichMessage")
        self.assertEqual(kwargs["message_thread_id"], 7)
        rich = kwargs["rich_message"]
        self.assertTrue(rich["markdown"].startswith("[#ab12] ## Итог"))
        self.assertIn('![](tg://photo?id=img0 "скрин")', rich["markdown"])
        file = kwargs["rich_media_files"][0]
        self.assertEqual(rich["media"], [
            {"id": "img0", "media": {"type": "photo", "media": file.attach_uri}}])
        self.assertEqual(file.input_file_content, b"\x89PNG")

    def test_rejected_rich_falls_back_to_html_and_files(self) -> None:
        api = AsyncMock(side_effect=RuntimeError("Bad Request: can't parse"))
        legacy = AsyncMock(return_value="legacy")
        files = AsyncMock()
        with patch.object(delivery, "_send_claude_reply_legacy", legacy), \
             patch.object(delivery, "deliver_file_markers", files):
            got = asyncio.run(delivery.send_claude_reply(
                _chat(api), 7, "текст", {}, images=[(self.png, None)]))
        self.assertEqual(got, "legacy")
        files.assert_awaited_once()
        self.assertEqual(files.await_args.args[2], [(self.png, None)])

    def test_too_long_skips_rich(self) -> None:
        api = AsyncMock()
        legacy = AsyncMock(return_value="legacy")
        with patch.object(delivery, "_send_claude_reply_legacy", legacy):
            asyncio.run(delivery.send_claude_reply(
                _chat(api), 7, "x" * (delivery.RICH_LIMIT + 1), {}))
        api.assert_not_awaited()
        legacy.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
