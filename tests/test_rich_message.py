import unittest

from telegram import Message

from bot.rich_message import RICH_MESSAGE, get_rich_message, rich_message_files, rich_message_to_markdown


def _p(text):
    return {"type": "plain", "text": text}


RICH = {
    "type": "rich",
    "blocks": [
        {"type": "section_heading", "text": _p("Задача")},
        {"type": "paragraph", "text": [
            _p("Сделай "),
            {"type": "bold", "text": _p("срочно")},
            _p(", см. "),
            {"type": "url", "text": _p("доку"), "url": "https://x.y"},
        ]},
        {"type": "list", "ordered": True, "items": [
            {"text": _p("раз")},
            {"text": _p("два"), "blocks": [
                {"type": "list", "items": [{"text": {"type": "code", "text": _p("x=1")}}]},
            ]},
        ]},
        {"type": "block_quotation", "text": _p("цитата")},
        {"type": "preformatted", "language": "py", "text": _p("print(1)")},
        {"type": "photo", "photo": [], "caption": {"text": _p("скрин")}},
        {"type": "unknown_future_block", "text": _p("новое")},
    ],
}

EXPECTED = """## Задача

Сделай **срочно**, см. [доку](https://x.y)

1. раз
2. два
  - `x=1`

> цитата

```py
print(1)
```

[фото] скрин

новое"""


class RichMessageTest(unittest.TestCase):
    def test_markdown(self) -> None:
        self.assertEqual(rich_message_to_markdown(RICH), EXPECTED)

    def test_filter_reads_api_kwargs(self) -> None:
        data = {
            "message_id": 1, "date": 0,
            "chat": {"id": -100, "type": "supergroup"},
            "rich_message": RICH,
        }
        msg = Message.de_json(data, None)
        self.assertIsNone(msg.text)
        self.assertEqual(get_rich_message(msg), RICH)
        self.assertTrue(RICH_MESSAGE.filter(msg))

    def test_live_list_and_photo(self) -> None:
        # Сырой rich_message из лога бота, 22.09.2026 (file_id укорочены).
        live = {"blocks": [
            {"type": "paragraph", "text": "## Заголовок "},
            {"type": "photo", "photo": [{"file_id": "small"}, {"file_id": "big"}],
             "caption": {"text": "Картинка"}},
            {"type": "list", "items": [
                {"label": "1.", "blocks": [{"type": "paragraph", "text": "Пункт 1"}], "type": "1", "value": 1},
                {"label": "2.", "blocks": [{"type": "paragraph", "text": "Пункт 2"}], "type": "1", "value": 2},
            ]},
        ]}
        self.assertEqual(
            rich_message_to_markdown(live),
            "## Заголовок \n\n[фото] Картинка\n\n1. Пункт 1\n2. Пункт 2",
        )
        self.assertEqual(rich_message_files(live), [("big", "photo.jpg")])

    def test_plain_message_is_not_rich(self) -> None:
        msg = Message.de_json({"message_id": 1, "date": 0, "chat": {"id": 1, "type": "private"}, "text": "hi"}, None)
        self.assertFalse(RICH_MESSAGE.filter(msg))


if __name__ == "__main__":
    unittest.main()
