"""/board: несколько миниаппов досок QueueWarden."""

from __future__ import annotations

import unittest

from bot.handlers.commands import board_keyboard
from bot.settings import parse_board_miniapps

BOARDS = [
    ("artsites", "https://artsites.test/miniapp", "qwboard"),
    ("tako", "https://tako.test/miniapp", "qwtako"),
]


class ParseBoardsTest(unittest.TestCase):
    def test_parses_list(self) -> None:
        raw = ("artsites|https://artsites.test/miniapp|qwboard, "
               "tako | https://tako.test/miniapp | qwtako")
        self.assertEqual(parse_board_miniapps(raw), BOARDS)

    def test_short_name_optional_and_bad_entries_skipped(self) -> None:
        raw = "a|https://a.test/miniapp,b|http://b.test,c,|https://x.test,,"
        self.assertEqual(parse_board_miniapps(raw), [("a", "https://a.test/miniapp", "")])
        self.assertEqual(parse_board_miniapps(None), [])


class BoardKeyboardTest(unittest.TestCase):
    def test_private_chat_gets_web_app_per_board(self) -> None:
        rows = board_keyboard(BOARDS, True, "jarvisbot").inline_keyboard
        self.assertEqual([r[0].text for r in rows], ["Доска artsites", "Доска tako"])
        self.assertEqual([r[0].web_app.url for r in rows],
                         ["https://artsites.test/miniapp", "https://tako.test/miniapp"])

    def test_group_gets_direct_links_and_skips_boards_without_short_name(self) -> None:
        boards = BOARDS + [("nolink", "https://n.test/miniapp", "")]
        rows = board_keyboard(boards, False, "jarvisbot").inline_keyboard
        self.assertEqual([r[0].url for r in rows],
                         ["https://t.me/jarvisbot/qwboard", "https://t.me/jarvisbot/qwtako"])

    def test_legacy_single_board_label_and_empty(self) -> None:
        rows = board_keyboard([("", "https://a.test/miniapp", "qwboard")], True, "b").inline_keyboard
        self.assertEqual(rows[0][0].text, "Открыть доску")
        self.assertIsNone(board_keyboard([], True, "b"))
        self.assertIsNone(board_keyboard([("x", "https://x.test", "")], False, "b"))


if __name__ == "__main__":
    unittest.main()
