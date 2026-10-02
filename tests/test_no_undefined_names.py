"""Статическая проверка: в модулях нет обращений к неопределённым именам.

Появился по конкретному поводу. При выносе `md_to_html` из монолита в
`bot/formatting.py` функции уехали, а четыре regex-константы, на которые они
опираются (`_FENCE_RE` и соседи), остались в `telegram_bot.py`. **Все 45 тестов
при этом прошли**: `md_to_html` ими не покрыт, а `NameError` внутри тела функции
живёт до первого вызова — то есть до первого ответа бота в Telegram.

Такую поломку не ловит ни импорт модуля, ни снапшот проводки. Ловит анализ
неопределённых имён, поэтому он и стоит отдельным тестом: при переносе кода
между модулями это самый вероятный класс ошибок, и он самый дешёвый в проверке.
"""

from __future__ import annotations

import pathlib
import unittest

try:
    from pyflakes.api import check
except ImportError:  # pragma: no cover - без pyflakes проверка просто пропускается
    check = None

REPO = pathlib.Path(__file__).resolve().parent.parent

# Интересуют только ошибки, ломающие исполнение. Неиспользованный импорт —
# вопрос опрятности, а не работоспособности, и валить тесты за него не нужно.
FATAL_MARKERS = (
    "undefined name",
    "syntax error",
    "redefinition of unused",
    "local variable",
)


class _Collect:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def unexpectedError(self, filename: str, msg: str) -> None:
        self.messages.append(f"{filename}: {msg}")

    def syntaxError(self, filename, msg, lineno, offset, text) -> None:
        self.messages.append(f"{filename}:{lineno}: syntax error: {msg}")

    def flake(self, message) -> None:
        self.messages.append(str(message))


@unittest.skipIf(check is None, "pyflakes не установлен")
class NoUndefinedNamesTest(unittest.TestCase):
    def _fatal_for(self, paths: list[pathlib.Path]) -> list[str]:
        reporter = _Collect()
        for path in paths:
            check(path.read_text(encoding="utf-8"), str(path.relative_to(REPO)), reporter)
        return [
            m for m in reporter.messages
            if any(marker in m.lower() for marker in FATAL_MARKERS)
        ]

    def test_bot_package_has_no_undefined_names(self) -> None:
        paths = sorted((REPO / "bot").rglob("*.py"))
        self.assertTrue(paths, "пакет bot/ не найден")
        self.assertEqual(self._fatal_for(paths), [])

    def test_entrypoints_and_engines_have_no_undefined_names(self) -> None:
        paths = [
            REPO / "telegram_bot.py",
            REPO / "config.py",
            *sorted((REPO / "engines").glob("*.py")),
            *sorted((REPO / "plugins").rglob("*.py")),
            *sorted((REPO / "mcp_server").rglob("*.py")),
            *sorted((REPO / "scripts").glob("*.py")),
        ]
        self.assertEqual(self._fatal_for([p for p in paths if p.is_file()]), [])


if __name__ == "__main__":
    unittest.main()
