"""Журнал хода: пишет в хвост топика и не глохнет от сетевой икоты.

Два подтверждённых логами дефекта, которые чинят эти тесты:

* журнал правил СВОЁ сообщение даже после того, как ниже уехал вопрос
  ask_user — трансляция уходила вверх экрана и выглядела зависшей;
* один `httpcore.ReadTimeout` ставил вечный флаг `_broken`, и журнал молчал
  до конца хода (37 таких случаев за сутки в journalctl).
"""

from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from telegram.error import TimedOut

from bot.delivery import ProgressJournal


class FakeMessage:
    def __init__(self, message_id: int, sink: list[tuple[str, str]]):
        self.message_id = message_id
        self._sink = sink
        self.text = ""

    async def edit_text(self, text, **kwargs):
        self.text = text
        self._sink.append(("edit", text))
        return self

    async def delete(self):
        self._sink.append(("delete", ""))


class FakeChat:
    """Минимальный chat: раздаёт растущие message_id и пишет всё в sink."""

    def __init__(self):
        self.id = -100500
        self.sink: list[tuple[str, str]] = []
        self.messages: list[FakeMessage] = []
        self._next_id = 100
        self.fail_edits_with: Exception | None = None

    async def send_message(self, text=None, **kwargs):
        self._next_id += 1
        msg = FakeMessage(self._next_id, self.sink)
        msg.text = text or ""
        self.sink.append(("send", text or ""))
        self.messages.append(msg)
        return msg


def _journal(chat) -> ProgressJournal:
    j = ProgressJournal(chat, thread_id=7)
    j._last_stale_check = -1e9   # снять троттлинг сверки с хвостом топика
    return j


class JournalTailTest(unittest.TestCase):
    def test_detaches_when_topic_moved_on(self) -> None:
        """Ниже журнала уехало чужое сообщение — шаги идут в новое, внизу."""
        chat = FakeChat()

        async def scenario():
            with patch("bot.delivery.latest_topic_message_id", return_value=0):
                journal = _journal(chat)
                await journal.start()
                await journal.append("🔧 шаг 1")
                first_id = journal.msg.message_id
            # Топик ушёл вперёд: чужое сообщение с бо́льшим id.
            with patch("bot.delivery.latest_topic_message_id",
                       return_value=first_id + 5):
                journal._last_stale_check = -1e9
                await journal.append("🔧 шаг 2")
            return first_id, journal

        first_id, journal = asyncio.run(scenario())
        self.assertNotEqual(journal.msg.message_id, first_id)
        # Новое сообщение содержит только неотрисованный хвост, без дублей.
        self.assertIn("шаг 2", journal.msg.text)
        self.assertNotIn("шаг 1", journal.msg.text)

    def test_stays_in_place_while_topic_quiet(self) -> None:
        chat = FakeChat()

        async def scenario():
            with patch("bot.delivery.latest_topic_message_id", return_value=0):
                journal = _journal(chat)
                await journal.start()
                await journal.append("🔧 шаг 1")
                first_id = journal.msg.message_id
                journal._last_stale_check = -1e9
                await journal.append("🔧 шаг 2")
                return first_id, journal

        first_id, journal = asyncio.run(scenario())
        self.assertEqual(journal.msg.message_id, first_id)
        self.assertIn("шаг 1", journal.msg.text)
        self.assertIn("шаг 2", journal.msg.text)


class JournalResilienceTest(unittest.TestCase):
    def test_timeout_does_not_kill_journal(self) -> None:
        """Сетевой таймаут — пропуск апдейта, а не конец трансляции."""
        chat = FakeChat()
        calls = {"n": 0}

        async def flaky_edit(text, **kwargs):
            calls["n"] += 1
            if calls["n"] <= 3:
                raise TimedOut("read timeout")
            return None

        async def scenario():
            with patch("bot.delivery.latest_topic_message_id", return_value=0):
                journal = _journal(chat)
                await journal.start()
                journal.msg.edit_text = flaky_edit
                for i in range(4):
                    journal._last_stale_check = -1e9
                    await journal.append(f"🔧 шаг {i}")
                return journal

        journal = asyncio.run(scenario())
        self.assertFalse(journal._broken)
        self.assertEqual(journal.total_steps, 4)
        # Пропущенные строки не потерялись — они в буфере и уехали последним флешем.
        self.assertEqual(len(journal.lines), 4)

    def test_permanent_errors_eventually_stop_journal(self) -> None:
        """Ошибка, которую повтор не лечит, гасит журнал — но не с первой."""
        chat = FakeChat()

        async def dead_edit(text, **kwargs):
            raise RuntimeError("chat not found")

        async def dead_send(text=None, **kwargs):
            raise RuntimeError("chat not found")

        async def scenario():
            with patch("bot.delivery.latest_topic_message_id", return_value=0):
                journal = _journal(chat)
                await journal.start()
                journal.msg.edit_text = dead_edit
                chat.send_message = dead_send
                states = []
                for i in range(3):
                    journal._last_stale_check = -1e9
                    await journal.append(f"🔧 шаг {i}")
                    states.append(journal._broken)
                return states

        states = asyncio.run(scenario())
        self.assertFalse(states[0])          # первая ошибка прощается
        self.assertTrue(states[-1])          # после лимита — молчим


class JournalHeartbeatTest(unittest.TestCase):
    def test_heartbeat_line_appears_on_silence(self) -> None:
        """Долгая тишина агента подписывается прямо в журнале."""
        chat = FakeChat()

        async def scenario():
            with patch("bot.delivery.latest_topic_message_id", return_value=0):
                journal = _journal(chat)
                await journal.start()
                await journal.append("🔧 шаг 1")
                journal._last_step_at -= 3600      # как будто час тишины
                journal._hb_suffix = "⏳ работаю 60 мин, шагов 1 (тишина 3600 с)"
                journal._last_stale_check = -1e9
                await journal._flush()
                text = journal.msg.text
                # Следующий шаг убирает подпись.
                journal._last_stale_check = -1e9
                await journal.append("🔧 шаг 2")
                return text, journal.msg.text

        with_hb, after_step = asyncio.run(scenario())
        self.assertIn("работаю", with_hb)
        self.assertNotIn("работаю", after_step)


if __name__ == "__main__":
    unittest.main()
