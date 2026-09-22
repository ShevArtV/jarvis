import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from telegram import Message, Update

from bot.handlers import messages


def _msg(**extra) -> Message:
    data = {"message_id": 5, "date": 0, "chat": {"id": -100, "type": "supergroup"},
            "from": {"id": 1, "is_bot": False, "first_name": "u"}, **extra}
    return Message.de_json(data, None)


class MessageKindTest(unittest.TestCase):
    def test_kinds(self) -> None:
        self.assertEqual(messages.message_kind(_msg(text="hi")), "text")
        voice = {"file_id": "v", "file_unique_id": "u", "duration": 1}
        self.assertEqual(messages.message_kind(_msg(voice=voice)), "voice")
        self.assertEqual(messages.message_kind(_msg(rich_message={"blocks": []})), "rich_message")
        self.assertEqual(messages.message_kind(_msg(pinned_message=None)), "other")


class UnhandledMessageTest(unittest.TestCase):
    def _run(self, msg: Message):
        update = Update(update_id=1, message=msg)
        with patch.object(messages, "_process_prompt", new=AsyncMock()) as prompt, \
                patch.object(Message, "reply_text", new=AsyncMock()) as reply:
            asyncio.run(messages.handle_unhandled_message(update, None))
        return prompt, reply

    def test_unknown_command_goes_to_agent(self) -> None:
        prompt, reply = self._run(_msg(text="/home/shevartv/x — глянь",
                                       entities=[{"type": "bot_command", "offset": 0, "length": 5}]))
        prompt.assert_awaited_once()
        self.assertEqual(prompt.await_args.args[1], "/home/shevartv/x — глянь")
        reply.assert_not_awaited()

    def test_voice_gets_reply(self) -> None:
        prompt, reply = self._run(_msg(voice={"file_id": "v", "file_unique_id": "u", "duration": 1}))
        prompt.assert_not_awaited()
        reply.assert_awaited_once()
        self.assertIn("voice", reply.await_args.args[0])

    def test_service_message_is_silent(self) -> None:
        prompt, reply = self._run(_msg(forum_topic_created={"name": "t", "icon_color": 1}))
        prompt.assert_not_awaited()
        reply.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
