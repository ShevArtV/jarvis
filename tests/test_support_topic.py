import unittest
from types import SimpleNamespace
from unittest.mock import patch

from telegram.ext import ApplicationHandlerStop

from bot import app
from plugins.support_topic import plugin as support


class SupportTopicTest(unittest.IsolatedAsyncioTestCase):
    async def test_support_messages_stop_before_agent_handlers(self):
        update = SimpleNamespace(effective_message=SimpleNamespace(chat_id=-100, message_thread_id=10))
        with patch.object(support, "SUPPORT_CHAT_ID", -100), patch.object(support, "SUPPORT_THREAD_ID", 10):
            with self.assertRaises(ApplicationHandlerStop):
                await support.skip_support_topic(update, None)

    async def test_other_chat_and_topic_continue(self):
        with patch.object(support, "SUPPORT_CHAT_ID", -100), patch.object(support, "SUPPORT_THREAD_ID", 10):
            for chat, thread in [(-100, 11), (-200, 10)]:
                update = SimpleNamespace(effective_message=SimpleNamespace(chat_id=chat, message_thread_id=thread))
                await support.skip_support_topic(update, None)
            await support.skip_support_topic(SimpleNamespace(effective_message=None), None)

    def test_gate_registered_only_with_config(self):
        with patch.object(app, "load_plugins", return_value=(support.PLUGIN,)):
            with patch.object(support, "SUPPORT_CHAT_ID", -100), patch.object(support, "SUPPORT_THREAD_ID", 10):
                application = app.build_application(token="123:FAKE", allowed_user_ids={1})
                self.assertEqual(application.handlers[-2][0].callback, support.skip_support_topic)
            with patch.object(support, "SUPPORT_THREAD_ID", 0):
                application = app.build_application(token="123:FAKE", allowed_user_ids={1})
                self.assertNotIn(-2, application.handlers)
