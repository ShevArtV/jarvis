"""Сторож опроса Telegram: при устойчивой недоступности API процесс должен
завершаться (systemd/Планировщик поднимут заново), а при живой сети — нет."""

import asyncio
import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from bot.workers import polling_watchdog


class _ExitCalled(Exception):
    """Сигнал, что сторож запросил os._exit(1)."""


def _app(get_me):
    return SimpleNamespace(bot=SimpleNamespace(get_me=get_me))


class PollingWatchdogTest(unittest.IsolatedAsyncioTestCase):
    async def test_exits_when_telegram_unreachable(self):
        async def boom():
            raise RuntimeError("telegram unreachable")

        with patch.dict(os.environ, {"JARVIS_POLL_WATCHDOG": "1"}), \
                patch("bot.workers._env_int", return_value=0.01), \
                patch("bot.workers.os._exit", side_effect=_ExitCalled) as ex:
            with self.assertRaises(_ExitCalled):
                await asyncio.wait_for(polling_watchdog(_app(boom)), timeout=2)
            ex.assert_called_once_with(1)

    async def test_stays_alive_when_network_ok(self):
        async def ok():
            return object()

        with patch.dict(os.environ, {"JARVIS_POLL_WATCHDOG": "1"}), \
                patch("bot.workers._env_int", return_value=0.01), \
                patch("bot.workers.os._exit") as ex:
            task = asyncio.create_task(polling_watchdog(_app(ok)))
            await asyncio.sleep(0.1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            ex.assert_not_called()

    async def test_disabled_by_env(self):
        async def boom():
            raise RuntimeError("telegram unreachable")

        with patch.dict(os.environ, {"JARVIS_POLL_WATCHDOG": "0"}), \
                patch("bot.workers.os._exit") as ex:
            await asyncio.wait_for(polling_watchdog(_app(boom)), timeout=1)
            ex.assert_not_called()


if __name__ == "__main__":
    unittest.main()
