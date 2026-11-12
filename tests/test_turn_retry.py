import asyncio
import json
import unittest
from unittest import mock

from bot import turn_retry
from engines.api_errors import is_network_api_error
from engines.persistent_claude import PersistentClaudeWorker

CERT = ("API Error: Unable to connect to API: Self-signed certificate detected "
        "(DEPTH_ZERO_SELF_SIGNED_CERT). The certificate comes from an authority…")


class DetectTest(unittest.TestCase):
    def test_network(self):
        self.assertTrue(is_network_api_error(CERT))
        self.assertTrue(is_network_api_error("API Error: Connection error."))

    def test_not_network(self):
        self.assertFalse(is_network_api_error("API Error: 429 rate_limit_error"))
        self.assertFalse(is_network_api_error("API Error: 401 authentication_error"))
        self.assertFalse(is_network_api_error("Готово, Unable to connect to API упомянул"))
        self.assertFalse(is_network_api_error(""))


class RetryTest(unittest.IsolatedAsyncioTestCase):
    async def _run(self, results, ok, text):
        turns = mock.AsyncMock(side_effect=results)
        kill = mock.AsyncMock(return_value=True)
        notify = mock.AsyncMock()
        with mock.patch.object(turn_retry, "RETRY_DELAYS", (0, 0, 0)), \
             mock.patch("bot.jobs._run_job_turn_persistent", turns), \
             mock.patch("bot.topics._kill_persistent_worker", kill):
            res = await turn_retry.retry_network_errors(
                None, 1, (1, 1), ok, text, notify)
        return res, turns, kill, notify

    async def test_ok_untouched(self):
        res, turns, kill, _ = await self._run([], True, "ответ")
        self.assertEqual(res, (True, "ответ"))
        turns.assert_not_called(); kill.assert_not_called()

    async def test_other_error_untouched(self):
        res, turns, _, _ = await self._run([], False, "API Error: 429")
        self.assertEqual(res, (False, "API Error: 429"))
        turns.assert_not_called()

    async def test_recovers(self):
        res, turns, kill, notify = await self._run(
            [(False, CERT), (True, "продолжил")], False, CERT)
        self.assertEqual(res, (True, "продолжил"))
        self.assertEqual(turns.await_count, 2)
        self.assertEqual(kill.await_count, 2)
        self.assertEqual(turns.await_args.args[3], turn_retry.CONTINUE_PROMPT)
        self.assertIn("1/3", notify.await_args_list[0].args[0])

    async def test_gives_up(self):
        res, turns, _, _ = await self._run([(False, CERT)] * 3, False, CERT)
        self.assertEqual(res, (False, CERT))
        self.assertEqual(turns.await_count, 3)


class _Stdout:
    def __init__(self, events):
        self.lines = [json.dumps(e).encode() + b"\n" for e in events]

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.lines:
            raise StopAsyncIteration
        return self.lines.pop(0)

    async def readline(self):
        return self.lines.pop(0) if self.lines else b""


class ReadLoopTest(unittest.IsolatedAsyncioTestCase):
    async def _turn(self, ev):
        proc = mock.Mock(stdout=_Stdout([ev]), returncode=None)
        w = PersistentClaudeWorker((1, 1), proc, "s", "/tmp")
        w.busy = True
        w.pending_future = asyncio.get_running_loop().create_future()
        fut = w.pending_future
        await w._read_loop()
        return fut.result()

    async def test_error_result_fails_turn(self):
        self.assertEqual(
            await self._turn({"type": "result", "is_error": True, "result": CERT}),
            (False, CERT))

    async def test_success(self):
        self.assertEqual(
            await self._turn({"type": "result", "is_error": False, "result": "ok"}),
            (True, "ok"))
