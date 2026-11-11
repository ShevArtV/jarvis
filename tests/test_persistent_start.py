from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

from engines import persistent
from engines.persistent import describe_error, start_persistent


class StartRetryTest(unittest.IsolatedAsyncioTestCase):
    async def test_one_retry_after_failure(self) -> None:
        starter = AsyncMock(side_effect=[TimeoutError(), "worker"])
        with patch.dict(persistent._STARTERS, {"cursor": starter}), \
             patch.object(persistent, "_RETRY_DELAY", 0):
            self.assertEqual(await start_persistent("cursor", key=(1, 2)), "worker")
        self.assertEqual(starter.await_count, 2)

    async def test_second_failure_propagates(self) -> None:
        starter = AsyncMock(side_effect=[RuntimeError("a"), RuntimeError("b")])
        with patch.dict(persistent._STARTERS, {"cursor": starter}), \
             patch.object(persistent, "_RETRY_DELAY", 0):
            with self.assertRaisesRegex(RuntimeError, "b"):
                await start_persistent("cursor", key=(1, 2))


class DescribeErrorTest(unittest.TestCase):
    def test_empty_message_names_exception(self) -> None:
        self.assertEqual(describe_error(TimeoutError()), "CLI не ответил вовремя (TimeoutError)")
        self.assertEqual(describe_error(ValueError()), "ValueError")
        self.assertEqual(describe_error(RuntimeError("Internal error")), "Internal error (RuntimeError)")


if __name__ == "__main__":
    unittest.main()
