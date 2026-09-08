from datetime import datetime, timedelta, timezone
import unittest

from engines.limits import EngineLimits, LimitWindow
from engines.throttle import pace_decision


class PaceDecisionTest(unittest.TestCase):
    def test_defers_when_weekly_spend_is_ahead_of_work_time(self):
        end = datetime(2026, 9, 14, 0, tzinfo=timezone.utc)
        result = pace_decision(
            EngineLimits("codex", [LimitWindow("7 дней", 50, end)]),
            datetime(2026, 9, 7, 9, tzinfo=timezone.utc),
        )
        self.assertIsNotNone(result.defer_until)
        self.assertLess(result.allowed_percent, result.used_percent)

    def test_allows_when_spend_is_on_pace(self):
        end = datetime(2026, 9, 14, 0, tzinfo=timezone.utc)
        result = pace_decision(
            EngineLimits("claude", [LimitWindow("7 дней", 1, end)]),
            datetime(2026, 9, 8, 12, tzinfo=timezone.utc),
        )
        self.assertIsNone(result.defer_until)

    def test_unknown_window_never_blocks_work(self):
        self.assertIsNone(pace_decision(EngineLimits("codex"), datetime.now(timezone.utc)).defer_until)

    def test_hard_short_window_blocks_even_when_weekly_is_on_pace(self):
        end = datetime(2026, 9, 8, 15, tzinfo=timezone.utc)
        result = pace_decision(EngineLimits("codex", [
            LimitWindow("5 часов", 100, end), LimitWindow("7 дней", 1, end + timedelta(days=5)),
        ]), datetime(2026, 9, 8, 12, tzinfo=timezone.utc))
        self.assertEqual(result.defer_until, end)
