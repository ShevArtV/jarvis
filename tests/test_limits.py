from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from engines.limits import (
    EngineLimits,
    LimitWindow,
    _claude_limits_from_cache,
    _codex_limits_from_rollouts,
    claude_limits,
    codex_limits,
    format_limits_block,
)


class ClaudeLimitsFromCacheTest(unittest.TestCase):
    def test_parses_two_windows_with_percent_and_resets_at(self) -> None:
        config = {
            "cachedUsageUtilization": {
                "fetchedAtMs": 1788615429095,
                "utilization": {
                    "five_hour": {
                        "utilization": 21,
                        "resets_at": "2026-09-05T15:59:59.954083+00:00",
                    },
                    "seven_day": {
                        "utilization": 15,
                        "resets_at": "2026-09-08T09:59:59.954105+00:00",
                    },
                    "seven_day_opus": None,
                    "seven_day_sonnet": None,
                },
            }
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "claude.json"
            path.write_text(json.dumps(config), encoding="utf-8")
            with patch.dict(os.environ, {"CLAUDE_CONFIG_JSON": str(path)}):
                result = _claude_limits_from_cache()

        self.assertEqual(result.engine, "claude")
        self.assertIsNone(result.note)
        self.assertEqual(len(result.windows), 2)
        five_hour, seven_day = result.windows
        self.assertEqual(five_hour.name, "5 часов")
        self.assertEqual(five_hour.used_percent, 21)
        self.assertEqual(
            five_hour.resets_at,
            datetime.fromisoformat("2026-09-05T15:59:59.954083+00:00"),
        )
        self.assertEqual(seven_day.name, "7 дней")
        self.assertEqual(seven_day.used_percent, 15)
        self.assertEqual(
            result.fetched_at,
            datetime.fromtimestamp(1788615429095 / 1000, tz=timezone.utc),
        )

    def test_missing_file_returns_empty_windows_and_note(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "does-not-exist.json"
            with patch.dict(os.environ, {"CLAUDE_CONFIG_JSON": str(path)}):
                result = _claude_limits_from_cache()
        self.assertEqual(result.windows, [])
        self.assertTrue(result.note)

    def test_broken_json_returns_empty_windows_and_note(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "claude.json"
            path.write_text("{не json", encoding="utf-8")
            with patch.dict(os.environ, {"CLAUDE_CONFIG_JSON": str(path)}):
                result = _claude_limits_from_cache()
        self.assertEqual(result.windows, [])
        self.assertTrue(result.note)


class CodexLimitsFromRolloutsTest(unittest.TestCase):
    def test_uses_freshest_file_and_last_rate_limits_line(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            older = root / "rollout-older.jsonl"
            newer = root / "rollout-newer.jsonl"

            older.write_text(
                json.dumps(
                    {
                        "payload": {
                            "rate_limits": {
                                "primary": {
                                    "used_percent": 99.0,
                                    "window_minutes": 43200,
                                    "resets_at": 1790000000,
                                },
                                "secondary": None,
                            }
                        }
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            # Более свежий файл, где несколько строк с rate_limits: должна
            # использоваться ПОСЛЕДНЯЯ, а secondary=null пропускаться.
            first_line = json.dumps(
                {
                    "payload": {
                        "rate_limits": {
                            "primary": {
                                "used_percent": 10.0,
                                "window_minutes": 43200,
                                "resets_at": 1790971501,
                            },
                            "secondary": None,
                        }
                    }
                }
            )
            last_line = json.dumps(
                {
                    "payload": {
                        "rate_limits": {
                            "primary": {
                                "used_percent": 26.0,
                                "window_minutes": 43200,
                                "resets_at": 1790971501,
                            },
                            "secondary": None,
                        }
                    }
                }
            )
            newer.write_text(first_line + "\n" + last_line + "\n", encoding="utf-8")

            now = datetime.now().timestamp()
            os.utime(older, (now - 3600, now - 3600))
            os.utime(newer, (now, now))

            with patch.dict(os.environ, {"CODEX_SESSIONS_DIR": str(root)}):
                result = _codex_limits_from_rollouts()

        self.assertEqual(result.source, str(newer))
        self.assertEqual(len(result.windows), 1)
        window = result.windows[0]
        self.assertEqual(window.used_percent, 26.0)
        self.assertEqual(window.name, "30 дней")
        self.assertEqual(
            window.resets_at, datetime.fromtimestamp(1790971501, tz=timezone.utc)
        )

    def test_missing_sessions_dir_returns_empty_windows_and_note(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "sessions"
            with patch.dict(os.environ, {"CODEX_SESSIONS_DIR": str(missing)}):
                result = _codex_limits_from_rollouts()
        self.assertEqual(result.windows, [])
        self.assertTrue(result.note)


class ClaudeLimitsLiveTest(unittest.TestCase):
    def test_live_success_returns_two_windows(self) -> None:
        payload = {
            "five_hour": {
                "utilization": 31.0,
                "resets_at": "2026-09-05T16:00:00+00:00",
            },
            "seven_day": {
                "utilization": 16.0,
                "resets_at": "2026-09-08T10:00:00+00:00",
            },
        }
        with (
            patch("engines.limits._claude_token", return_value=("t", None)),
            patch("engines.limits._fetch_json", return_value=(payload, None)) as fetch,
        ):
            result = claude_limits()

        fetch.assert_called_once()
        self.assertTrue(result.live)
        self.assertEqual(len(result.windows), 2)
        five_hour, seven_day = result.windows
        self.assertEqual(five_hour.name, "5 часов")
        self.assertEqual(five_hour.used_percent, 31.0)
        self.assertEqual(seven_day.name, "7 дней")
        self.assertEqual(seven_day.used_percent, 16.0)

    def test_api_error_falls_back_to_cache(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing_cache = Path(tmp) / "does-not-exist.json"
            with (
                patch("engines.limits._claude_token", return_value=("t", None)),
                patch(
                    "engines.limits._fetch_json",
                    return_value=(None, "API отклонил токен (HTTP 401)"),
                ) as fetch,
                patch.dict(os.environ, {"CLAUDE_CONFIG_JSON": str(missing_cache)}),
            ):
                result = claude_limits()

        fetch.assert_called_once()
        self.assertFalse(result.live)
        self.assertIn("API отклонил токен (HTTP 401)", result.note)


class CodexLimitsLiveTest(unittest.TestCase):
    def test_live_success_returns_one_window(self) -> None:
        payload = {
            "rate_limit": {
                "primary_window": {
                    "used_percent": 100,
                    "limit_window_seconds": 2592000,
                    "reset_at": 1790971501,
                },
                "secondary_window": None,
            }
        }
        with (
            patch("engines.limits._codex_token", return_value=("t", None, None)),
            patch("engines.limits._fetch_json", return_value=(payload, None)) as fetch,
        ):
            result = codex_limits()

        fetch.assert_called_once()
        self.assertTrue(result.live)
        self.assertEqual(len(result.windows), 1)
        window = result.windows[0]
        self.assertIn("30 дней", window.name)
        self.assertEqual(window.used_percent, 100)
        self.assertEqual(
            window.resets_at, datetime.fromtimestamp(1790971501, tz=timezone.utc)
        )

    def test_api_error_falls_back_to_rollouts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing_sessions = Path(tmp) / "sessions"
            with (
                patch("engines.limits._codex_token", return_value=("t", None, None)),
                patch(
                    "engines.limits._fetch_json",
                    return_value=(None, "API отклонил токен (HTTP 401)"),
                ) as fetch,
                patch.dict(os.environ, {"CODEX_SESSIONS_DIR": str(missing_sessions)}),
            ):
                result = codex_limits()

        fetch.assert_called_once()
        self.assertFalse(result.live)
        self.assertIn("API отклонил токен (HTTP 401)", result.note)


class FormatLimitsBlockTest(unittest.TestCase):
    def test_percentages_expired_window_and_opencode_note(self) -> None:
        now = datetime(2026, 9, 5, 13, 0, 0, tzinfo=timezone.utc)
        items = [
            EngineLimits(
                engine="claude",
                windows=[
                    LimitWindow(
                        name="5 часов",
                        used_percent=21,
                        resets_at=now + timedelta(hours=2, minutes=15),
                    ),
                    LimitWindow(
                        name="истёкшее",
                        used_percent=90,
                        resets_at=now - timedelta(minutes=5),
                    ),
                ],
            ),
            EngineLimits(engine="opencode", note="лимиты подписки opencode не отслеживаются"),
        ]
        body = format_limits_block(items, now=now)

        self.assertIn("**claude**", body)
        self.assertIn("осталось 79%", body)
        self.assertIn("осталось 10%", body)
        self.assertIn("сброс уже наступил", body)
        self.assertIn("лимиты подписки opencode не отслеживаются", body)

    def test_shows_remaining_not_spent(self) -> None:
        now = datetime(2026, 9, 5, 13, 0, 0, tzinfo=timezone.utc)
        items = [
            EngineLimits(
                engine="claude",
                windows=[LimitWindow(name="5 часов", used_percent=31.0)],
            ),
        ]
        body = format_limits_block(items, now=now)
        self.assertIn("осталось 69%", body)

    def test_non_live_result_marks_cache(self) -> None:
        now = datetime(2026, 9, 5, 13, 0, 0, tzinfo=timezone.utc)
        items = [
            EngineLimits(
                engine="claude",
                windows=[LimitWindow(name="5 часов", used_percent=21)],
                live=False,
                fetched_at=now - timedelta(hours=1),
            ),
        ]
        body = format_limits_block(items, now=now)
        self.assertIn("кэш", body)


if __name__ == "__main__":
    unittest.main()
