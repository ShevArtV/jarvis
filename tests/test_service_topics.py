from __future__ import annotations

import unittest
from unittest.mock import patch

from bot.topics import (
    resolve_manager_topic,
    resolve_secretary_topic,
    resolve_service_topic,
    resolve_teamlead_topic,
    resolve_topic_role,
)


SERVICE_ENV = {
    "JARVIS_MANAGER_CHAT_ID",
    "JARVIS_MANAGER_THREAD_ID",
    "JARVIS_SECRETARY_CHAT_ID",
    "JARVIS_SECRETARY_THREAD_ID",
    "JARVIS_TEAMLEAD_CHAT_ID",
    "JARVIS_TEAMLEAD_THREAD_ID",
}


def clean_env(**values: str):
    env = {key: value for key, value in values.items() if value is not None}
    return patch.dict("os.environ", env, clear=False)


class ServiceTopicResolutionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.env_patcher = patch.dict(
            "os.environ", {key: "" for key in SERVICE_ENV}, clear=False,
        )
        self.env_patcher.start()

    def tearDown(self) -> None:
        self.env_patcher.stop()

    def test_legacy_manager_env_is_secretary(self) -> None:
        with clean_env(
            JARVIS_MANAGER_CHAT_ID="-100",
            JARVIS_MANAGER_THREAD_ID="2338",
        ):
            self.assertEqual(resolve_secretary_topic(), (-100, 2338))
            self.assertEqual(resolve_manager_topic(), (-100, 2338))
            self.assertEqual(resolve_topic_role((-100, 2338)), "secretary")

    def test_explicit_secretary_overrides_legacy_manager(self) -> None:
        with clean_env(
            JARVIS_MANAGER_CHAT_ID="-100",
            JARVIS_MANAGER_THREAD_ID="2338",
            JARVIS_SECRETARY_CHAT_ID="-100",
            JARVIS_SECRETARY_THREAD_ID="2400",
        ):
            self.assertEqual(resolve_secretary_topic(), (-100, 2400))
            self.assertEqual(resolve_manager_topic(), (-100, 2400))

    def test_teamlead_role_is_distinct_when_configured(self) -> None:
        with clean_env(
            JARVIS_MANAGER_CHAT_ID="-100",
            JARVIS_MANAGER_THREAD_ID="2338",
            JARVIS_TEAMLEAD_CHAT_ID="-100",
            JARVIS_TEAMLEAD_THREAD_ID="2498",
        ):
            self.assertEqual(resolve_teamlead_topic(), (-100, 2498))
            self.assertEqual(resolve_topic_role((-100, 2498)), "teamlead")
            self.assertEqual(resolve_topic_role((-100, 2338)), "secretary")
            self.assertEqual(resolve_topic_role((-100, 9411)), "agent")

    def test_teamlead_delivery_falls_back_to_secretary_without_role_change(self) -> None:
        with clean_env(
            JARVIS_MANAGER_CHAT_ID="-100",
            JARVIS_MANAGER_THREAD_ID="2338",
        ):
            self.assertEqual(resolve_teamlead_topic(), (-100, 2338))
            self.assertEqual(resolve_service_topic("teamlead"), (-100, 2338))
            self.assertEqual(resolve_topic_role((-100, 2338)), "secretary")


if __name__ == "__main__":
    unittest.main()
