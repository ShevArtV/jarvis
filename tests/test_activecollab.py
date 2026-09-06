from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from bot import db as bot_db
from integrations.activecollab import ActiveCollabClient
from scripts import jarvis_mcp_server as manager_mcp


class _Response:
    def __init__(self, payload: object) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self) -> bytes:
        return self._body


class ActiveCollabClientTest(unittest.TestCase):
    def test_my_tasks_filters_authenticated_user_and_adds_stage(self) -> None:
        replies = iter([
            {"logged_user_id": 7},
            [{"id": 1, "name": "Project A"}],
            {
                "task_lists": [{"id": 10, "name": "В работе"}],
                "tasks": [
                    {"id": 11, "assignee_id": 7, "task_list_id": 10, "is_completed": False},
                    {"id": 12, "assignee_id": 8, "task_list_id": 10, "is_completed": False},
                    {"id": 13, "assignee_id": 7, "task_list_id": 10, "is_completed": True},
                ],
            },
        ])
        seen_paths: list[str] = []

        def urlopen(request, timeout):
            seen_paths.append(request.full_url)
            return _Response(next(replies))

        client = ActiveCollabClient("https://ac.example", "secret")
        with patch("urllib.request.urlopen", side_effect=urlopen):
            tasks = client.my_tasks()

        self.assertEqual([task["id"] for task in tasks], [11])
        self.assertEqual(tasks[0]["project_name"], "Project A")
        self.assertEqual(tasks[0]["stage_name"], "В работе")
        self.assertEqual(seen_paths, [
            "https://ac.example/api/v1/user-session",
            "https://ac.example/api/v1/projects",
            "https://ac.example/api/v1/projects/1/tasks",
        ])

    def test_write_requests_use_json_and_auth_header(self) -> None:
        captured = []

        def urlopen(request, timeout):
            captured.append(request)
            return _Response({"single": {"id": 42}})

        client = ActiveCollabClient("https://ac.example/api/v1", "secret-token")
        with patch("urllib.request.urlopen", side_effect=urlopen):
            result = client.add_comment(123, "Готово")

        self.assertEqual(result, {"id": 42})
        request = captured[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.full_url, "https://ac.example/api/v1/comments/task/123")
        self.assertEqual(request.get_header("X-angie-authapitoken"), "secret-token")
        self.assertEqual(json.loads(request.data.decode("utf-8")), {"body": "Готово"})

    def test_comment_notifications_resolve_related_comment_task(self) -> None:
        client = ActiveCollabClient("https://ac.example", "secret")
        with patch.object(client, "notifications", return_value=(
            [{"id": 1, "class": "NewCommentNotification", "parent_type": "Comment", "parent_id": 2}],
            {"Comment": {"2": {"parent_type": "Task", "parent_id": 99}}},
        )):
            notifications = client.comment_notifications_for({99})

        self.assertEqual(notifications[0]["task_id"], 99)


class ActiveCollabDeltaTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "bot_state.db"
        self.db_patch = patch.object(bot_db, "DB_PATH", str(self.db_path))
        self.db_patch.start()
        bot_db.init_db()
        self.old_db_path = manager_mcp._DB_PATH
        manager_mcp._DB_PATH = self.db_path

    def tearDown(self) -> None:
        manager_mcp._DB_PATH = self.old_db_path
        self.db_patch.stop()
        self.tmp.cleanup()

    @staticmethod
    def _client(tasks, notifications):
        return SimpleNamespace(
            my_tasks=lambda include_completed: tasks,
            comment_notifications_for=lambda task_ids: notifications,
        )

    def test_first_check_is_baseline_then_only_returns_delta(self) -> None:
        first_tasks = [{"id": 10, "is_completed": False, "name": "Existing"}]
        first_notifications = [{"id": 20, "task_id": 10}]
        with patch.object(manager_mcp, "_activecollab_client", return_value=self._client(first_tasks, first_notifications)):
            first = manager_mcp._activecollab_check_updates()

        self.assertTrue(first["initial_check"])
        self.assertEqual([task["id"] for task in first["open_tasks"]], [10])
        self.assertEqual(first["new_tasks"], [])
        self.assertEqual(first["new_comment_notifications"], [])

        second_tasks = first_tasks + [{"id": 11, "is_completed": False, "name": "New"}]
        second_notifications = first_notifications + [{"id": 21, "task_id": 11}]
        with patch.object(manager_mcp, "_activecollab_client", return_value=self._client(second_tasks, second_notifications)):
            second = manager_mcp._activecollab_check_updates()

        self.assertFalse(second["initial_check"])
        self.assertEqual([task["id"] for task in second["new_tasks"]], [11])
        self.assertEqual([item["id"] for item in second["new_comment_notifications"]], [21])


if __name__ == "__main__":
    unittest.main()
