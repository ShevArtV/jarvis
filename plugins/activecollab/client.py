"""Minimal ActiveCollab v1 API client used by Jarvis Manager MCP.

The client deliberately uses the standard library: Jarvis already runs a
separate MCP process, so adding another HTTP dependency for a handful of JSON
requests would only enlarge the runtime surface.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


class ActiveCollabError(RuntimeError):
    """A safe-to-display ActiveCollab request/configuration failure."""


def _api_base(url: str) -> str:
    value = url.strip().rstrip("/")
    if not value:
        raise ActiveCollabError("ACTIVE_COLLAB_URL is not configured")
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ActiveCollabError("ACTIVE_COLLAB_URL must be an absolute http(s) URL")
    path = parsed.path.rstrip("/")
    if not path.endswith("/api/v1"):
        path += "/api/v1"
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


class ActiveCollabClient:
    """Small typed-by-convention wrapper around the ActiveCollab JSON API."""

    def __init__(self, url: str, token: str, timeout: float = 20.0) -> None:
        if not token.strip():
            raise ActiveCollabError("ACTIVE_COLLAB_TOKEN is not configured")
        self.api_base = _api_base(url)
        self._token = token.strip()
        self.timeout = timeout

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        body = None
        headers = {
            "Accept": "application/json",
            "X-Angie-AuthApiToken": self._token,
        }
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            self.api_base + "/" + path.lstrip("/"),
            data=body,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            raise ActiveCollabError(f"ActiveCollab API returned HTTP {exc.code}") from exc
        except urllib.error.URLError as exc:
            raise ActiveCollabError(f"ActiveCollab API is unavailable: {exc.reason}") from exc
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ActiveCollabError("ActiveCollab API returned invalid JSON") from exc

    @staticmethod
    def _single(payload: Any) -> dict[str, Any]:
        if isinstance(payload, dict) and isinstance(payload.get("single"), dict):
            return payload["single"]
        if isinstance(payload, dict):
            return payload
        raise ActiveCollabError("ActiveCollab API returned an unexpected object")

    def logged_user_id(self) -> int:
        payload = self._request("GET", "/user-session")
        if not isinstance(payload, dict):
            raise ActiveCollabError("ActiveCollab user session response is invalid")
        value = payload.get("logged_user_id")
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise ActiveCollabError("ActiveCollab did not return logged_user_id") from exc

    def projects(self) -> list[dict[str, Any]]:
        payload = self._request("GET", "/projects")
        if not isinstance(payload, list):
            raise ActiveCollabError("ActiveCollab projects response is invalid")
        return [item for item in payload if isinstance(item, dict)]

    def project_tasks(self, project_id: int) -> dict[str, Any]:
        payload = self._request("GET", f"/projects/{project_id}/tasks")
        if not isinstance(payload, dict):
            raise ActiveCollabError("ActiveCollab tasks response is invalid")
        return payload

    def stages(self, project_id: int) -> list[dict[str, Any]]:
        payload = self.project_tasks(project_id)
        lists = payload.get("task_lists")
        if not isinstance(lists, list):
            return []
        return [item for item in lists if isinstance(item, dict)]

    def job_types(self) -> list[dict[str, Any]]:
        payload = self._request("GET", "/job-types")
        if not isinstance(payload, list):
            raise ActiveCollabError("ActiveCollab job types response is invalid")
        return [item for item in payload if isinstance(item, dict)]

    def my_tasks(self, include_completed: bool = False) -> list[dict[str, Any]]:
        user_id = self.logged_user_id()
        result: list[dict[str, Any]] = []
        for project in self.projects():
            project_id = project.get("id")
            if not isinstance(project_id, int):
                continue
            payload = self.project_tasks(project_id)
            lists = {
                item.get("id"): item.get("name")
                for item in payload.get("task_lists", [])
                if isinstance(item, dict)
            }
            for task in payload.get("tasks", []):
                if not isinstance(task, dict) or task.get("assignee_id") != user_id:
                    continue
                if not include_completed and task.get("is_completed"):
                    continue
                item = dict(task)
                item["project_name"] = project.get("name")
                item["stage_name"] = lists.get(task.get("task_list_id"))
                result.append(item)
        return result

    def task(self, project_id: int, task_id: int) -> dict[str, Any]:
        payload = self._request("GET", f"/projects/{project_id}/tasks/{task_id}")
        if not isinstance(payload, dict):
            raise ActiveCollabError("ActiveCollab task response is invalid")
        return payload

    def notifications(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        payload = self._request("GET", "/notifications")
        if not isinstance(payload, dict):
            raise ActiveCollabError("ActiveCollab notifications response is invalid")
        notifications = payload.get("notifications")
        related = payload.get("related")
        return (
            [item for item in notifications if isinstance(item, dict)]
            if isinstance(notifications, list) else [],
            related if isinstance(related, dict) else {},
        )

    @staticmethod
    def _notification_task_id(notification: dict[str, Any], related: dict[str, Any]) -> int | None:
        if notification.get("parent_type") == "Task":
            value = notification.get("parent_id")
            return value if isinstance(value, int) else None
        parent_id = notification.get("parent_id")
        comment = related.get("Comment", {}).get(str(parent_id))
        if comment is None:
            comment = related.get("Comment", {}).get(parent_id)
        if isinstance(comment, dict) and comment.get("parent_type") == "Task":
            value = comment.get("parent_id")
            return value if isinstance(value, int) else None
        return None

    def comment_notifications_for(self, task_ids: set[int]) -> list[dict[str, Any]]:
        notifications, related = self.notifications()
        result = []
        for notification in notifications:
            if notification.get("class") != "NewCommentNotification":
                continue
            task_id = self._notification_task_id(notification, related)
            if task_id not in task_ids:
                continue
            item = dict(notification)
            item["task_id"] = task_id
            result.append(item)
        return result

    def add_comment(self, task_id: int, body: str) -> dict[str, Any]:
        text = body.strip()
        if not text:
            raise ValueError("comment body is required")
        return self._single(self._request("POST", f"/comments/task/{task_id}", {"body": text}))

    def track_time(
        self,
        project_id: int,
        task_id: int,
        value: str,
        record_date: str,
        job_type_id: int,
        summary: str | None = None,
        billable_status: int | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "task_id": task_id,
            "value": value,
            "record_date": record_date,
            "job_type_id": job_type_id,
            "user_id": self.logged_user_id(),
        }
        if summary is not None:
            payload["summary"] = summary
        if billable_status is not None:
            payload["billable_status"] = billable_status
        return self._single(self._request("POST", f"/projects/{project_id}/time-records", payload))

    def move_to_stage(self, project_id: int, task_id: int, task_list_id: int) -> dict[str, Any]:
        return self._single(self._request(
            "PUT", f"/projects/{project_id}/tasks/{task_id}", {"task_list_id": task_list_id},
        ))

    def move_to_stage_name(self, project_id: int, task_id: int, stage_name: str) -> dict[str, Any]:
        wanted = stage_name.strip().casefold()
        if not wanted:
            raise ValueError("stage_name is required")
        matches = [
            stage for stage in self.stages(project_id)
            if str(stage.get("name") or "").casefold() == wanted
        ]
        if len(matches) != 1 or not isinstance(matches[0].get("id"), int):
            raise ActiveCollabError(f"ActiveCollab stage not found: {stage_name!r}")
        return self.move_to_stage(project_id, task_id, matches[0]["id"])
