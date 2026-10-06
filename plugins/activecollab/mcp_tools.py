"""MCP-тулы ActiveCollab. Регистрируются через register(mcp), если плагин
включён в JARVIS_PLUGINS.

Тулы — async и гоняют sync urllib-клиент через asyncio.to_thread: FastMCP
вызывает sync-функции прямо в event loop, а один медленный check_updates иначе
глушит весь stdio-MCP (включая manager_topics / manager_inbox).
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from bot.timeutil import utcnow
from mcp_server import common

# Жёсткий бюджет на весь check_updates: при деградации API не держать агента
# N×per-request секунд. Per-request короче обычного 20с — fail-fast внутри бюджета.
CHECK_UPDATES_BUDGET_SECONDS = 45.0
CHECK_UPDATES_REQUEST_TIMEOUT = 10.0


def _activecollab_client(
    *,
    timeout: float | None = None,
    deadline: float | None = None,
):
    """Build an ActiveCollab client from the bot environment without logging secrets."""
    url = common._env_or_dotenv("ACTIVE_COLLAB_URL")
    token = common._env_or_dotenv("ACTIVE_COLLAB_TOKEN")
    if not url or not token:
        raise RuntimeError(
            "ActiveCollab is not configured: set ACTIVE_COLLAB_URL and "
            "ACTIVE_COLLAB_TOKEN in Jarvis .env"
        )
    from plugins.activecollab.client import ActiveCollabClient  # type: ignore

    kwargs: dict[str, Any] = {}
    if timeout is not None:
        kwargs["timeout"] = timeout
    if deadline is not None:
        kwargs["deadline"] = deadline
    return ActiveCollabClient(url, token, **kwargs)


def _activecollab_check_updates() -> dict[str, Any]:
    """Fetch the user's ActiveCollab delta and persist the observed IDs."""
    client = _activecollab_client(
        timeout=CHECK_UPDATES_REQUEST_TIMEOUT,
        deadline=time.monotonic() + CHECK_UPDATES_BUDGET_SECONDS,
    )
    all_tasks = client.my_tasks(include_completed=True)
    open_tasks = [task for task in all_tasks if not task.get("is_completed")]
    task_ids = {
        task["id"] for task in all_tasks if isinstance(task.get("id"), int)
    }
    task_by_id = {
        task["id"]: task for task in all_tasks if isinstance(task.get("id"), int)
    }
    comment_notifications = client.comment_notifications_for(task_ids)
    for notification in comment_notifications:
        task = task_by_id.get(notification.get("task_id"))
        if task is not None:
            notification["project_id"] = task.get("project_id")
            notification["project_name"] = task.get("project_name")
            notification["task_name"] = task.get("name")
    now = utcnow().isoformat()

    with common._connect() as conn:
        initialized = conn.execute(
            "SELECT 1 FROM integration_sync_state "
            "WHERE integration='activecollab' AND state_key='updates_initialized'"
        ).fetchone() is not None

        new_tasks: list[dict[str, Any]] = []
        for task in open_tasks:
            task_id = task.get("id")
            if not isinstance(task_id, int):
                continue
            cur = conn.execute(
                "INSERT OR IGNORE INTO integration_seen_items "
                "(integration, kind, item_id, seen_at) VALUES "
                "('activecollab', 'task', ?, ?)",
                (task_id, now),
            )
            if initialized and cur.rowcount == 1:
                new_tasks.append(task)

        new_comments: list[dict[str, Any]] = []
        for notification in comment_notifications:
            notification_id = notification.get("id")
            if not isinstance(notification_id, int):
                continue
            cur = conn.execute(
                "INSERT OR IGNORE INTO integration_seen_items "
                "(integration, kind, item_id, seen_at) VALUES "
                "('activecollab', 'comment_notification', ?, ?)",
                (notification_id, now),
            )
            if initialized and cur.rowcount == 1:
                new_comments.append(notification)

        conn.execute(
            "INSERT INTO integration_sync_state "
            "(integration, state_key, state_value, updated_at) VALUES "
            "('activecollab', 'updates_initialized', '1', ?) "
            "ON CONFLICT(integration, state_key) DO UPDATE SET "
            "state_value=excluded.state_value, updated_at=excluded.updated_at",
            (now,),
        )

    return {
        "initial_check": not initialized,
        "open_tasks": open_tasks if not initialized else [],
        "new_tasks": new_tasks,
        "new_comment_notifications": new_comments,
    }


def register(mcp) -> None:
    """Зарегистрировать тулы ActiveCollab на сервере FastMCP."""

    @mcp.tool(
        name="manager_activecollab_my_tasks",
        description=(
            "Возвращает задачи, назначенные пользователю токена ActiveCollab. "
            "По умолчанию только незавершённые; каждая запись содержит проект и "
            "текущую стадию (task list). Только чтение."
        ),
    )
    async def manager_activecollab_my_tasks(include_completed: bool = False) -> dict[str, Any]:
        """List tasks assigned to the authenticated ActiveCollab user."""
        def work() -> dict[str, Any]:
            tasks = _activecollab_client().my_tasks(include_completed=include_completed)
            return {"count": len(tasks), "tasks": tasks}

        return await asyncio.to_thread(work)

    @mcp.tool(
        name="manager_activecollab_task",
        description=(
            "Читает задачу ActiveCollab и её комментарии. Нужны project_id и task_id; "
            "бери их из manager_activecollab_my_tasks или ссылки на задачу. Только чтение."
        ),
    )
    async def manager_activecollab_task(project_id: int, task_id: int) -> dict[str, Any]:
        """Return one task, including its comments and subscribers."""
        return await asyncio.to_thread(
            lambda: _activecollab_client().task(project_id, task_id),
        )

    @mcp.tool(
        name="manager_activecollab_stages",
        description=(
            "Возвращает допустимые стадии задачи (task lists) проекта ActiveCollab. "
            "Используй перед переносом, если целевая стадия не названа точно. Только чтение."
        ),
    )
    async def manager_activecollab_stages(project_id: int) -> dict[str, Any]:
        """List the project task lists used as workflow stages."""
        def work() -> dict[str, Any]:
            stages = _activecollab_client().stages(project_id)
            return {"count": len(stages), "stages": stages}

        return await asyncio.to_thread(work)

    @mcp.tool(
        name="manager_activecollab_job_types",
        description=(
            "Возвращает доступные типы работ ActiveCollab для трекинга времени. "
            "Только чтение."
        ),
    )
    async def manager_activecollab_job_types() -> dict[str, Any]:
        """List ActiveCollab job types available to the current user."""
        def work() -> dict[str, Any]:
            job_types = _activecollab_client().job_types()
            return {"count": len(job_types), "job_types": job_types}

        return await asyncio.to_thread(work)

    @mcp.tool(
        name="manager_activecollab_check_updates",
        description=(
            "Проверяет новые незавершённые задачи пользователя и новые уведомления "
            "о комментариях к его задачам. Запоминает уже увиденные ID в локальной "
            "SQLite БД: первый вызов возвращает стартовую сводку, следующие — только "
            "дельту. Этот tool предназначен для reminder-чеклиста Секретаря."
        ),
    )
    async def manager_activecollab_check_updates() -> dict[str, Any]:
        """Return new ActiveCollab tasks/comment notifications since the last check."""
        return await asyncio.to_thread(_activecollab_check_updates)

    @mcp.tool(
        name="manager_activecollab_add_comment",
        description=(
            "Создаёт комментарий к задаче ActiveCollab. ВНЕШНЕЕ WRITE-ДЕЙСТВИЕ: "
            "вызывай только после явного одобрения оператора с конкретной задачей "
            "и текстом комментария."
        ),
    )
    async def manager_activecollab_add_comment(task_id: int, body: str) -> dict[str, Any]:
        """Post an explicitly approved comment to an ActiveCollab task."""
        return await asyncio.to_thread(
            lambda: _activecollab_client().add_comment(task_id, body),
        )

    @mcp.tool(
        name="manager_activecollab_track_time",
        description=(
            "Добавляет учёт времени к задаче ActiveCollab. ВНЕШНЕЕ WRITE-ДЕЙСТВИЕ: "
            "вызывай только после явного одобрения оператора с задачей, длительностью, "
            "датой и типом работы. value: например '1:30' или '1.5'; record_date: YYYY-MM-DD."
        ),
    )
    async def manager_activecollab_track_time(
        project_id: int,
        task_id: int,
        value: str,
        record_date: str,
        job_type_id: int,
        summary: str | None = None,
        billable_status: int | None = None,
    ) -> dict[str, Any]:
        """Create an explicitly approved ActiveCollab time record."""
        def work() -> dict[str, Any]:
            return _activecollab_client().track_time(
                project_id, task_id, value, record_date, job_type_id, summary, billable_status,
            )

        return await asyncio.to_thread(work)

    @mcp.tool(
        name="manager_activecollab_move_task",
        description=(
            "Переносит задачу ActiveCollab на стадию по её точному названию, например "
            "'В работе' или 'Можно тестировать'. ВНЕШНЕЕ WRITE-ДЕЙСТВИЕ: вызывай "
            "только после явного одобрения оператора с конкретной задачей и стадией."
        ),
    )
    async def manager_activecollab_move_task(
        project_id: int,
        task_id: int,
        stage_name: str,
    ) -> dict[str, Any]:
        """Move a task to an explicitly approved workflow stage."""
        return await asyncio.to_thread(
            lambda: _activecollab_client().move_to_stage_name(project_id, task_id, stage_name),
        )
