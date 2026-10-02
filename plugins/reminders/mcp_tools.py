"""MCP-тулы напоминаний Секретаря. Регистрируются через register(mcp), если
плагин включён в JARVIS_PLUGINS."""

from __future__ import annotations

from typing import Any

from bot.timeutil import utcnow
from mcp_server import common
from mcp_server.common import logger


def register(mcp) -> None:
    """Зарегистрировать тулы напоминаний на сервере FastMCP."""

    @mcp.tool(
        name="manager_remind_add",
        description=(
            "Создаёт напоминание для Секретаря (старое имя manager_* сохранено "
            "для совместимости). Формат schedule (простой текст):\n"
            "  daily HH:MM              - каждый день\n"
            "  weekday HH:MM            - Пн-Пт\n"
            "  weekend HH:MM            - Сб-Вс\n"
            "  weekly DAY[,DAY,...] HH:MM   (DAY: mon|tue|wed|thu|fri|sat|sun)\n"
            "  monthly D HH:MM          - конкретное число (1..28)\n"
            "  once YYYY-MM-DD HH:MM    - one-time\n"
            "Все времена в Europe/Moscow (можно переопределить через "
            "JARVIS_REMINDERS_TZ). В назначенное время бот шлёт в топик "
            "Секретаря '🔔 Напоминание #N: <text>' и активирует Секретаря "
            "через auto-kick. Возвращает id, next_fire_at."
        ),
    )
    def manager_remind_add(
        text: str,
        schedule: str,
        thread_id: int | None = None,
        chat_id: int | None = None,
    ) -> dict[str, Any]:
        """Add a reminder."""
        text = text.strip()
        if not text:
            raise ValueError("text is required")
        if not schedule.strip():
            raise ValueError("schedule is required")
        target_chat_id = chat_id if chat_id is not None else common._default_chat_id()
        if thread_id is None:
            thread_id = common._manager_thread_id()

        # Парсим schedule и считаем next_fire_at через python из bot-модуля
        # (там же логика TZ). Импортируем lazy.
        from plugins.reminders.schedule import compute_next_fire, parse_reminder_schedule  # type: ignore

        parsed = parse_reminder_schedule(schedule)
        next_fire = compute_next_fire(parsed)
        if next_fire is None:
            raise ValueError(
                f"schedule {schedule!r} resolves to a past moment "
                "(once-reminder with past date?)"
            )

        now = utcnow().isoformat()
        with common._connect() as conn:
            cur = conn.execute(
                "INSERT INTO reminders(chat_id, thread_id, text, schedule, "
                "next_fire_at, enabled, created_at) VALUES (?, ?, ?, ?, ?, 1, ?)",
                (
                    target_chat_id, thread_id, text, schedule,
                    next_fire.isoformat(), now,
                ),
            )
            rid = cur.lastrowid
        logger.info(
            "reminder created id=%s schedule=%r next=%s",
            rid, schedule, next_fire.isoformat(),
        )
        return {
            "id": rid,
            "chat_id": target_chat_id,
            "thread_id": thread_id,
            "text": text,
            "schedule": schedule,
            "next_fire_at": next_fire.isoformat(),
            "enabled": True,
        }


    @mcp.tool(
        name="manager_remind_list",
        description=(
            "Список напоминаний. По умолчанию только enabled=true; задай "
            "only_enabled=false чтобы увидеть и отключённые (выполненные once, "
            "или те, которые ты отключил через manager_remind_toggle)."
        ),
    )
    def manager_remind_list(
        only_enabled: bool = True,
        chat_id: int | None = None,
    ) -> dict[str, Any]:
        """List reminders."""
        target_chat_id = chat_id if chat_id is not None else common._default_chat_id()
        sql = (
            "SELECT id, chat_id, thread_id, text, schedule, next_fire_at, "
            "last_fired_at, enabled, created_at FROM reminders "
            "WHERE chat_id = ?"
        )
        args: list[Any] = [target_chat_id]
        if only_enabled:
            sql += " AND enabled = 1"
        sql += " ORDER BY next_fire_at ASC"
        with common._connect() as conn:
            rows = conn.execute(sql, args).fetchall()
        reminders = []
        for r in rows:
            reminders.append({
                "id": r["id"],
                "chat_id": r["chat_id"],
                "thread_id": r["thread_id"],
                "text": r["text"],
                "schedule": r["schedule"],
                "next_fire_at": r["next_fire_at"],
                "last_fired_at": r["last_fired_at"],
                "enabled": bool(r["enabled"]),
                "created_at": r["created_at"],
            })
        return {"count": len(reminders), "reminders": reminders}

    @mcp.tool(
        name="manager_remind_delete",
        description="Удаляет напоминание по id. Возвращает deleted=true/false.",
    )
    def manager_remind_delete(reminder_id: int) -> dict[str, Any]:
        """Delete a reminder by id."""
        with common._connect() as conn:
            cur = conn.execute(
                "DELETE FROM reminders WHERE id = ?", (reminder_id,),
            )
        return {"reminder_id": reminder_id, "deleted": cur.rowcount == 1}


    @mcp.tool(
        name="manager_remind_toggle",
        description=(
            "Включает/выключает напоминание. enabled=false — не сработает, "
            "но запись остаётся (полезно для временной паузы). enabled=true — "
            "пересчитывает next_fire_at от текущего момента."
        ),
    )
    def manager_remind_toggle(reminder_id: int, enabled: bool) -> dict[str, Any]:
        """Toggle a reminder on/off."""
        with common._connect() as conn:
            row = conn.execute(
                "SELECT schedule FROM reminders WHERE id = ?", (reminder_id,),
            ).fetchone()
        if row is None:
            raise RuntimeError(f"reminder {reminder_id} not found")
        if enabled:
            from plugins.reminders.schedule import compute_next_fire, parse_reminder_schedule  # type: ignore
            try:
                parsed = parse_reminder_schedule(row["schedule"])
                next_fire = compute_next_fire(parsed)
            except Exception as exc:
                raise RuntimeError(
                    f"cannot re-enable: schedule unparseable / past: {exc}"
                ) from exc
            if next_fire is None:
                raise RuntimeError(
                    "cannot re-enable: schedule resolves to past (once-reminder?)"
                )
            with common._connect() as conn:
                conn.execute(
                    "UPDATE reminders SET enabled = 1, next_fire_at = ? WHERE id = ?",
                    (next_fire.isoformat(), reminder_id),
                )
            return {
                "reminder_id": reminder_id, "enabled": True,
                "next_fire_at": next_fire.isoformat(),
            }
        else:
            with common._connect() as conn:
                conn.execute(
                    "UPDATE reminders SET enabled = 0 WHERE id = ?", (reminder_id,),
                )
            return {"reminder_id": reminder_id, "enabled": False}
