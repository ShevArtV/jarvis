"""Напоминания по расписанию: воркер бота и таблица ``reminders``.

MCP-тулы (``manager_remind_*``) — в ``mcp_tools.py``."""

from bot.plugins import Plugin
from plugins.reminders.worker import reminders_worker

# schedule парсится в parse_reminder_schedule(): daily HH:MM, weekday HH:MM,
# weekend HH:MM, weekly DAY[,DAY] HH:MM, monthly D HH:MM, once YYYY-MM-DD HH:MM
# (все времена в JARVIS_REMINDERS_TZ). next_fire_at хранится в UTC ISO.
SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS reminders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER NOT NULL,
        thread_id INTEGER NOT NULL,
        text TEXT NOT NULL,
        schedule TEXT NOT NULL,
        next_fire_at TEXT NOT NULL,
        last_fired_at TEXT,
        enabled INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_reminders_due ON reminders(enabled, next_fire_at)",
)

PLUGIN = Plugin(name="reminders", workers=(reminders_worker,), schema=SCHEMA)
