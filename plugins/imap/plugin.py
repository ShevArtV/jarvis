"""IMAP: нотисы Менеджеру о новых письмах. Таблица ``imap_state`` — UID уже
отправленных нотисов, чтобы не дублировать."""

from telegram.ext import Application

from bot.delivery import _send_manager_notice
from bot.plugins import Plugin
from plugins.imap.watcher import run_imap_watcher

SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS imap_state (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        account TEXT NOT NULL,
        uid INTEGER NOT NULL,
        seen_at TEXT NOT NULL,
        UNIQUE(account, uid)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_imap_state_account ON imap_state(account, uid)",
)


async def imap_worker(app: Application) -> None:
    async def notice(text: str, kind: str = "job_notification") -> None:
        await _send_manager_notice(app, text, kind)

    await run_imap_watcher(notice)


PLUGIN = Plugin(name="imap", workers=(imap_worker,), schema=SCHEMA)
