"""Входящий вебхук (Битрикс24 и др.): событие → нотис Менеджеру.
Таблица ``webhook_log`` — журнал принятых событий."""

from telegram.ext import Application

from bot.delivery import _send_manager_notice
from bot.plugins import Plugin
from plugins.webhook.server import run_webhook_server

SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS webhook_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL,
        event TEXT NOT NULL,
        payload TEXT NOT NULL,
        received_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_webhook_log_received ON webhook_log(source, received_at)",
)


async def webhook_worker(app: Application) -> None:
    async def notice(text: str, kind: str = "job_notification") -> None:
        await _send_manager_notice(app, text, kind)

    await run_webhook_server(notice)


PLUGIN = Plugin(name="webhook", workers=(webhook_worker,), schema=SCHEMA)
