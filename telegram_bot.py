#!/usr/bin/env python3
"""Jarvis — тонкая Telegram-обёртка над LLM-CLI (claude, codex или opencode).

Модель: «один топик Telegram = одно рабочее место», внутри которого живут
сеансы — как окна терминала.

- Ключ топика — (chat_id, message_thread_id). В не-форумных чатах thread_id=0.
- Каждый топик может быть привязан к своей рабочей директории (cwd) командой /bind.
- Внутри ключа вызовы сериализуются через asyncio.Lock; разные ключи работают
  параллельно. Топик с ``/persistent`` — исключение: сообщение дописывается в
  живой процесс, не ожидая лока.
- Используется stream-json: промежуточные шаги (tool_use/exec, рассуждения)
  видны пользователю и остаются в топике журналом хода.
- Движок выбирается per-topic: env JARVIS_ENGINE задаёт дефолт для новых топиков,
  команда /engine — переключает движок текущего топика.

Сам код живёт в пакете ``bot/`` (карта — в README, раздел «Файлы»); здесь только
точка входа. До 2026-07-25 этот файл был монолитом на 4924 строки.
"""

import logging

from bot.app import build_application
from bot.db import init_db
from bot.settings import CLAUDE_CWD, DEFAULT_ENGINE, DEFAULT_ENGINE_NAME
from config import ALLOWED_USER_IDS, TELEGRAM_TOKEN
from engines import ensure_engine_tools

logger = logging.getLogger(__name__)


def main() -> None:
    print(f"=== Jarvis Telegram Bot (per-topic engine, default={DEFAULT_ENGINE_NAME}) ===")

    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN не задан. См. .env.example.")
    if not ALLOWED_USER_IDS:
        logger.warning("ALLOWED_USER_IDS пуст — никто не сможет писать боту.")

    init_db()
    mcp_ok, mcp_status = ensure_engine_tools(DEFAULT_ENGINE)
    if mcp_ok:
        logger.info("Default engine tools ready: %s", mcp_status)
    else:
        logger.warning("Default engine tools are not fully ready: %s", mcp_status)

    app = build_application()

    logger.info("Whitelisted user_ids: %s", sorted(ALLOWED_USER_IDS))
    logger.info("Default engine: %s  default cwd=%s", DEFAULT_ENGINE_NAME, CLAUDE_CWD)
    print(f"Бот запущен (default engine={DEFAULT_ENGINE_NAME}). Жду сообщения в Telegram...")
    app.run_polling()


if __name__ == "__main__":
    main()
