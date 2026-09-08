"""Сборка приложения: регистрация хендлеров и запуск фоновых задач.

``build_application`` намеренно отделён от ``main``: тест-снапшот проводки
строит приложение с фейковым токеном и проверяет, что каждая команда и каждый
callback-паттерн ведут в ту же функцию (``tests/test_wiring_snapshot.py``).
Порядок регистрации значим — PTB перебирает хендлеры по порядку, поэтому
``unauthorized_handler`` обязан оставаться последним.
"""

from __future__ import annotations

import asyncio
import logging

from telegram import (
    BotCommand,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    MenuButtonCommands,
)
from telegram.ext import (
    AIORateLimiter,
    Application,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    filters,
)

from config import ALLOWED_USER_IDS, TELEGRAM_TOKEN
from engines import prewarm_models
from imap_watcher import run_imap_watcher
from webhook_server import run_webhook_server

from bot.asks import on_ask_answer
from bot.delivery import _send_manager_notice
from bot.handlers.commands import (
    cmd_bind,
    cmd_board,
    cmd_close,
    cmd_reset,
    cmd_session,
    cmd_spawn,
    cmd_start,
    cmd_stop,
    cmd_tokens,
    cmd_throttle,
    cmd_unbind,
    cmd_usage,
    cmd_where,
    on_throttle_action,
    unauthorized_handler,
)
from bot.handlers.engine import (
    cmd_engine,
    on_engine_carry,
    on_engine_select,
    on_model_select,
)
from bot.handlers.messages import (
    handle_document,
    handle_photo,
    handle_text,
    on_cancel_queue,
)
from bot.handlers.toggles import (
    cmd_browser,
    cmd_persistent,
    on_browser_toggle,
    on_done_confirm,
    on_persistent_toggle,
)
from bot.workers import (
    agent_triggers_worker,
    cleanup_worker,
    close_requests_worker,
    health_worker,
    jobs_worker,
    persistent_reaper,
    reminders_worker,
)

logger = logging.getLogger(__name__)

# Команды, выводимые в нативное меню Telegram (синяя кнопка слева от поля ввода).
# Описания короткие — Telegram обрезает длинные.
BOT_COMMANDS: list[BotCommand] = [
    BotCommand("engine", "движок: показать/переключить (claude|codex|opencode)"),
    BotCommand("close", "закрыть сеанс (контекст сбрасывается)"),
    BotCommand("new", "закрыть сеанс и сразу открыть новый"),
    BotCommand("session", "session-id, cwd, движок и состояние сеанса"),
    BotCommand("tokens", "оценка размера текущей сессии"),
    BotCommand("usage", "остаток лимитов подписки claude и codex"),
    BotCommand("throttle", "очередь отложенных автотриггеров"),
    BotCommand("stop", "прервать текущий запрос"),
    BotCommand("spawn", "одноразовая параллельная сессия — /spawn <prompt>"),
    BotCommand("bind", "привязать топик к каталогу — /bind <abs path>"),
    BotCommand("unbind", "снять привязку cwd, вернуть дефолт"),
    BotCommand("where", "показать эффективный cwd"),
    BotCommand("board", "открыть доску QueueWarden (миниапп)"),
    BotCommand("persistent", "живой процесс claude/codex: сообщения на лету"),
    BotCommand("start", "приветствие и состояние топика"),
]

async def _post_init(application: Application) -> None:
    """Регистрируем команды для всех контекстов (default + private + group)
    и явно ставим MenuButtonCommands — иначе в форум-группах нативная кнопка
    меню часто не появляется без явной настройки."""
    bot = application.bot
    scopes = [
        ("default", None),
        ("all_private_chats", BotCommandScopeAllPrivateChats()),
        ("all_group_chats", BotCommandScopeAllGroupChats()),
    ]
    for label, scope in scopes:
        try:
            if scope is None:
                await bot.set_my_commands(BOT_COMMANDS)
            else:
                await bot.set_my_commands(BOT_COMMANDS, scope=scope)
            logger.info("bot commands registered for scope=%s (%d entries)",
                        label, len(BOT_COMMANDS))
        except Exception:
            logger.exception("set_my_commands failed for scope=%s", label)
    try:
        await bot.set_chat_menu_button(menu_button=MenuButtonCommands())
        logger.info("default menu button set to MenuButtonCommands")
    except Exception:
        logger.exception("set_chat_menu_button failed (меню не критично)")

    # Списки моделей движков спрашиваются у их CLI (engines/model_cache.py).
    # Прогреваем в потоке, чтобы первый /engine не ждал `opencode models`.
    application.bot_data["models_prewarm_task"] = asyncio.create_task(
        asyncio.to_thread(prewarm_models)
    )

    # Запускаем worker для очереди jobs (delegations from Manager via MCP).
    # Хранить ссылку в bot_data на случай нужды в shutdown'е/тестах.
    task = asyncio.create_task(jobs_worker(application))
    application.bot_data["jobs_worker_task"] = task

    # Non-job external triggers (внешние интеграции): обычный LLM turn в топике
    # без job_id, health_worker и safety-notice Менеджеру.
    trigger_task = asyncio.create_task(agent_triggers_worker(application))
    application.bot_data["agent_triggers_worker_task"] = trigger_task

    # Гигиена: hourly cleanup старых записей messages_log + завершённых jobs.
    # TTL — env JARVIS_LOG_TTL_DAYS (дефолт 30, 0/none/off отключает).
    cleanup_task = asyncio.create_task(cleanup_worker(application))
    application.bot_data["cleanup_worker_task"] = cleanup_task

    # /persistent: убивает простаивающие живые процессы claude.
    persistent_reaper_task = asyncio.create_task(persistent_reaper(application))
    application.bot_data["persistent_reaper_task"] = persistent_reaper_task

    # Закрытия сеансов, запрошенные Менеджером через manager_close_session:
    # MCP помечает строку в sessions, добивает процессы топика бот.
    close_requests_task = asyncio.create_task(close_requests_worker(application))
    application.bot_data["close_requests_worker_task"] = close_requests_task

    # Health: следит за долгими in_progress jobs, шлёт Менеджеру нотисы.
    # Параметры в env JARVIS_HEARTBEAT_INTERVAL/WARN/FAIL (300/900/3600с).
    health_task = asyncio.create_task(health_worker(application))
    application.bot_data["health_worker_task"] = health_task

    # Reminders: cron-light напоминания для Менеджера.
    reminders_task = asyncio.create_task(reminders_worker(application))
    application.bot_data["reminders_worker_task"] = reminders_task

    # Общий callback для отправки нотисов в топик Менеджера.
    async def _notice(text: str, kind: str = "job_notification") -> None:
        await _send_manager_notice(application, text, kind)

    # Webhook-сервер для входящих событий Битрикс24.
    webhook_task = asyncio.create_task(run_webhook_server(_notice))
    application.bot_data["webhook_task"] = webhook_task

    # IMAP-поллер для новых писем.
    imap_task = asyncio.create_task(run_imap_watcher(_notice))
    application.bot_data["imap_task"] = imap_task


def build_application(
    token: str | None = None,
    allowed_user_ids: set[int] | None = None,
) -> Application:
    """Собрать Application со всеми хендлерами.

    Вынесено из main() ради теста-снапшота регистраций: он строит приложение с
    фейковым токеном и сверяет, что каждая команда и каждый callback-паттерн
    по-прежнему ведут в ту же функцию. Сети не требует.
    """
    # concurrent_updates=True: без этого PTB обрабатывает апдейты последовательно,
    # и per-key asyncio.Lock не даёт параллельности между разными топиками —
    # второй топик ждёт, пока освободится воркер PTB, а не сам lock.
    # rate_limiter: Telegram троттлит отправку в один чат (~20 сообщений в
    # минуту) и отвечает RetryAfter. Без лимитера PTB просто пробрасывал ошибку
    # наверх — и ответ агента терялся молча. AIORateLimiter выдерживает паузы
    # сам и повторяет запрос, а не роняет его.
    app = (
        Application.builder()
        .token(token if token is not None else TELEGRAM_TOKEN)
        .concurrent_updates(True)
        .rate_limiter(AIORateLimiter(max_retries=3))
        .post_init(_post_init)
        .build()
    )

    allowed = filters.User(
        user_id=allowed_user_ids if allowed_user_ids is not None else ALLOWED_USER_IDS
    )

    app.add_handler(CommandHandler("start", cmd_start, filters=allowed))
    app.add_handler(CommandHandler("new", cmd_reset, filters=allowed))
    app.add_handler(CommandHandler("reset", cmd_reset, filters=allowed))
    app.add_handler(CommandHandler("stop", cmd_stop, filters=allowed))
    app.add_handler(CommandHandler("spawn", cmd_spawn, filters=allowed))
    app.add_handler(CommandHandler("session", cmd_session, filters=allowed))
    app.add_handler(CommandHandler("tokens", cmd_tokens, filters=allowed))
    app.add_handler(CommandHandler("usage", cmd_usage, filters=allowed))
    app.add_handler(CommandHandler("throttle", cmd_throttle, filters=allowed))
    app.add_handler(CommandHandler("close", cmd_close, filters=allowed))
    app.add_handler(CommandHandler("engine", cmd_engine, filters=allowed))
    app.add_handler(CommandHandler("browser", cmd_browser, filters=allowed))
    app.add_handler(CommandHandler("persistent", cmd_persistent, filters=allowed))
    app.add_handler(CommandHandler("bind", cmd_bind, filters=allowed))
    app.add_handler(CommandHandler("unbind", cmd_unbind, filters=allowed))
    app.add_handler(CommandHandler("where", cmd_where, filters=allowed))
    app.add_handler(CommandHandler("board", cmd_board, filters=allowed))

    app.add_handler(MessageHandler(allowed & filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(allowed & filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(allowed & filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(on_cancel_queue, pattern=r"^cancel_queue:"))
    app.add_handler(CallbackQueryHandler(on_throttle_action, pattern=r"^throttle_(now|cancel):"))
    app.add_handler(CallbackQueryHandler(on_engine_select, pattern=r"^engine_select:"))
    app.add_handler(CallbackQueryHandler(on_model_select, pattern=r"^model_select:"))
    app.add_handler(CallbackQueryHandler(on_engine_carry, pattern=r"^engine_carry:"))
    app.add_handler(CallbackQueryHandler(on_ask_answer, pattern=r"^ask:"))
    app.add_handler(CallbackQueryHandler(on_browser_toggle, pattern=r"^browser_toggle:"))
    app.add_handler(CallbackQueryHandler(on_persistent_toggle, pattern=r"^persistent_toggle:"))
    app.add_handler(CallbackQueryHandler(on_done_confirm, pattern=r"^done_confirm:"))

    app.add_handler(MessageHandler(~allowed, unauthorized_handler))

    return app
