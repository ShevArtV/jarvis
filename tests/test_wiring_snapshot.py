"""Страховочная сеть рефакторинга: снапшот проводки бота.

`telegram_bot.py` был монолитом на 4924 строки, и юнит-тестов на подавляющую
часть его кода нет. Разбирать такой файл на пакет `bot/` без сети — значит
двигать код вслепую: потерянный хендлер или забытая функция не упадут ни на
одном тесте, а проявятся в Telegram отказом одной команды.

Эти два теста дают минимальную, но точную гарантию для ПЕРЕМЕЩЕНИЙ:

* `HandlerWiringTest` — каждая команда и каждый callback-паттерн по-прежнему
  ведут в функцию с тем же именем. Ловит потерянную регистрацию, разъехавшийся
  паттерн, перепутанные местами колбэки.
* `PublicSurfaceTest` — карта «модуль → имена»: ничего не потерялось при
  переносе, и зафиксировано, где что живёт. Изначально это был плоский список
  имён на `telegram_bot`; после разъезда монолита по пакету он стал картой.

Эталоны снимались с монолита ДО первого переноса (2026-07-25). Менять их в
одном коммите с переездом кода нельзя — это ровно та ошибка, от которой они
защищают. Меняем осознанно и отдельно, когда проводка правда должна измениться.
"""

from __future__ import annotations

import importlib
import pathlib
import unittest

import telegram_bot


# (group, тип хендлера, команда или regex, имя колбэка). Порядок значим:
# PTB проверяет хендлеры в порядке регистрации, поэтому перестановка
# unauthorized_handler выше остальных отключила бы бота целиком.
EXPECTED_HANDLERS = [
    (0, 'CommandHandler', 'start', 'cmd_start'),
    (0, 'CommandHandler', 'new', 'cmd_reset'),
    (0, 'CommandHandler', 'reset', 'cmd_reset'),
    (0, 'CommandHandler', 'stop', 'cmd_stop'),
    (0, 'CommandHandler', 'spawn', 'cmd_spawn'),
    (0, 'CommandHandler', 'session', 'cmd_session'),
    (0, 'CommandHandler', 'tokens', 'cmd_tokens'),
    (0, 'CommandHandler', 'usage', 'cmd_usage'),
    (0, 'CommandHandler', 'close', 'cmd_close'),
    (0, 'CommandHandler', 'engine', 'cmd_engine'),
    (0, 'CommandHandler', 'browser', 'cmd_browser'),
    (0, 'CommandHandler', 'persistent', 'cmd_persistent'),
    (0, 'CommandHandler', 'bind', 'cmd_bind'),
    (0, 'CommandHandler', 'unbind', 'cmd_unbind'),
    (0, 'CommandHandler', 'where', 'cmd_where'),
    (0, 'CommandHandler', 'board', 'cmd_board'),
    (0, 'MessageHandler', '-', 'handle_photo'),
    (0, 'MessageHandler', '-', 'handle_document'),
    (0, 'MessageHandler', '-', 'handle_text'),
    (0, 'CallbackQueryHandler', '^cancel_queue:', 'on_cancel_queue'),
    (0, 'CallbackQueryHandler', '^engine_select:', 'on_engine_select'),
    (0, 'CallbackQueryHandler', '^model_select:', 'on_model_select'),
    (0, 'CallbackQueryHandler', '^engine_carry:', 'on_engine_carry'),
    (0, 'CallbackQueryHandler', '^ask:', 'on_ask_answer'),
    (0, 'CallbackQueryHandler', '^browser_toggle:', 'on_browser_toggle'),
    (0, 'CallbackQueryHandler', '^persistent_toggle:', 'on_persistent_toggle'),
    (0, 'CallbackQueryHandler', '^done_confirm:', 'on_done_confirm'),
    (0, 'MessageHandler', '-', 'unauthorized_handler'),
]

# Карта «модуль → имена, которые он обязан предоставлять». Заменила плоский
# список имён на `telegram_bot` после того, как монолит разъехался по пакету:
# сам факт «ничего не потеряно при переносе» проверяется по-прежнему, но теперь
# ещё и фиксирует, ГДЕ что живёт. Внешние потребители: MCP-сервер импортирует
# bot.reminders, остальное — тесты и bot.app.
EXPECTED_LAYOUT = {
    "bot.settings": [
        "CLAUDE_CWD", "CONTEXT_WARN_TOKENS", "DEFAULT_ENGINE", "DEFAULT_ENGINE_NAME",
        "DONE_CONFIRM_ON_DONE", "FILE_MARKER_RE", "MEDIA_DIR", "MSG_LIMIT",
        "SESSION_IDLE_MINUTES", "TG_FILE_LIMIT_MB", "TG_HARD_LIMIT",
        "DB_PATH", "int_env", "bool_env",
    ],
    "bot.db": ["DB_PATH", "init_db", "log_message", "_db"],
    "bot.queues": [
        "claim_next_job", "finish_job", "claim_next_agent_trigger",
        "finish_agent_trigger", "cleanup_old_log_entries",
    ],
    "bot.topics": [
        "_key", "_lock_for", "resolve_manager_topic", "resolve_topic_role",
        "save_message_context", "load_message_context",
        "_kill_persistent_worker",
        "chat_locks", "active_procs", "spawn_procs", "persistent_workers",
        "pending_queue",
    ],
    "bot.formatting": ["md_to_html", "split_html_for_telegram", "_html_escape"],
    "bot.sessions": [
        "get_session", "reset_session", "update_session_id", "close_session",
        "touch_session", "mark_session_start", "ensure_active_session",
        "clear_close_request", "set_engine", "get_model", "update_model_only",
        "get_mcp_playwright", "set_mcp_playwright", "get_persistent_for_engine",
        "set_persistent_for_engine", "update_actual_model", "get_actual_model",
        "set_cwd", "clear_cwd", "set_pending_summary", "get_pending_summary",
        "clear_pending_summary", "build_context_handoff", "INSTRUCTION_FILES",
    ],
    "bot.asks": ["get_pending_ask", "answer_ask", "ask_question_text", "on_ask_answer"],
    "bot.delivery": [
        "send_to_topic", "send_document_to_topic", "send_claude_reply",
        "extract_file_markers", "deliver_file_markers", "ProgressJournal",
        "_send_manager_notice",
    ],
    "bot.llm": ["build_system_prefix", "call_llm_stream"],
    # Импортируется MCP-сервером — ломать адрес нельзя.
    "bot.reminders": ["parse_reminder_schedule", "compute_next_fire"],
    "bot.jobs": ["_run_manager_job", "_run_spawn", "_process_agent_trigger"],
    "bot.workers": [
        "cleanup_worker", "reminders_worker", "persistent_reaper",
        "close_requests_worker", "health_worker", "jobs_worker",
        "agent_triggers_worker", "_apply_close_request",
    ],
    "bot.app": ["build_application", "_post_init", "BOT_COMMANDS"],
    "telegram_bot": ["main"],
}

# Разделяемые реестры живых объектов: делятся по ссылке, поэтому обязаны быть
# ровно одним объектом на процесс.
SHARED_STATE = [
    "chat_locks", "active_procs", "spawn_procs", "persistent_workers",
    "pending_queue",
]


def _wiring() -> list[tuple[int, str, str, str]]:
    app = telegram_bot.build_application(
        token="123456:fake-token-for-tests", allowed_user_ids={1},
    )
    rows: list[tuple[int, str, str, str]] = []
    for group, handlers in sorted(app.handlers.items()):
        for handler in handlers:
            commands = getattr(handler, "commands", None)
            pattern = getattr(getattr(handler, "pattern", None), "pattern", None)
            key = ",".join(sorted(commands)) if commands else (pattern or "-")
            rows.append((group, type(handler).__name__, key, handler.callback.__name__))
    return rows


class HandlerWiringTest(unittest.TestCase):
    def test_every_command_and_callback_routes_to_the_same_function(self) -> None:
        self.assertEqual(_wiring(), EXPECTED_HANDLERS)

    def test_unauthorized_handler_stays_last(self) -> None:
        """Он ловит всё, что не прошло whitelist. Уехав вверх по списку, он
        начал бы перехватывать сообщения разрешённых пользователей."""
        self.assertEqual(_wiring()[-1][3], "unauthorized_handler")

    def test_application_builds_without_network(self) -> None:
        app = telegram_bot.build_application(token="123456:fake", allowed_user_ids={1})
        self.assertTrue(app.handlers[0])


class PublicSurfaceTest(unittest.TestCase):
    def test_every_module_provides_its_names(self) -> None:
        missing: list[str] = []
        for module_name, names in EXPECTED_LAYOUT.items():
            module = importlib.import_module(module_name)
            missing += [f"{module_name}.{n}" for n in names if not hasattr(module, n)]
        self.assertEqual(missing, [], f"пропало при переносе: {missing}")

    def test_entrypoint_stays_thin(self) -> None:
        """telegram_bot.py — точка входа, а не место для кода: он был монолитом
        на 4924 строки, и разъезжаться обратно ему незачем."""
        path = pathlib.Path(telegram_bot.__file__)
        self.assertLess(len(path.read_text(encoding="utf-8").split("\n")), 120)

    def test_shared_state_containers_are_mutable_singletons(self) -> None:
        """Состояние делится по ссылке. Если контейнер подменят на новый объект
        (например пересоздадут при импорте), топики разойдутся по разным
        словарям и per-topic локи перестанут защищать."""
        topics = importlib.import_module("bot.topics")
        for name in SHARED_STATE:
            with self.subTest(name=name):
                first = getattr(topics, name)
                self.assertIsInstance(first, dict)
                self.assertIs(first, getattr(topics, name))


if __name__ == "__main__":
    unittest.main()
