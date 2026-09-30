"""Константы и env-настройки Jarvis.

Нижний слой пакета: не импортирует ничего из ``bot.*``, поэтому его можно
безопасно тянуть откуда угодно. Все остальные модули строятся над ним.
"""

from __future__ import annotations

import logging
import os
import re

from config import BASE_DIR
from engines import default_engine_name, get_engine_by_name


def int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def bool_env(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


# Движок per-topic: env JARVIS_ENGINE — дефолт для новых топиков; существующие
# топики хранят свой engine в БД и переключаются командой /engine.
DEFAULT_ENGINE_NAME = default_engine_name()
DEFAULT_ENGINE = get_engine_by_name(DEFAULT_ENGINE_NAME)

# Дефолтный cwd для топиков без явного /bind. Имя переменной историческое (CLAUDE_CWD),
# для обратной совместимости: задаёт дефолт для любого движка.
CLAUDE_CWD = os.environ.get("CLAUDE_CWD") or os.path.expanduser("~")

MSG_LIMIT = 3500           # порог отправки ответа как документ
TG_HARD_LIMIT = 4096       # жёсткий лимит Telegram
TG_FILE_LIMIT_MB = 50      # Telegram Bot API лимит на sendDocument

# Сеанс = окно терминала. Открывается первым сообщением, закрывается командой
# /close или сам — после SESSION_IDLE_MINUTES без активности в топике. Топик
# (cwd, движок, модель) переживает закрытие, контекст сессии — нет.
SESSION_IDLE_MINUTES = int_env("JARVIS_SESSION_IDLE_MINUTES", 180)
CONTEXT_WARN_TOKENS = int_env("JARVIS_CONTEXT_WARN_TOKENS", 150_000)
DONE_CONFIRM_ON_DONE = bool_env(
    "JARVIS_DONE_CONFIRM_ON_DONE",
    bool_env("JARVIS_AUTOCLOSE_ON_DONE", True),
)

# Маркер для отправки файлов из LLM-сессии: [[FILE: /abs/path]] или [[FILE: /path | подпись]].
# Должен стоять на отдельной строке (но допускаются пробелы вокруг).
FILE_MARKER_RE = re.compile(
    r"^[ \t]*\[\[FILE:\s*(?P<path>[^|\]\n]+?)(?:\s*\|\s*(?P<caption>[^\]\n]+?))?\s*\]\][ \t]*$",
    re.MULTILINE,
)

# Миниаппы досок QueueWarden: URL нужен для web_app-кнопки в личке (там разрешён
# только https-адрес), а короткое имя (direct link из BotFather) — для ссылки t.me
# в группах/форумах, где web_app-кнопки запрещены. Досок сколько угодно:
# BOARD_MINIAPPS=artsites|https://…/miniapp|qwboard,tako|https://…/miniapp|qwtako.
# Без списка — одна доска из BOARD_MINIAPP_URL/BOARD_MINIAPP_SHORT_NAME.
BOARD_MINIAPP_URL = os.environ.get("BOARD_MINIAPP_URL", "https://stage.queuewarden.ru/miniapp")
BOARD_MINIAPP_SHORT_NAME = os.environ.get("BOARD_MINIAPP_SHORT_NAME", "qwboard")


def parse_board_miniapps(raw: str | None) -> list[tuple[str, str, str]]:
    """``[(подпись, url, short_name)]`` из BOARD_MINIAPPS. Запись без https-адреса
    пропускается; short_name может быть пустым — тогда доска только в личке."""
    result: list[tuple[str, str, str]] = []
    for entry in (raw or "").split(","):
        parts = [p.strip() for p in entry.split("|")]
        if len(parts) < 2 or not parts[0]:
            continue
        label, url = parts[0], parts[1]
        short_name = parts[2] if len(parts) > 2 else ""
        if not url.startswith("https://"):
            logging.getLogger(__name__).warning(
                "BOARD_MINIAPPS: %r has no https url — skipped", label,
            )
            continue
        result.append((label, url, short_name))
    return result


BOARD_MINIAPPS = parse_board_miniapps(os.environ.get("BOARD_MINIAPPS")) or (
    [("", BOARD_MINIAPP_URL, BOARD_MINIAPP_SHORT_NAME)] if BOARD_MINIAPP_URL else []
)

DB_PATH = os.path.join(BASE_DIR, "bot_state.db")
MEDIA_DIR = os.path.join(BASE_DIR, "temp", "media")
os.makedirs(MEDIA_DIR, exist_ok=True)


def configure_logging() -> None:
    """Единый формат логов. Имя логгера в формат не входит, поэтому строки
    выглядят одинаково независимо от модуля, из которого пишут."""
    logging.basicConfig(
        format="[bot] %(asctime)s %(levelname)s %(message)s",
        level=logging.INFO,
    )


configure_logging()
