import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# JARVIS_DOTENV=0 выключает чтение .env: тесты обязаны видеть чистое окружение,
# а не настройки того, кто их запустил (см. tests/__init__.py).
if os.environ.get("JARVIS_DOTENV", "1") != "0":
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(BASE_DIR, ".env"))
    except ImportError:
        pass

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")


def _parse_user_ids(raw: str) -> set[int]:
    if not raw:
        return set()
    result = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            result.add(int(part))
        except ValueError:
            pass
    return result


ALLOWED_USER_IDS: set[int] = _parse_user_ids(os.environ.get("ALLOWED_USER_IDS", ""))
