import os

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

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
