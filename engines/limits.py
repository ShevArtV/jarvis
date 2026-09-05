"""Остаток лимитов подписки claude/codex/opencode.

Цифры берутся там же, где их берут сами CLI для своих ``/usage`` и ``/status``:
живым запросом в API с уже сохранённым на диске OAuth-токеном. Локальные файлы
(кэш ``~/.claude.json``, rollout'ы codex) остались **фолбэком** на случай
недоступной сети или протухшего токена — они отражают момент последнего запроса
CLI и легко показывают позавчерашние цифры, поэтому такой ответ помечается.

Стиль — как в engines/session_usage.py: dataclass'ы, никаких исключений
наружу, любая ошибка чтения превращается в поле ``note``."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from engines.session_usage import _codex_sessions_root

HTTP_TIMEOUT = 15.0

CLAUDE_USAGE_URL = "https://api.anthropic.com/api/oauth/usage"
CODEX_USAGE_URL = "https://chatgpt.com/backend-api/codex/usage"


@dataclass
class LimitWindow:
    name: str  # человекочитаемое: "5 часов", "7 дней", "неделя (opus)"
    used_percent: float | None
    resets_at: datetime | None = None  # timezone-aware UTC, если известно
    note: str | None = None


@dataclass
class EngineLimits:
    engine: str
    windows: list[LimitWindow] = field(default_factory=list)
    source: str | None = None  # путь к файлу-источнику
    fetched_at: datetime | None = None  # когда данные были собраны CLI (для кэша claude)
    note: str | None = None  # если данных нет — причина
    live: bool = False  # True — свежий ответ API, False — локальный фолбэк


def _fetch_json(url: str, headers: dict[str, str]) -> tuple[Any, str | None]:
    """GET с готовыми заголовками. Возвращает ``(payload, error)`` — ровно одно
    из двух непусто. Наружу не бросает ничего."""
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            return json.loads(resp.read().decode("utf-8")), None
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403):
            return None, f"API отклонил токен (HTTP {exc.code})"
        return None, f"API ответил HTTP {exc.code}"
    except urllib.error.URLError as exc:
        return None, f"сеть недоступна: {exc.reason}"
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None, "API вернул не-JSON"
    except Exception as exc:  # noqa: BLE001 — команда статуса не должна падать
        return None, f"{type(exc).__name__}: {exc}"


def _claude_config_path() -> Path:
    return Path(os.environ.get("CLAUDE_CONFIG_JSON", Path.home() / ".claude.json"))


def _claude_credentials_path() -> Path:
    return Path(
        os.environ.get("CLAUDE_CREDENTIALS_JSON", Path.home() / ".claude" / ".credentials.json")
    )


CLAUDE_WINDOW_NAMES = (
    ("five_hour", "5 часов"),
    ("seven_day", "7 дней"),
    ("seven_day_opus", "7 дней · opus"),
    ("seven_day_sonnet", "7 дней · sonnet"),
)


def _claude_windows(source: dict[str, Any]) -> list[LimitWindow]:
    """Окна из ответа /api/oauth/usage или из кэша — форма у них одинаковая."""
    windows: list[LimitWindow] = []
    for key, name in CLAUDE_WINDOW_NAMES:
        window = source.get(key)
        if not isinstance(window, dict):
            continue
        windows.append(
            LimitWindow(
                name=name,
                used_percent=_as_float(window.get("utilization")),
                resets_at=_parse_iso(window.get("resets_at")),
            )
        )
    return windows


def claude_limits() -> EngineLimits:
    """Остаток лимитов claude — тем же запросом, что делает TUI-команда
    ``/usage``: GET /api/oauth/usage с OAuth-токеном из
    ``~/.claude/.credentials.json``. Не вышло — читаем кэш последнего запроса
    самого CLI (``~/.claude.json`` → ``cachedUsageUtilization``)."""
    result = EngineLimits(engine="claude")
    token, token_error = _claude_token()
    if token:
        payload, error = _fetch_json(
            CLAUDE_USAGE_URL,
            {
                "Authorization": f"Bearer {token}",
                "anthropic-beta": "oauth-2025-04-20",
                "User-Agent": "claude-cli (jarvis)",
                "Accept": "application/json",
            },
        )
        if isinstance(payload, dict):
            result.windows = _claude_windows(payload)
            result.live = True
            result.source = CLAUDE_USAGE_URL
            result.fetched_at = datetime.now(timezone.utc)
            if not result.windows:
                result.note = "API ответил, но знакомых окон в ответе нет"
            return result
        token_error = error
    fallback = _claude_limits_from_cache()
    fallback.note = "; ".join(x for x in (token_error, fallback.note) if x) or None
    return fallback


def _claude_token() -> tuple[str | None, str | None]:
    path = _claude_credentials_path()
    if not path.is_file():
        return None, f"{path} не найден"
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"не удалось прочитать {path}: {exc}"
    oauth = data.get("claudeAiOauth")
    token = oauth.get("accessToken") if isinstance(oauth, dict) else None
    if not isinstance(token, str) or not token:
        return None, "в credentials нет claudeAiOauth.accessToken"
    return token, None


def _claude_limits_from_cache() -> EngineLimits:
    """Фолбэк: кэш последнего запроса самого CLI. Цифры могут быть старыми —
    вызывающий обязан показать `fetched_at`."""
    path = _claude_config_path()
    result = EngineLimits(engine="claude", source=str(path))
    if not path.is_file():
        result.note = "файл конфигурации claude не найден"
        return result
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        result.note = f"не удалось прочитать {path}: {exc}"
        return result

    cached = data.get("cachedUsageUtilization")
    if not isinstance(cached, dict):
        result.note = "ключ cachedUsageUtilization отсутствует в конфиге"
        return result

    fetched_at_ms = cached.get("fetchedAtMs")
    if isinstance(fetched_at_ms, (int, float)):
        result.fetched_at = datetime.fromtimestamp(fetched_at_ms / 1000, tz=timezone.utc)

    utilization = cached.get("utilization")
    if not isinstance(utilization, dict):
        result.note = "ключ utilization отсутствует в cachedUsageUtilization"
        return result

    result.windows = _claude_windows(utilization)
    if not result.windows:
        result.note = "в utilization нет ни одного известного окна"
    return result


def _find_key(obj: Any, key: str) -> Any:
    """Рекурсивно найти первое значение по ключу ``key`` в dict/list."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for value in obj.values():
            found = _find_key(value, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _find_key(item, key)
            if found is not None:
                return found
    return None


def _window_name(window_minutes: Any) -> str:
    try:
        minutes = int(window_minutes)
    except (TypeError, ValueError):
        return "окно"
    if minutes < 60:
        return f"{minutes} минут"
    if minutes < 1440:
        return f"{minutes // 60} часов"
    return f"{minutes // 1440} дней"


def _iter_recent_rollouts(root: Path, limit: int):
    """Обойти дерево сессий codex, отдавая rollout-*.jsonl в порядке убывания
    имени каталога (для схемы ~/.codex/sessions/YYYY/MM/DD это совпадает с
    порядком убывания даты) — самые свежие файлы проверяются первыми и не
    тонут в старых при обходе большого дерева. Не более ``limit`` файлов."""
    stack: list[Path] = [root]
    checked = 0
    while stack and checked < limit:
        current = stack.pop()
        try:
            entries = sorted(os.scandir(current), key=lambda e: e.name)
        except OSError:
            continue
        for entry in entries:
            if entry.is_dir(follow_symlinks=False):
                stack.append(Path(entry.path))
            elif entry.name.startswith("rollout-") and entry.name.endswith(".jsonl"):
                checked += 1
                yield Path(entry.path)
                if checked >= limit:
                    return


def _codex_auth_path() -> Path:
    return Path(os.environ.get("CODEX_AUTH_JSON", Path.home() / ".codex" / "auth.json"))


def _codex_token() -> tuple[str | None, str | None, str | None]:
    """``(access_token, account_id, error)``."""
    path = _codex_auth_path()
    if not path.is_file():
        return None, None, f"{path} не найден"
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        return None, None, f"не удалось прочитать {path}: {exc}"
    tokens = data.get("tokens")
    if not isinstance(tokens, dict):
        return None, None, "в auth.json нет секции tokens"
    token = tokens.get("access_token")
    if not isinstance(token, str) or not token:
        return None, None, "в auth.json нет tokens.access_token"
    account_id = tokens.get("account_id")
    return token, account_id if isinstance(account_id, str) else None, None


def _codex_api_windows(payload: dict[str, Any]) -> list[LimitWindow]:
    """Окна из ответа /backend-api/codex/usage.

    ``primary_window``/``secondary_window`` описаны длиной окна в секундах и
    абсолютным ``reset_at`` (unix). Кроме основного лимита в ответе бывают
    ``code_review_rate_limit`` и ``additional_rate_limits`` — их окна тоже
    показываем, помечая источником."""
    windows: list[LimitWindow] = []

    def add(block: Any, suffix: str = "") -> None:
        if not isinstance(block, dict):
            return
        for key in ("primary_window", "secondary_window"):
            window = block.get(key)
            if not isinstance(window, dict):
                continue
            reset_at = window.get("reset_at")
            name = _window_name_from_seconds(window.get("limit_window_seconds"))
            windows.append(
                LimitWindow(
                    name=f"{name} · {suffix}" if suffix else name,
                    used_percent=_as_float(window.get("used_percent")),
                    resets_at=(
                        datetime.fromtimestamp(reset_at, tz=timezone.utc)
                        if isinstance(reset_at, (int, float)) else None
                    ),
                )
            )

    add(payload.get("rate_limit"))
    add(payload.get("code_review_rate_limit"), "code review")
    extra = payload.get("additional_rate_limits")
    if isinstance(extra, dict):
        for label, block in extra.items():
            add(block, str(label))
    elif isinstance(extra, list):
        for block in extra:
            if isinstance(block, dict):
                add(block, str(block.get("name") or block.get("limit_id") or ""))
    return windows


def _window_name_from_seconds(seconds: Any) -> str:
    try:
        minutes = int(seconds) // 60
    except (TypeError, ValueError):
        return "окно"
    return _window_name(minutes)


def codex_limits() -> EngineLimits:
    """Остаток лимитов codex — тем же запросом, что стоит за TUI-командой
    ``/status``: GET /backend-api/codex/usage с ChatGPT-токеном из
    ``~/.codex/auth.json``. Не вышло — разбираем локальные rollout'ы."""
    result = EngineLimits(engine="codex")
    token, account_id, token_error = _codex_token()
    if token:
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": "codex_cli_rs (jarvis)",
            "originator": "codex_cli_rs",
        }
        if account_id:
            headers["chatgpt-account-id"] = account_id
        payload, error = _fetch_json(CODEX_USAGE_URL, headers)
        if isinstance(payload, dict):
            result.windows = _codex_api_windows(payload)
            result.live = True
            result.source = CODEX_USAGE_URL
            result.fetched_at = datetime.now(timezone.utc)
            if not result.windows:
                result.note = "API ответил, но окон лимитов в ответе нет"
            return result
        token_error = error
    fallback = _codex_limits_from_rollouts()
    fallback.note = "; ".join(x for x in (token_error, fallback.note) if x) or None
    return fallback


def _codex_limits_from_rollouts() -> EngineLimits:
    """Фолбэк: последний непустой ``rate_limits`` в самом свежем по mtime файле
    ``rollout-*.jsonl`` под ~/.codex/sessions. Цифры — на момент последнего
    хода codex, а не на сейчас."""
    result = EngineLimits(engine="codex")
    root = _codex_sessions_root()
    if not root.is_dir():
        result.note = "каталог сессий codex не найден"
        return result

    cutoff = datetime.now(timezone.utc).timestamp() - 30 * 86400
    latest_path: Path | None = None
    latest_mtime = -1.0
    try:
        for path in _iter_recent_rollouts(root, limit=200):
            try:
                mtime = path.stat().st_mtime
            except OSError:
                continue
            if mtime < cutoff:
                continue
            if mtime > latest_mtime:
                latest_mtime = mtime
                latest_path = path
    except OSError as exc:
        result.note = f"сканирование {root} не удалось: {exc}"
        return result

    if latest_path is None:
        result.note = "файлы rollout-*.jsonl за последние 30 дней не найдены"
        return result

    result.source = str(latest_path)
    # В одном файле встречаются несколько наборов лимитов (limit_id: codex,
    # premium, ...), и у части из них primary/secondary пустые. Брать буквально
    # последнюю строку нельзя: на живой машине последняя — как раз пустая, и
    # команда врала бы «данных нет» при заполненных процентах выше по файлу.
    # Поэтому храним последний НЕПУСТОЙ набор по каждому limit_id.
    by_limit: dict[str, dict[str, Any]] = {}
    seen_any = False
    try:
        with latest_path.open("r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if '"rate_limits"' not in line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                found = _find_key(row, "rate_limits")
                if not isinstance(found, dict):
                    continue
                seen_any = True
                if not any(
                    isinstance(found.get(k), dict) for k in ("primary", "secondary")
                ):
                    continue
                limit_id = str(found.get("limit_id") or "")
                by_limit[limit_id] = found
    except OSError as exc:
        result.note = f"не удалось прочитать {latest_path}: {exc}"
        return result

    if not by_limit:
        result.note = (
            "rate_limits найден, но окна пусты" if seen_any
            else f"строка с rate_limits не найдена в {latest_path}"
        )
        return result

    multi = len(by_limit) > 1
    for limit_id, rate_limits in by_limit.items():
        for key in ("primary", "secondary"):
            window = rate_limits.get(key)
            if not isinstance(window, dict):
                continue
            resets_at = window.get("resets_at")
            name = _window_name(window.get("window_minutes"))
            if multi and limit_id:
                name = f"{name} ({limit_id})"
            result.windows.append(
                LimitWindow(
                    name=name,
                    used_percent=_as_float(window.get("used_percent")),
                    resets_at=(
                        datetime.fromtimestamp(resets_at, tz=timezone.utc)
                        if isinstance(resets_at, (int, float))
                        else None
                    ),
                )
            )

    if not result.windows:
        result.note = "rate_limits найден, но primary и secondary пусты"
    return result


def opencode_limits() -> EngineLimits:
    return EngineLimits(engine="opencode", note="лимиты подписки opencode не отслеживаются")


def all_limits() -> list[EngineLimits]:
    return [claude_limits(), codex_limits(), opencode_limits()]


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_iso(raw: Any) -> datetime | None:
    if not isinstance(raw, str) or not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _format_delta(delta_seconds: float) -> str:
    """"через N ..." — до ближайшей осмысленной единицы (минуты/часы/дни)."""
    total_minutes = round(delta_seconds / 60)
    if total_minutes < 60:
        return f"{total_minutes} мин"
    total_hours = total_minutes // 60
    if total_hours < 24:
        rem_minutes = total_minutes % 60
        if rem_minutes:
            return f"{total_hours} ч {rem_minutes} мин"
        return f"{total_hours} ч"
    days = total_hours // 24
    rem_hours = total_hours % 24
    if rem_hours:
        return f"{days} д {rem_hours} ч"
    return f"{days} д"


def _plural(number: int, one: str, few: str, many: str) -> str:
    """Русское склонение: 1 день / 2 дня / 5 дней. 11–14 — всегда «дней»."""
    if 11 <= number % 100 <= 14:
        return many
    last = number % 10
    if last == 1:
        return one
    if 2 <= last <= 4:
        return few
    return many


def _format_countdown(delta_seconds: float) -> str:
    """«2 дня 5 часов 13 минут» — нулевые единицы опускаются, но если до сброса
    меньше минуты, строка не должна выйти пустой."""
    total_minutes = int(delta_seconds // 60)
    days, rem = divmod(total_minutes, 1440)
    hours, minutes = divmod(rem, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days} {_plural(days, 'день', 'дня', 'дней')}")
    if hours:
        parts.append(f"{hours} {_plural(hours, 'час', 'часа', 'часов')}")
    if minutes:
        parts.append(f"{minutes} {_plural(minutes, 'минуту', 'минуты', 'минут')}")
    return " ".join(parts) if parts else "менее минуты"


def _format_window_line(window: LimitWindow, now: datetime) -> str:
    """Строка вида «• 5 часов — осталось 69%, сброс 05.09 19:00».

    Показываем именно ОСТАТОК, а не расход: вопрос к команде всегда «сколько у
    меня ещё есть». Дата сброса — абсолютная, без «через сколько»."""
    if window.used_percent is None:
        left = "остаток неизвестен"
    else:
        left = f"осталось {max(0.0, 100.0 - window.used_percent):.0f}%"
    line = f"• {window.name} — {left}"
    if window.resets_at is not None:
        local = window.resets_at.astimezone()
        delta = (window.resets_at - now).total_seconds()
        if delta <= 0:
            line += ", сброс уже наступил"
        else:
            line += (
                f", сброс {local.strftime('%d.%m %H:%M')}"
                f" (через {_format_countdown(delta)})"
            )
    if window.note:
        line += f" ({window.note})"
    return line


def format_limits_block(items: list[EngineLimits], now: datetime | None = None) -> str:
    """Markdown-сводка: заголовок движка и по строке на окно лимита."""
    if now is None:
        now = datetime.now(timezone.utc)
    blocks: list[str] = []
    for item in items:
        lines = [f"**{item.engine}**"]
        for window in item.windows:
            lines.append(_format_window_line(window, now))
        if item.note:
            lines.append(f"*{item.note}*")
        if not item.live and item.fetched_at is not None:
            ago = _format_delta((now - item.fetched_at).total_seconds())
            lines.append(f"*API недоступен, показан кэш CLI ({ago} назад)*")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)
