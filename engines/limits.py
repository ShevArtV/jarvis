"""Best-effort чтение остатка лимитов подписки claude/codex/opencode.

Стиль — как в engines/session_usage.py: dataclass'ы, никаких исключений
наружу, любая ошибка чтения превращается в поле ``note``."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from engines.session_usage import _codex_sessions_root


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


def _claude_config_path() -> Path:
    return Path(os.environ.get("CLAUDE_CONFIG_JSON", Path.home() / ".claude.json"))


def claude_limits() -> EngineLimits:
    """Остаток лимитов claude — из кэша CLI ``~/.claude.json`` (ключ
    ``cachedUsageUtilization``). CLI обновляет этот кэш сам при своих запросах,
    здесь мы его только читаем."""
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

    for key, name in (
        ("five_hour", "5 часов"),
        ("seven_day", "7 дней"),
        ("seven_day_opus", "7 дней (opus)"),
        ("seven_day_sonnet", "7 дней (sonnet)"),
    ):
        window = utilization.get(key)
        if not isinstance(window, dict):
            continue
        result.windows.append(
            LimitWindow(
                name=name,
                used_percent=_as_float(window.get("utilization")),
                resets_at=_parse_iso(window.get("resets_at")),
            )
        )

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


def codex_limits() -> EngineLimits:
    """Остаток лимитов codex — из последней строки с ``rate_limits`` в самом
    свежем по mtime файле ``rollout-*.jsonl`` под ~/.codex/sessions."""
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


def _format_window_line(window: LimitWindow, now: datetime) -> str:
    percent_part = "  ?%" if window.used_percent is None else f"{window.used_percent:>3.0f}%"
    line = f"  {window.name:<10}: {percent_part}"
    if window.resets_at is not None:
        delta = (window.resets_at - now).total_seconds()
        if delta <= 0:
            line += "  сброс наступил"
        else:
            local = window.resets_at.astimezone()
            if delta < 86400:
                stamp = local.strftime("%H:%M")
            else:
                stamp = local.strftime("%d.%m %H:%M")
            line += f"  сброс через {_format_delta(delta)} ({stamp})"
    if window.note:
        line += f"  ({window.note})"
    return line


def format_limits_block(items: list[EngineLimits], now: datetime | None = None) -> str:
    """Моноширинный plain-текст со сводкой лимитов — вызывающий сам оборачивает
    в <pre>."""
    if now is None:
        now = datetime.now(timezone.utc)
    blocks: list[str] = []
    for item in items:
        lines = [item.engine]
        for window in item.windows:
            lines.append(_format_window_line(window, now))
        if item.note:
            lines.append(f"  {item.note}")
        if item.fetched_at is not None:
            ago = _format_delta((now - item.fetched_at).total_seconds())
            lines.append(f"  данные из кэша CLI, собраны {ago} назад")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)
