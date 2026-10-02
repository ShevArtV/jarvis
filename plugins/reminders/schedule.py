"""Напоминания Менеджеру: разбор расписания и вычисление следующего срабатывания.

Cron-light: расписание задаётся человеческим текстом (``daily 09:00``,
``weekday 18:30``, ``weekly mon,thu 12:00``, ``monthly 1 10:00``,
``once 2026-07-30 15:00``). Времена — в ``JARVIS_REMINDERS_TZ``, а в БД хранится
UTC, поэтому вычисление next_fire всегда идёт через локальную зону и обратно.

Импортируется MCP-сервером (``manager_remind_add``), поэтому обязан оставаться
свободным от зависимостей на Telegram и на Application.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from bot.timeutil import utcnow

logger = logging.getLogger(__name__)

_DAY_NAMES = {
    "mon": 0, "tue": 1, "wed": 2, "thu": 3,
    "fri": 4, "sat": 5, "sun": 6,
}

def _reminders_tz():
    """Local timezone для парсера schedule. Default Europe/Moscow."""
    from zoneinfo import ZoneInfo
    name = os.environ.get("JARVIS_REMINDERS_TZ", "Europe/Moscow")
    try:
        return ZoneInfo(name)
    except Exception:
        logger.warning("JARVIS_REMINDERS_TZ=%r invalid, using Europe/Moscow", name,
                       exc_info=True)
        return ZoneInfo("Europe/Moscow")


def parse_reminder_schedule(schedule: str) -> dict:
    """Парсит человекочитаемое расписание в структуру.

    Поддерживаемые форматы:
      daily HH:MM
      weekday HH:MM        (Пн-Пт)
      weekend HH:MM        (Сб-Вс)
      weekly DAY[,DAY,...] HH:MM   (DAY: mon|tue|wed|thu|fri|sat|sun)
      monthly D HH:MM      (D: 1..28)
      once YYYY-MM-DD HH:MM

    Возвращает dict с ключами:
      type: 'daily'|'weekday'|'weekend'|'weekly'|'monthly'|'once'
      hour, minute: int
      days: list[int] — для 'weekly', индексы 0=mon..6=sun
      day: int — для 'monthly' (1..28)
      date: 'YYYY-MM-DD' — для 'once'
    """
    raw = " ".join((schedule or "").split()).strip().lower()
    if not raw:
        raise ValueError("schedule is empty")

    def parse_hm(token: str) -> tuple[int, int]:
        try:
            h, m = token.split(":")
            h, m = int(h), int(m)
            if not (0 <= h <= 23 and 0 <= m <= 59):
                raise ValueError
            return h, m
        except (ValueError, AttributeError):
            raise ValueError(f"invalid HH:MM: {token!r}") from None

    parts = raw.split()
    kind = parts[0]

    if kind in ("daily", "weekday", "weekend") and len(parts) == 2:
        h, m = parse_hm(parts[1])
        return {"type": kind, "hour": h, "minute": m}

    if kind == "weekly" and len(parts) == 3:
        days_token = parts[1]
        days_idx: list[int] = []
        for d in days_token.split(","):
            d = d.strip()
            if d not in _DAY_NAMES:
                raise ValueError(f"unknown day: {d!r}; expected one of {list(_DAY_NAMES)}")
            if _DAY_NAMES[d] not in days_idx:
                days_idx.append(_DAY_NAMES[d])
        if not days_idx:
            raise ValueError("weekly: at least one day required")
        h, m = parse_hm(parts[2])
        return {"type": "weekly", "days": sorted(days_idx), "hour": h, "minute": m}

    if kind == "monthly" and len(parts) == 3:
        try:
            day = int(parts[1])
        except ValueError:
            raise ValueError(f"monthly: day must be int, got {parts[1]!r}") from None
        if not (1 <= day <= 28):
            raise ValueError("monthly: day must be 1..28 (защита от февраля)")
        h, m = parse_hm(parts[2])
        return {"type": "monthly", "day": day, "hour": h, "minute": m}

    if kind == "once" and len(parts) == 3:
        date_str = parts[1]
        h, m = parse_hm(parts[2])
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"once: date must be YYYY-MM-DD, got {date_str!r}") from None
        return {"type": "once", "date": date_str, "hour": h, "minute": m}

    raise ValueError(
        f"can't parse schedule: {schedule!r}. Examples: "
        "'daily 09:30', 'weekday 09:30', 'weekly mon,wed 14:00', "
        "'monthly 1 10:00', 'once 2026-06-01 09:00'."
    )


def compute_next_fire(parsed: dict, after_utc: datetime | None = None) -> datetime | None:
    """Возвращает следующий момент срабатывания (datetime, UTC, naive ISO-able).

    Возвращает None для 'once' если дата уже в прошлом — такой reminder
    в БД пометится disabled при INSERT/обновлении.
    """
    from zoneinfo import ZoneInfo
    tz = _reminders_tz()
    if after_utc is None:
        after_utc = utcnow()
    # UTC naive → aware
    now_aware = after_utc.replace(tzinfo=ZoneInfo("UTC")).astimezone(tz)
    hh = parsed["hour"]
    mm = parsed["minute"]

    def at_local(year: int, month: int, day: int) -> datetime:
        local = datetime(year, month, day, hh, mm, tzinfo=tz)
        return local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)

    ptype = parsed["type"]
    if ptype == "once":
        y, mo, d = map(int, parsed["date"].split("-"))
        fire = at_local(y, mo, d)
        return fire if fire > after_utc else None

    today_local = now_aware.replace(hour=hh, minute=mm, second=0, microsecond=0)

    def in_set(weekday: int) -> bool:
        if ptype == "daily":
            return True
        if ptype == "weekday":
            return weekday < 5
        if ptype == "weekend":
            return weekday >= 5
        if ptype == "weekly":
            return weekday in parsed["days"]
        if ptype == "monthly":
            return False  # для monthly другой механизм ниже
        return False

    if ptype == "monthly":
        target_day = parsed["day"]
        candidate_local = now_aware.replace(day=target_day, hour=hh, minute=mm, second=0, microsecond=0)
        if candidate_local <= now_aware:
            # следующий месяц
            year = candidate_local.year
            month = candidate_local.month + 1
            if month > 12:
                month = 1
                year += 1
            candidate_local = candidate_local.replace(year=year, month=month)
        return candidate_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)

    # daily/weekday/weekend/weekly — итеративный поиск ближайшего дня.
    for delta in range(0, 8):
        cand_local = today_local + timedelta(days=delta)
        if delta == 0 and cand_local <= now_aware:
            continue
        if in_set(cand_local.weekday()):
            return cand_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    return None  # не должно случаться для рекуррентных
