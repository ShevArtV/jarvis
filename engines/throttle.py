"""Pace an external-trigger queue against subscription usage windows."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from engines.limits import EngineLimits, LimitWindow, claude_limits, codex_limits

WORK_TZ = ZoneInfo("Europe/Volgograd")
WORK_START_HOUR = 9
WORK_END_HOUR = 21
RESERVE_PERCENT = 10.0


@dataclass(frozen=True)
class ThrottleDecision:
    defer_until: datetime | None
    used_percent: float | None
    allowed_percent: float | None
    reason: str | None = None


def _weekly_window(limits: EngineLimits) -> LimitWindow | None:
    # The API labels both providers' subscription window as "7 days".  Do not
    # accidentally throttle on the separate code-review window.
    return next((w for w in limits.windows if w.name.startswith("7 дней")), None)


def _worked_seconds(start: datetime, end: datetime) -> float:
    """Seconds inside Mon-Sat 09:00-21:00, with UTC endpoints."""
    start, end = start.astimezone(WORK_TZ), end.astimezone(WORK_TZ)
    total = 0.0
    day = start.replace(hour=0, minute=0, second=0, microsecond=0)
    while day < end:
        if day.weekday() != 6:  # Monday=0, Sunday=6
            a = day.replace(hour=WORK_START_HOUR)
            b = day.replace(hour=WORK_END_HOUR)
            total += max(0.0, (min(end, b) - max(start, a)).total_seconds())
        day += timedelta(days=1)
    return total


def pace_decision(limits: EngineLimits, now: datetime | None = None) -> ThrottleDecision:
    now = now or datetime.now(timezone.utc)
    # A force-start may bypass our *pace*, never a provider window that is
    # already exhausted (usually the short rolling window).
    for candidate in limits.windows:
        if candidate.used_percent is not None and candidate.used_percent >= 100:
            return ThrottleDecision(
                candidate.resets_at, candidate.used_percent, None,
                f"исчерпан лимит: {candidate.name}",
            )
    window = _weekly_window(limits)
    if window is None or window.used_percent is None or window.resets_at is None:
        return ThrottleDecision(None, None, None, "недельное окно лимита недоступно")
    end = window.resets_at.astimezone(timezone.utc)
    start = end - timedelta(days=7)
    total = _worked_seconds(start, end)
    elapsed = _worked_seconds(start, min(now, end))
    allowed = (100.0 - RESERVE_PERCENT) * elapsed / total if total else 0.0
    if now >= end or window.used_percent <= allowed:
        return ThrottleDecision(None, window.used_percent, allowed)
    # Find the first minute in the provider's rolling window at which the
    # current spend is on pace. This is deliberately recalculated before run.
    lo, hi = now, end
    for _ in range(18):
        mid = lo + (hi - lo) / 2
        mid_allowed = (100.0 - RESERVE_PERCENT) * _worked_seconds(start, mid) / total
        if mid_allowed >= window.used_percent:
            hi = mid
        else:
            lo = mid
    due = hi.replace(second=0, microsecond=0) + timedelta(minutes=1)
    return ThrottleDecision(due, window.used_percent, allowed, "расход опережает рабочий темп")


def engine_pace_decision(engine: str) -> ThrottleDecision:
    if engine == "claude":
        return pace_decision(claude_limits())
    if engine == "codex":
        return pace_decision(codex_limits())
    return ThrottleDecision(None, None, None, f"лимиты {engine} не поддерживаются")
