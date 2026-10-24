"""Timezone helpers for VARRAM's Seoul-local API timestamps."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo

VARRAM_TIME_ZONE = ZoneInfo("Asia/Seoul")


def feed_history_window(
    now: datetime | None = None, *, days: int = 7
) -> tuple[date, str, str]:
    """Return the Seoul calendar date and inclusive API range for recent days."""
    if days < 1:
        raise ValueError("History window must include at least one day")
    instant = now or datetime.now(timezone.utc)
    if instant.tzinfo is None:
        raise ValueError("History window requires a timezone-aware datetime")
    today = instant.astimezone(VARRAM_TIME_ZONE).date()
    start_day = today - timedelta(days=days - 1)
    start_time = datetime.combine(start_day, time.min).strftime("%Y-%m-%d %H:%M:%S")
    end_time = datetime.combine(today, time.max.replace(microsecond=0)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    return today, start_time, end_time


def to_display_timezone(value: datetime, target_timezone: tzinfo) -> datetime:
    """Interpret naive VARRAM values as Seoul time, then convert for HA display."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=VARRAM_TIME_ZONE)
    return value.astimezone(target_timezone)
