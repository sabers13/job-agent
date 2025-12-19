from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Optional


RELATIVE_RE = re.compile(r"vor\s+(\d+)\s+([a-zäöüß]+)", re.IGNORECASE)
ABSOLUTE_DATE_RE = re.compile(
    r"(\d{1,2})\.(\d{1,2})\.(\d{2,4})", re.IGNORECASE
)

UNIT_TO_DELTA = {
    "sekunde": "seconds",
    "sekunden": "seconds",
    "minute": "minutes",
    "minuten": "minutes",
    "stunde": "hours",
    "stunden": "hours",
    "tag": "days",
    "tage": "days",
    "tagen": "days",
    "woche": "weeks",
    "wochen": "weeks",
    "monat": "months",
    "monate": "months",
    "monaten": "months",
    "jahr": "years",
    "jahre": "years",
    "jahren": "years",
}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso8601_utc(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        txt = value.strip()
        if txt.endswith("Z"):
            txt = txt[:-1] + "+00:00"
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def isoformat_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _apply_months(now: datetime, months: int) -> datetime:
    # Simple month subtraction without external deps. Clamp to last day of target month.
    year = now.year
    month = now.month - months
    while month <= 0:
        month += 12
        year -= 1
    day = min(now.day, _days_in_month(year, month))
    return now.replace(year=year, month=month, day=day)


def _apply_years(now: datetime, years: int) -> datetime:
    year = now.year - years
    day = now.day
    month = now.month
    # Handle Feb 29 -> Feb 28 fallback
    if month == 2 and day == 29:
        day = 28
    return now.replace(year=year, month=month, day=day)


def _days_in_month(year: int, month: int) -> int:
    if month == 12:
        next_month = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        next_month = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    this_month = datetime(year, month, 1, tzinfo=timezone.utc)
    return (next_month - this_month).days


def parse_stepstone_listing_date(label: Optional[str], *, now: Optional[datetime] = None) -> Optional[datetime]:
    if not label:
        return None
    reference = now or _now_utc()
    text = label.strip()
    if not text:
        return None
    text_lower = text.lower()
    text_lower = text_lower.replace("erschienen:", "").strip()
    if not text_lower:
        return None

    if text_lower in ("heute",):
        return reference.replace(hour=0, minute=0, second=0, microsecond=0)
    if text_lower in ("gestern",):
        base = reference - timedelta(days=1)
        return base.replace(hour=0, minute=0, second=0, microsecond=0)

    match = RELATIVE_RE.search(text_lower)
    if match:
        amount = int(match.group(1))
        unit = match.group(2)
        delta_unit = UNIT_TO_DELTA.get(unit)
        if delta_unit == "seconds":
            return reference - timedelta(seconds=amount)
        if delta_unit == "minutes":
            return reference - timedelta(minutes=amount)
        if delta_unit == "hours":
            return reference - timedelta(hours=amount)
        if delta_unit == "days":
            return reference - timedelta(days=amount)
        if delta_unit == "weeks":
            return reference - timedelta(weeks=amount)
        if delta_unit == "months":
            return _apply_months(reference, amount)
        if delta_unit == "years":
            return _apply_years(reference, amount)

    # Absolute German date like 12.11.2024 or 12.11.24
    abs_match = ABSOLUTE_DATE_RE.search(text_lower)
    if abs_match:
        day, month, year = abs_match.groups()
        day_i = int(day)
        month_i = int(month)
        year_i = int(year)
        if year_i < 100:
            year_i += 2000
        try:
            return datetime(year_i, month_i, day_i, tzinfo=timezone.utc)
        except ValueError:
            return None

    return None
