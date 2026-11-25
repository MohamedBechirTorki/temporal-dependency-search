"""Time utilities.

All instants are stored as timezone-aware UTC datetimes truncated to
millisecond precision (the precision of BSON dates), so values round-trip
identically through MongoDB, Neo4j and Python.

Open interval bounds are represented internally with sentinels:
    MIN_TIME  -> "since forever"   (API exposes null)
    MAX_TIME  -> "until further notice" (API exposes null)
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

UTC = timezone.utc
MIN_TIME = datetime(1, 1, 1, tzinfo=UTC)
MAX_TIME = datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=UTC)
ONE_MS = timedelta(milliseconds=1)


def truncate_ms(dt: datetime) -> datetime:
    return dt.replace(microsecond=(dt.microsecond // 1000) * 1000)


def normalize(dt: datetime) -> datetime:
    """Validate tz-awareness, convert to UTC, truncate to milliseconds."""
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError("timezone-aware datetime required (e.g. '2026-01-01T00:00:00Z')")
    return truncate_ms(dt.astimezone(UTC))


def utc_now() -> datetime:
    return truncate_ms(datetime.now(UTC))


def as_utc(dt: datetime) -> datetime:
    """Convert a Python or Neo4j datetime to timezone-aware UTC."""
    if hasattr(dt, "to_native"):
        dt = dt.to_native()
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def end_to_api(dt: datetime | None) -> datetime | None:
    """MAX_TIME -> None for API output."""
    if dt is None or as_utc(dt) >= MAX_TIME:
        return None
    return as_utc(dt)


def start_to_api(dt: datetime | None) -> datetime | None:
    """MIN_TIME -> None for API output."""
    if dt is None or as_utc(dt) <= MIN_TIME:
        return None
    return as_utc(dt)
