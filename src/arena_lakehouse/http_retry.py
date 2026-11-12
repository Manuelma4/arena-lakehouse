"""Bounded Retry-After parsing for provider HTTP requests."""

import math
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime


def retry_after_delay(header: str | None, fallback: float, *, limit: float) -> float:
    if header is None:
        return fallback
    try:
        delay = float(header)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(header)
        except (ValueError, OverflowError):
            return fallback
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        delay = (retry_at - datetime.now(timezone.utc)).total_seconds()
    return min(max(delay, 0), limit) if math.isfinite(delay) else fallback
