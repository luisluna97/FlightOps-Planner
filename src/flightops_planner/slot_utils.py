"""
Utilities for rounding timestamps and expanding slot ranges.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Iterable, List

import pandas as pd


def round_to_slot(ts: pd.Timestamp, *, minutes: int) -> pd.Timestamp:
    """
    Round a timestamp to the nearest slot with tie-breaking upwards.
    """

    if pd.isna(ts):
        return ts

    if not ts.tzinfo:
        ts = ts.tz_localize("UTC")

    delta = timedelta(minutes=minutes)
    epoch = pd.Timestamp("1970-01-01", tz="UTC")
    diff = ts - epoch
    slots = diff.total_seconds() / delta.total_seconds()
    rounded = int(slots)
    diff_fraction = slots - rounded
    if diff_fraction > 0.5 or abs(diff_fraction - 0.5) < 1e-9:
        rounded += 1
    target = epoch + rounded * delta
    return target.tz_convert(ts.tzinfo)


def slot_range(start: pd.Timestamp, end: pd.Timestamp, *, minutes: int) -> List[pd.Timestamp]:
    """
    Build an inclusive range of slots between start and end, assuming both are aligned.
    """

    if pd.isna(start) or pd.isna(end):
        return []

    if end < start:
        return []

    freq = f"{minutes}T"
    rng = pd.date_range(start=start, end=end, freq=freq, tz=start.tz)
    return list(rng)


def expand_slots(
    base: pd.Timestamp,
    *,
    before: timedelta,
    after: timedelta,
    minutes: int,
) -> Iterable[pd.Timestamp]:
    """
    Generate slot timestamps centered around ``base``.
    """

    if pd.isna(base):
        return []

    start = base - before
    end = base + after
    start = round_to_slot(start, minutes=minutes)
    end = round_to_slot(end, minutes=minutes)
    return slot_range(start, end, minutes=minutes)


__all__ = ["round_to_slot", "slot_range", "expand_slots"]
