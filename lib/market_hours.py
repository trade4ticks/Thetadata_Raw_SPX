"""
Market hours utilities for US equity/options markets.

All datetime logic is in US/Eastern time.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

import pandas_market_calendars as mcal
import pytz

_ET = pytz.timezone("US/Eastern")
_NYSE = mcal.get_calendar("NYSE")

# Market open/close in ET
_OPEN_TIME  = time(9, 35)   # skip the erroneous 9:30 bar
_CLOSE_TIME = time(16, 0)


def now_et() -> datetime:
    return datetime.now(_ET)


def is_trading_day(d: date) -> bool:
    schedule = _NYSE.schedule(
        start_date=d.strftime("%Y-%m-%d"),
        end_date=d.strftime("%Y-%m-%d"),
    )
    return not schedule.empty


def is_market_open(dt: datetime | None = None) -> bool:
    """Return True if dt (default: now) is within 9:35–16:00 ET on a trading day."""
    if dt is None:
        dt = now_et()
    elif dt.tzinfo is None:
        dt = _ET.localize(dt)

    if dt.weekday() >= 5:
        return False
    if not is_trading_day(dt.date()):
        return False
    t = dt.time()
    return _OPEN_TIME <= t <= _CLOSE_TIME


def get_trading_days(start: date, end: date) -> list[date]:
    """Return list of NYSE trading days between start and end (inclusive)."""
    schedule = _NYSE.schedule(
        start_date=start.strftime("%Y-%m-%d"),
        end_date=end.strftime("%Y-%m-%d"),
    )
    return [d.date() for d in schedule.index]


def last_trading_day(reference: date | None = None) -> date:
    """Return the most recent completed trading day at or before reference (default: today)."""
    if reference is None:
        reference = now_et().date()
    # Walk backwards at most 7 days to find a trading day
    d = reference
    for _ in range(7):
        if is_trading_day(d):
            return d
        d -= timedelta(days=1)
    return reference
