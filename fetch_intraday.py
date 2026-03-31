"""
fetch_intraday.py — Append today's 5-minute SPX greeks bars (for all active expirations).

Designed to be called by cron every 5 minutes during market hours.
Safe to call any time — exits immediately outside 09:35–16:00 ET on trading days.

What it does:
  1. Checks market hours (9:35–16:00 ET, NYSE trading day). Exits if closed.
  2. Fetches active expirations (those not yet expired as of today).
  3. For each expiration, determines the last bar already stored for today.
  4. Fetches only the missing bars (from last_stored_time+5min to now).
  5. Appends new bars to {DATA_DIR}/{today}/{expiration}/{AM|PM}.parquet.
  6. Uses up to 4 concurrent workers.

Cron example (ET-equivalent, adjust for your VPS timezone):
  */5 9-16 * * 1-5  /path/to/venv/bin/python /path/to/fetch_intraday.py

Because the script checks market hours itself, it is safe to run via cron
at any time — off-hours calls will exit in under a second.
"""

from __future__ import annotations

import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time, timedelta

import pandas as pd

from config import DATA_DIR
from lib.client import fetch_greeks_history, list_expirations, test_connection
from lib.market_hours import is_market_open, now_et
from lib.storage import append, latest_timestamp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

MAX_WORKERS = 4
_ET_OPEN  = time(9, 35)
_ET_CLOSE = time(16, 0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ms_to_time(ms: int) -> time:
    total_seconds = ms // 1000
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    return time(h, m, s)


def _next_start_time(last_ts_str: str | None, today: date) -> str:
    """
    Given the latest stored timestamp for today, return the start_time for the next fetch.

    If no data yet:  start at 09:35:00
    Otherwise:       last bar time + 5 minutes
    """
    if last_ts_str is None:
        return "09:35:00"

    # last_ts_str is ISO 8601: "2025-01-17T09:35:00.000"
    try:
        dt = datetime.strptime(last_ts_str[:19], "%Y-%m-%dT%H:%M:%S")
        next_dt = dt + timedelta(minutes=5)
        return next_dt.strftime("%H:%M:%S")
    except ValueError:
        return "09:35:00"


def _fetch_and_append(td_symbol: str, expiration: str, settlement: str,
                      today: date, start_time: str, end_time: str) -> int:
    """
    Fetch bars for one active expiration from start_time to end_time today.
    Appends new rows to today's parquet file.
    Returns number of new rows written.
    """
    exp_nodash  = expiration.replace("-", "")
    today_str   = today.strftime("%Y%m%d")

    df = fetch_greeks_history(td_symbol, expiration, today, today)
    if df.empty:
        return 0

    # Filter to bars >= start_time
    def _ts_to_time(ts: str) -> str:
        # "2025-01-17T09:35:00.000" → "09:35:00"
        return ts[11:19] if len(ts) >= 19 else "00:00:00"

    df = df[df["timestamp"].apply(_ts_to_time) >= start_time].reset_index(drop=True)
    if df.empty:
        return 0

    append(today_str, exp_nodash, settlement, df)
    return len(df)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    now = now_et()

    if not is_market_open(now):
        log.info("Market closed (%s ET) — nothing to do.", now.strftime("%H:%M:%S"))
        sys.exit(0)

    today     = now.date()
    today_str = today.strftime("%Y%m%d")
    now_time  = now.strftime("%H:%M:%S")

    log.info("Intraday fetch starting — %s ET", now.strftime("%Y-%m-%d %H:%M:%S"))

    # ---- Connection check ----
    if not test_connection():
        log.error("Cannot reach ThetaData terminal. Aborting.")
        sys.exit(1)

    # ---- Active expirations (not yet expired as of today) ----
    all_expirations = list_expirations()
    active = [
        e for e in all_expirations
        if datetime.strptime(e["expiration"], "%Y-%m-%d").date() >= today
    ]
    log.info("%d active expirations", len(active))

    if not active:
        log.info("No active expirations. Nothing to fetch.")
        sys.exit(0)

    # ---- Build tasks ----
    tasks: list[tuple[str, str, str, date, str, str]] = []
    # (td_symbol, expiration, settlement, today, start_time, end_time)

    for exp_info in active:
        td_symbol  = exp_info["td_symbol"]
        expiration = exp_info["expiration"]
        settlement = exp_info["settlement"]
        exp_nodash = expiration.replace("-", "")

        last_ts = latest_timestamp(today_str, exp_nodash, settlement)
        start_t = _next_start_time(last_ts, today)

        # Nothing new to fetch if we're already up to date
        if start_t >= now_time:
            continue

        tasks.append((td_symbol, expiration, settlement, today, start_t, now_time))

    if not tasks:
        log.info("Already up to date for all active expirations.")
        sys.exit(0)

    log.info("%d expirations need updates (%d workers)", len(tasks), MAX_WORKERS)

    # ---- Fetch ----
    total_rows = 0
    errors     = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        future_map = {
            pool.submit(_fetch_and_append, *task): task
            for task in tasks
        }
        for future in as_completed(future_map):
            task = future_map[future]
            td_sym, exp, sett, _, start_t, _ = task
            try:
                rows = future.result()
                total_rows += rows
                log.debug("  %s %s %s: +%d rows", td_sym, exp, sett, rows)
            except Exception as exc:
                errors += 1
                log.warning("FAILED  %s %s %s (from %s): %s", td_sym, exp, sett, start_t, exc)

    log.info("Done. +%d new rows across %d expirations. %d errors.",
             total_rows, len(tasks) - errors, errors)


if __name__ == "__main__":
    main()
