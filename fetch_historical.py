"""
fetch_historical.py — Backfill SPX options greek data for a given date range.

Usage:
    python fetch_historical.py

You will be prompted to enter start and end dates (YYYYMMDD).

What it does:
  1. Computes NYSE trading days in the requested range.
  2. For each trading day, queries the EOD endpoint (expiration=*) to discover
     which expirations actually had data on that specific day.
  3. Builds one task per (trading_day, expiration) — only for expirations that
     existed on that day. No wasted calls on future-dated LEAPS.
  4. Fetches 5-minute first-order greeks for all strikes via ThreadPoolExecutor(2).
  5. Writes one parquet file per task:
       {DATA_DIR}/{YYYYMMDD_date}/{YYYYMMDD_expiration}/{AM|PM}.parquet

Resume-safe: skips any task where the parquet file already exists.
"""

from __future__ import annotations

import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime

from tqdm import tqdm

from config import DATA_DIR
from lib.client import fetch_greeks_history, list_active_expirations, test_connection
from lib.market_hours import get_trading_days, last_trading_day
from lib.storage import exists, write

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

MAX_WORKERS = 2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_date(s: str) -> date:
    s = s.strip().replace("-", "")
    return datetime.strptime(s, "%Y%m%d").date()


def _fetch_and_write(td_symbol: str, expiration: str, settlement: str,
                     trading_day: date) -> int:
    """
    Fetch one (trading_day, expiration) and write a single parquet file.
    Returns rows written.
    """
    df = fetch_greeks_history(td_symbol, expiration, trading_day, trading_day)
    if df.empty:
        return 0

    day_str    = trading_day.strftime("%Y%m%d")
    exp_nodash = expiration.replace("-", "")
    write(day_str, exp_nodash, settlement, df)
    return len(df)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    # ---- Date range prompt ----
    print("\n=== SPX Options Historical Fetch ===")
    print(f"Data will be saved to: {DATA_DIR}\n")

    while True:
        raw_start = input("Start date (YYYYMMDD): ").strip()
        try:
            start = _parse_date(raw_start)
            break
        except ValueError:
            print("  Invalid format. Use YYYYMMDD (e.g. 20240101)")

    while True:
        raw_end = input("End date   (YYYYMMDD): ").strip()
        try:
            end = _parse_date(raw_end)
            break
        except ValueError:
            print("  Invalid format. Use YYYYMMDD (e.g. 20241231)")

    if end < start:
        print("End date must be >= start date.")
        sys.exit(1)

    # Cap end at last completed trading day
    end = min(end, last_trading_day())
    if end < start:
        print("No completed trading days in the requested range.")
        sys.exit(0)

    trading_days = get_trading_days(start, end)
    print(f"\nRange: {start} → {end}  ({len(trading_days)} trading days)")

    # ---- Connection check ----
    print("Checking ThetaData connection...", end=" ", flush=True)
    if not test_connection():
        print("FAILED\nCannot reach ThetaData terminal. Check Tailscale and terminal status.")
        sys.exit(1)
    print("OK")

    # ---- Process day by day ----
    grand_total_rows  = 0
    grand_total_files = 0
    grand_errors      = 0

    for day_idx, day in enumerate(trading_days):
        day_str = day.strftime("%Y%m%d")
        day_label = f"[{day_idx + 1}/{len(trading_days)}] {day}"

        # Discover which expirations actually existed on this trading day
        print(f"\n{day_label}  Discovering expirations...", end=" ", flush=True)
        active = list_active_expirations(day)
        if not active:
            print("no expirations found — skipping")
            continue

        # Filter to only expirations we haven't already fetched
        tasks: list[tuple[str, str, str, date]] = []
        for exp_info in active:
            exp_nodash = exp_info["expiration"].replace("-", "")
            if not exists(day_str, exp_nodash, exp_info["settlement"]):
                tasks.append((
                    exp_info["td_symbol"],
                    exp_info["expiration"],
                    exp_info["settlement"],
                    day,
                ))

        if not tasks:
            print(f"{len(active)} expirations — all already fetched")
            continue

        print(f"{len(active)} expirations, {len(tasks)} to fetch")

        # Fetch greeks for each expiration on this day
        day_rows  = 0
        day_files = 0
        day_errors = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            future_map = {
                pool.submit(_fetch_and_write, *task): task
                for task in tasks
            }
            with tqdm(total=len(tasks), unit="exp", ncols=90,
                      desc=f"  {day_str}") as pbar:
                for future in as_completed(future_map):
                    task = future_map[future]
                    td_sym, exp, sett, _ = task
                    try:
                        rows = future.result()
                        day_rows += rows
                        if rows > 0:
                            day_files += 1
                        pbar.set_postfix_str(f"{td_sym} {exp} ({rows}r)")
                    except Exception as exc:
                        day_errors += 1
                        log.warning("FAILED  %s %s %s %s: %s",
                                    td_sym, exp, sett, day, exc)
                    finally:
                        pbar.update(1)

        grand_total_rows  += day_rows
        grand_total_files += day_files
        grand_errors      += day_errors

        print(f"  {day_label}  {day_rows:,} rows → {day_files} files"
              + (f"  ({day_errors} errors)" if day_errors else ""))

    print(f"\n{'='*60}")
    print(f"Done. {grand_total_rows:,} rows → {grand_total_files:,} files. "
          f"{grand_errors} errors across {len(trading_days)} trading days.")


if __name__ == "__main__":
    main()
