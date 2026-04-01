"""
fetch_historical.py — Backfill SPX options greek data for a given date range.

Usage:
    python fetch_historical.py

You will be prompted to enter start and end dates (YYYYMMDD).

What it does:
  1. Computes NYSE trading days in the requested range.
  2. Fetches all available expirations for SPXW (PM) and SPX (AM).
  3. Builds one task per (trading_day, expiration) pair — each task is a
     single-day API call for one expiration, all strikes, both rights, 5m bars.
  4. Runs tasks via ThreadPoolExecutor(2) to stay within ThetaData limits.
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
from lib.client import fetch_greeks_history, list_expirations, test_connection
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

    # ---- Expirations ----
    print("Fetching expiration list...", end=" ", flush=True)
    all_expirations = list_expirations()
    print(f"{len(all_expirations)} expirations found (SPXW + SPX)")

    # Pre-parse expiration dates
    exp_dates = []
    for e in all_expirations:
        e["_exp_date"] = datetime.strptime(e["expiration"], "%Y-%m-%d").date()
        exp_dates.append(e)

    # ---- Build task list: one task per (trading_day, expiration) ----
    tasks: list[tuple[str, str, str, date]] = []
    # (td_symbol, expiration, settlement, trading_day)

    for day in trading_days:
        # Active expirations for this day: not yet expired
        active = [e for e in exp_dates if e["_exp_date"] >= day]
        for exp_info in active:
            td_symbol  = exp_info["td_symbol"]
            expiration = exp_info["expiration"]
            settlement = exp_info["settlement"]
            exp_nodash = expiration.replace("-", "")
            day_str    = day.strftime("%Y%m%d")

            if exists(day_str, exp_nodash, settlement):
                continue  # already fetched

            tasks.append((td_symbol, expiration, settlement, day))

    if not tasks:
        print("\nAll data already present — nothing to fetch.")
        return

    n_days = len({t[3] for t in tasks})
    n_exps = len({(t[0], t[1]) for t in tasks})
    print(f"\n{len(tasks)} tasks ({n_days} days × ~{n_exps} expirations, {MAX_WORKERS} workers)\n")

    # ---- Fetch ----
    total_rows  = 0
    total_files = 0
    errors      = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        future_map = {
            pool.submit(_fetch_and_write, *task): task
            for task in tasks
        }
        with tqdm(total=len(tasks), unit="task", ncols=90) as pbar:
            for future in as_completed(future_map):
                task = future_map[future]
                td_sym, exp, sett, day = task
                try:
                    rows = future.result()
                    total_rows += rows
                    if rows > 0:
                        total_files += 1
                    pbar.set_postfix_str(f"{day} {td_sym} {exp} ({rows}r)")
                except Exception as exc:
                    errors += 1
                    log.warning("FAILED  %s %s %s %s: %s", td_sym, exp, sett, day, exc)
                finally:
                    pbar.update(1)

    print(f"\nDone. {total_rows:,} rows → {total_files:,} files. {errors} errors.")


if __name__ == "__main__":
    main()
