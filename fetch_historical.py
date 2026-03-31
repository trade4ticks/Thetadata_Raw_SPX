"""
fetch_historical.py — Backfill SPX options greek data for a given date range.

Usage:
    python fetch_historical.py

You will be prompted to enter start and end dates (YYYYMMDD).

What it does:
  1. Fetches all available expirations for SPXW (PM) and SPX (AM).
  2. Filters to expirations active during the requested range.
  3. Chunks each expiration into <=28-day windows (ThetaData limit).
  4. Fetches 5-minute first-order greeks for all strikes via ThreadPoolExecutor(4).
  5. Splits each response by trading date and writes:
       {DATA_DIR}/{YYYYMMDD_date}/{YYYYMMDD_expiration}/{AM|PM}.parquet

Resume-safe: a chunk is skipped only if parquet files exist for ALL trading
days in that chunk for that expiration+settlement.
"""

from __future__ import annotations

import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime

from tqdm import tqdm

from config import DATA_DIR
from lib.client import chunk_date_range, fetch_greeks_history, list_expirations, test_connection
from lib.market_hours import get_trading_days, last_trading_day
from lib.storage import exists, write

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

MAX_WORKERS = 4

# Cache trading day lookups — get_trading_days hits the NYSE calendar and is
# called once per chunk during task building; cache avoids redundant lookups
# for chunks that share the same calendar window.
_trading_day_cache: dict[tuple[date, date], list[date]] = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_date(s: str) -> date:
    s = s.strip().replace("-", "")
    return datetime.strptime(s, "%Y%m%d").date()


def _get_trading_days(start: date, end: date) -> list[date]:
    key = (start, end)
    if key not in _trading_day_cache:
        _trading_day_cache[key] = get_trading_days(start, end)
    return _trading_day_cache[key]


def _is_done(expiration: str, settlement: str, chunk_start: date, chunk_end: date) -> bool:
    """
    A chunk is considered complete only if parquet files exist for ALL
    trading days in that chunk for the given expiration+settlement.
    """
    exp_nodash   = expiration.replace("-", "")
    trading_days = _get_trading_days(chunk_start, chunk_end)
    if not trading_days:
        return True  # no trading days in range, nothing to fetch
    return all(
        exists(day.strftime("%Y%m%d"), exp_nodash, settlement)
        for day in trading_days
    )


def _fetch_and_write(td_symbol: str, expiration: str, settlement: str,
                     chunk_start: date, chunk_end: date) -> tuple[int, int]:
    """
    Fetch one (expiration, date-chunk) and write parquet files split by trading date.
    Returns (rows_written, files_written).
    """
    df = fetch_greeks_history(td_symbol, expiration, chunk_start, chunk_end)
    if df.empty:
        return 0, 0

    # Extract trading date from timestamp (first 10 chars: "YYYY-MM-DD")
    df["_date"] = df["timestamp"].str[:10].str.replace("-", "")

    exp_nodash = expiration.replace("-", "")
    rows_written = 0
    files_written = 0

    for trading_date, day_df in df.groupby("_date"):
        day_df = day_df.drop(columns=["_date"]).reset_index(drop=True)
        write(str(trading_date), exp_nodash, settlement, day_df)
        rows_written += len(day_df)
        files_written += 1

    return rows_written, files_written


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

    print(f"\nRange: {start} → {end}")

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

    # Filter: only expirations active during the requested range
    # (expiration date >= start, meaning the contract had not yet expired)
    active = [
        e for e in all_expirations
        if datetime.strptime(e["expiration"], "%Y-%m-%d").date() >= start
    ]
    print(f"{len(active)} expirations overlap with requested range")

    # ---- Build task list ----
    tasks: list[tuple[str, str, str, date, date]] = []
    # (td_symbol, expiration, settlement, chunk_start, chunk_end)

    for exp_info in active:
        td_symbol  = exp_info["td_symbol"]
        expiration = exp_info["expiration"]
        settlement = exp_info["settlement"]

        # Cap end_date at expiration date (no data exists after expiry)
        exp_date = datetime.strptime(expiration, "%Y-%m-%d").date()
        eff_end  = min(end, exp_date)

        if eff_end < start:
            continue

        for chunk_start, chunk_end in chunk_date_range(start, eff_end, chunk_days=28):
            if _is_done(expiration, settlement, chunk_start, chunk_end):
                continue
            tasks.append((td_symbol, expiration, settlement, chunk_start, chunk_end))

    if not tasks:
        print("\nAll data already present — nothing to fetch.")
        return

    print(f"\n{len(tasks)} chunks to fetch ({MAX_WORKERS} concurrent workers)\n")

    # ---- Fetch ----
    total_rows = 0
    total_files = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        future_map = {
            pool.submit(_fetch_and_write, *task): task
            for task in tasks
        }
        with tqdm(total=len(tasks), unit="chunk", ncols=80) as pbar:
            for future in as_completed(future_map):
                task = future_map[future]
                td_sym, exp, sett, cs, ce = task
                try:
                    rows, files = future.result()
                    total_rows  += rows
                    total_files += files
                    pbar.set_postfix_str(f"{td_sym} {exp} {cs}→{ce} ({rows}r)")
                except Exception as exc:
                    errors += 1
                    log.warning("FAILED  %s %s %s %s→%s: %s", td_sym, exp, sett, cs, ce, exc)
                finally:
                    pbar.update(1)

    print(f"\nDone. {total_rows:,} rows → {total_files:,} parquet files written. {errors} errors.")


if __name__ == "__main__":
    main()
