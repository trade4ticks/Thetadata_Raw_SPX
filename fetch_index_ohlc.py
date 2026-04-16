"""
fetch_index_ohlc.py — Backfill 5-minute index OHLC for SPX, VIX, VIX3M, VIX9D.

Connects to the secondary ThetaData terminal (THETADATA_INDEX_URL in .env).
Saves to a local SQLite file in wide format: one row per 5-min timeslice,
columns for each ticker's open/high/low/close.

Prerequisite: switch Tailscale to the secondary account before running.

Usage:
    python fetch_index_ohlc.py
    (prompts for start and end date)
"""

from __future__ import annotations

import logging
import os
import sqlite3
import time
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL = os.environ.get("THETADATA_INDEX_URL", "http://100.105.165.104:25503")
DB_PATH  = os.environ.get("INDEX_DB_PATH", "index_ohlc.db")

TICKERS = ["SPX", "VIX", "VIX3M", "VIX9D"]

OHLC_COLS = [f"{t.lower()}_{f}" for t in TICKERS for f in ("open", "high", "low", "close")]
ALL_COLS  = ["ts"] + OHLC_COLS


# ---------------------------------------------------------------------------
# HTTP + response parsing  (self-contained — no dependency on lib/client.py)
# ---------------------------------------------------------------------------

def _get(endpoint: str, params: dict, timeout: int = 60) -> dict | list:
    url    = f"{BASE_URL}{endpoint}"
    params = {**params, "format": "json"}
    try:
        resp = requests.get(url, params=params, timeout=timeout)
    except requests.exceptions.ConnectionError:
        raise ConnectionError(
            f"Cannot reach ThetaData at {BASE_URL}. "
            "Is Tailscale connected to the secondary account and the terminal running?"
        )
    if resp.status_code == 472:
        return {}                          # no data for this query — caller treats as empty
    if resp.status_code == 429:
        raise RuntimeError("rate_limited")
    if resp.status_code == 570:
        raise RuntimeError("too_large")
    resp.raise_for_status()
    return resp.json()


def _parse_rows(data: dict | list) -> list[dict]:
    """Convert any ThetaData v3 JSON shape into a flat list of row dicts."""
    if not data:
        return []
    # Columnar: {"header": {"format": [...]}, "response": [[...], ...]}
    if isinstance(data, dict) and "header" in data and "response" in data:
        fields = data["header"].get("format", [])
        return [dict(zip(fields, row)) for row in (data.get("response") or []) if row]
    # Parallel arrays: {"field": [v1, v2, ...], ...}
    if isinstance(data, dict):
        keys       = list(data.keys())
        first_list = next((data[k] for k in keys if isinstance(data[k], list)), None)
        if first_list is None:
            return []
        n = len(first_list)
        return [
            {k: (data[k][i] if isinstance(data[k], list) and i < len(data[k]) else data[k])
             for k in keys}
            for i in range(n)
        ]
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    return []


def _row_to_ts(row: dict) -> str | None:
    """Return an ISO timestamp string (YYYY-MM-DDTHH:MM:SS) from a response row."""
    ts = str(row.get("timestamp") or "")
    if ts and "T" in ts:
        return ts[:19]
    # Fallback: integer date (YYYYMMDD) + ms_of_day
    raw_date  = row.get("date")
    ms_of_day = row.get("ms_of_day")
    if not raw_date or ms_of_day is None:
        return None
    d       = str(int(raw_date))
    total_s = int(ms_of_day) // 1000
    h, rem  = divmod(total_s, 3600)
    mn, s   = divmod(rem, 60)
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}T{h:02d}:{mn:02d}:{s:02d}"


# ---------------------------------------------------------------------------
# Fetch one ticker  (auto-splits on 570, retries once on 429)
# ---------------------------------------------------------------------------

def fetch_ticker(symbol: str, start: date, end: date) -> pd.DataFrame:
    """
    Fetch 5-min OHLC bars for one index symbol over a date range.

    Returns a DataFrame with columns: ts, open, high, low, close.
    Returns an empty DataFrame if no data is available.
    """
    params = {
        "symbol":     symbol,
        "interval":   "5m",
        "start_date": start.strftime("%Y%m%d"),
        "end_date":   end.strftime("%Y%m%d"),
        "start_time": "09:30:00",
        "end_time":   "16:00:00",
    }
    try:
        data = _get("/v3/index/history/ohlc", params)
    except RuntimeError as exc:
        msg = str(exc)
        if msg == "too_large":
            mid = start + (end - start) // 2
            log.warning("570 too_large — splitting %s %s→%s at %s", symbol, start, end, mid)
            left  = fetch_ticker(symbol, start, mid)
            right = fetch_ticker(symbol, mid + timedelta(days=1), end)
            if left.empty and right.empty:
                return pd.DataFrame()
            return pd.concat([left, right], ignore_index=True)
        if msg == "rate_limited":
            log.warning("429 rate limited — sleeping 60s then retrying %s", symbol)
            time.sleep(60)
            data = _get("/v3/index/history/ohlc", params)
        else:
            raise

    rows = _parse_rows(data)
    if not rows:
        return pd.DataFrame()

    records = []
    for row in rows:
        ts = _row_to_ts(row)
        if ts is None:
            continue
        records.append({
            "ts":    ts,
            "open":  row.get("open"),
            "high":  row.get("high"),
            "low":   row.get("low"),
            "close": row.get("close"),
        })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    for col in ("open", "high", "low", "close"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------

def init_db(db_path: str) -> sqlite3.Connection:
    conn     = sqlite3.connect(db_path, timeout=30)
    col_defs = ",\n    ".join(f"{c} REAL" for c in OHLC_COLS)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS index_ohlc (
            ts TEXT PRIMARY KEY,
            {col_defs}
        )
    """)
    conn.commit()
    return conn


def upsert_wide(conn: sqlite3.Connection, wide: pd.DataFrame) -> int:
    if wide.empty:
        return 0

    # Guarantee every expected column is present (fill missing tickers with NULL)
    for c in ALL_COLS:
        if c not in wide.columns:
            wide[c] = None
    wide = wide[ALL_COLS]

    placeholders = ", ".join("?" * len(ALL_COLS))
    col_list     = ", ".join(ALL_COLS)
    update_set   = ", ".join(f"{c}=excluded.{c}" for c in OHLC_COLS)

    conn.executemany(
        f"""
        INSERT INTO index_ohlc ({col_list}) VALUES ({placeholders})
        ON CONFLICT(ts) DO UPDATE SET {update_set}
        """,
        wide.itertuples(index=False, name=None),
    )
    conn.commit()
    return len(wide)


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

def prompt_date(prompt: str) -> date:
    while True:
        raw = input(prompt).strip()
        try:
            return datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            print("  Expected YYYY-MM-DD — try again.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=== Index OHLC Backfill Fetch ===")
    print(f"Terminal : {BASE_URL}")
    print(f"DB       : {DB_PATH}")
    print()

    start = prompt_date("Start date (YYYY-MM-DD): ")
    end   = prompt_date("End   date (YYYY-MM-DD): ")
    if end < start:
        print("End date must be >= start date.")
        return

    conn = init_db(DB_PATH)
    log.info("SQLite ready: %s", DB_PATH)

    # --- Fetch each ticker ---
    ticker_dfs: dict[str, pd.DataFrame] = {}
    for ticker in TICKERS:
        log.info("Fetching %s  %s → %s …", ticker, start, end)
        df = fetch_ticker(ticker, start, end)
        log.info("  %s: %d bars", ticker, len(df))
        ticker_dfs[ticker] = df

    # --- Merge into wide format on ts ---
    wide: pd.DataFrame | None = None
    for ticker, df in ticker_dfs.items():
        if df.empty:
            log.warning("No data returned for %s — columns will be NULL", ticker)
            continue
        prefix  = ticker.lower()
        renamed = df.rename(columns={
            "open":  f"{prefix}_open",
            "high":  f"{prefix}_high",
            "low":   f"{prefix}_low",
            "close": f"{prefix}_close",
        })
        wide = renamed if wide is None else wide.merge(renamed, on="ts", how="outer")

    if wide is None or wide.empty:
        log.error("No data returned for any ticker — nothing written.")
        conn.close()
        return

    wide = wide.sort_values("ts").reset_index(drop=True)
    log.info("Wide table: %d rows, %d tickers", len(wide), sum(1 for t in TICKERS if f"{t.lower()}_close" in wide.columns))

    rows_written = upsert_wide(conn, wide)
    log.info("Upserted %d rows into %s", rows_written, DB_PATH)
    conn.close()
    print("\nDone. Switch Tailscale back to your primary account, then run push_index_ohlc.py.")


if __name__ == "__main__":
    main()
