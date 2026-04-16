"""
update_index_ohlc.py — Keep the Postgres index_ohlc table current via yfinance.

Fetches the last 7 days of 5-minute OHLC bars for SPX, VIX, VIX3M, and VIX9D,
then upserts into the index_ohlc table.  The 7-day window means a few missed
runs (or a weekend gap) are automatically healed on the next execution.

Designed to run inside run_pipeline.py on the VPS every ~5-10 minutes.
Exits immediately with code 0 when the market is closed — safe for cron at any hour.

Market hours: 09:30–16:00 ET, NYSE trading days.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import time

import pandas as pd
import psycopg2
import psycopg2.extras
import yfinance as yf
from dotenv import load_dotenv

from lib.market_hours import is_market_open, now_et

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

PG_HOST  = os.environ.get("PG_HOST", "")
PG_PORT  = int(os.environ.get("PG_PORT", "5432"))
PG_DB    = os.environ.get("PG_DB", "")
PG_USER  = os.environ.get("PG_USER", "")
PG_PASS  = os.environ.get("PG_PASSWORD", "")
PG_TABLE = os.environ.get("PG_TABLE", "index_ohlc")

# yfinance symbol → column prefix mapping
SYMBOL_MAP: dict[str, str] = {
    "^GSPC":  "spx",
    "^VIX":   "vix",
    "^VIX3M": "vix3m",
    "^VIX9D": "vix9d",
}

OHLC_COLS = [f"{p}_{f}" for p in SYMBOL_MAP.values() for f in ("open", "high", "low", "close")]
PG_COLS   = ["trade_date", "quote_time"] + OHLC_COLS

_ET_OPEN  = time(9, 30)
_ET_CLOSE = time(16, 0)


# ---------------------------------------------------------------------------
# yfinance fetch
# ---------------------------------------------------------------------------

def fetch_ticker(symbol: str, prefix: str) -> pd.DataFrame:
    """
    Fetch 7 days of 5-min OHLC for one symbol.
    Returns DataFrame with columns: trade_date (str), quote_time (str),
    {prefix}_open, {prefix}_high, {prefix}_low, {prefix}_close.
    Returns empty DataFrame if yfinance returns no data.
    """
    raw = yf.download(
        symbol,
        period="7d",
        interval="5m",
        auto_adjust=True,
        progress=False,
        multi_level_index=False,  # yfinance >=0.2.50: keep flat columns for single ticker
    )

    if raw.empty:
        return pd.DataFrame()

    # Flatten MultiIndex columns if present (older yfinance versions)
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [c[0] for c in raw.columns]

    # Normalise column names (yfinance capitalises them)
    raw.columns = [c.lower() for c in raw.columns]

    # Convert index to ET
    idx = raw.index
    if idx.tz is not None:
        idx = idx.tz_convert("America/New_York")
    else:
        idx = idx.tz_localize("America/New_York")
    raw.index = idx

    # Filter to regular market hours only
    raw = raw.between_time(_ET_OPEN.strftime("%H:%M"), _ET_CLOSE.strftime("%H:%M"))
    if raw.empty:
        return pd.DataFrame()

    return pd.DataFrame({
        "trade_date":      raw.index.strftime("%Y-%m-%d"),
        "quote_time":      raw.index.strftime("%H:%M:%S"),
        f"{prefix}_open":  pd.to_numeric(raw["open"],  errors="coerce").values,
        f"{prefix}_high":  pd.to_numeric(raw["high"],  errors="coerce").values,
        f"{prefix}_low":   pd.to_numeric(raw["low"],   errors="coerce").values,
        f"{prefix}_close": pd.to_numeric(raw["close"], errors="coerce").values,
    })


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------

def pg_connect() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS,
    )


def upsert_to_pg(conn: psycopg2.extensions.connection, wide: pd.DataFrame) -> int:
    for c in PG_COLS:
        if c not in wide.columns:
            wide[c] = None
    wide = wide[PG_COLS]

    col_list   = ", ".join(PG_COLS)
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in OHLC_COLS)
    sql = f"""
        INSERT INTO {PG_TABLE} ({col_list})
        VALUES %s
        ON CONFLICT (trade_date, quote_time) DO UPDATE SET {update_set}
    """
    rows = list(wide.itertuples(index=False, name=None))
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows)
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    now = now_et()

    if not is_market_open(now):
        log.info("Market closed (%s ET) — nothing to do.", now.strftime("%H:%M:%S"))
        sys.exit(0)

    log.info("Updating index OHLC — %s ET", now.strftime("%Y-%m-%d %H:%M:%S"))

    # --- Fetch each ticker and merge into wide format ---
    wide: pd.DataFrame | None = None

    for symbol, prefix in SYMBOL_MAP.items():
        log.info("Fetching %s (%s) …", symbol, prefix)
        df = fetch_ticker(symbol, prefix)
        if df.empty:
            log.warning("  %s: no data returned", symbol)
            continue
        log.info("  %s: %d bars", symbol, len(df))
        wide = df if wide is None else wide.merge(df, on=["trade_date", "quote_time"], how="outer")

    if wide is None or wide.empty:
        log.warning("No data returned for any ticker — skipping upsert.")
        sys.exit(0)

    wide = wide.sort_values(["trade_date", "quote_time"]).reset_index(drop=True)
    log.info("Wide table: %d rows", len(wide))

    # --- Upsert to Postgres ---
    conn = pg_connect()
    try:
        n = upsert_to_pg(conn, wide)
        log.info("Upserted %d rows into %s", n, PG_TABLE)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
