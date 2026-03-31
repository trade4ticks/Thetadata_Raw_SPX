"""
Parquet storage layer for SPX options data.

Directory layout:
  {DATA_DIR}/
    {YYYYMMDD_trading_date}/
      {YYYYMMDD_expiration}/
        AM.parquet   — SPX  (AM-settled monthly)
        PM.parquet   — SPXW (PM-settled weekly / monthly)

Each parquet file contains all strikes and rights for that (trading_date, expiration, settlement).

Columns:
  timestamp        str     ISO 8601, e.g. "2025-01-03T09:35:00.000"
  strike           float   Strike price in dollars
  right            str     "C" or "P"
  settlement       str     "AM" or "PM"
  bid              float
  ask              float
  delta            float
  theta            float
  vega             float
  rho              float
  implied_vol      float
  underlying_price float
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import DATA_DIR

_SCHEMA = pa.schema([
    ("timestamp",        pa.string()),
    ("strike",           pa.float64()),
    ("right",            pa.string()),
    ("settlement",       pa.string()),
    ("bid",              pa.float64()),
    ("ask",              pa.float64()),
    ("delta",            pa.float64()),
    ("theta",            pa.float64()),
    ("vega",             pa.float64()),
    ("rho",              pa.float64()),
    ("implied_vol",      pa.float64()),
    ("underlying_price", pa.float64()),
])


def _path(trading_date: str, expiration: str, settlement: str) -> Path:
    """
    trading_date : "YYYYMMDD"
    expiration   : "YYYYMMDD" or "YYYY-MM-DD"
    settlement   : "AM" or "PM"
    """
    exp = expiration.replace("-", "")
    return Path(DATA_DIR) / trading_date / exp / f"{settlement}.parquet"


def exists(trading_date: str, expiration: str, settlement: str) -> bool:
    return _path(trading_date, expiration, settlement).exists()


def read(trading_date: str, expiration: str, settlement: str) -> pd.DataFrame:
    p = _path(trading_date, expiration, settlement)
    if not p.exists():
        return pd.DataFrame()
    return pd.read_parquet(p)


def write(trading_date: str, expiration: str, settlement: str, df: pd.DataFrame) -> None:
    """Write df to parquet, overwriting any existing file for this (date, exp, settlement)."""
    if df.empty:
        return
    p = _path(trading_date, expiration, settlement)
    p.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df[list(_SCHEMA.names)], schema=_SCHEMA, preserve_index=False)
    pq.write_table(table, p, compression="snappy")


def append(trading_date: str, expiration: str, settlement: str, df: pd.DataFrame) -> None:
    """
    Append new rows to an existing parquet file, deduplicating on (timestamp, strike, right).
    Creates the file if it doesn't exist.
    """
    if df.empty:
        return
    existing = read(trading_date, expiration, settlement)
    if existing.empty:
        write(trading_date, expiration, settlement, df)
        return

    combined = pd.concat([existing, df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["timestamp", "strike", "right"], keep="last")
    combined = combined.sort_values(["timestamp", "strike", "right"]).reset_index(drop=True)
    write(trading_date, expiration, settlement, combined)


def latest_timestamp(trading_date: str, expiration: str, settlement: str) -> str | None:
    """Return the latest timestamp string in the parquet file, or None if no file."""
    df = read(trading_date, expiration, settlement)
    if df.empty or "timestamp" not in df.columns:
        return None
    return df["timestamp"].max()
