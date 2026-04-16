"""
push_index_ohlc.py — Push index OHLC rows from local SQLite to Postgres.

Run this AFTER switching Tailscale back to your primary VPS account.
Reads rows from the local SQLite file (written by fetch_index_ohlc.py)
and upserts them into the Postgres table defined in .env.

The target table is created automatically if it doesn't exist.

Usage:
    python push_index_ohlc.py
    (prompts for start and end date to select which rows to push)
"""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import date, datetime

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_PATH  = os.environ.get("INDEX_DB_PATH", "index_ohlc.db")
PG_HOST  = os.environ.get("PG_HOST", "")
PG_PORT  = int(os.environ.get("PG_PORT", "5432"))
PG_DB    = os.environ.get("PG_DB", "")
PG_USER  = os.environ.get("PG_USER", "")
PG_PASS  = os.environ.get("PG_PASSWORD", "")
PG_TABLE = os.environ.get("PG_TABLE", "index_ohlc")

TICKERS   = ["SPX", "VIX", "VIX3M", "VIX9D"]
OHLC_COLS = [f"{t.lower()}_{f}" for t in TICKERS for f in ("open", "high", "low", "close")]
SQLITE_COLS = ["ts"] + OHLC_COLS
PG_COLS     = ["trade_date", "quote_time"] + OHLC_COLS

BATCH_SIZE = 1000


# ---------------------------------------------------------------------------
# Postgres helpers
# ---------------------------------------------------------------------------

def pg_connect() -> psycopg2.extensions.connection:
    missing = [k for k, v in {
        "PG_HOST": PG_HOST, "PG_DB": PG_DB,
        "PG_USER": PG_USER, "PG_PASSWORD": PG_PASS,
    }.items() if not v]
    if missing:
        raise EnvironmentError(
            f"Missing required .env values: {', '.join(missing)}\n"
            "Fill them in before running push_index_ohlc.py."
        )
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS,
    )


def ensure_table(conn: psycopg2.extensions.connection) -> None:
    col_defs = ",\n    ".join(f"{c} DOUBLE PRECISION" for c in OHLC_COLS)
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {PG_TABLE} (
                trade_date DATE        NOT NULL,
                quote_time TIME        NOT NULL,
                {col_defs},
                PRIMARY KEY (trade_date, quote_time)
            )
        """)
    conn.commit()
    log.info("Table '%s' is ready.", PG_TABLE)


def upsert_to_pg(conn: psycopg2.extensions.connection, rows: list[tuple]) -> int:
    col_list     = ", ".join(PG_COLS)
    update_set   = ", ".join(f"{c} = EXCLUDED.{c}" for c in OHLC_COLS)
    sql = f"""
        INSERT INTO {PG_TABLE} ({col_list})
        VALUES %s
        ON CONFLICT (trade_date, quote_time) DO UPDATE SET {update_set}
    """
    total = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            psycopg2.extras.execute_values(cur, sql, batch)
            total += len(batch)
            log.info("  %d / %d rows upserted", total, len(rows))
    conn.commit()
    return total


# ---------------------------------------------------------------------------
# SQLite read
# ---------------------------------------------------------------------------

def read_sqlite(db_path: str, start: date, end: date) -> list[tuple]:
    if not os.path.exists(db_path):
        raise FileNotFoundError(
            f"SQLite file not found: {db_path}\n"
            "Run fetch_index_ohlc.py first."
        )
    start_ts = start.strftime("%Y-%m-%d") + "T00:00:00"
    end_ts   = end.strftime("%Y-%m-%d")   + "T23:59:59"
    sqlite_col_list = ", ".join(SQLITE_COLS)
    with sqlite3.connect(db_path) as conn:
        raw = conn.execute(
            f"SELECT {sqlite_col_list} FROM index_ohlc "
            f"WHERE ts >= ? AND ts <= ? ORDER BY ts",
            (start_ts, end_ts),
        ).fetchall()

    # Split "YYYY-MM-DDTHH:MM:SS" → ("YYYY-MM-DD", "HH:MM:SS") + remaining OHLC values
    rows = []
    for r in raw:
        ts_str     = r[0]                        # "2017-01-03T09:30:00"
        trade_date = ts_str[:10]                 # "2017-01-03"
        quote_time = ts_str[11:19]               # "09:30:00"
        rows.append((trade_date, quote_time) + r[1:])
    return rows


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
    print("=== Index OHLC Push to Postgres ===")
    print(f"SQLite   : {DB_PATH}")
    print(f"Postgres : {PG_HOST}:{PG_PORT}/{PG_DB}  →  table '{PG_TABLE}'")
    print()

    start = prompt_date("Start date (YYYY-MM-DD): ")
    end   = prompt_date("End   date (YYYY-MM-DD): ")
    if end < start:
        print("End date must be >= start date.")
        return

    rows = read_sqlite(DB_PATH, start, end)
    if not rows:
        log.warning("No rows found in SQLite for %s → %s. Nothing to push.", start, end)
        return
    log.info("Read %d rows from SQLite (%s → %s)", len(rows), start, end)

    try:
        pg_conn = pg_connect()
    except EnvironmentError as exc:
        print(f"\nConfiguration error:\n{exc}")
        return

    try:
        ensure_table(pg_conn)
        total = upsert_to_pg(pg_conn, rows)
        log.info("Done. %d rows pushed to %s.%s", total, PG_DB, PG_TABLE)
    finally:
        pg_conn.close()


if __name__ == "__main__":
    main()
