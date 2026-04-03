"""
ThetaData Terminal v3 HTTP client for SPX options data.

Connects via HTTP to the ThetaData terminal (typically on VPS via Tailscale).
No authentication header required — access control is enforced by the Tailscale VPN.

Endpoints used:
  /v3/option/history/greeks/first_order
    Fetches 5-minute first-order greeks for one expiration, all strikes.

  /v3/option/list/expirations
    Returns all available expirations for a given root symbol.

Response formats (ThetaData v3 returns one of two shapes):
  - Parallel arrays:  {"field": [v1, v2, ...], ...}
  - Columnar:         {"header": {"format": [...]}, "response": [[row], ...]}

Timestamp quirk:
  Short requests return a proper ISO "timestamp" field.
  Multi-day requests sometimes return separate integer "date" (YYYYMMDD)
  + "ms_of_day" (ms from midnight ET) fields instead. Both are handled.

HTTP error codes (from S2/ThetaData docs):
  429 → RateLimitError          — back off and retry
  472 → NoDataError             — no data for query, skip silently
  474 → ServerDisconnectedError — terminal lost backend, retry
  570 → LargeRequestError       — chunk too big, auto-halve and retry
"""

from __future__ import annotations

import logging
import time
from datetime import date, timedelta

import pandas as pd
import requests

from config import THETADATA_BASE_URL

log = logging.getLogger(__name__)

# SPXW = PM-settled (weeklies + PM monthlies on 3rd Friday)
# SPX  = AM-settled (traditional monthly, 3rd Friday)
ROOTS: dict[str, str] = {
    "SPXW": "PM",
    "SPX":  "AM",
}

_BULK_TIMEOUT = 300   # seconds; bulk chain fetches can be large


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class ThetaDataError(Exception):
    pass

class NoDataError(ThetaDataError):
    """HTTP 472 — no data available for this query."""

class RateLimitError(ThetaDataError):
    """HTTP 429 — OS-level rate limiting by ThetaData."""

class ServerDisconnectedError(ThetaDataError):
    """HTTP 474 — terminal lost its backend connection."""

class LargeRequestError(ThetaDataError):
    """HTTP 570 — request spans too much data; reduce chunk size."""


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------

_STATUS_EXCEPTIONS = {
    429: RateLimitError,
    472: NoDataError,
    474: ServerDisconnectedError,
    570: LargeRequestError,
}


def _get(endpoint: str, params: dict, timeout: int = 30) -> dict | list:
    """Base GET request. Raises typed exceptions for known ThetaData error codes."""
    params = {**params, "format": "json"}
    url = f"{THETADATA_BASE_URL}{endpoint}"
    try:
        resp = requests.get(url, params=params, timeout=timeout)
    except requests.exceptions.ConnectionError:
        raise ConnectionError(
            f"Cannot reach ThetaData at {THETADATA_BASE_URL}. "
            "Is Tailscale connected and the Terminal running?"
        )
    exc_cls = _STATUS_EXCEPTIONS.get(resp.status_code)
    if exc_cls:
        raise exc_cls(f"HTTP {resp.status_code}: {resp.text[:200]}")
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def _parse_rows(data: dict | list) -> list[dict]:
    """Convert any ThetaData v3 JSON response into a list of row dicts."""
    if not data:
        return []
    # Columnar: {"header": {"format": [...]}, "response": [[...], ...]}
    if isinstance(data, dict) and "header" in data and "response" in data:
        fields = data["header"].get("format", [])
        return [dict(zip(fields, row)) for row in (data.get("response") or []) if row]
    # Parallel arrays: {"field": [v1, v2, ...], ...}
    if isinstance(data, dict):
        keys = list(data.keys())
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


def _row_to_timestamp(row: dict) -> str | None:
    """
    Extract an ISO timestamp string from a response row.

    ThetaData returns an ISO "timestamp" field for short requests but
    switches to separate integer "date" (YYYYMMDD) + "ms_of_day"
    (milliseconds from midnight ET) for longer multi-day responses.
    Both are normalised to "YYYY-MM-DDTHH:MM:SS.mmm" here.
    """
    ts = str(row.get("timestamp") or "")
    if ts and "T" in ts:
        return ts[:23]

    # Fallback: reconstruct from date + ms_of_day
    raw_date  = row.get("date")
    ms_of_day = row.get("ms_of_day")
    if not raw_date or ms_of_day is None:
        return None

    d       = str(int(raw_date))       # "20260102"
    total_s = int(ms_of_day) // 1000
    ms_rem  = int(ms_of_day) % 1000
    h, rem  = divmod(total_s, 3600)
    mn, s   = divmod(rem, 60)
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}T{h:02d}:{mn:02d}:{s:02d}.{ms_rem:03d}"


# ---------------------------------------------------------------------------
# Expiration list
# ---------------------------------------------------------------------------

def list_expirations() -> list[dict]:
    """
    Fetch all available expirations for SPXW (PM) and SPX (AM).

    Returns list of dicts:
        [{"td_symbol": "SPXW", "expiration": "2026-01-17", "settlement": "PM"}, ...]
    Sorted by (expiration, settlement).
    """
    data = _get("/v3/option/list/expirations", {"symbol": "SPXW,SPX"})
    rows = _parse_rows(data)

    results = []
    for row in rows:
        sym = str(row.get("symbol") or "").upper()
        exp = str(row.get("expiration") or "")[:10]  # YYYY-MM-DD
        if sym in ROOTS and exp:
            results.append({
                "td_symbol":  sym,
                "expiration": exp,
                "settlement": ROOTS[sym],
            })

    return sorted(results, key=lambda r: (r["expiration"], r["settlement"]))


def list_active_expirations(trading_day: date) -> list[dict]:
    """
    Discover which expirations actually had data on a specific trading day
    by querying the EOD endpoint with expiration=*.

    Makes two calls (SPXW + SPX), extracts unique expirations from the
    response, and returns in the same format as list_expirations().

    Returns:
        [{"td_symbol": "SPXW", "expiration": "2026-01-17", "settlement": "PM"}, ...]
    """
    day_str = trading_day.strftime("%Y%m%d")
    results = []
    seen = set()

    for td_symbol, settlement in ROOTS.items():
        params = {
            "symbol":     td_symbol,
            "expiration": "*",
            "start_date": day_str,
            "end_date":   day_str,
        }
        try:
            data = _get("/v3/option/history/eod", params, timeout=120)
        except NoDataError:
            continue
        except Exception as e:
            log.warning("EOD lookup failed for %s on %s: %s", td_symbol, day_str, e)
            continue

        rows = _parse_rows(data)
        for row in rows:
            exp = str(row.get("expiration") or "")[:10]
            if not exp:
                continue
            key = (td_symbol, exp)
            if key in seen:
                continue
            seen.add(key)
            results.append({
                "td_symbol":  td_symbol,
                "expiration": exp,
                "settlement": settlement,
            })

    return sorted(results, key=lambda r: (r["expiration"], r["settlement"]))


# ---------------------------------------------------------------------------
# History fetch  (with auto-retry on 570 and backoff on 429/474)
# ---------------------------------------------------------------------------

def fetch_greeks_history(
    td_symbol: str,
    expiration: str,
    start_date: date,
    end_date: date,
    _chunk_days: int = 28,
) -> pd.DataFrame:
    """
    Fetch 5-minute first-order greeks for one expiration, all strikes.

    Handles:
      - 472 NoData         → returns empty DataFrame (logged at DEBUG)
      - 429 RateLimit      → sleeps 60 s and retries once
      - 474 Disconnected   → sleeps 10 s and retries once
      - 570 LargeRequest   → splits into two half-size chunks and merges

    Args:
        td_symbol:  "SPXW" or "SPX"
        expiration: "YYYY-MM-DD"
        start_date / end_date: inclusive date range (caller chunks to ≤28 days)

    Returns:
        DataFrame with columns:
            timestamp, strike, right, settlement,
            bid, ask, delta, theta, vega, rho, implied_vol, underlying_price
        Empty DataFrame on no-data or unrecoverable error.
    """
    settlement = ROOTS.get(td_symbol.upper(), "PM")
    exp_nodash = expiration.replace("-", "")

    params = {
        "symbol":     td_symbol.upper(),
        "expiration": exp_nodash,
        "strike":     "*",
        "right":      "both",
        "interval":   "5m",
        "start_date": start_date.strftime("%Y%m%d"),
        "end_date":   end_date.strftime("%Y%m%d"),
        "start_time": "09:35:00",
        "end_time":   "16:00:00",
    }

    try:
        data = _get("/v3/option/history/greeks/first_order", params, timeout=_BULK_TIMEOUT)

    except NoDataError:
        log.debug("No data: %s %s %s→%s", td_symbol, expiration, start_date, end_date)
        return pd.DataFrame()

    except RateLimitError:
        log.warning("Rate limited — sleeping 60s then retrying once")
        time.sleep(60)
        try:
            data = _get("/v3/option/history/greeks/first_order", params, timeout=_BULK_TIMEOUT)
        except Exception as e:
            log.error("Retry after rate limit failed: %s", e)
            return pd.DataFrame()

    except ServerDisconnectedError:
        log.warning("Server disconnected — sleeping 10s then retrying once")
        time.sleep(10)
        try:
            data = _get("/v3/option/history/greeks/first_order", params, timeout=_BULK_TIMEOUT)
        except Exception as e:
            log.error("Retry after disconnect failed: %s", e)
            return pd.DataFrame()

    except LargeRequestError:
        # Split into two halves and merge
        mid = start_date + (end_date - start_date) / 2
        log.warning(
            "LargeRequestError: %s %s %s→%s — splitting at %s",
            td_symbol, expiration, start_date, end_date, mid,
        )
        left  = fetch_greeks_history(td_symbol, expiration, start_date, mid)
        right = fetch_greeks_history(td_symbol, expiration, mid + timedelta(days=1), end_date)
        if left.empty and right.empty:
            return pd.DataFrame()
        return pd.concat([left, right], ignore_index=True)

    except Exception as e:
        log.error("Unexpected error fetching %s %s %s→%s: %s",
                  td_symbol, expiration, start_date, end_date, e)
        return pd.DataFrame()

    # --- Parse rows ---
    rows = _parse_rows(data)
    if not rows:
        return pd.DataFrame()

    records = []
    for row in rows:
        bid = row.get("bid")
        ask = row.get("ask")
        if bid == 0 and ask == 0:
            continue

        ts = _row_to_timestamp(row)
        if ts is None:
            continue

        right_raw = str(row.get("right") or "").strip().lower()
        right_val = (
            "C" if right_raw in ("c", "call") else
            "P" if right_raw in ("p", "put") else
            right_raw.upper()
        )

        records.append({
            "timestamp":        ts,
            "strike":           row.get("strike"),
            "right":            right_val,
            "settlement":       settlement,
            "bid":              bid,
            "ask":              ask,
            "delta":            row.get("delta"),
            "theta":            row.get("theta"),
            "vega":             row.get("vega"),
            "rho":              row.get("rho"),
            "implied_vol":      row.get("implied_vol"),
            "underlying_price": row.get("underlying_price"),
        })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    df["bid"]    = pd.to_numeric(df["bid"],    errors="coerce")
    df["ask"]    = pd.to_numeric(df["ask"],    errors="coerce")
    for col in ("delta", "theta", "vega", "rho", "implied_vol", "underlying_price"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ---------------------------------------------------------------------------
# Date chunking
# ---------------------------------------------------------------------------

def chunk_date_range(start: date, end: date, chunk_days: int = 28) -> list[tuple[date, date]]:
    """Split a date range into chunks of at most chunk_days calendar days."""
    chunks = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks


# ---------------------------------------------------------------------------
# Connection test
# ---------------------------------------------------------------------------

def test_connection() -> bool:
    """Quick smoke test: check if ThetaData terminal is reachable."""
    try:
        data = _get("/v3/option/list/expirations", {"symbol": "SPXW"}, timeout=10)
        return bool(_parse_rows(data))
    except Exception as e:
        log.error("Connection test failed: %s", e)
        return False
