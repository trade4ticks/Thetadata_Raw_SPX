"""
ThetaData Terminal v3 HTTP client for SPX options data.

Connects via HTTP to the ThetaData terminal (typically on VPS via Tailscale).
No authentication header required — access control is enforced by the Tailscale VPN.

Endpoint used:
  /v3/option/history/greeks/first_order
    Fetches 5-minute OHLC greeks for one expiration, all strikes, up to 1 month per call.

  /v3/option/list/expirations
    Returns all available expirations for a given root symbol.

Response format: ThetaData v3 returns one of two JSON shapes:
  - Parallel arrays:  {"field": [v1, v2, ...], ...}
  - Columnar:         {"header": {"format": [...]}, "response": [[row], [row], ...]}
Both are handled by _parse_rows().
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd
import requests

from config import THETADATA_BASE_URL

# SPXW = PM-settled (weeklies + PM monthlies on 3rd Friday)
# SPX  = AM-settled (traditional monthly, 3rd Friday)
ROOTS: dict[str, str] = {
    "SPXW": "PM",
    "SPX":  "AM",
}

# Timeout for bulk chain fetches (can return hundreds of thousands of rows)
_BULK_TIMEOUT = 300


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get(endpoint: str, params: dict, timeout: int = 30) -> dict | list:
    params = {**params, "format": "json"}
    url = f"{THETADATA_BASE_URL}{endpoint}"
    try:
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        raise ConnectionError(
            f"Cannot reach ThetaData at {THETADATA_BASE_URL}. "
            "Is Tailscale connected and the Terminal running?"
        )


def _parse_rows(data: dict | list) -> list[dict]:
    """Convert any ThetaData v3 JSON response into a list of row dicts."""
    if not data:
        return []
    # Columnar format: {"header": {"format": [...]}, "response": [[...], ...]}
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
    # List of row dicts
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    return []


# ---------------------------------------------------------------------------
# Expiration list
# ---------------------------------------------------------------------------

def list_expirations() -> list[dict]:
    """
    Fetch all available expirations for SPXW and SPX.

    Returns list of dicts: [{"td_symbol": "SPXW", "expiration": "2025-01-17", "settlement": "PM"}, ...]
    Expirations are sorted by date.
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


# ---------------------------------------------------------------------------
# History fetch
# ---------------------------------------------------------------------------

def fetch_greeks_history(
    td_symbol: str,
    expiration: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """
    Fetch 5-minute first-order greeks for one expiration, all strikes.

    - td_symbol: "SPXW" or "SPX"
    - expiration: "YYYY-MM-DD"
    - Date range must be <= 28 days (caller is responsible for chunking).
    - Returns DataFrame with columns:
        timestamp, strike, right, settlement,
        bid, ask, delta, theta, vega, rho, implied_vol, underlying_price
    - Returns empty DataFrame if no data or on error.
    """
    exp_nodash = expiration.replace("-", "")
    settlement = ROOTS.get(td_symbol.upper(), "PM")

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
    except Exception:
        return pd.DataFrame()

    rows = _parse_rows(data)
    if not rows:
        return pd.DataFrame()

    records = []
    for row in rows:
        bid = row.get("bid")
        ask = row.get("ask")
        # Skip zero-quote rows (erroneous open-bar artifacts)
        if bid == 0 and ask == 0:
            continue

        ts = str(row.get("timestamp") or "")
        if not ts or "T" not in ts:
            continue

        right_raw = str(row.get("right") or "").strip().lower()
        right = "C" if right_raw in ("c", "call") else "P" if right_raw in ("p", "put") else right_raw.upper()

        records.append({
            "timestamp":        ts[:23],   # trim to milliseconds
            "strike":           row.get("strike"),
            "right":            right,
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
    """Split a date range into chunks of at most chunk_days days."""
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
        print(f"Connection test failed: {e}")
        return False
