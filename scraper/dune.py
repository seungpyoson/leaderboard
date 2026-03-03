#!/usr/bin/env python3
"""Dune Analytics API client for Polymarket capital data.

Fetches USDC transfer history for proxy wallets on Polygon.
Uses net flow approach: total received - total sent = net capital.
No contract blacklist needed — internal routing cancels out.
Auth: DUNE_API_KEY env var (fallback: 1Password CLI).

Usage:
    from dune import fetch_capital
    capital = fetch_capital(["0xabc...", "0xdef..."])
    # => {"0xabc...": {"first_seen": "2023-01-15 ...", "total_received": 50000.0, "total_sent": 10000.0, "net_capital": 40000.0}}
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
import time

logger = logging.getLogger(__name__)

import requests

DUNE_API = "https://api.dune.com/api/v1"

# Bridged USDC.e on Polygon (6 decimals) — what Polymarket uses
USDC_POLYGON = "2791bca1f2de4661ed88a30c99a7a9449aa84174"

POLL_INTERVAL = 5   # seconds between status checks
MAX_POLL_TIME = 300  # 5 minutes max


def _get_api_key() -> str | None:
    """Get Dune API key from env or 1Password."""
    key = os.environ.get("DUNE_API_KEY")
    if key:
        return key
    try:
        result = subprocess.run(
            ["op", "item", "get", "dune-analytics-api",
             "--vault", "Development", "--fields", "api_key", "--reveal"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


_ADDR_RE = re.compile(r"^(0x)?[0-9a-fA-F]{40}$")


def _normalize_address(addr: str) -> str:
    """Validate and normalize an Ethereum address to lowercase 0x-prefixed hex."""
    if not _ADDR_RE.match(addr):
        raise ValueError(f"Invalid Ethereum address: {addr}")
    clean = addr.lower()
    return clean if clean.startswith("0x") else f"0x{clean}"


def _build_sql(wallets: list[str]) -> str:
    """Build DuneSQL query for USDC capital flows on Polygon.

    Net flow approach: counts ALL USDC transfers involving the wallet.
    No contract blacklist — internal routing (exchanges, relayers, etc.)
    cancels out in the received - sent math.
    """
    literals = ", ".join(_normalize_address(w) for w in wallets)

    return f"""
WITH transfers AS (
    SELECT "to" AS wallet,
           CAST(value AS DOUBLE) / 1e6 AS amount,
           evt_block_time,
           'in' AS direction
    FROM erc20_polygon.evt_Transfer
    WHERE contract_address = 0x{USDC_POLYGON}
      AND "to" IN ({literals})
    UNION ALL
    SELECT "from" AS wallet,
           CAST(value AS DOUBLE) / 1e6 AS amount,
           evt_block_time,
           'out' AS direction
    FROM erc20_polygon.evt_Transfer
    WHERE contract_address = 0x{USDC_POLYGON}
      AND "from" IN ({literals})
)
SELECT
    LOWER(CAST(wallet AS VARCHAR)) AS wallet,
    MIN(evt_block_time) AS first_seen,
    COALESCE(SUM(CASE WHEN direction = 'in' THEN amount END), 0) AS total_received,
    COALESCE(SUM(CASE WHEN direction = 'out' THEN amount END), 0) AS total_sent,
    COALESCE(SUM(CASE WHEN direction = 'in' THEN amount END), 0)
        - COALESCE(SUM(CASE WHEN direction = 'out' THEN amount END), 0) AS net_capital
FROM transfers
GROUP BY wallet
"""


def _headers(api_key: str) -> dict:
    return {"X-Dune-API-Key": api_key}


def _execute_sql(api_key: str, sql: str) -> str:
    """Submit inline SQL to Dune. Returns execution_id."""
    resp = requests.post(
        f"{DUNE_API}/sql/execute",
        headers=_headers(api_key),
        json={"sql": sql, "performance": "medium"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["execution_id"]


def _poll_results(api_key: str, execution_id: str) -> dict:
    """Poll until query completes, then fetch results."""
    start = time.time()
    while time.time() - start < MAX_POLL_TIME:
        resp = requests.get(
            f"{DUNE_API}/execution/{execution_id}/status",
            headers=_headers(api_key),
            timeout=15,
        )
        resp.raise_for_status()
        state = resp.json().get("state", "")

        if state == "QUERY_STATE_COMPLETED":
            results = requests.get(
                f"{DUNE_API}/execution/{execution_id}/results",
                headers=_headers(api_key),
                timeout=30,
            )
            results.raise_for_status()
            return results.json()

        if state in ("QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"):
            raise RuntimeError(f"Dune query {state}: {resp.json()}")

        time.sleep(POLL_INTERVAL)

    raise TimeoutError(f"Dune query timed out after {MAX_POLL_TIME}s")


def fetch_capital(wallets: list[str]) -> dict[str, dict]:
    """Fetch USDC capital data for proxy wallets.

    Returns: {wallet_lower: {"first_seen": str|None, "total_received": float, "total_sent": float, "net_capital": float}}
    """
    api_key = _get_api_key()
    if not api_key:
        print("  WARN: No DUNE_API_KEY — skipping capital data")
        return {}

    if not wallets:
        return {}

    valid = []
    for w in wallets:
        try:
            _normalize_address(w)
            valid.append(w)
        except ValueError:
            logger.warning("Skipping invalid wallet address: %s", w)
    if not valid:
        logger.warning("No valid wallet addresses — skipping Dune query")
        return {}

    print(f"  Querying Dune for {len(valid)} wallets...")
    sql = _build_sql(valid)

    try:
        execution_id = _execute_sql(api_key, sql)
        print(f"  Dune execution: {execution_id}")
        data = _poll_results(api_key, execution_id)
    except Exception as e:
        print(f"  ERROR: Dune query failed: {e}")
        return {}

    capital = {}
    for row in data.get("result", {}).get("rows", []):
        wallet = row.get("wallet", "").lower()
        if wallet:
            capital[wallet] = {
                "first_seen": row.get("first_seen"),
                "total_received": row.get("total_received", 0),
                "total_sent": row.get("total_sent", 0),
                "net_capital": row.get("net_capital", 0),
            }

    print(f"  Dune: got capital data for {len(capital)} wallets")
    return capital
