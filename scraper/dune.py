#!/usr/bin/env python3
"""Dune Analytics API client for Polymarket capital data.

Fetches USDC deposit/withdrawal history for proxy wallets on Polygon.
Auth: DUNE_API_KEY env var (fallback: 1Password CLI).

Usage:
    from dune import fetch_capital
    capital = fetch_capital(["0xabc...", "0xdef..."])
    # => {"0xabc...": {"first_deposit": "2023-01-15 ...", "deposits": 50000.0, "withdrawn": 10000.0}}
"""

from __future__ import annotations

import os
import re
import subprocess
import time

import requests

DUNE_API = "https://api.dune.com/api/v1"

# Bridged USDC.e on Polygon (6 decimals) — what Polymarket uses
USDC_POLYGON = "2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

# Polymarket exchange contracts — USDC flows to/from these are trading, not capital
# CTF Exchange (binary markets): https://polygonscan.com/address/0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e
# NegRisk CTF Exchange (multi-outcome): https://polygonscan.com/address/0xc5d563a36ae78145c45a50134d48a1215220f80a
EXCHANGE_CONTRACTS = [
    "4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
    "c5d563a36ae78145c45a50134d48a1215220f80a",
]

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

    Excludes USDC transfers to/from Polymarket exchange contracts
    (CTF Exchange, NegRisk CTF Exchange) so only external capital
    movements are counted — not trading activity.
    """
    literals = ", ".join(_normalize_address(w) for w in wallets)
    excludes = ", ".join(f"0x{c}" for c in EXCHANGE_CONTRACTS)

    return f"""
WITH deposits AS (
    SELECT "to" AS wallet,
           CAST(value AS DOUBLE) / 1e6 AS amount,
           evt_block_time
    FROM erc20_polygon.evt_Transfer
    WHERE contract_address = 0x{USDC_POLYGON}
      AND "to" IN ({literals})
      AND "from" NOT IN ({excludes})
),
withdrawals AS (
    SELECT "from" AS wallet,
           CAST(value AS DOUBLE) / 1e6 AS amount,
           evt_block_time
    FROM erc20_polygon.evt_Transfer
    WHERE contract_address = 0x{USDC_POLYGON}
      AND "from" IN ({literals})
      AND "to" NOT IN ({excludes})
),
combined AS (
    SELECT wallet, amount, evt_block_time, 'deposit' AS direction FROM deposits
    UNION ALL
    SELECT wallet, amount, evt_block_time, 'withdrawal' AS direction FROM withdrawals
)
SELECT
    LOWER(CAST(wallet AS VARCHAR)) AS wallet,
    MIN(CASE WHEN direction = 'deposit' THEN evt_block_time END) AS first_deposit,
    COALESCE(SUM(CASE WHEN direction = 'deposit' THEN amount END), 0) AS total_deposits,
    COALESCE(SUM(CASE WHEN direction = 'withdrawal' THEN amount END), 0) AS total_withdrawn
FROM combined
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

    Returns: {wallet_lower: {"first_deposit": str|None, "deposits": float, "withdrawn": float}}
    """
    api_key = _get_api_key()
    if not api_key:
        print("  WARN: No DUNE_API_KEY — skipping capital data")
        return {}

    if not wallets:
        return {}

    print(f"  Querying Dune for {len(wallets)} wallets...")
    sql = _build_sql(wallets)

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
                "first_deposit": row.get("first_deposit"),
                "deposits": row.get("total_deposits", 0),
                "withdrawn": row.get("total_withdrawn", 0),
            }

    print(f"  Dune: got capital data for {len(capital)} wallets")
    return capital
