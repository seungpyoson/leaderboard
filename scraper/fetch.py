#!/usr/bin/env python3
"""Fetch top crypto traders directly from Polymarket leaderboard API.

Single API call — no curated accounts list needed. Polymarket handles the
category filtering and PnL computation. Outputs data/profiles.json for the
static frontend.

Usage:
    python3 scraper/fetch.py [--limit 100]
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from common import REQUEST_DELAY, SESSION, fmt_money, fmt_pct

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_FILE = REPO_ROOT / "data" / "profiles.json"

LEADERBOARD_URL = "https://data-api.polymarket.com/v1/leaderboard"
PAGE_SIZE = 50  # API max per request


def fetch_leaderboard(limit: int) -> list[dict]:
    """Paginate the crypto leaderboard API. Returns up to `limit` traders."""
    all_traders = []
    pages_needed = (limit + PAGE_SIZE - 1) // PAGE_SIZE

    for page in range(pages_needed):
        offset = page * PAGE_SIZE
        remaining = limit - len(all_traders)
        batch_size = min(PAGE_SIZE, remaining)

        try:
            resp = SESSION.get(
                LEADERBOARD_URL,
                params={
                    "category": "CRYPTO",
                    "timePeriod": "ALL",
                    "orderBy": "PNL",
                    "limit": batch_size,
                    "offset": offset,
                },
                timeout=15,
            )
        except requests.RequestException as e:
            print(f"  ERROR: page {page}: {e}")
            break

        if resp.status_code != 200:
            print(f"  WARN: page {page} HTTP {resp.status_code}")
            break

        try:
            traders = resp.json()
        except json.JSONDecodeError:
            print(f"  ERROR: page {page} invalid JSON")
            break

        if not isinstance(traders, list) or not traders:
            break

        all_traders.extend(traders)

        if len(traders) < batch_size:
            break  # last page

        if page < pages_needed - 1:
            time.sleep(REQUEST_DELAY)

    return all_traders


def main():
    parser = argparse.ArgumentParser(description="Fetch crypto leaderboard")
    parser.add_argument("--limit", type=int, default=500, help="Number of traders (default: 500)")
    args = parser.parse_args()

    print(f"Fetching top {args.limit} crypto traders from Polymarket leaderboard API")

    traders = fetch_leaderboard(args.limit)
    print(f"  Got {len(traders)} traders")

    results = []
    for t in traders:
        pnl = t.get("pnl")
        volume = t.get("vol")
        efficiency = (pnl / volume * 100) if (pnl is not None and volume and volume > 0) else None
        username = t.get("userName") or t.get("proxyWallet", "")[:10]
        wallet = t.get("proxyWallet", "")

        # Build profile URL
        if username and username != wallet[:10]:
            profile_url = f"https://polymarket.com/@{username}"
        else:
            profile_url = f"https://polymarket.com/@{wallet}"

        results.append({
            "name": username,
            "wallet": wallet,
            "profile_url": profile_url,
            "pnl": pnl,
            "volume": volume,
            "efficiency": efficiency,
        })

    # Already sorted by PnL from API, but ensure it
    results.sort(key=lambda r: r["pnl"] if r["pnl"] is not None else float("-inf"), reverse=True)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    output = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "accounts": len(results),
        "profiles": results,
    }
    OUTPUT_FILE.write_text(json.dumps(output, indent=2))

    print(f"\nTop 5:")
    for i, r in enumerate(results[:5]):
        print(f"  {i+1}. {r['name']:<25} PnL: {fmt_money(r['pnl'])} | Vol: {fmt_money(r['volume'])} | Eff: {fmt_pct(r['efficiency'])}")

    print(f"\nOutput: {OUTPUT_FILE} ({len(results)} traders)")


if __name__ == "__main__":
    main()
