#!/usr/bin/env python3
"""Fetch top crypto traders from Polymarket leaderboard API + enrichment.

Pipeline:
  1. Leaderboard API → PnL, volume, efficiency (crypto-category)
  2. User-stats API → trades, largestWin (all-market — caveat in UI)
  3. Dune SQL → USDC net capital flows on Polygon (received - sent)
  4. Derived: ROIC (PnL / net capital), style tag (efficiency > 10% → DIR)

Outputs data/profiles.json for the static frontend.

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

from common import REQUEST_DELAY, SESSION, fmt_money, fmt_pct, to_float
from dune import fetch_capital

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_FILE = REPO_ROOT / "data" / "profiles.json"

LEADERBOARD_URL = "https://data-api.polymarket.com/v1/leaderboard"
USER_STATS_URL = "https://data-api.polymarket.com/v1/user-stats"
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


def fetch_user_stats(wallet: str) -> dict:
    """Fetch per-user stats (all-market, not crypto-filtered).

    Returns: {"trades": int|None, "largest_win": float|None}
    """
    try:
        resp = SESSION.get(
            USER_STATS_URL,
            params={"proxyAddress": wallet},
            timeout=10,
        )
        if resp.status_code != 200:
            return {"trades": None, "largest_win": None}
        data = resp.json()
        return {
            "trades": data.get("trades"),
            "largest_win": to_float(data.get("largestWin")),
        }
    except (requests.RequestException, json.JSONDecodeError):
        return {"trades": None, "largest_win": None}


def main():
    parser = argparse.ArgumentParser(description="Fetch crypto leaderboard")
    parser.add_argument("--limit", type=int, default=800, help="Number of traders (default: 800)")
    args = parser.parse_args()

    # --- Phase 1: Leaderboard (crypto-category) ---
    print(f"Fetching top {args.limit} crypto traders from Polymarket leaderboard API")
    traders = fetch_leaderboard(args.limit)
    print(f"  Got {len(traders)} traders")

    results = []
    for t in traders:
        pnl = t.get("pnl")
        volume = t.get("vol")
        efficiency = (pnl / volume * 100) if (pnl is not None and volume and volume > 0) else None
        raw_username = t.get("userName") or ""
        wallet = t.get("proxyWallet", "")

        # API returns "0xABC...-1772168966873" (wallet + epoch-ms) for users
        # without a display name — strip the timestamp suffix
        if raw_username.startswith("0x") and "-" in raw_username:
            raw_username = raw_username.split("-")[0]

        username = raw_username or wallet[:10]

        if username and username != wallet[:10]:
            profile_url = f"https://polymarket.com/@{username}"
        else:
            profile_url = f"https://polymarket.com/@{wallet}"

        # Style tag: >10% efficiency → directional, ≤10% → HFT/market-maker
        if efficiency is not None:
            style = "DIR" if efficiency > 10 else "HFT/MM"
        else:
            style = None

        results.append({
            "name": username,
            "wallet": wallet,
            "profile_url": profile_url,
            "pnl": pnl,
            "volume": volume,
            "efficiency": efficiency,
            "style": style,
            # Placeholders — enriched in phases 2-3
            "trades": None,
            "largest_win": None,
            "first_seen": None,
            "total_received": None,
            "total_sent": None,
            "net_capital": None,
            "roic": None,
        })

    # Already sorted by PnL from API, but ensure it
    results.sort(key=lambda r: r["pnl"] if r["pnl"] is not None else float("-inf"), reverse=True)

    # --- Phase 2: User stats (all-market) ---
    print(f"\nFetching user stats for {len(results)} traders...")
    for i, r in enumerate(results):
        stats = fetch_user_stats(r["wallet"])
        r["trades"] = stats["trades"]
        r["largest_win"] = stats["largest_win"]
        if (i + 1) % 50 == 0:
            print(f"  Stats: {i + 1}/{len(results)}")
        time.sleep(REQUEST_DELAY)
    print(f"  Stats: done")

    # --- Phase 3: Dune capital data ---
    wallets = [r["wallet"] for r in results if r["wallet"]]
    capital = fetch_capital(wallets)

    for r in results:
        cap = capital.get(r["wallet"].lower(), {})
        if cap:
            r["first_seen"] = cap.get("first_seen")
            r["total_received"] = cap.get("total_received")
            r["total_sent"] = cap.get("total_sent")
            r["net_capital"] = cap.get("net_capital")
            # ROIC = PnL / Net Capital × 100
            # Note: net_capital = total_received - total_sent, which includes
            # trading flows (exchange payouts/buys). So the denominator is
            # "net USDC position" not pure "capital deployed." This is a
            # known trade-off — filtering by contract blacklist was inflating
            # deposits by 10-50x (see #30 capital flow inflation fix).
            net_cap = to_float(cap.get("net_capital")) or 0
            # Require minimum $5K net capital to avoid division explosion
            # when net_capital is near-zero (e.g. $145 → 780,000% ROIC)
            if net_cap >= 5000 and r["pnl"] is not None:
                r["roic"] = r["pnl"] / net_cap * 100

    # --- Output ---
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
