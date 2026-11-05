#!/usr/bin/env python3
"""Fetch top crypto traders from Polymarket leaderboard API + enrichment.

Pipeline:
  1. Leaderboard API → PnL (crypto-category; v2 volume is shares, not USDC)
  2. User-stats API → markets traded, biggest win (all-market)
  3. Dune SQL → USDC net capital flows on Polygon (received - sent)
  4. Derived: ROIC (PnL / net capital)

Outputs data/profiles.json for the static frontend.

Usage:
    python3 scraper/fetch.py [--limit 800]
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from common import MIN_LEADERBOARD_COUNT, REQUEST_DELAY, SESSION, atomic_write, fetch_leaderboard, fmt_money, fmt_pct
from dune import fetch_capital
from external import UserStats, load_user_stats_response

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_FILE = REPO_ROOT / "data" / "profiles.json"

USER_STATS_URL = "https://data-api.polymarket.com/v2/user-stats"
USER_STATS_MAX_ATTEMPTS = 6
USER_STATS_RETRY_BASE_SECONDS = 1
# 136 past good snapshots had capital coverage from 0.9762 upward.
MIN_CAPITAL_COVERAGE = 0.97
MIN_USER_STATS_COVERAGE = MIN_CAPITAL_COVERAGE


def fetch_user_stats(wallet: str) -> UserStats | None:
    """Fetch per-user stats (all-market, not crypto-filtered).

    Returns None only for the API's documented ``data: null`` unknown user.
    Transient request failures are retried by the caller; malformed data refuses.
    """
    resp = SESSION.get(USER_STATS_URL, params={"user": wallet}, timeout=10)
    resp.raise_for_status()
    return load_user_stats_response(resp, wallet)


def main():
    parser = argparse.ArgumentParser(description="Fetch crypto leaderboard")
    parser.add_argument("--limit", type=int, default=MIN_LEADERBOARD_COUNT, help="Number of traders (default: 800)")
    args = parser.parse_args()
    if args.limit < MIN_LEADERBOARD_COUNT:
        parser.error(f"--limit must be at least {MIN_LEADERBOARD_COUNT} for tracked output")

    # --- Phase 1: Leaderboard (crypto-category) ---
    print(f"Fetching top {args.limit} crypto traders from Polymarket leaderboard API")
    traders = fetch_leaderboard(args.limit)
    print(f"  Got {len(traders)} traders")
    if len(traders) < args.limit:
        raise SystemExit(f"ERROR: short leaderboard fetch: {len(traders)}/{args.limit}; profiles.json preserved")

    results = []
    for t in traders:
        pnl = t.pnl
        # v2 volume is shares, not USDC; the existing volume and efficiency
        # columns require USDC and therefore cannot use it.
        volume = efficiency = style = None
        raw_username = t.user_name
        wallet = t.user_id

        # API returns "0xABC...-1772168966873" (wallet + epoch-ms) for users
        # without a display name — strip the timestamp suffix
        if raw_username.startswith("0x") and "-" in raw_username:
            raw_username = raw_username.split("-")[0]

        username = raw_username or wallet[:10]

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

    print("::warning title=Volume unavailable::Polymarket v2 board reports shares, not USDC; volume and efficiency are unavailable")

    # Already sorted by PnL from API, but ensure it
    results.sort(key=lambda r: r["pnl"], reverse=True)

    # --- Phase 2: User stats (all-market) ---
    print(f"\nFetching user stats for {len(results)} traders...")
    known_stats = 0
    for i, r in enumerate(results):
        stats = None
        for attempt in range(USER_STATS_MAX_ATTEMPTS):
            try:
                stats = fetch_user_stats(r["wallet"])
                break
            except (requests.RequestException, json.JSONDecodeError) as exc:
                if attempt + 1 == USER_STATS_MAX_ATTEMPTS:
                    raise RuntimeError(
                        f"User stats fetch failed for {r['wallet']} after "
                        f"{USER_STATS_MAX_ATTEMPTS} attempts; profiles.json preserved"
                    ) from exc
                time.sleep(USER_STATS_RETRY_BASE_SECONDS * 2 ** attempt)
        if stats is not None:
            known_stats += 1
            r["trades"] = stats.trades
            r["largest_win"] = stats.largest_win
        if (i + 1) % 50 == 0:
            print(f"  Stats: {i + 1}/{len(results)}")
        time.sleep(REQUEST_DELAY)
    if known_stats / len(results) < MIN_USER_STATS_COVERAGE:
        raise RuntimeError(
            f"User stats coverage {known_stats}/{len(results)} below "
            f"{MIN_USER_STATS_COVERAGE:.0%}; profiles.json preserved"
        )
    print(f"  Stats: done")

    # --- Phase 3: Dune capital data ---
    wallets = [r["wallet"] for r in results]
    capital = fetch_capital(wallets)

    for r in results:
        cap = capital.get(r["wallet"].lower())
        if cap is not None:
            r["first_seen"] = cap.first_seen
            r["total_received"] = cap.total_received
            r["total_sent"] = cap.total_sent
            r["net_capital"] = cap.net_capital
            # ROIC = PnL / Net Capital × 100
            # Note: net_capital = total_received - total_sent, which includes
            # trading flows (exchange payouts/buys). So the denominator is
            # "net USDC position" not pure "capital deployed." This is a
            # known trade-off — filtering by contract blacklist was inflating
            # deposits by 10-50x (see #30 capital flow inflation fix).
            net_cap = cap.net_capital
            # Require minimum $5K net capital to avoid division explosion
            # when net_capital is near-zero (e.g. $145 → 780,000% ROIC)
            if net_cap >= 5000:
                r["roic"] = r["pnl"] / net_cap * 100

    capital_count = sum(type(r["net_capital"]) in (int, float) for r in results)
    if capital_count / len(results) < MIN_CAPITAL_COVERAGE:
        raise RuntimeError(
            f"Capital enrichment coverage {capital_count}/{len(results)} below "
            f"{MIN_CAPITAL_COVERAGE:.0%}; profiles.json preserved"
        )

    # --- Output ---
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    output = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "accounts": len(results),
        "profiles": results,
    }
    atomic_write(OUTPUT_FILE, json.dumps(output, indent=2))

    print(f"\nTop 5:")
    for i, r in enumerate(results[:5]):
        print(f"  {i+1}. {r['name']:<25} PnL: {fmt_money(r['pnl'])} | Vol: {fmt_money(r['volume'])} | Eff: {fmt_pct(r['efficiency'])}")

    print(f"\nOutput: {OUTPUT_FILE} ({len(results)} traders)")


if __name__ == "__main__":
    main()
