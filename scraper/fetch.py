#!/usr/bin/env python3
"""Fetch PnL/volume/stats from Polymarket profile pages for all tracked accounts.

Scrapes __NEXT_DATA__ SSR payload from each profile page. Outputs data/profiles.json
for the static frontend to consume.

Usage:
    python3 scraper/fetch.py
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

from common import (
    REQUEST_DELAY,
    extract_profile_data,
    fetch_profile,
    fmt_money,
    fmt_pct,
    to_float,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
ACCOUNTS_FILE = REPO_ROOT / "scraper" / "accounts.json"
DUNE_FILE = REPO_ROOT / "scraper" / "dune-performance.csv"
OUTPUT_FILE = REPO_ROOT / "data" / "profiles.json"


DUNE_FIELDS = [
    "first_trade", "last_trade", "trading_days", "trading_hours",
    "total_fills", "fills_received", "fills_sent",
    "initial_deposit", "cumul_deposits", "total_withdrawn", "net_invested",
    "realized_pnl", "total_profit", "total_loss", "win_rate_pct",
    "pnl_per_fill_cents", "pnl_per_fill_pct", "sharpe", "usdc_balance", "roic_pct",
]


def parse_dune_data(path: Path) -> dict:
    """Parse Dune/Goldsky performance data from pipe-delimited markdown table. Keyed by name."""
    if not path.exists():
        return {}

    dune = {}
    lines = path.read_text().splitlines()
    if len(lines) < 3:
        return {}

    headers = [h.strip().lower() for h in lines[0].split("|")[1:-1]]
    for line in lines[2:]:
        cols = [c.strip() for c in line.split("|")[1:-1]]
        if len(cols) < len(headers):
            continue
        row = dict(zip(headers, cols))
        name = row.get("name", "").strip()
        if not name:
            continue
        entry = {}
        for field in DUNE_FIELDS:
            raw = row.get(field, "").strip()
            if field in ("first_trade", "last_trade"):
                entry[field] = raw[:19] if raw else None  # trim " UTC"
            else:
                entry[field] = to_float(raw)
        dune[name.lower()] = entry
    return dune


def main():
    if not ACCOUNTS_FILE.exists():
        print(f"ERROR: {ACCOUNTS_FILE} not found")
        raise SystemExit(1)

    accounts = json.loads(ACCOUNTS_FILE.read_text())
    dune_data = parse_dune_data(DUNE_FILE)
    print(f"Fetching {len(accounts)} accounts | Dune data for {len(dune_data)}")

    results = []
    failures = 0

    for i, acct in enumerate(accounts):
        name = acct["name"]
        print(f"[{i+1}/{len(accounts)}] {name}")

        data = fetch_profile(name, acct["profile_url"])

        dune = dune_data.get(name.lower(), {})

        if data is None:
            print("  FAILED: no API data (Dune data preserved)")
            failures += 1
            entry = {
                **acct,
                "pnl": None, "volume": None, "positions_value": None,
                "biggest_win": None, "trades": None, "markets": None,
                "join_date": None, "efficiency": None,
            }
        else:
            pnl = data["pnl"]
            volume = data["volume"]
            efficiency = (pnl / volume * 100) if (pnl is not None and volume and volume > 0) else None

            print(f"  PnL: {fmt_money(pnl)} | Vol: {fmt_money(volume)} | Eff: {fmt_pct(efficiency)}")

            entry = {
                "name": acct["name"],
                "wallet": acct["wallet"],
                "profile_url": acct["profile_url"],
                "style": acct.get("style", ""),
                "note": acct.get("note", ""),
                "pnl": pnl,
                "volume": volume,
                "positions_value": data["positions_value"],
                "biggest_win": data["biggest_win"],
                "trades": data["trades"],
                "markets": data["markets"],
                "join_date": data["join_date"],
                "efficiency": efficiency,
            }

        # Merge Dune/Goldsky data
        for field in DUNE_FIELDS:
            if field not in entry:
                entry[field] = dune.get(field)

        results.append(entry)

        if i < len(accounts) - 1:
            time.sleep(REQUEST_DELAY)

    # Sort by PnL descending
    results.sort(key=lambda r: r["pnl"] if r["pnl"] is not None else float("-inf"), reverse=True)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    output = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "accounts": len(results),
        "profiles": results,
    }
    OUTPUT_FILE.write_text(json.dumps(output, indent=2))

    print(f"\nOutput: {OUTPUT_FILE}")
    print(f"Accounts: {len(results)} | Failures: {failures}")

    if failures > len(accounts) * 0.5:
        print("ERROR: >50% of accounts failed — possible scraping breakage")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
