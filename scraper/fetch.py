#!/usr/bin/env python3
"""Fetch PnL/volume/stats from Polymarket profile pages for all tracked accounts.

Scrapes __NEXT_DATA__ SSR payload from each profile page. Outputs data/profiles.json
for the static frontend to consume.

Usage:
    python3 scraper/fetch.py
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
ACCOUNTS_FILE = REPO_ROOT / "scraper" / "accounts.json"
DUNE_FILE = REPO_ROOT / "scraper" / "dune-performance.csv"
OUTPUT_FILE = REPO_ROOT / "data" / "profiles.json"

REQUEST_DELAY = 0.5
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml",
})


def to_float(val) -> float | None:
    """Safely coerce a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def extract_profile_data(html: str) -> dict | None:
    """Extract PnL/volume/stats from __NEXT_DATA__ embedded in profile page HTML."""
    match = re.search(r'__NEXT_DATA__.*?>(.*?)</script', html, re.DOTALL)
    if not match:
        return None

    try:
        next_data = json.loads(match.group(1))
    except json.JSONDecodeError:
        return None

    queries = (next_data
               .get("props", {})
               .get("pageProps", {})
               .get("dehydratedState", {})
               .get("queries", []))

    result = {
        "pnl": None,
        "volume": None,
        "positions_value": None,
        "biggest_win": None,
        "trades": None,
        "markets": None,
        "join_date": None,
    }

    for q in queries:
        qk = q.get("queryKey", [])
        if not isinstance(qk, list):
            continue
        data = q.get("state", {}).get("data", None)
        key_str = " ".join(str(k) for k in qk)

        if "/api/profile/volume" in key_str and isinstance(data, dict):
            result["pnl"] = to_float(data.get("pnl"))
            result["volume"] = to_float(data.get("amount"))

        elif "user-stats" in key_str and isinstance(data, dict):
            result["biggest_win"] = to_float(data.get("largestWin"))
            result["trades"] = data.get("trades")
            join = data.get("joinDate", "")
            if join and "1969" not in join:
                result["join_date"] = join[:10]

        elif "/api/profile/marketsTraded" in key_str and isinstance(data, dict):
            result["markets"] = data.get("traded")

        elif key_str.startswith("positions value"):
            if isinstance(data, (int, float)):
                result["positions_value"] = data

        elif "/api/profile/userData" in key_str and isinstance(data, dict):
            if not result["join_date"]:
                created = data.get("createdAt", "")
                if created and "1969" not in created:
                    result["join_date"] = created[:10]

    return result


def fetch_profile(name: str, profile_url: str) -> dict | None:
    """Fetch profile page and extract data. Only fetches from polymarket.com."""
    slug_match = re.search(r'polymarket\.com/(@[^/?\s]+)', profile_url)
    if slug_match:
        url = f"https://polymarket.com/{slug_match.group(1)}"
    elif profile_url.startswith("https://polymarket.com/"):
        url = profile_url
    else:
        print(f"  SKIP: {name} — URL not on polymarket.com")
        return None

    try:
        resp = SESSION.get(url, timeout=30, allow_redirects=True)
        if resp.status_code != 200:
            print(f"  WARN: {name} HTTP {resp.status_code}")
            return None
        return extract_profile_data(resp.text)
    except requests.RequestException as e:
        print(f"  ERROR: {name} {e}")
        return None


def fmt_money(val) -> str:
    if val is None:
        return "—"
    v = float(val)
    if abs(v) >= 1_000_000:
        return f"${v/1_000_000:.2f}M"
    elif abs(v) >= 1_000:
        return f"${v/1_000:.1f}K"
    else:
        return f"${v:.2f}"


def fmt_pct(val) -> str:
    if val is None:
        return "—"
    return f"{val:.2f}%"


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
