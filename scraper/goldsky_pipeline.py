#!/usr/bin/env python3
"""Goldsky fill-level metrics pipeline for Polymarket leaderboard.

Fetches OrderFilled events from Goldsky subgraph, tracks positions, resolves
markets via Gamma API, computes PnL and performance metrics, outputs pipe-
delimited CSV consumed by fetch.py.

State is persisted in goldsky_state.json for incremental refresh.

Usage:
    python3 scraper/goldsky_pipeline.py [--max-minutes N] [--accounts NAME,...]
"""

from __future__ import annotations

import argparse
import json
import math
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GOLDSKY_URL = (
    "https://api.goldsky.com/api/public/"
    "project_cl6mb8i9h0003e201j6li0diw/subgraphs/"
    "orderbook-subgraph/0.0.1/gn"
)
GAMMA_URL = "https://gamma-api.polymarket.com/markets"

BATCH_SIZE = 1000
MAX_RETRIES = 10

REPO_ROOT = Path(__file__).resolve().parent.parent
ACCOUNTS_FILE = REPO_ROOT / "scraper" / "accounts.json"
STATE_FILE = REPO_ROOT / "scraper" / "goldsky_state.json"
CSV_FILE = REPO_ROOT / "scraper" / "dune-performance.csv"

SESSION = requests.Session()


# ---------------------------------------------------------------------------
# Goldsky client
# ---------------------------------------------------------------------------

def query_goldsky(graphql_query: str) -> dict | None:
    """Execute GraphQL query against Goldsky with exponential backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            r = SESSION.post(GOLDSKY_URL, json={"query": graphql_query}, timeout=90)
            if r.status_code == 200:
                data = r.json()
                if "data" in data:
                    return data
                err = str(data)[:120]
                if "timeout" in err.lower():
                    wait = min(5 * (2 ** attempt), 120)
                    print(f"    Timeout (attempt {attempt + 1}/{MAX_RETRIES}), wait {wait}s…")
                    time.sleep(wait)
                    continue
                print(f"    No data key: {err}")
            elif r.status_code == 429:
                wait = min(10 * (2 ** attempt), 120)
                print(f"    Rate limited, wait {wait}s…")
                time.sleep(wait)
                continue
            else:
                print(f"    HTTP {r.status_code} (attempt {attempt + 1})")
        except requests.exceptions.Timeout:
            wait = min(5 * (2 ** attempt), 120)
            print(f"    Timeout (attempt {attempt + 1}), wait {wait}s…")
            time.sleep(wait)
            continue
        except Exception as e:
            print(f"    Error (attempt {attempt + 1}): {e}")
        time.sleep(3 * (attempt + 1))
    return None


def fetch_fills(wallet: str, role: str, last_id: str = ""):
    """Yield (events, new_last_id) batches via id_gt pagination."""
    wallet = wallet.lower()
    consecutive_failures = 0

    while True:
        where = f'{role}: "{wallet}"'
        if last_id:
            where += f', id_gt: "{last_id}"'

        query = f"""{{
            orderFilledEvents(
                first: {BATCH_SIZE},
                orderBy: id,
                orderDirection: asc,
                where: {{{where}}}
            ) {{
                id timestamp maker taker
                makerAssetId takerAssetId
                makerAmountFilled takerAmountFilled
                fee transactionHash
            }}
        }}"""

        result = query_goldsky(query)
        if not result:
            consecutive_failures += 1
            if consecutive_failures >= 3:
                print(f"    3 consecutive failures — stopping {role} fetch")
                break
            time.sleep(30)
            continue

        consecutive_failures = 0
        events = result["data"]["orderFilledEvents"]
        if not events:
            break

        last_id = events[-1]["id"]
        yield events, last_id

        if len(events) < BATCH_SIZE:
            break

        time.sleep(0.3)


# ---------------------------------------------------------------------------
# Fill processing (adapted from vigil v3 process_fill)
# ---------------------------------------------------------------------------

def process_fill(f: dict, wallet: str) -> dict:
    """Process raw fill into direction/price/shares relative to *wallet*.

    Returns dict with keys: direction, price, shares, usd_amount, token_id,
    timestamp, role.
    """
    maker_amt = int(f["makerAmountFilled"]) / 1e6
    taker_amt = int(f["takerAmountFilled"]) / 1e6

    is_maker = f["maker"].lower() == wallet.lower()

    if f["makerAssetId"] == "0":
        # Maker paid USDC → maker is BUYing tokens
        usd_amount = maker_amt
        token_amount = taker_amt
        price = maker_amt / taker_amt if taker_amt > 0 else 0
        token_id = f["takerAssetId"]
        maker_dir = "BUY"
    else:
        # Taker paid USDC → maker is SELLing tokens
        usd_amount = taker_amt
        token_amount = maker_amt
        price = taker_amt / maker_amt if maker_amt > 0 else 0
        token_id = f["makerAssetId"]
        maker_dir = "SELL"

    if is_maker:
        direction = maker_dir
    else:
        direction = "SELL" if maker_dir == "BUY" else "BUY"

    return {
        "direction": direction,
        "price": price,
        "shares": token_amount,
        "usd_amount": usd_amount,
        "token_id": token_id,
        "timestamp": int(f["timestamp"]),
        "role": "maker" if is_maker else "taker",
    }


# ---------------------------------------------------------------------------
# Position tracking
# ---------------------------------------------------------------------------

def update_position(positions: dict, fill: dict) -> None:
    """Update net position for a token_id.

    BUY:  net_shares += shares, cost_basis += usd_amount
    SELL: net_shares -= shares, cost_basis -= usd_amount
    Resolution PnL = payout * net_shares - cost_basis
    """
    tid = fill["token_id"]
    pos = positions.setdefault(tid, {"net_shares": 0.0, "cost_basis": 0.0, "fill_count": 0})

    if fill["direction"] == "BUY":
        pos["net_shares"] += fill["shares"]
        pos["cost_basis"] += fill["usd_amount"]
    else:
        pos["net_shares"] -= fill["shares"]
        pos["cost_basis"] -= fill["usd_amount"]

    pos["fill_count"] += 1


# ---------------------------------------------------------------------------
# Gamma API — market resolution
# ---------------------------------------------------------------------------

def resolve_markets(token_ids: list[str], cache: dict) -> dict:
    """Query Gamma API for resolved markets. Returns updated cache.

    Cache format: {token_id: {"payout": float, "cached_at": str, "question": str}}
    Gamma API only accepts one clob_token_id per request.
    """
    uncached = [tid for tid in token_ids if tid not in cache]
    if not uncached:
        return cache

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for i, tid in enumerate(uncached):
        try:
            resp = SESSION.get(
                GAMMA_URL,
                params={"clob_token_ids": tid, "closed": "true"},
                timeout=30,
            )
            if resp.status_code == 429:
                wait = min(5 * 2 ** (i % 5), 60)
                print(f"    Gamma 429 — backoff {wait}s")
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                continue

            markets = resp.json()
            if not markets:
                continue

            market = markets[0]
            outcome_prices_raw = market.get("outcomePrices", "")
            clob_ids_raw = market.get("clobTokenIds", "")
            if not outcome_prices_raw or not clob_ids_raw:
                continue

            try:
                prices = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
                clob_ids = json.loads(clob_ids_raw) if isinstance(clob_ids_raw, str) else clob_ids_raw
            except (json.JSONDecodeError, TypeError):
                continue

            question = market.get("question", "")[:80]
            for j, cid in enumerate(clob_ids):
                if j < len(prices):
                    payout = float(prices[j])
                    # Validate binary resolution: payout must be 0 or 1
                    if payout not in (0.0, 1.0):
                        continue
                    cache[cid] = {
                        "payout": payout,
                        "cached_at": today,
                        "question": question,
                    }

        except Exception as e:
            print(f"    Gamma error for {tid[:20]}…: {e}")

        # Rate limit: ~2 req/s
        if (i + 1) % 2 == 0:
            time.sleep(0.5)

    return cache


# ---------------------------------------------------------------------------
# Welford online algorithm (mean / variance without storing raw values)
# ---------------------------------------------------------------------------

def welford_update(n: int, mean: float, m2: float, value: float):
    """Return (n, mean, m2) after incorporating *value*."""
    n += 1
    delta = value - mean
    mean += delta / n
    delta2 = value - mean
    m2 += delta * delta2
    return n, mean, m2


def welford_std(n: int, m2: float) -> float:
    """Sample standard deviation from Welford state."""
    if n < 2:
        return 0.0
    return math.sqrt(m2 / (n - 1))


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def new_account_state(wallet: str) -> dict:
    """Return a blank state dict for one account."""
    return {
        "wallet": wallet,
        "maker_last_id": "",
        "taker_last_id": "",
        "fills": {"total": 0, "maker": 0, "taker": 0},
        "day_bounds": {},          # {date_str: [min_ts, max_ts]}
        "positions": {},           # {token_id: {net_shares, cost_basis, fill_count}}
        "resolved_tokens": [],     # token_ids already resolved (prevent double-counting)
        "aggregates": {
            "first_trade": None,
            "last_trade": None,
            "total_volume": 0.0,
            "total_profit": 0.0,
            "total_loss": 0.0,
            "wins": 0,
            "losses": 0,
            "welford_n": 0,
            "welford_mean": 0.0,
            "welford_m2": 0.0,
            "top_markets": {},     # {question_short: abs_pnl}
        },
    }


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"accounts": {}, "resolution_cache": {}}


def save_state(state: dict) -> None:
    # Prune resolution cache entries older than 30 days
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    cache = state.get("resolution_cache", {})
    state["resolution_cache"] = {
        k: v for k, v in cache.items() if v.get("cached_at", "") >= cutoff
    }
    # Atomic write: temp file + os.replace prevents corruption on crash
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, separators=(",", ":")))
    os.replace(tmp, STATE_FILE)


def trading_hours_from_bounds(day_bounds: dict) -> int:
    """Sum per-day (max_ts - min_ts) / 3600, rounded."""
    total = 0.0
    for bounds in day_bounds.values():
        if len(bounds) == 2:
            total += max(0, bounds[1] - bounds[0]) / 3600
    return round(total)


# ---------------------------------------------------------------------------
# CSV output (pipe-delimited markdown table, same format as existing)
# ---------------------------------------------------------------------------

CSV_HEADERS = [
    "name", "profile", "started", "first_trade", "last_trade",
    "trading_days", "trading_hours", "total_fills", "fills_received", "fills_sent",
    "initial_deposit", "cumul_deposits", "total_withdrawn", "net_invested",
    "realized_pnl", "total_profit", "total_loss", "win_rate_pct",
    "pnl_per_fill_cents", "pnl_per_fill_pct", "sharpe", "usdc_balance", "roic_pct",
    "maker_taker_ratio", "top_market",
]


def write_csv(state: dict, accounts: list[dict]) -> None:
    """Write metrics for all accounts that have data."""
    lines: list[str] = []
    lines.append("| " + " | ".join(CSV_HEADERS) + " |")
    lines.append("| " + " | ".join(["---"] * len(CSV_HEADERS)) + " |")

    written = 0
    for acct in accounts:
        name = acct["name"]
        ast = state["accounts"].get(name)
        if not ast or not ast["fills"].get("total"):
            continue

        fills = ast["fills"]
        agg = ast["aggregates"]

        total = fills["total"]
        maker = fills["maker"]
        taker = fills["taker"]
        wins = agg["wins"]
        losses = agg["losses"]
        t_profit = agg["total_profit"]
        t_loss = agg["total_loss"]
        realized = t_profit - t_loss
        volume = agg["total_volume"]

        win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else ""
        pnl_cents = (realized / total * 100) if total > 0 else ""
        pnl_pct = (realized / volume * 100) if volume > 0 else ""

        w_n = agg["welford_n"]
        w_mean = agg["welford_mean"]
        w_m2 = agg["welford_m2"]
        std = welford_std(w_n, w_m2)
        sharpe = round(w_mean / std, 2) if std > 0 else ""

        mt_ratio = round(maker / taker, 2) if taker > 0 else (999.0 if maker > 0 else "")

        top_markets = agg.get("top_markets", {})
        top_market = max(top_markets, key=top_markets.get) if top_markets else ""
        top_market = top_market.replace("|", "/")  # Prevent pipe breaking CSV parser

        day_bounds = ast.get("day_bounds", {})

        row: dict[str, str | int | float] = {
            "name": name,
            "profile": acct["profile_url"],
            "started": "",
            "first_trade": agg.get("first_trade") or "",
            "last_trade": agg.get("last_trade") or "",
            "trading_days": len(day_bounds),
            "trading_hours": trading_hours_from_bounds(day_bounds),
            "total_fills": total,
            "fills_received": taker,
            "fills_sent": maker,
            "initial_deposit": "",
            "cumul_deposits": "",
            "total_withdrawn": "",
            "net_invested": "",
            "realized_pnl": round(realized, 2),
            "total_profit": round(t_profit, 2),
            "total_loss": round(t_loss, 2),
            "win_rate_pct": round(win_rate, 2) if win_rate != "" else "",
            "pnl_per_fill_cents": round(pnl_cents, 2) if pnl_cents != "" else "",
            "pnl_per_fill_pct": round(pnl_pct, 2) if pnl_pct != "" else "",
            "sharpe": sharpe,
            "usdc_balance": "",
            "roic_pct": "",
            "maker_taker_ratio": mt_ratio,
            "top_market": top_market,
        }

        vals = [str(row.get(h, "")) for h in CSV_HEADERS]
        lines.append("| " + " | ".join(vals) + " |")
        written += 1

    CSV_FILE.write_text("\n".join(lines) + "\n")
    print(f"CSV: {written} accounts written to {CSV_FILE.name}")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def process_account(name: str, wallet: str, state: dict, deadline: float) -> None:
    """Fetch fills, update positions, resolve markets, compute PnL for one account."""
    if name not in state["accounts"]:
        state["accounts"][name] = new_account_state(wallet)

    ast = state["accounts"][name]

    # --- Phase 1: Fetch & process fills ---
    for role in ("maker", "taker"):
        if time.time() >= deadline:
            break

        cursor_key = f"{role}_last_id"
        last_id = ast[cursor_key]
        batch_n = 0
        new_fills = 0

        print(f"  [{role}] fetching (cursor={'…' + last_id[-20:] if last_id else 'start'})…")

        for events, new_last_id in fetch_fills(wallet, role, last_id):
            batch_n += 1

            for f in events:
                fill = process_fill(f, wallet)
                new_fills += 1

                # Counters
                ast["fills"]["total"] += 1
                ast["fills"][role] += 1

                # Day bounds
                ts = fill["timestamp"]
                day = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                bounds = ast["day_bounds"].get(day)
                if bounds is None:
                    ast["day_bounds"][day] = [ts, ts]
                else:
                    if ts < bounds[0]:
                        bounds[0] = ts
                    if ts > bounds[1]:
                        bounds[1] = ts

                # First / last trade
                ts_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S.000 UTC"
                )
                if ast["aggregates"]["first_trade"] is None or ts_str < ast["aggregates"]["first_trade"]:
                    ast["aggregates"]["first_trade"] = ts_str
                if ast["aggregates"]["last_trade"] is None or ts_str > ast["aggregates"]["last_trade"]:
                    ast["aggregates"]["last_trade"] = ts_str

                # Volume
                ast["aggregates"]["total_volume"] += fill["usd_amount"]

                # Position
                update_position(ast["positions"], fill)

            ast[cursor_key] = new_last_id

            # Progress logging
            if batch_n % 10 == 0:
                latest = datetime.fromtimestamp(
                    int(events[-1]["timestamp"]), tz=timezone.utc
                ).strftime("%Y-%m-%d")
                print(f"    batch {batch_n}: +{new_fills:,} fills (latest ~{latest})")

            # Periodic checkpoint
            if batch_n % 50 == 0:
                save_state(state)

            if time.time() >= deadline:
                print(f"    time budget hit during {role} fetch")
                save_state(state)
                break

        print(f"  [{role}] +{new_fills:,} new fills")

    # --- Phase 2: Resolve markets & compute PnL ---
    positions = ast["positions"]
    token_ids = list(positions.keys())

    if token_ids and time.time() < deadline:
        print(f"  Resolving {len(token_ids)} open positions…")
        state["resolution_cache"] = resolve_markets(
            token_ids, state.get("resolution_cache", {})
        )

        resolved_set = set(ast.get("resolved_tokens", []))

        resolved_count = 0
        for tid in list(positions.keys()):
            if tid in resolved_set:
                del positions[tid]
                continue

            entry = state["resolution_cache"].get(tid)
            if entry is None:
                continue

            pos = positions[tid]
            payout = entry["payout"]
            pnl = payout * pos["net_shares"] - pos["cost_basis"]
            resolved_count += 1

            if pnl >= 0:
                ast["aggregates"]["total_profit"] += pnl
                ast["aggregates"]["wins"] += 1
            else:
                ast["aggregates"]["total_loss"] += abs(pnl)
                ast["aggregates"]["losses"] += 1

            # Welford for Sharpe
            n, mean, m2 = welford_update(
                ast["aggregates"]["welford_n"],
                ast["aggregates"]["welford_mean"],
                ast["aggregates"]["welford_m2"],
                pnl,
            )
            ast["aggregates"]["welford_n"] = n
            ast["aggregates"]["welford_mean"] = mean
            ast["aggregates"]["welford_m2"] = m2

            # Top markets
            question = entry.get("question", "")
            if question:
                short = question[:40]
                tm = ast["aggregates"]["top_markets"]
                tm[short] = tm.get(short, 0) + abs(pnl)

            resolved_set.add(tid)
            del positions[tid]

        ast["resolved_tokens"] = list(resolved_set)

        # Cap top_markets at 20 entries
        tm = ast["aggregates"]["top_markets"]
        if len(tm) > 20:
            ast["aggregates"]["top_markets"] = dict(
                sorted(tm.items(), key=lambda x: x[1], reverse=True)[:20]
            )

        print(f"  Resolved {resolved_count} | {len(positions)} still open")

    # Summary
    f = ast["fills"]
    agg = ast["aggregates"]
    pnl = agg["total_profit"] - agg["total_loss"]
    print(f"  Totals: {f['total']:,} fills ({f['maker']:,}M / {f['taker']:,}T)")
    print(f"  PnL: ${pnl:,.2f} | W/L: {agg['wins']}/{agg['losses']}")

    save_state(state)


def run_pipeline(max_minutes: int | None = None, account_filter: str | None = None):
    start = time.time()
    deadline = start + max_minutes * 60 if max_minutes else float("inf")

    accounts: list[dict] = json.loads(ACCOUNTS_FILE.read_text())
    state = load_state()

    if account_filter:
        names = {n.strip() for n in account_filter.split(",")}
        accounts = [a for a in accounts if a["name"] in names]

    # Process smallest accounts first so most get data within time budget
    def fill_count(a: dict) -> int:
        return state.get("accounts", {}).get(a["name"], {}).get("fills", {}).get("total", 0)

    work_order = sorted(accounts, key=fill_count)

    print(f"Pipeline: {len(work_order)} accounts | max_minutes={max_minutes}")
    print(f"Order: {', '.join(a['name'] for a in work_order[:5])}{'…' if len(work_order) > 5 else ''}")

    for i, acct in enumerate(work_order):
        now = time.time()
        if now >= deadline:
            print(f"\nTime budget exhausted ({(now - start) / 60:.1f} min)")
            break

        print(f"\n{'=' * 60}")
        print(f"  {acct['name']} ({acct['wallet'][:12]}…)")

        if deadline == float("inf"):
            # Unbounded run — no per-account cap
            account_deadline = deadline
            print(f"  Budget: unlimited")
        else:
            # Fair-share time cap: split remaining budget across remaining accounts
            remaining_accounts = len(work_order) - i
            remaining_budget = deadline - now
            fair_share = remaining_budget / remaining_accounts
            account_budget = min(max(120, min(fair_share, 3600)), remaining_budget)
            account_deadline = now + account_budget
            print(f"  Budget: {account_budget / 60:.1f} min (fair share of {remaining_budget / 60:.1f} min for {remaining_accounts} accounts)")

        print(f"{'=' * 60}")

        process_account(acct["name"], acct["wallet"], state, account_deadline)

    # Write CSV with all accounts (not just filtered)
    all_accounts: list[dict] = json.loads(ACCOUNTS_FILE.read_text())
    write_csv(state, all_accounts)
    save_state(state)

    elapsed = (time.time() - start) / 60
    total_fills = sum(
        s.get("fills", {}).get("total", 0) for s in state["accounts"].values()
    )
    print(f"\nDone in {elapsed:.1f} min | {total_fills:,} total fills across all accounts")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Goldsky fill-level metrics pipeline")
    parser.add_argument("--max-minutes", type=int, default=None, help="Time budget in minutes")
    parser.add_argument("--accounts", type=str, default=None, help="Comma-separated account names to process")
    args = parser.parse_args()
    run_pipeline(max_minutes=args.max_minutes, account_filter=args.accounts)
