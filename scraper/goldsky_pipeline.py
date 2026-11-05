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
import random
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from external import (GoldskyTimeoutError, Account, GoldskyFill, GoldskyState, StateCacheEntry,
                      load_accounts, load_gamma_response, load_goldsky_response, load_goldsky_state)

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

def query_goldsky(graphql_query: str) -> list[GoldskyFill] | None:
    """Execute GraphQL query against Goldsky with exponential backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            r = SESSION.post(GOLDSKY_URL, json={"query": graphql_query}, timeout=90)
            if r.status_code == 200:
                return load_goldsky_response(r)
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
        except GoldskyTimeoutError:
            wait = min(5 * (2 ** attempt), 120)
            print(f"    GraphQL timeout (attempt {attempt + 1}/{MAX_RETRIES}), wait {wait}s…")
            time.sleep(wait)
            continue
        except ValueError:
            raise
        except Exception as e:
            print(f"    Error (attempt {attempt + 1}): {e}")
        time.sleep(3 * (attempt + 1))
    return None


class FetchAbortedError(Exception):
    """Raised by fetch_fills() when 3 consecutive API failures abort the fetch.
    Signals _fetch_phase that the cursor stopped at a failure point, not at tip.
    """


def fetch_fills(wallet: str, role: str, last_id: str = ""):
    """Yield (events, new_last_id) batches via id_gt pagination.

    Raises FetchAbortedError after 3 consecutive API failures.
    Callers must wrap iteration in try/except FetchAbortedError.
    """
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
        if result is None:
            consecutive_failures += 1
            if consecutive_failures >= 3:
                print(f"    3 consecutive failures — stopping {role} fetch")
                raise FetchAbortedError(f"3 consecutive failures for {role}")
            time.sleep(30)
            continue

        consecutive_failures = 0
        events = result
        if not events:
            break

        last_id = events[-1].id
        yield events, last_id

        if len(events) < BATCH_SIZE:
            break

        time.sleep(0.3)


# ---------------------------------------------------------------------------
# Fill processing (adapted from vigil v3 process_fill)
# ---------------------------------------------------------------------------

def process_fill(f: GoldskyFill, wallet: str) -> dict:
    """Process validated fill into direction/price/shares relative to *wallet*.

    Returns dict with keys: direction, price, shares, usd_amount, token_id,
    timestamp, role.
    """
    maker_amt = int(f.makerAmountFilled) / 1e6
    taker_amt = int(f.takerAmountFilled) / 1e6

    is_maker = f.maker.lower() == wallet.lower()

    if f.makerAssetId == "0":
        # Maker paid USDC → maker is BUYing tokens
        usd_amount = maker_amt
        token_amount = taker_amt
        price = maker_amt / taker_amt if taker_amt > 0 else 0
        token_id = f.takerAssetId
        maker_dir = "BUY"
    else:
        # Taker paid USDC → maker is SELLing tokens
        usd_amount = taker_amt
        token_amount = maker_amt
        price = taker_amt / maker_amt if maker_amt > 0 else 0
        token_id = f.makerAssetId
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
        "timestamp": int(f.timestamp),
        "role": "maker" if is_maker else "taker",
    }


# ---------------------------------------------------------------------------
# Position tracking
# ---------------------------------------------------------------------------

def update_position(positions: dict[str, Position], fill: dict) -> None:
    """Update net position for a token_id.

    BUY:  net_shares += shares, cost_basis += usd_amount
    SELL: net_shares -= shares, cost_basis -= usd_amount
    Resolution PnL = payout * net_shares - cost_basis
    """
    tid = fill["token_id"]
    if tid not in positions:
        positions[tid] = Position()
    pos = positions[tid]

    if fill["direction"] == "BUY":
        pos.net_shares += fill["shares"]
        pos.cost_basis += fill["usd_amount"]
    else:
        pos.net_shares -= fill["shares"]
        pos.cost_basis -= fill["usd_amount"]

    pos.fill_count += 1


# ---------------------------------------------------------------------------
# Gamma API — market resolution
# ---------------------------------------------------------------------------

GAMMA_BATCH = 20  # Gamma caps each market response at 20 rows.


def resolve_markets(
    token_ids: list[str], cache: dict[str, StateCacheEntry], deadline: float = float("inf")
) -> dict[str, StateCacheEntry]:
    """Query Gamma API in batches of GAMMA_BATCH for resolved markets.

    Returns updated cache. Shuffles uncached list so all tokens are eventually
    queried across runs, preventing starvation of late-inserted positions.

    Cache values are validated StateCacheEntry records.
    """
    uncached = [tid for tid in token_ids if tid not in cache]
    if not uncached:
        return cache

    random.shuffle(uncached)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    total_batches = math.ceil(len(uncached) / GAMMA_BATCH)

    for batch_i, start in enumerate(range(0, len(uncached), GAMMA_BATCH)):
        if time.time() >= deadline:
            print(f"    resolution deadline — {batch_i}/{total_batches} batches done")
            break

        batch = uncached[start : start + GAMMA_BATCH]
        retries = 0
        while retries < 3:
            params = [("clob_token_ids", tid) for tid in batch]
            params.append(("closed", "true"))
            resp = SESSION.get(GAMMA_URL, params=params, timeout=30)

            if resp.status_code == 429:
                wait = min(5 * 2 ** retries, 60)
                print(f"    Gamma 429 — backoff {wait}s (batch {batch_i + 1})")
                time.sleep(wait)
                retries += 1
                continue  # retry same batch

            if resp.status_code != 200:
                raise RuntimeError(f"Gamma HTTP {resp.status_code} for batch {batch_i + 1}")

            markets = load_gamma_response(resp)
            for market in markets:
                question = market.question
                if question is None:
                    question = ""
                question = question[:80]
                for cid, price in zip(market.clob_ids, market.prices):
                    payout = float(price)
                    if payout not in (0.0, 1.0):
                        continue
                    cache[cid] = StateCacheEntry(payout=payout, cached_at=today, question=question)
            break  # success — exit retry loop

        else:
            raise RuntimeError(f"Gamma retries exhausted for batch {batch_i + 1}")

        # Rate limit: ~2 req/s
        if (batch_i + 1) % 2 == 0:
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

@dataclass
class FillCounts:
    total: int = 0
    maker: int = 0
    taker: int = 0


@dataclass
class Position:
    net_shares: float = 0.0
    cost_basis: float = 0.0
    fill_count: int = 0


@dataclass
class Aggregates:
    first_trade: str | None = None
    last_trade: str | None = None
    total_volume: float = 0.0
    total_profit: float = 0.0
    total_loss: float = 0.0
    wins: int = 0
    losses: int = 0
    welford_n: int = 0
    welford_mean: float = 0.0
    welford_m2: float = 0.0
    top_markets: dict[str, float] = field(default_factory=dict)


@dataclass
class AccountState:
    wallet: str
    maker_last_id: str = ""
    taker_last_id: str = ""
    maker_exhausted: bool | None = False
    taker_exhausted: bool | None = False
    maker_at_tip: bool | None = False
    taker_at_tip: bool | None = False
    fills: FillCounts = field(default_factory=FillCounts)
    day_bounds: dict[str, list[int]] = field(default_factory=dict)
    positions: dict[str, Position] = field(default_factory=dict)
    aggregates: Aggregates = field(default_factory=Aggregates)


@dataclass
class RuntimeState:
    accounts: dict[str, AccountState] = field(default_factory=dict)
    resolution_cache: dict[str, StateCacheEntry] = field(default_factory=dict)


def new_account_state(wallet: str) -> AccountState:
    return AccountState(wallet=wallet)


def runtime_state(snapshot: GoldskyState) -> RuntimeState:
    return RuntimeState(
        accounts={name: AccountState(
            wallet=account.wallet, maker_last_id=account.maker_last_id,
            taker_last_id=account.taker_last_id,
            maker_exhausted=account.maker_exhausted,
            taker_exhausted=account.taker_exhausted,
            maker_at_tip=account.maker_at_tip, taker_at_tip=account.taker_at_tip,
            fills=FillCounts(account.fills.total, account.fills.maker, account.fills.taker),
            day_bounds={day: list(pair) for day, pair in account.day_bounds.items()},
            positions={token: Position(pos.net_shares, pos.cost_basis, pos.fill_count)
                       for token, pos in account.positions.items()},
            aggregates=Aggregates(
                first_trade=account.aggregates.first_trade,
                last_trade=account.aggregates.last_trade,
                total_volume=account.aggregates.total_volume,
                total_profit=account.aggregates.total_profit,
                total_loss=account.aggregates.total_loss,
                wins=account.aggregates.wins, losses=account.aggregates.losses,
                welford_n=account.aggregates.welford_n,
                welford_mean=account.aggregates.welford_mean,
                welford_m2=account.aggregates.welford_m2,
                top_markets=dict(account.aggregates.top_markets),
            ),
        ) for name, account in snapshot.accounts.items()},
        resolution_cache=dict(snapshot.resolution_cache),
    )


def load_state() -> RuntimeState:
    if not STATE_FILE.is_file():
        raise RuntimeError(f"Required Goldsky state file missing: {STATE_FILE}")
    return runtime_state(load_goldsky_state(STATE_FILE))


def save_state(state: RuntimeState) -> None:
    # Prune resolution cache entries older than 30 days
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    cache = state.resolution_cache
    state.resolution_cache = {
        k: v for k, v in cache.items() if v.cached_at >= cutoff
    }
    # Atomic write: temp file + os.replace prevents corruption on crash
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(asdict(state), separators=(",", ":")))
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


def write_csv(state: RuntimeState, accounts: list[Account]) -> None:
    """Write metrics for all accounts that have data."""
    lines: list[str] = []
    lines.append("| " + " | ".join(CSV_HEADERS) + " |")
    lines.append("| " + " | ".join(["---"] * len(CSV_HEADERS)) + " |")

    written = 0
    for acct in accounts:
        name = acct.name
        ast = state.accounts.get(name)
        if not ast or not ast.fills.total:
            continue

        fills = ast.fills
        agg = ast.aggregates

        total = fills.total
        maker = fills.maker
        taker = fills.taker
        wins = agg.wins
        losses = agg.losses
        t_profit = agg.total_profit
        t_loss = agg.total_loss
        realized = t_profit - t_loss
        volume = agg.total_volume

        win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else ""
        pnl_cents = (realized / total * 100) if total > 0 else ""
        pnl_pct = (realized / volume * 100) if volume > 0 else ""

        w_n = agg.welford_n
        w_mean = agg.welford_mean
        w_m2 = agg.welford_m2
        std = welford_std(w_n, w_m2)
        sharpe = round(w_mean / std, 2) if std > 0 else ""

        mt_ratio = round(maker / taker, 2) if taker > 0 else (999.0 if maker > 0 else "")

        top_markets = agg.top_markets
        top_market = max(top_markets, key=top_markets.get) if top_markets else ""
        top_market = top_market.replace("|", "/")  # Prevent pipe breaking CSV parser

        day_bounds = ast.day_bounds
        first_trade = agg.first_trade
        last_trade = agg.last_trade
        if first_trade is None:
            first_trade = ""
        if last_trade is None:
            last_trade = ""

        row: dict[str, str | int | float] = {
            "name": name,
            "profile": acct.profile_url,
            "started": "",
            "first_trade": first_trade,
            "last_trade": last_trade,
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

        vals = [str(row[h]) for h in CSV_HEADERS]
        lines.append("| " + " | ".join(vals) + " |")
        written += 1

    CSV_FILE.write_text("\n".join(lines) + "\n")
    print(f"CSV: {written} accounts written to {CSV_FILE.name}")


# ---------------------------------------------------------------------------
# Pipeline phases
# ---------------------------------------------------------------------------

def _fetch_phase(ast: AccountState, wallet: str, role: str, deadline: float) -> None:
    """Fetch all fills for one role, advancing cursor. Sets {role}_exhausted on completion.

    If the deadline is hit mid-fetch, the final state write retains the cursor
    for the next run.
    """
    cursor_key = f"{role}_last_id"
    last_id = getattr(ast, cursor_key)
    batch_n = 0
    new_fills = 0
    deadline_hit = False
    setattr(ast, f"{role}_at_tip", False)  # clear stale flag

    print(f"  [{role}] fetching (cursor={'…' + last_id[-20:] if last_id else 'start'})…")

    for events, new_last_id in fetch_fills(wallet, role, last_id):
        batch_n += 1

        for f in events:
            fill = process_fill(f, wallet)
            new_fills += 1

            ast.fills.total += 1
            if role == "maker":
                ast.fills.maker += 1
            else:
                ast.fills.taker += 1

            ts = fill["timestamp"]
            day = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
            bounds = ast.day_bounds.get(day)
            if bounds is None:
                ast.day_bounds[day] = [ts, ts]
            else:
                if ts < bounds[0]:
                    bounds[0] = ts
                if ts > bounds[1]:
                    bounds[1] = ts

            ts_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S.000 UTC"
            )
            if ast.aggregates.first_trade is None or ts_str < ast.aggregates.first_trade:
                ast.aggregates.first_trade = ts_str
            if ast.aggregates.last_trade is None or ts_str > ast.aggregates.last_trade:
                ast.aggregates.last_trade = ts_str

            ast.aggregates.total_volume += fill["usd_amount"]
            update_position(ast.positions, fill)

        setattr(ast, cursor_key, new_last_id)

        if batch_n % 10 == 0:
            latest = datetime.fromtimestamp(
                int(events[-1].timestamp), tz=timezone.utc
            ).strftime("%Y-%m-%d")
            print(f"    batch {batch_n}: +{new_fills:,} fills (latest ~{latest})")

        if time.time() >= deadline:
            print(f"    time budget hit during [{role}] fetch")
            deadline_hit = True
            break

    print(f"  [{role}] +{new_fills:,} new fills")
    if not deadline_hit:
        setattr(ast, f"{role}_exhausted", True)
        setattr(ast, f"{role}_at_tip", True)
        print(f"  [{role}] cursor exhausted (at tip)")


def _resolve_phase(ast: AccountState, state: RuntimeState, deadline: float) -> None:
    """Resolve markets and compute PnL for all open positions.

    Queries Gamma in batches. If deadline is hit mid-way, unresolved positions
    remain in state and are retried on the next pipeline run.

    Deduplication relies entirely on the id_gt cursor: each fill is fetched
    exactly once, so each position entry reflects unique fills and is booked
    exactly once before being deleted. No cross-run resolved-set needed.
    """
    positions = ast.positions
    token_ids = list(positions.keys())
    if not token_ids:
        return

    print(f"  Resolving {len(token_ids)} open positions…")
    cache = resolve_markets(token_ids, state.resolution_cache, deadline)
    state.resolution_cache = cache

    resolved_count = 0

    for tid in list(positions.keys()):
        entry = state.resolution_cache.get(tid)
        if entry is None:
            continue

        pos = positions[tid]
        pnl = entry.payout * pos.net_shares - pos.cost_basis
        resolved_count += 1

        if pnl >= 0:
            ast.aggregates.total_profit += pnl
            ast.aggregates.wins += 1
        else:
            ast.aggregates.total_loss += abs(pnl)
            ast.aggregates.losses += 1

        n, mean, m2 = welford_update(
            ast.aggregates.welford_n,
            ast.aggregates.welford_mean,
            ast.aggregates.welford_m2,
            pnl,
        )
        ast.aggregates.welford_n = n
        ast.aggregates.welford_mean = mean
        ast.aggregates.welford_m2 = m2

        question = entry.question
        if question is None:
            question = ""
        if question:
            short = question[:40]
            tm = ast.aggregates.top_markets
            if short not in tm:
                tm[short] = 0
            tm[short] += abs(pnl)

        del positions[tid]

    tm = ast.aggregates.top_markets
    if len(tm) > 20:
        ast.aggregates.top_markets = dict(
            sorted(tm.items(), key=lambda x: x[1], reverse=True)[:20]
        )

    print(f"  Resolved {resolved_count} | {len(positions)} still open")

    # Always reset fill-phase flags after resolution so daily fills are
    # picked up each cycle. Partial Gamma queries (deadline hit) are fine —
    # uncached tokens are retried next run via random.shuffle ordering.
    ast.maker_exhausted = False
    ast.taker_exhausted = False
    ast.maker_at_tip = False
    ast.taker_at_tip = False


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def process_account(name: str, wallet: str, state: RuntimeState, deadline: float) -> None:
    """Advance phases for this account within the deadline: maker → taker → resolution.

    Phases run in sequence. If a phase completes before the deadline, the next
    phase starts immediately in the same invocation — budget is not wasted.
    If a phase hits the deadline mid-way, it saves cursor state and stops;
    the next run resumes from where it left off.
    """
    if name not in state.accounts:
        state.accounts[name] = new_account_state(wallet)

    ast = state.accounts[name]

    while time.time() < deadline:
        maker_done = ast.maker_exhausted
        taker_done = ast.taker_exhausted

        if not maker_done:
            _fetch_phase(ast, wallet, "maker", deadline)
        elif not taker_done:
            _fetch_phase(ast, wallet, "taker", deadline)
        else:
            # Both phases done for this run — check fill completeness before resolving
            if ast.positions:
                maker_tip = ast.maker_at_tip
                taker_tip = ast.taker_at_tip
                if maker_tip and taker_tip:
                    _resolve_phase(ast, state, deadline)
                    # Remaining positions are open markets — no point re-querying Gamma
                    # in the same run. Come back next invocation when more markets close.
                else:
                    print(f"  fills incomplete — resolution deferred to next run")
                    ast.maker_exhausted = False
                    ast.taker_exhausted = False
            else:
                # No positions to resolve — reset for next daily cycle.
                ast.maker_exhausted = False
                ast.taker_exhausted = False
                ast.maker_at_tip = False
                ast.taker_at_tip = False
                print(f"  Cycle complete — reset for next refresh")
            break

        if time.time() >= deadline:
            break

    f = ast.fills
    agg = ast.aggregates
    pnl = agg.total_profit - agg.total_loss
    print(f"  Totals: {f.total:,} fills ({f.maker:,}M / {f.taker:,}T)")
    print(f"  PnL: ${pnl:,.2f} | W/L: {agg.wins}/{agg.losses}")



def run_pipeline(max_minutes: int | None = None, account_filter: str | None = None):
    start = time.time()
    deadline = start + max_minutes * 60 if max_minutes else float("inf")

    all_accounts: list[Account] = load_accounts(ACCOUNTS_FILE)
    accounts = all_accounts
    state = load_state()

    if account_filter:
        names = {n.strip() for n in account_filter.split(",")}
        accounts = [a for a in accounts if a.name in names]

    # Phase-priority ordering: resolution > taker > maker > done
    # Accounts closest to completion run first — maximises fully-processed accounts per CI run.
    # Within the same phase, sort by fill count ascending (smallest accounts finish fastest).
    def phase_key(a: Account) -> tuple:
        ast = state.accounts.get(a.name)
        if ast is None:
            return (2, 0)
        maker_done = ast.maker_exhausted
        taker_done = ast.taker_exhausted
        fills = ast.fills.total
        if maker_done and taker_done and not ast.positions:
            return (3, fills)   # fully done — no-op, run last
        if maker_done and taker_done:
            return (0, fills)   # resolution phase — run first
        if maker_done:
            return (1, fills)   # taker phase
        return (2, fills)       # maker phase — largest last

    work_order = sorted(accounts, key=phase_key)

    print(f"Pipeline: {len(work_order)} accounts | max_minutes={max_minutes}")
    print(f"Order: {', '.join(a.name for a in work_order[:5])}{'…' if len(work_order) > 5 else ''}")

    for i, acct in enumerate(work_order):
        now = time.time()
        if now >= deadline:
            print(f"\nTime budget exhausted ({(now - start) / 60:.1f} min)")
            break

        print(f"\n{'=' * 60}")
        print(f"  {acct.name} ({acct.wallet[:12]}…)")

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

        process_account(acct.name, acct.wallet, state, account_deadline)

    # Write CSV with all accounts (not just filtered)
    write_csv(state, all_accounts)
    save_state(state)

    elapsed = (time.time() - start) / 60
    total_fills = sum(
        s.fills.total for s in state.accounts.values()
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
