#!/usr/bin/env python3
"""Parallel Goldsky fill crawler — 6 concurrent streams.

Wraps goldsky_pipeline.py's fetch/resolve functions with thread-safe
patches to parallelize the ~17-hour serial crawl into ~3 hours.

Threading safety:
  - ThreadLocalSession: each thread gets its own requests.Session
  - save_state disabled during parallel fetch (json.dumps + concurrent
    dict mutation = RuntimeError). Single save after all workers finish.
  - Per-account state isolation: workers touch disjoint state["accounts"][name]
    dicts. No worker reads resolution_cache during fetch.

Usage:
    python3 scraper/goldsky_parallel.py [--max-hours 4] [--workers 6]
    python3 scraper/goldsky_parallel.py --accounts alice,bob --max-hours 1
    python3 scraper/goldsky_parallel.py --skip-resolution
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ---------------------------------------------------------------------------
# Import pipeline module (same directory)
# ---------------------------------------------------------------------------

SCRAPER_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRAPER_DIR))

import goldsky_pipeline as gp  # noqa: E402

# ---------------------------------------------------------------------------
# Thread-safety patches (applied at import time, before any work)
# ---------------------------------------------------------------------------


class ThreadLocalSession:
    """Drop-in proxy for requests.Session — each thread gets its own."""

    def __init__(self):
        self._local = threading.local()

    def _get(self) -> requests.Session:
        if not hasattr(self._local, "session"):
            self._local.session = requests.Session()
        return self._local.session

    def post(self, *a, **kw):
        return self._get().post(*a, **kw)

    def get(self, *a, **kw):
        return self._get().get(*a, **kw)


# Swap module-level SESSION before any threads start
gp.SESSION = ThreadLocalSession()

# Guard save_state: no-op during parallel fetch, real save otherwise
_parallel_active = False
_original_save = gp.save_state


def _guarded_save(state: dict) -> None:
    if _parallel_active:
        return
    _original_save(state)


gp.save_state = _guarded_save


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------


def fetch_worker(
    name: str, wallet: str, state: dict, deadline: float
) -> tuple[str, int, int]:
    """Fetch maker + taker fills for one account. No resolution.

    Returns (name, fills_before, fills_after) for progress reporting.
    """
    wallet = wallet.lower()
    ast = state["accounts"][name]  # pre-initialized by caller
    fills_before = ast["fills"]["total"]

    print(f"\n[{name}] starting fetch ({fills_before:,} fills so far)")

    # Maker phase
    if not ast.get("maker_exhausted", False):
        gp._fetch_phase(ast, wallet, "maker", state, deadline)

    # Taker phase (only if time remains)
    if not ast.get("taker_exhausted", False) and time.time() < deadline:
        gp._fetch_phase(ast, wallet, "taker", state, deadline)

    fills_after = ast["fills"]["total"]
    delta = fills_after - fills_before
    print(f"[{name}] done: {fills_after:,} total (+{delta:,} new)")
    return name, fills_before, fills_after


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Parallel Goldsky fill crawler")
    parser.add_argument(
        "--max-hours", type=float, default=4.0, help="Time budget in hours (default: 4)"
    )
    parser.add_argument(
        "--workers", type=int, default=6, help="Concurrent streams (default: 6)"
    )
    parser.add_argument(
        "--accounts", type=str, default=None, help="Comma-separated account names"
    )
    parser.add_argument(
        "--skip-resolution", action="store_true", help="Fetch only, skip Gamma resolution"
    )
    args = parser.parse_args()

    start = time.time()
    deadline = start + args.max_hours * 3600

    # Load accounts + state
    accounts_all = json.loads(gp.ACCOUNTS_FILE.read_text())
    state = gp.load_state()

    accounts = accounts_all
    if args.accounts:
        names = set(args.accounts.split(","))
        accounts = [a for a in accounts_all if a["name"] in names]
        missing = names - {a["name"] for a in accounts}
        if missing:
            print(f"WARNING: accounts not found in accounts.json: {missing}")

    # Pre-initialize all account states (prevents dict resize during parallel phase)
    for acct in accounts:
        if acct["name"] not in state["accounts"]:
            state["accounts"][acct["name"]] = gp.new_account_state(acct["wallet"])

    # Sort by fill count ascending — smallest accounts finish first
    accounts.sort(key=lambda a: state["accounts"][a["name"]]["fills"]["total"])

    total_existing = sum(
        state["accounts"][a["name"]]["fills"]["total"] for a in accounts
    )
    deadline_str = datetime.fromtimestamp(deadline, tz=timezone.utc).strftime(
        "%H:%M UTC"
    )
    print(f"Parallel Goldsky crawl")
    print(f"  Accounts: {len(accounts)} | Workers: {args.workers}")
    print(f"  Existing fills: {total_existing:,}")
    print(f"  Time budget: {args.max_hours}h | Deadline: {deadline_str}")
    print(f"  Resolution cache: {len(state.get('resolution_cache', {})):,} entries")
    print()

    # --- Parallel fetch phase ---
    global _parallel_active
    _parallel_active = True

    completed = []
    failed = []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {}
        for acct in accounts:
            f = pool.submit(
                fetch_worker, acct["name"], acct["wallet"], state, deadline
            )
            futures[f] = acct["name"]

        for f in as_completed(futures):
            name = futures[f]
            try:
                result = f.result()
                completed.append(result)
            except Exception as e:
                failed.append((name, str(e)))
                print(f"\n[{name}] FAILED: {e}")

    _parallel_active = False

    # Save state after parallel phase (single-threaded, safe)
    elapsed_fetch = time.time() - start
    total_new = sum(after - before for _, before, after in completed)
    print(f"\n{'='*60}")
    print(f"Fetch phase complete in {elapsed_fetch/60:.1f} min")
    print(f"  Completed: {len(completed)} | Failed: {len(failed)}")
    print(f"  New fills: {total_new:,}")
    gp.save_state(state)
    print("  State saved.")

    if failed:
        print(f"\n  Failed accounts:")
        for name, err in failed:
            print(f"    {name}: {err}")

    # --- Serial resolution phase ---
    if not args.skip_resolution:
        print(f"\n{'='*60}")
        print("Resolution phase (serial)")
        resolved_count = 0

        for acct in accounts:
            name = acct["name"]
            ast = state["accounts"].get(name)
            if not ast or not ast.get("positions"):
                continue

            maker_tip = ast.get("maker_at_tip", False)
            taker_tip = ast.get("taker_at_tip", False)

            if maker_tip and taker_tip:
                n_pos = len(ast["positions"])
                print(f"\n[{name}] resolving {n_pos:,} positions...")
                gp._resolve_phase(ast, state, deadline)
                resolved_count += 1
            else:
                tips = []
                if not maker_tip:
                    tips.append("maker")
                if not taker_tip:
                    tips.append("taker")
                print(f"  [{name}] skipped — {'+'.join(tips)} not at tip")

        gp.save_state(state)
        print(f"\n  Resolved {resolved_count} accounts. State saved.")

    # --- Output CSV ---
    gp.write_csv(state, accounts_all)
    print(f"\n  CSV written to {gp.CSV_FILE}")

    # --- Summary ---
    elapsed_total = time.time() - start
    total_fills = sum(s["fills"]["total"] for s in state["accounts"].values())
    cache_size = len(state.get("resolution_cache", {}))

    print(f"\n{'='*60}")
    print(f"DONE in {elapsed_total/60:.1f} min")
    print(f"  Total fills: {total_fills:,}")
    print(f"  Resolution cache: {cache_size:,}")

    # Per-account summary
    print(f"\n  {'Account':<20} {'Fills':>12} {'Phase':>12} {'PnL':>14}")
    print(f"  {'-'*20} {'-'*12} {'-'*12} {'-'*14}")
    for acct in sorted(accounts_all, key=lambda a: a["name"]):
        ast = state["accounts"].get(acct["name"])
        if not ast:
            continue
        fills = ast["fills"]["total"]
        agg = ast["aggregates"]
        pnl = agg["total_profit"] - agg["total_loss"]

        # Determine phase
        if ast.get("maker_at_tip") and ast.get("taker_at_tip"):
            phase = "complete"
        elif ast.get("maker_exhausted"):
            phase = "taker"
        else:
            phase = "maker"

        print(f"  {acct['name']:<20} {fills:>12,} {phase:>12} ${pnl:>13,.2f}")


if __name__ == "__main__":
    main()
