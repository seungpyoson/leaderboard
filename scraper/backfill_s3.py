#!/usr/bin/env python3
"""Backfill 28 accounts from S3 parquet instead of crawling Goldsky API.

Reads pre-processed on-chain fills from s3://bolt-ticks/telescope/data/onchain_fills/
(1,154 date-partitioned parquet files covering 2022-12 through 2026-02-15),
filters to tracked wallets, builds positions, resolves markets via Gamma, and
writes goldsky_state.json + dune-performance.csv.

Usage:
    python3 scraper/backfill_s3.py [--dry-run] [--skip-resolve]
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

import requests

from goldsky_pipeline import (
    ACCOUNTS_FILE,
    CSV_FILE,
    GAMMA_BATCH,
    GAMMA_URL,
    SESSION,
    STATE_FILE,
    load_state,
    new_account_state,
    resolve_markets,
    save_state,
    update_position,
    welford_update,
    write_csv,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

S3_BASE = "s3://bolt-ticks/telescope/data/onchain_fills/"
S3_GLOB = S3_BASE + "*/onchain_fills.parquet"
S3_REGION = "us-east-1"

# Backfill cutoff — last partition date in the S3 dataset
BACKFILL_CUTOFF = "2026-02-15T23:59:59Z"


# ---------------------------------------------------------------------------
# Phase 1: S3 Scan via DuckDB
# ---------------------------------------------------------------------------

def scan_s3_fills(wallets: list[str]) -> list[tuple]:
    """Read all S3 parquet partitions, filter to tracked wallets, return rows.

    Returns list of tuples:
        (wallet, role, direction, price, usd_amount, token_amount, asset_id, timestamp_epoch)
    sorted by (wallet, timestamp_epoch).
    """
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_region='{S3_REGION}'")

    # Build wallet list for SQL IN clause
    wallet_list = ", ".join(f"'{w.lower()}'" for w in wallets)

    # UNION ALL: maker-perspective + taker-perspective rows
    query = f"""
    SELECT * FROM (
        SELECT
            lower(maker) AS wallet,
            'maker' AS role,
            maker_side AS direction,
            price,
            usd_amount,
            token_amount,
            asset_id,
            epoch_ms(timestamp) / 1000 AS ts
        FROM read_parquet('{S3_GLOB}', hive_partitioning=true)
        WHERE lower(maker) IN ({wallet_list})

        UNION ALL

        SELECT
            lower(taker) AS wallet,
            'taker' AS role,
            taker_side AS direction,
            price,
            usd_amount,
            token_amount,
            asset_id,
            epoch_ms(timestamp) / 1000 AS ts
        FROM read_parquet('{S3_GLOB}', hive_partitioning=true)
        WHERE lower(taker) IN ({wallet_list})
    )
    ORDER BY wallet, ts
    """

    print("Phase 1: Scanning S3 parquet partitions...")
    t0 = time.time()
    rows = con.execute(query).fetchall()
    elapsed = time.time() - t0
    print(f"  {len(rows):,} fill rows fetched in {elapsed:.1f}s")

    con.close()
    return rows


# ---------------------------------------------------------------------------
# Phase 2: Position Tracking
# ---------------------------------------------------------------------------

def build_positions(
    rows: list[tuple],
    accounts: list[dict],
    state: dict,
) -> None:
    """Iterate timestamp-sorted fills, update account states.

    Each row tuple: (wallet, role, direction, price, usd_amount, token_amount, asset_id, ts)
    """
    print("\nPhase 2: Building positions...")
    t0 = time.time()

    # Map wallet (lowercase) → account name
    wallet_to_name = {a["wallet"].lower(): a["name"] for a in accounts}

    # Initialize fresh account states for all 28 accounts
    # Set backfill_cutoff_ts so the daily pipeline skips fills already in S3 data
    cutoff_ts = int(datetime(2026, 2, 15, 23, 59, 59, tzinfo=timezone.utc).timestamp())
    for acct in accounts:
        state["accounts"][acct["name"]] = new_account_state(acct["wallet"])
        state["accounts"][acct["name"]]["backfill_cutoff_ts"] = cutoff_ts

    fill_counts: dict[str, int] = {}

    for wallet, role, direction, price, usd_amount, token_amount, asset_id, ts in rows:
        name = wallet_to_name.get(wallet)
        if not name:
            continue

        ast = state["accounts"][name]

        # Build fill dict matching update_position() expectations
        fill = {
            "direction": direction,     # "BUY" or "SELL"
            "price": price,
            "shares": token_amount,
            "usd_amount": usd_amount,
            "token_id": asset_id,
            "timestamp": int(ts),
            "role": role,
        }

        # Update fill counters
        ast["fills"]["total"] += 1
        ast["fills"][role] += 1

        # Day bounds
        timestamp = int(ts)
        day = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d")
        bounds = ast["day_bounds"].get(day)
        if bounds is None:
            ast["day_bounds"][day] = [timestamp, timestamp]
        else:
            if timestamp < bounds[0]:
                bounds[0] = timestamp
            if timestamp > bounds[1]:
                bounds[1] = timestamp

        # First/last trade
        ts_str = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S.000 UTC"
        )
        if ast["aggregates"]["first_trade"] is None or ts_str < ast["aggregates"]["first_trade"]:
            ast["aggregates"]["first_trade"] = ts_str
        if ast["aggregates"]["last_trade"] is None or ts_str > ast["aggregates"]["last_trade"]:
            ast["aggregates"]["last_trade"] = ts_str

        # Volume
        ast["aggregates"]["total_volume"] += usd_amount

        # Position tracking
        update_position(ast["positions"], fill)

        fill_counts[name] = fill_counts.get(name, 0) + 1

    elapsed = time.time() - t0
    print(f"  Positions built in {elapsed:.1f}s")
    print(f"\n  Fill counts per account:")
    for name, count in sorted(fill_counts.items(), key=lambda x: -x[1]):
        ast = state["accounts"][name]
        n_pos = len(ast["positions"])
        print(f"    {name:35s}  {count:>10,} fills  {n_pos:>6,} positions")


# ---------------------------------------------------------------------------
# Phase 3: Gamma Resolution
# ---------------------------------------------------------------------------

def _book_pnl(state: dict, label: str) -> None:
    """Book PnL for all accounts using the resolution cache. Shared by both phases."""
    cache = state.get("resolution_cache", {})
    total_resolved = 0
    total_remaining = 0

    for name, ast in state["accounts"].items():
        positions = ast["positions"]
        resolved_count = 0

        for tid in list(positions.keys()):
            entry = cache.get(tid)
            if entry is None:
                continue

            pos = positions[tid]
            pnl = entry["payout"] * pos["net_shares"] - pos["cost_basis"]
            resolved_count += 1

            if pnl >= 0:
                ast["aggregates"]["total_profit"] += pnl
                ast["aggregates"]["wins"] += 1
            else:
                ast["aggregates"]["total_loss"] += abs(pnl)
                ast["aggregates"]["losses"] += 1

            n, mean, m2 = welford_update(
                ast["aggregates"]["welford_n"],
                ast["aggregates"]["welford_mean"],
                ast["aggregates"]["welford_m2"],
                pnl,
            )
            ast["aggregates"]["welford_n"] = n
            ast["aggregates"]["welford_mean"] = mean
            ast["aggregates"]["welford_m2"] = m2

            question = entry.get("question", "")
            if question:
                short = question[:40]
                tm = ast["aggregates"]["top_markets"]
                tm[short] = tm.get(short, 0) + abs(pnl)

            del positions[tid]

        # Trim top_markets to top 20
        tm = ast["aggregates"]["top_markets"]
        if len(tm) > 20:
            ast["aggregates"]["top_markets"] = dict(
                sorted(tm.items(), key=lambda x: x[1], reverse=True)[:20]
            )

        total_resolved += resolved_count
        remaining = len(positions)
        total_remaining += remaining

        if resolved_count > 0 or remaining > 0:
            pnl = ast["aggregates"]["total_profit"] - ast["aggregates"]["total_loss"]
            print(f"    {name:35s}  resolved={resolved_count:>5,}  open={remaining:>5,}  PnL=${pnl:>12,.2f}")

        save_state(state)

    print(f"\n  [{label}] Total resolved: {total_resolved:,} | Still open: {total_remaining:,}")


def resolve_all(state: dict) -> None:
    """Resolve markets and book PnL for all 28 accounts."""
    print("\nPhase 3a: Resolving markets via Gamma (closed=true)...")
    t0 = time.time()

    all_token_ids: set[str] = set()
    for ast in state["accounts"].values():
        all_token_ids.update(ast["positions"].keys())

    cache = state.get("resolution_cache", {})
    cached_count = sum(1 for tid in all_token_ids if tid in cache)
    print(f"  {len(all_token_ids):,} unique token_ids | {cached_count:,} already cached | {len(all_token_ids) - cached_count:,} to query")

    cache = resolve_markets(list(all_token_ids), cache, deadline=float("inf"))
    state["resolution_cache"] = cache

    _book_pnl(state, "closed=true")
    elapsed = time.time() - t0
    print(f"  Phase 3a done in {elapsed:.1f}s")


def resolve_elapsed_markets(state: dict) -> None:
    """Phase 3b: Query Gamma WITHOUT closed=true for elapsed interval markets.

    Safety: only accepts resolution if market endDate is >1 hour in the past.
    This prevents booking PnL on markets that are still live or just barely closed.
    """
    print("\nPhase 3b: Resolving elapsed markets (no closed filter, endDate check)...")
    t0 = time.time()

    # Collect unresolved token_ids (not yet in cache)
    cache = state.get("resolution_cache", {})
    unresolved: list[str] = []
    for ast in state["accounts"].values():
        for tid in ast["positions"]:
            if tid not in cache:
                unresolved.append(tid)
    unresolved = list(set(unresolved))

    if not unresolved:
        print("  No unresolved token_ids — skipping")
        return

    print(f"  {len(unresolved):,} unresolved token_ids to query")

    now = datetime.now(timezone.utc)
    elapsed_buffer = timedelta(hours=1)
    today = now.strftime("%Y-%m-%d")

    # Gamma silently caps responses at 20 markets — use batch size 20 to avoid dropped results
    ELAPSED_BATCH = 20

    newly_cached = 0
    skipped_not_elapsed = 0
    not_in_gamma = 0
    total_batches = (len(unresolved) + ELAPSED_BATCH - 1) // ELAPSED_BATCH

    for batch_i, start in enumerate(range(0, len(unresolved), ELAPSED_BATCH)):
        batch = unresolved[start : start + ELAPSED_BATCH]

        retries = 0
        while retries < 3:
            try:
                params = [("clob_token_ids", tid) for tid in batch]
                # NO closed=true filter
                resp = SESSION.get(GAMMA_URL, params=params, timeout=30)

                if resp.status_code == 429:
                    wait = min(5 * 2 ** retries, 60)
                    print(f"    Gamma 429 — backoff {wait}s (batch {batch_i + 1})")
                    time.sleep(wait)
                    retries += 1
                    continue

                if resp.status_code != 200:
                    break

                markets = resp.json()
                if not isinstance(markets, list):
                    break

                returned_tids: set[str] = set()
                for market in markets:
                    outcome_prices_raw = market.get("outcomePrices", "")
                    clob_ids_raw = market.get("clobTokenIds", "")
                    end_date_str = market.get("endDate", "")

                    if not outcome_prices_raw or not clob_ids_raw:
                        continue

                    try:
                        prices = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
                        clob_ids = json.loads(clob_ids_raw) if isinstance(clob_ids_raw, str) else clob_ids_raw
                    except (json.JSONDecodeError, TypeError):
                        continue

                    # Safety: check endDate is >1hr in the past
                    if end_date_str:
                        try:
                            end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                            if end_dt + elapsed_buffer > now:
                                skipped_not_elapsed += len(clob_ids)
                                for cid in clob_ids:
                                    returned_tids.add(cid)
                                continue
                        except (ValueError, TypeError):
                            # Can't parse date — skip to be safe
                            continue
                    else:
                        # No endDate — skip to be safe
                        continue

                    question = market.get("question", "")[:80]
                    for j, cid in enumerate(clob_ids):
                        returned_tids.add(cid)
                        if j < len(prices):
                            payout = float(prices[j])
                            if payout not in (0.0, 1.0):
                                continue
                            cache[cid] = {"payout": payout, "cached_at": today, "question": question}
                            newly_cached += 1

                # Count batch members not found in Gamma at all
                for tid in batch:
                    if tid not in returned_tids and tid not in cache:
                        not_in_gamma += 1

                break  # success

            except Exception as e:
                print(f"    Gamma error (batch {batch_i + 1}): {e}")
                break

        if (batch_i + 1) % 2 == 0:
            time.sleep(0.5)

        if (batch_i + 1) % 100 == 0:
            print(f"    batch {batch_i + 1}/{total_batches}: +{newly_cached:,} cached")

    state["resolution_cache"] = cache
    save_state(state)

    print(f"  Newly cached:       {newly_cached:,}")
    print(f"  Skipped (not elapsed): {skipped_not_elapsed:,}")
    print(f"  Not in Gamma:       {not_in_gamma:,}")

    # Book PnL from newly cached resolutions
    _book_pnl(state, "elapsed")
    elapsed = time.time() - t0
    print(f"  Phase 3b done in {elapsed:.1f}s")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Backfill accounts from S3 parquet")
    parser.add_argument("--dry-run", action="store_true", help="Scan S3 and show fill counts, don't write state")
    parser.add_argument("--skip-resolve", action="store_true", help="Skip Gamma resolution phase")
    args = parser.parse_args()

    accounts: list[dict] = json.loads(ACCOUNTS_FILE.read_text())
    wallets = [a["wallet"] for a in accounts]

    print(f"Backfilling {len(accounts)} accounts from S3 parquet")
    print(f"S3 path: {S3_BASE}")
    print(f"Cutoff: {BACKFILL_CUTOFF}\n")

    # Phase 1: Scan S3
    rows = scan_s3_fills(wallets)

    if args.dry_run:
        # Count fills per wallet for summary
        from collections import Counter
        wallet_to_name = {a["wallet"].lower(): a["name"] for a in accounts}
        counts = Counter()
        for wallet, *_ in rows:
            name = wallet_to_name.get(wallet, wallet)
            counts[name] += 1
        print("\n[DRY RUN] Fill counts:")
        for name, count in counts.most_common():
            print(f"  {name:35s}  {count:>10,}")
        print(f"\n  Total: {len(rows):,} fills across {len(counts)} accounts")
        return

    # Load state — preserve resolution_cache, replace account states
    state = load_state()

    # Phase 2: Position tracking
    build_positions(rows, accounts, state)
    save_state(state)
    print("  State saved after position building")

    # Phase 3: Resolution
    if not args.skip_resolve:
        resolve_all(state)              # 3a: closed=true (existing cache)
        resolve_elapsed_markets(state)  # 3b: no closed filter, endDate safety
    else:
        print("\nPhase 3: SKIPPED (--skip-resolve)")
        save_state(state)

    # Phase 4: Output
    print("\nPhase 4: Writing CSV...")
    write_csv(state, accounts)
    save_state(state)

    # Summary
    print("\n" + "=" * 60)
    print("BACKFILL COMPLETE")
    print("=" * 60)
    total_fills = sum(s["fills"]["total"] for s in state["accounts"].values())
    total_pnl = sum(
        s["aggregates"]["total_profit"] - s["aggregates"]["total_loss"]
        for s in state["accounts"].values()
    )
    print(f"  Total fills: {total_fills:,}")
    print(f"  Total PnL:   ${total_pnl:,.2f}")
    print(f"  State:       {STATE_FILE}")
    print(f"  CSV:         {CSV_FILE}")


if __name__ == "__main__":
    main()
