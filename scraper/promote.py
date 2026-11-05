#!/usr/bin/env python3
"""Promote approved candidates from data/candidates.json to scraper/accounts.json.

Reads candidates with status "approved", appends them to accounts.json with proper
format, and marks them "promoted" in candidates.json.

The entire read-modify-write cycle runs under an exclusive lockfile (shared with
discover.py) to prevent concurrent modification. Write order: accounts.json first,
then candidates.json. If crash occurs between the two writes, candidates stay
"approved" and the next run re-promotes them — the wallet dedup check makes this
idempotent.

Usage:
    1. Edit data/candidates.json — set status to "approved" for desired candidates
    2. python3 scraper/promote.py
"""

from __future__ import annotations

import fcntl
from dataclasses import asdict, replace
import json
from pathlib import Path

from common import atomic_write
from external import Account, load_accounts, load_candidates

REPO_ROOT = Path(__file__).resolve().parent.parent
ACCOUNTS_FILE = REPO_ROOT / "scraper" / "accounts.json"
CANDIDATES_FILE = REPO_ROOT / "data" / "candidates.json"
LOCK_FILE = REPO_ROOT / "data" / ".candidates.lock"

def main():
    if not CANDIDATES_FILE.exists():
        print(f"ERROR: {CANDIDATES_FILE} not found — run discover.py first")
        raise SystemExit(1)

    # Acquire lockfile BEFORE reading — protects the entire read-modify-write cycle.
    # promote.py has no network calls, so holding the lock for the full run is fine.
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    lock_fd = open(LOCK_FILE, "a")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("ERROR: another instance (or discover.py) is running (lock held). Aborting.")
        lock_fd.close()
        raise SystemExit(1)

    try:
        _run_promotion()
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()


def _run_promotion():
    """Core promotion logic — must be called under lock."""

    # Validate and load candidates
    try:
        candidates_data = load_candidates(CANDIDATES_FILE)
    except (json.JSONDecodeError, OSError) as e:
        print(f"ERROR: failed to read {CANDIDATES_FILE}: {e}")
        raise SystemExit(1)

    except ValueError as exc:
        raise RuntimeError(f"Malformed candidate file: {exc}") from exc

    candidates = list(candidates_data.candidates)

    approved_indices = [i for i, c in enumerate(candidates)
                        if c.status in ("approved", "auto-approved")]

    # Validate and load existing accounts
    if not ACCOUNTS_FILE.is_file():
        raise RuntimeError(f"Required accounts file missing: {ACCOUNTS_FILE}")
    try:
        accounts = load_accounts(ACCOUNTS_FILE)
    except (json.JSONDecodeError, OSError, ValueError) as exc:
        raise RuntimeError(f"Malformed accounts file: {ACCOUNTS_FILE}: {exc}") from exc

    if not approved_indices:
        print("No candidates with status 'approved' — nothing to promote.")
        return

    existing_wallets = {a.wallet.lower() for a in accounts}

    promoted = 0
    for candidate_index in approved_indices:
        candidate = candidates[candidate_index]
        wallet = candidate.wallet.lower()
        username = candidate.username
        profile_url = candidate.profile_url
        if not username or not profile_url:
            raise RuntimeError(f"Approved candidate {wallet} lacks username or profile_url")

        if wallet in existing_wallets:
            print(f"  SKIP: {username} already in accounts.json")
            candidates[candidate_index] = replace(candidate, status="promoted")
            continue

        source = candidate.discovery_source
        rank = candidate.leaderboard_rank
        note = "Auto-discovered"
        if source:
            note += f" via {source}"
        if rank is not None:
            note += f". Rank {rank}"
        account_entry = Account(
            name=username, wallet=wallet, profile_url=profile_url,
            style=candidate.style_guess, note=note + ".",
        )
        accounts.append(account_entry)
        existing_wallets.add(wallet)
        candidates[candidate_index] = replace(candidate, status="promoted")
        promoted += 1
        pnl = candidate.pnl
        pnl_str = f"${pnl:,.0f}"
        print(f"  PROMOTED: {username} (PnL: {pnl_str})")

    # Write accounts FIRST — the authoritative addition.
    # If crash occurs here, candidates stay "approved" and next run re-promotes
    # (wallet dedup makes this idempotent).
    atomic_write(ACCOUNTS_FILE, json.dumps([asdict(a) for a in accounts], indent=2) + "\n")

    # Then mark candidates as "promoted" — safe because re-promotion is idempotent.
    candidate_output = {"candidates": [asdict(c) for c in candidates]}
    if candidates_data.updated is not None:
        candidate_output["updated"] = candidates_data.updated
    atomic_write(CANDIDATES_FILE, json.dumps(candidate_output, indent=2) + "\n")

    print(f"\nPromoted {promoted} candidate(s) to {ACCOUNTS_FILE}")


if __name__ == "__main__":
    main()
