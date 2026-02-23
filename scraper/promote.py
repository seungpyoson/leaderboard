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
import json
import re
from pathlib import Path

from common import atomic_write

REPO_ROOT = Path(__file__).resolve().parent.parent
ACCOUNTS_FILE = REPO_ROOT / "scraper" / "accounts.json"
CANDIDATES_FILE = REPO_ROOT / "data" / "candidates.json"
LOCK_FILE = REPO_ROOT / "data" / ".candidates.lock"

VALID_WALLET_RE = re.compile(r"^0x[0-9a-f]{40}$")


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
        candidates_data = json.loads(CANDIDATES_FILE.read_text())
    except (json.JSONDecodeError, OSError) as e:
        print(f"ERROR: failed to read {CANDIDATES_FILE}: {e}")
        raise SystemExit(1)

    if not isinstance(candidates_data, dict) or "candidates" not in candidates_data:
        print(f"ERROR: {CANDIDATES_FILE} has unexpected structure (expected dict with 'candidates' key)")
        raise SystemExit(1)

    candidates = candidates_data["candidates"]
    if not isinstance(candidates, list):
        print(f"ERROR: {CANDIDATES_FILE} 'candidates' is not a list")
        raise SystemExit(1)

    approved = [c for c in candidates if isinstance(c, dict) and c.get("status") in ("approved", "auto-approved")]
    if not approved:
        print("No candidates with status 'approved' — nothing to promote.")
        return

    # Validate and load existing accounts
    if ACCOUNTS_FILE.exists():
        try:
            accounts = json.loads(ACCOUNTS_FILE.read_text())
        except (json.JSONDecodeError, OSError) as e:
            print(f"ERROR: failed to read {ACCOUNTS_FILE}: {e}")
            raise SystemExit(1)
        if not isinstance(accounts, list):
            print(f"ERROR: {ACCOUNTS_FILE} is not a JSON array")
            raise SystemExit(1)
    else:
        accounts = []

    existing_wallets = {a["wallet"].lower() for a in accounts if isinstance(a, dict) and a.get("wallet")}

    promoted = 0
    for candidate in approved:
        wallet = (candidate.get("wallet") or "").lower()
        username = candidate.get("username", wallet[:10])
        profile_url = candidate.get("profile_url", "")

        if not wallet or not VALID_WALLET_RE.match(wallet):
            print(f"  SKIP: candidate has missing or malformed wallet address")
            candidate["status"] = "invalid"
            continue

        if wallet in existing_wallets:
            print(f"  SKIP: {username} already in accounts.json")
            candidate["status"] = "promoted"
            continue

        account_entry = {
            "name": username,
            "wallet": wallet,
            "profile_url": profile_url,
            "style": candidate.get("style_guess", ""),
            "note": f"Auto-discovered via {candidate.get('discovery_source', 'leaderboard')}. "
                    f"Rank {candidate.get('leaderboard_rank', '?')}.",
        }
        accounts.append(account_entry)
        existing_wallets.add(wallet)
        candidate["status"] = "promoted"
        promoted += 1
        pnl = candidate.get("pnl", 0)
        try:
            pnl_str = f"${float(pnl):,.0f}"
        except (ValueError, TypeError):
            pnl_str = str(pnl)
        print(f"  PROMOTED: {username} (PnL: {pnl_str})")

    # Write accounts FIRST — the authoritative addition.
    # If crash occurs here, candidates stay "approved" and next run re-promotes
    # (wallet dedup makes this idempotent).
    atomic_write(ACCOUNTS_FILE, json.dumps(accounts, indent=2) + "\n")

    # Then mark candidates as "promoted" — safe because re-promotion is idempotent.
    atomic_write(CANDIDATES_FILE, json.dumps(candidates_data, indent=2) + "\n")

    print(f"\nPromoted {promoted} candidate(s) to {ACCOUNTS_FILE}")


if __name__ == "__main__":
    main()
