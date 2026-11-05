#!/usr/bin/env python3
"""Discover profitable Polymarket crypto traders not yet tracked.

Paginates the Polymarket Data API leaderboard (crypto category), filters out
known wallets and low-PnL accounts, enriches via profile scraping, and writes
data/candidates.json for manual review.

Trust boundaries enforced:
  1. SESSION validation — no secrets leak via untrusted URLs
  2. Input validation — API data validated before URL construction
  3. Output validation — data quality check before overwriting
  4. Fail-closed errors — unexpected errors preserve existing data
  5. Single-writer — lockfile prevents concurrent corruption

Usage:
    python3 scraper/discover.py
"""

from __future__ import annotations

import fcntl
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

from common import (
    MIN_LEADERBOARD_COUNT,
    REQUEST_DELAY,
    SESSION,
    atomic_write,
    fetch_leaderboard as fetch_crypto_leaderboard,
    fetch_profile,
    fmt_money,
)
from external import accounts_snapshot, candidates_snapshot

REPO_ROOT = Path(__file__).resolve().parent.parent
ACCOUNTS_FILE = REPO_ROOT / "scraper" / "accounts.json"
CANDIDATES_FILE = REPO_ROOT / "data" / "candidates.json"
LOCK_FILE = REPO_ROOT / "data" / ".candidates.lock"

PNL_THRESHOLD = 50_000  # minimum $50K PnL
DISCOVERY_LIMIT = 1000

# Polymarket usernames: alphanumeric, hyphens, underscores (observed pattern)
VALID_SLUG_RE = re.compile(r"^[a-zA-Z0-9_-]+$")
# Wallet addresses: 0x + 40 hex chars
VALID_WALLET_RE = re.compile(r"^0x[0-9a-f]{40}$")


# ── Trust Boundary 1: SESSION validation ─────────────────────────────────────


def validate_session() -> None:
    """Assert SESSION carries no auth secrets before using with untrusted URLs.

    discover.py constructs URLs from untrusted API data (userName field).
    If SESSION carried auth tokens or cookies, those would leak to any URL
    an attacker could influence via their Polymarket username.
    """
    dangerous_headers = {"authorization", "cookie", "x-api-key"}
    for header in SESSION.headers:
        if header.lower() in dangerous_headers:
            raise RuntimeError(
                f"SESSION has '{header}' header — refusing to use with untrusted URLs"
            )
    if SESSION.cookies:
        raise RuntimeError("SESSION has cookies — refusing to use with untrusted URLs")


# ── Trust Boundary 2: Input validation ───────────────────────────────────────


def validate_slug(raw_username: str, wallet: str) -> str | None:
    """Validate and return a safe slug for URL construction.

    Returns None if no safe slug can be determined — caller should skip
    enrichment entirely rather than fall back to unsanitized values.
    """
    is_wallet_name = (
        raw_username.lower() == wallet
        or raw_username.lower().startswith(wallet[:20])
    )

    if raw_username and not is_wallet_name:
        # Custom username — must match strict allowlist (no dots, no slashes)
        if VALID_SLUG_RE.match(raw_username):
            return raw_username
        # Invalid chars in custom name — refuse rather than sanitize
        return None

    # No custom name — use wallet address if it matches hex pattern
    if VALID_WALLET_RE.match(wallet):
        return wallet

    return None


# ── Data loading ─────────────────────────────────────────────────────────────


def load_known_wallets() -> set[str]:
    """Load wallet addresses already in accounts.json (lowercased)."""
    if not ACCOUNTS_FILE.exists():
        raise RuntimeError(f"Required known-wallet file missing: {ACCOUNTS_FILE}")
    try:
        data = json.loads(ACCOUNTS_FILE.read_text())
    except (json.JSONDecodeError, OSError, UnicodeError) as e:
        raise RuntimeError(f"Failed to load {ACCOUNTS_FILE}: {e}") from e
    try:
        data = accounts_snapshot(data)
    except ValueError as exc:
        raise RuntimeError(f"Malformed known-wallet file: {ACCOUNTS_FILE}: {exc}") from exc
    return {acct["wallet"].lower() for acct in data}


def load_existing_candidates() -> dict[str, dict]:
    """Load existing candidates keyed by lowercase wallet. Preserves review decisions."""
    if not CANDIDATES_FILE.exists():
        raise RuntimeError(f"Required candidate file missing: {CANDIDATES_FILE}")
    try:
        data = json.loads(CANDIDATES_FILE.read_text())
    except (json.JSONDecodeError, OSError, UnicodeError) as e:
        raise RuntimeError(f"Failed to load {CANDIDATES_FILE}: {e}") from e
    try:
        rows = candidates_snapshot(data)
    except ValueError as exc:
        raise RuntimeError(f"Malformed candidate file: {CANDIDATES_FILE}: {exc}") from exc
    candidates = {c["wallet"].lower(): c for c in rows}
    if len(candidates) != len(rows):
        raise RuntimeError(f"Duplicate wallets in candidate file: {CANDIDATES_FILE}")
    return candidates


# ── Trust Boundary 4: Fail-closed leaderboard fetch ─────────────────────────


def fetch_leaderboard() -> list[dict]:
    """Fetch crypto traders; API failures abort discovery before writing."""
    traders = fetch_crypto_leaderboard(DISCOVERY_LIMIT)
    if len(traders) < MIN_LEADERBOARD_COUNT:
        raise RuntimeError(f"Short discovery leaderboard: {len(traders)}/{MIN_LEADERBOARD_COUNT}")
    return traders


# ── Trust Boundary 3: Output validation ──────────────────────────────────────


def validate_output(
    new_candidates: list[dict], existing: dict[str, dict], traders_fetched: int
) -> str | None:
    """Check data quality before writing. Returns error message or None if OK.

    Guards against:
    - API returning zero data (network/endpoint failure)
    - API returning garbage that passes filters but drops >80% of known candidates
    """
    if traders_fetched == 0 and existing:
        return "Leaderboard returned zero data but existing candidates exist"

    if existing:
        fresh_count = sum(1 for c in new_candidates if c.get("status") == "pending")
        prev_pending = sum(
            1 for c in existing.values() if c.get("status") == "pending"
        )
        if prev_pending > 10 and fresh_count < prev_pending * 0.2:
            return (
                f"Suspicious data drop: {fresh_count} fresh candidates vs "
                f"{prev_pending} previously pending (>80% loss)"
            )

    return None


# ── Main ─────────────────────────────────────────────────────────────────────


def main():
    print("=== Polymarket Trader Discovery ===\n")

    # TB1: Validate SESSION before any untrusted URL construction
    validate_session()

    # Load data
    known_wallets = load_known_wallets()
    existing = load_existing_candidates()

    # TB4: Fetch leaderboard (errors abort before any write)
    print("\nFetching leaderboard...")
    traders = fetch_leaderboard()
    print(f"Total leaderboard entries: {len(traders)}")
    print("::warning title=Discovery scoring unavailable::Polymarket v2 board volume is shares, not USDC; new candidates require manual review")

    # Fetch network data before locking. The first read is only a worklist hint;
    # decisions and known wallets are loaded again under the promotion lock.
    profiles = {}
    for trader in traders:
        wallet = trader["user_id"].lower()
        if wallet in known_wallets:
            continue
        prev = existing.get(wallet)
        if prev and prev.get("status") in ("promoted", "rejected", "approved", "auto-rejected", "auto-approved"):
            continue
        pnl = trader["pnl"]
        if pnl < PNL_THRESHOLD:
            continue
        raw_username = trader["user_name"]
        slug = validate_slug(raw_username, wallet)
        if slug:
            is_wallet_name = raw_username.lower() == wallet or raw_username.lower().startswith(wallet[:20])
            display_name = raw_username if raw_username and not is_wallet_name else wallet[:10]
            profiles[wallet] = fetch_profile(display_name, f"https://polymarket.com/@{slug}")
            time.sleep(REQUEST_DELAY)

    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    lock_fd = open(LOCK_FILE, "a")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        lock_fd.close()
        raise RuntimeError("Another discovery instance is running (lock held); candidates.json preserved")
    try:
        _merge_and_write(traders, profiles)
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()


def _merge_and_write(traders: list[dict], profiles: dict[str, dict | None]):
    """Re-read promotion state and write candidates while holding its lock."""
    known_wallets = load_known_wallets()
    existing = load_existing_candidates()
    print(f"Known accounts: {len(known_wallets)} | Existing candidates: {len(existing)}")

    # Filter and enrich
    print("\nFiltering and enriching candidates...")
    new_candidates = []
    skipped_known = 0
    skipped_pnl = 0
    skipped_slug = 0
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for trader in traders:
        wallet = trader["user_id"].lower()

        # Skip known wallets
        if wallet in known_wallets:
            skipped_known += 1
            continue

        # Preserve decisions and their historical scores and values.
        prev = existing.get(wallet)
        if prev and prev.get("status") in ("promoted", "rejected", "approved", "auto-rejected", "auto-approved"):
            new_candidates.append(prev)
            continue

        # PnL threshold
        pnl = trader["pnl"]
        if pnl < PNL_THRESHOLD:
            skipped_pnl += 1
            continue

        raw_username = trader["user_name"]
        rank = trader["rank"]
        # v2 board volume is shares, not USDC; scoring expects USDC.
        volume = prev.get("volume") if prev else None

        # TB2: Validate slug before URL construction — refuse rather than sanitize
        slug = validate_slug(raw_username, wallet)
        is_wallet_name = (
            raw_username.lower() == wallet
            or raw_username.lower().startswith(wallet[:20])
        )
        is_custom_name = raw_username and not is_wallet_name
        display_name = raw_username if is_custom_name else wallet[:10]

        print(f"  [{len(new_candidates) + 1}] {display_name} — PnL: {fmt_money(pnl)}, rank: {rank}")

        # Enrich via profile — only if slug validated
        trades = prev.get("trades") if prev else None
        markets = prev.get("markets") if prev else None
        efficiency = prev.get("efficiency") if prev else None
        profile_url = prev.get("profile_url") if prev else None

        if slug:
            profile_url = f"https://polymarket.com/@{slug}"
            profile_data = profiles.get(wallet)

            if profile_data:
                trades = profile_data.get("trades") if profile_data.get("trades") is not None else trades
                markets = profile_data.get("markets") if profile_data.get("markets") is not None else markets
                print(f"    Trades: {trades} | Markets: {markets}")
            else:
                print(f"    WARN: profile enrichment failed, using leaderboard data only")
        else:
            skipped_slug += 1
            print(f"    WARN: invalid slug for {wallet[:10]}, skipping enrichment")

        style = prev.get("style_guess") if prev else None
        first_seen = prev.get("first_seen") if prev else today
        prev_rank = prev.get("leaderboard_rank") if prev else None

        candidate = {
            "wallet": wallet,
            "username": display_name,
            "profile_url": profile_url,
            "discovery_source": "leaderboard",
            "leaderboard_rank": rank,
            "prev_rank": prev_rank,
            "pnl": pnl,
            "volume": volume,
            "efficiency": efficiency,
            "trades": trades,
            "markets": markets,
            "style_guess": style,
            "first_seen": first_seen,
            "is_new": prev is None,
            "status": prev["status"] if prev else "pending",
            "confidence_score": prev.get("confidence_score") if prev else None,
        }

        new_candidates.append(candidate)

    # Preserve candidates from previous runs not on current leaderboard
    current_wallets = {c["wallet"].lower() for c in new_candidates}
    for wallet, prev in existing.items():
        if wallet not in known_wallets and wallet not in current_wallets:
            new_candidates.append(prev)

    # Sort by PnL descending
    new_candidates.sort(
        key=lambda c: c["pnl"],
        reverse=True,
    )

    # TB3: Validate output quality before writing
    error = validate_output(new_candidates, existing, len(traders))
    if error:
        raise RuntimeError(f"Discovery refused: {error}; candidates.json preserved")

    output = {
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "candidates": new_candidates,
    }
    atomic_write(CANDIDATES_FILE, json.dumps(output, indent=2))

    # Summary stats
    auto_approved = sum(1 for c in new_candidates if c.get("status") == "auto-approved")
    auto_rejected = sum(1 for c in new_candidates if c.get("status") == "auto-rejected")
    pending = sum(1 for c in new_candidates if c.get("status") == "pending")
    new_this_run = sum(1 for c in new_candidates if c.get("is_new"))

    print(f"\n=== Results ===")
    print(f"Candidates written: {len(new_candidates)}")
    print(f"Skipped (known): {skipped_known}")
    print(f"Skipped (PnL < ${PNL_THRESHOLD:,}): {skipped_pnl}")
    if skipped_slug:
        print(f"Skipped enrichment (invalid slug): {skipped_slug}")

    print(f"\n--- Review status ---")
    print(f"Existing auto-approved: {auto_approved}")
    print(f"Existing auto-rejected: {auto_rejected}")
    print(f"Pending review: {pending}")

    if new_this_run:
        print(f"\n--- New This Run ---")
        for c in new_candidates:
            if c.get("is_new"):
                print(f"  NEW: {c['username']} — PnL: {fmt_money(c.get('pnl'))}, pending review")

    # Rank movers (candidates that were previously tracked)
    movers = []
    for c in new_candidates:
        prev_r = c.get("prev_rank")
        curr_r = c.get("leaderboard_rank")
        if prev_r is not None and curr_r is not None and prev_r != curr_r:
            movers.append((c["username"], prev_r, curr_r, curr_r - prev_r))
    if movers:
        movers.sort(key=lambda m: m[3])  # best movers first (negative = improved)
        print(f"\n--- Rank Changes ---")
        for name, prev_r, curr_r, delta in movers[:10]:
            arrow = "↑" if delta < 0 else "↓"
            print(f"  {arrow} {name}: #{prev_r} → #{curr_r} ({delta:+d})")

    print(f"\nOutput: {CANDIDATES_FILE}")


if __name__ == "__main__":
    main()
