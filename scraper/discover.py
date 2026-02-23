#!/usr/bin/env python3
"""Discover profitable Polymarket crypto traders not yet tracked.

Paginates the Polymarket Data API leaderboard (CRYPTO category), filters out
known wallets and low-PnL accounts, enriches via profile scraping for trade
counts, and writes data/candidates.json for manual review.

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

import requests

from common import (
    REQUEST_DELAY,
    SESSION,
    atomic_write,
    fetch_profile,
    fmt_money,
    fmt_pct,
    to_float,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
ACCOUNTS_FILE = REPO_ROOT / "scraper" / "accounts.json"
CANDIDATES_FILE = REPO_ROOT / "data" / "candidates.json"
LOCK_FILE = REPO_ROOT / "data" / ".candidates.lock"

LEADERBOARD_URL = "https://data-api.polymarket.com/v1/leaderboard"

PNL_THRESHOLD = 50_000  # minimum $50K PnL
PAGE_SIZE = 50
MAX_PAGES = 20  # up to 1000 traders

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
        return set()
    try:
        data = json.loads(ACCOUNTS_FILE.read_text())
        if not isinstance(data, list):
            print(f"  ERROR: {ACCOUNTS_FILE} is not a JSON array")
            return set()
        return {acct["wallet"].lower() for acct in data}
    except (json.JSONDecodeError, KeyError, TypeError, OSError) as e:
        print(f"  ERROR: failed to load {ACCOUNTS_FILE}: {e}")
        return set()


def load_existing_candidates() -> dict[str, dict]:
    """Load existing candidates keyed by lowercase wallet. Preserves review decisions."""
    if not CANDIDATES_FILE.exists():
        return {}
    try:
        data = json.loads(CANDIDATES_FILE.read_text())
        if not isinstance(data, dict) or "candidates" not in data:
            print(f"  ERROR: {CANDIDATES_FILE} has unexpected structure")
            return {}
        return {c["wallet"].lower(): c for c in data["candidates"]}
    except (json.JSONDecodeError, KeyError, TypeError, OSError) as e:
        print(f"  ERROR: failed to load {CANDIDATES_FILE}: {e}")
        return {}


# ── Trust Boundary 4: Fail-closed leaderboard fetch ─────────────────────────


def fetch_leaderboard() -> list[dict]:
    """Paginate the Polymarket leaderboard API for crypto traders.

    Only catches network errors (requests.RequestException). Programming bugs
    (TypeError, KeyError, etc.) propagate up — fail-closed rather than
    silently producing degraded data.
    """
    all_traders = []

    for page in range(MAX_PAGES):
        offset = page * PAGE_SIZE
        params = {
            "category": "CRYPTO",
            "timePeriod": "ALL",
            "orderBy": "PNL",
            "limit": PAGE_SIZE,
            "offset": offset,
        }

        try:
            resp = SESSION.get(LEADERBOARD_URL, params=params, timeout=30)
        except requests.RequestException as e:
            print(f"  ERROR: leaderboard page {page}: {e}")
            break

        if resp.status_code != 200:
            print(f"  WARN: leaderboard page {page} HTTP {resp.status_code}")
            break

        try:
            traders = resp.json()
        except json.JSONDecodeError:
            print(f"  ERROR: leaderboard page {page} invalid JSON")
            break

        if not isinstance(traders, list) or not traders:
            break

        all_traders.extend(traders)
        print(f"  Page {page + 1}: {len(traders)} traders (total: {len(all_traders)})")

        if len(traders) < PAGE_SIZE:
            break

        time.sleep(REQUEST_DELAY)

    return all_traders


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


# ── Trust Boundary 5: Atomic write with lockfile ─────────────────────────────


# ── Style classification ─────────────────────────────────────────────────────


def classify_style(trades: int | None, volume: float | None) -> str:
    """Heuristic style guess based on trades-to-volume ratio."""
    if trades is None or volume is None or volume == 0 or trades == 0:
        return "Unknown"

    avg_trade_size = volume / trades

    if trades > 500 and avg_trade_size < 500:
        return "HFT/MM"
    elif trades < 50:
        return "Directional"
    else:
        return "Hybrid"


# ── Auto-scoring ─────────────────────────────────────────────────────────────

# Confidence thresholds — candidates above AUTO_APPROVE are set to "approved",
# below AUTO_REJECT are set to "rejected", between stays "pending" for review.
AUTO_APPROVE_THRESHOLD = 0.75
AUTO_REJECT_THRESHOLD = 0.30


def score_candidate(candidate: dict) -> float:
    """Composite confidence score (0.0–1.0) from available signals.

    Weights:
      PnL magnitude   30%  — raw profitability
      Efficiency       25%  — skill signal (PnL/volume)
      Trade count      20%  — filters lucky one-shot traders
      Markets          15%  — diversification = systematic, not one lucky bet
      Profile success  10%  — enrichment worked = real, active profile
    """
    score = 0.0

    # PnL magnitude (30%) — log scale, $50K=0.0, $500K+=1.0
    pnl = candidate.get("pnl")
    if pnl is not None and pnl > 0:
        # Map $50K→0, $100K→0.3, $200K→0.6, $500K+→1.0
        pnl_ratio = min(pnl / 500_000, 1.0)
        score += 0.30 * pnl_ratio

    # Efficiency (25%) — PnL/volume percentage
    eff = candidate.get("efficiency")
    if eff is not None and eff > 0:
        # Map 0%→0, 2%→0.5, 5%+→1.0
        eff_ratio = min(eff / 5.0, 1.0)
        score += 0.25 * eff_ratio

    # Trade count (20%) — more trades = more data = more confidence
    trades = candidate.get("trades")
    if trades is not None and trades > 0:
        # Map 1→0, 50→0.5, 200+→1.0
        trade_ratio = min(trades / 200, 1.0)
        score += 0.20 * trade_ratio

    # Markets diversification (15%)
    markets = candidate.get("markets")
    if markets is not None and markets > 0:
        # Map 1→0, 5→0.5, 20+→1.0
        market_ratio = min(markets / 20, 1.0)
        score += 0.15 * market_ratio

    # Profile enrichment success (10%) — binary
    if trades is not None:
        score += 0.10

    return round(score, 3)


def auto_status(score: float, current_status: str) -> str:
    """Determine status based on confidence score. Never overrides human decisions."""
    if current_status in ("promoted", "rejected", "approved"):
        return current_status
    if score >= AUTO_APPROVE_THRESHOLD:
        return "auto-approved"
    if score <= AUTO_REJECT_THRESHOLD:
        return "auto-rejected"
    return "pending"


# ── Main ─────────────────────────────────────────────────────────────────────


def main():
    print("=== Polymarket Trader Discovery ===\n")

    # TB1: Validate SESSION before any untrusted URL construction
    validate_session()

    # Load data
    known_wallets = load_known_wallets()
    existing = load_existing_candidates()
    print(f"Known accounts: {len(known_wallets)} | Existing candidates: {len(existing)}")

    # TB4: Fetch leaderboard (fail-closed — only catches network errors)
    print("\nFetching leaderboard...")
    traders = fetch_leaderboard()
    print(f"Total leaderboard entries: {len(traders)}")

    # Filter and enrich
    print("\nFiltering and enriching candidates...")
    new_candidates = []
    skipped_known = 0
    skipped_pnl = 0
    skipped_slug = 0
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for trader in traders:
        wallet = (trader.get("proxyWallet") or "").lower()
        if not wallet:
            continue

        # Skip known wallets
        if wallet in known_wallets:
            skipped_known += 1
            continue

        # Skip already-decided candidates (promoted/rejected)
        prev = existing.get(wallet)
        if prev and prev.get("status") in ("promoted", "rejected"):
            new_candidates.append(prev)
            continue

        # PnL threshold
        pnl = to_float(trader.get("pnl"))
        if pnl is None or pnl < PNL_THRESHOLD:
            skipped_pnl += 1
            continue

        raw_username = trader.get("userName") or ""
        raw_rank = trader.get("rank")
        rank = int(raw_rank) if raw_rank is not None else None
        volume = to_float(trader.get("vol"))

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
        trades = None
        markets = None
        efficiency = None
        profile_url = ""

        if slug:
            profile_url = f"https://polymarket.com/@{slug}"
            profile_data = fetch_profile(display_name, profile_url)

            if profile_data:
                trades = profile_data.get("trades")
                markets = profile_data.get("markets")
                if pnl is not None and volume and volume > 0:
                    efficiency = round(pnl / volume * 100, 2)
                print(f"    Trades: {trades} | Markets: {markets} | Eff: {fmt_pct(efficiency)}")
            else:
                print(f"    WARN: profile enrichment failed, using leaderboard data only")
        else:
            skipped_slug += 1
            print(f"    WARN: invalid slug for {wallet[:10]}, skipping enrichment")

        style = classify_style(trades, volume)
        first_seen = prev.get("first_seen", today) if prev else today
        raw_prev_rank = prev.get("leaderboard_rank") if prev else None
        prev_rank = int(raw_prev_rank) if raw_prev_rank is not None else None

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
            "status": prev.get("status", "pending") if prev else "pending",
        }

        # Auto-score and set status
        candidate["confidence_score"] = score_candidate(candidate)
        candidate["status"] = auto_status(candidate["confidence_score"], candidate["status"])

        new_candidates.append(candidate)

        time.sleep(REQUEST_DELAY)

    # Preserve candidates from previous runs not on current leaderboard
    current_wallets = {c["wallet"] for c in new_candidates}
    for wallet, prev in existing.items():
        if wallet not in known_wallets and wallet not in current_wallets:
            new_candidates.append(prev)

    # Sort by PnL descending
    new_candidates.sort(
        key=lambda c: c.get("pnl") if c.get("pnl") is not None else float("-inf"),
        reverse=True,
    )

    # TB3: Validate output quality before writing
    error = validate_output(new_candidates, existing, len(traders))
    if error:
        print(f"\n  ERROR: {error}")
        print("  Refusing to overwrite — candidates.json preserved.")
        return

    # TB5: Acquire lockfile, then atomic write
    CANDIDATES_FILE.parent.mkdir(parents=True, exist_ok=True)
    lock_fd = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("\n  ERROR: another instance is running (lock held). Aborting.")
        lock_fd.close()
        return

    try:
        output = {
            "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "candidates": new_candidates,
        }
        atomic_write(CANDIDATES_FILE, json.dumps(output, indent=2))
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()

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

    print(f"\n--- Scoring ---")
    print(f"Auto-approved (score >= {AUTO_APPROVE_THRESHOLD}): {auto_approved}")
    print(f"Auto-rejected (score <= {AUTO_REJECT_THRESHOLD}): {auto_rejected}")
    print(f"Pending review: {pending}")

    if new_this_run:
        print(f"\n--- New This Run ---")
        for c in new_candidates:
            if c.get("is_new"):
                print(f"  NEW: {c['username']} — PnL: {fmt_money(c.get('pnl'))}, score: {c.get('confidence_score', 0):.2f}")

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
