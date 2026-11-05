"""Shared utilities for Polymarket profile scraping.

Used by fetch.py (daily refresh), discover.py (weekly candidate discovery),
and promote.py (candidate promotion).

Rate limiting contract: leaderboard pagination sleeps here; other callers
sleep REQUEST_DELAY between requests.
"""

from __future__ import annotations

import json
import re
import tempfile
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

from external import LeaderboardRow, ProfileQueries, load_leaderboard_response, load_profile_response, profile_html

REQUEST_DELAY = 0.5
LEADERBOARD_URL = "https://data-api.polymarket.com/v2/leaderboard"
LEADERBOARD_PAGE_SIZE = 50
MIN_LEADERBOARD_COUNT = 800  # Most historical daily tracked snapshots contain 800 rows.

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml",
})


def fetch_leaderboard(limit: int) -> list[LeaderboardRow]:
    """Read the crypto/all-time PnL board, following v2's opaque cursor."""
    if limit < 1:
        raise ValueError("leaderboard limit must be positive")
    traders: list[LeaderboardRow] = []
    cursor = None
    seen_cursors = set()

    while len(traders) < limit:
        params = ({"cursor": cursor} if cursor else {
            "category": "crypto",
            "time_period": "all",
            "sort_by": "PNL",
            "limit": min(LEADERBOARD_PAGE_SIZE, limit),
        })
        try:
            response = SESSION.get(LEADERBOARD_URL, params=params, timeout=30)
            response.raise_for_status()
            page, pagination = load_leaderboard_response(response)
        except (requests.RequestException, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Leaderboard page failed after {len(traders)} rows: {exc}") from exc
        except ValueError as exc:
            raise RuntimeError(f"Leaderboard returned a malformed page: {exc}") from exc
        traders.extend(page)
        if len(traders) >= limit:
            break
        if not pagination.has_more:
            break
        cursor = pagination.next_cursor
        if not isinstance(cursor, str) or not cursor or cursor in seen_cursors:
            raise RuntimeError("Leaderboard returned an invalid next cursor")
        seen_cursors.add(cursor)
        if len(traders) < limit:
            time.sleep(REQUEST_DELAY)

    traders = traders[:limit]
    wallets = [row.user_id.lower() for row in traders]
    if len(set(wallets)) != len(wallets):
        raise RuntimeError("Leaderboard returned duplicate wallets")
    return traders


def extract_profile_data(html: str) -> ProfileQueries | None:
    """Extract PnL/volume/stats from __NEXT_DATA__ embedded in profile page HTML."""
    return profile_html(html)


def _is_polymarket_host(url: str) -> bool:
    """Check if a URL's hostname is exactly polymarket.com."""
    try:
        parsed = urlparse(url)
        return parsed.hostname == "polymarket.com" and parsed.scheme == "https"
    except Exception:
        return False


def fetch_profile(label: str, profile_url: str) -> ProfileQueries | None:
    """Fetch profile page and extract data. Only fetches from polymarket.com.

    URL validation uses hostname parsing (not string matching) to prevent
    SSRF via crafted URLs like polymarket.com.attacker.com.
    """
    # Extract @slug and reconstruct a clean URL (prevents path injection)
    slug_match = re.search(r'polymarket\.com/(@[a-zA-Z0-9_-]+)', profile_url)
    if slug_match:
        url = f"https://polymarket.com/{slug_match.group(1)}"
    elif _is_polymarket_host(profile_url):
        url = profile_url
    else:
        print(f"  SKIP: {label} — URL not on polymarket.com")
        return None

    try:
        resp = SESSION.get(url, timeout=30, allow_redirects=False)
        # Follow redirects manually — only if they stay on polymarket.com
        redirects = 0
        while resp.status_code in (301, 302, 303, 307, 308) and redirects < 5:
            location = resp.headers.get("Location")
            if not location:
                raise RuntimeError("Polymarket redirect lacks Location")
            # Resolve relative redirects (e.g. /profile/...) against current URL
            resolved = urljoin(url, location)
            if not _is_polymarket_host(resolved):
                print(f"  WARN: {label} redirected to non-polymarket host: {location}")
                return None
            url = resolved
            resp = SESSION.get(url, timeout=30, allow_redirects=False)
            redirects += 1
        if resp.status_code != 200:
            print(f"  WARN: {label} HTTP {resp.status_code}")
            return None
        return load_profile_response(resp)
    except requests.RequestException as e:
        print(f"  ERROR: {label} {e}")
        return None


def atomic_write(path: Path, content: str) -> None:
    """Write content to file atomically via temp file + rename.

    Shared by discover.py and promote.py to prevent partial writes on crash.
    """
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with open(fd, "w") as f:
            f.write(content)
        Path(tmp).replace(path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def fmt_money(val) -> str:
    if val is None:
        return "—"
    try:
        v = float(val)
    except (ValueError, TypeError):
        return "—"
    sign = "-" if v < 0 else ""
    v = abs(v)
    if v >= 1_000_000:
        return f"{sign}${v/1_000_000:.2f}M"
    elif v >= 1_000:
        return f"{sign}${v/1_000:.1f}K"
    else:
        return f"{sign}${v:.2f}"


def fmt_pct(val) -> str:
    if val is None:
        return "—"
    try:
        return f"{float(val):.2f}%"
    except (ValueError, TypeError):
        return "—"
