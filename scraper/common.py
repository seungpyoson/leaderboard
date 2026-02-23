"""Shared utilities for Polymarket profile scraping.

Used by fetch.py (daily refresh), discover.py (weekly candidate discovery),
and promote.py (candidate promotion).

Rate limiting contract: REQUEST_DELAY is defined here but enforced by callers.
Each caller must sleep(REQUEST_DELAY) between requests. This avoids double-sleeping
when callers batch multiple operations between requests.
"""

from __future__ import annotations

import json
import re
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import requests

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


def _is_polymarket_host(url: str) -> bool:
    """Check if a URL's hostname is exactly polymarket.com."""
    try:
        parsed = urlparse(url)
        return parsed.hostname == "polymarket.com" and parsed.scheme == "https"
    except Exception:
        return False


def fetch_profile(label: str, profile_url: str) -> dict | None:
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
        resp = SESSION.get(url, timeout=30, allow_redirects=True)
        if resp.status_code != 200:
            print(f"  WARN: {label} HTTP {resp.status_code}")
            return None
        if not _is_polymarket_host(resp.url):
            print(f"  WARN: {label} redirected to {resp.url}")
            return None
        return extract_profile_data(resp.text)
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
