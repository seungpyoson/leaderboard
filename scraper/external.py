"""Validate untrusted records once, before scraper code uses their fields."""

from __future__ import annotations

import math
import json
from decimal import Decimal
import re


WALLET = re.compile(r"0x[0-9a-fA-F]{40}\Z")


def _object(value, label: str) -> dict:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return value


def _field(row: dict, key: str, kind: str, label: str, *, optional: bool = False):
    if key not in row:
        if optional:
            return None
        raise ValueError(f"{label}.{key} is required")
    value = row[key]
    if value is None and optional:
        return None
    valid = {
        "str": type(value) is str,
        "int": type(value) is int,
        "bool": type(value) is bool,
        "number": type(value) in (int, float) and math.isfinite(value) if type(value) in (int, float) else False,
    }[kind]
    if not valid:
        raise ValueError(f"{label}.{key} must be {kind}{' or null' if optional else ''}")
    return value


def _wallet(row: dict, key: str, label: str) -> str:
    value = _field(row, key, "str", label)
    if not WALLET.fullmatch(value):
        raise ValueError(f"{label}.{key} must be a wallet address")
    return value


def leaderboard_page(body) -> tuple[list[dict], dict]:
    body = _object(body, "leaderboard")
    page = body["data"] if "data" in body else None
    if not isinstance(page, list) or not page:
        raise ValueError("leaderboard.data must be a nonempty array")
    rows = []
    for i, raw in enumerate(page):
        row = _object(raw, f"leaderboard.data[{i}]")
        label = f"leaderboard.data[{i}]"
        _wallet(row, "user_id", label)
        for key in ("user_name", "profile_image", "x_username"):
            _field(row, key, "str", label)
        for key in ("pnl", "volume"):
            _field(row, key, "number", label)
        _field(row, "verified", "bool", label)
        rank = _field(row, "rank", "int", label)
        if rank < 0:
            raise ValueError(f"{label}.rank must be nonnegative")
        rows.append(row)
    pagination = _object(body["pagination"] if "pagination" in body else None, "leaderboard.pagination")
    for key in ("limit", "offset"):
        value = _field(pagination, key, "int", "leaderboard.pagination")
        if value < 0:
            raise ValueError(f"leaderboard.pagination.{key} must be nonnegative")
    _field(pagination, "has_more", "bool", "leaderboard.pagination")
    _field(pagination, "next_cursor", "str", "leaderboard.pagination", optional=True)
    return rows, pagination


def user_stats(body, wallet: str) -> dict | None:
    body = _object(body, "user stats")
    if "data" not in body:
        raise ValueError("user stats.data is required")
    if body["data"] is None:
        return None  # Documented unknown user.
    row = _object(body["data"], "user stats.data")
    if _wallet(row, "proxy_wallet", "user stats.data").lower() != wallet.lower():
        raise ValueError("user stats.data.proxy_wallet does not match request")
    for key in ("trades", "views"):
        if _field(row, key, "int", "user stats.data") < 0:
            raise ValueError(f"user stats.data.{key} must be nonnegative")
    _field(row, "biggest_win", "number", "user stats.data")
    _field(row, "join_date", "int", "user stats.data", optional=True)
    return {"trades": row["trades"], "largest_win": row["biggest_win"]}


def dune_results(body) -> list[dict]:
    body = _object(body, "Dune")
    result = _object(body["result"] if "result" in body else None, "Dune.result")
    rows = result["rows"] if "rows" in result else None
    if not isinstance(rows, list):
        raise ValueError("Dune.result.rows must be an array")
    parsed = []
    for i, raw in enumerate(rows):
        label = f"Dune.result.rows[{i}]"
        row = _object(raw, label)
        _wallet(row, "wallet", label)
        _field(row, "first_seen", "str", label)
        for key in ("total_received", "total_sent", "net_capital"):
            _field(row, key, "number", label)
        parsed.append(row)
    return parsed


def dune_execution(body) -> str:
    body = _object(body, "Dune execution")
    execution_id = _field(body, "execution_id", "str", "Dune execution")
    if not execution_id:
        raise ValueError("Dune execution.execution_id must be nonempty")
    return execution_id


def dune_status(body) -> str:
    body = _object(body, "Dune status")
    state = _field(body, "state", "str", "Dune status")
    if state not in {"QUERY_STATE_PENDING", "QUERY_STATE_EXECUTING", "QUERY_STATE_COMPLETED",
                     "QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"}:
        raise ValueError(f"Dune status.state is unknown: {state}")
    return state


def capital_map(body) -> dict[str, dict]:
    body = _object(body, "capital map")
    for wallet, raw in body.items():
        if not WALLET.fullmatch(wallet):
            raise ValueError("capital map key must be a wallet address")
        row = _object(raw, f"capital map[{wallet}]")
        _field(row, "first_seen", "str", "capital map")
        for key in ("total_received", "total_sent", "net_capital"):
            _field(row, key, "number", "capital map")
    return body


def profile_snapshot(body) -> list[dict]:
    body = _object(body, "profiles snapshot")
    rows = body["profiles"] if "profiles" in body else None
    if not isinstance(rows, list):
        raise ValueError("profiles snapshot.profiles must be an array")
    for i, raw in enumerate(rows):
        label = f"profiles snapshot.profiles[{i}]"
        row = _object(raw, label)
        _wallet(row, "wallet", label)
        trades = _field(row, "trades", "int", label, optional=True)
        if trades is not None and trades < 0:
            raise ValueError(f"{label}.trades must be nonnegative")
        _field(row, "largest_win", "number", label, optional=True)
        row["trades"] = trades
        row["largest_win"] = row["largest_win"] if "largest_win" in row else None
    return rows


def candidates_snapshot(body) -> list[dict]:
    body = _object(body, "candidates snapshot")
    rows = body["candidates"] if "candidates" in body else None
    if not isinstance(rows, list):
        raise ValueError("candidates snapshot.candidates must be an array")
    for i, raw in enumerate(rows):
        label = f"candidates snapshot.candidates[{i}]"
        row = _object(raw, label)
        _wallet(row, "wallet", label)
        _field(row, "status", "str", label)
        _field(row, "pnl", "number", label)
        for key in ("username", "profile_url", "discovery_source", "style_guess", "first_seen"):
            _field(row, key, "str", label, optional=True)
        for key in ("leaderboard_rank", "prev_rank", "trades", "markets"):
            value = row[key] if key in row else None
            if key in ("leaderboard_rank", "prev_rank") and type(value) is str and value.isdecimal():
                # Early committed candidate snapshots stored ranks as decimal text.
                row[key] = int(value)
            _field(row, key, "int", label, optional=True)
        for key in ("volume", "efficiency", "confidence_score"):
            _field(row, key, "number", label, optional=True)
        _field(row, "is_new", "bool", label, optional=True)
    return rows


def accounts_snapshot(body) -> list[dict]:
    if not isinstance(body, list):
        raise ValueError("accounts snapshot must be an array")
    for i, raw in enumerate(body):
        label = f"accounts snapshot[{i}]"
        row = _object(raw, label)
        _wallet(row, "wallet", label)
        for key in ("name", "profile_url"):
            _field(row, key, "str", label)
        for key in ("style", "note"):
            _field(row, key, "str", label, optional=True)
    return body


def goldsky_page(body) -> list[dict]:
    body = _object(body, "Goldsky")
    data = _object(body["data"] if "data" in body else None, "Goldsky.data")
    events = data["orderFilledEvents"] if "orderFilledEvents" in data else None
    if not isinstance(events, list):
        raise ValueError("Goldsky.data.orderFilledEvents must be an array")
    for i, raw in enumerate(events):
        label = f"Goldsky.data.orderFilledEvents[{i}]"
        row = _object(raw, label)
        for key in ("id", "timestamp", "makerAssetId", "takerAssetId",
                    "makerAmountFilled", "takerAmountFilled"):
            value = _field(row, key, "str", label)
            if key != "id" and not value.isdecimal():
                raise ValueError(f"{label}.{key} must be decimal text")
        for key in ("maker", "taker"):
            _wallet(row, key, label)
    return events


def gamma_market(raw, *, require_end_date: bool = False) -> dict:
    row = _object(raw, "Gamma market")
    parsed = {}
    for source, output in (("outcomePrices", "prices"), ("clobTokenIds", "clob_ids")):
        value = row[source] if source in row else None
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Gamma market.{source} is invalid JSON") from exc
        if not isinstance(value, list) or not value:
            raise ValueError(f"Gamma market.{source} must be a nonempty array")
        parsed[output] = value
    if len(parsed["prices"]) != len(parsed["clob_ids"]):
        raise ValueError("Gamma market price/token arrays have different lengths")
    for price in parsed["prices"]:
        if type(price) not in (str, int, float):
            raise ValueError("Gamma market price must be numeric")
        try:
            numeric = float(price)
        except ValueError as exc:
            raise ValueError("Gamma market price must be numeric") from exc
        if not math.isfinite(numeric):
            raise ValueError("Gamma market price must be finite")
    if not all(type(token) is str and token.isdecimal() for token in parsed["clob_ids"]):
        raise ValueError("Gamma market token ids must be decimal text")
    parsed["question"] = _field(row, "question", "str", "Gamma market", optional=True)
    parsed["end_date"] = _field(row, "endDate", "str", "Gamma market", optional=not require_end_date)
    return parsed


def goldsky_state(body) -> dict:
    body = _object(body, "Goldsky state")
    accounts = _object(body["accounts"] if "accounts" in body else None, "Goldsky state.accounts")
    cache = _object(body["resolution_cache"] if "resolution_cache" in body else None,
                    "Goldsky state.resolution_cache")
    for name, raw in accounts.items():
        if type(name) is not str:
            raise ValueError("Goldsky state account name must be text")
        label = f"Goldsky state.accounts[{name}]"
        account = _object(raw, label)
        _wallet(account, "wallet", label)
        for key in ("maker_last_id", "taker_last_id"):
            _field(account, key, "str", label)
        for key in ("maker_exhausted", "taker_exhausted", "maker_at_tip", "taker_at_tip"):
            _field(account, key, "bool", label, optional=True)  # Legacy state lacks these hints.
        fills = _object(account["fills"] if "fills" in account else None, f"{label}.fills")
        for key in ("total", "maker", "taker"):
            if _field(fills, key, "int", f"{label}.fills") < 0:
                raise ValueError(f"{label}.fills.{key} must be nonnegative")
        bounds = _object(account["day_bounds"] if "day_bounds" in account else None,
                         f"{label}.day_bounds")
        for day, pair in bounds.items():
            if type(day) is not str or not isinstance(pair, list) or len(pair) != 2 or any(type(n) is not int for n in pair):
                raise ValueError(f"{label}.day_bounds entry is malformed")
        positions = _object(account["positions"] if "positions" in account else None,
                            f"{label}.positions")
        for token, raw_pos in positions.items():
            if type(token) is not str:
                raise ValueError(f"{label}.positions token must be text")
            pos = _object(raw_pos, f"{label}.positions[{token}]")
            for key in ("net_shares", "cost_basis"):
                _field(pos, key, "number", f"{label}.positions[{token}]")
            _field(pos, "fill_count", "int", f"{label}.positions[{token}]")
        agg = _object(account["aggregates"] if "aggregates" in account else None,
                      f"{label}.aggregates")
        for key in ("first_trade", "last_trade"):
            _field(agg, key, "str", f"{label}.aggregates", optional=True)
        for key in ("total_volume", "total_profit", "total_loss", "welford_mean", "welford_m2"):
            _field(agg, key, "number", f"{label}.aggregates")
        for key in ("wins", "losses", "welford_n"):
            _field(agg, key, "int", f"{label}.aggregates")
        top = _object(agg["top_markets"] if "top_markets" in agg else None,
                      f"{label}.aggregates.top_markets")
        for question, value in top.items():
            if type(question) is not str or type(value) not in (int, float) or not math.isfinite(value):
                raise ValueError(f"{label}.aggregates.top_markets entry is malformed")
    for token, raw in cache.items():
        if type(token) is not str:
            raise ValueError("Goldsky state cache token must be text")
        label = f"Goldsky state.resolution_cache[{token}]"
        entry = _object(raw, label)
        _field(entry, "payout", "number", label)
        _field(entry, "cached_at", "str", label)
        _field(entry, "question", "str", label, optional=True)
    return body


def profile_queries(raw) -> dict:
    """Parse optional profile-page enrichment; absent metrics stay None."""
    body = _object(raw, "profile page")
    props = _object(body["props"] if "props" in body else None, "profile page.props")
    page = _object(props["pageProps"] if "pageProps" in props else None, "profile page.pageProps")
    dehydrated = _object(page["dehydratedState"] if "dehydratedState" in page else None,
                         "profile page.dehydratedState")
    queries = dehydrated["queries"] if "queries" in dehydrated else None
    if not isinstance(queries, list):
        raise ValueError("profile page.queries must be an array")
    result = {key: None for key in ("pnl", "volume", "positions_value", "biggest_win",
                                    "trades", "markets", "join_date")}
    for i, raw_query in enumerate(queries):
        label = f"profile page.queries[{i}]"
        query = _object(raw_query, label)
        keys = query["queryKey"] if "queryKey" in query else None
        if not isinstance(keys, list):
            raise ValueError(f"{label}.queryKey must be an array")
        key_text = " ".join(str(key) for key in keys)
        if not any(tag in key_text for tag in ("/api/profile/volume", "user-stats",
                                              "/api/profile/marketsTraded", "positions value",
                                              "/api/profile/userData")):
            continue
        state = _object(query["state"] if "state" in query else None, f"{label}.state")
        data = state["data"] if "data" in state else None
        if "positions value" in key_text:
            if data is not None:
                if type(data) not in (int, float) or not math.isfinite(data):
                    raise ValueError(f"{label}.state.data must be number or null")
                result["positions_value"] = data
            continue
        if data is None:
            continue
        data = _object(data, f"{label}.state.data")
        if "/api/profile/volume" in key_text:
            result["pnl"] = _field(data, "pnl", "number", label, optional=True)
            result["volume"] = _field(data, "amount", "number", label, optional=True)
        elif "user-stats" in key_text:
            result["biggest_win"] = _field(data, "largestWin", "number", label, optional=True)
            result["trades"] = _field(data, "trades", "int", label, optional=True)
            join = _field(data, "joinDate", "str", label, optional=True)
            if join and "1969" not in join:
                result["join_date"] = join[:10]
        elif "/api/profile/marketsTraded" in key_text:
            result["markets"] = _field(data, "traded", "int", label, optional=True)
        elif "/api/profile/userData" in key_text and result["join_date"] is None:
            created = _field(data, "createdAt", "str", label, optional=True)
            if created and "1969" not in created:
                result["join_date"] = created[:10]
    return result


def s3_fill(raw) -> tuple:
    if not isinstance(raw, (tuple, list)) or len(raw) != 8:
        raise ValueError("S3 fill must have eight columns")
    wallet, role, direction, price, usd_amount, token_amount, asset_id, timestamp = raw
    if type(wallet) is not str or not WALLET.fullmatch(wallet):
        raise ValueError("S3 fill wallet must be an address")
    if role not in ("maker", "taker") or direction not in ("BUY", "SELL"):
        raise ValueError("S3 fill role or direction is invalid")
    for label, value in (("price", price), ("usd_amount", usd_amount),
                         ("token_amount", token_amount), ("timestamp", timestamp)):
        if type(value) not in (int, float, Decimal) or not math.isfinite(float(value)):
            raise ValueError(f"S3 fill {label} must be finite numeric")
    if type(asset_id) is not str or not asset_id.isdecimal():
        raise ValueError("S3 fill asset_id must be decimal text")
    return tuple(raw)
