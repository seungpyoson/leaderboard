"""Validate untrusted records once, before scraper code uses their fields."""

from __future__ import annotations

import math
import json
import re
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Optional
from types import MappingProxyType
from typing import Mapping


WALLET = re.compile(r"0x[0-9a-fA-F]{40}\Z")


@dataclass(frozen=True)
class LeaderboardRow:
    rank: int
    user_id: str
    user_name: str
    profile_image: str
    x_username: str
    pnl: float
    volume: float
    verified: bool


@dataclass(frozen=True)
class Pagination:
    limit: int
    offset: int
    has_more: bool
    next_cursor: Optional[str]


@dataclass(frozen=True)
class UserStats:
    trades: int
    largest_win: float


@dataclass(frozen=True)
class DuneRow:
    wallet: str
    first_seen: str
    total_received: float
    total_sent: float
    net_capital: float


@dataclass(frozen=True)
class Candidate:
    wallet: str
    status: str
    pnl: float
    username: Optional[str]
    profile_url: Optional[str]
    discovery_source: Optional[str]
    style_guess: Optional[str]
    first_seen: Optional[str]
    leaderboard_rank: Optional[int]
    prev_rank: Optional[int]
    trades: Optional[int]
    markets: Optional[int]
    volume: Optional[float]
    efficiency: Optional[float]
    confidence_score: Optional[float]
    is_new: Optional[bool]


@dataclass(frozen=True)
class Account:
    wallet: str
    name: str
    profile_url: str
    style: Optional[str]
    note: Optional[str]


@dataclass(frozen=True)
class GoldskyFill:
    id: str
    timestamp: str
    maker: str
    taker: str
    makerAssetId: str
    takerAssetId: str
    makerAmountFilled: str
    takerAmountFilled: str


@dataclass(frozen=True)
class GammaMarket:
    prices: tuple[float, ...]
    clob_ids: tuple[str, ...]
    question: Optional[str]
    end_date: Optional[str]


@dataclass(frozen=True)
class ProfileQueries:
    pnl: Optional[float]
    volume: Optional[float]
    positions_value: Optional[float]
    biggest_win: Optional[float]
    trades: Optional[int]
    markets: Optional[int]
    join_date: Optional[str]


@dataclass(frozen=True)
class S3Fill:
    wallet: str
    role: str
    direction: str
    price: float
    usd_amount: float
    token_amount: float
    asset_id: str
    timestamp: float


@dataclass(frozen=True)
class CandidateDocument:
    updated: Optional[str]
    candidates: tuple[Candidate, ...]


@dataclass(frozen=True)
class StateFills:
    total: int
    maker: int
    taker: int


@dataclass(frozen=True)
class StatePosition:
    net_shares: float
    cost_basis: float
    fill_count: int


@dataclass(frozen=True)
class StateAggregates:
    first_trade: Optional[str]
    last_trade: Optional[str]
    total_volume: float
    total_profit: float
    total_loss: float
    wins: int
    losses: int
    welford_n: int
    welford_mean: float
    welford_m2: float
    top_markets: Mapping[str, float]


@dataclass(frozen=True)
class StateAccount:
    wallet: str
    maker_last_id: str
    taker_last_id: str
    maker_exhausted: Optional[bool]
    taker_exhausted: Optional[bool]
    maker_at_tip: Optional[bool]
    taker_at_tip: Optional[bool]
    fills: StateFills
    day_bounds: Mapping[str, tuple[int, int]]
    positions: Mapping[str, StatePosition]
    aggregates: StateAggregates


@dataclass(frozen=True)
class StateCacheEntry:
    payout: float
    cached_at: str
    question: Optional[str]


@dataclass(frozen=True)
class GoldskyState:
    accounts: Mapping[str, StateAccount]
    resolution_cache: Mapping[str, StateCacheEntry]


class GoldskyTimeoutError(Exception):
    """The API reported a transient GraphQL timeout."""


def dune_error_text(raw, api_key: str) -> str:
    if type(raw) is not str:
        raise ValueError("Dune error body must be text")
    return raw.replace(api_key, "[REDACTED]")[:2000]


def _finite_number(value) -> bool:
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


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
        "number": _finite_number(value),
    }[kind]
    if not valid:
        raise ValueError(f"{label}.{key} must be {kind}{' or null' if optional else ''}")
    return value


def _wallet(row: dict, key: str, label: str) -> str:
    value = _field(row, key, "str", label)
    if not WALLET.fullmatch(value):
        raise ValueError(f"{label}.{key} must be a wallet address")
    return value


def leaderboard_page(body) -> tuple[list[LeaderboardRow], Pagination]:
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
        _field(row, "pnl", "number", label)
        if _field(row, "volume", "number", label) < 0:
            raise ValueError(f"{label}.volume must be nonnegative")
        _field(row, "verified", "bool", label)
        rank = _field(row, "rank", "int", label)
        if rank < 0:
            raise ValueError(f"{label}.rank must be nonnegative")
        rows.append(LeaderboardRow(
            rank=rank, user_id=row["user_id"], user_name=row["user_name"],
            profile_image=row["profile_image"], x_username=row["x_username"],
            pnl=row["pnl"], volume=row["volume"], verified=row["verified"],
        ))
    pagination = _object(body["pagination"] if "pagination" in body else None, "leaderboard.pagination")
    for key in ("limit", "offset"):
        value = _field(pagination, key, "int", "leaderboard.pagination")
        if value < 0:
            raise ValueError(f"leaderboard.pagination.{key} must be nonnegative")
    _field(pagination, "has_more", "bool", "leaderboard.pagination")
    _field(pagination, "next_cursor", "str", "leaderboard.pagination", optional=True)
    return rows, Pagination(
        limit=pagination["limit"], offset=pagination["offset"],
        has_more=pagination["has_more"], next_cursor=pagination.get("next_cursor"),
    )


def user_stats(body, wallet: str) -> Optional[UserStats]:
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
    if _field(row, "biggest_win", "number", "user stats.data") < 0:
        raise ValueError("user stats.data.biggest_win must be nonnegative")
    join_date = _field(row, "join_date", "int", "user stats.data", optional=True)
    if join_date is not None and join_date < 0:
        raise ValueError("user stats.data.join_date must be nonnegative")
    return UserStats(trades=row["trades"], largest_win=row["biggest_win"])


def dune_results(body) -> list[DuneRow]:
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
            value = _field(row, key, "number", label)
            if key != "net_capital" and value < 0:
                raise ValueError(f"{label}.{key} must be nonnegative")
        parsed.append(DuneRow(**{key: row[key] for key in (
            "wallet", "first_seen", "total_received", "total_sent", "net_capital"
        )}))
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


def candidates_snapshot(body) -> list[Candidate]:
    body = _object(body, "candidates snapshot")
    rows = body["candidates"] if "candidates" in body else None
    if not isinstance(rows, list):
        raise ValueError("candidates snapshot.candidates must be an array")
    parsed = []
    for i, raw in enumerate(rows):
        label = f"candidates snapshot.candidates[{i}]"
        row = dict(_object(raw, label))
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
            value = _field(row, key, "int", label, optional=True)
            if value is not None and value < 0:
                raise ValueError(f"{label}.{key} must be nonnegative")
        for key in ("volume", "efficiency", "confidence_score"):
            value = _field(row, key, "number", label, optional=True)
            if value is not None and key != "efficiency" and value < 0:
                raise ValueError(f"{label}.{key} must be nonnegative")
        _field(row, "is_new", "bool", label, optional=True)
        parsed.append(Candidate(**{key: row.get(key) for key in Candidate.__dataclass_fields__}))
    return parsed


def accounts_snapshot(body) -> list[Account]:
    if not isinstance(body, list):
        raise ValueError("accounts snapshot must be an array")
    parsed = []
    for i, raw in enumerate(body):
        label = f"accounts snapshot[{i}]"
        row = _object(raw, label)
        _wallet(row, "wallet", label)
        for key in ("name", "profile_url"):
            _field(row, key, "str", label)
        for key in ("style", "note"):
            _field(row, key, "str", label, optional=True)
        parsed.append(Account(
            wallet=row["wallet"], name=row["name"], profile_url=row["profile_url"],
            style=row.get("style"), note=row.get("note"),
        ))
    return parsed


def goldsky_page(body) -> list[GoldskyFill]:
    body = _object(body, "Goldsky")
    if "data" not in body and "timeout" in str(body).lower():
        raise GoldskyTimeoutError("Goldsky GraphQL timeout")
    data = _object(body["data"] if "data" in body else None, "Goldsky.data")
    events = data["orderFilledEvents"] if "orderFilledEvents" in data else None
    if not isinstance(events, list):
        raise ValueError("Goldsky.data.orderFilledEvents must be an array")
    parsed = []
    for i, raw in enumerate(events):
        label = f"Goldsky.data.orderFilledEvents[{i}]"
        row = _object(raw, label)
        for key in ("id", "timestamp", "makerAssetId", "takerAssetId",
                    "makerAmountFilled", "takerAmountFilled"):
            value = _field(row, key, "str", label)
            if key != "id" and not value.isdecimal():
                raise ValueError(f"{label}.{key} must be decimal text")
            if key in ("makerAmountFilled", "takerAmountFilled"):
                try:
                    finite = math.isfinite(float(value))
                except (ValueError, OverflowError):
                    finite = False
                if not finite:
                    raise ValueError(f"{label}.{key} must be finite decimal text")
        for key in ("maker", "taker"):
            _wallet(row, key, label)
        parsed.append(GoldskyFill(**{key: row[key] for key in GoldskyFill.__dataclass_fields__}))
    return parsed


def gamma_market(raw, *, require_end_date: bool = False) -> Optional[GammaMarket]:
    row = _object(raw, "Gamma market")
    parsed = {}
    for source, output in (("outcomePrices", "prices"), ("clobTokenIds", "clob_ids")):
        if source not in row:
            return None  # A sparse market cannot resolve any token.
        value = _field(row, source, "str", "Gamma market")
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Gamma market.{source} is invalid JSON") from exc
        if not isinstance(value, list) or not value:
            raise ValueError(f"Gamma market.{source} must be a nonempty array")
        parsed[output] = value
    if len(parsed["prices"]) != len(parsed["clob_ids"]):
        raise ValueError("Gamma market price/token arrays have different lengths")
    prices = []
    for price in parsed["prices"]:
        if type(price) not in (str, int, float):
            raise ValueError("Gamma market price must be numeric")
        try:
            numeric = float(price)
        except (ValueError, OverflowError) as exc:
            raise ValueError("Gamma market price must be numeric") from exc
        if not math.isfinite(numeric):
            raise ValueError("Gamma market price must be finite")
        if numeric < 0 or numeric > 1:
            raise ValueError("Gamma market price must be in [0, 1]")
        prices.append(numeric)
    if not all(type(token) is str and token.isdecimal() for token in parsed["clob_ids"]):
        raise ValueError("Gamma market token ids must be decimal text")
    parsed["question"] = _field(row, "question", "str", "Gamma market") if "question" in row else None
    parsed["end_date"] = _field(row, "endDate", "str", "Gamma market") if "endDate" in row else None
    if require_end_date and parsed["end_date"] is None:
        return None  # The elapsed-market path cannot establish the one-hour buffer.
    return GammaMarket(
        prices=tuple(prices), clob_ids=tuple(parsed["clob_ids"]),
        question=parsed["question"], end_date=parsed["end_date"],
    )


def gamma_markets(body, *, require_end_date: bool = False) -> list[GammaMarket]:
    if body is None:
        return []  # Gamma's documented 200 null body is an empty result.
    if not isinstance(body, list):
        raise ValueError("Gamma markets response must be an array")
    parsed = [gamma_market(row, require_end_date=require_end_date) for row in body]
    return [row for row in parsed if row is not None]


def goldsky_state(body) -> GoldskyState:
    body = _object(body, "Goldsky state")
    accounts = _object(body["accounts"] if "accounts" in body else None, "Goldsky state.accounts")
    cache = _object(body["resolution_cache"] if "resolution_cache" in body else None,
                    "Goldsky state.resolution_cache")
    parsed_accounts = {}
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
            if (type(day) is not str or not isinstance(pair, list) or len(pair) != 2 or
                    any(type(n) is not int or n < 0 for n in pair) or pair[1] < pair[0]):
                raise ValueError(f"{label}.day_bounds entry is malformed")
        positions = _object(account["positions"] if "positions" in account else None,
                            f"{label}.positions")
        for token, raw_pos in positions.items():
            if type(token) is not str:
                raise ValueError(f"{label}.positions token must be text")
            pos = _object(raw_pos, f"{label}.positions[{token}]")
            for key in ("net_shares", "cost_basis"):
                _field(pos, key, "number", f"{label}.positions[{token}]")
            if _field(pos, "fill_count", "int", f"{label}.positions[{token}]") < 0:
                raise ValueError(f"{label}.positions[{token}].fill_count must be nonnegative")
        agg = _object(account["aggregates"] if "aggregates" in account else None,
                      f"{label}.aggregates")
        for key in ("first_trade", "last_trade"):
            _field(agg, key, "str", f"{label}.aggregates", optional=True)
        for key in ("total_volume", "total_profit", "total_loss", "welford_mean", "welford_m2"):
            value = _field(agg, key, "number", f"{label}.aggregates")
            if key != "welford_mean" and value < 0:
                raise ValueError(f"{label}.aggregates.{key} must be nonnegative")
        for key in ("wins", "losses", "welford_n"):
            if _field(agg, key, "int", f"{label}.aggregates") < 0:
                raise ValueError(f"{label}.aggregates.{key} must be nonnegative")
        top = _object(agg["top_markets"] if "top_markets" in agg else None,
                      f"{label}.aggregates.top_markets")
        for question, value in top.items():
            if type(question) is not str or not _finite_number(value) or value < 0:
                raise ValueError(f"{label}.aggregates.top_markets entry is malformed")
        parsed_accounts[name] = StateAccount(
            wallet=account["wallet"], maker_last_id=account["maker_last_id"],
            taker_last_id=account["taker_last_id"],
            maker_exhausted=account.get("maker_exhausted"),
            taker_exhausted=account.get("taker_exhausted"),
            maker_at_tip=account.get("maker_at_tip"), taker_at_tip=account.get("taker_at_tip"),
            fills=StateFills(**{key: fills[key] for key in ("total", "maker", "taker")}),
            day_bounds=MappingProxyType({day: tuple(pair) for day, pair in bounds.items()}),
            positions=MappingProxyType({token: StatePosition(
                net_shares=pos["net_shares"], cost_basis=pos["cost_basis"],
                fill_count=pos["fill_count"],
            ) for token, pos in positions.items()}),
            aggregates=StateAggregates(
                first_trade=agg.get("first_trade"), last_trade=agg.get("last_trade"),
                total_volume=agg["total_volume"], total_profit=agg["total_profit"],
                total_loss=agg["total_loss"], wins=agg["wins"], losses=agg["losses"],
                welford_n=agg["welford_n"], welford_mean=agg["welford_mean"],
                welford_m2=agg["welford_m2"], top_markets=MappingProxyType(dict(top)),
            ),
        )
    parsed_cache = {}
    for token, raw in cache.items():
        if type(token) is not str:
            raise ValueError("Goldsky state cache token must be text")
        label = f"Goldsky state.resolution_cache[{token}]"
        entry = _object(raw, label)
        if _field(entry, "payout", "number", label) not in (0, 1):
            raise ValueError(f"{label}.payout must be 0 or 1")
        _field(entry, "cached_at", "str", label)
        _field(entry, "question", "str", label, optional=True)
        parsed_cache[token] = StateCacheEntry(
            payout=entry["payout"], cached_at=entry["cached_at"],
            question=entry.get("question"),
        )
    return GoldskyState(
        accounts=MappingProxyType(parsed_accounts),
        resolution_cache=MappingProxyType(parsed_cache),
    )


def profile_queries(raw) -> ProfileQueries:
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
                if not _finite_number(data):
                    raise ValueError(f"{label}.state.data must be number or null")
                if data < 0:
                    raise ValueError(f"{label}.state.data must be nonnegative")
                result["positions_value"] = data
            continue
        if data is None:
            continue
        data = _object(data, f"{label}.state.data")
        if "/api/profile/volume" in key_text:
            result["pnl"] = _field(data, "pnl", "number", label, optional=True)
            result["volume"] = _field(data, "amount", "number", label, optional=True)
            if result["volume"] is not None and result["volume"] < 0:
                raise ValueError(f"{label}.amount must be nonnegative")
        elif "user-stats" in key_text:
            result["biggest_win"] = _field(data, "largestWin", "number", label, optional=True)
            result["trades"] = _field(data, "trades", "int", label, optional=True)
            if result["biggest_win"] is not None and result["biggest_win"] < 0:
                raise ValueError(f"{label}.largestWin must be nonnegative")
            if result["trades"] is not None and result["trades"] < 0:
                raise ValueError(f"{label}.trades must be nonnegative")
            join = _field(data, "joinDate", "str", label, optional=True)
            if join and "1969" not in join:
                result["join_date"] = join[:10]
        elif "/api/profile/marketsTraded" in key_text:
            result["markets"] = _field(data, "traded", "int", label, optional=True)
            if result["markets"] is not None and result["markets"] < 0:
                raise ValueError(f"{label}.traded must be nonnegative")
        elif "/api/profile/userData" in key_text and result["join_date"] is None:
            created = _field(data, "createdAt", "str", label, optional=True)
            if created and "1969" not in created:
                result["join_date"] = created[:10]
    return ProfileQueries(**result)


def s3_fill(raw) -> S3Fill:
    if not isinstance(raw, (tuple, list)) or len(raw) != 8:
        raise ValueError("S3 fill must have eight columns")
    wallet, role, direction, price, usd_amount, token_amount, asset_id, timestamp = raw
    if type(wallet) is not str or not WALLET.fullmatch(wallet):
        raise ValueError("S3 fill wallet must be an address")
    if role not in ("maker", "taker") or direction not in ("BUY", "SELL"):
        raise ValueError("S3 fill role or direction is invalid")
    for label, value in (("price", price), ("usd_amount", usd_amount),
                         ("token_amount", token_amount), ("timestamp", timestamp)):
        try:
            finite = type(value) in (int, float, Decimal) and math.isfinite(float(value))
        except (ValueError, OverflowError):
            finite = False
        if not finite:
            raise ValueError(f"S3 fill {label} must be finite numeric")
        if value < 0 or (label == "price" and value > 1):
            raise ValueError(f"S3 fill {label} is outside its domain")
    if type(asset_id) is not str or not asset_id.isdecimal():
        raise ValueError("S3 fill asset_id must be decimal text")
    return S3Fill(
        wallet=wallet, role=role, direction=direction, price=float(price),
        usd_amount=float(usd_amount), token_amount=float(token_amount),
        asset_id=asset_id, timestamp=float(timestamp),
    )


# Raw I/O is confined to these source loaders. Each returns validated records.
def load_leaderboard_response(response) -> tuple[list[LeaderboardRow], Pagination]:
    return leaderboard_page(response.json())


def load_user_stats_response(response, wallet: str) -> Optional[UserStats]:
    return user_stats(response.json(), wallet)


def load_dune_execution_response(response) -> str:
    return dune_execution(response.json())


def load_dune_status_response(response, api_key: str) -> tuple[str, str]:
    body = response.json()
    return dune_status(body), str(body).replace(api_key, "[REDACTED]")


def load_dune_results_response(response) -> list[DuneRow]:
    return dune_results(response.json())


def load_dune_error_response(response, api_key: str) -> str:
    return dune_error_text(response.text, api_key)


def load_goldsky_response(response) -> list[GoldskyFill]:
    return goldsky_page(response.json())


def load_gamma_response(response, *, require_end_date: bool = False) -> list[GammaMarket]:
    return gamma_markets(response.json(), require_end_date=require_end_date)


def profile_html(html: str) -> Optional[ProfileQueries]:
    match = re.search(r'__NEXT_DATA__.*?>(.*?)</script', html, re.DOTALL)
    if not match:
        return None
    return profile_queries(json.loads(match.group(1)))


def load_profile_response(response) -> Optional[ProfileQueries]:
    return profile_html(response.text)


def load_accounts(path: Path) -> list[Account]:
    return accounts_snapshot(json.loads(path.read_text()))


def load_candidates(path: Path) -> CandidateDocument:
    body = _object(json.loads(path.read_text()), "candidates snapshot")
    updated = _field(body, "updated", "str", "candidates snapshot", optional=True)
    return CandidateDocument(updated=updated, candidates=tuple(candidates_snapshot(body)))


def load_goldsky_state(path: Path):
    return goldsky_state(json.loads(path.read_text()))


def load_s3_rows(connection, query: str) -> list[S3Fill]:
    return [s3_fill(row) for row in connection.execute(query).fetchall()]
