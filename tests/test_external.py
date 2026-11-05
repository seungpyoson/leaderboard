"""Class-wide required-field and source-boundary checks."""

import copy
from dataclasses import MISSING, FrozenInstanceError, fields, is_dataclass
import inspect
import json
import sys
import tempfile
from types import SimpleNamespace
import unittest
from pathlib import Path
from typing import Union, get_args, get_origin, get_type_hints

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scraper"))

import external

WALLET = "0x" + "a" * 40


class Response:
    def __init__(self, body):
        self.body = body

    def json(self):
        return self.body


def load_candidate_fixture(body):
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "candidates.json"
        path.write_text(json.dumps(body))
        return external.load_candidates(path)


def contains_record(value, record_type):
    if isinstance(value, record_type):
        return True
    if is_dataclass(value):
        return any(contains_record(getattr(value, field.name), record_type) for field in fields(value))
    if isinstance(value, dict) or hasattr(value, "values"):
        return any(contains_record(item, record_type) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(contains_record(item, record_type) for item in value)
    return False


class ExternalBoundaryTests(unittest.TestCase):
    def test_domain_limits_and_frozen_records(self):
        board = {"rank": 1, "user_id": WALLET, "pnl": 0, "volume": 0,
                 "user_name": "", "profile_image": "", "x_username": "", "verified": False}
        page = {"data": [board], "pagination": {"limit": 1, "offset": 0, "has_more": False}}
        rows, pagination = external.leaderboard_page(page)
        self.assertTrue(is_dataclass(rows[0]) and is_dataclass(pagination))
        with self.assertRaises(FrozenInstanceError):
            rows[0].pnl = 9
        with self.assertRaises(AttributeError):
            rows[0].get("pnl")

        for field, value in (("rank", -1), ("volume", -1)):
            bad = copy.deepcopy(page)
            bad["data"][0][field] = value
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, field):
                external.leaderboard_page(bad)

        stats = {"data": {"proxy_wallet": WALLET, "trades": 0, "views": 0,
                          "biggest_win": 0}}
        bad = copy.deepcopy(stats)
        bad["data"]["proxy_wallet"] = "0x" + "b" * 40
        with self.assertRaisesRegex(ValueError, "does not match request"):
            external.user_stats(bad, WALLET)
        for field in ("trades", "views", "biggest_win"):
            bad = copy.deepcopy(stats)
            bad["data"][field] = -1
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, field):
                external.user_stats(bad, WALLET)

        market = {"outcomePrices": '["0", "1"]', "clobTokenIds": '["1", "2"]'}
        for field, value, message in (
            ("outcomePrices", '["1.01", "0"]', "price must be in"),
            ("outcomePrices", '["0"]', "different lengths"),
        ):
            bad = dict(market, **{field: value})
            with self.subTest(field=field, value=value), self.assertRaisesRegex(ValueError, message):
                external.gamma_market(bad)
        self.assertEqual(external.gamma_markets(None), [])

        base_fill = [WALLET, "maker", "BUY", 0.5, 1, 2, "1", 1]
        for index, value in ((3, -0.1), (3, 1.1), (4, -1), (5, -1), (7, -1)):
            bad = list(base_fill)
            bad[index] = value
            with self.subTest(index=index, value=value), self.assertRaises(ValueError):
                external.s3_fill(bad)

    def test_required_record_fields_refuse_missing_or_corrupt_source(self):
        board = {"rank": 1, "user_id": WALLET, "pnl": 0, "volume": 0,
                 "user_name": "", "profile_image": "", "x_username": "", "verified": False}
        leaderboard = {"data": [board], "pagination": {"limit": 1, "offset": 0,
                       "has_more": False}}
        candidate = {"wallet": WALLET, "status": "pending", "pnl": 0}
        state_account = {"wallet": WALLET, "maker_last_id": "", "taker_last_id": "",
                         "fills": {"total": 0, "maker": 0, "taker": 0}, "day_bounds": {},
                         "positions": {"1": {"net_shares": 1, "cost_basis": 1,
                                             "fill_count": 1}},
                         "aggregates": {"first_trade": None, "last_trade": None,
                         "total_volume": 0, "total_profit": 0, "total_loss": 0,
                         "wins": 0, "losses": 0, "welford_n": 0, "welford_mean": 0,
                         "welford_m2": 0, "top_markets": {}}}
        state = {"accounts": {"user": state_account},
                 "resolution_cache": {"1": {"payout": 0, "cached_at": "2026-01-01"}}}
        fill = {"id": "1", "timestamp": "1", "maker": WALLET, "taker": WALLET,
                "makerAssetId": "0", "takerAssetId": "1",
                "makerAmountFilled": "1", "takerAmountFilled": "1"}
        # Each case supplies a valid source fixture and the location of its record.
        # The required field names themselves come only from dataclasses.fields.
        cases = {
            external.LeaderboardRow: (external.leaderboard_page, leaderboard, ("data", 0), {}),
            external.Pagination: (external.leaderboard_page, leaderboard, ("pagination",), {}),
            external.UserStats: (lambda body: external.user_stats(body, WALLET),
                {"data": {"proxy_wallet": WALLET, "trades": 0, "biggest_win": 0,
                          "views": 0}}, ("data",), {"largest_win": "biggest_win"}),
            external.DuneRow: (external.dune_results,
                {"result": {"rows": [{"wallet": WALLET, "first_seen": "2026-01-01",
                                      "total_received": 0, "total_sent": 0, "net_capital": 0}]}},
                ("result", "rows", 0), {}),
            external.DuneExecution: (lambda body: external.load_dune_execution_response(Response(body)),
                {"execution_id": "id"}, (), {}),
            external.DuneStatus: (lambda body: external.load_dune_status_response(Response(body), "secret"),
                {"state": "QUERY_STATE_COMPLETED"}, (), {}),
            external.DuneError: (lambda body: external.load_dune_error_response(
                SimpleNamespace(**body), "secret"), {"text": "error"}, (),
                {"message": "text"}),
            external.Candidate: (external.candidates_snapshot,
                {"candidates": [candidate]}, ("candidates", 0), {}),
            external.Account: (external.accounts_snapshot,
                [{"wallet": WALLET, "name": "user", "profile_url": "https://polymarket.com/@user"}],
                (0,), {}),
            external.GoldskyFill: (external.goldsky_page,
                {"data": {"orderFilledEvents": [fill]}}, ("data", "orderFilledEvents", 0), {}),
            external.GammaMarket: (external.gamma_market,
                {"outcomePrices": '["0", "1"]', "clobTokenIds": '["1", "2"]'}, (),
                {"prices": "outcomePrices", "clob_ids": "clobTokenIds"}),
            external.ProfileQueries: (external.profile_queries,
                {"props": {"pageProps": {"dehydratedState": {"queries": []}}}}, (), {}),
            external.S3Fill: (external.s3_fill,
                [WALLET, "maker", "BUY", 0.5, 1, 2, "1", 1], (),
                {field.name: index for index, field in enumerate(fields(external.S3Fill))}),
            external.CandidateDocument: (load_candidate_fixture,
                {"candidates": [candidate]}, (), {}),
            external.StateFills: (external.goldsky_state, state,
                ("accounts", "user", "fills"), {}),
            external.StatePosition: (external.goldsky_state, state,
                ("accounts", "user", "positions", "1"), {}),
            external.StateAggregates: (external.goldsky_state, state,
                ("accounts", "user", "aggregates"), {}),
            external.StateAccount: (external.goldsky_state, state,
                ("accounts", "user"), {}),
            external.StateCacheEntry: (external.goldsky_state, state,
                ("resolution_cache", "1"), {}),
            external.GoldskyState: (external.goldsky_state, state, (), {}),
        }
        record_types = {cls for _, cls in inspect.getmembers(external, inspect.isclass)
                        if cls.__module__ == external.__name__ and is_dataclass(cls)}
        self.assertEqual(set(cases), record_types, "every external record needs a source fixture")
        for record_type, (parse, valid, base, aliases) in cases.items():
            with self.subTest(record=record_type.__name__, mutation="valid"):
                self.assertTrue(contains_record(parse(copy.deepcopy(valid)), record_type))
            hints = get_type_hints(record_type)
            required = [field for field in fields(record_type)
                        if type(None) not in get_args(hints[field.name])]
            for field in required:
                self.assertIs(field.default, MISSING,
                              f"required field has a default: {record_type.__name__}.{field.name}")
                self.assertIs(field.default_factory, MISSING,
                              f"required field has a default factory: {record_type.__name__}.{field.name}")
                path = base + (aliases.get(field.name, field.name),)
                target = valid
                for part in path[:-1]:
                    target = target[part]
                key = path[-1]
                self.assertTrue((isinstance(target, list) and isinstance(key, int) and
                                 0 <= key < len(target)) or
                                (isinstance(target, dict) and key in target),
                                f"fixture lacks required source field {record_type.__name__}.{field.name}")
                for mutation in ("drop", "corrupt"):
                    with self.subTest(record=record_type.__name__, field=field.name,
                                      mutation=mutation):
                        body = copy.deepcopy(valid)
                        target = body
                        for part in path[:-1]:
                            target = target[part]
                        if mutation == "drop":
                            del target[key]
                        else:
                            value = target[key]
                            target[key] = 7 if isinstance(value, (str, dict, list)) else "bad"
                        if record_type is external.GammaMarket and mutation == "drop":
                            self.assertIsNone(parse(body))
                        else:
                            with self.assertRaises(ValueError):
                                parse(body)

    def test_every_loader_annotation_contains_only_records(self):
        def record_annotation(annotation):
            if annotation is type(None):
                return True
            origin = get_origin(annotation)
            if origin in (list, tuple, Union):
                return all(record_annotation(arg) for arg in get_args(annotation)
                           if arg is not Ellipsis)
            return isinstance(annotation, type) and is_dataclass(annotation)

        loaders = [(name, fn) for name, fn in inspect.getmembers(external, inspect.isfunction)
                   if name.startswith("load_")]
        self.assertTrue(loaders)
        for name, loader in loaders:
            with self.subTest(loader=name):
                self.assertIn("return", get_type_hints(loader))
                self.assertTrue(record_annotation(get_type_hints(loader)["return"]))

    def test_fill_producer_domain_rejects_zero_amount_and_empty_id(self):
        event = {"id": "event", "timestamp": "1", "maker": WALLET, "taker": WALLET,
                 "makerAssetId": "0", "takerAssetId": "1",
                 "makerAmountFilled": "1", "takerAmountFilled": "1"}
        for key in ("id", "makerAmountFilled", "takerAmountFilled"):
            bad = dict(event, **{key: "" if key == "id" else "0"})
            with self.subTest(source="Goldsky", field=key), self.assertRaisesRegex(ValueError, key):
                external.goldsky_page({"data": {"orderFilledEvents": [bad]}})
        for index in (4, 5):
            row = [WALLET, "maker", "BUY", 0.5, 1, 2, "1", 1]
            row[index] = 0
            with self.subTest(source="S3", index=index), self.assertRaises(ValueError):
                external.s3_fill(row)

    def test_optional_missing_is_none_and_real_zero_is_zero(self):
        stats = external.user_stats({"data": {"proxy_wallet": WALLET, "trades": 0,
                                      "biggest_win": 0, "views": 0}}, WALLET)
        self.assertEqual(stats, external.UserStats(trades=0, largest_win=0))
        self.assertIsNone(external.user_stats({"data": None}, WALLET))
        market = external.gamma_market({"outcomePrices": '["0"]', "clobTokenIds": '["1"]'})
        self.assertIsNone(market.question)
        self.assertEqual(market.prices, (0.0,))
        with self.assertRaisesRegex(ValueError, "amount must be number"):
            external.profile_queries({"props": {"pageProps": {"dehydratedState": {"queries": [
                {"queryKey": ["/api/profile/volume"], "state": {"data": {"amount": "0"}}}
            ]}}}})
