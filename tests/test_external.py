"""Class-wide required-field and source-boundary checks."""

import ast
import copy
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scraper"))

import external

WALLET = "0x" + "a" * 40


class ExternalBoundaryTests(unittest.TestCase):
    def test_every_required_source_field_rejects_absence_or_wrong_type(self):
        board = {"rank": 1, "user_id": WALLET, "pnl": 0, "volume": 0,
                 "user_name": "", "profile_image": "", "x_username": "", "verified": False}
        account = {"wallet": WALLET, "name": "user", "profile_url": "https://polymarket.com/@user"}
        state_account = {"wallet": WALLET, "maker_last_id": "", "taker_last_id": "",
                         "fills": {"total": 0, "maker": 0, "taker": 0}, "day_bounds": {},
                         "positions": {}, "aggregates": {"first_trade": None, "last_trade": None,
                         "total_volume": 0, "total_profit": 0, "total_loss": 0,
                         "wins": 0, "losses": 0, "welford_n": 0, "welford_mean": 0,
                         "welford_m2": 0, "top_markets": {}}}
        cases = [
            ("leaderboard", external.leaderboard_page,
             {"data": [board], "pagination": {"limit": 1, "offset": 0,
              "has_more": False, "next_cursor": None}},
             [("data",), ("pagination",)] + [("data", 0, key) for key in board]
             + [("pagination", key) for key in ("limit", "offset", "has_more")]),
            ("user stats", lambda data: external.user_stats(data, WALLET),
             {"data": {"proxy_wallet": WALLET, "trades": 0, "biggest_win": 0, "views": 0}},
             [("data",)] + [("data", key) for key in ("proxy_wallet", "trades", "biggest_win", "views")]),
            ("Dune execution", external.dune_execution, {"execution_id": "id"}, [("execution_id",)]),
            ("Dune status", external.dune_status, {"state": "QUERY_STATE_COMPLETED"}, [("state",)]),
            ("Dune results", external.dune_results,
             {"result": {"rows": [{"wallet": WALLET, "first_seen": "2026-01-01",
                                   "total_received": 0, "total_sent": 0, "net_capital": 0}]}},
             [("result",), ("result", "rows")]
             + [("result", "rows", 0, key) for key in ("wallet", "first_seen", "total_received", "total_sent", "net_capital")]),
            ("capital map", external.capital_map,
             {WALLET: {"first_seen": "2026-01-01", "total_received": 0,
                       "total_sent": 0, "net_capital": 0}},
             [(WALLET, key) for key in ("first_seen", "total_received", "total_sent", "net_capital")]),
            ("profiles", external.profile_snapshot,
             {"profiles": [{"wallet": WALLET, "trades": 0, "largest_win": 0}]},
             [("profiles",), ("profiles", 0, "wallet")]),
            ("candidates", external.candidates_snapshot,
             {"candidates": [{"wallet": WALLET, "status": "pending", "pnl": 0}]},
             [("candidates",)] + [("candidates", 0, key) for key in ("wallet", "status", "pnl")]),
            ("accounts", external.accounts_snapshot, [account],
             [(0, key) for key in ("wallet", "name", "profile_url")]),
            ("Goldsky fill", external.goldsky_page,
             {"data": {"orderFilledEvents": [{"id": "1", "timestamp": "1", "maker": WALLET,
              "taker": WALLET, "makerAssetId": "0", "takerAssetId": "1",
              "makerAmountFilled": "0", "takerAmountFilled": "0"}]}},
             [("data",), ("data", "orderFilledEvents")]
             + [("data", "orderFilledEvents", 0, key) for key in ("id", "timestamp", "maker", "taker",
               "makerAssetId", "takerAssetId", "makerAmountFilled", "takerAmountFilled")]),
            ("Gamma market", lambda data: external.gamma_market(data, require_end_date=True),
             {"outcomePrices": '["0", "1"]', "clobTokenIds": '["1", "2"]', "endDate": "2026-01-01"},
             [(key,) for key in ("outcomePrices", "clobTokenIds", "endDate")]),
            ("Goldsky state", external.goldsky_state,
             {"accounts": {"user": state_account}, "resolution_cache": {"1": {
              "payout": 0, "cached_at": "2026-01-01", "question": ""}}},
             [("accounts",), ("resolution_cache",)]
             + [("accounts", "user", key) for key in ("wallet", "maker_last_id", "taker_last_id",
               "fills", "day_bounds", "positions", "aggregates")]
             + [("accounts", "user", "fills", key) for key in ("total", "maker", "taker")]
             + [("accounts", "user", "aggregates", key) for key in ("total_volume", "total_profit",
                "total_loss", "wins", "losses", "welford_n", "welford_mean", "welford_m2", "top_markets")]
             + [("resolution_cache", "1", key) for key in ("payout", "cached_at")]),
            ("profile page", external.profile_queries,
             {"props": {"pageProps": {"dehydratedState": {"queries": [
              {"queryKey": ["user-stats"], "state": {"data": {"trades": 0}}}
             ]}}}},
             [("props",), ("props", "pageProps"), ("props", "pageProps", "dehydratedState"),
              ("props", "pageProps", "dehydratedState", "queries"),
              ("props", "pageProps", "dehydratedState", "queries", 0, "queryKey"),
              ("props", "pageProps", "dehydratedState", "queries", 0, "state")]),
            ("S3 fill", external.s3_fill,
             [WALLET, "maker", "BUY", 0, 0, 0, "1", 0],
             [(i,) for i in range(8)]),
        ]
        for source, parse, valid, fields in cases:
            parse(copy.deepcopy(valid))
            for path in fields:
                for mutation in ("drop", "corrupt"):
                    with self.subTest(source=source, field=path, mutation=mutation):
                        data = copy.deepcopy(valid)
                        target = data
                        for part in path[:-1]:
                            target = target[part]
                        key = path[-1]
                        if mutation == "drop":
                            del target[key]
                        else:
                            value = target[key]
                            target[key] = "bad" if type(value) in (int, float, bool) else 7
                        with self.assertRaises(ValueError):
                            parse(data)

    def test_optional_missing_is_none_and_real_zero_is_zero(self):
        stats = external.user_stats({"data": {"proxy_wallet": WALLET, "trades": 0,
                                      "biggest_win": 0, "views": 0}}, WALLET)
        self.assertEqual(stats, {"trades": 0, "largest_win": 0})
        self.assertIsNone(external.user_stats({"data": None}, WALLET))
        old = external.profile_snapshot({"profiles": [{"wallet": WALLET}]})[0]
        self.assertIsNone(old["trades"])
        self.assertIsNone(old["largest_win"])
        market = external.gamma_market({"outcomePrices": '["0"]', "clobTokenIds": '["1"]'})
        self.assertIsNone(market["question"])
        self.assertEqual(market["prices"], ["0"])
        with self.assertRaisesRegex(ValueError, "amount must be number"):
            external.profile_queries({"props": {"pageProps": {"dehydratedState": {"queries": [
                {"queryKey": ["/api/profile/volume"], "state": {"data": {"amount": "0"}}}
            ]}}}})

    def test_no_defaulted_get_outside_validators(self):
        for path in (ROOT / "scraper").glob("*.py"):
            if path.name == "external.py":
                continue
            tree = ast.parse(path.read_text(), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "get":
                    self.assertLessEqual(len(node.args), 1, f"{path}:{node.lineno} uses .get(k, default)")
