"""Corrupt data at production network and tracked-file seams."""

import io
import json
import sys
import tempfile
import unittest
from dataclasses import asdict
from decimal import Decimal
from datetime import datetime, timezone
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scraper"))

import backfill_s3
import common
import discover
import fetch
import goldsky_parallel
import goldsky_pipeline as gp
import promote
import external


WALLET = "0x" + "a" * 40


class Response:
    status_code = 200
    text = ""

    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload

    def raise_for_status(self):
        pass


def state():
    account = asdict(gp.new_account_state(WALLET))
    account["positions"]["1"] = {"net_shares": 1, "cost_basis": 0.5, "fill_count": 1}
    account["aggregates"]["top_markets"]["market"] = 0.5
    return {"accounts": {"user": account}, "resolution_cache": {"1": {
        "payout": 1, "cached_at": "2026-01-01", "question": "market"}}}


def runtime_state():
    return gp.runtime_state(external.goldsky_state(state()))


class ProductionBoundaryTests(unittest.TestCase):
    def test_validated_fills_drive_position_updates(self):
        event = {"id": "1", "timestamp": "1", "maker": WALLET, "taker": WALLET,
                 "makerAssetId": "0", "takerAssetId": "1",
                 "makerAmountFilled": "1000000", "takerAmountFilled": "2000000"}
        fill = external.goldsky_page({"data": {"orderFilledEvents": [event]}})[0]
        positions = {}
        gp.update_position(positions, gp.process_fill(fill, WALLET))
        self.assertEqual((positions["1"].net_shares, positions["1"].cost_basis), (2, 1))

        source = external.s3_fill((WALLET, "maker", "BUY", Decimal("0.5"),
                                   Decimal("1"), Decimal("2"), "1", 1))
        runtime = gp.RuntimeState()
        account = external.Account(wallet=WALLET, name="user", profile_url="https://example.com",
                                   style=None, note=None)
        with redirect_stdout(io.StringIO()):
            backfill_s3.build_positions([source], [account], runtime)
        self.assertEqual(runtime.accounts["user"].positions["1"].net_shares, 2)

    def test_serial_late_input_failure_never_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            accounts = Path(directory) / "accounts.json"
            accounts.write_text(json.dumps([
                {"wallet": WALLET, "name": "first", "profile_url": "https://example.com/first"},
                {"wallet": "0x" + "b" * 40, "name": "second",
                 "profile_url": "https://example.com/second"},
            ]))
            with patch.object(gp, "ACCOUNTS_FILE", accounts), \
                    patch.object(gp, "load_state", return_value=gp.RuntimeState()), \
                    patch.object(gp, "process_account", side_effect=[None, ValueError("late input")]) as process, \
                    patch.object(gp, "save_state") as save, \
                    patch.object(gp, "write_csv") as csv, redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(ValueError, "late input"):
                    gp.run_pipeline()
            self.assertEqual(process.call_count, 2)
            save.assert_not_called()
            csv.assert_not_called()

    def test_state_snapshot_is_frozen_and_runtime_roundtrips(self):
        raw = state()
        raw["accounts"]["user"]["unused_source_field"] = 12
        snapshot = external.goldsky_state(raw)
        with self.assertRaises(AttributeError):
            snapshot.accounts["user"].get("wallet")
        with self.assertRaises(TypeError):
            snapshot.accounts["user"].positions["2"] = external.StatePosition(0, 0, 0)
        runtime = gp.runtime_state(snapshot)
        gp.update_position(runtime.accounts["user"].positions, {
            "token_id": "2", "direction": "BUY", "shares": 2,
            "usd_amount": 1, "timestamp": 1,
        })
        runtime.resolution_cache["1"] = external.StateCacheEntry(
            payout=1, cached_at=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            question="market",
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            with patch.object(gp, "STATE_FILE", path):
                gp.save_state(runtime)
                loaded = gp.load_state()
            self.assertEqual(loaded.accounts["user"].positions["2"].net_shares, 2)
            self.assertNotIn("unused_source_field", json.loads(path.read_text())["accounts"]["user"])

    def test_backfill_main_rejects_gamma_before_any_write(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            accounts = root / "accounts.json"
            tracked_state = root / "goldsky_state.json"
            accounts.write_text(json.dumps([{"wallet": WALLET, "name": "user",
                                             "profile_url": "https://polymarket.com/@user"}]))
            tracked_state.write_text("tracked state must stay untouched")
            malformed = {"outcomePrices": '["1.01"]', "clobTokenIds": '["1"]'}
            input_state = runtime_state()
            input_state.resolution_cache.clear()
            with patch.object(backfill_s3, "ACCOUNTS_FILE", accounts), \
                    patch.object(backfill_s3, "STATE_FILE", tracked_state), \
                    patch.object(backfill_s3, "scan_s3_fills", return_value=[]), \
                    patch.object(backfill_s3, "load_state", return_value=input_state), \
                    patch.object(backfill_s3, "build_positions"), \
                    patch.object(gp.SESSION, "get", return_value=Response([malformed])), \
                    patch.object(backfill_s3.SESSION, "get", return_value=Response([malformed])), \
                    patch.object(backfill_s3, "save_state") as save, \
                    patch.object(backfill_s3, "write_csv") as csv, \
                    patch.object(sys, "argv", ["backfill_s3.py"]), redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(ValueError, "price must be in"):
                    backfill_s3.main()
            save.assert_not_called()
            csv.assert_not_called()
            self.assertEqual(tracked_state.read_text(), "tracked state must stay untouched")

    def test_backfill_main_rejects_elapsed_gamma_before_any_write(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            accounts = root / "accounts.json"
            accounts.write_text(json.dumps([{"wallet": WALLET, "name": "user",
                                             "profile_url": "https://polymarket.com/@user"}]))
            input_state = runtime_state()
            input_state.resolution_cache.clear()
            malformed = {"outcomePrices": '["1.01"]', "clobTokenIds": '["1"]',
                         "endDate": "2026-01-01T00:00:00Z"}
            with patch.object(backfill_s3, "ACCOUNTS_FILE", accounts), \
                    patch.object(backfill_s3, "scan_s3_fills", return_value=[]), \
                    patch.object(backfill_s3, "load_state", return_value=input_state), \
                    patch.object(backfill_s3, "build_positions"), \
                    patch.object(backfill_s3, "resolve_all"), \
                    patch.object(backfill_s3.SESSION, "get", return_value=Response([malformed])), \
                    patch.object(backfill_s3, "save_state") as save, \
                    patch.object(backfill_s3, "write_csv") as csv, \
                    patch.object(sys, "argv", ["backfill_s3.py"]), redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(ValueError, "price must be in"):
                    backfill_s3.main()
            save.assert_not_called()
            csv.assert_not_called()

    def test_network_seams_refuse_inner_corruption(self):
        board = {"data": [{"rank": 1, "user_id": WALLET, "pnl": float("nan"),
                            "volume": 0, "user_name": "user", "profile_image": "",
                            "x_username": "", "verified": False}],
                 "pagination": {"limit": 1, "offset": 0, "has_more": False}}
        with patch.object(common.SESSION, "get", return_value=Response(board)):
            with self.assertRaisesRegex(RuntimeError, "pnl"):
                common.fetch_leaderboard(1)

        html = '<script id="__NEXT_DATA__" type="application/json">' + json.dumps({
            "props": {"pageProps": {"dehydratedState": {"queries": [{
                "queryKey": ["/api/profile/volume"],
                "state": {"data": {"amount": "5"}},
            }]}}}}) + '</script>'
        with self.assertRaisesRegex(ValueError, "amount"):
            common.extract_profile_data(html)
        response = Response({})
        response.text = html
        with patch.object(common.SESSION, "get", return_value=response):
            with self.assertRaisesRegex(ValueError, "amount"):
                common.fetch_profile("user", "https://polymarket.com/@user")

        stats = {"data": {"proxy_wallet": WALLET, "trades": "1", "views": 0,
                          "biggest_win": 1}}
        with patch.object(common.SESSION, "get", return_value=Response(stats)):
            with self.assertRaisesRegex(ValueError, "trades"):
                fetch.fetch_user_stats(WALLET)

        with patch.object(common.SESSION, "get", return_value=Response({
            "data": [{"type": "DEPOSIT", "usdc_size": float("inf"), "timestamp": 1}],
            "pagination": {"has_more": False, "next_cursor": None},
        })):
            with self.assertRaisesRegex(ValueError, "usdc_size"):
                fetch.fetch_activity(WALLET)

        event = {"id": "1", "timestamp": "1", "maker": WALLET, "taker": WALLET,
                 "makerAssetId": "1", "takerAssetId": "2",
                 "makerAmountFilled": "bad", "takerAmountFilled": "1"}
        with patch.object(gp.SESSION, "post", return_value=Response({"data": {"orderFilledEvents": [event]}})):
            with self.assertRaisesRegex(ValueError, "makerAmountFilled"):
                gp.query_goldsky("query")

        market = {"outcomePrices": '["NaN"]', "clobTokenIds": '["1"]'}
        with patch.object(gp.SESSION, "get", return_value=Response([market])):
            with self.assertRaisesRegex(ValueError, "finite"):
                gp.resolve_markets(["1"], {})

        elapsed_state = gp.RuntimeState(accounts={"user": gp.AccountState(
            wallet=WALLET, positions={"1": gp.Position()})})
        with patch.object(backfill_s3.SESSION, "get", return_value=Response([market])), \
                patch.object(backfill_s3, "save_state") as save, redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(ValueError, "finite"):
                backfill_s3.resolve_elapsed_markets(elapsed_state)
        save.assert_not_called()

    def test_s3_row_refuses_at_scan(self):
        connection = MagicMock()
        connection.execute.return_value.fetchall.return_value = [
            (WALLET, "maker", "BUY", float("inf"), 1, 1, "1", 1)]
        with patch.object(backfill_s3.duckdb, "connect", return_value=connection):
            with self.assertRaisesRegex(ValueError, "price"):
                backfill_s3.scan_s3_fills([WALLET])

    def test_discovery_profile_validation_refuses_before_candidate_write(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            accounts = root / "accounts.json"
            candidates = root / "candidates.json"
            accounts.write_text("[]")
            candidates.write_text('{"candidates": []}')
            response = Response({})
            response.text = ('<script id="__NEXT_DATA__">' + json.dumps({
                "props": {"pageProps": {"dehydratedState": {"queries": [{
                    "queryKey": ["/api/profile/volume"],
                    "state": {"data": {"amount": "5"}},
                }]}}}}) + '</script>')
            board = {"user_id": WALLET, "pnl": 100000, "user_name": "user"}
            with patch.object(discover, "ACCOUNTS_FILE", accounts), \
                    patch.object(discover, "CANDIDATES_FILE", candidates), \
                    patch.object(discover, "LOCK_FILE", root / ".lock"), \
                    patch.object(discover, "fetch_leaderboard", return_value=[external.LeaderboardRow(
                        rank=1, user_id=WALLET, user_name="user", profile_image="",
                        x_username="", pnl=100000, volume=0, verified=False)]), \
                    patch.object(common.SESSION, "get", return_value=response), \
                    redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(ValueError, "amount"):
                    discover.main()
            self.assertEqual(candidates.read_text(), '{"candidates": []}')

    def test_tracked_files_refuse_before_write(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            accounts = root / "accounts.json"
            candidates = root / "candidates.json"
            state_file = root / "goldsky_state.json"
            missing_paths = (
                (discover, "ACCOUNTS_FILE", accounts, discover.load_known_wallets),
                (discover, "CANDIDATES_FILE", candidates, discover.load_existing_candidates),
                (gp, "STATE_FILE", state_file, gp.load_state),
            )
            for module, name, path, load in missing_paths:
                with self.subTest(path=path), patch.object(module, name, path):
                    with self.assertRaisesRegex(RuntimeError, path.name):
                        load()

            for module, path_name, run in (
                    (gp, "ACCOUNTS_FILE", gp.run_pipeline),
                    (goldsky_parallel.gp, "ACCOUNTS_FILE", goldsky_parallel.main),
                    (backfill_s3, "ACCOUNTS_FILE", backfill_s3.main)):
                with self.subTest(entry=run.__module__), patch.object(module, path_name, accounts), \
                        patch.object(sys, "argv", ["scraper.py"]), redirect_stdout(io.StringIO()):
                    with self.assertRaisesRegex(FileNotFoundError, accounts.name):
                        run()
            message = io.StringIO()
            with patch.object(promote, "CANDIDATES_FILE", candidates), \
                    patch.object(promote, "atomic_write") as write, redirect_stdout(message):
                with self.assertRaises(SystemExit):
                    promote._run_promotion()
            self.assertIn(candidates.name, message.getvalue())
            write.assert_not_called()

            accounts.write_text("[]")
            candidates.write_text(json.dumps({"candidates": [{"wallet": WALLET,
                                                "status": "auto-approved", "pnl": 1,
                                                "username": "user", "profile_url": "https://example.com"}]}))
            with patch.object(promote, "ACCOUNTS_FILE", root / "absent.json"), \
                    patch.object(promote, "CANDIDATES_FILE", candidates), \
                    patch.object(promote, "atomic_write") as write:
                with self.assertRaisesRegex(RuntimeError, "absent.json"):
                    promote._run_promotion()
            write.assert_not_called()

            corrupt = state()
            corrupt["resolution_cache"]["1"]["payout"] = 0.5
            bad_position = state()
            bad_position["accounts"]["user"]["positions"]["1"]["net_shares"] = "1"
            bad_top = state()
            bad_top["accounts"]["user"]["aggregates"]["top_markets"]["market"] = float("inf")
            bad_volume = state()
            bad_volume["accounts"]["user"]["aggregates"]["total_volume"] = -1
            mutations = [(corrupt, "payout"), (bad_position, "net_shares"),
                         (bad_top, "top_markets"), (bad_volume, "total_volume")]
            for body, field in mutations:
                state_file.write_text(json.dumps(body))
                with self.subTest(state_field=field), patch.object(gp, "STATE_FILE", state_file):
                    with self.assertRaisesRegex(ValueError, field):
                        gp.load_state()

            accounts.write_text(json.dumps([{"wallet": WALLET, "name": "user",
                                             "profile_url": 7}]))
            with patch.object(discover, "ACCOUNTS_FILE", accounts):
                with self.assertRaisesRegex(RuntimeError, "profile_url"):
                    discover.load_known_wallets()
            with patch.object(gp, "ACCOUNTS_FILE", accounts), \
                    patch.object(gp, "STATE_FILE", state_file), \
                    patch.object(gp, "save_state") as save, \
                    patch.object(gp, "write_csv") as csv:
                with self.assertRaisesRegex(ValueError, "profile_url"):
                    gp.run_pipeline()
            save.assert_not_called()
            csv.assert_not_called()

            with patch.object(gp, "ACCOUNTS_FILE", accounts), \
                    patch.object(goldsky_parallel.sys, "argv", ["goldsky_parallel.py"]), \
                    patch.object(gp, "save_state") as save, \
                    patch.object(gp, "write_csv") as csv:
                with self.assertRaisesRegex(ValueError, "profile_url"):
                    goldsky_parallel.main()
            save.assert_not_called()
            csv.assert_not_called()

            with patch.object(backfill_s3, "ACCOUNTS_FILE", accounts), \
                    patch.object(sys, "argv", ["backfill_s3.py"]), \
                    patch.object(backfill_s3, "save_state") as save:
                with self.assertRaisesRegex(ValueError, "profile_url"):
                    backfill_s3.main()
            save.assert_not_called()

            candidates.write_text(json.dumps({"candidates": [{"wallet": WALLET,
                                                "status": "approved", "pnl": float("nan") }]}))
            with patch.object(discover, "CANDIDATES_FILE", candidates):
                with self.assertRaisesRegex(RuntimeError, "pnl"):
                    discover.load_existing_candidates()
            with patch.object(promote, "CANDIDATES_FILE", candidates), \
                    patch.object(promote, "ACCOUNTS_FILE", accounts), \
                    patch.object(promote, "atomic_write") as write:
                with self.assertRaisesRegex(RuntimeError, "pnl"):
                    promote._run_promotion()
            write.assert_not_called()

            candidates.write_text(json.dumps({"candidates": [{"wallet": WALLET,
                "status": "approved", "pnl": 1, "username": "user",
                "profile_url": "https://example.com"}]}))
            accounts.write_text(json.dumps([{"wallet": WALLET, "name": "user",
                                             "profile_url": 7}]))
            with patch.object(promote, "CANDIDATES_FILE", candidates), \
                    patch.object(promote, "ACCOUNTS_FILE", accounts), \
                    patch.object(promote, "atomic_write") as write:
                with self.assertRaisesRegex(RuntimeError, "profile_url"):
                    promote._run_promotion()
            write.assert_not_called()

    def test_sparse_gamma_rows_skip_only_unusable_markets(self):
        sparse = {"clobTokenIds": '["1"]'}
        valid = {"outcomePrices": '["1"]', "clobTokenIds": '["1"]'}
        with patch.object(gp.SESSION, "get", return_value=Response([sparse, valid])):
            cache = gp.resolve_markets(["1"], {})
        self.assertEqual(cache["1"].payout, 1)
        with patch.object(gp.SESSION, "get", return_value=Response([{
                "outcomePrices": '["1"]', "clobTokenIds": '["1"]', "endDate": 7}])):
            with self.assertRaisesRegex(ValueError, "endDate"):
                gp.resolve_markets(["1"], {})
        with patch.object(gp.SESSION, "get", return_value=Response([{
                "outcomePrices": '["1"]', "clobTokenIds": None}])):
            with self.assertRaisesRegex(ValueError, "clobTokenIds"):
                gp.resolve_markets(["1"], {})

    def test_promotion_note_omits_missing_optional_parts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            accounts = root / "accounts.json"
            candidates = root / "candidates.json"
            accounts.write_text("[]")
            candidates.write_text(json.dumps({"candidates": [{"wallet": WALLET,
                "status": "approved", "pnl": 1, "username": "user",
                "profile_url": "https://example.com"}]}))
            with patch.object(promote, "ACCOUNTS_FILE", accounts), \
                    patch.object(promote, "CANDIDATES_FILE", candidates), \
                    patch.object(promote, "atomic_write") as write, redirect_stdout(io.StringIO()):
                promote._run_promotion()
            note = json.loads(write.call_args_list[0].args[1])[0]["note"]
            self.assertEqual(note, "Auto-discovered.")

    def test_parallel_worker_failure_never_saves(self):
        with tempfile.TemporaryDirectory() as directory:
            accounts = Path(directory) / "accounts.json"
            accounts.write_text(json.dumps([{"wallet": WALLET, "name": "user",
                                             "profile_url": "https://example.com"}]))
            event = {"id": "1", "timestamp": "1", "maker": WALLET, "taker": WALLET,
                     "makerAssetId": "1", "takerAssetId": "2",
                     "makerAmountFilled": "bad", "takerAmountFilled": "1"}
            with patch.object(gp, "ACCOUNTS_FILE", accounts), \
                    patch.object(gp, "load_state", return_value=gp.RuntimeState()), \
                    patch.object(gp.SESSION, "post", return_value=Response({"data": {"orderFilledEvents": [event]}})), \
                    patch.object(goldsky_parallel.sys, "argv", ["goldsky_parallel.py", "--workers", "1"]), \
                    patch.object(gp, "save_state") as save, \
                    patch.object(gp, "write_csv") as csv, redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(RuntimeError, "user"):
                    goldsky_parallel.main()
            save.assert_not_called()
            csv.assert_not_called()

    def test_serial_goldsky_failure_never_saves(self):
        with tempfile.TemporaryDirectory() as directory:
            accounts = Path(directory) / "accounts.json"
            accounts.write_text(json.dumps([{"wallet": WALLET, "name": "user",
                                             "profile_url": "https://example.com"}]))
            event = {"id": "1", "timestamp": "1", "maker": WALLET, "taker": WALLET,
                     "makerAssetId": "1", "takerAssetId": "2",
                     "makerAmountFilled": "NaN", "takerAmountFilled": "1"}
            with patch.object(gp, "ACCOUNTS_FILE", accounts), \
                    patch.object(gp, "load_state", return_value=runtime_state()), \
                    patch.object(gp.SESSION, "post", return_value=Response({"data": {"orderFilledEvents": [event]}})), \
                    patch.object(gp, "save_state") as save, \
                    patch.object(gp, "write_csv") as csv, redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(ValueError, "makerAmountFilled"):
                    gp.run_pipeline()
            save.assert_not_called()
            csv.assert_not_called()
