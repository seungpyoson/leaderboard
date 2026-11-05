import json
import io
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scraper"))

import common
import discover
import dune
import fetch


class Response:
    def __init__(self, status, payload, text=""):
        self.status_code = status
        self.payload = payload
        self.text = text

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise common.requests.HTTPError(f"HTTP {self.status_code}")


class RefreshTests(unittest.TestCase):
    def test_http_error_preserves_profiles_and_exits_nonzero(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "profiles.json"
            output.write_text("existing data")
            with patch.object(fetch, "OUTPUT_FILE", output), \
                    patch.object(common.SESSION, "get", return_value=Response(410, {})), \
                    patch.object(sys, "argv", ["fetch.py"]):
                with self.assertRaises(RuntimeError) as failure:
                    fetch.main()
            self.assertIn("HTTP 410", str(failure.exception))
            self.assertEqual(output.read_text(), "existing data")

    def test_short_fetch_preserves_profiles_and_exits_nonzero(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "profiles.json"
            output.write_text("existing data")
            one_row = {"user_id": "0x" + "a" * 40, "user_name": "one", "pnl": 10, "volume": 20}
            page = {"data": [one_row], "pagination": {"has_more": False, "next_cursor": None}}
            with patch.object(fetch, "OUTPUT_FILE", output), \
                    patch.object(common.SESSION, "get", return_value=Response(200, page)), \
                    patch.object(sys, "argv", ["fetch.py"]):
                with self.assertRaises(SystemExit) as failure:
                    fetch.main()
            self.assertNotEqual(failure.exception.code, 0)
            self.assertEqual(output.read_text(), "existing data")

    def test_second_page_error_preserves_profiles(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "profiles.json"
            output.write_text("existing data")
            row = {"user_id": "0x" + "a" * 40, "user_name": "one", "pnl": 10, "volume": 20}
            pages = [
                Response(200, {"data": [row], "pagination": {"has_more": True, "next_cursor": "next"}}),
                Response(503, {}),
            ]
            with patch.object(fetch, "OUTPUT_FILE", output), \
                    patch.object(common.SESSION, "get", side_effect=pages), \
                    patch.object(common.time, "sleep"), \
                    patch.object(sys, "argv", ["fetch.py"]):
                with self.assertRaisesRegex(RuntimeError, "HTTP 503"):
                    fetch.main()
            self.assertEqual(output.read_text(), "existing data")

    def test_malformed_success_preserves_profiles(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "profiles.json"
            output.write_text("existing data")
            bad_row = {"user_id": "0x" + "a" * 40, "user_name": "one", "pnl": None}
            page = {"data": [bad_row], "pagination": {"has_more": False, "next_cursor": None}}
            with patch.object(fetch, "OUTPUT_FILE", output), \
                    patch.object(common.SESSION, "get", return_value=Response(200, page)), \
                    patch.object(sys, "argv", ["fetch.py"]):
                with self.assertRaisesRegex(RuntimeError, "malformed page"):
                    fetch.main()
            self.assertEqual(output.read_text(), "existing data")

    def test_v2_paging_and_fields(self):
        rows = [
            {"user_id": "0x" + "a" * 40, "user_name": "one", "pnl": 10, "volume": 20},
            {"user_id": "0x" + "b" * 40, "user_name": "two", "pnl": 5, "volume": 10},
        ]
        pages = [
            Response(200, {"data": rows[:1], "pagination": {"has_more": True, "next_cursor": "next"}}),
            Response(200, {"data": rows[1:], "pagination": {"has_more": False, "next_cursor": None}}),
        ]
        with patch.object(common.SESSION, "get", side_effect=pages) as get, \
                patch.object(common.time, "sleep"):
            traders = fetch.fetch_leaderboard(2)
        self.assertEqual(traders, rows)
        self.assertEqual(get.call_args_list[0].kwargs["params"], {
            "category": "crypto", "time_period": "all", "sort_by": "PNL", "limit": 2,
        })
        self.assertEqual(get.call_args_list[1].kwargs["params"], {"cursor": "next"})

    def test_repeated_cursor_is_rejected(self):
        row = {"user_id": "0x" + "a" * 40, "user_name": "one", "pnl": 10}
        page = Response(200, {"data": [row], "pagination": {"has_more": True, "next_cursor": "same"}})
        with patch.object(common.SESSION, "get", return_value=page), patch.object(common.time, "sleep"):
            with self.assertRaisesRegex(RuntimeError, "invalid next cursor"):
                common.fetch_leaderboard(3)

    def test_duplicate_wallet_is_rejected(self):
        row = {"user_id": "0x" + "a" * 40, "user_name": "one", "pnl": 10}
        page = Response(200, {"data": [row, row], "pagination": {"has_more": False, "next_cursor": None}})
        with patch.object(common.SESSION, "get", return_value=page):
            with self.assertRaisesRegex(RuntimeError, "duplicate wallets"):
                common.fetch_leaderboard(2)

    def test_small_limit_cannot_replace_tracked_file(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "profiles.json"
            output.write_text("existing data")
            with patch.object(fetch, "OUTPUT_FILE", output), \
                    patch.object(sys, "argv", ["fetch.py", "--limit", "2"]):
                with self.assertRaises(SystemExit) as failure:
                    fetch.main()
            self.assertNotEqual(failure.exception.code, 0)
            self.assertEqual(output.read_text(), "existing data")

    def test_discovery_http_error_is_fatal(self):
        with patch.object(common.SESSION, "get", return_value=Response(410, {})):
            with self.assertRaises(RuntimeError):
                discover.fetch_leaderboard()

    def test_discovery_short_fetch_is_fatal(self):
        with patch.object(discover, "fetch_crypto_leaderboard", return_value=[{}] * 799):
            with self.assertRaisesRegex(RuntimeError, "Short discovery leaderboard"):
                discover.fetch_leaderboard()

    def test_discovery_main_preserves_decisions_and_known_values(self):
        rows = [
            {"user_id": f"0x{i:040x}", "user_name": f"user{i}", "pnl": 100_000 - i, "rank": i + 1}
            for i in range(800)
        ]
        old = []
        for i, status in enumerate(("auto-rejected", "auto-approved", "approved", "pending")):
            old.append({"wallet": rows[i]["user_id"], "status": status, "pnl": 90_000,
                        "volume": 150_000, "efficiency": 3.5, "trades": 42,
                        "confidence_score": 0.85, "username": f"old{i}"})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            accounts = root / "accounts.json"
            candidates = root / "candidates.json"
            lock = root / ".candidates.lock"
            accounts.write_text("[]")
            candidates.write_text(json.dumps({"candidates": old}))
            with patch.object(discover, "ACCOUNTS_FILE", accounts), \
                    patch.object(discover, "CANDIDATES_FILE", candidates), \
                    patch.object(discover, "LOCK_FILE", lock), \
                    patch.object(discover, "fetch_leaderboard", return_value=rows), \
                    patch.object(discover, "fetch_profile", return_value=None), \
                    patch.object(discover.time, "sleep"), redirect_stdout(io.StringIO()):
                discover.main()
            fresh = {c["wallet"]: c for c in json.loads(candidates.read_text())["candidates"]}
        for before in old[:3]:
            self.assertEqual(fresh[before["wallet"]], before)
        pending = fresh[old[3]["wallet"]]
        self.assertEqual(pending["status"], "pending")
        self.assertEqual(pending["volume"], 150_000)
        self.assertEqual(pending["efficiency"], 3.5)
        self.assertEqual(pending["trades"], 42)
        self.assertEqual(pending["confidence_score"], 0.85)
        self.assertEqual(fresh[rows[4]["user_id"]]["status"], "pending")

    def test_malformed_discovery_inputs_preserve_candidates(self):
        cases = (
            ('{"candidates": [{"status": "approved"}]}', "[]"),
            ('{"candidates": "bad"}', "[]"),
            ('{bad json', "[]"),
            ('{"candidates": []}', '{"bad": true}'),
            ('{"candidates": []}', '[{"name": "no wallet"}]'),
        )
        for candidate_text, account_text in cases:
            with self.subTest(candidate_text=candidate_text, account_text=account_text), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                accounts = root / "accounts.json"
                candidates = root / "candidates.json"
                lock = root / ".candidates.lock"
                accounts.write_text(account_text)
                candidates.write_text(candidate_text)
                before = candidates.read_bytes()
                with patch.object(discover, "ACCOUNTS_FILE", accounts), \
                        patch.object(discover, "CANDIDATES_FILE", candidates), \
                        patch.object(discover, "LOCK_FILE", lock), \
                        patch.object(discover, "fetch_leaderboard") as leaderboard, \
                        redirect_stdout(io.StringIO()):
                    with self.assertRaises((RuntimeError, SystemExit)):
                        discover.main()
                leaderboard.assert_not_called()
                self.assertEqual(candidates.read_bytes(), before)
                self.assertFalse(lock.exists())

    def test_discovery_refusal_is_nonzero_and_preserves_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            accounts = root / "accounts.json"
            candidates = root / "candidates.json"
            accounts.write_text("[]")
            candidates.write_text('{"candidates": []}')
            with patch.object(discover, "ACCOUNTS_FILE", accounts), \
                    patch.object(discover, "CANDIDATES_FILE", candidates), \
                    patch.object(discover, "LOCK_FILE", root / ".candidates.lock"), \
                    patch.object(discover, "fetch_leaderboard", return_value=[]), \
                    patch.object(discover, "validate_output", return_value="refused"), \
                    redirect_stdout(io.StringIO()):
                with self.assertRaises((RuntimeError, SystemExit)):
                    discover.main()
            self.assertEqual(candidates.read_text(), '{"candidates": []}')

    def test_discovery_lock_refusal_is_nonzero(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            accounts = root / "accounts.json"
            candidates = root / "candidates.json"
            accounts.write_text("[]")
            candidates.write_text('{"candidates": []}')
            with patch.object(discover, "ACCOUNTS_FILE", accounts), \
                    patch.object(discover, "CANDIDATES_FILE", candidates), \
                    patch.object(discover, "LOCK_FILE", root / ".candidates.lock"), \
                    patch.object(discover, "fetch_leaderboard", return_value=[]), \
                    patch.object(discover.fcntl, "flock", side_effect=OSError("locked")), \
                    redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(RuntimeError, "lock held"):
                    discover.main()
            self.assertEqual(candidates.read_text(), '{"candidates": []}')

    def test_user_stats_v2_envelope(self):
        wallet = "0x" + "a" * 40
        body = {"data": {"trades": 12, "biggest_win": 34.5}}
        with patch.object(common.SESSION, "get", return_value=Response(200, body)) as get:
            stats = fetch.fetch_user_stats(wallet)
        self.assertEqual(stats, {"trades": 12, "largest_win": 34.5})
        self.assertEqual(get.call_args.kwargs["params"], {"user": wallet})

    def test_user_stats_bad_envelope_is_failure(self):
        with patch.object(common.SESSION, "get", return_value=Response(200, {"data": None})):
            self.assertIsNone(fetch.fetch_user_stats("0x" + "a" * 40))

    def test_dune_error_body_redacts_key(self):
        response = Response(400, {}, "invalid query; key=private-test-key")
        output = io.StringIO()
        with patch.object(dune, "_get_api_key", return_value="private-test-key"), \
                patch.object(dune.requests, "post", return_value=response), \
                redirect_stdout(output):
            self.assertEqual(dune.fetch_capital(["0x" + "a" * 40]), {})
        self.assertIn("Dune HTTP 400", output.getvalue())
        self.assertIn("[REDACTED]", output.getvalue())
        self.assertNotIn("private-test-key", output.getvalue())

    def test_v2_profiles_warn_when_capital_missing(self):
        rows = [
            {"user_id": f"0x{i:040x}", "user_name": f"user{i}", "pnl": 800 - i, "volume": 20}
            for i in range(800)
        ]
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "profiles.json"
            printed = io.StringIO()
            with patch.object(fetch, "OUTPUT_FILE", output), \
                    patch.object(fetch, "fetch_leaderboard", return_value=rows), \
                    patch.object(fetch, "fetch_user_stats", return_value={"trades": 1, "largest_win": 2}), \
                    patch.object(fetch, "fetch_capital", return_value={}), \
                    patch.object(fetch.time, "sleep"), \
                    patch.object(sys, "argv", ["fetch.py"]), \
                    redirect_stdout(printed):
                fetch.main()
            data = json.loads(output.read_text())
            self.assertEqual(data["accounts"], 800)
            self.assertEqual(data["profiles"][0]["wallet"], rows[0]["user_id"])
            self.assertIsNone(data["profiles"][0]["volume"])
            self.assertIsNone(data["profiles"][0]["efficiency"])
            self.assertIn("::warning title=Capital enrichment missing::800/800", printed.getvalue())

    def test_user_stats_outage_preserves_profiles(self):
        rows = [
            {"user_id": f"0x{i:040x}", "user_name": f"user{i}", "pnl": 800 - i}
            for i in range(800)
        ]
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "profiles.json"
            output.write_text("existing data")
            printed = io.StringIO()
            with patch.object(fetch, "OUTPUT_FILE", output), \
                    patch.object(fetch, "fetch_leaderboard", return_value=rows), \
                    patch.object(fetch, "fetch_user_stats", return_value=None), \
                    patch.object(fetch, "fetch_capital", side_effect=AssertionError("Dune must not run")), \
                    patch.object(fetch.time, "sleep"), \
                    patch.object(sys, "argv", ["fetch.py"]), redirect_stdout(printed):
                with self.assertRaises((RuntimeError, SystemExit)):
                    fetch.main()
            self.assertEqual(output.read_text(), "existing data")
            self.assertIn("::warning title=User stats", printed.getvalue())

    def test_user_stats_failure_fraction_boundary(self):
        rows = [
            {"user_id": f"0x{i:040x}", "user_name": f"user{i}", "pnl": 800 - i}
            for i in range(800)
        ]
        for failures in (40, 41):
            with self.subTest(failures=failures), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "profiles.json"
                output.write_text('{"profiles": []}')
                stats = [None] * failures + [{"trades": 1, "largest_win": 2}] * (800 - failures)
                with patch.object(fetch, "OUTPUT_FILE", output), \
                        patch.object(fetch, "fetch_leaderboard", return_value=rows), \
                        patch.object(fetch, "fetch_user_stats", side_effect=stats), \
                        patch.object(fetch, "fetch_capital", return_value={}), \
                        patch.object(fetch.time, "sleep"), \
                        patch.object(sys, "argv", ["fetch.py"]), redirect_stdout(io.StringIO()):
                    if failures == 41:
                        with self.assertRaisesRegex(RuntimeError, "maximum 5%"):
                            fetch.main()
                        self.assertEqual(output.read_text(), '{"profiles": []}')
                    else:
                        fetch.main()
                        self.assertEqual(json.loads(output.read_text())["accounts"], 800)

    def test_one_user_stats_failure_warns(self):
        rows = [
            {"user_id": f"0x{i:040x}", "user_name": f"user{i}", "pnl": 800 - i}
            for i in range(800)
        ]
        stats = [None] + [{"trades": 1, "largest_win": 2}] * 799
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "profiles.json"
            output.write_text(json.dumps({"profiles": [{"wallet": rows[0]["user_id"],
                                                        "trades": 91, "largest_win": 1234}]}))
            printed = io.StringIO()
            with patch.object(fetch, "OUTPUT_FILE", output), \
                    patch.object(fetch, "fetch_leaderboard", return_value=rows), \
                    patch.object(fetch, "fetch_user_stats", side_effect=stats), \
                    patch.object(fetch, "fetch_capital", return_value={}), \
                    patch.object(fetch.time, "sleep"), \
                    patch.object(sys, "argv", ["fetch.py"]), redirect_stdout(printed):
                fetch.main()
            fresh = json.loads(output.read_text())
            self.assertEqual(fresh["accounts"], 800)
            self.assertEqual(fresh["profiles"][0]["trades"], 91)
            self.assertEqual(fresh["profiles"][0]["largest_win"], 1234)
            self.assertIn("::warning title=User stats fetch failure::1/800", printed.getvalue())


if __name__ == "__main__":
    unittest.main()
