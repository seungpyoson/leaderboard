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
    @staticmethod
    def board_row(wallet, name="one", pnl=10, volume=20, rank=1):
        return {"rank": rank, "user_id": wallet, "pnl": pnl, "volume": volume,
                "user_name": name, "profile_image": "", "x_username": "", "verified": False}

    @staticmethod
    def board_page(rows, has_more=False, next_cursor=None):
        return {"data": rows, "pagination": {"limit": len(rows), "offset": 0,
                "has_more": has_more, "next_cursor": next_cursor}}

    @staticmethod
    def leaderboard_rows():
        return [
            RefreshTests.board_row(f"0x{i:040x}", f"user{i}", 800 - i, rank=i + 1)
            for i in range(800)
        ]

    @staticmethod
    def capital_for(rows, count=800):
        return {row["user_id"].lower(): {"first_seen": "2026-01-01", "total_received": 5000,
                "total_sent": 0, "net_capital": 5000} for row in rows[:count]}

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
            one_row = self.board_row("0x" + "a" * 40)
            page = self.board_page([one_row])
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
            row = self.board_row("0x" + "a" * 40)
            pages = [
                Response(200, self.board_page([row], True, "next")),
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
            bad_row = self.board_row("0x" + "a" * 40)
            bad_row["pnl"] = None
            page = self.board_page([bad_row])
            with patch.object(fetch, "OUTPUT_FILE", output), \
                    patch.object(common.SESSION, "get", return_value=Response(200, page)), \
                    patch.object(sys, "argv", ["fetch.py"]):
                with self.assertRaisesRegex(RuntimeError, "malformed page"):
                    fetch.main()
            self.assertEqual(output.read_text(), "existing data")

    def test_v2_paging_and_fields(self):
        rows = [self.board_row("0x" + "a" * 40), self.board_row("0x" + "b" * 40, "two", 5, 10, 2)]
        pages = [
            Response(200, self.board_page(rows[:1], True, "next")),
            Response(200, self.board_page(rows[1:])),
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
        row = self.board_row("0x" + "a" * 40)
        page = Response(200, self.board_page([row], True, "same"))
        with patch.object(common.SESSION, "get", return_value=page), patch.object(common.time, "sleep"):
            with self.assertRaisesRegex(RuntimeError, "invalid next cursor"):
                common.fetch_leaderboard(3)

    def test_duplicate_wallet_is_rejected(self):
        row = self.board_row("0x" + "a" * 40)
        page = Response(200, self.board_page([row, row]))
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

    def test_missing_discovery_candidates_refuses(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            accounts = root / "accounts.json"
            candidates = root / "candidates.json"
            accounts.write_text("[]")
            with patch.object(discover, "ACCOUNTS_FILE", accounts), \
                    patch.object(discover, "CANDIDATES_FILE", candidates), \
                    patch.object(discover, "LOCK_FILE", root / ".candidates.lock"), \
                    patch.object(discover, "fetch_leaderboard", return_value=[]), \
                    redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(RuntimeError, "Required candidate file missing"):
                    discover.main()
            self.assertFalse(candidates.exists())

    def test_discovery_rechecks_promotion_after_network_fetch(self):
        wallet = "0x" + "a" * 40
        row = {"user_id": wallet, "user_name": "trader", "pnl": 100_000, "rank": 1}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            accounts = root / "accounts.json"
            candidates = root / "candidates.json"
            accounts.write_text("[]")
            candidates.write_text(json.dumps({"candidates": [{"wallet": wallet, "status": "pending", "pnl": 90_000}]}))

            def promote_during_fetch(*_args):
                accounts.write_text(json.dumps([{"wallet": wallet, "name": "trader", "profile_url": "https://polymarket.com/@trader"}]))
                candidates.write_text(json.dumps({"candidates": [{"wallet": wallet, "status": "promoted", "pnl": 90_000}]}))
                return None

            with patch.object(discover, "ACCOUNTS_FILE", accounts), \
                    patch.object(discover, "CANDIDATES_FILE", candidates), \
                    patch.object(discover, "LOCK_FILE", root / ".candidates.lock"), \
                    patch.object(discover, "fetch_leaderboard", return_value=[row]), \
                    patch.object(discover, "fetch_profile", side_effect=promote_during_fetch), \
                    patch.object(discover.time, "sleep"), redirect_stdout(io.StringIO()):
                discover.main()
            self.assertEqual(json.loads(candidates.read_text())["candidates"], [])

    def test_discovery_reloads_decisions_while_lock_held(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            accounts = root / "accounts.json"
            candidates = root / "candidates.json"
            lock = root / ".candidates.lock"
            accounts.write_text("[]")
            candidates.write_text('{"candidates": []}')
            load_candidates = discover.load_existing_candidates
            reads = []

            def check_read():
                reads.append(1)
                if len(reads) == 2:
                    with open(lock, "a") as contender:
                        with self.assertRaises(OSError):
                            discover.fcntl.flock(contender, discover.fcntl.LOCK_EX | discover.fcntl.LOCK_NB)
                return load_candidates()

            with patch.object(discover, "ACCOUNTS_FILE", accounts), \
                    patch.object(discover, "CANDIDATES_FILE", candidates), \
                    patch.object(discover, "LOCK_FILE", lock), \
                    patch.object(discover, "load_existing_candidates", side_effect=check_read), \
                    patch.object(discover, "fetch_leaderboard", return_value=[]), \
                    redirect_stdout(io.StringIO()):
                discover.main()
            self.assertEqual(len(reads), 2)

    def test_discovery_does_not_duplicate_mixed_case_protected_wallet(self):
        wallet = "0x" + "a" * 40
        protected = {"wallet": "0x" + "A" * 40, "status": "rejected", "pnl": 90_000}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            accounts = root / "accounts.json"
            candidates = root / "candidates.json"
            accounts.write_text("[]")
            candidates.write_text(json.dumps({"candidates": [protected]}))
            with patch.object(discover, "ACCOUNTS_FILE", accounts), \
                    patch.object(discover, "CANDIDATES_FILE", candidates), \
                    patch.object(discover, "LOCK_FILE", root / ".candidates.lock"), \
                    patch.object(discover, "fetch_leaderboard", return_value=[{"user_id": wallet, "pnl": 100_000}]), \
                    redirect_stdout(io.StringIO()):
                discover.main()
            self.assertEqual(json.loads(candidates.read_text())["candidates"], [protected])

    def test_user_stats_v2_envelope(self):
        wallet = "0x" + "a" * 40
        body = {"data": {"proxy_wallet": wallet, "trades": 12, "biggest_win": 34.5, "views": 0}}
        with patch.object(common.SESSION, "get", return_value=Response(200, body)) as get:
            stats = fetch.fetch_user_stats(wallet)
        self.assertEqual(stats, {"trades": 12, "largest_win": 34.5})
        self.assertEqual(get.call_args.kwargs["params"], {"user": wallet})

    def test_user_stats_bad_envelope_is_failure(self):
        with patch.object(common.SESSION, "get", return_value=Response(200, {"data": None})):
            self.assertIsNone(fetch.fetch_user_stats("0x" + "a" * 40))

    def test_user_stats_missing_or_null_biggest_win_is_failure(self):
        for payload in ({"trades": 12}, {"trades": 12, "biggest_win": None}):
            with self.subTest(payload=payload), patch.object(common.SESSION, "get", return_value=Response(200, {"data": payload})):
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

    def test_dune_explicit_zero_capital_stays_zero(self):
        wallet = "0x" + "a" * 40
        with patch.object(dune, "_get_api_key", return_value="test-key"), \
                patch.object(dune, "_execute_sql", return_value="test-execution"), \
                patch.object(dune, "_poll_results", return_value={"result": {"rows": [
                    {"wallet": wallet, "first_seen": "2026-01-01", "total_received": 0,
                     "total_sent": 0, "net_capital": 0}
                ]}}), redirect_stdout(io.StringIO()):
            capital = dune.fetch_capital([wallet])
        self.assertEqual(capital[wallet]["net_capital"], 0)

    def test_missing_capital_preserves_profiles_with_counts(self):
        rows = self.leaderboard_rows()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "profiles.json"
            output.write_text("existing data")
            printed = io.StringIO()
            with patch.object(fetch, "OUTPUT_FILE", output), \
                    patch.object(fetch, "fetch_leaderboard", return_value=rows), \
                    patch.object(fetch, "fetch_user_stats", return_value={"trades": 1, "largest_win": 2}), \
                    patch.object(fetch, "fetch_capital", return_value={}), \
                    patch.object(fetch.time, "sleep"), \
                    patch.object(sys, "argv", ["fetch.py"]), \
                    redirect_stdout(printed):
                with self.assertRaisesRegex(RuntimeError, "0/800"):
                    fetch.main()
            self.assertEqual(output.read_text(), "existing data")

    def test_dune_rows_without_net_capital_preserve_profiles(self):
        rows = self.leaderboard_rows()
        dune_rows = [{"wallet": row["user_id"], "first_seen": "2026-01-01",
                      "total_received": 1, "total_sent": 1} for row in rows]
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "profiles.json"
            output.write_text("existing data")
            with patch.object(fetch, "OUTPUT_FILE", output), \
                    patch.object(fetch, "fetch_leaderboard", return_value=rows), \
                    patch.object(fetch, "fetch_user_stats", return_value={"trades": 1, "largest_win": 2}), \
                    patch.object(dune, "_get_api_key", return_value="test-key"), \
                    patch.object(dune, "_execute_sql", return_value="test-execution"), \
                    patch.object(dune, "_poll_results", return_value={"result": {"rows": dune_rows}}), \
                    patch.object(fetch, "atomic_write") as atomic, \
                    patch.object(fetch.time, "sleep"), \
                    patch.object(sys, "argv", ["fetch.py"]), redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(ValueError, "net_capital is required"):
                    fetch.main()
            atomic.assert_not_called()
            self.assertEqual(output.read_text(), "existing data")

    def test_776_of_800_capital_uses_atomic_write(self):
        rows = self.leaderboard_rows()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "profiles.json"
            with patch.object(fetch, "OUTPUT_FILE", output), \
                    patch.object(fetch, "fetch_leaderboard", return_value=rows), \
                    patch.object(fetch, "fetch_user_stats", return_value={"trades": 1, "largest_win": 2}), \
                    patch.object(fetch, "fetch_capital", return_value=self.capital_for(rows, 776)), \
                    patch.object(fetch, "atomic_write", wraps=fetch.atomic_write) as atomic, \
                    patch.object(fetch.time, "sleep"), \
                    patch.object(sys, "argv", ["fetch.py"]), redirect_stdout(io.StringIO()):
                fetch.main()
            atomic.assert_called_once()
            self.assertEqual(json.loads(output.read_text())["accounts"], 800)

    def test_775_of_800_capital_refuses_write(self):
        rows = self.leaderboard_rows()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "profiles.json"
            output.write_text("existing data")
            with patch.object(fetch, "OUTPUT_FILE", output), \
                    patch.object(fetch, "fetch_leaderboard", return_value=rows), \
                    patch.object(fetch, "fetch_user_stats", return_value={"trades": 1, "largest_win": 2}), \
                    patch.object(fetch, "fetch_capital", return_value=self.capital_for(rows, 775)), \
                    patch.object(fetch.time, "sleep"), \
                    patch.object(sys, "argv", ["fetch.py"]), redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(RuntimeError, "775/800"):
                    fetch.main()
            self.assertEqual(output.read_text(), "existing data")

    def test_string_net_capital_refuses_before_write(self):
        rows = self.leaderboard_rows()
        capital = self.capital_for(rows)
        capital[rows[0]["user_id"]]["net_capital"] = "5000"
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "profiles.json"
            output.write_text("existing data")
            with patch.object(fetch, "OUTPUT_FILE", output), \
                    patch.object(fetch, "fetch_leaderboard", return_value=rows), \
                    patch.object(fetch, "fetch_user_stats", return_value={"trades": 1, "largest_win": 2}), \
                    patch.object(fetch, "fetch_capital", return_value=capital), \
                    patch.object(fetch, "atomic_write") as atomic, \
                    patch.object(fetch.time, "sleep"), \
                    patch.object(sys, "argv", ["fetch.py"]), redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(ValueError, "net_capital must be number"):
                    fetch.main()
            atomic.assert_not_called()
            self.assertEqual(output.read_text(), "existing data")

    def test_user_stats_outage_preserves_profiles(self):
        rows = self.leaderboard_rows()
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
                with self.assertRaisesRegex(RuntimeError, "User stats fetch failed"):
                    fetch.main()
            self.assertEqual(output.read_text(), "existing data")

    def test_single_unrestored_stats_failure_preserves_profiles(self):
        rows = self.leaderboard_rows()
        failed = rows[0]["user_id"]
        calls = []

        def stats(wallet):
            calls.append(wallet)
            return None if wallet == failed else {"trades": 1, "largest_win": 2}

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "profiles.json"
            output.write_text('{"profiles": []}')
            with patch.object(fetch, "OUTPUT_FILE", output), \
                    patch.object(fetch, "fetch_leaderboard", return_value=rows), \
                    patch.object(fetch, "fetch_user_stats", side_effect=stats), \
                    patch.object(fetch, "fetch_capital", side_effect=AssertionError("Dune must not run")), \
                    patch.object(fetch.time, "sleep"), \
                    patch.object(sys, "argv", ["fetch.py"]), redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(RuntimeError, "1/800"):
                    fetch.main()
            self.assertEqual(calls.count(failed), 3)
            self.assertEqual(output.read_text(), '{"profiles": []}')

    def test_transient_stats_failure_is_retried(self):
        rows = self.leaderboard_rows()
        failed = rows[0]["user_id"]
        calls = []
        events = []

        def stats(wallet):
            calls.append(wallet)
            events.append(("stats", wallet))
            if wallet == failed and calls.count(wallet) == 1:
                return None
            return {"trades": 1, "largest_win": 2}

        def sleep(delay):
            events.append(("sleep", delay))

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "profiles.json"
            with patch.object(fetch, "OUTPUT_FILE", output), \
                    patch.object(fetch, "fetch_leaderboard", return_value=rows), \
                    patch.object(fetch, "fetch_user_stats", side_effect=stats), \
                    patch.object(fetch, "fetch_capital", return_value=self.capital_for(rows)), \
                    patch.object(fetch.time, "sleep", side_effect=sleep), \
                    patch.object(sys, "argv", ["fetch.py"]), redirect_stdout(io.StringIO()):
                fetch.main()
            self.assertEqual(calls.count(failed), 2)
            self.assertEqual(events[:3], [
                ("stats", failed), ("sleep", fetch.REQUEST_DELAY), ("stats", failed)
            ])
            self.assertEqual(json.loads(output.read_text())["profiles"][0]["largest_win"], 2)

    def test_failed_stats_refuse_even_with_previous_complete_values(self):
        rows = self.leaderboard_rows()
        failed = rows[0]["user_id"]
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "profiles.json"
            previous = json.dumps({"profiles": [{"wallet": rows[0]["user_id"],
                                                   "trades": 91, "largest_win": 1234}]})
            output.write_text(previous)
            printed = io.StringIO()
            with patch.object(fetch, "OUTPUT_FILE", output), \
                    patch.object(fetch, "fetch_leaderboard", return_value=rows), \
                    patch.object(fetch, "fetch_user_stats", side_effect=lambda wallet: None if wallet == failed else {"trades": 1, "largest_win": 2}) as stats, \
                    patch.object(fetch, "fetch_capital", return_value=self.capital_for(rows)), \
                    patch.object(fetch.time, "sleep"), \
                    patch.object(sys, "argv", ["fetch.py"]), redirect_stdout(printed):
                with self.assertRaisesRegex(RuntimeError, "User stats fetch failed"):
                    fetch.main()
            self.assertEqual(sum(call.args[0] == failed for call in stats.call_args_list), 3)
            self.assertEqual(output.read_text(), previous)

    def test_incomplete_previous_stats_also_refuse_write(self):
        rows = self.leaderboard_rows()
        failed = rows[0]["user_id"]
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "profiles.json"
            previous = json.dumps({"profiles": [{"wallet": failed, "trades": 91, "largest_win": None}]})
            output.write_text(previous)
            with patch.object(fetch, "OUTPUT_FILE", output), \
                    patch.object(fetch, "fetch_leaderboard", return_value=rows), \
                    patch.object(fetch, "fetch_user_stats", side_effect=lambda wallet: None if wallet == failed else {"trades": 1, "largest_win": 2}), \
                    patch.object(fetch, "fetch_capital", side_effect=AssertionError("Dune must not run")), \
                    patch.object(fetch.time, "sleep"), \
                    patch.object(sys, "argv", ["fetch.py"]), redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(RuntimeError, "User stats fetch failed"):
                    fetch.main()
            self.assertEqual(output.read_text(), previous)


if __name__ == "__main__":
    unittest.main()
