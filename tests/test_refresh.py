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
import fetch


class Response:
    def __init__(self, status, payload):
        self.status_code = status
        self.payload = payload

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
        self.assertEqual(get.call_args_list[0].kwargs["params"]["time_period"], "all")
        self.assertEqual(get.call_args_list[1].kwargs["params"], {"cursor": "next"})

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

    def test_discovery_does_not_auto_reject_without_efficiency(self):
        self.assertEqual(discover.auto_status(0.0, "pending", False), "pending")
        self.assertEqual(discover.auto_status(0.0, "approved", False), "approved")

    def test_user_stats_v2_envelope(self):
        wallet = "0x" + "a" * 40
        body = {"data": {"trades": 12, "biggest_win": 34.5}}
        with patch.object(common.SESSION, "get", return_value=Response(200, body)) as get:
            stats = fetch.fetch_user_stats(wallet)
        self.assertEqual(stats, {"trades": 12, "largest_win": 34.5})
        self.assertEqual(get.call_args.kwargs["params"], {"user": wallet})

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


if __name__ == "__main__":
    unittest.main()
