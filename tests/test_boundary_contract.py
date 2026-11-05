"""Only external.py may dereference a raw source response or file."""

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "scraper"

# This guards the I/O seam only. Typed records enforce required fields in the rest
# of production; spelling-based guesses about default expressions do not.
RAW_SEAM_TOKENS = (
    r"\.json\s*\(",
    r"\b(?:json\.)?loads?\s*\(",
    r"\.read_text\s*\(",
    r"\.read\s*\(",
    r"\.fetch(?:all|one|df)\s*\(",
    r"\b(?:csv\.)?(?:reader|DictReader)\s*\(",
    r"\.read_csv\s*\(",
    r"\.(?:text|content)\b",
)


class BoundarySourceTests(unittest.TestCase):
    def test_raw_reads_are_confined_to_external(self):
        for path in ROOT.glob("*.py"):
            if path.name == "external.py":
                continue
            source = path.read_text()
            for token in RAW_SEAM_TOKENS:
                match = re.search(token, source)
                self.assertIsNone(match, f"raw source read outside external.py: {path.name}: {token}")
