"""Production external-read inventory and syntax guard.

Each entry names the read expression and its validator in the same production
function. Adding a read requires adding its boundary here and exercising it.
"""

import ast
import copy
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1] / "scraper"

# module, function, read expression, validator, number of occurrences
READ_SITES = (
    ("common.py", "fetch_leaderboard", "response.json()", "leaderboard_page", 1),
    ("common.py", "extract_profile_data", "json.loads(match.group(1))", "profile_queries", 1),
    ("common.py", "fetch_profile", "resp.text", "extract_profile_data", 1),
    ("fetch.py", "fetch_user_stats", "resp.json()", "user_stats", 1),
    ("dune.py", "_execute_sql", "resp.json()", "dune_execution", 1),
    ("dune.py", "_check_response", "resp.text", "dune_error_text", 1),
    ("dune.py", "_poll_results", "resp.json()", "dune_status", 1),
    ("dune.py", "_poll_results", "results.json()", "dune_results", 1),
    ("goldsky_pipeline.py", "query_goldsky", "r.json()", "goldsky_page", 1),
    ("goldsky_pipeline.py", "resolve_markets", "resp.json()", "gamma_markets", 1),
    ("goldsky_pipeline.py", "load_state", "json.loads(STATE_FILE.read_text())", "goldsky_state", 1),
    ("goldsky_pipeline.py", "run_pipeline", "json.loads(ACCOUNTS_FILE.read_text())", "accounts_snapshot", 1),
    ("goldsky_parallel.py", "main", "json.loads(gp.ACCOUNTS_FILE.read_text())", "accounts_snapshot", 1),
    ("backfill_s3.py", "scan_s3_fills", "con.execute(query).fetchall()", "s3_fill", 1),
    ("backfill_s3.py", "resolve_elapsed_markets", "resp.json()", "gamma_markets", 1),
    ("backfill_s3.py", "main", "json.loads(ACCOUNTS_FILE.read_text())", "accounts_snapshot", 1),
    ("discover.py", "load_known_wallets", "json.loads(ACCOUNTS_FILE.read_text())", "accounts_snapshot", 1),
    ("discover.py", "load_existing_candidates", "json.loads(CANDIDATES_FILE.read_text())", "candidates_snapshot", 1),
    ("promote.py", "_run_promotion", "json.loads(CANDIDATES_FILE.read_text())", "candidates_snapshot", 1),
    ("promote.py", "_run_promotion", "json.loads(ACCOUNTS_FILE.read_text())", "accounts_snapshot", 1),
)


def shape(node):
    return ast.dump(node, include_attributes=False)


def calls(tree, expression):
    expected = shape(ast.parse(expression, mode="eval").body)
    return [node for node in ast.walk(tree) if isinstance(node, ast.expr) and shape(node) == expected]


def owner(tree, name):
    matches = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name]
    if len(matches) != 1:
        raise AssertionError(f"Expected one function {name}")
    return matches[0]


def linked(function, read, validator):
    """Require direct wrapping, assigned-value validation, or validated row mapping."""
    parents = {child: parent for parent in ast.walk(function) for child in ast.iter_child_nodes(parent)}
    validation = [node for node in ast.walk(function) if isinstance(node, ast.Call)
                  and isinstance(node.func, ast.Name) and node.func.id == validator]
    current = read
    while current in parents:
        current = parents[current]
        if current in validation:
            return True
        if isinstance(current, ast.Assign):
            names = [target.id for target in current.targets if isinstance(target, ast.Name)]
            for call in validation:
                if call.lineno > read.lineno and any(
                    isinstance(arg, ast.Name) and arg.id in names for arg in call.args
                ):
                    return True
        if isinstance(current, (ast.ListComp, ast.GeneratorExp)):
            for generator in current.generators:
                if read in ast.walk(generator.iter) and isinstance(generator.target, ast.Name):
                    if any(isinstance(arg, ast.Name) and arg.id == generator.target.id
                           for call in validation if call in ast.walk(current.elt) for arg in call.args):
                        return True
    return False


def external_reads(function):
    """Inventory network JSON, file JSON, CSV and S3 row reads."""
    parents = {child: parent for parent in ast.walk(function) for child in ast.iter_child_nodes(parent)}
    for node in ast.walk(function):
        if (isinstance(node, ast.Attribute) and node.attr in ("text", "content") and
                isinstance(node.value, ast.Name) and node.value.id in ("resp", "response", "r", "results")):
            yield node
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Attribute):
            method = node.func.attr
            if method in ("json", "fetchall", "read_csv"):
                yield node
            elif method == "read_text":
                current = node
                while current in parents and not (isinstance(current, ast.Call) and
                                                   isinstance(current.func, ast.Attribute) and
                                                   isinstance(current.func.value, ast.Name) and
                                                   current.func.value.id == "json" and
                                                   current.func.attr == "loads"):
                    current = parents[current]
                if current not in parents:
                    yield node
            elif method in ("reader", "DictReader") and isinstance(node.func.value, ast.Name) and node.func.value.id == "csv":
                yield node
            elif method in ("load", "loads") and isinstance(node.func.value, ast.Name) and node.func.value.id == "json":
                yield node


def default_violations(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            name = node.func.attr
            if name == "setdefault" or (name in ("get", "pop") and
                (len(node.args) > 1 or any(isinstance(arg, ast.Starred) for arg in node.args) or
                 any(kw.arg in ("default", None) for kw in node.keywords))):
                yield node, f"defaulted {name}"
        if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.Or):
            first = node.values[0]
            if isinstance(first, ast.Subscript) or (isinstance(first, ast.Call) and
                isinstance(first.func, ast.Attribute) and first.func.attr == "get"):
                yield node, "or-default"
        if (isinstance(node, ast.IfExp) and isinstance(node.test, ast.Compare) and
                any(isinstance(op, (ast.In, ast.NotIn)) for op in node.test.ops)):
            yield node, "conditional-key default"
        if (isinstance(node, ast.If) and node.orelse and isinstance(node.test, ast.Compare) and
                any(isinstance(op, (ast.In, ast.NotIn)) for op in node.test.ops)):
            yield node, "conditional-key default"


class BoundarySourceTests(unittest.TestCase):
    def test_all_external_reads_are_in_inventory_and_validated(self):
        inventory = set()
        for module, function_name, expression, validator, count in READ_SITES:
            function = owner(ast.parse((ROOT / module).read_text()), function_name)
            matches = calls(function, expression)
            self.assertEqual(len(matches), count, (module, function_name, expression))
            for read in matches:
                self.assertTrue(linked(function, read, validator),
                                f"{module}:{read.lineno} must pass through {validator}")
                inventory.add((module, read.lineno, read.col_offset))
        for path in ROOT.glob("*.py"):
            if path.name == "external.py":
                continue
            tree = ast.parse(path.read_text())
            for read in external_reads(tree):
                self.assertIn((path.name, read.lineno, read.col_offset), inventory,
                              f"Unlisted external read at {path.name}:{read.lineno}")

    def test_no_default_syntax_outside_validators(self):
        for path in ROOT.glob("*.py"):
            if path.name == "external.py":
                continue
            for node, reason in default_violations(ast.parse(path.read_text())):
                self.fail(f"{path.name}:{node.lineno} uses {reason}")

    def test_default_syntax_mutations_are_rejected(self):
        forms = (
            'row.get("x", 0)', 'row.get("x") or 0',
            'row["x"] if "x" in row else 0',
            'row.setdefault("x", 0)', 'row.pop("x", 0)',
            'row.get(*args)', 'row.get("x", default=0)',
            'if "x" in row:\n    value = row["x"]\nelse:\n    value = 0',
        )
        for form in forms:
            with self.subTest(form=form):
                self.assertTrue(list(default_violations(ast.parse(form))))

    def test_raw_call_site_mutations_are_rejected(self):
        rejected = 0
        for module, function_name, expression, validator, count in READ_SITES:
            source = (ROOT / module).read_text()
            for occurrence in range(count):
                original = owner(ast.parse(source), function_name)
                original_read = calls(original, expression)[occurrence]
                self.assertTrue(linked(original, original_read, validator))
                candidates = [node for node in ast.walk(original) if isinstance(node, ast.Call)
                              and isinstance(node.func, ast.Name) and node.func.id == validator]
                caught = False
                for candidate in candidates:
                    mutated = copy.deepcopy(original)
                    position = (candidate.lineno, candidate.col_offset)

                    class Revert(ast.NodeTransformer):
                        def visit_Call(self, node):
                            if (node.lineno, node.col_offset) == position:
                                return self.visit(node.args[0])
                            return self.generic_visit(node)

                    mutated = Revert().visit(mutated)
                    reads = [read for read in calls(mutated, expression)
                             if (read.lineno, read.col_offset) ==
                             (original_read.lineno, original_read.col_offset)]
                    if len(reads) == 1 and not linked(mutated, reads[0], validator):
                        caught = True
                        rejected += 1
                        break
                self.assertTrue(caught, f"raw access escaped at {module}:{original_read.lineno}")
        self.assertEqual(rejected, sum(site[4] for site in READ_SITES))
