"""Synthetic acceptance tests for the offline Python debt ratchet."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
BASELINE = "scripts/python-lint-baseline.json"
SPEC = importlib.util.spec_from_file_location(
    "python_lint", ROOT / "scripts/check_python_lint.py")
LINT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(LINT)


def complex_source(count, name="branching"):
    return f"def {name}(value):\n" + "".join(
        f"    if value == {index}:\n        return {index}\n" for index in range(count))


def statement_source(count, name="statements"):
    return f"def {name}():\n" + "    value = 0\n" * count


CLEAN = "def old_name():\n    pass\n"
INDEBTED = "\n".join(["def OldName():\n    pass\n", complex_source(12, "old_branching"),
                      statement_source(52, "old_statements")])
NEW_DEBT = [("N802", "NewName", "def NewName():\n    pass\n"),
            ("C901", "new_branching", complex_source(11, "new_branching")),
            ("PLR0915", "new_statements", statement_source(51, "new_statements"))]


def write_tree(root, files):
    for name, source in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source, encoding="utf-8")


def committed_violations():
    return json.loads((ROOT / BASELINE).read_text())["violations"]


def most_indebted_path(violations):
    """Return the baselined file with the most distinct rules, or None."""
    paths = sorted({row["path"] for row in violations})
    rules = {path: {row["rule"] for row in violations if row["path"] == path} for path in paths}
    return max(paths, key=lambda path: len(rules[path]), default=None)


def run_checker(root):
    return subprocess.run([sys.executable, str(root / "scripts/check_python_lint.py")],
                          capture_output=True, text=True, timeout=300).returncode


class PythonLintTests(unittest.TestCase):
    def measure(self, source):
        return self.measure_tree({"sample.py": source})

    def measure_tree(self, files):
        with tempfile.TemporaryDirectory() as directory:
            write_tree(Path(directory), files)
            return LINT.measure(Path(directory))

    def assert_new_debt_fails(self, baseline, current, rule, symbol):
        added = {(row["rule"], row["symbol"]) for row in current if row not in baseline}
        self.assertIn((rule, symbol), added)
        self.assertFalse(LINT.compare(baseline, current))

    def test_new_rules_in_clean_and_indebted_files(self):
        for original, debt in [(CLEAN, set()), (INDEBTED, {"N802", "C901", "PLR0915"})]:
            baseline = self.measure(original)
            self.assertEqual({row["rule"] for row in baseline}, debt)
            for rule, symbol, addition in NEW_DEBT:
                with self.subTest(indebted=bool(debt), rule=rule):
                    current = self.measure(original + "\n" + addition)
                    self.assert_new_debt_fails(baseline, current, rule, symbol)

    def test_new_file_is_part_of_the_inventory(self):
        baseline = self.measure_tree({"scripts/tool.py": INDEBTED})
        for rule, symbol, addition in NEW_DEBT:
            with self.subTest(rule=rule):
                current = self.measure_tree({"scripts/tool.py": INDEBTED,
                                             "tests/nested/test_new.py": addition})
                self.assert_new_debt_fails(baseline, current, rule, symbol)

    def test_measured_growth_and_deletion(self):
        for prefix in ("", "def OldName():\n    pass\n\n"):
            for builder, count in [(complex_source, 11), (statement_source, 51)]:
                with self.subTest(indebted=bool(prefix), builder=builder.__name__):
                    baseline = self.measure(prefix + builder(count))
                    measured = [row for row in baseline if row["value"] is not None]
                    self.assertEqual(len(measured), 1)
                    grown = self.measure(prefix + builder(count + 1))
                    self.assertEqual([dict(row, value=None) for row in grown],
                                     [dict(row, value=None) for row in baseline])
                    self.assertGreater(sum(row["value"] or 0 for row in grown),
                                       measured[0]["value"])
                    self.assertFalse(LINT.compare(baseline, grown))
                    self.assertFalse(LINT.compare(baseline, self.measure(prefix)))
                    self.assertFalse(LINT.compare(baseline, self.measure(prefix + builder(count - 1))))

    def test_every_identity_field_and_value(self):
        row = self.measure("def BadName():\n    pass\n")[0]
        for field in ("path", "rule", "symbol", "detail", "value"):
            with self.subTest(field=field):
                changed = dict(row)
                changed[field] = 99 if field == "value" else "different"
                self.assertFalse(LINT.compare([row], [changed]))
        self.assertFalse(LINT.compare([row], [row, row]))
        self.assertTrue(LINT.compare([row], [dict(row)]))

    def test_relative_path_and_qualified_symbol(self):
        files = {"scripts/nested/sample.py":
                 "class Outer:\n    def BadName(self):\n        pass\n\n\nmixedCase = 1\n"}
        rows = self.measure_tree(files)
        self.assertEqual({(row["path"], row["rule"], row["symbol"]) for row in rows},
                         {("scripts/nested/sample.py", "N802", "Outer.BadName"),
                          ("scripts/nested/sample.py", "N816", "<module>")})
        self.assertTrue(LINT.compare(rows, self.measure_tree(files)))

    def test_ignored_file_and_noqa_still_scanned(self):
        rows = self.measure("def BadName():  # noqa: N802\n    pass\n")
        self.assertEqual(rows[0]["rule"], "N802")

    def test_local_directories_skipped_and_symlinks_rejected(self):
        bad = "def BadName():\n    pass\n"
        rows = self.measure_tree({f"{name}/bad.py": bad for name in LINT.EXCLUDED} |
                                 {"results/bad.py": bad})
        self.assertEqual([row["path"] for row in rows], ["results/bad.py"])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_tree(root, {"real.py": CLEAN})
            (root / "link.py").symlink_to(root / "real.py")
            with self.assertRaises(ValueError):
                LINT.inventory(root)

    def test_other_ruff_version_rejected(self):
        other = subprocess.CompletedProcess([], 0, stdout="ruff 0.0.0\n", stderr="")
        with mock.patch.object(LINT.subprocess, "run", return_value=other):
            with self.assertRaisesRegex(ValueError, "required Ruff version"):
                LINT.measure(ROOT / "tests")

    def test_committed_baseline_records_configuration(self):
        saved = json.loads((ROOT / BASELINE).read_text())
        header ={key: value for key, value in LINT.document([]).items() if key != "violations"}
        self.assertEqual({key: value for key, value in saved.items() if key != "violations"}, header)
        self.assertEqual((ROOT / "requirements-lint.txt").read_text(), f"ruff=={LINT.VERSION}\n")
        config = tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["ruff"]
        self.assertEqual(config, {"required-version": "==" + LINT.VERSION, "lint": {
            "select": ["N", "C90", "PLR0915"],
            "mccabe": {"max-complexity": saved["thresholds"]["complexity"]},
            "pylint": {"max-statements": saved["thresholds"]["statements"]}}})

    def test_copy_of_baselined_file_rejects_new_debt(self):
        saved = committed_violations()
        path = most_indebted_path(saved)
        if path is None:
            self.skipTest("no baselined debt remains")
        source = (ROOT / path).read_text(encoding="utf-8")
        expected = [row for row in saved if row["path"] == path]
        self.assertTrue(LINT.compare(expected, self.measure_tree({path: source})))
        for rule, symbol, addition in NEW_DEBT:
            with self.subTest(path=path, rule=rule):
                current = self.measure_tree({path: source + "\n\n" + addition})
                self.assert_new_debt_fails(expected, current, rule, symbol)

    def test_checker_exit_status_on_repository_copy(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for source in LINT.inventory(ROOT) + [ROOT / "pyproject.toml", ROOT / BASELINE]:
                target = root / source.relative_to(ROOT)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(source.read_bytes())
            self.assertEqual(run_checker(root), 0)
            write_tree(root, {"scripts/new_tool.py": NEW_DEBT[0][2]})
            self.assertEqual(run_checker(root), 1)
            (root / "scripts/new_tool.py").unlink()
            path = most_indebted_path(committed_violations())
            if path is None:
                self.skipTest("no baselined debt remains")
            indebted = root / path
            source = indebted.read_bytes()
            indebted.write_bytes(source + b"\n\n" + NEW_DEBT[0][2].encode())
            self.assertEqual(run_checker(root), 1)
            indebted.write_bytes(b"")
            self.assertEqual(run_checker(root), 1)


if __name__ == "__main__":
    unittest.main()
