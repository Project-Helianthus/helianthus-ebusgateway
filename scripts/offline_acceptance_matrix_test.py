#!/usr/bin/env python3
"""Offline tests of the offline acceptance matrix validator.

Every matrix, page and test file here is synthetic, except the tests that read
the committed matrix and page of this repository.
"""

from __future__ import annotations

import copy
import importlib.util
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).with_name("offline_acceptance_matrix.py")
REPO_ROOT = SCRIPT.resolve().parents[1]
COMMITTED_MATRIX = REPO_ROOT / "docs" / "testing" / "offline-acceptance-matrix.json"
COMMITTED_PAGE = REPO_ROOT / "docs" / "testing" / "offline-acceptance-matrix.md"
SPEC = importlib.util.spec_from_file_location("offline_acceptance_matrix", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MATRIX = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MATRIX)

PREFIX = "offline acceptance matrix: "
GO_TEST = "internal/sample TestComposedSourcePublishesEveryFact"
PY_TEST = "scripts/sample_test.py test_replayed_source_reaches_every_surface"
GO_SOURCE = (
    "package sample\n\nimport \"testing\"\n\n"
    "func TestComposedSourcePublishesEveryFact(t *testing.T) {}\n\n"
    "func (s *suite) TestMethodIsNotATopLevelTest(t *testing.T) {}\n\n"
    "// Not a declaration: func TestOnlyMentionedInAComment(t *testing.T)\n"
)
OUTSIDE_SOURCE = "package outside\n\nimport \"testing\"\n\nfunc TestDeclaredOutsideTheRoot(t *testing.T) {}\n"
GO_PRODUCTION = "package sample\n\nfunc TestOnlyDeclaredInProductionCode() {}\n"
PY_SOURCE = (
    "import unittest\n\n\nclass Sample(unittest.TestCase):\n"
    "    def test_replayed_source_reaches_every_surface(self):\n        pass\n"
)


def cell(state="not_run", test=None, reason=None, issue=None):
    return {"state": state, "test": test, "reason": reason, "issue": issue}


def members(prefix, count, release=None):
    entries = []
    for index in range(count):
        entry = {"id": f"{prefix}_{index}", "title": f"Synthetic {prefix} {index}"}
        if release is not None:
            entry["release"] = release
        entries.append(entry)
    return entries


def synthetic_matrix():
    families = members("family", 2, release="in_0_7")
    surfaces = members("surface", 2)
    graph_rows = members("graph_row", 2)
    graph_negatives = members("negative", 2)
    conditions = members("condition", 2)

    def grid(columns):
        return {f["id"]: {c["id"]: cell() for c in columns} for f in families}

    return {
        "format": "helianthus-offline-acceptance-matrix/1",
        "note": "Synthetic matrix.",
        "families": families,
        "surfaces": surfaces,
        "graph_rows": graph_rows,
        "graph_negatives": graph_negatives,
        "conditions": conditions,
        "cells": {
            "family_surface": grid(surfaces),
            "family_graph": grid(graph_rows),
            "family_condition": grid(conditions),
            "graph_negative": {n["id"]: cell() for n in graph_negatives},
        },
    }


def every_cell(document):
    cells = document["cells"]
    for axis in ("family_surface", "family_graph", "family_condition"):
        for row in cells[axis].values():
            yield from row.values()
    yield from cells["graph_negative"].values()


def filled_matrix():
    document = synthetic_matrix()
    for item in every_cell(document):
        item.update(cell("pass", test=GO_TEST))
    return document


class SyntheticRepository:
    """A temporary repository root holding one Go test package and one Python test script."""

    def __init__(self, case):
        holder = tempfile.TemporaryDirectory()
        case.addCleanup(holder.cleanup)
        self.root = pathlib.Path(holder.name) / "repository"
        outside = pathlib.Path(holder.name) / "outside"
        outside.mkdir()
        (outside / "outside_test.go").write_text(OUTSIDE_SOURCE, encoding="utf-8")
        package = self.root / "internal" / "sample"
        package.mkdir(parents=True)
        (package / "sample_test.go").write_text(GO_SOURCE, encoding="utf-8")
        (package / "sample.go").write_text(GO_PRODUCTION, encoding="utf-8")
        (self.root / "scripts").mkdir()
        (self.root / "scripts" / "sample_test.py").write_text(PY_SOURCE, encoding="utf-8")
        self.matrix = self.root / "matrix.json"
        self.page = self.root / "page.md"

    def write(self, document, page_tables=None):
        self.matrix.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
        tables = MATRIX.render(document) if page_tables is None else page_tables
        self.page.write_text(
            "# Synthetic page\n\n" + MATRIX.BEGIN + "\n\n" + tables + "\n" + MATRIX.END + "\n",
            encoding="utf-8",
        )

    def run(self, *arguments):
        env = dict(os.environ)
        env.pop("PYTHONPATH", None)
        page = ["--page", str(self.page)] if arguments[0] == "check" else []
        return subprocess.run(
            [sys.executable, str(SCRIPT), *arguments, "--root", str(self.root),
             "--matrix", str(self.matrix), *page],
            check=False, capture_output=True, text=True, env=env, timeout=60,
        )


class OfflineAcceptanceMatrixTests(unittest.TestCase):
    def setUp(self):
        self.repo = SyntheticRepository(self)

    def errors(self, document):
        return MATRIX.validate(document, self.repo.root)

    def assert_refused(self, document, fragment):
        errors = self.errors(document)
        self.assertTrue(any(fragment in error for error in errors), f"{fragment!r} not in {errors}")

    def test_valid_matrix_with_every_state_is_accepted(self):
        document = synthetic_matrix()
        surface = document["cells"]["family_surface"]["family_0"]
        surface["surface_0"] = cell("pass", test=GO_TEST)
        surface["surface_1"] = cell("pass", test=PY_TEST)
        document["cells"]["family_graph"]["family_1"]["graph_row_0"] = cell(
            "not_applicable", reason="The family publishes no relation.")
        document["cells"]["graph_negative"]["negative_1"] = cell(
            "not_in_0_7", reason="Publication is withheld by its contract.", issue="helianthus-sample#7")
        self.assertEqual(self.errors(document), [])
        self.repo.write(document)
        result = self.repo.run("check")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(result.stdout.startswith(PREFIX), result.stdout)

    def test_axis_member_without_cell_is_refused(self):
        cases = {
            "family_surface family_1 x surface_0 has no cell":
                lambda cells: cells["family_surface"]["family_1"].pop("surface_0"),
            "family_graph family_0 x graph_row_1 has no cell":
                lambda cells: cells["family_graph"]["family_0"].pop("graph_row_1"),
            "family_condition family_1 x condition_0 has no cell":
                lambda cells: cells["family_condition"].pop("family_1"),
            "graph_negative negative_0 has no cell":
                lambda cells: cells["graph_negative"].pop("negative_0"),
        }
        for message, mutate in cases.items():
            with self.subTest(message=message):
                document = synthetic_matrix()
                mutate(document["cells"])
                self.assert_refused(document, message)

    def test_cell_naming_unknown_member_is_refused(self):
        cases = {
            "family_surface family_9 names an unknown member":
                lambda cells: cells["family_surface"].setdefault("family_9", {}),
            "family_graph family_0 x graph_row_9 names an unknown member":
                lambda cells: cells["family_graph"]["family_0"].setdefault("graph_row_9", cell()),
            "family_condition family_1 x condition_9 names an unknown member":
                lambda cells: cells["family_condition"]["family_1"].setdefault("condition_9", cell()),
            "graph_negative negative_9 names an unknown member":
                lambda cells: cells["graph_negative"].setdefault("negative_9", cell()),
            "cells.surface_graph is not an axis":
                lambda cells: cells.setdefault("surface_graph", {}),
        }
        for message, mutate in cases.items():
            with self.subTest(message=message):
                document = synthetic_matrix()
                mutate(document["cells"])
                self.assert_refused(document, message)

    def test_duplicate_cell_is_refused(self):
        text = json.dumps(synthetic_matrix(), indent=2)
        duplicated = text.replace(
            '"negative_0": {', '"negative_0": ' + json.dumps(cell()) + ',\n    "negative_0": {', 1)
        self.assertNotEqual(text, duplicated)
        self.repo.matrix.write_text(duplicated, encoding="utf-8")
        with self.assertRaisesRegex(MATRIX.MatrixError, "duplicate key 'negative_0'"):
            MATRIX.load(self.repo.matrix)
        result = self.repo.run("check")
        self.assertEqual(result.returncode, 1)
        self.assertIn(PREFIX + "duplicate key 'negative_0'", result.stderr)

    def test_duplicate_axis_member_is_refused(self):
        document = synthetic_matrix()
        document["surfaces"].append(copy.deepcopy(document["surfaces"][0]))
        self.assert_refused(document, "surfaces surface_0 is declared twice")

    def test_pass_without_test_is_refused(self):
        for test in (None, "", "   "):
            with self.subTest(test=test):
                document = synthetic_matrix()
                document["cells"]["family_surface"]["family_0"]["surface_1"] = cell("pass", test=test)
                self.assert_refused(document, "family_surface family_0 x surface_1")

    def test_pass_naming_missing_test_is_refused(self):
        cases = (
            "internal/absent TestComposedSourcePublishesEveryFact",
            "internal/sample TestComposedSourcePublishes",
            "internal/sample TestOnlyDeclaredInProductionCode",
            "internal/sample TestMethodIsNotATopLevelTest",
            "internal/sample TestOnlyMentionedInAComment",
            "scripts/absent_test.py test_replayed_source_reaches_every_surface",
            "scripts/sample_test.py test_replayed_source",
            "TestComposedSourcePublishesEveryFact",
            "internal/sample TestComposedSourcePublishesEveryFact extra",
            "../outside TestDeclaredOutsideTheRoot",
            "internal/../../outside TestDeclaredOutsideTheRoot",
            "/internal/sample TestComposedSourcePublishesEveryFact",
            "internal/sample Test(.*)",
        )
        for test in cases:
            with self.subTest(test=test):
                document = synthetic_matrix()
                document["cells"]["family_condition"]["family_1"]["condition_1"] = cell("pass", test=test)
                self.assert_refused(document, "family_condition family_1 x condition_1")

    def test_reasoned_state_without_one_line_reason_is_refused(self):
        for state in ("not_applicable", "not_in_0_7"):
            for reason in (None, "", "  ", "first line\nsecond line"):
                with self.subTest(state=state, reason=reason):
                    document = synthetic_matrix()
                    document["cells"]["graph_negative"]["negative_1"] = cell(
                        state, reason=reason, issue="helianthus-sample#7")
                    self.assert_refused(document, "graph_negative negative_1")

    def test_not_in_release_without_owner_issue_is_refused(self):
        for issue in (None, "#7", "helianthus-sample#", "helianthus-sample#0", "Helianthus Sample#7", "7"):
            with self.subTest(issue=issue):
                document = synthetic_matrix()
                document["cells"]["family_graph"]["family_0"]["graph_row_0"] = cell(
                    "not_in_0_7", reason="Outside the release.", issue=issue)
                self.assert_refused(document, "family_graph family_0 x graph_row_0")

    def test_unknown_state_or_cell_key_is_refused(self):
        document = synthetic_matrix()
        document["cells"]["family_surface"]["family_0"]["surface_0"] = cell("passed")
        self.assert_refused(document, "family_surface family_0 x surface_0 has unknown state 'passed'")
        document = synthetic_matrix()
        document["cells"]["family_surface"]["family_0"]["surface_0"]["owner"] = "someone"
        self.assert_refused(document, "family_surface family_0 x surface_0 must be an object")

    def test_require_complete_fails_on_one_not_run_cell(self):
        document = filled_matrix()
        document["cells"]["family_condition"]["family_1"]["condition_0"] = cell()
        self.repo.write(document)
        self.assertEqual(self.repo.run("check").returncode, 0)
        result = self.repo.run("check", "--require-complete")
        self.assertEqual(result.returncode, 1)
        self.assertIn(
            PREFIX + "incomplete: 1 cells not_run (family_surface 0, family_graph 0, "
            "family_condition 1, graph_negative 0)", result.stderr)
        document["cells"]["family_condition"]["family_1"]["condition_0"] = cell(
            "not_applicable", reason="The condition cannot occur for this family.")
        self.repo.write(document)
        result = self.repo.run("check", "--require-complete")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_page_tables_that_differ_from_the_rendered_tables_are_refused(self):
        document = synthetic_matrix()
        self.repo.write(document, page_tables=MATRIX.render(document).replace("not_run", "pass", 1))
        result = self.repo.run("check")
        self.assertEqual(result.returncode, 1)
        self.assertIn(PREFIX + "the generated tables of", result.stderr)

    def test_every_message_carries_the_prefix(self):
        document = synthetic_matrix()
        document["cells"]["graph_negative"].pop("negative_0")
        document["cells"]["family_surface"]["family_0"]["surface_0"] = cell("pass", test="absent TestNothing")
        self.repo.write(document, page_tables="")
        result = self.repo.run("check")
        self.assertEqual(result.returncode, 1)
        lines = result.stderr.splitlines()
        self.assertGreaterEqual(len(lines), 2)
        for line in lines:
            self.assertTrue(line.startswith(PREFIX), line)

    def test_render_prints_the_generated_tables(self):
        document = synthetic_matrix()
        self.repo.write(document)
        result = self.repo.run("render")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, MATRIX.render(document))


class CommittedMatrixTests(unittest.TestCase):
    def test_committed_matrix_has_a_row_per_composed_growatt_can_family(self):
        document = MATRIX.load(COMMITTED_MATRIX)
        families = {member["id"] for member in document["families"]}
        for family in ("growatt_bms_can", "growatt_bms_pcs_can", "growatt_bms_link_can"):
            self.assertIn(family, families)
            for axis in ("family_surface", "family_graph", "family_condition"):
                self.assertIn(family, document["cells"][axis])

    def test_committed_matrix_is_valid(self):
        document = MATRIX.load(COMMITTED_MATRIX)
        self.assertEqual(MATRIX.validate(document, REPO_ROOT), [])

    def test_huawei_gateway_families_are_tracked_separately(self):
        document = MATRIX.load(COMMITTED_MATRIX)
        ids = [family["id"] for family in document["families"]]
        self.assertIn("huawei_smartlogger", ids)
        self.assertIn("huawei_sdongle", ids)
        self.assertNotIn("huawei_logger_dongle", ids)
        for table in ("family_surface", "family_graph", "family_condition"):
            self.assertIn("huawei_smartlogger", document["cells"][table])
            self.assertIn("huawei_sdongle", document["cells"][table])

    def test_rendered_tables_equal_the_committed_page(self):
        document = MATRIX.load(COMMITTED_MATRIX)
        section = MATRIX.generated_section(COMMITTED_PAGE.read_text(encoding="utf-8"))
        self.assertIsNotNone(section)
        self.assertEqual(section.strip("\n"), MATRIX.render(document).strip("\n"))

    def test_committed_check_passes_and_complete_check_counts_the_open_cells(self):
        env = dict(os.environ)
        env.pop("PYTHONPATH", None)
        result = subprocess.run([sys.executable, str(SCRIPT), "check"], check=False,
                                capture_output=True, text=True, env=env, timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr)
        complete = subprocess.run([sys.executable, str(SCRIPT), "check", "--require-complete"],
                                  check=False, capture_output=True, text=True, env=env, timeout=60)
        self.assertEqual(complete.returncode, 1)
        self.assertIn(PREFIX + "incomplete: ", complete.stderr)


if __name__ == "__main__":
    unittest.main()
