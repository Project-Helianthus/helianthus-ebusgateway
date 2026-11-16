#!/usr/bin/env python3
"""Offline tests of the modbusreg package adoption script.

Every Go file here is synthetic. One test reads the repository and checks that
the committed tree is already in the rewritten form; gofmt is replaced by a
recording stand-in so the tests need no Go toolchain.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import pathlib
import sys
import tempfile
import textwrap
import unittest
from unittest import mock

SCRIPT = pathlib.Path(__file__).with_name("adopt_modbusreg_packages.py")
REPO_ROOT = SCRIPT.resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("adopt_modbusreg_packages", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
ADOPT = importlib.util.module_from_spec(SPEC)
sys.modules["adopt_modbusreg_packages"] = ADOPT
SPEC.loader.exec_module(ADOPT)

ROOT = ADOPT.ROOT_IMPORT


def go(text: str) -> str:
    return textwrap.dedent(text).lstrip("\n")


BLOCK_KEEPS_ROOT = go(
    f'''
    package sample

    import (
    \t"fmt"

    \tmodbusreg "{ROOT}"
    \tsemreg "example.com/semreg"
    )

    // first names modbusreg.GrowattAggregateAttempt and modbusreg.Observation.
    func first(observation modbusreg.Observation) modbusreg.GrowattAggregateAttempt {{
    \tfmt.Println("modbusreg.GrowattAggregateAttempt stays in this literal", semreg.X)
    \treturn modbusreg.GrowattAggregateAttempt{{}}
    }}

    func second(states ...modbusreg.SampleLedgerState) modbusreg.SunSpecChain {{
    \treturn modbusreg.NewSunSpecChain(nil)
    }}
    '''
)

BLOCK_KEEPS_ROOT_REWRITTEN = go(
    f'''
    package sample

    import (
    \t"fmt"

    \tmodbusreg "{ROOT}"
    \t"{ROOT}/growatt"
    \t"{ROOT}/ledger"
    \t"{ROOT}/sunspec"
    \tsemreg "example.com/semreg"
    )

    // first names growatt.AggregateAttempt and modbusreg.Observation.
    func first(observation modbusreg.Observation) growatt.AggregateAttempt {{
    \tfmt.Println("modbusreg.GrowattAggregateAttempt stays in this literal", semreg.X)
    \treturn growatt.AggregateAttempt{{}}
    }}

    func second(states ...ledger.SampleLedgerState) sunspec.Chain {{
    \treturn sunspec.NewChain(nil)
    }}
    '''
)


class TableTests(unittest.TestCase):
    def test_every_family_has_a_table_and_rows_are_unique(self) -> None:
        families = {"growatt", "tesla", "huawei", "sunspec", "fronius", "outback", "detection", "ledger"}
        self.assertEqual(set(ADOPT.RENAMED), families)
        self.assertEqual(set(ADOPT.QUALIFIER_ONLY), families)
        mapping = ADOPT.build_mapping()
        rows = sum(len(rows) for rows in ADOPT.RENAMED.values()) + sum(len(rows) for rows in ADOPT.QUALIFIER_ONLY.values())
        self.assertEqual(len(mapping), rows)
        for family, rows in ADOPT.RENAMED.items():
            for previous, current in rows:
                self.assertNotEqual(previous, current, f"{family}: a renamed row keeps its spelling")
        self.assertEqual(mapping["GrowattAggregateAttempt"], ("growatt", "AggregateAttempt"))
        self.assertEqual(mapping["SampleLedgerState"], ("ledger", "SampleLedgerState"))
        self.assertNotIn("Observation", mapping)

    def test_a_previous_name_in_two_tables_is_refused(self) -> None:
        renamed = dict(ADOPT.RENAMED)
        renamed["tesla"] = renamed["tesla"] + (("GrowattAggregateAttempt", "Other"),)
        with mock.patch.object(ADOPT, "RENAMED", renamed):
            with self.assertRaisesRegex(ADOPT.AdoptionError, "two tables"):
                ADOPT.build_mapping()

    def test_two_previous_names_for_one_current_name_are_refused(self) -> None:
        qualifier_only = dict(ADOPT.QUALIFIER_ONLY)
        qualifier_only["growatt"] = qualifier_only["growatt"] + ("AggregateAttempt",)
        with mock.patch.object(ADOPT, "QUALIFIER_ONLY", qualifier_only):
            with self.assertRaisesRegex(ADOPT.AdoptionError, "current name"):
                ADOPT.build_mapping()


class RewriteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.mapping = ADOPT.build_mapping()
        self.names = ADOPT.current_names(self.mapping)

    def rewrite(self, source: str, relative: str = "") -> str:
        rewritten, problems = ADOPT.rewrite_go(source, self.mapping, relative)
        self.assertEqual(problems, [])
        return rewritten

    def test_block_import_keeps_root_and_adds_sorted_packages(self) -> None:
        rewritten = self.rewrite(BLOCK_KEEPS_ROOT)
        self.assertEqual(rewritten, BLOCK_KEEPS_ROOT_REWRITTEN)
        self.assertEqual(ADOPT.check_go(rewritten, self.mapping, self.names), [])

    def test_rewrite_is_idempotent(self) -> None:
        once = self.rewrite(BLOCK_KEEPS_ROOT)
        self.assertEqual(self.rewrite(once), once)

    def test_root_import_is_dropped_when_no_root_name_is_left(self) -> None:
        source = go(
            f'''
            package sample

            import (
            \t"time"

            \tmodbusreg "{ROOT}"
            )

            var limit = modbusreg.TeslaGen3CurrentLimitOperationVersion24443
            var at time.Time
            '''
        )
        rewritten = self.rewrite(source)
        self.assertNotIn(f'"{ROOT}"', rewritten)
        self.assertIn(f'\t"{ROOT}/tesla"\n', rewritten)
        self.assertIn("var limit = tesla.Gen3CurrentLimitOperationVersion24443", rewritten)

    def test_single_line_import_becomes_a_block_or_stays_single(self) -> None:
        both = f'package sample\n\nimport modbusreg "{ROOT}"\n\nvar a modbusreg.Observation\nvar b modbusreg.HuaweiDecimal\n'
        rewritten = self.rewrite(both)
        self.assertIn(f'import (\n\tmodbusreg "{ROOT}"\n\t"{ROOT}/huawei"\n)\n', rewritten)
        moved_only = f'package sample\n\nimport modbusreg "{ROOT}"\n\nvar b modbusreg.HuaweiDecimal\n'
        self.assertIn(f'import "{ROOT}/huawei"\n', self.rewrite(moved_only))

    def test_comment_only_use_adds_no_import(self) -> None:
        source = f'package sample\n\nimport modbusreg "{ROOT}"\n\n// see modbusreg.HuaweiDecimal\nvar a modbusreg.Observation\n'
        rewritten = self.rewrite(source)
        self.assertIn("// see huawei.Decimal\n", rewritten)
        self.assertNotIn("/huawei", rewritten)
        self.assertEqual(ADOPT.check_go(rewritten, self.mapping, self.names), [])

    def test_unaliased_root_import_uses_the_package_name(self) -> None:
        source = f'package sample\n\nimport "{ROOT}"\n\nvar a modbusreg.Observation\nvar b modbusreg.OutBackWordSpan\n'
        rewritten = self.rewrite(source)
        self.assertIn(f'import (\n\t"{ROOT}"\n\t"{ROOT}/outback"\n)\n', rewritten)
        self.assertIn("var b outback.WordSpan", rewritten)

    def test_other_root_alias_is_followed(self) -> None:
        source = f'package sample\n\nimport reg "{ROOT}"\n\nvar a reg.DetectionCandidate\nvar b modbusreg.DetectionCandidate\n'
        rewritten = self.rewrite(source)
        self.assertIn("var a detection.Candidate", rewritten)
        self.assertIn("var b modbusreg.DetectionCandidate", rewritten)

    def test_blank_or_dot_root_import_is_refused(self) -> None:
        for name in ("_", "."):
            source = f'package sample\n\nimport {name} "{ROOT}"\n'
            _, problems = ADOPT.rewrite_go(source, self.mapping)
            self.assertEqual(problems, [f"the root package is imported as {name!r}"])

    def test_file_without_root_import_is_untouched(self) -> None:
        source = 'package sample\n\nvar a = modbusreg.GrowattAggregateAttempt{}\n'
        self.assertEqual(self.rewrite(source), source)

    def test_method_call_becomes_family_function_with_receiver_first(self) -> None:
        source = go(
            f'''
            package sample

            import modbusreg "{ROOT}"

            func pick(producer *producerType, snapshot modbusreg.SunSpecChainSnapshot) {{
            \tselection := producer.registry.SelectFroniusObservedFlavor(snapshot)
            \tempty := registry.EvaluateFroniusObservedFlavorV12()
            \t_, _ = selection, empty
            }}
            '''
        )
        rewritten = self.rewrite(source)
        self.assertIn("selection := fronius.SelectObservedFlavor(producer.registry, snapshot)", rewritten)
        self.assertIn("empty := fronius.EvaluateObservedFlavorV12(registry)", rewritten)
        self.assertIn(f'import (\n\t"{ROOT}/fronius"\n\t"{ROOT}/sunspec"\n)\n', rewritten)

    def test_import_name_table_matches_path_and_family(self) -> None:
        source = go(
            f'''
            package sample

            import modbusreg "{ROOT}"

            func build(ledger modbusreg.SampleLedgerState) modbusreg.GrowattAggregateAttempt {{
            \treturn modbusreg.GrowattAggregateAttempt{{}}
            }}
            '''
        )
        table = {("pkg/sample_test.go", "ledger"): "modbusledger"}
        with mock.patch.object(ADOPT, "IMPORT_NAMES", table):
            named = self.rewrite(source, "pkg/sample_test.go")
            other_path = self.rewrite(source, "pkg/other_test.go")
        # The path field of the key: only the listed file gets the import name.
        self.assertIn(f'\tmodbusledger "{ROOT}/ledger"\n', named)
        self.assertIn("func build(ledger modbusledger.SampleLedgerState)", named)
        self.assertNotIn("modbusledger", other_path)
        # The family field of the key: the growatt package keeps its own name.
        self.assertIn(f'\t"{ROOT}/growatt"\n', named)
        self.assertIn("return growatt.AggregateAttempt{}", named)
        with mock.patch.object(ADOPT, "IMPORT_NAMES", table):
            self.assertEqual(ADOPT.check_go(named, self.mapping, self.names, "pkg/sample_test.go"), [])


class CheckTests(unittest.TestCase):
    def setUp(self) -> None:
        self.mapping = ADOPT.build_mapping()
        self.names = ADOPT.current_names(self.mapping)

    def test_moved_name_left_behind_is_reported(self) -> None:
        problems = ADOPT.check_go(BLOCK_KEEPS_ROOT, self.mapping, self.names)
        self.assertIn("a moved name or a replaced method call is left behind", problems)

    def test_unused_family_import_is_reported(self) -> None:
        source = f'package sample\n\nimport (\n\tmodbusreg "{ROOT}"\n\t"{ROOT}/tesla"\n)\n\nvar a modbusreg.Observation\nvar tesla = 1\n'
        self.assertEqual(ADOPT.check_go(source, self.mapping, self.names), [f'import "{ROOT}/tesla" is not used'])

    def test_unused_root_import_is_reported(self) -> None:
        source = f'package sample\n\nimport (\n\tmodbusreg "{ROOT}"\n\t"{ROOT}/tesla"\n)\n\nvar a tesla.Gen3HSCProfile\n'
        self.assertEqual(ADOPT.check_go(source, self.mapping, self.names), [f'import "{ROOT}" is not used'])

    def test_remaining_method_call_is_reported(self) -> None:
        source = f'package sample\n\nimport modbusreg "{ROOT}"\n\nvar a modbusreg.Observation\nvar b = build().SelectFroniusObservedFlavor(a)\n'
        problems = ADOPT.check_go(source, self.mapping, self.names)
        self.assertIn("call of SelectFroniusObservedFlavor remains", problems)

    def test_method_name_inside_a_literal_is_not_a_call(self) -> None:
        source = f'package sample\n\nimport modbusreg "{ROOT}"\n\nvar a modbusreg.Observation\nvar b = "x.SelectFroniusObservedFlavor(a)"\n'
        self.assertEqual(ADOPT.check_go(source, self.mapping, self.names), [])


class DocumentationAndExpectationTests(unittest.TestCase):
    def test_doc_rewrites_backquoted_moved_names_only(self) -> None:
        mapping = ADOPT.build_mapping()
        text = "Uses `TeslaGen3ReadAcquisition`, `modbusreg.HuaweiDecimal`, `Observation` and TeslaGen3ReadAcquisition.\n"
        self.assertEqual(
            ADOPT.rewrite_doc(text, mapping),
            "Uses `tesla.Gen3ReadAcquisition`, `huawei.Decimal`, `Observation` and TeslaGen3ReadAcquisition.\n",
        )

    def test_source_text_expectation_is_rewritten_or_reported(self) -> None:
        path, previous, current = ADOPT.SOURCE_TEXT_EXPECTATIONS[0]
        text, problems = ADOPT.rewrite_expectations(path, f"\t{previous}\n")
        self.assertEqual((text, problems), (f"\t{current}\n", []))
        self.assertEqual(ADOPT.rewrite_expectations(path, f"\t{current}\n"), (f"\t{current}\n", []))
        _, problems = ADOPT.rewrite_expectations(path, "nothing\n")
        self.assertEqual(problems, [f"expected source text {previous!r} not found"])
        self.assertEqual(ADOPT.rewrite_expectations("other/path_test.go", f"{previous}\n"), (f"{previous}\n", []))


class RunTests(unittest.TestCase):
    def make_repo(self, directory: pathlib.Path) -> None:
        (directory / "pkg").mkdir()
        (directory / "pkg" / "sample.go").write_text(BLOCK_KEEPS_ROOT, encoding="utf-8")
        (directory / ".hidden").mkdir()
        (directory / ".hidden" / "skip.go").write_text(BLOCK_KEEPS_ROOT, encoding="utf-8")
        for relative in ADOPT.DOC_PAGES:
            page = directory / relative
            page.parent.mkdir(parents=True, exist_ok=True)
            page.write_text("The `TeslaGen3ReadAcquisition` lifecycle.\n", encoding="utf-8")
        for relative, previous, _ in ADOPT.SOURCE_TEXT_EXPECTATIONS:
            path = directory / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"package sample\n\nvar quoted = []string{{\n\t{previous}\n}}\n", encoding="utf-8")

    def fake_gofmt(self, directory: pathlib.Path, listed: str = "") -> tuple[str, pathlib.Path]:
        log = directory / "gofmt.log"
        tool = directory / "gofmt"
        tool.write_text(
            "#!/usr/bin/env python3\n"
            "import sys\n"
            f"open({str(log)!r}, 'a').write(' '.join(sys.argv[1:]) + '\\n')\n"
            f"print({listed!r}) if sys.argv[1] == '-l' and {listed!r} else None\n",
            encoding="utf-8",
        )
        tool.chmod(0o755)
        return str(tool), log

    def test_rewrite_then_check_over_a_synthetic_repository(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            repo = pathlib.Path(temporary) / "repo"
            tools = pathlib.Path(temporary) / "tools"
            repo.mkdir()
            tools.mkdir()
            self.make_repo(repo)
            gofmt, log = self.fake_gofmt(tools)
            changed, problems = ADOPT.run(repo, check=True, gofmt=gofmt)
            self.assertIn("pkg/sample.go: a moved name or a replaced method call is left behind", problems)
            self.assertEqual(len(changed), 3)
            changed, problems = ADOPT.run(repo, check=False, gofmt=gofmt)
            self.assertEqual(problems, [])
            expectation = ADOPT.SOURCE_TEXT_EXPECTATIONS[0][0]
            self.assertEqual(changed, sorted([expectation, "pkg/sample.go"]) + list(ADOPT.DOC_PAGES))
            self.assertEqual((repo / "pkg" / "sample.go").read_text(encoding="utf-8"), BLOCK_KEEPS_ROOT_REWRITTEN)
            self.assertEqual((repo / ".hidden" / "skip.go").read_text(encoding="utf-8"), BLOCK_KEEPS_ROOT)
            self.assertIn("`tesla.Gen3ReadAcquisition`", (repo / ADOPT.DOC_PAGES[0]).read_text(encoding="utf-8"))
            self.assertEqual(log.read_text(encoding="utf-8").splitlines(), [f"-w {expectation} pkg/sample.go"])
            changed, problems = ADOPT.run(repo, check=True, gofmt=gofmt)
            self.assertEqual((changed, problems), ([], []))
            self.assertEqual(log.read_text(encoding="utf-8").splitlines()[-1], "-l pkg/sample.go")

    def test_check_reports_unformatted_files_and_main_exit_status(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            repo = pathlib.Path(temporary) / "repo"
            tools = pathlib.Path(temporary) / "tools"
            repo.mkdir()
            tools.mkdir()
            self.make_repo(repo)
            gofmt, _ = self.fake_gofmt(tools, listed="pkg/sample.go")
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(ADOPT.main(["--root", str(repo), "--gofmt", gofmt]), 0)
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                self.assertEqual(ADOPT.main(["--root", str(repo), "--gofmt", gofmt, "--check"]), 1)
            self.assertIn("pkg/sample.go: not gofmt-formatted", stderr.getvalue())

    def test_committed_tree_is_already_rewritten(self) -> None:
        changed, problems = ADOPT.run(REPO_ROOT, check=True, gofmt=None)
        self.assertEqual(problems, [])
        self.assertEqual(changed, [])


if __name__ == "__main__":
    unittest.main()
