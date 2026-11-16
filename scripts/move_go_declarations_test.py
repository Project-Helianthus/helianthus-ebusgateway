#!/usr/bin/env python3
"""Offline tests of the Go declaration-move script.

Every Go package here is synthetic; the one test that reads a repository file
reads the committed split manifest and checks only its own consistency.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).with_name("move_go_declarations.py")
REPO_ROOT = SCRIPT.resolve().parents[1]
COMMITTED_MANIFEST = REPO_ROOT / "scripts" / "semantic_vaillant_split.json"
SPEC = importlib.util.spec_from_file_location("move_go_declarations", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
MOVE = importlib.util.module_from_spec(SPEC)
sys.modules["move_go_declarations"] = MOVE
SPEC.loader.exec_module(MOVE)

BIG = '''package sample

import (
\t"fmt"
\t"os"
\t"strings"

\tsample "example.com/lib/go-sample"
)

// limit bounds the sample size.
const (
\tlimit = 3
\tfloor = 1
)

var table = map[string]int{"a": int(1)}

var started = fmt.Sprint("x")

// counter counts things.
type counter struct {
\tn int
}

// Add increments the counter; strings.ToUpper is only named in this comment.
func (c *counter) Add() {
\tc.n++
}

func init() {
\t_ = os.Args
}

// --- section header ---

// render formats a value and keeps "os.Exit" inside a string literal.
func render(v int) string {
\treturn fmt.Sprintf("%d os.Exit", v) + strings.Repeat("!", sample.Width)
}

func exitCode() int {
\treturn len(os.Args)
}
'''


def write_package(root: pathlib.Path, files: dict) -> pathlib.Path:
    package = root / "pkg"
    package.mkdir(parents=True, exist_ok=True)
    for name, text in files.items():
        (package / name).write_text(text, encoding="utf-8")
    return package


def write_manifest(root: pathlib.Path, files: dict, source: str = "big.go") -> pathlib.Path:
    path = root / "manifest.json"
    path.write_text(json.dumps({"package_dir": "pkg", "source": source, "files": files}),
                    encoding="utf-8")
    return path


BIG_KEEP = ["const:limit", "var:table", "var:started", "type:counter",
            "method:counter.Add", "func:init#1", "func:exitCode"]


def quiet(function, *args):
    with contextlib.redirect_stdout(io.StringIO()) as out:
        status = function(*args)
    return status, out.getvalue()


class MoveTestCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)

    def package(self, files=None):
        return write_package(self.root, files or {"big.go": BIG})


class ParserTests(MoveTestCase):
    def test_keys_cover_grouped_declarations_methods_and_generic_receivers(self):
        package = self.package({"keys.go": (
            "package sample\n\n"
            "type box[T any] struct{ v T }\n\n"
            "func (b *box[T]) Get() T { return b.v }\n\n"
            "func (b box[T]) Put(v T) {}\n\n"
            "type (\n\talpha int\n\tbeta int\n)\n\n"
            "var (\n\t// gamma is documented.\n\tgamma, delta = 1, 2\n)\n\n"
            "func init() {}\n\nfunc init() {}\n")})
        keys = [unit.key for unit in MOVE.parse_file(package / "keys.go").units]
        self.assertEqual(keys, ["type:box", "method:box.Get", "method:box.Put", "type:alpha",
                                "var:gamma", "func:init#1", "func:init#2"])

    def test_leading_comments_including_a_detached_header_stay_with_the_declaration(self):
        units = {u.key: u for u in MOVE.parse_file(self.package() / "big.go").units}
        self.assertEqual(units["func:render"].lead[0], "// --- section header ---")
        self.assertEqual(units["type:counter"].lead, ["// counter counts things."])

    def test_raw_strings_and_block_comments_do_not_end_or_start_declarations(self):
        package = self.package({"raw.go": (
            "package sample\n\n"
            "var text = `\nfunc fake() {\n`\n\n"
            "/*\nfunc alsoFake() {}\n*/\n"
            "func real() string { return \"}\" + text }\n\n"
            "const tail = \"ends with =\"\n")})
        keys = [unit.key for unit in MOVE.parse_file(package / "raw.go").units]
        self.assertEqual(keys, ["var:text", "func:real", "const:tail"])

    def test_call_initializer_detection_ignores_conversions_and_function_types(self):
        units = {u.key: u for u in MOVE.parse_file(self.package() / "big.go").units}
        self.assertTrue(units["var:started"].has_call_initializer())
        self.assertFalse(units["var:table"].has_call_initializer())
        package = self.package({"types.go": (
            "package sample\n\nvar hooks = map[string]func(int) error{}\n\n"
            "var nested = []int{\n\tlen(\"ab\"),\n}\n")})
        units = {u.key: u for u in MOVE.parse_file(package / "types.go").units}
        self.assertFalse(units["var:hooks"].has_call_initializer())
        self.assertTrue(units["var:nested"].has_call_initializer())

    def test_call_initializer_detection_sees_parenthesized_calls(self):
        package = self.package({"paren.go": (
            "package sample\n\nvar state = (register)()\n\nvar wrapped = (build())(1)\n")})
        units = {u.key: u for u in MOVE.parse_file(package / "paren.go").units}
        self.assertTrue(units["var:state"].has_call_initializer())
        self.assertTrue(units["var:wrapped"].has_call_initializer())

    def test_call_initializer_detection_sees_channel_receives(self):
        package = self.package({"recv.go": (
            "package sample\n\nvar state = <-source\n\nvar first = struct{ v int }{v: <-ready}\n\n"
            "var sink chan<- int = nil\n\nvar tap = map[string]<-chan int{}\n\n"
            "var arrow = \"<-\"\n")})
        units = {u.key: u for u in MOVE.parse_file(package / "recv.go").units}
        self.assertTrue(units["var:state"].has_call_initializer())
        self.assertTrue(units["var:first"].has_call_initializer())
        self.assertFalse(units["var:sink"].has_call_initializer())
        self.assertFalse(units["var:tap"].has_call_initializer())
        self.assertFalse(units["var:arrow"].has_call_initializer())
        manifest = write_manifest(self.root, {"recv.go": ["var:first", "var:sink", "var:tap",
                                                          "var:arrow"],
                                              "moved.go": ["var:state"]},
                                  source="recv.go")
        errors = MOVE.check_manifest(manifest, self.root)
        self.assertIn("var:state must stay in recv.go (init or call initializer)", errors)


    def test_call_initializer_detection_treats_shadowed_predeclared_names_as_calls(self):
        package = self.package({"shadow.go": (
            "package sample\n\nfunc int() int64 { return 1 }\n\nvar state = int()\n")})
        located = {u.key: u for u in MOVE.parse_file(package / "shadow.go").units}
        self.assertFalse(located["var:state"].has_call_initializer())
        self.assertTrue(located["var:state"].has_call_initializer(frozenset({"int"})))
        manifest = write_manifest(self.root, {"shadow.go": ["func:int"], "moved.go": ["var:state"]},
                                  source="shadow.go")
        errors = MOVE.check_manifest(manifest, self.root)
        self.assertIn("var:state must stay in shadow.go (init or call initializer)", errors)

    def test_check_sees_a_predeclared_name_shadowed_by_a_later_grouped_spec(self):
        self.package({"shadow.go": (
            "package sample\n\nvar (\n\tmarker = 0\n\tint    = func() int64 { return 1 }\n)\n\n"
            "var first, uint8 = 1, func() int { return 2 }\n\nvar state = int()\n")})
        units = MOVE.parse_file(self.root / "pkg" / "shadow.go").units
        self.assertEqual([MOVE.declared_names(u) for u in units],
                         [["marker", "int"], ["first", "uint8"], ["state"]])
        manifest = write_manifest(self.root, {"shadow.go": ["var:marker", "var:first"],
                                              "moved.go": ["var:state"]},
                                  source="shadow.go")
        errors = MOVE.check_manifest(manifest, self.root)
        self.assertIn("var:state must stay in shadow.go (init or call initializer)", errors)

    def test_grouped_spec_names_skip_continued_and_nested_lines(self):
        self.package({"group.go": (
            "package sample\n\nvar (\n\ttotal = 1 +\n\t\tint64(2)\n\tpair = struct {\n"
            "\t\tbyte int\n\t}{}\n)\n")})
        units = MOVE.parse_file(self.root / "pkg" / "group.go").units
        self.assertEqual(MOVE.declared_names(units[0]), ["total", "pair"])


class ApplyTests(MoveTestCase):
    def apply(self, files):
        manifest = write_manifest(self.root, files)
        MOVE.apply_manifest(manifest, self.root)
        return manifest

    def test_apply_moves_declarations_with_comments_and_computes_imports_from_use(self):
        package = self.package()
        manifest = self.apply({"big.go": BIG_KEEP, "render.go": ["func:render"]})
        moved = (package / "render.go").read_text(encoding="utf-8")
        kept = (package / "big.go").read_text(encoding="utf-8")
        self.assertIn("// --- section header ---\n\n// render formats", moved)
        self.assertIn('\t"fmt"\n\t"strings"\n\n\tsample "example.com/lib/go-sample"\n', moved)
        self.assertNotIn('"os"', moved)
        self.assertIn('"os"', kept)
        self.assertNotIn('"strings"', kept)
        self.assertNotIn("go-sample", kept)
        self.assertNotIn("func render(", kept)
        self.assertEqual(MOVE.check_manifest(manifest, self.root), [])
        self.assert_gofmt_clean(package)

    def test_apply_merges_into_an_existing_file_and_keeps_its_header(self):
        package = self.package({"big.go": BIG, "small.go": (
            "// Package sample is synthetic.\npackage sample\n\nimport \"fmt\"\n\n"
            "func hello() { fmt.Println(\"hi\") }\n")})
        keep = [key for key in BIG_KEEP if key != "func:exitCode"]
        self.apply({"big.go": keep + ["func:render"], "small.go": ["func:hello", "func:exitCode"]})
        small = (package / "small.go").read_text(encoding="utf-8")
        self.assertTrue(small.startswith("// Package sample is synthetic.\npackage sample\n"))
        self.assertIn('"fmt"', small)
        self.assertIn('"os"', small)
        self.assertLess(small.index("func hello("), small.index("func exitCode("))

    def test_apply_is_idempotent(self):
        package = self.package()
        manifest = self.apply({"big.go": BIG_KEEP, "render.go": ["func:render"]})
        before = {p.name: p.read_text(encoding="utf-8") for p in package.glob("*.go")}
        self.assertEqual(MOVE.apply_manifest(manifest, self.root), [])
        after = {p.name: p.read_text(encoding="utf-8") for p in package.glob("*.go")}
        self.assertEqual(before, after)

    def test_apply_refuses_an_invalid_manifest_and_writes_nothing(self):
        package = self.package()
        manifest = write_manifest(self.root, {"big.go": BIG_KEEP, "init.go": ["func:render"]})
        data = json.loads(manifest.read_text(encoding="utf-8"))
        data["files"]["big.go"].remove("func:init#1")
        data["files"]["init.go"].append("func:init#1")
        manifest.write_text(json.dumps(data), encoding="utf-8")
        with self.assertRaises(MOVE.MoveError):
            MOVE.apply_manifest(manifest, self.root)
        self.assertFalse((package / "init.go").exists())
        self.assertEqual((package / "big.go").read_text(encoding="utf-8"), BIG)

    def assert_gofmt_clean(self, package):
        result = subprocess.run([MOVE.gofmt_binary(), "-l", str(package)], capture_output=True,
                                text=True, check=True, timeout=60)
        self.assertEqual(result.stdout, "")


class CheckTests(MoveTestCase):
    def check(self, files):
        return MOVE.check_manifest(write_manifest(self.root, files), self.root)

    def test_check_reports_declarations_not_yet_in_their_target(self):
        self.package()
        errors = self.check({"big.go": BIG_KEEP, "render.go": ["func:render"]})
        self.assertEqual(errors, ["func:render is not in render.go"])

    def test_check_rejects_unlisted_unknown_and_duplicate_declarations(self):
        self.package()
        keep = [key for key in BIG_KEEP if key != "func:exitCode"]
        errors = self.check({"big.go": keep + ["func:render", "func:missing"],
                             "other.go": ["func:render"]})
        self.assertIn("listed more than once: func:render", errors)
        self.assertIn("func:missing resolves to 0 declarations", errors)
        self.assertIn("unlisted declaration: func:exitCode", errors)

    def test_check_rejects_moving_init_or_a_var_with_a_call_initializer(self):
        self.package()
        keep = [key for key in BIG_KEEP if key not in ("func:init#1", "var:started")]
        errors = self.check({"big.go": keep + ["func:render"],
                             "pinned.go": ["func:init#1", "var:started"]})
        self.assertIn("func:init#1 must stay in big.go (init or call initializer)", errors)
        self.assertIn("var:started must stay in big.go (init or call initializer)", errors)

    def test_check_allows_moving_a_var_with_conversions_only(self):
        self.package()
        keep = [key for key in BIG_KEEP if key != "var:table"]
        errors = self.check({"big.go": keep + ["func:render"], "table.go": ["var:table"]})
        self.assertEqual(errors, ["var:table is not in table.go"])

    def test_main_check_exit_status(self):
        self.package()
        manifest = write_manifest(self.root, {"big.go": BIG_KEEP + ["func:render"]})
        status, out = quiet(MOVE.main, ["--root", str(self.root), "--manifest", str(manifest),
                                        "--check"])
        self.assertEqual((status, out.strip()), (0, "manifest matches the tree"))


class CompareTests(MoveTestCase):
    def test_compare_is_equal_after_a_move_and_ignores_imports_and_tests(self):
        before = self.root / "before"
        shutil.copytree(self.package(), before)
        (before / "x_test.go").write_text("package sample\n\nfunc helper() {}\n", encoding="utf-8")
        write_package(self.root, {})
        MOVE.apply_manifest(write_manifest(self.root, {"big.go": BIG_KEEP,
                                                       "render.go": ["func:render"]}), self.root)
        self.assertEqual(MOVE.compare_dirs(before, self.root / "pkg"), [])

    def test_compare_detects_a_changed_declaration(self):
        before = self.root / "before"
        package = self.package()
        shutil.copytree(package, before)
        text = (package / "big.go").read_text(encoding="utf-8")
        (package / "big.go").write_text(text.replace("c.n++", "c.n += 2"), encoding="utf-8")
        problems = MOVE.compare_dirs(before, package)
        self.assertEqual(len(problems), 2)
        self.assertTrue(problems[0].startswith("missing after (1): // Add increments"))
        self.assertTrue(problems[1].startswith("added after (1): // Add increments"))

    def test_compare_detects_a_changed_comment(self):
        before = self.root / "before"
        package = self.package()
        shutil.copytree(package, before)
        text = (package / "big.go").read_text(encoding="utf-8")
        (package / "big.go").write_text(text.replace("counts things", "counts"), encoding="utf-8")
        self.assertEqual(len(MOVE.compare_dirs(before, package)), 2)


    def test_compare_detects_a_changed_literal_whitespace(self):
        before = self.root / "before"
        package = self.package({"lit.go": 'package sample\n\nvar a = "x  y"\n\nvar b = `p  q`\n'})
        shutil.copytree(package, before)
        text = (package / "lit.go").read_text(encoding="utf-8")
        (package / "lit.go").write_text(text.replace("x  y", "x y"), encoding="utf-8")
        self.assertEqual(len(MOVE.compare_dirs(before, package)), 2)
        (package / "lit.go").write_text(text.replace("var a =", "var a  ="), encoding="utf-8")
        self.assertEqual(MOVE.compare_dirs(before, package), [])

    def test_compare_detects_whitespace_inside_a_compiler_directive(self):
        before = self.root / "before"
        body = 'package sample\n\nimport "embed"\n\n//go:embed "a  b"\nvar files embed.FS\n'
        package = self.package({"embed.go": body})
        shutil.copytree(package, before)
        (package / "embed.go").write_text(body.replace('"a  b"', '"a b"'), encoding="utf-8")
        self.assertEqual(len(MOVE.compare_dirs(before, package)), 2)
        self.assertNotEqual(MOVE.normalize("//go:embed a  b\nvar f embed.FS"),
                            MOVE.normalize("//go:embed a b\nvar f embed.FS"))
        self.assertEqual(MOVE.normalize("// plain  note\nvar f int"),
                         MOVE.normalize("// plain note\nvar f int"))


class NewlineCompareTests(MoveTestCase):
    def test_compare_detects_a_statement_separating_newline(self):
        before = self.root / "before"
        body = "package sample\n\nfunc f() (n int) {\n\treturn\n\tg()\n}\n"
        package = self.package({"nl.go": body})
        shutil.copytree(package, before)
        (package / "nl.go").write_text(body.replace("return\n\tg()", "return g()"),
                                       encoding="utf-8")
        self.assertEqual(len(MOVE.compare_dirs(before, package)), 2)

    def test_compare_detects_a_statement_separating_newline_after_a_comment(self):
        before = self.root / "before"
        body = "package sample\n\nfunc f() (n int) {\n\treturn /* early */\n\tg()\n}\n"
        package = self.package({"nl.go": body})
        shutil.copytree(package, before)
        (package / "nl.go").write_text(body.replace("/* early */\n\tg()", "/* early */ g()"),
                                       encoding="utf-8")
        self.assertEqual(len(MOVE.compare_dirs(before, package)), 2)

    def test_normalize_treats_comments_as_go_does_for_semicolons(self):
        self.assertNotEqual(MOVE.normalize("return /* a\nb */ g()"),
                            MOVE.normalize("return /* a b */ g()"))
        self.assertNotEqual(MOVE.normalize("return // c\ng()"), MOVE.normalize("return g()"))
        self.assertNotEqual(MOVE.normalize("i++\ng()"), MOVE.normalize("i++ g()"))
        self.assertEqual(MOVE.normalize("x := a + /* c */\n\tb"),
                         MOVE.normalize("x := a + /* c */ b"))
        self.assertEqual(MOVE.normalize("// one\n//  two\nfunc f() {}"),
                         MOVE.normalize("// one\n// two\nfunc f() {}"))


class ReportTests(MoveTestCase):
    def test_report_counts_lines_functions_and_long_functions(self):
        body = "".join("\t_ = %d\n" % i for i in range(MOVE.MAX_FUNCTION_LINES))
        package = self.package({"long.go": "package sample\n\nfunc long() {\n%s}\n\nfunc short() {}\n" % body,
                                "long_test.go": "package sample\n"})
        status, out = quiet(MOVE.report, [package / "long.go", package / "long_test.go"])
        self.assertEqual(status, 0)
        lines = out.splitlines()
        self.assertEqual(lines[0], "%s lines=%d functions=2" % (package / "long.go",
                                                                MOVE.MAX_FUNCTION_LINES + 6))
        self.assertEqual(lines[1], "  long function func:long lines=%d" % (MOVE.MAX_FUNCTION_LINES + 2))
        self.assertTrue(lines[2].endswith("lines=1 functions=0 (test)"))

    def test_report_fails_a_production_file_over_budget_but_not_a_test_file(self):
        functions = "".join("func f%d() {}\n\n" % i for i in range(MOVE.MAX_FILE_FUNCTIONS + 1))
        package = self.package({"many.go": "package sample\n\n" + functions,
                                "many_test.go": "package sample\n\n" + functions})
        status, out = quiet(MOVE.report, [package / "many_test.go"])
        self.assertEqual(status, 0)
        status, out = quiet(MOVE.report, [package / "many.go"])
        self.assertEqual(status, 1)
        self.assertIn("OVER-BUDGET", out)


class CommittedManifestTests(unittest.TestCase):
    def test_committed_manifest_is_consistent(self):
        data = json.loads(COMMITTED_MANIFEST.read_text(encoding="utf-8"))
        self.assertIn(data["source"], data["files"])
        keys = [key for listed in data["files"].values() for key in listed]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual(len(data["files"]), 17)
        self.assertTrue(all(name.endswith(".go") and not name.endswith("_test.go")
                            for name in data["files"]))


if __name__ == "__main__":
    unittest.main()
