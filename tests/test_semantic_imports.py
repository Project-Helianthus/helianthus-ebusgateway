"""Synthetic import mutations exercise syntax, source placement and graph edges."""

import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("semantic_imports", ROOT / "scripts/check_semantic_imports.py")
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)
NATIVE = "github.com/Project-Helianthus/helianthus-ebusgo/protocol"


class SemanticImportsTest(unittest.TestCase):
    def setUp(self):
        scratch = ROOT / ".tmp"
        scratch.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        for owner in guard.PROTECTED:
            self.write(f"{owner}/base.go", "package neutral\n")

    def write(self, path, text):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)

    def errors(self):
        return guard.violations(guard.parse_sources(self.root))

    def test_direct_import_syntax_and_each_owner(self):
        for owner in guard.PROTECTED:
            for alias in ("", "renamed ", "_ ", ". "):
                with self.subTest(owner=owner, alias=alias):
                    self.write(f"{owner}/base.go", f'package neutral\nimport {alias}"{NATIVE}"\n')
                    errors = "\n".join(self.errors())
                    self.assertIn(f"{guard.MODULE}/{owner}", errors)
                    self.assertIn(NATIVE, errors)
                    self.write(f"{owner}/base.go", "package neutral\n")

    def test_all_native_families_and_gateway_implementation(self):
        for native in guard.FORBIDDEN_PREFIXES:
            with self.subTest(native=native):
                self.write("internal/semhub/base.go", f'package neutral\nimport "{native}/example"\n')
                self.assertIn("forbidden native dependency", "\n".join(self.errors()))

    def test_transitive_helper_and_cycle(self):
        helper = f"{guard.MODULE}/internal/helper"
        self.write("internal/semhub/base.go", f'package neutral\nimport "{helper}"\n')
        self.write("internal/helper/helper.go", f'package helper\nimport ("{NATIVE}"; "{guard.MODULE}/internal/semhub")\n')
        errors = "\n".join(self.errors())
        self.assertIn(f"{guard.MODULE}/internal/semhub -> {helper} -> {NATIVE}", errors)
        self.assertIn("dependency requires reviewed rule", errors)

    def test_transitive_allowed_semantic_owner(self):
        self.write("internal/semhub/base.go", f'package neutral\nimport "{guard.MODULE}/internal/semanticview"\n')
        self.write("internal/semanticview/base.go", f'package neutral\nimport "{NATIVE}"\n')
        self.assertIn(f"internal/semhub -> {guard.MODULE}/internal/semanticview -> {NATIVE}", "\n".join(self.errors()))

    def test_platform_build_tags_and_package_names_do_not_exempt_production(self):
        for filename, package, header in (
            ("native_windows.go", "neutral", ""),
            ("native_linux_arm64.go", "neutral", "//go:build linux && arm64\n\n"),
            ("native.go", "neutral_test", "//go:build custom_boundary_test\n\n"),
            ("testdata/native.go", "fixture_test", "//go:build ignore\n\n"),
        ):
            with self.subTest(filename=filename, package=package, header=header):
                self.write(f"internal/semhub/{filename}", f'{header}package {package}\nimport "{NATIVE}"\n')
                self.assertIn(NATIVE, "\n".join(self.errors()))
                (self.root / "internal/semhub" / filename).unlink()

    def test_test_imports_and_transitive_test_dependency(self):
        helper = f"{guard.MODULE}/internal/helper"
        for package in ("neutral", "neutral_test"):
            with self.subTest(package=package):
                self.write("internal/semhub/native_test.go", f'package {package}\nimport "{helper}"\n')
                self.write("internal/helper/base.go", f'package helper\nimport "{NATIVE}"\n')
                self.assertIn(f"{helper} -> {NATIVE}", "\n".join(self.errors()))
        self.write("internal/semhub/native_test.go", f'package neutral_test\nimport "{NATIVE}"\n')
        self.assertIn(NATIVE, "\n".join(self.errors()))

    def test_allowed_semreg_standard_and_semantic_owners(self):
        imports = ["fmt", "syscall/js", *guard.ALLOWED_LOCAL,
                   guard.ALLOWED_EXTERNAL[0] + "/semreg/v1"]
        self.write("internal/semhub/base.go", 'package neutral\nimport (\n' +
                   "\n".join(f'"{imp}"' for imp in imports) + '\n)\n')
        self.assertEqual([], self.errors())

    def test_semreg_replacements_fail_closed(self):
        semreg = guard.ALLOWED_EXTERNAL[0]
        self.write("internal/semhub/base.go", f'package neutral\nimport "{semreg}/semreg/v1"\n')
        self.write("replacement/native.go", f'package replacement\nimport "{NATIVE}"\n')
        for directive in (
            f"replace {semreg} => ./replacement",
            f"replace (\n {semreg} v0.0.0 => ./replacement\n)",
            f'replace "{semreg}" => "./replacement"',
            f"replace {semreg} => example.org/replacement v0.0.0",
            f"replace {semreg}/submodule => ./replacement",
        ):
            with self.subTest(directive=directive):
                self.write("go.mod", f"module {guard.MODULE}\n\n{directive}\n")
                with self.assertRaisesRegex(ValueError, "SemReg replacement is not allowed"):
                    self.errors()
        result = subprocess.run([sys.executable, str(ROOT / "scripts/check_semantic_imports.py"),
                                 "--root", str(self.root)], capture_output=True, text=True, timeout=160)
        self.assertEqual(1, result.returncode)
        self.assertIn("SemReg replacement is not allowed", result.stderr)

    def test_unrelated_module_replacement_is_allowed(self):
        self.write("go.mod", f"module {guard.MODULE}\n\nreplace example.org/helper => ./helper\n")
        self.assertEqual([], self.errors())

    def test_semreg_workspace_replacements_fail_closed(self):
        semreg = guard.ALLOWED_EXTERNAL[0]
        self.write("go.mod", f"module {guard.MODULE}\n")
        self.write("internal/semhub/base.go", f'package neutral\nimport "{semreg}/semreg/v1"\n')
        self.write("replacement/native.go", f'package replacement\nimport "{NATIVE}"\n')
        for directive in (
            f"replace {semreg} => ./replacement",
            f"replace (\n {semreg} v0.0.0 => ./replacement\n)",
            f'replace "{semreg}" => "./replacement"',
            f"replace {semreg} => example.org/replacement v0.0.0",
            f"replace {semreg}/submodule => ./replacement",
        ):
            with self.subTest(directive=directive):
                self.write("go.work", f"go 1.25.0\nuse .\n{directive}\n")
                with self.assertRaisesRegex(ValueError, "SemReg replacement is not allowed"):
                    self.errors()
        result = subprocess.run([sys.executable, str(ROOT / "scripts/check_semantic_imports.py"),
                                 "--root", str(self.root)], capture_output=True, text=True, timeout=160)
        self.assertEqual(1, result.returncode)
        self.assertIn("SemReg replacement is not allowed", result.stderr)

    def test_unrelated_workspace_replacement_is_allowed(self):
        self.write("go.mod", f"module {guard.MODULE}\n")
        self.write("go.work", "go 1.25.0\nuse .\nreplace example.org/helper => ./helper\n")
        self.assertEqual([], self.errors())

    def test_malformed_workspace_fails_closed(self):
        self.write("go.work", "replace (\n")
        with self.assertRaises(subprocess.CalledProcessError):
            self.errors()

    def test_vendored_semreg_fails_closed(self):
        semreg = guard.ALLOWED_EXTERNAL[0]
        self.write("internal/semhub/base.go", f'package neutral\nimport "{semreg}/semreg/v1"\n')
        self.write(f"vendor/{semreg}/semreg/v1/base.go",
                   f'package neutral\nimport "{NATIVE}"\n')
        with self.assertRaisesRegex(ValueError, "vendored SemReg is not allowed"):
            self.errors()
        result = subprocess.run([sys.executable, str(ROOT / "scripts/check_semantic_imports.py"),
                                 "--root", str(self.root)], capture_output=True, text=True, timeout=160)
        self.assertEqual(1, result.returncode)
        self.assertIn("vendored SemReg is not allowed", result.stderr)

    def test_symlinked_vendored_semreg_fails_closed(self):
        semreg = guard.ALLOWED_EXTERNAL[0]
        self.write("replacement/base.go", "package neutral\n")
        target = self.root / "vendor" / semreg
        target.parent.mkdir(parents=True)
        target.symlink_to(self.root / "replacement", target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "vendored SemReg is not allowed"):
            self.errors()

    def test_unrelated_vendored_package_is_allowed(self):
        self.write("vendor/example.org/helper/base.go", f'package helper\nimport "{NATIVE}"\n')
        self.assertEqual([], self.errors())

    def test_exact_test_helper_rule_checks_its_production_sources(self):
        helper = next(iter(guard.ALLOWED_TEST_LOCAL))
        self.write("internal/semanticoperations/semoptest/base.go", "package fixture_test\n")
        self.write("internal/semhub/helper_test.go", f'package neutral_test\nimport "{helper}"\n')
        self.assertEqual([], self.errors())
        self.write("internal/semanticoperations/semoptest/base.go", f'package fixture_test\nimport "{NATIVE}"\n')
        self.assertIn(NATIVE, "\n".join(self.errors()))
        self.write("internal/semanticoperations/semoptest/base.go", "package fixture_test\n")
        self.write("internal/semhub/helper.go", f'package neutral_test\nimport "{helper}"\n')
        self.assertIn("dependency requires reviewed rule", "\n".join(self.errors()))

    def test_comments_and_strings_are_not_imports(self):
        self.write("internal/semhub/base.go", 'package neutral\n' +
                   f'// import "{NATIVE}"\n/* import "{NATIVE}" */\n' +
                   f'const example = `import "{NATIVE}"`\n')
        self.assertEqual([], self.errors())

    def test_unknown_external_and_prefix_impostor_fail_closed(self):
        for imp in ("example.org/neutral", "fictionalstdlib", "C",
                    guard.ALLOWED_EXTERNAL[0] + "-native/semreg/v1",
                    f"{guard.MODULE}/internal/semanticviewextra"):
            with self.subTest(imp=imp):
                self.write("internal/semhub/base.go", f'package neutral\nimport "{imp}"\n')
                self.assertIn("dependency requires reviewed rule", "\n".join(self.errors()))

    def test_missing_owner_and_unresolved_local_fail_closed(self):
        (self.root / "internal/semanticview/base.go").unlink()
        self.write("internal/semanticview/only_test.go", "package neutral_test\n")
        self.write("internal/semhub/base.go", f'package neutral\nimport "{guard.MODULE}/internal/semanticview"\n')
        errors = "\n".join(self.errors())
        self.assertIn("missing protected package", errors)
        self.assertIn("unresolved local dependency", errors)

    def test_malformed_source_fails_parser(self):
        self.write("internal/semhub/broken.go", "package neutral\nimport (\n")
        with self.assertRaises(subprocess.CalledProcessError):
            self.errors()

    def test_cli_planted_import_has_nonzero_exit_and_both_packages(self):
        self.write("internal/semhub/base.go", f'package neutral\nimport "{NATIVE}"\n')
        result = subprocess.run([sys.executable, str(ROOT / "scripts/check_semantic_imports.py"),
                                 "--root", str(self.root)], capture_output=True, text=True, timeout=160)
        self.assertEqual(1, result.returncode)
        self.assertIn(f"{guard.MODULE}/internal/semhub", result.stderr)
        self.assertIn(NATIVE, result.stderr)


if __name__ == "__main__":
    unittest.main()
