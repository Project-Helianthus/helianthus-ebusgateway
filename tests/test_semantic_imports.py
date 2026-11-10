"""Synthetic import mutations exercise syntax, source placement and graph edges."""

import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

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

    def test_platform_syscalls_allowed_only_for_semanticmetrics(self):
        self.write("internal/semanticmetrics/base.go",
                   'package neutral\nimport "golang.org/x/sys/unix"\n')
        self.assertEqual([], self.errors())

    def test_platform_syscalls_rejected_for_other_semantic_packages(self):
        for owner in (*guard.PROTECTED, "internal/semanticmetrics/helper"):
            if owner == "internal/semanticmetrics":
                continue
            with self.subTest(owner=owner):
                self.write(f"{owner}/base.go",
                           'package neutral\nimport "golang.org/x/sys/unix"\n')
                errors = "\n".join(self.errors())
                self.assertIn("dependency requires reviewed rule", errors)
                self.assertIn(f"{guard.MODULE}/{owner} -> golang.org/x/sys/unix", errors)
                self.write(f"{owner}/base.go", "package neutral\n")

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

    def test_buildable_semreg_overrides_fail_closed(self):
        semreg = guard.ALLOWED_EXTERNAL[0]
        ancestor = semreg.rsplit("/", 1)[0]
        native_module = NATIVE.rsplit("/", 1)[0]
        cases_root = self.root
        for mode, supplied in (
            ("module-replace", ancestor),
            ("root-replace", semreg),
            ("root-use", semreg),
            ("ancestor-replace", semreg),
            ("ancestor-use", semreg),
            ("root-replace", ancestor),
            ("ancestor-replace", ancestor),
            ("root-use", ancestor),
            ("ancestor-use", ancestor),
        ):
            with self.subTest(mode=mode, supplied=supplied):
                # Each case discovers only its own root or ancestor workspace.
                self.root = cases_root / f"{mode}-{supplied.rsplit('/', 1)[-1]}"
                clone = self.workspace_clone()
                self.write("native/go.mod", f"module {native_module}\ngo 1.22.0\n")
                self.write("native/protocol/base.go", "package protocol\n")
                self.write("gateway/internal/semhub/base.go",
                           f'package neutral\nimport _ "{semreg}/semreg/v1"\n')
                relative = semreg.removeprefix(supplied).lstrip("/")
                candidate = "candidate"
                self.write(f"{candidate}/go.mod", f"module {supplied}\ngo 1.22.0\n")
                self.write(f"{candidate}/{relative}/semreg/v1/base.go",
                           f'package neutral\nimport _ "{NATIVE}"\n')
                module = (f"module {guard.MODULE}\ngo 1.22.0\n"
                          f"require {supplied} v0.0.0\n")
                if mode == "module-replace":
                    module += (f"require {native_module} v0.0.0\n"
                               f"replace {supplied} => ../{candidate}\n"
                               f"replace {native_module} => ../native\n")
                    selected = "off"
                else:
                    workspace = clone / "go.work" if mode.startswith("root-") else self.root / "go.work"
                    gateway_path = "." if mode.startswith("root-") else "./gateway"
                    prefix = "../" if mode.startswith("root-") else "./"
                    work = f"go 1.22.0\nuse (\n {gateway_path}\n {prefix}native\n"
                    if mode.endswith("use"):
                        work += f" {prefix}{candidate}\n)\n"
                    else:
                        work += f")\nreplace {supplied} => {prefix}{candidate}\n"
                    workspace.write_text(work)
                    selected = ""  # Exercise Go's root/ancestor workspace discovery.
                self.write("gateway/go.mod", module)
                env = dict(os.environ, GOWORK=selected, GOPROXY="off", GOSUMDB="off",
                           GOTOOLCHAIN="local", GOFLAGS="-buildvcs=false",
                           GOCACHE=str(ROOT / ".tmp/gocache"))
                built = subprocess.run(
                    ["go", "build", "./internal/semhub/..."], cwd=clone, env=env,
                    capture_output=True, text=True, timeout=150,
                )
                self.assertEqual(0, built.returncode, built.stderr)
                listed = subprocess.run(
                    ["go", "list", "-deps", "./internal/semhub/..."], cwd=clone, env=env,
                    capture_output=True, text=True, timeout=30,
                )
                self.assertEqual(0, listed.returncode, listed.stderr)
                self.assertIn(NATIVE, listed.stdout.splitlines())
                with mock.patch.dict(os.environ, GOWORK=selected):
                    with self.assertRaisesRegex(ValueError, "SemReg .* is not allowed"):
                        guard.parse_sources(clone)

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

    def test_semreg_workspace_use_modules_fail_closed(self):
        semreg = guard.ALLOWED_EXTERNAL[0]
        self.write("go.mod", f"module {guard.MODULE}\n")
        self.write("internal/semhub/base.go", f'package neutral\nimport "{semreg}/semreg/v1"\n')
        self.write("workspace-semreg/semreg/v1/base.go",
                   f'package neutral\nimport "{NATIVE}"\n')
        for module in (semreg, semreg + "/submodule"):
            for path in ("./workspace-semreg", str(self.root / "workspace-semreg")):
                with self.subTest(module=module, path=path):
                    self.write("workspace-semreg/go.mod", f"module {module}\n")
                    self.write("go.work", f'go 1.25.0\nuse (\n .\n "{path}"\n)\n')
                    with self.assertRaisesRegex(ValueError, "SemReg workspace module is not allowed"):
                        self.errors()
        result = subprocess.run([sys.executable, str(ROOT / "scripts/check_semantic_imports.py"),
                                 "--root", str(self.root)], capture_output=True, text=True, timeout=160)
        self.assertEqual(1, result.returncode)
        self.assertIn("SemReg workspace module is not allowed", result.stderr)

    def test_unrelated_workspace_use_module_is_allowed(self):
        self.write("go.mod", f"module {guard.MODULE}\n")
        self.write("workspace-helper/go.mod", "module example.org/helper\n")
        self.write("go.work", "go 1.25.0\nuse (\n .\n ./workspace-helper\n)\n")
        self.assertEqual([], self.errors())

    def test_workspace_use_module_replacement_fails_closed(self):
        semreg = guard.ALLOWED_EXTERNAL[0]
        self.write("go.mod", f"module {guard.MODULE}\n")
        self.write("workspace-helper/go.mod",
                   f"module example.org/helper\nreplace {semreg} => ../replacement\n")
        self.write("go.work", "go 1.25.0\nuse (\n .\n ./workspace-helper\n)\n")
        with self.assertRaisesRegex(ValueError, "SemReg replacement is not allowed"):
            self.errors()

    def test_workspace_use_missing_or_malformed_module_fails_closed(self):
        self.write("go.work", "go 1.25.0\nuse ./workspace-helper\n")
        for content in (None, "module (\n"):
            with self.subTest(content=content):
                if content is not None:
                    self.write("workspace-helper/go.mod", content)
                with self.assertRaises(subprocess.CalledProcessError):
                    self.errors()

    def test_malformed_workspace_fails_closed(self):
        self.write("go.work", "replace (\n")
        with self.assertRaises(subprocess.CalledProcessError):
            self.errors()

    def workspace_clone(self):
        self.write("gateway/go.mod", f"module {guard.MODULE}\n")
        for owner in guard.PROTECTED:
            self.write(f"gateway/{owner}/base.go", "package neutral\n")
        return self.root / "gateway"

    def test_selected_workspace_semreg_use_fails_closed(self):
        clone = self.workspace_clone()
        semreg = guard.ALLOWED_EXTERNAL[0]
        self.write("local-semreg/go.mod", f"module {semreg}\n")
        self.write("local-semreg/base.go", f'package neutral\nimport "{NATIVE}"\n')
        for selection in ("", str(self.root / "external.work")):
            with self.subTest(selection=selection):
                filename = "external.work" if selection else "go.work"
                self.write(filename, "go 1.25.0\nuse (\n ./gateway\n ./local-semreg\n)\n")
                with mock.patch.dict(os.environ, GOWORK=selection):
                    with self.assertRaisesRegex(ValueError, "SemReg workspace module is not allowed"):
                        guard.parse_sources(clone)

    def test_ancestor_workspace_semreg_replacement_fails_closed(self):
        clone = self.workspace_clone()
        semreg = guard.ALLOWED_EXTERNAL[0]
        self.write("go.work", f"go 1.25.0\nuse ./gateway\nreplace {semreg} => ./replacement\n")
        with mock.patch.dict(os.environ, GOWORK=""):
            with self.assertRaisesRegex(ValueError, "SemReg replacement is not allowed"):
                guard.parse_sources(clone)

    def test_selected_workspace_use_module_replacement_fails_closed(self):
        clone = self.workspace_clone()
        semreg = guard.ALLOWED_EXTERNAL[0]
        self.write("helper/go.mod", f"module example.org/helper\nreplace {semreg} => ../replacement\n")
        self.write("go.work", "go 1.25.0\nuse (\n ./gateway\n ./helper\n)\n")
        with mock.patch.dict(os.environ, GOWORK=""):
            with self.assertRaisesRegex(ValueError, "SemReg replacement is not allowed"):
                guard.parse_sources(clone)

    def test_unrelated_ancestor_workspace_is_allowed(self):
        clone = self.workspace_clone()
        self.write("helper/go.mod", "module example.org/helper\n")
        self.write("go.work", "go 1.25.0\nuse (\n ./gateway\n ./helper\n)\n")
        with mock.patch.dict(os.environ, GOWORK=""):
            self.assertEqual([], guard.violations(guard.parse_sources(clone)))

    def test_disabled_ancestor_workspace_is_ignored(self):
        clone = self.workspace_clone()
        self.write("go.work", f"go 1.25.0\nuse ./gateway\nreplace {guard.ALLOWED_EXTERNAL[0]} => ./replacement\n")
        with mock.patch.dict(os.environ, GOWORK="off"):
            self.assertEqual([], guard.violations(guard.parse_sources(clone)))

    def test_missing_explicit_workspace_fails_closed(self):
        with mock.patch.dict(os.environ, GOWORK=str(self.root / "missing.work")):
            with self.assertRaises(subprocess.CalledProcessError):
                self.errors()

    def test_buildable_selected_workspace_vendor_fails_closed(self):
        clone = self.workspace_clone()
        semreg = guard.ALLOWED_EXTERNAL[0]
        native_module = NATIVE.rsplit("/", 1)[0]
        self.write("gateway/go.mod", f"module {guard.MODULE}\ngo 1.22.0\n"
                   f"require (\n {semreg} v0.0.0\n {native_module} v0.0.0\n)\n")
        self.write("gateway/internal/semhub/base.go",
                   f'package neutral\nimport _ "{semreg}/semreg/v1"\n')
        self.write(f"vendor/{semreg}/semreg/v1/base.go",
                   f'package neutral\nimport _ "{NATIVE}"\n')
        self.write(f"vendor/{NATIVE}/base.go", "package protocol\n")
        self.write("vendor/modules.txt", "## workspace\n"
                   f"# {semreg} v0.0.0\n## explicit; go 1.22.0\n{semreg}/semreg/v1\n"
                   f"# {native_module} v0.0.0\n## explicit; go 1.22.0\n{NATIVE}\n")
        for selection in ("", str(self.root / "external.work")):
            with self.subTest(selection=selection):
                self.write("external.work" if selection else "go.work",
                           "go 1.22.0\nuse ./gateway\n")
                env = dict(os.environ, GOWORK=selection, GOPROXY="off", GOSUMDB="off",
                           GOTOOLCHAIN="local", GOFLAGS="-buildvcs=false",
                           GOCACHE=str(ROOT / ".tmp/gocache"))
                built = subprocess.run(
                    ["go", "build", "./internal/semhub/..."], cwd=clone, env=env,
                    capture_output=True, text=True, timeout=150,
                )
                self.assertEqual(0, built.returncode, built.stderr)
                with mock.patch.dict(os.environ, GOWORK=selection, GOFLAGS="-buildvcs=false"):
                    with self.assertRaisesRegex(ValueError, "vendored SemReg is not allowed"):
                        guard.parse_sources(clone)

    def test_disabled_workspace_vendor_is_ignored(self):
        clone = self.workspace_clone()
        self.write("go.work", "go 1.22.0\nuse ./gateway\n")
        self.write(f"vendor/{guard.ALLOWED_EXTERNAL[0]}/semreg/v1/base.go",
                   f'package neutral\nimport _ "{NATIVE}"\n')
        with mock.patch.dict(os.environ, GOWORK="off"):
            self.assertEqual([], guard.violations(guard.parse_sources(clone)))

    def test_buildable_alternate_module_file_fails_closed(self):
        semreg = guard.ALLOWED_EXTERNAL[0]
        native_module = NATIVE.rsplit("/", 1)[0]
        self.write("go.mod", f"module {guard.MODULE}\ngo 1.22.0\n")
        self.write("alternate.mod", f"module {guard.MODULE}\ngo 1.22.0\n"
                   f"require (\n {semreg} v0.0.0\n {native_module} v0.0.0\n)\n"
                   f"replace {semreg} => ./candidate\n"
                   f"replace {native_module} => ./native\n")
        self.write("candidate/go.mod", f"module {semreg}\ngo 1.22.0\n")
        self.write("candidate/semreg/v1/base.go", f'package neutral\nimport _ "{NATIVE}"\n')
        self.write("native/go.mod", f"module {native_module}\ngo 1.22.0\n")
        self.write("native/protocol/base.go", "package protocol\n")
        self.write("internal/semhub/base.go", f'package neutral\nimport _ "{semreg}/semreg/v1"\n')
        for modfile in ("alternate.mod", str(self.root / "alternate.mod")):
            with self.subTest(modfile=modfile):
                flags = f'-buildvcs=false "-modfile={modfile}"'
                env = dict(os.environ, GOWORK="off", GOPROXY="off", GOSUMDB="off",
                           GOTOOLCHAIN="local", GOFLAGS=flags,
                           GOCACHE=str(ROOT / ".tmp/gocache"))
                built = subprocess.run(
                    ["go", "build", "./internal/semhub/..."], cwd=self.root, env=env,
                    capture_output=True, text=True, timeout=150,
                )
                self.assertEqual(0, built.returncode, built.stderr)
                with mock.patch.dict(os.environ, GOWORK="off", GOFLAGS=flags):
                    with self.assertRaisesRegex(ValueError, "GOFLAGS -modfile is not allowed"):
                        self.errors()
                result = subprocess.run(
                    [sys.executable, str(ROOT / "scripts/check_semantic_imports.py"),
                     "--root", str(self.root)], env=env,
                    capture_output=True, text=True, timeout=160,
                )
                self.assertEqual(1, result.returncode)
                self.assertIn("GOFLAGS -modfile is not allowed", result.stderr)

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

    def test_directory_exclusions_are_repository_relative(self):
        for directory in ("node_modules", "vendor", ".tmp", ".git"):
            with self.subTest(directory=directory):
                filename = f"internal/semhub/{directory}/base.go"
                self.write(filename, f'package neutral\nimport _ "{NATIVE}"\n')
                self.assertIn(filename, "\n".join(self.errors()))
        for directory in (".git", ".tmp", "portal/web/node_modules", "vendor"):
            self.write(f"{directory}/broken.go", "not Go syntax\n")
        # Known asset locations remain excluded, but the protected paths above
        # must still be visited even when they have the same directory names.
        self.assertIn(NATIVE, "\n".join(self.errors()))

    def test_buildable_protected_node_modules_subpackage_is_checked(self):
        native_module = NATIVE.rsplit("/", 1)[0]
        self.write("go.mod", f"module {guard.MODULE}\ngo 1.22.0\n"
                   f"require {native_module} v0.0.0\n"
                   f"replace {native_module} => ./native\n")
        self.write("native/go.mod", f"module {native_module}\ngo 1.22.0\n")
        self.write("native/protocol/base.go", "package protocol\n")
        filename = "internal/semhub/node_modules/base.go"
        self.write(filename, f'package neutral\nimport _ "{NATIVE}"\n')
        env = dict(os.environ, GOWORK="off", GOPROXY="off", GOSUMDB="off",
                   GOTOOLCHAIN="local", GOFLAGS="-buildvcs=false",
                   GOCACHE=str(ROOT / ".tmp/gocache"))
        built = subprocess.run(
            ["go", "build", "./internal/semhub/..."], cwd=self.root, env=env,
            capture_output=True, text=True, timeout=150,
        )
        self.assertEqual(0, built.returncode, built.stderr)
        self.assertIn(filename, "\n".join(self.errors()))

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
