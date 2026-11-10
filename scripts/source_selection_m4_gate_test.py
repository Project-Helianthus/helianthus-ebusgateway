#!/usr/bin/env python3
import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest


SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
GATE_SCRIPT = REPO_ROOT / "scripts" / "source_selection_m4_gate.py"


class SourceSelectionM4GateTests(unittest.TestCase):
    def _create_temp_repo(self, filename: str, text: str) -> pathlib.Path:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        repo_path = pathlib.Path(temp_dir.name)

        subprocess.run(["git", "init", "-b", "main"], cwd=repo_path, check=True, capture_output=True, text=True)
        subprocess.run(["git", "config", "user.name", "Codex"], cwd=repo_path, check=True, capture_output=True, text=True)
        subprocess.run(
            ["git", "config", "user.email", "codex@example.com"],
            cwd=repo_path,
            check=True,
            capture_output=True,
            text=True,
        )

        scripts_dir = repo_path / "scripts"
        scripts_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(GATE_SCRIPT, scripts_dir / "source_selection_m4_gate.py")

        target = repo_path / filename
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")

        subprocess.run(["git", "add", "."], cwd=repo_path, check=True, capture_output=True, text=True)
        subprocess.run(["git", "commit", "-m", "base"], cwd=repo_path, check=True, capture_output=True, text=True)
        return repo_path

    def _run_gate(self, repo_path: pathlib.Path) -> subprocess.CompletedProcess[str]:
        env = dict(os.environ)
        env.pop("PYTHONPATH", None)
        return subprocess.run(
            ["python3", "scripts/source_selection_m4_gate.py"],
            cwd=repo_path,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_allows_retained_status_mirror_aliases(self) -> None:
        text = (
            "# retained status mirrors\n"
            "GraphQL still exposes daemonStatus.initiatorAddress and daemon_status.initiator_address.\n"
        )
        result = self._run_gate(self._create_temp_repo("docs/status.md", text))
        self.assertEqual(result.returncode, 0, msg=result.stderr)

    def test_retains_exact_source_filename_values_in_naming_inventories(self) -> None:
        filename = "join" + "bus.go"
        for path in ("naming-inventory.json", "naming-consumers.json"):
            for text in (f'{{"owning_file": "{filename}"}}', f'["{filename}"]'):
                with self.subTest(path=path, text=text):
                    result = self._run_gate(self._create_temp_repo(f"docs/architecture/{path}", text))
                    self.assertEqual(result.returncode, 0, msg=result.stderr)
                    self.assertIn("retained", result.stdout)

    def test_rejects_other_legacy_uses_in_naming_inventories(self) -> None:
        term = "join" + "bus"
        for path in ("naming-inventory.json", "naming-consumers.json"):
            for text in (
                f'{{"owning_file": "{term}"}}',
                f'{{"owning_file": "src/{term}.go"}}',
                f'{{"{term}.go": "file"}}',
                f'{{"owning_file": "{term}.go", "description": "{term}"}}',
                f'{{"description": "quoted \\"{term}.go\\""}}',
                '{"owning_file": "' + term + '.go", "description": "Join' + 'Bus"}',
            ):
                with self.subTest(path=path, text=text):
                    result = self._run_gate(self._create_temp_repo(f"docs/architecture/{path}", text))
                    self.assertNotEqual(result.returncode, 0)

    def test_rejects_source_filename_value_outside_naming_inventories(self) -> None:
        filename = "join" + "bus.go"
        result = self._run_gate(self._create_temp_repo("docs/other.json", f'["{filename}"]'))
        self.assertNotEqual(result.returncode, 0)

    def test_blocks_legacy_admission_public_surface(self) -> None:
        legacy = "admission" + "_path_selected"
        result = self._run_gate(self._create_temp_repo("docs/status.md", f"Old field: {legacy}\n"))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(legacy, result.stderr)

    def test_blocks_legacy_go_string_literal(self) -> None:
        legacy = "startup" + "_admission_state"
        text = f'package main\n\nvar _ = "{legacy}"\n'
        result = self._run_gate(self._create_temp_repo("x.go", text))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(legacy, result.stderr)

    def test_blocks_legacy_go_multiline_raw_string_literal(self) -> None:
        legacy_alias = "bus" + "Admission"
        text = (
            "package main\n\n"
            "var _ = `query {\n"
            "  busSummary { status {\n"
            f"    {legacy_alias} {{ state }}\n"
            "  } }\n"
            "}`\n"
        )
        result = self._run_gate(self._create_temp_repo("x.go", text))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(legacy_alias, result.stderr)

    def test_blocks_unjustified_source_addr_flag_spelling(self) -> None:
        legacy = "source" + "-addr"
        result = self._run_gate(self._create_temp_repo("docs/status.md", f"Old flag: {legacy}\n"))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(legacy, result.stderr)

    def test_ignores_tracked_file_deleted_from_worktree(self) -> None:
        repo = self._create_temp_repo("deleted.go", "package deleted\n")
        (repo / "deleted.go").unlink()
        result = self._run_gate(repo)
        self.assertEqual(result.returncode, 0, msg=result.stderr)


if __name__ == "__main__":
    unittest.main()
