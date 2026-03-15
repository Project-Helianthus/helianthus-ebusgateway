#!/usr/bin/env python3
import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest


SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
TRANSPORT_GATE_SCRIPT = REPO_ROOT / "scripts" / "transport_gate.sh"


class TransportGateTests(unittest.TestCase):
    def _create_temp_repo(self, changed_file: str) -> tuple[pathlib.Path, pathlib.Path]:
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

        (repo_path / "scripts").mkdir(parents=True, exist_ok=True)
        shutil.copy2(TRANSPORT_GATE_SCRIPT, repo_path / "scripts" / "transport_gate.sh")

        tracked_file = repo_path / changed_file
        tracked_file.parent.mkdir(parents=True, exist_ok=True)
        tracked_file.write_text("// base\n", encoding="utf-8")

        subprocess.run(["git", "add", "."], cwd=repo_path, check=True, capture_output=True, text=True)
        subprocess.run(
            ["git", "commit", "-m", "base"],
            cwd=repo_path,
            check=True,
            capture_output=True,
            text=True,
        )

        tracked_file.write_text("// modified\n", encoding="utf-8")

        report_path = repo_path / "report.json"
        report_path.write_text(
            json.dumps({"cases": [{"case_id": f"T{i:02d}", "outcome": "pass"} for i in range(1, 89)]}),
            encoding="utf-8",
        )
        return repo_path, report_path

    def test_transport_gate_triggers_for_cmd_gateway_main(self) -> None:
        repo_path, report_path = self._create_temp_repo("cmd/gateway/main.go")
        result = subprocess.run(
            ["bash", "scripts/transport_gate.sh"],
            cwd=repo_path,
            env={
                **dict(os.environ),
                "TRANSPORT_GATE_BASE_REF": "HEAD",
                "TRANSPORT_MATRIX_REPORT": str(report_path),
            },
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("transport gate: PASS", result.stdout)

    def test_transport_gate_skips_non_transport_gateway_observability_file(self) -> None:
        repo_path, _ = self._create_temp_repo("cmd/gateway/bus_observability_provider.go")
        result = subprocess.run(
            ["bash", "scripts/transport_gate.sh"],
            cwd=repo_path,
            env={**dict(os.environ), "TRANSPORT_GATE_BASE_REF": "HEAD"},
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("transport gate: not triggered.", result.stdout)


if __name__ == "__main__":
    unittest.main()
