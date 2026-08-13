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
PASSIVE_SMOKE_GATE_SCRIPT = REPO_ROOT / "scripts" / "passive_smoke_gate.sh"


class PassiveSmokeGateTests(unittest.TestCase):
    def _script_env(self, **extra: str) -> dict[str, str]:
        env = dict(os.environ)
        for key in (
            "PASSIVE_SMOKE_GATE_OWNER_OVERRIDE",
            "PASSIVE_SMOKE_GATE_OWNER_REASON",
            "PASSIVE_SMOKE_REPORT",
            "TRANSPORT_MATRIX_REPORT",
        ):
            env.pop(key, None)
        env.update(extra)
        return env

    def _create_temp_repo(
        self,
        changed_file: str,
        base_text: str = "// base\n",
        modified_text: str = "// modified\n",
    ) -> tuple[pathlib.Path, pathlib.Path]:
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
        shutil.copy2(PASSIVE_SMOKE_GATE_SCRIPT, repo_path / "scripts" / "passive_smoke_gate.sh")

        tracked_file = repo_path / changed_file
        tracked_file.parent.mkdir(parents=True, exist_ok=True)
        tracked_file.write_text(base_text, encoding="utf-8")

        subprocess.run(["git", "add", "."], cwd=repo_path, check=True, capture_output=True, text=True)
        subprocess.run(["git", "commit", "-m", "base"], cwd=repo_path, check=True, capture_output=True, text=True)

        tracked_file.write_text(modified_text, encoding="utf-8")

        report_path = repo_path / "passive.json"
        report_path.write_text(
            json.dumps(
                {
                    "suite": "passive",
                    "cases": [
                        {"case_id": "P01", "passive_mode": "unsupported_or_misconfigured", "outcome": "pass"},
                        {"case_id": "P02", "passive_mode": "unsupported_or_misconfigured", "outcome": "pass"},
                        {"case_id": "P03", "passive_mode": "required", "outcome": "pass"},
                        {"case_id": "P04", "passive_mode": "required", "outcome": "pass"},
                        {"case_id": "P05", "passive_mode": "required", "outcome": "pass"},
                        {"case_id": "P06", "passive_mode": "unsupported_or_misconfigured", "outcome": "pass"},
                    ],
                }
            ),
            encoding="utf-8",
        )
        return repo_path, report_path

    def test_passive_smoke_gate_triggers_for_real_main_diff(self) -> None:
        repo_path, report_path = self._create_temp_repo(
            "cmd/gateway/main.go",
            base_text="package main\n\nfunc main() {\n\tpassiveMode := \"required\"\n\t_ = passiveMode\n}\n",
            modified_text="package main\n\nfunc main() {\n\tpassiveMode := \"unsupported_or_misconfigured\"\n\t_ = passiveMode\n}\n",
        )
        result = subprocess.run(
            ["bash", "scripts/passive_smoke_gate.sh"],
            cwd=repo_path,
            env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD", PASSIVE_SMOKE_REPORT=str(report_path)),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("passive smoke gate: PASS", result.stdout)

    def test_passive_smoke_gate_fails_without_report(self) -> None:
        repo_path, _ = self._create_temp_repo(
            "cmd/gateway/main.go",
            base_text="package main\n\nfunc main() {\n\tpassiveMode := \"required\"\n\t_ = passiveMode\n}\n",
            modified_text="package main\n\nfunc main() {\n\tpassiveMode := \"unsupported_or_misconfigured\"\n\t_ = passiveMode\n}\n",
        )
        result = subprocess.run(
            ["bash", "scripts/passive_smoke_gate.sh"],
            cwd=repo_path,
            env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("PASSIVE_SMOKE_REPORT is required", result.stdout)

    def test_passive_smoke_gate_fails_for_runtime_control_flow_main_diff(self) -> None:
        repo_path, _ = self._create_temp_repo(
            "cmd/gateway/main.go",
            base_text=(
                "package main\n\nfunc main() {\n"
                "\trunAdvisorySourceSelector := admissionPath == TransportAdmissionSourceSelectionCapable && shouldStartPassiveObserveFirst(cfg)\n"
                "\t_ = runAdvisorySourceSelector\n}\n"
            ),
            modified_text=(
                "package main\n\nfunc main() {\n"
                "\trunAdvisorySourceSelector := false\n"
                "\t_ = runAdvisorySourceSelector\n}\n"
            ),
        )
        result = subprocess.run(
            ["bash", "scripts/passive_smoke_gate.sh"],
            cwd=repo_path,
            env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("PASSIVE_SMOKE_REPORT is required", result.stdout)

    def test_passive_smoke_gate_fails_for_runtime_call_reorder_main_diff(self) -> None:
        repo_path, _ = self._create_temp_repo(
            "cmd/gateway/main.go",
            base_text=(
                "package main\n\nfunc main() {\n"
                "\tstartPassiveTransactionReconstructor()\n"
                "\trunBackgroundFullScan()\n"
                "}\n\n"
                "func startPassiveTransactionReconstructor() {}\n"
                "func runBackgroundFullScan() {}\n"
            ),
            modified_text=(
                "package main\n\nfunc main() {\n"
                "\trunBackgroundFullScan()\n"
                "\tstartPassiveTransactionReconstructor()\n"
                "}\n\n"
                "func startPassiveTransactionReconstructor() {}\n"
                "func runBackgroundFullScan() {}\n"
            ),
        )
        result = subprocess.run(
            ["bash", "scripts/passive_smoke_gate.sh"],
            cwd=repo_path,
            env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("PASSIVE_SMOKE_REPORT is required", result.stdout)

    def test_passive_smoke_gate_skips_sas05_public_api_main_diff(self) -> None:
        repo_path, _ = self._create_temp_repo(
            "cmd/gateway/main.go",
            base_text=(
                "package main\n\nfunc main() {\n"
                "\tlogLine := \"startup admission artifact emit\"\n"
                "\tmode := \"override\"\n"
                "\tpath := \"/tmp/helianthus-admission-artifact.json\"\n"
                "\t_ = logLine\n\t_ = mode\n\t_ = path\n}\n"
            ),
            modified_text=(
                "package main\n\nfunc main() {\n"
                "\tlogLine := \"startup source selection artifact emit\"\n"
                "\tmode := \"explicit_validate_only\"\n"
                "\tpath := \"/tmp/helianthus-source-selection-artifact.json\"\n"
                "\t_ = logLine\n\t_ = mode\n\t_ = path\n}\n"
            ),
        )
        result = subprocess.run(
            ["bash", "scripts/passive_smoke_gate.sh"],
            cwd=repo_path,
            env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("passive smoke gate: not triggered.", result.stdout)

    def test_passive_smoke_gate_skips_exact_m9_graphql_wiring_call(self) -> None:
        repo_path, _ = self._create_temp_repo(
            "cmd/gateway/main.go",
            base_text="package main\n\nfunc main() {\n\tsemanticRuntime.Start(ctx)\n}\n",
            modified_text=(
                "package main\n\nfunc main() {\n"
                "\tportalSemanticProvider := wireEEBusPromotedSemanticGraphQL(ctx, builder, semanticRuntime.Provider(), eebusAdapter)\n"
                "\tOperationModeChangeable:    cloneBoolPtr(zone.Config.OperationModeChangeable),\n"
                "\tportalSemanticProvider,\n"
                "\tsemanticRuntime.Start(ctx)\n}\n"
            ),
        )
        result = subprocess.run(
            ["bash", "scripts/passive_smoke_gate.sh"],
            cwd=repo_path,
            env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("passive smoke gate: not triggered.", result.stdout)

    def test_passive_smoke_gate_rejects_nearby_m9_runtime_call(self) -> None:
        repo_path, _ = self._create_temp_repo(
            "cmd/gateway/main.go",
            base_text="package main\n\nfunc main() {\n\tsemanticRuntime.Start(ctx)\n}\n",
            modified_text=(
                "package main\n\nfunc main() {\n"
                "\tportalSemanticProvider := wireEEBusPromotedSemanticGraphQL(ctx, builder, semanticRuntime.Provider(), transport)\n"
                "\tsemanticRuntime.Start(ctx)\n}\n"
            ),
        )
        result = subprocess.run(
            ["bash", "scripts/passive_smoke_gate.sh"],
            cwd=repo_path,
            env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("PASSIVE_SMOKE_REPORT is required", result.stdout)


if __name__ == "__main__":
    unittest.main()
