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
CONFIG_CLASSIFIER = REPO_ROOT / "scripts" / "semreg_public_config_classifier.py"


class TransportGateTests(unittest.TestCase):

    storage_config_only = '''// PortalStorageConfig is an independently disabled, read-only BFF for the
// versioned SemReg storage projection. It deliberately does not expose any
// operation or native fallback fields.
type PortalStorageConfig = PortalPVConfig

func (cfg Config) ValidatePortalStorage() error {
if cfg.PortalStorage.RawReadEnabled {
return errors.New("portal storage configuration does not permit raw reads")
}
copy := cfg
copy.PortalPV = cfg.PortalStorage
if err := copy.ValidatePortalPV(); err != nil {
return err
}
if !cfg.PortalStorage.SemanticEnabled {
return nil
}
producer := cfg.ModbusTCPConfig.GrowattBMSRS485
if !producer.Enabled || producer.AssetID != cfg.PortalStorage.AssetRef {
return errors.New("portal storage semantic BFF requires the enabled matching Growatt BMS RS-485 producer")
}
return nil
}

type Config struct {
	PortalStorage            PortalStorageConfig
}
'''
    def _script_env(self, **extra: str) -> dict[str, str]:
        env = dict(os.environ)
        for key in (
            "TRANSPORT_GATE_OWNER_OVERRIDE",
            "TRANSPORT_GATE_OWNER_REASON",
            "TRANSPORT_MATRIX_REPORT",
            "PASSIVE_SMOKE_REPORT",
        ):
            env.pop(key, None)
        env.update(extra)
        return env

    def test_storage_config_allowlist_is_exact(self) -> None:
        repo_path, _ = self._create_temp_repo("config.go", base_text="type Config struct {\n}\n", modified_text=self.storage_config_only)
        allowed = subprocess.run(["bash", "scripts/transport_gate.sh"], cwd=repo_path, env=self._script_env(TRANSPORT_GATE_BASE_REF="HEAD"), text=True, capture_output=True, check=False)
        self.assertEqual(allowed.returncode, 0, msg=allowed.stdout + allowed.stderr)
        self.assertIn("not triggered", allowed.stdout)

        (repo_path / "config.go").write_text(self.storage_config_only + "HTTPAddr string\n", encoding="utf-8")
        hostile = subprocess.run(["bash", "scripts/transport_gate.sh"], cwd=repo_path, env=self._script_env(TRANSPORT_GATE_BASE_REF="HEAD"), text=True, capture_output=True, check=False)
        self.assertNotEqual(hostile.returncode, 0)
        self.assertIn("TRANSPORT_MATRIX_REPORT is required", hostile.stdout)

        for line in ("return err\n", "return nil\n", "}\n"):
            (repo_path / "config.go").write_text(self.storage_config_only + line, encoding="utf-8")
            hostile = subprocess.run(["bash", "scripts/transport_gate.sh"], cwd=repo_path, env=self._script_env(TRANSPORT_GATE_BASE_REF="HEAD"), text=True, capture_output=True, check=False)
            self.assertNotEqual(hostile.returncode, 0, line)

        base_other = "type Config struct {\n}\n\nfunc validateOther() error {\n\tif changed {\n\t\treturn err\n\t}\n\treturn nil\n}\n"
        hostile_others = (
            "func validateOther() error {\n\tif changed {\n\t\treturn nil\n\t}\n\treturn err\n}\n",
            "func validateOther() error {\n\treturn err\n}\n",
            "func validateOther() error {\n\tif changed {\n\t}\n\treturn nil\n}\n",
        )
        for modified_other in hostile_others:
            wrong_hunk_repo, _ = self._create_temp_repo(
                "config.go",
                base_text=base_other,
                modified_text=self.storage_config_only + "\n" + modified_other,
            )
            hostile = subprocess.run(
                ["bash", "scripts/transport_gate.sh"],
                cwd=wrong_hunk_repo,
                env=self._script_env(TRANSPORT_GATE_BASE_REF="HEAD"),
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertNotEqual(hostile.returncode, 0, modified_other)
            self.assertIn("TRANSPORT_MATRIX_REPORT is required", hostile.stdout)

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
        shutil.copy2(TRANSPORT_GATE_SCRIPT, repo_path / "scripts" / "transport_gate.sh")
        shutil.copy2(CONFIG_CLASSIFIER, repo_path / "scripts" / "semreg_public_config_classifier.py")

        tracked_file = repo_path / changed_file
        tracked_file.parent.mkdir(parents=True, exist_ok=True)
        tracked_file.write_text(base_text, encoding="utf-8")

        subprocess.run(["git", "add", "."], cwd=repo_path, check=True, capture_output=True, text=True)
        subprocess.run(
            ["git", "commit", "-m", "base"],
            cwd=repo_path,
            check=True,
            capture_output=True,
            text=True,
        )

        tracked_file.write_text(modified_text, encoding="utf-8")

        report_path = repo_path / "report.json"
        report_path.write_text(
            json.dumps({"cases": [{"case_id": f"T{i:02d}", "outcome": "pass"} for i in range(1, 89)]}),
            encoding="utf-8",
        )
        return repo_path, report_path

    def _fake_go_env(self, repo_path: pathlib.Path, exit_code: int, missing_test: str = "") -> dict[str, str]:
        bin_dir = repo_path / "fake-bin"
        bin_dir.mkdir()
        fake_go = bin_dir / "go"
        fake_go.write_text(
            "#!/usr/bin/env bash\n"
            "if [[ \"$1\" == \"list\" ]]; then\n"
            "  echo /pinned/helianthus-modbus\n"
            "elif [[ \"$1\" == \"test\" && \" $* \" == *\" -list \"* ]]; then\n"
            "  while IFS= read -r test_name; do\n"
            f"    [[ \"$test_name\" == \"{missing_test}\" ]] || echo \"$test_name\"\n"
            "  done <<'TESTS'\n"
            "TestRTUProductionReadRetainsImmutableCorrelatedEvidence\n"
            "TestRTUProductionExceptionDoesNotFenceButShortWriteDoes\n"
            "TestRTUProductionRejectsUnadmittedReadBeforeWrite\n"
            "TestRTUProductionRecoveryWaitsForRetiringReadOwnership\n"
            "TestRTUProductionFourSequentialReadsRemainBounded\n"
            "TestRTUProductionCancellationFencesAndPartialFramesRetainEvidence\n"
            "TestRTUProductionMalformedAndCRCFramesRemainTerminalEvidence\n"
            "TestRTUProductionRejectsTimingAndRecoveryBoundMismatch\n"
            "TestRTUProductionRecoveryDiscardsDelayedOldGenerationFrame\n"
            "TestRTUProductionRejectsRecoveryBoundsAndNoByteTimeout\n"
            "TESTS\n"
            "fi\n"
            f"exit {exit_code}\n",
            encoding="utf-8",
        )
        fake_go.chmod(0o755)
        return self._script_env(
            TRANSPORT_GATE_BASE_REF="HEAD",
            PATH=f"{bin_dir}:{os.environ['PATH']}",
        )

    def test_transport_gate_triggers_for_cmd_gateway_main(self) -> None:
        repo_path, report_path = self._create_temp_repo(
            "cmd/gateway/main.go",
            base_text="package main\n\nfunc main() {\n\ttransportProtocol := \"ens\"\n\t_ = transportProtocol\n}\n",
            modified_text="package main\n\nfunc main() {\n\ttransportProtocol := \"udp-plain\"\n\t_ = transportProtocol\n}\n",
        )
        env = self._fake_go_env(repo_path, 0)
        env["TRANSPORT_MATRIX_REPORT"] = str(report_path)
        result = subprocess.run(
            ["bash", "scripts/transport_gate.sh"],
            cwd=repo_path,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("transport gate: PASS", result.stdout)
        self.assertIn("Modbus RTU production conformance", result.stdout)

    def test_transport_gate_main_fails_closed_for_rtu_conformance(self) -> None:
        repo_path, report_path = self._create_temp_repo(
            "cmd/gateway/main.go",
            base_text="package main\n\nfunc main() {\n\ttransportProtocol := \"ens\"\n\t_ = transportProtocol\n}\n",
            modified_text="package main\n\nfunc main() {\n\ttransportProtocol := \"udp-plain\"\n\t_ = transportProtocol\n}\n",
        )
        env = self._fake_go_env(repo_path, 1)
        env["TRANSPORT_MATRIX_REPORT"] = str(report_path)
        result = subprocess.run(
            ["bash", "scripts/transport_gate.sh"], cwd=repo_path, env=env,
            text=True, capture_output=True, check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Modbus RTU gateway composition evidence failed", result.stdout)

    def test_modbus_rtu_only_owner_override_clears_gate(self) -> None:
        repo_path, _ = self._create_temp_repo("cmd/gateway/growatt_bms_rs485_runtime.go")
        result = subprocess.run(
            ["bash", "scripts/transport_gate.sh"],
            cwd=repo_path,
            env=self._script_env(
                TRANSPORT_GATE_BASE_REF="HEAD",
                TRANSPORT_GATE_OWNER_OVERRIDE="OVERRIDE_TRANSPORT_GATE_BY_OWNER",
                TRANSPORT_GATE_OWNER_REASON="test RTU scope and residual risk",
            ),
            text=True, capture_output=True, check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("owner override active (test RTU scope and residual risk)", result.stdout)

    def test_combined_main_owner_override_clears_both_gates(self) -> None:
        repo_path, _ = self._create_temp_repo(
            "cmd/gateway/main.go",
            base_text="package main\n\nfunc main() {\n\ttransportProtocol := \"ens\"\n\t_ = transportProtocol\n}\n",
            modified_text="package main\n\nfunc main() {\n\ttransportProtocol := \"udp-plain\"\n\t_ = transportProtocol\n}\n",
        )
        result = subprocess.run(
            ["bash", "scripts/transport_gate.sh"],
            cwd=repo_path,
            env=self._script_env(
                TRANSPORT_GATE_BASE_REF="HEAD",
                TRANSPORT_GATE_OWNER_OVERRIDE="OVERRIDE_TRANSPORT_GATE_BY_OWNER",
                TRANSPORT_GATE_OWNER_REASON="test combined scope and residual risk",
            ),
            text=True, capture_output=True, check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("owner override active (test combined scope and residual risk)", result.stdout)

    def test_modbus_rtu_owner_override_requires_reason(self) -> None:
        repo_path, _ = self._create_temp_repo("cmd/gateway/growatt_bms_rs485_runtime.go")
        result = subprocess.run(
            ["bash", "scripts/transport_gate.sh"],
            cwd=repo_path,
            env=self._script_env(
                TRANSPORT_GATE_BASE_REF="HEAD",
                TRANSPORT_GATE_OWNER_OVERRIDE="OVERRIDE_TRANSPORT_GATE_BY_OWNER",
            ),
            text=True, capture_output=True, check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("override requires TRANSPORT_GATE_OWNER_REASON", result.stdout)

    def test_config_change_triggers_ebus_and_modbus_rtu_gates(self) -> None:
        repo_path, report_path = self._create_temp_repo(
            "config.go", "package gateway\n", "package gateway\nvar growattRTUEnabled = true\n"
        )
        env = self._fake_go_env(repo_path, 0)
        env["TRANSPORT_MATRIX_REPORT"] = str(report_path)
        result = subprocess.run(
            ["bash", "scripts/transport_gate.sh"], cwd=repo_path, env=env,
            text=True, capture_output=True, check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("transport gate: PASS (pass=88", result.stdout)
        self.assertIn("Modbus RTU production conformance", result.stdout)

    def test_config_change_fails_closed_when_modbus_rtu_conformance_fails(self) -> None:
        repo_path, report_path = self._create_temp_repo(
            "config.go", "package gateway\n", "package gateway\nvar growattRTUEnabled = true\n"
        )
        env = self._fake_go_env(repo_path, 1)
        env["TRANSPORT_MATRIX_REPORT"] = str(report_path)
        result = subprocess.run(
            ["bash", "scripts/transport_gate.sh"], cwd=repo_path, env=env,
            text=True, capture_output=True, check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Modbus RTU gateway composition evidence failed", result.stdout)

    def test_transport_gate_fails_for_cmd_gateway_main_without_report(self) -> None:
        repo_path, _ = self._create_temp_repo(
            "cmd/gateway/main.go",
            base_text="package main\n\nfunc main() {\n\ttransportProtocol := \"ens\"\n\t_ = transportProtocol\n}\n",
            modified_text="package main\n\nfunc main() {\n\ttransportProtocol := \"udp-plain\"\n\t_ = transportProtocol\n}\n",
        )
        result = subprocess.run(
            ["bash", "scripts/transport_gate.sh"],
            cwd=repo_path,
            env=self._script_env(TRANSPORT_GATE_BASE_REF="HEAD"),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("TRANSPORT_MATRIX_REPORT is required", result.stdout)

    def test_modbus_rtu_gate_triggers_for_composition_inputs(self) -> None:
        go_mod_base = (
            "module test\n\n"
            "go 1.22\n\n"
            "require (\n"
            "  github.com/Project-Helianthus/helianthus-modbus v0.3.0\n"
            "  github.com/Project-Helianthus/helianthus-modbusreg v0.6.7\n"
            ")\n"
        )
        cases = (
            ("modbus_config.go", "// base\n", "// modified\n"),
            ("cmd/gateway/gateway_cli.go", "// base\n", "// modified\n"),
            ("cmd/gateway/gateway_http_server.go", "// base\n", "// modified\n"),
            ("cmd/gateway/growatt_bms_rs485_runtime.go", "// base\n", "// modified\n"),
			("cmd/gateway/growatt_storage_semreg.go", "// base\n", "// modified\n"),
			("cmd/gateway/m2m_graphql_runtime.go", "// base\n", "// modified\n"),
            ("cmd/gateway/gateway_run_lifecycle.go", "// base\n", "// modified\n"),
            ("cmd/gateway/modbus_endpoint_file.go", "// base\n", "// modified\n"),
            ("cmd/gateway/modbus_mcp_provider.go", "// base\n", "// modified\n"),
            ("mcp/growatt_bms_rs485_v202.go", "// base\n", "// modified\n"),
            ("mcp/growatt_bms_rs485_v202_runtime.go", "// base\n", "// modified\n"),
            ("mcp/modbus_v1.go", "// base\n", "// modified\n"),
            ("go.mod", go_mod_base, go_mod_base.replace("v0.3.0", "v0.3.1")),
            ("go.mod", go_mod_base, go_mod_base.replace("v0.6.7", "v0.6.8")),
        )
        for changed_file, base_text, modified_text in cases:
            with self.subTest(changed_file=changed_file):
                repo_path, _ = self._create_temp_repo(changed_file, base_text, modified_text)
                result = subprocess.run(
                    ["bash", "scripts/transport_gate.sh"],
                    cwd=repo_path,
                    env=self._fake_go_env(repo_path, 0),
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, msg=result.stderr)
                self.assertIn("Modbus RTU production conformance", result.stdout)
                self.assertIn("Modbus RTU production composition and pinned endpoint conformance", result.stdout)

    def test_modbus_rtu_gate_fails_closed_for_lifecycle_and_dependency_changes(self) -> None:
        go_mod_base = (
            "module test\n\n"
            "go 1.22\n\n"
            "require (\n"
            "  github.com/Project-Helianthus/helianthus-modbus v0.3.0\n"
            "  github.com/Project-Helianthus/helianthus-modbusreg v0.6.7\n"
            ")\n"
        )
        cases = (
            ("modbus_config.go", "// base\n", "// modified\n"),
            ("cmd/gateway/gateway_cli.go", "// base\n", "// modified\n"),
            ("cmd/gateway/gateway_http_server.go", "// base\n", "// modified\n"),
            ("cmd/gateway/growatt_bms_rs485_runtime.go", "// base\n", "// modified\n"),
			("cmd/gateway/growatt_storage_semreg.go", "// base\n", "// modified\n"),
			("cmd/gateway/m2m_graphql_runtime.go", "// base\n", "// modified\n"),
            ("cmd/gateway/gateway_run_lifecycle.go", "// base\n", "// modified\n"),
            ("cmd/gateway/modbus_endpoint_file.go", "// base\n", "// modified\n"),
            ("cmd/gateway/modbus_mcp_provider.go", "// base\n", "// modified\n"),
            ("mcp/growatt_bms_rs485_v202.go", "// base\n", "// modified\n"),
            ("mcp/growatt_bms_rs485_v202_runtime.go", "// base\n", "// modified\n"),
            ("mcp/modbus_v1.go", "// base\n", "// modified\n"),
            ("go.mod", go_mod_base, go_mod_base.replace("v0.3.0", "v0.3.1")),
            ("go.mod", go_mod_base, go_mod_base.replace("v0.6.7", "v0.6.8")),
        )
        for changed_file, base_text, modified_text in cases:
            with self.subTest(changed_file=changed_file):
                repo_path, _ = self._create_temp_repo(changed_file, base_text, modified_text)
                result = subprocess.run(
                    ["bash", "scripts/transport_gate.sh"],
                    cwd=repo_path,
                    env=self._fake_go_env(repo_path, 1),
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Modbus RTU gateway composition evidence failed", result.stdout)

    def test_modbus_rtu_gate_fails_closed_for_missing_pinned_endpoint_test(self) -> None:
        repo_path, _ = self._create_temp_repo("cmd/gateway/growatt_bms_rs485_runtime.go")
        result = subprocess.run(
            ["bash", "scripts/transport_gate.sh"],
            cwd=repo_path,
            env=self._fake_go_env(repo_path, 0, "TestRTUProductionFourSequentialReadsRemainBounded"),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("pinned Modbus RTU endpoint inventory missing or duplicates", result.stdout)

    def test_modbus_rtu_gate_skips_unrelated_go_mod_change(self) -> None:
        base_text = "module test\n\ngo 1.22\n\nrequire example.com/other v1.0.0\n"
        repo_path, _ = self._create_temp_repo("go.mod", base_text, base_text.replace("v1.0.0", "v1.0.1"))
        result = subprocess.run(
            ["bash", "scripts/transport_gate.sh"],
            cwd=repo_path,
            env=self._script_env(TRANSPORT_GATE_BASE_REF="HEAD"),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("transport gate: not triggered.", result.stdout)

    def test_transport_gate_fails_when_matrix_contains_xpass(self) -> None:
        repo_path, report_path = self._create_temp_repo(
            "cmd/gateway/main.go",
            base_text="package main\n\nfunc main() {\n\ttransportProtocol := \"ens\"\n\t_ = transportProtocol\n}\n",
            modified_text="package main\n\nfunc main() {\n\ttransportProtocol := \"udp-plain\"\n\t_ = transportProtocol\n}\n",
        )
        report = json.loads(report_path.read_text(encoding="utf-8"))
        report["cases"][0]["outcome"] = "xpass"
        report_path.write_text(json.dumps(report), encoding="utf-8")

        result = subprocess.run(
            ["bash", "scripts/transport_gate.sh"],
            cwd=repo_path,
            env=self._script_env(
                TRANSPORT_GATE_BASE_REF="HEAD",
                TRANSPORT_MATRIX_REPORT=str(report_path),
            ),
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("xpass", result.stdout)
        self.assertIn("T01", result.stdout)

    def test_transport_gate_fails_for_runtime_control_flow_main_diff(self) -> None:
        repo_path, _ = self._create_temp_repo(
            "cmd/gateway/main.go",
            base_text=(
                "package main\n\nfunc main() {\n"
                "\toverrideSet := admissionPath == TransportAdmissionSourceSelectionCapable && cfg.StartupSource.Source != nil\n"
                "\t_ = overrideSet\n}\n"
            ),
            modified_text="package main\n\nfunc main() {\n\toverrideSet := false\n\t_ = overrideSet\n}\n",
        )
        result = subprocess.run(
            ["bash", "scripts/transport_gate.sh"],
            cwd=repo_path,
            env=self._script_env(TRANSPORT_GATE_BASE_REF="HEAD"),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("TRANSPORT_MATRIX_REPORT is required", result.stdout)

    def test_transport_gate_fails_for_runtime_call_reorder_main_diff(self) -> None:
        repo_path, _ = self._create_temp_repo(
            "cmd/gateway/main.go",
            base_text=(
                "package main\n\nfunc main() {\n"
                "\tstartHTTPServer()\n"
                "\trunBackgroundFullScan()\n"
                "}\n\n"
                "func startHTTPServer() {}\n"
                "func runBackgroundFullScan() {}\n"
            ),
            modified_text=(
                "package main\n\nfunc main() {\n"
                "\trunBackgroundFullScan()\n"
                "\tstartHTTPServer()\n"
                "}\n\n"
                "func startHTTPServer() {}\n"
                "func runBackgroundFullScan() {}\n"
            ),
        )
        result = subprocess.run(
            ["bash", "scripts/transport_gate.sh"],
            cwd=repo_path,
            env=self._script_env(TRANSPORT_GATE_BASE_REF="HEAD"),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("TRANSPORT_MATRIX_REPORT is required", result.stdout)

    def test_transport_gate_skips_sas05_public_api_main_diff(self) -> None:
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
            ["bash", "scripts/transport_gate.sh"],
            cwd=repo_path,
            env=self._script_env(TRANSPORT_GATE_BASE_REF="HEAD"),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("transport gate: not triggered.", result.stdout)

    def test_transport_gate_skips_exact_m9_graphql_wiring_call(self) -> None:
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
            ["bash", "scripts/transport_gate.sh"],
            cwd=repo_path,
            env=self._script_env(TRANSPORT_GATE_BASE_REF="HEAD"),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("transport gate: not triggered.", result.stdout)

    def test_transport_gate_rejects_nearby_m9_runtime_call(self) -> None:
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
            ["bash", "scripts/transport_gate.sh"],
            cwd=repo_path,
            env=self._script_env(TRANSPORT_GATE_BASE_REF="HEAD"),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("TRANSPORT_MATRIX_REPORT is required", result.stdout)

    def test_transport_gate_skips_non_transport_gateway_observability_file(self) -> None:
        repo_path, _ = self._create_temp_repo("cmd/gateway/bus_observability_provider.go")
        result = subprocess.run(
            ["bash", "scripts/transport_gate.sh"],
            cwd=repo_path,
            env=self._script_env(TRANSPORT_GATE_BASE_REF="HEAD"),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("transport gate: not triggered.", result.stdout)

    def test_transport_gate_skips_test_only_adaptermux_change(self) -> None:
        repo_path, _ = self._create_temp_repo("internal/adaptermux/e2e_test.go")
        result = subprocess.run(
            ["bash", "scripts/transport_gate.sh"],
            cwd=repo_path,
            env=self._script_env(TRANSPORT_GATE_BASE_REF="HEAD"),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("transport gate: not triggered.", result.stdout)

    def test_modbus_rtu_gate_ignores_test_only_adaptermux_change(self) -> None:
        repo_path, _ = self._create_temp_repo("cmd/gateway/growatt_bms_rs485_runtime.go")
        adapter_test = repo_path / "internal" / "adaptermux" / "connection_health_test.go"
        adapter_test.parent.mkdir(parents=True)
        adapter_test.write_text("package adaptermux\n", encoding="utf-8")
        subprocess.run(["git", "add", str(adapter_test.relative_to(repo_path))], cwd=repo_path, check=True, capture_output=True, text=True)
        subprocess.run(["git", "commit", "-m", "adapter test base"], cwd=repo_path, check=True, capture_output=True, text=True)
        adapter_test.write_text("package adaptermux\n// deterministic test only\n", encoding="utf-8")
        result = subprocess.run(
            ["bash", "scripts/transport_gate.sh"],
            cwd=repo_path,
            env=self._fake_go_env(repo_path, 0),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("Modbus RTU production conformance", result.stdout)
        self.assertNotIn("AD01..AD12", result.stdout)

    def test_config_only_exemption_rejects_nonstructural_brace_line(self) -> None:
        repo_path, _ = self._create_temp_repo(
            "config.go",
            base_text="package gateway\n",
            modified_text="package gateway\nfunc openM2MGraphQLTransport() { openSerial() }\n",
        )
        result = subprocess.run(
            ["bash", "scripts/transport_gate.sh"],
            cwd=repo_path,
            env=self._script_env(TRANSPORT_GATE_BASE_REF="HEAD"),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("TRANSPORT_MATRIX_REPORT is required", result.stdout)


if __name__ == "__main__":
    unittest.main()
