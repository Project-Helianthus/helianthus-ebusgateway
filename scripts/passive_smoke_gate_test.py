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
CONFIG_CLASSIFIER = REPO_ROOT / "scripts" / "semreg_public_config_classifier.py"


class PassiveSmokeGateTests(unittest.TestCase):

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
	producer := cfg.ModbusTCPConfig.GrowattBMSRS485
	if producer.Enabled && !cfg.M2MGraphQL.Disabled() {
		if !growattStorageOperationFitsDeadline(producer.MaxQuiescence, producer.ResponseTimeout, 10*time.Second, 750*time.Millisecond) {
			return errors.New("growatt storage GraphQL requires MaxQuiescence plus four reads plus 250ms processing and 500ms response headroom below the M2M server deadline")
		}
	}
	if !cfg.PortalStorage.SemanticEnabled {
		return nil
	}
	if !producer.Enabled || producer.AssetID != cfg.PortalStorage.AssetRef {
		return errors.New("portal storage semantic BFF requires the enabled matching Growatt BMS RS-485 producer")
	}
	if !growattStorageOperationFitsDeadline(producer.MaxQuiescence, producer.ResponseTimeout, 5*time.Second, 500*time.Millisecond) {
		return errors.New("portal storage semantic BFF requires MaxQuiescence plus four Growatt reads plus 500ms headroom below the M2M deadline")
	}
	return nil
}

// growattStorageOperationFitsDeadline checks the entire public RTU operation:
// recovery can consume one MaxQuiescence interval before its four serial reads.
// The subtraction/division formulation avoids overflowing time.Duration while
// preserving the strict response-deadline boundary.
func growattStorageOperationFitsDeadline(maxQuiescence, responseTimeout, deadline, headroom time.Duration) bool {
	if maxQuiescence < 0 || responseTimeout <= 0 || deadline <= headroom {
		return false
	}
	budget := deadline - headroom
	if maxQuiescence >= budget {
		return false
	}
	return responseTimeout <= (budget-maxQuiescence-time.Nanosecond)/4
}

type Config struct {
	PortalStorage            PortalStorageConfig
}
'''
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

    def test_storage_config_allowlist_is_exact(self) -> None:
        repo_path, _ = self._create_temp_repo("config.go", base_text="type Config struct {\n}\n", modified_text=self.storage_config_only)
        allowed = subprocess.run(["bash", "scripts/passive_smoke_gate.sh"], cwd=repo_path, env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"), text=True, capture_output=True, check=False)
        self.assertEqual(allowed.returncode, 0, msg=allowed.stdout + allowed.stderr)
        self.assertIn("not triggered", allowed.stdout)

        (repo_path / "config.go").write_text(self.storage_config_only + "HTTPAddr string\n", encoding="utf-8")
        hostile = subprocess.run(["bash", "scripts/passive_smoke_gate.sh"], cwd=repo_path, env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"), text=True, capture_output=True, check=False)
        self.assertNotEqual(hostile.returncode, 0)
        self.assertIn("PASSIVE_SMOKE_REPORT is required", hostile.stdout)

        for line in ("return err\n", "return nil\n", "}\n"):
            (repo_path / "config.go").write_text(self.storage_config_only + line, encoding="utf-8")
            hostile = subprocess.run(["bash", "scripts/passive_smoke_gate.sh"], cwd=repo_path, env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"), text=True, capture_output=True, check=False)
            self.assertNotEqual(hostile.returncode, 0, line)

        hostile_validators = (
            self.storage_config_only.replace(
                "if !producer.Enabled || producer.AssetID != cfg.PortalStorage.AssetRef {\n",
                "if producer.AssetID != cfg.PortalStorage.AssetRef {\n",
            ),
            self.storage_config_only.replace(
			"growattStorageOperationFitsDeadline(producer.MaxQuiescence, producer.ResponseTimeout, 5*time.Second, 500*time.Millisecond)",
			"growattStorageOperationFitsDeadline(0, producer.ResponseTimeout, 5*time.Second, 500*time.Millisecond)",
            ),
			self.storage_config_only.replace("maxQuiescence >= budget", "maxQuiescence > budget"),
        )
        for modified in hostile_validators:
            (repo_path / "config.go").write_text(modified, encoding="utf-8")
            hostile = subprocess.run(
                ["bash", "scripts/passive_smoke_gate.sh"], cwd=repo_path,
                env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"),
                text=True, capture_output=True, check=False,
            )
            self.assertNotEqual(hostile.returncode, 0)
            self.assertIn("PASSIVE_SMOKE_REPORT is required", hostile.stdout)

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
                ["bash", "scripts/passive_smoke_gate.sh"],
                cwd=wrong_hunk_repo,
                env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"),
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertNotEqual(hostile.returncode, 0, modified_other)
            self.assertIn("PASSIVE_SMOKE_REPORT is required", hostile.stdout)

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
        shutil.copy2(CONFIG_CLASSIFIER, repo_path / "scripts" / "semreg_public_config_classifier.py")

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

    def test_bus_observability_semreg_append_is_only_exception(self) -> None:
        base = '''type BusObservabilityStore struct {
}
func (store *BusObservabilityStore) RenderPrometheus() {
}
'''
        allowed = '''type BusObservabilityStore struct {
    // semanticMetricsProvider returns detached, already-evaluated SemReg views.
    // It is invoked after the store snapshot is released: a /metrics request must
    // never take the store lock across a driver or publication lock.
    semanticMetricsProvider func(time.Time) []SemanticMetricsDomain
}
// SetSemanticMetricsProvider installs the read-only PV/Storage SemReg view
// supplier used by the existing /metrics renderer. The supplier must neither
// acquire native data nor publish; nil removes the optional semantic section.
func (store *BusObservabilityStore) SetSemanticMetricsProvider(provider func(time.Time) []SemanticMetricsDomain) {
    if store == nil {
        return
    }
    store.mu.Lock()
    store.semanticMetricsProvider = provider
    store.mu.Unlock()
}
func (store *BusObservabilityStore) RenderPrometheus() {
    semanticMetricsProvider := store.semanticMetricsProvider
    var semanticDomains []SemanticMetricsDomain
    haveSemanticMetricsProvider := semanticMetricsProvider != nil
    if semanticMetricsProvider != nil {
        semanticDomains = semanticMetricsProvider(now)
    }
    if haveSemanticMetricsProvider {
        writeSemanticMetrics(writer, semanticDomains, now)
    }
}
'''
        repo_path, _ = self._create_temp_repo("bus_observability_store.go", base_text=base, modified_text=allowed)
        result = subprocess.run(["bash", "scripts/passive_smoke_gate.sh"], cwd=repo_path, env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"), text=True, capture_output=True, check=False)
        self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)
        (repo_path / "bus_observability_store.go").write_text(allowed + "func (store *BusObservabilityStore) MutatePassive() { store.passive.state = \"unsafe\" }\n", encoding="utf-8")
        result = subprocess.run(["bash", "scripts/passive_smoke_gate.sh"], cwd=repo_path, env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"), text=True, capture_output=True, check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("PASSIVE_SMOKE_REPORT is required", result.stdout)

        for hostile in (
            'writer.writeGaugeSample("ebus_unreviewed", 1, nil)\n',
            'store.mu.RLock()\n',
            'store.passive.state = "unsafe"\n',
            'semanticMetricsProvider = func(time.Time) []SemanticMetricsDomain { return nil }\n',
        ):
            (repo_path / "bus_observability_store.go").write_text(allowed + hostile, encoding="utf-8")
            result = subprocess.run(["bash", "scripts/passive_smoke_gate.sh"], cwd=repo_path, env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"), text=True, capture_output=True, check=False)
            self.assertNotEqual(result.returncode, 0, hostile)

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

    def test_config_only_exemption_rejects_nonstructural_brace_line(self) -> None:
        repo_path, _ = self._create_temp_repo(
            "config.go",
            base_text="package gateway\n",
            modified_text="package gateway\nfunc openM2MGraphQLTransport() { openSerial() }\n",
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
