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
            "PASSIVE_REQUIRED_CELL_DIR",
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

    def test_config_comment_only_change_does_not_require_passive_report(self) -> None:
        base = "// M2MGraphQLConfig configures the dedicated public SemReg PV listener.\ntype Config struct{}\n"
        comments_only = "// M2MGraphQLConfig configures the dedicated public M2M API listener.\n// It also hosts the Matter binding feed.\ntype Config struct{}\n"
        repo_path, _ = self._create_temp_repo("config.go", base, comments_only)
        allowed = subprocess.run(
            ["bash", "scripts/passive_smoke_gate.sh"], cwd=repo_path,
            env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"),
            text=True, capture_output=True, check=False,
        )
        self.assertEqual(allowed.returncode, 0, msg=allowed.stdout + allowed.stderr)
        self.assertIn("not triggered", allowed.stdout)

        (repo_path / "config.go").write_text(comments_only + "PassiveMode bool\n", encoding="utf-8")
        hostile = subprocess.run(
            ["bash", "scripts/passive_smoke_gate.sh"], cwd=repo_path,
            env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"),
            text=True, capture_output=True, check=False,
        )
        self.assertNotEqual(hostile.returncode, 0)
        self.assertIn("PASSIVE_SMOKE_REPORT is required", hostile.stdout)

    def test_evse_prometheus_config_exemption_is_exact(self) -> None:
        base = "type Config struct {\n}\n"
        exact = base + "// PrometheusEVSEEnabled enables only the detached EVSE SemReg observation\n// section. It neither configures acquisition nor grants a native runtime.\nPrometheusEVSEEnabled bool\n"
        for name, modified, expected in (
            ("exact", exact, True),
            ("extra-passive-state", exact + "PassiveStateDirectApply bool\n", False),
            ("nearby-runtime", exact.replace("PrometheusEVSEEnabled bool", "PrometheusEVSEEnabled func()"), False),
        ):
            with self.subTest(name=name):
                repo_path, _ = self._create_temp_repo("config.go", base, modified)
                result = subprocess.run(
                    ["bash", "scripts/passive_smoke_gate.sh"], cwd=repo_path,
                    env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"),
                    text=True, capture_output=True, check=False,
                )
                self.assertEqual(result.returncode == 0, expected, msg=result.stdout + result.stderr)

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

    def _pre_candidate_bus_observability_base(self) -> str:
        # Immutable issue fixture pinned to the pre-candidate source; never
        # derive this classifier baseline from moving branch history.
        fixture = SCRIPT_DIR / "testdata" / "passive_smoke_bus_observability_store_base.go"
        try:
            return fixture.read_text(encoding="utf-8")
        except OSError as exc:
            self.fail(f"cannot read immutable pre-candidate fixture: {exc}")

    def _transport_snapshot_candidate(self) -> str:
        # Freeze both sides of the transport-only exception. Reading the live
        # store here would fold unrelated future edits into the allowed case.
        candidate = self._pre_candidate_bus_observability_base()
        fixture = SCRIPT_DIR / "testdata" / "passive_smoke_transport_snapshot_edits.json"
        for edit in json.loads(fixture.read_text(encoding="utf-8")):
            self.assertEqual(candidate.count(edit["before"]), 1,
                             "transport fixture edit must match exactly once")
            candidate = candidate.replace(edit["before"], edit["after"], 1)
        return candidate

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
            json.dumps({"suite": "passive", "cases": self._passive_cases()}),
            encoding="utf-8",
        )
        return repo_path, report_path

    def _passive_cases(self) -> list[dict[str, str]]:
        return [
            {"case_id": "P01", "passive_mode": "unsupported_or_misconfigured", "outcome": "pass"},
            {"case_id": "P02", "passive_mode": "unsupported_or_misconfigured", "outcome": "pass"},
            {"case_id": "P06", "passive_mode": "unsupported_or_misconfigured", "outcome": "pass"},
        ]

    def _required_cells(self) -> dict[str, bool]:
        return {
            "ENS-AD02": False,
            "ENS-AD03": False,
            "ENS-CX01": True,
            "ENH-CX01": True,
        }

    def _write_required_evidence(self, root: pathlib.Path, mutate=None) -> None:
        for case_id, second_client in self._required_cells().items():
            evidence = {
                "result": "pass",
                "passive_required": {
                    "mode": "required",
                    "passed": True,
                    "second_client": second_client,
                    "devices": 2,
                },
            }
            verdict = {"case_id": case_id, "outcome": "pass", "suite": "adaptermux-v2"}
            if mutate is not None:
                mutate(case_id, evidence, verdict)
            case_dir = root / case_id
            case_dir.mkdir(parents=True, exist_ok=True)
            (case_dir / "smoke_evidence.json").write_text(json.dumps(evidence), encoding="utf-8")
            (case_dir / "verdict.json").write_text(json.dumps(verdict), encoding="utf-8")

    def _run_gate(self, repo_path: pathlib.Path, report_path: pathlib.Path, evidence_dir: pathlib.Path | None):
        env = {"PASSIVE_SMOKE_GATE_BASE_REF": "HEAD", "PASSIVE_SMOKE_REPORT": str(report_path)}
        if evidence_dir is not None:
            env["PASSIVE_REQUIRED_CELL_DIR"] = str(evidence_dir)
        return subprocess.run(
            ["bash", "scripts/passive_smoke_gate.sh"],
            cwd=repo_path,
            env=self._script_env(**env),
            text=True,
            capture_output=True,
            check=False,
        )

    def _run_triggered_gate(self, cases: list[dict[str, str]]) -> subprocess.CompletedProcess[str]:
        repo_path, report_path = self._create_temp_repo(
            "cmd/gateway/main.go",
            base_text="package main\n\nfunc main() {\n\tpassiveMode := \"required\"\n\t_ = passiveMode\n}\n",
            modified_text="package main\n\nfunc main() {\n\tpassiveMode := \"unsupported_or_misconfigured\"\n\t_ = passiveMode\n}\n",
        )
        report_path.write_text(
            json.dumps({"suite": "passive", "cases": cases}),
            encoding="utf-8",
        )
        return subprocess.run(
            ["bash", "scripts/passive_smoke_gate.sh"],
            cwd=repo_path,
            env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD", PASSIVE_SMOKE_REPORT=str(report_path)),
            text=True,
            capture_output=True,
            check=False,
        )

    def test_passive_smoke_gate_triggers_for_real_main_diff(self) -> None:
        repo_path, report_path = self._create_temp_repo(
            "cmd/gateway/main.go",
            base_text="package main\n\nfunc main() {\n\tpassiveMode := \"required\"\n\t_ = passiveMode\n}\n",
            modified_text="package main\n\nfunc main() {\n\tpassiveMode := \"unsupported_or_misconfigured\"\n\t_ = passiveMode\n}\n",
        )
        evidence_dir = repo_path / "required-cells"
        self._write_required_evidence(evidence_dir)
        result = self._run_gate(repo_path, report_path, evidence_dir)
        self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)
        self.assertIn("passive smoke gate: PASS", result.stdout)
        self.assertIn("P01, P02 and P06", result.stdout)
        self.assertIn("ENS-AD02", result.stdout)
        self.assertIn("ENH-CX01", result.stdout)

    def test_reduced_report_without_required_discovery_evidence_is_rejected(self) -> None:
        repo_path, report_path = self._create_temp_repo(
            "cmd/gateway/main.go",
            base_text="package main\n\nfunc main() {\n\tpassiveMode := \"required\"\n\t_ = passiveMode\n}\n",
            modified_text="package main\n\nfunc main() {\n\tpassiveMode := \"unsupported_or_misconfigured\"\n\t_ = passiveMode\n}\n",
        )
        missing = self._run_gate(repo_path, report_path, None)
        self.assertNotEqual(missing.returncode, 0, msg=missing.stdout)
        self.assertIn("required passive discovery evidence is missing", missing.stdout)
        self.assertIn("ENS-AD02", missing.stdout)
        self.assertIn("ENS-CX01", missing.stdout)

        evidence_dir = repo_path / "required-cells"

        def fail_second_client(case_id, evidence, verdict):
            if case_id == "ENS-CX01":
                verdict["outcome"] = "fail"

        self._write_required_evidence(evidence_dir, fail_second_client)
        client_failed = self._run_gate(repo_path, report_path, evidence_dir)
        self.assertNotEqual(client_failed.returncode, 0, msg=client_failed.stdout)
        self.assertIn("ENS-CX01 did not pass", client_failed.stdout)

        def empty_devices(case_id, evidence, verdict):
            if case_id == "ENS-AD03":
                evidence["passive_required"]["devices"] = 0
                evidence["passive_required"]["passed"] = True

        self._write_required_evidence(evidence_dir, empty_devices)
        empty = self._run_gate(repo_path, report_path, evidence_dir)
        self.assertNotEqual(empty.returncode, 0, msg=empty.stdout)
        self.assertIn("ENS-AD03 required passive discovery did not pass", empty.stdout)

        def wrong_client_flag(case_id, evidence, verdict):
            if case_id == "ENS-AD02":
                evidence["passive_required"]["second_client"] = True

        self._write_required_evidence(evidence_dir, wrong_client_flag)
        flagged = self._run_gate(repo_path, report_path, evidence_dir)
        self.assertNotEqual(flagged.returncode, 0, msg=flagged.stdout)
        self.assertIn("ENS-AD02 second client flag is True", flagged.stdout)
        self.assertIn("want False", flagged.stdout)

    def test_retired_six_case_form_names_the_new_form(self) -> None:
        cases = [
            {"case_id": "P01", "passive_mode": "unsupported_or_misconfigured", "outcome": "pass"},
            {"case_id": "P02", "passive_mode": "unsupported_or_misconfigured", "outcome": "pass"},
            {"case_id": "P03", "passive_mode": "required", "outcome": "pass"},
            {"case_id": "P04", "passive_mode": "required", "outcome": "pass"},
            {"case_id": "P05", "passive_mode": "required", "outcome": "pass"},
            {"case_id": "P06", "passive_mode": "unsupported_or_misconfigured", "outcome": "pass"},
        ]
        result = self._run_triggered_gate(cases)
        self.assertNotEqual(result.returncode, 0, msg=result.stdout)
        self.assertIn("retired six-case form", result.stdout)
        self.assertIn("P01, P02 and P06", result.stdout)

    def test_each_remaining_case_field_is_required(self) -> None:
        # case_id: dropping one required id must name that id.
        # passive_mode: a different mode on that id must name the id and the mode.
        # outcome: a non-pass outcome on that id must name the id and the outcome.
        for case_id, passive_mode in (
            ("P01", "unsupported_or_misconfigured"),
            ("P02", "unsupported_or_misconfigured"),
            ("P06", "unsupported_or_misconfigured"),
        ):
            missing = [dict(case) for case in self._passive_cases() if case["case_id"] != case_id]
            missing_result = self._run_triggered_gate(missing)
            self.assertNotEqual(missing_result.returncode, 0, msg=missing_result.stdout)
            self.assertIn(f"missing required cases: {case_id}", missing_result.stdout)

            mutated_mode = "required" if passive_mode != "required" else "unsupported_or_misconfigured"
            wrong_mode = [dict(case) for case in self._passive_cases()]
            for case in wrong_mode:
                if case["case_id"] == case_id:
                    case["passive_mode"] = mutated_mode
            mode_result = self._run_triggered_gate(wrong_mode)
            self.assertNotEqual(mode_result.returncode, 0, msg=mode_result.stdout)
            self.assertIn(f"{case_id} passive_mode='{mutated_mode}'", mode_result.stdout)

            failing = [dict(case) for case in self._passive_cases()]
            for case in failing:
                if case["case_id"] == case_id:
                    case["outcome"] = "fail"
            fail_result = self._run_triggered_gate(failing)
            self.assertNotEqual(fail_result.returncode, 0, msg=fail_result.stdout)
            self.assertIn(f"{case_id} outcome='fail'", fail_result.stdout)

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
    existing int
}
func (store *BusObservabilityStore) RenderPrometheus() {
    writer := newPrometheusWriter(buffer)
}
'''
        allowed = '''type BusObservabilityStore struct {
    // semanticMetricsProvider returns detached, already-evaluated SemReg views.
    // It is invoked after the store snapshot is released: a /metrics request must
    // never take the store lock across a driver or publication lock.
    semanticMetricsProvider func(time.Time) []SemanticMetricsDomain
    existing int
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
    writer := newPrometheusWriter(buffer)
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

        omitted_unlock = allowed.replace("    store.mu.Unlock()\n}\nfunc (store *BusObservabilityStore) RenderPrometheus()", "}\nfunc (store *BusObservabilityStore) RenderPrometheus()", 1)
        exchanged_unlock = omitted_unlock + "\nfunc unrelatedLockChange(store *BusObservabilityStore) { store.mu.Unlock() }\n"
        for hostile in (omitted_unlock, exchanged_unlock):
            (repo_path / "bus_observability_store.go").write_text(hostile, encoding="utf-8")
            result = subprocess.run(["bash", "scripts/passive_smoke_gate.sh"], cwd=repo_path, env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"), text=True, capture_output=True, check=False)
            self.assertNotEqual(result.returncode, 0, "classifier accepted changed lock structure")
            self.assertIn("PASSIVE_SMOKE_REPORT is required", result.stdout)

    def test_bus_observability_transport_snapshot_is_only_exception(self) -> None:
        base = self._pre_candidate_bus_observability_base()
        allowed = self._transport_snapshot_candidate()
        repo_path, _ = self._create_temp_repo(
            "bus_observability_store.go", base_text=base, modified_text=allowed
        )
        result = subprocess.run(
            ["bash", "scripts/passive_smoke_gate.sh"], cwd=repo_path,
            env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"),
            text=True, capture_output=True, check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)
        self.assertIn("not triggered", result.stdout)

        specimen_change = allowed.replace(
            "specimens map[string]*specimenFamilyBucket // keyed by family",
            "specimens specimens.Store",
        )
        self.assertNotEqual(specimen_change, allowed)
        (repo_path / "bus_observability_store.go").write_text(specimen_change, encoding="utf-8")
        result = subprocess.run(
            ["bash", "scripts/passive_smoke_gate.sh"], cwd=repo_path,
            env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"),
            text=True, capture_output=True, check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("PASSIVE_SMOKE_REPORT is required", result.stdout)

        for hostile in (
            '\nfunc transportSnapshotPassiveMutation(store *BusObservabilityStore) { store.passive.state = "unsafe" }\n',
            '\nfunc transportSnapshotLockMutation(store *BusObservabilityStore) { store.mu.RLock() }\n',
            '\nfunc transportSnapshotCounterMutation(writer *prometheusWriter) { writer.writeGaugeSample("ebus_unreviewed", 1, nil) }\n',
            '\nfunc transportSnapshotRenderMutation(store *BusObservabilityStore) { _ = store.RenderPrometheus() }\n',
        ):
            (repo_path / "bus_observability_store.go").write_text(allowed + hostile, encoding="utf-8")
            result = subprocess.run(
                ["bash", "scripts/passive_smoke_gate.sh"], cwd=repo_path,
                env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"),
                text=True, capture_output=True, check=False,
            )
            self.assertNotEqual(result.returncode, 0, hostile)
            self.assertIn("PASSIVE_SMOKE_REPORT is required", result.stdout)

    def test_baud_presence_is_only_metrics_exception(self) -> None:
        allowed = (REPO_ROOT / "bus_observability_store.go").read_text(encoding="utf-8")
        # Reconstruct the source immediately before the configured-presence
        # addition; the older runtime-snapshot fixture remains immutable.
        base = allowed
        for addition, previous in (
            ("\t// Captured before defaults; independent of readiness and transport counters.\n\tebusConfigured bool\n\n", ""),
            ("// PreserveEBusConfigurationPresence captures operator configuration before runtime\n// wrappers or defaults are injected. Repeated calls retain the original fact.\nfunc (cfg *Config) PreserveEBusConfigurationPresence() {\n\tif cfg.ebusConfigurationPresence == nil {\n\t\tpresent := cfg.ebusConfigured()\n\t\tcfg.ebusConfigurationPresence = &present\n\t}\n}\n\nfunc (cfg Config) ebusConfigured() bool {\n\tif cfg.ebusConfigurationPresence != nil {\n\t\treturn *cfg.ebusConfigurationPresence\n\t}\n\treturn cfg.Transport != nil || cfg.PassiveTransport != nil ||\n\t\tcfg.TransportConfig.Protocol != \"\" || cfg.TransportConfig.Address != \"\"\n}\n\nfunc NewBusObservabilityStore(cfg Config) *BusObservabilityStore {\n\tebusConfigured := cfg.ebusConfigured()\n",
             "func NewBusObservabilityStore(cfg Config) *BusObservabilityStore {\n"),
            ("\t\tcfg:            cfg,\n\t\tnow:            time.Now,\n\t\tebusConfigured: ebusConfigured,", "\t\tcfg: cfg,\n\t\tnow: time.Now,"),
            ("writeTransportRuntimeMetrics(writer, transportRuntimeStatus, store.ebusConfigured)", "writeTransportRuntimeMetrics(writer, transportRuntimeStatus)"),
        ):
            self.assertEqual(base.count(addition), 1)
            base = base.replace(addition, previous)
        repo_path, _ = self._create_temp_repo(
            "bus_observability_store.go", base_text=base, modified_text=allowed
        )
        tracked = repo_path / "bus_observability_store.go"

        for staged in (False, True):
            with self.subTest(staged=staged):
                tracked.write_text(allowed, encoding="utf-8")
                if staged:
                    subprocess.run(["git", "add", "bus_observability_store.go"], cwd=repo_path, check=True, capture_output=True, text=True)
                result = subprocess.run(
                    ["bash", "scripts/passive_smoke_gate.sh"], cwd=repo_path,
                    env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"),
                    text=True, capture_output=True, check=False,
                )
                self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)
                self.assertIn("not triggered", result.stdout)
                for hostile in (
                    allowed.replace('cfg.TransportConfig.Address != ""', 'cfg.TransportConfig.Address == ""'),
                    allowed.replace("return *cfg.ebusConfigurationPresence", "return true"),
                    allowed.replace("\tif cfg.ebusConfigurationPresence == nil {", "\tstorePassiveMutation()\n\tif cfg.ebusConfigurationPresence == nil {"),
                    allowed.replace("\tcfg = applyDefaults(cfg)", "\tstorePassiveMutation()\n\tcfg = applyDefaults(cfg)"),
                    allowed.replace("\tebusConfigured :=", "\tstore.mu.Lock()\n\tebusConfigured :="),
                    allowed.replace("store.ebusConfigured)", "true)"),
                ):
                    self.assertNotEqual(hostile, allowed)
                    tracked.write_text(hostile, encoding="utf-8")
                    result = subprocess.run(
                        ["bash", "scripts/passive_smoke_gate.sh"], cwd=repo_path,
                        env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD"),
                        text=True, capture_output=True, check=False,
                    )
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("PASSIVE_SMOKE_REPORT is required", result.stdout)

    def test_config_ebus_presence_field_exemption_is_exact(self) -> None:
        base = "type Config struct {\n\tTransport int\n}\n"
        allowed = "type Config struct {\n\tebusConfigurationPresence *bool\n\n\tTransport int\n}\n"
        repo_path, _ = self._create_temp_repo("config.go", base_text=base, modified_text=allowed)
        env = self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD")
        result = subprocess.run(["bash", "scripts/passive_smoke_gate.sh"], cwd=repo_path, env=env, text=True, capture_output=True, check=False)
        self.assertEqual(result.returncode, 0, msg=result.stdout + result.stderr)
        for hostile in (
            allowed.replace("*bool", "*bool\n\tPassiveForce bool"),
            allowed.replace("ebusConfigurationPresence *bool", "ebusConfigurationPresence bool"),
        ):
            (repo_path / "config.go").write_text(hostile, encoding="utf-8")
            result = subprocess.run(["bash", "scripts/passive_smoke_gate.sh"], cwd=repo_path, env=env, text=True, capture_output=True, check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("PASSIVE_SMOKE_REPORT is required", result.stdout)

    def test_transport_snapshot_fixture_falls_back_without_local_history(self) -> None:
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        repo_path = pathlib.Path(temp_dir.name)
        subprocess.run(["git", "init", "-b", "main"], cwd=repo_path, check=True, capture_output=True, text=True)
        subprocess.run(["git", "config", "user.name", "Codex"], cwd=repo_path, check=True, capture_output=True, text=True)
        subprocess.run(["git", "config", "user.email", "codex@example.com"], cwd=repo_path, check=True, capture_output=True, text=True)
        (repo_path / "bus_observability_store.go").write_text(self._transport_snapshot_candidate(), encoding="utf-8")
        subprocess.run(["git", "add", "."], cwd=repo_path, check=True, capture_output=True, text=True)
        subprocess.run(["git", "commit", "-m", "depth-one candidate"], cwd=repo_path, check=True, capture_output=True, text=True)
        subprocess.run(["git", "checkout", "--detach", "HEAD"], cwd=repo_path, check=True, capture_output=True, text=True)
        subprocess.run(["git", "update-ref", "-d", "refs/heads/main"], cwd=repo_path, check=True, capture_output=True, text=True)
        self.assertNotEqual(subprocess.run(["git", "rev-parse", "--verify", "HEAD^"], cwd=repo_path, capture_output=True, text=True, check=False).returncode, 0)
        self.assertNotEqual(subprocess.run(["git", "rev-parse", "--verify", "main"], cwd=repo_path, capture_output=True, text=True, check=False).returncode, 0)
        self.assertEqual(self._pre_candidate_bus_observability_base(), (SCRIPT_DIR / "testdata" / "passive_smoke_bus_observability_store_base.go").read_text(encoding="utf-8"))

    def test_transport_snapshot_fixture_survives_empty_successor_commit(self) -> None:
        base = self._pre_candidate_bus_observability_base()
        allowed = self._transport_snapshot_candidate()
        repo_path, _ = self._create_temp_repo("bus_observability_store.go", base_text=base, modified_text=base)
        tracked = repo_path / "bus_observability_store.go"
        tracked.write_text(allowed, encoding="utf-8")
        subprocess.run(["git", "add", "bus_observability_store.go"], cwd=repo_path, check=True, capture_output=True, text=True)
        subprocess.run(["git", "commit", "-m", "candidate"], cwd=repo_path, check=True, capture_output=True, text=True)
        subprocess.run(["git", "commit", "--allow-empty", "-m", "successor"], cwd=repo_path, check=True, capture_output=True, text=True)
        self.assertEqual(self._pre_candidate_bus_observability_base(), base)
        allowed_result = subprocess.run(["bash", "scripts/passive_smoke_gate.sh"], cwd=repo_path, env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD^^"), text=True, capture_output=True, check=False)
        self.assertEqual(allowed_result.returncode, 0, msg=allowed_result.stdout + allowed_result.stderr)
        tracked.write_text(allowed + '\nfunc successorHostile(store *BusObservabilityStore) { store.passive.state = "unsafe" }\n', encoding="utf-8")
        hostile = subprocess.run(["bash", "scripts/passive_smoke_gate.sh"], cwd=repo_path, env=self._script_env(PASSIVE_SMOKE_GATE_BASE_REF="HEAD^^"), text=True, capture_output=True, check=False)
        self.assertNotEqual(hostile.returncode, 0)
        self.assertIn("PASSIVE_SMOKE_REPORT is required", hostile.stdout)

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
