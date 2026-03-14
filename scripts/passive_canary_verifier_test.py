#!/usr/bin/env python3
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import passive_canary_verifier as verifier  # noqa: E402


def write_json(path: pathlib.Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


class ManifestValidationTests(unittest.TestCase):
    def test_manifest_requires_expected_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = pathlib.Path(temp_dir) / "manifest.json"
            payload = verifier.load_json(
                SCRIPT_DIR.parent / "testdata" / "passive_proof" / "p03_canary_manifest.json"
            )
            payload["schema"] = "unexpected_schema"
            write_json(manifest_path, payload)

            with self.assertRaises(ValueError) as ctx:
                verifier.load_and_validate_manifest(manifest_path, "P03")
            self.assertIn("schema", str(ctx.exception))

    def test_manifest_requires_b509_minimum(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = pathlib.Path(temp_dir) / "manifest.json"
            write_json(
                manifest_path,
                {
                    "schema": "p03_canary_manifest_v1",
                    "case_id": "P03",
                    "canaries": [
                        {
                            "id": "b524_1",
                            "family": "B524",
                            "address": 0x15,
                            "plane": "system",
                            "method": "get_ext_register",
                            "params": {"source": 0x10, "group": 0, "instance": 0, "addr": 0x5C00},
                        },
                        {
                            "id": "b524_2",
                            "family": "B524",
                            "address": 0x15,
                            "plane": "system",
                            "method": "get_ext_register",
                            "params": {"source": 0x10, "group": 0, "instance": 0, "addr": 0x5E00},
                        },
                        {
                            "id": "b524_3",
                            "family": "B524",
                            "address": 0x15,
                            "plane": "system",
                            "method": "get_ext_register",
                            "params": {"source": 0x10, "group": 0, "instance": 0, "addr": 0x6200},
                        },
                        {
                            "id": "b509_only_1",
                            "family": "B509",
                            "address": 0x08,
                            "plane": "system",
                            "method": "get_register",
                            "params": {"source": 0x10, "addr": 0x0200},
                        },
                        {
                            "id": "x_1",
                            "family": "X",
                            "address": 0x08,
                            "plane": "system",
                            "method": "get_register",
                            "params": {"source": 0x10, "addr": 0x0500},
                        },
                        {
                            "id": "x_2",
                            "family": "X",
                            "address": 0x08,
                            "plane": "system",
                            "method": "get_register",
                            "params": {"source": 0x10, "addr": 0x0600},
                        },
                    ],
                },
            )
            with self.assertRaises(ValueError) as ctx:
                verifier.load_and_validate_manifest(manifest_path, "P03")
            self.assertIn("B509", str(ctx.exception))

    def test_manifest_rejects_non_read_methods(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = pathlib.Path(temp_dir) / "manifest.json"
            payload = verifier.load_json(
                SCRIPT_DIR.parent / "testdata" / "passive_proof" / "p03_canary_manifest.json"
            )
            payload["canaries"][0]["method"] = "set_target"
            write_json(manifest_path, payload)

            with self.assertRaises(ValueError) as ctx:
                verifier.load_and_validate_manifest(manifest_path, "P03")
            self.assertIn("read-only", str(ctx.exception))

    def test_manifest_rejects_prefix_bypass_method_name(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = pathlib.Path(temp_dir) / "manifest.json"
            payload = verifier.load_json(
                SCRIPT_DIR.parent / "testdata" / "passive_proof" / "p03_canary_manifest.json"
            )
            payload["canaries"][0]["method"] = "get_then_set_target"
            write_json(manifest_path, payload)

            with self.assertRaises(ValueError) as ctx:
                verifier.load_and_validate_manifest(manifest_path, "P03")
            self.assertIn("read-only", str(ctx.exception))


class RetryClassificationTests(unittest.TestCase):
    def test_inconclusive_after_three_retries(self) -> None:
        _, canaries = verifier.load_and_validate_manifest(
            SCRIPT_DIR.parent / "testdata" / "passive_proof" / "p03_canary_manifest.json",
            "P03",
        )
        original_invoke = verifier.invoke_canary

        def fail_invoke(*_args, **_kwargs):
            raise RuntimeError("timeout")

        verifier.invoke_canary = fail_invoke
        try:
            results = verifier.verify_phase(
                canaries=canaries[:1],
                graphql_url="http://unused/graphql",
                phase="start",
                run_id="run-1",
                retries=3,
                timeout_sec=0.01,
                baseline_map={},
            )
        finally:
            verifier.invoke_canary = original_invoke

        self.assertEqual(results["summary"]["inconclusive"], 1)
        self.assertEqual(results["summary"]["conclusive"], 0)
        entry = results["results"][0]
        self.assertEqual(entry["status"], "inconclusive")
        self.assertEqual(entry["attempts_used"], 3)
        self.assertEqual(entry["max_retries"], 3)

    def test_mismatch_when_value_differs_from_baseline(self) -> None:
        _, canaries = verifier.load_and_validate_manifest(
            SCRIPT_DIR.parent / "testdata" / "passive_proof" / "p03_canary_manifest.json",
            "P03",
        )
        original_invoke = verifier.invoke_canary
        verifier.invoke_canary = lambda *_args, **_kwargs: "DEAD"
        try:
            results = verifier.verify_phase(
                canaries=canaries[:1],
                graphql_url="http://unused/graphql",
                phase="sample_0002",
                run_id="run-2",
                retries=3,
                timeout_sec=0.01,
                baseline_map={canaries[0].canary_id: "BEEF"},
            )
        finally:
            verifier.invoke_canary = original_invoke

        entry = results["results"][0]
        self.assertEqual(entry["status"], "mismatch")
        self.assertEqual(results["summary"]["mismatch"], 1)


class BaselineSeedingPhaseTests(unittest.TestCase):
    def test_start_phase_seeds_baseline(self) -> None:
        _, canaries = verifier.load_and_validate_manifest(
            SCRIPT_DIR.parent / "testdata" / "passive_proof" / "p03_canary_manifest.json",
            "P03",
        )
        original_invoke = verifier.invoke_canary
        verifier.invoke_canary = lambda *_args, **_kwargs: "BEEF"
        baseline_map = {}
        try:
            results = verifier.verify_phase(
                canaries=canaries[:1],
                graphql_url="http://unused/graphql",
                phase="start",
                run_id="run-start",
                retries=3,
                timeout_sec=0.01,
                baseline_map=baseline_map,
            )
        finally:
            verifier.invoke_canary = original_invoke

        canary_id = canaries[0].canary_id
        self.assertEqual(baseline_map[canary_id], "BEEF")
        self.assertEqual(results["results"][0]["status"], "pass")

    def test_non_start_phase_with_missing_baseline_is_inconclusive_and_not_seeded(self) -> None:
        _, canaries = verifier.load_and_validate_manifest(
            SCRIPT_DIR.parent / "testdata" / "passive_proof" / "p03_canary_manifest.json",
            "P03",
        )
        original_invoke = verifier.invoke_canary

        def fail_if_called(*_args, **_kwargs):
            raise AssertionError("invoke_canary should not run without baseline in non-start phases")

        verifier.invoke_canary = fail_if_called
        baseline_map = {}
        try:
            results = verifier.verify_phase(
                canaries=canaries[:1],
                graphql_url="http://unused/graphql",
                phase="sample_0001",
                run_id="run-sample",
                retries=3,
                timeout_sec=0.01,
                baseline_map=baseline_map,
            )
        finally:
            verifier.invoke_canary = original_invoke

        self.assertEqual(baseline_map, {})
        self.assertEqual(results["summary"]["inconclusive"], 1)
        entry = results["results"][0]
        self.assertEqual(entry["status"], "inconclusive")
        self.assertEqual(entry["attempts_used"], 0)
        self.assertIn("missing baseline", entry["reason"])


class IntervalSchedulingTests(unittest.TestCase):
    def test_compute_next_sample_epoch(self) -> None:
        self.assertEqual(verifier.compute_next_sample_epoch(100, 5), 105)
        self.assertEqual(verifier.compute_next_sample_epoch(0, 1), 1)
        with self.assertRaises(ValueError):
            verifier.compute_next_sample_epoch(100, 0)


class StaleArtifactRejectionTests(unittest.TestCase):
    def test_summary_rejects_stale_only_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_json(
                proof_dir / "canary_phase_start.json",
                {
                    "run_id": "old-run",
                    "phase": "start",
                    "results": [{"id": "a", "status": "pass"}],
                },
            )
            write_json(
                proof_dir / "canary_phase_end.json",
                {
                    "run_id": "old-run",
                    "phase": "end",
                    "results": [{"id": "a", "status": "pass"}],
                },
            )

            with self.assertRaises(ValueError) as ctx:
                verifier.summarize_run(proof_dir, "new-run")
            self.assertIn("stale artifacts", str(ctx.exception))

    def test_summary_requires_interval_sample_phase(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_json(
                proof_dir / "canary_phase_start.json",
                {
                    "run_id": "run-1",
                    "phase": "start",
                    "results": [{"id": "a", "status": "pass"}],
                },
            )
            write_json(
                proof_dir / "canary_phase_end.json",
                {
                    "run_id": "run-1",
                    "phase": "end",
                    "results": [{"id": "a", "status": "pass"}],
                },
            )

            with self.assertRaises(ValueError) as ctx:
                verifier.summarize_run(proof_dir, "run-1")
            self.assertIn("interval", str(ctx.exception))

    def test_summary_last_status_uses_final_phase_order(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_json(
                proof_dir / "canary_phase_start.json",
                {
                    "run_id": "run-1",
                    "phase": "start",
                    "results": [{"id": "a", "status": "pass"}],
                },
            )
            write_json(
                proof_dir / "canary_phase_end.json",
                {
                    "run_id": "run-1",
                    "phase": "end",
                    "results": [{"id": "a", "status": "mismatch"}],
                },
            )

            summary = verifier.summarize_run(proof_dir, "run-1", require_interval_phase=False)
            self.assertEqual(summary["per_canary"]["a"]["last_status"], "mismatch")

    def test_summary_allows_missing_interval_when_not_required(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_json(
                proof_dir / "canary_phase_start.json",
                {
                    "run_id": "run-1",
                    "phase": "start",
                    "results": [{"id": "a", "status": "pass"}],
                },
            )
            write_json(
                proof_dir / "canary_phase_end.json",
                {
                    "run_id": "run-1",
                    "phase": "end",
                    "results": [{"id": "a", "status": "pass"}],
                },
            )

            summary = verifier.summarize_run(proof_dir, "run-1", require_interval_phase=False)
            self.assertEqual(summary["interval_phase_count"], 0)
            self.assertFalse(summary["interval_phase_required"])


class CanaryVerdictTests(unittest.TestCase):
    def build_summary_payload(
        self,
        *,
        mismatch_count: int,
        interval_required: bool,
        interval_results: int,
        interval_conclusive: int,
        per_canary_interval: dict[str, dict[str, int]],
    ) -> dict:
        per_canary = {canary_id: {"last_status": "pass"} for canary_id in per_canary_interval}
        return {
            "schema": "p03_canary_overall_summary_v1",
            "run_id": "run-1",
            "interval_phase_required": interval_required,
            "totals": {
                "results": 20,
                "pass": 20 - mismatch_count,
                "mismatch": mismatch_count,
                "inconclusive": 0,
                "conclusive": 20,
            },
            "interval_totals": {
                "results": interval_results,
                "conclusive": interval_conclusive,
                "pass": max(interval_conclusive - mismatch_count, 0),
                "mismatch": mismatch_count,
                "inconclusive": max(interval_results - interval_conclusive, 0),
            },
            "per_canary": per_canary,
            "per_canary_interval": per_canary_interval,
        }

    def test_verdict_fails_on_any_mismatch(self) -> None:
        summary = self.build_summary_payload(
            mismatch_count=1,
            interval_required=True,
            interval_results=10,
            interval_conclusive=10,
            per_canary_interval={
                "a": {"pass": 5, "mismatch": 0, "inconclusive": 0, "conclusive": 5},
                "b": {"pass": 4, "mismatch": 1, "inconclusive": 0, "conclusive": 5},
            },
        )
        verdict = verifier.build_canary_verdict(summary)
        self.assertFalse(verdict["ok"])
        self.assertFalse(verdict["criteria"]["no_mismatches"]["ok"])

    def test_verdict_fails_when_overall_interval_conclusive_ratio_below_threshold(self) -> None:
        summary = self.build_summary_payload(
            mismatch_count=0,
            interval_required=True,
            interval_results=10,
            interval_conclusive=8,
            per_canary_interval={
                "a": {"pass": 4, "mismatch": 0, "inconclusive": 1, "conclusive": 4},
                "b": {"pass": 4, "mismatch": 0, "inconclusive": 1, "conclusive": 4},
            },
        )
        verdict = verifier.build_canary_verdict(summary)
        self.assertFalse(verdict["ok"])
        self.assertFalse(verdict["criteria"]["overall_interval_conclusive_rate"]["ok"])

    def test_verdict_fails_when_any_canary_interval_conclusive_ratio_below_threshold(self) -> None:
        summary = self.build_summary_payload(
            mismatch_count=0,
            interval_required=True,
            interval_results=20,
            interval_conclusive=18,
            per_canary_interval={
                "a": {"pass": 9, "mismatch": 0, "inconclusive": 0, "conclusive": 9},
                "b": {"pass": 9, "mismatch": 0, "inconclusive": 0, "conclusive": 9},
                "c": {"pass": 0, "mismatch": 0, "inconclusive": 2, "conclusive": 0},
            },
        )
        verdict = verifier.build_canary_verdict(summary)
        self.assertFalse(verdict["ok"])
        self.assertFalse(verdict["criteria"]["per_canary_interval_conclusive_rate"]["ok"])
        self.assertEqual(verdict["criteria"]["per_canary_interval_conclusive_rate"]["failing_canaries"], ["c"])

    def test_verdict_passes_when_all_thresholds_are_healthy(self) -> None:
        summary = self.build_summary_payload(
            mismatch_count=0,
            interval_required=True,
            interval_results=10,
            interval_conclusive=9,
            per_canary_interval={
                "a": {"pass": 4, "mismatch": 0, "inconclusive": 1, "conclusive": 4},
                "b": {"pass": 5, "mismatch": 0, "inconclusive": 0, "conclusive": 5},
            },
        )
        verdict = verifier.build_canary_verdict(summary)
        self.assertTrue(verdict["ok"])
        self.assertTrue(verdict["criteria"]["no_mismatches"]["ok"])
        self.assertTrue(verdict["criteria"]["overall_interval_conclusive_rate"]["ok"])
        self.assertTrue(verdict["criteria"]["per_canary_interval_conclusive_rate"]["ok"])

    def test_verdict_waives_interval_thresholds_when_interval_phase_not_required(self) -> None:
        summary = self.build_summary_payload(
            mismatch_count=0,
            interval_required=False,
            interval_results=0,
            interval_conclusive=0,
            per_canary_interval={
                "a": {"pass": 0, "mismatch": 0, "inconclusive": 0, "conclusive": 0},
                "b": {"pass": 0, "mismatch": 0, "inconclusive": 0, "conclusive": 0},
            },
        )
        verdict = verifier.build_canary_verdict(summary)
        self.assertTrue(verdict["ok"])
        self.assertTrue(verdict["criteria"]["overall_interval_conclusive_rate"]["waived"])
        self.assertTrue(verdict["criteria"]["per_canary_interval_conclusive_rate"]["waived"])


class PassiveSmokeCanaryVerdictGateTests(unittest.TestCase):
    def _run_smoke_with_fake_tools(
        self,
        canary_status: str,
        *,
        extra_env: dict[str, str] | None = None,
        collect_artifacts: bool = False,
    ) -> tuple[subprocess.CompletedProcess[str], dict]:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = pathlib.Path(temp_dir)
            fake_bin = temp_path / "bin"
            fake_bin.mkdir(parents=True, exist_ok=True)
            log_dir = temp_path / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)

            fake_python = fake_bin / "python3"
            fake_python.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -euo pipefail
                    real_python="${REAL_PYTHON3:?REAL_PYTHON3 is required}"

                    if [[ "$#" -ge 2 && "$1" == *"/scripts/passive_canary_verifier.py" && "$2" == "verify-phase" ]]; then
                      shift 2
                      output=""
                      phase=""
                      run_id=""
                      baseline=""
                      while [[ "$#" -gt 0 ]]; do
                        case "$1" in
                          --output) output="$2"; shift 2 ;;
                          --phase) phase="$2"; shift 2 ;;
                          --run-id) run_id="$2"; shift 2 ;;
                          --baseline) baseline="$2"; shift 2 ;;
                          *) shift ;;
                        esac
                      done
                      status="${FAKE_CANARY_STATUS:-pass}"
                      pass_count=0
                      mismatch_count=0
                      if [[ "${status}" == "pass" ]]; then
                        pass_count=1
                      elif [[ "${status}" == "mismatch" ]]; then
                        mismatch_count=1
                      fi
                      mkdir -p "$(dirname "${output}")"
                      cat > "${output}" <<JSON
                    {
                      "schema": "p03_canary_phase_result_v1",
                      "run_id": "${run_id}",
                      "phase": "${phase}",
                      "results": [
                        {
                          "id": "canary_1",
                          "family": "B524",
                          "status": "${status}",
                          "conclusive": true
                        }
                      ],
                      "summary": {
                        "total": 1,
                        "pass": ${pass_count},
                        "mismatch": ${mismatch_count},
                        "inconclusive": 0,
                        "conclusive": 1
                      }
                    }
                    JSON
                      if [[ -n "${baseline}" ]]; then
                        mkdir -p "$(dirname "${baseline}")"
                        printf '{"canary_1":"BEEF"}\\n' > "${baseline}"
                      fi
                      exit 0
                    fi

                    exec "${real_python}" "$@"
                    """
                ),
                encoding="utf-8",
            )
            fake_python.chmod(0o755)

            fake_curl = fake_bin / "curl"
            fake_curl.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -euo pipefail

                    url="${@: -1}"
                    data=""
                    while [[ "$#" -gt 0 ]]; do
                      case "$1" in
                        -d)
                          data="$2"
                          shift 2
                          ;;
                        *)
                          shift
                          ;;
                      esac
                    done

                    if [[ "${url}" == *"/metrics" ]]; then
                      cat <<'EOF'
                    ebus_passive_capability_probe_outcomes_total{outcome="timed_out"} 0
                    ebus_passive_tap_connected 1
                    ebus_passive_warmup_state{state="available"} 1
                    ebus_passive_capability_probe_outcomes_total{outcome="confirmed"} 1
                    EOF
                      exit 0
                    fi

                    if [[ "${url}" == *"/portal/api/v1/bus/observability" ]]; then
                      cat <<'EOF'
                    {"summary":{"status":{"feature_flags":{"observeFirstEnabled":true}}}}
                    EOF
                      exit 0
                    fi

                    if [[ "${url}" == *"/graphql" ]]; then
                      if [[ "${data}" == *"busSummary"* ]]; then
                        cat <<'EOF'
                    {"data":{"busSummary":{"status":{"featureFlags":{"observeFirstEnabled":true}}},"watchSummary":{"inventory":{"totalEntries":1},"activationCounts":{"catalogDescriptors":1,"activeKeys":1,"sourceClasses":[]},"directApplyEligibilityClasses":[],"degraded":{"active":false,"shadowingEnabled":false,"pinnedBudgetDegraded":false,"compactorDegraded":false,"reasons":[]}}}}
                    EOF
                        exit 0
                      fi
                      cat <<'EOF'
                    {"data":{"devices":[{"address":"0x15","deviceId":"BASV2"}]}}
                    EOF
                      exit 0
                    fi

                    echo "unsupported fake curl url: ${url}" >&2
                    exit 22
                    """
                ),
                encoding="utf-8",
            )
            fake_curl.chmod(0o755)

            env = os.environ.copy()
            env.update(
                {
                    "MATRIX_CASE_ID": "P03",
                    "MATRIX_PASSIVE_MODE": "required",
                    "MATRIX_GW15_PROOF_MODE": "1",
                    "PASSIVE_SMOKE_TIMEOUT_SEC": "6",
                    "PASSIVE_SMOKE_POLL_INTERVAL_SEC": "1",
                    "PASSIVE_PROOF_SAMPLE_INTERVAL_SEC": "3600",
                    "MATRIX_LOG_DIR": str(log_dir),
                    "MATRIX_GATEWAY_BASE_URL": "http://fake-gateway:18083",
                    "MATRIX_GRAPHQL_URL": "http://fake-gateway:18083/graphql",
                    "MATRIX_METRICS_URL": "http://fake-gateway:18083/metrics",
                    "REAL_PYTHON3": sys.executable,
                    "FAKE_CANARY_STATUS": canary_status,
                    "PATH": f"{fake_bin}:{env.get('PATH', '')}",
                }
            )
            if extra_env:
                env.update(extra_env)
            script_path = SCRIPT_DIR / "passive_smoke_check.sh"
            started = time.monotonic()
            result = subprocess.run(
                ["bash", str(script_path)],
                cwd=SCRIPT_DIR.parent,
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )
            elapsed = time.monotonic() - started
            artifacts = {"elapsed_sec": elapsed}
            if collect_artifacts:
                proof_dir = log_dir / "proof_artifacts"
                summary_path = proof_dir / "canary_summary.json"
                verdict_path = proof_dir / "canary_verdict.json"
                if summary_path.exists():
                    artifacts["summary"] = json.loads(summary_path.read_text(encoding="utf-8"))
                if verdict_path.exists():
                    artifacts["verdict"] = json.loads(verdict_path.read_text(encoding="utf-8"))
                artifacts["sample_phase_files"] = sorted(
                    path.name for path in proof_dir.glob("canary_phase_sample_*.json")
                )
            return result, artifacts

    def run_smoke_with_fake_tools(
        self,
        canary_status: str,
        *,
        extra_env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        result, _ = self._run_smoke_with_fake_tools(
            canary_status,
            extra_env=extra_env,
            collect_artifacts=False,
        )
        return result

    def run_smoke_with_fake_tools_detailed(
        self,
        canary_status: str,
        *,
        extra_env: dict[str, str] | None = None,
    ) -> tuple[subprocess.CompletedProcess[str], dict]:
        return self._run_smoke_with_fake_tools(
            canary_status,
            extra_env=extra_env,
            collect_artifacts=True,
        )

    def test_smoke_exits_non_zero_when_canary_verdict_is_bad(self) -> None:
        result = self.run_smoke_with_fake_tools("mismatch")
        self.assertNotEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("canary verdict gate failed", result.stderr)

    def test_smoke_exits_zero_when_canary_verdict_is_good(self) -> None:
        result = self.run_smoke_with_fake_tools("pass")
        self.assertEqual(result.returncode, 0, msg=result.stderr)

    def test_smoke_holds_until_proof_window_end_and_requires_interval_phase(self) -> None:
        result, artifacts = self.run_smoke_with_fake_tools_detailed(
            "pass",
            extra_env={
                "PASSIVE_PROOF_HOLD_SEC": "4",
                "PASSIVE_SMOKE_TIMEOUT_SEC": "10",
                "PASSIVE_PROOF_SAMPLE_INTERVAL_SEC": "3600",
            },
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertGreaterEqual(artifacts["elapsed_sec"], 3.0, msg=result.stderr)
        summary = artifacts.get("summary")
        self.assertIsInstance(summary, dict)
        self.assertTrue(summary["interval_phase_required"])
        self.assertGreaterEqual(summary["interval_phase_count"], 1)
        self.assertGreaterEqual(len(artifacts.get("sample_phase_files", [])), 1)


if __name__ == "__main__":
    unittest.main()
