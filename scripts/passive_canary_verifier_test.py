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


def write_metrics(path: pathlib.Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class ManifestValidationTests(unittest.TestCase):
    def test_manifest_matches_canonical_proxy_p03_stable_set(self) -> None:
        _, canaries = verifier.load_and_validate_manifest(
            SCRIPT_DIR.parent / "testdata" / "passive_proof" / "p03_canary_manifest.json",
            "P03",
        )
        self.assertEqual(
            [item.canary_id for item in canaries],
            [
                "b524_dhw_mode",
                "b524_dhw_target",
                "b524_circuit_type",
                "b524_room_temp_control",
                "b509_a0200",
                "b509_a0f04",
            ],
        )
        for item in canaries:
            self.assertEqual(item.params["source"], 0xF7)

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

    def test_replay_corpus_locks_negative_falsification_expectations(self) -> None:
        corpus = verifier.load_json(SCRIPT_DIR.parent / "testdata" / "observe_first_replay_cases.json")
        verdict = verifier.build_replay_falsification_verdict(corpus)

        self.assertEqual(verdict["schema"], verifier.REPLAY_FALSIFICATION_VERDICT_SCHEMA)
        self.assertTrue(verdict["ok"])
        self.assertEqual(verdict["summary"]["locked_cases"], 3)
        self.assertEqual(verdict["summary"]["fail"], 0)

        by_name = {case["name"]: case for case in verdict["cases"]}
        self.assertEqual(by_name["b524_value_bearing_enh"]["status"], "pass")
        self.assertFalse(by_name["b524_value_bearing_enh"]["direct_apply"])
        self.assertEqual(by_name["b524_value_bearing_enh"]["disposition"], "ambiguity")
        self.assertEqual(by_name["collision_episode"]["status"], "pass")
        self.assertFalse(by_name["collision_episode"]["direct_apply"])
        self.assertEqual(by_name["collision_episode"]["disposition"], "falsification")
        self.assertEqual(by_name["timeout_no_progress"]["status"], "pass")
        self.assertFalse(by_name["timeout_no_progress"]["direct_apply"])
        self.assertEqual(by_name["timeout_no_progress"]["disposition"], "falsification")


class RetryClassificationTests(unittest.TestCase):
    def test_invoke_canary_adds_internal_nonce_without_mutating_manifest_params(self) -> None:
        _, canaries = verifier.load_and_validate_manifest(
            SCRIPT_DIR.parent / "testdata" / "passive_proof" / "p03_canary_manifest.json",
            "P03",
        )
        captured_request = {}
        original_urlopen = verifier.urllib.request.urlopen

        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return b'{"data":{"invoke":{"ok":true,"result":{"value":"BEEF"}}}}'

        def fake_urlopen(request, timeout):
            captured_request["timeout"] = timeout
            captured_request["payload"] = json.loads(request.data.decode("utf-8"))
            return FakeResponse()

        verifier.urllib.request.urlopen = fake_urlopen
        try:
            result = verifier.invoke_canary(
                "http://unused/graphql",
                canaries[0],
                0.25,
                nonce="nonce-1",
            )
        finally:
            verifier.urllib.request.urlopen = original_urlopen

        self.assertEqual(result, "BEEF")
        params = captured_request["payload"]["variables"]["params"]
        self.assertEqual(params[verifier.CANARY_NONCE_PARAM], "nonce-1")
        self.assertNotIn(verifier.CANARY_NONCE_PARAM, canaries[0].params)

    def test_verify_phase_uses_distinct_nonce_per_retry(self) -> None:
        _, canaries = verifier.load_and_validate_manifest(
            SCRIPT_DIR.parent / "testdata" / "passive_proof" / "p03_canary_manifest.json",
            "P03",
        )
        original_invoke = verifier.invoke_canary
        seen_nonces: list[str | None] = []

        def flaky_invoke(_graphql_url, _canary, _timeout_sec, nonce=None):
            seen_nonces.append(nonce)
            if len(seen_nonces) < 3:
                raise RuntimeError("timeout")
            return "BEEF"

        verifier.invoke_canary = flaky_invoke
        try:
            results = verifier.verify_phase(
                canaries=canaries[:1],
                graphql_url="http://unused/graphql",
                phase="start",
                run_id="run-retry",
                retries=3,
                timeout_sec=0.01,
                baseline_map={},
            )
        finally:
            verifier.invoke_canary = original_invoke

        self.assertEqual(results["results"][0]["status"], "pass")
        self.assertEqual(
            seen_nonces,
            [
                "run-retry:start:b524_dhw_mode:1",
                "run-retry:start:b524_dhw_mode:2",
                "run-retry:start:b524_dhw_mode:3",
            ],
        )

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
    def write_required_read_avoidance_metrics(
        self,
        proof_dir: pathlib.Path,
        *,
        start_direct_apply: float,
        start_avoided: float,
        end_direct_apply: float,
        end_avoided: float,
        start_completed_transactions: float = 10_000,
        end_completed_transactions: float = 12_000,
        start_direct_apply_candidates: float = 1_000,
        end_direct_apply_candidates: float = 1_200,
    ) -> None:
        write_metrics(
            proof_dir / "start_metrics.prom",
            [
                f'direct_apply_total{{family="B524",freshness_profile="state_fast"}} {start_direct_apply}',
                f'active_reads_avoided_total{{family="B524",freshness_profile="state_fast"}} {start_avoided}',
                'active_read_saved_seconds{family="B524",freshness_profile="state_fast"} 1',
                f"ebus_passive_completed_transactions_total {start_completed_transactions}",
                f"ebus_passive_direct_apply_candidates_evaluated_total {start_direct_apply_candidates}",
            ],
        )
        write_metrics(
            proof_dir / "end_metrics.prom",
            [
                f'direct_apply_total{{family="B524",freshness_profile="state_fast"}} {end_direct_apply}',
                f'active_reads_avoided_total{{family="B524",freshness_profile="state_fast"}} {end_avoided}',
                'active_read_saved_seconds{family="B524",freshness_profile="state_fast"} 2',
                f"ebus_passive_completed_transactions_total {end_completed_transactions}",
                f"ebus_passive_direct_apply_candidates_evaluated_total {end_direct_apply_candidates}",
            ],
        )

    def write_sample_read_avoidance_metrics(
        self,
        proof_dir: pathlib.Path,
        phase: str,
        *,
        direct_apply: float,
        avoided: float,
        saved_seconds: float = 1,
        completed_transactions: float = 11_000,
        direct_apply_candidates: float = 1_100,
    ) -> None:
        write_metrics(
            proof_dir / "samples" / f"{phase}_metrics.prom",
            [
                f'direct_apply_total{{family="B524",freshness_profile="state_fast"}} {direct_apply}',
                f'active_reads_avoided_total{{family="B524",freshness_profile="state_fast"}} {avoided}',
                f'active_read_saved_seconds{{family="B524",freshness_profile="state_fast"}} {saved_seconds}',
                f"ebus_passive_completed_transactions_total {completed_transactions}",
                f"ebus_passive_direct_apply_candidates_evaluated_total {direct_apply_candidates}",
            ],
        )

    def write_run_phase_artifacts(
        self,
        proof_dir: pathlib.Path,
        run_id: str = "run-1",
        *,
        include_interval: bool = False,
        interval_status: str = "pass",
    ) -> None:
        write_json(
            proof_dir / "canary_phase_start.json",
            {
                "run_id": run_id,
                "phase": "start",
                "results": [{"id": "a", "status": "pass"}],
            },
        )
        if include_interval:
            write_json(
                proof_dir / "canary_phase_sample_0001.json",
                {
                    "run_id": run_id,
                    "phase": "sample_0001",
                    "results": [{"id": "a", "status": interval_status}],
                },
            )
        write_json(
            proof_dir / "canary_phase_end.json",
            {
                "run_id": run_id,
                "phase": "end",
                "results": [{"id": "a", "status": "pass"}],
            },
        )

    def test_verify_phase_marks_read_avoidance_accounting_non_authoritative(self) -> None:
        _, canaries = verifier.load_and_validate_manifest(
            SCRIPT_DIR.parent / "testdata" / "passive_proof" / "p03_canary_manifest.json",
            "P03",
        )
        phase = verifier.verify_phase(
            canaries=canaries[:1],
            graphql_url="http://unused/graphql",
            phase="start",
            run_id="run-1",
            retries=3,
            timeout_sec=0.01,
            baseline_map={},
        )
        self.assertIn("read_avoidance_accounting", phase)
        self.assertFalse(phase["read_avoidance_accounting"]["authoritative"])


    def test_summary_rejects_stale_only_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            self.write_required_read_avoidance_metrics(
                proof_dir,
                start_direct_apply=1,
                start_avoided=1,
                end_direct_apply=1,
                end_avoided=1,
            )
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
            self.write_required_read_avoidance_metrics(
                proof_dir,
                start_direct_apply=1,
                start_avoided=1,
                end_direct_apply=2,
                end_avoided=2,
            )
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
            self.write_required_read_avoidance_metrics(
                proof_dir,
                start_direct_apply=1,
                start_avoided=2,
                end_direct_apply=3,
                end_avoided=4,
            )
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
            self.assertEqual(summary["read_avoidance_accounting"]["delta_totals"]["direct_apply_total"], 2.0)
            self.assertEqual(
                summary["read_avoidance_accounting"]["delta_totals"]["active_reads_avoided_total"], 2.0
            )
            self.assertEqual(
                summary["read_avoidance_accounting"]["claim_scope"],
                "bounded_proof_window_lower_bound_activity",
            )
            self.assertNotIn("excluded", summary["read_avoidance_accounting"])
            self.assertTrue(summary["proof_window_traffic_minimums"]["ok"])
            self.assertTrue(
                summary["proof_window_traffic_minimums"]["thresholds"][
                    "ebus_passive_completed_transactions_total"
                ]["ok"]
            )
            self.assertTrue(
                summary["proof_window_traffic_minimums"]["thresholds"][
                    "ebus_passive_direct_apply_candidates_evaluated_total"
                ]["ok"]
            )

    def test_summary_allows_missing_interval_when_not_required(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            self.write_required_read_avoidance_metrics(
                proof_dir,
                start_direct_apply=4,
                start_avoided=6,
                end_direct_apply=4,
                end_avoided=7,
            )
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

    def test_summary_fails_closed_when_direct_apply_counter_decreases(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            self.write_required_read_avoidance_metrics(
                proof_dir,
                start_direct_apply=10,
                start_avoided=10,
                end_direct_apply=9,
                end_avoided=20,
            )
            self.write_run_phase_artifacts(proof_dir, include_interval=False)
            with self.assertRaises(ValueError) as ctx:
                verifier.summarize_run(proof_dir, "run-1", require_interval_phase=False)
            self.assertIn("decreased", str(ctx.exception))

    def test_summary_fails_closed_when_direct_apply_metric_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_metrics(
                proof_dir / "start_metrics.prom",
                ['active_reads_avoided_total{family="B524",freshness_profile="state_fast"} 2'],
            )
            write_metrics(
                proof_dir / "end_metrics.prom",
                ['active_reads_avoided_total{family="B524",freshness_profile="state_fast"} 3'],
            )
            self.write_run_phase_artifacts(proof_dir, include_interval=False)
            with self.assertRaises(ValueError) as ctx:
                verifier.summarize_run(proof_dir, "run-1", require_interval_phase=False)
            self.assertIn("direct_apply_total", str(ctx.exception))

    def test_summary_fails_closed_when_avoided_counter_decreases(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            self.write_required_read_avoidance_metrics(
                proof_dir,
                start_direct_apply=5,
                start_avoided=10,
                end_direct_apply=6,
                end_avoided=9,
            )
            self.write_run_phase_artifacts(proof_dir, include_interval=False)
            with self.assertRaises(ValueError) as ctx:
                verifier.summarize_run(proof_dir, "run-1", require_interval_phase=False)
            self.assertIn("decreased", str(ctx.exception))

    def test_summary_fails_closed_when_avoided_delta_less_than_direct_delta(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            self.write_required_read_avoidance_metrics(
                proof_dir,
                start_direct_apply=10,
                start_avoided=10,
                end_direct_apply=15,
                end_avoided=11,
            )
            self.write_run_phase_artifacts(proof_dir, include_interval=False)
            with self.assertRaises(ValueError) as ctx:
                verifier.summarize_run(proof_dir, "run-1", require_interval_phase=False)
            self.assertIn("incoherent", str(ctx.exception))

    def test_summary_fails_closed_when_direct_apply_counter_regresses_mid_window(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            self.write_required_read_avoidance_metrics(
                proof_dir,
                start_direct_apply=10,
                start_avoided=15,
                end_direct_apply=12,
                end_avoided=18,
            )
            self.write_sample_read_avoidance_metrics(
                proof_dir,
                "sample_0001",
                direct_apply=14,
                avoided=18,
            )
            self.write_sample_read_avoidance_metrics(
                proof_dir,
                "sample_0002",
                direct_apply=11,
                avoided=19,
            )
            write_json(
                proof_dir / "canary_phase_start.json",
                {"run_id": "run-1", "phase": "start", "results": [{"id": "a", "status": "pass"}]},
            )
            write_json(
                proof_dir / "canary_phase_sample_0001.json",
                {"run_id": "run-1", "phase": "sample_0001", "results": [{"id": "a", "status": "pass"}]},
            )
            write_json(
                proof_dir / "canary_phase_sample_0002.json",
                {"run_id": "run-1", "phase": "sample_0002", "results": [{"id": "a", "status": "pass"}]},
            )
            write_json(
                proof_dir / "canary_phase_end.json",
                {"run_id": "run-1", "phase": "end", "results": [{"id": "a", "status": "pass"}]},
            )
            with self.assertRaises(ValueError) as ctx:
                verifier.summarize_run(proof_dir, "run-1", require_interval_phase=False)
            self.assertIn("decreased at phase sample_0002", str(ctx.exception))

    def test_summary_fails_closed_when_avoided_counter_regresses_mid_window(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            self.write_required_read_avoidance_metrics(
                proof_dir,
                start_direct_apply=10,
                start_avoided=15,
                end_direct_apply=12,
                end_avoided=16,
            )
            self.write_sample_read_avoidance_metrics(
                proof_dir,
                "sample_0001",
                direct_apply=12,
                avoided=20,
            )
            self.write_sample_read_avoidance_metrics(
                proof_dir,
                "sample_0002",
                direct_apply=12,
                avoided=14,
            )
            write_json(
                proof_dir / "canary_phase_start.json",
                {"run_id": "run-1", "phase": "start", "results": [{"id": "a", "status": "pass"}]},
            )
            write_json(
                proof_dir / "canary_phase_sample_0001.json",
                {"run_id": "run-1", "phase": "sample_0001", "results": [{"id": "a", "status": "pass"}]},
            )
            write_json(
                proof_dir / "canary_phase_sample_0002.json",
                {"run_id": "run-1", "phase": "sample_0002", "results": [{"id": "a", "status": "pass"}]},
            )
            write_json(
                proof_dir / "canary_phase_end.json",
                {"run_id": "run-1", "phase": "end", "results": [{"id": "a", "status": "pass"}]},
            )
            with self.assertRaises(ValueError) as ctx:
                verifier.summarize_run(proof_dir, "run-1", require_interval_phase=False)
            self.assertIn("decreased at phase sample_0002", str(ctx.exception))

    def test_summary_fails_closed_on_non_finite_read_avoidance_totals(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_metrics(
                proof_dir / "start_metrics.prom",
                [
                    'direct_apply_total{family="B524",freshness_profile="state_fast"} inf',
                    'active_reads_avoided_total{family="B524",freshness_profile="state_fast"} 1',
                ],
            )
            write_metrics(
                proof_dir / "end_metrics.prom",
                [
                    'direct_apply_total{family="B524",freshness_profile="state_fast"} inf',
                    'active_reads_avoided_total{family="B524",freshness_profile="state_fast"} 2',
                ],
            )
            self.write_run_phase_artifacts(proof_dir, include_interval=False)
            with self.assertRaises(ValueError) as ctx:
                verifier.summarize_run(proof_dir, "run-1", require_interval_phase=False)
            self.assertIn("non-finite", str(ctx.exception))

    def test_summary_fails_closed_when_completed_transactions_metric_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_metrics(
                proof_dir / "start_metrics.prom",
                [
                    'direct_apply_total{family="B524",freshness_profile="state_fast"} 1',
                    'active_reads_avoided_total{family="B524",freshness_profile="state_fast"} 1',
                    "ebus_passive_direct_apply_candidates_evaluated_total 1000",
                ],
            )
            write_metrics(
                proof_dir / "end_metrics.prom",
                [
                    'direct_apply_total{family="B524",freshness_profile="state_fast"} 2',
                    'active_reads_avoided_total{family="B524",freshness_profile="state_fast"} 2',
                    "ebus_passive_direct_apply_candidates_evaluated_total 1200",
                ],
            )
            self.write_run_phase_artifacts(proof_dir, include_interval=False)
            with self.assertRaises(ValueError) as ctx:
                verifier.summarize_run(proof_dir, "run-1", require_interval_phase=False)
            self.assertIn("ebus_passive_completed_transactions_total", str(ctx.exception))

    def test_summary_fails_closed_when_direct_apply_candidates_regress_mid_window(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            self.write_required_read_avoidance_metrics(
                proof_dir,
                start_direct_apply=10,
                start_avoided=15,
                end_direct_apply=12,
                end_avoided=18,
                start_direct_apply_candidates=1_000,
                end_direct_apply_candidates=1_050,
            )
            self.write_sample_read_avoidance_metrics(
                proof_dir,
                "sample_0001",
                direct_apply=11,
                avoided=16,
                direct_apply_candidates=1_200,
            )
            self.write_sample_read_avoidance_metrics(
                proof_dir,
                "sample_0002",
                direct_apply=12,
                avoided=17,
                direct_apply_candidates=900,
            )
            write_json(
                proof_dir / "canary_phase_start.json",
                {"run_id": "run-1", "phase": "start", "results": [{"id": "a", "status": "pass"}]},
            )
            write_json(
                proof_dir / "canary_phase_sample_0001.json",
                {"run_id": "run-1", "phase": "sample_0001", "results": [{"id": "a", "status": "pass"}]},
            )
            write_json(
                proof_dir / "canary_phase_sample_0002.json",
                {"run_id": "run-1", "phase": "sample_0002", "results": [{"id": "a", "status": "pass"}]},
            )
            write_json(
                proof_dir / "canary_phase_end.json",
                {"run_id": "run-1", "phase": "end", "results": [{"id": "a", "status": "pass"}]},
            )
            with self.assertRaises(ValueError) as ctx:
                verifier.summarize_run(proof_dir, "run-1", require_interval_phase=False)
            self.assertIn("ebus_passive_direct_apply_candidates_evaluated_total", str(ctx.exception))
            self.assertIn("decreased at phase sample_0002", str(ctx.exception))

    def test_summary_reports_proof_window_threshold_boundaries(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            self.write_required_read_avoidance_metrics(
                proof_dir,
                start_direct_apply=1,
                start_avoided=2,
                end_direct_apply=2,
                end_avoided=3,
                start_completed_transactions=100,
                end_completed_transactions=1_100,
                start_direct_apply_candidates=50,
                end_direct_apply_candidates=150,
            )
            self.write_run_phase_artifacts(proof_dir, include_interval=False)
            summary = verifier.summarize_run(proof_dir, "run-1", require_interval_phase=False)
            thresholds = summary["proof_window_traffic_minimums"]["thresholds"]
            self.assertTrue(summary["proof_window_traffic_minimums"]["ok"])
            self.assertTrue(thresholds["ebus_passive_completed_transactions_total"]["ok"])
            self.assertTrue(thresholds["ebus_passive_direct_apply_candidates_evaluated_total"]["ok"])

    def test_summary_reports_proof_window_threshold_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            self.write_required_read_avoidance_metrics(
                proof_dir,
                start_direct_apply=1,
                start_avoided=2,
                end_direct_apply=2,
                end_avoided=3,
                start_completed_transactions=100,
                end_completed_transactions=1_099,
                start_direct_apply_candidates=50,
                end_direct_apply_candidates=149,
            )
            self.write_run_phase_artifacts(proof_dir, include_interval=False)
            summary = verifier.summarize_run(proof_dir, "run-1", require_interval_phase=False)
            thresholds = summary["proof_window_traffic_minimums"]["thresholds"]
            self.assertFalse(summary["proof_window_traffic_minimums"]["ok"])
            self.assertFalse(thresholds["ebus_passive_completed_transactions_total"]["ok"])
            self.assertFalse(thresholds["ebus_passive_direct_apply_candidates_evaluated_total"]["ok"])


class CanaryVerdictTests(unittest.TestCase):
    def build_summary_payload(
        self,
        *,
        mismatch_count: int,
        interval_required: bool,
        interval_results: int,
        interval_conclusive: int,
        per_canary_interval: dict[str, dict[str, int]],
        completed_transactions_delta: float = 1_200,
        direct_apply_candidates_delta: float = 120,
    ) -> dict:
        per_canary = {canary_id: {"last_status": "pass"} for canary_id in per_canary_interval}
        return {
            "schema": "p03_canary_overall_summary_v1",
            "run_id": "run-1",
            "read_avoidance_accounting": {
                "delta_totals": {
                    "direct_apply_total": 1,
                    "active_reads_avoided_total": 2,
                }
            },
            "proof_window_traffic_minimums": {
                "delta_totals": {
                    "ebus_passive_completed_transactions_total": completed_transactions_delta,
                    "ebus_passive_direct_apply_candidates_evaluated_total": direct_apply_candidates_delta,
                }
            },
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

    def test_verdict_fails_closed_when_read_avoidance_accounting_is_missing(self) -> None:
        summary = self.build_summary_payload(
            mismatch_count=0,
            interval_required=False,
            interval_results=0,
            interval_conclusive=0,
            per_canary_interval={"a": {"pass": 0, "mismatch": 0, "inconclusive": 0, "conclusive": 0}},
        )
        summary.pop("read_avoidance_accounting", None)
        verdict = verifier.build_canary_verdict(summary)
        self.assertFalse(verdict["ok"])
        self.assertFalse(verdict["criteria"]["read_avoidance_accounting"]["ok"])

    def test_verdict_fails_when_read_avoidance_delta_is_negative(self) -> None:
        summary = self.build_summary_payload(
            mismatch_count=0,
            interval_required=False,
            interval_results=0,
            interval_conclusive=0,
            per_canary_interval={"a": {"pass": 0, "mismatch": 0, "inconclusive": 0, "conclusive": 0}},
        )
        summary["read_avoidance_accounting"]["delta_totals"]["direct_apply_total"] = -1
        verdict = verifier.build_canary_verdict(summary)
        self.assertFalse(verdict["ok"])
        self.assertIn("invalid", verdict["criteria"]["read_avoidance_accounting"]["reason"])

    def test_verdict_fails_when_read_avoidance_delta_is_non_finite(self) -> None:
        summary = self.build_summary_payload(
            mismatch_count=0,
            interval_required=False,
            interval_results=0,
            interval_conclusive=0,
            per_canary_interval={"a": {"pass": 0, "mismatch": 0, "inconclusive": 0, "conclusive": 0}},
        )
        summary["read_avoidance_accounting"]["delta_totals"]["active_reads_avoided_total"] = float("inf")
        verdict = verifier.build_canary_verdict(summary)
        self.assertFalse(verdict["ok"])
        self.assertIn("invalid", verdict["criteria"]["read_avoidance_accounting"]["reason"])

    def test_verdict_fails_closed_when_proof_window_traffic_minimums_are_missing(self) -> None:
        summary = self.build_summary_payload(
            mismatch_count=0,
            interval_required=False,
            interval_results=0,
            interval_conclusive=0,
            per_canary_interval={"a": {"pass": 0, "mismatch": 0, "inconclusive": 0, "conclusive": 0}},
        )
        summary.pop("proof_window_traffic_minimums", None)
        verdict = verifier.build_canary_verdict(summary)
        self.assertFalse(verdict["ok"])
        self.assertFalse(verdict["criteria"]["proof_window_traffic_minimums"]["ok"])

    def test_verdict_fails_when_proof_window_traffic_minimums_are_below_threshold(self) -> None:
        summary = self.build_summary_payload(
            mismatch_count=0,
            interval_required=False,
            interval_results=0,
            interval_conclusive=0,
            per_canary_interval={"a": {"pass": 0, "mismatch": 0, "inconclusive": 0, "conclusive": 0}},
            completed_transactions_delta=999,
            direct_apply_candidates_delta=99,
        )
        verdict = verifier.build_canary_verdict(summary)
        self.assertFalse(verdict["ok"])
        self.assertFalse(verdict["criteria"]["proof_window_traffic_minimums"]["ok"])
        self.assertIn("minimum not met", verdict["criteria"]["proof_window_traffic_minimums"]["reason"])


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
                      if [[ "${phase}" == "end" && "${FAKE_CANARY_END_DELAY_SEC:-0}" != "0" ]]; then
                        sleep "${FAKE_CANARY_END_DELAY_SEC}"
                      fi
                      status="${FAKE_CANARY_STATUS:-pass}"
                      pass_count=0
                      mismatch_count=0
                      if [[ "${status}" == "pass" ]]; then
                        pass_count=1
                      elif [[ "${status}" == "mismatch" ]]; then
                        mismatch_count=1
                      fi
                      if [[ -n "${FAKE_CANARY_PHASE_LOG:-}" ]]; then
                        metrics_count="unknown"
                        if [[ -n "${FAKE_METRICS_STATE_FILE:-}" && -f "${FAKE_METRICS_STATE_FILE}" ]]; then
                          metrics_count="$(cat "${FAKE_METRICS_STATE_FILE}")"
                        fi
                        printf '%s:%s\\n' "${phase}" "${metrics_count}" >> "${FAKE_CANARY_PHASE_LOG}"
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
                      timed_out=0
                      metrics_mode="${FAKE_METRICS_MODE:-always_healthy}"
                      metrics_quality="${FAKE_METRICS_QUALITY_MODE:-healthy}"
                      state_file="${FAKE_METRICS_STATE_FILE:?FAKE_METRICS_STATE_FILE is required}"
                      metrics_count=0
                      if [[ -f "${state_file}" ]]; then
                        metrics_count="$(cat "${state_file}")"
                      fi
                      metrics_count=$((metrics_count + 1))
                      printf '%s\\n' "${metrics_count}" > "${state_file}"
                      completed_total=$((metrics_count * 1200))
                      candidates_total=$((metrics_count * 120))

                      if [[ "${metrics_quality}" == "missing_read_avoidance" ]]; then
                        cat <<EOF
                    ebus_passive_capability_probe_outcomes_total{outcome="timed_out"} 0
                    ebus_passive_tap_connected 1
                    ebus_passive_warmup_state{state="available"} 1
                    ebus_passive_capability_probe_outcomes_total{outcome="confirmed"} 1
                    EOF
                        exit 0
                      fi
                      if [[ "${metrics_quality}" == "corrupt_read_avoidance" ]]; then
                        cat <<EOF
                    ebus_passive_capability_probe_outcomes_total{outcome="timed_out"} 0
                    ebus_passive_tap_connected 1
                    ebus_passive_warmup_state{state="available"} 1
                    ebus_passive_capability_probe_outcomes_total{outcome="confirmed"} 1
                    direct_apply_total{family="B524",freshness_profile="state_fast"} not_a_number
                    active_reads_avoided_total{family="B524",freshness_profile="state_fast"} 1
                    ebus_passive_completed_transactions_total ${completed_total}
                    ebus_passive_direct_apply_candidates_evaluated_total ${candidates_total}
                    EOF
                        exit 0
                      fi
                      if [[ "${metrics_mode}" == "healthy_once_then_bad" ]]; then
                        healthy_calls="${FAKE_METRICS_HEALTHY_CALLS:-1}"
                        if [[ "${metrics_count}" -gt "${healthy_calls}" ]]; then
                          timed_out=1
                        fi
                      elif [[ "${metrics_mode}" == "healthy_then_hard_fail_then_healthy" ]]; then
                        healthy_before_fail_calls="${FAKE_METRICS_HEALTHY_BEFORE_FAIL_CALLS:-2}"
                        hard_fail_calls="${FAKE_METRICS_HARD_FAIL_CALLS:-3}"
                        fail_start=$((healthy_before_fail_calls + 1))
                        fail_end=$((healthy_before_fail_calls + hard_fail_calls))
                        if [[ "${metrics_count}" -ge "${fail_start}" && "${metrics_count}" -le "${fail_end}" ]]; then
                          echo "simulated hard metrics poll failure on call ${metrics_count}" >&2
                          exit 28
                        fi
                      elif [[ "${metrics_mode}" == "initially_unhealthy_then_healthy" ]]; then
                        unhealthy_calls="${FAKE_METRICS_UNHEALTHY_CALLS:-1}"
                        if [[ "${metrics_count}" -le "${unhealthy_calls}" ]]; then
                          cat <<EOF
                    ebus_passive_capability_probe_outcomes_total{outcome="timed_out"} 0
                    ebus_passive_tap_connected 1
                    ebus_passive_warmup_state{state="available"} 0
                    ebus_passive_capability_probe_outcomes_total{outcome="confirmed"} 0
                    direct_apply_total{family="B524",freshness_profile="state_fast"} 2
                    active_reads_avoided_total{family="B524",freshness_profile="state_fast"} 3
                    active_read_saved_seconds{family="B524",freshness_profile="state_fast"} 1
                    ebus_passive_completed_transactions_total ${completed_total}
                    ebus_passive_direct_apply_candidates_evaluated_total ${candidates_total}
                    EOF
                          exit 0
                        fi
                      fi
                      cat <<EOF
                    ebus_passive_capability_probe_outcomes_total{outcome="timed_out"} ${timed_out}
                    ebus_passive_tap_connected 1
                    ebus_passive_warmup_state{state="available"} 1
                    ebus_passive_capability_probe_outcomes_total{outcome="confirmed"} 1
                    direct_apply_total{family="B524",freshness_profile="state_fast"} 2
                    active_reads_avoided_total{family="B524",freshness_profile="state_fast"} 3
                    active_read_saved_seconds{family="B524",freshness_profile="state_fast"} 1
                    ebus_passive_completed_transactions_total ${completed_total}
                    ebus_passive_direct_apply_candidates_evaluated_total ${candidates_total}
                    EOF
                      exit 0
                    fi

                    if [[ "${url}" == *"/portal/api/v1/bus/observability" ]]; then
                      startup_mode="${FAKE_BUS_STARTUP_MODE:-always_live_ready}"
                      phase="LIVE_READY"
                      if [[ "${startup_mode}" == "initially_live_warmup_then_live_ready" ]]; then
                        state_file="${FAKE_METRICS_STATE_FILE:?FAKE_METRICS_STATE_FILE is required}"
                        warmup_calls="${FAKE_BUS_LIVE_WARMUP_CALLS:-1}"
                        metrics_count=0
                        if [[ -f "${state_file}" ]]; then
                          metrics_count="$(cat "${state_file}")"
                        fi
                        if [[ "${metrics_count}" -le "${warmup_calls}" ]]; then
                          phase="LIVE_WARMUP"
                        fi
                      fi
                      cat <<EOF
                    {"status":{"startup":{"phase":"${phase}"},"feature_flags":{"observeFirstEnabled":true}}}
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
                    "FAKE_CANARY_PHASE_LOG": str(temp_path / "fake_canary_phase_log.txt"),
                    "FAKE_METRICS_STATE_FILE": str(temp_path / "fake_metrics_count.txt"),
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
                phase_log_path = temp_path / "fake_canary_phase_log.txt"
                if summary_path.exists():
                    artifacts["summary"] = json.loads(summary_path.read_text(encoding="utf-8"))
                if verdict_path.exists():
                    artifacts["verdict"] = json.loads(verdict_path.read_text(encoding="utf-8"))
                if phase_log_path.exists():
                    artifacts["phase_log"] = [
                        line.strip()
                        for line in phase_log_path.read_text(encoding="utf-8").splitlines()
                        if line.strip()
                    ]
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

    def test_smoke_defers_start_canary_until_first_healthy_snapshot(self) -> None:
        result, artifacts = self.run_smoke_with_fake_tools_detailed(
            "pass",
            extra_env={
                "PASSIVE_PROOF_HOLD_SEC": "4",
                "PASSIVE_SMOKE_TIMEOUT_SEC": "10",
                "PASSIVE_SMOKE_POLL_INTERVAL_SEC": "1",
                "PASSIVE_PROOF_SAMPLE_INTERVAL_SEC": "3600",
                "FAKE_METRICS_MODE": "initially_unhealthy_then_healthy",
                "FAKE_METRICS_UNHEALTHY_CALLS": "2",
            },
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        phase_log = artifacts.get("phase_log")
        self.assertIsInstance(phase_log, list)
        self.assertGreaterEqual(len(phase_log), 1)
        self.assertEqual(phase_log[0].split(":", 1)[0], "start")
        self.assertGreaterEqual(int(phase_log[0].split(":", 1)[1]), 3)

    def test_smoke_fails_when_read_avoidance_metrics_are_missing(self) -> None:
        result = self.run_smoke_with_fake_tools(
            "pass",
            extra_env={
                "PASSIVE_PROOF_HOLD_SEC": "0",
                "PASSIVE_SMOKE_TIMEOUT_SEC": "6",
                "PASSIVE_SMOKE_POLL_INTERVAL_SEC": "1",
                "FAKE_METRICS_QUALITY_MODE": "missing_read_avoidance",
            },
        )
        self.assertNotEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("proof mode: failed to build canary summary", result.stderr)

    def test_smoke_fails_when_read_avoidance_metrics_are_corrupt(self) -> None:
        result = self.run_smoke_with_fake_tools(
            "pass",
            extra_env={
                "PASSIVE_PROOF_HOLD_SEC": "0",
                "PASSIVE_SMOKE_TIMEOUT_SEC": "6",
                "PASSIVE_SMOKE_POLL_INTERVAL_SEC": "1",
                "FAKE_METRICS_QUALITY_MODE": "corrupt_read_avoidance",
            },
        )
        self.assertNotEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("proof mode: failed to build canary summary", result.stderr)

    def test_smoke_defers_start_canary_until_bus_startup_phase_is_live_ready(self) -> None:
        result, artifacts = self.run_smoke_with_fake_tools_detailed(
            "pass",
            extra_env={
                "PASSIVE_PROOF_HOLD_SEC": "4",
                "PASSIVE_SMOKE_TIMEOUT_SEC": "8",
                "PASSIVE_SMOKE_POLL_INTERVAL_SEC": "1",
                "PASSIVE_PROOF_SAMPLE_INTERVAL_SEC": "3600",
                "FAKE_METRICS_MODE": "initially_unhealthy_then_healthy",
                "FAKE_METRICS_UNHEALTHY_CALLS": "0",
                "FAKE_BUS_STARTUP_MODE": "initially_live_warmup_then_live_ready",
                "FAKE_BUS_LIVE_WARMUP_CALLS": "2",
            },
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        phase_log = artifacts.get("phase_log")
        self.assertIsInstance(phase_log, list)
        self.assertGreaterEqual(len(phase_log), 1)
        self.assertEqual(phase_log[0].split(":", 1)[0], "start")
        self.assertGreaterEqual(int(phase_log[0].split(":", 1)[1]), 3)

    def test_smoke_hold_zero_still_waits_for_deferred_start_canary(self) -> None:
        result, artifacts = self.run_smoke_with_fake_tools_detailed(
            "pass",
            extra_env={
                "PASSIVE_PROOF_HOLD_SEC": "0",
                "PASSIVE_SMOKE_TIMEOUT_SEC": "8",
                "PASSIVE_SMOKE_POLL_INTERVAL_SEC": "1",
                "FAKE_METRICS_MODE": "initially_unhealthy_then_healthy",
                "FAKE_METRICS_UNHEALTHY_CALLS": "0",
                "FAKE_BUS_STARTUP_MODE": "initially_live_warmup_then_live_ready",
                "FAKE_BUS_LIVE_WARMUP_CALLS": "2",
            },
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        phase_log = artifacts.get("phase_log")
        self.assertIsInstance(phase_log, list)
        self.assertGreaterEqual(len(phase_log), 2)
        self.assertEqual(phase_log[0].split(":", 1)[0], "start")
        self.assertGreaterEqual(int(phase_log[0].split(":", 1)[1]), 3)
        self.assertEqual(phase_log[-1].split(":", 1)[0], "end")
        self.assertEqual(artifacts.get("sample_phase_files", []), [])

    def test_smoke_hold_zero_waits_for_first_interval_when_interval_phase_is_required(self) -> None:
        result, artifacts = self.run_smoke_with_fake_tools_detailed(
            "pass",
            extra_env={
                "PASSIVE_PROOF_HOLD_SEC": "0",
                "PASSIVE_SMOKE_TIMEOUT_SEC": "8",
                "PASSIVE_SMOKE_POLL_INTERVAL_SEC": "1",
                "PASSIVE_PROOF_SAMPLE_INTERVAL_SEC": "1",
                "FAKE_METRICS_MODE": "initially_unhealthy_then_healthy",
                "FAKE_METRICS_UNHEALTHY_CALLS": "0",
                "FAKE_BUS_STARTUP_MODE": "initially_live_warmup_then_live_ready",
                "FAKE_BUS_LIVE_WARMUP_CALLS": "2",
            },
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        phase_log = artifacts.get("phase_log")
        self.assertIsInstance(phase_log, list)
        self.assertGreaterEqual(len(phase_log), 3)
        self.assertEqual(phase_log[0].split(":", 1)[0], "start")
        self.assertIn("sample_0001", [entry.split(":", 1)[0] for entry in phase_log])
        self.assertEqual(phase_log[-1].split(":", 1)[0], "end")
        self.assertEqual(artifacts.get("sample_phase_files", []), ["canary_phase_sample_0001.json"])

    def test_smoke_fails_when_hold_times_out_before_window_completion_despite_slow_cleanup(self) -> None:
        result, artifacts = self.run_smoke_with_fake_tools_detailed(
            "pass",
            extra_env={
                "PASSIVE_PROOF_HOLD_SEC": "8",
                "PASSIVE_SMOKE_TIMEOUT_SEC": "5",
                "PASSIVE_SMOKE_POLL_INTERVAL_SEC": "1",
                "PASSIVE_PROOF_SAMPLE_INTERVAL_SEC": "1",
                "FAKE_CANARY_END_DELAY_SEC": "4",
            },
        )
        self.assertNotEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("timed out waiting", result.stderr)
        self.assertGreaterEqual(artifacts["elapsed_sec"], 4.0, msg=result.stderr)
        summary = artifacts.get("summary")
        self.assertIsInstance(summary, dict)
        self.assertGreaterEqual(summary["interval_phase_count"], 1)

    def test_smoke_fails_when_health_is_healthy_once_then_bad_forever(self) -> None:
        result, artifacts = self.run_smoke_with_fake_tools_detailed(
            "pass",
            extra_env={
                "PASSIVE_PROOF_HOLD_SEC": "4",
                "PASSIVE_SMOKE_TIMEOUT_SEC": "6",
                "PASSIVE_SMOKE_POLL_INTERVAL_SEC": "1",
                "PASSIVE_PROOF_SAMPLE_INTERVAL_SEC": "1",
                "FAKE_METRICS_MODE": "healthy_once_then_bad",
                "FAKE_METRICS_HEALTHY_CALLS": "2",
            },
        )
        self.assertNotEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("timed out waiting", result.stderr)
        summary = artifacts.get("summary")
        self.assertIsInstance(summary, dict)
        self.assertGreaterEqual(summary["interval_phase_count"], 1)

    def test_smoke_fails_when_hard_poll_failures_interrupt_proof_window(self) -> None:
        result, artifacts = self.run_smoke_with_fake_tools_detailed(
            "pass",
            extra_env={
                "PASSIVE_PROOF_HOLD_SEC": "6",
                "PASSIVE_SMOKE_TIMEOUT_SEC": "8",
                "PASSIVE_SMOKE_POLL_INTERVAL_SEC": "1",
                "PASSIVE_PROOF_SAMPLE_INTERVAL_SEC": "1",
                "FAKE_METRICS_MODE": "healthy_then_hard_fail_then_healthy",
                "FAKE_METRICS_HEALTHY_BEFORE_FAIL_CALLS": "2",
                "FAKE_METRICS_HARD_FAIL_CALLS": "3",
            },
        )
        self.assertNotEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("timed out waiting", result.stderr)
        summary = artifacts.get("summary")
        self.assertIsInstance(summary, dict)
        self.assertGreaterEqual(summary["interval_phase_count"], 1)


if __name__ == "__main__":
    unittest.main()
