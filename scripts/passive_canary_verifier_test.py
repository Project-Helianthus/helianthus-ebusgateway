#!/usr/bin/env python3
import json
import pathlib
import sys
import tempfile
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


if __name__ == "__main__":
    unittest.main()
