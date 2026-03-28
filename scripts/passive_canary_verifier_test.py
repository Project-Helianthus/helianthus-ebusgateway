#!/usr/bin/env python3
import argparse
import contextlib
import io
import json
import os
import pathlib
import subprocess
import shutil
import sys
import tempfile
import textwrap
import time
import unittest

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import passive_canary_verifier as verifier  # noqa: E402

CANONICAL_NO_EBUSD_TRANSPORT = verifier.CANONICAL_NO_EBUSD_TRANSPORT


def write_json(path: pathlib.Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_metrics(path: pathlib.Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def canonical_feature_flags(
    *,
    observe_first_enabled: bool = True,
    passive_state_direct_apply: bool = False,
    passive_config_direct_apply: bool = False,
    external_write_policy: str = "record_only",
    normalizations: object = (),
) -> dict:
    return {
        "observeFirstEnabled": observe_first_enabled,
        "passiveStateDirectApply": passive_state_direct_apply,
        "passiveConfigDirectApply": passive_config_direct_apply,
        "externalWritePolicy": external_write_policy,
        "normalizations": normalizations,
    }


def phase_timestamps(phase: str) -> dict[str, dict[str, str]]:
    if phase == "start":
        bus_time = "2026-03-28T00:00:00Z"
        graphql_time = "2026-03-28T00:00:01Z"
        watch_time = "2026-03-28T00:00:02Z"
    elif phase == "end":
        bus_time = "2026-03-28T00:05:00Z"
        graphql_time = "2026-03-28T00:05:01Z"
        watch_time = "2026-03-28T00:05:02Z"
    elif phase.startswith("sample_"):
        suffix = phase.removeprefix("sample_")
        if not suffix.isdigit():
            raise ValueError(f"unsupported phase {phase}")
        minute = int(suffix) + 1
        bus_time = f"2026-03-28T00:{minute:02d}:00Z"
        graphql_time = f"2026-03-28T00:{minute:02d}:01Z"
        watch_time = f"2026-03-28T00:{minute:02d}:02Z"
    else:
        raise ValueError(f"unsupported phase {phase}")
    return {
        "bus_observability": {
            "summary_last_updated_at": bus_time,
            "status_last_updated_at": bus_time,
            "startup_last_updated_at": bus_time,
            "feature_flags_last_updated_at": bus_time,
        },
        "graphql_bus_watch": {
            "summary_last_updated_at": graphql_time,
            "status_last_updated_at": graphql_time,
            "startup_last_updated_at": graphql_time,
            "feature_flags_last_updated_at": graphql_time,
            "watch_summary_last_updated_at": watch_time,
        },
    }


def publisher_cadence_snapshot(phase: str, cadence_sec: float = 3600.0) -> dict[str, object]:
    timestamps = phase_timestamps(phase)
    return {
        "publisher_cadence_sec": cadence_sec,
        "publisher_cadence_source": "config.semantic_state_interval",
        "graphql_publisher_cadence_sec": cadence_sec,
        "graphql_publisher_cadence_source": "config.semantic_state_interval",
        "last_updated_at": timestamps["bus_observability"]["status_last_updated_at"],
        "graphql_last_updated_at": timestamps["graphql_bus_watch"]["status_last_updated_at"],
    }


def bus_feature_flags_snapshot(phase: str) -> dict:
    flags = canonical_feature_flags()
    flags["last_updated_at"] = phase_timestamps(phase)["bus_observability"]["feature_flags_last_updated_at"]
    return flags


def graphql_feature_flags_snapshot(phase: str) -> dict:
    flags = canonical_feature_flags()
    flags["lastUpdatedAt"] = phase_timestamps(phase)["graphql_bus_watch"]["feature_flags_last_updated_at"]
    return flags


def watch_summary_snapshot(phase: str) -> dict:
    return {
        "lastUpdatedAt": phase_timestamps(phase)["graphql_bus_watch"]["watch_summary_last_updated_at"],
        "inventory": {"totalEntries": 1},
        "activationCounts": {"catalogDescriptors": 1, "activeKeys": 1, "sourceClasses": []},
        "directApplyEligibilityClasses": [],
        "degraded": {
            "active": False,
            "shadowingEnabled": False,
            "pinnedBudgetDegraded": False,
            "compactorDegraded": False,
            "reasons": [],
        },
    }


def canonical_family_proof_canary_ids() -> list[str]:
    _, canaries = verifier.load_and_validate_manifest(
        SCRIPT_DIR.parent / "testdata" / "passive_proof" / "p03_canary_manifest.json",
        "P03",
    )
    return [item.canary_id for item in canaries]


def write_replay_behavior_artifact(proof_dir: pathlib.Path) -> dict:
    artifact = {
        "schema": "observe_first_replay_behavior_v1",
        "captured_at": "2026-03-28T00:00:00+00:00",
        "source": "go_replay_harness",
        "ok": True,
        "summary": {
            "total_cases": 3,
            "locked_cases": 3,
            "observed_cases": 3,
            "observation_failure_cases": 0,
        },
        "cases": [
            {
                "name": "b524_value_bearing_enh",
                "status": "observed",
                "reason": "observed B524 runtime observer fallback produced unmatched third-party disposition",
                "observed": {
                    "direct_apply": False,
                    "disposition": "ambiguity",
                    "raw_disposition": "unmatched_third_party",
                    "third_party_eligible": True,
                    "direct_apply_policy": "state_default",
                    "replay_harness": "active_passive_deduplicator",
                },
            },
            {
                "name": "collision_episode",
                "status": "observed",
                "reason": "observed proxy-observer collision stream produced no direct-apply replay path",
                "observed": {
                    "direct_apply": False,
                    "disposition": "falsification",
                    "transaction_events": 0,
                    "observed_symbols": 8,
                    "completed_transactions": 0,
                    "passive_state": "warming_up",
                    "replay_harness": "proxy_ens_observer",
                },
            },
            {
                "name": "timeout_no_progress",
                "status": "observed",
                "reason": "observed truncated observer stream produced no progress and no direct-apply path",
                "observed": {
                    "direct_apply": False,
                    "disposition": "falsification",
                    "transaction_events": 0,
                    "observed_symbols": 2,
                    "completed_transactions": 0,
                    "passive_state": "warming_up",
                    "replay_harness": "proxy_ens_observer_timeout",
                },
            },
        ],
    }
    write_json(proof_dir / "replay_behavior.json", artifact)
    return artifact


def write_structured_warmup_snapshot_bundle(
    proof_dir: pathlib.Path,
    phase: str,
    *,
    startup_phase: str,
    warmup_state: str,
    cache_epoch: int = 1,
    live_epoch: int = 1,
    transport_class: str = "ens",
    publisher_cadence_sec: float = 3600.0,
    publisher_cadence_source: str = "config.semantic_state_interval",
    graphql_feature_flags: dict | None = None,
    bus_feature_flags: dict | None = None,
) -> None:
    if graphql_feature_flags is None:
        graphql_feature_flags = graphql_feature_flags_snapshot(phase)
    else:
        graphql_feature_flags = dict(graphql_feature_flags)
        graphql_feature_flags.setdefault(
            "lastUpdatedAt",
            phase_timestamps(phase)["graphql_bus_watch"]["feature_flags_last_updated_at"],
        )
    if bus_feature_flags is None:
        bus_feature_flags = bus_feature_flags_snapshot(phase)
    else:
        bus_feature_flags = dict(bus_feature_flags)
        bus_feature_flags.setdefault(
            "last_updated_at",
            phase_timestamps(phase)["bus_observability"]["feature_flags_last_updated_at"],
        )
    snapshot_dir = proof_dir if phase in ("start", "end") else proof_dir / "samples"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    write_json(
        snapshot_dir / f"{phase}_bus_observability.json",
        {
            "summary": {
                "last_updated_at": phase_timestamps(phase)["bus_observability"]["summary_last_updated_at"],
                "status": {
                    "last_updated_at": phase_timestamps(phase)["bus_observability"]["status_last_updated_at"],
                    "startup": {
                        "phase": startup_phase,
                        "cache_epoch": cache_epoch,
                        "live_epoch": live_epoch,
                        "last_updated_at": phase_timestamps(phase)["bus_observability"]["startup_last_updated_at"],
                    },
                    "publisher_cadence_sec": publisher_cadence_sec,
                    "publisher_cadence_source": publisher_cadence_source,
                    "feature_flags": bus_feature_flags,
                }
            }
        },
    )
    write_json(
        snapshot_dir / f"{phase}_graphql_bus_watch.json",
        {
            "data": {
                "busSummary": {
                    "lastUpdatedAt": phase_timestamps(phase)["graphql_bus_watch"]["summary_last_updated_at"],
                    "status": {
                        "lastUpdatedAt": phase_timestamps(phase)["graphql_bus_watch"]["status_last_updated_at"],
                        "transportClass": transport_class,
                        "startup": {
                            "phase": startup_phase,
                            "cacheEpoch": cache_epoch,
                            "liveEpoch": live_epoch,
                            "lastUpdatedAt": phase_timestamps(phase)["graphql_bus_watch"]["startup_last_updated_at"],
                        },
                        "publisherCadenceSec": publisher_cadence_sec,
                        "publisherCadenceSource": publisher_cadence_source,
                        "warmup": {
                            "state": warmup_state,
                            "blocker": "",
                            "elapsedSeconds": 0.0,
                            "completedTransactions": 0,
                            "requiredTransactions": 0,
                            "completionMode": "proof_window",
                        },
                        "featureFlags": graphql_feature_flags,
                    }
                },
                "watchSummary": watch_summary_snapshot(phase),
            }
        },
    )
    write_json(
        snapshot_dir / f"{phase}_feature_flags.json",
        {
            "captured_at": "2026-03-28T00:00:00+00:00",
            "graphql_feature_flags": graphql_feature_flags,
            "bus_observability_feature_flags": bus_feature_flags,
        },
    )


def write_family_proof_artifacts(
    proof_dir: pathlib.Path,
    *,
    transport_class: str = "ens",
    kind: str = "proxy-single-client",
    passive_mode: str = "required",
) -> dict:
    write_metrics(
        proof_dir / "start_metrics.prom",
        [
            'ebus_passive_capability_probe_outcomes_total{outcome="timed_out"} 0',
            'ebus_passive_tap_connected 1',
            'ebus_passive_warmup_state{state="warming_up"} 1',
            'ebus_passive_capability_probe_outcomes_total{outcome="confirmed"} 1',
        ],
    )
    write_metrics(
        proof_dir / "end_metrics.prom",
        [
            'ebus_passive_capability_probe_outcomes_total{outcome="timed_out"} 0',
            'ebus_passive_tap_connected 1',
            'ebus_passive_warmup_state{state="available"} 1',
            'ebus_passive_capability_probe_outcomes_total{outcome="confirmed"} 1',
        ],
    )
    write_structured_warmup_snapshot_bundle(
        proof_dir,
        "start",
        startup_phase="LIVE_WARMUP",
        warmup_state="warming_up",
        cache_epoch=1,
        live_epoch=0,
        transport_class=transport_class,
    )
    write_structured_warmup_snapshot_bundle(
        proof_dir,
        "end",
        startup_phase="LIVE_READY",
        warmup_state="available",
        cache_epoch=1,
        live_epoch=1,
        transport_class=transport_class,
    )

    summary_builder = CanaryVerdictTests("runTest")
    canary_ids = canonical_family_proof_canary_ids()
    per_canary_interval = {
        canary_id: {"pass": 9, "mismatch": 0, "inconclusive": 1, "conclusive": 9}
        for canary_id in canary_ids
    }
    summary = summary_builder.build_summary_payload(
        mismatch_count=0,
        interval_required=True,
        interval_results=10 * len(canary_ids),
        interval_conclusive=9 * len(canary_ids),
        per_canary_interval=per_canary_interval,
        transport_class=transport_class,
    )
    verdict = verifier.build_canary_verdict(summary)
    write_json(proof_dir / "canary_summary.json", summary)
    write_json(proof_dir / "canary_verdict.json", verdict)
    write_replay_behavior_artifact(proof_dir)
    corpus = verifier.load_json(SCRIPT_DIR.parent / "testdata" / "observe_first_replay_cases.json")
    replay_verdict = verifier.build_replay_falsification_verdict(corpus, proof_dir)
    write_json(proof_dir / "replay_falsification.json", replay_verdict)
    return {
        "summary": summary,
        "verdict": verdict,
        "replay_verdict": replay_verdict,
        "kind": kind,
        "passive_mode": passive_mode,
        "transport_class": transport_class,
    }


def write_family_proof_eligibility_artifact(
    proof_dir: pathlib.Path,
    *,
    run_id: str = "run-1",
    case_id: str = "P03",
    kind: str = "proxy-single-client",
    passive_mode: str = "required",
    gateway_transport: str = "ens",
    proxy_transport: str = "ens",
    ebusd_transport: str = "ebusd-tcp",
) -> dict:
    artifact = verifier.build_family_proof_eligibility_artifact_for_run(
        proof_dir,
        run_id,
        case_id,
        kind,
        passive_mode,
        gateway_transport,
        proxy_transport=proxy_transport,
        ebusd_transport=ebusd_transport,
    )
    write_json(proof_dir / "family_proof_eligibility.json", artifact)
    return artifact


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
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_replay_behavior_artifact(proof_dir)
            verdict = verifier.build_replay_falsification_verdict(corpus, proof_dir)

        self.assertEqual(verdict["schema"], verifier.REPLAY_FALSIFICATION_VERDICT_SCHEMA)
        self.assertTrue(verdict["ok"])
        self.assertEqual(verdict["summary"]["locked_cases"], 3)
        self.assertEqual(verdict["summary"]["fail"], 0)
        self.assertTrue(verdict["summary"]["behavior_artifact_ok"])

        by_name = {case["name"]: case for case in verdict["cases"]}
        self.assertEqual(by_name["b524_value_bearing_enh"]["status"], "pass")
        self.assertFalse(by_name["b524_value_bearing_enh"]["direct_apply"])
        self.assertEqual(by_name["b524_value_bearing_enh"]["disposition"], "ambiguity")
        self.assertEqual(by_name["b524_value_bearing_enh"]["observed"]["replay_harness"], "active_passive_deduplicator")
        self.assertEqual(by_name["collision_episode"]["status"], "pass")
        self.assertFalse(by_name["collision_episode"]["direct_apply"])
        self.assertEqual(by_name["collision_episode"]["disposition"], "falsification")
        self.assertEqual(by_name["timeout_no_progress"]["status"], "pass")
        self.assertFalse(by_name["timeout_no_progress"]["direct_apply"])
        self.assertEqual(by_name["timeout_no_progress"]["disposition"], "falsification")

    def test_extract_locked_replay_case_names_rejects_non_string_case_names(self) -> None:
        source_path = pathlib.Path("/tmp/corrupt_observe_first_replay_cases.json")
        for invalid_name in (None, 17):
            with self.subTest(invalid_name=invalid_name):
                corpus = verifier.load_json(SCRIPT_DIR.parent / "testdata" / "observe_first_replay_cases.json")
                locked_index = next(
                    index
                    for index, case_payload in enumerate(corpus["cases"])
                    if isinstance(case_payload, dict) and case_payload.get("replay_expected") is not None
                )
                corpus["cases"][locked_index]["name"] = invalid_name
                names, error = verifier.extract_locked_replay_case_names(corpus, source_path)
                self.assertEqual(names, tuple())
                self.assertIn("name must be non-empty string", error)

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
        self.write_feature_flag_snapshot(proof_dir, "start")
        self.write_feature_flag_snapshot(proof_dir, "end")

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
        metrics_path = proof_dir / "samples" / f"{phase}_metrics.prom"
        existing_metrics = []
        if metrics_path.exists():
            existing_metrics = metrics_path.read_text(encoding="utf-8").splitlines()
        write_metrics(
            metrics_path,
            [
                *existing_metrics,
                f'direct_apply_total{{family="B524",freshness_profile="state_fast"}} {direct_apply}',
                f'active_reads_avoided_total{{family="B524",freshness_profile="state_fast"}} {avoided}',
                f'active_read_saved_seconds{{family="B524",freshness_profile="state_fast"}} {saved_seconds}',
                f"ebus_passive_completed_transactions_total {completed_transactions}",
                f"ebus_passive_direct_apply_candidates_evaluated_total {direct_apply_candidates}",
            ],
        )
        self.write_feature_flag_snapshot(proof_dir, phase)

    def write_feature_flag_snapshot(
        self,
        proof_dir: pathlib.Path,
        phase: str,
        *,
        graphql_flags: dict | None = None,
        bus_flags: dict | None = None,
    ) -> None:
        if graphql_flags is None:
            graphql_flags = graphql_feature_flags_snapshot(phase)
        else:
            graphql_flags = dict(graphql_flags)
            graphql_flags.setdefault(
                "lastUpdatedAt",
                phase_timestamps(phase)["graphql_bus_watch"]["feature_flags_last_updated_at"],
            )
        if bus_flags is None:
            bus_flags = bus_feature_flags_snapshot(phase)
        else:
            bus_flags = dict(bus_flags)
            bus_flags.setdefault(
                "last_updated_at",
                phase_timestamps(phase)["bus_observability"]["feature_flags_last_updated_at"],
            )
        snapshot_path = (
            proof_dir / f"{phase}_feature_flags.json"
            if phase in ("start", "end")
            else proof_dir / "samples" / f"{phase}_feature_flags.json"
        )
        write_json(
            snapshot_path,
            {
                "captured_at": "2026-03-28T00:00:00+00:00",
                "graphql_feature_flags": graphql_flags,
                "bus_observability_feature_flags": bus_flags,
            },
        )

    def write_structured_warmup_snapshot_bundle(
        self,
        proof_dir: pathlib.Path,
        phase: str,
        *,
        startup_phase: str,
        warmup_state: str,
        cache_epoch: int = 1,
        live_epoch: int = 1,
        transport_class: str = "ens",
        publisher_cadence_sec: float = 3600.0,
        publisher_cadence_source: str = "config.semantic_state_interval",
        graphql_feature_flags: dict | None = None,
        bus_feature_flags: dict | None = None,
    ) -> None:
        write_structured_warmup_snapshot_bundle(
            proof_dir,
            phase,
            startup_phase=startup_phase,
            warmup_state=warmup_state,
            cache_epoch=cache_epoch,
            live_epoch=live_epoch,
            transport_class=transport_class,
            publisher_cadence_sec=publisher_cadence_sec,
            publisher_cadence_source=publisher_cadence_source,
            graphql_feature_flags=graphql_feature_flags,
            bus_feature_flags=bus_feature_flags,
        )

    def write_run_phase_artifacts(
        self,
        proof_dir: pathlib.Path,
        run_id: str = "run-1",
        *,
        include_interval: bool = False,
        interval_status: str = "pass",
    ) -> None:
        self.write_structured_warmup_snapshot_bundle(
            proof_dir,
            "start",
            startup_phase="LIVE_WARMUP",
            warmup_state="warming_up",
            cache_epoch=1,
            live_epoch=0,
            transport_class="ens",
        )
        write_json(
            proof_dir / "canary_phase_start.json",
            {
                "run_id": run_id,
                "phase": "start",
                "results": [{"id": "a", "status": "pass"}],
            },
        )
        if include_interval:
            self.write_structured_warmup_snapshot_bundle(
                proof_dir,
                "sample_0001",
                startup_phase="LIVE_WARMUP",
                warmup_state="warming_up",
                cache_epoch=1,
                live_epoch=1,
                transport_class="ens",
            )
            write_json(
                proof_dir / "canary_phase_sample_0001.json",
                {
                    "run_id": run_id,
                    "phase": "sample_0001",
                    "results": [{"id": "a", "status": interval_status}],
                },
            )
        self.write_structured_warmup_snapshot_bundle(
            proof_dir,
            "end",
            startup_phase="LIVE_READY",
            warmup_state="available",
            cache_epoch=1,
            live_epoch=1,
            transport_class="ens",
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
            self.write_structured_warmup_snapshot_bundle(
                proof_dir,
                "start",
                startup_phase="LIVE_WARMUP",
                warmup_state="warming_up",
                cache_epoch=1,
                live_epoch=0,
            )
            self.write_structured_warmup_snapshot_bundle(
                proof_dir,
                "end",
                startup_phase="LIVE_READY",
                warmup_state="available",
                cache_epoch=1,
                live_epoch=1,
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
            self.write_structured_warmup_snapshot_bundle(
                proof_dir,
                "start",
                startup_phase="LIVE_WARMUP",
                warmup_state="warming_up",
                cache_epoch=1,
                live_epoch=0,
            )
            self.write_structured_warmup_snapshot_bundle(
                proof_dir,
                "end",
                startup_phase="LIVE_READY",
                warmup_state="available",
                cache_epoch=1,
                live_epoch=1,
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
            self.assertIn("warmup_behavior", summary)
            self.assertFalse(summary["warmup_behavior"]["ok"])
            self.assertFalse(summary["warmup_behavior"]["transition"]["established"])
            self.assertEqual(summary["warmup_behavior"]["transition"]["interval_snapshot_count"], 0)

    def test_summary_derives_warmup_behavior_artifact_from_structured_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            self.write_required_read_avoidance_metrics(
                proof_dir,
                start_direct_apply=4,
                start_avoided=6,
                end_direct_apply=4,
                end_avoided=7,
            )
            self.write_run_phase_artifacts(proof_dir, include_interval=True)
            self.write_sample_read_avoidance_metrics(
                proof_dir,
                "sample_0001",
                direct_apply=4,
                avoided=7,
            )

            summary = verifier.summarize_run(proof_dir, "run-1")
            warmup = summary["warmup_behavior"]
            self.assertEqual(warmup["schema"], verifier.WARMUP_BEHAVIOR_ARTIFACT_SCHEMA)
            self.assertTrue(warmup["ok"])
            self.assertTrue(warmup["transition"]["established"])
            self.assertEqual(warmup["cold_start"]["snapshot_prefix"], "start")
            self.assertEqual(warmup["cold_start"]["startup_phase"], "LIVE_WARMUP")
            self.assertEqual(warmup["cold_start"]["warmup_state"], "warming_up")
            self.assertEqual(warmup["post_warmup"]["snapshot_prefix"], "end")
            self.assertEqual(warmup["post_warmup"]["startup_phase"], "LIVE_READY")
            self.assertEqual(warmup["post_warmup"]["warmup_state"], "available")
            self.assertEqual(warmup["transition"]["from_snapshot_prefix"], "start")
            self.assertEqual(warmup["transition"]["to_snapshot_prefix"], "end")
            self.assertGreaterEqual(warmup["transition"]["interval_snapshot_count"], 1)
            self.assertEqual(warmup["transition"]["interval_snapshot_prefixes"], ["sample_0001"])
            self.assertIn("start_bus_observability.json", warmup["evidence"]["start_snapshot_paths"]["bus_observability"])
            self.assertIn("end_graphql_bus_watch.json", warmup["evidence"]["end_snapshot_paths"]["graphql_bus_watch"])

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
            self.write_structured_warmup_snapshot_bundle(
                proof_dir,
                "start",
                startup_phase="LIVE_WARMUP",
                warmup_state="warming_up",
                cache_epoch=1,
                live_epoch=0,
            )
            self.write_sample_read_avoidance_metrics(
                proof_dir,
                "sample_0001",
                direct_apply=14,
                avoided=18,
            )
            self.write_structured_warmup_snapshot_bundle(
                proof_dir,
                "sample_0001",
                startup_phase="LIVE_WARMUP",
                warmup_state="warming_up",
                cache_epoch=1,
                live_epoch=1,
            )
            self.write_sample_read_avoidance_metrics(
                proof_dir,
                "sample_0002",
                direct_apply=11,
                avoided=19,
            )
            self.write_structured_warmup_snapshot_bundle(
                proof_dir,
                "sample_0002",
                startup_phase="LIVE_WARMUP",
                warmup_state="warming_up",
                cache_epoch=1,
                live_epoch=1,
            )
            self.write_structured_warmup_snapshot_bundle(
                proof_dir,
                "end",
                startup_phase="LIVE_READY",
                warmup_state="available",
                cache_epoch=1,
                live_epoch=1,
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
            self.write_structured_warmup_snapshot_bundle(
                proof_dir,
                "start",
                startup_phase="LIVE_WARMUP",
                warmup_state="warming_up",
                cache_epoch=1,
                live_epoch=0,
            )
            self.write_sample_read_avoidance_metrics(
                proof_dir,
                "sample_0001",
                direct_apply=12,
                avoided=20,
            )
            self.write_structured_warmup_snapshot_bundle(
                proof_dir,
                "sample_0001",
                startup_phase="LIVE_WARMUP",
                warmup_state="warming_up",
                cache_epoch=1,
                live_epoch=1,
            )
            self.write_sample_read_avoidance_metrics(
                proof_dir,
                "sample_0002",
                direct_apply=12,
                avoided=14,
            )
            self.write_structured_warmup_snapshot_bundle(
                proof_dir,
                "sample_0002",
                startup_phase="LIVE_WARMUP",
                warmup_state="warming_up",
                cache_epoch=1,
                live_epoch=1,
            )
            self.write_structured_warmup_snapshot_bundle(
                proof_dir,
                "end",
                startup_phase="LIVE_READY",
                warmup_state="available",
                cache_epoch=1,
                live_epoch=1,
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
            self.write_run_phase_artifacts(proof_dir, include_interval=False)
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
            self.write_structured_warmup_snapshot_bundle(
                proof_dir,
                "start",
                startup_phase="LIVE_WARMUP",
                warmup_state="warming_up",
                cache_epoch=1,
                live_epoch=0,
            )
            self.write_sample_read_avoidance_metrics(
                proof_dir,
                "sample_0001",
                direct_apply=11,
                avoided=16,
                direct_apply_candidates=1_200,
            )
            self.write_structured_warmup_snapshot_bundle(
                proof_dir,
                "sample_0001",
                startup_phase="LIVE_WARMUP",
                warmup_state="warming_up",
                cache_epoch=1,
                live_epoch=1,
            )
            self.write_sample_read_avoidance_metrics(
                proof_dir,
                "sample_0002",
                direct_apply=12,
                avoided=17,
                direct_apply_candidates=900,
            )
            self.write_structured_warmup_snapshot_bundle(
                proof_dir,
                "sample_0002",
                startup_phase="LIVE_WARMUP",
                warmup_state="warming_up",
                cache_epoch=1,
                live_epoch=1,
            )
            self.write_structured_warmup_snapshot_bundle(
                proof_dir,
                "end",
                startup_phase="LIVE_READY",
                warmup_state="available",
                cache_epoch=1,
                live_epoch=1,
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

    def test_summary_fails_closed_when_feature_flag_snapshot_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            self.write_required_read_avoidance_metrics(
                proof_dir,
                start_direct_apply=1,
                start_avoided=2,
                end_direct_apply=2,
                end_avoided=3,
            )
            self.write_run_phase_artifacts(proof_dir, include_interval=False)
            (proof_dir / "start_feature_flags.json").unlink()

            with self.assertRaises(ValueError) as ctx:
                verifier.summarize_run(proof_dir, "run-1", require_interval_phase=False)
            self.assertIn("feature flag proof artifact", str(ctx.exception))

    def test_summary_fails_closed_when_feature_flag_snapshot_is_malformed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            self.write_required_read_avoidance_metrics(
                proof_dir,
                start_direct_apply=1,
                start_avoided=2,
                end_direct_apply=2,
                end_avoided=3,
            )
            self.write_run_phase_artifacts(proof_dir, include_interval=False)
            write_json(
                proof_dir / "end_feature_flags.json",
                {
                    "captured_at": "2026-03-28T00:00:00+00:00",
                    "graphql_feature_flags": canonical_feature_flags(),
                    "bus_observability_feature_flags": {
                        "observeFirstEnabled": True,
                    },
                },
            )

            with self.assertRaises(ValueError) as ctx:
                verifier.summarize_run(proof_dir, "run-1", require_interval_phase=False)
            self.assertIn("missing passiveStateDirectApply", str(ctx.exception))

    def test_summary_fails_closed_when_feature_flags_drift_mid_window(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            self.write_required_read_avoidance_metrics(
                proof_dir,
                start_direct_apply=1,
                start_avoided=2,
                end_direct_apply=2,
                end_avoided=3,
            )
            self.write_sample_read_avoidance_metrics(
                proof_dir,
                "sample_0001",
                direct_apply=2,
                avoided=3,
            )
            self.write_run_phase_artifacts(proof_dir, include_interval=True)
            sample_graphql_flags = graphql_feature_flags_snapshot("sample_0001")
            sample_graphql_flags["observeFirstEnabled"] = False
            sample_bus_flags = bus_feature_flags_snapshot("sample_0001")
            sample_bus_flags["observeFirstEnabled"] = False
            self.write_feature_flag_snapshot(
                proof_dir,
                "sample_0001",
                graphql_flags=sample_graphql_flags,
                bus_flags=sample_bus_flags,
            )

            with self.assertRaises(ValueError) as ctx:
                verifier.summarize_run(proof_dir, "run-1")
            self.assertIn("feature flag drift", str(ctx.exception))
            self.assertIn("observeFirstEnabled", str(ctx.exception))

    def test_summary_collects_matching_feature_flag_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            self.write_required_read_avoidance_metrics(
                proof_dir,
                start_direct_apply=1,
                start_avoided=2,
                end_direct_apply=2,
                end_avoided=3,
            )
            self.write_sample_read_avoidance_metrics(
                proof_dir,
                "sample_0001",
                direct_apply=2,
                avoided=3,
            )
            self.write_run_phase_artifacts(proof_dir, include_interval=True)

            summary = verifier.summarize_run(proof_dir, "run-1")
            feature_flags = summary["feature_flag_consistency"]
            self.assertTrue(feature_flags["ok"])
            self.assertEqual(feature_flags["schema"], verifier.FEATURE_FLAG_CONSISTENCY_SCHEMA)
            self.assertEqual(feature_flags["evidence"]["phases"], ["start", "sample_0001", "end"])
            self.assertEqual(len(feature_flags["snapshots"]), 3)

    def test_summary_accepts_omitted_normalizations_as_canonical_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            self.write_required_read_avoidance_metrics(
                proof_dir,
                start_direct_apply=1,
                start_avoided=2,
                end_direct_apply=2,
                end_avoided=3,
            )
            self.write_run_phase_artifacts(proof_dir, include_interval=False)
            self.write_feature_flag_snapshot(proof_dir, "start")
            end_bus_flags = bus_feature_flags_snapshot("end")
            end_bus_flags.pop("normalizations")
            self.write_feature_flag_snapshot(
                proof_dir,
                "end",
                graphql_flags=graphql_feature_flags_snapshot("end"),
                bus_flags=end_bus_flags,
            )

            summary = verifier.summarize_run(proof_dir, "run-1", require_interval_phase=False)
            feature_flags = summary["feature_flag_consistency"]
            self.assertTrue(feature_flags["ok"])
            self.assertEqual(feature_flags["snapshots"][1]["bus_observability_feature_flags"]["normalizations"], [])

    def test_summary_fails_closed_when_graphql_normalizations_are_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            self.write_required_read_avoidance_metrics(
                proof_dir,
                start_direct_apply=1,
                start_avoided=2,
                end_direct_apply=2,
                end_avoided=3,
            )
            self.write_run_phase_artifacts(proof_dir, include_interval=False)
            write_json(
                proof_dir / "start_feature_flags.json",
                {
                    "captured_at": "2026-03-28T00:00:00+00:00",
                    "graphql_feature_flags": {
                        "observeFirstEnabled": True,
                        "passiveStateDirectApply": False,
                        "passiveConfigDirectApply": False,
                        "externalWritePolicy": "record_only",
                    },
                    "bus_observability_feature_flags": canonical_feature_flags(),
                },
            )

            with self.assertRaises(ValueError) as ctx:
                verifier.summarize_run(proof_dir, "run-1", require_interval_phase=False)
            self.assertIn("graphql feature flags missing normalizations", str(ctx.exception))

    def test_summary_fails_closed_when_graphql_normalizations_are_null(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            self.write_required_read_avoidance_metrics(
                proof_dir,
                start_direct_apply=1,
                start_avoided=2,
                end_direct_apply=2,
                end_avoided=3,
            )
            self.write_run_phase_artifacts(proof_dir, include_interval=False)
            write_json(
                proof_dir / "start_feature_flags.json",
                {
                    "captured_at": "2026-03-28T00:00:00+00:00",
                    "graphql_feature_flags": {
                        "observeFirstEnabled": True,
                        "passiveStateDirectApply": False,
                        "passiveConfigDirectApply": False,
                        "externalWritePolicy": "record_only",
                        "normalizations": None,
                    },
                    "bus_observability_feature_flags": canonical_feature_flags(),
                },
            )

            with self.assertRaises(ValueError) as ctx:
                verifier.summarize_run(proof_dir, "run-1", require_interval_phase=False)
            self.assertIn("graphql feature flags field 'normalizations' is null", str(ctx.exception))


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
        transport_class: str = "ens",
    ) -> dict:
        per_canary = {canary_id: {"last_status": "pass"} for canary_id in per_canary_interval}
        warmup_established = interval_results > 0
        warmup_interval_prefixes = ["sample_0001"] if warmup_established else []
        start_snapshot_paths = {
            "metrics": "/tmp/proof/start_metrics.prom",
            "bus_observability": "/tmp/proof/start_bus_observability.json",
            "graphql_bus_watch": "/tmp/proof/start_graphql_bus_watch.json",
            "feature_flags": "/tmp/proof/start_feature_flags.json",
        }
        end_snapshot_paths = {
            "metrics": "/tmp/proof/end_metrics.prom",
            "bus_observability": "/tmp/proof/end_bus_observability.json",
            "graphql_bus_watch": "/tmp/proof/end_graphql_bus_watch.json",
            "feature_flags": "/tmp/proof/end_feature_flags.json",
        }
        start_bus_observability = {
            "summary": {
                "last_updated_at": phase_timestamps("start")["bus_observability"]["summary_last_updated_at"],
                "status": {
                    "last_updated_at": phase_timestamps("start")["bus_observability"]["status_last_updated_at"],
                    "startup": {
                        "phase": "LIVE_WARMUP",
                        "cache_epoch": 1,
                        "live_epoch": 0,
                        "last_updated_at": phase_timestamps("start")["bus_observability"]["startup_last_updated_at"],
                    },
                    "feature_flags": bus_feature_flags_snapshot("start"),
                }
            }
        }
        end_bus_observability = {
            "summary": {
                "last_updated_at": phase_timestamps("end")["bus_observability"]["summary_last_updated_at"],
                "status": {
                    "last_updated_at": phase_timestamps("end")["bus_observability"]["status_last_updated_at"],
                    "startup": {
                        "phase": "LIVE_READY",
                        "cache_epoch": 1,
                        "live_epoch": 1,
                        "last_updated_at": phase_timestamps("end")["bus_observability"]["startup_last_updated_at"],
                    },
                    "feature_flags": bus_feature_flags_snapshot("end"),
                }
            }
        }
        start_graphql_bus_watch = {
            "data": {
                "busSummary": {
                    "lastUpdatedAt": phase_timestamps("start")["graphql_bus_watch"]["summary_last_updated_at"],
                    "status": {
                        "lastUpdatedAt": phase_timestamps("start")["graphql_bus_watch"]["status_last_updated_at"],
                        "transportClass": transport_class,
                        "startup": {
                            "phase": "LIVE_WARMUP",
                            "cacheEpoch": 1,
                            "liveEpoch": 0,
                            "lastUpdatedAt": phase_timestamps("start")["graphql_bus_watch"]["startup_last_updated_at"],
                        },
                        "warmup": {
                            "state": "warming_up",
                            "blocker": "",
                            "elapsedSeconds": 0.0,
                            "completedTransactions": 0,
                            "requiredTransactions": 0,
                            "completionMode": "proof_window",
                        },
                        "featureFlags": graphql_feature_flags_snapshot("start"),
                    }
                },
                "watchSummary": {
                    "lastUpdatedAt": phase_timestamps("start")["graphql_bus_watch"]["watch_summary_last_updated_at"],
                    "inventory": {"totalEntries": 1},
                    "activationCounts": {"catalogDescriptors": 1, "activeKeys": 1, "sourceClasses": []},
                    "directApplyEligibilityClasses": [],
                    "degraded": {
                        "active": False,
                        "shadowingEnabled": False,
                        "pinnedBudgetDegraded": False,
                        "compactorDegraded": False,
                        "reasons": [],
                    },
                },
            }
        }
        end_graphql_bus_watch = {
            "data": {
                "busSummary": {
                    "lastUpdatedAt": phase_timestamps("end")["graphql_bus_watch"]["summary_last_updated_at"],
                    "status": {
                        "lastUpdatedAt": phase_timestamps("end")["graphql_bus_watch"]["status_last_updated_at"],
                        "transportClass": transport_class,
                        "startup": {
                            "phase": "LIVE_READY",
                            "cacheEpoch": 1,
                            "liveEpoch": 1,
                            "lastUpdatedAt": phase_timestamps("end")["graphql_bus_watch"]["startup_last_updated_at"],
                        },
                        "warmup": {
                            "state": "available",
                            "blocker": "",
                            "elapsedSeconds": 0.0,
                            "completedTransactions": 0,
                            "requiredTransactions": 0,
                            "completionMode": "proof_window",
                        },
                        "featureFlags": graphql_feature_flags_snapshot("end"),
                    }
                },
                "watchSummary": {
                    "lastUpdatedAt": phase_timestamps("end")["graphql_bus_watch"]["watch_summary_last_updated_at"],
                    "inventory": {"totalEntries": 1},
                    "activationCounts": {"catalogDescriptors": 1, "activeKeys": 1, "sourceClasses": []},
                    "directApplyEligibilityClasses": [],
                    "degraded": {
                        "active": False,
                        "shadowingEnabled": False,
                        "pinnedBudgetDegraded": False,
                        "compactorDegraded": False,
                        "reasons": [],
                    },
                },
            }
        }
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
            "feature_flag_consistency": {
                "schema": verifier.FEATURE_FLAG_CONSISTENCY_SCHEMA,
                "captured_at": "2026-03-28T00:00:00+00:00",
                "source": "proof_artifact_feature_flags",
                "claim_scope": "bounded_proof_window_feature_flag_consistency",
                "evidence": {
                    "feature_flag_snapshot_paths": [
                        "/tmp/proof/start_feature_flags.json",
                        "/tmp/proof/end_feature_flags.json",
                    ],
                    "phases": ["start", "end"],
                },
                "snapshots": [
                    {
                        "phase": "start",
                        "feature_flags_snapshot_path": "/tmp/proof/start_feature_flags.json",
                        "graphql_feature_flags": graphql_feature_flags_snapshot("start"),
                        "bus_observability_feature_flags": bus_feature_flags_snapshot("start"),
                        "graphql_feature_flags_last_updated_at": phase_timestamps("start")["graphql_bus_watch"]["feature_flags_last_updated_at"],
                        "bus_observability_feature_flags_last_updated_at": phase_timestamps("start")["bus_observability"]["feature_flags_last_updated_at"],
                        "canonical_feature_flags": canonical_feature_flags(),
                        "canonical_feature_flags_key": json.dumps(
                            canonical_feature_flags(), sort_keys=True, separators=(",", ":"), ensure_ascii=True
                        ),
                    },
                    {
                        "phase": "end",
                        "feature_flags_snapshot_path": "/tmp/proof/end_feature_flags.json",
                        "graphql_feature_flags": graphql_feature_flags_snapshot("end"),
                        "bus_observability_feature_flags": bus_feature_flags_snapshot("end"),
                        "graphql_feature_flags_last_updated_at": phase_timestamps("end")["graphql_bus_watch"]["feature_flags_last_updated_at"],
                        "bus_observability_feature_flags_last_updated_at": phase_timestamps("end")["bus_observability"]["feature_flags_last_updated_at"],
                        "canonical_feature_flags": canonical_feature_flags(),
                        "canonical_feature_flags_key": json.dumps(
                            canonical_feature_flags(), sort_keys=True, separators=(",", ":"), ensure_ascii=True
                        ),
                    },
                ],
                "ok": True,
            },
            "warmup_behavior": {
                "schema": verifier.WARMUP_BEHAVIOR_ARTIFACT_SCHEMA,
                "captured_at": "2026-03-28T00:00:00+00:00",
                "run_id": "run-1",
                "claim_scope": "bounded_proof_window_warmup_behavior",
                "evidence": {
                    "start_snapshot_paths": start_snapshot_paths,
                    "end_snapshot_paths": end_snapshot_paths,
                    "interval_snapshot_paths": [
                        {
                            "metrics": "/tmp/proof/samples/sample_0001_metrics.prom",
                            "bus_observability": "/tmp/proof/samples/sample_0001_bus_observability.json",
                            "graphql_bus_watch": "/tmp/proof/samples/sample_0001_graphql_bus_watch.json",
                            "feature_flags": "/tmp/proof/samples/sample_0001_feature_flags.json",
                        },
                    ] if warmup_established else [],
                    "structured_snapshot_prefixes": ["start", *warmup_interval_prefixes, "end"],
                },
                "cold_start": {
                    "snapshot_prefix": "start",
                    "snapshot_paths": start_snapshot_paths,
                    "bus_observability": start_bus_observability,
                    "graphql_bus_watch": start_graphql_bus_watch,
                    "feature_flag_snapshot": {
                        "phase": "start",
                        "feature_flags_snapshot_path": "/tmp/proof/start_feature_flags.json",
                        "graphql_feature_flags": canonical_feature_flags(),
                        "bus_observability_feature_flags": canonical_feature_flags(),
                        "canonical_feature_flags": canonical_feature_flags(),
                        "canonical_feature_flags_key": json.dumps(
                            canonical_feature_flags(), sort_keys=True, separators=(",", ":"), ensure_ascii=True
                        ),
                    },
                    "startup_phase": "LIVE_WARMUP",
                    "warmup_state": "warming_up",
                    "timestamps": phase_timestamps("start"),
                },
                "post_warmup": {
                    "snapshot_prefix": "end",
                    "snapshot_paths": end_snapshot_paths,
                    "bus_observability": end_bus_observability,
                    "graphql_bus_watch": end_graphql_bus_watch,
                    "feature_flag_snapshot": {
                        "phase": "end",
                        "feature_flags_snapshot_path": "/tmp/proof/end_feature_flags.json",
                        "graphql_feature_flags": canonical_feature_flags(),
                        "bus_observability_feature_flags": canonical_feature_flags(),
                        "canonical_feature_flags": canonical_feature_flags(),
                        "canonical_feature_flags_key": json.dumps(
                            canonical_feature_flags(), sort_keys=True, separators=(",", ":"), ensure_ascii=True
                        ),
                    },
                    "startup_phase": "LIVE_READY",
                    "warmup_state": "available",
                    "timestamps": phase_timestamps("end"),
                },
                "transition": {
                    "established": warmup_established,
                    "from_snapshot_prefix": "start",
                    "to_snapshot_prefix": "end",
                    "cold_start_proven": True,
                    "post_warmup_proven": True,
                    "interval_snapshot_count": len(warmup_interval_prefixes),
                    "interval_snapshot_prefixes": warmup_interval_prefixes,
                    "first_interval_snapshot_prefix": warmup_interval_prefixes[0] if warmup_interval_prefixes else None,
                    "last_interval_snapshot_prefix": warmup_interval_prefixes[-1] if warmup_interval_prefixes else None,
                    "evidence": {
                        "start_snapshot_paths": start_snapshot_paths,
                        "end_snapshot_paths": end_snapshot_paths,
                        "interval_snapshot_paths": [
                            {
                                "metrics": "/tmp/proof/samples/sample_0001_metrics.prom",
                                "bus_observability": "/tmp/proof/samples/sample_0001_bus_observability.json",
                                "graphql_bus_watch": "/tmp/proof/samples/sample_0001_graphql_bus_watch.json",
                                "feature_flags": "/tmp/proof/samples/sample_0001_feature_flags.json",
                            },
                        ] if warmup_established else [],
                        "structured_snapshot_prefixes": ["start", *warmup_interval_prefixes, "end"],
                    },
                },
                "ok": warmup_established,
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

    def test_verdict_fails_closed_when_feature_flag_consistency_is_missing(self) -> None:
        summary = self.build_summary_payload(
            mismatch_count=0,
            interval_required=False,
            interval_results=0,
            interval_conclusive=0,
            per_canary_interval={"a": {"pass": 0, "mismatch": 0, "inconclusive": 0, "conclusive": 0}},
        )
        summary.pop("feature_flag_consistency", None)
        verdict = verifier.build_canary_verdict(summary)
        self.assertFalse(verdict["ok"])
        self.assertFalse(verdict["criteria"]["feature_flag_consistency"]["ok"])

    def test_verdict_fails_closed_when_warmup_behavior_artifact_is_missing(self) -> None:
        summary = self.build_summary_payload(
            mismatch_count=0,
            interval_required=True,
            interval_results=10,
            interval_conclusive=9,
            per_canary_interval={"a": {"pass": 9, "mismatch": 0, "inconclusive": 1, "conclusive": 9}},
        )
        summary.pop("warmup_behavior", None)
        verdict = verifier.build_canary_verdict(summary)
        self.assertFalse(verdict["ok"])
        self.assertFalse(verdict["criteria"]["warmup_behavior"]["ok"])
        self.assertIn("missing warmup_behavior payload", verdict["criteria"]["warmup_behavior"]["reason"])

    def test_verdict_fails_closed_when_warmup_transition_is_incomplete(self) -> None:
        summary = self.build_summary_payload(
            mismatch_count=0,
            interval_required=True,
            interval_results=10,
            interval_conclusive=9,
            per_canary_interval={"a": {"pass": 9, "mismatch": 0, "inconclusive": 1, "conclusive": 9}},
        )
        summary["warmup_behavior"]["transition"]["established"] = False
        summary["warmup_behavior"]["transition"]["interval_snapshot_count"] = 0
        summary["warmup_behavior"]["transition"]["interval_snapshot_prefixes"] = []
        summary["warmup_behavior"]["transition"]["first_interval_snapshot_prefix"] = None
        summary["warmup_behavior"]["transition"]["last_interval_snapshot_prefix"] = None
        summary["warmup_behavior"]["transition"]["evidence"]["interval_snapshot_paths"] = []
        summary["warmup_behavior"]["evidence"]["interval_snapshot_paths"] = []
        summary["warmup_behavior"]["ok"] = False
        verdict = verifier.build_canary_verdict(summary)
        self.assertFalse(verdict["ok"])
        self.assertFalse(verdict["criteria"]["warmup_behavior"]["ok"])
        self.assertIn("structured interval evidence", verdict["criteria"]["warmup_behavior"]["reason"])

    def test_verdict_fails_closed_when_cold_start_snapshot_proves_live_ready(self) -> None:
        summary = self.build_summary_payload(
            mismatch_count=0,
            interval_required=True,
            interval_results=10,
            interval_conclusive=9,
            per_canary_interval={"a": {"pass": 9, "mismatch": 0, "inconclusive": 1, "conclusive": 9}},
        )
        summary["warmup_behavior"]["cold_start"]["startup_phase"] = "LIVE_READY"
        summary["warmup_behavior"]["cold_start"]["bus_observability"]["summary"]["status"]["startup"]["phase"] = (
            "LIVE_READY"
        )
        summary["warmup_behavior"]["cold_start"]["warmup_state"] = "available"
        summary["warmup_behavior"]["cold_start"]["graphql_bus_watch"]["data"]["busSummary"]["status"]["warmup"][
            "state"
        ] = "available"
        verdict = verifier.build_canary_verdict(summary)
        self.assertFalse(verdict["ok"])
        self.assertFalse(verdict["criteria"]["warmup_behavior"]["ok"])
        self.assertIn("pre-LIVE_READY", verdict["criteria"]["warmup_behavior"]["reason"])

    def test_verdict_fails_closed_when_post_warmup_snapshot_is_not_available(self) -> None:
        summary = self.build_summary_payload(
            mismatch_count=0,
            interval_required=True,
            interval_results=10,
            interval_conclusive=9,
            per_canary_interval={"a": {"pass": 9, "mismatch": 0, "inconclusive": 1, "conclusive": 9}},
        )
        summary["warmup_behavior"]["post_warmup"]["startup_phase"] = "LIVE_WARMUP"
        summary["warmup_behavior"]["post_warmup"]["bus_observability"]["summary"]["status"]["startup"]["phase"] = (
            "LIVE_WARMUP"
        )
        summary["warmup_behavior"]["post_warmup"]["warmup_state"] = "warming_up"
        summary["warmup_behavior"]["post_warmup"]["graphql_bus_watch"]["data"]["busSummary"]["status"]["warmup"][
            "state"
        ] = "warming_up"
        verdict = verifier.build_canary_verdict(summary)
        self.assertFalse(verdict["ok"])
        self.assertFalse(verdict["criteria"]["warmup_behavior"]["ok"])
        self.assertIn("LIVE_READY", verdict["criteria"]["warmup_behavior"]["reason"])

    def test_verdict_fails_closed_when_canonical_normalizations_are_missing(self) -> None:
        summary = self.build_summary_payload(
            mismatch_count=0,
            interval_required=False,
            interval_results=0,
            interval_conclusive=0,
            per_canary_interval={"a": {"pass": 0, "mismatch": 0, "inconclusive": 0, "conclusive": 0}},
        )
        summary["feature_flag_consistency"]["snapshots"][0]["canonical_feature_flags"].pop("normalizations", None)
        verdict = verifier.build_canary_verdict(summary)
        self.assertFalse(verdict["ok"])
        self.assertIn("canonical feature flags missing normalizations", verdict["criteria"]["feature_flag_consistency"]["reason"])

    def test_verdict_fails_closed_when_canonical_normalizations_are_null(self) -> None:
        summary = self.build_summary_payload(
            mismatch_count=0,
            interval_required=False,
            interval_results=0,
            interval_conclusive=0,
            per_canary_interval={"a": {"pass": 0, "mismatch": 0, "inconclusive": 0, "conclusive": 0}},
        )
        summary["feature_flag_consistency"]["snapshots"][0]["canonical_feature_flags"]["normalizations"] = None
        verdict = verifier.build_canary_verdict(summary)
        self.assertFalse(verdict["ok"])
        self.assertIn("canonical feature flags field 'normalizations' is null", verdict["criteria"]["feature_flag_consistency"]["reason"])

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


class ReplayFalsificationVerdictTests(unittest.TestCase):
    def write_proof_artifacts(self, proof_dir: pathlib.Path) -> dict:
        summary = {
            "schema": "p03_canary_overall_summary_v1",
            "run_id": "run-1",
            "read_avoidance_accounting": {
                "delta_totals": {
                    "direct_apply_total": 1,
                    "active_reads_avoided_total": 2,
                    "active_read_saved_seconds": 3,
                },
                "current_run": {
                    "direct_apply_total": 1,
                    "active_reads_avoided_total": 2,
                    "active_read_saved_seconds": 3,
                },
            },
            "proof_window_traffic_minimums": {
                "delta_totals": {
                    "ebus_passive_completed_transactions_total": 1_200,
                    "ebus_passive_direct_apply_candidates_evaluated_total": 120,
                },
                "thresholds": {
                    "ebus_passive_completed_transactions_total": {"ok": True, "observed_delta": 1_200},
                    "ebus_passive_direct_apply_candidates_evaluated_total": {"ok": True, "observed_delta": 120},
                },
                "ok": True,
            },
            "interval_phase_required": False,
            "totals": {"results": 3, "pass": 3, "mismatch": 0, "inconclusive": 0, "conclusive": 3},
            "interval_totals": {"results": 0, "pass": 0, "mismatch": 0, "inconclusive": 0, "conclusive": 0},
            "per_canary": {
                "b524_value_bearing_enh": {"last_status": "pass"},
                "collision_episode": {"last_status": "pass"},
                "timeout_no_progress": {"last_status": "pass"},
            },
            "per_canary_interval": {},
            "overall_conclusive_count": 3,
            "overall_interval_conclusive_count": 0,
        }
        verdict = verifier.build_canary_verdict(summary)
        write_json(proof_dir / "canary_summary.json", summary)
        write_json(proof_dir / "canary_verdict.json", verdict)
        return summary

    def test_replay_verdict_requires_proof_artifacts(self) -> None:
        corpus = verifier.load_json(SCRIPT_DIR.parent / "testdata" / "observe_first_replay_cases.json")
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            with self.assertRaises(ValueError) as ctx:
                verifier.build_replay_falsification_verdict(corpus, proof_dir)
            self.assertIn("missing replay behavior artifact", str(ctx.exception))

    def test_replay_verdict_uses_proof_artifacts_and_passes(self) -> None:
        corpus = verifier.load_json(SCRIPT_DIR.parent / "testdata" / "observe_first_replay_cases.json")
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_replay_behavior_artifact(proof_dir)
            verdict = verifier.build_replay_falsification_verdict(corpus, proof_dir)

        self.assertEqual(verdict["schema"], verifier.REPLAY_FALSIFICATION_VERDICT_SCHEMA)
        self.assertTrue(verdict["ok"])
        self.assertTrue(verdict["summary"]["behavior_artifact_ok"])
        self.assertEqual(verdict["summary"]["locked_cases"], 3)
        self.assertEqual(verdict["summary"]["fail"], 0)
        by_name = {case["name"]: case for case in verdict["cases"]}
        self.assertEqual(by_name["b524_value_bearing_enh"]["observed"]["replay_harness"], "active_passive_deduplicator")
        self.assertEqual(by_name["collision_episode"]["observed"]["transaction_events"], 0)
        self.assertEqual(by_name["timeout_no_progress"]["observed"]["completed_transactions"], 0)

    def test_replay_verdict_fails_closed_when_behavior_mismatch_is_observed(self) -> None:
        corpus = verifier.load_json(SCRIPT_DIR.parent / "testdata" / "observe_first_replay_cases.json")
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            artifact = write_replay_behavior_artifact(proof_dir)
            artifact["cases"][2]["observed"]["disposition"] = "ambiguity"
            write_json(proof_dir / "replay_behavior.json", artifact)

            verdict = verifier.build_replay_falsification_verdict(corpus, proof_dir)

        self.assertFalse(verdict["ok"])
        self.assertEqual(verdict["status"], "fail")
        by_name = {case["name"]: case for case in verdict["cases"]}
        self.assertEqual(by_name["timeout_no_progress"]["status"], "fail")
        self.assertIn("expected", by_name["timeout_no_progress"]["reason"])

    def test_replay_verdict_validation_accepts_self_generated_failing_reason_mismatch(self) -> None:
        corpus = verifier.load_json(SCRIPT_DIR.parent / "testdata" / "observe_first_replay_cases.json")
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            artifact = write_replay_behavior_artifact(proof_dir)
            artifact["ok"] = False
            artifact["cases"][2]["observed"]["disposition"] = "ambiguity"
            write_json(proof_dir / "replay_behavior.json", artifact)
            verdict = verifier.build_replay_falsification_verdict(corpus, proof_dir)
            behavior_payload = verifier.load_replay_behavior_artifact(proof_dir / "replay_behavior.json")
            valid, reason = verifier.validate_family_upstream_replay_verdict(
                verdict,
                proof_dir / "replay_falsification.json",
                behavior_artifact_payload=behavior_payload,
                behavior_artifact_path=proof_dir / "replay_behavior.json",
            )

        by_name = {case["name"]: case for case in verdict["cases"]}
        timeout_case = by_name["timeout_no_progress"]
        self.assertEqual(timeout_case["status"], "fail")
        self.assertNotEqual(timeout_case["reason"], timeout_case["behavior_evidence"]["observed_reason"])
        self.assertTrue(valid, reason)

    def test_replay_verdict_validation_rejects_forged_fail_observed_reason_anchor(self) -> None:
        corpus = verifier.load_json(SCRIPT_DIR.parent / "testdata" / "observe_first_replay_cases.json")
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            artifact = write_replay_behavior_artifact(proof_dir)
            artifact["ok"] = False
            artifact["cases"][2]["observed"]["disposition"] = "ambiguity"
            write_json(proof_dir / "replay_behavior.json", artifact)
            verdict = verifier.build_replay_falsification_verdict(corpus, proof_dir)
            by_name = {case["name"]: case for case in verdict["cases"]}
            by_name["timeout_no_progress"]["behavior_evidence"]["observed_reason"] = (
                "forged replay behavior anchor reason"
            )
            behavior_payload = verifier.load_replay_behavior_artifact(proof_dir / "replay_behavior.json")
            valid, reason = verifier.validate_family_upstream_replay_verdict(
                verdict,
                proof_dir / "replay_falsification.json",
                behavior_artifact_payload=behavior_payload,
                behavior_artifact_path=proof_dir / "replay_behavior.json",
            )

        self.assertFalse(valid)
        self.assertIn(
            "behavior_evidence.observed_reason mismatches anchored replay behavior artifact",
            reason,
        )

    def test_replay_verdict_validation_rejects_pass_reason_mismatch(self) -> None:
        corpus = verifier.load_json(SCRIPT_DIR.parent / "testdata" / "observe_first_replay_cases.json")
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_replay_behavior_artifact(proof_dir)
            verdict = verifier.build_replay_falsification_verdict(corpus, proof_dir)
            pass_case = verdict["cases"][0]
            self.assertEqual(pass_case["status"], "pass")
            pass_case["reason"] = "forged pass reason"
            behavior_payload = verifier.load_replay_behavior_artifact(proof_dir / "replay_behavior.json")
            valid, reason = verifier.validate_family_upstream_replay_verdict(
                verdict,
                proof_dir / "replay_falsification.json",
                behavior_artifact_payload=behavior_payload,
                behavior_artifact_path=proof_dir / "replay_behavior.json",
            )

        self.assertFalse(valid)
        self.assertIn("reason mismatches behavior_evidence.observed_reason", reason)


class FamilyProofEligibilityArtifactTests(unittest.TestCase):
    def test_family_proof_eligibility_accepts_proven_family(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport=CANONICAL_NO_EBUSD_TRANSPORT,
            )

        self.assertEqual(artifact["schema"], verifier.FAMILY_PROOF_ELIGIBILITY_SCHEMA)
        self.assertTrue(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "proven_for_default_flip")
        self.assertEqual(artifact["family_identity"]["family_key"], "proxy-single-client/required/ens")
        self.assertEqual(artifact["family_identity"]["transport_class"], "ens")

    def test_family_proof_eligibility_blocks_banned_topology_slices(self) -> None:
        cases = (
            (
                "via-ebusd-tcp",
                {
                    "proxy_transport": "ens",
                    "ebusd_transport": "ebusd-tcp",
                },
                "topology='via-ebusd-tcp'",
            ),
            (
                "contradictory proxy transport axis",
                {
                    "proxy_transport": "tcp",
                    "ebusd_transport": CANONICAL_NO_EBUSD_TRANSPORT,
                },
                "proxy_transport mismatch: got 'tcp'; want 'ens'",
            ),
            (
                "contradictory ebusd transport axis",
                {
                    "proxy_transport": "ens",
                    "ebusd_transport": "ens",
                },
                f"ebusd_transport mismatch: got 'ens'; want {CANONICAL_NO_EBUSD_TRANSPORT!r}",
            ),
        )

        for label, topology, expected_reason in cases:
            with self.subTest(label=label):
                with tempfile.TemporaryDirectory() as temp_dir:
                    proof_dir = pathlib.Path(temp_dir)
                    write_family_proof_artifacts(proof_dir, transport_class="ens")
                    artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                        proof_dir,
                        "run-1",
                        "P03",
                        "proxy-single-client",
                        "required",
                        "ens",
                        proxy_transport=topology["proxy_transport"],
                        ebusd_transport=topology["ebusd_transport"],
                    )

                self.assertFalse(artifact["ok"])
                self.assertEqual(artifact["eligibility"]["status"], "blocked")
                self.assertIn(expected_reason, artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_rejects_non_canonical_case_id(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P99",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("family proof case_id mismatch", artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_rejects_warmup_transition_anomalies(self) -> None:
        def rewrite_json(path: pathlib.Path, mutator) -> None:
            payload = verifier.load_json(path)
            mutator(payload)
            write_json(path, payload)

        def set_nested_value(path: pathlib.Path, keys: list[str], value: object) -> None:
            def mutator(payload: dict) -> None:
                target = payload
                for key in keys[:-1]:
                    target = target[key]
                target[keys[-1]] = value

            rewrite_json(path, mutator)

        cases = (
            (
                "split start/end transport class",
                lambda proof_dir: (
                    set_nested_value(
                        proof_dir / "end_graphql_bus_watch.json",
                        ["data", "busSummary", "status", "transportClass"],
                        "tcp",
                    )
                ),
                "ambiguous transport class across structured warmup snapshots",
            ),
            (
                "cold start already LIVE_READY",
                lambda proof_dir: (
                    set_nested_value(
                        proof_dir / "start_bus_observability.json",
                        ["summary", "status", "startup", "phase"],
                        "LIVE_READY",
                    )
                ),
                "family proof cold_start is not pre-LIVE_READY",
            ),
            (
                "cold start already available",
                lambda proof_dir: (
                    set_nested_value(
                        proof_dir / "start_graphql_bus_watch.json",
                        ["data", "busSummary", "status", "warmup", "state"],
                        "available",
                    )
                ),
                "family proof cold_start is not pre-available",
            ),
            (
                "end not LIVE_READY",
                lambda proof_dir: (
                    set_nested_value(
                        proof_dir / "end_bus_observability.json",
                        ["summary", "status", "startup", "phase"],
                        "LIVE_WARMUP",
                    )
                ),
                "family proof post_warmup is not LIVE_READY",
            ),
            (
                "end not available",
                lambda proof_dir: (
                    set_nested_value(
                        proof_dir / "end_graphql_bus_watch.json",
                        ["data", "busSummary", "status", "warmup", "state"],
                        "warming_up",
                    )
                ),
                "family proof post_warmup is not warmup available",
            ),
        )

        for label, mutator, expected_reason in cases:
            with self.subTest(label=label):
                with tempfile.TemporaryDirectory() as temp_dir:
                    proof_dir = pathlib.Path(temp_dir)
                    write_family_proof_artifacts(proof_dir, transport_class="ens")
                    mutator(proof_dir)
                    artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                        proof_dir,
                        "run-1",
                        "P03",
                        "proxy-single-client",
                        "required",
                        "ens",
                        proxy_transport="ens",
                        ebusd_transport="ens",
                    )

                self.assertFalse(artifact["ok"])
                self.assertEqual(artifact["eligibility"]["status"], "blocked")
                self.assertIn(expected_reason, artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_rejects_missing_family_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="")
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("missing transport class", artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_rejects_overclaim_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-dual-client",
                "optional",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "not_proven")
        self.assertIn("family scope mismatch", artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_rejects_non_ens_transport_class(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="tcp")
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "tcp",
                proxy_transport="tcp",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "not_proven")
        self.assertIn("transport_class='tcp'", artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_rejects_non_ens_gateway_transport(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "tcp",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "not_proven")
        self.assertIn("gateway_transport='tcp'", artifact["eligibility"]["reason"])

    def test_family_eligibility_command_allows_explicit_not_proven_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            output_path = proof_dir / "family_proof_eligibility.json"
            write_family_proof_artifacts(proof_dir, transport_class="tcp")
            exit_code = verifier.family_eligibility_command(
                argparse.Namespace(
                    proof_dir=str(proof_dir),
                    run_id="run-1",
                    case_id="P03",
                    kind="proxy-single-client",
                    passive_mode="required",
                    gateway_transport="tcp",
                    proxy_transport="tcp",
                    ebusd_transport="ebusd-tcp",
                    output=str(output_path),
                )
            )
            self.assertEqual(exit_code, 0)
            artifact = verifier.load_json(output_path)
            self.assertFalse(artifact["ok"])
            self.assertEqual(artifact["eligibility"]["status"], "not_proven")

    def test_family_proof_eligibility_rejects_incomplete_transport_class_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            end_graphql_path = proof_dir / "end_graphql_bus_watch.json"
            payload = verifier.load_json(end_graphql_path)
            payload["data"]["busSummary"]["status"].pop("transportClass", None)
            write_json(end_graphql_path, payload)
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("incomplete transport class", artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_rejects_null_transport_class_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            for graph_path in ("start_graphql_bus_watch.json", "end_graphql_bus_watch.json"):
                payload = verifier.load_json(proof_dir / graph_path)
                payload["data"]["busSummary"]["status"]["transportClass"] = None
                write_json(proof_dir / graph_path, payload)
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("missing data.busSummary.status.transportClass", artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_blocks_malformed_canary_verdict_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            (proof_dir / "canary_verdict.json").write_text("{\n", encoding="utf-8")
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("invalid canary verdict artifact", artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_blocks_schema_less_ok_canary_verdict(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            write_json(proof_dir / "canary_verdict.json", {"ok": True})
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("invalid canary verdict artifact", artifact["eligibility"]["reason"])
        self.assertIn("schema mismatch", artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_blocks_schema_valid_but_truncated_canary_verdict(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            write_json(
                proof_dir / "canary_verdict.json",
                {
                    "schema": verifier.CANARY_VERDICT_SCHEMA,
                    "ok": True,
                    "status": "pass",
                    "criteria": {
                        "no_mismatches": {
                            "ok": True,
                            "mismatch_count": 0,
                        }
                    },
                },
            )
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("invalid canary verdict artifact", artifact["eligibility"]["reason"])
        self.assertIn("missing canonical criteria gates", artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_blocks_schema_valid_single_canary_subset(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            canary_path = proof_dir / "canary_verdict.json"
            payload = verifier.load_json(canary_path)
            keep_id = sorted(payload["per_canary"].keys())[0]
            payload["per_canary"] = {keep_id: payload["per_canary"][keep_id]}
            payload["criteria"]["per_canary_interval_conclusive_rate"]["canaries_evaluated"] = 1
            payload["criteria"]["per_canary_interval_conclusive_rate"]["failing_canaries"] = []
            write_json(canary_path, payload)
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("invalid canary verdict artifact", artifact["eligibility"]["reason"])
        self.assertIn("canonical proof-set canary coverage mismatch", artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_blocks_schema_valid_but_zero_evidence_canary_verdict(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            canary_path = proof_dir / "canary_verdict.json"
            payload = verifier.load_json(canary_path)
            payload["ok"] = True
            payload["status"] = "pass"
            payload["criteria"]["per_canary_interval_conclusive_rate"]["ok"] = True
            payload["criteria"]["per_canary_interval_conclusive_rate"]["failing_canaries"] = []
            payload["criteria"]["per_canary_interval_conclusive_rate"]["canaries_evaluated"] = 0
            payload["per_canary"] = {}
            write_json(canary_path, payload)
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("invalid canary verdict artifact", artifact["eligibility"]["reason"])
        self.assertIn("missing evaluated canary evidence", artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_blocks_forged_full_canonical_canary_without_gate_evidence_details(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            canary_path = proof_dir / "canary_verdict.json"
            payload = verifier.load_json(canary_path)
            payload["ok"] = True
            payload["status"] = "pass"
            payload["criteria"]["read_avoidance_accounting"] = {
                "ok": True,
                "reason": "forged gate without canonical evidence detail",
            }
            payload["criteria"]["proof_window_traffic_minimums"] = {
                "ok": True,
                "reason": "forged gate without canonical evidence detail",
            }
            payload["criteria"]["feature_flag_consistency"] = {
                "ok": True,
                "reason": "forged gate without canonical evidence detail",
            }
            payload["criteria"]["warmup_behavior"] = {
                "ok": True,
                "waived": False,
                "reason": "forged gate without canonical evidence detail",
            }
            write_json(canary_path, payload)
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("invalid canary verdict artifact", artifact["eligibility"]["reason"])
        self.assertIn(
            "criteria.read_avoidance_accounting missing numeric direct_apply_total_delta",
            artifact["eligibility"]["reason"],
        )

    def test_family_proof_eligibility_blocks_forged_canonical_canary_verdict_without_summary_anchor_match(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            canary_path = proof_dir / "canary_verdict.json"
            payload = verifier.load_json(canary_path)
            payload["criteria"]["read_avoidance_accounting"]["direct_apply_total_delta"] = 999.0
            payload["criteria"]["read_avoidance_accounting"]["active_reads_avoided_total_delta"] = 1000.0
            write_json(canary_path, payload)
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("invalid canary verdict artifact", artifact["eligibility"]["reason"])
        self.assertIn("does not match anchored canary summary artifact", artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_blocks_contradictory_ok_canary_verdict(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            canary_path = proof_dir / "canary_verdict.json"
            payload = verifier.load_json(canary_path)
            payload["ok"] = True
            payload["status"] = "fail"
            write_json(canary_path, payload)
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("invalid canary verdict artifact", artifact["eligibility"]["reason"])
        self.assertIn("contradictory success semantics", artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_blocks_contradictory_canary_no_mismatches_accounting(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            canary_path = proof_dir / "canary_verdict.json"
            payload = verifier.load_json(canary_path)
            payload["ok"] = True
            payload["status"] = "pass"
            payload["criteria"]["no_mismatches"]["ok"] = True
            payload["criteria"]["no_mismatches"]["mismatch_count"] = 5
            write_json(canary_path, payload)
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("invalid canary verdict artifact", artifact["eligibility"]["reason"])
        self.assertIn("contradictory no_mismatches accounting", artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_blocks_contradictory_ok_replay_verdict(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            replay_path = proof_dir / "replay_falsification.json"
            payload = verifier.load_json(replay_path)
            payload["ok"] = True
            payload["status"] = "fail"
            payload["summary"]["fail"] = 1
            payload["summary"]["pass"] = max(0, int(payload["summary"].get("pass", 0)) - 1)
            payload["cases"][0]["status"] = "fail"
            write_json(replay_path, payload)
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("invalid replay falsification artifact", artifact["eligibility"]["reason"])
        self.assertIn("contradictory success semantics", artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_blocks_schema_valid_but_status_only_replay_verdict(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            write_json(
                proof_dir / "replay_falsification.json",
                {
                    "schema": verifier.REPLAY_FALSIFICATION_VERDICT_SCHEMA,
                    "ok": True,
                    "status": "pass",
                    "summary": {
                        "total_cases": 1,
                        "locked_cases": 1,
                        "pass": 1,
                        "fail": 0,
                        "informational": 0,
                        "behavior_artifact_ok": True,
                        "proof_run_ok": True,
                    },
                    "cases": [
                        {
                            "status": "pass",
                        }
                    ],
                },
            )
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("invalid replay falsification artifact", artifact["eligibility"]["reason"])
        self.assertIn("missing non-empty name", artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_blocks_non_string_replay_identity_fields(self) -> None:
        invalid_cases = (
            ("name", None, "missing non-empty name"),
            ("family", 17, "missing non-empty family"),
            ("response_class", 2, "missing non-empty response_class"),
            ("behavior_evidence.behavior_artifact_path", 123, "missing behavior_evidence.behavior_artifact_path"),
        )

        for field_name, invalid_value, expected_reason in invalid_cases:
            with self.subTest(field_name=field_name, invalid_value=invalid_value):
                with tempfile.TemporaryDirectory() as temp_dir:
                    proof_dir = pathlib.Path(temp_dir)
                    write_family_proof_artifacts(proof_dir, transport_class="ens")
                    replay_path = proof_dir / "replay_falsification.json"
                    payload = verifier.load_json(replay_path)
                    case_payload = payload["cases"][0]
                    if field_name == "behavior_evidence.behavior_artifact_path":
                        case_payload["behavior_evidence"]["behavior_artifact_path"] = invalid_value
                    else:
                        case_payload[field_name] = invalid_value
                    write_json(replay_path, payload)
                    artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                        proof_dir,
                        "run-1",
                        "P03",
                        "proxy-single-client",
                        "required",
                        "ens",
                        proxy_transport="ens",
                        ebusd_transport="ebusd-tcp",
                    )

                self.assertFalse(artifact["ok"])
                self.assertEqual(artifact["eligibility"]["status"], "blocked")
                self.assertIn("invalid replay falsification artifact", artifact["eligibility"]["reason"])
                self.assertIn(expected_reason, artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_blocks_forged_replay_case_semantic_contract_fields(self) -> None:
        forged_cases = (
            ("family", "FORGED", "family mismatches canonical replay corpus case contract"),
            ("response_class", "forged_response", "response_class mismatches canonical replay corpus case contract"),
            ("scenario_tags", ["passive", "forged_semantic_tag"], "scenario_tags mismatch canonical replay corpus case contract"),
            ("expected.reason", "forged replay expected reason", "expected.reason mismatches canonical replay corpus case contract"),
        )

        for field_name, forged_value, expected_reason in forged_cases:
            with self.subTest(field_name=field_name):
                with tempfile.TemporaryDirectory() as temp_dir:
                    proof_dir = pathlib.Path(temp_dir)
                    write_family_proof_artifacts(proof_dir, transport_class="ens")
                    replay_path = proof_dir / "replay_falsification.json"
                    payload = verifier.load_json(replay_path)
                    case_payload = payload["cases"][0]
                    if field_name == "expected.reason":
                        case_payload["expected"]["reason"] = forged_value
                    else:
                        case_payload[field_name] = forged_value
                    write_json(replay_path, payload)
                    artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                        proof_dir,
                        "run-1",
                        "P03",
                        "proxy-single-client",
                        "required",
                        "ens",
                        proxy_transport="ens",
                        ebusd_transport="ebusd-tcp",
                    )

                self.assertFalse(artifact["ok"])
                self.assertEqual(artifact["eligibility"]["status"], "blocked")
                self.assertIn("invalid replay falsification artifact", artifact["eligibility"]["reason"])
                self.assertIn(expected_reason, artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_blocks_coherent_forged_replay_expected_contract_fields(self) -> None:
        forged_cases = (
            ("expected.disposition", "falsification", "expected.disposition mismatches canonical replay corpus case contract"),
            ("expected.direct_apply", True, "expected.direct_apply mismatches canonical replay corpus case contract"),
        )

        for field_name, forged_value, expected_reason in forged_cases:
            with self.subTest(field_name=field_name):
                with tempfile.TemporaryDirectory() as temp_dir:
                    proof_dir = pathlib.Path(temp_dir)
                    write_family_proof_artifacts(proof_dir, transport_class="ens")
                    replay_path = proof_dir / "replay_falsification.json"
                    behavior_path = proof_dir / "replay_behavior.json"
                    replay_payload = verifier.load_json(replay_path)
                    behavior_payload = verifier.load_json(behavior_path)

                    replay_case = None
                    for case_payload in replay_payload["cases"]:
                        if str(case_payload.get("name", "")).strip() == "b524_value_bearing_enh":
                            replay_case = case_payload
                            break
                    if replay_case is None:
                        raise AssertionError("missing replay verdict case b524_value_bearing_enh")

                    behavior_case = None
                    for case_payload in behavior_payload["cases"]:
                        if str(case_payload.get("name", "")).strip() == "b524_value_bearing_enh":
                            behavior_case = case_payload
                            break
                    if behavior_case is None:
                        raise AssertionError("missing replay behavior case b524_value_bearing_enh")

                    if field_name == "expected.disposition":
                        replay_case["expected"]["disposition"] = forged_value
                        replay_case["disposition"] = forged_value
                        replay_case["observed"]["disposition"] = forged_value
                        replay_case["behavior_evidence"]["observed_disposition"] = forged_value
                        behavior_case["observed"]["disposition"] = forged_value
                    elif field_name == "expected.direct_apply":
                        replay_case["expected"]["direct_apply"] = forged_value
                        replay_case["direct_apply"] = forged_value
                        replay_case["observed"]["direct_apply"] = forged_value
                        replay_case["behavior_evidence"]["observed_direct_apply"] = forged_value
                        behavior_case["observed"]["direct_apply"] = forged_value
                    else:
                        raise AssertionError(f"unsupported field_name {field_name!r}")

                    write_json(replay_path, replay_payload)
                    write_json(behavior_path, behavior_payload)
                    artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                        proof_dir,
                        "run-1",
                        "P03",
                        "proxy-single-client",
                        "required",
                        "ens",
                        proxy_transport="ens",
                        ebusd_transport="ebusd-tcp",
                    )

                self.assertFalse(artifact["ok"])
                self.assertEqual(artifact["eligibility"]["status"], "blocked")
                self.assertIn("invalid replay falsification artifact", artifact["eligibility"]["reason"])
                self.assertIn(expected_reason, artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_blocks_forged_full_canonical_replay_without_behavior_evidence_anchors(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            replay_path = proof_dir / "replay_falsification.json"
            payload = verifier.load_json(replay_path)
            for case_payload in payload["cases"]:
                behavior_evidence = case_payload["behavior_evidence"]
                case_payload["behavior_evidence"] = {
                    "behavior_artifact_path": behavior_evidence["behavior_artifact_path"],
                    "behavior_artifact_ok": behavior_evidence["behavior_artifact_ok"],
                    "behavior_schema": behavior_evidence["behavior_schema"],
                }
            write_json(replay_path, payload)
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("invalid replay falsification artifact", artifact["eligibility"]["reason"])
        self.assertIn("missing behavior_evidence.case_name", artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_blocks_missing_replay_behavior_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            (proof_dir / "replay_behavior.json").unlink()
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("missing replay behavior artifact", artifact["eligibility"]["reason"])
        self.assertIn("invalid replay falsification artifact", artifact["eligibility"]["reason"])
        self.assertIn("missing anchored replay behavior artifact", artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_blocks_string_false_replay_behavior_ok(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            replay_path = proof_dir / "replay_behavior.json"
            payload = verifier.load_json(replay_path)
            payload["ok"] = "false"
            write_json(replay_path, payload)
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("replay behavior artifact missing boolean ok", artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_blocks_forged_replay_behavior_artifact_path_strings(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            replay_path = proof_dir / "replay_falsification.json"
            payload = verifier.load_json(replay_path)
            for case_payload in payload["cases"]:
                case_payload["behavior_evidence"]["behavior_artifact_path"] = "/tmp/forged/replay_behavior.json"
            write_json(replay_path, payload)
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("invalid replay falsification artifact", artifact["eligibility"]["reason"])
        self.assertIn(
            "behavior_evidence.behavior_artifact_path mismatches anchored replay_behavior artifact",
            artifact["eligibility"]["reason"],
        )

    def test_family_proof_eligibility_blocks_schema_valid_but_zero_evidence_replay_verdict(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            replay_path = proof_dir / "replay_falsification.json"
            payload = verifier.load_json(replay_path)
            payload["ok"] = True
            payload["status"] = "pass"
            payload["summary"]["total_cases"] = 0
            payload["summary"]["locked_cases"] = 0
            payload["summary"]["pass"] = 0
            payload["summary"]["fail"] = 0
            payload["summary"]["informational"] = 0
            payload["cases"] = []
            write_json(replay_path, payload)
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("invalid replay falsification artifact", artifact["eligibility"]["reason"])
        self.assertIn("missing evaluated replay evidence", artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_blocks_contradictory_replay_summary_pass_accounting(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            replay_path = proof_dir / "replay_falsification.json"
            payload = verifier.load_json(replay_path)
            payload["ok"] = True
            payload["status"] = "pass"
            payload["summary"]["fail"] = 0
            payload["summary"]["pass"] = 0
            for case in payload["cases"]:
                case["status"] = "pass"
            write_json(replay_path, payload)
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("invalid replay falsification artifact", artifact["eligibility"]["reason"])
        self.assertIn("contradictory summary pass/fail lock accounting", artifact["eligibility"]["reason"])

    def test_family_proof_eligibility_blocks_schema_valid_single_replay_case_subset(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            replay_path = proof_dir / "replay_falsification.json"
            payload = verifier.load_json(replay_path)
            payload["cases"] = [payload["cases"][0]]
            payload["summary"]["total_cases"] = 1
            payload["summary"]["locked_cases"] = 1
            payload["summary"]["pass"] = 1
            payload["summary"]["fail"] = 0
            payload["summary"]["informational"] = 0
            payload["ok"] = True
            payload["status"] = "pass"
            write_json(replay_path, payload)
            artifact = verifier.build_family_proof_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("invalid replay falsification artifact", artifact["eligibility"]["reason"])
        self.assertIn("canonical proof-set case coverage mismatch", artifact["eligibility"]["reason"])

    def test_family_eligibility_command_blocks_out_of_scope_family_with_malformed_replay_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            output_path = proof_dir / "family_proof_eligibility.json"
            write_family_proof_artifacts(proof_dir, transport_class="tcp")
            (proof_dir / "replay_falsification.json").write_text("{\n", encoding="utf-8")
            exit_code = verifier.family_eligibility_command(
                argparse.Namespace(
                    proof_dir=str(proof_dir),
                    run_id="run-1",
                    case_id="P03",
                    kind="proxy-dual-client",
                    passive_mode="optional",
                    gateway_transport="tcp",
                    proxy_transport="tcp",
                    ebusd_transport="ebusd-tcp",
                    output=str(output_path),
                )
            )
            artifact = verifier.load_json(output_path)

        self.assertEqual(exit_code, 1)
        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("invalid replay falsification artifact", artifact["eligibility"]["reason"])

    def test_family_eligibility_command_blocks_out_of_scope_family_with_schema_less_ok_upstream_verdicts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            output_path = proof_dir / "family_proof_eligibility.json"
            write_family_proof_artifacts(proof_dir, transport_class="tcp")
            write_json(proof_dir / "canary_verdict.json", {"ok": True})
            write_json(proof_dir / "replay_falsification.json", {"ok": True})
            exit_code = verifier.family_eligibility_command(
                argparse.Namespace(
                    proof_dir=str(proof_dir),
                    run_id="run-1",
                    case_id="P03",
                    kind="proxy-dual-client",
                    passive_mode="optional",
                    gateway_transport="tcp",
                    proxy_transport="tcp",
                    ebusd_transport="ebusd-tcp",
                    output=str(output_path),
                )
            )
            artifact = verifier.load_json(output_path)

        self.assertEqual(exit_code, 1)
        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("invalid canary verdict artifact", artifact["eligibility"]["reason"])
        self.assertIn("invalid replay falsification artifact", artifact["eligibility"]["reason"])

    def test_family_eligibility_command_blocks_corrupt_out_of_scope_family(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            output_path = proof_dir / "family_proof_eligibility.json"
            write_family_proof_artifacts(proof_dir, transport_class="tcp")
            (proof_dir / "canary_verdict.json").unlink()
            exit_code = verifier.family_eligibility_command(
                argparse.Namespace(
                    proof_dir=str(proof_dir),
                    run_id="run-1",
                    case_id="P03",
                    kind="proxy-dual-client",
                    passive_mode="optional",
                    gateway_transport="tcp",
                    proxy_transport="tcp",
                    ebusd_transport="ebusd-tcp",
                    output=str(output_path),
                )
            )
            artifact = verifier.load_json(output_path)

        self.assertEqual(exit_code, 1)
        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("missing canary verdict artifact", artifact["eligibility"]["reason"])


class PromotionEligibilityArtifactTests(unittest.TestCase):
    def test_promotion_eligibility_accepts_proven_family(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            write_family_proof_eligibility_artifact(
                proof_dir,
                proxy_transport="ens",
                ebusd_transport=CANONICAL_NO_EBUSD_TRANSPORT,
            )
            artifact = verifier.build_promotion_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport=CANONICAL_NO_EBUSD_TRANSPORT,
            )

        self.assertEqual(artifact["schema"], verifier.PROMOTION_ELIGIBILITY_SCHEMA)
        self.assertTrue(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "eligible_for_default_flip")
        self.assertEqual(artifact["promotion_scope"]["family_key"], "proxy-single-client/required/ens")
        self.assertEqual(artifact["matrix_topology"]["transport_class"], "ens")

    def test_promotion_eligibility_blocks_missing_proxy_transport_metadata(self) -> None:
        cases = (
            (
                "missing current proxy_transport",
                {
                    "family_proxy_transport": "ens",
                    "family_ebusd_transport": "",
                    "current_proxy_transport": "",
                    "current_ebusd_transport": "",
                },
                "missing promotion topology metadata: proxy_transport",
            ),
            (
                "missing family proof proxy_transport",
                {
                    "family_proxy_transport": "",
                    "family_ebusd_transport": "",
                    "current_proxy_transport": "ens",
                    "current_ebusd_transport": "",
                },
                "family proof eligibility missing proof_scope.proxy_transport",
            ),
        )

        for label, topology, expected_reason in cases:
            with self.subTest(label=label):
                with tempfile.TemporaryDirectory() as temp_dir:
                    proof_dir = pathlib.Path(temp_dir)
                    write_family_proof_artifacts(proof_dir, transport_class="ens")
                    write_family_proof_eligibility_artifact(
                        proof_dir,
                        proxy_transport=topology["family_proxy_transport"],
                        ebusd_transport=topology["family_ebusd_transport"],
                    )
                    artifact = verifier.build_promotion_eligibility_artifact_for_run(
                        proof_dir,
                        "run-1",
                        "P03",
                        "proxy-single-client",
                        "required",
                        "ens",
                        proxy_transport=topology["current_proxy_transport"],
                        ebusd_transport=topology["current_ebusd_transport"],
                    )

                self.assertFalse(artifact["ok"])
                self.assertEqual(artifact["eligibility"]["status"], "blocked")
                self.assertIn(expected_reason, artifact["eligibility"]["reason"])

    def test_promotion_eligibility_rejects_explicitly_banned_topologies(self) -> None:
        cases = (
            (
                "via-ebusd-tcp",
                {
                    "kind": "proxy-single-client",
                    "passive_mode": "required",
                    "gateway_transport": "ens",
                    "proxy_transport": "",
                    "ebusd_transport": "ebusd-tcp",
                },
                {
                    "kind": "proxy-single-client",
                    "passive_mode": "required",
                    "gateway_transport": "ens",
                    "proxy_transport": "",
                    "ebusd_transport": "ebusd-tcp",
                },
                "not_proven",
                "topology='via-ebusd-tcp'",
            ),
            (
                "contradictory ebusd transport axis",
                {
                    "kind": "proxy-single-client",
                    "passive_mode": "required",
                    "gateway_transport": "ens",
                    "proxy_transport": "ens",
                    "ebusd_transport": "ens",
                },
                {
                    "kind": "proxy-single-client",
                    "passive_mode": "required",
                    "gateway_transport": "ens",
                    "proxy_transport": "ens",
                    "ebusd_transport": "ens",
                },
                "not_proven",
                "ebusd_transport='ens'",
            ),
            (
                "proxy-dual-client",
                {
                    "kind": "proxy-dual-client",
                    "passive_mode": "optional",
                    "gateway_transport": "ens",
                    "proxy_transport": "ens",
                    "ebusd_transport": CANONICAL_NO_EBUSD_TRANSPORT,
                },
                {
                    "kind": "proxy-dual-client",
                    "passive_mode": "optional",
                    "gateway_transport": "ens",
                    "proxy_transport": "ens",
                    "ebusd_transport": CANONICAL_NO_EBUSD_TRANSPORT,
                },
                "not_proven",
                "topology='proxy-dual-client'",
            ),
            (
                "direct-adapter",
                {
                    "kind": "direct-adapter",
                    "passive_mode": "required",
                    "gateway_transport": "ens",
                    "proxy_transport": "",
                    "ebusd_transport": CANONICAL_NO_EBUSD_TRANSPORT,
                },
                {
                    "kind": "direct-adapter",
                    "passive_mode": "required",
                    "gateway_transport": "ens",
                    "proxy_transport": "",
                    "ebusd_transport": CANONICAL_NO_EBUSD_TRANSPORT,
                },
                "not_proven",
                "topology='direct-adapter'",
            ),
        )

        for label, family_topology, topology, expected_status, expected_reason in cases:
            with self.subTest(label=label):
                with tempfile.TemporaryDirectory() as temp_dir:
                    proof_dir = pathlib.Path(temp_dir)
                    write_family_proof_artifacts(
                        proof_dir,
                        transport_class="ens",
                        kind=family_topology["kind"],
                        passive_mode=family_topology["passive_mode"],
                    )
                    write_family_proof_eligibility_artifact(
                        proof_dir,
                        kind=family_topology["kind"],
                        passive_mode=family_topology["passive_mode"],
                        gateway_transport=family_topology["gateway_transport"],
                        proxy_transport=family_topology["proxy_transport"],
                        ebusd_transport=family_topology["ebusd_transport"],
                    )
                    artifact = verifier.build_promotion_eligibility_artifact_for_run(
                        proof_dir,
                        "run-1",
                        "P03",
                        topology["kind"],
                        topology["passive_mode"],
                        topology["gateway_transport"],
                        proxy_transport=topology["proxy_transport"],
                        ebusd_transport=topology["ebusd_transport"],
                    )

                self.assertFalse(artifact["ok"])
                self.assertEqual(artifact["eligibility"]["status"], expected_status)
                if expected_status == "not_proven":
                    self.assertIn("promotion scope mismatch", artifact["eligibility"]["reason"])
                self.assertIn(expected_reason, artifact["eligibility"]["reason"])

    def test_promotion_eligibility_rejects_unproven_family(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(
                proof_dir,
                transport_class="ens",
                kind="proxy-dual-client",
                passive_mode="optional",
            )
            write_family_proof_eligibility_artifact(
                proof_dir,
                kind="proxy-dual-client",
                passive_mode="optional",
            )
            artifact = verifier.build_promotion_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-dual-client",
                "optional",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "not_proven")
        self.assertIn("promotion scope mismatch", artifact["eligibility"]["reason"])

    def test_promotion_eligibility_rejects_upstream_ebusd_axis_when_current_run_drops_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            write_family_proof_eligibility_artifact(proof_dir, ebusd_transport="ebusd-tcp")
            artifact = verifier.build_promotion_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport=CANONICAL_NO_EBUSD_TRANSPORT,
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("proof_scope.ebusd_transport mismatch", artifact["eligibility"]["reason"])

    def test_promotion_eligibility_blocks_missing_upstream_ebusd_transport_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            write_family_proof_eligibility_artifact(
                proof_dir,
                proxy_transport="ens",
                ebusd_transport=CANONICAL_NO_EBUSD_TRANSPORT,
            )
            family_path = proof_dir / "family_proof_eligibility.json"
            family_artifact = verifier.load_json(family_path)
            del family_artifact["proof_scope"]["ebusd_transport"]
            write_json(family_path, family_artifact)
            artifact = verifier.build_promotion_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport=CANONICAL_NO_EBUSD_TRANSPORT,
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("missing proof_scope.ebusd_transport", artifact["eligibility"]["reason"])

    def test_promotion_eligibility_blocks_malformed_upstream_boolean_flags(self) -> None:
        cases = (
            ("canary_ok", "family proof eligibility upstream canary_ok must be boolean"),
            ("replay_ok", "family proof eligibility upstream replay_ok must be boolean"),
        )

        for field, expected_reason in cases:
            with self.subTest(field=field):
                with tempfile.TemporaryDirectory() as temp_dir:
                    proof_dir = pathlib.Path(temp_dir)
                    write_family_proof_artifacts(proof_dir, transport_class="ens")
                    write_family_proof_eligibility_artifact(
                        proof_dir,
                        proxy_transport="ens",
                        ebusd_transport=CANONICAL_NO_EBUSD_TRANSPORT,
                    )
                    family_path = proof_dir / "family_proof_eligibility.json"
                    family_artifact = verifier.load_json(family_path)
                    family_artifact["upstream_proof"][field] = "false"
                    write_json(family_path, family_artifact)
                    artifact = verifier.build_promotion_eligibility_artifact_for_run(
                        proof_dir,
                        "run-1",
                        "P03",
                        "proxy-single-client",
                        "required",
                        "ens",
                        proxy_transport="ens",
                        ebusd_transport=CANONICAL_NO_EBUSD_TRANSPORT,
                    )

                self.assertFalse(artifact["ok"])
                self.assertEqual(artifact["eligibility"]["status"], "blocked")
                self.assertIn(expected_reason, artifact["eligibility"]["reason"])

    def test_promotion_eligibility_blocks_string_false_replay_behavior_ok(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            write_family_proof_eligibility_artifact(
                proof_dir,
                proxy_transport="ens",
                ebusd_transport=CANONICAL_NO_EBUSD_TRANSPORT,
            )
            replay_path = proof_dir / "replay_behavior.json"
            payload = verifier.load_json(replay_path)
            payload["ok"] = "false"
            write_json(replay_path, payload)
            artifact = verifier.build_promotion_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport=CANONICAL_NO_EBUSD_TRANSPORT,
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("replay behavior artifact missing boolean ok", artifact["eligibility"]["reason"])

    def test_promotion_eligibility_blocks_contradictory_upstream_family_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            write_family_proof_eligibility_artifact(
                proof_dir,
                ebusd_transport=CANONICAL_NO_EBUSD_TRANSPORT,
            )
            family_path = proof_dir / "family_proof_eligibility.json"
            family_artifact = verifier.load_json(family_path)
            family_artifact["ok"] = False
            family_artifact["eligibility"]["status"] = "proven_for_default_flip"
            write_json(family_path, family_artifact)
            artifact = verifier.build_promotion_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport=CANONICAL_NO_EBUSD_TRANSPORT,
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("ok/status mismatch", artifact["eligibility"]["reason"])

    def test_promotion_eligibility_blocks_contradictory_family_identity_and_scope_fields(self) -> None:
        def set_nested_value(payload: dict, keys: list[str], value: object) -> None:
            target = payload
            for key in keys[:-1]:
                target = target[key]
            target[keys[-1]] = value

        cases = (
            (
                "family_identity.kind",
                ["family_identity", "kind"],
                "proxy-dual-client",
                "family_identity.kind mismatch",
            ),
            (
                "family_identity.passive_mode",
                ["family_identity", "passive_mode"],
                "optional",
                "family_identity.passive_mode mismatch",
            ),
            (
                "family_identity.transport_class",
                ["family_identity", "transport_class"],
                "tcp",
                "family_identity.transport_class mismatch",
            ),
            (
                "proof_scope.family_key",
                ["proof_scope", "family_key"],
                "proxy-dual-client/optional/ens",
                "proof_scope.family_key mismatch",
            ),
        )

        for label, path, value, expected_reason in cases:
            with self.subTest(label=label):
                with tempfile.TemporaryDirectory() as temp_dir:
                    proof_dir = pathlib.Path(temp_dir)
                    write_family_proof_artifacts(proof_dir, transport_class="ens")
                    write_family_proof_eligibility_artifact(
                        proof_dir,
                        ebusd_transport=CANONICAL_NO_EBUSD_TRANSPORT,
                    )
                    family_path = proof_dir / "family_proof_eligibility.json"
                    family_artifact = verifier.load_json(family_path)
                    set_nested_value(family_artifact, path, value)
                    write_json(family_path, family_artifact)
                    artifact = verifier.build_promotion_eligibility_artifact_for_run(
                        proof_dir,
                        "run-1",
                        "P03",
                        "proxy-single-client",
                        "required",
                        "ens",
                        proxy_transport="ens",
                        ebusd_transport=CANONICAL_NO_EBUSD_TRANSPORT,
                    )

                self.assertFalse(artifact["ok"])
                self.assertEqual(artifact["eligibility"]["status"], "blocked")
                self.assertIn(expected_reason, artifact["eligibility"]["reason"])

    def test_promotion_eligibility_rejects_missing_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            write_family_proof_eligibility_artifact(proof_dir)
            artifact = verifier.build_promotion_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("missing family kind", artifact["eligibility"]["reason"])

    def test_promotion_eligibility_rejects_failing_upstream_proof_criterion(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            write_family_proof_artifacts(proof_dir, transport_class="ens")
            write_family_proof_eligibility_artifact(proof_dir)
            canary_path = proof_dir / "canary_verdict.json"
            payload = verifier.load_json(canary_path)
            payload["criteria"]["no_mismatches"]["mismatch_count"] = 1
            payload["criteria"]["no_mismatches"]["ok"] = False
            payload["ok"] = False
            payload["status"] = "fail"
            write_json(canary_path, payload)
            artifact = verifier.build_promotion_eligibility_artifact_for_run(
                proof_dir,
                "run-1",
                "P03",
                "proxy-single-client",
                "required",
                "ens",
                proxy_transport="ens",
                ebusd_transport="ebusd-tcp",
            )

        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["eligibility"]["status"], "blocked")
        self.assertIn("invalid canary verdict artifact", artifact["eligibility"]["reason"])


class PublisherCadenceArtifactTests(unittest.TestCase):
    def write_publisher_cadence_proof_window(
        self,
        proof_dir: pathlib.Path,
        *,
        start_cadence_sec: object = 3600.0,
        end_cadence_sec: object = 3600.0,
        start_cadence_source: str = "config.semantic_state_interval",
        end_cadence_source: str = "config.semantic_state_interval",
    ) -> None:
        write_metrics(
            proof_dir / "start_metrics.prom",
            [
                'ebus_passive_capability_probe_outcomes_total{outcome="timed_out"} 0',
                'ebus_passive_tap_connected 1',
                'ebus_passive_warmup_state{state="warming_up"} 1',
                'ebus_passive_capability_probe_outcomes_total{outcome="confirmed"} 1',
            ],
        )
        write_metrics(
            proof_dir / "end_metrics.prom",
            [
                'ebus_passive_capability_probe_outcomes_total{outcome="timed_out"} 0',
                'ebus_passive_tap_connected 1',
                'ebus_passive_warmup_state{state="available"} 1',
                'ebus_passive_capability_probe_outcomes_total{outcome="confirmed"} 1',
            ],
        )
        write_structured_warmup_snapshot_bundle(
            proof_dir,
            "start",
            startup_phase="LIVE_WARMUP",
            warmup_state="warming_up",
            cache_epoch=1,
            live_epoch=0,
            transport_class="ens",
            publisher_cadence_sec=start_cadence_sec,  # type: ignore[arg-type]
            publisher_cadence_source=start_cadence_source,
        )
        write_structured_warmup_snapshot_bundle(
            proof_dir,
            "end",
            startup_phase="LIVE_READY",
            warmup_state="available",
            cache_epoch=1,
            live_epoch=1,
            transport_class="ens",
            publisher_cadence_sec=end_cadence_sec,  # type: ignore[arg-type]
            publisher_cadence_source=end_cadence_source,
        )

    def run_publisher_cadence_command(self, proof_dir: pathlib.Path, output_path: pathlib.Path) -> tuple[int, str]:
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            exit_code = verifier.main(
                [
                    "publisher-cadence",
                    "--proof-dir",
                    str(proof_dir),
                    "--run-id",
                    "run-1",
                    "--output",
                    str(output_path),
                ]
            )
        return exit_code, stderr.getvalue()

    def test_publisher_cadence_command_accepts_valid_structured_warmup_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            output_path = proof_dir / "publisher_cadence.json"
            self.write_publisher_cadence_proof_window(proof_dir)

            exit_code, stderr = self.run_publisher_cadence_command(proof_dir, output_path)
            artifact = verifier.load_json(output_path)

        self.assertEqual(exit_code, 0, msg=stderr)
        self.assertEqual(stderr, "")
        self.assertEqual(artifact["schema"], verifier.PUBLISHER_CADENCE_ARTIFACT_SCHEMA)
        self.assertTrue(artifact["ok"])
        self.assertEqual(artifact["coherence"]["source_anchor"], verifier.PUBLISHER_CADENCE_SOURCE_ANCHOR)
        self.assertEqual(
            artifact["start"]["graphql_bus_watch"]["data"]["busSummary"]["status"]["transportClass"],
            "ens",
        )
        self.assertEqual(
            artifact["end"]["publisher_cadence"]["publisher_cadence_source"],
            verifier.PUBLISHER_CADENCE_SOURCE_ANCHOR,
        )

    def test_publisher_cadence_artifact_rejects_source_mismatch_across_window(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            self.write_publisher_cadence_proof_window(proof_dir)
            artifact = verifier.build_publisher_cadence_artifact_for_phases(proof_dir, "run-1")
            artifact["start"]["publisher_cadence"]["publisher_cadence_source"] = "config.other_interval"

        ok, reason, details = verifier.evaluate_publisher_cadence(artifact)
        self.assertFalse(ok)
        self.assertEqual(details, {})
        self.assertIn("publisher cadence source mismatch across proof window", reason)

    def test_publisher_cadence_command_fails_closed_when_cadence_evidence_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            output_path = proof_dir / "publisher_cadence.json"
            self.write_publisher_cadence_proof_window(proof_dir)
            payload = verifier.load_json(proof_dir / "start_graphql_bus_watch.json")
            del payload["data"]["busSummary"]["status"]["publisherCadenceSec"]
            write_json(proof_dir / "start_graphql_bus_watch.json", payload)

            exit_code, stderr = self.run_publisher_cadence_command(proof_dir, output_path)

        self.assertEqual(exit_code, 1)
        self.assertIn("missing numeric publisherCadenceSec", stderr)
        self.assertFalse(output_path.exists())

    def test_publisher_cadence_command_fails_closed_on_malformed_cadence_value(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            output_path = proof_dir / "publisher_cadence.json"
            self.write_publisher_cadence_proof_window(proof_dir)
            payload = verifier.load_json(proof_dir / "end_graphql_bus_watch.json")
            payload["data"]["busSummary"]["status"]["publisherCadenceSec"] = "malformed"
            write_json(proof_dir / "end_graphql_bus_watch.json", payload)

            exit_code, stderr = self.run_publisher_cadence_command(proof_dir, output_path)

        self.assertEqual(exit_code, 1)
        self.assertIn("missing numeric publisherCadenceSec", stderr)
        self.assertFalse(output_path.exists())

    def test_publisher_cadence_command_fails_closed_on_source_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            output_path = proof_dir / "publisher_cadence.json"
            self.write_publisher_cadence_proof_window(proof_dir)
            bus_payload = verifier.load_json(proof_dir / "end_bus_observability.json")
            graphql_payload = verifier.load_json(proof_dir / "end_graphql_bus_watch.json")
            bus_payload["summary"]["status"]["publisher_cadence_source"] = "config.other_interval"
            graphql_payload["data"]["busSummary"]["status"]["publisherCadenceSource"] = "config.other_interval"
            write_json(proof_dir / "end_bus_observability.json", bus_payload)
            write_json(proof_dir / "end_graphql_bus_watch.json", graphql_payload)

            exit_code, stderr = self.run_publisher_cadence_command(proof_dir, output_path)

        self.assertEqual(exit_code, 1)
        self.assertIn("publisher cadence source anchor mismatch", stderr)
        self.assertFalse(output_path.exists())

    def test_publisher_cadence_command_fails_closed_on_value_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            output_path = proof_dir / "publisher_cadence.json"
            self.write_publisher_cadence_proof_window(proof_dir)
            bus_payload = verifier.load_json(proof_dir / "end_bus_observability.json")
            graphql_payload = verifier.load_json(proof_dir / "end_graphql_bus_watch.json")
            bus_payload["summary"]["status"]["publisher_cadence_sec"] = 1800.0
            graphql_payload["data"]["busSummary"]["status"]["publisherCadenceSec"] = 1800.0
            write_json(proof_dir / "end_bus_observability.json", bus_payload)
            write_json(proof_dir / "end_graphql_bus_watch.json", graphql_payload)

            exit_code, stderr = self.run_publisher_cadence_command(proof_dir, output_path)

        self.assertEqual(exit_code, 1)
        self.assertIn("publisher cadence mismatch across proof window", stderr)
        self.assertFalse(output_path.exists())


class CrossPlaneSkewArtifactTests(unittest.TestCase):
    def write_cross_plane_skew_proof_window(
        self,
        proof_dir: pathlib.Path,
        *,
        publisher_cadence_sec: object = 3600.0,
        configured_proof_sample_interval_sec: object = 300.0,
    ) -> None:
        PublisherCadenceArtifactTests.write_publisher_cadence_proof_window(
            self,
            proof_dir,
            start_cadence_sec=publisher_cadence_sec,
            end_cadence_sec=publisher_cadence_sec,
        )
        exit_code, stderr = PublisherCadenceArtifactTests.run_publisher_cadence_command(
            self,
            proof_dir,
            proof_dir / "publisher_cadence.json",
        )
        self.assertEqual(exit_code, 0, msg=stderr)
        self.assertTrue((proof_dir / "publisher_cadence.json").exists())

    @staticmethod
    def set_nested_value(payload: dict[str, object], path: tuple[str, ...], value: object) -> None:
        target: dict[str, object] = payload
        for key in path[:-1]:
            nested = target.get(key)
            if not isinstance(nested, dict):
                raise AssertionError(f"missing nested path component {'.'.join(path)}")
            target = nested
        target[path[-1]] = value

    @staticmethod
    def delete_nested_value(payload: dict[str, object], path: tuple[str, ...]) -> None:
        target: dict[str, object] = payload
        for key in path[:-1]:
            nested = target.get(key)
            if not isinstance(nested, dict):
                raise AssertionError(f"missing nested path component {'.'.join(path)}")
            target = nested
        del target[path[-1]]

    def run_cross_plane_skew_command(
        self,
        proof_dir: pathlib.Path,
        output_path: pathlib.Path,
        *,
        configured_proof_sample_interval_sec: object,
    ) -> tuple[int, str]:
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            try:
                exit_code = verifier.main(
                    [
                        "cross-plane-skew",
                        "--proof-dir",
                        str(proof_dir),
                        "--run-id",
                        "run-1",
                        "--output",
                        str(output_path),
                        "--configured-proof-sample-interval-sec",
                        str(configured_proof_sample_interval_sec),
                    ]
                )
            except SystemExit as exc:
                exit_code = int(exc.code) if isinstance(exc.code, int) else 1
        return exit_code, stderr.getvalue()

    def test_cross_plane_skew_command_accepts_same_phase_semantic_timestamps_and_bounds_against_maximum(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            output_path = proof_dir / "cross_plane_skew.json"
            self.write_cross_plane_skew_proof_window(
                proof_dir,
                publisher_cadence_sec=60.0,
                configured_proof_sample_interval_sec=300.0,
            )
            start_bus = verifier.load_json(proof_dir / "start_bus_observability.json")
            start_graphql = verifier.load_json(proof_dir / "start_graphql_bus_watch.json")
            end_bus = verifier.load_json(proof_dir / "end_bus_observability.json")
            end_graphql = verifier.load_json(proof_dir / "end_graphql_bus_watch.json")

            for payload, payload_kind, timestamp in (
                (start_bus, "bus", "2026-03-28T00:00:00Z"),
                (start_graphql, "graphql", "2026-03-28T00:00:01Z"),
                (end_bus, "bus", "2026-03-28T00:01:00Z"),
                (end_graphql, "graphql", "2026-03-28T00:01:01Z"),
            ):
                if payload_kind == "bus":
                    self.set_nested_value(payload, ("summary", "last_updated_at"), timestamp)
                    self.set_nested_value(payload, ("summary", "status", "last_updated_at"), timestamp)
                    self.set_nested_value(
                        payload,
                        ("summary", "status", "startup", "last_updated_at"),
                        "1999-01-01T00:00:00Z",
                    )
                    self.set_nested_value(
                        payload,
                        ("summary", "status", "feature_flags", "last_updated_at"),
                        "1999-01-01T00:00:00Z",
                    )
                else:
                    self.set_nested_value(payload, ("data", "busSummary", "lastUpdatedAt"), timestamp)
                    self.set_nested_value(payload, ("data", "busSummary", "status", "lastUpdatedAt"), timestamp)
                    self.set_nested_value(
                        payload,
                        ("data", "busSummary", "status", "startup", "lastUpdatedAt"),
                        "1999-01-01T00:00:00Z",
                    )
                    self.set_nested_value(
                        payload,
                        ("data", "busSummary", "status", "featureFlags", "lastUpdatedAt"),
                        "1999-01-01T00:00:00Z",
                    )
                    self.set_nested_value(payload, ("data", "watchSummary", "lastUpdatedAt"), timestamp)

            write_json(proof_dir / "start_bus_observability.json", start_bus)
            write_json(proof_dir / "start_graphql_bus_watch.json", start_graphql)
            write_json(proof_dir / "end_bus_observability.json", end_bus)
            write_json(proof_dir / "end_graphql_bus_watch.json", end_graphql)

            exit_code, stderr = self.run_cross_plane_skew_command(
                proof_dir,
                output_path,
                configured_proof_sample_interval_sec=300.0,
            )
            artifact = verifier.load_json(output_path)

        self.assertEqual(exit_code, 0, msg=stderr)
        self.assertTrue(artifact["ok"])
        self.assertEqual(artifact["status"], "pass")
        self.assertEqual(artifact["proof_metadata"]["configured_proof_sample_interval_sec"], 300.0)
        self.assertEqual(artifact["proof_metadata"]["publisher_cadence_sec"], 60.0)
        self.assertEqual(artifact["summary"]["target_max_skew_sec"], 300.0)
        self.assertTrue(artifact["summary"]["phases_within_target"])
        self.assertEqual(artifact["publisher_cadence"]["ok"], True)
        self.assertEqual(
            artifact["evidence"]["same_phase_semantic_last_updated_at_fields"],
            [
                "bus_observability.summary_last_updated_at",
                "bus_observability.status_last_updated_at",
                "graphql_bus_watch.summary_last_updated_at",
                "graphql_bus_watch.status_last_updated_at",
                "graphql_bus_watch.watch_summary_last_updated_at",
            ],
        )
        self.assertIn("cross_plane_skew.json", str(output_path))

    def test_cross_plane_skew_command_blocks_missing_publisher_cadence_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            output_path = proof_dir / "cross_plane_skew.json"
            self.write_cross_plane_skew_proof_window(proof_dir)
            (proof_dir / "publisher_cadence.json").unlink()

            exit_code, _stderr = self.run_cross_plane_skew_command(
                proof_dir,
                output_path,
                configured_proof_sample_interval_sec=300.0,
            )
            artifact = verifier.load_json(output_path)

        self.assertNotEqual(exit_code, 0)
        self.assertEqual(artifact["status"], "fail")
        self.assertFalse(artifact["ok"])
        self.assertFalse(artifact["publisher_cadence"]["ok"])
        self.assertIn("publisher cadence", " ".join(artifact["reasons"]).lower())

    def test_cross_plane_skew_command_blocks_malformed_proof_sample_interval(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            output_path = proof_dir / "cross_plane_skew.json"
            self.write_cross_plane_skew_proof_window(proof_dir)

            exit_code, _stderr = self.run_cross_plane_skew_command(
                proof_dir,
                output_path,
                configured_proof_sample_interval_sec=-1.0,
            )
            artifact = verifier.load_json(output_path)

        self.assertNotEqual(exit_code, 0)
        self.assertEqual(artifact["status"], "fail")
        self.assertFalse(artifact["ok"])
        self.assertEqual(artifact["proof_metadata"]["configured_proof_sample_interval_sec"], -1.0)
        self.assertIn("invalid configured_proof_sample_interval_sec", " ".join(artifact["reasons"]).lower())

    def test_cross_plane_skew_command_blocks_missing_same_phase_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            output_path = proof_dir / "cross_plane_skew.json"
            self.write_cross_plane_skew_proof_window(proof_dir)
            end_graphql = verifier.load_json(proof_dir / "end_graphql_bus_watch.json")
            self.delete_nested_value(end_graphql, ("data", "watchSummary", "lastUpdatedAt"))
            write_json(proof_dir / "end_graphql_bus_watch.json", end_graphql)

            exit_code, _stderr = self.run_cross_plane_skew_command(
                proof_dir,
                output_path,
                configured_proof_sample_interval_sec=300.0,
            )
            artifact = verifier.load_json(output_path)

        self.assertNotEqual(exit_code, 0)
        self.assertEqual(artifact["status"], "fail")
        self.assertFalse(artifact["ok"])
        self.assertIn("watchsummary", " ".join(artifact["reasons"]).lower())

    def test_cross_plane_skew_command_blocks_skew_exceeded(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            proof_dir = pathlib.Path(temp_dir)
            output_path = proof_dir / "cross_plane_skew.json"
            self.write_cross_plane_skew_proof_window(
                proof_dir,
                publisher_cadence_sec=60.0,
                configured_proof_sample_interval_sec=300.0,
            )
            end_graphql = verifier.load_json(proof_dir / "end_graphql_bus_watch.json")
            self.set_nested_value(
                end_graphql,
                ("data", "watchSummary", "lastUpdatedAt"),
                "2026-03-28T01:10:00Z",
            )
            write_json(proof_dir / "end_graphql_bus_watch.json", end_graphql)

            exit_code, _stderr = self.run_cross_plane_skew_command(
                proof_dir,
                output_path,
                configured_proof_sample_interval_sec=300.0,
            )
            artifact = verifier.load_json(output_path)

        self.assertNotEqual(exit_code, 0)
        self.assertEqual(artifact["status"], "fail")
        self.assertFalse(artifact["ok"])
        self.assertIn("skew exceeded", " ".join(artifact["reasons"]).lower())


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
            canonical_canary_ids_literal = json.dumps(canonical_family_proof_canary_ids())

            fake_python = fake_bin / "python3"
            fake_python.write_text(
                "\n".join(
                    [
                        "#!/usr/bin/python3",
                        "import json",
                        "import os",
                        "import pathlib",
                        "import sys",
                        "",
                        "real_python = os.environ.get('REAL_PYTHON3')",
                        "if not real_python:",
                        "    raise SystemExit('REAL_PYTHON3 is required')",
                        "",
                        "args = sys.argv[1:]",
                        "script_path = args[0] if len(args) >= 1 else ''",
                        "command = args[1] if len(args) >= 2 else ''",
                        "",
                        "def write_json(path_text, payload):",
                        "    path = pathlib.Path(path_text)",
                        "    path.parent.mkdir(parents=True, exist_ok=True)",
                        "    path.write_text(json.dumps(payload, indent=2) + '\\n', encoding='utf-8')",
                        "",
                        "if script_path.endswith('/scripts/passive_canary_verifier.py') and command == 'verify-phase':",
                        "    output = ''",
                        "    phase = ''",
                        "    run_id = ''",
                        "    baseline = ''",
                        "    i = 2",
                        "    while i < len(args):",
                        "        token = args[i]",
                        "        if token == '--output' and i + 1 < len(args):",
                        "            output = args[i + 1]",
                        "            i += 2",
                        "            continue",
                        "        if token == '--phase' and i + 1 < len(args):",
                        "            phase = args[i + 1]",
                        "            i += 2",
                        "            continue",
                        "        if token == '--run-id' and i + 1 < len(args):",
                        "            run_id = args[i + 1]",
                        "            i += 2",
                        "            continue",
                        "        if token == '--baseline' and i + 1 < len(args):",
                        "            baseline = args[i + 1]",
                        "            i += 2",
                        "            continue",
                        "        i += 1",
                        "",
                        "    if phase == 'end' and int(os.environ.get('FAKE_CANARY_END_DELAY_SEC', '0')) != 0:",
                        "        import time",
                        "        time.sleep(int(os.environ['FAKE_CANARY_END_DELAY_SEC']))",
                        "",
                        f"    canary_ids = {canonical_canary_ids_literal}",
                        "    status = os.environ.get('FAKE_CANARY_STATUS', 'pass')",
                        "    pass_count = len(canary_ids) if status == 'pass' else 0",
                        "    mismatch_count = len(canary_ids) if status == 'mismatch' else 0",
                        "    metrics_count = 'unknown'",
                        "    metrics_state_file = os.environ.get('FAKE_METRICS_STATE_FILE')",
                        "    if metrics_state_file and pathlib.Path(metrics_state_file).exists():",
                        "        metrics_count = pathlib.Path(metrics_state_file).read_text(encoding='utf-8').strip()",
                        "    results = [",
                        "        {'id': canary_id, 'family': 'B524', 'status': status, 'conclusive': True}",
                        "        for canary_id in canary_ids",
                        "    ]",
                        "",
                        "    payload = {",
                        "        'schema': 'p03_canary_phase_result_v1',",
                        "        'run_id': run_id,",
                        "        'phase': phase,",
                        "        'results': results,",
                        "        'summary': {'total': len(results), 'pass': pass_count, 'mismatch': mismatch_count, 'inconclusive': 0, 'conclusive': len(results)},",
                        "    }",
                        "    if output:",
                        "        write_json(output, payload)",
                        "    if baseline:",
                        "        baseline_path = pathlib.Path(baseline)",
                        "        baseline_path.parent.mkdir(parents=True, exist_ok=True)",
                        "        baseline_values = {canary_id: 'BEEF' for canary_id in canary_ids}",
                        "        baseline_path.write_text(json.dumps(baseline_values, separators=(',', ':')) + '\\n', encoding='utf-8')",
                        "    phase_log = os.environ.get('FAKE_CANARY_PHASE_LOG')",
                        "    if phase_log:",
                        "        with open(phase_log, 'a', encoding='utf-8') as handle:",
                        "            handle.write(f'{phase}:{metrics_count}\\n')",
                        "    matrix_log_dir = os.environ.get('MATRIX_LOG_DIR')",
                        "    if matrix_log_dir:",
                        "        write_json(pathlib.Path(matrix_log_dir) / 'proof_artifacts' / f'canary_phase_{phase}.json', payload)",
                        "    raise SystemExit(0)",
                        "",
                        "if script_path.endswith('/scripts/passive_canary_verifier.py') and command == 'replay-verdict':",
                        "    proof_dir = ''",
                        "    i = 2",
                        "    while i < len(args):",
                        "        token = args[i]",
                        "        if token == '--proof-dir' and i + 1 < len(args):",
                        "            proof_dir = args[i + 1]",
                        "            i += 2",
                        "            continue",
                        "        i += 1",
                        "    if os.environ.get('FAKE_CANARY_PHASE_LOG'):",
                        "        with open(os.environ['FAKE_CANARY_PHASE_LOG'], 'a', encoding='utf-8') as handle:",
                        "            handle.write(f'replay:{proof_dir}\\n')",
                        "    os.execv(real_python, [real_python] + sys.argv[1:])",
                        "",
                        "os.execv(real_python, [real_python] + sys.argv[1:])",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            fake_python.chmod(0o755)

            fake_go = fake_bin / "go"
            fake_go.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -euo pipefail

                    real_go="${REAL_GO:?REAL_GO is required}"
                    if [[ "${1:-}" == "test" ]]; then
                      out_path="${REPLAY_BEHAVIOR_ARTIFACT_PATH:-}"
                      if [[ -z "${out_path}" ]]; then
                        echo "REPLAY_BEHAVIOR_ARTIFACT_PATH is required" >&2
                        exit 2
                      fi
                      mkdir -p "$(dirname "${out_path}")"
                      cat > "${out_path}" <<'EOF'
                    {
                      "schema": "observe_first_replay_behavior_v1",
                      "captured_at": "2026-03-28T00:00:00+00:00",
                      "source": "go_replay_harness",
                      "ok": true,
                      "summary": {
                        "total_cases": 3,
                        "locked_cases": 3,
                        "observed_cases": 3,
                        "observation_failure_cases": 0
                      },
                      "cases": [
                        {
                          "name": "b524_value_bearing_enh",
                          "status": "observed",
                          "reason": "observed B524 runtime observer fallback produced unmatched third-party disposition",
                          "observed": {
                            "direct_apply": false,
                            "disposition": "ambiguity",
                            "raw_disposition": "unmatched_third_party",
                            "third_party_eligible": true,
                            "direct_apply_policy": "state_default",
                            "replay_harness": "active_passive_deduplicator"
                          }
                        },
                        {
                          "name": "collision_episode",
                          "status": "observed",
                          "reason": "observed proxy-observer collision stream produced no direct-apply replay path",
                          "observed": {
                            "direct_apply": false,
                            "disposition": "falsification",
                            "transaction_events": 0,
                            "observed_symbols": 8,
                            "completed_transactions": 0,
                            "passive_state": "warming_up",
                            "replay_harness": "proxy_ens_observer"
                          }
                        },
                        {
                          "name": "timeout_no_progress",
                          "status": "observed",
                          "reason": "observed truncated observer stream produced no progress and no direct-apply path",
                          "observed": {
                            "direct_apply": false,
                            "disposition": "falsification",
                            "transaction_events": 0,
                            "observed_symbols": 2,
                            "completed_transactions": 0,
                            "passive_state": "warming_up",
                            "replay_harness": "proxy_ens_observer_timeout"
                          }
                        }
                      ]
                    }
                    EOF
                      exit 0
                    fi
                    exec "${real_go}" "$@"
                    """
                ),
                encoding="utf-8",
            )
            fake_go.chmod(0o755)

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
                      startup_mode="${FAKE_BUS_STARTUP_MODE:-initially_live_warmup_then_live_ready}"
                      phase="LIVE_READY"
                      summary_last_updated_at="2026-03-28T00:05:00Z"
                      status_last_updated_at="2026-03-28T00:05:00Z"
                      startup_last_updated_at="2026-03-28T00:05:00Z"
                      feature_flags_last_updated_at="2026-03-28T00:00:00Z"
                      cache_epoch=1
                      live_epoch=1
                      if [[ "${startup_mode}" == "initially_live_warmup_then_live_ready" ]]; then
                        state_file="${FAKE_METRICS_STATE_FILE:?FAKE_METRICS_STATE_FILE is required}"
                        warmup_calls="${FAKE_BUS_LIVE_WARMUP_CALLS:-1}"
                        metrics_count=0
                        if [[ -f "${state_file}" ]]; then
                          metrics_count="$(cat "${state_file}")"
                        fi
                        if [[ "${metrics_count}" -le "${warmup_calls}" ]]; then
                          phase="LIVE_WARMUP"
                          summary_last_updated_at="2026-03-28T00:00:00Z"
                          status_last_updated_at="2026-03-28T00:00:00Z"
                          startup_last_updated_at="2026-03-28T00:00:00Z"
                          live_epoch=0
                        fi
                      fi
                      publisher_cadence_mode="${FAKE_PUBLISHER_CADENCE_MODE:-present}"
                      publisher_cadence_sec="${FAKE_PUBLISHER_CADENCE_SEC:-3600}"
                      publisher_cadence_source="${FAKE_PUBLISHER_CADENCE_SOURCE:-config.semantic_state_interval}"
                      include_publisher_cadence=1
                      if [[ "${publisher_cadence_mode}" == "missing" ]]; then
                        include_publisher_cadence=0
                      elif [[ "${publisher_cadence_mode}" == "mismatch" ]]; then
                        publisher_cadence_sec="1800"
                        publisher_cadence_source="config.semantic_state_interval.alt"
                      elif [[ "${publisher_cadence_mode}" == "malformed" ]]; then
                        publisher_cadence_sec="not_a_number"
                      fi
                      if [[ "${include_publisher_cadence}" == "1" ]]; then
                        cat <<EOF
                    {"summary":{"last_updated_at":"${summary_last_updated_at}","status":{"last_updated_at":"${status_last_updated_at}","startup":{"phase":"${phase}","cache_epoch":${cache_epoch},"live_epoch":${live_epoch},"last_updated_at":"${startup_last_updated_at}"},"publisher_cadence_sec":${publisher_cadence_sec},"publisher_cadence_source":"${publisher_cadence_source}","feature_flags":{"observe_first_enabled":true,"passive_state_direct_apply":false,"passive_config_direct_apply":false,"external_write_policy":"record_only","normalizations":[],"last_updated_at":"${feature_flags_last_updated_at}"}}}}
                    EOF
                      else
                        cat <<EOF
                    {"summary":{"last_updated_at":"${summary_last_updated_at}","status":{"last_updated_at":"${status_last_updated_at}","startup":{"phase":"${phase}","cache_epoch":${cache_epoch},"live_epoch":${live_epoch},"last_updated_at":"${startup_last_updated_at}"},"feature_flags":{"observe_first_enabled":true,"passive_state_direct_apply":false,"passive_config_direct_apply":false,"external_write_policy":"record_only","normalizations":[],"last_updated_at":"${feature_flags_last_updated_at}"}}}}
                    EOF
                      fi
                      exit 0
                    fi

                    if [[ "${url}" == *"/graphql" ]]; then
                      if [[ "${data}" == *"busSummary"* ]]; then
                      startup_mode="${FAKE_BUS_STARTUP_MODE:-initially_live_warmup_then_live_ready}"
                      phase="LIVE_READY"
                      bus_summary_last_updated_at="2026-03-28T00:05:01Z"
                      bus_status_last_updated_at="2026-03-28T00:05:01Z"
                      startup_last_updated_at="2026-03-28T00:05:01Z"
                      feature_flags_last_updated_at="2026-03-28T00:00:01Z"
                      watch_summary_last_updated_at="2026-03-28T00:05:02Z"
                      cache_epoch=1
                      live_epoch=1
                      if [[ "${startup_mode}" == "initially_live_warmup_then_live_ready" ]]; then
                        state_file="${FAKE_METRICS_STATE_FILE:?FAKE_METRICS_STATE_FILE is required}"
                        warmup_calls="${FAKE_BUS_LIVE_WARMUP_CALLS:-1}"
                        metrics_count=0
                        if [[ -f "${state_file}" ]]; then
                          metrics_count="$(cat "${state_file}")"
                        fi
                        if [[ "${metrics_count}" -le "${warmup_calls}" ]]; then
                          phase="LIVE_WARMUP"
                          bus_summary_last_updated_at="2026-03-28T00:00:01Z"
                          bus_status_last_updated_at="2026-03-28T00:00:01Z"
                          startup_last_updated_at="2026-03-28T00:00:01Z"
                          watch_summary_last_updated_at="2026-03-28T00:00:02Z"
                          live_epoch=0
                        fi
                      fi
                      publisher_cadence_mode="${FAKE_PUBLISHER_CADENCE_MODE:-present}"
                      publisher_cadence_sec="${FAKE_PUBLISHER_CADENCE_SEC:-3600}"
                      publisher_cadence_source="${FAKE_PUBLISHER_CADENCE_SOURCE:-config.semantic_state_interval}"
                      include_publisher_cadence=1
                      if [[ "${publisher_cadence_mode}" == "missing" ]]; then
                        include_publisher_cadence=0
                      elif [[ "${publisher_cadence_mode}" == "mismatch" ]]; then
                        publisher_cadence_sec="900"
                        publisher_cadence_source="config.semantic_state_interval.alt"
                      elif [[ "${publisher_cadence_mode}" == "malformed" ]]; then
                        publisher_cadence_sec="not_a_number"
                      fi
                      warmup_state="warming_up"
                      if [[ "${phase}" == "LIVE_READY" ]]; then
                        warmup_state="available"
                      fi
                      if [[ "${include_publisher_cadence}" == "1" ]]; then
                        cat <<EOF
                    {
                      "data": {
                        "busSummary": {
                          "lastUpdatedAt": "${bus_summary_last_updated_at}",
                          "status": {
                            "lastUpdatedAt": "${bus_status_last_updated_at}",
                            "transportClass": "ens",
                            "startup": {
                              "phase": "${phase}",
                              "cacheEpoch": ${cache_epoch},
                              "liveEpoch": ${live_epoch},
                              "lastUpdatedAt": "${startup_last_updated_at}"
                            },
                            "publisherCadenceSec": ${publisher_cadence_sec},
                            "publisherCadenceSource": "${publisher_cadence_source}",
                            "warmup": {
                              "state": "${warmup_state}",
                              "blocker": "",
                              "elapsedSeconds": 0,
                              "completedTransactions": 0,
                              "requiredTransactions": 0,
                              "completionMode": "proof_window"
                            },
                            "featureFlags": {
                              "lastUpdatedAt": "${feature_flags_last_updated_at}",
                              "observeFirstEnabled": true,
                              "passiveStateDirectApply": false,
                              "passiveConfigDirectApply": false,
                              "externalWritePolicy": "record_only",
                              "normalizations": []
                            }
                          }
                        },
                        "watchSummary": {
                          "lastUpdatedAt": "${watch_summary_last_updated_at}",
                          "inventory": {"totalEntries": 1},
                          "activationCounts": {"catalogDescriptors": 1, "activeKeys": 1, "sourceClasses": []},
                          "directApplyEligibilityClasses": [],
                          "degraded": {
                            "active": false,
                            "shadowingEnabled": false,
                            "pinnedBudgetDegraded": false,
                            "compactorDegraded": false,
                            "reasons": []
                          }
                        }
                      }
                    }
                    EOF
                      else
                        cat <<EOF
                    {
                      "data": {
                        "busSummary": {
                          "lastUpdatedAt": "${bus_summary_last_updated_at}",
                          "status": {
                            "lastUpdatedAt": "${bus_status_last_updated_at}",
                            "transportClass": "ens",
                            "startup": {
                              "phase": "${phase}",
                              "cacheEpoch": ${cache_epoch},
                              "liveEpoch": ${live_epoch},
                              "lastUpdatedAt": "${startup_last_updated_at}"
                            },
                            "warmup": {
                              "state": "${warmup_state}",
                              "blocker": "",
                              "elapsedSeconds": 0,
                              "completedTransactions": 0,
                              "requiredTransactions": 0,
                              "completionMode": "proof_window"
                            },
                            "featureFlags": {
                              "lastUpdatedAt": "${feature_flags_last_updated_at}",
                              "observeFirstEnabled": true,
                              "passiveStateDirectApply": false,
                              "passiveConfigDirectApply": false,
                              "externalWritePolicy": "record_only",
                              "normalizations": []
                            }
                          }
                        },
                        "watchSummary": {
                          "lastUpdatedAt": "${watch_summary_last_updated_at}",
                          "inventory": {"totalEntries": 1},
                          "activationCounts": {"catalogDescriptors": 1, "activeKeys": 1, "sourceClasses": []},
                          "directApplyEligibilityClasses": [],
                          "degraded": {
                            "active": false,
                            "shadowingEnabled": false,
                            "pinnedBudgetDegraded": false,
                            "compactorDegraded": false,
                            "reasons": []
                          }
                        }
                      }
                    }
                    EOF
                      fi
                        exit 0
                      fi
                      printf '%s\\n' '{"data":{"devices":[{"address":"0x15","deviceId":"BASV2"}]}}'
                      exit 0
                    fi
                    fi

                    echo "unsupported fake curl url: ${url}" >&2
                    exit 22
                    """
                ),
                encoding="utf-8",
            )
            fake_curl.chmod(0o755)

            env = os.environ.copy()
            real_go = shutil.which("go")
            if not real_go:
                self.fail("go binary not found on PATH")
            env.update(
                {
                    "MATRIX_CASE_ID": "P03",
                    "MATRIX_CASE_KIND": "proxy-single-client",
                    "MATRIX_PASSIVE_MODE": "required",
                    "MATRIX_GATEWAY_TRANSPORT": "ens",
                    "MATRIX_PROXY_TRANSPORT": "ens",
                    "MATRIX_EBUSD_TRANSPORT": CANONICAL_NO_EBUSD_TRANSPORT,
                    "MATRIX_USES_EBUSD": "0",
                    "MATRIX_GW15_PROOF_MODE": "1",
                    "PASSIVE_SMOKE_TIMEOUT_SEC": "6",
                    "PASSIVE_SMOKE_POLL_INTERVAL_SEC": "1",
                    "PASSIVE_PROOF_SAMPLE_INTERVAL_SEC": "3600",
                    "MATRIX_LOG_DIR": str(log_dir),
                    "MATRIX_GATEWAY_BASE_URL": "http://fake-gateway:18083",
                    "MATRIX_GRAPHQL_URL": "http://fake-gateway:18083/graphql",
                    "MATRIX_METRICS_URL": "http://fake-gateway:18083/metrics",
                    "REAL_PYTHON3": sys.executable,
                    "REAL_GO": real_go,
                    "FAKE_CANARY_STATUS": canary_status,
                    "FAKE_CANARY_PHASE_LOG": str(temp_path / "fake_canary_phase_log.txt"),
                    "FAKE_METRICS_STATE_FILE": str(temp_path / "fake_metrics_count.txt"),
                    "PATH": f"{fake_bin}:{env.get('PATH', '')}",
                }
            )
            pathlib.Path(env["FAKE_METRICS_STATE_FILE"]).write_text("-1\n", encoding="utf-8")
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
                replay_behavior_path = proof_dir / "replay_behavior.json"
                replay_verdict_path = proof_dir / "replay_falsification.json"
                family_eligibility_path = proof_dir / "family_proof_eligibility.json"
                promotion_eligibility_path = proof_dir / "promotion_eligibility.json"
                publisher_cadence_path = proof_dir / "publisher_cadence.json"
                cross_plane_skew_path = proof_dir / "cross_plane_skew.json"
                phase_log_path = temp_path / "fake_canary_phase_log.txt"
                if summary_path.exists():
                    artifacts["summary"] = json.loads(summary_path.read_text(encoding="utf-8"))
                if verdict_path.exists():
                    artifacts["verdict"] = json.loads(verdict_path.read_text(encoding="utf-8"))
                if replay_behavior_path.exists():
                    artifacts["replay_behavior"] = json.loads(replay_behavior_path.read_text(encoding="utf-8"))
                if replay_verdict_path.exists():
                    artifacts["replay_verdict"] = json.loads(replay_verdict_path.read_text(encoding="utf-8"))
                if family_eligibility_path.exists():
                    artifacts["family_eligibility"] = json.loads(
                        family_eligibility_path.read_text(encoding="utf-8")
                    )
                if promotion_eligibility_path.exists():
                    artifacts["promotion_eligibility"] = json.loads(
                        promotion_eligibility_path.read_text(encoding="utf-8")
                    )
                if publisher_cadence_path.exists():
                    artifacts["publisher_cadence"] = json.loads(
                        publisher_cadence_path.read_text(encoding="utf-8")
                    )
                if cross_plane_skew_path.exists():
                    artifacts["cross_plane_skew"] = json.loads(
                        cross_plane_skew_path.read_text(encoding="utf-8")
                    )
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

    def test_smoke_emits_replay_falsification_artifact_when_canary_verdict_is_good(self) -> None:
        result, artifacts = self.run_smoke_with_fake_tools_detailed("pass")
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        replay_behavior = artifacts.get("replay_behavior")
        self.assertIsInstance(replay_behavior, dict)
        self.assertTrue(replay_behavior["ok"])
        replay_verdict = artifacts.get("replay_verdict")
        self.assertIsInstance(replay_verdict, dict)
        self.assertTrue(replay_verdict["ok"])
        self.assertEqual(replay_verdict["summary"]["behavior_artifact_ok"], True)

    def test_smoke_emits_family_eligibility_artifact_when_canary_verdict_is_good(self) -> None:
        result, artifacts = self.run_smoke_with_fake_tools_detailed("pass")
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        family_eligibility = artifacts.get("family_eligibility")
        self.assertIsInstance(family_eligibility, dict)
        self.assertTrue(family_eligibility["ok"])
        self.assertEqual(
            family_eligibility["family_identity"]["family_key"],
            "proxy-single-client/required/ens",
        )
        self.assertEqual(
            family_eligibility["proof_scope"]["ebusd_transport"],
            CANONICAL_NO_EBUSD_TRANSPORT,
        )
        promotion_eligibility = artifacts.get("promotion_eligibility")
        self.assertIsInstance(promotion_eligibility, dict)
        self.assertTrue(promotion_eligibility["ok"])
        self.assertEqual(
            promotion_eligibility["eligibility"]["status"],
            "eligible_for_default_flip",
        )
        self.assertEqual(
            promotion_eligibility["promotion_scope"]["ebusd_transport"],
            CANONICAL_NO_EBUSD_TRANSPORT,
        )

    def test_smoke_emits_cross_plane_skew_artifact_when_canary_verdict_is_good(self) -> None:
        result, artifacts = self.run_smoke_with_fake_tools_detailed(
            "pass",
            extra_env={
                "PASSIVE_PROOF_HOLD_SEC": "0",
                "PASSIVE_SMOKE_TIMEOUT_SEC": "8",
                "PASSIVE_SMOKE_POLL_INTERVAL_SEC": "1",
                "PASSIVE_PROOF_SAMPLE_INTERVAL_SEC": "300",
                "FAKE_PUBLISHER_CADENCE_SEC": "60",
            },
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        cross_plane_skew = artifacts.get("cross_plane_skew")
        self.assertIsInstance(cross_plane_skew, dict)
        self.assertTrue(cross_plane_skew["ok"])
        self.assertEqual(cross_plane_skew["status"], "pass")
        self.assertEqual(cross_plane_skew["summary"]["target_max_skew_sec"], 300.0)
        self.assertTrue(cross_plane_skew["summary"]["phases_within_target"])

    def test_smoke_emits_family_eligibility_artifact_for_not_proven_family(self) -> None:
        result, artifacts = self.run_smoke_with_fake_tools_detailed(
            "pass",
            extra_env={
                "MATRIX_CASE_KIND": "proxy-dual-client",
            },
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        family_eligibility = artifacts.get("family_eligibility")
        self.assertIsInstance(family_eligibility, dict)
        self.assertFalse(family_eligibility["ok"])
        self.assertEqual(family_eligibility["eligibility"]["status"], "not_proven")
        self.assertIn("family scope mismatch", family_eligibility["eligibility"]["reason"])
        promotion_eligibility = artifacts.get("promotion_eligibility")
        self.assertIsInstance(promotion_eligibility, dict)
        self.assertFalse(promotion_eligibility["ok"])
        self.assertEqual(promotion_eligibility["eligibility"]["status"], "not_proven")
        self.assertIn("promotion scope mismatch", promotion_eligibility["eligibility"]["reason"])

    def test_smoke_fails_closed_when_topology_metadata_is_missing(self) -> None:
        result, artifacts = self.run_smoke_with_fake_tools_detailed(
            "pass",
            extra_env={
                "MATRIX_CASE_KIND": "",
                "MATRIX_GATEWAY_TRANSPORT": "",
                "MATRIX_PROXY_TRANSPORT": "",
            },
        )
        self.assertNotEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("missing P03 topology metadata", result.stderr)
        self.assertNotIn("family_eligibility", artifacts)
        self.assertNotIn("promotion_eligibility", artifacts)

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
                "PASSIVE_SMOKE_TIMEOUT_SEC": "12",
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
        phase_names = [entry.split(":", 1)[0] for entry in phase_log]
        self.assertEqual(phase_names[0], "start")
        self.assertGreaterEqual(int(phase_log[0].split(":", 1)[1]), 3)
        self.assertIn("end", phase_names)
        self.assertIn("replay", phase_names)
        self.assertLess(phase_names.index("end"), phase_names.index("replay"))
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
        phase_names = [entry.split(":", 1)[0] for entry in phase_log]
        self.assertEqual(phase_names[0], "start")
        self.assertIn("sample_0001", phase_names)
        self.assertIn("end", phase_names)
        self.assertIn("replay", phase_names)
        self.assertLess(phase_names.index("end"), phase_names.index("replay"))
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
                "FAKE_METRICS_HEALTHY_BEFORE_FAIL_CALLS": "3",
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
