#!/usr/bin/env python3
"""Fail-closed validator for the public adversarial runtime report v1."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = ROOT / "docs/platform/schemas/adversarial-runtime-report-v1.schema.json"
SCHEMA_ID = "https://raw.githubusercontent.com/Project-Helianthus/helianthus-docs-ebus/main/docs/platform/schemas/adversarial-runtime-report-v1.schema.json"
SUITE_ID = "helianthus.adversarial.ADV01-04"
FIXTURE_MANIFEST = ROOT / "docs/platform/fixtures/adversarial-runtime/v1/fixture-input-manifest.json"
FIXTURE_SCHEMA = ROOT / "docs/platform/schemas/adversarial-runtime-offline-fixture-v1.schema.json"
FIXTURE_SCHEMA_ID = "https://raw.githubusercontent.com/Project-Helianthus/helianthus-docs-ebus/main/docs/platform/schemas/adversarial-runtime-offline-fixture-v1.schema.json"
MAX_BYTES = 1024 * 1024
MAX_FIXTURE_BYTES = 4 * 1024 * 1024
TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

CATALOG = {
    "ADV-01": {
        "name": "HA Core/integration consumer restart while gateway stays stable",
        "trigger_kind": "ha_consumer_restart",
        "recovery_target": "ha_consumer_synchronized",
        "maximum_recovery_ms": 90000,
        "zones_required": True,
        "dhw_required": True,
        "maximum_collisions_delta": 5,
        "baseline_phase": "LIVE_READY",
        "end_phase": "LIVE_READY",
        "infrastructure_reasons": {"ha_harness_unavailable", "observer_unavailable"},
        "events": ["restart_requested", "consumer_stopped", "consumer_started", "ha_consumer_synchronized"],
        "activation_event": "consumer_stopped",
        "anchor": "consumer_stopped",
        "recovery_event": "ha_consumer_synchronized",
    },
    "ADV-02": {
        "name": "eBUS adapter reset while polling",
        "trigger_kind": "adapter_reset",
        "recovery_target": "gateway_live_ready",
        "maximum_recovery_ms": 120000,
        "zones_required": True,
        "dhw_required": False,
        "maximum_collisions_delta": 20,
        "baseline_phase": "LIVE_READY",
        "end_phase": "LIVE_READY",
        "infrastructure_reasons": {"adapter_control_unavailable", "observer_unavailable"},
        "events": ["reset_requested", "reset_started", "transport_unavailable", "transport_available", "gateway_live_ready"],
        "activation_event": "reset_started",
        "anchor": "reset_started",
        "recovery_event": "gateway_live_ready",
    },
    "ADV-03": {
        "name": "60 second gateway-to-adapter transport partition and recovery",
        "trigger_kind": "transport_partition",
        "recovery_target": "gateway_live_ready",
        "maximum_recovery_ms": 90000,
        "zones_required": True,
        "dhw_required": False,
        "maximum_collisions_delta": 10,
        "baseline_phase": "LIVE_READY",
        "end_phase": "LIVE_READY",
        "infrastructure_reasons": {"network_fault_injector_unavailable", "observer_unavailable"},
        "events": ["partition_requested", "partition_active", "partition_cleared", "gateway_live_ready"],
        "activation_event": "partition_active",
        "anchor": "partition_cleared",
        "recovery_event": "gateway_live_ready",
    },
    "ADV-04": {
        "name": "fresh isolated gateway boot with corrupted cache fixture",
        "trigger_kind": "isolated_corrupt_cache_boot",
        "recovery_target": "gateway_live_ready",
        "maximum_recovery_ms": 120000,
        "zones_required": True,
        "dhw_required": True,
        "maximum_collisions_delta": 5,
        "baseline_phase": "BOOT_INIT",
        "end_phase": "LIVE_READY",
        "infrastructure_reasons": {"isolated_cache_sandbox_unavailable", "observer_unavailable"},
        "events": ["isolated_cache_staged", "runtime_started", "gateway_live_ready"],
        "activation_event": "runtime_started",
        "anchor": "runtime_started",
        "recovery_event": "gateway_live_ready",
    },
}


class ValidationError(ValueError):
    pass


def _no_duplicates(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValidationError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def _non_integer_number(value):
    raise ValidationError(f"non-integer JSON number: {value}")


def _non_finite_number(value):
    raise ValidationError(f"non-finite JSON number: {value}")


def _bounded_integer(value):
    if len(value.lstrip("-")) > 16:
        raise ValidationError("JSON integer token exceeds 16 digits")
    return int(value)


def parse_json_bytes(raw: bytes, label: str):
    if len(raw) > MAX_BYTES:
        raise ValidationError(f"{label} exceeds {MAX_BYTES} bytes")
    try:
        text = raw.decode("utf-8", "strict")
    except UnicodeDecodeError as error:
        raise ValidationError(f"{label} is not valid UTF-8") from error
    try:
        return json.loads(
            text,
            object_pairs_hook=_no_duplicates,
            parse_int=_bounded_integer,
            parse_float=_non_integer_number,
            parse_constant=_non_finite_number,
        )
    except ValidationError:
        raise
    except (json.JSONDecodeError, ValueError, RecursionError) as error:
        raise ValidationError(f"malformed {label} JSON: {error}") from error


def parse_report_bytes(raw: bytes):
    return parse_json_bytes(raw, "report")


def read_regular_bytes(path: Path, limit: int, label: str):
    fd = None
    try:
        flags = os.O_RDONLY | os.O_NONBLOCK
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(path, flags)
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValidationError(f"{label} read failed")
        with os.fdopen(fd, "rb") as stream:
            fd = None
            return stream.read(limit + 1)
    except OSError as error:
        raise ValidationError(f"{label} read failed") from error
    finally:
        if fd is not None:
            os.close(fd)


def read_report_bytes(path: Path):
    return read_regular_bytes(path, MAX_BYTES, "report")


def load_report(path: Path):
    return parse_report_bytes(read_report_bytes(path))


def _stamp(value):
    try:
        return dt.datetime.strptime(value, TS_FORMAT).replace(tzinfo=dt.timezone.utc)
    except (TypeError, ValueError) as error:
        raise ValidationError("timestamp is outside the restricted RFC3339 profile") from error


def _same_timestamp(anchor, offset_ms, observed):
    try:
        expected = anchor + dt.timedelta(milliseconds=offset_ms)
    except OverflowError as error:
        raise ValidationError("timestamp offset overflows supported datetime range") from error
    return abs((expected - _stamp(observed)).total_seconds() * 1000) <= 1


def _error(errors, rule):
    errors.add(rule)


def _validate_catalog(definition, errors):
    scenario_id = definition.get("scenario_id")
    expected = CATALOG.get(scenario_id)
    if expected is None:
        _error(errors, "scenario_catalog")
        return None
    fields = {
        "name": expected["name"],
        "duration_limit_ms": 180000,
        "trigger_kind": expected["trigger_kind"],
        "recovery_target": expected["recovery_target"],
        "maximum_recovery_ms": expected["maximum_recovery_ms"],
        "minimum_live_epoch_delta": 2,
        "zones_required": expected["zones_required"],
        "dhw_required": expected["dhw_required"],
        "maximum_collisions_delta": expected["maximum_collisions_delta"],
    }
    for field, value in fields.items():
        if definition.get(field) != value:
            _error(errors, "scenario_catalog")
    return expected


def _event_offsets(events, start, errors, *, elapsed_ms=None):
    offsets = {}
    previous = -1
    for event in events:
        kind = event["kind"]
        offset = event["offset_ms"]
        if offset < previous or kind in offsets:
            _error(errors, "action_order")
        previous = offset
        offsets[kind] = offset
        if elapsed_ms is not None and offset > elapsed_ms:
            _error(errors, "action_bounds")
            continue
        if not _same_timestamp(start, offset, event["at"]):
            _error(errors, "wall_monotonic_binding")
    return offsets


def _validate_recovery_fields(timing, events, expected, errors):
    by_kind = {event["kind"]: event for event in events}
    anchor = expected["anchor"]
    observed = expected["recovery_event"]
    if anchor not in by_kind or observed not in by_kind:
        if any(timing[field] is not None for field in ("recovery_anchor", "recovery_observed", "recovery_ms")):
            _error(errors, "recovery_fields")
        return
    recovery_ms = by_kind[observed]["offset_ms"] - by_kind[anchor]["offset_ms"]
    if (timing["recovery_anchor"], timing["recovery_observed"], timing["recovery_ms"]) != (anchor, observed, recovery_ms):
        _error(errors, "recovery_fields")


def _validate_evaluated(scenario, expected, errors):
    definition = scenario["definition"]
    timing = scenario["timing"]
    metrics = scenario["metrics"]
    evaluation = scenario["evaluation"]
    start = _stamp(timing["scenario_started_at"])
    end = _stamp(timing["scenario_ended_at"])
    if end < start or not _same_timestamp(start, timing["elapsed_ms"], timing["scenario_ended_at"]):
        _error(errors, "wall_monotonic_binding")

    events = scenario["action"]["events"]
    kinds = [event["kind"] for event in events]
    if kinds != expected["events"]:
        _error(errors, "action_semantics")
    offsets = _event_offsets(events, start, errors, elapsed_ms=timing["elapsed_ms"])
    if timing["error_bound_ms"] < max((event["error_bound_ms"] for event in events), default=0):
        _error(errors, "timing_error_bound")
    anchor = expected["anchor"]
    recovery_event = expected["recovery_event"]
    if timing["recovery_anchor"] != anchor or timing["recovery_observed"] != recovery_event:
        _error(errors, "action_semantics")
    if anchor not in offsets or recovery_event not in offsets:
        _error(errors, "action_semantics")
        recovery_ms = None
        recovery_error_bound_ms = None
    else:
        recovery_ms = offsets[recovery_event] - offsets[anchor]
        event_by_kind = {event["kind"]: event for event in events}
        recovery_error_bound_ms = (
            event_by_kind[anchor]["error_bound_ms"]
            + event_by_kind[recovery_event]["error_bound_ms"]
        )
    if recovery_ms is None or timing["recovery_ms"] != recovery_ms:
        _error(errors, "recovery_measurement")

    if definition["scenario_id"] == "ADV-03":
        if "partition_active" not in offsets or "partition_cleared" not in offsets:
            _error(errors, "partition_duration")
        else:
            event_by_kind = {event["kind"]: event for event in events}
            partition_error_bound_ms = (
                event_by_kind["partition_active"]["error_bound_ms"]
                + event_by_kind["partition_cleared"]["error_bound_ms"]
            )
            if abs(offsets["partition_cleared"] - offsets["partition_active"] - 60000) + partition_error_bound_ms > 1000:
                _error(errors, "partition_duration")

    baseline, finish, delta = metrics["baseline"], metrics["end"], metrics["delta"]
    if baseline["semantic_startup_current_phase"] != expected["baseline_phase"] or finish["semantic_startup_current_phase"] != expected["end_phase"]:
        _error(errors, "startup_phase")
    if baseline["counter_epoch"] != finish["counter_epoch"]:
        _error(errors, "counter_epoch")
    for snap in (baseline, finish):
        if not _same_timestamp(start, snap["offset_ms"], snap["captured_at"]):
            _error(errors, "wall_monotonic_binding")
    if baseline["offset_ms"] > finish["offset_ms"] or finish["offset_ms"] > timing["elapsed_ms"]:
        _error(errors, "snapshot_order")
    if abs(baseline["offset_ms"]) > timing["error_bound_ms"] or abs(finish["offset_ms"] - timing["elapsed_ms"]) > timing["error_bound_ms"]:
        _error(errors, "snapshot_window")
    live_delta = finish["semantic_live_epoch"] - baseline["semantic_live_epoch"]
    collision_delta = finish["semantic_bus_collisions_total"] - baseline["semantic_bus_collisions_total"]
    if live_delta < 0 or collision_delta < 0:
        _error(errors, "negative_counter_delta")
    if delta != {"semantic_live_epoch": live_delta, "semantic_bus_collisions_total": collision_delta}:
        _error(errors, "counter_delta")

    duration = evaluation["duration"]
    action = evaluation["action"]
    recovery = evaluation["recovery"]
    live = evaluation["live_epoch"]
    zones = evaluation["zones"]
    dhw = evaluation["dhw"]
    collisions = evaluation["collisions"]
    if duration["error_bound_ms"] != timing["error_bound_ms"] or recovery["error_bound_ms"] != recovery_error_bound_ms:
        _error(errors, "timing_error_bound")
    expected_decisions = {
        "duration": abs(timing["elapsed_ms"] - 180000) + timing["error_bound_ms"] <= 1000,
        "action": not {"action_semantics", "partition_duration"} & errors,
        "recovery": recovery_ms is not None and recovery_ms + recovery_error_bound_ms <= expected["maximum_recovery_ms"],
        "live_epoch": live_delta >= 2,
        "zones": (not expected["zones_required"]) or finish["semantic_zone_count"] > 0,
        "dhw": (not expected["dhw_required"]) or finish["semantic_dhw_present"],
        "collisions": collision_delta <= expected["maximum_collisions_delta"],
    }
    if timing["elapsed_ms"] < 179000 or timing["elapsed_ms"] > 181000:
        _error(errors, "duration_window")
    if duration["observed_ms"] != timing["elapsed_ms"] or duration["passed"] != expected_decisions["duration"]:
        _error(errors, "duration_decision")
    if action != {"expected_kind": definition["trigger_kind"], "observed_kind": definition["trigger_kind"], "passed": expected_decisions["action"]}:
        _error(errors, "action_decision")
    if recovery["maximum_ms"] != expected["maximum_recovery_ms"] or recovery["observed_ms"] != recovery_ms or recovery["passed"] != expected_decisions["recovery"]:
        _error(errors, "recovery_decision")
    if live != {"minimum": 2, "observed": live_delta, "passed": expected_decisions["live_epoch"]}:
        _error(errors, "live_epoch_decision")
    if zones != {"required": expected["zones_required"], "observed": finish["semantic_zone_count"] > 0, "passed": expected_decisions["zones"]}:
        _error(errors, "zones_decision")
    if dhw != {"required": expected["dhw_required"], "observed": finish["semantic_dhw_present"], "passed": expected_decisions["dhw"]}:
        _error(errors, "dhw_decision")
    if collisions != {"maximum": expected["maximum_collisions_delta"], "observed": collision_delta, "passed": expected_decisions["collisions"]}:
        _error(errors, "collisions_decision")
    expected_outcome = "pass" if all(expected_decisions.values()) and not errors else "fail"
    if scenario["outcome"] != expected_outcome:
        _error(errors, "evaluated_outcome")


def _fixture_error(rule):
    raise ValidationError(f"fixture {rule}")


def _closed_object(value, required):
    return isinstance(value, dict) and set(value) == set(required)


def _fixture_identifier(value):
    return isinstance(value, str) and re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", value) is not None and len(value) <= 64


def _fixture_path(relative):
    if not isinstance(relative, str) or not relative or "\\" in relative or "\x00" in relative:
        _fixture_error("artifact_path")
    parts = relative.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        _fixture_error("artifact_path")
    root = FIXTURE_MANIFEST.parent
    candidate = root
    for part in parts:
        candidate /= part
        if candidate.is_symlink():
            _fixture_error("artifact_path")
    try:
        candidate.resolve().relative_to(root.resolve())
    except ValueError:
        _fixture_error("artifact_path")
    return candidate


def _project_fixture_snapshot(snapshot, start):
    if snapshot is None:
        return None
    result = dict(snapshot)
    result["captured_at"] = _fixture_timestamp(start + dt.timedelta(milliseconds=snapshot["offset_ms"]))
    return result


def _fixture_timestamp(value):
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"


def _project_fixture_events(events, start):
    return [
        {
            "kind": event["kind"],
            "source": "fixture",
            "at": _fixture_timestamp(start + dt.timedelta(milliseconds=event["offset_ms"])),
            "offset_ms": event["offset_ms"],
            "error_bound_ms": event["error_bound_ms"],
        }
        for event in events
    ]


def _validate_fixture_projection(report, case, driver):
    if driver["fixture_case_id"] != case["case_id"] or driver["suite"] != {"id": SUITE_ID, "version": 1}:
        _fixture_error("case_binding")
    if report["execution"]["run_id"] != driver["run_id"]:
        _fixture_error("run_id_projection")
    scenarios = driver["scenarios"]
    if [item["scenario_id"] for item in scenarios] != list(CATALOG):
        _fixture_error("driver_catalog")
    if len(scenarios) != len(report["scenarios"]):
        _fixture_error("driver_projection")
    manifest_resources = case["resource_artifact_ids"]
    seen_resources = []
    for report_scenario, driver_scenario in zip(report["scenarios"], scenarios):
        scenario_id = report_scenario["definition"]["scenario_id"]
        expected = CATALOG.get(scenario_id)
        if expected is None or driver_scenario["scenario_id"] != scenario_id or driver_scenario["trigger_kind"] != expected["trigger_kind"]:
            _fixture_error("driver_catalog")
        precondition = driver_scenario["precondition"]
        unavailable = not precondition["available"]
        resources = driver_scenario["resource_artifact_ids"]
        seen_resources.extend(resources)
        reaches_cache = any(event["kind"] == "isolated_cache_staged" for event in driver_scenario["events"])
        if scenario_id == "ADV-04" and reaches_cache:
            if len(resources) != 1:
                _fixture_error("driver_resources")
        elif resources:
            _fixture_error("driver_resources")
        if unavailable:
            if precondition["unavailable_reason"] not in expected["infrastructure_reasons"] or driver_scenario["events"] or driver_scenario["observations"] != {"baseline": None, "end": None} or driver_scenario["terminal_error"] is not None or resources:
                _fixture_error("driver_precondition")
            expected_errors = [{"phase": "precondition", "code": "precondition_unavailable"}]
            expected_reason = precondition["unavailable_reason"]
        else:
            if precondition["unavailable_reason"] is not None:
                _fixture_error("driver_precondition")
            expected_errors = [] if driver_scenario["terminal_error"] is None else [driver_scenario["terminal_error"]]
            expected_reason = None
        start = _stamp(report_scenario["timing"]["scenario_started_at"])
        if report_scenario["action"]["events"] != _project_fixture_events(driver_scenario["events"], start):
            _fixture_error("event_projection")
        observations = driver_scenario["observations"]
        if report_scenario["metrics"]["baseline"] != _project_fixture_snapshot(observations["baseline"], start) or report_scenario["metrics"]["end"] != _project_fixture_snapshot(observations["end"], start):
            _fixture_error("observation_projection")
        if report_scenario["errors"] != expected_errors or report_scenario["infrastructure_reason"] != expected_reason:
            _fixture_error("terminal_projection")
    if seen_resources != manifest_resources or len(set(seen_resources)) != len(seen_resources):
        _fixture_error("case_resources")


def load_fixture_inputs(report):
    manifest_raw = read_regular_bytes(FIXTURE_MANIFEST, MAX_BYTES, "fixture manifest")
    manifest = parse_json_bytes(manifest_raw, "fixture manifest")
    required = {"fixture_contract", "suite", "purpose", "artifacts", "cases"}
    if not _closed_object(manifest, required) or manifest["fixture_contract"] != "helianthus.adversarial-runtime.fixture-set/v1" or manifest["suite"] != {"id": SUITE_ID, "version": 1} or manifest["purpose"] != "deterministic synthetic offline executor inputs; not live evidence":
        _fixture_error("manifest_shape")
    artifact_keys = {"artifact_id", "role", "path", "media_type", "size_bytes", "sha256"}
    case_keys = {"case_id", "driver_artifact_id", "resource_artifact_ids"}
    artifacts = manifest["artifacts"]
    cases = manifest["cases"]
    if not isinstance(artifacts, list) or not isinstance(cases, list) or not artifacts or not cases:
        _fixture_error("manifest_shape")
    artifact_ids = []
    paths = []
    artifacts_by_id = {}
    total = 0
    for artifact in artifacts:
        if not _closed_object(artifact, artifact_keys) or artifact["role"] not in {"scenario-driver", "cache-image"} or not _fixture_identifier(artifact["artifact_id"]) or not isinstance(artifact["size_bytes"], int) or artifact["size_bytes"] < 0 or artifact["size_bytes"] > MAX_BYTES or not isinstance(artifact["sha256"], str) or re.fullmatch(r"[0-9a-f]{64}", artifact["sha256"]) is None:
            _fixture_error("manifest_artifact")
        expected_media = "application/json" if artifact["role"] == "scenario-driver" else "application/octet-stream"
        if artifact["media_type"] != expected_media:
            _fixture_error("manifest_artifact")
        artifact_ids.append(artifact["artifact_id"])
        paths.append(artifact["path"])
        raw = read_regular_bytes(_fixture_path(artifact["path"]), MAX_BYTES, "fixture artifact")
        if len(raw) > MAX_BYTES or len(raw) != artifact["size_bytes"] or hashlib.sha256(raw).hexdigest() != artifact["sha256"]:
            _fixture_error("artifact_digest")
        total += len(raw)
        artifacts_by_id[artifact["artifact_id"]] = (artifact, raw)
    if artifact_ids != sorted(artifact_ids) or len(set(artifact_ids)) != len(artifact_ids) or len(set(paths)) != len(paths) or total > MAX_FIXTURE_BYTES:
        _fixture_error("manifest_artifact")
    case_ids = []
    cases_by_id = {}
    drivers_by_case = {}
    driver_run_ids = []
    for case in cases:
        if not _closed_object(case, case_keys) or not _fixture_identifier(case["case_id"]) or not isinstance(case["resource_artifact_ids"], list):
            _fixture_error("manifest_case")
        case_ids.append(case["case_id"])
        driver = artifacts_by_id.get(case["driver_artifact_id"])
        if driver is None or driver[0]["role"] != "scenario-driver" or case["driver_artifact_id"] != f"driver-{case['case_id']}" or any(not _fixture_identifier(resource) or resource not in artifacts_by_id or artifacts_by_id[resource][0]["role"] != "cache-image" for resource in case["resource_artifact_ids"]):
            _fixture_error("manifest_case")
        if len(set(case["resource_artifact_ids"])) != len(case["resource_artifact_ids"]):
            _fixture_error("manifest_case")
        driver_value = parse_json_bytes(driver[1], "fixture driver")
        validate_schema_bytes(driver[1], FIXTURE_SCHEMA)
        if driver_value["fixture_case_id"] != case["case_id"] or driver_value["suite"] != {"id": SUITE_ID, "version": 1}:
            _fixture_error("case_binding")
        driver_run_ids.append(driver_value["run_id"])
        drivers_by_case[case["case_id"]] = driver_value
        cases_by_id[case["case_id"]] = case
    if case_ids != sorted(case_ids) or len(set(case_ids)) != len(case_ids) or len(set(driver_run_ids)) != len(driver_run_ids):
        _fixture_error("manifest_case")
    declared_artifacts = {case["driver_artifact_id"] for case in cases} | {resource for case in cases for resource in case["resource_artifact_ids"]}
    if declared_artifacts != set(artifacts_by_id):
        _fixture_error("manifest_case")
    provenance = report["provenance"]
    digest = hashlib.sha256(manifest_raw).hexdigest()
    if provenance["fixture_set_sha256"] != digest or provenance["subject"]["artifact_sha256"] != digest:
        _fixture_error("manifest_digest")
    case = cases_by_id.get(provenance["fixture_case_id"])
    if case is None:
        _fixture_error("case_binding")
    return case, drivers_by_case[case["case_id"]]


def validate_fixture_inputs(report):
    case, driver = load_fixture_inputs(report)
    _validate_fixture_projection(report, case, driver)


def validate_gateway_input(path: Path, report):
    raw = read_report_bytes(path)
    if hashlib.sha256(raw).hexdigest() != report["provenance"]["producer"]["input_gateway_report_sha256"]:
        raise ValidationError("gateway input digest mismatch")
    gateway_report = parse_report_bytes(raw)
    validate_schema_bytes(raw)
    case, driver = load_fixture_inputs(gateway_report)
    errors = validate_semantics(gateway_report)
    if errors:
        raise ValidationError("gateway input semantic validation failed")
    _validate_fixture_projection(gateway_report, case, driver)
    producer = gateway_report["provenance"]["producer"]
    if (producer["repository"], producer["component"], producer["build_kind"], producer["input_gateway_report_sha256"]) != (
        "Project-Helianthus/helianthus-ebusgateway",
        "internal/adversarial",
        "go-test-binary",
        None,
    ):
        raise ValidationError("gateway input producer mismatch")
    for field in ("subject", "fixture_set_sha256", "fixture_case_id"):
        if gateway_report["provenance"][field] != report["provenance"][field]:
            raise ValidationError("gateway input provenance mismatch")


def validate_semantics(report):
    errors = set()
    try:
        execution = report["execution"]
        if report["$schema"] != SCHEMA_ID or report["schema_version"] != 1 or report["suite"] != {"id": SUITE_ID, "version": 1}:
            _error(errors, "contract_identity")
        if _stamp(execution["completed_at"]) < _stamp(execution["started_at"]):
            _error(errors, "execution_order")
        provenance = report["provenance"]
        subject = provenance["subject"]
        producer = provenance["producer"]
        if subject["source_tree"] == "dirty" and report["summary"]["verdict"] == "pass":
            _error(errors, "dirty_provenance_pass")
        if execution["mode"] != "offline-fixture" or subject["artifact_kind"] != "gateway-fixture-set" or any(any(event["source"] != "fixture" for event in item["action"]["events"]) for item in report["scenarios"]):
            _error(errors, "producer_subject_pairing")
        is_gateway_producer = producer["repository"] == "Project-Helianthus/helianthus-ebusgateway"
        is_ha_producer = producer["repository"] == "Project-Helianthus/helianthus-ha-integration"
        if is_gateway_producer:
            if (producer["component"], producer["build_kind"], producer["input_gateway_report_sha256"]) != ("internal/adversarial", "go-test-binary", None):
                _error(errors, "producer_subject_pairing")
        elif is_ha_producer:
            if (producer["component"], producer["build_kind"]) != ("ha-adversarial-harness", "ha-harness") or producer["input_gateway_report_sha256"] is None:
                _error(errors, "producer_subject_pairing")
        else:
            _error(errors, "producer_subject_pairing")
        scenarios = report["scenarios"]
        run_start = _stamp(execution["started_at"])
        run_end = _stamp(execution["completed_at"])
        previous_end = None
        for index, scenario in enumerate(scenarios):
            scenario_start = _stamp(scenario["timing"]["scenario_started_at"])
            scenario_end = _stamp(scenario["timing"]["scenario_ended_at"])
            if not _same_timestamp(scenario_start, scenario["timing"]["elapsed_ms"], scenario["timing"]["scenario_ended_at"]):
                _error(errors, "timing_binding")
            if scenario_start < run_start or scenario_end > run_end or scenario_end < scenario_start:
                _error(errors, "scenario_run_containment")
            if (index == 0 and scenario_start != run_start) or (index == len(scenarios) - 1 and scenario_end != run_end):
                _error(errors, "scenario_run_binding")
            if previous_end is not None and scenario_start != previous_end:
                _error(errors, "scenario_sequence")
            if scenario["timing"]["error_bound_ms"] < max((event["error_bound_ms"] for event in scenario["action"]["events"]), default=0):
                _error(errors, "timing_error_bound")
            previous_end = scenario_end
        ids = [item["definition"]["scenario_id"] for item in scenarios]
        if ids != list(CATALOG):
            _error(errors, "scenario_order")
        if len(set(ids)) != 4:
            _error(errors, "scenario_identity")
        for scenario in scenarios:
            scenario_errors = set()
            expected = _validate_catalog(scenario["definition"], scenario_errors)
            if expected is None:
                errors.update(scenario_errors)
                continue
            kind = scenario["result_kind"]
            events = scenario["action"]["events"]
            timing = scenario["timing"]
            start = _stamp(timing["scenario_started_at"])
            if timing["elapsed_ms"] > scenario["definition"]["duration_limit_ms"]:
                _error(scenario_errors, "duration_limit")
            event_kinds = [event["kind"] for event in events]
            if event_kinds != expected["events"][: len(events)]:
                _error(scenario_errors, "action_prefix")
            _event_offsets(events, start, scenario_errors, elapsed_ms=timing["elapsed_ms"])
            if events and events[0]["offset_ms"] > timing["error_bound_ms"]:
                _error(scenario_errors, "action_start")
            event_by_kind = {event["kind"]: event for event in events}
            activation_event = expected["activation_event"]
            if activation_event in event_by_kind:
                activation = event_by_kind[activation_event]
                if activation["offset_ms"] + activation["error_bound_ms"] > 1000:
                    _error(scenario_errors, "action_activation")
            _validate_recovery_fields(timing, events, expected, scenario_errors)
            if kind == "evaluated":
                if scenario["errors"] or any(scenario["metrics"][field] is None for field in ("baseline", "end", "delta")) or scenario["evaluation"] is None:
                    _error(scenario_errors, "evaluated_shape")
                _validate_evaluated(scenario, expected, scenario_errors)
            elif kind == "infrastructure-block":
                if events or scenario["outcome"] != "blocked-infra" or scenario["errors"] != [{"phase": "precondition", "code": "precondition_unavailable"}] or scenario["infrastructure_reason"] not in expected["infrastructure_reasons"] or scenario["evaluation"] is not None or any(scenario["metrics"][field] is not None for field in ("baseline", "end", "delta")) or any(timing[field] is not None for field in ("recovery_anchor", "recovery_observed", "recovery_ms")):
                    _error(scenario_errors, "infrastructure_block_precedence")
            elif kind == "execution-error":
                continuity_error = (
                    len(scenario["errors"]) == 1
                    and scenario["errors"][0]["phase"] == "evaluation"
                    and scenario["errors"][0]["code"] in {"counter_epoch_changed", "negative_counter_delta"}
                )
                contains_continuity_code = any(
                    error["code"] in {"counter_epoch_changed", "negative_counter_delta"}
                    for error in scenario["errors"]
                )
                metrics = scenario["metrics"]
                if scenario["outcome"] != "fail" or not scenario["errors"] or scenario["evaluation"] is not None or metrics["delta"] is not None:
                    _error(scenario_errors, "execution_error_precedence")
                if continuity_error:
                    baseline, finish = metrics["baseline"], metrics["end"]
                    if baseline is None or finish is None:
                        _error(scenario_errors, "continuity_error_evidence")
                    else:
                        for snapshot in (baseline, finish):
                            if not _same_timestamp(start, snapshot["offset_ms"], snapshot["captured_at"]):
                                _error(scenario_errors, "continuity_error_evidence")
                        if baseline["offset_ms"] > finish["offset_ms"] or finish["offset_ms"] > timing["elapsed_ms"] or abs(baseline["offset_ms"]) > timing["error_bound_ms"] or abs(finish["offset_ms"] - timing["elapsed_ms"]) > timing["error_bound_ms"]:
                            _error(scenario_errors, "continuity_error_evidence")
                        if baseline["semantic_startup_current_phase"] != expected["baseline_phase"] or finish["semantic_startup_current_phase"] != expected["end_phase"]:
                            _error(scenario_errors, "continuity_error_evidence")
                        code = scenario["errors"][0]["code"]
                        if code == "counter_epoch_changed" and baseline["counter_epoch"] == finish["counter_epoch"]:
                            _error(scenario_errors, "continuity_error_evidence")
                        if code == "negative_counter_delta" and (baseline["counter_epoch"] != finish["counter_epoch"] or (finish["semantic_live_epoch"] >= baseline["semantic_live_epoch"] and finish["semantic_bus_collisions_total"] >= baseline["semantic_bus_collisions_total"])):
                            _error(scenario_errors, "continuity_error_evidence")
                elif contains_continuity_code and not continuity_error:
                    _error(scenario_errors, "execution_error_precedence")
                elif metrics["baseline"] is not None or metrics["end"] is not None:
                    _error(scenario_errors, "execution_error_precedence")
                complete = len(events) == len(expected["events"])
                partition_out_of_bounds = False
                if scenario["definition"]["scenario_id"] == "ADV-03" and {"partition_active", "partition_cleared"} <= event_by_kind.keys():
                    active = event_by_kind["partition_active"]
                    cleared = event_by_kind["partition_cleared"]
                    partition_out_of_bounds = (
                        abs(cleared["offset_ms"] - active["offset_ms"] - 60000)
                        + active["error_bound_ms"]
                        + cleared["error_bound_ms"]
                        > 1000
                    )
                for error in scenario["errors"]:
                    phase, code = error["phase"], error["code"]
                    valid = (
                        (phase == "trigger" and code in {"trigger_rejected", "trigger_timeout", "trigger_failed"} and len(events) == 1)
                        or (phase == "observer" and code in {"observer_timeout", "observer_failed"} and activation_event in event_by_kind)
                        or (phase == "evaluation" and code in {"counter_epoch_changed", "negative_counter_delta", "evidence_incomplete"} and complete)
                        or (phase == "evaluation" and code == "action_duration_out_of_bounds" and complete and partition_out_of_bounds)
                        or (phase == "artifact" and code == "evidence_incomplete" and complete)
                    )
                    if not valid:
                        _error(scenario_errors, "execution_error_progress")
            else:
                _error(scenario_errors, "result_kind")
            errors.update(scenario_errors)

        outcomes = [item["outcome"] for item in scenarios]
        summary = report["summary"]
        counts = {
            "total": len(scenarios),
            "passed": outcomes.count("pass"),
            "failed": outcomes.count("fail"),
            "xfailed": 0,
            "blocked": outcomes.count("blocked-infra"),
            "unknown": 0,
        }
        if any(summary.get(key) != value for key, value in counts.items()):
            _error(errors, "summary_accounting")
        verdict = "fail" if counts["failed"] else "blocked-infra" if counts["blocked"] else "pass"
        if summary.get("verdict") != verdict:
            _error(errors, "summary_verdict")
    except (KeyError, TypeError, ValidationError):
        _error(errors, "structural_shape")
    return sorted(errors)


def validate_schema_bytes(raw: bytes, schema: Path = SCHEMA):
    try:
        result = subprocess.run(["jv", str(schema), "-"], input=raw, capture_output=True, check=False)
    except OSError as error:
        raise ValidationError("schema validator unavailable") from error
    if result.returncode:
        raise ValidationError("schema validation failed")


def validate_path(path: Path, input_gateway_report: Path | None = None):
    raw = read_report_bytes(path)
    report = parse_report_bytes(raw)
    validate_schema_bytes(raw)
    case, driver = load_fixture_inputs(report)
    errors = validate_semantics(report)
    if errors:
        raise ValidationError("semantic: " + ",".join(errors))
    _validate_fixture_projection(report, case, driver)
    is_ha = report["provenance"]["producer"]["repository"] == "Project-Helianthus/helianthus-ha-integration"
    if is_ha:
        if input_gateway_report is None:
            raise ValidationError("gateway input required")
        validate_gateway_input(input_gateway_report, report)
    elif input_gateway_report is not None:
        raise ValidationError("gateway input is only valid for HA producers")


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    parser.add_argument("--input-gateway-report", type=Path)
    args = parser.parse_args(argv)
    try:
        validate_path(args.report, args.input_gateway_report)
    except ValidationError as error:
        print(f"adversarial_runtime_report_v1_invalid: {error}", file=sys.stderr)
        return 1
    print("adversarial_runtime_report_v1_ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
