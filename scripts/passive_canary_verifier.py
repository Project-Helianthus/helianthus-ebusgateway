#!/usr/bin/env python3
"""Active direct-read canary verifier for passive proof mode (P03)."""

from __future__ import annotations

import argparse
import base64
import glob
import json
import math
import pathlib
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Tuple

MIN_TOTAL_CANARIES = 6
MIN_FAMILY_COUNTS = {"B524": 2, "B509": 2}
MAX_RETRIES = 3
CANARY_PHASE_PREFIX = "canary_phase_"
MANIFEST_SCHEMA = "p03_canary_manifest_v1"
P03_ALLOWED_METHODS = {"get_register", "get_ext_register"}
CANARY_VERDICT_SCHEMA = "p03_canary_verdict_v1"
REPLAY_BEHAVIOR_ARTIFACT_SCHEMA = "observe_first_replay_behavior_v1"
REPLAY_FALSIFICATION_VERDICT_SCHEMA = "observe_first_replay_falsification_verdict_v1"
OVERALL_INTERVAL_CONCLUSIVE_MIN = 0.90
PER_CANARY_INTERVAL_CONCLUSIVE_MIN = 0.75
CANARY_NONCE_PARAM = "_canary_nonce"
READ_AVOIDANCE_ACCOUNTING_SCHEMA = "p03_read_avoidance_accounting_v1"
READ_AVOIDANCE_DIRECT_APPLY_METRIC = "direct_apply_total"
READ_AVOIDANCE_ACTIVE_AVOIDED_METRIC = "active_reads_avoided_total"
READ_AVOIDANCE_SAVED_SECONDS_METRIC = "active_read_saved_seconds"
WARMUP_BEHAVIOR_ARTIFACT_SCHEMA = "p03_warmup_behavior_v1"
PUBLISHER_CADENCE_ARTIFACT_SCHEMA = "p03_publisher_cadence_v1"
PUBLISHER_CADENCE_SOURCE_ANCHOR = "config.semantic_state_interval"
CROSS_PLANE_SKEW_ARTIFACT_SCHEMA = "p03_cross_plane_skew_v1"
ROLLBACK_EXECUTION_ARTIFACT_SCHEMA = "observe_first_rollback_execution_v1"
ROLLBACK_RESULT_ARTIFACT_SCHEMA = "observe_first_rollback_result_v1"
TIMING_REFERENCE_VERDICT_SCHEMA = "p03_timing_reference_verdict_v1"
TIMING_REFERENCE_BUSY_RELATIVE_ERROR_MAX = 0.15
TIMING_REFERENCE_PERIODICITY_RELATIVE_ERROR_MAX = 0.20
TIMING_REFERENCE_PERIODICITY_ABSOLUTE_ERROR_SEC_MAX = 2.0
TIMING_REFERENCE_PERIODICITY_MIN_SAMPLES = 10
TIMING_REFERENCE_BUSY_METRIC = "ebus_bus_busy_seconds_total"
ROLLBACK_TARGET_FEATURE_FLAGS = {
    "observeFirstEnabled": False,
    "passiveStateDirectApply": False,
    "passiveConfigDirectApply": False,
    "externalWritePolicy": "record_only",
    "normalizations": [],
}
ROLLBACK_PROOF_FEATURE_FLAGS = {
    "observeFirstEnabled": True,
    "passiveStateDirectApply": True,
    "passiveConfigDirectApply": False,
    "externalWritePolicy": "record_only",
    "normalizations": [],
}
CROSS_PLANE_SKEW_SEMANTIC_FIELDS = (
    "bus_observability.summary_last_updated_at",
    "bus_observability.status_last_updated_at",
    "graphql_bus_watch.summary_last_updated_at",
    "graphql_bus_watch.status_last_updated_at",
    "graphql_bus_watch.watch_summary_last_updated_at",
)
CROSS_PLANE_SKEW_EXCLUDED_SURFACES = (
    "bus_observability.status.startup.last_updated_at",
    "bus_observability.status.feature_flags.last_updated_at",
    "graphql_bus_watch.status.startup.lastUpdatedAt",
    "graphql_bus_watch.status.featureFlags.lastUpdatedAt",
    "captured_at",
    "mtimes",
    "harness_poll_interval",
)
FAMILY_PROOF_ELIGIBILITY_SCHEMA = "p03_family_proof_eligibility_v1"
PROMOTION_ELIGIBILITY_SCHEMA = "p03_promotion_eligibility_v1"
CANONICAL_NO_EBUSD_TRANSPORT = "no-ebusd"
PROOF_WINDOW_COMPLETED_TRANSACTIONS_METRIC = "ebus_passive_completed_transactions_total"
PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_METRIC = "ebus_passive_direct_apply_candidates_evaluated_total"
PROOF_WINDOW_COMPLETED_TRANSACTIONS_MIN_DELTA = 1000.0
PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_MIN_DELTA = 100.0
FEATURE_FLAG_FIELDS = (
    "observeFirstEnabled",
    "passiveStateDirectApply",
    "passiveConfigDirectApply",
    "externalWritePolicy",
    "normalizations",
)
FEATURE_FLAG_FIELD_ALIASES = {
    "observeFirstEnabled": ("observeFirstEnabled", "observe_first_enabled"),
    "passiveStateDirectApply": ("passiveStateDirectApply", "passive_state_direct_apply"),
    "passiveConfigDirectApply": ("passiveConfigDirectApply", "passive_config_direct_apply"),
    "externalWritePolicy": ("externalWritePolicy", "external_write_policy"),
    "normalizations": ("normalizations",),
}
FEATURE_FLAG_CONSISTENCY_SCHEMA = "p03_feature_flag_consistency_v1"
REPLAY_EXPECTED_DISPOSITIONS = {"ambiguity", "falsification"}
PROM_SAMPLE_RE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([^\s]+)$")
GO_DURATION_COMPONENT_RE = re.compile(r"(\d+(?:\.\d+)?)(ns|us|µs|μs|ms|s|m|h)")
REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
CANONICAL_FAMILY_PROOF_CASE_ID = "P03"
CANONICAL_P03_CANARY_MANIFEST_PATH = REPO_ROOT / "testdata" / "passive_proof" / "p03_canary_manifest.json"
CANONICAL_REPLAY_CORPUS_PATH = REPO_ROOT / "testdata" / "observe_first_replay_cases.json"
_CANONICAL_FAMILY_PROOF_CANARY_IDS: Tuple[str, ...] | None = None
_CANONICAL_FAMILY_PROOF_REPLAY_CASE_NAMES: Tuple[str, ...] | None = None
_CANONICAL_FAMILY_PROOF_REPLAY_CASE_CONTRACTS: Dict[str, Dict[str, Any]] | None = None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json(path: pathlib.Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: pathlib.Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def parse_iso8601_timestamp(value: Any, field_name: str) -> datetime:
    text = str(value).strip()
    if text == "":
        raise ValueError(f"{field_name} must be non-empty RFC3339 timestamp")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be RFC3339 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include timezone")
    return parsed


def timestamp_has_subsecond_precision(value: Any) -> bool:
    text = str(value).strip()
    if text == "":
        return False
    return "." in text


def timestamp_not_after_boundary(captured_at: datetime, boundary: datetime, boundary_raw: Any) -> bool:
    if timestamp_has_subsecond_precision(boundary_raw):
        return captured_at <= boundary
    return captured_at.replace(microsecond=0) <= boundary.replace(microsecond=0)


def timestamp_not_before_boundary(captured_at: datetime, boundary: datetime, boundary_raw: Any) -> bool:
    if timestamp_has_subsecond_precision(boundary_raw):
        return captured_at >= boundary
    return captured_at.replace(microsecond=0) >= boundary.replace(microsecond=0)


def parse_duration_seconds(value: Any, field_name: str) -> float:
    if isinstance(value, (int, float)):
        seconds = float(value)
        if abs(seconds) >= 1e6:
            seconds /= 1e9
    else:
        text = str(value).strip()
        if text == "":
            raise ValueError(f"{field_name} must be non-empty duration")
        try:
            seconds = float(text)
        except ValueError:
            if text.startswith(("+", "-")):
                raise ValueError(f"{field_name} must be non-negative finite duration")
            seconds = 0.0
            consumed = 0
            factors = {
                "ns": 1e-9,
                "us": 1e-6,
                "µs": 1e-6,
                "μs": 1e-6,
                "ms": 1e-3,
                "s": 1.0,
                "m": 60.0,
                "h": 3600.0,
            }
            for match in GO_DURATION_COMPONENT_RE.finditer(text):
                if match.start() != consumed:
                    raise ValueError(f"{field_name} must be non-negative finite duration")
                amount = float(match.group(1))
                unit = match.group(2)
                seconds += amount * factors[unit]
                consumed = match.end()
            if consumed != len(text) or consumed == 0:
                raise ValueError(f"{field_name} must be non-negative finite duration")
    if not math.isfinite(seconds) or seconds < 0:
        raise ValueError(f"{field_name} must be non-negative finite duration")
    return seconds


def extract_mapping_value(payload: Dict[str, Any], aliases: Tuple[str, ...]) -> Any:
    for alias in aliases:
        if alias in payload:
            return payload.get(alias)
    return None


def parse_cli_bool(value: Any, field_name: str) -> bool:
    normalized = str(value).strip().lower()
    if normalized in ("1", "true", "yes", "on"):
        return True
    if normalized in ("0", "false", "no", "off"):
        return False
    raise ValueError(f"{field_name} must be boolean-like, got {value!r}")


def normalize_hex(value: str) -> str:
    cleaned = value.strip().lower()
    if cleaned.startswith("0x"):
        cleaned = cleaned[2:]
    if cleaned == "":
        raise ValueError("hex value is empty")
    if len(cleaned) % 2 != 0:
        cleaned = "0" + cleaned
    if not re.fullmatch(r"[0-9a-f]+", cleaned):
        raise ValueError(f"invalid hex value: {value!r}")
    return cleaned.upper()


def parse_bytes(value: Any) -> bytes:
    if isinstance(value, list):
        out = bytearray()
        for entry in value:
            if not isinstance(entry, int) or entry < 0 or entry > 0xFF:
                raise ValueError(f"invalid byte entry: {entry!r}")
            out.append(entry)
        return bytes(out)
    if isinstance(value, str):
        text = value.strip()
        if text == "":
            raise ValueError("empty string payload")
        if re.fullmatch(r"(?:0x)?[0-9a-fA-F]+", text):
            return bytes.fromhex(normalize_hex(text))
        try:
            return base64.b64decode(text, validate=True)
        except Exception as exc:
            raise ValueError(f"unable to decode payload {value!r}") from exc
    raise ValueError(f"unsupported payload type {type(value).__name__}")


def extract_value_hex(result: Any, field_name: str) -> str:
    if not isinstance(result, dict):
        raise ValueError(f"invoke result must be object, got {type(result).__name__}")
    if field_name not in result:
        raise ValueError(f"invoke result missing {field_name!r}")
    raw = result[field_name]
    if raw is None:
        raise ValueError(f"invoke result field {field_name!r} is null")
    parsed = parse_bytes(raw)
    if len(parsed) == 0:
        raise ValueError(f"invoke result field {field_name!r} is empty")
    return parsed.hex().upper()


@dataclass(frozen=True)
class CanarySpec:
    canary_id: str
    family: str
    address: int
    plane: str
    method: str
    params: Dict[str, Any]
    result_field: str
    expected_hex: str | None


def is_start_phase(phase: str) -> bool:
    return phase.strip().lower() == "start"


def is_read_only_canary_method(method: str) -> bool:
    normalized = method.strip().lower()
    return normalized in P03_ALLOWED_METHODS


def promotion_topology_label(kind: str, gateway_transport: str, proxy_transport: str, ebusd_transport: str) -> str:
    normalized_kind = str(kind).strip().lower()
    normalized_gateway_transport = str(gateway_transport).strip().lower()
    normalized_proxy_transport = str(proxy_transport).strip().lower()
    normalized_ebusd_transport = str(ebusd_transport).strip().lower()

    if normalized_kind in ("direct-adapter", "proxy-dual-client"):
        return normalized_kind
    if normalized_ebusd_transport == "ebusd-tcp":
        return "via-ebusd-tcp"
    if normalized_ebusd_transport == CANONICAL_NO_EBUSD_TRANSPORT:
        return "ebusd-free"
    if normalized_ebusd_transport == "":
        return "missing-ebusd-transport"
    if normalized_ebusd_transport not in ("",):
        return f"contradictory-ebusd-{normalized_ebusd_transport}"
    if normalized_kind == "proxy-single-client":
        if normalized_gateway_transport == "ens" and normalized_proxy_transport == "ens":
            return "proxy-single-client"
        return "proxy-single-client"
    return normalized_kind or "unknown"


def promotion_topology_requires_proxy_transport(kind: str, ebusd_transport: str) -> bool:
    normalized_kind = str(kind).strip().lower()
    normalized_ebusd_transport = str(ebusd_transport).strip().lower()

    if normalized_kind == "proxy-dual-client":
        return True
    if normalized_kind == "proxy-single-client":
        return normalized_ebusd_transport != "ebusd-tcp"
    return False


def normalize_canary(raw: Any, index: int) -> CanarySpec:
    if not isinstance(raw, dict):
        raise ValueError(f"canary[{index}] must be object")
    canary_id = str(raw.get("id", "")).strip()
    if canary_id == "":
        raise ValueError(f"canary[{index}] missing id")
    family = str(raw.get("family", "")).strip().upper()
    if family == "":
        raise ValueError(f"canary[{index}] missing family")
    address = raw.get("address")
    if not isinstance(address, int) or address < 0 or address > 0xFF:
        raise ValueError(f"canary[{index}] invalid address {address!r}")
    plane = str(raw.get("plane", "")).strip()
    method = str(raw.get("method", "")).strip()
    if plane == "" or method == "":
        raise ValueError(f"canary[{index}] missing plane/method")
    if not is_read_only_canary_method(method):
        raise ValueError(
            f"canary[{index}] method {method!r} is not read-only; only read methods are allowed"
        )
    params = raw.get("params")
    if not isinstance(params, dict):
        raise ValueError(f"canary[{index}] missing params object")
    result_field = str(raw.get("result_field", "value")).strip()
    if result_field == "":
        raise ValueError(f"canary[{index}] invalid result_field")
    expected_hex = raw.get("expected_hex")
    if expected_hex is not None:
        expected_hex = normalize_hex(str(expected_hex))

    return CanarySpec(
        canary_id=canary_id,
        family=family,
        address=address,
        plane=plane,
        method=method,
        params=params,
        result_field=result_field,
        expected_hex=expected_hex,
    )


def load_and_validate_manifest(path: pathlib.Path, require_case_id: str | None = None) -> Tuple[Dict[str, Any], List[CanarySpec]]:
    if not path.exists():
        raise ValueError(f"manifest not found: {path}")
    payload = load_json(path)
    if not isinstance(payload, dict):
        raise ValueError("manifest must be a JSON object")
    got_schema = str(payload.get("schema", "")).strip()
    if got_schema != MANIFEST_SCHEMA:
        raise ValueError(f"manifest schema={got_schema!r}; want {MANIFEST_SCHEMA!r}")
    if require_case_id:
        got_case = str(payload.get("case_id", "")).strip()
        if got_case != require_case_id:
            raise ValueError(f"manifest case_id={got_case!r}; want {require_case_id!r}")
    canaries_raw = payload.get("canaries")
    if not isinstance(canaries_raw, list):
        raise ValueError("manifest missing canaries array")
    if len(canaries_raw) < MIN_TOTAL_CANARIES:
        raise ValueError(f"manifest canaries={len(canaries_raw)}; want >= {MIN_TOTAL_CANARIES}")

    canaries = [normalize_canary(raw, index) for index, raw in enumerate(canaries_raw)]
    family_counts: Dict[str, int] = {}
    seen_ids = set()
    for item in canaries:
        if item.canary_id in seen_ids:
            raise ValueError(f"duplicate canary id: {item.canary_id}")
        seen_ids.add(item.canary_id)
        family_counts[item.family] = family_counts.get(item.family, 0) + 1
    for family, minimum in MIN_FAMILY_COUNTS.items():
        got = family_counts.get(family, 0)
        if got < minimum:
            raise ValueError(f"manifest {family} canaries={got}; want >= {minimum}")

    return payload, canaries


def build_rollback_execution_artifact(
    *,
    run_id: str,
    case_id: str,
    exec_case_id: str,
    gateway_base_url: str,
    remote_case_dir: str,
    proof_gateway_log_path: str,
    rollback_gateway_log_path: str,
    started_at: str,
    completed_at: str,
    ok: bool,
    reason: str,
    restart_exit_code: int,
    restart_succeeded: bool,
    gateway_health_check_ok: bool,
    source: str,
    action: str,
) -> Dict[str, Any]:
    normalized_run_id = str(run_id).strip()
    normalized_case_id = str(case_id).strip()
    normalized_exec_case_id = str(exec_case_id).strip()
    normalized_gateway_base_url = str(gateway_base_url).strip()
    normalized_remote_case_dir = str(remote_case_dir).strip()
    normalized_proof_gateway_log_path = str(proof_gateway_log_path).strip()
    normalized_rollback_gateway_log_path = str(rollback_gateway_log_path).strip()
    normalized_reason = str(reason).strip()
    normalized_source = str(source).strip()
    normalized_action = str(action).strip()

    if normalized_run_id == "":
        raise ValueError("run_id must be non-empty")
    if normalized_case_id == "":
        raise ValueError("case_id must be non-empty")
    if normalized_exec_case_id == "":
        raise ValueError("exec_case_id must be non-empty")
    if normalized_gateway_base_url == "":
        raise ValueError("gateway_base_url must be non-empty")
    if normalized_remote_case_dir == "":
        raise ValueError("remote_case_dir must be non-empty")
    if normalized_proof_gateway_log_path == "":
        raise ValueError("proof_gateway_log_path must be non-empty")
    if normalized_rollback_gateway_log_path == "":
        raise ValueError("rollback_gateway_log_path must be non-empty")
    if normalized_reason == "":
        raise ValueError("reason must be non-empty")
    if normalized_source == "":
        raise ValueError("source must be non-empty")
    if normalized_action == "":
        raise ValueError("action must be non-empty")

    started_at_dt = parse_iso8601_timestamp(started_at, "started_at")
    completed_at_dt = parse_iso8601_timestamp(completed_at, "completed_at")
    if completed_at_dt < started_at_dt:
        raise ValueError("completed_at must be >= started_at")
    if ok and (not restart_succeeded or not gateway_health_check_ok or restart_exit_code != 0):
        raise ValueError(
            "ok rollback execution artifact must record restart_exit_code=0, restart_succeeded=true, and gateway_health_check_ok=true"
        )

    return {
        "schema": ROLLBACK_EXECUTION_ARTIFACT_SCHEMA,
        "captured_at": utc_now(),
        "source": normalized_source,
        "claim_scope": "bounded_ha_harness_rollback_execution_source",
        "ok": bool(ok),
        "run_id": normalized_run_id,
        "case_id": normalized_case_id,
        "exec_case_id": normalized_exec_case_id,
        "action": normalized_action,
        "reason": normalized_reason,
        "requested_to_flags": dict(ROLLBACK_TARGET_FEATURE_FLAGS),
        "evidence": {
            "gateway_base_url": normalized_gateway_base_url,
            "remote_case_dir": normalized_remote_case_dir,
            "proof_gateway_log_path": normalized_proof_gateway_log_path,
            "rollback_gateway_log_path": normalized_rollback_gateway_log_path,
            "started_at": started_at_dt.isoformat(),
            "completed_at": completed_at_dt.isoformat(),
            "restart_exit_code": restart_exit_code,
            "restart_succeeded": bool(restart_succeeded),
            "gateway_health_check_ok": bool(gateway_health_check_ok),
        },
    }


def load_rollback_execution_artifact(path: pathlib.Path, expected_run_id: str) -> Dict[str, Any]:
    if not path.exists():
        raise ValueError(f"missing rollback execution artifact: {path}")
    payload = load_json(path)
    if not isinstance(payload, dict):
        raise ValueError(f"rollback execution artifact must be object: {path}")
    if str(payload.get("schema", "")).strip() != ROLLBACK_EXECUTION_ARTIFACT_SCHEMA:
        raise ValueError(f"rollback execution artifact schema mismatch: {path}")
    if str(payload.get("run_id", "")).strip() != str(expected_run_id).strip():
        raise ValueError(f"rollback execution artifact run_id mismatch: {path}")
    if str(payload.get("case_id", "")).strip() == "":
        raise ValueError(f"rollback execution artifact missing case_id: {path}")
    if str(payload.get("exec_case_id", "")).strip() == "":
        raise ValueError(f"rollback execution artifact missing exec_case_id: {path}")
    if str(payload.get("source", "")).strip() == "":
        raise ValueError(f"rollback execution artifact missing source: {path}")
    if str(payload.get("action", "")).strip() == "":
        raise ValueError(f"rollback execution artifact missing action: {path}")
    reason = str(payload.get("reason", "")).strip()
    if reason == "":
        raise ValueError(f"rollback execution artifact missing reason: {path}")

    parse_iso8601_timestamp(payload.get("captured_at"), f"{path}:captured_at")
    if not isinstance(payload.get("ok"), bool):
        raise ValueError(f"rollback execution artifact missing boolean ok: {path}")

    target_feature_flags = payload.get("requested_to_flags")
    if target_feature_flags != ROLLBACK_TARGET_FEATURE_FLAGS:
        raise ValueError(f"rollback execution artifact requested_to_flags mismatch: {path}")

    evidence = payload.get("evidence")
    if not isinstance(evidence, dict):
        raise ValueError(f"rollback execution artifact missing evidence: {path}")
    if str(evidence.get("gateway_base_url", "")).strip() == "":
        raise ValueError(f"rollback execution artifact missing evidence.gateway_base_url: {path}")
    if str(evidence.get("remote_case_dir", "")).strip() == "":
        raise ValueError(f"rollback execution artifact missing evidence.remote_case_dir: {path}")

    artifact_dir = path.resolve().parent

    def require_local_log_bundle(field_name: str) -> pathlib.Path:
        raw_path = str(evidence.get(field_name, "")).strip()
        if raw_path == "":
            raise ValueError(f"rollback execution artifact missing evidence.{field_name}: {path}")
        candidate = pathlib.Path(raw_path)
        if not candidate.is_absolute():
            candidate = artifact_dir / candidate
        resolved = candidate.resolve()
        try:
            resolved.relative_to(artifact_dir)
        except ValueError as exc:
            raise ValueError(
                f"rollback execution artifact {field_name} must stay within artifact directory: {path}"
            ) from exc
        if not resolved.is_file():
            raise ValueError(f"rollback execution artifact missing {field_name} bundle: {path}")
        if resolved.stat().st_size <= 0:
            raise ValueError(f"rollback execution artifact empty {field_name} bundle: {path}")
        return resolved

    proof_log_path = require_local_log_bundle("proof_gateway_log_path")
    rollback_log_path = require_local_log_bundle("rollback_gateway_log_path")
    if proof_log_path == rollback_log_path:
        raise ValueError(f"rollback execution artifact log bundles must be distinct: {path}")

    started_at_dt = parse_iso8601_timestamp(evidence.get("started_at"), f"{path}:evidence.started_at")
    completed_at_dt = parse_iso8601_timestamp(evidence.get("completed_at"), f"{path}:evidence.completed_at")
    if completed_at_dt < started_at_dt:
        raise ValueError(f"rollback execution artifact completed_at must be >= started_at: {path}")
    restart_exit_code = evidence.get("restart_exit_code")
    if not isinstance(restart_exit_code, int):
        raise ValueError(f"rollback execution artifact missing integer restart_exit_code: {path}")
    restart_succeeded = evidence.get("restart_succeeded")
    health_check_ok = evidence.get("gateway_health_check_ok")
    if not isinstance(restart_succeeded, bool):
        raise ValueError(f"rollback execution artifact missing boolean restart_succeeded: {path}")
    if not isinstance(health_check_ok, bool):
        raise ValueError(f"rollback execution artifact missing boolean gateway_health_check_ok: {path}")
    if bool(payload.get("ok")) and (restart_exit_code != 0 or not restart_succeeded or not health_check_ok):
        raise ValueError(f"rollback execution artifact ok status is inconsistent with evidence booleans: {path}")
    return payload


def build_rollback_result_artifact(proof_dir: pathlib.Path, run_id: str) -> Dict[str, Any]:
    rollback_execution_path = proof_dir / "rollback_execution.json"
    reasons: List[str] = []
    rollback_execution: Dict[str, Any] | None = None
    rollback_pre: Dict[str, Any] | None = None
    rollback_post: Dict[str, Any] | None = None
    rollback_target_state: Dict[str, Any] | None = None
    execution_ok = False
    pre_matches_proof_state = False
    post_matches_target = False
    observed_transition = False
    pre_live_ready = False
    post_live_ready = False
    pre_snapshot_ordered = False
    post_snapshot_ordered = False
    execution_started_at: datetime | None = None
    execution_completed_at: datetime | None = None

    try:
        rollback_execution = load_rollback_execution_artifact(rollback_execution_path, run_id)
    except Exception as exc:  # noqa: BLE001 - fail closed into artifact reasons.
        reasons.append(str(exc))
    else:
        target_raw = rollback_execution.get("requested_to_flags")
        if not isinstance(target_raw, dict):
            reasons.append(
                f"rollback execution artifact missing requested_to_flags object: {rollback_execution_path}"
            )
        else:
            try:
                rollback_target_state = normalize_feature_flag_state(
                    target_raw,
                    rollback_execution_path,
                    "rollback execution requested_to_flags",
                )
            except Exception as exc:  # noqa: BLE001 - fail closed into artifact reasons.
                reasons.append(str(exc))
        evidence = rollback_execution.get("evidence")
        if isinstance(evidence, dict):
            try:
                started_at_raw = evidence.get("started_at")
                completed_at_raw = evidence.get("completed_at")
                execution_started_at = parse_iso8601_timestamp(
                    started_at_raw,
                    f"{rollback_execution_path}:evidence.started_at",
                )
                execution_completed_at = parse_iso8601_timestamp(
                    completed_at_raw,
                    f"{rollback_execution_path}:evidence.completed_at",
                )
            except Exception as exc:  # noqa: BLE001 - fail closed into artifact reasons.
                reasons.append(str(exc))
                started_at_raw = None
                completed_at_raw = None
        else:
            started_at_raw = None
            completed_at_raw = None
        execution_ok = bool(rollback_execution.get("ok", False))
        if not execution_ok:
            reasons.append(
                "rollback execution artifact did not complete successfully: "
                f"{str(rollback_execution.get('reason', '')).strip() or 'unknown reason'}"
            )

    for snapshot_prefix, slot_name in (("rollback_pre", "pre"), ("rollback_post", "post")):
        try:
            snapshot = load_structured_snapshot_bundle(proof_dir, snapshot_prefix)
        except Exception as exc:  # noqa: BLE001 - fail closed into artifact reasons.
            reasons.append(str(exc))
            continue
        if slot_name == "pre":
            rollback_pre = snapshot
        else:
            rollback_post = snapshot

    if rollback_pre is not None:
        pre_state = rollback_pre["feature_flag_snapshot"]["canonical_feature_flags"]
        pre_matches_proof_state, mismatch_field = compare_feature_flag_states(
            pre_state,
            ROLLBACK_PROOF_FEATURE_FLAGS,
        )
        if not pre_matches_proof_state:
            reasons.append(
                "rollback pre snapshot does not match expected proof-mode state"
                if mismatch_field is None
                else f"rollback pre snapshot does not match expected proof-mode state ({mismatch_field})"
            )
        pre_live_ready = rollback_pre["startup_phase"] == "LIVE_READY"
        if not pre_live_ready:
            reasons.append(
                f"rollback pre snapshot not live-ready: startup_phase={rollback_pre['startup_phase']!r}"
            )

    if rollback_target_state is not None and rollback_post is not None:
        post_state = rollback_post["feature_flag_snapshot"]["canonical_feature_flags"]
        post_matches_target, mismatch_field = compare_feature_flag_states(post_state, rollback_target_state)
        if not post_matches_target:
            reasons.append(
                "rollback post snapshot does not match rollback target state"
                if mismatch_field is None
                else f"rollback post snapshot does not match rollback target state ({mismatch_field})"
            )
        post_live_ready = rollback_post["startup_phase"] == "LIVE_READY"
        if not post_live_ready:
            reasons.append(
                f"rollback post snapshot not live-ready: startup_phase={rollback_post['startup_phase']!r}"
            )

    if rollback_pre is not None and rollback_post is not None:
        observed_transition = (
            rollback_pre["feature_flag_snapshot"]["canonical_feature_flags_key"]
            != rollback_post["feature_flag_snapshot"]["canonical_feature_flags_key"]
        )
        if not observed_transition:
            reasons.append("rollback pre/post snapshots did not observe a feature-flag transition")

    if execution_started_at is not None and rollback_pre is not None:
        pre_captured_at = parse_iso8601_timestamp(
            rollback_pre["feature_flag_snapshot"]["captured_at"],
            f"{rollback_pre['feature_flag_snapshot']['feature_flags_snapshot_path']}:captured_at",
        )
        pre_snapshot_ordered = timestamp_not_after_boundary(
            pre_captured_at,
            execution_started_at,
            started_at_raw,
        )
        if not pre_snapshot_ordered:
            reasons.append("rollback pre snapshot was captured after rollback execution started")

    if execution_completed_at is not None and rollback_post is not None:
        post_captured_at = parse_iso8601_timestamp(
            rollback_post["feature_flag_snapshot"]["captured_at"],
            f"{rollback_post['feature_flag_snapshot']['feature_flags_snapshot_path']}:captured_at",
        )
        post_snapshot_ordered = timestamp_not_before_boundary(
            post_captured_at,
            execution_completed_at,
            completed_at_raw,
        )
        if not post_snapshot_ordered:
            reasons.append("rollback post snapshot was captured before rollback execution completed")

    status = "pass" if len(reasons) == 0 else "fail"
    reason = "ok" if status == "pass" else "; ".join(reasons)

    return {
        "schema": ROLLBACK_RESULT_ARTIFACT_SCHEMA,
        "captured_at": utc_now(),
        "run_id": run_id,
        "source": "rollback_execution_plus_structured_snapshots",
        "claim_scope": "bounded_proof_window_rollback_result",
        "evidence": {
            "rollback_execution_path": str(rollback_execution_path),
            "rollback_pre_snapshot_paths": None if rollback_pre is None else rollback_pre["snapshot_paths"],
            "rollback_post_snapshot_paths": None if rollback_post is None else rollback_post["snapshot_paths"],
        },
        "rollback_execution": rollback_execution,
        "rollback_target_state": rollback_target_state,
        "rollback_pre": rollback_pre,
        "rollback_post": rollback_post,
        "criteria": {
            "rollback_execution_ok": {
                "ok": execution_ok,
                "reason": None
                if execution_ok
                else (
                    "missing rollback execution artifact"
                    if rollback_execution is None
                    else str(rollback_execution.get("reason", "")).strip() or "rollback execution failed"
                ),
            },
            "rollback_pre_matches_proof_state": {
                "ok": rollback_pre is not None and pre_matches_proof_state,
                "reason": (
                    "missing rollback pre snapshot"
                    if rollback_pre is None
                    else (
                        "ok"
                        if pre_matches_proof_state
                        else "rollback pre snapshot does not match expected proof-mode state"
                    )
                ),
            },
            "rollback_pre_live_ready": {
                "ok": rollback_pre is not None and pre_live_ready,
                "reason": (
                    "missing rollback pre snapshot"
                    if rollback_pre is None
                    else (
                        f"rollback pre snapshot not live-ready: startup_phase={rollback_pre['startup_phase']!r}"
                        if not pre_live_ready
                        else "ok"
                    )
                ),
            },
            "rollback_post_matches_target": {
                "ok": rollback_post is not None and rollback_target_state is not None and post_matches_target,
                "reason": (
                    "missing rollback post snapshot or rollback target state"
                    if rollback_post is None or rollback_target_state is None
                    else "ok" if post_matches_target else "rollback post snapshot does not match rollback target state"
                ),
            },
            "rollback_observed_transition": {
                "ok": rollback_pre is not None and rollback_post is not None and observed_transition,
                "reason": (
                    "missing rollback pre/post snapshots"
                    if rollback_pre is None or rollback_post is None
                    else (
                        "rollback pre/post snapshots did not observe a feature-flag transition"
                        if not observed_transition
                        else "ok"
                    )
                ),
            },
            "rollback_pre_captured_before_execution": {
                "ok": rollback_pre is not None and execution_started_at is not None and pre_snapshot_ordered,
                "reason": (
                    "missing rollback pre snapshot or rollback execution started_at"
                    if rollback_pre is None or execution_started_at is None
                    else (
                        "rollback pre snapshot was captured after rollback execution started"
                        if not pre_snapshot_ordered
                        else "ok"
                    )
                ),
            },
            "rollback_post_captured_after_execution": {
                "ok": rollback_post is not None and execution_completed_at is not None and post_snapshot_ordered,
                "reason": (
                    "missing rollback post snapshot or rollback execution completed_at"
                    if rollback_post is None or execution_completed_at is None
                    else (
                        "rollback post snapshot was captured before rollback execution completed"
                        if not post_snapshot_ordered
                        else "ok"
                    )
                ),
            },
            "rollback_post_live_ready": {
                "ok": rollback_post is not None and post_live_ready,
                "reason": (
                    "missing rollback post snapshot"
                    if rollback_post is None
                    else (
                        f"rollback post snapshot not live-ready: startup_phase={rollback_post['startup_phase']!r}"
                        if not post_live_ready
                        else "ok"
                    )
                ),
            },
        },
        "ok": status == "pass",
        "status": status,
        "reason": reason,
    }


def load_wire_timing_reference_artifact(path: pathlib.Path) -> Dict[str, Any]:
    if not path.exists():
        raise ValueError(f"missing wire timing reference artifact at {path}")
    payload = load_json(path)
    if not isinstance(payload, dict):
        raise ValueError("wire timing reference artifact must be a JSON object")
    if str(payload.get("schema", "")).strip() != "observe_first_wire_timing_reference_v1":
        raise ValueError("wire timing reference artifact schema mismatch")
    if str(payload.get("source", "")).strip() != "proxy_log_session_send_plus_wire_rx":
        raise ValueError("wire timing reference artifact source mismatch")
    if bool(payload.get("ok")) is not True:
        raise ValueError("wire timing reference artifact is not ok")
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        raise ValueError("wire timing reference artifact missing summary object")
    busy_seconds_total = summary.get("busy_seconds_total")
    if not isinstance(busy_seconds_total, (int, float)) or not math.isfinite(float(busy_seconds_total)):
        raise ValueError("wire timing reference artifact missing finite summary.busy_seconds_total")
    if float(busy_seconds_total) <= 0:
        raise ValueError("wire timing reference artifact summary.busy_seconds_total must be > 0")
    families_with_intervals = summary.get("families_with_intervals")
    if not isinstance(families_with_intervals, int) or families_with_intervals <= 0:
        raise ValueError("wire timing reference artifact missing positive summary.families_with_intervals")
    periodicity = payload.get("periodicity")
    if not isinstance(periodicity, list):
        raise ValueError("wire timing reference artifact missing periodicity array")
    evidence = payload.get("evidence")
    if not isinstance(evidence, dict):
        raise ValueError("wire timing reference artifact missing evidence object")
    if str(evidence.get("proxy_log_path", "")).strip() == "":
        raise ValueError("wire timing reference artifact missing evidence.proxy_log_path")
    return payload


def proof_log_bundle_root(proof_dir: pathlib.Path) -> pathlib.Path:
    resolved = proof_dir.resolve()
    if resolved.name == "proof_artifacts":
        return resolved.parent
    return resolved


def require_wire_reference_proxy_log_path(
    proof_dir: pathlib.Path,
    wire_reference_path: pathlib.Path,
    wire_reference: Dict[str, Any],
) -> pathlib.Path:
    evidence = wire_reference.get("evidence")
    if not isinstance(evidence, dict):
        raise ValueError("wire timing reference artifact missing evidence object")
    raw_proxy_log_path = str(evidence.get("proxy_log_path", "")).strip()
    if raw_proxy_log_path == "":
        raise ValueError("wire timing reference artifact missing evidence.proxy_log_path")
    candidate = pathlib.Path(raw_proxy_log_path)
    if not candidate.is_absolute():
        candidate = wire_reference_path.parent / candidate
    resolved = candidate.resolve()
    bundle_root = proof_log_bundle_root(proof_dir)
    try:
        resolved.relative_to(bundle_root)
    except ValueError as exc:
        raise ValueError(
            "wire timing reference artifact evidence.proxy_log_path must stay within the current proof log bundle"
        ) from exc
    if resolved.name != "proxy.log":
        raise ValueError("wire timing reference artifact evidence.proxy_log_path must reference proxy.log")
    if not resolved.is_file():
        raise ValueError("wire timing reference artifact evidence.proxy_log_path is missing")
    if resolved.stat().st_size <= 0:
        raise ValueError("wire timing reference artifact evidence.proxy_log_path is empty")
    return resolved


def normalize_periodicity_entry(
    raw: Dict[str, Any],
    field_name: str,
    *,
    require_available_state: bool = False,
    require_complete_intervals: bool = False,
) -> Dict[str, Any]:
    source_bucket = str(
        extract_mapping_value(raw, ("source_bucket", "sourceBucket", "SourceBucket")) or ""
    ).strip().upper()
    target_bucket = str(
        extract_mapping_value(raw, ("target_bucket", "targetBucket", "TargetBucket")) or ""
    ).strip().upper()
    family = str(extract_mapping_value(raw, ("family", "Family")) or "").strip().upper()
    primary_raw = extract_mapping_value(raw, ("primary", "Primary"))
    secondary_raw = extract_mapping_value(raw, ("secondary", "Secondary"))
    sample_count_raw = extract_mapping_value(raw, ("sample_count", "sampleCount", "SampleCount"))
    state_raw = extract_mapping_value(raw, ("state", "State"))
    last_interval_raw = extract_mapping_value(
        raw,
        ("last_interval_sec", "last_interval", "lastInterval", "LastInterval"),
    )
    mean_interval_raw = extract_mapping_value(
        raw,
        ("mean_interval_sec", "mean_interval", "meanInterval", "MeanInterval"),
    )
    min_interval_raw = extract_mapping_value(
        raw,
        ("min_interval_sec", "min_interval", "minInterval", "MinInterval"),
    )
    max_interval_raw = extract_mapping_value(
        raw,
        ("max_interval_sec", "max_interval", "maxInterval", "MaxInterval"),
    )
    if source_bucket == "" or target_bucket == "" or family == "":
        raise ValueError(f"{field_name} missing tuple identity")
    if not isinstance(primary_raw, int) or not isinstance(secondary_raw, int):
        raise ValueError(f"{field_name} missing integer primary/secondary")
    if not isinstance(sample_count_raw, int) or sample_count_raw < 0:
        raise ValueError(f"{field_name} missing non-negative sample_count")
    state = None
    if state_raw is not None:
        state = str(state_raw).strip().lower()
        if state == "":
            raise ValueError(f"{field_name} has empty state")
    if require_available_state and state != "available":
        raise ValueError(f"{field_name} must be state=available")
    if require_complete_intervals:
        last_interval_sec = parse_duration_seconds(last_interval_raw, f"{field_name}.last_interval")
        min_interval_sec = parse_duration_seconds(min_interval_raw, f"{field_name}.min_interval")
        max_interval_sec = parse_duration_seconds(max_interval_raw, f"{field_name}.max_interval")
    else:
        last_interval_sec = (
            None if last_interval_raw is None else parse_duration_seconds(last_interval_raw, f"{field_name}.last_interval")
        )
        min_interval_sec = (
            None if min_interval_raw is None else parse_duration_seconds(min_interval_raw, f"{field_name}.min_interval")
        )
        max_interval_sec = (
            None if max_interval_raw is None else parse_duration_seconds(max_interval_raw, f"{field_name}.max_interval")
        )
    mean_interval_sec = parse_duration_seconds(mean_interval_raw, f"{field_name}.mean_interval")
    return {
        "key": f"{source_bucket}>{target_bucket}:{primary_raw}:{secondary_raw}:{family}",
        "source_bucket": source_bucket,
        "target_bucket": target_bucket,
        "family": family,
        "primary": primary_raw,
        "secondary": secondary_raw,
        "sample_count": sample_count_raw,
        "state": state,
        "last_interval_sec": last_interval_sec,
        "mean_interval_sec": mean_interval_sec,
        "min_interval_sec": min_interval_sec,
        "max_interval_sec": max_interval_sec,
    }


def build_timing_reference_verdict(
    proof_dir: pathlib.Path,
    run_id: str,
    wire_reference_path: pathlib.Path | None = None,
) -> Dict[str, Any]:
    if wire_reference_path is None:
        wire_reference_path = proof_dir / "wire_timing_reference.json"
    reasons: List[str] = []
    start_bundle: Dict[str, Any] | None = None
    end_bundle: Dict[str, Any] | None = None
    wire_reference: Dict[str, Any] | None = None
    wire_reference_valid = False
    wire_proxy_log_path: str | None = None
    observed_busy_seconds = 0.0
    reference_busy_seconds = 0.0
    busy_relative_error: float | None = None
    busy_within_tolerance = False
    stable_reference_tuple_count = 0
    matched_tuple_count = 0
    tuple_comparisons: List[Dict[str, Any]] = []

    try:
        wire_reference = load_wire_timing_reference_artifact(wire_reference_path)
        wire_proxy_log_path = str(
            require_wire_reference_proxy_log_path(proof_dir, wire_reference_path, wire_reference)
        )
        wire_reference_valid = True
    except Exception as exc:  # noqa: BLE001
        reasons.append(str(exc))

    for phase_name, slot_name in (("start", "start"), ("end", "end")):
        try:
            snapshot = load_structured_snapshot_bundle(proof_dir, phase_name)
        except Exception as exc:  # noqa: BLE001
            reasons.append(str(exc))
            continue
        if slot_name == "start":
            start_bundle = snapshot
        else:
            end_bundle = snapshot

    if start_bundle is not None and end_bundle is not None:
        try:
            start_samples = parse_prometheus_samples(
                pathlib.Path(start_bundle["snapshot_paths"]["metrics"]).read_text(encoding="utf-8")
            )
            end_samples = parse_prometheus_samples(
                pathlib.Path(end_bundle["snapshot_paths"]["metrics"]).read_text(encoding="utf-8")
            )
            start_busy_total = aggregate_metric_total(start_samples, TIMING_REFERENCE_BUSY_METRIC, required=True)
            end_busy_total = aggregate_metric_total(end_samples, TIMING_REFERENCE_BUSY_METRIC, required=True)
            if start_busy_total is None or end_busy_total is None:
                reasons.append("missing busy metrics for timing comparator")
            elif end_busy_total < start_busy_total:
                reasons.append("busy metric regressed across proof window")
            else:
                observed_busy_seconds = end_busy_total - start_busy_total
        except Exception as exc:  # noqa: BLE001
            reasons.append(str(exc))

    if wire_reference_valid and wire_reference is not None:
        reference_busy_seconds = float(((wire_reference.get("summary") or {}).get("busy_seconds_total")) or 0.0)

    if reference_busy_seconds > 0 and observed_busy_seconds >= 0:
        busy_relative_error = abs(observed_busy_seconds - reference_busy_seconds) / reference_busy_seconds
        busy_within_tolerance = busy_relative_error <= TIMING_REFERENCE_BUSY_RELATIVE_ERROR_MAX
        if not busy_within_tolerance:
            reasons.append(
                "busy timing mismatch exceeds tolerance "
                f"(observed={observed_busy_seconds:.6f}s reference={reference_busy_seconds:.6f}s "
                f"relative_error={busy_relative_error:.6f} max={TIMING_REFERENCE_BUSY_RELATIVE_ERROR_MAX:.6f})"
            )

    if wire_reference_valid and wire_reference is not None and end_bundle is not None:
        reference_entries: Dict[str, Dict[str, Any]] = {}
        for index, raw_entry in enumerate(wire_reference.get("periodicity") or []):
            if not isinstance(raw_entry, dict):
                reasons.append(f"wire timing reference periodicity[{index}] must be object")
                continue
            try:
                normalized = normalize_periodicity_entry(raw_entry, f"wire_timing_reference.periodicity[{index}]")
            except Exception as exc:  # noqa: BLE001
                reasons.append(str(exc))
                continue
            if normalized["key"] in reference_entries:
                reasons.append(f"duplicate wire timing reference periodicity tuple {normalized['key']}")
                continue
            reference_entries[normalized["key"]] = normalized

        bus_observability = end_bundle["bus_observability"] or {}
        observed_raw = bus_observability.get("periodicity")
        if not isinstance(observed_raw, list):
            observed_raw = (((bus_observability.get("summary") or {}).get("status") or {}).get("periodicity"))
        if not isinstance(observed_raw, list):
            reasons.append("end bus observability snapshot missing periodicity list")
        else:
            observed_entries: Dict[str, Dict[str, Any]] = {}
            for index, raw_entry in enumerate(observed_raw):
                if not isinstance(raw_entry, dict):
                    reasons.append(f"end bus periodicity[{index}] must be object")
                    continue
                try:
                    normalized = normalize_periodicity_entry(
                        raw_entry,
                        f"end.bus_observability.periodicity[{index}]",
                        require_available_state=True,
                        require_complete_intervals=True,
                    )
                except Exception as exc:  # noqa: BLE001
                    reasons.append(str(exc))
                    continue
                if normalized["key"] in observed_entries:
                    reasons.append(f"duplicate observed periodicity tuple {normalized['key']}")
                    continue
                observed_entries[normalized["key"]] = normalized

            for key, reference_entry in reference_entries.items():
                if reference_entry["sample_count"] < TIMING_REFERENCE_PERIODICITY_MIN_SAMPLES:
                    continue
                stable_reference_tuple_count += 1
                observed_entry = observed_entries.get(key)
                if observed_entry is None:
                    reasons.append(f"missing observed periodicity tuple {key}")
                    tuple_comparisons.append({"key": key, "ok": False, "reason": "missing_observed_tuple"})
                    continue
                if observed_entry["sample_count"] < TIMING_REFERENCE_PERIODICITY_MIN_SAMPLES:
                    reasons.append(
                        f"observed periodicity tuple {key} has sample_count={observed_entry['sample_count']}; "
                        f"want >= {TIMING_REFERENCE_PERIODICITY_MIN_SAMPLES}"
                    )
                    tuple_comparisons.append({"key": key, "ok": False, "reason": "observed_sample_count_below_min"})
                    continue
                tolerance_sec = max(
                    reference_entry["mean_interval_sec"] * TIMING_REFERENCE_PERIODICITY_RELATIVE_ERROR_MAX,
                    TIMING_REFERENCE_PERIODICITY_ABSOLUTE_ERROR_SEC_MAX,
                )
                delta_sec = abs(observed_entry["mean_interval_sec"] - reference_entry["mean_interval_sec"])
                ok = delta_sec <= tolerance_sec
                if not ok:
                    reasons.append(
                        "periodicity timing mismatch exceeds tolerance "
                        f"for {key} (observed={observed_entry['mean_interval_sec']:.6f}s "
                        f"reference={reference_entry['mean_interval_sec']:.6f}s "
                        f"delta={delta_sec:.6f}s max={tolerance_sec:.6f}s)"
                    )
                else:
                    matched_tuple_count += 1
                tuple_comparisons.append(
                    {
                        "key": key,
                        "ok": ok,
                        "observed_mean_interval_sec": observed_entry["mean_interval_sec"],
                        "reference_mean_interval_sec": reference_entry["mean_interval_sec"],
                        "delta_sec": delta_sec,
                        "tolerance_sec": tolerance_sec,
                        "observed_sample_count": observed_entry["sample_count"],
                        "reference_sample_count": reference_entry["sample_count"],
                    }
                )
            if stable_reference_tuple_count == 0:
                reasons.append(
                    f"wire timing reference has no stable periodicity tuples with >= {TIMING_REFERENCE_PERIODICITY_MIN_SAMPLES} samples"
                )

    status = "pass" if len(reasons) == 0 else "fail"
    reason = "ok" if status == "pass" else "; ".join(reasons)
    periodicity_within_tolerance = stable_reference_tuple_count > 0 and matched_tuple_count == stable_reference_tuple_count

    return {
        "schema": TIMING_REFERENCE_VERDICT_SCHEMA,
        "captured_at": utc_now(),
        "run_id": run_id,
        "source": "wire_timing_reference_plus_structured_snapshots",
        "claim_scope": "bounded_proof_window_busy_periodicity_comparator",
        "evidence": {
            "wire_timing_reference_path": str(wire_reference_path),
            "wire_proxy_log_path": wire_proxy_log_path,
            "start_snapshot_paths": None if start_bundle is None else start_bundle["snapshot_paths"],
            "end_snapshot_paths": None if end_bundle is None else end_bundle["snapshot_paths"],
        },
        "summary": {
            "reference_busy_seconds_total": reference_busy_seconds,
            "observed_busy_seconds_total": observed_busy_seconds,
            "busy_relative_error": busy_relative_error,
            "busy_relative_error_max": TIMING_REFERENCE_BUSY_RELATIVE_ERROR_MAX,
            "stable_reference_tuple_count": stable_reference_tuple_count,
            "matched_tuple_count": matched_tuple_count,
            "periodicity_relative_error_max": TIMING_REFERENCE_PERIODICITY_RELATIVE_ERROR_MAX,
            "periodicity_absolute_error_sec_max": TIMING_REFERENCE_PERIODICITY_ABSOLUTE_ERROR_SEC_MAX,
            "periodicity_min_samples": TIMING_REFERENCE_PERIODICITY_MIN_SAMPLES,
        },
        "periodicity": tuple_comparisons,
        "criteria": {
            "wire_reference_valid": {
                "ok": wire_reference_valid,
                "reason": "ok" if wire_reference_valid else "missing or invalid wire timing reference artifact",
            },
            "busy_within_tolerance": {
                "ok": wire_reference_valid and start_bundle is not None and end_bundle is not None and busy_within_tolerance,
                "reason": "ok" if busy_within_tolerance else "busy timing mismatch exceeds tolerance or evidence is incomplete",
            },
            "stable_periodicity_reference_present": {
                "ok": stable_reference_tuple_count > 0,
                "reason": (
                    "ok"
                    if stable_reference_tuple_count > 0
                    else f"wire timing reference has no stable periodicity tuples with >= {TIMING_REFERENCE_PERIODICITY_MIN_SAMPLES} samples"
                ),
            },
            "periodicity_within_tolerance": {
                "ok": periodicity_within_tolerance,
                "reason": (
                    "ok"
                    if periodicity_within_tolerance
                    else "periodicity timing mismatch exceeds tolerance or evidence is incomplete"
                ),
            },
        },
        "ok": status == "pass",
        "status": status,
        "reason": reason,
    }


def extract_locked_replay_case_names(corpus: Any, source_path: pathlib.Path) -> Tuple[Tuple[str, ...], str]:
    replay_case_contracts, replay_case_contracts_error = extract_locked_replay_case_contracts(
        corpus,
        source_path,
    )
    if replay_case_contracts_error:
        return tuple(), replay_case_contracts_error
    return tuple(sorted(replay_case_contracts.keys())), ""


def extract_locked_replay_case_contracts(
    corpus: Any,
    source_path: pathlib.Path,
) -> Tuple[Dict[str, Dict[str, Any]], str]:
    if not isinstance(corpus, dict):
        return {}, f"{source_path}: replay corpus must be a JSON object"
    cases = corpus.get("cases")
    if not isinstance(cases, list):
        return {}, f"{source_path}: replay corpus missing cases array"

    seen_names: set[str] = set()
    contracts: Dict[str, Dict[str, Any]] = {}
    for index, raw_case in enumerate(cases):
        if not isinstance(raw_case, dict):
            return {}, f"{source_path}: replay case[{index}] must be object"
        expected = raw_case.get("replay_expected")
        if expected is None:
            continue
        if not isinstance(expected, dict):
            return {}, f"{source_path}: replay case[{index}] replay_expected contract must be an object"
        raw_name = raw_case.get("name")
        if not isinstance(raw_name, str):
            return {}, f"{source_path}: replay case[{index}] name must be non-empty string"
        name = raw_name.strip()
        if name == "":
            return {}, f"{source_path}: replay case[{index}] missing name"
        if name in seen_names:
            return {}, f"{source_path}: replay case[{index}] duplicate replay case name {name!r}"
        seen_names.add(name)
        family_raw = raw_case.get("family")
        if not isinstance(family_raw, str):
            return {}, f"{source_path}: replay case[{index}] family must be non-empty string"
        family = family_raw.strip().upper()
        if family == "":
            return {}, f"{source_path}: replay case[{index}] missing family"
        response_class_raw = raw_case.get("response_class")
        if not isinstance(response_class_raw, str):
            return {}, f"{source_path}: replay case[{index}] response_class must be non-empty string"
        response_class = response_class_raw.strip()
        if response_class == "":
            return {}, f"{source_path}: replay case[{index}] missing response_class"
        raw_scenario_tags = raw_case.get("scenario_tags")
        if not isinstance(raw_scenario_tags, list):
            return {}, f"{source_path}: replay case[{index}] scenario_tags must be an array"
        scenario_tags: List[str] = []
        for tag_index, tag_raw in enumerate(raw_scenario_tags):
            if not isinstance(tag_raw, str) or tag_raw.strip() == "":
                return {}, f"{source_path}: replay case[{index}] invalid scenario_tags[{tag_index}]"
            scenario_tags.append(tag_raw.strip())
        expected_reason_raw = expected.get("reason")
        if not isinstance(expected_reason_raw, str):
            return {}, f"{source_path}: replay case[{index}] replay_expected.reason must be non-empty string"
        expected_reason = expected_reason_raw.strip()
        if expected_reason == "":
            return {}, f"{source_path}: replay case[{index}] replay_expected.reason must be non-empty string"
        expected_direct_apply = expected.get("direct_apply")
        if not isinstance(expected_direct_apply, bool):
            return {}, f"{source_path}: replay case[{index}] replay_expected.direct_apply must be boolean"
        expected_disposition_raw = expected.get("disposition")
        if not isinstance(expected_disposition_raw, str):
            return {}, (
                f"{source_path}: replay case[{index}] replay_expected.disposition must be non-empty string"
            )
        expected_disposition = expected_disposition_raw.strip().lower()
        if expected_disposition == "":
            return {}, (
                f"{source_path}: replay case[{index}] replay_expected.disposition must be non-empty string"
            )
        if expected_disposition not in REPLAY_EXPECTED_DISPOSITIONS:
            return {}, (
                f"{source_path}: replay case[{index}] unsupported replay_expected.disposition "
                f"{expected_disposition!r}"
            )
        contracts[name] = {
            "family": family,
            "response_class": response_class,
            "scenario_tags": scenario_tags,
            "expected_reason": expected_reason,
            "expected_direct_apply": expected_direct_apply,
            "expected_disposition": expected_disposition,
        }

    if len(contracts) == 0:
        return {}, f"{source_path}: canonical replay proof-set has no locked replay cases"
    return contracts, ""


def load_canonical_family_proof_canary_ids() -> Tuple[Tuple[str, ...], str]:
    global _CANONICAL_FAMILY_PROOF_CANARY_IDS

    if _CANONICAL_FAMILY_PROOF_CANARY_IDS is not None:
        return _CANONICAL_FAMILY_PROOF_CANARY_IDS, ""
    try:
        _, canaries = load_and_validate_manifest(
            CANONICAL_P03_CANARY_MANIFEST_PATH,
            require_case_id=CANONICAL_FAMILY_PROOF_CASE_ID,
        )
    except Exception as exc:
        return tuple(), (
            f"unable to load canonical canary proof-set from {CANONICAL_P03_CANARY_MANIFEST_PATH}: {exc}"
        )
    canary_ids = tuple(sorted(item.canary_id for item in canaries))
    if len(canary_ids) == 0:
        return tuple(), (
            f"unable to load canonical canary proof-set from {CANONICAL_P03_CANARY_MANIFEST_PATH}: "
            "empty canary set"
        )
    _CANONICAL_FAMILY_PROOF_CANARY_IDS = canary_ids
    return _CANONICAL_FAMILY_PROOF_CANARY_IDS, ""


def load_canonical_family_proof_replay_case_names() -> Tuple[Tuple[str, ...], str]:
    global _CANONICAL_FAMILY_PROOF_REPLAY_CASE_NAMES

    if _CANONICAL_FAMILY_PROOF_REPLAY_CASE_NAMES is not None:
        return _CANONICAL_FAMILY_PROOF_REPLAY_CASE_NAMES, ""
    try:
        corpus = load_json(CANONICAL_REPLAY_CORPUS_PATH)
    except Exception as exc:
        return tuple(), (
            f"unable to load canonical replay proof-set from {CANONICAL_REPLAY_CORPUS_PATH}: {exc}"
        )
    replay_case_names, replay_case_names_error = extract_locked_replay_case_names(
        corpus,
        CANONICAL_REPLAY_CORPUS_PATH,
    )
    if replay_case_names_error:
        return tuple(), replay_case_names_error
    _CANONICAL_FAMILY_PROOF_REPLAY_CASE_NAMES = replay_case_names
    return _CANONICAL_FAMILY_PROOF_REPLAY_CASE_NAMES, ""


def load_canonical_family_proof_replay_case_contracts() -> Tuple[Dict[str, Dict[str, Any]], str]:
    global _CANONICAL_FAMILY_PROOF_REPLAY_CASE_CONTRACTS

    if _CANONICAL_FAMILY_PROOF_REPLAY_CASE_CONTRACTS is not None:
        return _CANONICAL_FAMILY_PROOF_REPLAY_CASE_CONTRACTS, ""
    try:
        corpus = load_json(CANONICAL_REPLAY_CORPUS_PATH)
    except Exception as exc:
        return {}, (
            f"unable to load canonical replay proof-set from {CANONICAL_REPLAY_CORPUS_PATH}: {exc}"
        )
    replay_case_contracts, replay_case_contracts_error = extract_locked_replay_case_contracts(
        corpus,
        CANONICAL_REPLAY_CORPUS_PATH,
    )
    if replay_case_contracts_error:
        return {}, replay_case_contracts_error
    _CANONICAL_FAMILY_PROOF_REPLAY_CASE_CONTRACTS = replay_case_contracts
    return _CANONICAL_FAMILY_PROOF_REPLAY_CASE_CONTRACTS, ""


def normalize_retries(raw: int) -> int:
    if raw < 1:
        return 1
    if raw > MAX_RETRIES:
        return MAX_RETRIES
    return raw


def invoke_canary(
    graphql_url: str,
    canary: CanarySpec,
    timeout_sec: float,
    nonce: str | None = None,
) -> str:
    mutation = """
mutation($address:Int!, $plane:String!, $method:String!, $params: JSON) {
  invoke(address: $address, plane: $plane, method: $method, params: $params) {
    ok
    error { message code category }
    result
  }
}
""".strip()
    params = dict(canary.params)
    if nonce:
        params[CANARY_NONCE_PARAM] = nonce
    payload = {
        "query": mutation,
        "variables": {
            "address": canary.address,
            "plane": canary.plane,
            "method": canary.method,
            "params": params,
        },
    }
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        graphql_url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.URLError as exc:
        raise RuntimeError(f"transport error: {exc}") from exc
    decoded = json.loads(raw)
    if isinstance(decoded.get("errors"), list) and decoded["errors"]:
        raise RuntimeError(f"graphql errors: {decoded['errors']!r}")
    invoke_payload = (((decoded.get("data") or {}).get("invoke")) or {})
    if not isinstance(invoke_payload, dict):
        raise RuntimeError("graphql response missing data.invoke")
    if not bool(invoke_payload.get("ok", False)):
        error = invoke_payload.get("error")
        if isinstance(error, dict):
            message = str(error.get("message", "")).strip()
            code = str(error.get("code", "")).strip()
            category = str(error.get("category", "")).strip()
            raise RuntimeError(f"invoke not ok: code={code} category={category} message={message}")
        raise RuntimeError("invoke not ok")
    return extract_value_hex(invoke_payload.get("result"), canary.result_field)


def parse_prometheus_samples(metrics_text: str) -> Dict[str, List[float]]:
    if metrics_text.strip() == "":
        raise ValueError("metrics payload is empty")
    samples: Dict[str, List[float]] = {}
    for raw_line in metrics_text.splitlines():
        line = raw_line.strip()
        if line == "" or line.startswith("#"):
            continue
        match = PROM_SAMPLE_RE.match(line)
        if not match:
            continue
        metric_name = match.group(1)
        raw_value = match.group(3)
        try:
            value = float(raw_value)
        except ValueError:
            continue
        samples.setdefault(metric_name, []).append(value)
    return samples


def aggregate_metric_total(samples: Dict[str, List[float]], metric_name: str, *, required: bool) -> float | None:
    values = samples.get(metric_name, [])
    if not values:
        if required:
            raise ValueError(f"missing required metric sample: {metric_name}")
        return None
    total = float(sum(values))
    if not math.isfinite(total):
        raise ValueError(f"metric {metric_name} has non-finite total {total!r}")
    if total < 0:
        raise ValueError(f"metric {metric_name} has negative total {total!r}")
    return total


def collect_read_avoidance_totals(samples: Dict[str, List[float]]) -> Dict[str, float | None]:
    return {
        READ_AVOIDANCE_DIRECT_APPLY_METRIC: aggregate_metric_total(
            samples,
            READ_AVOIDANCE_DIRECT_APPLY_METRIC,
            required=True,
        ),
        READ_AVOIDANCE_ACTIVE_AVOIDED_METRIC: aggregate_metric_total(
            samples,
            READ_AVOIDANCE_ACTIVE_AVOIDED_METRIC,
            required=True,
        ),
        READ_AVOIDANCE_SAVED_SECONDS_METRIC: aggregate_metric_total(
            samples,
            READ_AVOIDANCE_SAVED_SECONDS_METRIC,
            required=False,
        ),
    }


def collect_proof_window_traffic_totals(samples: Dict[str, List[float]]) -> Dict[str, float]:
    return {
        PROOF_WINDOW_COMPLETED_TRANSACTIONS_METRIC: float(
            aggregate_metric_total(
                samples,
                PROOF_WINDOW_COMPLETED_TRANSACTIONS_METRIC,
                required=True,
            )
            or 0.0
        ),
        PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_METRIC: float(
            aggregate_metric_total(
                samples,
                PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_METRIC,
                required=True,
            )
            or 0.0
        ),
    }


def phase_metrics_snapshot_path(output_path: pathlib.Path, phase: str) -> pathlib.Path:
    proof_dir = output_path.parent
    return proof_phase_metrics_snapshot_path(proof_dir, phase)


def proof_phase_metrics_snapshot_path(proof_dir: pathlib.Path, phase: str) -> pathlib.Path:
    normalized = phase.strip().lower()
    if normalized in ("start", "end"):
        return proof_dir / f"{normalized}_metrics.prom"
    if is_interval_phase(normalized):
        return proof_dir / "samples" / f"{normalized}_metrics.prom"
    raise ValueError(f"unsupported canary phase for metrics snapshot lookup: {phase!r}")


def proof_phase_result_path(proof_dir: pathlib.Path, phase: str) -> pathlib.Path:
    normalized = phase.strip().lower()
    if normalized == "":
        raise ValueError("unsupported empty canary phase for result lookup")
    return proof_dir / f"canary_phase_{normalized}.json"


def proof_phase_bus_observability_snapshot_path(proof_dir: pathlib.Path, phase: str) -> pathlib.Path:
    normalized = phase.strip().lower()
    if normalized in ("start", "end"):
        return proof_dir / f"{normalized}_bus_observability.json"
    if is_interval_phase(normalized):
        return proof_dir / "samples" / f"{normalized}_bus_observability.json"
    raise ValueError(f"unsupported canary phase for bus observability snapshot lookup: {phase!r}")


def proof_phase_graphql_bus_watch_snapshot_path(proof_dir: pathlib.Path, phase: str) -> pathlib.Path:
    normalized = phase.strip().lower()
    if normalized in ("start", "end"):
        return proof_dir / f"{normalized}_graphql_bus_watch.json"
    if is_interval_phase(normalized):
        return proof_dir / "samples" / f"{normalized}_graphql_bus_watch.json"
    raise ValueError(f"unsupported canary phase for graphql bus watch snapshot lookup: {phase!r}")


def proof_phase_feature_flag_snapshot_path(proof_dir: pathlib.Path, phase: str) -> pathlib.Path:
    normalized = phase.strip().lower()
    if normalized in ("start", "end"):
        return proof_dir / f"{normalized}_feature_flags.json"
    if is_interval_phase(normalized):
        return proof_dir / "samples" / f"{normalized}_feature_flags.json"
    raise ValueError(f"unsupported canary phase for feature flag snapshot lookup: {phase!r}")


def proof_phase_bus_observability_path(proof_dir: pathlib.Path, phase: str) -> pathlib.Path:
    normalized = phase.strip().lower()
    if normalized in ("start", "end"):
        return proof_dir / f"{normalized}_bus_observability.json"
    if is_interval_phase(normalized):
        return proof_dir / "samples" / f"{normalized}_bus_observability.json"
    raise ValueError(f"unsupported bus-observability phase lookup: {phase!r}")


def proof_phase_graphql_bus_watch_path(proof_dir: pathlib.Path, phase: str) -> pathlib.Path:
    normalized = phase.strip().lower()
    if normalized in ("start", "end"):
        return proof_dir / f"{normalized}_graphql_bus_watch.json"
    if is_interval_phase(normalized):
        return proof_dir / "samples" / f"{normalized}_graphql_bus_watch.json"
    raise ValueError(f"unsupported GraphQL bus-watch phase lookup: {phase!r}")


def structured_snapshot_paths(proof_dir: pathlib.Path, snapshot_prefix: str) -> Dict[str, pathlib.Path]:
    normalized = snapshot_prefix.strip().lower()
    if normalized == "":
        raise ValueError("unsupported empty structured snapshot prefix")
    return {
        "metrics": proof_dir / f"{normalized}_metrics.prom",
        "bus_observability": proof_dir / f"{normalized}_bus_observability.json",
        "graphql_bus_watch": proof_dir / f"{normalized}_graphql_bus_watch.json",
        "feature_flags": proof_dir / f"{normalized}_feature_flags.json",
    }


def canonicalize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): canonicalize_json_value(value[key]) for key in sorted(value.keys())}
    if isinstance(value, list):
        return [canonicalize_json_value(item) for item in value]
    return value


def normalize_timestamp_value(raw: Any, snapshot_path: pathlib.Path, source_name: str, field_name: str) -> str:
    if not isinstance(raw, str):
        raise ValueError(f"{snapshot_path}: {source_name} {field_name} must be a string")
    value = raw.strip()
    if value == "":
        raise ValueError(f"{snapshot_path}: {source_name} {field_name} must be a non-empty string")
    if not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d)", value):
        raise ValueError(f"{snapshot_path}: {source_name} {field_name} must be RFC3339")
    if value.endswith("+00:00") or value.endswith("-00:00"):
        return value[:-6] + "Z"
    return value


def parse_timestamp_to_utc(raw: Any, snapshot_path: pathlib.Path, source_name: str, field_name: str) -> datetime:
    normalized = normalize_timestamp_value(raw, snapshot_path, source_name, field_name)
    parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"{snapshot_path}: {source_name} {field_name} must contain timezone information")
    return parsed.astimezone(timezone.utc)


def extract_timestamp_alias(raw: Any, snapshot_path: pathlib.Path, source_name: str, aliases: Iterable[str]) -> str | None:
    if not isinstance(raw, dict):
        raise ValueError(f"{snapshot_path}: {source_name} payload must be a JSON object")
    for field_name in aliases:
        if field_name in raw:
            return normalize_timestamp_value(raw[field_name], snapshot_path, source_name, field_name)
    return None


def feature_flag_field_default(source_name: str, field: str) -> Any:
    if source_name == "bus_observability" and field == "normalizations":
        return []
    return None


def normalize_feature_flag_state(raw: Any, snapshot_path: pathlib.Path, source_name: str) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"{snapshot_path}: {source_name} feature flags must be a JSON object")
    state: Dict[str, Any] = {}
    for field in FEATURE_FLAG_FIELDS:
        alias_names = FEATURE_FLAG_FIELD_ALIASES.get(field, (field,))
        present_name = None
        for candidate in alias_names:
            if candidate in raw:
                present_name = candidate
                break
        if present_name is None:
            default_value = feature_flag_field_default(source_name, field)
            if default_value is not None:
                state[field] = canonicalize_json_value(default_value)
                continue
            raise ValueError(f"{snapshot_path}: {source_name} feature flags missing {field}")
        value = raw[present_name]
        if value is None:
            default_value = feature_flag_field_default(source_name, field)
            if default_value is not None:
                state[field] = canonicalize_json_value(default_value)
                continue
            raise ValueError(f"{snapshot_path}: {source_name} feature flags field {field!r} is null")
        state[field] = canonicalize_json_value(value)
    return state


def canonical_feature_flag_key(state: Dict[str, Any]) -> str:
    return json.dumps(state, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def compare_feature_flag_states(
    left_state: Dict[str, Any],
    right_state: Dict[str, Any],
) -> Tuple[bool, str | None]:
    for field in FEATURE_FLAG_FIELDS:
        if left_state.get(field) != right_state.get(field):
            return False, field
    return True, None


def load_feature_flag_snapshot(snapshot_path: pathlib.Path, phase: str) -> Dict[str, Any]:
    if not snapshot_path.exists():
        raise ValueError(f"missing required feature flag proof artifact: {snapshot_path}")
    payload = load_json(snapshot_path)
    if not isinstance(payload, dict):
        raise ValueError(f"{snapshot_path}: feature flag snapshot must be a JSON object")

    graphql_state = normalize_feature_flag_state(
        payload.get("graphql_feature_flags"),
        snapshot_path,
        "graphql",
    )
    graphql_last_updated_at = extract_timestamp_alias(
        payload.get("graphql_feature_flags"),
        snapshot_path,
        "graphql",
        ("lastUpdatedAt", "last_updated_at"),
    )
    bus_state = normalize_feature_flag_state(
        payload.get("bus_observability_feature_flags"),
        snapshot_path,
        "bus_observability",
    )
    bus_last_updated_at = extract_timestamp_alias(
        payload.get("bus_observability_feature_flags"),
        snapshot_path,
        "bus_observability",
        ("last_updated_at", "lastUpdatedAt"),
    )
    if graphql_last_updated_at is None:
        raise ValueError(f"{snapshot_path}: graphql feature flags missing lastUpdatedAt")
    if bus_last_updated_at is None:
        raise ValueError(f"{snapshot_path}: bus-observability feature flags missing last_updated_at")
    captured_at = normalize_timestamp_value(
        payload.get("captured_at"),
        snapshot_path,
        "feature flag snapshot",
        "captured_at",
    )
    canonical_graphql_key = canonical_feature_flag_key(graphql_state)
    canonical_bus_key = canonical_feature_flag_key(bus_state)
    if canonical_graphql_key != canonical_bus_key:
        mismatch_field = None
        for field in FEATURE_FLAG_FIELDS:
            if graphql_state[field] != bus_state[field]:
                mismatch_field = field
                break
        raise ValueError(
            f"{snapshot_path}: canonical feature flag state drift between GraphQL and bus-observability "
            f"at phase {phase} ({mismatch_field or 'unknown field'})"
        )

    return {
        "phase": phase,
        "feature_flags_snapshot_path": str(snapshot_path),
        "captured_at": captured_at,
        "graphql_feature_flags": graphql_state,
        "bus_observability_feature_flags": bus_state,
        "graphql_feature_flags_last_updated_at": graphql_last_updated_at,
        "bus_observability_feature_flags_last_updated_at": bus_last_updated_at,
        "canonical_feature_flags": graphql_state,
        "canonical_feature_flags_key": canonical_graphql_key,
    }


def load_phase_status_snapshot(proof_dir: pathlib.Path, phase: str) -> Dict[str, Any]:
    bus_path = proof_phase_bus_observability_path(proof_dir, phase)
    graphql_path = proof_phase_graphql_bus_watch_path(proof_dir, phase)
    if not bus_path.exists():
        raise ValueError(f"missing required bus-observability proof artifact: {bus_path}")
    if not graphql_path.exists():
        raise ValueError(f"missing required GraphQL proof artifact: {graphql_path}")

    bus_payload = load_json(bus_path)
    graphql_payload = load_json(graphql_path)
    if not isinstance(bus_payload, dict):
        raise ValueError(f"{bus_path}: bus-observability payload must be a JSON object")
    if not isinstance(graphql_payload, dict):
        raise ValueError(f"{graphql_path}: GraphQL payload must be a JSON object")

    startup = (((bus_payload.get("summary") or {}).get("status") or {}).get("startup") or {})
    if not isinstance(startup, dict):
        raise ValueError(f"{bus_path}: missing summary.status.startup object")
    startup_phase = str(startup.get("phase", "")).strip()
    if startup_phase == "":
        raise ValueError(f"{bus_path}: missing summary.status.startup.phase")
    bus_summary = bus_payload.get("summary") or {}
    bus_status = (bus_summary.get("status") or {}) if isinstance(bus_summary, dict) else {}
    graphql_data = graphql_payload.get("data") or {}
    graphql_bus_summary = (graphql_data.get("busSummary") or {}) if isinstance(graphql_data, dict) else {}
    graphql_status = (graphql_bus_summary.get("status") or {}) if isinstance(graphql_bus_summary, dict) else {}
    graphql_startup = graphql_status.get("startup") or {}
    graphql_feature_flags = graphql_status.get("featureFlags") or {}
    watch_summary = (graphql_data.get("watchSummary") or {}) if isinstance(graphql_data, dict) else {}
    if not isinstance(bus_summary, dict):
        raise ValueError(f"{bus_path}: missing summary object")
    if not isinstance(bus_status, dict):
        raise ValueError(f"{bus_path}: missing summary.status object")
    if not isinstance(graphql_data, dict):
        raise ValueError(f"{graphql_path}: missing data object")
    if not isinstance(graphql_bus_summary, dict):
        raise ValueError(f"{graphql_path}: missing data.busSummary object")
    if not isinstance(graphql_status, dict):
        raise ValueError(f"{graphql_path}: missing data.busSummary.status object")
    if not isinstance(graphql_startup, dict):
        raise ValueError(f"{graphql_path}: missing data.busSummary.status.startup object")
    if not isinstance(graphql_feature_flags, dict):
        raise ValueError(f"{graphql_path}: missing data.busSummary.status.featureFlags object")
    if not isinstance(watch_summary, dict):
        raise ValueError(f"{graphql_path}: missing data.watchSummary object")
    bus_feature_flags = bus_status.get("feature_flags")
    if not isinstance(bus_feature_flags, dict):
        raise ValueError(f"{bus_path}: missing summary.status.feature_flags object")

    summary_last_updated_at = normalize_timestamp_value(
        bus_summary.get("last_updated_at"),
        bus_path,
        "bus observability",
        "summary.last_updated_at",
    )
    status_last_updated_at = normalize_timestamp_value(
        bus_status.get("last_updated_at"),
        bus_path,
        "bus observability",
        "summary.status.last_updated_at",
    )
    startup_last_updated_at = normalize_timestamp_value(
        startup.get("last_updated_at"),
        bus_path,
        "bus observability",
        "summary.status.startup.last_updated_at",
    )
    feature_flags_last_updated_at = normalize_timestamp_value(
        bus_feature_flags.get("last_updated_at"),
        bus_path,
        "bus observability",
        "summary.status.feature_flags.last_updated_at",
    )
    graphql_summary_last_updated_at = normalize_timestamp_value(
        graphql_bus_summary.get("lastUpdatedAt"),
        graphql_path,
        "graphql",
        "data.busSummary.lastUpdatedAt",
    )
    graphql_status_last_updated_at = normalize_timestamp_value(
        graphql_status.get("lastUpdatedAt"),
        graphql_path,
        "graphql",
        "data.busSummary.status.lastUpdatedAt",
    )
    graphql_startup_last_updated_at = normalize_timestamp_value(
        graphql_startup.get("lastUpdatedAt"),
        graphql_path,
        "graphql",
        "data.busSummary.status.startup.lastUpdatedAt",
    )
    graphql_feature_flags_last_updated_at = normalize_timestamp_value(
        graphql_feature_flags.get("lastUpdatedAt"),
        graphql_path,
        "graphql",
        "data.busSummary.status.featureFlags.lastUpdatedAt",
    )
    watch_summary_last_updated_at = normalize_timestamp_value(
        watch_summary.get("lastUpdatedAt"),
        graphql_path,
        "graphql",
        "data.watchSummary.lastUpdatedAt",
    )

    warmup = (((((graphql_payload.get("data") or {}).get("busSummary") or {}).get("status") or {}).get("warmup")) or {})
    if not isinstance(warmup, dict):
        raise ValueError(f"{graphql_path}: missing data.busSummary.status.warmup object")
    warmup_state = str(warmup.get("state", "")).strip()
    if warmup_state == "":
        raise ValueError(f"{graphql_path}: missing data.busSummary.status.warmup.state")

    return {
        "phase": phase,
        "bus_observability_path": str(bus_path),
        "graphql_bus_watch_path": str(graphql_path),
        "startup_phase": startup_phase,
        "warmup_state": warmup_state,
        "timestamps": {
            "bus_observability": {
                "summary_last_updated_at": summary_last_updated_at,
                "status_last_updated_at": status_last_updated_at,
                "startup_last_updated_at": startup_last_updated_at,
                "feature_flags_last_updated_at": feature_flags_last_updated_at,
            },
            "graphql_bus_watch": {
                "summary_last_updated_at": graphql_summary_last_updated_at,
                "status_last_updated_at": graphql_status_last_updated_at,
                "startup_last_updated_at": graphql_startup_last_updated_at,
                "feature_flags_last_updated_at": graphql_feature_flags_last_updated_at,
                "watch_summary_last_updated_at": watch_summary_last_updated_at,
            },
        },
    }


def build_phase_read_avoidance_observation(
    output_path: pathlib.Path,
    phase: str,
    run_id: str,
) -> Dict[str, Any]:
    snapshot_path = phase_metrics_snapshot_path(output_path, phase)
    if not snapshot_path.exists():
        raise ValueError(f"missing required proof metrics artifact: {snapshot_path}")
    totals = collect_read_avoidance_totals(parse_prometheus_samples(snapshot_path.read_text(encoding="utf-8")))
    return {
        "schema": READ_AVOIDANCE_ACCOUNTING_SCHEMA,
        "claim_scope": "phase_local_non_authoritative_observation",
        "captured_at": utc_now(),
        "run_id": run_id,
        "phase": phase,
        "evidence": {
            "metrics_snapshot_path": str(snapshot_path),
        },
        "totals": totals,
        "notes": [
            "Non-authoritative phase-local observation only.",
            "Authoritative proof claim is derived from start/end deltas in canary_summary.",
        ],
    }


def build_window_read_avoidance_accounting(proof_dir: pathlib.Path) -> Dict[str, Any]:
    return build_window_read_avoidance_accounting_for_phases(proof_dir, ["start", "end"])


def build_window_feature_flag_consistency(proof_dir: pathlib.Path) -> Dict[str, Any]:
    return build_window_feature_flag_consistency_for_phases(proof_dir, ["start", "end"])


def build_window_read_avoidance_accounting_for_phases(proof_dir: pathlib.Path, phases: Iterable[str]) -> Dict[str, Any]:
    start_metrics_path = proof_dir / "start_metrics.prom"
    end_metrics_path = proof_dir / "end_metrics.prom"
    if not start_metrics_path.exists():
        raise ValueError(f"missing required proof metrics artifact: {start_metrics_path}")
    if not end_metrics_path.exists():
        raise ValueError(f"missing required proof metrics artifact: {end_metrics_path}")

    start_samples = parse_prometheus_samples(start_metrics_path.read_text(encoding="utf-8"))
    end_samples = parse_prometheus_samples(end_metrics_path.read_text(encoding="utf-8"))
    start_totals = collect_read_avoidance_totals(start_samples)
    end_totals = collect_read_avoidance_totals(end_samples)
    start_proof_window_totals = collect_proof_window_traffic_totals(start_samples)
    end_proof_window_totals = collect_proof_window_traffic_totals(end_samples)

    ordered_phases: List[str] = []
    seen_phases = set()
    for raw_phase in phases:
        phase = str(raw_phase).strip().lower()
        if phase == "" or phase in seen_phases:
            continue
        if phase in ("start", "end") or is_interval_phase(phase):
            ordered_phases.append(phase)
            seen_phases.add(phase)
    if "start" not in seen_phases:
        ordered_phases.insert(0, "start")
    if "end" not in seen_phases:
        ordered_phases.append("end")

    metrics_sequence: List[Dict[str, Any]] = []
    previous_direct_apply: float | None = None
    previous_avoided: float | None = None
    previous_completed_transactions: float | None = None
    previous_direct_apply_candidates: float | None = None
    for phase in ordered_phases:
        snapshot_path = proof_phase_metrics_snapshot_path(proof_dir, phase)
        if not snapshot_path.exists():
            raise ValueError(f"missing required proof metrics artifact: {snapshot_path}")
        phase_samples = parse_prometheus_samples(snapshot_path.read_text(encoding="utf-8"))
        totals = collect_read_avoidance_totals(phase_samples)
        proof_window_totals = collect_proof_window_traffic_totals(phase_samples)
        direct_apply_total = float(totals[READ_AVOIDANCE_DIRECT_APPLY_METRIC] or 0.0)
        avoided_total = float(totals[READ_AVOIDANCE_ACTIVE_AVOIDED_METRIC] or 0.0)
        completed_transactions_total = float(proof_window_totals[PROOF_WINDOW_COMPLETED_TRANSACTIONS_METRIC])
        direct_apply_candidates_total = float(
            proof_window_totals[PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_METRIC]
        )
        if previous_direct_apply is not None and direct_apply_total + 1e-9 < previous_direct_apply:
            raise ValueError(
                "incoherent read-avoidance metrics: "
                f"{READ_AVOIDANCE_DIRECT_APPLY_METRIC} decreased at phase {phase}"
            )
        if previous_avoided is not None and avoided_total + 1e-9 < previous_avoided:
            raise ValueError(
                "incoherent read-avoidance metrics: "
                f"{READ_AVOIDANCE_ACTIVE_AVOIDED_METRIC} decreased at phase {phase}"
            )
        if (
            previous_completed_transactions is not None
            and completed_transactions_total + 1e-9 < previous_completed_transactions
        ):
            raise ValueError(
                "incoherent proof-window traffic metrics: "
                f"{PROOF_WINDOW_COMPLETED_TRANSACTIONS_METRIC} decreased at phase {phase}"
            )
        if (
            previous_direct_apply_candidates is not None
            and direct_apply_candidates_total + 1e-9 < previous_direct_apply_candidates
        ):
            raise ValueError(
                "incoherent proof-window traffic metrics: "
                f"{PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_METRIC} decreased at phase {phase}"
            )
        metrics_sequence.append(
            {
                "phase": phase,
                "metrics_snapshot_path": str(snapshot_path),
                "totals": totals,
                "proof_window_totals": proof_window_totals,
            }
        )
        previous_direct_apply = direct_apply_total
        previous_avoided = avoided_total
        previous_completed_transactions = completed_transactions_total
        previous_direct_apply_candidates = direct_apply_candidates_total

    start_direct_apply = float(start_totals[READ_AVOIDANCE_DIRECT_APPLY_METRIC] or 0.0)
    end_direct_apply = float(end_totals[READ_AVOIDANCE_DIRECT_APPLY_METRIC] or 0.0)
    start_avoided = float(start_totals[READ_AVOIDANCE_ACTIVE_AVOIDED_METRIC] or 0.0)
    end_avoided = float(end_totals[READ_AVOIDANCE_ACTIVE_AVOIDED_METRIC] or 0.0)
    start_completed_transactions = float(start_proof_window_totals[PROOF_WINDOW_COMPLETED_TRANSACTIONS_METRIC])
    end_completed_transactions = float(end_proof_window_totals[PROOF_WINDOW_COMPLETED_TRANSACTIONS_METRIC])
    start_direct_apply_candidates = float(
        start_proof_window_totals[PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_METRIC]
    )
    end_direct_apply_candidates = float(
        end_proof_window_totals[PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_METRIC]
    )

    delta_direct_apply = end_direct_apply - start_direct_apply
    delta_avoided = end_avoided - start_avoided
    delta_completed_transactions = end_completed_transactions - start_completed_transactions
    delta_direct_apply_candidates = end_direct_apply_candidates - start_direct_apply_candidates
    if delta_direct_apply < -1e-9:
        raise ValueError(
            "incoherent read-avoidance metrics: "
            f"{READ_AVOIDANCE_DIRECT_APPLY_METRIC} decreased across proof window"
        )
    if delta_avoided < -1e-9:
        raise ValueError(
            "incoherent read-avoidance metrics: "
            f"{READ_AVOIDANCE_ACTIVE_AVOIDED_METRIC} decreased across proof window"
        )
    if delta_avoided + 1e-9 < delta_direct_apply:
        raise ValueError(
            "incoherent read-avoidance metrics: "
            f"delta {READ_AVOIDANCE_ACTIVE_AVOIDED_METRIC}={delta_avoided} "
            f"< delta {READ_AVOIDANCE_DIRECT_APPLY_METRIC}={delta_direct_apply}"
        )
    if delta_completed_transactions < -1e-9:
        raise ValueError(
            "incoherent proof-window traffic metrics: "
            f"{PROOF_WINDOW_COMPLETED_TRANSACTIONS_METRIC} decreased across proof window"
        )
    if delta_direct_apply_candidates < -1e-9:
        raise ValueError(
            "incoherent proof-window traffic metrics: "
            f"{PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_METRIC} decreased across proof window"
        )

    saved_start = start_totals[READ_AVOIDANCE_SAVED_SECONDS_METRIC]
    saved_end = end_totals[READ_AVOIDANCE_SAVED_SECONDS_METRIC]
    saved_delta: float | None = None
    if saved_start is not None and saved_end is not None:
        saved_delta = float(saved_end) - float(saved_start)

    proof_window_thresholds = {
        PROOF_WINDOW_COMPLETED_TRANSACTIONS_METRIC: {
            "observed_delta": delta_completed_transactions,
            "minimum_delta": PROOF_WINDOW_COMPLETED_TRANSACTIONS_MIN_DELTA,
            "ok": delta_completed_transactions + 1e-9 >= PROOF_WINDOW_COMPLETED_TRANSACTIONS_MIN_DELTA,
        },
        PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_METRIC: {
            "observed_delta": delta_direct_apply_candidates,
            "minimum_delta": PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_MIN_DELTA,
            "ok": delta_direct_apply_candidates + 1e-9 >= PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_MIN_DELTA,
        },
    }
    proof_window_traffic_minimums = {
        "start_totals": start_proof_window_totals,
        "end_totals": end_proof_window_totals,
        "delta_totals": {
            PROOF_WINDOW_COMPLETED_TRANSACTIONS_METRIC: delta_completed_transactions,
            PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_METRIC: delta_direct_apply_candidates,
        },
        "thresholds": proof_window_thresholds,
        "ok": all(bool(item["ok"]) for item in proof_window_thresholds.values()),
    }

    return {
        "schema": READ_AVOIDANCE_ACCOUNTING_SCHEMA,
        "captured_at": utc_now(),
        "source": "proof_artifact_metrics",
        "claim_scope": "bounded_proof_window_lower_bound_activity",
        "evidence": {
            "start_metrics_path": str(start_metrics_path),
            "end_metrics_path": str(end_metrics_path),
            "metrics_sequence": metrics_sequence,
        },
        "start_totals": start_totals,
        "end_totals": end_totals,
        "delta_totals": {
            READ_AVOIDANCE_DIRECT_APPLY_METRIC: delta_direct_apply,
            READ_AVOIDANCE_ACTIVE_AVOIDED_METRIC: delta_avoided,
        },
        "active_read_saved_seconds": {
            "start_total": saved_start,
            "end_total": saved_end,
            "delta_total": saved_delta,
        },
        "coherence": {
            "counter_monotonic": True,
            "active_reads_avoided_gte_direct_apply_delta": True,
        },
        "proof_window_traffic_minimums": proof_window_traffic_minimums,
    }


def build_window_feature_flag_consistency_for_phases(
    proof_dir: pathlib.Path,
    phases: Iterable[str],
) -> Dict[str, Any]:
    ordered_phases: List[str] = []
    seen_phases = set()
    for raw_phase in phases:
        phase = str(raw_phase).strip().lower()
        if phase == "" or phase in seen_phases:
            continue
        if phase in ("start", "end") or is_interval_phase(phase):
            ordered_phases.append(phase)
            seen_phases.add(phase)
    if "start" not in seen_phases:
        ordered_phases.insert(0, "start")
    if "end" not in seen_phases:
        ordered_phases.append("end")

    snapshots: List[Dict[str, Any]] = []
    previous_key: str | None = None
    previous_phase: str | None = None
    for phase in ordered_phases:
        snapshot_path = proof_phase_feature_flag_snapshot_path(proof_dir, phase)
        snapshot = load_feature_flag_snapshot(snapshot_path, phase)
        canonical_key = str(snapshot["canonical_feature_flags_key"])
        if previous_key is not None and canonical_key != previous_key:
            previous_state = snapshots[-1]["canonical_feature_flags"]
            current_state = snapshot["canonical_feature_flags"]
            drift_field = None
            for field in FEATURE_FLAG_FIELDS:
                if previous_state[field] != current_state[field]:
                    drift_field = field
                    break
            raise ValueError(
                "feature flag drift detected across proof window: "
                f"{drift_field or 'unknown field'} changed at phase {phase} "
                f"(previous phase {previous_phase})"
            )
        snapshots.append(snapshot)
        previous_key = canonical_key
        previous_phase = phase

    return {
        "schema": FEATURE_FLAG_CONSISTENCY_SCHEMA,
        "captured_at": utc_now(),
        "source": "proof_artifact_feature_flags",
        "claim_scope": "bounded_proof_window_feature_flag_consistency",
        "evidence": {
            "feature_flag_snapshot_paths": [item["feature_flags_snapshot_path"] for item in snapshots],
            "phases": [item["phase"] for item in snapshots],
        },
        "snapshots": snapshots,
        "ok": True,
    }


def load_structured_snapshot_bundle(proof_dir: pathlib.Path, snapshot_prefix: str) -> Dict[str, Any]:
    normalized = snapshot_prefix.strip().lower()
    if normalized == "":
        raise ValueError("unsupported empty structured snapshot prefix")

    if normalized in ("start", "end") or is_interval_phase(normalized):
        metrics_path = proof_phase_metrics_snapshot_path(proof_dir, normalized)
        bus_path = proof_phase_bus_observability_snapshot_path(proof_dir, normalized)
        graphql_path = proof_phase_graphql_bus_watch_snapshot_path(proof_dir, normalized)
        feature_flag_path = proof_phase_feature_flag_snapshot_path(proof_dir, normalized)
    else:
        paths = structured_snapshot_paths(proof_dir, normalized)
        metrics_path = paths["metrics"]
        bus_path = paths["bus_observability"]
        graphql_path = paths["graphql_bus_watch"]
        feature_flag_path = paths["feature_flags"]
    for snapshot_path, label in (
        (metrics_path, "metrics"),
        (bus_path, "bus observability"),
        (graphql_path, "graphql bus watch"),
        (feature_flag_path, "feature flags"),
    ):
        if not snapshot_path.exists():
            raise ValueError(f"missing required warmup proof artifact ({label}): {snapshot_path}")

    bus_payload = load_json(bus_path)
    if not isinstance(bus_payload, dict):
        raise ValueError(f"{bus_path}: bus observability snapshot must be a JSON object")
    summary = bus_payload.get("summary")
    if not isinstance(summary, dict):
        raise ValueError(f"{bus_path}: bus observability snapshot missing summary object")
    status = summary.get("status")
    if not isinstance(status, dict):
        raise ValueError(f"{bus_path}: bus observability snapshot missing summary.status object")
    startup = status.get("startup")
    if not isinstance(startup, dict):
        raise ValueError(f"{bus_path}: bus observability snapshot missing summary.status.startup object")
    startup_phase = str(startup.get("phase", "")).strip().upper()
    if startup_phase == "":
        raise ValueError(f"{bus_path}: bus observability snapshot missing summary.status.startup.phase")
    summary_last_updated_at = normalize_timestamp_value(
        summary.get("last_updated_at"),
        bus_path,
        "bus observability",
        "summary.last_updated_at",
    )
    status_last_updated_at = normalize_timestamp_value(
        status.get("last_updated_at"),
        bus_path,
        "bus observability",
        "summary.status.last_updated_at",
    )
    startup_last_updated_at = normalize_timestamp_value(
        startup.get("last_updated_at"),
        bus_path,
        "bus observability",
        "summary.status.startup.last_updated_at",
    )
    bus_feature_flags = status.get("feature_flags")
    if not isinstance(bus_feature_flags, dict):
        raise ValueError(f"{bus_path}: bus observability snapshot missing summary.status.feature_flags object")
    bus_feature_flags_last_updated_at = normalize_timestamp_value(
        bus_feature_flags.get("last_updated_at"),
        bus_path,
        "bus observability",
        "summary.status.feature_flags.last_updated_at",
    )

    graphql_payload = load_json(graphql_path)
    if not isinstance(graphql_payload, dict):
        raise ValueError(f"{graphql_path}: graphql bus watch snapshot must be a JSON object")
    data = graphql_payload.get("data")
    if not isinstance(data, dict):
        raise ValueError(f"{graphql_path}: graphql bus watch snapshot missing data object")
    bus_summary = data.get("busSummary")
    if not isinstance(bus_summary, dict):
        raise ValueError(f"{graphql_path}: graphql bus watch snapshot missing data.busSummary object")
    graphql_status = bus_summary.get("status")
    if not isinstance(graphql_status, dict):
        raise ValueError(f"{graphql_path}: graphql bus watch snapshot missing data.busSummary.status object")
    graphql_summary_last_updated_at = normalize_timestamp_value(
        bus_summary.get("lastUpdatedAt"),
        graphql_path,
        "graphql",
        "data.busSummary.lastUpdatedAt",
    )
    graphql_status_last_updated_at = normalize_timestamp_value(
        graphql_status.get("lastUpdatedAt"),
        graphql_path,
        "graphql",
        "data.busSummary.status.lastUpdatedAt",
    )
    graphql_startup = graphql_status.get("startup")
    if not isinstance(graphql_startup, dict):
        raise ValueError(f"{graphql_path}: graphql bus watch snapshot missing data.busSummary.status.startup object")
    graphql_startup_last_updated_at = normalize_timestamp_value(
        graphql_startup.get("lastUpdatedAt"),
        graphql_path,
        "graphql",
        "data.busSummary.status.startup.lastUpdatedAt",
    )
    graphql_feature_flags = graphql_status.get("featureFlags")
    if not isinstance(graphql_feature_flags, dict):
        raise ValueError(f"{graphql_path}: graphql bus watch snapshot missing data.busSummary.status.featureFlags object")
    graphql_feature_flags_last_updated_at = normalize_timestamp_value(
        graphql_feature_flags.get("lastUpdatedAt"),
        graphql_path,
        "graphql",
        "data.busSummary.status.featureFlags.lastUpdatedAt",
    )
    watch_summary = data.get("watchSummary")
    if not isinstance(watch_summary, dict):
        raise ValueError(f"{graphql_path}: graphql bus watch snapshot missing data.watchSummary object")
    watch_summary_last_updated_at = normalize_timestamp_value(
        watch_summary.get("lastUpdatedAt"),
        graphql_path,
        "graphql",
        "data.watchSummary.lastUpdatedAt",
    )
    bus_publisher_cadence_sec, bus_publisher_cadence_error = required_finite_numeric_value(
        graphql_status,
        "publisherCadenceSec",
        path=graphql_path,
        context="graphql data.busSummary.status",
    )
    if bus_publisher_cadence_error:
        raise ValueError(bus_publisher_cadence_error)
    bus_publisher_cadence_source = str(graphql_status.get("publisherCadenceSource", "")).strip()
    if bus_publisher_cadence_source == "":
        raise ValueError(
            f"{graphql_path}: graphql bus watch snapshot missing data.busSummary.status.publisherCadenceSource"
        )
    bus_observability_status = summary.get("status")
    if not isinstance(bus_observability_status, dict):
        raise ValueError(f"{bus_path}: bus observability snapshot missing summary.status object")
    bus_status_publisher_cadence_sec, bus_status_publisher_cadence_error = required_finite_numeric_value(
        bus_observability_status,
        "publisher_cadence_sec",
        path=bus_path,
        context="bus observability summary.status",
    )
    if bus_status_publisher_cadence_error:
        raise ValueError(bus_status_publisher_cadence_error)
    bus_status_publisher_cadence_source = str(
        bus_observability_status.get("publisher_cadence_source", "")
    ).strip()
    if bus_status_publisher_cadence_source == "":
        raise ValueError(
            f"{bus_path}: bus observability snapshot missing summary.status.publisher_cadence_source"
        )
    if abs(bus_publisher_cadence_sec - bus_status_publisher_cadence_sec) > 1e-9:
        raise ValueError(
            f"{graphql_path}: publisher cadence mismatch between graphql and bus observability snapshots: "
            f"graphql={bus_publisher_cadence_sec} bus={bus_status_publisher_cadence_sec}"
        )
    if bus_publisher_cadence_source != bus_status_publisher_cadence_source:
        raise ValueError(
            f"{graphql_path}: publisher cadence source mismatch between graphql and bus observability snapshots: "
            f"graphql={bus_publisher_cadence_source!r} bus={bus_status_publisher_cadence_source!r}"
        )
    if bus_publisher_cadence_source != PUBLISHER_CADENCE_SOURCE_ANCHOR:
        raise ValueError(
            f"{graphql_path}: publisher cadence source anchor mismatch: "
            f"got {bus_publisher_cadence_source!r}; want {PUBLISHER_CADENCE_SOURCE_ANCHOR!r}"
        )
    transport_class_raw = graphql_status.get("transportClass")
    if not isinstance(transport_class_raw, str):
        raise ValueError(
            f"{graphql_path}: graphql bus watch snapshot missing data.busSummary.status.transportClass"
        )
    transport_class = transport_class_raw.strip()
    if transport_class == "":
        raise ValueError(
            f"{graphql_path}: graphql bus watch snapshot missing data.busSummary.status.transportClass"
        )
    warmup = graphql_status.get("warmup")
    if not isinstance(warmup, dict):
        raise ValueError(f"{graphql_path}: graphql bus watch snapshot missing data.busSummary.status.warmup object")
    warmup_state = str(warmup.get("state", "")).strip().lower()
    if warmup_state == "":
        raise ValueError(f"{graphql_path}: graphql bus watch snapshot missing data.busSummary.status.warmup.state")

    feature_flag_snapshot = load_feature_flag_snapshot(feature_flag_path, normalized)

    return {
        "snapshot_prefix": normalized,
        "snapshot_paths": {
            "metrics": str(metrics_path),
            "bus_observability": str(bus_path),
            "graphql_bus_watch": str(graphql_path),
            "feature_flags": str(feature_flag_path),
        },
        "bus_observability": bus_payload,
        "graphql_bus_watch": graphql_payload,
        "feature_flag_snapshot": feature_flag_snapshot,
        "startup_phase": startup_phase,
        "warmup_state": warmup_state,
        "transport_class": transport_class,
        "publisher_cadence": {
            "publisher_cadence_sec": bus_status_publisher_cadence_sec,
            "publisher_cadence_source": bus_status_publisher_cadence_source,
            "graphql_publisher_cadence_sec": bus_publisher_cadence_sec,
            "graphql_publisher_cadence_source": bus_publisher_cadence_source,
        },
        "timestamps": {
            "bus_observability": {
                "summary_last_updated_at": summary_last_updated_at,
                "status_last_updated_at": status_last_updated_at,
                "startup_last_updated_at": startup_last_updated_at,
                "feature_flags_last_updated_at": bus_feature_flags_last_updated_at,
            },
            "graphql_bus_watch": {
                "summary_last_updated_at": graphql_summary_last_updated_at,
                "status_last_updated_at": graphql_status_last_updated_at,
                "startup_last_updated_at": graphql_startup_last_updated_at,
                "feature_flags_last_updated_at": graphql_feature_flags_last_updated_at,
                "watch_summary_last_updated_at": watch_summary_last_updated_at,
            },
        },
    }


def load_structured_warmup_snapshot(proof_dir: pathlib.Path, phase: str) -> Dict[str, Any]:
    normalized = phase.strip().lower()
    if normalized == "":
        raise ValueError("unsupported empty canary phase for structured warmup snapshot lookup")
    return load_structured_snapshot_bundle(proof_dir, normalized)


def required_finite_numeric_value(
    payload: Dict[str, Any],
    field_name: str,
    *,
    path: pathlib.Path,
    context: str,
) -> Tuple[float | None, str]:
    raw = payload.get(field_name)
    if not isinstance(raw, (int, float)):
        return None, f"{path}: {context} missing numeric {field_name}"
    value = float(raw)
    if not math.isfinite(value):
        return None, f"{path}: {context} has non-finite {field_name}"
    return value, ""


def canonicalize_artifact_for_anchor_compare(payload: Any) -> Any:
    if isinstance(payload, dict):
        return {
            key: canonicalize_artifact_for_anchor_compare(value)
            for key, value in payload.items()
            if key != "captured_at"
        }
    if isinstance(payload, list):
        return [canonicalize_artifact_for_anchor_compare(value) for value in payload]
    return payload


def resolve_anchor_artifact_path(raw_path: str, *, base_dir: pathlib.Path) -> pathlib.Path:
    normalized = raw_path.strip()
    candidate = pathlib.Path(normalized)
    if not candidate.is_absolute():
        candidate = base_dir / candidate
    return candidate.resolve()


def validate_family_upstream_canary_verdict(
    payload: Any,
    path: pathlib.Path,
    *,
    summary_payload: Any | None = None,
    summary_path: pathlib.Path | None = None,
) -> Tuple[bool, str]:
    if not isinstance(payload, dict):
        return False, f"{path}: canary verdict must be a JSON object"
    if str(payload.get("schema", "")).strip() != CANARY_VERDICT_SCHEMA:
        return False, f"{path}: canary verdict schema mismatch"
    if not isinstance(payload.get("ok"), bool):
        return False, f"{path}: canary verdict missing boolean ok"
    status = str(payload.get("status", "")).strip().lower()
    if status not in ("pass", "fail"):
        return False, f"{path}: canary verdict missing valid status"
    criteria = payload.get("criteria")
    if not isinstance(criteria, dict) or len(criteria) == 0:
        return False, f"{path}: canary verdict missing criteria object"
    required_criteria_keys = (
        "no_mismatches",
        "overall_interval_conclusive_rate",
        "per_canary_interval_conclusive_rate",
        "read_avoidance_accounting",
        "proof_window_traffic_minimums",
        "feature_flag_consistency",
        "warmup_behavior",
    )
    missing_criteria = [name for name in required_criteria_keys if not isinstance(criteria.get(name), dict)]
    if missing_criteria:
        return False, (
            f"{path}: canary verdict missing canonical criteria gates: "
            + ", ".join(missing_criteria)
        )
    criterion_results: List[bool] = []
    for criterion_name, criterion_payload in criteria.items():
        if not isinstance(criterion_payload, dict):
            return False, (
                f"{path}: canary verdict criteria.{criterion_name} must be an object "
                "(success semantics mismatch)"
            )
        criterion_ok = criterion_payload.get("ok")
        if not isinstance(criterion_ok, bool):
            return False, (
                f"{path}: canary verdict criteria.{criterion_name}.ok must be boolean "
                "(success semantics mismatch)"
            )
        criterion_results.append(criterion_ok)

    no_mismatches = criteria.get("no_mismatches")
    if not isinstance(no_mismatches, dict):
        return False, f"{path}: canary verdict missing criteria.no_mismatches object"
    mismatch_count = no_mismatches.get("mismatch_count")
    if not isinstance(mismatch_count, int):
        return False, f"{path}: canary verdict missing integer criteria.no_mismatches.mismatch_count"
    if mismatch_count < 0:
        return False, f"{path}: canary verdict has negative criteria.no_mismatches.mismatch_count"
    no_mismatches_ok = no_mismatches.get("ok")
    if no_mismatches_ok != (mismatch_count == 0):
        return False, (
            f"{path}: canary verdict contradictory no_mismatches accounting: "
            f"criteria.no_mismatches.ok={no_mismatches_ok!r} "
            f"but mismatch_count={mismatch_count}"
        )
    overall_interval = criteria.get("overall_interval_conclusive_rate")
    if not isinstance(overall_interval, dict):
        return False, f"{path}: canary verdict missing criteria.overall_interval_conclusive_rate object"
    overall_waived = overall_interval.get("waived")
    if not isinstance(overall_waived, bool):
        return False, (
            f"{path}: canary verdict missing boolean "
            "criteria.overall_interval_conclusive_rate.waived"
        )
    overall_interval_conclusive = overall_interval.get("interval_conclusive")
    if not isinstance(overall_interval_conclusive, int):
        return False, (
            f"{path}: canary verdict missing integer "
            "criteria.overall_interval_conclusive_rate.interval_conclusive"
        )
    if overall_interval_conclusive < 0:
        return False, (
            f"{path}: canary verdict has negative "
            "criteria.overall_interval_conclusive_rate.interval_conclusive"
        )
    overall_interval_total = overall_interval.get("interval_total")
    if not isinstance(overall_interval_total, int):
        return False, (
            f"{path}: canary verdict missing integer "
            "criteria.overall_interval_conclusive_rate.interval_total"
        )
    if overall_interval_total < 0:
        return False, (
            f"{path}: canary verdict has negative "
            "criteria.overall_interval_conclusive_rate.interval_total"
        )
    overall_interval_rate = overall_interval.get("interval_conclusive_rate")
    if not isinstance(overall_interval_rate, (int, float)):
        return False, (
            f"{path}: canary verdict missing numeric "
            "criteria.overall_interval_conclusive_rate.interval_conclusive_rate"
        )
    overall_interval_rate_value = float(overall_interval_rate)
    if not math.isfinite(overall_interval_rate_value):
        return False, (
            f"{path}: canary verdict has non-finite "
            "criteria.overall_interval_conclusive_rate.interval_conclusive_rate"
        )
    overall_threshold = overall_interval.get("threshold")
    if not isinstance(overall_threshold, (int, float)):
        return False, (
            f"{path}: canary verdict missing numeric "
            "criteria.overall_interval_conclusive_rate.threshold"
        )
    overall_threshold_value = float(overall_threshold)
    if not math.isfinite(overall_threshold_value):
        return False, (
            f"{path}: canary verdict has non-finite "
            "criteria.overall_interval_conclusive_rate.threshold"
        )
    if not math.isclose(overall_threshold_value, OVERALL_INTERVAL_CONCLUSIVE_MIN, rel_tol=0.0, abs_tol=1e-9):
        return False, (
            f"{path}: canary verdict non-canonical "
            "criteria.overall_interval_conclusive_rate.threshold"
        )
    overall_expected_rate = safe_ratio(overall_interval_conclusive, overall_interval_total)
    if not math.isclose(overall_interval_rate_value, overall_expected_rate, rel_tol=0.0, abs_tol=1e-9):
        return False, (
            f"{path}: canary verdict contradictory overall interval accounting: "
            f"interval_conclusive_rate={overall_interval_rate_value!r} "
            f"but counts imply {overall_expected_rate!r}"
        )
    overall_expected_ok = True if overall_waived else overall_expected_rate + 1e-9 >= overall_threshold_value
    if overall_interval.get("ok") != overall_expected_ok:
        return False, (
            f"{path}: canary verdict contradictory overall interval semantics: "
            f"ok={overall_interval.get('ok')!r} but derived_ok={overall_expected_ok!r}"
        )

    per_canary_interval = criteria.get("per_canary_interval_conclusive_rate")
    if not isinstance(per_canary_interval, dict):
        return False, (
            f"{path}: canary verdict missing "
            "criteria.per_canary_interval_conclusive_rate object"
        )
    per_canary_waived = per_canary_interval.get("waived")
    if not isinstance(per_canary_waived, bool):
        return False, (
            f"{path}: canary verdict missing boolean "
            "criteria.per_canary_interval_conclusive_rate.waived"
        )
    per_canary_threshold = per_canary_interval.get("threshold")
    if not isinstance(per_canary_threshold, (int, float)):
        return False, (
            f"{path}: canary verdict missing numeric "
            "criteria.per_canary_interval_conclusive_rate.threshold"
        )
    per_canary_threshold_value = float(per_canary_threshold)
    if not math.isfinite(per_canary_threshold_value):
        return False, (
            f"{path}: canary verdict has non-finite "
            "criteria.per_canary_interval_conclusive_rate.threshold"
        )
    if not math.isclose(per_canary_threshold_value, PER_CANARY_INTERVAL_CONCLUSIVE_MIN, rel_tol=0.0, abs_tol=1e-9):
        return False, (
            f"{path}: canary verdict non-canonical "
            "criteria.per_canary_interval_conclusive_rate.threshold"
        )
    failing_canaries = per_canary_interval.get("failing_canaries")
    if not isinstance(failing_canaries, list):
        return False, (
            f"{path}: canary verdict missing list "
            "criteria.per_canary_interval_conclusive_rate.failing_canaries"
        )
    normalized_failing_canaries: List[str] = []
    for canary_index, canary_name in enumerate(failing_canaries):
        if not isinstance(canary_name, str) or canary_name.strip() == "":
            return False, (
                f"{path}: canary verdict has invalid "
                "criteria.per_canary_interval_conclusive_rate.failing_canaries"
                f"[{canary_index}]"
            )
        normalized_failing_canaries.append(canary_name.strip())
    canaries_evaluated = per_canary_interval.get("canaries_evaluated")
    if not isinstance(canaries_evaluated, int):
        return False, (
            f"{path}: canary verdict missing integer "
            "criteria.per_canary_interval_conclusive_rate.canaries_evaluated"
        )
    if canaries_evaluated < 0:
        return False, (
            f"{path}: canary verdict has negative "
            "criteria.per_canary_interval_conclusive_rate.canaries_evaluated"
        )
    if canaries_evaluated == 0:
        return False, (
            f"{path}: canary verdict missing evaluated canary evidence: "
            "criteria.per_canary_interval_conclusive_rate.canaries_evaluated must be >= 1"
        )

    per_canary = payload.get("per_canary")
    if not isinstance(per_canary, dict):
        return False, f"{path}: canary verdict missing canonical per_canary object"
    if len(per_canary) != canaries_evaluated:
        return False, (
            f"{path}: canary verdict contradictory per-canary accounting: "
            f"canaries_evaluated={canaries_evaluated} "
            f"(per_canary_entries={len(per_canary)})"
        )
    canonical_canary_ids, canonical_canary_ids_error = load_canonical_family_proof_canary_ids()
    if canonical_canary_ids_error != "":
        return False, f"{path}: {canonical_canary_ids_error}"
    canonical_canary_id_set = set(canonical_canary_ids)
    missing_canary_ids = [canary_id for canary_id in canonical_canary_ids if canary_id not in per_canary]
    unexpected_canary_ids = sorted(
        [canary_id for canary_id in per_canary.keys() if canary_id not in canonical_canary_id_set]
    )
    if missing_canary_ids or unexpected_canary_ids:
        return False, (
            f"{path}: canary verdict canonical proof-set canary coverage mismatch: "
            f"missing={missing_canary_ids or []} unexpected={unexpected_canary_ids or []}"
        )
    if canaries_evaluated != len(canonical_canary_ids):
        return False, (
            f"{path}: canary verdict canonical proof-set canary count mismatch: "
            f"canaries_evaluated={canaries_evaluated} "
            f"canonical_canaries={len(canonical_canary_ids)}"
        )
    derived_failing_canaries: List[str] = []
    for canary_id, canary_payload in per_canary.items():
        if not isinstance(canary_id, str) or canary_id.strip() == "":
            return False, f"{path}: canary verdict has invalid per_canary key {canary_id!r}"
        if not isinstance(canary_payload, dict):
            return False, f"{path}: canary verdict per_canary.{canary_id} must be an object"
        interval_conclusive = canary_payload.get("interval_conclusive")
        if not isinstance(interval_conclusive, int):
            return False, f"{path}: canary verdict per_canary.{canary_id}.interval_conclusive must be integer"
        if interval_conclusive < 0:
            return False, f"{path}: canary verdict per_canary.{canary_id}.interval_conclusive is negative"
        interval_total = canary_payload.get("interval_total")
        if not isinstance(interval_total, int):
            return False, f"{path}: canary verdict per_canary.{canary_id}.interval_total must be integer"
        if interval_total < 0:
            return False, f"{path}: canary verdict per_canary.{canary_id}.interval_total is negative"
        interval_rate = canary_payload.get("interval_conclusive_rate")
        if not isinstance(interval_rate, (int, float)):
            return False, (
                f"{path}: canary verdict per_canary.{canary_id}.interval_conclusive_rate "
                "must be numeric"
            )
        interval_rate_value = float(interval_rate)
        if not math.isfinite(interval_rate_value):
            return False, (
                f"{path}: canary verdict per_canary.{canary_id}.interval_conclusive_rate "
                "is non-finite"
            )
        expected_interval_rate = safe_ratio(interval_conclusive, interval_total)
        if not math.isclose(interval_rate_value, expected_interval_rate, rel_tol=0.0, abs_tol=1e-9):
            return False, (
                f"{path}: canary verdict contradictory per-canary interval accounting for "
                f"{canary_id!r}: interval_conclusive_rate={interval_rate_value!r} "
                f"but counts imply {expected_interval_rate!r}"
            )
        canary_threshold = canary_payload.get("threshold")
        if not isinstance(canary_threshold, (int, float)):
            return False, f"{path}: canary verdict per_canary.{canary_id}.threshold must be numeric"
        canary_threshold_value = float(canary_threshold)
        if not math.isfinite(canary_threshold_value):
            return False, f"{path}: canary verdict per_canary.{canary_id}.threshold is non-finite"
        if not math.isclose(canary_threshold_value, PER_CANARY_INTERVAL_CONCLUSIVE_MIN, rel_tol=0.0, abs_tol=1e-9):
            return False, f"{path}: canary verdict per_canary.{canary_id}.threshold is non-canonical"
        canary_ok = canary_payload.get("ok")
        if not isinstance(canary_ok, bool):
            return False, f"{path}: canary verdict per_canary.{canary_id}.ok must be boolean"
        canary_waived = canary_payload.get("waived")
        if not isinstance(canary_waived, bool):
            return False, f"{path}: canary verdict per_canary.{canary_id}.waived must be boolean"
        expected_canary_ok = True if canary_waived else expected_interval_rate + 1e-9 >= canary_threshold_value
        if canary_ok != expected_canary_ok:
            return False, (
                f"{path}: canary verdict contradictory per-canary semantics for {canary_id!r}: "
                f"ok={canary_ok!r} but derived_ok={expected_canary_ok!r}"
            )
        if not canary_ok:
            derived_failing_canaries.append(canary_id.strip())

    if sorted(normalized_failing_canaries) != sorted(derived_failing_canaries):
        return False, (
            f"{path}: canary verdict contradictory failing_canaries accounting: "
            f"listed={sorted(normalized_failing_canaries)!r} "
            f"derived={sorted(derived_failing_canaries)!r}"
        )
    per_canary_expected_ok = len(derived_failing_canaries) == 0
    if per_canary_interval.get("ok") != per_canary_expected_ok:
        return False, (
            f"{path}: canary verdict contradictory per-canary semantics: "
            f"ok={per_canary_interval.get('ok')!r} "
            f"but derived_ok={per_canary_expected_ok!r}"
        )
    read_avoidance_gate = criteria.get("read_avoidance_accounting")
    if not isinstance(read_avoidance_gate, dict):
        return False, f"{path}: canary verdict missing criteria.read_avoidance_accounting object"
    read_avoidance_reason = read_avoidance_gate.get("reason")
    if not isinstance(read_avoidance_reason, str):
        return False, f"{path}: canary verdict missing string criteria.read_avoidance_accounting.reason"
    read_direct_delta, read_direct_delta_error = required_finite_numeric_value(
        read_avoidance_gate,
        "direct_apply_total_delta",
        path=path,
        context="canary verdict criteria.read_avoidance_accounting",
    )
    if read_direct_delta_error:
        return False, read_direct_delta_error
    read_avoided_delta, read_avoided_delta_error = required_finite_numeric_value(
        read_avoidance_gate,
        "active_reads_avoided_total_delta",
        path=path,
        context="canary verdict criteria.read_avoidance_accounting",
    )
    if read_avoided_delta_error:
        return False, read_avoided_delta_error
    assert read_direct_delta is not None
    assert read_avoided_delta is not None
    if read_direct_delta < 0:
        return False, (
            f"{path}: canary verdict criteria.read_avoidance_accounting "
            "has negative direct_apply_total_delta"
        )
    if read_avoided_delta < 0:
        return False, (
            f"{path}: canary verdict criteria.read_avoidance_accounting "
            "has negative active_reads_avoided_total_delta"
        )
    if read_avoided_delta + 1e-9 < read_direct_delta:
        return False, (
            f"{path}: canary verdict contradictory read_avoidance_accounting evidence: "
            f"active_reads_avoided_total_delta={read_avoided_delta!r} "
            f"< direct_apply_total_delta={read_direct_delta!r}"
        )

    proof_window_gate = criteria.get("proof_window_traffic_minimums")
    if not isinstance(proof_window_gate, dict):
        return False, f"{path}: canary verdict missing criteria.proof_window_traffic_minimums object"
    proof_window_reason = proof_window_gate.get("reason")
    if not isinstance(proof_window_reason, str):
        return False, f"{path}: canary verdict missing string criteria.proof_window_traffic_minimums.reason"
    completed_delta, completed_delta_error = required_finite_numeric_value(
        proof_window_gate,
        "completed_transactions_delta",
        path=path,
        context="canary verdict criteria.proof_window_traffic_minimums",
    )
    if completed_delta_error:
        return False, completed_delta_error
    completed_minimum, completed_minimum_error = required_finite_numeric_value(
        proof_window_gate,
        "completed_transactions_minimum",
        path=path,
        context="canary verdict criteria.proof_window_traffic_minimums",
    )
    if completed_minimum_error:
        return False, completed_minimum_error
    candidates_delta, candidates_delta_error = required_finite_numeric_value(
        proof_window_gate,
        "direct_apply_candidates_evaluated_delta",
        path=path,
        context="canary verdict criteria.proof_window_traffic_minimums",
    )
    if candidates_delta_error:
        return False, candidates_delta_error
    candidates_minimum, candidates_minimum_error = required_finite_numeric_value(
        proof_window_gate,
        "direct_apply_candidates_evaluated_minimum",
        path=path,
        context="canary verdict criteria.proof_window_traffic_minimums",
    )
    if candidates_minimum_error:
        return False, candidates_minimum_error
    assert completed_delta is not None
    assert completed_minimum is not None
    assert candidates_delta is not None
    assert candidates_minimum is not None
    if completed_delta < 0:
        return False, (
            f"{path}: canary verdict criteria.proof_window_traffic_minimums "
            "has negative completed_transactions_delta"
        )
    if candidates_delta < 0:
        return False, (
            f"{path}: canary verdict criteria.proof_window_traffic_minimums "
            "has negative direct_apply_candidates_evaluated_delta"
        )
    if not math.isclose(
        completed_minimum,
        PROOF_WINDOW_COMPLETED_TRANSACTIONS_MIN_DELTA,
        rel_tol=0.0,
        abs_tol=1e-9,
    ):
        return False, (
            f"{path}: canary verdict non-canonical "
            "criteria.proof_window_traffic_minimums.completed_transactions_minimum"
        )
    if not math.isclose(
        candidates_minimum,
        PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_MIN_DELTA,
        rel_tol=0.0,
        abs_tol=1e-9,
    ):
        return False, (
            f"{path}: canary verdict non-canonical "
            "criteria.proof_window_traffic_minimums.direct_apply_candidates_evaluated_minimum"
        )
    if proof_window_gate.get("ok") is True:
        if completed_delta + 1e-9 < completed_minimum:
            return False, (
                f"{path}: canary verdict contradictory proof_window_traffic_minimums evidence: "
                f"completed_transactions_delta={completed_delta!r} "
                f"< completed_transactions_minimum={completed_minimum!r}"
            )
        if candidates_delta + 1e-9 < candidates_minimum:
            return False, (
                f"{path}: canary verdict contradictory proof_window_traffic_minimums evidence: "
                f"direct_apply_candidates_evaluated_delta={candidates_delta!r} "
                f"< direct_apply_candidates_evaluated_minimum={candidates_minimum!r}"
            )

    feature_flags_gate = criteria.get("feature_flag_consistency")
    if not isinstance(feature_flags_gate, dict):
        return False, f"{path}: canary verdict missing criteria.feature_flag_consistency object"
    feature_flags_reason = feature_flags_gate.get("reason")
    if not isinstance(feature_flags_reason, str):
        return False, f"{path}: canary verdict missing string criteria.feature_flag_consistency.reason"
    if feature_flags_gate.get("ok") is True:
        snapshot_count = feature_flags_gate.get("snapshot_count")
        if not isinstance(snapshot_count, int):
            return False, (
                f"{path}: canary verdict missing integer "
                "criteria.feature_flag_consistency.snapshot_count"
            )
        if snapshot_count < 1:
            return False, (
                f"{path}: canary verdict missing feature-flag snapshot evidence: "
                "criteria.feature_flag_consistency.snapshot_count must be >= 1"
            )
        phases = feature_flags_gate.get("phases")
        if not isinstance(phases, list):
            return False, f"{path}: canary verdict missing list criteria.feature_flag_consistency.phases"
        normalized_phases: List[str] = []
        for phase_index, phase_name in enumerate(phases):
            if not isinstance(phase_name, str) or phase_name.strip() == "":
                return False, (
                    f"{path}: canary verdict has invalid criteria.feature_flag_consistency.phases"
                    f"[{phase_index}]"
                )
            normalized_phases.append(phase_name.strip().lower())
        if len(normalized_phases) != snapshot_count:
            return False, (
                f"{path}: canary verdict contradictory feature_flag_consistency evidence: "
                f"snapshot_count={snapshot_count} phases={len(normalized_phases)}"
            )
        for required_phase in ("start", "end"):
            if required_phase not in normalized_phases:
                return False, (
                    f"{path}: canary verdict missing feature-flag {required_phase} phase evidence"
                )

    warmup_behavior = criteria.get("warmup_behavior")
    if not isinstance(warmup_behavior, dict):
        return False, f"{path}: canary verdict missing criteria.warmup_behavior object"
    warmup_reason = warmup_behavior.get("reason")
    if not isinstance(warmup_reason, str):
        return False, f"{path}: canary verdict missing string criteria.warmup_behavior.reason"
    warmup_waived = warmup_behavior.get("waived")
    if not isinstance(warmup_waived, bool):
        return False, f"{path}: canary verdict missing boolean criteria.warmup_behavior.waived"
    if warmup_behavior.get("ok") is True and not warmup_waived:
        interval_snapshot_count = warmup_behavior.get("interval_snapshot_count")
        if not isinstance(interval_snapshot_count, int):
            return False, (
                f"{path}: canary verdict missing integer "
                "criteria.warmup_behavior.interval_snapshot_count"
            )
        if interval_snapshot_count < 1:
            return False, (
                f"{path}: canary verdict missing warmup interval evidence: "
                "criteria.warmup_behavior.interval_snapshot_count must be >= 1"
            )
        interval_snapshot_prefixes = warmup_behavior.get("interval_snapshot_prefixes")
        if not isinstance(interval_snapshot_prefixes, list):
            return False, (
                f"{path}: canary verdict missing list "
                "criteria.warmup_behavior.interval_snapshot_prefixes"
            )
        if len(interval_snapshot_prefixes) != interval_snapshot_count:
            return False, (
                f"{path}: canary verdict contradictory warmup interval evidence: "
                f"interval_snapshot_count={interval_snapshot_count} "
                f"interval_snapshot_prefixes={len(interval_snapshot_prefixes)}"
            )
        for prefix_index, prefix_name in enumerate(interval_snapshot_prefixes):
            if not isinstance(prefix_name, str) or prefix_name.strip() == "":
                return False, (
                    f"{path}: canary verdict has invalid criteria.warmup_behavior."
                    f"interval_snapshot_prefixes[{prefix_index}]"
                )
        if str(warmup_behavior.get("cold_start_snapshot_prefix", "")).strip().lower() != "start":
            return False, (
                f"{path}: canary verdict missing criteria.warmup_behavior.cold_start_snapshot_prefix='start'"
            )
        if str(warmup_behavior.get("post_warmup_snapshot_prefix", "")).strip().lower() != "end":
            return False, (
                f"{path}: canary verdict missing criteria.warmup_behavior.post_warmup_snapshot_prefix='end'"
            )
        cold_start_phase = str(warmup_behavior.get("cold_start_startup_phase", "")).strip().upper()
        post_warmup_phase = str(warmup_behavior.get("post_warmup_startup_phase", "")).strip().upper()
        if cold_start_phase == "":
            return False, (
                f"{path}: canary verdict missing non-empty "
                "criteria.warmup_behavior.cold_start_startup_phase"
            )
        if cold_start_phase == "LIVE_READY":
            return False, (
                f"{path}: canary verdict contradictory warmup evidence: "
                "criteria.warmup_behavior.cold_start_startup_phase must be pre-LIVE_READY"
            )
        if post_warmup_phase != "LIVE_READY":
            return False, (
                f"{path}: canary verdict contradictory warmup evidence: "
                "criteria.warmup_behavior.post_warmup_startup_phase must be LIVE_READY"
            )
        cold_start_warmup_state = str(warmup_behavior.get("cold_start_warmup_state", "")).strip().lower()
        if cold_start_warmup_state == "":
            return False, (
                f"{path}: canary verdict missing non-empty "
                "criteria.warmup_behavior.cold_start_warmup_state"
            )
        if cold_start_warmup_state == "available":
            return False, (
                f"{path}: canary verdict contradictory warmup evidence: "
                "criteria.warmup_behavior.cold_start_warmup_state must be pre-available"
            )
        if str(warmup_behavior.get("post_warmup_warmup_state", "")).strip().lower() != "available":
            return False, (
                f"{path}: canary verdict contradictory warmup evidence: "
                "criteria.warmup_behavior.post_warmup_warmup_state must be available"
            )

    derived_ok = all(criterion_results)
    verdict_ok = bool(payload.get("ok"))
    if verdict_ok != derived_ok:
        return False, (
            f"{path}: canary verdict contradictory success semantics: "
            f"ok={verdict_ok!r} but criteria imply ok={derived_ok!r}"
        )
    if (status == "pass") != derived_ok:
        return False, (
            f"{path}: canary verdict contradictory success semantics: "
            f"status={status!r} but criteria imply {'pass' if derived_ok else 'fail'!r}"
        )
    if not isinstance(summary_path, pathlib.Path):
        return False, f"{path}: missing anchored canary summary artifact path"
    if not isinstance(summary_payload, dict):
        return False, f"{path}: missing anchored canary summary artifact: {summary_path}"
    summary_schema = str(summary_payload.get("schema", "")).strip()
    if summary_schema != "p03_canary_overall_summary_v1":
        return False, f"{path}: anchored canary summary schema mismatch at {summary_path}"
    verdict_summary_schema = str(payload.get("summary_schema", "")).strip()
    if verdict_summary_schema != summary_schema:
        return False, (
            f"{path}: canary verdict summary_schema mismatch: "
            f"summary_schema={verdict_summary_schema!r} anchored_summary_schema={summary_schema!r}"
        )
    summary_run_id = str(summary_payload.get("run_id", "")).strip()
    verdict_run_id = str(payload.get("run_id", "")).strip()
    if summary_run_id == "" or verdict_run_id == "" or summary_run_id != verdict_run_id:
        return False, (
            f"{path}: canary verdict run_id mismatch: "
            f"verdict_run_id={verdict_run_id!r} anchored_summary_run_id={summary_run_id!r}"
        )
    try:
        anchored_verdict = build_canary_verdict(summary_payload)
    except Exception as exc:
        return False, (
            f"{path}: unable to derive canary verdict from anchored summary artifact "
            f"{summary_path}: {exc}"
        )
    canonical_payload = canonicalize_artifact_for_anchor_compare(payload)
    canonical_anchored = canonicalize_artifact_for_anchor_compare(anchored_verdict)
    if canonical_payload != canonical_anchored:
        return False, (
            f"{path}: canary verdict does not match anchored canary summary artifact: "
            f"{summary_path}"
        )
    return True, ""


def validate_family_upstream_replay_verdict(
    payload: Any,
    path: pathlib.Path,
    *,
    behavior_artifact_payload: Any | None = None,
    behavior_artifact_path: pathlib.Path | None = None,
) -> Tuple[bool, str]:
    if not isinstance(payload, dict):
        return False, f"{path}: replay falsification verdict must be a JSON object"
    if str(payload.get("schema", "")).strip() != REPLAY_FALSIFICATION_VERDICT_SCHEMA:
        return False, f"{path}: replay falsification verdict schema mismatch"
    if not isinstance(payload.get("ok"), bool):
        return False, f"{path}: replay falsification verdict missing boolean ok"
    status = str(payload.get("status", "")).strip().lower()
    if status not in ("pass", "fail"):
        return False, f"{path}: replay falsification verdict missing valid status"
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        return False, f"{path}: replay falsification verdict missing summary object"
    summary_total_cases = summary.get("total_cases")
    if not isinstance(summary_total_cases, int):
        return False, f"{path}: replay falsification verdict missing summary.total_cases"
    if summary_total_cases < 0:
        return False, f"{path}: replay falsification verdict has negative summary.total_cases"
    if summary_total_cases == 0:
        return False, (
            f"{path}: replay falsification verdict missing evaluated replay evidence: "
            "summary.total_cases must be >= 1"
        )
    summary_locked_cases = summary.get("locked_cases")
    if not isinstance(summary_locked_cases, int):
        return False, f"{path}: replay falsification verdict missing summary.locked_cases"
    if summary_locked_cases < 0:
        return False, f"{path}: replay falsification verdict has negative summary.locked_cases"
    if summary_locked_cases == 0:
        return False, (
            f"{path}: replay falsification verdict missing locked replay cases: "
            "summary.locked_cases must be >= 1"
        )
    summary_pass = summary.get("pass")
    if not isinstance(summary_pass, int):
        return False, f"{path}: replay falsification verdict missing summary.pass"
    if summary_pass < 0:
        return False, f"{path}: replay falsification verdict has negative summary.pass"
    summary_fail = summary.get("fail")
    if not isinstance(summary_fail, int):
        return False, f"{path}: replay falsification verdict missing summary.fail"
    if summary_fail < 0:
        return False, f"{path}: replay falsification verdict has negative summary.fail"
    summary_informational = summary.get("informational")
    if not isinstance(summary_informational, int):
        return False, f"{path}: replay falsification verdict missing summary.informational"
    if summary_informational < 0:
        return False, f"{path}: replay falsification verdict has negative summary.informational"
    summary_behavior_ok = summary.get("behavior_artifact_ok")
    if not isinstance(summary_behavior_ok, bool):
        return False, f"{path}: replay falsification verdict missing boolean summary.behavior_artifact_ok"
    summary_proof_run_ok = summary.get("proof_run_ok")
    if not isinstance(summary_proof_run_ok, bool):
        return False, f"{path}: replay falsification verdict missing boolean summary.proof_run_ok"
    if not isinstance(behavior_artifact_path, pathlib.Path):
        return False, f"{path}: missing anchored replay behavior artifact path"
    if not isinstance(behavior_artifact_payload, dict):
        return False, f"{path}: missing anchored replay behavior artifact: {behavior_artifact_path}"
    behavior_schema = str(behavior_artifact_payload.get("schema", "")).strip()
    if behavior_schema != REPLAY_BEHAVIOR_ARTIFACT_SCHEMA:
        return False, f"{path}: anchored replay behavior artifact schema mismatch at {behavior_artifact_path}"
    anchored_behavior_artifact_ok = behavior_artifact_payload.get("ok")
    if not isinstance(anchored_behavior_artifact_ok, bool):
        return False, (
            f"{path}: anchored replay behavior artifact missing boolean ok at {behavior_artifact_path}"
        )
    behavior_cases_payload = behavior_artifact_payload.get("cases")
    if not isinstance(behavior_cases_payload, list):
        return False, f"{path}: anchored replay behavior artifact missing cases array at {behavior_artifact_path}"
    behavior_cases_by_name: Dict[str, Dict[str, Any]] = {}
    for behavior_case_index, behavior_case_payload in enumerate(behavior_cases_payload):
        if not isinstance(behavior_case_payload, dict):
            return False, (
                f"{path}: anchored replay behavior artifact case[{behavior_case_index}] "
                f"must be an object at {behavior_artifact_path}"
            )
        behavior_case_name = str(behavior_case_payload.get("name", "")).strip()
        if behavior_case_name == "":
            return False, (
                f"{path}: anchored replay behavior artifact case[{behavior_case_index}] "
                f"missing name at {behavior_artifact_path}"
            )
        if behavior_case_name in behavior_cases_by_name:
            return False, (
                f"{path}: anchored replay behavior artifact duplicate case name "
                f"{behavior_case_name!r} at {behavior_artifact_path}"
            )
        behavior_case_observed = behavior_case_payload.get("observed")
        if not isinstance(behavior_case_observed, dict):
            return False, (
                f"{path}: anchored replay behavior artifact case[{behavior_case_index}] "
                f"missing observed object at {behavior_artifact_path}"
            )
        behavior_case_reason = str(behavior_case_payload.get("reason", "")).strip()
        if behavior_case_reason == "":
            return False, (
                f"{path}: anchored replay behavior artifact case[{behavior_case_index}] "
                f"missing reason at {behavior_artifact_path}"
            )
        behavior_cases_by_name[behavior_case_name] = {
            "status": str(behavior_case_payload.get("status", "")).strip().lower() or "observed",
            "reason": behavior_case_reason,
            "observed": behavior_case_observed,
        }
    expected_behavior_artifact_resolved = behavior_artifact_path.resolve()
    cases = payload.get("cases")
    if not isinstance(cases, list):
        return False, f"{path}: replay falsification verdict missing cases array"
    case_pass_count = 0
    case_fail_count = 0
    case_informational_count = 0
    case_locked_count = 0
    case_behavior_artifact_ok_all = True
    case_names: set[str] = set()
    canonical_replay_case_contracts, canonical_replay_case_contracts_error = (
        load_canonical_family_proof_replay_case_contracts()
    )
    if canonical_replay_case_contracts_error != "":
        return False, f"{path}: {canonical_replay_case_contracts_error}"
    for case_index, case_payload in enumerate(cases):
        if not isinstance(case_payload, dict):
            return False, f"{path}: replay falsification verdict case[{case_index}] must be object"
        case_name_raw = case_payload.get("name")
        if not isinstance(case_name_raw, str):
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing non-empty name"
            )
        case_name = case_name_raw.strip()
        if case_name == "":
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing non-empty name"
            )
        if case_name in case_names:
            return False, (
                f"{path}: replay falsification verdict duplicate case name {case_name!r}"
            )
        case_names.add(case_name)
        case_family_raw = case_payload.get("family")
        if not isinstance(case_family_raw, str):
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing non-empty family"
            )
        case_family = case_family_raw.strip()
        if case_family == "":
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing non-empty family"
            )
        response_class_raw = case_payload.get("response_class")
        if not isinstance(response_class_raw, str):
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing non-empty response_class"
            )
        response_class = response_class_raw.strip()
        if response_class == "":
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing non-empty response_class"
            )
        scenario_tags = case_payload.get("scenario_tags")
        if not isinstance(scenario_tags, list):
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing scenario_tags list"
            )
        normalized_scenario_tags: List[str] = []
        for tag_index, tag in enumerate(scenario_tags):
            if not isinstance(tag, str) or tag.strip() == "":
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] has invalid "
                    f"scenario_tags[{tag_index}]"
                )
            normalized_scenario_tags.append(tag.strip())
        if not isinstance(case_payload.get("reason"), str):
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing string reason"
            )
        case_status = str(case_payload.get("status", "")).strip().lower()
        if case_status not in ("pass", "fail", "informational"):
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing valid status"
            )
        expected = case_payload.get("expected")
        if not isinstance(expected, dict):
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing expected object"
            )
        expected_direct_apply = expected.get("direct_apply")
        if not isinstance(expected_direct_apply, bool):
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing boolean expected.direct_apply"
            )
        expected_disposition = str(expected.get("disposition", "")).strip().lower()
        if expected_disposition not in REPLAY_EXPECTED_DISPOSITIONS:
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing valid expected.disposition"
            )
        expected_reason_raw = expected.get("reason")
        if not isinstance(expected_reason_raw, str):
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing string expected.reason"
            )
        expected_reason = expected_reason_raw.strip()
        canonical_case_contract = canonical_replay_case_contracts.get(case_name)
        if isinstance(canonical_case_contract, dict):
            canonical_case_family = str(canonical_case_contract.get("family", "")).strip()
            if case_family != canonical_case_family:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "family mismatches canonical replay corpus case contract"
                )
            canonical_response_class = str(canonical_case_contract.get("response_class", "")).strip()
            if response_class != canonical_response_class:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "response_class mismatches canonical replay corpus case contract"
                )
            canonical_scenario_tags_raw = canonical_case_contract.get("scenario_tags")
            canonical_scenario_tags = (
                list(canonical_scenario_tags_raw) if isinstance(canonical_scenario_tags_raw, list) else None
            )
            if canonical_scenario_tags is None or normalized_scenario_tags != canonical_scenario_tags:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "scenario_tags mismatch canonical replay corpus case contract"
                )
            canonical_expected_reason = str(canonical_case_contract.get("expected_reason", "")).strip()
            if expected_reason != canonical_expected_reason:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "expected.reason mismatches canonical replay corpus case contract"
                )
            canonical_expected_direct_apply = canonical_case_contract.get("expected_direct_apply")
            if not isinstance(canonical_expected_direct_apply, bool):
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "canonical replay corpus case contract missing expected.direct_apply"
                )
            if expected_direct_apply != canonical_expected_direct_apply:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "expected.direct_apply mismatches canonical replay corpus case contract"
                )
            canonical_expected_disposition = str(
                canonical_case_contract.get("expected_disposition", "")
            ).strip().lower()
            if canonical_expected_disposition not in REPLAY_EXPECTED_DISPOSITIONS:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "canonical replay corpus case contract missing expected.disposition"
                )
            if expected_disposition != canonical_expected_disposition:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "expected.disposition mismatches canonical replay corpus case contract"
                )
        behavior_evidence = case_payload.get("behavior_evidence")
        if not isinstance(behavior_evidence, dict):
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing behavior_evidence object"
            )
        behavior_artifact_path_raw = behavior_evidence.get("behavior_artifact_path")
        if not isinstance(behavior_artifact_path_raw, str):
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing behavior_evidence.behavior_artifact_path"
            )
        behavior_artifact_path_text = behavior_artifact_path_raw.strip()
        if behavior_artifact_path_text == "":
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing behavior_evidence.behavior_artifact_path"
            )
        resolved_behavior_artifact_path = resolve_anchor_artifact_path(
            behavior_artifact_path_text,
            base_dir=path.parent,
        )
        if resolved_behavior_artifact_path != expected_behavior_artifact_resolved:
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] "
                "behavior_evidence.behavior_artifact_path mismatches anchored replay_behavior artifact"
            )
        if not resolved_behavior_artifact_path.exists():
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] "
                "behavior_evidence.behavior_artifact_path does not exist"
            )
        if not isinstance(behavior_evidence.get("behavior_artifact_ok"), bool):
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing boolean "
                "behavior_evidence.behavior_artifact_ok"
            )
        if behavior_evidence.get("behavior_artifact_ok") != anchored_behavior_artifact_ok:
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] "
                "behavior_evidence.behavior_artifact_ok mismatches anchored replay behavior artifact"
            )
        if str(behavior_evidence.get("behavior_schema", "")).strip() != REPLAY_BEHAVIOR_ARTIFACT_SCHEMA:
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] "
                "behavior_evidence.behavior_schema mismatch"
            )
        behavior_case_name_raw = behavior_evidence.get("case_name")
        if not isinstance(behavior_case_name_raw, str) or behavior_case_name_raw.strip() == "":
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing behavior_evidence.case_name"
            )
        if behavior_case_name_raw.strip() != case_name:
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] "
                "behavior_evidence.case_name mismatch"
            )
        behavior_case_anchor = behavior_cases_by_name.get(case_name)
        if not isinstance(behavior_case_anchor, dict):
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing anchored behavior "
                f"case {case_name!r}"
            )
        behavior_observed_present = behavior_evidence.get("observed_present")
        if not isinstance(behavior_observed_present, bool):
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing boolean "
                "behavior_evidence.observed_present"
            )
        behavior_observed_status_raw = behavior_evidence.get("observed_status")
        if not isinstance(behavior_observed_status_raw, str) or behavior_observed_status_raw.strip() == "":
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing behavior_evidence.observed_status"
            )
        behavior_observed_status = behavior_observed_status_raw.strip().lower()
        behavior_observed_reason_raw = behavior_evidence.get("observed_reason")
        if not isinstance(behavior_observed_reason_raw, str) or behavior_observed_reason_raw.strip() == "":
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing behavior_evidence.observed_reason"
            )
        behavior_observed_reason = behavior_observed_reason_raw.strip()
        case_reason = case_payload.get("reason", "").strip()
        if case_reason == "":
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] missing non-empty reason"
            )
        if case_status in ("pass", "informational") and case_reason != behavior_observed_reason:
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] "
                "reason mismatches behavior_evidence.observed_reason"
            )
        if behavior_observed_status != str(behavior_case_anchor.get("status", "")).strip().lower():
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] "
                "behavior_evidence.observed_status mismatches anchored replay behavior artifact"
            )
        if behavior_observed_reason != str(behavior_case_anchor.get("reason", "")).strip():
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] "
                "behavior_evidence.observed_reason mismatches anchored replay behavior artifact"
            )
        case_behavior_artifact_ok = behavior_evidence.get("behavior_artifact_ok")
        assert isinstance(case_behavior_artifact_ok, bool)
        if not case_behavior_artifact_ok:
            case_behavior_artifact_ok_all = False
        case_direct_apply = case_payload.get("direct_apply")
        if case_direct_apply is not None and not isinstance(case_direct_apply, bool):
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] direct_apply must be boolean or null"
            )
        case_disposition_raw = case_payload.get("disposition")
        case_disposition = ""
        if case_disposition_raw is not None:
            if not isinstance(case_disposition_raw, str):
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] disposition must be string or null"
                )
            case_disposition = case_disposition_raw.strip().lower()
            if case_disposition == "":
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] disposition must be non-empty when present"
                )
            if case_disposition not in REPLAY_EXPECTED_DISPOSITIONS:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] has unsupported disposition"
                )
        observed = case_payload.get("observed")
        if observed is not None and not isinstance(observed, dict):
            return False, (
                f"{path}: replay falsification verdict case[{case_index}] observed must be object or null"
            )
        if observed is None:
            if behavior_observed_present:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "behavior_evidence.observed_present=true without observed evidence"
                )
            if behavior_case_anchor.get("observed") is not None:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "missing observed evidence from anchored replay behavior artifact"
                )
            if behavior_observed_status != "missing":
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "behavior_evidence.observed_status must be 'missing' when observed is null"
                )
            if behavior_evidence.get("observed_direct_apply") is not None:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "behavior_evidence.observed_direct_apply must be null when observed is null"
                )
            if behavior_evidence.get("observed_disposition") is not None:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "behavior_evidence.observed_disposition must be null when observed is null"
                )
            if case_direct_apply is not None or case_disposition_raw is not None:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] has replay semantics without observed evidence"
                )
            if case_status == "pass":
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] pass case missing observed evidence"
                )
        else:
            if not behavior_observed_present:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "behavior_evidence.observed_present=false with observed evidence"
                )
            if behavior_observed_status == "missing":
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "behavior_evidence.observed_status='missing' with observed evidence"
                )
            anchored_observed = behavior_case_anchor.get("observed")
            if not isinstance(anchored_observed, dict):
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "anchored replay behavior observation is missing or malformed"
                )
            if canonicalize_json_value(observed) != canonicalize_json_value(anchored_observed):
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "observed evidence mismatches anchored replay behavior artifact"
                )
            observed_direct_apply = observed.get("direct_apply")
            if not isinstance(observed_direct_apply, bool):
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] observed.direct_apply must be boolean"
                )
            observed_disposition = str(observed.get("disposition", "")).strip().lower()
            if observed_disposition not in REPLAY_EXPECTED_DISPOSITIONS:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] observed.disposition must be valid"
                )
            behavior_observed_direct_apply = behavior_evidence.get("observed_direct_apply")
            if not isinstance(behavior_observed_direct_apply, bool):
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] missing boolean "
                    "behavior_evidence.observed_direct_apply"
                )
            if behavior_observed_direct_apply != observed_direct_apply:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "behavior_evidence.observed_direct_apply mismatches observed.direct_apply"
                )
            behavior_observed_disposition_raw = behavior_evidence.get("observed_disposition")
            if not isinstance(behavior_observed_disposition_raw, str):
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] missing string "
                    "behavior_evidence.observed_disposition"
                )
            behavior_observed_disposition = behavior_observed_disposition_raw.strip().lower()
            if behavior_observed_disposition not in REPLAY_EXPECTED_DISPOSITIONS:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "behavior_evidence.observed_disposition must be valid"
                )
            if behavior_observed_disposition != observed_disposition:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "behavior_evidence.observed_disposition mismatches observed.disposition"
                )
            if case_direct_apply is None or case_direct_apply != observed_direct_apply:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "direct_apply mismatches observed.direct_apply"
                )
            if case_disposition == "" or case_disposition != observed_disposition:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] "
                    "disposition mismatches observed.disposition"
                )
        if case_status == "pass":
            if case_direct_apply is None or case_disposition == "":
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] pass case missing replay semantics"
                )
            if case_direct_apply != expected_direct_apply:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] pass case "
                    "direct_apply mismatches expected.direct_apply"
                )
            if case_disposition != expected_disposition:
                return False, (
                    f"{path}: replay falsification verdict case[{case_index}] pass case "
                    "disposition mismatches expected.disposition"
                )
        if case_status == "fail":
            case_fail_count += 1
            case_locked_count += 1
        elif case_status == "pass":
            case_pass_count += 1
            case_locked_count += 1
        else:
            case_informational_count += 1

    canonical_replay_case_names = tuple(sorted(canonical_replay_case_contracts.keys()))
    canonical_replay_case_name_set = set(canonical_replay_case_names)
    missing_replay_case_names = [name for name in canonical_replay_case_names if name not in case_names]
    unexpected_replay_case_names = sorted(
        [name for name in case_names if name not in canonical_replay_case_name_set]
    )
    if missing_replay_case_names or unexpected_replay_case_names:
        return False, (
            f"{path}: replay falsification verdict canonical proof-set case coverage mismatch: "
            f"missing={missing_replay_case_names or []} unexpected={unexpected_replay_case_names or []}"
        )
    canonical_locked_case_total = len(canonical_replay_case_names)
    if summary_total_cases != canonical_locked_case_total:
        return False, (
            f"{path}: replay falsification verdict canonical proof-set case count mismatch: "
            f"summary.total_cases={summary_total_cases} "
            f"canonical_locked_cases={canonical_locked_case_total}"
        )
    if summary_locked_cases != canonical_locked_case_total:
        return False, (
            f"{path}: replay falsification verdict canonical proof-set locked case count mismatch: "
            f"summary.locked_cases={summary_locked_cases} "
            f"canonical_locked_cases={canonical_locked_case_total}"
        )

    if summary_total_cases != len(cases):
        return False, (
            f"{path}: replay falsification verdict contradictory summary.total_cases="
            f"{summary_total_cases} (cases={len(cases)})"
        )
    if summary_locked_cases > summary_total_cases:
        return False, (
            f"{path}: replay falsification verdict contradictory summary.locked_cases="
            f"{summary_locked_cases} (total_cases={summary_total_cases})"
        )
    if summary_locked_cases != case_locked_count:
        return False, (
            f"{path}: replay falsification verdict contradictory summary.locked_cases="
            f"{summary_locked_cases} (case_locked_count={case_locked_count})"
        )
    if summary_informational != case_informational_count:
        return False, (
            f"{path}: replay falsification verdict contradictory summary.informational="
            f"{summary_informational} (case_informational_count={case_informational_count})"
        )
    if summary_locked_cases + summary_informational != summary_total_cases:
        return False, (
            f"{path}: replay falsification verdict contradictory summary lock accounting: "
            f"locked_cases={summary_locked_cases} informational={summary_informational} "
            f"total_cases={summary_total_cases}"
        )
    if summary_pass + summary_fail != summary_locked_cases:
        return False, (
            f"{path}: replay falsification verdict contradictory summary pass/fail lock accounting: "
            f"pass={summary_pass} fail={summary_fail} locked_cases={summary_locked_cases}"
        )
    if summary_pass + summary_fail > summary_total_cases:
        return False, (
            f"{path}: replay falsification verdict contradictory summary pass/fail totals: "
            f"pass={summary_pass} fail={summary_fail} total_cases={summary_total_cases}"
        )
    if summary_pass != case_pass_count:
        return False, (
            f"{path}: replay falsification verdict contradictory summary.pass={summary_pass} "
            f"(case_pass_count={case_pass_count})"
        )
    if summary_fail != case_fail_count:
        return False, (
            f"{path}: replay falsification verdict contradictory summary.fail={summary_fail} "
            f"(case_fail_count={case_fail_count})"
        )
    if summary_behavior_ok != case_behavior_artifact_ok_all:
        return False, (
            f"{path}: replay falsification verdict contradictory summary.behavior_artifact_ok="
            f"{summary_behavior_ok!r} (derived_behavior_artifact_ok={case_behavior_artifact_ok_all!r})"
        )

    verdict_ok = bool(payload.get("ok"))
    summary_success = summary_fail == 0
    if verdict_ok != summary_success:
        return False, (
            f"{path}: replay falsification verdict contradictory success semantics: "
            f"ok={verdict_ok!r} but summary.fail={summary_fail}"
        )
    if (status == "pass") != summary_success:
        return False, (
            f"{path}: replay falsification verdict contradictory success semantics: "
            f"status={status!r} but summary.fail={summary_fail}"
        )
    if summary_success and (not summary_behavior_ok or not summary_proof_run_ok):
        return False, (
            f"{path}: replay falsification verdict contradictory success semantics: "
            "summary indicates success but behavior/proof_run flags are false"
        )
    return True, ""


def build_family_proof_eligibility_artifact_for_run(
    proof_dir: pathlib.Path,
    run_id: str,
    case_id: str,
    kind: str,
    passive_mode: str,
    gateway_transport: str,
    proxy_transport: str = "",
    ebusd_transport: str = "",
) -> Dict[str, Any]:
    normalized_case_id = str(case_id).strip()
    normalized_kind = str(kind).strip()
    normalized_passive_mode = str(passive_mode).strip().lower()
    normalized_gateway_transport = str(gateway_transport).strip().lower()
    normalized_proxy_transport = str(proxy_transport).strip().lower()
    normalized_ebusd_transport = str(ebusd_transport).strip().lower()

    reasons: List[str] = []
    status = "blocked"

    if normalized_kind == "":
        reasons.append("missing family kind")
    if normalized_passive_mode == "":
        reasons.append("missing passive mode")
    if normalized_gateway_transport == "":
        reasons.append("missing gateway transport")
    if normalized_case_id == "":
        reasons.append("missing case_id")
    elif normalized_case_id != CANONICAL_FAMILY_PROOF_CASE_ID:
        reasons.append(
            f"family proof case_id mismatch: got {normalized_case_id!r}; "
            f"want {CANONICAL_FAMILY_PROOF_CASE_ID!r}"
        )

    canary_verdict_path = proof_dir / "canary_verdict.json"
    canary_summary_path = proof_dir / "canary_summary.json"
    replay_behavior_path = proof_dir / "replay_behavior.json"
    replay_verdict_path = proof_dir / "replay_falsification.json"
    if not canary_summary_path.exists():
        reasons.append(f"missing canary summary artifact: {canary_summary_path}")
        canary_summary = None
    else:
        try:
            canary_summary = load_json(canary_summary_path)
        except Exception as exc:
            reasons.append(f"invalid canary summary artifact: {canary_summary_path}: {exc}")
            canary_summary = None
    if not replay_behavior_path.exists():
        reasons.append(f"missing replay behavior artifact: {replay_behavior_path}")
        replay_behavior = None
    else:
        try:
            replay_behavior = load_replay_behavior_artifact(replay_behavior_path)
        except Exception as exc:
            reasons.append(f"invalid replay behavior artifact: {replay_behavior_path}: {exc}")
            replay_behavior = None
    if not canary_verdict_path.exists():
        reasons.append(f"missing canary verdict artifact: {canary_verdict_path}")
        canary_verdict = None
    else:
        try:
            canary_verdict = load_json(canary_verdict_path)
        except Exception as exc:
            reasons.append(f"invalid canary verdict artifact: {canary_verdict_path}: {exc}")
            canary_verdict = None
        if canary_verdict is not None:
            canary_valid, canary_reason = validate_family_upstream_canary_verdict(
                canary_verdict,
                canary_verdict_path,
                summary_payload=canary_summary,
                summary_path=canary_summary_path,
            )
            if not canary_valid:
                reasons.append(f"invalid canary verdict artifact: {canary_reason}")
                canary_verdict = None
    if not replay_verdict_path.exists():
        reasons.append(f"missing replay falsification artifact: {replay_verdict_path}")
        replay_verdict = None
    else:
        try:
            replay_verdict = load_json(replay_verdict_path)
        except Exception as exc:
            reasons.append(f"invalid replay falsification artifact: {replay_verdict_path}: {exc}")
            replay_verdict = None
        if replay_verdict is not None:
            replay_valid, replay_reason = validate_family_upstream_replay_verdict(
                replay_verdict,
                replay_verdict_path,
                behavior_artifact_payload=replay_behavior,
                behavior_artifact_path=replay_behavior_path,
            )
            if not replay_valid:
                reasons.append(f"invalid replay falsification artifact: {replay_reason}")
                replay_verdict = None

    start_snapshot = None
    end_snapshot = None
    try:
        start_snapshot = load_structured_warmup_snapshot(proof_dir, "start")
    except Exception as exc:
        reasons.append(str(exc))
    try:
        end_snapshot = load_structured_warmup_snapshot(proof_dir, "end")
    except Exception as exc:
        reasons.append(str(exc))

    start_transport_class = ""
    end_transport_class = ""
    if isinstance(start_snapshot, dict):
        start_transport_class = str(start_snapshot.get("transport_class", "")).strip().lower()
    if isinstance(end_snapshot, dict):
        end_transport_class = str(end_snapshot.get("transport_class", "")).strip().lower()
    transport_class = ""
    if start_transport_class == "" and end_transport_class == "":
        reasons.append("missing transport class in structured warmup snapshots")
    elif start_transport_class == "" or end_transport_class == "":
        reasons.append(
            "incomplete transport class across structured warmup snapshots: "
            f"start={start_transport_class!r} end={end_transport_class!r}"
        )
    elif start_transport_class != end_transport_class:
        reasons.append(
            "ambiguous transport class across structured warmup snapshots: "
            f"start={start_transport_class!r} end={end_transport_class!r}"
        )
    else:
        transport_class = start_transport_class

    if isinstance(start_snapshot, dict):
        if start_snapshot.get("startup_phase") == "LIVE_READY":
            reasons.append("family proof cold_start is not pre-LIVE_READY")
        if str(start_snapshot.get("warmup_state", "")).strip().lower() == "available":
            reasons.append("family proof cold_start is not pre-available")
    if isinstance(end_snapshot, dict):
        if end_snapshot.get("startup_phase") != "LIVE_READY":
            reasons.append("family proof post_warmup is not LIVE_READY")
        if str(end_snapshot.get("warmup_state", "")).strip().lower() != "available":
            reasons.append("family proof post_warmup is not warmup available")

    canary_ok = bool(isinstance(canary_verdict, dict) and canary_verdict.get("ok", False))
    replay_ok = bool(isinstance(replay_verdict, dict) and replay_verdict.get("ok", False))
    if not canary_ok:
        reasons.append("canary verdict gate failed")
    if not replay_ok:
        reasons.append("replay falsification gate failed")

    canonical_family_shape = (
        normalized_kind == "proxy-single-client"
        and normalized_passive_mode == "required"
        and normalized_gateway_transport == "ens"
        and transport_class == "ens"
    )
    canonical_proven_scope = (
        canonical_family_shape
        and normalized_proxy_transport == "ens"
        and normalized_ebusd_transport == CANONICAL_NO_EBUSD_TRANSPORT
    )
    family_identity_missing = normalized_kind == "" or normalized_passive_mode == "" or transport_class == ""
    family_identity_ambiguous = any(
        reason.startswith("ambiguous transport class across structured warmup snapshots:")
        for reason in reasons
    )
    family_scope_mismatch = False
    if family_identity_missing or family_identity_ambiguous:
        status = "blocked"
    elif canonical_family_shape and not canonical_proven_scope:
        family_scope_mismatch = True
        status = "blocked"
        if normalized_proxy_transport == "":
            reasons.append("family proof missing proof_scope.proxy_transport for canonical proven scope")
        elif normalized_proxy_transport != "ens":
            reasons.append(
                "family proof proof_scope.proxy_transport mismatch: "
                f"got {normalized_proxy_transport!r}; want 'ens'"
            )
        if normalized_ebusd_transport == "":
            reasons.append("family proof missing proof_scope.ebusd_transport for canonical no-ebusd scope")
        elif normalized_ebusd_transport == "ebusd-tcp":
            reasons.append("family proof proof_scope.ebusd_transport mismatch: topology='via-ebusd-tcp'")
        elif normalized_ebusd_transport != CANONICAL_NO_EBUSD_TRANSPORT:
            reasons.append(
                "family proof proof_scope.ebusd_transport mismatch: "
                f"got {normalized_ebusd_transport!r}; want {CANONICAL_NO_EBUSD_TRANSPORT!r}"
            )
    elif not canonical_family_shape:
        family_scope_mismatch = True
        status = "not_proven"
        reasons.append(
            f"family scope mismatch: kind={normalized_kind!r} passive_mode={normalized_passive_mode!r} "
            f"gateway_transport={normalized_gateway_transport!r} transport_class={transport_class!r}; "
            "want proxy-single-client/required/ens/ens"
        )

    if len(reasons) == 0:
        status = "proven_for_default_flip"
    elif not (family_scope_mismatch and len(reasons) == 1):
        status = "blocked"

    artifact = {
        "schema": FAMILY_PROOF_ELIGIBILITY_SCHEMA,
        "captured_at": utc_now(),
        "run_id": str(run_id).strip(),
        "case_id": normalized_case_id,
        "proof_scope": {
            "kind": normalized_kind,
            "passive_mode": normalized_passive_mode,
            "gateway_transport": normalized_gateway_transport,
            "proxy_transport": normalized_proxy_transport or None,
            "ebusd_transport": normalized_ebusd_transport or None,
            "transport_class": transport_class or None,
            "family_key": f"{normalized_kind}/{normalized_passive_mode}/{transport_class}" if transport_class else None,
        },
        "family_identity": {
            "kind": normalized_kind or None,
            "passive_mode": normalized_passive_mode or None,
            "transport_class": transport_class or None,
            "family_key": f"{normalized_kind}/{normalized_passive_mode}/{transport_class}" if transport_class else None,
            "source": "structured_warmup_snapshots+run_metadata",
        },
        "evidence": {
            "start_snapshot_paths": start_snapshot["snapshot_paths"] if isinstance(start_snapshot, dict) else {},
            "end_snapshot_paths": end_snapshot["snapshot_paths"] if isinstance(end_snapshot, dict) else {},
            "canary_summary_path": str(canary_summary_path),
            "canary_verdict_path": str(canary_verdict_path),
            "replay_behavior_path": str(replay_behavior_path),
            "replay_falsification_path": str(replay_verdict_path),
        },
        "upstream_proof": {
            "canary_ok": canary_ok,
            "replay_ok": replay_ok,
            "cold_start_startup_phase": start_snapshot.get("startup_phase") if isinstance(start_snapshot, dict) else None,
            "post_warmup_startup_phase": end_snapshot.get("startup_phase") if isinstance(end_snapshot, dict) else None,
            "cold_start_warmup_state": start_snapshot.get("warmup_state") if isinstance(start_snapshot, dict) else None,
            "post_warmup_warmup_state": end_snapshot.get("warmup_state") if isinstance(end_snapshot, dict) else None,
        },
        "eligibility": {
            "status": status,
            "eligible_for_default_flip": status == "proven_for_default_flip",
            "proven_for_default_flip": status == "proven_for_default_flip",
            "not_proven": status == "not_proven",
            "blocked": status == "blocked",
            "reason": "; ".join(reasons),
        },
        "ok": status == "proven_for_default_flip",
    }
    return artifact


def build_promotion_eligibility_artifact_for_run(
    proof_dir: pathlib.Path,
    run_id: str,
    case_id: str,
    kind: str,
    passive_mode: str,
    gateway_transport: str,
    proxy_transport: str = "",
    ebusd_transport: str = "",
) -> Dict[str, Any]:
    normalized_run_id = str(run_id).strip()
    normalized_case_id = str(case_id).strip()
    normalized_kind = str(kind).strip()
    normalized_passive_mode = str(passive_mode).strip().lower()
    normalized_gateway_transport = str(gateway_transport).strip().lower()
    normalized_proxy_transport = str(proxy_transport).strip().lower()
    normalized_ebusd_transport = str(ebusd_transport).strip().lower()

    reasons: List[str] = []
    status = "blocked"

    if normalized_case_id == "":
        reasons.append("missing case_id")
    elif normalized_case_id != CANONICAL_FAMILY_PROOF_CASE_ID:
        reasons.append(
            f"promotion case_id mismatch: got {normalized_case_id!r}; "
            f"want {CANONICAL_FAMILY_PROOF_CASE_ID!r}"
        )
    if normalized_kind == "":
        reasons.append("missing family kind")
    if normalized_passive_mode == "":
        reasons.append("missing passive mode")
    if normalized_gateway_transport == "":
        reasons.append("missing gateway transport")

    family_eligibility_path = proof_dir / "family_proof_eligibility.json"
    canary_verdict_path = proof_dir / "canary_verdict.json"
    canary_summary_path = proof_dir / "canary_summary.json"
    replay_behavior_path = proof_dir / "replay_behavior.json"
    replay_verdict_path = proof_dir / "replay_falsification.json"

    family_eligibility = None
    if not family_eligibility_path.exists():
        reasons.append(f"missing family proof eligibility artifact: {family_eligibility_path}")
    else:
        try:
            family_eligibility = load_json(family_eligibility_path)
        except Exception as exc:
            reasons.append(f"invalid family proof eligibility artifact: {family_eligibility_path}: {exc}")

    if not canary_summary_path.exists():
        reasons.append(f"missing canary summary artifact: {canary_summary_path}")
        canary_summary = None
    else:
        try:
            canary_summary = load_json(canary_summary_path)
        except Exception as exc:
            reasons.append(f"invalid canary summary artifact: {canary_summary_path}: {exc}")
            canary_summary = None
    if not replay_behavior_path.exists():
        reasons.append(f"missing replay behavior artifact: {replay_behavior_path}")
        replay_behavior = None
    else:
        try:
            replay_behavior = load_replay_behavior_artifact(replay_behavior_path)
        except Exception as exc:
            reasons.append(f"invalid replay behavior artifact: {replay_behavior_path}: {exc}")
            replay_behavior = None
    if not canary_verdict_path.exists():
        reasons.append(f"missing canary verdict artifact: {canary_verdict_path}")
        canary_verdict = None
    else:
        try:
            canary_verdict = load_json(canary_verdict_path)
        except Exception as exc:
            reasons.append(f"invalid canary verdict artifact: {canary_verdict_path}: {exc}")
            canary_verdict = None
        if canary_verdict is not None:
            canary_valid, canary_reason = validate_family_upstream_canary_verdict(
                canary_verdict,
                canary_verdict_path,
                summary_payload=canary_summary,
                summary_path=canary_summary_path,
            )
            if not canary_valid:
                reasons.append(f"invalid canary verdict artifact: {canary_reason}")
                canary_verdict = None
    if not replay_verdict_path.exists():
        reasons.append(f"missing replay falsification artifact: {replay_verdict_path}")
        replay_verdict = None
    else:
        try:
            replay_verdict = load_json(replay_verdict_path)
        except Exception as exc:
            reasons.append(f"invalid replay falsification artifact: {replay_verdict_path}: {exc}")
            replay_verdict = None
        if replay_verdict is not None:
            replay_valid, replay_reason = validate_family_upstream_replay_verdict(
                replay_verdict,
                replay_verdict_path,
                behavior_artifact_payload=replay_behavior,
                behavior_artifact_path=replay_behavior_path,
            )
            if not replay_valid:
                reasons.append(f"invalid replay falsification artifact: {replay_reason}")
                replay_verdict = None

    start_snapshot = None
    end_snapshot = None
    try:
        start_snapshot = load_structured_warmup_snapshot(proof_dir, "start")
    except Exception as exc:
        reasons.append(str(exc))
    try:
        end_snapshot = load_structured_warmup_snapshot(proof_dir, "end")
    except Exception as exc:
        reasons.append(str(exc))

    start_transport_class = ""
    end_transport_class = ""
    if isinstance(start_snapshot, dict):
        start_transport_class = str(start_snapshot.get("transport_class", "")).strip().lower()
    if isinstance(end_snapshot, dict):
        end_transport_class = str(end_snapshot.get("transport_class", "")).strip().lower()
    transport_class = ""
    if start_transport_class == "" and end_transport_class == "":
        reasons.append("missing transport class in structured warmup snapshots")
    elif start_transport_class == "" or end_transport_class == "":
        reasons.append(
            "incomplete transport class across structured warmup snapshots: "
            f"start={start_transport_class!r} end={end_transport_class!r}"
        )
    elif start_transport_class != end_transport_class:
        reasons.append(
            "ambiguous transport class across structured warmup snapshots: "
            f"start={start_transport_class!r} end={end_transport_class!r}"
        )
    else:
        transport_class = start_transport_class

    if isinstance(start_snapshot, dict):
        if start_snapshot.get("startup_phase") == "LIVE_READY":
            reasons.append("promotion proof cold_start is not pre-LIVE_READY")
        if str(start_snapshot.get("warmup_state", "")).strip().lower() == "available":
            reasons.append("promotion proof cold_start is not pre-available")
    if isinstance(end_snapshot, dict):
        if end_snapshot.get("startup_phase") != "LIVE_READY":
            reasons.append("promotion proof post_warmup is not LIVE_READY")
        if str(end_snapshot.get("warmup_state", "")).strip().lower() != "available":
            reasons.append("promotion proof post_warmup is not warmup available")

    family_scope = family_eligibility.get("proof_scope") if isinstance(family_eligibility, dict) else None
    family_identity = family_eligibility.get("family_identity") if isinstance(family_eligibility, dict) else None
    family_eligibility_block = (
        family_eligibility.get("eligibility") if isinstance(family_eligibility, dict) else None
    )
    family_upstream = family_eligibility.get("upstream_proof") if isinstance(family_eligibility, dict) else None
    family_ok = bool(isinstance(family_eligibility, dict) and family_eligibility.get("ok", False))
    family_eligibility_status = str(
        ((family_eligibility_block or {}).get("status", "")) if isinstance(family_eligibility_block, dict) else ""
    ).strip().lower()
    family_key = str((family_scope or {}).get("family_key", "")).strip()
    family_transport_class = str((family_scope or {}).get("transport_class", "")).strip().lower()
    family_kind = str((family_scope or {}).get("kind", "")).strip()
    family_passive_mode = str((family_scope or {}).get("passive_mode", "")).strip().lower()
    family_gateway_transport = str((family_scope or {}).get("gateway_transport", "")).strip().lower()
    family_proxy_transport = str((family_scope or {}).get("proxy_transport", "")).strip().lower()
    family_ebusd_transport = str(((family_scope or {}).get("ebusd_transport", "") or "")).strip().lower()
    family_identity_kind = str((family_identity or {}).get("kind", "")).strip()
    family_identity_passive_mode = str((family_identity or {}).get("passive_mode", "")).strip().lower()
    family_identity_transport_class = str((family_identity or {}).get("transport_class", "")).strip().lower()
    family_identity_key = str((family_identity or {}).get("family_key", "")).strip()
    expected_family_key = (
        f"{normalized_kind}/{normalized_passive_mode}/{transport_class}"
        if normalized_kind and normalized_passive_mode and transport_class
        else ""
    )
    family_scope_has_ebusd_transport = isinstance(family_scope, dict) and family_ebusd_transport != ""
    proxy_transport_required = promotion_topology_requires_proxy_transport(
        normalized_kind,
        normalized_ebusd_transport,
    )

    if not isinstance(family_eligibility, dict):
        reasons.append("family proof eligibility artifact is not a JSON object")
    elif str(family_eligibility.get("schema", "")).strip() != FAMILY_PROOF_ELIGIBILITY_SCHEMA:
        reasons.append("family proof eligibility schema mismatch")
    elif not isinstance(family_eligibility.get("ok"), bool):
        reasons.append("family proof eligibility missing boolean ok")
    elif not isinstance(family_scope, dict):
        reasons.append("family proof eligibility missing proof_scope object")
    elif not isinstance(family_identity, dict):
        reasons.append("family proof eligibility missing family_identity object")
    elif not isinstance(family_eligibility_block, dict):
        reasons.append("family proof eligibility missing eligibility object")
    elif not isinstance(family_upstream, dict):
        reasons.append("family proof eligibility missing upstream_proof object")

    if family_eligibility_status == "proven_for_default_flip" and not family_ok:
        reasons.append(
            "family proof eligibility ok/status mismatch: "
            f"ok={family_ok!r} status={family_eligibility_status!r}"
        )
    if family_ok and family_eligibility_status != "proven_for_default_flip":
        reasons.append(
            "family proof eligibility ok/status mismatch: "
            f"ok={family_ok!r} status={family_eligibility_status!r}"
        )
    if family_eligibility_status in {"blocked", "not_proven", "proven_for_default_flip"}:
        pass
    elif isinstance(family_eligibility_block, dict):
        reasons.append(f"family proof eligibility has invalid status: {family_eligibility_status!r}")

    if family_scope and isinstance(family_scope, dict):
        if family_scope.get("kind") in (None, ""):
            reasons.append("family proof eligibility missing proof_scope.kind")
        if family_scope.get("passive_mode") in (None, ""):
            reasons.append("family proof eligibility missing proof_scope.passive_mode")
        if family_scope.get("gateway_transport") in (None, ""):
            reasons.append("family proof eligibility missing proof_scope.gateway_transport")
        if proxy_transport_required and family_scope.get("proxy_transport") in (None, ""):
            reasons.append("family proof eligibility missing proof_scope.proxy_transport")
        if family_scope.get("transport_class") in (None, ""):
            reasons.append("family proof eligibility missing proof_scope.transport_class")
        if family_scope.get("family_key") in (None, ""):
            reasons.append("family proof eligibility missing proof_scope.family_key")
        if not family_scope_has_ebusd_transport:
            reasons.append("family proof eligibility missing proof_scope.ebusd_transport")

    if isinstance(family_eligibility, dict) and isinstance(family_identity, dict):
        if family_identity.get("kind") in (None, ""):
            reasons.append("family proof eligibility missing family_identity.kind")
        elif family_identity_kind != normalized_kind:
            reasons.append(
                "family proof eligibility family_identity.kind mismatch: "
                f"got {family_identity_kind!r}; want {normalized_kind!r}"
            )
        if family_identity.get("passive_mode") in (None, ""):
            reasons.append("family proof eligibility missing family_identity.passive_mode")
        elif family_identity_passive_mode != normalized_passive_mode:
            reasons.append(
                "family proof eligibility family_identity.passive_mode mismatch: "
                f"got {family_identity_passive_mode!r}; want {normalized_passive_mode!r}"
            )
        if family_identity.get("transport_class") in (None, ""):
            reasons.append("family proof eligibility missing family_identity.transport_class")
        elif transport_class and family_identity_transport_class != transport_class:
            reasons.append(
                "family proof eligibility family_identity.transport_class mismatch: "
                f"got {family_identity_transport_class!r}; want {transport_class!r}"
            )
        if family_identity.get("family_key") in (None, ""):
            reasons.append("family proof eligibility missing family_identity.family_key")
        elif expected_family_key and family_identity_key != expected_family_key:
            reasons.append(
                "family proof eligibility family_identity.family_key mismatch: "
                f"got {family_identity_key!r}; want {expected_family_key!r}"
            )
        if family_identity_key and family_key and family_identity_key != family_key:
            reasons.append(
                "family proof eligibility family_identity.family_key mismatch: "
                f"got {family_identity_key!r}; want {family_key!r}"
            )

    if isinstance(family_eligibility, dict) and isinstance(family_scope, dict):
        if str(family_eligibility.get("case_id", "")).strip() != normalized_case_id:
            reasons.append(
                "family proof eligibility case_id mismatch: "
                f"got {family_eligibility.get('case_id', '')!r}; want {normalized_case_id!r}"
            )
        if family_kind and family_kind != normalized_kind:
            reasons.append(
                "family proof eligibility proof_scope.kind mismatch: "
                f"got {family_kind!r}; want {normalized_kind!r}"
            )
        if family_passive_mode and family_passive_mode != normalized_passive_mode:
            reasons.append(
                "family proof eligibility proof_scope.passive_mode mismatch: "
                f"got {family_passive_mode!r}; want {normalized_passive_mode!r}"
            )
        if family_gateway_transport and family_gateway_transport != normalized_gateway_transport:
            reasons.append(
                "family proof eligibility proof_scope.gateway_transport mismatch: "
                f"got {family_gateway_transport!r}; want {normalized_gateway_transport!r}"
            )
        if proxy_transport_required:
            if normalized_proxy_transport == "":
                reasons.append("missing promotion topology metadata: proxy_transport")
            elif family_proxy_transport and family_proxy_transport != normalized_proxy_transport:
                reasons.append(
                    "family proof eligibility proof_scope.proxy_transport mismatch: "
                    f"got {family_proxy_transport!r}; want {normalized_proxy_transport!r}"
                )
        if normalized_ebusd_transport == "":
            reasons.append("missing promotion topology metadata: ebusd_transport")
        elif family_scope_has_ebusd_transport and family_ebusd_transport != normalized_ebusd_transport:
            reasons.append(
                "family proof eligibility proof_scope.ebusd_transport mismatch: "
                f"got {family_ebusd_transport!r}; want {normalized_ebusd_transport!r}"
            )
        if family_transport_class and transport_class and family_transport_class != transport_class:
            reasons.append(
                "family proof eligibility proof_scope.transport_class mismatch: "
                f"got {family_transport_class!r}; want {transport_class!r}"
            )
        if family_key and expected_family_key and family_key != expected_family_key:
            reasons.append(
                "family proof eligibility proof_scope.family_key mismatch: "
                f"got {family_key!r}; want {expected_family_key!r}"
            )

    if isinstance(family_upstream, dict):
        family_canary_ok_raw = family_upstream.get("canary_ok")
        family_replay_ok_raw = family_upstream.get("replay_ok")
        if not isinstance(family_canary_ok_raw, bool):
            reasons.append("family proof eligibility upstream canary_ok must be boolean")
        if not isinstance(family_replay_ok_raw, bool):
            reasons.append("family proof eligibility upstream replay_ok must be boolean")
        family_canary_ok = family_canary_ok_raw if isinstance(family_canary_ok_raw, bool) else False
        family_replay_ok = family_replay_ok_raw if isinstance(family_replay_ok_raw, bool) else False
        canary_ok = bool(isinstance(canary_verdict, dict) and canary_verdict.get("ok", False))
        replay_ok = bool(isinstance(replay_verdict, dict) and replay_verdict.get("ok", False))
        if isinstance(family_canary_ok_raw, bool) and family_canary_ok != canary_ok:
            reasons.append(
                "family proof eligibility upstream canary_ok mismatch: "
                f"got {family_canary_ok!r}; want {canary_ok!r}"
            )
        if isinstance(family_replay_ok_raw, bool) and family_replay_ok != replay_ok:
            reasons.append(
                "family proof eligibility upstream replay_ok mismatch: "
                f"got {family_replay_ok!r}; want {replay_ok!r}"
            )
    else:
        canary_ok = bool(isinstance(canary_verdict, dict) and canary_verdict.get("ok", False))
        replay_ok = bool(isinstance(replay_verdict, dict) and replay_verdict.get("ok", False))

    if not canary_ok:
        reasons.append("canary verdict gate failed")
    if not replay_ok:
        reasons.append("replay falsification gate failed")

    if isinstance(family_eligibility, dict):
        family_run_id = str(family_eligibility.get("run_id", "")).strip()
        if family_run_id == "":
            reasons.append("family proof eligibility missing run_id")
        elif family_run_id != normalized_run_id:
            reasons.append(
                "family proof eligibility run_id mismatch: "
                f"got {family_run_id!r}; want {normalized_run_id!r}"
            )

    family_claims_proven = family_ok and family_eligibility_status == "proven_for_default_flip"
    promotion_topology = promotion_topology_label(
        normalized_kind,
        normalized_gateway_transport,
        normalized_proxy_transport,
        normalized_ebusd_transport,
    )
    canonical_proven_scope = (
        normalized_kind == "proxy-single-client"
        and normalized_passive_mode == "required"
        and normalized_gateway_transport == "ens"
        and normalized_proxy_transport == "ens"
        and normalized_ebusd_transport == CANONICAL_NO_EBUSD_TRANSPORT
        and transport_class == "ens"
    )
    scope_reason = (
        f"promotion scope mismatch: kind={normalized_kind!r} passive_mode={normalized_passive_mode!r} "
        f"gateway_transport={normalized_gateway_transport!r} proxy_transport={normalized_proxy_transport!r} "
        f"ebusd_transport={normalized_ebusd_transport!r} transport_class={transport_class!r} "
        f"topology={promotion_topology!r}; want proxy-single-client/required/ens with ebusd_transport={CANONICAL_NO_EBUSD_TRANSPORT!r}"
    )
    if len(reasons) == 0:
        if family_claims_proven and canonical_proven_scope:
            status = "eligible_for_default_flip"
        else:
            reasons.append(scope_reason)
            status = "not_proven"
    else:
        status = "blocked"

    artifact = {
        "schema": PROMOTION_ELIGIBILITY_SCHEMA,
        "captured_at": utc_now(),
        "run_id": normalized_run_id,
        "case_id": normalized_case_id,
        "matrix_topology": {
            "case_id": normalized_case_id or None,
            "kind": normalized_kind or None,
            "passive_mode": normalized_passive_mode or None,
            "gateway_transport": normalized_gateway_transport or None,
            "proxy_transport": normalized_proxy_transport or None,
            "ebusd_transport": normalized_ebusd_transport or None,
            "transport_class": transport_class or None,
            "family_key": f"{normalized_kind}/{normalized_passive_mode}/{transport_class}" if transport_class else None,
        },
        "family_proof_eligibility": {
            "path": str(family_eligibility_path),
            "schema": family_eligibility.get("schema") if isinstance(family_eligibility, dict) else None,
            "ok": family_ok,
            "eligibility": family_eligibility_block,
            "proof_scope": family_scope,
            "family_identity": family_identity,
            "upstream_proof": family_upstream,
        },
        "promotion_scope": {
            "case_id": normalized_case_id,
            "kind": normalized_kind,
            "passive_mode": normalized_passive_mode,
            "gateway_transport": normalized_gateway_transport,
            "proxy_transport": normalized_proxy_transport or None,
            "ebusd_transport": normalized_ebusd_transport or None,
            "transport_class": transport_class or None,
            "family_key": f"{normalized_kind}/{normalized_passive_mode}/{transport_class}" if transport_class else None,
        },
        "eligibility": {
            "status": status,
            "eligible_for_default_flip": status == "eligible_for_default_flip",
            "proven_for_default_flip": status == "eligible_for_default_flip",
            "not_proven": status == "not_proven",
            "blocked": status == "blocked",
            "reason": "; ".join(reasons),
        },
        "ok": status == "eligible_for_default_flip",
    }
    return artifact


def build_warmup_behavior_artifact_for_phases(
    proof_dir: pathlib.Path,
    run_id: str,
    require_interval_phase: bool,
) -> Dict[str, Any]:
    structured_phase_prefixes = sorted(
        {
            path.name[: -len("_bus_observability.json")]
            for path in proof_dir.glob("**/*_bus_observability.json")
            if path.is_file()
        },
        key=phase_sort_key,
    )
    if "start" not in structured_phase_prefixes or "end" not in structured_phase_prefixes:
        raise ValueError("missing current-run start/end structured warmup artifacts (stale artifact rejection)")

    interval_phase_prefixes = [phase for phase in structured_phase_prefixes if is_interval_phase(phase)]
    cold_start = load_structured_warmup_snapshot(proof_dir, "start")
    post_warmup = load_structured_warmup_snapshot(proof_dir, "end")
    interval_snapshots = [load_structured_warmup_snapshot(proof_dir, phase) for phase in interval_phase_prefixes]

    cold_start_proven = cold_start["startup_phase"] != "LIVE_READY" and cold_start["warmup_state"] != "available"
    post_warmup_proven = post_warmup["startup_phase"] == "LIVE_READY" and post_warmup["warmup_state"] == "available"
    interval_established = len(interval_snapshots) >= 1
    transition_established = cold_start_proven and post_warmup_proven and interval_established
    transition_evidence = {
        "start_snapshot_paths": cold_start["snapshot_paths"],
        "end_snapshot_paths": post_warmup["snapshot_paths"],
        "interval_snapshot_paths": [snapshot["snapshot_paths"] for snapshot in interval_snapshots],
        "structured_snapshot_prefixes": structured_phase_prefixes,
    }

    return {
        "schema": WARMUP_BEHAVIOR_ARTIFACT_SCHEMA,
        "captured_at": utc_now(),
        "run_id": run_id,
        "claim_scope": "bounded_proof_window_warmup_behavior",
        "evidence": transition_evidence,
        "cold_start": {
            "snapshot_prefix": cold_start["snapshot_prefix"],
            "snapshot_paths": cold_start["snapshot_paths"],
            "bus_observability": cold_start["bus_observability"],
            "graphql_bus_watch": cold_start["graphql_bus_watch"],
            "feature_flag_snapshot": cold_start["feature_flag_snapshot"],
            "startup_phase": cold_start["startup_phase"],
            "warmup_state": cold_start["warmup_state"],
            "timestamps": cold_start["timestamps"],
        },
        "post_warmup": {
            "snapshot_prefix": post_warmup["snapshot_prefix"],
            "snapshot_paths": post_warmup["snapshot_paths"],
            "bus_observability": post_warmup["bus_observability"],
            "graphql_bus_watch": post_warmup["graphql_bus_watch"],
            "feature_flag_snapshot": post_warmup["feature_flag_snapshot"],
            "startup_phase": post_warmup["startup_phase"],
            "warmup_state": post_warmup["warmup_state"],
            "timestamps": post_warmup["timestamps"],
        },
        "transition": {
            "established": transition_established,
            "from_snapshot_prefix": "start",
            "to_snapshot_prefix": "end",
            "cold_start_proven": cold_start_proven,
            "post_warmup_proven": post_warmup_proven,
            "interval_snapshot_count": len(interval_snapshots),
            "interval_snapshot_prefixes": [snapshot["snapshot_prefix"] for snapshot in interval_snapshots],
            "first_interval_snapshot_prefix": interval_snapshots[0]["snapshot_prefix"] if interval_snapshots else None,
            "last_interval_snapshot_prefix": interval_snapshots[-1]["snapshot_prefix"] if interval_snapshots else None,
            "evidence": transition_evidence,
        },
        "ok": transition_established,
    }


def build_publisher_cadence_artifact_for_phases(
    proof_dir: pathlib.Path,
    run_id: str,
) -> Dict[str, Any]:
    if not isinstance(proof_dir, pathlib.Path):
        raise ValueError("proof_dir must be a pathlib.Path")

    structured_phase_prefixes = sorted(
        {
            path.name[: -len("_bus_observability.json")]
            for path in proof_dir.glob("**/*_bus_observability.json")
            if path.is_file()
        },
        key=phase_sort_key,
    )
    if "start" not in structured_phase_prefixes or "end" not in structured_phase_prefixes:
        raise ValueError("missing current-run start/end structured publisher cadence artifacts")

    start_snapshot = load_structured_warmup_snapshot(proof_dir, "start")
    end_snapshot = load_structured_warmup_snapshot(proof_dir, "end")
    start_cadence = start_snapshot.get("publisher_cadence")
    end_cadence = end_snapshot.get("publisher_cadence")
    if not isinstance(start_cadence, dict):
        raise ValueError("publisher cadence start snapshot missing cadence payload")
    if not isinstance(end_cadence, dict):
        raise ValueError("publisher cadence end snapshot missing cadence payload")

    start_sec = start_cadence.get("publisher_cadence_sec")
    end_sec = end_cadence.get("publisher_cadence_sec")
    start_source = str(start_cadence.get("publisher_cadence_source", "")).strip()
    end_source = str(end_cadence.get("publisher_cadence_source", "")).strip()
    if not isinstance(start_sec, (int, float)):
        raise ValueError("publisher cadence start snapshot missing numeric publisher_cadence_sec")
    if not isinstance(end_sec, (int, float)):
        raise ValueError("publisher cadence end snapshot missing numeric publisher_cadence_sec")
    start_value = float(start_sec)
    end_value = float(end_sec)
    if not math.isfinite(start_value) or start_value <= 0:
        raise ValueError(f"publisher cadence start snapshot has invalid publisher_cadence_sec {start_value!r}")
    if not math.isfinite(end_value) or end_value <= 0:
        raise ValueError(f"publisher cadence end snapshot has invalid publisher_cadence_sec {end_value!r}")
    if start_source == "":
        raise ValueError("publisher cadence start snapshot missing publisher_cadence_source")
    if end_source == "":
        raise ValueError("publisher cadence end snapshot missing publisher_cadence_source")
    if start_source != end_source:
        raise ValueError(
            "publisher cadence source mismatch across proof window: "
            f"start={start_source!r} end={end_source!r}"
        )
    if abs(start_value - end_value) > 1e-9:
        raise ValueError(
            "publisher cadence mismatch across proof window: "
            f"start={start_value} end={end_value}"
        )
    if start_source != PUBLISHER_CADENCE_SOURCE_ANCHOR:
        raise ValueError(
            "publisher cadence source anchor mismatch: "
            f"got {start_source!r}; want {PUBLISHER_CADENCE_SOURCE_ANCHOR!r}"
        )

    return {
        "schema": PUBLISHER_CADENCE_ARTIFACT_SCHEMA,
        "captured_at": utc_now(),
        "run_id": run_id,
        "claim_scope": "bounded_proof_window_publisher_cadence",
        "source": "proof_artifact_publisher_cadence",
        "evidence": {
            "start_snapshot_paths": start_snapshot["snapshot_paths"],
            "end_snapshot_paths": end_snapshot["snapshot_paths"],
            "structured_snapshot_prefixes": structured_phase_prefixes,
        },
        "start": {
            "snapshot_prefix": start_snapshot["snapshot_prefix"],
            "snapshot_paths": start_snapshot["snapshot_paths"],
            "bus_observability": start_snapshot["bus_observability"],
            "graphql_bus_watch": start_snapshot["graphql_bus_watch"],
            "publisher_cadence": start_cadence,
            "timestamps": start_snapshot["timestamps"],
        },
        "end": {
            "snapshot_prefix": end_snapshot["snapshot_prefix"],
            "snapshot_paths": end_snapshot["snapshot_paths"],
            "bus_observability": end_snapshot["bus_observability"],
            "graphql_bus_watch": end_snapshot["graphql_bus_watch"],
            "publisher_cadence": end_cadence,
            "timestamps": end_snapshot["timestamps"],
        },
        "coherence": {
            "source_anchor": start_source,
            "same_source_across_window": True,
            "same_value_across_window": True,
            "bus_graphql_agree": True,
        },
        "ok": True,
    }


def collect_cross_plane_semantic_timestamps(
    snapshot: Dict[str, Any],
    snapshot_path: pathlib.Path,
) -> Dict[str, Any]:
    timestamps = snapshot.get("timestamps")
    if not isinstance(timestamps, dict):
        raise ValueError(f"{snapshot_path}: structured warmup snapshot missing timestamps object")
    bus_timestamps = timestamps.get("bus_observability")
    graphql_timestamps = timestamps.get("graphql_bus_watch")
    if not isinstance(bus_timestamps, dict):
        raise ValueError(f"{snapshot_path}: structured warmup snapshot missing bus_observability timestamps")
    if not isinstance(graphql_timestamps, dict):
        raise ValueError(f"{snapshot_path}: structured warmup snapshot missing graphql_bus_watch timestamps")

    semantic_last_updated_at = {
        "bus_observability": {
            "summary_last_updated_at": normalize_timestamp_value(
                bus_timestamps.get("summary_last_updated_at"),
                snapshot_path,
                "bus observability",
                "summary.last_updated_at",
            ),
            "status_last_updated_at": normalize_timestamp_value(
                bus_timestamps.get("status_last_updated_at"),
                snapshot_path,
                "bus observability",
                "status.last_updated_at",
            ),
        },
        "graphql_bus_watch": {
            "summary_last_updated_at": normalize_timestamp_value(
                graphql_timestamps.get("summary_last_updated_at"),
                snapshot_path,
                "graphql",
                "summary.last_updated_at",
            ),
            "status_last_updated_at": normalize_timestamp_value(
                graphql_timestamps.get("status_last_updated_at"),
                snapshot_path,
                "graphql",
                "status.last_updated_at",
            ),
            "watch_summary_last_updated_at": normalize_timestamp_value(
                graphql_timestamps.get("watch_summary_last_updated_at"),
                snapshot_path,
                "graphql",
                "watch_summary.last_updated_at",
            ),
        },
    }

    timeline: List[Tuple[str, datetime]] = []
    for surface_name in CROSS_PLANE_SKEW_SEMANTIC_FIELDS:
        plane_name, field_name = surface_name.split(".", 1)
        plane_snapshot = semantic_last_updated_at.get(plane_name)
        if not isinstance(plane_snapshot, dict):
            raise ValueError(f"{snapshot_path}: structured warmup snapshot missing {plane_name} semantic timestamps")
        raw_value = plane_snapshot.get(field_name)
        timeline.append(
            (
                surface_name,
                parse_timestamp_to_utc(
                    raw_value,
                    snapshot_path,
                    plane_name.replace("_", " "),
                    field_name,
                ),
            )
        )

    oldest_surface, oldest_timestamp = min(timeline, key=lambda item: item[1])
    newest_surface, newest_timestamp = max(timeline, key=lambda item: item[1])
    observed_skew_sec = max(0.0, (newest_timestamp - oldest_timestamp).total_seconds())
    observed_skew_ms = observed_skew_sec * 1000.0

    return {
        "semantic_last_updated_at": semantic_last_updated_at,
        "timeline": [
            {
                "surface": surface,
                "last_updated_at": timestamp.isoformat().replace("+00:00", "Z"),
            }
            for surface, timestamp in timeline
        ],
        "oldest_surface": oldest_surface,
        "oldest_last_updated_at": oldest_timestamp.isoformat().replace("+00:00", "Z"),
        "newest_surface": newest_surface,
        "newest_last_updated_at": newest_timestamp.isoformat().replace("+00:00", "Z"),
        "observed_skew_sec": observed_skew_sec,
        "observed_skew_ms": observed_skew_ms,
    }


def build_cross_plane_skew_artifact_for_phases(
    proof_dir: pathlib.Path,
    run_id: str,
    configured_proof_sample_interval_sec: float,
    publisher_cadence_path: pathlib.Path | None = None,
) -> Dict[str, Any]:
    if not isinstance(proof_dir, pathlib.Path):
        raise ValueError("proof_dir must be a pathlib.Path")
    if publisher_cadence_path is None:
        publisher_cadence_path = proof_dir / "publisher_cadence.json"
    if not isinstance(publisher_cadence_path, pathlib.Path):
        raise ValueError("publisher_cadence_path must be a pathlib.Path")

    structured_phase_prefixes = sorted(
        {
            path.name[: -len("_bus_observability.json")]
            for path in proof_dir.glob("**/*_bus_observability.json")
            if path.is_file()
        },
        key=phase_sort_key,
    )
    reasons: List[str] = []
    if "start" not in structured_phase_prefixes or "end" not in structured_phase_prefixes:
        reasons.append("missing current-run start/end structured cross-plane skew artifacts (stale artifact rejection)")

    publisher_cadence_payload: Dict[str, Any] | None = None
    publisher_cadence_ok = False
    publisher_cadence_reason = ""
    publisher_cadence_details: Dict[str, Any] = {}
    publisher_cadence_sec: float | None = None
    publisher_cadence_source = ""
    if not publisher_cadence_path.exists():
        reasons.append(f"missing required publisher cadence artifact: {publisher_cadence_path}")
    else:
        try:
            publisher_cadence_payload = load_json(publisher_cadence_path)
            publisher_cadence_ok, publisher_cadence_reason, publisher_cadence_details = evaluate_publisher_cadence(
                publisher_cadence_payload
            )
            if not publisher_cadence_ok:
                reasons.append(publisher_cadence_reason)
            else:
                publisher_cadence_sec = float(publisher_cadence_details["start_publisher_cadence_sec"])
                publisher_cadence_source = str(publisher_cadence_details["publisher_cadence_source"]).strip()
        except Exception as exc:
            reasons.append(f"invalid publisher cadence artifact: {publisher_cadence_path}: {exc}")

    configured_interval_sec: float | None = None
    try:
        configured_interval_sec = float(configured_proof_sample_interval_sec)
    except (TypeError, ValueError):
        reasons.append(
            f"invalid configured_proof_sample_interval_sec {configured_proof_sample_interval_sec!r}"
        )
    else:
        if not math.isfinite(configured_interval_sec) or configured_interval_sec <= 0:
            reasons.append(
                f"invalid configured_proof_sample_interval_sec {configured_interval_sec!r}"
            )

    target_max_skew_sec: float | None = None
    target_max_skew_ms: float | None = None
    if publisher_cadence_sec is not None and configured_interval_sec is not None:
        target_max_skew_sec = max(publisher_cadence_sec, configured_interval_sec)
        target_max_skew_ms = target_max_skew_sec * 1000.0

    phases: List[Dict[str, Any]] = []
    phase_failures: List[str] = []
    max_observed_skew_sec = 0.0
    max_observed_skew_ms = 0.0
    max_observed_phase = ""
    phases_exceeding_target: List[str] = []

    for phase in structured_phase_prefixes:
        try:
            snapshot = load_structured_warmup_snapshot(proof_dir, phase)
            snapshot_path = pathlib.Path(snapshot["snapshot_paths"]["bus_observability"])
            phase_timestamps = collect_cross_plane_semantic_timestamps(snapshot, snapshot_path)
            observed_skew_sec = float(phase_timestamps["observed_skew_sec"])
            observed_skew_ms = float(phase_timestamps["observed_skew_ms"])
            within_target = True
            if target_max_skew_sec is not None:
                within_target = observed_skew_sec <= target_max_skew_sec + 1e-9
            if not within_target:
                phases_exceeding_target.append(phase)
            if observed_skew_sec > max_observed_skew_sec:
                max_observed_skew_sec = observed_skew_sec
                max_observed_skew_ms = observed_skew_ms
                max_observed_phase = phase
            phases.append(
                {
                    "phase": phase,
                    "snapshot_prefix": snapshot["snapshot_prefix"],
                    "snapshot_paths": snapshot["snapshot_paths"],
                    "semantic_last_updated_at": phase_timestamps["semantic_last_updated_at"],
                    "timeline": phase_timestamps["timeline"],
                    "oldest_surface": phase_timestamps["oldest_surface"],
                    "oldest_last_updated_at": phase_timestamps["oldest_last_updated_at"],
                    "newest_surface": phase_timestamps["newest_surface"],
                    "newest_last_updated_at": phase_timestamps["newest_last_updated_at"],
                    "observed_skew_sec": observed_skew_sec,
                    "observed_skew_ms": observed_skew_ms,
                    "target_max_skew_sec": target_max_skew_sec,
                    "target_max_skew_ms": target_max_skew_ms,
                    "within_target": within_target,
                }
            )
        except Exception as exc:
            phase_failures.append(f"{phase}: {exc}")

    if not phases:
        phase_failures.append("missing current-run cross-plane skew phase evidence")

    ok = (
        len(reasons) == 0
        and len(phase_failures) == 0
        and len(phases_exceeding_target) == 0
        and target_max_skew_sec is not None
    )
    status = "pass" if ok else "fail"
    if phases_exceeding_target:
        reasons.append(
            "observed cross-plane skew exceeded target bound in phases: "
            + ", ".join(phases_exceeding_target)
        )

    summary = {
        "phase_count": len(phases),
        "max_observed_phase": max_observed_phase or None,
        "max_observed_skew_sec": max_observed_skew_sec if phases else None,
        "max_observed_skew_ms": max_observed_skew_ms if phases else None,
        "target_max_skew_sec": target_max_skew_sec,
        "target_max_skew_ms": target_max_skew_ms,
        "configured_proof_sample_interval_sec": configured_interval_sec,
        "publisher_cadence_sec": publisher_cadence_sec,
        "publisher_cadence_source": publisher_cadence_source or None,
        "phases_within_target": len(phases_exceeding_target) == 0,
        "phases_exceeding_target": phases_exceeding_target,
        "publisher_cadence_ok": publisher_cadence_ok,
    }

    return {
        "schema": CROSS_PLANE_SKEW_ARTIFACT_SCHEMA,
        "captured_at": utc_now(),
        "run_id": str(run_id).strip(),
        "claim_scope": "bounded_proof_window_cross_plane_skew",
        "source": "proof_artifact_cross_plane_skew",
        "target_bound_sec": target_max_skew_sec,
        "target_bound_ms": target_max_skew_ms,
        "proof_metadata": {
            "configured_proof_sample_interval_sec": configured_interval_sec,
            "publisher_cadence_sec": publisher_cadence_sec,
            "publisher_cadence_source": publisher_cadence_source or None,
            "target_max_skew_sec": target_max_skew_sec,
            "target_max_skew_ms": target_max_skew_ms,
        },
        "publisher_cadence": {
            "path": str(publisher_cadence_path),
            "schema": str((publisher_cadence_payload or {}).get("schema", "")).strip() or None,
            "ok": publisher_cadence_ok,
            "reason": publisher_cadence_reason or None,
            "publisher_cadence_sec": publisher_cadence_sec,
            "publisher_cadence_source": publisher_cadence_source or None,
            "graphql_publisher_cadence_sec": publisher_cadence_details.get("start_publisher_cadence_sec"),
            "graphql_publisher_cadence_source": publisher_cadence_details.get("publisher_cadence_source"),
            "details": publisher_cadence_details,
        },
        "evidence": {
            "publisher_cadence_path": str(publisher_cadence_path),
            "structured_snapshot_prefixes": structured_phase_prefixes,
            "phase_snapshot_paths": [phase["snapshot_paths"] for phase in phases],
            "same_phase_semantic_last_updated_at_fields": list(CROSS_PLANE_SKEW_SEMANTIC_FIELDS),
            "excluded_surface_groups": list(CROSS_PLANE_SKEW_EXCLUDED_SURFACES),
        },
        "phases": phases,
        "summary": summary,
        "reasons": reasons + phase_failures,
        "ok": ok,
        "status": status,
    }


def evaluate_publisher_cadence(payload: Any) -> Tuple[bool, str, Dict[str, Any]]:
    if not isinstance(payload, dict):
        return False, "missing publisher cadence payload", {}
    if str(payload.get("schema", "")).strip() != PUBLISHER_CADENCE_ARTIFACT_SCHEMA:
        return False, "publisher cadence schema mismatch", {}
    if payload.get("ok") is not True:
        return False, "publisher cadence artifact is not ok", {}

    start = payload.get("start")
    end = payload.get("end")
    coherence = payload.get("coherence")
    if not isinstance(start, dict):
        return False, "publisher cadence missing start payload", {}
    if not isinstance(end, dict):
        return False, "publisher cadence missing end payload", {}
    if not isinstance(coherence, dict):
        return False, "publisher cadence missing coherence payload", {}

    start_cadence = start.get("publisher_cadence")
    end_cadence = end.get("publisher_cadence")
    if not isinstance(start_cadence, dict):
        return False, "publisher cadence start missing cadence payload", {}
    if not isinstance(end_cadence, dict):
        return False, "publisher cadence end missing cadence payload", {}

    start_sec, start_sec_error = required_finite_numeric_value(
        start_cadence,
        "publisher_cadence_sec",
        path=pathlib.Path("publisher_cadence.json"),
        context="publisher cadence start",
    )
    if start_sec_error:
        return False, start_sec_error, {}
    end_sec, end_sec_error = required_finite_numeric_value(
        end_cadence,
        "publisher_cadence_sec",
        path=pathlib.Path("publisher_cadence.json"),
        context="publisher cadence end",
    )
    if end_sec_error:
        return False, end_sec_error, {}

    start_source = str(start_cadence.get("publisher_cadence_source", "")).strip()
    end_source = str(end_cadence.get("publisher_cadence_source", "")).strip()
    if start_source == "":
        return False, "publisher cadence start missing publisher_cadence_source", {}
    if end_source == "":
        return False, "publisher cadence end missing publisher_cadence_source", {}
    if start_source != end_source:
        return False, "publisher cadence source mismatch across proof window", {}
    if start_source != PUBLISHER_CADENCE_SOURCE_ANCHOR:
        return False, "publisher cadence source anchor mismatch", {}
    if abs(start_sec - end_sec) > 1e-9:
        return False, "publisher cadence value mismatch across proof window", {}

    details = {
        "start_publisher_cadence_sec": start_sec,
        "end_publisher_cadence_sec": end_sec,
        "publisher_cadence_source": start_source,
        "source_anchor": coherence.get("source_anchor"),
    }
    return True, "", details


def evaluate_warmup_behavior(payload: Any) -> Tuple[bool, str, Dict[str, Any]]:
    if not isinstance(payload, dict):
        return False, "missing warmup_behavior payload", {}
    if str(payload.get("schema", "")).strip() != WARMUP_BEHAVIOR_ARTIFACT_SCHEMA:
        return False, "warmup_behavior schema mismatch", {}

    cold_start = payload.get("cold_start")
    post_warmup = payload.get("post_warmup")
    transition = payload.get("transition")
    evidence = payload.get("evidence")
    if not isinstance(cold_start, dict):
        return False, "warmup_behavior missing cold_start", {}
    if not isinstance(post_warmup, dict):
        return False, "warmup_behavior missing post_warmup", {}
    if not isinstance(transition, dict):
        return False, "warmup_behavior missing transition", {}
    if not isinstance(evidence, dict):
        return False, "warmup_behavior missing evidence", {}

    for side_name, side in (("cold_start", cold_start), ("post_warmup", post_warmup)):
        snapshot_prefix = str(side.get("snapshot_prefix", "")).strip()
        snapshot_paths = side.get("snapshot_paths")
        bus_observability = side.get("bus_observability")
        graphql_bus_watch = side.get("graphql_bus_watch")
        feature_flag_snapshot = side.get("feature_flag_snapshot")
        timestamps = side.get("timestamps")
        startup_phase = str(side.get("startup_phase", "")).strip().upper()
        warmup_state = str(side.get("warmup_state", "")).strip().lower()
        if snapshot_prefix not in ("start", "end"):
            return False, f"warmup_behavior {side_name} snapshot prefix mismatch", {}
        if not isinstance(snapshot_paths, dict):
            return False, f"warmup_behavior {side_name} missing snapshot_paths", {}
        for field in ("metrics", "bus_observability", "graphql_bus_watch", "feature_flags"):
            if not isinstance(snapshot_paths.get(field), str) or snapshot_paths[field].strip() == "":
                return False, f"warmup_behavior {side_name} missing {field} snapshot path", {}
        if not isinstance(bus_observability, dict):
            return False, f"warmup_behavior {side_name} missing bus_observability snapshot", {}
        if not isinstance(graphql_bus_watch, dict):
            return False, f"warmup_behavior {side_name} missing graphql_bus_watch snapshot", {}
        if not isinstance(feature_flag_snapshot, dict):
            return False, f"warmup_behavior {side_name} missing feature_flag_snapshot", {}
        if not isinstance(timestamps, dict):
            return False, f"warmup_behavior {side_name} missing timestamps snapshot", {}

        for timestamp_source in ("bus_observability", "graphql_bus_watch"):
            source_timestamps = timestamps.get(timestamp_source)
            if not isinstance(source_timestamps, dict):
                return False, f"warmup_behavior {side_name} missing {timestamp_source} timestamps", {}
            for timestamp_field in (
                "summary_last_updated_at",
                "status_last_updated_at",
                "startup_last_updated_at",
            ):
                if not isinstance(source_timestamps.get(timestamp_field), str) or str(
                    source_timestamps.get(timestamp_field, "")
                ).strip() == "":
                    return False, (
                        f"warmup_behavior {side_name} missing {timestamp_source}.{timestamp_field}"
                    ), {}
        graphql_timestamps = timestamps.get("graphql_bus_watch")
        if not isinstance(graphql_timestamps, dict):
            return False, f"warmup_behavior {side_name} missing graphql_bus_watch timestamps", {}
        if not isinstance(graphql_timestamps.get("feature_flags_last_updated_at"), str) or str(
            graphql_timestamps.get("feature_flags_last_updated_at", "")
        ).strip() == "":
            return False, f"warmup_behavior {side_name} missing graphql_bus_watch.feature_flags_last_updated_at", {}
        if not isinstance(graphql_timestamps.get("watch_summary_last_updated_at"), str) or str(
            graphql_timestamps.get("watch_summary_last_updated_at", "")
        ).strip() == "":
            return False, f"warmup_behavior {side_name} missing graphql_bus_watch.watch_summary_last_updated_at", {}

        summary = bus_observability.get("summary")
        if not isinstance(summary, dict):
            return False, f"warmup_behavior {side_name} missing bus summary", {}
        status = summary.get("status")
        if not isinstance(status, dict):
            return False, f"warmup_behavior {side_name} missing bus status", {}
        startup = status.get("startup")
        if not isinstance(startup, dict):
            return False, f"warmup_behavior {side_name} missing startup snapshot", {}
        startup_phase_raw = str(startup.get("phase", "")).strip().upper()
        if startup_phase_raw == "":
            return False, f"warmup_behavior {side_name} missing startup phase", {}

        data = graphql_bus_watch.get("data")
        if not isinstance(data, dict):
            return False, f"warmup_behavior {side_name} missing graphql data", {}
        bus_summary = data.get("busSummary")
        if not isinstance(bus_summary, dict):
            return False, f"warmup_behavior {side_name} missing graphql busSummary", {}
        graphql_status = bus_summary.get("status")
        if not isinstance(graphql_status, dict):
            return False, f"warmup_behavior {side_name} missing graphql status", {}
        warmup = graphql_status.get("warmup")
        if not isinstance(warmup, dict):
            return False, f"warmup_behavior {side_name} missing graphql warmup", {}
        warmup_state_raw = str(warmup.get("state", "")).strip().lower()
        if warmup_state_raw == "":
            return False, f"warmup_behavior {side_name} missing warmup state", {}

        if startup_phase != startup_phase_raw:
            return False, f"warmup_behavior {side_name} startup phase mismatch", {}
        if warmup_state != warmup_state_raw:
            return False, f"warmup_behavior {side_name} warmup state mismatch", {}

        if side_name == "cold_start":
            if startup_phase_raw == "LIVE_READY":
                return False, "warmup_behavior cold_start is not pre-LIVE_READY", {}
            if warmup_state_raw == "available":
                return False, "warmup_behavior cold_start is not pre-available", {}
        else:
            if startup_phase_raw != "LIVE_READY":
                return False, "warmup_behavior post_warmup is not LIVE_READY", {}
            if warmup_state_raw != "available":
                return False, "warmup_behavior post_warmup is not warmup available", {}

    start_paths = evidence.get("start_snapshot_paths")
    end_paths = evidence.get("end_snapshot_paths")
    interval_paths = evidence.get("interval_snapshot_paths")
    structured_prefixes = evidence.get("structured_snapshot_prefixes")
    if not isinstance(start_paths, dict):
        return False, "warmup_behavior missing start structured evidence", {}
    if not isinstance(end_paths, dict):
        return False, "warmup_behavior missing end structured evidence", {}
    if not isinstance(interval_paths, list):
        return False, "warmup_behavior missing interval structured evidence", {}
    if not isinstance(structured_prefixes, list):
        return False, "warmup_behavior missing structured snapshot prefixes", {}
    if len(interval_paths) < 1:
        return False, "warmup_behavior transition lacks structured interval evidence", {}

    interval_snapshot_count = transition.get("interval_snapshot_count")
    interval_snapshot_prefixes = transition.get("interval_snapshot_prefixes")
    if transition.get("established") is not True:
        return False, "warmup_behavior transition is not established", {}
    if not isinstance(interval_snapshot_count, int) or interval_snapshot_count < 1:
        return False, "warmup_behavior transition lacks structured interval count", {}
    if not isinstance(interval_snapshot_prefixes, list) or len(interval_snapshot_prefixes) < 1:
        return False, "warmup_behavior transition lacks interval snapshot prefixes", {}

    return (
        True,
        "",
        {
            "interval_snapshot_count": interval_snapshot_count,
            "interval_snapshot_prefixes": interval_snapshot_prefixes,
            "cold_start_snapshot_prefix": cold_start.get("snapshot_prefix"),
            "post_warmup_snapshot_prefix": post_warmup.get("snapshot_prefix"),
            "cold_start_startup_phase": cold_start.get("startup_phase"),
            "post_warmup_startup_phase": post_warmup.get("startup_phase"),
            "cold_start_warmup_state": cold_start.get("warmup_state"),
            "post_warmup_warmup_state": post_warmup.get("warmup_state"),
        },
    )


def evaluate_read_avoidance_accounting(payload: Any) -> Tuple[bool, str, Dict[str, Any]]:
    if not isinstance(payload, dict):
        return False, "missing read_avoidance_accounting payload", {}
    delta_totals = payload.get("delta_totals")
    if not isinstance(delta_totals, dict):
        return False, "read_avoidance_accounting missing delta_totals", {}

    direct_delta = delta_totals.get(READ_AVOIDANCE_DIRECT_APPLY_METRIC)
    avoided_delta = delta_totals.get(READ_AVOIDANCE_ACTIVE_AVOIDED_METRIC)
    if not isinstance(direct_delta, (int, float)):
        return False, f"read_avoidance_accounting missing numeric {READ_AVOIDANCE_DIRECT_APPLY_METRIC} delta", {}
    if not isinstance(avoided_delta, (int, float)):
        return (
            False,
            f"read_avoidance_accounting missing numeric {READ_AVOIDANCE_ACTIVE_AVOIDED_METRIC} delta",
            {},
        )
    direct_value = float(direct_delta)
    avoided_value = float(avoided_delta)
    if not math.isfinite(direct_value) or direct_value < 0:
        return False, f"invalid {READ_AVOIDANCE_DIRECT_APPLY_METRIC} delta {direct_value!r}", {}
    if not math.isfinite(avoided_value) or avoided_value < 0:
        return False, f"invalid {READ_AVOIDANCE_ACTIVE_AVOIDED_METRIC} delta {avoided_value!r}", {}
    if avoided_value + 1e-9 < direct_value:
        return (
            False,
            f"incoherent accounting: delta {READ_AVOIDANCE_ACTIVE_AVOIDED_METRIC}={avoided_value} "
            f"< delta {READ_AVOIDANCE_DIRECT_APPLY_METRIC}={direct_value}",
            {},
        )
    details = {
        "direct_apply_total_delta": direct_value,
        "active_reads_avoided_total_delta": avoided_value,
    }
    return True, "", details


def evaluate_proof_window_traffic_minimums(payload: Any) -> Tuple[bool, str, Dict[str, Any]]:
    if not isinstance(payload, dict):
        return False, "missing proof_window_traffic_minimums payload", {}
    delta_totals = payload.get("delta_totals")
    if not isinstance(delta_totals, dict):
        return False, "proof_window_traffic_minimums missing delta_totals", {}

    completed_delta = delta_totals.get(PROOF_WINDOW_COMPLETED_TRANSACTIONS_METRIC)
    candidates_delta = delta_totals.get(PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_METRIC)
    if not isinstance(completed_delta, (int, float)):
        return False, f"proof_window_traffic_minimums missing numeric {PROOF_WINDOW_COMPLETED_TRANSACTIONS_METRIC} delta", {}
    if not isinstance(candidates_delta, (int, float)):
        return (
            False,
            f"proof_window_traffic_minimums missing numeric {PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_METRIC} delta",
            {},
        )

    completed_value = float(completed_delta)
    candidates_value = float(candidates_delta)
    if not math.isfinite(completed_value) or completed_value < 0:
        return False, f"invalid {PROOF_WINDOW_COMPLETED_TRANSACTIONS_METRIC} delta {completed_value!r}", {}
    if not math.isfinite(candidates_value) or candidates_value < 0:
        return False, f"invalid {PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_METRIC} delta {candidates_value!r}", {}

    completed_ok = completed_value + 1e-9 >= PROOF_WINDOW_COMPLETED_TRANSACTIONS_MIN_DELTA
    candidates_ok = candidates_value + 1e-9 >= PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_MIN_DELTA
    if not completed_ok:
        return (
            False,
            f"proof-window traffic minimum not met: {PROOF_WINDOW_COMPLETED_TRANSACTIONS_METRIC} "
            f"delta={completed_value} < {PROOF_WINDOW_COMPLETED_TRANSACTIONS_MIN_DELTA}",
            {},
        )
    if not candidates_ok:
        return (
            False,
            f"proof-window traffic minimum not met: {PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_METRIC} "
            f"delta={candidates_value} < {PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_MIN_DELTA}",
            {},
        )

    return (
        True,
        "",
        {
            "completed_transactions_delta": completed_value,
            "completed_transactions_minimum": PROOF_WINDOW_COMPLETED_TRANSACTIONS_MIN_DELTA,
            "direct_apply_candidates_evaluated_delta": candidates_value,
            "direct_apply_candidates_evaluated_minimum": PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_MIN_DELTA,
        },
    )


def evaluate_feature_flag_consistency(payload: Any) -> Tuple[bool, str, Dict[str, Any]]:
    if not isinstance(payload, dict):
        return False, "missing feature_flag_consistency payload", {}
    snapshots = payload.get("snapshots")
    if not isinstance(snapshots, list):
        return False, "feature_flag_consistency missing snapshots", {}
    if len(snapshots) == 0:
        return False, "feature_flag_consistency missing snapshots", {}

    previous_key: str | None = None
    previous_phase: str | None = None
    phase_names: List[str] = []
    for index, snapshot in enumerate(snapshots):
        if not isinstance(snapshot, dict):
            return False, f"feature_flag_consistency snapshot[{index}] must be a JSON object", {}
        phase = str(snapshot.get("phase", "")).strip()
        if phase == "":
            return False, f"feature_flag_consistency snapshot[{index}] missing phase", {}
        phase_names.append(phase)
        graphql_state = snapshot.get("graphql_feature_flags")
        bus_state = snapshot.get("bus_observability_feature_flags")
        canonical_state = snapshot.get("canonical_feature_flags")
        if not isinstance(graphql_state, dict):
            return False, f"feature_flag_consistency snapshot {phase} missing graphql_feature_flags", {}
        if not isinstance(bus_state, dict):
            return False, f"feature_flag_consistency snapshot {phase} missing bus_observability_feature_flags", {}
        if not isinstance(canonical_state, dict):
            return False, f"feature_flag_consistency snapshot {phase} missing canonical_feature_flags", {}

        snapshot_path = pathlib.Path(str(snapshot.get("feature_flags_snapshot_path", "")).strip() or ".")
        try:
            canonical_graphql_key = canonical_feature_flag_key(
                normalize_feature_flag_state(graphql_state, snapshot_path, "graphql")
            )
            canonical_bus_key = canonical_feature_flag_key(
                normalize_feature_flag_state(bus_state, snapshot_path, "bus_observability")
            )
            canonical_state_key = canonical_feature_flag_key(
                normalize_feature_flag_state(canonical_state, snapshot_path, "canonical")
            )
        except Exception as exc:
            return False, str(exc), {}
        if canonical_graphql_key != canonical_state_key or canonical_bus_key != canonical_state_key:
            return (
                False,
                f"feature_flag_consistency snapshot {phase} is malformed or internally inconsistent",
                {},
            )

        if previous_key is not None and canonical_state_key != previous_key:
            drift_field = None
            previous_state = snapshots[index - 1]["canonical_feature_flags"]
            for field in FEATURE_FLAG_FIELDS:
                if previous_state.get(field) != canonical_state.get(field):
                    drift_field = field
                    break
            return (
                False,
                f"feature flag drift detected across proof window: {drift_field or 'unknown field'} changed "
                f"at phase {phase} (previous phase {previous_phase})",
                {
                    "drift_field": drift_field,
                    "previous_phase": previous_phase,
                    "current_phase": phase,
                },
            )
        previous_key = canonical_state_key
        previous_phase = phase

    return (
        True,
        "",
        {
            "snapshot_count": len(snapshots),
            "phases": phase_names,
        },
    )


def load_replay_behavior_artifact(path: pathlib.Path) -> Dict[str, Any]:
    if not path.exists():
        raise ValueError(f"missing replay behavior artifact at {path}")
    payload = load_json(path)
    if not isinstance(payload, dict):
        raise ValueError("replay behavior artifact must be a JSON object")
    if str(payload.get("schema", "")).strip() != REPLAY_BEHAVIOR_ARTIFACT_SCHEMA:
        raise ValueError("replay behavior artifact schema mismatch")
    if str(payload.get("source", "")).strip() != "go_replay_harness":
        raise ValueError("replay behavior artifact source mismatch")
    if not isinstance(payload.get("ok"), bool):
        raise ValueError("replay behavior artifact missing boolean ok")
    cases = payload.get("cases")
    if not isinstance(cases, list):
        raise ValueError("replay behavior artifact missing cases array")
    return payload


def build_replay_falsification_verdict(
    corpus: Any,
    proof_dir: pathlib.Path,
    behavior_artifact_path: pathlib.Path | None = None,
) -> Dict[str, Any]:
    if not isinstance(corpus, dict):
        raise ValueError("replay corpus must be a JSON object")
    if not isinstance(proof_dir, pathlib.Path):
        raise ValueError("proof_dir must be a pathlib.Path")
    if behavior_artifact_path is None:
        behavior_artifact_path = proof_dir / "replay_behavior.json"
    elif not isinstance(behavior_artifact_path, pathlib.Path):
        raise ValueError("behavior_artifact_path must be a pathlib.Path")

    cases = corpus.get("cases")
    if not isinstance(cases, list):
        raise ValueError("replay corpus missing cases array")

    behavior = load_replay_behavior_artifact(behavior_artifact_path)
    behavior_cases_raw = behavior.get("cases")
    if not isinstance(behavior_cases_raw, list):
        raise ValueError("replay behavior artifact missing cases array")
    if not isinstance(behavior.get("ok"), bool):
        raise ValueError("replay behavior artifact missing boolean ok")

    expected_cases: Dict[str, Dict[str, Any]] = {}
    locked_order: List[str] = []
    for index, raw_case in enumerate(cases):
        if not isinstance(raw_case, dict):
            raise ValueError(f"replay case[{index}] must be object")
        expected = raw_case.get("replay_expected")
        if expected is None:
            continue
        if not isinstance(expected, dict):
            raise ValueError(f"replay case[{index}] replay_expected contract must be an object")
        name = str(raw_case.get("name", "")).strip()
        if name == "":
            raise ValueError(f"replay case[{index}] missing name")
        expected_cases[name] = {
            "family": str(raw_case.get("family", "")).strip().upper(),
            "response_class": str(raw_case.get("response_class", "")).strip(),
            "scenario_tags": [
                str(tag).strip()
                for tag in (raw_case.get("scenario_tags") or [])
                if str(tag).strip()
            ],
            "expected": expected,
        }
        locked_order.append(name)

    observed_cases: Dict[str, Dict[str, Any]] = {}
    observed_order: List[str] = []
    for index, raw_case in enumerate(behavior_cases_raw):
        if not isinstance(raw_case, dict):
            raise ValueError(f"replay behavior case[{index}] must be object")
        name = str(raw_case.get("name", "")).strip()
        if name == "":
            raise ValueError(f"replay behavior case[{index}] missing name")
        if name in observed_cases:
            raise ValueError(f"duplicate replay behavior case: {name}")
        observed = raw_case.get("observed")
        if not isinstance(observed, dict):
            raise ValueError(f"replay behavior case[{index}] missing observed object")
        direct_apply = observed.get("direct_apply")
        disposition = str(observed.get("disposition", "")).strip().lower()
        if not isinstance(direct_apply, bool):
            raise ValueError(f"replay behavior case[{index}] missing boolean observed.direct_apply")
        if disposition == "":
            raise ValueError(f"replay behavior case[{index}] missing observed.disposition")
        if disposition not in REPLAY_EXPECTED_DISPOSITIONS:
            raise ValueError(
                f"replay behavior case[{index}] unsupported observed.disposition {disposition!r}"
            )
        observed_cases[name] = {
            "observed": observed,
            "status": str(raw_case.get("status", "")).strip().lower() or "observed",
            "reason": str(raw_case.get("reason", "")).strip(),
        }
        observed_order.append(name)

    expected_names = set(expected_cases)
    missing_names = [name for name in locked_order if name not in observed_cases]
    unexpected_names = [name for name in observed_order if name not in expected_names]
    if missing_names or unexpected_names:
        raise ValueError(
            "replay behavior cases mismatch: "
            f"missing={missing_names or []} unexpected={unexpected_names or []}"
        )

    verdict_cases: List[Dict[str, Any]] = []
    locked_total = 0
    pass_total = 0
    fail_total = 0
    behavior_ok = True
    for name in locked_order:
        case_meta = expected_cases[name]
        family = case_meta["family"]
        response_class = case_meta["response_class"]
        scenario_tags = case_meta["scenario_tags"]
        expected = case_meta["expected"]
        observed_entry = observed_cases.get(name)
        case_result: Dict[str, Any] = {
            "name": name,
            "family": family,
            "response_class": response_class,
            "scenario_tags": scenario_tags,
            "status": "informational",
            "direct_apply": None,
            "disposition": None,
            "reason": "",
            "expected": expected,
            "observed": None,
            "behavior_evidence": {
                "behavior_artifact_path": str(behavior_artifact_path),
                "behavior_artifact_ok": behavior.get("ok"),
                "behavior_schema": str(behavior.get("schema", "")).strip(),
                "case_name": name,
                "observed_present": observed_entry is not None,
                "observed_status": str((observed_entry or {}).get("status", "missing")).strip().lower() or "missing",
                "observed_reason": str((observed_entry or {}).get("reason", "")).strip() or "missing replay behavior observation",
                "observed_direct_apply": None,
                "observed_disposition": None,
            },
        }

        locked_total += 1
        direct_apply_expected = expected.get("direct_apply")
        disposition_expected = str(expected.get("disposition", "")).strip().lower()
        reason_expected = str(expected.get("reason", "")).strip()
        if not isinstance(direct_apply_expected, bool):
            case_result["status"] = "fail"
            case_result["reason"] = "replay_expected direct_apply must be boolean"
            fail_total += 1
            verdict_cases.append(case_result)
            continue
        if direct_apply_expected:
            case_result["status"] = "fail"
            case_result["reason"] = "ambiguous or garbled replay must not be direct_apply eligible"
            fail_total += 1
            verdict_cases.append(case_result)
            continue
        if disposition_expected not in REPLAY_EXPECTED_DISPOSITIONS:
            case_result["status"] = "fail"
            case_result["reason"] = f"unsupported replay disposition {disposition_expected!r}"
            fail_total += 1
            verdict_cases.append(case_result)
            continue

        if observed_entry is None:
            case_result["status"] = "fail"
            case_result["reason"] = f"missing replay behavior observation for {name}"
            fail_total += 1
            behavior_ok = False
            verdict_cases.append(case_result)
            continue

        observed = observed_entry["observed"]
        observed_direct_apply = bool(observed.get("direct_apply", False))
        observed_disposition = str(observed.get("disposition", "")).strip().lower()
        case_result["direct_apply"] = observed_direct_apply
        case_result["disposition"] = observed_disposition
        case_result["reason"] = observed_entry["reason"] or reason_expected
        case_result["observed"] = observed
        case_result["behavior_evidence"]["observed_status"] = observed_entry["status"]
        case_result["behavior_evidence"]["observed_reason"] = case_result["reason"]
        case_result["behavior_evidence"]["observed_direct_apply"] = observed_direct_apply
        case_result["behavior_evidence"]["observed_disposition"] = observed_disposition

        if observed_direct_apply != direct_apply_expected:
            case_result["status"] = "fail"
            case_result["reason"] = (
                f"observed direct_apply={observed_direct_apply!r} "
                f"!= expected {direct_apply_expected!r}"
            )
            fail_total += 1
            behavior_ok = False
            verdict_cases.append(case_result)
            continue
        if observed_disposition != disposition_expected:
            case_result["status"] = "fail"
            case_result["reason"] = (
                f"observed disposition={observed_disposition!r} "
                f"!= expected {disposition_expected!r}"
            )
            fail_total += 1
            behavior_ok = False
            verdict_cases.append(case_result)
            continue

        if family == "B524" and observed_disposition != "ambiguity":
            case_result["status"] = "fail"
            case_result["reason"] = "B524 dual-namespace replay must be reported as ambiguity"
            fail_total += 1
            behavior_ok = False
            verdict_cases.append(case_result)
            continue
        if response_class == "error_or_ambiguous" and observed_disposition != "falsification":
            case_result["status"] = "fail"
            case_result["reason"] = "garbled replay must be reported as falsification"
            fail_total += 1
            behavior_ok = False
            verdict_cases.append(case_result)
            continue

        if family == "B524":
            if observed.get("third_party_eligible") is not True:
                case_result["status"] = "fail"
                case_result["reason"] = "B524 replay observation missing third_party_eligible=true"
                fail_total += 1
                behavior_ok = False
                verdict_cases.append(case_result)
                continue
            if str(observed.get("direct_apply_policy", "")).strip() != "state_default":
                case_result["status"] = "fail"
                case_result["reason"] = "B524 replay observation missing state_default direct_apply_policy"
                fail_total += 1
                behavior_ok = False
                verdict_cases.append(case_result)
                continue
            if str(observed.get("raw_disposition", "")).strip() == "":
                case_result["status"] = "fail"
                case_result["reason"] = "B524 replay observation missing raw_disposition"
                fail_total += 1
                behavior_ok = False
                verdict_cases.append(case_result)
                continue
        else:
            transaction_events = observed.get("transaction_events")
            completed_transactions = observed.get("completed_transactions")
            if not isinstance(transaction_events, int) or transaction_events != 0:
                case_result["status"] = "fail"
                case_result["reason"] = "garbled replay must have zero classified transaction events"
                fail_total += 1
                behavior_ok = False
                verdict_cases.append(case_result)
                continue
            if not isinstance(completed_transactions, int) or completed_transactions != 0:
                case_result["status"] = "fail"
                case_result["reason"] = "garbled replay must have zero completed transactions"
                fail_total += 1
                behavior_ok = False
                verdict_cases.append(case_result)
                continue

        case_result["status"] = "pass"
        pass_total += 1
        verdict_cases.append(case_result)

    ok = fail_total == 0
    return {
        "schema": REPLAY_FALSIFICATION_VERDICT_SCHEMA,
        "captured_at": utc_now(),
        "ok": ok,
        "status": "pass" if ok else "fail",
        "summary": {
            "total_cases": len(verdict_cases),
            "locked_cases": locked_total,
            "pass": pass_total,
            "fail": fail_total,
            "informational": len(verdict_cases) - locked_total,
            "behavior_artifact_ok": behavior_ok,
            "proof_run_ok": behavior_ok,
        },
        "cases": verdict_cases,
    }


def classify_canary_value(
    canary: CanarySpec,
    value_hex: str,
    baseline_hex: str | None,
) -> Tuple[str, str | None]:
    value_hex = normalize_hex(value_hex)
    if canary.expected_hex and value_hex != canary.expected_hex:
        return "mismatch", f"value {value_hex} != expected {canary.expected_hex}"
    if baseline_hex is not None and value_hex != baseline_hex:
        return "mismatch", f"value {value_hex} != baseline {baseline_hex}"
    return "pass", None


def compute_next_sample_epoch(now_epoch: int, interval_sec: int) -> int:
    if interval_sec < 1:
        raise ValueError("interval_sec must be >= 1")
    return now_epoch + interval_sec


def phase_sort_key(phase: str) -> Tuple[int, int, str]:
    normalized = phase.strip().lower()
    if normalized == "start":
        return (0, 0, normalized)
    sample_match = re.fullmatch(r"sample_([0-9]+)", normalized)
    if sample_match:
        return (1, int(sample_match.group(1)), normalized)
    if normalized == "end":
        return (2, 0, normalized)
    return (1, 0, normalized)


def is_interval_phase(phase: str) -> bool:
    return re.fullmatch(r"sample_[0-9]+", phase.strip().lower()) is not None


def safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def verify_phase(
    canaries: List[CanarySpec],
    graphql_url: str,
    phase: str,
    run_id: str,
    retries: int,
    timeout_sec: float,
    baseline_map: Dict[str, str],
    read_avoidance_accounting: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    retries = normalize_retries(retries)
    allow_baseline_seed = is_start_phase(phase)
    results: List[Dict[str, Any]] = []
    for canary in canaries:
        status = "inconclusive"
        reason = ""
        attempts_used = 0
        value_hex = None
        baseline_hex = baseline_map.get(canary.canary_id)
        if baseline_hex is None and not allow_baseline_seed:
            reason = f"missing baseline for phase {phase!r}; baseline can only be seeded during start"
            results.append(
                {
                    "id": canary.canary_id,
                    "family": canary.family,
                    "status": status,
                    "conclusive": False,
                    "attempts_used": attempts_used,
                    "max_retries": retries,
                    "value_hex": value_hex,
                    "baseline_hex": baseline_hex,
                    "expected_hex": canary.expected_hex,
                    "reason": reason,
                }
            )
            continue

        for attempt in range(1, retries + 1):
            attempts_used = attempt
            attempt_nonce = f"{run_id}:{phase}:{canary.canary_id}:{attempt}"
            try:
                candidate_hex = invoke_canary(
                    graphql_url,
                    canary,
                    timeout_sec,
                    nonce=attempt_nonce,
                )
                value_hex = normalize_hex(candidate_hex)
                status, reason = classify_canary_value(canary, value_hex, baseline_hex)
                if baseline_hex is None and allow_baseline_seed:
                    baseline_map[canary.canary_id] = value_hex
                    baseline_hex = value_hex
                break
            except Exception as exc:  # noqa: PERF203 - keep explicit retries simple.
                reason = str(exc)

        conclusive = status in ("pass", "mismatch")
        results.append(
            {
                "id": canary.canary_id,
                "family": canary.family,
                "status": status,
                "conclusive": conclusive,
                "attempts_used": attempts_used,
                "max_retries": retries,
                "value_hex": value_hex,
                "baseline_hex": baseline_hex,
                "expected_hex": canary.expected_hex,
                "reason": reason,
            }
        )

    summary = {
        "total": len(results),
        "pass": sum(1 for item in results if item["status"] == "pass"),
        "mismatch": sum(1 for item in results if item["status"] == "mismatch"),
        "inconclusive": sum(1 for item in results if item["status"] == "inconclusive"),
    }
    summary["conclusive"] = summary["pass"] + summary["mismatch"]

    return {
        "schema": "p03_canary_phase_result_v1",
        "captured_at": utc_now(),
        "run_id": run_id,
        "phase": phase,
        "verification_mode": "active_direct_read",
        "read_avoidance_accounting": read_avoidance_accounting
        or {
            "authoritative": False,
            "reason": "read-avoidance accounting is only reconstructed from proof window artifacts",
        },
        "results": results,
        "summary": summary,
    }


def collect_phase_files_for_run(proof_dir: pathlib.Path, run_id: str) -> Tuple[List[pathlib.Path], int]:
    stale_ignored = 0
    selected: List[pathlib.Path] = []
    pattern = str(proof_dir / f"{CANARY_PHASE_PREFIX}*.json")
    for candidate in sorted(glob.glob(pattern)):
        path = pathlib.Path(candidate)
        try:
            payload = load_json(path)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        if str(payload.get("run_id", "")).strip() != run_id:
            stale_ignored += 1
            continue
        selected.append(path)
    return selected, stale_ignored


def summarize_run(
    proof_dir: pathlib.Path,
    run_id: str,
    require_interval_phase: bool = True,
) -> Dict[str, Any]:
    phase_files, stale_ignored = collect_phase_files_for_run(proof_dir, run_id)
    if not phase_files:
        raise ValueError("no current-run canary phase artifacts found (stale artifacts rejected)")

    phases_seen = set()
    interval_phase_count = 0
    totals = {"results": 0, "pass": 0, "mismatch": 0, "inconclusive": 0, "conclusive": 0}
    interval_totals = {"results": 0, "pass": 0, "mismatch": 0, "inconclusive": 0, "conclusive": 0}
    per_canary: Dict[str, Dict[str, Any]] = {}
    per_canary_interval: Dict[str, Dict[str, Any]] = {}
    phase_payloads: List[Tuple[Tuple[int, int, str], str, Dict[str, Any]]] = []
    for path in phase_files:
        payload = load_json(path)
        if not isinstance(payload, dict):
            continue
        phase = str(payload.get("phase", "")).strip()
        if phase:
            phases_seen.add(phase)
            if is_interval_phase(phase):
                interval_phase_count += 1
        phase_payloads.append((phase_sort_key(phase), phase, payload))

    for _, phase, payload in sorted(phase_payloads, key=lambda item: item[0]):
        interval_phase = is_interval_phase(phase)
        entries = payload.get("results")
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            canary_id = str(entry.get("id", "")).strip()
            status = str(entry.get("status", "")).strip()
            if canary_id == "" or status not in ("pass", "mismatch", "inconclusive"):
                continue
            totals["results"] += 1
            totals[status] += 1
            if status in ("pass", "mismatch"):
                totals["conclusive"] += 1
            canary_bucket = per_canary.setdefault(
                canary_id,
                {"pass": 0, "mismatch": 0, "inconclusive": 0, "conclusive": 0, "last_status": ""},
            )
            canary_bucket[status] += 1
            if status in ("pass", "mismatch"):
                canary_bucket["conclusive"] += 1
            canary_bucket["last_status"] = status

            if interval_phase:
                interval_totals["results"] += 1
                interval_totals[status] += 1
                if status in ("pass", "mismatch"):
                    interval_totals["conclusive"] += 1
                canary_interval_bucket = per_canary_interval.setdefault(
                    canary_id,
                    {"pass": 0, "mismatch": 0, "inconclusive": 0, "conclusive": 0},
                )
                canary_interval_bucket[status] += 1
                if status in ("pass", "mismatch"):
                    canary_interval_bucket["conclusive"] += 1

    if "start" not in phases_seen or "end" not in phases_seen:
        raise ValueError("missing current-run start/end canary artifacts (stale artifact rejection)")
    if require_interval_phase and interval_phase_count < 1:
        raise ValueError("missing current-run interval canary artifacts (no elapsed sample phase)")
    ordered_phases = [phase for _, phase, _ in sorted(phase_payloads, key=lambda item: item[0])]
    read_avoidance_accounting = build_window_read_avoidance_accounting_for_phases(proof_dir, ordered_phases)
    proof_window_traffic_minimums = read_avoidance_accounting.get("proof_window_traffic_minimums")
    feature_flag_consistency = build_window_feature_flag_consistency_for_phases(proof_dir, ordered_phases)
    warmup_behavior = build_warmup_behavior_artifact_for_phases(
        proof_dir,
        run_id,
        require_interval_phase,
    )

    return {
        "schema": "p03_canary_overall_summary_v1",
        "captured_at": utc_now(),
        "run_id": run_id,
        "verification_mode": "active_direct_read",
        "read_avoidance_accounting": read_avoidance_accounting,
        "proof_window_traffic_minimums": proof_window_traffic_minimums,
        "feature_flag_consistency": feature_flag_consistency,
        "warmup_behavior": warmup_behavior,
        "phase_files_total": len(list((proof_dir).glob(f"{CANARY_PHASE_PREFIX}*.json"))),
        "phase_files_used": len(phase_files),
        "phase_files_stale_ignored": stale_ignored,
        "phases_seen": sorted(phases_seen),
        "interval_phase_count": interval_phase_count,
        "interval_phase_required": require_interval_phase,
        "totals": totals,
        "interval_totals": interval_totals,
        "per_canary": per_canary,
        "per_canary_interval": per_canary_interval,
        "overall_conclusive_count": totals["conclusive"],
        "overall_interval_conclusive_count": interval_totals["conclusive"],
    }


def build_canary_verdict(summary: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(summary, dict):
        raise ValueError("summary must be a JSON object")

    totals = summary.get("totals")
    if not isinstance(totals, dict):
        totals = {}
    interval_totals = summary.get("interval_totals")
    if not isinstance(interval_totals, dict):
        interval_totals = {}
    per_canary = summary.get("per_canary")
    if not isinstance(per_canary, dict):
        per_canary = {}
    per_canary_interval = summary.get("per_canary_interval")
    if not isinstance(per_canary_interval, dict):
        per_canary_interval = {}
    read_avoidance_ok, read_avoidance_reason, read_avoidance_details = evaluate_read_avoidance_accounting(
        summary.get("read_avoidance_accounting")
    )
    proof_window_ok, proof_window_reason, proof_window_details = evaluate_proof_window_traffic_minimums(
        summary.get("proof_window_traffic_minimums")
    )
    feature_flags_ok, feature_flags_reason, feature_flags_details = evaluate_feature_flag_consistency(
        summary.get("feature_flag_consistency")
    )
    warmup_behavior_ok, warmup_behavior_reason, warmup_behavior_details = evaluate_warmup_behavior(
        summary.get("warmup_behavior")
    )

    mismatch_count = int(totals.get("mismatch", 0) or 0)
    no_mismatches_ok = mismatch_count == 0

    interval_required = bool(summary.get("interval_phase_required", True))
    overall_interval_total = int(interval_totals.get("results", 0) or 0)
    overall_interval_conclusive = int(interval_totals.get("conclusive", 0) or 0)
    overall_interval_rate = safe_ratio(overall_interval_conclusive, overall_interval_total)
    if interval_required:
        overall_interval_ok = overall_interval_rate >= OVERALL_INTERVAL_CONCLUSIVE_MIN
        overall_interval_waived = False
    else:
        overall_interval_ok = True
        overall_interval_waived = True
    warmup_behavior_required = interval_required
    if warmup_behavior_required:
        warmup_behavior_gate_ok = warmup_behavior_ok
        warmup_behavior_waived = False
    else:
        warmup_behavior_gate_ok = True
        warmup_behavior_waived = True

    per_canary_details: Dict[str, Dict[str, Any]] = {}
    failing_canaries: List[str] = []
    for canary_id in sorted(per_canary.keys()):
        bucket = per_canary_interval.get(canary_id)
        if not isinstance(bucket, dict):
            bucket = {}
        pass_count = int(bucket.get("pass", 0) or 0)
        mismatch_bucket = int(bucket.get("mismatch", 0) or 0)
        inconclusive_count = int(bucket.get("inconclusive", 0) or 0)
        interval_total = pass_count + mismatch_bucket + inconclusive_count
        interval_conclusive = int(bucket.get("conclusive", pass_count + mismatch_bucket) or 0)
        interval_rate = safe_ratio(interval_conclusive, interval_total)
        if interval_required:
            canary_ok = interval_rate >= PER_CANARY_INTERVAL_CONCLUSIVE_MIN
            canary_waived = False
        else:
            canary_ok = True
            canary_waived = True
        if not canary_ok:
            failing_canaries.append(canary_id)
        per_canary_details[canary_id] = {
            "interval_conclusive": interval_conclusive,
            "interval_total": interval_total,
            "interval_conclusive_rate": interval_rate,
            "threshold": PER_CANARY_INTERVAL_CONCLUSIVE_MIN,
            "ok": canary_ok,
            "waived": canary_waived,
        }

    per_canary_ok = len(failing_canaries) == 0
    verdict_ok = (
        no_mismatches_ok
        and overall_interval_ok
        and per_canary_ok
        and read_avoidance_ok
        and proof_window_ok
        and feature_flags_ok
        and warmup_behavior_gate_ok
    )

    return {
        "schema": CANARY_VERDICT_SCHEMA,
        "captured_at": utc_now(),
        "run_id": str(summary.get("run_id", "")).strip(),
        "summary_schema": str(summary.get("schema", "")).strip(),
        "ok": verdict_ok,
        "status": "pass" if verdict_ok else "fail",
        "criteria": {
            "no_mismatches": {
                "ok": no_mismatches_ok,
                "mismatch_count": mismatch_count,
            },
            "overall_interval_conclusive_rate": {
                "ok": overall_interval_ok,
                "waived": overall_interval_waived,
                "interval_conclusive": overall_interval_conclusive,
                "interval_total": overall_interval_total,
                "interval_conclusive_rate": overall_interval_rate,
                "threshold": OVERALL_INTERVAL_CONCLUSIVE_MIN,
            },
            "per_canary_interval_conclusive_rate": {
                "ok": per_canary_ok,
                "waived": not interval_required,
                "threshold": PER_CANARY_INTERVAL_CONCLUSIVE_MIN,
                "failing_canaries": failing_canaries,
                "canaries_evaluated": len(per_canary_details),
            },
            "read_avoidance_accounting": {
                "ok": read_avoidance_ok,
                "reason": read_avoidance_reason,
                **read_avoidance_details,
            },
            "proof_window_traffic_minimums": {
                "ok": proof_window_ok,
                "reason": proof_window_reason,
                **proof_window_details,
            },
            "feature_flag_consistency": {
                "ok": feature_flags_ok,
                "reason": feature_flags_reason,
                **feature_flags_details,
            },
            "warmup_behavior": {
                "ok": warmup_behavior_gate_ok,
                "waived": warmup_behavior_waived,
                "reason": warmup_behavior_reason,
                **warmup_behavior_details,
            },
        },
        "per_canary": per_canary_details,
    }


def load_baseline_map(path: pathlib.Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    payload = load_json(path)
    if not isinstance(payload, dict):
        raise ValueError(f"baseline file must be object: {path}")
    output: Dict[str, str] = {}
    for key, value in payload.items():
        output[str(key)] = normalize_hex(str(value))
    return output


def validate_command(args: argparse.Namespace) -> int:
    _, canaries = load_and_validate_manifest(pathlib.Path(args.manifest), args.require_case_id)
    summary = {
        "ok": True,
        "manifest": str(pathlib.Path(args.manifest)),
        "total_canaries": len(canaries),
        "family_counts": {
            "B524": sum(1 for item in canaries if item.family == "B524"),
            "B509": sum(1 for item in canaries if item.family == "B509"),
        },
    }
    sys.stdout.write(json.dumps(summary, sort_keys=True) + "\n")
    return 0


def verify_phase_command(args: argparse.Namespace) -> int:
    _, canaries = load_and_validate_manifest(pathlib.Path(args.manifest), args.require_case_id)
    baseline_path = pathlib.Path(args.baseline)
    output_path = pathlib.Path(args.output)
    read_avoidance_accounting = build_phase_read_avoidance_observation(
        output_path=output_path,
        phase=args.phase,
        run_id=args.run_id,
    )
    baseline_map = load_baseline_map(baseline_path)
    phase_result = verify_phase(
        canaries=canaries,
        graphql_url=args.graphql_url,
        phase=args.phase,
        run_id=args.run_id,
        retries=args.retries,
        timeout_sec=args.timeout_sec,
        baseline_map=baseline_map,
        read_avoidance_accounting=read_avoidance_accounting,
    )
    write_json(output_path, phase_result)
    write_json(baseline_path, baseline_map)
    return 0


def summarize_command(args: argparse.Namespace) -> int:
    require_interval_phase = str(args.require_interval_phase).strip() != "0"
    summary = summarize_run(
        pathlib.Path(args.proof_dir),
        args.run_id,
        require_interval_phase=require_interval_phase,
    )
    write_json(pathlib.Path(args.output), summary)
    return 0


def verdict_command(args: argparse.Namespace) -> int:
    summary = load_json(pathlib.Path(args.summary))
    verdict = build_canary_verdict(summary)
    write_json(pathlib.Path(args.output), verdict)
    return 0 if bool(verdict.get("ok", False)) else 1


def replay_verdict_command(args: argparse.Namespace) -> int:
    corpus = load_json(pathlib.Path(args.manifest))
    behavior_artifact_path = pathlib.Path(args.behavior_artifact) if args.behavior_artifact else None
    verdict = build_replay_falsification_verdict(
        corpus,
        pathlib.Path(args.proof_dir),
        behavior_artifact_path=behavior_artifact_path,
    )
    write_json(pathlib.Path(args.output), verdict)
    return 0 if bool(verdict.get("ok", False)) else 1


def family_eligibility_command(args: argparse.Namespace) -> int:
    artifact = build_family_proof_eligibility_artifact_for_run(
        pathlib.Path(args.proof_dir),
        args.run_id,
        args.case_id,
        args.kind,
        args.passive_mode,
        args.gateway_transport,
        proxy_transport=args.proxy_transport,
        ebusd_transport=args.ebusd_transport,
    )
    write_json(pathlib.Path(args.output), artifact)
    status = str((((artifact.get("eligibility") or {})).get("status", ""))).strip().lower()
    return 0 if status in ("proven_for_default_flip", "not_proven") else 1


def promotion_eligibility_command(args: argparse.Namespace) -> int:
    artifact = build_promotion_eligibility_artifact_for_run(
        pathlib.Path(args.proof_dir),
        args.run_id,
        args.case_id,
        args.kind,
        args.passive_mode,
        args.gateway_transport,
        proxy_transport=args.proxy_transport,
        ebusd_transport=args.ebusd_transport,
    )
    write_json(pathlib.Path(args.output), artifact)
    status = str((((artifact.get("eligibility") or {})).get("status", ""))).strip().lower()
    return 0 if status in ("eligible_for_default_flip", "not_proven") else 1


def publisher_cadence_command(args: argparse.Namespace) -> int:
    artifact = build_publisher_cadence_artifact_for_phases(
        pathlib.Path(args.proof_dir),
        args.run_id,
    )
    write_json(pathlib.Path(args.output), artifact)
    return 0 if bool(artifact.get("ok", False)) else 1


def cross_plane_skew_command(args: argparse.Namespace) -> int:
    publisher_cadence_path = pathlib.Path(args.publisher_cadence) if args.publisher_cadence else None
    artifact = build_cross_plane_skew_artifact_for_phases(
        pathlib.Path(args.proof_dir),
        args.run_id,
        args.configured_proof_sample_interval_sec,
        publisher_cadence_path=publisher_cadence_path,
    )
    write_json(pathlib.Path(args.output), artifact)
    return 0 if bool(artifact.get("ok", False)) else 1


def timing_reference_verdict_command(args: argparse.Namespace) -> int:
    wire_reference_path = (
        pathlib.Path(args.wire_reference_path)
        if args.wire_reference_path
        else pathlib.Path(args.proof_dir) / "wire_timing_reference.json"
    )
    artifact = build_timing_reference_verdict(
        pathlib.Path(args.proof_dir),
        args.run_id,
        wire_reference_path=wire_reference_path,
    )
    write_json(pathlib.Path(args.output), artifact)
    return 0 if bool(artifact.get("ok", False)) else 1


def rollback_execution_command(args: argparse.Namespace) -> int:
    artifact = build_rollback_execution_artifact(
        run_id=args.run_id,
        case_id=args.case_id,
        exec_case_id=args.exec_case_id,
        gateway_base_url=args.gateway_base_url,
        remote_case_dir=args.remote_case_dir,
        proof_gateway_log_path=args.proof_gateway_log_path,
        rollback_gateway_log_path=args.rollback_gateway_log_path,
        started_at=args.started_at,
        completed_at=args.completed_at,
        ok=parse_cli_bool(args.ok, "ok"),
        reason=args.reason,
        restart_exit_code=args.restart_exit_code,
        restart_succeeded=parse_cli_bool(args.restart_succeeded, "restart_succeeded"),
        gateway_health_check_ok=parse_cli_bool(args.gateway_health_check_ok, "gateway_health_check_ok"),
        source=args.source,
        action=args.action,
    )
    write_json(pathlib.Path(args.output), artifact)
    return 0


def validate_rollback_execution_command(args: argparse.Namespace) -> int:
    load_rollback_execution_artifact(pathlib.Path(args.artifact), args.run_id)
    return 0


def rollback_result_command(args: argparse.Namespace) -> int:
    artifact = build_rollback_result_artifact(pathlib.Path(args.proof_dir), str(args.run_id).strip())
    write_json(pathlib.Path(args.output), artifact)
    return 0 if bool(artifact.get("ok", False)) else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="P03 canary manifest verifier")
    sub = parser.add_subparsers(dest="command", required=True)

    validate = sub.add_parser("validate-manifest", help="validate canary manifest")
    validate.add_argument("--manifest", required=True)
    validate.add_argument("--require-case-id", default=None)
    validate.set_defaults(func=validate_command)

    verify = sub.add_parser("verify-phase", help="verify one canary phase")
    verify.add_argument("--manifest", required=True)
    verify.add_argument("--graphql-url", required=True)
    verify.add_argument("--phase", required=True)
    verify.add_argument("--run-id", required=True)
    verify.add_argument("--baseline", required=True)
    verify.add_argument("--output", required=True)
    verify.add_argument("--retries", type=int, default=MAX_RETRIES)
    verify.add_argument("--timeout-sec", type=float, default=8.0)
    verify.add_argument("--require-case-id", default=None)
    verify.set_defaults(func=verify_phase_command)

    summarize = sub.add_parser("summarize", help="summarize current-run canary artifacts")
    summarize.add_argument("--proof-dir", required=True)
    summarize.add_argument("--run-id", required=True)
    summarize.add_argument("--output", required=True)
    summarize.add_argument("--require-interval-phase", choices=("0", "1"), default="1")
    summarize.set_defaults(func=summarize_command)

    verdict = sub.add_parser("verdict", help="build proof-mode canary verdict from summary")
    verdict.add_argument("--summary", required=True)
    verdict.add_argument("--output", required=True)
    verdict.set_defaults(func=verdict_command)

    replay_verdict = sub.add_parser(
        "replay-verdict",
        help="build replay falsification verdict from replay behavior plus locked corpus expectations",
    )
    replay_verdict.add_argument("--manifest", required=True)
    replay_verdict.add_argument("--proof-dir", required=True)
    replay_verdict.add_argument("--behavior-artifact", default=None)
    replay_verdict.add_argument("--output", required=True)
    replay_verdict.set_defaults(func=replay_verdict_command)

    family_eligibility = sub.add_parser(
        "family-eligibility",
        help="build family-scoped proof eligibility artifact from proof window artifacts",
    )
    family_eligibility.add_argument("--proof-dir", required=True)
    family_eligibility.add_argument("--run-id", required=True)
    family_eligibility.add_argument("--case-id", required=True)
    family_eligibility.add_argument("--kind", required=True)
    family_eligibility.add_argument("--passive-mode", required=True)
    family_eligibility.add_argument("--gateway-transport", required=True)
    family_eligibility.add_argument("--proxy-transport", default="")
    family_eligibility.add_argument("--ebusd-transport", default="")
    family_eligibility.add_argument("--output", required=True)
    family_eligibility.set_defaults(func=family_eligibility_command)

    promotion_eligibility = sub.add_parser(
        "promotion-eligibility",
        help="build promotion eligibility artifact from family proof eligibility and proof window artifacts",
    )
    promotion_eligibility.add_argument("--proof-dir", required=True)
    promotion_eligibility.add_argument("--run-id", required=True)
    promotion_eligibility.add_argument("--case-id", required=True)
    promotion_eligibility.add_argument("--kind", required=True)
    promotion_eligibility.add_argument("--passive-mode", required=True)
    promotion_eligibility.add_argument("--gateway-transport", required=True)
    promotion_eligibility.add_argument("--proxy-transport", default="")
    promotion_eligibility.add_argument("--ebusd-transport", default="")
    promotion_eligibility.add_argument("--output", required=True)
    promotion_eligibility.set_defaults(func=promotion_eligibility_command)

    publisher_cadence = sub.add_parser(
        "publisher-cadence",
        help="build publisher cadence proof artifact from proof window snapshots",
    )
    publisher_cadence.add_argument("--proof-dir", required=True)
    publisher_cadence.add_argument("--run-id", required=True)
    publisher_cadence.add_argument("--output", required=True)
    publisher_cadence.set_defaults(func=publisher_cadence_command)

    cross_plane_skew = sub.add_parser(
        "cross-plane-skew",
        help="build cross-plane skew proof artifact from proof window snapshots",
    )
    cross_plane_skew.add_argument("--proof-dir", required=True)
    cross_plane_skew.add_argument("--run-id", required=True)
    cross_plane_skew.add_argument("--configured-proof-sample-interval-sec", type=float, required=True)
    cross_plane_skew.add_argument("--publisher-cadence", default=None)
    cross_plane_skew.add_argument("--output", required=True)
    cross_plane_skew.set_defaults(func=cross_plane_skew_command)

    timing_reference_verdict = sub.add_parser(
        "timing-reference-verdict",
        help="build timing-reference comparator artifact from wire timing evidence",
    )
    timing_reference_verdict.add_argument("--proof-dir", required=True)
    timing_reference_verdict.add_argument("--run-id", required=True)
    timing_reference_verdict.add_argument("--wire-reference-path", default=None)
    timing_reference_verdict.add_argument("--output", required=True)
    timing_reference_verdict.set_defaults(func=timing_reference_verdict_command)

    rollback_execution = sub.add_parser(
        "rollback-execution",
        help="emit bounded rollback execution artifact from a real harness restart",
    )
    rollback_execution.add_argument("--run-id", required=True)
    rollback_execution.add_argument("--case-id", required=True)
    rollback_execution.add_argument("--exec-case-id", required=True)
    rollback_execution.add_argument("--gateway-base-url", required=True)
    rollback_execution.add_argument("--remote-case-dir", required=True)
    rollback_execution.add_argument("--proof-gateway-log-path", required=True)
    rollback_execution.add_argument("--rollback-gateway-log-path", required=True)
    rollback_execution.add_argument("--started-at", required=True)
    rollback_execution.add_argument("--completed-at", required=True)
    rollback_execution.add_argument("--ok", required=True)
    rollback_execution.add_argument("--reason", required=True)
    rollback_execution.add_argument("--restart-exit-code", required=True, type=int)
    rollback_execution.add_argument("--restart-succeeded", required=True)
    rollback_execution.add_argument("--gateway-health-check-ok", required=True)
    rollback_execution.add_argument("--source", required=True)
    rollback_execution.add_argument("--action", required=True)
    rollback_execution.add_argument("--output", required=True)
    rollback_execution.set_defaults(func=rollback_execution_command)

    validate_rollback_execution = sub.add_parser(
        "validate-rollback-execution",
        help="validate rollback execution artifact shape and same-run provenance",
    )
    validate_rollback_execution.add_argument("--artifact", required=True)
    validate_rollback_execution.add_argument("--run-id", required=True)
    validate_rollback_execution.set_defaults(func=validate_rollback_execution_command)

    rollback_result = sub.add_parser(
        "rollback-result",
        help="build rollback result artifact from rollback execution plus bounded snapshots",
    )
    rollback_result.add_argument("--proof-dir", required=True)
    rollback_result.add_argument("--run-id", required=True)
    rollback_result.add_argument("--output", required=True)
    rollback_result.set_defaults(func=rollback_result_command)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except Exception as exc:
        sys.stderr.write(f"canary verifier error: {exc}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
