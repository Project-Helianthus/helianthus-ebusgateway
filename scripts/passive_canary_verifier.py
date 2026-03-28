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
FAMILY_PROOF_ELIGIBILITY_SCHEMA = "p03_family_proof_eligibility_v1"
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


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json(path: pathlib.Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: pathlib.Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


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


def canonicalize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): canonicalize_json_value(value[key]) for key in sorted(value.keys())}
    if isinstance(value, list):
        return [canonicalize_json_value(item) for item in value]
    return value


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
    bus_state = normalize_feature_flag_state(
        payload.get("bus_observability_feature_flags"),
        snapshot_path,
        "bus_observability",
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
        "graphql_feature_flags": graphql_state,
        "bus_observability_feature_flags": bus_state,
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


def load_structured_warmup_snapshot(proof_dir: pathlib.Path, phase: str) -> Dict[str, Any]:
    normalized = phase.strip().lower()
    if normalized == "":
        raise ValueError("unsupported empty canary phase for structured warmup snapshot lookup")

    metrics_path = proof_phase_metrics_snapshot_path(proof_dir, normalized)
    bus_path = proof_phase_bus_observability_snapshot_path(proof_dir, normalized)
    graphql_path = proof_phase_graphql_bus_watch_snapshot_path(proof_dir, normalized)
    feature_flag_path = proof_phase_feature_flag_snapshot_path(proof_dir, normalized)
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
    transport_class = str(graphql_status.get("transportClass", "")).strip()
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
    }


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

    canary_verdict_path = proof_dir / "canary_verdict.json"
    replay_verdict_path = proof_dir / "replay_falsification.json"
    if not canary_verdict_path.exists():
        reasons.append(f"missing canary verdict artifact: {canary_verdict_path}")
        canary_verdict = None
    else:
        canary_verdict = load_json(canary_verdict_path)
        if not isinstance(canary_verdict, dict):
            reasons.append(f"{canary_verdict_path}: canary verdict must be a JSON object")
            canary_verdict = None
    if not replay_verdict_path.exists():
        reasons.append(f"missing replay falsification artifact: {replay_verdict_path}")
        replay_verdict = None
    else:
        replay_verdict = load_json(replay_verdict_path)
        if not isinstance(replay_verdict, dict):
            reasons.append(f"{replay_verdict_path}: replay falsification verdict must be a JSON object")
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
    if start_transport_class == "" and end_transport_class == "":
        reasons.append("missing transport class in structured warmup snapshots")
    elif start_transport_class != "" and end_transport_class != "" and start_transport_class != end_transport_class:
        reasons.append(
            "ambiguous transport class across structured warmup snapshots: "
            f"start={start_transport_class!r} end={end_transport_class!r}"
        )
    transport_class = start_transport_class or end_transport_class

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

    is_p03_family = (
        normalized_kind == "proxy-single-client"
        and normalized_passive_mode == "required"
        and transport_class == "ens"
    )
    family_identity_missing = normalized_kind == "" or normalized_passive_mode == "" or transport_class == ""
    family_identity_ambiguous = any(
        reason.startswith("ambiguous transport class across structured warmup snapshots:")
        for reason in reasons
    )
    if family_identity_missing or family_identity_ambiguous:
        status = "blocked"
    elif not is_p03_family:
        status = "not_proven"
        reasons.append(
            f"family scope mismatch: kind={normalized_kind!r} passive_mode={normalized_passive_mode!r} "
            f"transport_class={transport_class!r}; want proxy-single-client/required/ens"
        )

    if len(reasons) == 0:
        status = "proven_for_default_flip"
    elif status != "not_proven":
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
            "canary_verdict_path": str(canary_verdict_path),
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
        },
        "post_warmup": {
            "snapshot_prefix": post_warmup["snapshot_prefix"],
            "snapshot_paths": post_warmup["snapshot_paths"],
            "bus_observability": post_warmup["bus_observability"],
            "graphql_bus_watch": post_warmup["graphql_bus_watch"],
            "feature_flag_snapshot": post_warmup["feature_flag_snapshot"],
            "startup_phase": post_warmup["startup_phase"],
            "warmup_state": post_warmup["warmup_state"],
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
                "behavior_artifact_ok": bool(behavior.get("ok", False)),
                "behavior_schema": str(behavior.get("schema", "")).strip(),
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
