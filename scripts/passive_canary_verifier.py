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
REPLAY_FALSIFICATION_VERDICT_SCHEMA = "observe_first_replay_falsification_verdict_v1"
OVERALL_INTERVAL_CONCLUSIVE_MIN = 0.90
PER_CANARY_INTERVAL_CONCLUSIVE_MIN = 0.75
CANARY_NONCE_PARAM = "_canary_nonce"
READ_AVOIDANCE_ACCOUNTING_SCHEMA = "p03_read_avoidance_accounting_v1"
READ_AVOIDANCE_DIRECT_APPLY_METRIC = "direct_apply_total"
READ_AVOIDANCE_ACTIVE_AVOIDED_METRIC = "active_reads_avoided_total"
READ_AVOIDANCE_SAVED_SECONDS_METRIC = "active_read_saved_seconds"
PROOF_WINDOW_COMPLETED_TRANSACTIONS_METRIC = "ebus_passive_completed_transactions_total"
PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_METRIC = "ebus_passive_direct_apply_candidates_evaluated_total"
PROOF_WINDOW_COMPLETED_TRANSACTIONS_MIN_DELTA = 1000.0
PROOF_WINDOW_DIRECT_APPLY_CANDIDATES_EVALUATED_MIN_DELTA = 100.0
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


def build_replay_falsification_verdict(corpus: Any, proof_dir: pathlib.Path) -> Dict[str, Any]:
    if not isinstance(corpus, dict):
        raise ValueError("replay corpus must be a JSON object")
    if not isinstance(proof_dir, pathlib.Path):
        raise ValueError("proof_dir must be a pathlib.Path")

    cases = corpus.get("cases")
    if not isinstance(cases, list):
        raise ValueError("replay corpus missing cases array")

    summary_path = proof_dir / "canary_summary.json"
    verdict_path = proof_dir / "canary_verdict.json"
    if not summary_path.exists():
        raise ValueError(f"missing proof summary artifact at {summary_path}")
    if not verdict_path.exists():
        raise ValueError(f"missing proof verdict artifact at {verdict_path}")

    summary = load_json(summary_path)
    verdict = load_json(verdict_path)
    if not isinstance(summary, dict):
        raise ValueError("proof summary must be a JSON object")
    if not isinstance(verdict, dict):
        raise ValueError("proof verdict must be a JSON object")
    summary_run_id = str(summary.get("run_id", "")).strip()
    verdict_run_id = str(verdict.get("run_id", "")).strip()
    if summary_run_id == "" or verdict_run_id == "":
        raise ValueError("proof summary/verdict missing run_id")
    if summary_run_id != verdict_run_id:
        raise ValueError("proof summary/verdict run_id mismatch")
    if str(summary.get("schema", "")).strip() != "p03_canary_overall_summary_v1":
        raise ValueError("proof summary schema mismatch")
    if str(verdict.get("summary_schema", "")).strip() != "p03_canary_overall_summary_v1":
        raise ValueError("proof verdict summary_schema mismatch")

    proof_window_ok, proof_window_reason, proof_window_details = evaluate_proof_window_traffic_minimums(
        summary.get("proof_window_traffic_minimums")
    )
    read_avoidance_ok, read_avoidance_reason, read_avoidance_details = evaluate_read_avoidance_accounting(
        summary.get("read_avoidance_accounting")
    )
    canary_gate_ok = bool(verdict.get("ok", False))
    canary_gate_status = str(verdict.get("status", "")).strip().lower()
    proof_gate_ok = canary_gate_ok and proof_window_ok and read_avoidance_ok and canary_gate_status == "pass"
    proof_gate_reason = ""
    if not canary_gate_ok:
        proof_gate_reason = "proof canary verdict did not pass"
    elif not proof_window_ok:
        proof_gate_reason = proof_window_reason
    elif not read_avoidance_ok:
        proof_gate_reason = read_avoidance_reason
    elif canary_gate_status != "pass":
        proof_gate_reason = f"proof canary verdict status {canary_gate_status!r}"

    verdict_cases: List[Dict[str, Any]] = []
    locked_total = 0
    pass_total = 0
    fail_total = 0
    for index, raw_case in enumerate(cases):
        if not isinstance(raw_case, dict):
            raise ValueError(f"replay case[{index}] must be object")

        name = str(raw_case.get("name", "")).strip()
        family = str(raw_case.get("family", "")).strip().upper()
        response_class = str(raw_case.get("response_class", "")).strip()
        scenario_tags = raw_case.get("scenario_tags")
        if not isinstance(scenario_tags, list):
            scenario_tags = []

        expected = raw_case.get("replay_expected")
        case_result: Dict[str, Any] = {
            "name": name,
            "family": family,
            "response_class": response_class,
            "scenario_tags": [str(tag).strip() for tag in scenario_tags if str(tag).strip()],
            "status": "informational",
            "direct_apply": None,
            "disposition": None,
            "reason": "",
            "proof_evidence": {
                "run_id": summary_run_id,
                "summary_schema": str(summary.get("schema", "")).strip(),
                "canary_verdict_status": canary_gate_status,
                "canary_verdict_ok": canary_gate_ok,
                "read_avoidance_accounting_ok": read_avoidance_ok,
                "read_avoidance_accounting_reason": read_avoidance_reason,
                "proof_window_traffic_minimums_ok": proof_window_ok,
                "proof_window_traffic_minimums_reason": proof_window_reason,
                "read_avoidance_accounting": read_avoidance_details,
                "proof_window_traffic_minimums": proof_window_details,
            },
        }

        if expected is None:
            verdict_cases.append(case_result)
            continue
        if not isinstance(expected, dict):
            case_result["status"] = "fail"
            case_result["reason"] = "replay_expected contract must be an object"
            fail_total += 1
            locked_total += 1
            verdict_cases.append(case_result)
            continue

        locked_total += 1
        direct_apply = expected.get("direct_apply")
        disposition = str(expected.get("disposition", "")).strip().lower()
        reason = str(expected.get("reason", "")).strip()
        case_result["direct_apply"] = direct_apply
        case_result["disposition"] = disposition
        case_result["reason"] = reason

        if not isinstance(direct_apply, bool):
            case_result["status"] = "fail"
            case_result["reason"] = "replay_expected direct_apply must be boolean"
            fail_total += 1
            verdict_cases.append(case_result)
            continue
        if direct_apply:
            case_result["status"] = "fail"
            case_result["reason"] = "ambiguous or garbled replay must not be direct_apply eligible"
            fail_total += 1
            verdict_cases.append(case_result)
            continue
        if disposition not in REPLAY_EXPECTED_DISPOSITIONS:
            case_result["status"] = "fail"
            case_result["reason"] = f"unsupported replay disposition {disposition!r}"
            fail_total += 1
            verdict_cases.append(case_result)
            continue

        if family == "B524" and disposition != "ambiguity":
            case_result["status"] = "fail"
            case_result["reason"] = "B524 dual-namespace replay must be reported as ambiguity"
            fail_total += 1
            verdict_cases.append(case_result)
            continue
        if response_class == "error_or_ambiguous" and disposition != "falsification":
            case_result["status"] = "fail"
            case_result["reason"] = "garbled replay must be reported as falsification"
            fail_total += 1
            verdict_cases.append(case_result)
            continue
        if not proof_gate_ok:
            case_result["status"] = "fail"
            case_result["reason"] = proof_gate_reason or "proof run evidence did not satisfy replay gate"
            fail_total += 1
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
            "proof_run_ok": proof_gate_ok,
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

    return {
        "schema": "p03_canary_overall_summary_v1",
        "captured_at": utc_now(),
        "run_id": run_id,
        "verification_mode": "active_direct_read",
        "read_avoidance_accounting": read_avoidance_accounting,
        "proof_window_traffic_minimums": proof_window_traffic_minimums,
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
    verdict_ok = no_mismatches_ok and overall_interval_ok and per_canary_ok and read_avoidance_ok and proof_window_ok

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
    verdict = build_replay_falsification_verdict(corpus, pathlib.Path(args.proof_dir))
    write_json(pathlib.Path(args.output), verdict)
    return 0 if bool(verdict.get("ok", False)) else 1


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

    replay_verdict = sub.add_parser("replay-verdict", help="build replay falsification verdict from corpus")
    replay_verdict.add_argument("--manifest", required=True)
    replay_verdict.add_argument("--proof-dir", required=True)
    replay_verdict.add_argument("--output", required=True)
    replay_verdict.set_defaults(func=replay_verdict_command)
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
