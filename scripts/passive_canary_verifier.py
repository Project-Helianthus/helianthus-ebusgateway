#!/usr/bin/env python3
"""Active direct-read canary verifier for passive proof mode (P03)."""

from __future__ import annotations

import argparse
import base64
import glob
import json
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
OVERALL_INTERVAL_CONCLUSIVE_MIN = 0.90
PER_CANARY_INTERVAL_CONCLUSIVE_MIN = 0.75


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
    payload = {
        "query": mutation,
        "variables": {
            "address": canary.address,
            "plane": canary.plane,
            "method": canary.method,
            "params": canary.params,
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
            try:
                candidate_hex = invoke_canary(graphql_url, canary, timeout_sec)
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
        "read_avoidance_accounting": {"excluded": True},
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

    return {
        "schema": "p03_canary_overall_summary_v1",
        "captured_at": utc_now(),
        "run_id": run_id,
        "verification_mode": "active_direct_read",
        "read_avoidance_accounting": {"excluded": True},
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
    verdict_ok = no_mismatches_ok and overall_interval_ok and per_canary_ok

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
    baseline_map = load_baseline_map(baseline_path)
    phase_result = verify_phase(
        canaries=canaries,
        graphql_url=args.graphql_url,
        phase=args.phase,
        run_id=args.run_id,
        retries=args.retries,
        timeout_sec=args.timeout_sec,
        baseline_map=baseline_map,
    )
    write_json(pathlib.Path(args.output), phase_result)
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
