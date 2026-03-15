#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

case_id="${MATRIX_CASE_CANONICAL_ID:-${MATRIX_CASE_ID:-}}"
passive_mode="${MATRIX_PASSIVE_MODE:-}"
if [[ -z "${case_id}" ]]; then
  echo "passive smoke: MATRIX_CASE_ID is required" >&2
  exit 2
fi
if [[ -z "${passive_mode}" ]]; then
  echo "passive smoke: MATRIX_PASSIVE_MODE is required" >&2
  exit 2
fi

map_exec_case_id() {
  local value="$1"
  local number
  if [[ "${value}" =~ ^T([0-9]+)$ ]]; then
    printf '%s\n' "${value}"
    return 0
  fi
  if [[ "${value}" =~ ^P([0-9]+)$ ]]; then
    number=$((10#${BASH_REMATCH[1]}))
    printf 'T%03d\n' $((100 + number))
    return 0
  fi
  echo "unsupported matrix case id: ${value}" >&2
  return 1
}

exec_case_id="${MATRIX_CASE_EXEC_ID:-$(map_exec_case_id "${case_id}")}"
ha_host="${HA_HOST:-192.168.100.4}"
exec_case_num="${exec_case_id#T}"
gateway_http_port=$((18080 + 10#${exec_case_num}))
gateway_base_url="${MATRIX_GATEWAY_BASE_URL:-http://${ha_host}:${gateway_http_port}}"
graphql_url="${MATRIX_GRAPHQL_URL:-${gateway_base_url}/graphql}"
metrics_url="${MATRIX_METRICS_URL:-${gateway_base_url}/metrics}"
poll_interval_sec="${PASSIVE_SMOKE_POLL_INTERVAL_SEC:-2}"
timeout_sec="${PASSIVE_SMOKE_TIMEOUT_SEC:-120}"
log_dir="${MATRIX_LOG_DIR:-${REPO_ROOT}/results/${case_id}/logs}"
gw15_proof_mode="${MATRIX_GW15_PROOF_MODE:-0}"
proof_hold_sec_raw="${PASSIVE_PROOF_HOLD_SEC:-${MATRIX_GW15_PROOF_HOLD_SEC:-0}}"

normalize_bool_flag() {
  local value="${1:-}"
  local lowered
  lowered="$(printf '%s' "${value}" | tr '[:upper:]' '[:lower:]')"
  case "${lowered}" in
    1|true|yes|on)
      printf '1\n'
      ;;
    0|false|no|off)
      printf '0\n'
      ;;
    *)
      return 1
      ;;
  esac
}

proof_artifacts_enabled="${PASSIVE_PROOF_ARTIFACTS_ENABLED:-${gw15_proof_mode}}"
if [[ -n "${PASSIVE_PROOF_SAMPLE_INTERVAL_SEC:-}" ]]; then
  proof_sample_interval_sec="${PASSIVE_PROOF_SAMPLE_INTERVAL_SEC}"
elif [[ "${gw15_proof_mode}" == "1" ]]; then
  proof_sample_interval_sec=3600
else
  proof_sample_interval_sec=5
fi

mkdir -p "${log_dir}"

metrics_path="${log_dir}/passive_metrics.prom"
graphql_path="${log_dir}/passive_devices.json"
proof_dir="${log_dir}/proof_artifacts"
proof_samples_dir="${proof_dir}/samples"
proof_sample_index=0
proof_next_sample_epoch=0
canary_verifier_script="${SCRIPT_DIR}/passive_canary_verifier.py"
canary_manifest_path="${PASSIVE_P03_CANARY_MANIFEST:-${REPO_ROOT}/testdata/passive_proof/p03_canary_manifest.json}"
canary_manifest_validation_path="${proof_dir}/canary_manifest_validation.json"
canary_baseline_path="${proof_dir}/canary_baseline.json"
canary_summary_path="${proof_dir}/canary_summary.json"
canary_verdict_path="${proof_dir}/canary_verdict.json"
canary_retries_raw="${PASSIVE_CANARY_MAX_RETRIES:-3}"
canary_retries=3
canary_enabled=0
canary_require_interval_phase=1
canary_run_id="p03-$(date +%s)-$$"
proof_hold_sec=0
proof_window_start_epoch=0
proof_window_end_epoch=0
proof_window_started=0
proof_window_completed=0
proof_first_sample_delay_sec=1

deadline=$(( $(date +%s) + timeout_sec ))
last_metrics=""
last_graphql=""
smoke_ok=0

if [[ ! "${proof_sample_interval_sec}" =~ ^[0-9]+$ ]] || [[ "${proof_sample_interval_sec}" -lt 1 ]]; then
  if [[ "${gw15_proof_mode}" == "1" ]]; then
    proof_sample_interval_sec=3600
  else
    proof_sample_interval_sec=5
  fi
fi
if [[ "${poll_interval_sec}" =~ ^[0-9]+$ ]] && [[ "${poll_interval_sec}" -gt 0 ]]; then
  proof_first_sample_delay_sec="${poll_interval_sec}"
fi

if [[ ! "${proof_hold_sec_raw}" =~ ^[0-9]+$ ]]; then
  echo "invalid PASSIVE_PROOF_HOLD_SEC=${proof_hold_sec_raw}" >&2
  exit 2
fi
proof_hold_sec="${proof_hold_sec_raw}"
if [[ "${gw15_proof_mode}" != "1" ]]; then
  proof_hold_sec=0
fi

if [[ "${proof_sample_interval_sec}" -gt "${timeout_sec}" ]]; then
  canary_require_interval_phase=0
fi
if [[ "${gw15_proof_mode}" == "1" && "${proof_hold_sec}" -gt 0 ]]; then
  canary_require_interval_phase=1
fi

if [[ "${gw15_proof_mode}" == "1" ]]; then
  if [[ -n "${PASSIVE_PROOF_ARTIFACTS_ENABLED:-}" ]]; then
    normalized_artifacts="$(normalize_bool_flag "${PASSIVE_PROOF_ARTIFACTS_ENABLED}")" || {
      echo "proof mode: invalid PASSIVE_PROOF_ARTIFACTS_ENABLED=${PASSIVE_PROOF_ARTIFACTS_ENABLED}" >&2
      exit 2
    }
    if [[ "${normalized_artifacts}" != "1" ]]; then
      echo "proof mode requires PASSIVE_PROOF_ARTIFACTS_ENABLED=1" >&2
      exit 2
    fi
  fi
  proof_artifacts_enabled=1
else
  normalized_artifacts="$(normalize_bool_flag "${proof_artifacts_enabled}")" || {
    echo "invalid PASSIVE_PROOF_ARTIFACTS_ENABLED=${proof_artifacts_enabled}" >&2
    exit 2
  }
  proof_artifacts_enabled="${normalized_artifacts}"
fi

if [[ "${proof_artifacts_enabled}" == "1" ]]; then
  rm -rf "${proof_dir}"
  mkdir -p "${proof_samples_dir}"
fi

if [[ ! "${canary_retries_raw}" =~ ^[0-9]+$ ]]; then
  canary_retries=3
elif [[ "${canary_retries_raw}" -lt 1 ]]; then
  canary_retries=1
elif [[ "${canary_retries_raw}" -gt 3 ]]; then
  canary_retries=3
else
  canary_retries="${canary_retries_raw}"
fi

if [[ "${gw15_proof_mode}" == "1" ]]; then
  canary_enabled=1
  if [[ ! -f "${canary_verifier_script}" ]]; then
    echo "proof mode: missing canary verifier script: ${canary_verifier_script}" >&2
    exit 2
  fi
  if ! python3 "${canary_verifier_script}" validate-manifest \
      --manifest "${canary_manifest_path}" \
      --require-case-id "${case_id}" \
      > "${canary_manifest_validation_path}"; then
    echo "proof mode: invalid canary manifest: ${canary_manifest_path}" >&2
    exit 2
  fi
fi

graphql_bus_watch_query='{"query":"{ busSummary { status { transportClass featureFlags { observeFirstEnabled passiveStateDirectApply passiveConfigDirectApply externalWritePolicy normalizations } capability { passiveSupported passiveAvailable passiveState passiveReason endpointState tapConnected } warmup { state blocker elapsedSeconds completedTransactions requiredTransactions completionMode } degraded { active reasons } } } watchSummary { inventory { totalEntries pinnedEntries evictableEntries staticPinnedFootprint writeConfirmPinnedActive } activationCounts { catalogDescriptors activeKeys sourceClasses { class count } } directApplyEligibilityClasses { class count } degraded { active shadowingEnabled pinnedBudgetDegraded compactorDegraded reasons } } }"}'

validate_snapshot() {
  METRICS_PAYLOAD="${1}" \
  GRAPHQL_PAYLOAD="${2}" \
  PASSIVE_MODE_VALUE="${passive_mode}" \
  CASE_ID_VALUE="${case_id}" \
  python3 - <<'PY'
import json
import os
import re
import sys

metrics_text = os.environ.get("METRICS_PAYLOAD", "")
graphql_text = os.environ.get("GRAPHQL_PAYLOAD", "")
passive_mode = os.environ.get("PASSIVE_MODE_VALUE", "")
case_id = os.environ.get("CASE_ID_VALUE", "")

sample_re = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([^\s]+)$")
label_re = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"])*)"')

samples = {}
for raw_line in metrics_text.splitlines():
    line = raw_line.strip()
    if not line or line.startswith("#"):
        continue
    match = sample_re.match(line)
    if not match:
        continue
    name, label_blob, raw_value = match.groups()
    try:
        value = float(raw_value)
    except ValueError:
        continue
    labels = {}
    if label_blob:
        for key, raw in label_re.findall(label_blob):
            labels[key] = bytes(raw, "utf-8").decode("unicode_escape")
    key = (name, tuple(sorted(labels.items())))
    samples[key] = value

def metric(name, **labels):
    key = (name, tuple(sorted(labels.items())))
    return samples.get(key)

try:
    graphql = json.loads(graphql_text)
except json.JSONDecodeError as exc:
    print(f"{case_id}: invalid GraphQL JSON: {exc}", file=sys.stderr)
    raise SystemExit(1)

devices = ((graphql.get("data") or {}).get("devices")) or []
if not isinstance(devices, list) or len(devices) == 0:
    print(f"{case_id}: devices list is empty", file=sys.stderr)
    raise SystemExit(1)

timed_out = metric("ebus_passive_capability_probe_outcomes_total", outcome="timed_out")
if timed_out is None:
    print(f"{case_id}: missing timed_out passive probe metric", file=sys.stderr)
    raise SystemExit(1)
if timed_out != 0:
    print(f"{case_id}: timed_out passive probe outcome = {timed_out}; want 0", file=sys.stderr)
    raise SystemExit(1)

if passive_mode == "required":
    connected = metric("ebus_passive_tap_connected")
    available = metric("ebus_passive_warmup_state", state="available")
    confirmed = metric("ebus_passive_capability_probe_outcomes_total", outcome="confirmed")
    if connected != 1:
        print(f"{case_id}: passive tap connected = {connected}; want 1", file=sys.stderr)
        raise SystemExit(1)
    if available != 1:
        print(f"{case_id}: passive warmup available = {available}; want 1", file=sys.stderr)
        raise SystemExit(1)
    if confirmed is None or confirmed < 1:
        print(f"{case_id}: passive confirmed outcome = {confirmed}; want >= 1", file=sys.stderr)
        raise SystemExit(1)
    raise SystemExit(0)

if passive_mode == "unsupported_or_misconfigured":
    unavailable = metric("ebus_passive_warmup_state", state="unavailable")
    reason = metric("ebus_passive_capability_unavailable_reason", reason="unsupported_or_misconfigured")
    available = metric("ebus_passive_warmup_state", state="available")
    if unavailable != 1:
        print(f"{case_id}: passive unavailable state = {unavailable}; want 1", file=sys.stderr)
        raise SystemExit(1)
    if reason != 1:
        print(f"{case_id}: passive unsupported_or_misconfigured reason = {reason}; want 1", file=sys.stderr)
        raise SystemExit(1)
    if available not in (None, 0):
        print(f"{case_id}: passive available state = {available}; want 0", file=sys.stderr)
        raise SystemExit(1)
    raise SystemExit(0)

print(f"{case_id}: unsupported passive mode {passive_mode!r}", file=sys.stderr)
raise SystemExit(1)
PY
}

write_feature_flag_snapshot() {
  local graphql_file="$1"
  local bus_file="$2"
  local output_file="$3"
  python3 - "${graphql_file}" "${bus_file}" "${output_file}" <<'PY'
import json
import pathlib
import sys
from datetime import datetime, timezone

graphql_file = pathlib.Path(sys.argv[1])
bus_file = pathlib.Path(sys.argv[2])
output_file = pathlib.Path(sys.argv[3])

graphql_flags = None
bus_flags = None

if graphql_file.exists():
    try:
        payload = json.loads(graphql_file.read_text())
        graphql_flags = (((payload.get("data") or {}).get("busSummary") or {}).get("status") or {}).get("featureFlags")
    except Exception:
        graphql_flags = None

if bus_file.exists():
    try:
        payload = json.loads(bus_file.read_text())
        bus_flags = (((payload.get("summary") or {}).get("status") or {}).get("feature_flags"))
    except Exception:
        bus_flags = None

snapshot = {
    "captured_at": datetime.now(timezone.utc).isoformat(),
    "graphql_feature_flags": graphql_flags,
    "bus_observability_feature_flags": bus_flags,
}
output_file.write_text(json.dumps(snapshot, indent=2) + "\n")
PY
}

graphql_bus_watch_payload_valid() {
  local payload="${1:-}"
  GRAPHQL_PAYLOAD="${payload}" python3 - <<'PY'
import json
import os
import sys

payload_text = os.environ.get("GRAPHQL_PAYLOAD", "")
try:
    payload = json.loads(payload_text)
except Exception:
    raise SystemExit(1)

errors = payload.get("errors")
if isinstance(errors, list) and len(errors) > 0:
    raise SystemExit(1)

data = payload.get("data")
if not isinstance(data, dict):
    raise SystemExit(1)

if data.get("busSummary") is None:
    raise SystemExit(1)
if data.get("watchSummary") is None:
    raise SystemExit(1)

raise SystemExit(0)
PY
}

capture_proof_snapshot() {
  local prefix="$1"
  local metrics_payload="${2:-}"
  local require_complete="${3:-0}"
  local metrics_file="${prefix}_metrics.prom"
  local bus_file="${prefix}_bus_observability.json"
  local graphql_file="${prefix}_graphql_bus_watch.json"
  local flags_file="${prefix}_feature_flags.json"
  local bus_payload=""
  local graphql_payload=""
  local sampled_metrics=""
  local have_metrics=0
  local have_bus=0
  local have_graphql=0

  rm -f "${metrics_file}" "${bus_file}" "${graphql_file}" "${flags_file}"

  if [[ -z "${metrics_payload}" ]]; then
    if sampled_metrics="$(curl -fsS -m 8 "${metrics_url}" 2>/dev/null)"; then
      metrics_payload="${sampled_metrics}"
    fi
  fi
  if [[ -n "${metrics_payload}" ]]; then
    printf '%s\n' "${metrics_payload}" > "${metrics_file}"
    have_metrics=1
  fi

  if bus_payload="$(curl -fsS -m 8 "${gateway_base_url}/portal/api/v1/bus/observability" 2>/dev/null)"; then
    printf '%s\n' "${bus_payload}" > "${bus_file}"
    have_bus=1
  fi

  if graphql_payload="$(curl -fsS -m 8 -H 'Content-Type: application/json' -d "${graphql_bus_watch_query}" "${graphql_url}" 2>/dev/null)"; then
    if graphql_bus_watch_payload_valid "${graphql_payload}"; then
      printf '%s\n' "${graphql_payload}" > "${graphql_file}"
      have_graphql=1
    fi
  fi

  if [[ "${have_bus}" == "1" || "${have_graphql}" == "1" ]]; then
    write_feature_flag_snapshot "${graphql_file}" "${bus_file}" "${flags_file}" || true
  fi

  if [[ "${require_complete}" == "1" ]]; then
    if [[ "${have_metrics}" != "1" || "${have_bus}" != "1" || "${have_graphql}" != "1" || ! -f "${flags_file}" ]]; then
      return 1
    fi
  fi
  return 0
}

run_canary_phase() {
  local phase="$1"
  local output_path="${proof_dir}/canary_phase_${phase}.json"
  python3 "${canary_verifier_script}" verify-phase \
    --manifest "${canary_manifest_path}" \
    --require-case-id "${case_id}" \
    --graphql-url "${graphql_url}" \
    --phase "${phase}" \
    --run-id "${canary_run_id}" \
    --baseline "${canary_baseline_path}" \
    --output "${output_path}" \
    --retries "${canary_retries}" \
    --timeout-sec 8
}

build_canary_summary() {
  local require_interval_phase="${1:-1}"
  python3 "${canary_verifier_script}" summarize \
    --proof-dir "${proof_dir}" \
    --run-id "${canary_run_id}" \
    --require-interval-phase "${require_interval_phase}" \
    --output "${canary_summary_path}"
}

build_canary_verdict() {
  python3 "${canary_verifier_script}" verdict \
    --summary "${canary_summary_path}" \
    --output "${canary_verdict_path}"
}

reset_active_proof_window() {
  proof_window_started=0
  proof_window_start_epoch=0
  proof_window_end_epoch=0
  proof_next_sample_epoch=0
}

if [[ "${proof_artifacts_enabled}" == "1" ]]; then
  start_captured=0
  for _ in $(seq 1 5); do
    if capture_proof_snapshot "${proof_dir}/start" "" 1; then
      start_captured=1
      break
    fi
    sleep 1
  done
  if [[ "${start_captured}" != "1" ]]; then
    echo "proof mode: failed to capture required start artifacts" >&2
    exit 1
  fi
  if [[ "${gw15_proof_mode}" == "1" && "${proof_hold_sec}" -gt 0 ]]; then
    proof_next_sample_epoch=0
  else
    proof_next_sample_epoch="$(date +%s)"
  fi
  if [[ "${canary_enabled}" == "1" ]]; then
    if ! run_canary_phase "start"; then
      echo "proof mode: failed to run start canary verification" >&2
      exit 1
    fi
  fi
fi

while [[ "$(date +%s)" -lt "${deadline}" ]]; do
  if metrics="$(curl -fsS -m 8 "${metrics_url}" 2>/dev/null)" && \
     graphql="$(curl -fsS -m 8 -H 'Content-Type: application/json' \
       -d '{"query":"{ devices { address deviceId } }"}' "${graphql_url}" 2>/dev/null)"; then
    now_epoch="$(date +%s)"
    snapshot_healthy=0
    last_metrics="${metrics}"
    last_graphql="${graphql}"
    printf '%s\n' "${metrics}" > "${metrics_path}"
    printf '%s\n' "${graphql}" > "${graphql_path}"
    if validate_snapshot "${metrics}" "${graphql}"; then
      smoke_ok=1
      snapshot_healthy=1
      if [[ "${gw15_proof_mode}" == "1" && "${proof_hold_sec}" -gt 0 && "${proof_window_started}" != "1" ]]; then
        proof_window_started=1
        proof_window_start_epoch="${now_epoch}"
        proof_window_end_epoch=$((now_epoch + proof_hold_sec))
        proof_next_sample_epoch=$((now_epoch + proof_first_sample_delay_sec))
      fi
    fi
    if [[ "${proof_artifacts_enabled}" == "1" ]]; then
      sample_allowed=1
      if [[ "${gw15_proof_mode}" == "1" && "${proof_hold_sec}" -gt 0 && "${proof_window_started}" != "1" ]]; then
        sample_allowed=0
      fi
      if [[ "${sample_allowed}" == "1" && "${now_epoch}" -ge "${proof_next_sample_epoch}" ]]; then
        proof_sample_index=$((proof_sample_index + 1))
        sample_prefix="${proof_samples_dir}/sample_$(printf '%04d' "${proof_sample_index}")"
        sample_phase="sample_$(printf '%04d' "${proof_sample_index}")"
        capture_proof_snapshot "${sample_prefix}" "${metrics}" 0
        if [[ "${canary_enabled}" == "1" ]]; then
          if ! run_canary_phase "${sample_phase}"; then
            echo "proof mode: failed to run interval canary verification (${sample_phase})" >&2
            exit 1
          fi
        fi
        proof_next_sample_epoch=$((now_epoch + proof_sample_interval_sec))
      fi
    fi
    if [[ "${gw15_proof_mode}" == "1" && "${proof_hold_sec}" -gt 0 && "${snapshot_healthy}" != "1" && "${proof_window_started}" == "1" ]]; then
      reset_active_proof_window
    fi
    if [[ "${snapshot_healthy}" == "1" ]]; then
      if [[ "${gw15_proof_mode}" == "1" && "${proof_hold_sec}" -gt 0 ]]; then
        if [[ "${proof_window_started}" == "1" && "${now_epoch}" -ge "${proof_window_end_epoch}" ]]; then
          proof_window_completed=1
          break
        fi
      else
        break
      fi
    fi
  elif [[ "${gw15_proof_mode}" == "1" && "${proof_hold_sec}" -gt 0 && "${proof_window_started}" == "1" ]]; then
    reset_active_proof_window
  fi
  sleep "${poll_interval_sec}"
done

if [[ -n "${last_metrics}" ]]; then
  printf '%s\n' "${last_metrics}" > "${metrics_path}"
fi
if [[ -n "${last_graphql}" ]]; then
  printf '%s\n' "${last_graphql}" > "${graphql_path}"
fi

if [[ "${proof_artifacts_enabled}" == "1" ]]; then
  end_captured=0
  for _ in $(seq 1 5); do
    if capture_proof_snapshot "${proof_dir}/end" "${last_metrics}" 1; then
      end_captured=1
      break
    fi
    sleep 1
  done
  if [[ "${end_captured}" != "1" ]]; then
    echo "proof mode: failed to capture required end artifacts" >&2
    exit 1
  fi
  if [[ "${canary_enabled}" == "1" ]]; then
    if ! run_canary_phase "end"; then
      echo "proof mode: failed to run end canary verification" >&2
      exit 1
    fi
    if ! build_canary_summary "${canary_require_interval_phase}"; then
      echo "proof mode: failed to build canary summary" >&2
      exit 1
    fi
    if ! build_canary_verdict; then
      echo "proof mode: canary verdict gate failed (see ${canary_verdict_path})" >&2
      exit 1
    fi
  fi
fi

if [[ "${gw15_proof_mode}" == "1" && "${proof_hold_sec}" -gt 0 ]]; then
  if [[ "${proof_window_completed}" == "1" ]]; then
    exit 0
  fi
elif [[ "${smoke_ok}" -eq 1 ]]; then
  exit 0
fi

if [[ "${gw15_proof_mode}" != "1" || "${proof_hold_sec}" -le 0 ]]; then
  if [[ -n "${last_metrics}" && -n "${last_graphql}" ]] && validate_snapshot "${last_metrics}" "${last_graphql}"; then
    exit 0
  fi
fi

echo "passive smoke: timed out waiting for ${case_id} (${passive_mode}) at ${gateway_base_url}" >&2
exit 1
