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

mkdir -p "${log_dir}"

metrics_path="${log_dir}/passive_metrics.prom"
graphql_path="${log_dir}/passive_devices.json"

deadline=$(( $(date +%s) + timeout_sec ))
last_metrics=""
last_graphql=""

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

while [[ "$(date +%s)" -lt "${deadline}" ]]; do
  if metrics="$(curl -fsS -m 8 "${metrics_url}" 2>/dev/null)" && \
     graphql="$(curl -fsS -m 8 -H 'Content-Type: application/json' \
       -d '{"query":"{ devices { address deviceId } }"}' "${graphql_url}" 2>/dev/null)"; then
    last_metrics="${metrics}"
    last_graphql="${graphql}"
    printf '%s\n' "${metrics}" > "${metrics_path}"
    printf '%s\n' "${graphql}" > "${graphql_path}"
    if validate_snapshot "${metrics}" "${graphql}"; then
      exit 0
    fi
  fi
  sleep "${poll_interval_sec}"
done

if [[ -n "${last_metrics}" ]]; then
  printf '%s\n' "${last_metrics}" > "${metrics_path}"
fi
if [[ -n "${last_graphql}" ]]; then
  printf '%s\n' "${last_graphql}" > "${graphql_path}"
fi

if [[ -n "${last_metrics}" && -n "${last_graphql}" ]]; then
  validate_snapshot "${last_metrics}" "${last_graphql}"
fi

echo "passive smoke: timed out waiting for ${case_id} (${passive_mode}) at ${gateway_base_url}" >&2
exit 1
