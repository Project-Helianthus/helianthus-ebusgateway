#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <gateway-start|gateway-stop|proxy-start|proxy-stop|ebusd-start|ebusd-stop|smoke>" >&2
  exit 2
fi

ACTION="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE_ROOT="$(cd "${REPO_ROOT}/.." && pwd)"
LOCAL_OPS_SCRIPT="${WORKSPACE_ROOT}/local_ops/matrix_case_ha.sh"
PASSIVE_CHECK_SCRIPT="${SCRIPT_DIR}/passive_smoke_check.sh"

if [[ ! -x "${LOCAL_OPS_SCRIPT}" ]]; then
  echo "local_ops matrix helper missing or not executable: ${LOCAL_OPS_SCRIPT}" >&2
  exit 1
fi

canonical_case_id="${MATRIX_CASE_CANONICAL_ID:-${MATRIX_CASE_ID:-}}"
if [[ -z "${canonical_case_id}" ]]; then
  echo "MATRIX_CASE_ID is required" >&2
  exit 2
fi

gw15_proof_mode="${MATRIX_GW15_PROOF_MODE:-0}"
if [[ "${gw15_proof_mode}" == "1" && "${canonical_case_id}" != "P03" ]]; then
  echo "MATRIX_GW15_PROOF_MODE=1 currently supports only P03 (got ${canonical_case_id})" >&2
  exit 2
fi

if [[ "${gw15_proof_mode}" == "1" ]]; then
  : "${MATRIX_OBSERVE_FIRST_ENABLED:=true}"
  : "${MATRIX_PASSIVE_STATE_DIRECT_APPLY:=true}"
  : "${MATRIX_PASSIVE_CONFIG_DIRECT_APPLY:=false}"
  : "${MATRIX_EXTERNAL_WRITE_POLICY:=record_only}"
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

exec_case_id="$(map_exec_case_id "${canonical_case_id}")"
exec_case_num="${exec_case_id#T}"
ha_host="${HA_HOST:-192.168.100.4}"
ha_ssh_port="${HA_SSH_PORT:-22222}"
ha_ssh_user="${HA_SSH_USER:-root}"
adapter_addr="${ADAPTER_ADDR:-192.168.100.2:9999}"
gateway_http_port=$((18080 + 10#${exec_case_num}))
proxy_tcp_port=$((19080 + 10#${exec_case_num}))
proxy_udp_port=$((20080 + 10#${exec_case_num}))
ebusd_cmd_port=$((21080 + 10#${exec_case_num}))
gateway_base_url="${MATRIX_GATEWAY_BASE_URL:-http://${ha_host}:${gateway_http_port}}"
remote_base="${REMOTE_BASE:-/mnt/data/supervisor/tmp/helianthus-matrix}"
remote_bin="${REMOTE_BIN:-${remote_base}/bin}"
remote_case_dir="${remote_base}/runs/${exec_case_id}"

SSH=(ssh -p "${ha_ssh_port}" "${ha_ssh_user}@${ha_host}")

remote_exec() {
  "${SSH[@]}" "$@"
}

normalize_bool_flag() {
  local value="${1:-}"
  local lowered
  lowered="$(printf '%s' "${value}" | tr '[:upper:]' '[:lower:]')"
  case "${lowered}" in
    1|true|yes|on)
      printf 'true\n'
      ;;
    0|false|no|off)
      printf 'false\n'
      ;;
    *)
      return 1
      ;;
  esac
}

enforce_gw15_proof_flag_state() {
  [[ "${gw15_proof_mode}" == "1" ]] || return 0

  local observe state config policy
  local rollback_execution="${MATRIX_GW15_ROLLBACK_EXECUTION:-0}"
  observe="$(normalize_bool_flag "${MATRIX_OBSERVE_FIRST_ENABLED}")" || {
    echo "invalid MATRIX_OBSERVE_FIRST_ENABLED=${MATRIX_OBSERVE_FIRST_ENABLED}" >&2
    return 1
  }
  state="$(normalize_bool_flag "${MATRIX_PASSIVE_STATE_DIRECT_APPLY}")" || {
    echo "invalid MATRIX_PASSIVE_STATE_DIRECT_APPLY=${MATRIX_PASSIVE_STATE_DIRECT_APPLY}" >&2
    return 1
  }
  config="$(normalize_bool_flag "${MATRIX_PASSIVE_CONFIG_DIRECT_APPLY}")" || {
    echo "invalid MATRIX_PASSIVE_CONFIG_DIRECT_APPLY=${MATRIX_PASSIVE_CONFIG_DIRECT_APPLY}" >&2
    return 1
  }
  policy="$(printf '%s' "${MATRIX_EXTERNAL_WRITE_POLICY}" | xargs)"

  if [[ "${rollback_execution}" == "1" ]]; then
    if [[ "${observe}" != "false" ]]; then
      echo "GW-15 rollback execution requires MATRIX_OBSERVE_FIRST_ENABLED=false" >&2
      return 1
    fi
    if [[ "${state}" != "false" ]]; then
      echo "GW-15 rollback execution requires MATRIX_PASSIVE_STATE_DIRECT_APPLY=false" >&2
      return 1
    fi
    if [[ "${config}" != "false" ]]; then
      echo "GW-15 rollback execution requires MATRIX_PASSIVE_CONFIG_DIRECT_APPLY=false" >&2
      return 1
    fi
    if [[ "${policy}" != "record_only" ]]; then
      echo "GW-15 rollback execution requires MATRIX_EXTERNAL_WRITE_POLICY=record_only" >&2
      return 1
    fi
  else
    if [[ "${observe}" != "true" ]]; then
      echo "GW-15 proof mode requires MATRIX_OBSERVE_FIRST_ENABLED=true" >&2
      return 1
    fi
    if [[ "${state}" != "true" ]]; then
      echo "GW-15 proof mode requires MATRIX_PASSIVE_STATE_DIRECT_APPLY=true" >&2
      return 1
    fi
    if [[ "${config}" != "false" ]]; then
      echo "GW-15 proof mode requires MATRIX_PASSIVE_CONFIG_DIRECT_APPLY=false" >&2
      return 1
    fi
    if [[ "${policy}" != "record_only" ]]; then
      echo "GW-15 proof mode requires MATRIX_EXTERNAL_WRITE_POLICY=record_only" >&2
      return 1
    fi
  fi
}

build_observe_first_cli_flags() {
  local flags=()
  local normalized=""
  local value=""

  value="$(printf '%s' "${MATRIX_OBSERVE_FIRST_ENABLED:-}" | xargs)"
  if [[ -n "${value}" ]]; then
    normalized="$(normalize_bool_flag "${value}")" || {
      echo "invalid MATRIX_OBSERVE_FIRST_ENABLED=${MATRIX_OBSERVE_FIRST_ENABLED:-}" >&2
      return 1
    }
    flags+=("--observe-first-enabled=${normalized}")
  fi

  value="$(printf '%s' "${MATRIX_PASSIVE_STATE_DIRECT_APPLY:-}" | xargs)"
  if [[ -n "${value}" ]]; then
    normalized="$(normalize_bool_flag "${value}")" || {
      echo "invalid MATRIX_PASSIVE_STATE_DIRECT_APPLY=${MATRIX_PASSIVE_STATE_DIRECT_APPLY:-}" >&2
      return 1
    }
    flags+=("--passive-state-direct-apply=${normalized}")
  fi

  value="$(printf '%s' "${MATRIX_PASSIVE_CONFIG_DIRECT_APPLY:-}" | xargs)"
  if [[ -n "${value}" ]]; then
    normalized="$(normalize_bool_flag "${value}")" || {
      echo "invalid MATRIX_PASSIVE_CONFIG_DIRECT_APPLY=${MATRIX_PASSIVE_CONFIG_DIRECT_APPLY:-}" >&2
      return 1
    }
    flags+=("--passive-config-direct-apply=${normalized}")
  fi

  value="$(printf '%s' "${MATRIX_EXTERNAL_WRITE_POLICY:-}" | xargs)"
  if [[ -n "${value}" ]]; then
    case "${value}" in
      invalidate_only|record_only|record_and_invalidate)
        flags+=("--external-write-policy=${value}")
        ;;
      *)
        echo "invalid MATRIX_EXTERNAL_WRITE_POLICY=${MATRIX_EXTERNAL_WRITE_POLICY:-}" >&2
        return 1
        ;;
    esac
  fi

  if [[ "${#flags[@]}" -eq 0 ]]; then
    printf '\n'
    return 0
  fi

  printf '%s\n' "${flags[*]}"
}

build_startup_probe_cli_flags() {
  local targets
  targets="$(printf '%s' "${MATRIX_STARTUP_PROBE_TARGETS:-}" | xargs)"
  if [[ -z "${targets}" && "${MATRIX_PASSIVE_MODE:-}" == "required" ]]; then
    targets="${MATRIX_REQUIRED_PASSIVE_STARTUP_PROBE_TARGETS:-0x08,0x15,0x26,0x04}"
  fi
  if [[ -z "${targets}" ]]; then
    printf '\n'
    return 0
  fi
  printf '%s\n' "--startup-probe-targets '${targets}'"
}

select_ebusd_config_src() {
  if [[ -n "${EBUSD_CONFIG_SRC:-}" ]]; then
    printf '%s\n' "${EBUSD_CONFIG_SRC}"
    return 0
  fi

  local selected
  selected="$(
    remote_exec "for d in \
      '/mnt/data/supervisor/homeassistant/ebusd-configuration.old/ebusd-1.x.x/vaillant_de' \
      '/mnt/data/supervisor/homeassistant/ebusd-configuration.old/ebusd-2.1.x/de/vaillant' \
      '/mnt/data/supervisor/homeassistant/ebusd-configuration/outcsv-pr564-9097c032a6cc'; do \
        if [ -d \"\$d\" ] && find \"\$d\" -maxdepth 1 -type f | grep -q .; then \
          printf '%s\n' \"\$d\"; \
          exit 0; \
        fi; \
      done"
  )"
  if [[ -z "${selected}" ]]; then
    echo "unable to resolve EBUSD_CONFIG_SRC on HA host" >&2
    return 1
  fi
  printf '%s\n' "${selected}"
}

gateway_connection() {
  local protocol network address
  if [[ "${MATRIX_USES_EBUSD:-0}" == "1" && "${MATRIX_USES_PROXY:-0}" == "0" ]]; then
    protocol="ebusd-tcp"
    network="tcp"
    address="127.0.0.1:${ebusd_cmd_port}"
  elif [[ "${MATRIX_USES_PROXY:-0}" == "1" ]]; then
    case "${MATRIX_GATEWAY_TRANSPORT:-enh}" in
      enh)
        protocol="enh"
        network="tcp"
        address="127.0.0.1:${proxy_tcp_port}"
        ;;
      ens)
        protocol="ens"
        network="tcp"
        address="127.0.0.1:${proxy_tcp_port}"
        ;;
      udp)
        protocol="udp-plain"
        network="udp"
        address="127.0.0.1:${proxy_udp_port}"
        ;;
      tcp)
        protocol="tcp-plain"
        network="tcp"
        address="127.0.0.1:${proxy_tcp_port}"
        ;;
      *)
        echo "unsupported MATRIX_GATEWAY_TRANSPORT=${MATRIX_GATEWAY_TRANSPORT:-}" >&2
        return 1
        ;;
    esac
  else
    case "${MATRIX_GATEWAY_TRANSPORT:-enh}" in
      enh)
        protocol="enh"
        network="tcp"
        address="${adapter_addr}"
        ;;
      ens)
        protocol="ens"
        network="tcp"
        address="${adapter_addr}"
        ;;
      udp)
        protocol="udp-plain"
        network="udp"
        address="${adapter_addr}"
        ;;
      tcp)
        protocol="tcp-plain"
        network="tcp"
        address="${adapter_addr}"
        ;;
      *)
        echo "unsupported MATRIX_GATEWAY_TRANSPORT=${MATRIX_GATEWAY_TRANSPORT:-}" >&2
        return 1
        ;;
    esac
  fi

  printf '%s;%s;%s\n' "${protocol}" "${network}" "${address}"
}

run_local_ops() {
  enforce_gw15_proof_flag_state
  if [[ "${MATRIX_USES_EBUSD:-0}" == "1" ]]; then
    local ebusd_config_src
    ebusd_config_src="$(select_ebusd_config_src)"
    MATRIX_CASE_CANONICAL_ID="${canonical_case_id}" \
    MATRIX_CASE_EXEC_ID="${exec_case_id}" \
    MATRIX_GATEWAY_BASE_URL="${gateway_base_url}" \
    MATRIX_GRAPHQL_URL="${MATRIX_GRAPHQL_URL:-${gateway_base_url}/graphql}" \
    MATRIX_METRICS_URL="${MATRIX_METRICS_URL:-${gateway_base_url}/metrics}" \
    MATRIX_OBSERVE_FIRST_ENABLED="${MATRIX_OBSERVE_FIRST_ENABLED:-}" \
    MATRIX_PASSIVE_STATE_DIRECT_APPLY="${MATRIX_PASSIVE_STATE_DIRECT_APPLY:-}" \
    MATRIX_PASSIVE_CONFIG_DIRECT_APPLY="${MATRIX_PASSIVE_CONFIG_DIRECT_APPLY:-}" \
    MATRIX_EXTERNAL_WRITE_POLICY="${MATRIX_EXTERNAL_WRITE_POLICY:-}" \
    EBUSD_CONFIG_SRC="${ebusd_config_src}" \
    ADAPTER_REQUIRE_SIGNAL="${ADAPTER_REQUIRE_SIGNAL:-0}" \
    MATRIX_CASE_ID="${exec_case_id}" \
      "${LOCAL_OPS_SCRIPT}" "$@"
    return
  fi

  MATRIX_CASE_CANONICAL_ID="${canonical_case_id}" \
  MATRIX_CASE_EXEC_ID="${exec_case_id}" \
  MATRIX_GATEWAY_BASE_URL="${gateway_base_url}" \
  MATRIX_GRAPHQL_URL="${MATRIX_GRAPHQL_URL:-${gateway_base_url}/graphql}" \
  MATRIX_METRICS_URL="${MATRIX_METRICS_URL:-${gateway_base_url}/metrics}" \
  MATRIX_OBSERVE_FIRST_ENABLED="${MATRIX_OBSERVE_FIRST_ENABLED:-}" \
  MATRIX_PASSIVE_STATE_DIRECT_APPLY="${MATRIX_PASSIVE_STATE_DIRECT_APPLY:-}" \
  MATRIX_PASSIVE_CONFIG_DIRECT_APPLY="${MATRIX_PASSIVE_CONFIG_DIRECT_APPLY:-}" \
  MATRIX_EXTERNAL_WRITE_POLICY="${MATRIX_EXTERNAL_WRITE_POLICY:-}" \
  ADAPTER_REQUIRE_SIGNAL="${ADAPTER_REQUIRE_SIGNAL:-0}" \
  MATRIX_CASE_ID="${exec_case_id}" \
    "${LOCAL_OPS_SCRIPT}" "$@"
}

restart_gateway_with_passive_mode() {
  local gateway_log_path="${1:-${MATRIX_GATEWAY_LOG_PATH:-${remote_case_dir}/logs/gateway.log}}"
  local protocol network address
  local observe_first_flags=""
  local startup_probe_flags=""
  enforce_gw15_proof_flag_state
  IFS=';' read -r protocol network address < <(gateway_connection)
  observe_first_flags="$(build_observe_first_cli_flags)"
  startup_probe_flags="$(build_startup_probe_cli_flags)"

  remote_exec "mkdir -p '${remote_case_dir}/state' '${remote_case_dir}/logs'"
  remote_exec "if [ -f '${remote_case_dir}/state/gateway.pid' ]; then kill \$(cat '${remote_case_dir}/state/gateway.pid') >/dev/null 2>&1 || true; rm -f '${remote_case_dir}/state/gateway.pid'; fi"
  remote_exec "pids=\$(ss -ltnup '( sport = :${gateway_http_port} )' 2>/dev/null | awk -F'pid=' '/pid=/{split(\$2,a,\",\"); print a[1]}' | grep -E '^[0-9]+$' | sort -u); for pid in \$pids; do kill -9 \"\$pid\" >/dev/null 2>&1 || true; done"
  remote_exec "nohup '${remote_bin}/helianthus-gateway' \
    --transport '${protocol}' --network '${network}' --address '${address}' \
    --source-addr auto \
    --read-timeout 400ms --write-timeout 400ms --dial-timeout 3s \
    --scan-timeout 90s --scan-request-timeout 2s --scan-interval 0s \
    --semantic-discovery-interval 5m --semantic-config-interval 5m --semantic-state-interval 1m --semantic-request-timeout 2s \
    --semantic-cache-path '${remote_case_dir}/state/semantic_cache.json' \
    ${observe_first_flags} \
    ${startup_probe_flags} \
    --http-addr ':${gateway_http_port}' --mdns=false --broadcast=true \
    > '${gateway_log_path}' 2>&1 & echo \$! > '${remote_case_dir}/state/gateway.pid'"
  remote_exec "for i in \$(seq 1 60); do kill -0 \$(cat '${remote_case_dir}/state/gateway.pid') || exit 2; ss -ltn '( sport = :${gateway_http_port} )' | grep -q LISTEN && exit 0; sleep 1; done; exit 1"
}

gateway_graphql_health_check() {
  local attempts="${MATRIX_GW15_ROLLBACK_GRAPHQL_HEALTH_RETRIES:-5}"
  local sleep_sec="${MATRIX_GW15_ROLLBACK_GRAPHQL_HEALTH_RETRY_SLEEP_SEC:-2}"
  local response=""
  local attempt=0

  while [[ "${attempt}" -lt "${attempts}" ]]; do
    response="$(
      curl -fsS -m 8 -H 'Content-Type: application/json' \
        -d '{"query":"{ __typename }"}' \
        "${MATRIX_GRAPHQL_URL:-${gateway_base_url}/graphql}" 2>/dev/null || true
    )"
    if [[ -n "${response}" ]] && printf '%s' "${response}" | grep -q '"__typename"'; then
      return 0
    fi
    attempt=$((attempt + 1))
    if [[ "${attempt}" -lt "${attempts}" ]]; then
      sleep "${sleep_sec}"
    fi
  done
  return 1
}

write_rollback_execution_artifact() {
  local artifact_path="$1"
  local started_at="$2"
  local completed_at="$3"
  local ok="$4"
  local reason="$5"
  local restart_exit_code="$6"
  local restart_succeeded="$7"
  local gateway_health_check_ok="$8"
  local proof_gateway_log_path="$9"
  local rollback_gateway_log_path="${10}"

  python3 "${PASSIVE_CHECK_SCRIPT%/*}/passive_canary_verifier.py" rollback-execution \
    --run-id "${ROLLBACK_EXECUTION_RUN_ID}" \
    --case-id "${canonical_case_id}" \
    --exec-case-id "${exec_case_id}" \
    --gateway-base-url "${gateway_base_url}" \
    --remote-case-dir "${remote_case_dir}" \
    --proof-gateway-log-path "${proof_gateway_log_path}" \
    --rollback-gateway-log-path "${rollback_gateway_log_path}" \
    --started-at "${started_at}" \
    --completed-at "${completed_at}" \
    --ok "${ok}" \
    --reason "${reason}" \
    --restart-exit-code "${restart_exit_code}" \
    --restart-succeeded "${restart_succeeded}" \
    --gateway-health-check-ok "${gateway_health_check_ok}" \
    --source "passive_matrix_case_ha.sh rollback-execute" \
    --action "gateway_restart_with_rollback_target" \
    --output "${artifact_path}"
}

run_rollback_execute() {
  local artifact_path="${ROLLBACK_EXECUTION_ARTIFACT_PATH:-}"
  local started_at completed_at
  local ok="false"
  local reason="gateway_restart_failed"
  local restart_exit_code=1
  local restart_succeeded="false"
  local gateway_health_check_ok="false"
  local rollback_gateway_log_path="${remote_case_dir}/logs/gateway_rollback.log"
  local local_log_dir=""
  local proof_gateway_log_bundle_path=""
  local rollback_gateway_log_bundle_path=""

  if [[ -z "${artifact_path}" ]]; then
    echo "ROLLBACK_EXECUTION_ARTIFACT_PATH is required" >&2
    return 2
  fi
  if [[ -z "${ROLLBACK_EXECUTION_RUN_ID:-}" ]]; then
    echo "ROLLBACK_EXECUTION_RUN_ID is required" >&2
    return 2
  fi

  started_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  local_log_dir="$(cd "$(dirname "${artifact_path}")" && pwd)"
  mkdir -p "${local_log_dir}"
  proof_gateway_log_bundle_path="${local_log_dir}/gateway_pre_rollback.log"
  rollback_gateway_log_bundle_path="${local_log_dir}/gateway_rollback.log"
  if remote_exec "test -f '${remote_case_dir}/logs/gateway.log'"; then
    if ! remote_exec "cat '${remote_case_dir}/logs/gateway.log'" > "${proof_gateway_log_bundle_path}"; then
      reason="failed_to_bundle_pre_rollback_gateway_log"
      completed_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
      write_rollback_execution_artifact \
        "${artifact_path}" \
        "${started_at}" \
        "${completed_at}" \
        "${ok}" \
        "${reason}" \
        "${restart_exit_code}" \
        "${restart_succeeded}" \
        "${gateway_health_check_ok}" \
        "${proof_gateway_log_bundle_path}" \
        "${rollback_gateway_log_bundle_path}"
      return 1
    fi
  else
    reason="missing_pre_rollback_gateway_log"
    completed_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    write_rollback_execution_artifact \
      "${artifact_path}" \
      "${started_at}" \
      "${completed_at}" \
      "${ok}" \
      "${reason}" \
      "${restart_exit_code}" \
      "${restart_succeeded}" \
      "${gateway_health_check_ok}" \
      "${proof_gateway_log_bundle_path}" \
      "${rollback_gateway_log_bundle_path}"
    return 1
  fi

  if MATRIX_GW15_ROLLBACK_EXECUTION=1 \
    MATRIX_GATEWAY_LOG_PATH="${rollback_gateway_log_path}" \
    MATRIX_OBSERVE_FIRST_ENABLED=false \
    MATRIX_PASSIVE_STATE_DIRECT_APPLY=false \
    MATRIX_PASSIVE_CONFIG_DIRECT_APPLY=false \
    MATRIX_EXTERNAL_WRITE_POLICY=record_only \
    "$0" gateway-start; then
    restart_exit_code=0
    restart_succeeded="true"
    if gateway_graphql_health_check; then
      gateway_health_check_ok="true"
      ok="true"
      reason="ok"
    else
      reason="gateway_health_check_failed"
    fi
  else
    restart_exit_code=$?
  fi
  remote_exec "cat '${rollback_gateway_log_path}'" > "${rollback_gateway_log_bundle_path}" || true
  if [[ ! -s "${rollback_gateway_log_bundle_path}" ]]; then
    if [[ "${reason}" == "ok" ]]; then
      ok="false"
      reason="missing_rollback_gateway_log_bundle"
    elif [[ "${reason}" == "gateway_restart_failed" ]]; then
      reason="gateway_restart_failed_and_missing_rollback_gateway_log_bundle"
    fi
  fi
  completed_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  write_rollback_execution_artifact \
    "${artifact_path}" \
    "${started_at}" \
    "${completed_at}" \
    "${ok}" \
    "${reason}" \
    "${restart_exit_code}" \
    "${restart_succeeded}" \
    "${gateway_health_check_ok}" \
    "${proof_gateway_log_bundle_path}" \
    "${rollback_gateway_log_bundle_path}"
  [[ "${ok}" == "true" ]]
}

case "${ACTION}" in
  gateway-start)
    run_local_ops gateway-start
    restart_gateway_with_passive_mode
    ;;
  rollback-execute)
    run_rollback_execute
    ;;
  gateway-stop|proxy-start|proxy-stop|ebusd-start|ebusd-stop)
    run_local_ops "${ACTION}"
    ;;
  smoke)
    run_local_ops smoke
    MATRIX_CASE_CANONICAL_ID="${canonical_case_id}" \
    MATRIX_CASE_EXEC_ID="${exec_case_id}" \
    MATRIX_CASE_ID="${canonical_case_id}" \
    MATRIX_GATEWAY_BASE_URL="${gateway_base_url}" \
    MATRIX_GRAPHQL_URL="${MATRIX_GRAPHQL_URL:-${gateway_base_url}/graphql}" \
    MATRIX_METRICS_URL="${MATRIX_METRICS_URL:-${gateway_base_url}/metrics}" \
    MATRIX_GW15_PROOF_MODE="${gw15_proof_mode}" \
    MATRIX_GW15_PROOF_HOLD_SEC="${MATRIX_GW15_PROOF_HOLD_SEC:-}" \
    PASSIVE_SMOKE_TIMEOUT_SEC="${PASSIVE_SMOKE_TIMEOUT_SEC:-${MATRIX_CASE_TIMEOUT_SEC:-}}" \
    PASSIVE_SMOKE_POLL_INTERVAL_SEC="${PASSIVE_SMOKE_POLL_INTERVAL_SEC:-}" \
    PASSIVE_PROOF_HOLD_SEC="${PASSIVE_PROOF_HOLD_SEC:-${MATRIX_GW15_PROOF_HOLD_SEC:-}}" \
    PASSIVE_PROOF_SAMPLE_INTERVAL_SEC="${PASSIVE_PROOF_SAMPLE_INTERVAL_SEC:-}" \
    PASSIVE_CANARY_MAX_RETRIES="${PASSIVE_CANARY_MAX_RETRIES:-}" \
      "${PASSIVE_CHECK_SCRIPT}"
    ;;
  *)
    echo "unknown action: ${ACTION}" >&2
    exit 2
    ;;
esac
