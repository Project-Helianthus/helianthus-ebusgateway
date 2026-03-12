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
  if [[ "${MATRIX_USES_EBUSD:-0}" == "1" ]]; then
    local ebusd_config_src
    ebusd_config_src="$(select_ebusd_config_src)"
    MATRIX_CASE_CANONICAL_ID="${canonical_case_id}" \
    MATRIX_CASE_EXEC_ID="${exec_case_id}" \
    MATRIX_GATEWAY_BASE_URL="${gateway_base_url}" \
    MATRIX_GRAPHQL_URL="${MATRIX_GRAPHQL_URL:-${gateway_base_url}/graphql}" \
    MATRIX_METRICS_URL="${MATRIX_METRICS_URL:-${gateway_base_url}/metrics}" \
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
  ADAPTER_REQUIRE_SIGNAL="${ADAPTER_REQUIRE_SIGNAL:-0}" \
  MATRIX_CASE_ID="${exec_case_id}" \
    "${LOCAL_OPS_SCRIPT}" "$@"
}

restart_gateway_with_passive_mode() {
  local protocol network address
  IFS=';' read -r protocol network address < <(gateway_connection)

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
    --http-addr ':${gateway_http_port}' --mdns=false --broadcast=true \
    > '${remote_case_dir}/logs/gateway.log' 2>&1 & echo \$! > '${remote_case_dir}/state/gateway.pid'"
  remote_exec "for i in \$(seq 1 60); do kill -0 \$(cat '${remote_case_dir}/state/gateway.pid') || exit 2; ss -ltn '( sport = :${gateway_http_port} )' | grep -q LISTEN && exit 0; sleep 1; done; exit 1"
}

case "${ACTION}" in
  gateway-start)
    run_local_ops gateway-start
    restart_gateway_with_passive_mode
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
      "${PASSIVE_CHECK_SCRIPT}"
    ;;
  *)
    echo "unknown action: ${ACTION}" >&2
    exit 2
    ;;
esac
