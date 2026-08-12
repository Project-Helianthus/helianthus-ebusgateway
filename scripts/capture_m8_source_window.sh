#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 --host HOST --port PORT --phase PRE_RESTART|POST_RESTART --window-id ID --output ABSOLUTE_DIR" >&2
  exit 2
}

host=""
port=""
phase=""
window_id=""
output=""
while (($#)); do
  case "$1" in
    --host) host="${2:-}"; shift 2 ;;
    --port) port="${2:-}"; shift 2 ;;
    --phase) phase="${2:-}"; shift 2 ;;
    --window-id) window_id="${2:-}"; shift 2 ;;
    --output) output="${2:-}"; shift 2 ;;
    *) usage ;;
  esac
done

[[ -n "${host}" && "${port}" =~ ^[0-9]+$ ]] || usage
[[ "${phase}" == "PRE_RESTART" || "${phase}" == "POST_RESTART" ]] || usage
[[ "${window_id}" =~ ^[A-Za-z0-9._-]+$ ]] || usage
[[ "${output}" == /* && "${output}" != */../* && "${output}" != */.. ]] || usage
command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }
command -v ssh >/dev/null || { echo "ssh is required" >&2; exit 1; }

umask 077
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
parent="$(dirname "${output}")"
mkdir -p "${parent}"
chmod 0700 "${parent}"
[[ ! -L "${parent}" && ! -e "${output}" && ! -e "${output}.manifest.json" && ! -e "${output}.metadata.json" ]] || { echo "unsafe or existing output" >&2; exit 1; }
staging="${parent}/.$(basename "${output}").tmp.$$"
metadata="${parent}/.$(basename "${output}").metadata.tmp.$$"
manifest="${output}.manifest.json"
trap 'rm -rf -- "${staging}" "${metadata}"' EXIT
mkdir -m 0700 "${staging}"

ssh_cmd=(ssh -o BatchMode=yes -o ConnectTimeout=10 -p "${port}" "root@${host}")

rpc() {
  local boundary="$1" tool="$2" arguments="$3" destination="$4"
  local payload
  payload="$(jq -cn --arg name "${tool}" --argjson arguments "${arguments}" \
    '{jsonrpc:"2.0",id:1,method:"tools/call",params:{name:$name,arguments:$arguments}}')"
  if [[ "${boundary}" == "public" ]]; then
    printf '%s' "${payload}" | "${ssh_cmd[@]}" \
      "curl -fsS -H 'Content-Type: application/json' --data-binary @- http://127.0.0.1:8080/mcp" \
      >"${destination}"
  elif [[ "${boundary}" == "scoped" ]]; then
    printf '%s' "${payload}" | "${ssh_cmd[@]}" \
      "docker exec -i app_local_helianthus curl -fsS --unix-socket /data/eebus/operator-mcp.sock -H 'Content-Type: application/json' -H 'X-Helianthus-Evidence-Scope: m8-read-only-v1' --data-binary @- http://localhost/mcp" \
      >"${destination}"
  elif [[ "${boundary}" == "operator" ]]; then
    printf '%s' "${payload}" | "${ssh_cmd[@]}" \
      "docker exec -i app_local_helianthus curl -fsS --unix-socket /data/eebus/operator-mcp.sock -H 'Content-Type: application/json' --data-binary @- http://localhost/mcp" \
      >"${destination}"
  else
    echo "unsupported MCP boundary: ${boundary}" >&2
    return 1
  fi
  jq -e '.result.isError == false and (.result.content | length) == 1' "${destination}" >/dev/null
}

capture_start="$(python3 -c 'import time; print(time.monotonic_ns())')"
captured_at="$(python3 -c 'import datetime; print(datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"))')"

printf '%s' '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' | "${ssh_cmd[@]}" \
  "docker exec -i app_local_helianthus curl -fsS --unix-socket /data/eebus/operator-mcp.sock -H 'Content-Type: application/json' -H 'X-Helianthus-Evidence-Scope: m8-read-only-v1' --data-binary @- http://localhost/mcp" \
  >"${staging}/tools-list.json"
jq -e '[.result.tools[].name] == [
  "ebus.v1.registry.devices.list",
  "ebus.v1.semantic.snapshot.get",
  "eebus.v1.runtime.status.get",
  "eebus.v1.services.list",
  "eebus.v1.services.get",
  "eebus.v1.sessions.list",
  "eebus.v1.sessions.get",
  "eebus.v1.topology.get",
  "eebus.v1.snapshot.capture",
  "eebus.v1.snapshot.drop",
  "eebus.v1.pairing.status.get"
]' "${staging}/tools-list.json" >/dev/null

rpc public ebus.v1.registry.devices.list '{}' "${staging}/ebus-devices.json"
rpc public ebus.v1.semantic.snapshot.get '{}' "${staging}/ebus-semantic.json"
rpc scoped eebus.v1.runtime.status.get '{}' "${staging}/eebus-runtime.json"
rpc scoped eebus.v1.services.list '{}' "${staging}/eebus-services.json"
rpc scoped eebus.v1.sessions.list '{}' "${staging}/eebus-sessions.json"
rpc scoped eebus.v1.pairing.status.get '{}' "${staging}/eebus-pairing.json"
rpc scoped eebus.v1.topology.get '{}' "${staging}/eebus-topology.json"

schema_query='{__schema{queryType{fields{name}} mutationType{fields{name}}}}'
values_query='{zones{id name config{operatingMode targetTempC}} dhw{config{operatingMode}}}'
"${ssh_cmd[@]}" "curl -fsS -H 'Content-Type: application/json' --data-binary '$(jq -cn --arg query "${schema_query}" '{query:$query}')' http://127.0.0.1:8080/graphql" \
  >"${staging}/graphql-schema.json"
"${ssh_cmd[@]}" "curl -fsS -H 'Content-Type: application/json' --data-binary '$(jq -cn --arg query "${values_query}" '{query:$query}')' http://127.0.0.1:8080/graphql" \
  >"${staging}/graphql-values.json"
"${ssh_cmd[@]}" "curl -fsS http://127.0.0.1:8080/portal/api/v1/bootstrap" \
  >"${staging}/portal-bootstrap.json"

for input_id in ebus.debug command.routing semantic.registry; do
  case "${input_id}" in
    ebus.debug) destination="ebus-debug.json" ;;
    command.routing) destination="command-routing.json" ;;
    semantic.registry) destination="semantic-registry.json" ;;
  esac
  rpc operator helianthus.experimental.m8_source_state.get "$(jq -cn --arg input_id "${input_id}" '{input_id:$input_id}')" "${staging}/.${destination}.envelope.json"
  jq -ce --arg input_id "${input_id}" \
    '.result.content[0].text | fromjson | select(.input_id == $input_id) | .data' \
    "${staging}/.${destination}.envelope.json" >"${staging}/${destination}"
  rm -f -- "${staging}/.${destination}.envelope.json"
done
"${ssh_cmd[@]}" \
  "docker inspect app_local_helianthus | jq -c '[.[0] | {Id: .Id, State: {StartedAt: .State.StartedAt}}]'" \
  >"${staging}/container-inspect.json"
printf '%s\n' "${captured_at}" >"${staging}/captured-at.txt"
capture_end="$(python3 -c 'import time; print(time.monotonic_ns())')"

find "${staging}" -type f -exec chmod 0600 {} +
jq -cn \
  --arg phase "${phase}" --arg window_id "${window_id}" --arg captured_at "${captured_at}" \
  --argjson capture_start_monotonic_ns "${capture_start}" --argjson capture_end_monotonic_ns "${capture_end}" \
  '{phase:$phase,window_id:$window_id,captured_at:$captured_at,capture_start_monotonic_ns:$capture_start_monotonic_ns,capture_end_monotonic_ns:$capture_end_monotonic_ns}' \
  >"${metadata}"
chmod 0600 "${metadata}"

publisher_args=(
  --source-root "${staging}"
  --destination "${output}"
  --manifest "${manifest}"
  --phase "${phase}"
  --window-id "${window_id}"
  --auth-scope-hash "sha256:fd99b012975e5e1c4309d4e64ab6f0520aaf890b95b6e3dd7a4b460df30e1223"
  --captured-at "${captured_at}"
  --capture-start-offset-ns "${capture_start}"
  --capture-end-offset-ns "${capture_end}"
)
if [[ -n "${M8_SOURCE_CAPTURE_PUBLISHER:-}" ]]; then
  "${M8_SOURCE_CAPTURE_PUBLISHER}" "${publisher_args[@]}" >/dev/null
else
  (
    cd "${repo_root}"
    go run ./internal/m8sourcecapture/cmd/m8sourcecapture "${publisher_args[@]}" >/dev/null
  )
fi
rm -rf -- "${staging}"
mv -- "${metadata}" "${output}.metadata.json"
trap - EXIT
printf '%s %s\n' "${output}" "${manifest}"
