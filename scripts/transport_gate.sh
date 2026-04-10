#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

base_ref="${TRANSPORT_GATE_BASE_REF:-origin/main}"
if ! git rev-parse --verify "${base_ref}" >/dev/null 2>&1; then
  base_ref="main"
fi
if ! git rev-parse --verify "${base_ref}" >/dev/null 2>&1; then
  echo "transport gate: base ref not found, skipping."
  exit 0
fi

changed_files="$(
  {
    git diff --name-only "${base_ref}...HEAD"
    git diff --name-only
    git diff --name-only --cached
  } | awk 'NF { print }' | sort -u
)"
if [[ -z "${changed_files}" ]]; then
  echo "transport gate: no changes against ${base_ref}."
  exit 0
fi

requires_gate=0

requires_transport_gate() {
  local file="$1"
  case "${file}" in
    config.go|\
    gateway.go|\
    cmd/gateway/main.go|\
    cmd/gateway/startup_scan*.go|\
    cmd/matrix-runner/*|\
    internal/matrix/*|\
    smoke.go|\
    smoke_config.go|\
    cmd/smoke/*|\
    cmd/ebusdscan/*|\
    internal/adaptermux/*)
      return 0
      ;;
  esac
  return 1
}

while IFS= read -r file; do
  [[ -z "${file}" ]] && continue
  # The transport gate is for transport/protocol and matrix topology execution
  # surfaces. Public observability/API changes under cmd/gateway must not demand
  # an unrelated 88-case transport report.
  if requires_transport_gate "${file}"; then
    requires_gate=1
    break
  fi
done <<< "${changed_files}"

if [[ "${requires_gate}" -eq 0 ]]; then
  echo "transport gate: not triggered."
  exit 0
fi

if [[ "${TRANSPORT_GATE_OWNER_OVERRIDE:-}" == "OVERRIDE_TRANSPORT_GATE_BY_OWNER" ]]; then
  if [[ -z "${TRANSPORT_GATE_OWNER_REASON:-}" ]]; then
    echo "transport gate override requires TRANSPORT_GATE_OWNER_REASON."
    exit 1
  fi
  echo "transport gate: owner override active (${TRANSPORT_GATE_OWNER_REASON})."
  exit 0
fi

report_path="${TRANSPORT_MATRIX_REPORT:-}"
if [[ -z "${report_path}" ]]; then
  echo "transport gate: TRANSPORT_MATRIX_REPORT is required for transport/protocol changes."
  exit 1
fi
if [[ ! -f "${report_path}" ]]; then
  echo "transport gate: report not found at ${report_path}."
  exit 1
fi

# --- Adapter-direct (AD01..AD12) coverage tracking ---
# The embedded proxy / adapter multiplexer (internal/adaptermux/) has its
# own test matrix (AD01..AD12) covering:
#   AD01: INIT handshake fidelity
#   AD02: START arbitration (gateway priority, external FIFO)
#   AD03: START cancel (SYN) with ownership release
#   AD04: SEND echo suppression (gateway + external sessions)
#   AD05: In-band RESETTED handling + delayed re-INIT
#   AD06: Reconnect with session broadcast
#   AD07: Ownership timeout enforcement
#   AD08: Session backpressure (send buffer overflow)
#   AD09: Passive path filtering (third-party only)
#   AD10: INFO request forwarding
#   AD11: Multi-session concurrent arbitration
#   AD12: Wire phase tracking across transaction boundaries
#
# Current status: AD01..AD12 are covered by unit tests in
# internal/adaptermux/*_test.go. A formal matrix report (like the
# 88-case transport matrix) is pending implementation. This gate
# section surfaces the gap visibly without blocking CI.
adapter_direct_touched=0
while IFS= read -r file; do
  [[ -z "${file}" ]] && continue
  case "${file}" in
    internal/adaptermux/*.go)
      adapter_direct_touched=1
      break
      ;;
  esac
done <<< "${changed_files}"

if [[ "${adapter_direct_touched}" -eq 1 ]]; then
  # AD gate: adaptermux changes require AD01..AD12 coverage evidence.
  # Until the formal AD matrix report is implemented, allow explicit
  # bypass via HELIANTHUS_AD_GATE_SKIP with a documented reason.
  if [[ -n "${HELIANTHUS_AD_GATE_SKIP:-}" ]]; then
    echo "transport gate: AD gate bypassed — ${HELIANTHUS_AD_GATE_SKIP}"
  else
    echo "transport gate: FAIL — adapter-direct files (internal/adaptermux/) modified."
    echo "transport gate: AD01..AD12 coverage evidence required."
    echo "transport gate: Unit tests in internal/adaptermux/*_test.go must pass."
    echo "transport gate: Set HELIANTHUS_AD_GATE_SKIP='<reason>' to bypass with documented reason."
    exit 1
  fi
fi

python3 - "${report_path}" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)

cases = payload.get("cases")
if not isinstance(cases, list):
    print("transport gate: invalid matrix report (missing cases list).")
    raise SystemExit(1)

if len(cases) != 88:
    print(f"transport gate: expected 88 cases, got {len(cases)}.")
    raise SystemExit(1)

def normalized_outcome(case):
    outcome = case.get("outcome")
    if isinstance(outcome, str) and outcome:
        return outcome
    status = case.get("status")
    if status == "passed":
        return "pass"
    if status == "planned":
        return "planned"
    return "fail"

unexpected = []
xfailed = 0
xpassed = 0
passed = 0
blocked = 0
blocked_invalid = []
for case in cases:
    value = normalized_outcome(case)
    case_id = case.get("case_id", "?")
    if value == "pass":
        passed += 1
    elif value == "xfail":
        xfailed += 1
    elif value == "xpass":
        xpassed += 1
    elif value == "blocked-infra":
        reason = str(case.get("infra_reason", "")).strip()
        if reason != "adapter_no_signal":
            blocked_invalid.append((case_id, reason))
            continue
        blocked += 1
    else:
        unexpected.append(case_id)

if blocked_invalid:
    preview = ",".join(f"{case_id}:{reason or 'missing'}" for case_id, reason in blocked_invalid[:10])
    print(f"transport gate: matrix has blocked-infra with unsupported reason ({len(blocked_invalid)}). sample={preview}")
    raise SystemExit(1)

if unexpected:
    preview = ",".join(unexpected[:10])
    print(f"transport gate: matrix has unexpected failures/planned ({len(unexpected)}). sample={preview}")
    raise SystemExit(1)

msg = f"transport gate: PASS (pass={passed}, xfail={xfailed}, xpass={xpassed}, blocked={blocked}, total={len(cases)})."
if xpassed:
    msg += " review expected-failure list (xpass present)."
print(msg)
PY
