#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

base_ref="${PASSIVE_SMOKE_GATE_BASE_REF:-origin/main}"
if ! git rev-parse --verify "${base_ref}" >/dev/null 2>&1; then
  base_ref="main"
fi
if ! git rev-parse --verify "${base_ref}" >/dev/null 2>&1; then
  echo "passive smoke gate: base ref not found, skipping."
  exit 0
fi

changed_files="$(
  {
    git diff --name-only "${base_ref}...HEAD"
    git diff --name-only
    git diff --name-only --cached
    git ls-files --others --exclude-standard
  } | awk 'NF { print }' | sort -u
)"
if [[ -z "${changed_files}" ]]; then
  echo "passive smoke gate: no changes against ${base_ref}."
  exit 0
fi

requires_passive_smoke_gate() {
  local file="$1"
  case "${file}" in
    active_passive_deduplicator.go|\
    broadcast_listener.go|\
    bus_observability_store.go|\
    config.go|\
    cmd/gateway/main.go|\
    cmd/gateway/startup_scan*.go|\
    gateway.go|\
    passive_*.go|\
    cmd/matrix-runner/*|\
    internal/matrix/*|\
    scripts/passive_*.sh)
      return 0
      ;;
  esac
  return 1
}

requires_gate=0
while IFS= read -r file; do
  [[ -z "${file}" ]] && continue
  if requires_passive_smoke_gate "${file}"; then
    requires_gate=1
    break
  fi
done <<< "${changed_files}"

if [[ "${requires_gate}" -eq 0 ]]; then
  echo "passive smoke gate: not triggered."
  exit 0
fi

if [[ "${PASSIVE_SMOKE_GATE_OWNER_OVERRIDE:-}" == "OVERRIDE_PASSIVE_SMOKE_GATE_BY_OWNER" ]]; then
  if [[ -z "${PASSIVE_SMOKE_GATE_OWNER_REASON:-}" ]]; then
    echo "passive smoke gate override requires PASSIVE_SMOKE_GATE_OWNER_REASON."
    exit 1
  fi
  echo "passive smoke gate: owner override active (${PASSIVE_SMOKE_GATE_OWNER_REASON})."
  exit 0
fi

report_path="${PASSIVE_SMOKE_REPORT:-}"
if [[ -z "${report_path}" ]]; then
  echo "passive smoke gate: PASSIVE_SMOKE_REPORT is required when passive smoke coverage changes."
  exit 1
fi
if [[ ! -f "${report_path}" ]]; then
  echo "passive smoke gate: report not found at ${report_path}."
  exit 1
fi

python3 - "${report_path}" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)

suite = payload.get("suite")
cases = payload.get("cases")
if suite not in ("passive", None):
    print(f"passive smoke gate: expected suite 'passive', got {suite!r}.")
    raise SystemExit(1)
if not isinstance(cases, list):
    print("passive smoke gate: invalid passive report (missing cases list).")
    raise SystemExit(1)

expected = {
    "P01": "unsupported_or_misconfigured",
    "P02": "unsupported_or_misconfigured",
    "P03": "required",
    "P04": "required",
    "P05": "required",
    "P06": "unsupported_or_misconfigured",
}

if len(cases) != len(expected):
    print(f"passive smoke gate: expected {len(expected)} cases, got {len(cases)}.")
    raise SystemExit(1)

seen = set()
unexpected = []
for case in cases:
    case_id = str(case.get("case_id", "")).strip()
    if case_id not in expected:
      unexpected.append(case_id or "?")
      continue
    seen.add(case_id)
    passive_mode = str(case.get("passive_mode", "")).strip()
    if passive_mode != expected[case_id]:
        print(
            f"passive smoke gate: {case_id} passive_mode={passive_mode!r}; want {expected[case_id]!r}."
        )
        raise SystemExit(1)
    outcome = str(case.get("outcome", "")).strip()
    if outcome != "pass":
        print(f"passive smoke gate: {case_id} outcome={outcome!r}; want 'pass'.")
        raise SystemExit(1)

missing = sorted(set(expected) - seen)
if unexpected:
    print(f"passive smoke gate: unexpected case ids present: {','.join(unexpected[:10])}")
    raise SystemExit(1)
if missing:
    print(f"passive smoke gate: missing required cases: {','.join(missing)}")
    raise SystemExit(1)

print("passive smoke gate: PASS (6 passive cases, all pass).")
PY
