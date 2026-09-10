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

requires_ebus_gate=0
requires_modbus_rtu_gate=0

requires_ebus_transport_gate() {
  local file="$1"
  case "${file}" in
    *_test.go)
      return 1
      ;;
  esac
  case "${file}" in
    config.go|\
    gateway.go|\
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

# The Modbus RTU gate is deliberately separate from the eBUS M6a topology
# matrix. It covers the gateway's explicit RTU composition boundary and the
# exact pinned upstream endpoint contract, without pretending an ENH/ENS/ebusd
# result proves an unrelated serial RTU path.
modbus_rtu_dependency_changed() {
  local changes
  changes="$({
    git diff --unified=0 "${base_ref}...HEAD" -- go.mod
    git diff --cached --unified=0 -- go.mod
    git diff --unified=0 -- go.mod
  } | awk '/^[+-][^+-]/ { print substr($0, 2) }')"
  grep -Eq '(^|[[:space:]])github\.com/Project-Helianthus/helianthus-modbus(reg)?[[:space:]]+' <<< "${changes}"
}

requires_modbus_rtu_transport_gate() {
  local file="$1"
  case "${file}" in
    *_test.go)
      return 1
      ;;
  esac
  case "${file}" in
    # Every non-test source input to issue #953's RTU configuration, lifecycle,
    # observer runtime, qualification/projection, provider registration, or
    # Portal provider binding is classified here.
    modbus_config.go|\
    cmd/gateway/gateway_cli.go|\
    cmd/gateway/gateway_http_server.go|\
    cmd/gateway/growatt_bms_rs485_runtime.go|\
	cmd/gateway/growatt_storage_semreg.go|\
    cmd/gateway/gateway_run_lifecycle.go|\
    cmd/gateway/modbus_endpoint_file.go|\
    cmd/gateway/modbus_mcp_provider.go|\
    mcp/growatt_bms_rs485_v202.go|\
    mcp/growatt_bms_rs485_v202_runtime.go|\
    mcp/modbus_v1.go)
      return 0
      ;;
    go.mod)
      modbus_rtu_dependency_changed
      return
      ;;
  esac
  return 1
}

run_modbus_rtu_transport_gate() {
  local module_dir inventory expected count
  local -a expected_tests=(
    TestRTUProductionReadRetainsImmutableCorrelatedEvidence
    TestRTUProductionExceptionDoesNotFenceButShortWriteDoes
    TestRTUProductionRejectsUnadmittedReadBeforeWrite
    TestRTUProductionRecoveryWaitsForRetiringReadOwnership
    TestRTUProductionFourSequentialReadsRemainBounded
    TestRTUProductionCancellationFencesAndPartialFramesRetainEvidence
    TestRTUProductionMalformedAndCRCFramesRemainTerminalEvidence
    TestRTUProductionRejectsTimingAndRecoveryBoundMismatch
    TestRTUProductionRecoveryDiscardsDelayedOldGenerationFrame
    TestRTUProductionRejectsRecoveryBoundsAndNoByteTimeout
  )
  echo "transport gate: Modbus RTU production conformance."
  if ! GOWORK=off go test ./cmd/gateway \
    -run 'Test(GrowattBMSRS485|PortalRawModbusUsesOnlyTCPAvailableComposition)' -count=1; then
    echo "transport gate: FAIL — Modbus RTU gateway composition evidence failed."
    return 1
  fi
  module_dir="$(GOWORK=off go list -m -f '{{.Dir}}' github.com/Project-Helianthus/helianthus-modbus)" || {
    echo "transport gate: FAIL — pinned helianthus-modbus dependency is unavailable."
    return 1
  }
  inventory="$(GOWORK=off go test "${module_dir}" -list '^TestRTUProduction')" || {
    echo "transport gate: FAIL — pinned Modbus RTU endpoint test inventory failed."
    return 1
  }
  for expected in "${expected_tests[@]}"; do
    count="$(grep -Fxc "${expected}" <<< "${inventory}" || true)"
    if [[ "${count}" -ne 1 ]]; then
      echo "transport gate: FAIL — pinned Modbus RTU endpoint inventory missing or duplicates ${expected}."
      return 1
    fi
  done
  if ! GOWORK=off go test "${module_dir}" -run '^TestRTUProduction' -count=1; then
    echo "transport gate: FAIL — pinned Modbus RTU endpoint conformance failed."
    return 1
  fi
  echo "transport gate: PASS (Modbus RTU production composition and pinned endpoint conformance)."
}

# SemReg public read-surface configuration (PV or storage) does not alter a
# protocol transport, topology, acquisition, or runtime admission. Keep the
# 88-case transport gate whenever an actual transport family is touched.
semreg_public_config_only() {
	local changes line trimmed
  changes="$({
    git diff --unified=0 "${base_ref}...HEAD" -- config.go
    git diff --cached --unified=0 -- config.go
    git diff --unified=0 -- config.go
  } | awk '/^[+-][^+-]/ { print substr($0, 2) }')"
	[[ -n "${changes}" ]] || return 1
	# Storage-only changes are exempt only when the complete source outside the
	# exact declaration, Config field, and validator is byte-equivalent.
	if grep -Fq "PortalStorage" <<< "${changes}"; then
		python3 scripts/semreg_public_config_classifier.py "${base_ref}"
		return
	fi
	while IFS= read -r line; do
		trimmed="${line#"${line%%[![:space:]]*}"}"
		trimmed="${trimmed%"${trimmed##*[![:space:]]}"}"
		if [[ "${trimmed}" == "}" ]]; then continue; fi
		case "${trimmed}" in
			"// M2MGraphQLConfig configures the dedicated public canonical-PV listener."|\
			"// M2MGraphQLConfig configures the dedicated public SemReg PV listener."|\
			"KnownAssets                 []string"|\
			"len(config.KnownAssets) == 0 && len(config.DeniedPrincipalFingerprints) == 0"|\
			"len(config.DeniedPrincipalFingerprints) == 0"|\
			"known := make(map[string]struct{}, len(config.KnownAssets))"|\
			"for _, asset := range config.KnownAssets {"|\
			"if _, allowed := assets[asset]; !allowed {"|\
			"return errors.New(\"M2M GraphQL configuration contains a known asset outside the allowlist\")"|\
			"if _, duplicate := known[asset]; duplicate {"|\
			"return errors.New(\"M2M GraphQL configuration contains a duplicate known asset\")"|\
			"known[asset] = struct{}{}") ;;
			*) return 1 ;;
		esac
	done <<< "${changes}"
	return 0
}

cmd_gateway_main_requires_transport_gate() {
  python3 - "$base_ref" <<'PY'
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

base_ref = sys.argv[1]
diffs: list[str] = []
for args in (
    ["git", "diff", "--unified=0", f"{base_ref}...HEAD", "--", "cmd/gateway/main.go"],
    ["git", "diff", "--cached", "--unified=0", "--", "cmd/gateway/main.go"],
    ["git", "diff", "--unified=0", "--", "cmd/gateway/main.go"],
):
    result = subprocess.run(args, text=True, capture_output=True, check=False)
    if result.returncode == 0 and result.stdout:
        diffs.append(result.stdout)

replacements = (
    ("startup admission", "startup source selection"),
    ("startup-admission", "startup-source-selection"),
    ("admission_path_selected", "source_selection.mode"),
    ("StartupAdmission", "StartupSourceSelection"),
    ("AdmissionArtifact", "SourceSelectionArtifact"),
    ("admission-artifact", "source-selection-artifact"),
    ("SetAdmissionPathSelected", "SetSourceSelectionMode"),
    ("NewAdmissionArtifactBuilder", "NewSourceSelectionArtifactBuilder"),
    ("GetOrInitStartupAdmissionMetrics", "GetOrInitStartupSourceSelectionMetrics"),
    ("FormatStartupAdmissionOverrideLog", "FormatStartupSourceSelectionExplicitLog"),
    ("FormatStartupSourceSelectionOverrideLog", "FormatStartupSourceSelectionExplicitLog"),
    ("CheckOverrideCompanionConflict", "CheckExplicitSourceCompanionConflict"),
    ("SetOverrideActive", "SetExplicitSourceActive"),
    ("RecordOverrideBypass", "RecordExplicitValidateOnly"),
    ("SetOverrideSource", "SetExplicitSource"),
    ("SetActiveOverride", "SetActiveExplicitSource"),
    ("startup_admission_source_selection_bus", "startup_source_selection_bus"),
    ("startup_admission_", "startup_source_selection_"),
    ("override path", "explicit source path"),
    ("override source", "explicit source"),
    ("override Initiator", "explicit source"),
    ("override", "explicit_validate_only"),
    ("AD23", "SAS M4"),
    ("; legacy static-source path active", ""),
    ("JoinCapable", "source-selection"),
    ("→", "->"),
)

def normalize(line: str) -> str | None:
    stripped = line.strip()
    if not stripped or stripped in {"}", ")"} or stripped.startswith("//"):
        return None
    compact = re.sub(r"\s+", " ", stripped)
    if compact in {
        "wireEEBusPromotedSemanticGraphQL(ctx, builder, semanticRuntime.Provider(), eebusAdapter)",
        "portalSemanticProvider := wireEEBusPromotedSemanticGraphQL(ctx, builder, semanticRuntime.Provider(), eebusAdapter)",
        "portalSemanticProvider,",
        "semanticRuntime.Provider(),",
        "OperationModeChangeable: cloneBoolPtr(zone.Config.OperationModeChangeable),",
        "SourceLabel: cloneStringPtr(zone.Config.SourceLabel),",
        "OverrunActive: cloneBoolPtr(status.State.OverrunActive),",
        "OperationModeChangeable: cloneBoolPtr(status.Config.OperationModeChangeable),",
        "GatewayBrand: cloneStringPtr(status.GatewayBrand),",
        "GatewayVendor: cloneStringPtr(status.GatewayVendor),",
    }:
        return None
    if re.fullmatch(r'buildVersion\s*=\s*"[0-9]+(?:\.[0-9]+)+(?:[-+][0-9A-Za-z.-]+)?"', stripped):
        return None
    for old, new in replacements:
        stripped = stripped.replace(old, new)
    return re.sub(r"\s+", " ", stripped)

def normalize_text(text: str) -> list[str]:
    lines: list[str] = []
    for line in text.splitlines():
        normalized = normalize(line)
        if normalized is not None:
            lines.append(normalized)
    return lines

if not diffs:
    raise SystemExit(0)

base_result = subprocess.run(
    ["git", "show", f"{base_ref}:cmd/gateway/main.go"],
    text=True,
    capture_output=True,
    check=False,
)
if base_result.returncode != 0:
    print("transport gate: cannot read base cmd/gateway/main.go", file=sys.stderr)
    raise SystemExit(1)

current_text = Path("cmd/gateway/main.go").read_text(encoding="utf-8")
removed = normalize_text(base_result.stdout)
added = normalize_text(current_text)

if removed != added:
    print("transport gate: cmd/gateway/main.go contains non-rename runtime diff", file=sys.stderr)
    print(f"transport gate: removed={removed}", file=sys.stderr)
    print(f"transport gate: added={added}", file=sys.stderr)
    raise SystemExit(1)

raise SystemExit(0)
PY
}

while IFS= read -r file; do
  [[ -z "${file}" ]] && continue
  # The transport gate is for transport/protocol and matrix topology execution
  # surfaces. Public observability/API changes under cmd/gateway must not demand
  # an unrelated 88-case transport report.
  if [[ "${file}" == "cmd/gateway/main.go" ]]; then
    if cmd_gateway_main_requires_transport_gate; then
      continue
    fi
    requires_ebus_gate=1
    requires_modbus_rtu_gate=1
    continue
  fi
	if [[ "${file}" == "config.go" ]] && semreg_public_config_only; then
    continue
  fi
  if [[ "${file}" == "config.go" ]]; then
    requires_ebus_gate=1
    requires_modbus_rtu_gate=1
    continue
  fi
  if requires_ebus_transport_gate "${file}"; then
    requires_ebus_gate=1
  fi
  if requires_modbus_rtu_transport_gate "${file}"; then
    requires_modbus_rtu_gate=1
  fi
done <<< "${changed_files}"

if [[ "${requires_ebus_gate}" -eq 0 && "${requires_modbus_rtu_gate}" -eq 0 ]]; then
  echo "transport gate: not triggered."
  exit 0
fi

if [[ "${TRANSPORT_GATE_OWNER_OVERRIDE:-}" == "OVERRIDE_TRANSPORT_GATE_BY_OWNER" ]]; then
  if [[ -z "${TRANSPORT_GATE_OWNER_REASON:-}" ]]; then
    echo "transport gate override requires TRANSPORT_GATE_OWNER_REASON."
    exit 1
  fi
  echo "transport gate: owner override active (${TRANSPORT_GATE_OWNER_REASON})."
  # A documented owner override is scope-specific but applies to the current
  # gate invocation as a whole: main.go can activate both eBUS and RTU gates.
  requires_ebus_gate=0
  requires_modbus_rtu_gate=0
  exit 0
fi

if [[ "${requires_ebus_gate}" -eq 1 ]]; then
  report_path="${TRANSPORT_MATRIX_REPORT:-}"
  if [[ -z "${report_path}" ]]; then
    echo "transport gate: TRANSPORT_MATRIX_REPORT is required for eBUS transport/protocol changes."
    exit 1
  fi
  if [[ ! -f "${report_path}" ]]; then
    echo "transport gate: report not found at ${report_path}."
    exit 1
  fi
fi

# --- Adapter-direct (AD01..AD12) coverage tracking ---
# The embedded proxy / adapter multiplexer (internal/adaptermux/) has its
# own test matrix (AD01..AD12) covering:
#   AD01: Active path sends/receives transactions
#   AD02: Passive path zero self-echo
#   AD03: Passive path reconstructs third-party traffic
#   AD04: RESETTED propagates to all consumers
#   AD05: ebusd connects, receives broadcasts
#   AD06: ebusd sends START/SEND successfully
#   AD07: Gateway and ebusd compete fairly at SYN
#   AD08: Per-session echo suppression
#   AD09: Adapter disconnect triggers reconnection recovery
#   AD10: Migration preserves semantics
#   AD11: Rollback restores behavior
#   AD12: No 0xA9/0xAA escape corruption
#
# Current status: AD01..AD12 are covered by unit tests in
# internal/adaptermux/*_test.go. A formal matrix report (like the
# 88-case transport matrix) is pending implementation. This gate
# section surfaces the gap visibly without blocking CI.
adapter_direct_touched=0
while IFS= read -r file; do
  [[ -z "${file}" ]] && continue
  case "${file}" in
    internal/adaptermux/*_test.go)
      continue
      ;;
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

if [[ "${requires_ebus_gate}" -eq 1 ]]; then
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
xpassed = []
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
        xpassed.append(case_id)
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

if xpassed:
    preview = ",".join(xpassed[:10])
    print(f"transport gate: matrix has unexpected xpass ({len(xpassed)}). sample={preview}")
    raise SystemExit(1)

msg = f"transport gate: PASS (pass={passed}, xfail={xfailed}, xpass={len(xpassed)}, blocked={blocked}, total={len(cases)})."
print(msg)
PY
fi

if [[ "${requires_modbus_rtu_gate}" -eq 1 ]]; then
  run_modbus_rtu_transport_gate
fi
