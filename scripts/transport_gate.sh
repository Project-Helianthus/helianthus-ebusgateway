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
owner_override_active=0

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
    go.mod)
      ebus_dependency_changed
      return
      ;;
  esac
  return 1
}

# B503's session manager, dispatcher, installation wiring, MCP operation owner,
# and GraphQL operation provider are native eBUS operation surfaces. They can
# construct, invoke, issue, or fence raw-frame requests and select the production
# sender, so a source change must require a current AdapterMux conformance
# matrix (adaptermux-v2) report even though their paths do not live under the
# generic scanner or mux directories
# above. Keep this classifier explicit: a filename match is auditable and does
# not turn unrelated Portal B503 presentation changes into transport work.
requires_b503_native_transport_gate() {
  local file="$1"
  case "${file}" in
    *_test.go)
      return 1
      ;;
  esac
  case "${file}" in
    cmd/gateway/vaillant_b503_dispatcher.go|\
    cmd/gateway/vaillant_b503_graphql_provider.go|\
    cmd/gateway/vaillant_b503_wiring.go|\
    mcp/vaillant_b503.go|\
    internal/vaillant/b503session/*.go)
      return 0
      ;;
  esac
  return 1
}

# Dependency-only repins can replace production eBUS transport/protocol code.
ebus_dependency_changed() {
  local changes
  changes="$({
    git diff --unified=0 "${base_ref}...HEAD" -- go.mod
    git diff --cached --unified=0 -- go.mod
    git diff --unified=0 -- go.mod
  } | awk '/^[+-][^+-]/ { print substr($0, 2) }')"
  grep -Eq '(^|[[:space:]])github\.com/Project-Helianthus/helianthus-ebusgo[[:space:]]+' <<< "${changes}"
}

# The Modbus RTU gate is deliberately separate from the eBUS AdapterMux
# conformance matrix. It covers the gateway's explicit RTU composition boundary and the
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
	cmd/gateway/gateway_http_portal_surface.go|\
	cmd/gateway/gateway_http_mcp_surface.go|\
	cmd/gateway/gateway_run_setup.go|\
    cmd/gateway/growatt_bms_rs485_runtime.go|\
    cmd/gateway/growatt_managed_driver.go|\
	cmd/gateway/tesla_hsc_retained_owner.go|\
	cmd/gateway/growatt_storage_semreg.go|\
	cmd/gateway/m2m_graphql_runtime.go|\
	cmd/gateway/m2m_graphql_config.go|\
	cmd/gateway/portal_pv_client.go|\
	m2mgraphql/handler.go|\
	m2mgraphql/client.go|\
	portal/handler.go|\
	mcp/growatt_storage_semantic.go|\
    cmd/gateway/gateway_run_lifecycle.go|\
    cmd/gateway/gateway_process_shell.go|\
    cmd/gateway/gateway_api_composition.go|\
    cmd/gateway/gateway_lifecycle_observability.go|\
    cmd/gateway/gateway_retained_consumers.go|\
    cmd/gateway/gateway_ebus_generation_owners.go|\
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
    -run 'Test(M04|M06|GrowattBMSRS485|PortalRawModbusUsesOnlyTCPAvailableComposition|TeslaHSCRetained)' -count=1; then
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
# adaptermux-v2 transport gate whenever an actual transport family is touched.
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
			"// PrometheusEVSEEnabled enables only the detached EVSE SemReg observation"|\
			"// section. It neither configures acquisition nor grants a native runtime."|\
			"PrometheusEVSEEnabled bool"|\
			"// M2MGraphQLConfig configures the dedicated public canonical-PV listener."|\
			"// M2MGraphQLConfig configures the dedicated public SemReg PV listener."|\
			"KnownAssets                 []string"|\
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

# The Matter binding feed is a detached HTTP/M2M composition seam. Its
# allowlist is an exact source diff, so any adjacent runtime, protocol, or
# native change fails the classifier and remains subject to the normal gates.
matter_feed_api_composition_only() {
  python3 scripts/matter_feed_transport_classifier.py "${base_ref}" "$1"
}

# Documentation-only changes cannot alter a transport. Keep this deliberately
# narrow: every added or removed non-blank line must be a Go line comment.
config_comments_only() {
  python3 - "${base_ref}" <<'PY'
from __future__ import annotations

import difflib
import subprocess
import sys
from pathlib import Path

base_ref = sys.argv[1]
result = subprocess.run(
    ["git", "show", f"{base_ref}:config.go"],
    text=True,
    capture_output=True,
    check=False,
)
if result.returncode:
    raise SystemExit(1)
try:
    current = Path("config.go").read_text(encoding="utf-8")
except OSError:
    raise SystemExit(1)

changed = []
for line in difflib.unified_diff(result.stdout.splitlines(), current.splitlines(), n=0):
    if line.startswith(("---", "+++", "@@")):
        continue
    if line.startswith(("+", "-")):
        changed.append(line[1:].strip())

if changed and all(not line or line.startswith("//") for line in changed):
    raise SystemExit(0)
raise SystemExit(1)
PY
}

while IFS= read -r file; do
  [[ -z "${file}" ]] && continue
  # The transport gate is for transport/protocol and matrix topology execution
  # surfaces. Public observability/API changes under cmd/gateway must not demand
  # an unrelated adaptermux-v2 transport report.
  if [[ "${file}" == "cmd/gateway/main.go" ]]; then
    if cmd_gateway_main_requires_transport_gate; then
      continue
    fi
    requires_ebus_gate=1
    requires_modbus_rtu_gate=1
    continue
  fi
  if matter_feed_api_composition_only "${file}"; then
    continue
  fi
  if [[ "${file}" == "config.go" ]] && config_comments_only; then
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
	if [[ "${file}" == "cmd/gateway/gateway_cli.go" || "${file}" == "cmd/gateway/gateway_run_lifecycle.go" ]] && python3 scripts/semreg_prometheus_transport_classifier.py "${base_ref}" "${file}"; then
		continue
	fi
  if requires_b503_native_transport_gate "${file}"; then
    requires_ebus_gate=1
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
  # The owner override covers only the physical requirements: the eBUS
  # TRANSPORT_MATRIX_REPORT check of the AdapterMux conformance matrix
  # (adaptermux-v2), AD cells included. The offline Modbus RTU conformance gate
  # is never skipped by the override.
  requires_ebus_gate=0
  owner_override_active=1
fi

if [[ "${requires_ebus_gate}" -eq 1 ]]; then
  report_path="${TRANSPORT_MATRIX_REPORT:-}"
  if [[ -z "${report_path}" ]]; then
    echo "transport gate: TRANSPORT_MATRIX_REPORT is required for eBUS transport/protocol changes (an adaptermux-v2 index written by matrix-runner --suite adaptermux-v2)."
    exit 1
  fi
  if [[ ! -f "${report_path}" ]]; then
    echo "transport gate: report not found at ${report_path}."
    exit 1
  fi
fi

# --- Adapter-direct cells ---
# The AD cells of the AdapterMux (internal/adaptermux/) are part of the
# adaptermux-v2 report: AD01..AD10 and AD12 on every southbound transport (AD11
# is retired). A non-test change under internal/adaptermux/ triggers the eBUS
# gate above, so the report check below proves that the report's AD cells are
# present and accepted; the owner override covers them like every other cell.
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

if [[ "${requires_ebus_gate}" -eq 1 ]]; then
python3 - "${report_path}" "${adapter_direct_touched}" <<'ADAPTERMUX_V2_REPORT_CHECK'
"""Check an adaptermux-v2 aggregate index against the declared inventory.

The rules are those of docs/testing/transport-gate-policy-v2.md, section 4.
The declaration below must equal the inventory of internal/matrix; a Go test
of that package renders it from the generator and fails on any difference.
"""
import json
import sys

# BEGIN adaptermux-v2 inventory declaration
INVENTORY_VERSION = "2"
SUITE = "adaptermux-v2"
# Accepted categories: the outcome a cell of the category carries and the
# reason string it must name. BI-01 applies to executable cells only.
CATEGORIES = {
    "BI-01": ("blocked-infra", "adapter_no_signal"),
    "BI-02": ("blocked-infra", "uart_direct_hardware_absent"),
    "PL-01": ("planned", "mux_southbound_plain_not_implemented"),
    "PL-02": ("planned", "mux_local_responder_not_implemented"),
    "PL-03": ("planned", "mux_raw_symbol_forwarding_not_implemented"),
    "PL-04": ("planned", "fifo_order_superseded_by_address_priority"),
}
EXPECTED_FAILURES = {
    "TCP-CX01": ("XF-01", "southbound plain tcp under dual-client load reports no signal (carried from proxy dual-client)"),
}
# case_id, row whose hardware setup runs it, southbound transport, invariant,
# reserved category ("-" for an executable cell).
CELLS = """
ENS-AD01 ENS ens AD01 -
ENS-AD02 ENS ens AD02 -
ENS-AD03 ENS ens AD03 -
ENS-AD04 ENS ens AD04 -
ENS-AD05 ENS ens AD05 PL-03
ENS-AD06 ENS ens AD06 -
ENS-AD07 ENS ens AD07 -
ENS-AD08 ENS ens AD08 PL-03
ENS-AD09 ENS ens AD09 -
ENS-AD10 ENS ens AD10 -
ENS-AD12 ENS ens AD12 PL-03
ENS-PX01 ENS ens PX01 -
ENS-PX02 ENS ens PX02 -
ENS-PX03 ENS ens PX03 -
ENS-PX04 ENS ens PX04 -
ENS-PX05 ENS ens PX05 PL-04
ENS-PX06 ENS ens PX06 PL-04
ENS-PX07 ENS ens PX07 PL-04
ENS-PX08 ENS ens PX08 PL-04
ENS-PX09 ENS ens PX09 PL-02
ENS-PX10 ENS ens PX10 PL-02
ENS-PX11 ENS ens PX11 PL-02
ENS-PX12 ENS ens PX12 -
ENS-CX01 ENS ens CX01 -
ENH-AD01 ENH enh AD01 -
ENH-AD02 ENH enh AD02 -
ENH-AD03 ENH enh AD03 -
ENH-AD04 ENH enh AD04 -
ENH-AD05 ENH enh AD05 PL-03
ENH-AD06 ENH enh AD06 -
ENH-AD07 ENH enh AD07 -
ENH-AD08 ENH enh AD08 PL-03
ENH-AD09 ENH enh AD09 -
ENH-AD10 ENH enh AD10 -
ENH-AD12 ENH enh AD12 PL-03
ENH-PX01 ENH enh PX01 -
ENH-PX02 ENH enh PX02 -
ENH-PX03 ENH enh PX03 -
ENH-PX04 ENH enh PX04 -
ENH-PX05 ENH enh PX05 PL-04
ENH-PX06 ENH enh PX06 PL-04
ENH-PX07 ENH enh PX07 PL-04
ENH-PX08 ENH enh PX08 PL-04
ENH-PX09 ENH enh PX09 PL-02
ENH-PX10 ENH enh PX10 PL-02
ENH-PX11 ENH enh PX11 PL-02
ENH-PX12 ENH enh PX12 -
ENH-CX01 ENH enh CX01 -
UDP-AD01 UDP udp AD01 PL-01
UDP-AD02 UDP udp AD02 PL-01
UDP-AD03 UDP udp AD03 PL-01
UDP-AD04 UDP udp AD04 PL-01
UDP-AD05 UDP udp AD05 PL-01
UDP-AD06 UDP udp AD06 PL-01
UDP-AD07 UDP udp AD07 PL-01
UDP-AD08 UDP udp AD08 PL-01
UDP-AD09 UDP udp AD09 PL-01
UDP-AD10 UDP udp AD10 PL-01
UDP-AD12 UDP udp AD12 PL-01
UDP-PX01 UDP udp PX01 PL-01
UDP-PX02 UDP udp PX02 PL-01
UDP-PX03 UDP udp PX03 PL-01
UDP-PX04 UDP udp PX04 PL-01
UDP-PX05 UDP udp PX05 PL-01
UDP-PX06 UDP udp PX06 PL-01
UDP-PX07 UDP udp PX07 PL-01
UDP-PX08 UDP udp PX08 PL-01
UDP-PX09 UDP udp PX09 PL-01
UDP-PX10 UDP udp PX10 PL-01
UDP-PX11 UDP udp PX11 PL-01
UDP-PX12 UDP udp PX12 PL-01
UDP-CX01 UDP udp CX01 PL-01
TCP-AD01 TCP tcp AD01 PL-01
TCP-AD02 TCP tcp AD02 PL-01
TCP-AD03 TCP tcp AD03 PL-01
TCP-AD04 TCP tcp AD04 PL-01
TCP-AD05 TCP tcp AD05 PL-01
TCP-AD06 TCP tcp AD06 PL-01
TCP-AD07 TCP tcp AD07 PL-01
TCP-AD08 TCP tcp AD08 PL-01
TCP-AD09 TCP tcp AD09 PL-01
TCP-AD10 TCP tcp AD10 PL-01
TCP-AD12 TCP tcp AD12 PL-01
TCP-PX01 TCP tcp PX01 PL-01
TCP-PX02 TCP tcp PX02 PL-01
TCP-PX03 TCP tcp PX03 PL-01
TCP-PX04 TCP tcp PX04 PL-01
TCP-PX05 TCP tcp PX05 PL-01
TCP-PX06 TCP tcp PX06 PL-01
TCP-PX07 TCP tcp PX07 PL-01
TCP-PX08 TCP tcp PX08 PL-01
TCP-PX09 TCP tcp PX09 PL-01
TCP-PX10 TCP tcp PX10 PL-01
TCP-PX11 TCP tcp PX11 PL-01
TCP-PX12 TCP tcp PX12 PL-01
TCP-CX01 TCP tcp CX01 PL-01
UARTENS-AD01 UARTENS uart-ens AD01 BI-02
UARTENS-AD02 UARTENS uart-ens AD02 BI-02
UARTENS-AD03 UARTENS uart-ens AD03 BI-02
UARTENS-AD04 UARTENS uart-ens AD04 BI-02
UARTENS-AD05 UARTENS uart-ens AD05 BI-02
UARTENS-AD06 UARTENS uart-ens AD06 BI-02
UARTENS-AD07 UARTENS uart-ens AD07 BI-02
UARTENS-AD08 UARTENS uart-ens AD08 BI-02
UARTENS-AD09 UARTENS uart-ens AD09 BI-02
UARTENS-AD10 UARTENS uart-ens AD10 BI-02
UARTENS-AD12 UARTENS uart-ens AD12 BI-02
UARTENS-PX01 UARTENS uart-ens PX01 BI-02
UARTENS-PX02 UARTENS uart-ens PX02 BI-02
UARTENS-PX03 UARTENS uart-ens PX03 BI-02
UARTENS-PX04 UARTENS uart-ens PX04 BI-02
UARTENS-PX05 UARTENS uart-ens PX05 BI-02
UARTENS-PX06 UARTENS uart-ens PX06 BI-02
UARTENS-PX07 UARTENS uart-ens PX07 BI-02
UARTENS-PX08 UARTENS uart-ens PX08 BI-02
UARTENS-PX09 UARTENS uart-ens PX09 BI-02
UARTENS-PX10 UARTENS uart-ens PX10 BI-02
UARTENS-PX11 UARTENS uart-ens PX11 BI-02
UARTENS-PX12 UARTENS uart-ens PX12 BI-02
UARTENS-CX01 UARTENS uart-ens CX01 BI-02
UARTENH-AD01 UARTENH uart-enh AD01 BI-02
UARTENH-AD02 UARTENH uart-enh AD02 BI-02
UARTENH-AD03 UARTENH uart-enh AD03 BI-02
UARTENH-AD04 UARTENH uart-enh AD04 BI-02
UARTENH-AD05 UARTENH uart-enh AD05 BI-02
UARTENH-AD06 UARTENH uart-enh AD06 BI-02
UARTENH-AD07 UARTENH uart-enh AD07 BI-02
UARTENH-AD08 UARTENH uart-enh AD08 BI-02
UARTENH-AD09 UARTENH uart-enh AD09 BI-02
UARTENH-AD10 UARTENH uart-enh AD10 BI-02
UARTENH-AD12 UARTENH uart-enh AD12 BI-02
UARTENH-PX01 UARTENH uart-enh PX01 BI-02
UARTENH-PX02 UARTENH uart-enh PX02 BI-02
UARTENH-PX03 UARTENH uart-enh PX03 BI-02
UARTENH-PX04 UARTENH uart-enh PX04 BI-02
UARTENH-PX05 UARTENH uart-enh PX05 BI-02
UARTENH-PX06 UARTENH uart-enh PX06 BI-02
UARTENH-PX07 UARTENH uart-enh PX07 BI-02
UARTENH-PX08 UARTENH uart-enh PX08 BI-02
UARTENH-PX09 UARTENH uart-enh PX09 BI-02
UARTENH-PX10 UARTENH uart-enh PX10 BI-02
UARTENH-PX11 UARTENH uart-enh PX11 BI-02
UARTENH-PX12 UARTENH uart-enh PX12 BI-02
UARTENH-CX01 UARTENH uart-enh CX01 BI-02
UARTRAW-AD01 UARTRAW uart-raw AD01 BI-02
UARTRAW-AD02 UARTRAW uart-raw AD02 BI-02
UARTRAW-AD03 UARTRAW uart-raw AD03 BI-02
UARTRAW-AD04 UARTRAW uart-raw AD04 BI-02
UARTRAW-AD05 UARTRAW uart-raw AD05 BI-02
UARTRAW-AD06 UARTRAW uart-raw AD06 BI-02
UARTRAW-AD07 UARTRAW uart-raw AD07 BI-02
UARTRAW-AD08 UARTRAW uart-raw AD08 BI-02
UARTRAW-AD09 UARTRAW uart-raw AD09 BI-02
UARTRAW-AD10 UARTRAW uart-raw AD10 BI-02
UARTRAW-AD12 UARTRAW uart-raw AD12 BI-02
UARTRAW-PX01 UARTRAW uart-raw PX01 BI-02
UARTRAW-PX02 UARTRAW uart-raw PX02 BI-02
UARTRAW-PX03 UARTRAW uart-raw PX03 BI-02
UARTRAW-PX04 UARTRAW uart-raw PX04 BI-02
UARTRAW-PX05 UARTRAW uart-raw PX05 BI-02
UARTRAW-PX06 UARTRAW uart-raw PX06 BI-02
UARTRAW-PX07 UARTRAW uart-raw PX07 BI-02
UARTRAW-PX08 UARTRAW uart-raw PX08 BI-02
UARTRAW-PX09 UARTRAW uart-raw PX09 BI-02
UARTRAW-PX10 UARTRAW uart-raw PX10 BI-02
UARTRAW-PX11 UARTRAW uart-raw PX11 BI-02
UARTRAW-PX12 UARTRAW uart-raw PX12 BI-02
UARTRAW-CX01 UARTRAW uart-raw CX01 BI-02
ENS-DS01 ENS ens DS01 -
ENH-DS01 ENH enh DS01 -
UDP-DS01 UDP udp DS01 -
TCP-DS01 TCP tcp DS01 -
EBUSD-ES01 ENH ebusd-tcp ES01 -
"""
# END adaptermux-v2 inventory declaration

path = sys.argv[1]
adapter_direct_touched = len(sys.argv) > 2 and sys.argv[2] == "1"
PREFIX = "transport gate: "


def fail(message, problems=()):
    print(PREFIX + message)
    for problem in list(problems)[:10]:
        print(PREFIX + "  " + problem)
    if len(problems) > 10:
        print(PREFIX + "  ... and %d more" % (len(problems) - 10))
    raise SystemExit(1)


cells = {}
order = []
for line in CELLS.strip().splitlines():
    case_id, row, southbound, invariant, category = line.split()
    cells[case_id] = {"row": row, "southbound": southbound, "invariant": invariant,
                      "category": None if category == "-" else category}
    order.append(case_id)

try:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
except (OSError, ValueError) as error:
    fail("TRANSPORT_MATRIX_REPORT is not readable JSON (%s)." % type(error).__name__)
if not isinstance(payload, dict):
    fail("TRANSPORT_MATRIX_REPORT is not an adaptermux-v2 index (not a JSON object).")

cases = payload.get("cases")
suite = payload.get("suite")
if suite != SUITE:
    retired = suite in (None, "", "full88") and isinstance(cases, list) and any(
        isinstance(case, dict) and str(case.get("case_id", "")).startswith("T") for case in cases)
    detail = "the retired 88-case full88 form" if retired or suite == "full88" else "suite %r" % (suite,)
    fail("TRANSPORT_MATRIX_REPORT must be an adaptermux-v2 index (AdapterMux conformance matrix), "
         "got %s. Point it at the index.json written by matrix-runner --suite adaptermux-v2." % detail)

version = payload.get("matrix_inventory_version")
if version != INVENTORY_VERSION:
    fail("adaptermux-v2 report has matrix_inventory_version %r; the gate declares %r." % (version, INVENTORY_VERSION))

identity = {}
for key in ("session_id", "candidate", "target"):
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        fail("adaptermux-v2 report has no %s." % key)
    identity[key] = value

if not isinstance(cases, list):
    fail("adaptermux-v2 report has no cases list.")

problems = []
seen = {}
for index, case in enumerate(cases):
    if not isinstance(case, dict):
        problems.append("case #%d is not an object" % index)
        continue
    case_id = case.get("case_id")
    if not isinstance(case_id, str) or case_id not in cells:
        problems.append("unknown case %r" % (case_id,))
        continue
    if case_id in seen:
        problems.append("duplicate case %s" % case_id)
        continue
    seen[case_id] = case
missing = [case_id for case_id in order if case_id not in seen]
if missing:
    problems.append("missing %d declared case(s): %s" % (len(missing), ",".join(missing[:10])))
if problems:
    fail("adaptermux-v2 report does not carry the declared inventory of %d cells exactly (%d problem(s))."
         % (len(order), len(problems)), problems)

counts = {}
per_transport = {}
setups = {}
for case_id in order:
    case = seen[case_id]
    cell = cells[case_id]
    for key, value in identity.items():
        if case.get(key) != value:
            problems.append("%s: %s %r differs from the report's %r" % (case_id, key, case.get(key), value))
    if case.get("southbound_transport") != cell["southbound"] or case.get("invariant") != cell["invariant"]:
        problems.append("%s: carries %s/%s, the inventory says %s/%s" % (
            case_id, case.get("southbound_transport"), case.get("invariant"), cell["southbound"], cell["invariant"]))

    outcome = case.get("outcome")
    expected = case.get("expected")
    expectation = case.get("expectation") or ""
    infra_reason = case.get("infra_reason") or ""
    planned_reason = case.get("planned_reason") or ""
    entry = EXPECTED_FAILURES.get(case_id)
    if expected != ("fail" if entry else "pass"):
        problems.append("%s: expected %r does not match the expected-failure inventory" % (case_id, expected))
    category = cell["category"]
    if category is not None:
        want_outcome, want_reason = CATEGORIES[category]
        got_reason = infra_reason if want_outcome == "blocked-infra" else planned_reason
        if outcome != want_outcome or got_reason != want_reason:
            problems.append("%s: reserved cell of %s must be %s with reason %s, got %s (%s)" % (
                case_id, category, want_outcome, want_reason, outcome, got_reason or "no reason"))
    elif outcome == "pass":
        pass
    elif outcome == "xfail":
        if entry is None or expectation != entry[1]:
            problems.append("%s: xfail outside the expected-failure inventory or with another expectation" % case_id)
    elif outcome == "xpass":
        problems.append("%s: unexpected xpass" % case_id)
    elif outcome == "fail":
        problems.append("%s: unexpected failure" % case_id)
    elif outcome == "blocked-infra":
        if infra_reason != CATEGORIES["BI-01"][1]:
            problems.append("%s: blocked-infra with unaccepted reason %s" % (case_id, infra_reason or "missing"))
    elif outcome == "planned":
        problems.append("%s: planned outside PL-01..PL-04 (reason %s)" % (case_id, planned_reason or "missing"))
    else:
        problems.append("%s: unknown outcome %r" % (case_id, outcome))

    setup = case.get("hardware_setup") or ""
    if not isinstance(setup, str):
        problems.append("%s: hardware_setup is not a label" % case_id)
        setup = ""
    if category is None and not setup.strip():
        problems.append("%s: executable cell without a hardware setup" % case_id)
    if setup:
        first = setups.setdefault(cell["row"], (setup, case_id))
        if first[0] != setup:
            problems.append("%s: second hardware setup for transport %s (%s already ran on another setup)" % (
                case_id, cell["row"], first[1]))

    counts[outcome] = counts.get(outcome, 0) + 1
    transport = per_transport.setdefault(cell["row"], {})
    transport[outcome] = transport.get(outcome, 0) + 1

if problems:
    fail("adaptermux-v2 report has %d unaccepted result(s)." % len(problems), problems)

ad_cells = [case_id for case_id in order if cells[case_id]["invariant"].startswith("AD")]
if adapter_direct_touched:
    print(PREFIX + "adapter-direct files changed; the %d AD cells of the report are present and accepted." % len(ad_cells))

def render(values):
    return ", ".join("%s=%d" % (name, values[name]) for name in sorted(values))

transports = "; ".join("%s: %s" % (row, render(per_transport[row]))
                       for row in ("ENS", "ENH", "UDP", "TCP", "UARTENS", "UARTENH", "UARTRAW") if row in per_transport)
print(PREFIX + "PASS (suite=%s, matrix_inventory_version=%s, total=%d; %s; by transport: %s)." % (
    SUITE, INVENTORY_VERSION, len(order), render(counts), transports))
ADAPTERMUX_V2_REPORT_CHECK
fi

if [[ "${requires_modbus_rtu_gate}" -eq 1 ]]; then
  run_modbus_rtu_transport_gate
fi
