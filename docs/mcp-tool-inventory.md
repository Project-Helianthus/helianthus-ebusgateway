# MCP tool inventory

The JSON below is the committed registration snapshot, generated from actual
`tools/list` responses by [the inventory test](../mcp/tool_inventory_test.go).
It contains no tool-call results. Schema version 1 identifies each row by
`surface`, `registration_profile`, and `name`; rows are sorted by that key.
Duplicate keys are rejected, including identical duplicates. Conditions and
implementation sources are also checked against the committed snapshot.

Run from the repository root with cached dependencies and a local scratch directory:

```sh
mkdir -p "$PWD/.tmp/pt" "$PWD/.tmp/gocache" "$PWD/.tmp/lint"
export TMPDIR="$PWD/.tmp" GOCACHE="$PWD/.tmp/gocache" GOWORK=off
export GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local GOFLAGS=-buildvcs=false
export GOLANGCI_LINT_CACHE="$PWD/.tmp/lint" npm_config_cache="$PWD/.tmp/npm"
export PYTHONDONTWRITEBYTECODE=1 PIP_NO_INDEX=1 RUFF_CACHE_DIR="$PWD/.tmp/ruff"
export PYTEST_ADDOPTS="--basetemp=\"$PWD/.tmp/pt\""
HELIANTHUS_MCP_INVENTORY_OUT="$PWD/.tmp/mcp-tools.json" go test ./mcp -run "^TestMCPToolInventory" -count=1 -timeout=120s
go test ./cmd/gateway -run "^TestMCPInventoryRegistrationCoverage" -count=1 -timeout=120s
python3 -m json.tool .tmp/mcp-tools.json
```

Without the output variable the test checks the committed snapshot without
writing an artifact. When explicitly requested, output is restricted to a JSON
file beneath this worktree's real scratch directory; symlink redirection is
rejected. A changed registration produces the candidate artifact before failing
the snapshot comparison. Review that difference, then deliberately replace this
JSON block with the generated JSON. There is no automatic baseline update flag.
The command and schema are the interface for the companion docs parity check.

The constructor profile includes `defaultServerTools` and
`RegisterEbusStandardTools`. Independent profiles cover B503, bus observation,
identification evidence, watch summaries, shared semantic snapshots and runtime
control, including absent providers. Combined ordinary and operator profiles check for
registration collisions across enabled surfaces. Both runtime profiles retain all five
`helianthus.runtime` tools with eeBUS absent. The semantic snapshot helper is
composed by the gateway's ordinary HTTP control plane.

The production Modbus profiles cover every combination of three owners. Their
mask uses SunSpec core = 1, Growatt storage = 2 and Tesla retained = 4.
The [gateway coverage test](../cmd/gateway/mcp_tool_inventory_test.go) compares
actual production constructors with these profiles without starting a listener.
It also checks every present and withheld entry in the
[provider registry](../cmd/gateway/modbus_provider_registry.go), rejects an
unknown branch and checks the production registration construction points.
Huawei, Growatt protocol, OutBack, Fronius and additional Tesla helper profiles
are explicitly target/uncomposed where the ordinary production composition does
not install their structural interfaces. A target row establishes helper
registration coverage, not production availability. Core and HSC structural
profiles also have separately checked production compositions.

`ordinary` means the public MCP handler. `operator` means the separately scoped
operator handler, with command routing and capture providers enabled or absent. Separate profiles
exercise each operator provider alone, including command and capture registration
with eeBUS absent.
`operator_source` means the restricted read-only operator source scope. Its
allowlist is smaller than the operator inventory. These profiles execute handlers
in memory; they open no listeners and perform no native acquisition.

Enumerated means the named tools appeared in a synthetic profile's discovery
response. Configured means the running gateway has composed the corresponding
providers and route. Physically qualified means the owning native protocol
contract and required device evidence have established qualification. Enumeration
proves neither live configuration nor physical qualification. Proven registration
coverage, Hypothesis about deployment and Unknown physical state remain separate
from qualification. A listed tool can report unavailable when its service is
absent; a withheld tool is not silently treated as enabled. Native evidence,
last-known-good state, MCP compatibility and GraphQL parity are unchanged.

Completeness negative controls remove a whole profile, remove a tool, duplicate
a key and omit runtime registration. Field controls vary name, surface,
registration profile, availability condition and implementation source separately;
they catch a renamed tool, moved surface, moved profile, changed availability
claim or changed source attribution respectively. The production composition
comparator additionally rejects omission of runtime registration.

```json
{
  "schema_version": 1,
  "tools": [
    {"name":"ebus_v1_ebus_standard_command_get","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"helianthus_v1_synchronized_evidence_capture","surface":"operator","registration_profile":"operator_capture_only","availability_condition":"synchronized capture provider present; eeBUS provider absent","implementation_source":"mcp/synchronized_evidence.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"eebus_v1_features_data_get","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"command router present; eeBUS provider absent","implementation_source":"mcp/eebus_v1_commands.go"},
    {"name":"eebus_v1_features_data_set","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"command router present; eeBUS provider absent","implementation_source":"mcp/eebus_v1_commands.go"},
    {"name":"eebus_v1_features_get","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"command router present; eeBUS provider absent","implementation_source":"mcp/eebus_v1_commands.go"},
    {"name":"eebus_v1_mutations_get","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"command router present; eeBUS provider absent","implementation_source":"mcp/eebus_v1_commands.go"},
    {"name":"eebus_v1_mutations_rollback","surface":"operator","registration_profile":"operator_commands_only","availability_condition":"command router present; eeBUS provider absent","implementation_source":"mcp/eebus_v1_commands.go"},
    {"name":"ebus_v1_bus_identification_evidence_list","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/identification_evidence.go"},
    {"name":"ebus_v1_bus_messages_list","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/server.go"},
    {"name":"ebus_v1_bus_periodicity_list","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/server.go"},
    {"name":"ebus_v1_bus_protocol_specimens_list","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/server.go"},
    {"name":"ebus_v1_bus_summary_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/server.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"operator","registration_profile":"operator_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_vaillant_errors_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/vaillant_b503.go"},
    {"name":"ebus_v1_vaillant_errors_history_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/vaillant_b503.go"},
    {"name":"ebus_v1_vaillant_errors_history_list","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/vaillant_b503.go"},
    {"name":"ebus_v1_vaillant_live_monitor_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/vaillant_b503.go"},
    {"name":"ebus_v1_vaillant_live_monitor_session_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/vaillant_b503.go"},
    {"name":"ebus_v1_vaillant_service_current_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/vaillant_b503.go"},
    {"name":"ebus_v1_vaillant_service_history_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/vaillant_b503.go"},
    {"name":"ebus_v1_watch_summary_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/server.go"},
    {"name":"eebus_v1_features_data_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/eebus_v1_commands.go"},
    {"name":"eebus_v1_features_data_set","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/eebus_v1_commands.go"},
    {"name":"eebus_v1_features_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/eebus_v1_commands.go"},
    {"name":"eebus_v1_mutations_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/eebus_v1_commands.go"},
    {"name":"eebus_v1_mutations_rollback","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/eebus_v1_commands.go"},
    {"name":"eebus_v1_pairing_status_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_runtime_status_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_services_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_services_list","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_sessions_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_sessions_list","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_snapshot_capture","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_snapshot_drop","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_topology_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"helianthus_experimental_leaf_promotion_capture","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/leaf_promotion_capture.go"},
    {"name":"helianthus_experimental_m8_source_state_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/m8_source_state.go"},
    {"name":"helianthus_runtime_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/runtime_control.go"},
    {"name":"helianthus_runtime_list","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/runtime_control.go"},
    {"name":"helianthus_runtime_restart","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/runtime_control.go"},
    {"name":"helianthus_runtime_start","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/runtime_control.go"},
    {"name":"helianthus_runtime_stop","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/runtime_control.go"},
    {"name":"helianthus_v1_synchronized_evidence_capture","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/synchronized_evidence.go"},
    {"name":"modbus_v1_growatt_bms_rs485_status_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/growatt_bms_rs485_v202.go"},
    {"name":"modbus_v1_profile_observation_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_raw_read","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_tesla_gen3_evse_current_limit_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/tesla_gen3_evse_current_limit_v1.go"},
    {"name":"modbus_v1_tesla_hsc_status_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/tesla_hsc_v1.go"},
    {"name":"semantic_v1_evse_current_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/tesla_gen3_evse_semantic.go"},
    {"name":"semantic_v1_pv_current_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/modbus_v1.go"},
    {"name":"semantic_v1_snapshot_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/semantic_snapshot.go"},
    {"name":"semantic_v1_storage_growatt_current_get","surface":"operator","registration_profile":"operator_composed","availability_condition":"all composed ordinary and operator providers present","implementation_source":"mcp/growatt_storage_semantic.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"operator","registration_profile":"operator_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"eebus_v1_pairing_status_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"eeBUS provider present; command and capture providers absent","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_runtime_status_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"eeBUS provider present; command and capture providers absent","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_services_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"eeBUS provider present; command and capture providers absent","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_services_list","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"eeBUS provider present; command and capture providers absent","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_sessions_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"eeBUS provider present; command and capture providers absent","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_sessions_list","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"eeBUS provider present; command and capture providers absent","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_snapshot_capture","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"eeBUS provider present; command and capture providers absent","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_snapshot_drop","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"eeBUS provider present; command and capture providers absent","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_topology_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"eeBUS provider present; command and capture providers absent","implementation_source":"mcp/eebus_v1.go"},
    {"name":"helianthus_experimental_m8_source_state_get","surface":"operator","registration_profile":"operator_eebus_only","availability_condition":"eeBUS provider present; command and capture providers absent","implementation_source":"mcp/m8_source_state.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"operator","registration_profile":"operator_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"eebus_v1_features_data_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"eeBUS provider, command router and capture providers present","implementation_source":"mcp/eebus_v1_commands.go"},
    {"name":"eebus_v1_features_data_set","surface":"operator","registration_profile":"operator_enabled","availability_condition":"eeBUS provider, command router and capture providers present","implementation_source":"mcp/eebus_v1_commands.go"},
    {"name":"eebus_v1_features_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"eeBUS provider, command router and capture providers present","implementation_source":"mcp/eebus_v1_commands.go"},
    {"name":"eebus_v1_mutations_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"eeBUS provider, command router and capture providers present","implementation_source":"mcp/eebus_v1_commands.go"},
    {"name":"eebus_v1_mutations_rollback","surface":"operator","registration_profile":"operator_enabled","availability_condition":"eeBUS provider, command router and capture providers present","implementation_source":"mcp/eebus_v1_commands.go"},
    {"name":"eebus_v1_pairing_status_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"eeBUS provider, command router and capture providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_runtime_status_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"eeBUS provider, command router and capture providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_services_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"eeBUS provider, command router and capture providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_services_list","surface":"operator","registration_profile":"operator_enabled","availability_condition":"eeBUS provider, command router and capture providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_sessions_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"eeBUS provider, command router and capture providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_sessions_list","surface":"operator","registration_profile":"operator_enabled","availability_condition":"eeBUS provider, command router and capture providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_snapshot_capture","surface":"operator","registration_profile":"operator_enabled","availability_condition":"eeBUS provider, command router and capture providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_snapshot_drop","surface":"operator","registration_profile":"operator_enabled","availability_condition":"eeBUS provider, command router and capture providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_topology_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"eeBUS provider, command router and capture providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"helianthus_experimental_leaf_promotion_capture","surface":"operator","registration_profile":"operator_enabled","availability_condition":"eeBUS provider, command router and capture providers present","implementation_source":"mcp/leaf_promotion_capture.go"},
    {"name":"helianthus_experimental_m8_source_state_get","surface":"operator","registration_profile":"operator_enabled","availability_condition":"eeBUS provider, command router and capture providers present","implementation_source":"mcp/m8_source_state.go"},
    {"name":"helianthus_v1_synchronized_evidence_capture","surface":"operator","registration_profile":"operator_enabled","availability_condition":"eeBUS provider, command router and capture providers present","implementation_source":"mcp/synchronized_evidence.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"helianthus_experimental_leaf_promotion_capture","surface":"operator","registration_profile":"operator_promotion_only","availability_condition":"leaf promotion capture provider present; eeBUS provider absent","implementation_source":"mcp/leaf_promotion_capture.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"operator_source","registration_profile":"operator_source_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"operator_source","registration_profile":"operator_source_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"operator_source","registration_profile":"operator_source_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"operator_source","registration_profile":"operator_source_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"eebus_v1_pairing_status_get","surface":"operator_source","registration_profile":"operator_source_enabled","availability_condition":"operator source scope; eeBUS provider present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_runtime_status_get","surface":"operator_source","registration_profile":"operator_source_enabled","availability_condition":"operator source scope; eeBUS provider present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_services_get","surface":"operator_source","registration_profile":"operator_source_enabled","availability_condition":"operator source scope; eeBUS provider present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_services_list","surface":"operator_source","registration_profile":"operator_source_enabled","availability_condition":"operator source scope; eeBUS provider present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_sessions_get","surface":"operator_source","registration_profile":"operator_source_enabled","availability_condition":"operator source scope; eeBUS provider present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_sessions_list","surface":"operator_source","registration_profile":"operator_source_enabled","availability_condition":"operator source scope; eeBUS provider present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_snapshot_capture","surface":"operator_source","registration_profile":"operator_source_enabled","availability_condition":"operator source scope; eeBUS provider present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_snapshot_drop","surface":"operator_source","registration_profile":"operator_source_enabled","availability_condition":"operator source scope; eeBUS provider present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_topology_get","surface":"operator_source","registration_profile":"operator_source_enabled","availability_condition":"operator source scope; eeBUS provider present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"b503_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_vaillant_errors_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"B503 dispatcher and session manager present","implementation_source":"mcp/vaillant_b503.go"},
    {"name":"ebus_v1_vaillant_errors_history_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"B503 dispatcher and session manager present","implementation_source":"mcp/vaillant_b503.go"},
    {"name":"ebus_v1_vaillant_errors_history_list","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"B503 dispatcher and session manager present","implementation_source":"mcp/vaillant_b503.go"},
    {"name":"ebus_v1_vaillant_live_monitor_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"B503 dispatcher and session manager present","implementation_source":"mcp/vaillant_b503.go"},
    {"name":"ebus_v1_vaillant_live_monitor_session_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"B503 dispatcher and session manager present","implementation_source":"mcp/vaillant_b503.go"},
    {"name":"ebus_v1_vaillant_service_current_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"B503 dispatcher and session manager present","implementation_source":"mcp/vaillant_b503.go"},
    {"name":"ebus_v1_vaillant_service_history_get","surface":"ordinary","registration_profile":"b503_enabled","availability_condition":"B503 dispatcher and session manager present","implementation_source":"mcp/vaillant_b503.go"},
    {"name":"ebus_v1_bus_messages_list","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"bus observability provider present","implementation_source":"mcp/server.go"},
    {"name":"ebus_v1_bus_periodicity_list","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"bus observability provider present","implementation_source":"mcp/server.go"},
    {"name":"ebus_v1_bus_protocol_specimens_list","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"bus observability provider present","implementation_source":"mcp/server.go"},
    {"name":"ebus_v1_bus_summary_get","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"bus observability provider present","implementation_source":"mcp/server.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"bus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"constructor","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"eebus_v1_pairing_status_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"eeBUS provider present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"eeBUS provider present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_services_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"eeBUS provider present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_services_list","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"eeBUS provider present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_sessions_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"eeBUS provider present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_sessions_list","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"eeBUS provider present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"eeBUS provider present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"eeBUS provider present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_topology_get","surface":"ordinary","registration_profile":"eebus_enabled","availability_condition":"eeBUS provider present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"ebus_v1_bus_identification_evidence_list","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"bus provider retains identification evidence","implementation_source":"mcp/identification_evidence.go"},
    {"name":"ebus_v1_bus_messages_list","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"bus provider retains identification evidence","implementation_source":"mcp/server.go"},
    {"name":"ebus_v1_bus_periodicity_list","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"bus provider retains identification evidence","implementation_source":"mcp/server.go"},
    {"name":"ebus_v1_bus_protocol_specimens_list","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"bus provider retains identification evidence","implementation_source":"mcp/server.go"},
    {"name":"ebus_v1_bus_summary_get","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"bus provider retains identification evidence","implementation_source":"mcp/server.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"identification_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"modbus_production_0","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"modbus_v1_profile_observation_get","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"owner mask 1: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_raw_read","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"owner mask 1: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_tesla_hsc_status_get","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"owner mask 1: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/tesla_hsc_v1.go"},
    {"name":"semantic_v1_pv_current_get","surface":"ordinary","registration_profile":"modbus_production_1","availability_condition":"owner mask 1: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/modbus_v1.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"modbus_v1_growatt_bms_rs485_status_get","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"owner mask 2: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/growatt_bms_rs485_v202.go"},
    {"name":"semantic_v1_storage_growatt_current_get","surface":"ordinary","registration_profile":"modbus_production_2","availability_condition":"owner mask 2: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/growatt_storage_semantic.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"modbus_v1_growatt_bms_rs485_status_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"owner mask 3: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/growatt_bms_rs485_v202.go"},
    {"name":"modbus_v1_profile_observation_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"owner mask 3: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_raw_read","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"owner mask 3: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_tesla_hsc_status_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"owner mask 3: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/tesla_hsc_v1.go"},
    {"name":"semantic_v1_pv_current_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"owner mask 3: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/modbus_v1.go"},
    {"name":"semantic_v1_storage_growatt_current_get","surface":"ordinary","registration_profile":"modbus_production_3","availability_condition":"owner mask 3: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/growatt_storage_semantic.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"modbus_v1_tesla_gen3_evse_current_limit_get","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"owner mask 4: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/tesla_gen3_evse_current_limit_v1.go"},
    {"name":"semantic_v1_evse_current_get","surface":"ordinary","registration_profile":"modbus_production_4","availability_condition":"owner mask 4: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/tesla_gen3_evse_semantic.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"modbus_v1_profile_observation_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"owner mask 5: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_raw_read","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"owner mask 5: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_tesla_gen3_evse_current_limit_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"owner mask 5: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/tesla_gen3_evse_current_limit_v1.go"},
    {"name":"modbus_v1_tesla_hsc_status_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"owner mask 5: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/tesla_hsc_v1.go"},
    {"name":"semantic_v1_evse_current_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"owner mask 5: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/tesla_gen3_evse_semantic.go"},
    {"name":"semantic_v1_pv_current_get","surface":"ordinary","registration_profile":"modbus_production_5","availability_condition":"owner mask 5: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/modbus_v1.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"modbus_v1_growatt_bms_rs485_status_get","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"owner mask 6: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/growatt_bms_rs485_v202.go"},
    {"name":"modbus_v1_tesla_gen3_evse_current_limit_get","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"owner mask 6: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/tesla_gen3_evse_current_limit_v1.go"},
    {"name":"semantic_v1_evse_current_get","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"owner mask 6: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/tesla_gen3_evse_semantic.go"},
    {"name":"semantic_v1_storage_growatt_current_get","surface":"ordinary","registration_profile":"modbus_production_6","availability_condition":"owner mask 6: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/growatt_storage_semantic.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"modbus_v1_growatt_bms_rs485_status_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"owner mask 7: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/growatt_bms_rs485_v202.go"},
    {"name":"modbus_v1_profile_observation_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"owner mask 7: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_raw_read","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"owner mask 7: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_tesla_gen3_evse_current_limit_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"owner mask 7: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/tesla_gen3_evse_current_limit_v1.go"},
    {"name":"modbus_v1_tesla_hsc_status_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"owner mask 7: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/tesla_hsc_v1.go"},
    {"name":"semantic_v1_evse_current_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"owner mask 7: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/tesla_gen3_evse_semantic.go"},
    {"name":"semantic_v1_pv_current_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"owner mask 7: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/modbus_v1.go"},
    {"name":"semantic_v1_storage_growatt_current_get","surface":"ordinary","registration_profile":"modbus_production_7","availability_condition":"owner mask 7: SunSpec=1, Growatt storage=2, Tesla retained=4","implementation_source":"mcp/growatt_storage_semantic.go"},
    {"name":"ebus_v1_bus_identification_evidence_list","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/identification_evidence.go"},
    {"name":"ebus_v1_bus_messages_list","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/server.go"},
    {"name":"ebus_v1_bus_periodicity_list","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/server.go"},
    {"name":"ebus_v1_bus_protocol_specimens_list","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/server.go"},
    {"name":"ebus_v1_bus_summary_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/server.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_vaillant_errors_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/vaillant_b503.go"},
    {"name":"ebus_v1_vaillant_errors_history_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/vaillant_b503.go"},
    {"name":"ebus_v1_vaillant_errors_history_list","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/vaillant_b503.go"},
    {"name":"ebus_v1_vaillant_live_monitor_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/vaillant_b503.go"},
    {"name":"ebus_v1_vaillant_live_monitor_session_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/vaillant_b503.go"},
    {"name":"ebus_v1_vaillant_service_current_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/vaillant_b503.go"},
    {"name":"ebus_v1_vaillant_service_history_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/vaillant_b503.go"},
    {"name":"ebus_v1_watch_summary_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/server.go"},
    {"name":"eebus_v1_pairing_status_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_services_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_services_list","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_sessions_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_sessions_list","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"eebus_v1_topology_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/eebus_v1.go"},
    {"name":"helianthus_runtime_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/runtime_control.go"},
    {"name":"helianthus_runtime_list","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/runtime_control.go"},
    {"name":"helianthus_runtime_restart","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/runtime_control.go"},
    {"name":"helianthus_runtime_start","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/runtime_control.go"},
    {"name":"helianthus_runtime_stop","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/runtime_control.go"},
    {"name":"modbus_v1_growatt_bms_rs485_status_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/growatt_bms_rs485_v202.go"},
    {"name":"modbus_v1_profile_observation_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_raw_read","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_tesla_gen3_evse_current_limit_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/tesla_gen3_evse_current_limit_v1.go"},
    {"name":"modbus_v1_tesla_hsc_status_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/tesla_hsc_v1.go"},
    {"name":"semantic_v1_evse_current_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/tesla_gen3_evse_semantic.go"},
    {"name":"semantic_v1_pv_current_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/modbus_v1.go"},
    {"name":"semantic_v1_snapshot_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/semantic_snapshot.go"},
    {"name":"semantic_v1_storage_growatt_current_get","surface":"ordinary","registration_profile":"ordinary_composed","availability_condition":"all composed ordinary optional providers present","implementation_source":"mcp/growatt_storage_semantic.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"helianthus_runtime_get","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"runtime service absent; eeBUS absent","implementation_source":"mcp/runtime_control.go"},
    {"name":"helianthus_runtime_list","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"runtime service absent; eeBUS absent","implementation_source":"mcp/runtime_control.go"},
    {"name":"helianthus_runtime_restart","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"runtime service absent; eeBUS absent","implementation_source":"mcp/runtime_control.go"},
    {"name":"helianthus_runtime_start","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"runtime service absent; eeBUS absent","implementation_source":"mcp/runtime_control.go"},
    {"name":"helianthus_runtime_stop","surface":"ordinary","registration_profile":"runtime_disabled","availability_condition":"runtime service absent; eeBUS absent","implementation_source":"mcp/runtime_control.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"helianthus_runtime_get","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"runtime service present; eeBUS absent","implementation_source":"mcp/runtime_control.go"},
    {"name":"helianthus_runtime_list","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"runtime service present; eeBUS absent","implementation_source":"mcp/runtime_control.go"},
    {"name":"helianthus_runtime_restart","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"runtime service present; eeBUS absent","implementation_source":"mcp/runtime_control.go"},
    {"name":"helianthus_runtime_start","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"runtime service present; eeBUS absent","implementation_source":"mcp/runtime_control.go"},
    {"name":"helianthus_runtime_stop","surface":"ordinary","registration_profile":"runtime_enabled","availability_condition":"runtime service present; eeBUS absent","implementation_source":"mcp/runtime_control.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"semantic_v1_snapshot_get","surface":"ordinary","registration_profile":"semantic_disabled","availability_condition":"shared semantic provider absent; discovery remains configured","implementation_source":"mcp/semantic_snapshot.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"semantic_v1_snapshot_get","surface":"ordinary","registration_profile":"semantic_enabled","availability_condition":"shared semantic provider present","implementation_source":"mcp/semantic_snapshot.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"modbus_v1_fronius_sunspec_status_get","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/fronius_sunspec_v1.go"},
    {"name":"modbus_v1_profile_observation_get","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_raw_read","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"semantic_v1_pv_current_get","surface":"ordinary","registration_profile":"target_fronius","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"modbus_v1_growatt_protocol_ii_identity_get","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/growatt_protocol_ii_v1.go"},
    {"name":"modbus_v1_profile_observation_get","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_raw_read","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"semantic_v1_pv_current_get","surface":"ordinary","registration_profile":"target_growatt_protocol","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"modbus_v1_huawei_sdongle_status_get","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/huawei_sdongle_v1.go"},
    {"name":"modbus_v1_profile_observation_get","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_raw_read","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"semantic_v1_pv_current_get","surface":"ordinary","registration_profile":"target_huawei_dongle","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"modbus_v1_huawei_emma_identity_get","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/huawei_emma_v1.go"},
    {"name":"modbus_v1_profile_observation_get","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_raw_read","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"semantic_v1_pv_current_get","surface":"ordinary","registration_profile":"target_huawei_emma","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"modbus_v1_huawei_emma_status_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/huawei_smartlogger_v1.go"},
    {"name":"modbus_v1_huawei_sdongle_status_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/huawei_smartlogger_v1.go"},
    {"name":"modbus_v1_huawei_smartlogger_status_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/huawei_smartlogger_v1.go"},
    {"name":"modbus_v1_profile_observation_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_raw_read","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"semantic_v1_pv_current_get","surface":"ordinary","registration_profile":"target_huawei_instances","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"modbus_v1_huawei_emma_status_get","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/huawei_smartlogger_v1.go"},
    {"name":"modbus_v1_huawei_sdongle_status_get","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/huawei_smartlogger_v1.go"},
    {"name":"modbus_v1_huawei_smartlogger_status_get","surface":"ordinary","registration_profile":"target_huawei_instances_coreless","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/huawei_smartlogger_v1.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"modbus_v1_huawei_smartlogger_status_get","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/huawei_smartlogger_v1.go"},
    {"name":"modbus_v1_profile_observation_get","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_raw_read","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"semantic_v1_pv_current_get","surface":"ordinary","registration_profile":"target_huawei_logger","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"modbus_v1_profile_observation_get","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"structural helper profile; production owner availability checked separately","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_raw_read","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"structural helper profile; production owner availability checked separately","implementation_source":"mcp/modbus_v1.go"},
    {"name":"semantic_v1_pv_current_get","surface":"ordinary","registration_profile":"target_modbus_core","availability_condition":"structural helper profile; production owner availability checked separately","implementation_source":"mcp/modbus_v1.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"target_outback","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"modbus_v1_outback_axs_status_get","surface":"ordinary","registration_profile":"target_outback","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/outback_axs_v1.go"},
    {"name":"modbus_v1_profile_observation_get","surface":"ordinary","registration_profile":"target_outback","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_raw_read","surface":"ordinary","registration_profile":"target_outback","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"semantic_v1_pv_current_get","surface":"ordinary","registration_profile":"target_outback","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"modbus_v1_profile_observation_get","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"structural helper profile; production owner availability checked separately","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_raw_read","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"structural helper profile; production owner availability checked separately","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_tesla_hsc_status_get","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"structural helper profile; production owner availability checked separately","implementation_source":"mcp/tesla_hsc_v1.go"},
    {"name":"semantic_v1_pv_current_get","surface":"ordinary","registration_profile":"target_tesla_hsc","availability_condition":"structural helper profile; production owner availability checked separately","implementation_source":"mcp/modbus_v1.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"modbus_v1_profile_observation_get","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_raw_read","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_tesla_fc100_summary_get","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/tesla_fc100_summary_v1.go"},
    {"name":"semantic_v1_pv_current_get","surface":"ordinary","registration_profile":"target_tesla_summary","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"modbus_v1_profile_observation_get","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_raw_read","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"modbus_v1_tesla_wcapi_get_vitals_replay_get","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/tesla_wcapi_get_vitals_v1.go"},
    {"name":"semantic_v1_pv_current_get","surface":"ordinary","registration_profile":"target_tesla_vitals","availability_condition":"target/uncomposed structural provider; not a production enablement claim","implementation_source":"mcp/modbus_v1.go"},
    {"name":"ebus_v1_ebus_standard_command_get","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_commands_list","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_decode","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_ebus_standard_services_list","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/ebus_standard_wiring.go"},
    {"name":"ebus_v1_registry_devices_get","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_devices_list","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_methods_list","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_registry_planes_list","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_rpc_invoke","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_runtime_status_get","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_adapter_info_get","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_get","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_boiler_status_set_config","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_circuits_get","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_cylinders_get","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_dhw_get","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_energy_totals_get","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_interpretation_get","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_fm5_mode_get","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_radio_devices_get","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_get","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_dhw_time_program","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_schedules_set_zone_time_program","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_snapshot_get","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_solar_get","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_get","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_system_set_config","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_semantic_zones_get","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_capture","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_snapshot_drop","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"always: NewServer","implementation_source":"mcp/server_tools.go"},
    {"name":"ebus_v1_watch_summary_get","surface":"ordinary","registration_profile":"watch_enabled","availability_condition":"watch summary provider present","implementation_source":"mcp/server.go"}
  ]
}
```

## Current default registration contract

This emitted snapshot covers default registration, including the standard eBUS service tools. Optional driver registrations retain their separately scoped contracts. Namespace token changes and the two explicit legacy removals preserve every remaining descriptor after name normalization.

```json
[
  {
    "name": "ebus_v1_runtime_status_get",
    "description": "Get runtime daemon and adapter status.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "consistency": {
          "additionalProperties": false,
          "properties": {
            "mode": {
              "enum": [
                "LIVE",
                "SNAPSHOT"
              ],
              "type": "string"
            },
            "snapshot_id": {
              "type": "string"
            }
          },
          "type": "object"
        }
      },
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_semantic_zones_get",
    "description": "Get semantic zones snapshot.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "consistency": {
          "additionalProperties": false,
          "properties": {
            "mode": {
              "enum": [
                "LIVE",
                "SNAPSHOT"
              ],
              "type": "string"
            },
            "snapshot_id": {
              "type": "string"
            }
          },
          "type": "object"
        }
      },
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_semantic_circuits_get",
    "description": "Get semantic heating circuits snapshot.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "consistency": {
          "additionalProperties": false,
          "properties": {
            "mode": {
              "enum": [
                "LIVE",
                "SNAPSHOT"
              ],
              "type": "string"
            },
            "snapshot_id": {
              "type": "string"
            }
          },
          "type": "object"
        }
      },
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_semantic_radio_devices_get",
    "description": "Get semantic remote-slot radio devices snapshot.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "consistency": {
          "additionalProperties": false,
          "properties": {
            "mode": {
              "enum": [
                "LIVE",
                "SNAPSHOT"
              ],
              "type": "string"
            },
            "snapshot_id": {
              "type": "string"
            }
          },
          "type": "object"
        }
      },
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_semantic_fm5_mode_get",
    "description": "Get semantic FM5 mode snapshot.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "consistency": {
          "additionalProperties": false,
          "properties": {
            "mode": {
              "enum": [
                "LIVE",
                "SNAPSHOT"
              ],
              "type": "string"
            },
            "snapshot_id": {
              "type": "string"
            }
          },
          "type": "object"
        }
      },
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_semantic_fm5_interpretation_get",
    "description": "Get the atomic semantic FM5 mode, degraded reason, and evidence revision.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "consistency": {
          "additionalProperties": false,
          "properties": {
            "mode": {
              "enum": [
                "LIVE",
                "SNAPSHOT"
              ],
              "type": "string"
            },
            "snapshot_id": {
              "type": "string"
            }
          },
          "type": "object"
        }
      },
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_semantic_solar_get",
    "description": "Get semantic solar snapshot (interpreted mode only).",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "consistency": {
          "additionalProperties": false,
          "properties": {
            "mode": {
              "enum": [
                "LIVE",
                "SNAPSHOT"
              ],
              "type": "string"
            },
            "snapshot_id": {
              "type": "string"
            }
          },
          "type": "object"
        }
      },
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_semantic_cylinders_get",
    "description": "Get semantic cylinders snapshot (interpreted mode only).",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "consistency": {
          "additionalProperties": false,
          "properties": {
            "mode": {
              "enum": [
                "LIVE",
                "SNAPSHOT"
              ],
              "type": "string"
            },
            "snapshot_id": {
              "type": "string"
            }
          },
          "type": "object"
        }
      },
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_semantic_dhw_get",
    "description": "Get semantic domestic hot water snapshot.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "consistency": {
          "additionalProperties": false,
          "properties": {
            "mode": {
              "enum": [
                "LIVE",
                "SNAPSHOT"
              ],
              "type": "string"
            },
            "snapshot_id": {
              "type": "string"
            }
          },
          "type": "object"
        }
      },
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_semantic_energy_totals_get",
    "description": "Get semantic energy totals snapshot.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "consistency": {
          "additionalProperties": false,
          "properties": {
            "mode": {
              "enum": [
                "LIVE",
                "SNAPSHOT"
              ],
              "type": "string"
            },
            "snapshot_id": {
              "type": "string"
            }
          },
          "type": "object"
        }
      },
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_semantic_boiler_status_get",
    "description": "Get semantic boiler status snapshot (flow/return temps, pump, diagnostics).",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "consistency": {
          "additionalProperties": false,
          "properties": {
            "mode": {
              "enum": [
                "LIVE",
                "SNAPSHOT"
              ],
              "type": "string"
            },
            "snapshot_id": {
              "type": "string"
            }
          },
          "type": "object"
        }
      },
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_semantic_system_get",
    "description": "Get semantic system status snapshot (outdoor temp, water pressure, flow temp, maintenance, config).",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "consistency": {
          "additionalProperties": false,
          "properties": {
            "mode": {
              "enum": [
                "LIVE",
                "SNAPSHOT"
              ],
              "type": "string"
            },
            "snapshot_id": {
              "type": "string"
            }
          },
          "type": "object"
        }
      },
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_semantic_adapter_info_get",
    "description": "Get adapter hardware identity and telemetry (firmware, temperature, voltages, WiFi RSSI, reset info).",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "consistency": {
          "additionalProperties": false,
          "properties": {
            "mode": {
              "enum": [
                "LIVE",
                "SNAPSHOT"
              ],
              "type": "string"
            },
            "snapshot_id": {
              "type": "string"
            }
          },
          "type": "object"
        }
      },
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_semantic_schedules_get",
    "description": "Get semantic weekly timer schedules snapshot (B555 protocol).",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "consistency": {
          "additionalProperties": false,
          "properties": {
            "mode": {
              "enum": [
                "LIVE",
                "SNAPSHOT"
              ],
              "type": "string"
            },
            "snapshot_id": {
              "type": "string"
            }
          },
          "type": "object"
        }
      },
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_semantic_schedules_set_zone_time_program",
    "description": "Write a zone heating time program for a specific weekday (B555 protocol). Writes individual slots sequentially (SC=1 per slot for reliability).",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "slots": {
          "items": {
            "properties": {
              "end_hour": {
                "maximum": 24,
                "minimum": 0,
                "type": "integer"
              },
              "end_minute": {
                "maximum": 59,
                "minimum": 0,
                "type": "integer"
              },
              "start_hour": {
                "maximum": 24,
                "minimum": 0,
                "type": "integer"
              },
              "start_minute": {
                "maximum": 59,
                "minimum": 0,
                "type": "integer"
              },
              "temperature_c": {
                "type": "number"
              }
            },
            "required": [
              "start_hour",
              "start_minute",
              "end_hour",
              "end_minute",
              "temperature_c"
            ],
            "type": "object"
          },
          "type": "array"
        },
        "weekday": {
          "description": "0=Monday, 6=Sunday",
          "maximum": 6,
          "minimum": 0,
          "type": "integer"
        },
        "zone": {
          "maximum": 2,
          "minimum": 0,
          "type": "integer"
        }
      },
      "required": [
        "zone",
        "weekday",
        "slots"
      ],
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_semantic_schedules_set_dhw_time_program",
    "description": "Write a DHW (domestic hot water) time program for a specific weekday (B555 protocol). Temperature is optional \u2014 omit to keep current B524 setpoint (0xFFFF sentinel).",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "slots": {
          "items": {
            "properties": {
              "end_hour": {
                "maximum": 24,
                "minimum": 0,
                "type": "integer"
              },
              "end_minute": {
                "maximum": 59,
                "minimum": 0,
                "type": "integer"
              },
              "start_hour": {
                "maximum": 24,
                "minimum": 0,
                "type": "integer"
              },
              "start_minute": {
                "maximum": 59,
                "minimum": 0,
                "type": "integer"
              },
              "temperature_c": {
                "type": "number"
              }
            },
            "required": [
              "start_hour",
              "start_minute",
              "end_hour",
              "end_minute"
            ],
            "type": "object"
          },
          "type": "array"
        },
        "weekday": {
          "description": "0=Monday, 6=Sunday",
          "maximum": 6,
          "minimum": 0,
          "type": "integer"
        }
      },
      "required": [
        "weekday",
        "slots"
      ],
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_semantic_system_set_config",
    "description": "Write a system configuration field (B524 controller). Accepts the snake_case field names of the GraphQL set_system_config mutation.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "field": {
          "description": "snake_case field name (e.g. installer_name, maintenance_date, installer_access_code)",
          "type": "string"
        },
        "value": {
          "description": "Value to write (string representation)",
          "type": "string"
        }
      },
      "required": [
        "field",
        "value"
      ],
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_semantic_boiler_status_set_config",
    "description": "Write a boiler configuration field (B509 BAI00). Accepts the field names of the GraphQL set_boiler_config mutation.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "field": {
          "description": "field name (e.g. installer_phone_number, maximum_heating_flow_temperature_setpoint)",
          "type": "string"
        },
        "value": {
          "description": "Value to write (string representation)",
          "type": "string"
        }
      },
      "required": [
        "field",
        "value"
      ],
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_semantic_snapshot_get",
    "description": "Get a consistent semantic snapshot across selected semantic planes.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "allow_partial": {
          "type": "boolean"
        },
        "consistency": {
          "additionalProperties": false,
          "properties": {
            "mode": {
              "enum": [
                "LIVE",
                "SNAPSHOT"
              ],
              "type": "string"
            },
            "snapshot_id": {
              "type": "string"
            }
          },
          "type": "object"
        },
        "planes": {
          "items": {
            "enum": [
              "runtime_status",
              "zones",
              "dhw",
              "energy_totals",
              "boiler_status",
              "system",
              "circuits",
              "radio_devices",
              "fm5_mode",
              "solar",
              "cylinders",
              "schedules",
              "adapter_info"
            ],
            "type": "string"
          },
          "type": "array"
        },
        "timeout_ms": {
          "minimum": 1,
          "type": "integer"
        }
      },
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_snapshot_capture",
    "description": "Capture a read snapshot for deterministic MCP reads.",
    "inputSchema": {
      "additionalProperties": false,
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_snapshot_drop",
    "description": "Drop a previously captured snapshot.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "snapshot_id": {
          "type": "string"
        }
      },
      "required": [
        "snapshot_id"
      ],
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_registry_devices_list",
    "description": "List devices discovered on the eBUS, including planes and methods.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "consistency": {
          "additionalProperties": false,
          "properties": {
            "mode": {
              "enum": [
                "LIVE",
                "SNAPSHOT"
              ],
              "type": "string"
            },
            "snapshot_id": {
              "type": "string"
            }
          },
          "type": "object"
        }
      },
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_registry_devices_get",
    "description": "Get one device by address.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "address": {
          "maximum": 255,
          "minimum": 0,
          "type": "integer"
        },
        "consistency": {
          "additionalProperties": false,
          "properties": {
            "mode": {
              "enum": [
                "LIVE",
                "SNAPSHOT"
              ],
              "type": "string"
            },
            "snapshot_id": {
              "type": "string"
            }
          },
          "type": "object"
        }
      },
      "required": [
        "address"
      ],
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_registry_planes_list",
    "description": "List registry planes for one device address.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "address": {
          "maximum": 255,
          "minimum": 0,
          "type": "integer"
        },
        "consistency": {
          "additionalProperties": false,
          "properties": {
            "mode": {
              "enum": [
                "LIVE",
                "SNAPSHOT"
              ],
              "type": "string"
            },
            "snapshot_id": {
              "type": "string"
            }
          },
          "type": "object"
        }
      },
      "required": [
        "address"
      ],
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_registry_methods_list",
    "description": "List registry methods for a device address and plane.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "address": {
          "maximum": 255,
          "minimum": 0,
          "type": "integer"
        },
        "consistency": {
          "additionalProperties": false,
          "properties": {
            "mode": {
              "enum": [
                "LIVE",
                "SNAPSHOT"
              ],
              "type": "string"
            },
            "snapshot_id": {
              "type": "string"
            }
          },
          "type": "object"
        },
        "plane": {
          "type": "string"
        }
      },
      "required": [
        "address",
        "plane"
      ],
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_rpc_invoke",
    "description": "Discover a device with ebus_v1_registry_devices_list, then select its plane with ebus_v1_registry_planes_list and method with ebus_v1_registry_methods_list before invoking it.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "address": {
          "maximum": 255,
          "minimum": 0,
          "type": "integer"
        },
        "allow_dangerous": {
          "type": "boolean"
        },
        "idempotency_key": {
          "type": "string"
        },
        "intent": {
          "enum": [
            "READ_ONLY",
            "MUTATE"
          ],
          "type": "string"
        },
        "method": {
          "description": "Method name returned by ebus_v1_registry_methods_list for this address and plane.",
          "type": "string"
        },
        "params": {
          "description": "Method-specific parameters. Omit source to use the startup-admitted source; an explicit source overrides it when supplied as a nonzero byte.",
          "type": "object"
        },
        "plane": {
          "description": "Plane name returned by ebus_v1_registry_planes_list for this address.",
          "type": "string"
        },
        "timeout_ms": {
          "minimum": 1,
          "type": "integer"
        }
      },
      "required": [
        "address",
        "plane",
        "method",
        "intent",
        "allow_dangerous"
      ],
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_ebus_standard_services_list",
    "description": "List ebus_standard L7 services from the catalog.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {},
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_ebus_standard_commands_list",
    "description": "List ebus_standard commands, optionally filtered by PB.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "pb": {
          "maximum": 255,
          "minimum": 0,
          "type": "integer"
        }
      },
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_ebus_standard_command_get",
    "description": "Get an ebus_standard command by id with full 14-tuple identity.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "id": {
          "type": "string"
        }
      },
      "required": [
        "id"
      ],
      "type": "object"
    }
  },
  {
    "name": "ebus_v1_ebus_standard_decode",
    "description": "Decode an observed eBUS payload against catalog identity (pb, sb, direction, frame_type, payload_hex).",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "direction": {
          "type": "string"
        },
        "frame_type": {
          "type": "string"
        },
        "payload_hex": {
          "type": "string"
        },
        "pb": {
          "maximum": 255,
          "minimum": 0,
          "type": "integer"
        },
        "sb": {
          "maximum": 255,
          "minimum": 0,
          "type": "integer"
        }
      },
      "required": [
        "pb",
        "sb",
        "direction",
        "frame_type",
        "payload_hex"
      ],
      "type": "object"
    }
  }
]
```

## Current public eeBUS registration contract

The public eeBUS descriptors below preserve their original permission scope identifiers. Operator raw commands use underscore registration names while retaining library-owned authorization tool identities internally.

```json
[
  {
    "description": "Get eeBUS pairing status.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "evidence_ref": {
          "pattern": "^[A-Za-z0-9_-]{42}[AEIMQUYcgkosw048]$",
          "type": "string"
        }
      },
      "type": "object"
    },
    "name": "eebus_v1_pairing_status_get"
  },
  {
    "description": "Get the eeBUS runtime status.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "evidence_ref": {
          "pattern": "^[A-Za-z0-9_-]{42}[AEIMQUYcgkosw048]$",
          "type": "string"
        }
      },
      "type": "object"
    },
    "name": "eebus_v1_runtime_status_get"
  },
  {
    "description": "Get one eeBUS service.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "evidence_ref": {
          "pattern": "^[A-Za-z0-9_-]{42}[AEIMQUYcgkosw048]$",
          "type": "string"
        },
        "id_digest": {
          "pattern": "^(?:[A-Za-z0-9_-]{42}[AEIMQUYcgkosw048]|sha256:[0-9a-f]{64})$",
          "type": "string"
        }
      },
      "required": [
        "id_digest"
      ],
      "type": "object"
    },
    "name": "eebus_v1_services_get"
  },
  {
    "description": "List eeBUS services.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "evidence_ref": {
          "pattern": "^[A-Za-z0-9_-]{42}[AEIMQUYcgkosw048]$",
          "type": "string"
        }
      },
      "type": "object"
    },
    "name": "eebus_v1_services_list"
  },
  {
    "description": "Get one eeBUS session.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "evidence_ref": {
          "pattern": "^[A-Za-z0-9_-]{42}[AEIMQUYcgkosw048]$",
          "type": "string"
        },
        "id_digest": {
          "pattern": "^(?:[A-Za-z0-9_-]{42}[AEIMQUYcgkosw048]|sha256:[0-9a-f]{64})$",
          "type": "string"
        }
      },
      "required": [
        "id_digest"
      ],
      "type": "object"
    },
    "name": "eebus_v1_sessions_get"
  },
  {
    "description": "List eeBUS sessions.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "evidence_ref": {
          "pattern": "^[A-Za-z0-9_-]{42}[AEIMQUYcgkosw048]$",
          "type": "string"
        }
      },
      "type": "object"
    },
    "name": "eebus_v1_sessions_list"
  },
  {
    "description": "Capture one immutable eeBUS evidence root.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {},
      "type": "object"
    },
    "name": "eebus_v1_snapshot_capture"
  },
  {
    "description": "Drop one immutable eeBUS evidence root.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "snapshot_ref": {
          "pattern": "^[A-Za-z0-9_-]{42}[AEIMQUYcgkosw048]$",
          "type": "string"
        }
      },
      "required": [
        "snapshot_ref"
      ],
      "type": "object"
    },
    "name": "eebus_v1_snapshot_drop"
  },
  {
    "description": "Get the eeBUS topology.",
    "inputSchema": {
      "additionalProperties": false,
      "properties": {
        "evidence_ref": {
          "pattern": "^[A-Za-z0-9_-]{42}[AEIMQUYcgkosw048]$",
          "type": "string"
        }
      },
      "type": "object"
    },
    "name": "eebus_v1_topology_get"
  }
]
```
