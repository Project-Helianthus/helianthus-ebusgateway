# MCP namespaces

Every MCP tool the gateway registers belongs to exactly one namespace, and every
namespace has one owner. A tool name is the namespace prefix followed by
snake_case tokens that describe the behaviour of the tool. Before 1.0 no name is
stable, so there is no separate namespace for unfinished tools: a tool is
registered under its owner's namespace or not at all.

Renames are applied directly in one wave with every caller. There is no alias,
forwarding name or deprecation window: a previous name is an unknown tool.

## Namespaces

| Prefix | Owner | Contents |
| --- | --- | --- |
| `ebus_v1_` | protocol-native: eBUS | eBUS bus observation, registry, invocation, standard catalog and vendor-family service reads |
| `eebus_v1_` | protocol-native: eeBUS | eeBUS runtime, pairing, sessions, services, topology, features and operator commands |
| `modbus_v1_` | protocol-native: Modbus | Modbus raw reads, profile observations and device-family status reads |
| `semantic_v1_` | protocol-neutral semantic | semantic reads that do not depend on the transport that produced the data |
| `helianthus_v1_` | host | gateway-owned operator tools: evidence capture, leaf promotion capture and source-state reads |
| `helianthus_runtime_` | host: runtime control | driver lifecycle reads and requests |

No other prefix is admitted, and no tool name contains `experimental`.
[The namespace plan test](../../mcp/tool_namespace_plan_test.go) enforces both
rules over every tool registered by any profile of
[the inventory test](../../mcp/tool_inventory_test.go), checks that this page
lists exactly that set, and checks that every tool name the gateway dispatches
on is a registered tool.

## Operation identifiers are not tool names

An evidence contract may name the native operation that produced a sample, for
example `eebus.v1.features.data.get`. That string is an operation identifier
inside the contract, not a tool name, and keeps its dotted spelling. A caller
reads the sample through the registered tool that serves the operation, here
`eebus_v1_features_data_get`.

## Semantic reads

Semantic reads are still split between the family-keyed `ebus_v1_semantic_*`
reads and the protocol-neutral `semantic_v1_*` tools. The family-keyed reads
move with the retrieval work of #1540; until then they keep their names. The
`ebus_v1_semantic_*` writes stay in the eBUS namespace.

## Registered tools

The target name equals the current name unless the row says otherwise.

| Tool | Namespace | Target name |
| --- | --- | --- |
| `ebus_v1_bus_identification_evidence_list` | `ebus_v1_` | `ebus_v1_bus_identification_evidence_list` |
| `ebus_v1_bus_messages_list` | `ebus_v1_` | `ebus_v1_bus_messages_list` |
| `ebus_v1_bus_periodicity_list` | `ebus_v1_` | `ebus_v1_bus_periodicity_list` |
| `ebus_v1_bus_protocol_specimens_list` | `ebus_v1_` | `ebus_v1_bus_protocol_specimens_list` |
| `ebus_v1_bus_summary_get` | `ebus_v1_` | `ebus_v1_bus_summary_get` |
| `ebus_v1_ebus_standard_command_get` | `ebus_v1_` | `ebus_v1_ebus_standard_command_get` |
| `ebus_v1_ebus_standard_commands_list` | `ebus_v1_` | `ebus_v1_ebus_standard_commands_list` |
| `ebus_v1_ebus_standard_decode` | `ebus_v1_` | `ebus_v1_ebus_standard_decode` |
| `ebus_v1_ebus_standard_services_list` | `ebus_v1_` | `ebus_v1_ebus_standard_services_list` |
| `ebus_v1_registry_devices_get` | `ebus_v1_` | `ebus_v1_registry_devices_get` |
| `ebus_v1_registry_devices_list` | `ebus_v1_` | `ebus_v1_registry_devices_list` |
| `ebus_v1_registry_methods_list` | `ebus_v1_` | `ebus_v1_registry_methods_list` |
| `ebus_v1_registry_planes_list` | `ebus_v1_` | `ebus_v1_registry_planes_list` |
| `ebus_v1_rpc_invoke` | `ebus_v1_` | `ebus_v1_rpc_invoke` |
| `ebus_v1_runtime_status_get` | `ebus_v1_` | `ebus_v1_runtime_status_get` |
| `ebus_v1_semantic_adapter_info_get` | `ebus_v1_` | moves with the retrieval work of #1540 |
| `ebus_v1_semantic_boiler_status_get` | `ebus_v1_` | moves with the retrieval work of #1540 |
| `ebus_v1_semantic_boiler_status_set_config` | `ebus_v1_` | `ebus_v1_semantic_boiler_status_set_config` |
| `ebus_v1_semantic_circuits_get` | `ebus_v1_` | moves with the retrieval work of #1540 |
| `ebus_v1_semantic_cylinders_get` | `ebus_v1_` | moves with the retrieval work of #1540 |
| `ebus_v1_semantic_dhw_get` | `ebus_v1_` | moves with the retrieval work of #1540 |
| `ebus_v1_semantic_energy_totals_get` | `ebus_v1_` | moves with the retrieval work of #1540 |
| `ebus_v1_semantic_fm5_interpretation_get` | `ebus_v1_` | moves with the retrieval work of #1540 |
| `ebus_v1_semantic_fm5_mode_get` | `ebus_v1_` | moves with the retrieval work of #1540 |
| `ebus_v1_semantic_radio_devices_get` | `ebus_v1_` | moves with the retrieval work of #1540 |
| `ebus_v1_semantic_schedules_get` | `ebus_v1_` | moves with the retrieval work of #1540 |
| `ebus_v1_semantic_schedules_set_dhw_time_program` | `ebus_v1_` | `ebus_v1_semantic_schedules_set_dhw_time_program` |
| `ebus_v1_semantic_schedules_set_zone_time_program` | `ebus_v1_` | `ebus_v1_semantic_schedules_set_zone_time_program` |
| `ebus_v1_semantic_snapshot_get` | `ebus_v1_` | moves with the retrieval work of #1540 |
| `ebus_v1_semantic_solar_get` | `ebus_v1_` | moves with the retrieval work of #1540 |
| `ebus_v1_semantic_system_get` | `ebus_v1_` | moves with the retrieval work of #1540 |
| `ebus_v1_semantic_system_set_config` | `ebus_v1_` | `ebus_v1_semantic_system_set_config` |
| `ebus_v1_semantic_zones_get` | `ebus_v1_` | moves with the retrieval work of #1540 |
| `ebus_v1_snapshot_capture` | `ebus_v1_` | `ebus_v1_snapshot_capture` |
| `ebus_v1_snapshot_drop` | `ebus_v1_` | `ebus_v1_snapshot_drop` |
| `ebus_v1_vaillant_errors_get` | `ebus_v1_` | `ebus_v1_vaillant_errors_get` |
| `ebus_v1_vaillant_errors_history_get` | `ebus_v1_` | `ebus_v1_vaillant_errors_history_get` |
| `ebus_v1_vaillant_errors_history_list` | `ebus_v1_` | `ebus_v1_vaillant_errors_history_list` |
| `ebus_v1_vaillant_live_monitor_get` | `ebus_v1_` | `ebus_v1_vaillant_live_monitor_get` |
| `ebus_v1_vaillant_live_monitor_session_get` | `ebus_v1_` | `ebus_v1_vaillant_live_monitor_session_get` |
| `ebus_v1_vaillant_service_current_get` | `ebus_v1_` | `ebus_v1_vaillant_service_current_get` |
| `ebus_v1_vaillant_service_history_get` | `ebus_v1_` | `ebus_v1_vaillant_service_history_get` |
| `ebus_v1_watch_summary_get` | `ebus_v1_` | `ebus_v1_watch_summary_get` |
| `eebus_v1_features_data_get` | `eebus_v1_` | `eebus_v1_features_data_get` |
| `eebus_v1_features_data_set` | `eebus_v1_` | `eebus_v1_features_data_set` |
| `eebus_v1_features_get` | `eebus_v1_` | `eebus_v1_features_get` |
| `eebus_v1_mutations_get` | `eebus_v1_` | `eebus_v1_mutations_get` |
| `eebus_v1_mutations_rollback` | `eebus_v1_` | `eebus_v1_mutations_rollback` |
| `eebus_v1_pairing_status_get` | `eebus_v1_` | `eebus_v1_pairing_status_get` |
| `eebus_v1_runtime_status_get` | `eebus_v1_` | `eebus_v1_runtime_status_get` |
| `eebus_v1_services_get` | `eebus_v1_` | `eebus_v1_services_get` |
| `eebus_v1_services_list` | `eebus_v1_` | `eebus_v1_services_list` |
| `eebus_v1_sessions_get` | `eebus_v1_` | `eebus_v1_sessions_get` |
| `eebus_v1_sessions_list` | `eebus_v1_` | `eebus_v1_sessions_list` |
| `eebus_v1_snapshot_capture` | `eebus_v1_` | `eebus_v1_snapshot_capture` |
| `eebus_v1_snapshot_drop` | `eebus_v1_` | `eebus_v1_snapshot_drop` |
| `eebus_v1_topology_get` | `eebus_v1_` | `eebus_v1_topology_get` |
| `helianthus_runtime_get` | `helianthus_runtime_` | `helianthus_runtime_get` |
| `helianthus_runtime_list` | `helianthus_runtime_` | `helianthus_runtime_list` |
| `helianthus_runtime_restart` | `helianthus_runtime_` | `helianthus_runtime_restart` |
| `helianthus_runtime_start` | `helianthus_runtime_` | `helianthus_runtime_start` |
| `helianthus_runtime_stop` | `helianthus_runtime_` | `helianthus_runtime_stop` |
| `helianthus_v1_leaf_promotion_capture` | `helianthus_v1_` | `helianthus_v1_leaf_promotion_capture` |
| `helianthus_v1_source_state_get` | `helianthus_v1_` | `helianthus_v1_source_state_get` |
| `helianthus_v1_synchronized_evidence_capture` | `helianthus_v1_` | `helianthus_v1_synchronized_evidence_capture` |
| `modbus_v1_fronius_sunspec_status_get` | `modbus_v1_` | `modbus_v1_fronius_sunspec_status_get` |
| `modbus_v1_growatt_bms_rs485_status_get` | `modbus_v1_` | `modbus_v1_growatt_bms_rs485_status_get` |
| `modbus_v1_growatt_protocol_ii_identity_get` | `modbus_v1_` | `modbus_v1_growatt_protocol_ii_identity_get` |
| `modbus_v1_huawei_emma_identity_get` | `modbus_v1_` | `modbus_v1_huawei_emma_identity_get` |
| `modbus_v1_huawei_emma_status_get` | `modbus_v1_` | `modbus_v1_huawei_emma_status_get` |
| `modbus_v1_huawei_sdongle_status_get` | `modbus_v1_` | `modbus_v1_huawei_sdongle_status_get` |
| `modbus_v1_huawei_smartlogger_status_get` | `modbus_v1_` | `modbus_v1_huawei_smartlogger_status_get` |
| `modbus_v1_outback_axs_status_get` | `modbus_v1_` | `modbus_v1_outback_axs_status_get` |
| `modbus_v1_profile_observation_get` | `modbus_v1_` | `modbus_v1_profile_observation_get` |
| `modbus_v1_raw_read` | `modbus_v1_` | `modbus_v1_raw_read` |
| `modbus_v1_tesla_fc100_summary_get` | `modbus_v1_` | `modbus_v1_tesla_fc100_summary_get` |
| `modbus_v1_tesla_gen3_evse_current_limit_get` | `modbus_v1_` | `modbus_v1_tesla_gen3_evse_current_limit_get` |
| `modbus_v1_tesla_hsc_status_get` | `modbus_v1_` | `modbus_v1_tesla_hsc_status_get` |
| `modbus_v1_tesla_wcapi_get_vitals_replay_get` | `modbus_v1_` | `modbus_v1_tesla_wcapi_get_vitals_replay_get` |
| `semantic_v1_evse_current_get` | `semantic_v1_` | `semantic_v1_evse_current_get` |
| `semantic_v1_pv_current_get` | `semantic_v1_` | `semantic_v1_pv_current_get` |
| `semantic_v1_snapshot_get` | `semantic_v1_` | `semantic_v1_snapshot_get` |
| `semantic_v1_storage_growatt_current_get` | `semantic_v1_` | `semantic_v1_storage_growatt_current_get` |
