# Current transport and semantic metric coverage limits

This is a partial coverage audit for [issue #1005](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1005), not closure of its original acceptance checklist. **Proven** below means established by the current source renderer; **Hypothesis** means a proposed interpretation or future design; **Unknown** means evidence is missing. These evidence grades are separate from qualification, admission, freshness and physical acceptance. Nothing here promises stability before Helianthus 1.0.

The public [native transport and semantic coverage contract](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/deployment/prometheus-semantic-coverage.md) owns the coverage boundary. This page inventories gateway exposition; it introduces no protocol facts, decoder meaning, acquisition or electrical capability. Native drivers retain native evidence and qualification. SemReg owns promoted protocol-neutral projections; the gateway composes them.

## Coverage of the original groups

Declared means supported by renderer code, configured means selected for this target, exported means present in scrape text, and physically verified requires separate device/transport evidence. A declared renderer is not an exported production measurement. All source observations in this table are **Proven**; physical verification for every row is **Unknown** in this audit.

| Original group | Declared and configured | Currently exported / absence semantics | Owner and remaining limit |
| --- | --- | --- | --- |
| Actual transport lifecycle status | `helianthus_transport_runtime_status`, unitless gauge; fixed `modbus_tcp` and `eebus` inventory | Exactly one sample of value `1` per protocol, including disabled/unconfigured; the selected labels describe local lifecycle, not electrical or peer capability | Gateway lifecycle snapshot; no native counter acquisition in this family. [Runtime renderer](../../runtime_transport_metrics.go), [contract](../transport-prometheus-runtime-v1.md). |
| eBUS observation counters and passive status | Store renderer has frame/error/byte counters, passive-state gauges and conditional busy-time families; inventory below | Frame/error/byte maps emit only present entries. Flat counters mean no observed increment; missing entries mean Unknown, not zero traffic. Busy-time families are omitted unless passive state is `available` and passive timing is usable | Native eBUS evidence feeds the gateway store; observed errors, retries and inferred timing do not establish electrical health. [Store renderer and classifiers](../../bus_observability_store.go), [alert interpretation](current-transport-alerts.md). |
| eBUS baud | `ebus_baud_observation_available`, unitless gauge, no labels; conditional on stored eBUS configuration presence | Configured: availability `0`. Unconfigured: omit HELP, TYPE and sample. Measured baud value is omitted entirely; no baud value name is reserved here | Native transport must supply a qualified timing observation with expiry/reconnect invalidation. [Baud renderer](../../baud_observation_metrics.go), [configuration semantics](../transport-prometheus-runtime-v1.md#ebus-baud-observation-availability). |
| CAN frames, errors and interface state | No composed CAN owner and no emitted CAN series; `not_composed` is documentation, not a runtime-status label value | No CAN counters, interface-state or availability samples. Absence is not disabled CAN, zero traffic or a healthy interface | `helianthus-canbus` owns native transport acquisition; native registry/qualification stays with `helianthus-canbusreg`; gateway owns future composition. [Observation boundary](../architecture/can-metric-observation-contract.md) is **Hypothesis**, not an implemented exporter or reserved metric inventory. |
| Thermal and Infrastructure semantic families | Renderer supports the 32 fields below, but production registry has no bindings | Only HELP/TYPE for `helianthus_semantic_field_available`; no availability samples and no value families, including value HELP/TYPE. No hub capture for the empty registry | SemReg owns projections; gateway owns export binding. [Production route](../../cmd/gateway/semantic_metrics_runtime.go) and [route contract](../semantic-prometheus-thermal-infrastructure-v1.md). Durable assignment/revalidation remains [issue #1341](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1341); O21 does not establish production values here. |

The baud omission is intentional: cumulative symbol ingress, framing/reset counters, symbol throughput, application timing or a configured constant cannot establish measured baud. The current baud renderer reads none of them. This audit makes no new symbol/framing/reset export claim and never relabels throughput as baud.

## Bounded transport inventory

This inventory covers the existing transport signals relevant to the original groups, not every diagnostic family in the store.

| Metric | Type / unit | Renderer labels and bounds | Export condition / interpretation |
| --- | --- | --- | --- |
| `helianthus_transport_runtime_status` | gauge / unitless | `protocol`: `modbus_tcp`, `eebus`; `state`: `disabled`, `starting`, `ready`, `degraded`, `retired`, `unknown`; `reason`: `not_configured`, `none`, `startup_failed`, `shutdown`, `unknown`; `outcome`: `unavailable`, `pending`, `available`, `unknown` | Two samples per scrape; at most 240 label tuples across the finite vocabulary. Invalid state/reason/outcome normalizes to unknown; unsupported protocols are not admitted. `ready` is local runtime composition, not a qualified/live peer. |
| `ebus_frames_observed_total` | counter / frames | `scope`: `active`, `passive`; `src`, `dst`: byte buckets (256 each); `family`: uppercase four-hex-digit command family (65,536 possible); `frame_type`: `broadcast`, `initiator_initiator`, `initiator_target`, `abandoned_partial`, `local_participant_inbound` | Present map entries only; insertion overflow is reported, not proof of lost frames. Active attempts and retries prevent interpreting frame/error ratios as failure probabilities. |
| `ebus_errors_total` | counter / events | `scope`: `active`, `passive`; `class`: `collision`, `timeout`, `nack`, `crc_mismatch`, `echo_mismatch`, `transport_reset`, `decode_fault`, `abandoned`; `phase`: `request`, `terminal`, `ack`, `response` | Present map entries only; class/phase combinations follow source classifiers, not the full Cartesian product. No family/address labels. |
| `ebus_frame_bytes_total` | counter / retained frame bytes | `scope`, `family`, `frame_type` as above; `part`: `request`, `response` | Present entries only; byte volume is not symbol rate or baud. |
| `ebus_observability_series_budget_overflow_total` | counter / overflow events | none | Store emits the count; overflowed insertion still occurs, so the configured budget is not a strict series ceiling. |
| `ebus_passive_warmup_state` | gauge / one-hot state | `state`: `unavailable`, `warming_up`, `available` | All three samples; one selected state. Does not establish active-path health. |
| `ebus_passive_capability_unavailable_reason` | gauge / one-hot reason while unavailable | `reason`: `startup_timeout`, `reconnect_timeout`, `socket_loss`, `flap_dampened`, `unsupported_or_misconfigured`, `capability_withdrawn` | All six samples; all-zero during warmup does not mean available. |
| `ebus_bus_busy_seconds_total` | counter / seconds | none | Only when passive state is available and timing quality is not unavailable; estimated observation, not electrical proof. |
| `ebus_bus_busy_ratio` | gauge / ratio | `window`: `1m`, `5m`, `15m`, `1h` | Same condition as busy seconds; four clipped ratios. Absence is Unknown, not idle bus. |
| `ebus_baud_observation_available` | gauge / 0 or 1 | none | Configured eBUS only; current implementation always exports `0`, never a measured baud value. |

The frame/error insertion checks use configured budgets and hard-cap thresholds of 2,048/512, but do not evict or reject the new entry when exceeded. Their label spaces are finite; those thresholds must not be described as strict retained-series caps. Native byte buckets and command families are protocol vocabulary, not device-model labels. No actual participant values or capture data are reproduced here. Scrape-added deployment labels are outside renderer bounds and need their own bounded, non-identifying policy.

## Semantic field inventory and conversions

[Renderer source](../../internal/semanticmetrics/metrics.go) is the exact implemented inventory below. Every value is a gauge with only `slot`; `slot` is the decimal label `0` through `15`. Availability is the unitless gauge `helianthus_semantic_field_available{domain,field,slot}`: `domain` is exactly `thermal` or `infrastructure`, and `field` is exactly one of the matching domain's rows. No provenance, projection-loss, native identity, dimensions, error text, source, service or binding labels are emitted by this renderer. Native evidence is retained upstream; export requires exact projection rather than fabricating a loss label.

| `field` | Value metric | Unit / conversion or symbol encoding |
| --- | --- | --- |
| `thermal.action.state` | `helianthus_thermal_action_state` | enum: idle=0, heating=1, cooling=2, dhw=3, ventilating=4 |
| `thermal.demand.level` | `helianthus_thermal_demand_ratio` | ratio; percent / 100 |
| `thermal.measurement.air_temperature` | `helianthus_thermal_air_temperature_celsius` | Celsius; identity |
| `thermal.measurement.flow_rate` | `helianthus_thermal_flow_cubic_meters_per_second` | m^3/s; litre/minute / 60,000 |
| `thermal.measurement.humidity_relative` | `helianthus_thermal_relative_humidity_ratio` | ratio; percent / 100 |
| `thermal.measurement.power` | `helianthus_thermal_power_watts` | watts; preserve sign |
| `thermal.measurement.water_temperature` | `helianthus_thermal_water_temperature_celsius` | Celsius; identity |
| `thermal.mode.system` | `helianthus_thermal_system_mode` | enum: off=0, auto=1, heat=2, cool=3 |
| `thermal.mode.zone` | `helianthus_thermal_zone_mode` | enum: off=0, auto=1, comfort=2, eco=3 |
| `thermal.setpoint.dhw_temperature` | `helianthus_thermal_dhw_setpoint_celsius` | Celsius; identity; read-only observation |
| `thermal.setpoint.temperature` | `helianthus_thermal_setpoint_celsius` | Celsius; identity; read-only observation |
| `thermal.status.fault` | `helianthus_thermal_fault_present` | boolean: clear=0, present=1 |
| `thermal.status.operation` | `helianthus_thermal_operation_active` | boolean: idle=0, active=1 |
| `infrastructure.ac.active_power` | `helianthus_infrastructure_ac_active_power_watts` | watts; preserve sign |
| `infrastructure.ac.apparent_power` | `helianthus_infrastructure_ac_apparent_power_volt_amperes` | VA; identity |
| `infrastructure.ac.current` | `helianthus_infrastructure_ac_current_amperes` | amperes; identity |
| `infrastructure.ac.frequency` | `helianthus_infrastructure_ac_frequency_hertz` | hertz; identity |
| `infrastructure.ac.power_factor` | `helianthus_infrastructure_ac_power_factor_ratio` | ratio; preserve sign |
| `infrastructure.ac.reactive_power` | `helianthus_infrastructure_ac_reactive_power_vars` | var; preserve sign |
| `infrastructure.ac.voltage` | `helianthus_infrastructure_ac_voltage_volts` | volts; identity |
| `infrastructure.energy.export` | `helianthus_infrastructure_energy_export_joules` | joules; kWh x 3,600,000; gauge |
| `infrastructure.energy.import` | `helianthus_infrastructure_energy_import_joules` | joules; kWh x 3,600,000; gauge |
| `infrastructure.power.apparent` | `helianthus_infrastructure_apparent_power_volt_amperes` | VA; identity |
| `infrastructure.power.export_active` | `helianthus_infrastructure_export_active_power_watts` | watts; nonnegative directional value |
| `infrastructure.power.export_reactive` | `helianthus_infrastructure_export_reactive_power_vars` | var; nonnegative directional value |
| `infrastructure.power.import_active` | `helianthus_infrastructure_import_active_power_watts` | watts; nonnegative directional value |
| `infrastructure.power.import_reactive` | `helianthus_infrastructure_import_reactive_power_vars` | var; nonnegative directional value |
| `infrastructure.status.breaker` | `helianthus_infrastructure_breaker_open` | boolean: closed=0, open=1 |
| `infrastructure.status.connection` | `helianthus_infrastructure_connection_connected` | boolean: disconnected=0, connected=1 |
| `infrastructure.status.fault` | `helianthus_infrastructure_fault_present` | boolean: clear=0, present=1 |
| `infrastructure.status.interlock` | `helianthus_infrastructure_interlock_active` | boolean: clear=0, active=1 |
| `infrastructure.status.readiness` | `helianthus_infrastructure_ready` | boolean: not_ready=0, ready=1 |

There are 13 Thermal and 19 Infrastructure fields. Sixteen slots per domain give at most 208 Thermal plus 304 Infrastructure value samples and the same 512 availability samples (1,024 combined per target). A binding has at least one field, one dimension, a unique domain/slot and a unique source/service tuple; unknown, duplicate or cross-domain fields and invalid/out-of-range slots fail registry validation. At most 32 distinct assets can be selected for capture. Retired slots remain reserved by contract; the renderer does not allocate or persist them.

Only configured fields get availability samples. A validated, non-retired binding with a current matching source epoch and driver generation, qualified/available service and read capability, exact projection, a single non-conflicting qualified/promoted/good candidate, fresh matching evaluation and a valid finite converted value emits availability `1` and its value. Unvalidated, retired, unavailable, stale, malformed or withheld selections emit availability `0` for configured fields and omit their numeric values; an invalid registry fails composition. Zero is a legitimate measurement only when admitted. Unconfigured fields produce neither availability samples nor value metadata. A failed capture/view withholds values without reusing a previous scrape or acquiring native data; valid sibling selections survive field-specific rejection. This is an export rule, not permission to erase upstream per-field last-known-good evidence.

Production uses an empty registry, so the configured upper bounds above are implementation capacity, not currently exported production values. Durable publication, restart revalidation and retired-slot reservation remain the owner work in [#1341](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1341). Native CAN composition remains separate. eeBUS output belongs to `helianthus-eebusreg`; it is not implied by gateway lifecycle status. Existing eBUS MCP envelopes, deterministic goldens and GraphQL parity are unchanged.

## Acceptance limits

**Proven:** manual reconciliation of current renderers establishes this inventory and the omission rules. **Hypothesis:** future native CAN observations can feed a bounded host exporter once native contracts and composition are accepted. **Unknown:** actual deployed values, electrical timing/capability, physical transport behavior, device qualification and final-candidate acceptance. No hardware observation follows from source inspection. Full committed-change CI, independent review and applicable transport/passive classification remain separate acceptance work; this document does not close #1005 or introduce alert rules.
