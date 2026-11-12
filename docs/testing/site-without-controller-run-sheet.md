# Reduced run sheet: eBUS site without a system controller

Refs [gateway #1469](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1469),
a sub-issue of [gateway #1449](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1449).
This is the reduced form of the single operator session in the
[consolidated candidate handoff](consolidated-candidate-handoff.md) for one site
profile: the eBUS carries a heat source and the vendor's internet module, and
no system controller ever appears. A third-party room thermostat that is not an
eBUS participant is invisible to the gateway and does not change the profile.
The sheet grants no authority to operate equipment, adds no device command and
records no result. The handoff's preconditions, stop conditions, evidence
grades and per-row evidence rules apply unchanged. Site names, addresses of the
installation, serial numbers and device identifiers are recorded only in the
operator's private record.

The handoff is the only hardware session package in this repository. Its
transport, adapter-direct, passive, retained and consumer rows are defined by
their governing references, which the handoff links. No separate site run
sheet exists under `docs/testing`, `docs/observability`, `docs/architecture` or
`matrix`, so this sheet is self-contained.

## Offline expectation

The offline scenario
[`controllerless_site_acceptance_test.go`](../../cmd/gateway/controllerless_site_acceptance_test.go)
runs in the gateway's normal `go test` and states what the current gateway does
on this profile. It drives the production startup scan, registry scan, semantic
poller and poll schedule over a synthetic bus in simulated time. Compare the
live session against it; a difference is a finding, not a reason to adjust the
expectation.

| Topic | Expected on a controller-less bus |
|---|---|
| Startup, add-on default (adapter-direct, automatic source) | One startup scan pass. The heat source answers identification; the controller (`0x15`) and module-slot (`0x26`) addresses stay silent and are each tried four times in that pass. Semantic polling is released by the active probe, not by a controller. |
| Startup, explicitly configured source | Two passes, then root confirmation is given up: semantic polling starts and the scan loop reports admission failure. |
| Startup phase, boiler-class heat source | `BOOT_INIT` → `LIVE_WARMUP` → `LIVE_READY` during the first boiler priming pass, from the boiler stream alone, well inside the 120 s boot-live timeout. |
| Startup phase, heat-pump-class heat source | No stream is published (boiler status is read only from boiler-class identities); the boot-live timer moves `BOOT_INIT` to `DEGRADED`. |
| Controller-class traffic | No B524 register read, no B555 read, no write. B524 root discovery probes each candidate once (`0x15` first, then `0x08`, `0x26` and any identified device) on every 60 s state tick, again on every 5 min configuration tick and every 10 min discovery tick: 3 probe calls per minute, 6 at 5 min and 9 at 10 min marks with three candidates. A silent address costs at most three bus attempts per probe call. Each failed pass logs `semantic_b524_root: … no coherent responder`. It never stops and is never a tight loop. |
| Published streams | Boiler status from the heat source's own B509 registers (flow temperature stays null in production; no DHW mirror field). Empty lists for zones, circuits, radio devices and cylinders; null DHW, system and schedules; an all-null solar plane; FM5 mode `ABSENT`; energy totals at the 0 placeholder with provenance `none`. |
| Absence signal | `vaillant_regulator_capability` is `NONE` from the first discovery pass in GraphQL, in MCP `ebus_v1_runtime_status_get` and in the semantic snapshot's runtime status; `UNKNOWN` when any identified Vaillant device has no readable catalogued serial. The debug-only absence state (`semantic_regulator_state` on `/debug/vars`) goes `PRESENT` → `ABSENCE_GRACE` at the first 60 s recheck → `ABSENT` five minutes later. Neither the Portal nor the Home Assistant integration reads the capability today. |

## Session rules for this profile

- Allowed operations: start, stop and restart the add-on; read GraphQL, MCP,
  the Portal, Home Assistant, logs and `/debug/vars`; observe the bus
  passively. Every gateway frame on this profile is a read.
- Not allowed: any write through GraphQL, MCP, the Portal or Home Assistant;
  any device or configuration change not named by a row; anything that answers
  or acts as a controller.
- Additional stop condition: the heat source shows a fault, a changed
  operating state or the internet module loses its service while the gateway
  probes. Record the time and stop the affected row.
- Every row starts **Not run**, evidence **Unknown**, qualification and
  admission **Unassessed**, as in the handoff.

## Controller rows removed

| Row | Reason |
|---|---|
| Explorer Pi (consumer row) | The VRC Explorer scans the system controller's B524 register space; this bus has no such target. |
| P03 (passive suite) | Its bounded observe-first proof uses a mixed B524/B509 canary set; the B524 part needs a controller. |
| Adapter-multiplexer v2 cells with the VRC Explorer as northbound client | Same reason as the Explorer row; applies only when the v2 form is active. |
| ADV-01 to ADV-04 (runtime adversarial contract, outside the handoff) | Each requires zone epochs, ADV-01 and ADV-04 also DHW; a controller-less bus publishes neither. |

## Retained rows

Rows CL-01 to CL-10 are the profile-specific observations. The handoff rows
below them keep their governing references; where a row needs a workload
target, use the heat source instead of a controller.

| ID | Precondition | Allowed operation | Pass criterion | Result |
|---|---|---|---|---|
| CL-01 Fresh-install startup | Candidate build installed fresh, add-on defaults (adapter-direct, automatic source), no semantic cache, heat source powered and on the bus. | Start the add-on; read logs, the Portal bus observability view and `semantic_startup_current_phase`. | Startup scan finishes its first pass with the heat source identified and does not repeat; semantic polling starts. Boiler-class heat source: phase `LIVE_READY` within 120 s of add-on start and no `DEGRADED`. Heat-pump-class heat source: record the phase (`DEGRADED` expected) as a finding, not a pass. | Not run |
| CL-02 Inventory | CL-01 done. | Read GraphQL `devices` and MCP device listing. | The heat source is listed with manufacturer and device identity; no device at the controller addresses `0x10`/`0x15`; the internet module is listed if it was identified. Serial presence recorded privately. | Not run |
| CL-03 Absence signal | CL-01 done; 7 min since polling started. | Read GraphQL `vaillant_regulator_capability`, MCP `ebus_v1_runtime_status_get` and `/debug/vars`. | GraphQL and MCP agree; value `NONE`, or `UNKNOWN` with the unidentified device named in the private record; `semantic_regulator_state` is `ABSENT`, reached through `ABSENCE_GRACE`. | Not run |
| CL-04 Controller-class traffic | Passive observation available (adapter-direct); 30 min of polling. | Count gateway-originated B524 frames per target and minute from passive evidence and logs. | Only root discovery requests from the gateway source, within the per-minute plan above (at most three attempts per probe call); no B524 register read, B555 read or write; no increase over the 30 min. | Not run |
| CL-05 Heat-source stream | Boiler-class heat source; CL-01 done. | Read GraphQL `boiler_status` twice, 60 s apart; compare with the appliance display. | Water pressure and flame state match the appliance; values refresh; `flow_temperature_c`, `dhw_temperature_c`, `dhw_target_temperature_c` and `dhw_operating_mode` are null. | Not run |
| CL-06 Controller-owned planes | CL-01 done. | Read GraphQL zones, circuits, radio devices, cylinders, DHW, system, schedules, solar, FM5 mode and energy totals. | Matches the expectation table: empty lists, nulls, all-null solar, FM5 `ABSENT`, energy 0 with provenance `none`. No invented zone, DHW or system value. | Not run |
| CL-07 Heat-source tolerance of root probes | CL-04 running. | Passively record how the heat source answers B524 reads (no answer, NACK or reply) and watch the appliance. | No fault or changed operating state at the heat source over 30 min. How it answers is recorded as Unknown evidence today. | Not run |
| CL-08 Internet module coexistence | Vendor app access available to the operator. | Use the vendor app read-only; observe the module's bus traffic. | The module and its app keep working throughout. Once the gateway has identified the module (one identification and its serial windows), the module joins the root discovery candidates and receives the same per-minute B524 root probe; nothing else is addressed to it. | Not run |
| CL-09 Restart with cache | CL-01 to CL-06 done. | Restart the add-on once. | Phase passes through `CACHE_LOADED_STALE` to `LIVE_READY` (boiler-class) within 120 s; CL-03 and CL-06 hold again. The offline scenario does not cover a restart with a cache; this row is the first evidence. | Not run |
| CL-10 Home Assistant view | Integration configured against this gateway. | Read the integration's devices and entities. | Boiler sensors present and plausible; no climate or water-heater entity; record what the six energy sensors show (0 kWh is expected from the placeholder and is reported, not accepted). | Not run |
| T01–T04 | Only the installed direct transport; matrix workload aimed at the heat source. | As the [smoke matrix](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/development/smoke-matrix.md) defines for that case; read-only smoke. | Gateway smoke (`startup`, `scan`, `graphql`, `mcp`) and the Home Assistant inventory verifier pass without a controller. | Not run |
| T05–T88, PX01–PX12 | Only if the session brings ebusd or the proxy to this site; otherwise outside this site's installed topology. | As the handoff's governing references define. | As defined there; workload aimed at the heat source. | Not run |
| AD01–AD04, AD09, AD12 | Adapter-direct installed; third-party traffic between the internet module and the heat source present for AD03. | As the [AD01–AD12 index](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/development/ad01-ad12-evidence-index.md) requires per row. | That row's assertion, with the heat source as active target. | Not run |
| AD05–AD08, AD10, AD11 | Only if an ebusd client or a migration target is brought to this site. | As the AD index requires per row. | That row's assertion. | Not run |
| P01, P02, P04, P05, P06 | The topology each case names. | As the passive gate defines. | That case's passive mode and outcome. | Not run |
| ESERA / FOLLOWUP-01 | ESERA passive equipment and its separate authorization. | As K05 defines. | As K05 defines. | Not run |
| LEGACY-PERSIST P1–P6, N1–N4; LEGACY-IDENTITY P1–P6, N1–N5; LEGACY-MUX | Owner reviews each retained criterion first. | As the retained acceptance defines. | As defined there; a criterion that needs zones, DHW or a controller identity is recorded Not applicable with that reason, never passed. | Not run |
| HA/Ingress routing, trust, lifecycle and identity | Add-on installed with Ingress. | As the [HA checklist](https://github.com/Project-Helianthus/helianthus-ha-addon/blob/main/docs/portal-ingress-offline-acceptance.md) defines. | As defined there; independent of the controller. | Not run |
| INT-12, INT-13, prior RTU applicability, HUAWEI rows | Their own equipment, not part of this profile. | As the handoff defines. | Unchanged; not run at this site unless that equipment is present. | Not run |

## Open points carried into the session

- Root discovery keeps probing about once a minute for the life of the process
  on this profile; the absence state does not throttle it.
- The debug absence state starts at `PRESENT` for the first minute on a bus
  that never had a controller.
- With an explicitly configured source the startup scan reports admission
  failure on this profile.
- A heat-pump-class heat source yields no semantic stream and a `DEGRADED`
  phase.
- Energy totals are served as 0 with provenance `none`; the Home Assistant
  energy sensors ignore provenance.

These are recorded for review; the session observes them and changes nothing.
