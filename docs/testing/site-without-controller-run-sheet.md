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
on this profile. It drives the production startup scan, registry scan, B524 root
probe, semantic poller, read scheduler and poll schedule over a recording
synthetic bus on a simulated clock that starts at process start, so every
frame, retry and pause the gateway produces is counted and timed. No device on
the synthetic bus answers B524. Two timing bounds are used: wire timing (a
frame costs its symbols at 2400 Bd; an unanswered request is noticed one symbol
after it) and timeout timing (every unanswered request runs to its per-attempt
timeout: 400 ms for a scan request, 5 s for a root probe attempt). Times below
are from add-on start, wire timing first. Compare the live session against it;
a difference is a finding, not a reason to adjust the expectation.

| Topic | Expected on a controller-less bus |
|---|---|
| Startup, add-on default (adapter-direct, automatic source) | 5 s passive warm-up, then one startup scan pass. The heat source answers identification and its four serial windows; the controller (`0x15`) and module-slot (`0x26`) addresses stay silent and are each tried four times in that pass. One root confirmation probe call goes to `0x08` (three B524 read attempts). Semantic polling is released by the active probe, not by a controller: at 7.2 s (wire) or 25.0 s (timeout timing). |
| Startup, explicitly configured source | The same single pass and root confirmation probe call as the automatic source. Root confirmation finds no B524 root and the product catalog classifies every Vaillant identity of the inventory as a non-controller, so active confirmation decides: the heat source answered identification, the active probe passes and the gateway logs `startup scan: root confirmation not required: inventory has no regulator identity; active confirmation=true`. Semantic polling starts at 7.2 s (wire) or 25.0 s (timeout timing); no admission failure. Because the probe passes, the runtime-state revalidator starts: expect directed identification reads (service 07 04) from the gateway source to the cached runtime-state members (the heat source and any identified module), one cycle at once and then every 15 min. If the inventory holds a regulator identity, or a Vaillant identity without a readable cataloged serial, root confirmation stays required: two passes 30 s apart, then admission failure while semantic polling still starts (about 40 s at wire speed with such an identity at `0x15`). |
| Startup phase, boiler-class heat source | `BOOT_INIT` → `LIVE_WARMUP` → `LIVE_READY` from the boiler stream alone, after the first discovery pass and the boiler fast and medium tiers. Automatic and explicit source alike: `LIVE_READY` at 11.5 s (wire) or 73.8 s (timeout timing), 108 s and 46 s before the 120 s boot-live deadline. With an explicit source and an unclassified identity at `0x15`, timeout timing lets the deadline pass first: `DEGRADED` at 120 s, then `LIVE_READY`. |
| Startup phase, heat-pump-class heat source | No stream is published (boiler status is read only from boiler-class identities) and no heat-pump register is read. The first discovery pass (1.7 s after polling starts) finds no B524 root and no boiler-class heat source while the inventory holds a heat-pump-class identity (a heat-pump device-ID family `EHP`, `HMU`, `VWL`, `VWS` or `VWZ`, or a part number the product catalog lists with role Heat Pump). It sets the startup reason `no_supported_heat_source_path` on the bus observability startup block next to the phase (GraphQL `bus_summary.status.startup.reason`, MCP `ebus_v1_bus_summary_get` `status.startup.reason`) and logs `semantic_startup_reason reason=no_supported_heat_source_path state=set heat_source=0x08`. The boot-live timer still moves `BOOT_INIT` to `DEGRADED` at 120 s; that transition logs `reason="boot_live_timeout elapsed=2m0s startup_reason=no_supported_heat_source_path"`. A heat source of unknown class (neither family nor catalog role) gets no reason and the plain `boot_live_timeout` transition. A heat source that answers the B524 root probe coherently is adopted as semantic root as before; the reason is then cleared (`state=cleared`) or never set. A controller that appears later is found as the controller-class traffic row describes, and the found root clears the reason (with a registry regulator identity after `ABSENT`: at the next 60 s regulator recheck). |
| Controller-class traffic | No B524 register read, no B555 read, no write. B524 root discovery makes one production probe call per candidate (`0x15` first, then `0x08`, `0x26` and any identified device) per pass. Until the absence state is `ABSENT` (6 min of polling) a pass runs on every 60 s state tick, again on every 5 min configuration tick and on the 10 min discovery tick: 3 probe calls per minute and 6 at the 5 min mark with three candidates. Once `ABSENT`, with no controller known and none discovered since the gateway started, the state and configuration ticks skip their pass and the gateway logs `semantic_discovery_fallback_suspended regulator_state=ABSENT discovery_interval=10m0s` once; passes then run only on the discovery tick (3 calls every 10 min) and on event-driven refreshes. The skipped ticks still read the heat source from the registry, so a heat source identified after `ABSENT` starts its status reads on the next tick. With the periodic discovery task disabled (discovery interval zero or less) the per-minute search is never suspended. In 30 min that is 33 calls, 11 per candidate. Each call to an address that does not answer is three B524 read attempts with the regulator register selector, 200 ms apart: 99 frames in 30 min. Each failed pass logs `semantic_b524_root: … no coherent responder`. Nothing runs as a tight loop. A controller that appears while `ABSENT` is found at once when the passive layer inserts its address (enqueued discovery refresh), at the next 60 s regulator recheck when the registry gains its identity, and otherwise at the next discovery tick, within 10 min. |
| Published streams | Boiler status from the heat source's own B509 registers (flow temperature stays null in production; no DHW mirror field). The fast-tier registers are read from the bus once a minute: the 30 s tick on the half minute finds the values under 30 s old and serves them from the read cache. Medium-tier registers every 5 min, slow-tier every 10 min. Empty lists for zones, circuits, radio devices and cylinders; null DHW, system and schedules; an all-null solar plane; FM5 mode `ABSENT`; energy totals null in GraphQL (`energy_totals`) and in the MCP energy plane, because no energy point is ever accepted. |
| Absence signal | `vaillant_regulator_capability` is `NONE` from the first discovery pass (1.7 s after polling starts) in GraphQL, in MCP `ebus_v1_runtime_status_get` and in the semantic snapshot's runtime status; `UNKNOWN` when any identified Vaillant device has no readable cataloged serial. The debug-only absence state (`semantic_regulator_state` on `/debug/vars`) starts at `UNKNOWN` (not yet evaluated), goes `UNKNOWN` → `ABSENCE_GRACE` at the first recheck (63 s after polling starts) → `ABSENT` five minutes later; the transition log and `semantic_regulator_transitions_total` name `UNKNOWN` as the first source state. Neither the Portal nor the Home Assistant integration reads the capability today. |

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

Rows CL-01 to CL-11 are the profile-specific observations. The handoff rows
below them keep their governing references; where a row needs a workload
target, use the heat source instead of a controller.

| ID | Precondition | Allowed operation | Pass criterion | Result |
|---|---|---|---|---|
| CL-01 Fresh-install startup | Candidate build installed fresh, add-on defaults (adapter-direct, automatic source), no semantic cache, heat source powered and on the bus. | Start the add-on; read logs, the Portal bus observability view and `semantic_startup_current_phase`. | Startup scan finishes its first pass with the heat source identified and does not repeat; semantic polling starts with the source admitted and no admission failure. If the installation uses an explicitly configured source, the same holds and the log shows `startup scan: root confirmation not required: inventory has no regulator identity`. Boiler-class heat source: phase `LIVE_READY` within 120 s of add-on start and no `DEGRADED`; record the time (11.5 s expected at wire speed). Heat-pump-class heat source: startup reason `no_supported_heat_source_path` from the first discovery pass and phase `DEGRADED` after 120 s with the same reason in the transition log; record both, and record the phase as a finding, not a pass. | Not run |
| CL-02 Inventory | CL-01 done. | Read GraphQL `devices` and MCP device listing. | The heat source is listed with manufacturer and device identity; no device at the controller addresses `0x10`/`0x15`; the internet module is listed if it was identified. Serial presence recorded privately. | Not run |
| CL-03 Absence signal | CL-01 done; 7 min since polling started. | Read GraphQL `vaillant_regulator_capability`, MCP `ebus_v1_runtime_status_get` and `/debug/vars`. | GraphQL and MCP agree; value `NONE`, or `UNKNOWN` with the unidentified device named in the private record; `semantic_regulator_state` is `ABSENT`, reached through `UNKNOWN` → `ABSENCE_GRACE` → `ABSENT`; no transition from `PRESENT`. | Not run |
| CL-04 Controller-class traffic | Passive observation available (adapter-direct); 30 min of polling. | Count gateway-originated B524 frames per target and minute from passive evidence and logs. | Only root discovery requests from the gateway source: the per-minute plan above for the first 6 min of polling, then one pass per 10 min discovery tick (three attempts per probe call to a non-answering address); the suspension log line once; no B524 register read, B555 read or write; no increase over the 30 min. | Not run |
| CL-05 Heat-source stream | Boiler-class heat source; CL-01 done. | Read GraphQL `boiler_status` twice, 2 min apart; compare with the appliance display. | Water pressure and flame state match the appliance; values refresh (fast-tier registers are re-read once a minute); `flow_temperature_c`, `dhw_temperature_c`, `dhw_target_temperature_c` and `dhw_operating_mode` are null. | Not run |
| CL-06 Controller-owned planes | CL-01 done. | Read GraphQL zones, circuits, radio devices, cylinders, DHW, system, schedules, solar, FM5 mode and energy totals. | Matches the expectation table: empty lists, nulls, all-null solar, FM5 `ABSENT`, a null energy plane in GraphQL and MCP. No invented zone, DHW, system or energy value. Heat-pump-class heat source: in addition no heat-source stream, `boiler_status` null, and the startup block shows reason `no_supported_heat_source_path`. | Not run |
| CL-07 Heat-source tolerance of root probes | CL-04 running. | Passively record how the heat source answers B524 reads (no answer, NACK or reply) and watch the appliance. | No fault or changed operating state at the heat source over 30 min. How it answers is recorded as Unknown evidence today. | Not run |
| CL-08 Internet module coexistence | Vendor app access available to the operator. | Use the vendor app read-only; observe the module's bus traffic. | The module and its app keep working throughout. Once the gateway has identified the module (one identification and its serial windows), the module joins the root discovery candidates and receives the same B524 root probe as the other candidates: per minute until the absence state is `ABSENT`, then once per 10 min discovery tick; nothing else is addressed to it. | Not run |
| CL-09 Restart with cache | CL-01 to CL-06 done. | Restart the add-on once. | Phase passes through `CACHE_LOADED_STALE` to `LIVE_READY` (boiler-class) within 120 s; CL-03 and CL-06 hold again. The offline scenario does not cover a restart with a cache; this row is the first evidence. | Not run |
| CL-10 Home Assistant view | Integration configured against this gateway. | Read the integration's devices and entities. | Boiler-class heat source: boiler sensors present and plausible. Heat-pump-class heat source: no entity for the heat source (no boiler sensors), and the gateway's startup block shows reason `no_supported_heat_source_path` (GraphQL and MCP, as in CL-11). Either class: no climate or water-heater entity; the six energy sensors exist with state unknown, not 0 kWh. | Not run |
| CL-11 Heat-pump-class heat source | Heat-pump-class heat source; CL-01 and CL-04 running. | Read GraphQL `bus_summary { status { startup { phase reason } } }`, MCP `ebus_v1_bus_summary_get` and logs; observe the bus passively. | GraphQL and MCP show the same phase and reason `no_supported_heat_source_path` while no root and no boiler-class heat source exist. Recorded as evidence (Unknown until reviewed): the heat source's device identity family (exact identity in the private record); whether it answers the gateway's B524 root probe, and how (no answer, NACK, a reply; a coherent reply makes it the semantic root and clears the reason). Which documented heat-pump register ids it answers is not observed under this sheet: those reads need the operator's separate authorization of an allowlist of read-only requests. | Not run |
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

- Once the absence state is `ABSENT`, a controller that becomes reachable
  without passive observation of its traffic and without a registry change
  is found at the next discovery tick, within 10 min instead of one minute.
- With an explicitly configured source, a Vaillant identity the product
  catalog cannot classify (no readable serial, or a part number the catalog
  does not list) keeps root confirmation required, so the startup scan still
  reports admission failure on such an inventory.
- A heat-pump-class heat source yields no semantic stream and a `DEGRADED`
  phase; the startup reason `no_supported_heat_source_path` says that the
  gateway has no supported data path for it, not that reads fail. Publishing
  heat-pump values and refusing such a unit as semantic root wait for the
  observations of CL-11.

These are recorded for review; the session observes them and changes nothing.
