# Run sheet of the consolidated hardware session

Refs [gateway #1099](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1099)
and [gateway #551](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/551).
This page is the run sheet of the single operator session on the release
candidate in the `adaptermux-v2` form of the transport matrix. It extends the
[consolidated candidate handoff](consolidated-candidate-handoff.md) and the
[reduced run sheet for a site without a system controller](site-without-controller-run-sheet.md).
Nothing on this page was run. It records no result and grants no authority to
operate equipment: every step that changes anything names the operator's word,
given for that step.

Two site profiles are used, by opaque label only:

- **site A**: a heat source, a vendor system controller with zones and
  circuits, and the vendor's internet module;
- **site B**: no vendor system controller; a heat source, the vendor's internet
  module and a third-party room-control bridge that is the controller-class
  initiator on the bus.

Site names, addresses of the installation, serial numbers, device identities
and capture times are kept only in the operator's private record. Every row
starts **Not run**, evidence **Unknown**, qualification and admission
**Unassessed**; a row leaves Not run only with its own record.

The stop conditions and the revalidation after a fix are those of the handoff
([stop conditions and post-fix revalidation](consolidated-candidate-handoff.md#stop-conditions-and-post-fix-revalidation)).
After any stop, the as-found state is restored first and reported afterwards.

## Entry conditions

| ID | Condition | Who |
|---|---|---|
| E1 | The candidate is frozen and its private candidate record names every component and pin; the add-on contents equal that record. | orchestrator |
| E2 | Offline acceptance ([offline acceptance matrix](offline-acceptance-matrix.md), [gateway #1449](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1449)) is closed on that record, graph axis included. | orchestrator |
| E3 | The matrix tooling is on main and in the candidate: the `adaptermux-v2` suite of the [matrix runner](../../cmd/matrix-runner/README.md), its smoke command, the AD10 comparison and the add-on target commands. No cell awaits re-specification. | orchestrator |
| E4 | The private site profiles are filled in, the restore steps are written and dry-run, and the as-found snapshot of each site is taken at the start of its part. | orchestrator |
| E5 | The operator's word for the session at each site names every changing step: add-on installs and restarts, option changes, adapter host-link switches, the class W adapter actions (including `UDP-DS01` and `TCP-DS01`), the compatibility compositions (the proxy listener is gone while they run), the optional native point export switch, and separately each operator-only row of the last table. | operator |
| E6 | A fix during the session invalidates the affected results as the handoff states; nothing from the earlier candidate is carried forward silently. | orchestrator |

## Hardware setups

One setup serves one transport row of the
[`adaptermux-v2` matrix](../../matrix/M7-adaptermux-conformance-matrix.md). A
site runs the setups its adapter offers.

| Setup | Adapter host link | Gateway composition | Matrix row |
|---|---|---|---|
| network, enhanced framing | wired or wireless network | adapter-direct mux on a network endpoint | `ENH` |
| network, ENS selector | wired or wireless network | adapter-direct mux with the ENS selector | `ENS` |
| plain UDP | the adapter's plain UDP endpoint | gateway on `udp-plain`, no mux | `UDP-DS01` is executed. Every `UDP-*` grid cell stays planned and is written without execution |
| plain TCP | the adapter's plain TCP endpoint | gateway on `tcp-plain`, no mux | `TCP-DS01` is executed. Every `TCP-*` grid cell stays planned and is written without execution |
| USB serial | USB serial device path | not executed: the matrix defines no executable mux row for a serial host link | `UARTENS`, `UARTENH` and `UARTRAW`, written without execution; they stay `blocked-infra` |

`ENS` is ENS over the network and `ENH` is ENH over the network. A serial host
link stays on `UARTENS`, `UARTENH` and `UARTRAW`. Those rows stay
`blocked-infra` and are written without execution. The session issue does not
make a serial path an executable network row, and a `--hardware-setup` label
does not either: the operator does not file `ENS` or `ENH` results for a USB
serial path.

`UDP-DS01` and `TCP-DS01` are executable class W cells of the plain UDP and
plain TCP setups. They are not planned grid cells, and the operator does not
add them later by name. Each runs in the per-setup order of its row. The
operator types the case ID when the runner asks; without that answer the cell
fails and no command runs. The cell's pass criterion stays a smoke exit of 0,
which the runner records as outcome `pass`. Leaving either cell out of the
session report leaves an executable case id absent, so A-03 and B-01 are not
met.

Each setup has an opaque label passed as `--hardware-setup`; one report holds
one setup per row. Each site produces one report under its own session label
and the same candidate label; which report the gate reads is decided by the
[transport gate policy v2](transport-gate-policy-v2.md). The session report
is that aggregate, and its case-id set is the full inventory.

## Per-setup order

Each setup runs in this order. The runner flags, the smoke command and the
add-on target commands are those of the [matrix runner](../../cmd/matrix-runner/README.md).
Steps 3, 5 and 6 run on the `ENS` and `ENH` setups, where those cells are
executable. On the plain UDP setup the only executable cell is `UDP-DS01`,
and on the plain TCP setup it is `TCP-DS01`, both in step 4. Their grid
cells, including `AD10`, stay planned and are written in step 8 without
execution.

The ebusd add-on is the external client on the mux proxy listener. The
as-found snapshot, the health connection in A-01, the free-initiator check
in B-01 and the device option in B-02 all mean that add-on: its settings,
its connection and its `network_device` option. They do not mean the
gateway, the vendor internet module, the third-party bridge or the VRC
Explorer.

| Step | What | Notes |
|---|---|---|
| 1 | As-found snapshot | add-on options, gateway composition, adapter host link, the ebusd add-on's settings, including `network_device` |
| 2 | Baseline measurement | ten minutes of read-only observation: the gateway's counters, the passive frame list, the startup phase |
| 3 | Smoke-checked cells | `AD01`, `AD02`, `AD03`, `AD06`, `AD07`, `PX01` to `PX04`, `PX12`, and `CX01` with the smoke command and the default external client `ebusd`. That `CX01` verdict stays in the session report. Step 7 does not run `CX01` again in that output directory. `AD10` waits for step 6 |
| 4 | Class W cells | `AD04` (adapter reset) and `AD09` (host-link disconnect and reconnect) on the `ENS` and `ENH` setups, and `UDP-DS01` and `TCP-DS01` on the plain UDP and plain TCP setups. When the runner names the case and asks for its ID, the operator types it; without that answer the cell fails and no command runs. That answer only lets the cell start: for `AD04` and `AD09` it is not the moment of the adapter action, and the operator does not touch the adapter then. The operator performs the adapter action only after the smoke command has taken its first reading, announced the action and the operator has typed the case ID again at that cue; the action then falls inside the case window. When the smoke command ends before that cue, for example with status 4 because a series it reads is missing, no cue is given and no adapter action is performed: the adapter is not reset and its host link is not touched, and the cell keeps the runner's `fail`. `UDP-DS01` and `TCP-DS01` have no adapter action from the smoke command and pass only when it exits 0 |
| 5 | Compatibility compositions | `ENS-DS01` on the ENS setup; `ENH-DS01` and `EBUSD-ES01` on the ENH setup. `UDP-DS01` and `TCP-DS01` are the class W cells of their own setups in step 4, not cells of this step. The add-on is reconfigured to the non-mux composition on the operator's word; the ebusd add-on has no proxy listener while these cells run. At site A, on the ENH setup only, VB-L3 and VB-L4 are performed in this step after `EBUSD-ES01`, while the ebusd-tcp composition is still the running composition, and before step 6. On that setup they are recorded, or marked Not run for the reason in the B503 section, before the composition is left. They are not deferred to A-10, and that composition is not opened again for them. Other setups do not run them and do not wait for them |
| 6 | Return to the mux composition | `ENS` and `ENH` only: read back the options and the transport class, then `AD10` at once against the paired compatibility records from step 5. No measurement interval is inserted before `AD10`: the comparison includes live on/off and temperature fields, and a multi-minute gap is not part of the step. On the plain UDP and plain TCP setups `AD10` is planned and is not run |
| 7 | Explorer certification run | site A only, for the `ENS` and `ENH` setups whose `CX01` cells were smoke-checked in step 3. One `CX01` cell per transport stores the invocation's external client, and selecting that cell again in the session report's output directory replaces its case directory. Step 7 is a separate invocation with `--external-client vrc-explorer` through the guarded relay, written to its own output directory. That invocation does not use the session report's output directory, so the smoke-checked `CX01` verdict from step 3 stays the session report's `CX01` |
| 8 | Reserved cells | the planned and `blocked-infra` cells of the row are written without execution. This does not include `UDP-DS01` or `TCP-DS01` |
| 9 | Restore | the as-found snapshot of step 1 is restored and read back; final measurement |

The runner records every non-zero smoke status as a failed step and stores
that step as outcome `fail`. Smoke status 4 means a series the check reads
is missing. `AD04`, `AD07` and `AD09` end with that status while the gateway
does not publish those series, and the recorded outcome of each stays
`fail`. That outcome is not a pass, and it does not satisfy A-02, A-03 or
B-01. A-02 is judged on the executable cells of the `ENS` setup alone, the
first setup site A runs. A-03 and B-01 are met only when, after every setup
of the site has run, every executable cell in the session report has runner
outcome `pass`. An executable cell that was not run is absent, and an absent
executable cell fails the row as well.

## Site A rows (vendor system controller)

| ID | Precondition | Operation | Pass criterion | Result |
|---|---|---|---|---|
| A-01 Candidate install and health | E1 to E5 | install the candidate on the operator's word; read health and the startup phase | health passes; startup phase `LIVE_READY`; the ebusd add-on is connected to the mux proxy listener; no out-of-step hold reported | Not run |
| A-02 Matrix, `ENS` setup | A-01 | the per-setup order on the `ENS` setup | every executable cell of that setup has runner outcome `pass`: the `ENS` mux cells, including `AD04`, `AD07` and `AD09`, and `ENS-DS01`. Smoke status 4 stays outcome `fail` and does not satisfy this row. An executable cell of that setup absent from the session report fails the row. Class W confirmation is required and does not by itself pass the cell. Cells of later setups are not judged here | Not run |
| A-03 Matrix, remaining setups and session report | A-02; before each setup the operator switches the adapter or its host link to it, on the operator's word | the per-setup order on the `ENH` setup, then the plain UDP setup, then the plain TCP setup, then the serial rows written without execution | every executable cell in the session report has runner outcome `pass`, including the `ENS` cells of A-02, the `ENH` mux cells with `AD04`, `AD07` and `AD09`, `ENH-DS01`, `EBUSD-ES01`, `UDP-DS01` and `TCP-DS01`. Smoke status 4 stays outcome `fail` and does not satisfy this row. An executable cell absent from the session report fails the row | Not run |
| A-04 Explorer certification | A-02 or A-03 | step 7 | the scan completes with no guard refusal. The separate record names `vrc-explorer` as the northbound client. That record is not the session report's `CX01` verdict, which remains the smoke-checked ebusd result from step 3. An Explorer failure does not replace that verdict and is not the `CX01` outcome A-02 or A-03 reads | Not run |
| A-05 Identification exchange record (operator) | the gateway has run its startup scan and at least one revalidation cycle | [identification exchange record](#identification-exchange-record) | both exchanges recorded and handed over, complete on the retained request and response bytes; acknowledge and checksum bytes are not required | Not run |
| A-06 Graph axis, thermal | the family is in the graph in the candidate | [graph-axis checks](#graph-axis-checks), thermal block | every row of that block | Not run |
| A-07 Native point export | the candidate carries the export | [native point export checks](#native-point-export-checks) | every row | Not run |
| A-08 Home Assistant surface of the one write (operator, optional) | the integration matches the candidate, and the operator names Home Assistant as the surface of A-09 | no setpoint change of its own. The only optional live setpoint change is A-09; this row does not add one. When that one change uses Home Assistant, the write, the return and any doubt are the [one live setpoint write](#one-live-setpoint-write) | Not run unless A-09 is that one change on this surface; then A-09's criterion, including a hand restore and no further write on any doubtful readback or return | Not run |
| A-09 One live setpoint write (operator, optional) | the operator present at the controller | [one live setpoint write](#one-live-setpoint-write). This is the only optional live setpoint change on the sheet | every step of that section. A doubtful readback or return stops for a hand restore and no further write | Not run |
| A-10 B503 live re-verification | boiler-class heat source; the operator's word | [B503 rows](#b503-live-re-verification): VB-L1 and VB-L2 while the mux composition is running. VB-L3 and VB-L4 are taken from the ENH step 5 record; A-10 does not open the ebusd-tcp composition | VB-L1 and VB-L2. VB-L3 and VB-L4 are the records made while ebusd-tcp was still the running composition, or Not run with the B503 reason when that setup does not apply | Not run |
| A-11 Retained rows and consumers | the operator reviews each criterion first | the handoff's retained and consumer rows | as the handoff defines | Not run |
| A-12 Restore and exit | all rows done or stopped | restore, final measurement, the operator looks at the appliance and the controller | as found, or the candidate left installed on the operator's word | Not run |

## Site B rows (third-party controller bridge)

The reduced run sheet assumes that no device answers at the controller
addresses. At this site a third-party bridge takes the controller's place on
the bus: it arbitrates as an initiator, addresses the heat source, and may or
may not answer identification or the gateway's root probe. Rows `CL-01` to
`CL-11` of the reduced sheet apply with the changes of its
[third-party controller bridge section](site-without-controller-run-sheet.md#variant-third-party-controller-bridge).
A difference from the reduced sheet's expectation is a **finding**; it is
recorded and never adjusted away.

| ID | Precondition | Operation | Pass criterion | Result |
|---|---|---|---|---|
| B-01 Matrix, each setup | the reduced sheet's `CL-01` passed | the per-setup order with the heat source as workload target and a free initiator address used by neither the bridge, the ebusd add-on nor the gateway | as A-03, after every setup of the site has run: every executable cell in the session report has runner outcome `pass`. Smoke status 4 stays `fail`, and an absent `UDP-DS01` or `TCP-DS01` does not satisfy this row | Not run |
| B-02 ebusd add-on | the ebusd add-on's as-found `network_device` option, read in step 1 | if that option does not name the adapter of the session, the operator decides to connect the ebusd add-on for the session only, restored afterwards, or to keep the as-found value | the operator's decision recorded | Not run |
| B-03 Bridge variant rows | B-01 running | the bridge variant rows of the reduced sheet | as that section defines | Not run |
| B-04 Graph axis, reduced block | the family is in the graph in the candidate | [graph-axis checks](#graph-axis-checks), reduced block | every row of that block | Not run |
| B-05 Native point export | the candidate carries the export | [native point export checks](#native-point-export-checks) | heat-source points only; no controller-class point | Not run |
| B-06 Restore and exit | all rows done or stopped | restore, final measurement, the operator looks at the appliance and the room-control app | as found, or the candidate left installed on the operator's word | Not run |

Not at site B: A-04 (the Explorer scans a vendor controller), A-05 (the
qualification record is taken where a vendor controller exists), A-08 and A-09
(no vendor controller to write to; the bridge is never written to). Site B has
no live setpoint change.

## Graph-axis checks

Read-only, except the one R-06 refusal invoke below, which is not a setpoint
write and must not reach the bus. Each check is bound, before the session, to
the published retrieval pages of the plane and lens it reads: the
[graph contract](https://github.com/Project-Helianthus/helianthus-docs-semantic/blob/main/api/v1/graph.md)
and the gateway's [family graph state](../architecture/semantic-family-graph-state.md).
A check whose query is not bound to a published page, or whose family is not
in the graph in the candidate, stays **Not run** with that reason.

| ID | Check | Pass criterion | Result |
|---|---|---|---|
| R-01 Thermal chain (site A) | read the thermal plane through GraphQL, the semantic MCP tools and the Portal navigation | the same resources on all three: the controller, its zones, the circuits serving each zone, the heat source; every relation instance has both endpoints present; the operator confirms the zone and circuit counts | Not run |
| R-02 One identity per device | compare resource identities across planes | one physical device is one resource in every plane it appears in; no device appears under a second identity | Not run |
| R-03 Nothing stale after a restart | restart the add-on once on the operator's word; read again after `LIVE_READY` | identities and relation instances unchanged; no relation refers to an absent resource; nothing read before the restart is presented as current | Not run |
| R-04 Memberships | for each published resource | its plane memberships equal the element catalog for its kind; no resource without a membership | Not run |
| R-05 Energy chain (where the equipment is) | inverter, storage and meter; charging equipment and its circuit | as R-01 for those kinds; a chain whose attribution the contract withholds is absent with its stated reason | Not run |
| R-06 Visibility is not authority | separate from the one live setpoint write; the operator's word for this one invoke | one invoke names a write on a visible resource that has no admitted operation. It is not a setpoint change, it is not sent from the one-write steps, and it is not a second live setpoint change. The invoke is refused with the documented error and nothing reaches the bus. It is not followed by another write. When A-09 was performed, that admitted operation is the only one accepted. If the refusal is doubtful or anything reaches the bus: stop, hand restore if the controller changed, and send no further write | Not run |
| R-07 Reduced block (site B) | the thermal plane on a bus without a vendor controller | the heat-source resource only; no zone, circuit or controller resource; the bridge is not a semantic resource | Not run |

## Identification exchange record

Site A, operator, read-only. Purpose: the raw bytes of the directed
identification exchange (service `07 04`) of the system controller and of the
heat source, which the documentation's qualification rule needs. No scan is
triggered: the gateway sends these requests in its startup scan and its
revalidation cycle and retains each exchange.

1. The operator reads the retained exchanges with the MCP tool
   `ebus_v1_bus_identification_evidence_list` (one read request).
2. The operator saves the answer privately, unedited, with the site label and
   the candidate label.
3. The operator marks which address is the controller and which the heat
   source, and judges each exchange complete only from the request and
   response bytes this read returns (`request_hex` and `response_hex`). The
   tool retains those two fields. Acknowledge and checksum bytes are not
   retained, and this confirmation does not require them.
4. Sanitizing happens afterwards, off site, under the publication rules: the
   serial and every site-specific field are removed before anything is
   published. The operator approves the sanitized record before publication.

If one of the two exchanges is missing, wait for the next revalidation cycle;
do not trigger a scan by other means. Stop if the answer holds an exchange
the operator does not recognise.

## Native point export checks

These rows run only when the candidate carries the native point export and
the add-on its option; otherwise each stays **Not run** with the reason "the
candidate carries no native point export". The operator agrees to switch the
option on (one add-on restart) and off again at the end unless they keep it.

| ID | Check | Pass criterion | Result |
|---|---|---|---|
| M-01 Off by default | scrape before the option is switched on | no native point series | Not run |
| M-02 Values present | scrape after two poll cycles | the points the export documents for the site's equipment are present, under the device class of their source | Not run |
| M-03 Equal to the planes | compare with the GraphQL plane read at the same moment and, at site A, with the controller display | equal after unit conversion; the operator confirms two values on the display | Not run |
| M-04 Nothing private | read every native series | no code, boolean, enumeration, name, address or serial appears as a value or as a label | Not run |
| M-05 A scrape sends nothing | ten scrapes between two poll cycles | the gateway's own frame counters do not change across them | Not run |
| M-06 Restart | restart the add-on | no native value before the first observation; label sets unchanged afterwards | Not run |
| M-07 Heat source only (site B) | scrape at site B | heat-source points only; no controller-class point | Not run |

## One live setpoint write

Optional, site A only, and only on the operator's word at that moment with the
operator at or in sight of the controller. The sheet directs one optional live
setpoint change: one zone, one write, one restore. Home Assistant (A-08) is
only a surface for that same change. It is not a second change. R-06 is not a
step of this section and nothing in these steps sends its invoke.

1. The operator reads the present setpoint of one zone on the controller; the
   same value is read through GraphQL. They must agree. If they do not, stop
   and send no write.
2. The operator names the new value (one step from the present one, inside the
   controller's limits) and says go.
3. Exactly one semantic operation is sent for that zone and value, on the one
   surface the operator named. Nothing else is sent in this procedure.
4. The gateway reports the readback value; the operator confirms it on the
   controller. A doubtful readback (none, a different value, or an error on
   the controller) stops the row here: the operator restores the value at the
   controller by hand, the row is recorded as failed with what was seen, and
   no further write is sent. The restore in the next step is not attempted.
5. Restore, only when step 4 agreed: one operation back to the original value,
   readback, and the operator confirms that the controller shows it. A
   doubtful return stops the same way: hand restore, the row failed, and no
   further write.
6. On any other doubt: stop; the operator restores the value at the controller
   by hand if it is not already the original value, and the row is recorded as
   failed with what was seen. No further write is sent.

Never, from this procedure: a second zone, a second setpoint write, a mode
change, a hot-water write, a schedule write, a write at site B, a write
without the operator present, or a Home Assistant change in addition to this
one. The R-06 refusal invoke is not part of this procedure. It is authorized
only in the graph-axis checks, it is not a setpoint change, and it must not
reach the bus.

## B503 live re-verification

The rows of [M6a-vaillant-b503.md](../../matrix/M6a-vaillant-b503.md) marked
`PASS (stub)` need live re-verification on a boiler-class heat source
([gateway #551](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/551)).
Those rows are VB-01 to VB-04, VB-19 and VB-20, recorded here as VB-L1, VB-L3
and VB-L4. VB-05 is marked `PASS`, not `PASS (stub)`; VB-L2 is that live
monitor lifecycle on the mux composition and is not a stub row. These rows run
only when the candidate's B503 reads reach the bus (not the stub dispatcher);
otherwise they stay **Not run** with that reason. The record is kept
privately; a row of that page changes only by that page's own procedure.

VB-L3 and VB-L4 are performed in step 5 of the ENH setup, after `EBUSD-ES01`,
while the ebusd-tcp composition is still the running composition, and before
the return to the mux composition. If that setup is not run, VB-L3 and VB-L4
stay **Not run** with that reason. A-10 does not start them and does not open
the ebusd-tcp composition for them.

| ID | Matrix row | Composition | Operation | Pass criterion | Result |
|---|---|---|---|---|---|
| VB-L1 | VB-01 to VB-04 (`PASS (stub)`) | adapter-direct mux, from A-10 while that composition is running | the MCP read tools for current errors, error history, current service messages and service history, once each | request and response recorded; the decode matches the selector catalog; no write frame from the gateway | Not run |
| VB-L2 | VB-05 (`PASS`, not a stub row) | adapter-direct mux, from A-10 while that composition is running | live monitor enable, read, disable, on the operator's word at that moment | each step answered; the session is released at disable | Not run |
| VB-L3 | VB-19 (`PASS (stub)`) | ebusd-tcp inside step 5, after `EBUSD-ES01`, while that composition is still running | the current-errors read tool once | as VB-L1 | Not run |
| VB-L4 | VB-20 (`PASS (stub)`) | ebusd-tcp inside step 5, after `EBUSD-ES01`, while that composition is still running | as VB-L2, on the operator's word at that moment | as VB-L2 | Not run |

## The operator's part

| When | What |
|---|---|
| Before each site's part | the word for the session scope (E5) |
| At the start, after each host-link switch and at the end | a look at the appliance and the controller or the room-control app |
| Class W cells, once per setup | type the case ID when the runner asks; this does not start the adapter action. For `AD04` and `AD09`, reset the adapter (`AD04`) or disconnect and reconnect its host link (`AD09`) only after the smoke command announces the action and the case ID is typed again at that cue. When the smoke command ends without that cue, for example with status 4, perform no adapter action. On the plain UDP and plain TCP setups, type the case ID for `UDP-DS01` or `TCP-DS01`; those cells pass only on a smoke exit of 0 |
| Compatibility compositions | the word for reconfiguring the add-on and for the proxy listener's absence. At site A, VB-L3 and the word for VB-L4 while the ebusd-tcp composition of step 5 is still running |
| Site A | read and save the identification exchange record, complete on the retained request and response bytes |
| Site A | two display values for M-03; the zone and circuit counts for R-01; the word for the one R-06 refusal invoke, which is not a setpoint write |
| Site A, optional | the one live setpoint change (A-09), including when Home Assistant is its surface (A-08). Not a second change. A doubtful readback or return: hand restore, and no further write |
| Site A, B503 rows | the word for VB-L2 while the mux composition is running. The word for VB-L4 is given in step 5, not at A-10 |
| Site B | the decision on the ebusd add-on's `network_device` option (B-02) |
| End of each site | confirm the as-found state, or decide that the candidate stays installed |
