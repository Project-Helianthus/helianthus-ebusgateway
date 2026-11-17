# Run sheet of the consolidated hardware session

Refs [gateway #1099](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1099)
and [gateway #551](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/551).
This page is the run sheet of the single operator session on the release
candidate in the `adaptermux-v2` form of the transport matrix, with the last
run of the `full88` form on the same setups. It extends the
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
| E5 | The operator's word for the session at each site names every changing step: add-on installs and restarts, option changes, adapter host-link switches, the class W adapter actions (including `UDP-DS01` and `TCP-DS01`), the compatibility compositions (the proxy listener is gone while they run), the optional native point export switch, VB-L2 while adapter-direct mux is still the running composition at the end of step 4 on the `ENS` setup, the composition of the one `T13` or `T14` case run immediately after `AD10`, the return to adapter-direct mux after that case and before the explorer run, the compositions of the other `full88` cases of each setup, and separately each operator-only row of the last table. | operator |
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
is that aggregate, and its case-id set is the full inventory. That aggregate
is the `adaptermux-v2` report: those invocations share one output directory
and the runner rebuilds `index.json` from the case directories left there.
The `full88` runner does not rebuild its index that way. Its one report is
the assembled index in the [last `full88` report](#last-full88-report).

## Per-setup order

Each setup runs in this order. The runner flags, the smoke command and the
add-on target commands are those of the [matrix runner](../../cmd/matrix-runner/README.md).
Steps 3, 5, 6 and 10 run on the `ENS` and `ENH` setups, where those cells
are executable. Step 9 runs on the `ENS`, `ENH`, plain UDP and plain TCP
setups, each with only the cases that belong to that setup, and not on the
serial rows. On the plain UDP setup the only executable cell is `UDP-DS01`,
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
| 4 | Class W cells | `AD04` (adapter reset) and `AD09` (host-link disconnect and reconnect) on the `ENS` and `ENH` setups, and `UDP-DS01` and `TCP-DS01` on the plain UDP and plain TCP setups. When the runner names the case and asks for its ID, the operator types it; without that answer the cell fails and no command runs. That answer only lets the cell start: for `AD04` and `AD09` it is not the moment of the adapter action, and the operator does not touch the adapter then. The operator performs the adapter action only after the smoke command has taken its first reading, announced the action and the operator has typed the case ID again at that cue; the action then falls inside the case window. When the smoke command ends before that cue, for example with status 4 because a series it reads is missing, no cue is given and no adapter action is performed: the adapter is not reset and its host link is not touched, and the cell keeps the runner's `fail`. `UDP-DS01` and `TCP-DS01` have no adapter action from the smoke command and pass only when it exits 0. At site A, on the `ENS` setup only, after those class W cells and before step 5 reconfigures the add-on, VB-L1 and VB-L2 run while adapter-direct mux is still the running composition. No composition change is made for them. If that composition is no longer the running one, they stay Not run with that reason and are not attempted on another composition. On that setup they are recorded, or marked Not run for the reason in the B503 section, before the composition is left. They are not deferred to A-10. A-10 does not run them and does not switch the gateway back to the mux composition for them. Other setups do not run them |
| 5 | Compatibility compositions | `ENS-DS01` on the ENS setup; `ENH-DS01` and `EBUSD-ES01` on the ENH setup. `UDP-DS01` and `TCP-DS01` are the class W cells of their own setups in step 4, not cells of this step. The add-on is reconfigured to the non-mux composition on the operator's word; the ebusd add-on has no proxy listener while these cells run. At site A, on the ENH setup only, VB-L3 and VB-L4 are performed in this step after `EBUSD-ES01`, while the ebusd-tcp composition is still the running composition, and before step 6. On that setup they are recorded, or marked Not run for the reason in the B503 section, before the composition is left. They are not deferred to A-10, and that composition is not opened again for them. Other setups do not run them and do not wait for them |
| 6 | Return to the mux composition | `ENS` and `ENH` only: read back the options and the transport class, then `AD10` at once against the paired compatibility records from step 5. No measurement interval is inserted before `AD10`: the comparison includes live on/off and temperature fields, and a multi-minute gap is not part of the step. The next invocation on the `ENS` setup is `T13` alone, and on the `ENH` setup it is `T14` alone, each into that setup's `full88` output directory, after the operator's word for that case's composition. No explorer run, no reserved cell and no other `full88` case is inserted between the `AD10` semantic record and that case's semantic record. Step 9 does not select that case again, so it does not replace the case directory. On a site that runs step 7, after that invocation the operator's word returns the gateway to adapter-direct mux before step 7, so the explorer run is on that composition. A site that does not run step 7 does not make that return for the explorer. On the plain UDP and plain TCP setups `AD10` is planned and is not run, and there is no such invocation |
| 7 | Explorer certification run | site A only, for the `ENS` and `ENH` setups whose `CX01` cells were smoke-checked in step 3, and only after step 6 has returned the gateway to adapter-direct mux. One `CX01` cell per transport stores the invocation's external client, and selecting that cell again in the session report's output directory replaces its case directory. Step 7 is a separate invocation with `--external-client vrc-explorer` through the guarded relay, written to its own output directory. That invocation does not use the session report's output directory, so the smoke-checked `CX01` verdict from step 3 stays the session report's `CX01`. The invocation meets the coexistence cell's pass criterion only when its runner outcome is `pass`: the gateway smoke command and the client workload both pass, with no corrupted frame attributable to the mux and no starvation of either side. A finished scan with no guard refusal does not meet that criterion while the smoke command fails |
| 8 | Reserved cells | the planned and `blocked-infra` cells of the row are written without execution. This does not include `UDP-DS01` or `TCP-DS01` |
| 9 | Last `full88` run | the [last `full88` run](#last-full88-report) of the cases that belong to this setup. On `ENS` the case list does not include `T13`, and on `ENH` it does not include `T14`: those two cases are only the invocation immediately after `AD10`, and a later run would replace that case directory. Each composition is set on the operator's word. The invocation uses this setup's own output directory. On `ENS` and `ENH` that is the same directory as the adjacent `T13` or `T14` case. It is not another setup's directory or the aggregate directory. It runs before any restore of this setup; step 11 does not start until it has ended or stopped. The serial rows have no case of this step |
| 10 | `AD10` proxy-side baseline | `ENS` and `ENH` only, from records: the [proxy-side baseline comparison](#ad10-proxy-side-baseline) of the `T13` record written immediately after `ENS-AD10`, or the `T14` record written immediately after `ENH-AD10`, with the `AD10` record of step 6. The case is read from that setup's `full88` output directory. No bus action. On the plain UDP and plain TCP setups it is not done, because `UDP-AD10` and `TCP-AD10` stay planned |
| 11 | Restore | only after steps 9 and 10 of this setup, the as-found snapshot of step 1 is restored and read back; final measurement |

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

## Last `full88` report

This session is the last run of the old form and the first run of the new
one. The [transport gate policy v2](transport-gate-policy-v2.md#1-when-this-policy-takes-force)
takes force only when the session has produced both reports in the same
hardware session, against the same candidate and the same per-transport
setups: the last `full88` report (`T01` to `T88`) and the first
`adaptermux-v2` report. Until then the 88-case gate stays in force. A session
that ends without the `full88` report gives no evidence for the switch.

- The `full88` suite (the runner's default) runs the cases that belong to
  each of the `ENS`, `ENH`, plain UDP and plain TCP setups. A case belongs
  to the setup of the transport that faces the adapter: the gateway
  transport of a direct-adapter case, the ebusd transport of a via-ebusd-tcp
  case, and the proxy transport of a proxy case. The cases of each setup are
  listed below. A case is not run on any other setup. The serial rows have
  none. `T13` belongs to `ENS` and `T14` belongs to `ENH`.
- On `ENS` and `ENH`, the one case `T13` or `T14` is the invocation
  immediately after step 6, into that setup's own output directory. Step 9
  of the same setup then runs the remaining cases of its list, with the
  runner's case list naming those remaining cases and not `T13` or `T14`,
  into that same directory. On plain UDP and plain TCP, step 9 runs every
  case of the list into that setup's own directory. Step 9 runs after step
  8 and before the restore in step 11. The adjacent `T13` or `T14`
  invocation also runs before that restore. The as-found snapshot of step 1
  is therefore restored only after both forms have run on that setup, and
  the setup rows (A-02, A-03, B-01) include this step. A setup never writes
  into another setup's directory or into the aggregate directory.
- The runner writes `index.json` from the cases of that invocation only. It
  does not rebuild the index from case directories left by an earlier
  invocation. A later invocation replaces `index.json` and replaces the case
  directory of every case it selects. The per-setup `index.json` is therefore
  not the report A-13 or B-07 reads.
- After the last of those setups has finished step 9, and before A-13 or
  B-07 is judged, the operator assembles one directory that is not a setup
  directory and not the `adaptermux-v2` directory. For each case `T01` to
  `T88`, that case's directory is copied from the output directory of the
  setup the case belongs to, once. `T13` is the directory written
  immediately after `ENS-AD10`, and `T14` is the directory written
  immediately after `ENH-AD10`. The assembled `index.json` has `suite`
  `full88`, the same `target` as those invocations, and `cases` equal to the
  `verdict.json` of each copied directory, from `T01` to `T88`, with no case
  missing and no case repeated. The runner is not started against that
  directory. A-13 and B-07 read this index and do not run `full88` again
  after a restore. A missing case, or a case copied from any other setup,
  fails the row.
- Its cases, proxy adjuncts and expected-failure inventory are those of the
  handoff's [transport step](consolidated-candidate-handoff.md#one-operator-session)
  and the [smoke matrix](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/development/smoke-matrix.md#matrix-coverage);
  this page does not restate them. Each composition its cases need is set on
  the operator's word (E5).
- Each setup directory, and the assembled directory, is never that of the
  `adaptermux-v2` session report. The `full88` cases do not replace, and are
  not counted as, any cell of the `adaptermux-v2` report, and no
  `adaptermux-v2` cell is counted in them.
- The operator's private record names the same session label, candidate label
  and hardware setup labels for both reports, and names each setup directory
  and the aggregate directory.

| Setup | Cases that belong to it |
|---|---|
| `ENS` | `T01`, `T05`, `T09`, `T13`, `T17`, `T21`, `T25` to `T28`, `T41` to `T44`, `T57` to `T60`, `T73` to `T76` |
| `ENH` | `T02`, `T06`, `T10`, `T14`, `T18`, `T22`, `T29` to `T32`, `T45` to `T48`, `T61` to `T64`, `T77` to `T80` |
| plain UDP | `T03`, `T07`, `T11`, `T15`, `T19`, `T23`, `T33` to `T36`, `T49` to `T52`, `T65` to `T68`, `T81` to `T84` |
| plain TCP | `T04`, `T08`, `T12`, `T16`, `T20`, `T24`, `T37` to `T40`, `T53` to `T56`, `T69` to `T72`, `T85` to `T88` |

### `AD10` proxy-side baseline

In this session `T13` (gateway to proxy over ENH, proxy to the adapter over
ENS) and `T14` (the same, proxy to the adapter over ENH) are the additional
replaced side of `ENS-AD10` and `ENH-AD10`, as the
[matrix page](../../matrix/M7-adaptermux-conformance-matrix.md#6-index-completion-ad10-ad11-ad12)
and the [transport gate policy v2](transport-gate-policy-v2.md#5-the-transition-session)
state. The runner pairs `AD10` only with `S-DS01` and, on ENH, `EBUSD-ES01`,
so this comparison is a separate record, made in step 10 of the `ENS` and
`ENH` setups:

- the record of `T13`, written on `ENS` immediately after the step 6 `AD10`
  record, or of `T14`, written on `ENH` the same way, is compared with the
  mux record of that setup's `AD10` cell from step 6, with the record design
  `development/ad10-migration-equivalence.md` of `helianthus-docs-ebus`:
  presence, types, units, values, lineage and MCP/GraphQL parity as separate
  decisions; equal values with another lineage are not equivalent;
- the two semantic records are not separated by the explorer run, by
  reserved-cell processing, or by any other `full88` case. The comparison
  includes live on/off and temperature fields, so that gap is not part of
  it. Step 9 does not select `T13` or `T14` again, and no other setup runs
  those cases, so the case directory step 10 reads is still that adjacent
  run;
- the comparison does not change the runner outcome of the `AD10` cell and is
  not counted as a cell of either report;
- when the `T13` or `T14` run did not pass, or holds no record that the
  record design can compare, the baseline of that setup is recorded as **not
  done** with that reason. That is a finding of the session; it is not
  replaced by the `S-DS01` or `EBUSD-ES01` pair;
- `T15` and `T16` have no executed `AD10` cell in this session, because
  `UDP-AD10` and `TCP-AD10` stay planned; their baseline is recorded as **not
  done** with that reason.

## Site A rows (vendor system controller)

| ID | Precondition | Operation | Pass criterion | Result |
|---|---|---|---|---|
| A-01 Candidate install and health | E1 to E5 | install the candidate on the operator's word; read health and the startup phase | health passes; startup phase `LIVE_READY`; the ebusd add-on is connected to the mux proxy listener; no out-of-step hold reported | Not run |
| A-02 Matrix, `ENS` setup | A-01 | the per-setup order on the `ENS` setup | every executable cell of that setup has runner outcome `pass`: the `ENS` mux cells, including `AD04`, `AD07` and `AD09`, and `ENS-DS01`. Smoke status 4 stays outcome `fail` and does not satisfy this row. An executable cell of that setup absent from the session report fails the row. Class W confirmation is required and does not by itself pass the cell. Cells of later setups are not judged here | Not run |
| A-03 Matrix, remaining setups and session report | A-02; before each setup the operator switches the adapter or its host link to it, on the operator's word | the per-setup order on the `ENH` setup, then the plain UDP setup, then the plain TCP setup, then the serial rows written without execution | every executable cell in the session report has runner outcome `pass`, including the `ENS` cells of A-02, the `ENH` mux cells with `AD04`, `AD07` and `AD09`, `ENH-DS01`, `EBUSD-ES01`, `UDP-DS01` and `TCP-DS01`. Smoke status 4 stays outcome `fail` and does not satisfy this row. An executable cell absent from the session report fails the row | Not run |
| A-04 Explorer certification | A-02 or A-03 | step 7 | the explorer invocation's runner outcome is `pass`. That is the coexistence cell's pass criterion: the gateway smoke command and the client workload both pass, with no corrupted frame attributable to the mux and no starvation of either side. A finished scan with no guard refusal does not pass this row while the smoke command fails. The separate record names `vrc-explorer` as the northbound client. That record is not the session report's `CX01` verdict, which remains the smoke-checked ebusd result from step 3. An Explorer failure does not replace that verdict and is not the `CX01` outcome A-02 or A-03 reads | Not run |
| A-05 Identification exchange record (operator) | the gateway has run its startup scan and at least one revalidation cycle | [identification exchange record](#identification-exchange-record) | both exchanges recorded and handed over, complete on the retained request and response bytes; acknowledge and checksum bytes are not required | Not run |
| A-06 Graph axis, thermal | the family is in the graph in the candidate | [graph-axis checks](#graph-axis-checks), thermal block | every row of that block | Not run |
| A-07 Native point export | the candidate carries the export | [native point export checks](#native-point-export-checks) | every row | Not run |
| A-08 Home Assistant surface of the one write (operator, optional) | the integration matches the candidate, and the operator names Home Assistant as the surface of A-09 | no setpoint change of its own. The only optional live setpoint change is A-09; this row does not add one. When that one change uses Home Assistant, the write, the return and any doubt are the [one live setpoint write](#one-live-setpoint-write) | Not run unless A-09 is that one change on this surface; then A-09's criterion, including a hand restore and no further write on any doubtful readback or return | Not run |
| A-09 One live setpoint write (operator, optional) | the operator present at the controller | [one live setpoint write](#one-live-setpoint-write). This is the only optional live setpoint change on the sheet | every step of that section. A doubtful readback or return stops for a hand restore and no further write | Not run |
| A-10 B503 live re-verification | boiler-class heat source; the operator's word | [B503 rows](#b503-live-re-verification): read the records. VB-L1 and VB-L2 were taken at the end of step 4 on the `ENS` setup, while adapter-direct mux was still the running composition. VB-L3 and VB-L4 were taken from the ENH step 5 record. A-10 does not run the rows again, does not switch back to the mux composition, and does not open the ebusd-tcp composition | VB-L1 and VB-L2 are the records made while adapter-direct mux was still the running composition, or Not run with the B503 reason when that setup does not apply. VB-L3 and VB-L4 are the records made while ebusd-tcp was still the running composition, or Not run with the B503 reason when that setup does not apply | Not run |
| A-11 Retained rows and consumers | the operator reviews each criterion first | the handoff's retained and consumer rows | as the handoff defines | Not run |
| A-13 Last `full88` report | A-02 and A-03, whose per-setup order ran step 9 on each of the `ENS`, `ENH`, plain UDP and plain TCP setups before its restore in step 11 | assemble and read the [last `full88` report](#last-full88-report) from those case directories, including the `T13` and `T14` directories written beside `AD10`; nothing is run again after a restore | one assembled `full88` index covering every case `T01` to `T88`, each the verdict copied from the setup that case belongs to, on the same session, candidate and setups as the `adaptermux-v2` session report, classified by the current transport gate. A missing case, a case taken from another setup, or a missing report fails the row. A per-setup `index.json` is not this report | Not run |
| A-14 `AD10` proxy-side baseline | A-02 and A-03, whose per-setup order ran step 10 on the `ENS` and `ENH` setups | read the [proxy-side baseline](#ad10-proxy-side-baseline) records of step 10 | the `T13` comparison with `ENS-AD10` and the `T14` comparison with `ENH-AD10` are recorded and equivalent on every decision, including live on/off and temperature fields. The `T13` and `T14` records are the ones taken immediately after that setup's `AD10`, not a later case and not a case from another setup. A baseline recorded as not done, or a decision that is not equivalent, fails the row and is a finding. A difference is not excused by the explorer run or by the other cases of step 9, because those runs are not between the two records. `T15` and `T16` are recorded as not done because `UDP-AD10` and `TCP-AD10` stay planned, and that does not fail the row | Not run |
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
| B-07 Last `full88` report | B-01, whose per-setup order ran step 9 on each of the `ENS`, `ENH`, plain UDP and plain TCP setups before its restore in step 11 | assemble and read the [last `full88` report](#last-full88-report), as A-13, from the case directories of B-01, run with the heat source as workload target; nothing is run again after a restore | as A-13 | Not run |
| B-08 `AD10` proxy-side baseline | B-01, whose per-setup order ran step 10 on the `ENS` and `ENH` setups | read the [proxy-side baseline](#ad10-proxy-side-baseline) records of step 10 | as A-14 | Not run |
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

VB-L1 and VB-L2 are performed at the end of step 4 of the `ENS` setup, after
the class W cells, while adapter-direct mux is still the running composition,
and before step 5 reconfigures the add-on. No composition change is made for
them. If that composition is no longer running, or that setup is not run, they
stay **Not run** with that reason and are not attempted later on the restored
plain TCP composition. A-10 does not start them and does not switch the
gateway back to the mux composition for them.

VB-L3 and VB-L4 are performed in step 5 of the ENH setup, after `EBUSD-ES01`,
while the ebusd-tcp composition is still the running composition, and before
the return to the mux composition. If that setup is not run, VB-L3 and VB-L4
stay **Not run** with that reason. A-10 does not start them and does not open
the ebusd-tcp composition for them.

| ID | Matrix row | Composition | Operation | Pass criterion | Result |
|---|---|---|---|---|---|
| VB-L1 | VB-01 to VB-04 (`PASS (stub)`) | adapter-direct mux, at the end of step 4 of the `ENS` setup, while that composition is still running, before step 5 | the MCP read tools for current errors, error history, current service messages and service history, once each | request and response recorded; the decode matches the selector catalog; no write frame from the gateway | Not run |
| VB-L2 | VB-05 (`PASS`, not a stub row) | adapter-direct mux, at the end of step 4 of the `ENS` setup, while that composition is still running, before step 5 | live monitor enable, read, disable, on the operator's word at that moment | each step answered; the session is released at disable | Not run |
| VB-L3 | VB-19 (`PASS (stub)`) | ebusd-tcp inside step 5, after `EBUSD-ES01`, while that composition is still running | the current-errors read tool once | as VB-L1 | Not run |
| VB-L4 | VB-20 (`PASS (stub)`) | ebusd-tcp inside step 5, after `EBUSD-ES01`, while that composition is still running | as VB-L2, on the operator's word at that moment | as VB-L2 | Not run |

## The operator's part

| When | What |
|---|---|
| Before each site's part | the word for the session scope (E5) |
| At the start, after each host-link switch and at the end | a look at the appliance and the controller or the room-control app |
| Class W cells, once per setup | type the case ID when the runner asks; this does not start the adapter action. For `AD04` and `AD09`, reset the adapter (`AD04`) or disconnect and reconnect its host link (`AD09`) only after the smoke command announces the action and the case ID is typed again at that cue. When the smoke command ends without that cue, for example with status 4, perform no adapter action. On the plain UDP and plain TCP setups, type the case ID for `UDP-DS01` or `TCP-DS01`; those cells pass only on a smoke exit of 0 |
| Last `full88` run, each setup | the word for the `T13` composition immediately after `AD10` on the `ENS` setup, and for the `T14` composition immediately after `AD10` on the `ENH` setup; then, on a site that runs the explorer, the word to return to adapter-direct mux before that run; then the word for each remaining case of that setup in step 9, before its restore in step 11. After the last setup's step 9, assemble the one index; that assembly does not run the suite |
| Compatibility compositions | the word for reconfiguring the add-on and for the proxy listener's absence. At site A, VB-L1 and the word for VB-L2 at the end of step 4 on the `ENS` setup, while adapter-direct mux is still running, before this reconfiguration. VB-L3 and the word for VB-L4 while the ebusd-tcp composition of step 5 is still running |
| Site A | read and save the identification exchange record, complete on the retained request and response bytes |
| Site A | two display values for M-03; the zone and circuit counts for R-01; the word for the one R-06 refusal invoke, which is not a setpoint write |
| Site A, optional | the one live setpoint change (A-09), including when Home Assistant is its surface (A-08). Not a second change. A doubtful readback or return: hand restore, and no further write |
| Site A, B503 rows | the word for VB-L2 at the end of step 4 on the `ENS` setup, while adapter-direct mux is still the running composition, before step 5. Not at A-10, and not after the plain TCP restore. The word for VB-L4 is given in step 5, not at A-10 |
| Site B | the decision on the ebusd add-on's `network_device` option (B-02) |
| End of each site | confirm the as-found state, or decide that the candidate stays installed |
