# Transport gate policy v2 (AdapterMux conformance matrix)

Status: applied. This page is deliverable (d) of
[#1378](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1378),
and the switch condition of §1 is the owner's decision recorded on
[#1544](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1544).
The transport gate checks the AdapterMux conformance matrix
([M7-adaptermux-conformance-matrix.md](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/matrix/M7-adaptermux-conformance-matrix.md)):
`TRANSPORT_MATRIX_REPORT` points at an `adaptermux-v2` index, checked by the
rules of §4 in
[scripts/transport_gate.sh](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/scripts/transport_gate.sh).
The 88-case `full88` form is no longer accepted. The owner override that cites
[#1099](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1099)
keeps its meaning: pull requests merge under it until the consolidated
session's `adaptermux-v2` report exists.

## 1. When this policy takes force

The policy is in force. The switch did not wait for a run of the old form:

1. The 88-case form cannot run again. Eighty of its cases (T09–T88) are proxy
   topologies that the steer on #1378 retired; the external proxy is not part
   of the candidate image.
2. The cases that survive, T01–T08, have keepers in the matrix already: the
   [cell map](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/matrix/M7-adaptermux-conformance-cell-map.md)
   assigns `ENS-DS01`, `ENH-DS01`, `UDP-DS01` and `TCP-DS01` to T01–T04 and
   `EBUSD-ES01` to T05–T08. A `full88` report from the switch session would
   only repeat what those cells check.
3. The runner support for `adaptermux-v2` (#1411) is merged, so a session can
   produce the new form.

The switch therefore replaced the old form's report check in one pull request
(§3), without a last `full88` report. No hardware result is implied by the
switch itself: until a session produces an `adaptermux-v2` report, the eBUS
part of the gate is met only through the owner override of §2, as before. No
hardware waiver is implied either way.

## 2. What does not change

- The trigger surface of the eBUS gate: `requires_ebus_transport_gate`
  (including `internal/adaptermux/*`, `internal/matrix/*` and
  `cmd/matrix-runner/*`), `requires_b503_native_transport_gate`, and the
  `go.mod` rule for `helianthus-ebusgo` in
  [scripts/transport_gate.sh](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/scripts/transport_gate.sh).
- The override mechanism and its variable names: both
  `TRANSPORT_GATE_OWNER_OVERRIDE=OVERRIDE_TRANSPORT_GATE_BY_OWNER` and a
  non-empty `TRANSPORT_GATE_OWNER_REASON` naming scope and residual risk. An
  override never authorizes live writes and never skips a triggered offline
  Modbus RTU gate (`run_modbus_rtu_transport_gate`).
- The report path variable `TRANSPORT_MATRIX_REPORT`: it keeps its name and
  points at an `adaptermux-v2` index. A `full88` report is rejected by the suite
  check of §4 instead of by a second variable.
- The passive smoke gate ([scripts/passive_smoke_gate.sh](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/scripts/passive_smoke_gate.sh)),
  `PASSIVE_SMOKE_REPORT` and its own override (`PASSIVE_SMOKE_GATE_OWNER_OVERRIDE`,
  `PASSIVE_SMOKE_GATE_OWNER_REASON`). The steer does not re-point them; three of
  its cases (P03–P05) depend on proxy topologies and need a separate decision
  ([cell map](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/matrix/M7-adaptermux-conformance-cell-map.md), last section).

## 3. Locations changed at the switch

| Location | Before the switch | Applied |
|---|---|---|
| [scripts/transport_gate.sh](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/scripts/transport_gate.sh), report check (the embedded Python after `if [[ "${requires_ebus_gate}" -eq 1 ]]`) | required `len(cases) == 88`; accepted `blocked-infra` only with `adapter_no_signal`; any `planned` was unexpected | checks the `adaptermux-v2` rules of §4: suite, inventory version, exact case-ID set, per-category acceptance, one hardware setup per transport. The script declares the inventory it checks; a test of `internal/matrix` renders that declaration from the generator and fails on any difference |
| same file, owner-override block | comment: covered "the eBUS TRANSPORT_MATRIX_REPORT (T01..T88) check and the AD01..AD12 block" | covers the `adaptermux-v2` `TRANSPORT_MATRIX_REPORT` check, AD cells included; behavior unchanged |
| same file, AD block (`adapter_direct_touched`, `HELIANTHUS_AD_GATE_SKIP`) | an `internal/adaptermux/*.go` change failed unless `HELIANTHUS_AD_GATE_SKIP` named a reason, because "a formal matrix report ... is pending implementation" | the AD cells are part of the report; for such a change the gate states that the report's AD cells are present and accepted, and `HELIANTHUS_AD_GATE_SKIP` is removed |
| same file, messages and comments naming the old matrix | "TRANSPORT_MATRIX_REPORT is required for eBUS transport/protocol changes", "expected 88 cases", "require current M6a T01..T88 evidence" (B503 classifier comment), "Keep the 88-case transport gate" (SemReg comment), "an unrelated 88-case transport report" (main loop comment) | name the `adaptermux-v2` matrix; a `full88` report is rejected with a message that names the new form; no behavior change in the classifiers |
| [scripts/transport_gate_test.py](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/scripts/transport_gate_test.py) | 88-case report fixtures | `adaptermux-v2` fixtures, with negative cases for: wrong suite, the retired form, wrong inventory version, unknown or missing case, duplicate case, `planned` outside PL-01/PL-02/PL-03/PL-04, a cell in a category the gate does not accept, `uart_direct_hardware_absent` on a non-UART cell, `xfail` outside XF-01, `xpass`, unexpected `fail`, missing setup, two hardware setups within one transport |
| [AGENTS.md](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/AGENTS.md), "Gates and validation" | "require the applicable T01..T88 result"; the override "covers only the physical requirements (the eBUS `TRANSPORT_MATRIX_REPORT` T01..T88 check and the AD01..AD12 block)" | "require the applicable AdapterMux conformance matrix (`adaptermux-v2`) result"; the override covers the eBUS `TRANSPORT_MATRIX_REPORT` `adaptermux-v2` check, AD cells included |
| [cmd/matrix-runner/README.md](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/matrix-runner/README.md), suite table | `full88` "still the gate's report form"; `adaptermux-v2` "not yet the gate's form" | `adaptermux-v2` is the gate's report form; `full88` is historical |
| [internal/matrix/topology.go](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/matrix/topology.go), [internal/matrix/runner.go](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/matrix/runner.go), [cmd/matrix-runner/main.go](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/matrix-runner/main.go) | suites `full88` (default) and `passive`; `adaptermux-v2` added by the implementation ticket | unchanged by the switch: `full88` stays the runner's default flag value and stays generable to reproduce historical reports until the proxy repository is archived; the gate accepts only `adaptermux-v2` |
| [docs/testing/consolidated-candidate-handoff.md](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/docs/testing/consolidated-candidate-handoff.md) | range rows T01–T04, T05–T08, T09–T24, T25–T88, PX01–PX12; rows AD01–AD12 | to change with the next session's hand-off: one row per axis-1 transport and family (AD, PX, CX), the compatibility block, AD11 marked retired; the session section names one report |
| operator ship wrappers `ship_ticket.zsh` and `ship_fix.zsh` (outside this repository) | export `TRANSPORT_GATE_OWNER_OVERRIDE=OVERRIDE_TRANSPORT_GATE_BY_OWNER` with `TRANSPORT_GATE_OWNER_REASON="T01..T88/AD01..AD12 deferred to the consolidated 0.7 candidate hardware session (gateway #1099); offline gates and exact-HEAD reviews required; no live writes"`; never set `TRANSPORT_MATRIX_REPORT` | either pass `TRANSPORT_MATRIX_REPORT` with an `adaptermux-v2` report covering the change, or export the same two override variables with a reason that names the `adaptermux-v2` cells in scope (transport × invariant family) and the tracking issue of the consolidated session. A reason that cites #1099 is invalid once #1099 is closed. |

Unchanged by the switch, but read by it:
[matrix/M6a-transport-matrix.md](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/matrix/M6a-transport-matrix.md) is the L7
CM/FC matrix of #513, not the T01–T88 definition, and stays as it is. Outside
this repository, the 88-topology runbook and the PX adjunct section of
`development/smoke-matrix.md` and the `T/PX` gate row of
`architecture/adaptermux/first-byte-arbitration-revalidation.md` in
`helianthus-docs-ebus`, and `profiles/proxy-wire-semantics/px-cases.md` in
`helianthus-ebus-adapter-proxy`, are re-pointed through the dependent tickets
listed in `author-evidence/issue1378-matrix-v2/dependent-ticket-comments.md`.

## 4. Report format the gate checks

`TRANSPORT_MATRIX_REPORT` points at the `index.json` written by
`matrix-runner --suite adaptermux-v2`; when the session runs one hardware setup
at a time, the filtered invocations share one output directory and session
and the last one leaves the aggregated index (matrix page §7). Existing fields keep their meaning; the
new fields are those proposed in §7 of the matrix page.

| Field | Rule |
|---|---|
| `suite` | exactly `adaptermux-v2`; a `full88` report, or one without a suite, is rejected with a message naming the new form |
| `matrix_inventory_version` | equals the version declared by the gate; adding, removing or renaming a cell bumps it |
| `session_id` | one opaque value for the whole report |
| `candidate` | opaque association label; no source coordinates, addresses or serials |
| `cases[].case_id` | the set equals the declared inventory exactly (173 cells today); no duplicates, no extras |
| `cases[].session_id`, `cases[].candidate`, `cases[].target` | each equals the report-level value (`target` for `session_id`, `candidate`); a case carrying another value fails the gate |
| `cases[].southbound_transport`, `cases[].invariant` | consistent with the case ID (`ENH-AD01` → `enh`, `AD01`) |
| `cases[].hardware_setup` | non-empty on every executable cell; every cell of one transport that carries a setup carries the same value; `EBUSD-ES01` carries the ENH value |
| `cases[].expected` | `fail` for the cells of the expected-failure inventory (XF-01), `pass` for every other cell |
| `cases[].outcome` with `expectation`, `infra_reason`, `planned_reason` | accepted as below |

A reserved cell carries the outcome and the reason of its category, and
nothing else: the runner never executes it, so a `pass` on it is not
evidence. Acceptance per outcome on an executable cell:

| Outcome | Accepted when | Otherwise |
|---|---|---|
| `pass` | always | — |
| `xfail` | the cell is in XF-01 and `expectation` equals its reason string | gate fails |
| `xpass` | never | gate fails |
| `fail` | never | gate fails |
| `blocked-infra` | `infra_reason = adapter_no_signal` (BI-01) | gate fails |
| `planned` | never | gate fails |

Acceptance per reserved category:

| Category | Cells | Accepted outcome and reason |
|---|---|---|
| BI-02 | every UART cell | `blocked-infra`, `infra_reason = uart_direct_hardware_absent` |
| PL-01, PL-02, PL-03, PL-04 | as declared by the inventory | `planned`, `planned_reason` equal to the category reason |
| RS (`cell_awaiting_respecification`) | none today | not accepted: a report carrying it fails the gate |

XF-01 names `TCP-CX01`, which PL-01 reserves today, so no `xfail` can pass the
gate until the plain rows leave PL-01.

The gate prints one summary line with counts per outcome and per transport,
like `transport gate: PASS (suite=adaptermux-v2, matrix_inventory_version=2,
total=173; ...; by transport: ...)`. When an `internal/adaptermux/*.go` file
changed, it also states that the report's AD cells are present and accepted.

Synthetic shape (values illustrative, two of 173 cases shown):

```json
{
  "generated_at": "synthetic",
  "target": "ha-addon",
  "suite": "adaptermux-v2",
  "matrix_inventory_version": "2",
  "session_id": "session-a",
  "candidate": "candidate-a",
  "cases": [
    {"case_id": "ENH-AD01", "southbound_transport": "enh", "invariant": "AD01",
     "session_id": "session-a", "candidate": "candidate-a", "target": "ha-addon", "hardware_setup": "setup-enh", "outcome": "pass", "expected": "pass",
     "expectation": "", "infra_reason": "", "planned_reason": ""},
    {"case_id": "UARTRAW-CX01", "southbound_transport": "uart-raw", "invariant": "CX01",
     "session_id": "session-a", "candidate": "candidate-a", "target": "ha-addon", "hardware_setup": "", "outcome": "blocked-infra", "expected": "pass",
     "expectation": "", "infra_reason": "uart_direct_hardware_absent", "planned_reason": ""}
  ]
}
```

## 5. The consolidated session

The consolidated session on the 0.7 candidate (#1099) runs one report form,
`adaptermux-v2`, on one hardware setup per transport:

- per network transport setup, the `adaptermux-v2` cells of that row and the
  compatibility cells that run on it (`EBUSD-ES01` on the ENH setup), in one
  output directory and one session, so the last invocation leaves the
  aggregated index the gate checks;
- the T01–T08 questions are asked by their keepers, the compatibility cells
  `ENS-DS01`, `ENH-DS01`, `UDP-DS01`, `TCP-DS01` and `EBUSD-ES01`; no old-form
  case runs;
- every pull request listed on #1099 as deferred to the session is covered by
  that report, as #1099 requires, and the report is the first baseline of the
  gate.

After the session, #1099 closes on its own acceptance and the override reason
in the ship wrappers changes as in §3.

## 6. Residual risk stated for the switch

- 68 of 173 cells are `planned` in the projected first run (no plain southbound
  framing in the mux; no local responder for PX09–PX11; raw symbol forwarding
  not implemented for the `ENS` and `ENH` cells of AD05, AD08 and AD12; the
  FIFO-order cells PX05–PX08 of `ENS` and `ENH` reserved because address
  priority replaced FIFO order), 33 are executed, and 72 are
  `blocked-infra` (UART-direct). The gate accepts them only through their
  categories; they are not passing evidence.
- No `full88` report pairs with the first `adaptermux-v2` report. The T01–T08
  questions rest on their keeper cells alone; the retired T09–T88 questions
  have no counterpart beyond the legs the cell map assigns.
- The compatibility cells keep the non-mux direct transports and ebusd-tcp
  covered while they remain configurable.
- The passive suite and its override are outside this policy.
