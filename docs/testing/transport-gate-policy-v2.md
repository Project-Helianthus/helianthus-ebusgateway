# Transport gate policy v2 (AdapterMux conformance matrix)

Status: proposed policy text, deliverable (d) of
[#1378](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1378).
Nothing on this page is applied. It states what the transport gate, the ship
scripts and the owner override must point at once the AdapterMux conformance
matrix ([M7-adaptermux-conformance-matrix.md](../../matrix/M7-adaptermux-conformance-matrix.md))
has run once on hardware. Until then the current gate stays in force unchanged:
the 88-case `TRANSPORT_MATRIX_REPORT` check and the AD01..AD12 block of
[scripts/transport_gate.sh](../../scripts/transport_gate.sh), and the owner
override that cites
[#1099](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1099).

## 1. When this policy takes force

The switch happens in one pull request, after all of the following hold:

1. The consolidated hardware session on the 0.7 candidate (#1099) has produced
   two reports in the same hardware session, against the same candidate and the
   same per-transport hardware setups: the last `full88` report (T01–T88, the
   old form) and the first `adaptermux-v2` report (the new form). That session
   is the last run of the old form and the first run of the new one.
2. The `adaptermux-v2` report satisfies the gate rules of §4, and its
   unexpected results, if any, were resolved through the session's normal stop
   and revalidation rules ([consolidated-candidate-handoff.md](consolidated-candidate-handoff.md)).
3. The runner support for `adaptermux-v2` (a separate ticket) was merged
   before that session, since the session cannot produce the new form without
   it; the gate changes of §3 land only in the switch pull request.

If the new form cannot complete in that session, the policy does not take force
and the old gate and the #1099 override stay as they are. No hardware waiver is
implied either way.

## 2. What does not change

- The trigger surface of the eBUS gate: `requires_ebus_transport_gate`
  (including `internal/adaptermux/*`, `internal/matrix/*` and
  `cmd/matrix-runner/*`), `requires_b503_native_transport_gate`, and the
  `go.mod` rule for `helianthus-ebusgo` in
  [scripts/transport_gate.sh](../../scripts/transport_gate.sh).
- The override mechanism and its variable names: both
  `TRANSPORT_GATE_OWNER_OVERRIDE=OVERRIDE_TRANSPORT_GATE_BY_OWNER` and a
  non-empty `TRANSPORT_GATE_OWNER_REASON` naming scope and residual risk. An
  override never authorizes live writes and never skips a triggered offline
  Modbus RTU gate (`run_modbus_rtu_transport_gate`).
- The report path variable `TRANSPORT_MATRIX_REPORT`: it keeps its name and
  points at an `adaptermux-v2` index. A `full88` report is rejected by the suite
  check of §4 instead of by a second variable.
- The passive smoke gate ([scripts/passive_smoke_gate.sh](../../scripts/passive_smoke_gate.sh)),
  `PASSIVE_SMOKE_REPORT` and its own override (`PASSIVE_SMOKE_GATE_OWNER_OVERRIDE`,
  `PASSIVE_SMOKE_GATE_OWNER_REASON`). The steer does not re-point them; three of
  its cases (P03–P05) depend on proxy topologies and need a separate decision
  ([cell map](../../matrix/M7-adaptermux-conformance-cell-map.md), last section).

## 3. Exact locations to change at the switch

| Location | Today | After the switch |
|---|---|---|
| [scripts/transport_gate.sh](../../scripts/transport_gate.sh), report check (the embedded Python after `if [[ "${requires_ebus_gate}" -eq 1 ]]`) | requires `len(cases) == 88`; accepts `blocked-infra` only with `adapter_no_signal`; any `planned` is unexpected | checks the `adaptermux-v2` rules of §4: suite, inventory version, exact case-ID set, per-category acceptance, one hardware setup per transport |
| same file, owner-override block | comment: covers "the eBUS TRANSPORT_MATRIX_REPORT (T01..T88) check and the AD01..AD12 block" | covers the `adaptermux-v2` `TRANSPORT_MATRIX_REPORT` check, AD cells included; behavior unchanged |
| same file, AD block (`adapter_direct_touched`, `HELIANTHUS_AD_GATE_SKIP`) | an `internal/adaptermux/*.go` change fails unless `HELIANTHUS_AD_GATE_SKIP` names a reason, because "a formal matrix report ... is pending implementation" | the AD cells are part of the report; the block checks that the report's AD cells are present and accepted, and `HELIANTHUS_AD_GATE_SKIP` is removed |
| same file, messages and comments naming the old matrix | "TRANSPORT_MATRIX_REPORT is required for eBUS transport/protocol changes", "expected 88 cases", "require current M6a T01..T88 evidence" (B503 classifier comment), "Keep the 88-case transport gate" (SemReg comment), "an unrelated 88-case transport report" (main loop comment) | name the `adaptermux-v2` matrix; no behavior change in the classifiers |
| [scripts/transport_gate_test.py](../../scripts/transport_gate_test.py) | 88-case report fixtures | `adaptermux-v2` fixtures, with negative cases for: wrong suite, unknown or missing case, duplicate case, `planned` outside PL-01/PL-02, `uart_direct_hardware_absent` on a non-UART cell, `xfail` outside XF-01, `xpass`, two hardware setups within one transport |
| [AGENTS.md](../../AGENTS.md), "Gates and validation" | "require the applicable T01..T88 result"; the override "covers only the physical requirements (the eBUS `TRANSPORT_MATRIX_REPORT` T01..T88 check and the AD01..AD12 block)" | "require the applicable AdapterMux conformance matrix (`adaptermux-v2`) result"; the override covers the eBUS `TRANSPORT_MATRIX_REPORT` `adaptermux-v2` check, AD cells included |
| [internal/matrix/topology.go](../../internal/matrix/topology.go), [internal/matrix/runner.go](../../internal/matrix/runner.go), [cmd/matrix-runner/main.go](../../cmd/matrix-runner/main.go) | suites `full88` (default) and `passive` | suite `adaptermux-v2` added by the implementation ticket; the switch makes it the default; `full88` stays generable to reproduce historical reports until the proxy repository is archived |
| [docs/testing/consolidated-candidate-handoff.md](consolidated-candidate-handoff.md) | range rows T01–T04, T05–T08, T09–T24, T25–T88, PX01–PX12; rows AD01–AD12 | one row per axis-1 transport and family (AD, PX, CX), the compatibility block, AD11 marked retired; the session section names both reports |
| operator ship wrappers `ship_ticket.zsh` and `ship_fix.zsh` (outside this repository) | export `TRANSPORT_GATE_OWNER_OVERRIDE=OVERRIDE_TRANSPORT_GATE_BY_OWNER` with `TRANSPORT_GATE_OWNER_REASON="T01..T88/AD01..AD12 deferred to the consolidated 0.7 candidate hardware session (gateway #1099); offline gates and exact-HEAD reviews required; no live writes"`; never set `TRANSPORT_MATRIX_REPORT` | either pass `TRANSPORT_MATRIX_REPORT` with an `adaptermux-v2` report covering the change, or export the same two override variables with a reason that names the `adaptermux-v2` cells in scope (transport × invariant family) and the successor tracking issue of the next consolidated session. A reason that cites #1099 is invalid once #1099 is closed. |

Unchanged by the switch, but read by it:
[matrix/M6a-transport-matrix.md](../../matrix/M6a-transport-matrix.md) is the L7
CM/FC matrix of #513, not the T01–T88 definition, and stays as it is. Outside
this repository, the 88-topology runbook and the PX adjunct section of
`development/smoke-matrix.md` and the `T/PX` gate row of
`architecture/adaptermux/first-byte-arbitration-revalidation.md` in
`helianthus-docs-ebus`, and `profiles/proxy-wire-semantics/px-cases.md` in
`helianthus-ebus-adapter-proxy`, are re-pointed through the dependent tickets
listed in `author-evidence/issue1378-matrix-v2/dependent-ticket-comments.md`.

## 4. Report format the gate will check

`TRANSPORT_MATRIX_REPORT` points at the `index.json` written by
`matrix-runner --suite adaptermux-v2`. Existing fields keep their meaning; the
new fields are those proposed in §7 of the matrix page.

| Field | Rule |
|---|---|
| `suite` | exactly `adaptermux-v2` |
| `matrix_inventory_version` | equals the version declared by the gate; adding, removing or renaming a cell bumps it |
| `session_id` | one opaque value for the whole report |
| `candidate` | opaque association label; no source coordinates, addresses or serials |
| `cases[].case_id` | the set equals the declared inventory exactly (173 cells today); no duplicates, no extras |
| `cases[].southbound_transport`, `cases[].invariant` | consistent with the case ID (`ENH-AD12` → `enh`, `AD12`) |
| `cases[].hardware_setup` | every executed cell of one transport carries the same value; `EBUSD-ES01` carries the ENH value |
| `cases[].outcome` with `expected`, `expectation`, `infra_reason`, `planned_reason` | accepted as below |

Acceptance per outcome:

| Outcome | Accepted when | Otherwise |
|---|---|---|
| `pass` | always | — |
| `xfail` | the cell is in XF-01 and `expectation` equals its reason string | gate fails |
| `xpass` | never | gate fails |
| `fail` | never | gate fails |
| `blocked-infra` | `infra_reason = adapter_no_signal` on an executable cell (BI-01), or `infra_reason = uart_direct_hardware_absent` on a UART cell (BI-02) | gate fails |
| `planned` | the cell is in PL-01 or PL-02 and `planned_reason` equals its category reason | gate fails |

The gate prints one summary line with counts per outcome and per transport,
like today's `transport gate: PASS (...)` line. Pairing the two reports of the
transition session (same `session_id`, same `candidate`, same per-transport
setups) is checked by the session's acceptance review, not by the per-PR gate.

Synthetic shape (values illustrative, two of 173 cases shown):

```json
{
  "generated_at": "synthetic",
  "target": "ha-addon",
  "suite": "adaptermux-v2",
  "matrix_inventory_version": "1",
  "session_id": "session-a",
  "candidate": "candidate-a",
  "cases": [
    {"case_id": "ENH-AD12", "southbound_transport": "enh", "invariant": "AD12",
     "hardware_setup": "setup-enh", "outcome": "pass", "expected": "pass",
     "expectation": "", "infra_reason": "", "planned_reason": ""},
    {"case_id": "UARTRAW-CX01", "southbound_transport": "uart-raw", "invariant": "CX01",
     "hardware_setup": "", "outcome": "blocked-infra", "expected": "pass",
     "expectation": "", "infra_reason": "uart_direct_hardware_absent", "planned_reason": ""}
  ]
}
```

## 5. The transition session

The consolidated session on the 0.7 candidate (#1099) runs both forms on the
same hardware:

- per network transport setup, the old-form cases that use it and the
  `adaptermux-v2` cells of that row, so both reports share setups;
- the old-form proxy single-client runs with an ENH gateway leg (T13–T16) are
  attached to the `AD10` cells as the additional proxy-side baseline (matrix
  page, §6);
- every pull request listed on #1099 as deferred to the session is covered by
  the old-form report, as #1099 requires; the new-form report is the first
  baseline of the new gate.

After the session, the switch pull request applies §3, #1099 closes on its own
acceptance, and the override reason in the ship wrappers changes as in §3.

## 6. Residual risk stated for the switch

- 54 of 173 cells are `planned` in the projected first run (no plain southbound
  framing in the mux; no local responder for PX09–PX11) and 72 are
  `blocked-infra` (UART-direct). The gate accepts them only through their
  categories; they are not passing evidence.
- The compatibility cells keep the non-mux direct transports and ebusd-tcp
  covered while they remain configurable.
- The passive suite and its override are outside this policy.
