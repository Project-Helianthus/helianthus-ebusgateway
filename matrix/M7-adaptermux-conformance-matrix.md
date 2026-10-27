# M7 — AdapterMux conformance matrix v2 (schema)

Status: planning deliverable (b) and (c) of
[#1378](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1378),
following the operator steer of 2026-10-27 recorded on that issue. This page
defines the schema of the matrix that replaces the 88-case transport matrix. It
is not in force, claims no result and changes no code: until the new matrix has
run once on hardware, the 88-case `TRANSPORT_MATRIX_REPORT` check, the
AD01..AD12 block of [scripts/transport_gate.sh](../scripts/transport_gate.sh)
and the owner override citing
[#1099](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1099)
remain the gate. Companion pages:

- [M7-adaptermux-conformance-cell-map.md](M7-adaptermux-conformance-cell-map.md):
  where each of T01–T88 goes (deliverable a).
- [transport-gate-policy-v2.md](../docs/testing/transport-gate-policy-v2.md):
  what the gate, the ship scripts and the override point at once this matrix has
  run (deliverable d).

## 1. Shape of the product

The proxy is deprecated and its multiplexing role is carried by the AdapterMux
([internal/adaptermux](../internal/adaptermux/)). The matrix is one product with
one report:

- a **grid** of axis 1 (southbound transport: where the symbol stream comes
  from) × axis 2 (northbound contract: what the mux does with the stream);
- a small **compatibility block** for configured transports that bypass the mux
  and that the gateway still supports (cell map, findings 1 and 2).

Every axis-2 invariant runs on every axis-1 transport; the framing differences
between transports (escape pairs, echo, structural SYN) are what the grid is
for. Hardware is needed once per transport, never once per permutation (§8).

## 2. Axis 1 — southbound transports

| Token | Transport | Framing (public contract in `helianthus-docs-ebus`) | Mux today | Default state |
|---|---|---|---|---|
| `ENS` | ENS over the network | ENH framing; on a network connection `ens:` and `enh:` are equivalent selectors (`protocols/ens.md`) | yes: `adapter-direct-ens://` | executable |
| `ENH` | ENH over the network | ENH command/response framing (`protocols/enh.md`) | yes: `adapter-direct://` | executable |
| `UDP` | UDP-plain | raw eBUS bytes; the transport defines no ENH framing, arbitration, escaping or client ownership (`protocols/udp-plain.md`) | no: the mux accepts only `enh`/`ens` upstream | planned (PL-01) |
| `TCP` | TCP-plain | raw eBUS bytes over a stream connection; no separate public page, read with the UDP-plain consequences | no | planned (PL-01) |
| `UARTENS` | UART-direct, ENS-over-serial | ENH framing at 115200 baud on a serial line (`protocols/ens.md`) | no | reserved: blocked-infra (BI-02) |
| `UARTENH` | UART-direct, ENH-over-serial | ENH framing at 9600 baud on a serial line (`protocols/ens.md`) | no | reserved: blocked-infra (BI-02) |
| `UARTRAW` | UART-direct, RAW | raw eBUS symbols from a line interface; no public native serial contract exists yet (Unknown) | no | reserved: blocked-infra (BI-02) |

"Mux today" is read from [internal/adaptermux/mux.go](../internal/adaptermux/mux.go)
and [cmd/gateway/gateway_adapter_direct.go](../cmd/gateway/gateway_adapter_direct.go).
The UART-direct rows are reserved now and stay blocked-infra until the hardware
exists, as the steer requires.

What changes between rows, and therefore why each invariant runs on each row:

| Southbound | Arbitration | Echo | Escape pairs | Structural SYN | Reset / loss boundary |
|---|---|---|---|---|---|
| ENS, ENH, UARTENS, UARTENH | adapter-side `START` / `STARTED` / `FAILED`; the arbitration byte reaches the winner as `STARTED`, not as `RECEIVED` | `RECEIVED` for every host-written byte after arbitration, both bytes of an escape pair included | host-side in both directions: each `SEND` is one physical wire byte, each `RECEIVED` carries one physical wire byte (`protocols/enh.md`, framing and escape responsibility) | unescaped `RECEIVED(0xAA)` | `RESETTED`; connection or line loss |
| UDP, TCP | none in the transport; the mux must arbitrate itself | raw bus echo of every written byte (Hypothesis until the mux's plain southbound defines it) | raw pairs in the stream; host-side in both directions | raw `0xAA` | no `RESETTED`; TCP disconnect; UDP has only silence |
| UARTRAW | as UDP/TCP, on a line | as UDP/TCP (Hypothesis) | as UDP/TCP | raw `0xAA` | line loss (Unknown) |

## 3. Axis 2 — northbound contract

Twenty-four invariants: AD01–AD10 and AD12 (AD11 is retired, §6), PX01–PX12,
and CX01. AD assertions are quoted from the AD01..AD12 block of
[scripts/transport_gate.sh](../scripts/transport_gate.sh); their public
contracts and offline candidates are indexed in `helianthus-docs-ebus`
(`development/ad01-ad12-evidence-index.md`). The multiplexer architecture page
in that repository numbers its design decisions AD01–AD12 as well; those are a
different namespace, and this page never joins them by number.

| ID | Assertion | Consumer side | Framing-sensitive aspect |
|---|---|---|---|
| AD01 | Active path sends/receives transactions | gateway session | arbitration handshake and echo verification per §2 |
| AD02 | Passive path zero self-echo | passive | self-echo identification when echo is `RECEIVED` (ENH) versus raw bus echo (plain) |
| AD03 | Passive path reconstructs third-party traffic | passive | logical view built from raw symbols with escape provenance |
| AD04 | RESETTED propagates to all consumers | all | plain and RAW rows have no `RESETTED`; the cell asserts the mux's discontinuity boundary for that southbound |
| AD05 | ebusd connects, receives broadcasts | external | raw symbols delivered unchanged to the external session |
| AD06 | ebusd sends START/SEND successfully | external | on plain rows the mux produces the `STARTED`/`FAILED` result from its own arbitration |
| AD07 | Gateway and ebusd compete fairly at SYN | gateway + external | owner arbitration between the gateway session and the external class |
| AD08 | Per-session echo suppression | external | which session receives which echo, per raw symbol |
| AD09 | Adapter disconnect triggers reconnection recovery | all | connection loss versus datagram silence |
| AD10 | Migration preserves semantics | semantic surfaces (MCP, GraphQL) | equivalence pair per transport (§6) |
| AD12 | No 0xA9/0xAA escape corruption | all | the wire-symbol contract (§6) |
| PX01 | stale `STARTED` absorb succeeds when matching result arrives in absorb window | external | adapter-side `STARTED` exists only on ENH-framed rows |
| PX02 | stale `STARTED` absorb expires via bounded fail path | external | as PX01 |
| PX03 | `SYN` while waiting for command `ACK` reopens arbitration immediately | gateway + external | structural SYN detection per row |
| PX04 | `SYN` while waiting for target response bytes reopens arbitration immediately | gateway + external | as PX03 |
| PX05 | same-boundary competition is resolved by FIFO registration order, not initiator priority | external | see the scope note below |
| PX06 | queued higher initiator keeps its FIFO turn when a lower initiator arrives before the next round closes | external | as PX05 |
| PX07 | requeue-after-timeout receives a new FIFO position and cannot steal priority from an older contender | external | as PX05 |
| PX08 | equal-initiator FIFO ordering is preserved | external | as PX05 |
| PX09 | local target observes request only from echoed `RECEIVED` path, never from owner `SEND` intent | local responder | planned (PL-02) |
| PX10 | local emulated target response inside responder window remains coherent | local responder | planned (PL-02) |
| PX11 | late responder bytes are rejected and counted | local responder | planned (PL-02) |
| PX12 | non-owner/non-responder sends are rejected during active transaction | external | ownership check per raw `SEND` |
| CX01 | Gateway and an external ENH client (ebusd or VRC Explorer) simultaneously through the mux | gateway + external | the whole stream under two owners |

PX statements are quoted from `profiles/proxy-wire-semantics/px-cases.md` in
`helianthus-ebus-adapter-proxy`, where they are tested offline against the
proxy's own code (`scripts/run_proxy_semantics_matrix.py`). In the matrix they
become cells of the mux, with these scope notes:

- PX05–PX08 apply within the external class. Between the gateway session and
  the external class, the mux's owner arbitration model (fairness ratio,
  external FIFO) governs, and AD07 asserts it.
- PX09–PX11 need a local emulated target with a responder window. The proxy had
  one (its emulation target registry); the mux has none. Registered responder
  ownership in the mux is
  [#991](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/991),
  which is deferred, so these cells are planned (PL-02).
- The proxy's outcome policy carries over: `xpass` is never accepted.
- The external contender in PX cells may be a synthetic ENH client from the
  test harness; that is software, not additional hardware.

CX01 inherits the coexistence question of T25–T88. A cell passes when the
runner's smoke command for the gateway passes while the external client
completes its own workload through the mux on the same southbound (an ebusd
scan and read cycle, or a VRC Explorer scan), with no corrupted frame
attributable to the mux and no starvation of either side. The verdict records
which client was used (`northbound_client`). The Explorer certification run
contributes CX01 records with `northbound_client = vrc-explorer`; it does not
add cells.

## 4. Compatibility block

| Cell | Composition | Old case | Kept while |
|---|---|---|---|
| `ENS-DS01` | gateway `-transport ens`, no mux; gateway is the sole owner | T01 | the gateway accepts `ens` without `adapter-direct-ens://` |
| `ENH-DS01` | gateway `-transport enh`, no mux | T02 | the gateway accepts `enh` without `adapter-direct://` |
| `UDP-DS01` | gateway `-transport udp-plain`, no mux | T03 | the gateway accepts `udp-plain` |
| `TCP-DS01` | gateway `-transport tcp-plain`, no mux | T04 | the gateway accepts `tcp-plain` |
| `EBUSD-ES01` | gateway `-transport ebusd-tcp`; ebusd reaches the adapter over ENH | T06 (T05, T07, T08 retired into it) | ebusd-tcp remains a configured transport |

Each compatibility cell runs the runner's smoke command, as T01–T06 did, on the
hardware setup of its transport (`EBUSD-ES01` on the ENH setup). They are not
axis-2 invariants and do not run on other rows.

## 5. Inventory, categories and projected first run

Cells: 7 transports × 24 invariants = 168 grid cells, plus 5 compatibility
cells: **173**.

Carried-over and new categories (the gate accepts a non-pass verdict only
through one of these):

| Category | Verdict | Cells | Exact reason string | Origin |
|---|---|---|---|---|
| XF-01 | `xfail` | `UDP-CX01`, `TCP-CX01`, once they leave PL-01 | `southbound plain udp/tcp under dual-client load reports no signal (carried from proxy dual-client)` | the 30 carried dual-client expected failures (cell map) |
| BI-01 | `blocked-infra` | any executable cell | `adapter_no_signal` | unchanged from the current runner and gate |
| BI-02 | `blocked-infra` | every `UARTENS-*`, `UARTENH-*`, `UARTRAW-*` cell, without execution | `uart_direct_hardware_absent` | steer: UART-direct reserved |
| PL-01 | `planned` | every `UDP-*` and `TCP-*` grid cell | `mux_southbound_plain_not_implemented` | the mux accepts only `enh`/`ens` upstream; no open ticket implements plain southbound framing |
| PL-02 | `planned` | every `*-PX09`, `*-PX10`, `*-PX11` cell | `mux_local_responder_not_implemented` | #991 (deferred) |
| PL-03 | `planned` | `ENS-AD05`, `ENH-AD05`, `ENS-AD08`, `ENH-AD08`, `ENS-AD12`, `ENH-AD12` | `mux_raw_symbol_forwarding_not_implemented` | the raw-symbol contract is settled but not implemented: ENH/ENS decoding still collapses escape pairs before the mux sees them, so raw-pair delivery to an external session (AD05), per-symbol echo of raw symbols (AD08) and the wire-symbol assertions (AD12) cannot hold ([mux-raw-symbol-continuity.md](../docs/architecture/mux-raw-symbol-continuity.md)); the cells leave PL-03 when the raw-forwarding slice of #1003 lands and the inventory version is bumped |

Precedence for the reported reason when categories overlap: BI-02, then PL-01,
then PL-02, then PL-03. Old expected-failure reasons that are not carried (T07/T08, and
ebusd → proxy over plain UDP) are listed in the cell map.

Projected verdict mix for the first hardware run if the candidate code keeps
today's mux scope (no plain southbound, no local responder):

| Cells | Count | Expected verdict |
|---|---|---|
| `ENS`, `ENH` × (AD01–AD04, AD06, AD07, AD09, AD10, PX01–PX08, PX12, CX01) | 36 | executed; `pass` expected |
| `ENS`, `ENH` × (AD05, AD08, AD12) | 6 | `planned` (PL-03) |
| `ENS`, `ENH` × PX09–PX11 | 6 | `planned` (PL-02) |
| `UDP`, `TCP` × all 24 | 48 | `planned` (PL-01) |
| UART rows × all 24 | 72 | `blocked-infra` (BI-02) |
| compatibility block | 5 | executed; `pass` expected |
| **total** | **173** | 41 executed, 60 planned, 72 blocked-infra |

## 6. Index completion (AD10, AD11, AD12)

**AD10 — kept, with an equivalence test plan.** Migration into the mux still
happens after the proxy goes away: from the non-mux direct transports (the
add-on's default transport is `enh` without the mux), from ebusd-tcp, and one
last time from proxy deployments. The `AD10` cell of each southbound S pairs two
compositions of the same bus in the same hardware setup: the configuration the
mux replaces (`S-DS01`) and the mux (`S-AD01` workload). In the transition
session the old-form proxy run with an ENH client leg on that southbound (T13,
T14, T15, T16 for ENS, ENH, UDP-plain, TCP-plain) is attached as an additional
replaced side. Each pair is compared with the record design of
`development/ad10-migration-equivalence.md` in `helianthus-docs-ebus`: values,
presence, types, units and lineage per semantic field, and MCP/GraphQL parity,
as separate decisions; equal values with wrong provenance are not equivalence.
`ENH-AD10` is also paired with `EBUSD-ES01` (ebusd-tcp over the same ENH setup) as a second replaced side, with the same record design, so a semantic or lineage regression seen by users migrating from ebusd-tcp is not hidden behind the `S-DS01` pair. `UDP-AD10` and `TCP-AD10` stay planned with the rest of PL-01. UART rows have no
non-mux composition today; their `AD10` cells stay blocked-infra and need a
replaced side defined before they can execute.

**AD11 — retired, with an explicit note.** AD11 ("Rollback restores behavior")
compares a return from adapter-direct to the pre-migration proxy topology with
the baseline before migration. The standalone proxy is deprecated and read-only
(its repository says so), so a return to it is not a supported operation and
cannot be an acceptance obligation. Leaving the mux for a configuration that
bypasses it (direct ENS, ENH, UDP-plain, TCP-plain, or ebusd-tcp) stays a
supported configuration change; those configurations are accepted directly by
the compatibility cells, run in the same session as the grid, not by a
before/after comparison. A downgrade of the gateway itself is release rollback,
not an AdapterMux invariant. The AD11 comparison design in
`helianthus-docs-ebus` (`development/ad11-rollback-equivalence.md`) remains as
an unexecuted design; no artifact is required for AD11.

**AD12 — wording replaced by the wire-symbol contract of #1003.** The
architecture text "ENH protocol delivers logical bytes (no escape sequences)"
contradicts the native ENH receive path, as the evidence index already records.
The settled contract
([#1003](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1003),
[mux-raw-symbol-continuity.md](../docs/architecture/mux-raw-symbol-continuity.md)):

- raw wire symbols at the mux boundary: each escape lead and suffix, and each
  unescaped `0xAA`, is its own symbol, delivered immediately and in order to
  external ENH sessions, without waiting for a suffix;
- host-side escaping on both paths: the gateway encodes its logical bytes into
  physical wire symbols on the host; a raw `SEND` from an external ENH client
  is already one physical wire symbol and is passed through, never escaped
  again; receive
  decoding of `A9 00` / `A9 01` happens on the host side, never in the adapter
  stream;
- internal consumers (gateway active path, passive reconstruction, phase
  tracking) use a separately decoded logical view that keeps escape provenance;
  that view is a host projection, not the wire contract.

The `AD12` cells assert this on every row once raw forwarding is implemented; until then `ENS-AD12` and `ENH-AD12`, and the AD05 and AD08 cells whose raw-pair delivery and per-symbol echo depend on the same forwarding, are `planned` (PL-03) rather than failing or falsely passing, and the switch to `adaptermux-v2` does not wait for them. The assertion: no double escaping, no collapsed
pair, raw `0xAA` as the only structural SYN, reserved bytes in a CRC position
sent and verified as both raw symbols.

**Prepared documentation change.** The matching edit of
`helianthus-docs-ebus` is prepared on its branch `issue/1378-ad-index-matrix-v2`
(not yet published): the AD12 decision text in
`architecture/adapter-direct-multiplexer.md` and its two statements that the
standalone proxy remains available; the AD10, AD11 and AD12 rows, the result
vocabulary and the conflict note of `development/ad01-ad12-evidence-index.md`;
a status note at the top of `development/ad11-rollback-equivalence.md`; and a
pairing note in `development/ad10-migration-equivalence.md`.

## 7. matrix-runner case naming for 0.7 (proposal, not implemented)

Current scheme, read from [internal/matrix](../internal/matrix/) and
[cmd/matrix-runner](../cmd/matrix-runner/): case IDs are an upper-case prefix
and two digits (`T01`–`T88`, `P01`–`P06`); suites are `full88` (default) and
`passive`; expected-failure keys are upper-cased (`normalizeExpectedFailures`);
each case writes `<output-dir>/<case_id>/configs`, `logs/runner.log` and
`verdict.json`; the run writes `index.json` with `generated_at`, `target`,
`suite` and `cases`; transports are the lower-case constants `ens`, `enh`,
`udp`, `tcp`, `ebusd-tcp`.

Proposal for the implementation ticket:

- **Suite** `adaptermux-v2`, generated next to `full88`. `full88` stays the
  default and stays unchanged until the switch described in the policy page.
- **Case ID** `<TRANSPORT>-<INVARIANT>`, upper case, one hyphen: transport
  tokens `ENS`, `ENH`, `UDP`, `TCP`, `UARTENS`, `UARTENH`, `UARTRAW`, `EBUSD`;
  invariant tokens are the invariant's own ID (`AD07`, `PX05`, `CX01`) or the
  compatibility smoke IDs `DS01` (direct, non-mux) and `ES01` (ebusd-tcp).
  Examples: `ENH-AD12`, `UDP-PX05`, `UARTRAW-CX01`, `TCP-DS01`, `EBUSD-ES01`.
  IDs stay upper case so the existing expected-failure normalization applies,
  and they are valid directory names.
- **Order** transports in axis-1 order (ENS, ENH, UDP, TCP, UARTENS, UARTENH,
  UARTRAW), each with AD01–AD10, AD12, PX01–PX12, CX01; then the compatibility
  block in the order `ENS-DS01`, `ENH-DS01`, `UDP-DS01`, `TCP-DS01`,
  `EBUSD-ES01`.
- **Kinds** new `TopologyKind` values `mux-invariant`, `mux-coexistence`,
  `direct-compat`, `ebusd-tcp-compat`.
- **Verdict fields** added to `CaseVerdict` (existing fields unchanged):
  `southbound_transport` (`ens`, `enh`, `udp`, `tcp`, `uart-ens`, `uart-enh`,
  `uart-raw`, `ebusd-tcp`), `invariant`, `invariant_family` (`AD`, `PX`, `CX`,
  `COMPAT`), `composition` (`mux`, `direct`, `ebusd-tcp`), `northbound_client`
  (`none`, `ebusd`, `vrc-explorer`, `synthetic-enh`), `hardware_setup` (opaque
  per-transport setup label), `session_id` (opaque), `candidate` (the same opaque label as the index; the aggregation below compares it per case), `planned_reason`.
- **Index fields** added to `index.json`: `matrix_inventory_version`,
  `session_id`, `candidate` (opaque association label, no source coordinates).
- **Environment** keeps the existing `MATRIX_*` variables and adds
  `MATRIX_SOUTHBOUND_TRANSPORT`, `MATRIX_INVARIANT`, `MATRIX_COMPOSITION`,
  `MATRIX_NORTHBOUND_CLIENT`.
- **Aggregation of per-setup runs** `--session-id <label>` and `--candidate <label>` are required with `--transports`; all filtered invocations of one session share one `--output-dir`. Each invocation writes only its own case directories and then rebuilds `index.json` from every `verdict.json` in the directory whose `session_id` and `candidate` match, so the last invocation yields the full inventory; a verdict carrying a different `session_id` or `candidate`, or a second `hardware_setup` for one transport, makes the rebuild fail instead of merging. A case re-run replaces its own directory. Cells not yet run are absent from the index, so the gate's exact-inventory rule rejects a partial aggregate.
- **Flags** `--transports ENS,ENH` selects the cells of one hardware setup;
  `--external-client ebusd|vrc-explorer` selects the CX01 client; the
  `--start-proxy`/`--stop-proxy` commands are not used by `adaptermux-v2`;
  `--start-ebusd`/`--stop-ebusd` remain for the external client.
- **Reserved cells** UART cells are written with outcome `blocked-infra` and
  `infra_reason = uart_direct_hardware_absent` without executing commands;
  PL-01 and PL-02 cells are written with outcome `planned` and their
  `planned_reason`. This is new: today `planned` comes only from a dry run.
- **Default expected failures** move from `defaultExpectedFailure` to an
  explicit inventory table for the suite (XF-01 only).

Synthetic example of one case entry (no real run):

```json
{
  "case_id": "ENH-AD01",
  "suite": "adaptermux-v2",
  "kind": "mux-invariant",
  "southbound_transport": "enh",
  "invariant": "AD01",
  "invariant_family": "AD",
  "composition": "mux",
  "northbound_client": "ebusd",
  "hardware_setup": "setup-enh",
  "session_id": "session-a",
  "candidate": "candidate-a",
  "target": "ha-addon",
  "status": "passed",
  "outcome": "pass",
  "expected": "pass",
  "expectation": "",
  "infra_reason": "",
  "planned_reason": ""
}
```

## 8. Verdicts and hardware

The vocabulary is unchanged: `pass`, `fail`, `xfail`, `xpass`, `blocked-infra`,
`planned`, with the current runner meanings (`classifyCaseOutcome`). A non-pass
verdict is acceptable only through a §5 category; `fail`, `xpass`, and any
`planned` or `blocked-infra` outside its category fail the gate (policy page).

Hardware is required once per transport. One setup per axis-1 row (four network
setups now, three UART setups once they exist) runs every grid cell of that row
and the compatibility cells that share it. External clients (ebusd, VRC
Explorer, a synthetic ENH client) are software on the host side and need no
extra hardware. The old matrix needed a separate topology per permutation of
gateway leg, proxy southbound and ebusd leg; none of those permutations exists
any more.

The consolidated session on the 0.7 candidate
([#1099](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1099))
is the last run of `full88` and the first run of `adaptermux-v2`, on the same
hardware session and the same per-transport setups.

## 9. Open points

- PX09–PX11 stay planned unless #991 is brought into 0.7; accepting planned for
  them in the first run is an operator decision.
- No open ticket implements UDP-plain or TCP-plain southbound framing in the
  mux; 48 cells stay planned until one does. A draft issue is in
  `author-evidence/issue1378-matrix-v2/dependent-ticket-comments.md`.
- P03–P05 of the passive suite depend on proxy topologies and are not decided
  here (cell map, last section).
- Keeping `*-DS01` depends on the non-mux direct transports remaining
  configurable.
- The INFO-retention conflict between the multiplexer architecture and the
  native ENH reset rule, recorded in the evidence index, is untouched; it
  affects AD01 and AD04 cells on ENH-framed rows.
