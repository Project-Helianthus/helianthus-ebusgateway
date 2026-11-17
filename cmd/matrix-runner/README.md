# matrix-runner

`matrix-runner` generates and runs the transport conformance matrix. It writes
one directory per case (`configs/`, `logs/runner.log`, `verdict.json`) and an
`index.json` under `--output-dir`.

| Suite | Cases | Status |
|---|---|---|
| `full88` (default) | `T01`–`T88` | unchanged; still the gate's report form |
| `passive` | `P01`–`P06` | unchanged |
| `adaptermux-v2` | 173 cells of the two-axis AdapterMux conformance matrix | new; not yet the gate's form |

`full88` stays the default. Switching the gate to `adaptermux-v2` is a separate
change ([transport-gate-policy-v2.md](../../docs/testing/transport-gate-policy-v2.md)).
The schema is defined in
[M7-adaptermux-conformance-matrix.md](../../matrix/M7-adaptermux-conformance-matrix.md).

Without `--execute` every suite is a dry run: nothing is executed, and each
log lists the commands a real run would execute.

## adaptermux-v2

### Inventory

Case IDs are `<TRANSPORT>-<INVARIANT>`. Transports in axis-1 order are `ENS`,
`ENH`, `UDP`, `TCP`, `UARTENS`, `UARTENH` and `UARTRAW`. Each transport has
AD01–AD10, AD12, PX01–PX12 and CX01. The compatibility cells come last:
`ENS-DS01`, `ENH-DS01`, `UDP-DS01`, `TCP-DS01` and `EBUSD-ES01`.
`matrix_inventory_version` is `2`. Adding, removing or renaming a cell, or
changing a cell's assertion, category or workload, bumps it.

Reserved cells are written without executing any command. When a cell matches
more than one category, the first matching row below wins:

| Category | Outcome | Reason | Cells |
|---|---|---|---|
| BI-02 | `blocked-infra` (`infra_reason`) | `uart_direct_hardware_absent` | every `UART*` cell |
| PL-01 | `planned` (`planned_reason`) | `mux_southbound_plain_not_implemented` | every `UDP-*`, `TCP-*` grid cell |
| PL-02 | `planned` | `mux_local_responder_not_implemented` | `*-PX09`, `*-PX10`, `*-PX11` |
| PL-03 | `planned` | `mux_raw_symbol_forwarding_not_implemented` | `ENS`/`ENH` × AD05, AD08, AD12 |
| PL-04 | `planned` | `fifo_order_superseded_by_address_priority` | `ENS`/`ENH` × PX05–PX08 |

PL-04 holds the cells that assert FIFO registration order between external
contenders: address priority replaced FIFO order
([adapter-arbitration-decision.md](../../docs/architecture/adapter-arbitration-decision.md)),
so they stay reserved until they are re-specified for address priority. The
category RS (`cell_awaiting_respecification`, not accepted by the gate) stays
defined for a future re-specification, and no cell carries it.

The expected-failure inventory is fixed: XF-01 on `TCP-CX01` only.
`--expected-failures` and `--expected-failures-file` are rejected for this
suite. The 33 executable cells are `ENS`/`ENH` × AD01, AD02, AD03, AD04, AD06,
AD07, AD09, AD10, PX01–PX04, PX12, CX01, plus the five compatibility cells.

### Flags

| Flag | Meaning |
|---|---|
| `--suite adaptermux-v2` | select the suite |
| `--transports ENS,ENH` | run only the cells of these transports, which one hardware setup serves; `EBUSD-ES01` goes with `ENH` |
| `--session-id <label>` | opaque session label, shared by every invocation of one session |
| `--candidate <label>` | opaque candidate label, shared by every invocation of one session |
| `--target local\|ha-addon` | execution target; must be given explicitly with `--transports` |
| `--hardware-setup <label>` | opaque label of the setup that runs the selected transports |
| `--external-client ebusd\|vrc-explorer` | CX01 external client (default `ebusd`) |
| `--mux-enh-addr host:port` | mux ENH listener for the synthetic contender (PX cells) |
| `--ebusd-command-addr host:port` | ebusd command port for the ebusd workload and the guarded relay |
| `--vrc-explorer-command <path>` | VRC Explorer executable for CX01 with `--external-client vrc-explorer` |
| `--workload-initiator-addr <hex>` | initiator address of the harness clients |
| `--workload-target-addr <hex>` | target of the identification workload |

`--session-id`, `--candidate` and `--hardware-setup` are required with
`--transports` and with `--execute`. Labels may contain letters, digits, `.`,
`_` and `-`, up to 64 characters. An empty label is rejected at start, and so
is any label containing `:` or `/` (a host and port, or a path). Labels
are opaque: do not put addresses, serials or other source coordinates in
them. `--start-proxy` and
`--stop-proxy` are rejected. `--start-ebusd` and `--stop-ebusd` start and stop
ebusd for the cells that use it. The adaptermux-v2 flags are rejected for the
other suites.

### One session, one invocation per setup

```sh
for setup in ENS ENH UDP TCP; do
  matrix-runner --suite adaptermux-v2 --execute \
    --output-dir results-session-a \
    --transports "$setup" --hardware-setup "setup-$(echo "$setup" | tr A-Z a-z)" \
    --session-id session-a --candidate candidate-a --target ha-addon \
    --start-gateway ... --stop-gateway ... --start-ebusd ... --stop-ebusd ... \
    --smoke-command ... \
    --mux-enh-addr ... --ebusd-command-addr ... \
    --workload-initiator-addr f7 --workload-target-addr 15
done
matrix-runner --suite adaptermux-v2 --output-dir results-session-a \
  --transports UARTENS,UARTENH,UARTRAW --hardware-setup setup-none \
  --session-id session-a --candidate candidate-a --target ha-addon
```

Each invocation writes only its own case directories. It then rebuilds
`index.json` from every `verdict.json` in the output directory, in inventory
order. The invocation fails before running anything, and the rebuild fails
rather than merging, when the directory holds:

- a verdict from another session, candidate or target;
- a second hardware setup for a transport;
- a directory that is not an inventory case, or a verdict whose case ID,
  suite, southbound transport or invariant disagrees with the inventory;
- an executed verdict without a setup label.

Re-running a case replaces its directory. Cells that have not run yet are
missing from the index, so the gate's exact-inventory rule rejects a partial
aggregate. UART cells record an empty `hardware_setup` because no setup for
them exists.

### Workloads and pass criteria

Each case's `configs/workload.json` records the cell's class, assertion,
workload, pass criterion and category. A run sets the usual `MATRIX_*`
variables, plus `MATRIX_SOUTHBOUND_TRANSPORT`, `MATRIX_INVARIANT`,
`MATRIX_INVARIANT_FAMILY`, `MATRIX_COMPOSITION`, `MATRIX_NORTHBOUND_CLIENT`,
`MATRIX_HARDWARE_SETUP`, `MATRIX_SESSION_ID`, `MATRIX_CANDIDATE`,
`MATRIX_CELL_CLASS`, `MATRIX_OUTPUT_DIR` and `MATRIX_AD10_PAIRED_CASES`. The
smoke command uses them to run the check for that invariant.

- AD cells and compatibility cells pass when the smoke command exits 0.
- AD06 also runs the ebusd workload: `state`, `scan ZZ`, and `hex ZZ070400`.
- AD07 runs the same ebusd workload while the gateway runs its own
  transactions, so both sessions bid at the same SYN boundaries.
- AD04 and AD09 are operator adapter actions. The runner names the case and
  asks for its ID; after the smoke command announces the action (on its
  output and on the controlling terminal, once its first reading is taken)
  the operator resets the adapter (AD04) or disconnects the adapter's host
  link and reconnects it (AD09) inside the case window. The evidence records
  `operator_cue` as `terminal` or `case_log_only`. The smoke command asks
  again, at that moment, for the case ID typed on the terminal; without it
  (skipped, or no terminal) the cell fails and says so in its reasons. Set
  `MATRIX_SMOKE_WINDOW_SEC` long enough for the action and the recovery.
- PX cells also run the synthetic ENH contender: INIT, then three 07 04
  identification telegrams through START/SEND, with every echoed symbol and
  the response CRC verified.
- CX01 runs the client chosen with `--external-client`. With `ebusd`, it is
  the ebusd workload. With `vrc-explorer`, it is a read-only scan whose only
  endpoint is the guarded ebusd command relay.
- AD10 compares against the cases in `MATRIX_AD10_PAIRED_CASES`: `S-DS01`, and
  `EBUSD-ES01` for ENH.

A client's result is written to `logs/client.json`. The cell passes only when
the smoke command and the client workload both pass.

### Smoke command

`scripts/adaptermux_v2_smoke.py` is the smoke command of this suite. It uses
the Python standard library only:

```sh
matrix-runner --suite adaptermux-v2 --execute ... \
  --smoke-command 'python3 scripts/adaptermux_v2_smoke.py'
```

It reads the gateway, never writes: HTTP GET of the bus observability
snapshot, the Prometheus text and the expvar counters, plus, for the
semantic record, one GraphQL query and one MCP read tool. It refuses a
GraphQL mutation or subscription and any MCP tool outside its read list, and
it never invokes a bus method.

The check reads every counter once when it starts, then again every poll
interval until the invariant's flow requirements hold or the window ends; an
invariant that forbids a counter moving observes the whole window.
After the requirements hold it reads once more and judges that last reading.
No poll sleeps past the end of the window, and a reading that completes after
the window ends is discarded: the check judges the last reading completed
inside the window, so traffic after the window never counts. A cell passes only when the gateway reports the transport class of the cell,
the write guard is confirmed and eBUS frames moved on the selected southbound
transport. A surface that answers without bus flow fails the cell.

Environment:

| Variable | Use |
|---|---|
| `MATRIX_CASE_ID`, `MATRIX_INVARIANT`, `MATRIX_SOUTHBOUND_TRANSPORT`, `MATRIX_COMPOSITION`, `MATRIX_CELL_CLASS` | the cell; set by the runner |
| `MATRIX_CASE_DIR`, `MATRIX_CONFIG_DIR`, `MATRIX_OUTPUT_DIR` | where the evidence goes and where `configs/workload.json` and the paired cases are read; set by the runner |
| `MATRIX_SESSION_ID`, `MATRIX_CANDIDATE`, `MATRIX_HARDWARE_SETUP`, `MATRIX_AD10_PAIRED_CASES` | the AD10 pairing; set by the runner |
| `MATRIX_GATEWAY_BASE_URL` | gateway HTTP base; the surfaces default to `/portal/api/v1/bus/observability`, `/metrics`, `/debug/vars`, `/graphql` and `/mcp` under it |
| `MATRIX_BUS_OBSERVABILITY_URL`, `MATRIX_METRICS_URL`, `MATRIX_DEBUG_VARS_URL`, `MATRIX_GRAPHQL_URL`, `MATRIX_MCP_URL` | override one surface |
| `MATRIX_EXTERNAL_WRITE_POLICY` | the external write policy the gateway was started with; required |
| `MATRIX_SMOKE_WINDOW_SEC`, `MATRIX_SMOKE_POLL_SEC`, `MATRIX_SMOKE_HTTP_TIMEOUT_SEC` | case window (default 60), poll interval (default 3) and per-request timeout (default 10), in seconds |

The operator exports the URLs and the write policy before starting the
runner, which passes its own environment on to the command. Keep the window
plus the gateway start inside `--case-timeout`.

Every cell checks:

- **Transport.** The bus observability `transport_class` is the one of the
  cell: `adapter-direct-ens` or `adapter-direct` for mux cells on ENS or ENH,
  `ens`, `enh`, `udp-plain` or `tcp-plain` for `*-DS01`, and `ebusd-tcp` for
  `EBUSD-ES01`. Mux cells also need the AdapterMux diagnostics in the
  Prometheus text, and the compatibility cells need them to be absent.
- **Write guard.** `MATRIX_EXTERNAL_WRITE_POLICY` is one of the gateway's
  policies, the gateway reports the same policy, and `configs/workload.json`
  carries exactly the read-only client allowlist.
- **Counters.** No monotonic counter (the `_total` series and the frame counts) it reads moves backwards during the window; gauges may fall.

Measures are differences between the first and the last reading. Frame counts
come from the bus observability frame list. Partial frames are not counted.
"Own" frames come from the gateway's source address, which the bus admission
reports as `bus_admission.source_selection.selected_source`. "External identification" frames are passive `07 04` frames from
another source.

| Invariant | Requires |
|---|---|
| AD01 | active frames > 0 and mux `transaction_done` endings > 0 |
| AD02 | active frames > 0, passive frames > 0, passive copies matched to the gateway's own transactions > 0, and self-echo frames (own passive frames not matched) = 0; passive tap connected |
| AD03 | third-party passive frames > 0 and completed passive transactions > 0; passive tap connected |
| AD04, AD09 | the consumer sessions the mux reports at the first reading each observe the discontinuity (their discontinuity count moves by at least 1) and at least one does; upstream reconnects = 1; after the first reading that shows the discontinuity, active frames > 0, mux `transaction_done` endings > 0 and completed passive transactions > 0; no reading after the first shows the out-of-step hold; passive tap connected |
| AD06 | AD01, plus external identification frames > 0 |
| AD07 | AD06; contended selections won by the side with initiator priority > 0 and won by the other side = 0, where the gateway's source and the single external initiator seen in the window are ordered by the lowest low nibble, then the lowest high nibble; losing bids kept and later granted > 0, and losing bids that ended any other way = 0 |
| AD10 | AD01, plus the semantic record and the comparison below |
| PX01, PX02 | AD06, plus no `max_ownership` ending |
| PX03 | AD06, plus no `syn_timeout` ending |
| PX04 | AD06, plus no `active_read_timeout` ending |
| PX12 | AD06, plus no foreign-initiator release of a gateway ownership |
| CX01 | AD06, plus no escape-pair verification failure |
| DS01, ES01 | active frames > 0, plus the semantic record |

The semantic record covers the `system` plane's state fields. For each field
it holds presence, JSON type, unit, value and lineage (plane, path and MCP
contract). The contract is the `meta.contract` object of the MCP answer, with
its name, major and minor version; an absent or malformed one fails the cell.
It also lists the fields where the GraphQL value differs from the MCP value,
and any such difference fails the cell.

AD10 reads `verdict.json` and `smoke_evidence.json` of every case in
`MATRIX_AD10_PAIRED_CASES` from the same output directory. A paired case
must have passed in the same session, candidate and hardware setup, with a
semantic record, or the AD10 cell fails and names it. Each pair is then
judged as separate decisions: presence, types, units, values, lineage, and
MCP/GraphQL parity. Equal values with another lineage are not equivalent.
Values are equal when strings and booleans match exactly. Numbers may differ
by the field's tolerance: 2.0 for the flow and outdoor temperatures, 1.0 for
the 24-hour outdoor average, 0.2 for the water pressure. In inventory order
AD10 runs before the compatibility cells of its setup, so run those first
(for example `--cases ENH-DS01,EBUSD-ES01`), then the rest of the setup. If a
paired case runs again later, run AD10 again too.

Every run writes one JSON object, `smoke_evidence.json`, into the case
directory. It holds the cell, the result, the reasons, the window (readings
judged and readings discarded after the window), the
measures with each requirement and whether it was met, the transport and
write-guard evidence, and, where taken, the semantic record and the AD10
decisions. It holds no URL and no time.

Exit statuses:

| Status | Meaning |
|---|---|
| 0 | pass |
| 1 | fail: a requirement is not met, a surface did not answer, or an AD10 pair is missing or not equivalent |
| 2 | usage error: the case context or a setting is missing or invalid |
| 3 | refused: the invariant cannot be judged (a reserved or planned cell, PL-04 included, a retired or unknown invariant, a row or composition that does not match the cell) |
| 4 | signal missing: the gateway runs the mux but does not publish a series the invariant needs; the reasons and `missing_signals` name it |

A refusal does not contact the gateway. A missing signal is found on the
first reading, before any operator action is announced. The runner records
every non-zero status as a failed smoke step, so a refused cell or a cell
with a missing signal never passes.

AD04, AD07 and AD09 read AdapterMux series besides the ones above. The
gateway publishes the out-of-step hold gauge
`ebus_adaptermux_active_arbitration_held`; it does not publish the others
yet, so these checks end with status 4 on today's gateway:

| Series | Meaning | Read by |
|---|---|---|
| `ebus_adaptermux_session_discontinuities_total{session}` | discontinuity boundaries delivered to each consumer session of the mux | AD04, AD09 |
| `ebus_adaptermux_upstream_reconnects_total` | completed upstream reconnects of the mux | AD04, AD09 |
| `ebus_adaptermux_contended_selections_total{winner}` | SYN boundaries at which a gateway bid and an external bid competed, by winner (`gateway` or `external`) | AD07 |
| `ebus_adaptermux_retained_bids_total{result}` | losing bids kept for a later boundary, by how they ended (`granted` or another ending) | AD07 |

`scripts/adaptermux_v2_smoke_test.py` tests every check offline against the
synthetic responses in `scripts/testdata/adaptermux_v2_smoke/`, served by a
local stub. `./scripts/ci_local.sh` runs it.

### Add-on target commands

On `--target ha-addon` the supervisor starts the gateway and the external
client from their add-on options, and the operator prepares each hardware
setup before its invocation: the adapter, the gateway's southbound transport,
adapter-direct with the proxy listener for the mux cells, and the client.
The harness changes no adapter setting and no add-on option, and never sends
a credential. The start and stop commands therefore start and stop nothing.
They verify:

```sh
matrix-runner --suite adaptermux-v2 --execute ... --target ha-addon \
  --start-gateway 'sh scripts/adaptermux_v2_addon_gateway_started.sh' \
  --start-ebusd 'sh scripts/adaptermux_v2_addon_client_started.sh' \
  --stop-ebusd 'sh scripts/adaptermux_v2_addon_stop.sh' \
  --stop-gateway 'sh scripts/adaptermux_v2_addon_stop.sh' \
  --smoke-command 'python3 scripts/adaptermux_v2_smoke.py'
```

| Command | Checks | Exit statuses |
|---|---|---|
| `adaptermux_v2_addon_gateway_started.sh` | the gateway answers; the bus observability `transport_class` is the one of the cell (the table of the smoke command); for mux cells the health `readiness.proxy_readiness` is `READY`; the startup phase is `LIVE_READY` | 0 ready, 2 usage, 10 no answer, 11 wrong transport class, 12 proxy listener not ready, 13 startup phase not ready |
| `adaptermux_v2_addon_client_started.sh` | mux cells with ebusd or the VRC Explorer: the proxy listener is `READY` and the expvar `adaptermux_session_frame_latency_us_bucket_total` delivery count grows between two readings, so an external session on the proxy listener receives bus symbols; every ebusd cell, `EBUSD-ES01` included: the command port in `MATRIX_EBUSD_COMMAND_ADDR` answers `state` with `signal acquired` | 0 connected, 2 usage, 3 refused (the cell has no external client started before its workload), 10 no answer, 12 proxy listener not ready, 14 no external session receives symbols, 15 ebusd did not answer or answered `ERR`, 16 ebusd has no signal |
| `adaptermux_v2_addon_stop.sh` | writes `gateway_counters_end.json` into the case directory: the Prometheus `_total` samples, the numeric expvar counters (without `cmdline` and `memstats`) and the bus observability counters and frame counts per scope and frame type; a surface that does not answer is recorded as not read | 0 recorded, 2 usage (`MATRIX_CASE_DIR` is not a directory) |

The runner runs `--start-ebusd` and `--stop-ebusd` only for the cells that
use ebusd. The gateway lists no proxy session by client, so the session
check shows that an external session is connected, not which one; on an
add-on setup the operator runs the cell's client as the only proxy client.
The command `state` is the only line sent to ebusd.

The commands read only `MATRIX_*` variables: `MATRIX_GATEWAY_BASE_URL` (or
the surface overrides `MATRIX_HEALTH_URL`, `MATRIX_BUS_OBSERVABILITY_URL`,
`MATRIX_METRICS_URL` and `MATRIX_DEBUG_VARS_URL`), `MATRIX_EBUSD_COMMAND_ADDR`
(the runner does not derive it from `--ebusd-command-addr`: export it in the
runner's environment with the same host:port, which the commands inherit; an
ebusd cell without it ends with usage status 2),
the cell variables set by the runner, and `MATRIX_ADDON_WAIT_SEC`,
`MATRIX_ADDON_POLL_SEC` and `MATRIX_ADDON_HTTP_TIMEOUT_SEC` (default 60, 2
and 10 seconds). The start commands poll until the state holds or the wait
ends and judge the last reading; the client check needs a wait of at least
one poll interval. A URL carrying user information is rejected. They send
HTTP GET only, use no HTTP proxy, print no URL or address, and only the stop
command writes, inside `MATRIX_CASE_DIR`. Keep the wait of both start
commands plus the smoke window inside `--case-timeout`.

`scripts/adaptermux_v2_addon_commands_test.py` tests the ready, not-ready,
wrong-transport and client-absent cases offline against a local stub, and
checks that the commands contain no option write, no adapter request and no
credential variable. `./scripts/ci_local.sh` runs it.

### Write safety

No cell writes to a field device. Every client the harness drives passes an
allowlist of identification and read requests:

- 07 04 identification;
- B509 register read (`0D`, `29`);
- B524 read (`02`/`06` with operation `00`, and `03`);
- B555 timer read (`A3`–`A5`).

These match the gateway's own request-intent classification. Broadcast
targets, initiator targets, and every other request are refused.

- The synthetic contender and the ebusd workload check every request before
  writing the first byte. A single refused request means nothing is sent.
- The ebusd workload accepts only `state`, `info`, `scan ZZ` and
  `hex [-s QQ] ZZPBSBNN…`.
- The VRC Explorer runs only as `scan --transport tcp --preset recommended`
  against a local relay. The relay checks each command line. A refused line is
  answered `ERR` locally and never reaches ebusd, and any refusal fails the
  cell.

No flag overrides the guard. An owner override of the transport gate never
authorizes live writes.

Class W cells are adapter-side actions: `UDP-DS01`, `TCP-DS01` and the AD04
and AD09 cells of the `ENS` and `ENH` rows today, and the plain rows once they
leave PL-01. Immediately
before such a cell's first command, the runner asks the operator to type the
case ID. Without that answer the cell fails and no command runs. An
`--execute` run with a W cell selected is rejected at start unless a
confirmation prompt is available.
