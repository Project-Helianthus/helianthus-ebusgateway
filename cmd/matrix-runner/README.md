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
`matrix_inventory_version` is `1`. Adding, removing or renaming a cell, or
changing a cell's assertion, category or workload, bumps it.

Reserved cells are written without executing any command. When a cell matches
more than one category, the first matching row below wins:

| Category | Outcome | Reason | Cells |
|---|---|---|---|
| BI-02 | `blocked-infra` (`infra_reason`) | `uart_direct_hardware_absent` | every `UART*` cell |
| PL-01 | `planned` (`planned_reason`) | `mux_southbound_plain_not_implemented` | every `UDP-*`, `TCP-*` grid cell |
| PL-02 | `planned` | `mux_local_responder_not_implemented` | `*-PX09`, `*-PX10`, `*-PX11` |
| PL-03 | `planned` | `mux_raw_symbol_forwarding_not_implemented` | `ENS`/`ENH` × AD05, AD08, AD12 |
| RS | `planned` | `cell_awaiting_respecification` | `ENS`/`ENH` × AD04, AD07, AD09, PX05–PX08 |

RS is not a category the gate accepts. Its cells wait for the
re-specification on
[#1378](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1378)
(the arbitration model changed and the adapter-side procedures are not written
yet), so a report that contains them does not pass. Re-specifying a cell moves
it out of RS and bumps the inventory version.

The expected-failure inventory is fixed: XF-01 on `TCP-CX01` only.
`--expected-failures` and `--expected-failures-file` are rejected for this
suite. The 27 executable cells are `ENS`/`ENH` × AD01, AD02, AD03, AD06, AD10,
PX01–PX04, PX12, CX01, plus the five compatibility cells.

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

Class W cells are adapter-side actions: `UDP-DS01` and `TCP-DS01` today, and
AD04, AD09 and the plain rows once they leave their categories. Immediately
before such a cell's first command, the runner asks the operator to type the
case ID. Without that answer the cell fails and no command runs. An
`--execute` run with a W cell selected is rejected at start unless a
confirmation prompt is available.
