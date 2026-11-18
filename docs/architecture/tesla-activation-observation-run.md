# Tesla Gen3 HSC activation observation run

## Status

This page describes the gateway's `tesla-observe` subcommand: a NO_SEND
observation run for the Tesla Wall Connector Gen3 HSC family. The run produces
local review input for the second-site
[activation evidence worksheet](https://github.com/Project-Helianthus/helianthus-docs-modbus/blob/main/protocols/tesla/activation-evidence-worksheet.md).
It implements the receive side of the
[activation observation hook](https://github.com/Project-Helianthus/helianthus-docs-modbus/blob/main/protocols/tesla/activation-observation-hook-v1.md)
inside the gateway, over the pinned registry's
[read-acquisition lifecycle](https://github.com/Project-Helianthus/helianthus-docs-modbus/blob/main/protocols/tesla/gen3-read-acquisition-v1.md).
Read those public native contracts first.

| Claim | Grade | Separate disposition |
| --- | --- | --- |
| The run transmits nothing: its byte source has no transmit, flush, reset or discovery method | Proven for this software, by construction and focused tests | Not a statement about device behavior |
| A passive observation window cannot make the lifecycle ready | Proven for this software, by construction and focused tests | Not a statement about device behavior |
| Offline controls reject missing, partial, reordered, wrong-direction, late, canceled, expired, prior-generation, closed and reused observations | Proven as offline software policy against the pinned lifecycle | Establishes nothing about firmware |
| Reported `TESLA` / `PASS` activation exchange | Hypothesis | The run neither assumes nor prefills it |
| Token origin, timing, framing and reset repetition at a second site | Unknown | Settled only by owner-reviewed evidence |

The run is not a production activation witness, a qualification decision, a
live acquisition or send authority. The retained owner described in
[Tesla Gen3 HSC retained owner](../tesla-gen3-hsc-retained-owner.md) stays
non-send and unchanged, and the gateway command flags gain no Tesla serial,
activation or write flag. Building a witness is follow-up work that starts
only after the owner has reviewed a filled worksheet.

## Observation hook

`internal/teslaobserve` owns one receive-only byte source per endpoint
instance and follows the hook contract:

- Inputs are explicit and checked before any byte is read: endpoint label
  (`[a-z0-9._-]`, never a path), opaque instance, expected nonzero
  generation, clock epoch, absolute deadline (at most 60 s), requested byte
  count (at most 4096), segment cap (at most 1024) and segment gap. A busy,
  spent, closed or stale-generation endpoint refuses without consuming bytes.
- The result is detached: ordered segments with the exact bytes, the
  `receive` direction and per-byte receipt offsets from the observation start;
  the deadline and return offsets; the termination; and the coverage
  (requested and captured counts, segment count, timing resolution
  `host_read_return`, framing `unknown`, and the fixed limitations). No wall
  clock time is recorded.
- Terminations are `captured`, `deadline`, `canceled`, `closed`,
  `generation_lost`, `read_error` and `segment_bound`. Only `captured`, returned
  before the deadline, is usable. A byte consumed after the segment cap is
  kept as overflow evidence; nothing is trimmed, searched or discarded.
- Segment boundaries are host receipt gaps, not wire frames. Sender
  attribution stays Unknown.
- Close and fencing interrupt a blocked read. One observation is allowed per
  instance, because bytes queued after a window have unknown provenance; an
  independent repeat opens a fresh instance. Bytes buffered by the host before
  the port opened are a recorded limitation (on Linux the opener also discards
  what arrived before its configuration).
- `WithCurrent` rechecks label, instance, generation, observation identity,
  clock epoch, the exact detached bytes and offsets, the deadline and the
  caller's context, then runs the commit under the same mutex that Close and
  Fence take. An observation can be used once.

## Offline lifecycle replay and controls

Each attempt's observation is passed once through the pinned registry
`tesla.Gen3ReadAcquisition` lifecycle. The driver for that pass is the
passive observation. It keeps the label, instance, generation, termination,
receive-only segment and currentness checks, then reports that a passive
observation has no host echo. The lifecycle therefore ends unavailable and
acquisition is refused. A passive window can never make the lifecycle ready.
The lifecycle is configured with the five observation operations
(`common.system_info`, `wc.get_vitals`, `wc.get_lifetime_stats`,
`wc.get_system_info`, `wc.get_load_sharing_state`) and with a control
endpoint that has no transport. The replay records the lifecycle state and
confirms that acquisition is refused when the lifecycle is not ready. It
never calls an acquisition on a ready lifecycle. The replay result is stored
beside the attempt, not in the ordered evidence rows, and is labeled an
offline comparison that is not activation evidence. The run transmits nothing.

Every run also executes thirteen synthetic offline controls. The positive
control is a scripted exchange: the responder greeting, the host echo of that
greeting, and the confirmation are synthetic input through the same lifecycle
and the same control endpoint. That exchange can reach offline readiness and
sends no frames. Every observation-derived control, including the former
fragmented baseline, expects the lifecycle not ready. The negative controls
are: missing, partial, reordered, wrong direction, leading unrelated byte,
segment exhaustion, late return, cancellation before commit, prior
generation, close before commit, expired activation window and a reused
observation. Each keeps its own termination expectation. Every control ends
with zero frames sent.

## Worksheet rows

The evidence file carries the worksheet sections with the worksheet's own
row wording:

| Section | How the run fills it |
| --- | --- |
| Preconditions and observation bounds (nine rows) | Tool facts (method, bounds, NO_SEND allowlist with zero frames sent) are graded Proven; serial format at the device, family version, prior state, other initiators and authorization stay Unknown or Hypothesis from the operator's per-attempt record |
| Ordered native evidence | One row per observed segment, plus overflow and empty-window rows. Bytes, completeness, offsets and gaps come from the observation; direction is receive with sender Unknown; framing is Unknown |
| Independent repeats and negative controls (seven rows) | Initial, recovery and idle rows summarize attempts of that case and stay Unknown pending review; the two offline rows are Proven only when every control and the positive control pass; visibility and contradiction rows list incomplete and differing windows |
| Review disposition (five rows) | Always Unknown; the run never fills review fields |

## Running an observation

The subcommand is selected from argv before any gateway configuration. It
opens the port at 115200 baud, 8 data bits, no parity and 1 stop bit (Linux
only; other platforms record an open failure) and transmits nothing.

The operator starts `tesla-observe` before the charger gets power. The
working hypothesis, graded Hypothesis in the status table, is that the
activation exchange happens once after the charger gets power, and only if
the host answers the greeting. This tool sends nothing, so it does not
expect the confirmation. Starting the tool before the charger gets power
keeps that hypothesized exchange inside the window. Whether the exchange
occurs, when it occurs and whether a reset repeats it stay Unknown pending
owner review. A window that captures no exchange is retained as evidence
for that review and is not dismissed as a missed exchange. Starting the tool
does not power the charger and does not answer the greeting. Each attempt
closes its source when the window ends. A later repeat opens a new source
after any pause and does not observe the exchange from the earlier power-up.
The command writes nothing until every attempt has finished, so it does not
report when a source has opened or when its window is active. A preceding
attempt may also end before its deadline, once it reaches the byte or
segment limit, so the configured window and pause are not a schedule for
the next power-up. Starting the invocation does not open the window. Before
it opens the port, the command resolves and locks its outputs and reads and
checks any existing evidence file. It then opens the port, applies the
receive-only settings and flushes both queues, which discards every byte
received before that flush. None of these steps is reported, so the start of
the invocation is not a signal to give the charger power. Use one repeat for
a startup. Before giving the charger power, confirm on the observation host
that the running `tesla-observe` process itself holds the serial device open,
for example by listing the processes that have that device open. The
settings and the flush follow the open directly, without a wait, so wait at
least one more second after that confirmation. Then give the charger power,
well before the window deadline, which is counted from the open. If the
invocation has already exited, it holds no open window for this power-up:
read its output and do not give the charger power for it. Do not power the
charger again for a
later repeat of the same invocation. Record another startup by starting
another invocation before that power-up. The `-window-bytes` flag default
remains 64. A sufficient capture count is not established and must be
measured.

```sh
helianthus-gateway tesla-observe \
  -serial-path /dev/ttyUSB0 \
  -case initial -repeats 1 \
  -window 30s -window-bytes 64 -segment-cap 64 -segment-gap 2ms \
  -prior-state unavailable -prior-state-basis "observation started before the charger received power" \
  -other-initiators unknown -action-time-authorization recorded \
  -out /var/lib/helianthus-observe/evidence.json \
  -worksheet-out /var/lib/helianthus-observe/worksheet.md
```

- `-case` is `initial`, `recovery` or `idle`. The operator starts
  `tesla-observe` before the charger gets power, and performs any power-up
  or reset separately, under its own action-time authorization; the run
  never does. The tool sends nothing and does not expect the confirmation.
  The worked example uses one repeat, so its prior-state basis matches that
  single startup. Each repeat is a new window on a freshly opened source.
  Under the working hypothesis, the exchange from one power-up is visible
  only in the window that was already open. A later repeat is not another initial session of that
  power-up, and nothing is printed when that later window opens. Do not
  coordinate a fresh power-up with a repeated window. Record another
  initial session by starting another invocation before a fresh power-up;
  `-append` continues the labels and stores that invocation's prior-state
  basis only on the attempts it adds. One invocation records its
  prior-state basis on every attempt, so that basis has to be true for
  every attempt the invocation records.
- `-append` adds attempts to an existing evidence file. Labels continue
  (`a01`, `a02`, ...). The rows are rebuilt and the offline controls are
  rerun. Without `-append` an existing file is never overwritten. Evidence and
  worksheet paths must differ, including relative paths, symlinks and hard links.
  Aliases are refused before the serial port is opened. Output symlinks are
  resolved before replacement, preserving the links and updating their targets.
  Dangling output symlinks, including dangling parent-directory symlinks, are
  refused before collection. An ordinary absent output file remains allowed.
  Every run holds exclusive directory locks at each resolved output path plus
  `.lock`, acquired in sorted path order across the read, observation and
  publication steps. Sharing either evidence or worksheet with a concurrent
  run is refused before opening the port; retry after that invocation exits.
  Append also refuses any existing output with more than one hard link before
  opening the port, because distinct hard-link names cannot share that lock.
  An abrupt process exit can leave lock directories behind. Before removing
  them, confirm that no observation invocation is still running.
- If every new attempt fails to open or ends with a read error, the command
  saves the attempts and worksheet and exits `1`. Cancellation also exits `1`.
  A quiet window ending at its deadline is a valid observation, and offline
  readiness refusal alone does not fail the run.
- An existing file is also checked for consistency before the port opens, not
  only for shape. Each attempt's observation metadata must be something this
  observer could have written: the case; the instance, generation, identifier
  and clock epoch; the `receive` direction of every segment; receipt offsets
  that stay within the return offset, never run backwards and split segments
  exactly at the segment gap; captured, segment and overflow counts that agree
  with the slices; a termination that the counts, the deadline and the cause
  allow; the fixed coverage fields and limitations; and a lifecycle replay that
  matches the observation. Rows are rebuilt from this metadata with fixed
  receive wording, so a hand edit such as a segment marked `transmit` would
  leave the attempt and its rows in conflict. Such a file is refused with exit
  code `2` and left unchanged. `closed` and `generation_lost` are not accepted
  in a persisted attempt, because the run never closes or fences the endpoint
  it is observing with.
- `-prepare` runs the receive-only opener once and exits: it opens the port,
  applies the raw settings, flushes both queues and closes the port. It reads
  nothing, observes nothing, records no attempt and writes no evidence or
  worksheet file, so `-out` is not required. It accepts only `-serial-path`
  (and `-prepare` itself). A missing `-serial-path`, a positional argument or
  any other explicitly set flag, including `-out`, `-append`,
  `-worksheet-out`, `-case` and the observation bounds, is refused with exit
  code `2` before the port opens, so a preparation run can never be mistaken
  for a recorded observation. It exits `0` when the port was prepared and `1`
  when opening or closing it failed; other platforms fail to open. See
  [Receive-only boundary on Linux](#receive-only-boundary-on-linux) for when to
  run it.
- `-window-bytes` defaults to 64, and the example passes that default. A
  sufficient capture count is not established and must be measured.
  `-window-bytes 11` limits a window to the comparator length. The default
  of 64 keeps leading and trailing bytes around that comparator length.
  Unread bytes outside a window are not evidence of absence.
- The serial path is used only to open the port. It never appears in the
  evidence, and file errors are redacted before they are recorded. Operator
  text is limited to 200 printable ASCII characters and must not contain
  names, credentials, serials, locations or times.

On Linux, the receive-only opener opens the device read-only, applies raw
115200 8N1 settings with echo and software flow control disabled together in
its first termios update, checks them by reading them back and then flushes
both queues. The read-back must match every requested input, output, local and
control flag, plus the minimum read size and read timeout; any mismatch fails
the open before flushing. It never opens a bidirectional stream. What this does
and does not prove is described in
[Receive-only boundary on Linux](#receive-only-boundary-on-linux).

Every output is staged and synced as an owner-only (`0600`) file before
publication, including an evidence-only first run. New outputs are published
without replacing an existing entry; interrupted staging never exposes a
partial file at the final name. When a worksheet is requested, both complete
files are staged and synced before either output is published.
A staging failure leaves the previous outputs unchanged. If worksheet
publication fails, the command restores the previous evidence or removes the
new evidence for a first run. If the filesystem also refuses restoration, the
run fails and retains the staged recovery copy for manual recovery. The two
paths do not form a crash-atomic transaction: interruption between their
publications can leave different generations, so verify their consistency
before using outputs from an interrupted run.

After publication, both parent directories are synced before reporting success.
A directory-sync failure reports a run failure and retains the published pair
for recovery; its crash durability is then unknown.
They are local review input in the owner's private evidence lane. They are not
repository material, and
public worksheet entries must be sanitized from them under the worksheet's
publication rules.

### Receive-only boundary on Linux

The receive-only guarantee at the bus is a hardware boundary that the operator
owns: hold the transceiver's driver-enable inactive, or leave its transmit
line unconnected. Software on the host cannot prove that no byte left the
UART. The measures below are defence in depth around that boundary:

- the device is opened read-only, so this process has no write path to it;
- the first termios update is a single raw configuration that disables echo
  and software flow control, and it is read back and checked;
- immediately after the check, both queues are flushed (`TCIOFLUSH`). The
  flush discards every byte that was received or queued before the
  configuration, so those bytes, which were handled under the previous
  attributes and have Unknown provenance, never enter the evidence window. It
  discards local buffers only and sends nothing; the source the observer holds
  still has only receive and close methods;
- the preparation run (`-prepare`) is made before the bus is connected, and no
  other program opens the port between the preparation and the observation.

The residual case is exact. User space cannot set terminal attributes before
`open(2)`. When a port has never been prepared, its first open starts with the
attributes that the driver kept from the previous user. If those include
`ECHO` or `IXOFF`, a byte that arrives between the open and the first termios
update can make the driver transmit an echo or a flow-control character
despite the read-only descriptor. The flush removes such a byte from the
evidence but cannot undo a transmission, and the opener cannot prove that none
happened. Only an unprepared port, and only that window, is affected.

The preparation run closes the window. Linux serial drivers (the UART core and
USB serial drivers) keep the last terminal attributes across close and restore
them on the next open. A preparation run made before the RS-485 pair is
connected to the bus therefore leaves the port with echo and software flow
control already disabled; nothing is connected for its own window to reach.
The later observation open inherits that state, so the window between open and
configuration carries no inherited transmission. Pseudo terminals reset their
attributes on close, so this persistence is not shown by the pseudo terminal
tests. The tests show the termios update, the flush and the absence of echo
after configuration, and the command test shows that the preparation run opens
and closes the port through the same opener. Re-enumeration or replugging of a
USB adapter, a reboot, a driver reload and any other program that opens the
port and changes its settings can replace the retained attributes, so repeat
the preparation after any of them.

Required run order:

1. Prepare the port with the bus pair not connected:
   `helianthus-gateway tesla-observe -prepare -serial-path <serial-device>`.
2. Connect the RS-485 pair to the bus, with the hardware boundary above in
   place.
3. Start `tesla-observe` before the charger gets power, with one repeat for
   that startup, using the same `<serial-device>` and with no other program
   opening it in between. The start of the invocation is not a signal to
   give the charger power, because the outputs are checked first and the
   flush that follows the open discards every byte received before it.
4. Confirm on the observation host that the running `tesla-observe` process
   itself holds `<serial-device>` open, then wait at least one more second.
   If the invocation has already exited, read its output and do not give the
   charger power for it.
5. Give the charger power for that one window, well before the window
   deadline, which is counted from the open. The tool sends nothing and does
   not expect the confirmation. The command writes nothing until the run
   finishes, so a later repeat does not show that a new window is open. Do
   not power the charger again for a later repeat. Start a separate
   invocation before the next power-up.

## Building for ARM Linux boards

The run is part of the gateway binary. For 32-bit ARMv7 and 64-bit ARM64
Linux single-board computers, such as the Olimex Linux board families, build
static binaries:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=arm GOARM=7 \
  go build -trimpath -ldflags='-s -w' -o helianthus-gateway-linux-armv7 ./cmd/gateway
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
  go build -trimpath -ldflags='-s -w' -o helianthus-gateway-linux-arm64 ./cmd/gateway
```

Pick the binary that matches `uname -m` (`armv7l` or `aarch64`). Any
installation, wiring or device access requires the operator's separate
approval at action time.

Observation is a supervised manual operation. Do not install an enabled
systemd unit, timer, restart policy or other automatic observation launcher.
An unattended start after a reboot, replug or driver reload cannot establish
that preparation happened while the bus was disconnected. A marker file or an
`ExecStartPre` preparation command cannot establish that physical condition
either; preparation while connected still has the inherited-attributes window.

Before each manual invocation, the operator must enforce the required run
order above: keep the bus pair disconnected during preparation, establish the
hardware receive-only boundary, then connect and observe without an intervening
port opener or device reset. If that sequence cannot be established, leave the
pair disconnected and do not observe. After a reboot, replug, driver reload or
another port opener, disconnect and repeat the sequence before any further
observation. The executable cannot verify wiring or retained device state.

Use a new output path for a first run. Add `-append` only for an existing
reviewed evidence file; repeating the physical preparation procedure does not
by itself authorize another observation.

## Limitations

- Only the receive side of one port is visible. Direction attribution,
  competing initiators and on-wire timing stay Unknown.
- No framing or integrity diagnostic is applied, and activation bytes are not
  assumed to be RTU frames.
- The offline replay's currentness check precedes the registry's own
  readiness commit; they do not share one atomic boundary. Providing that
  boundary is part of the witness follow-up.
- The run gives no qualification, readiness or acquisition result for a
  device. The live read path stays hard-stopped until a witness exists.
