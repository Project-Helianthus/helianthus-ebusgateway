# Mux raw-symbol and echo continuity

Status: contract and current-code inventory for issue #1142, partly
implemented by the parent #1003 slice summarized under "Implementation status"
below. The inventory tables describe the code before that slice. No
external-client compatibility success is claimed until the open items listed
there are closed.

## Implementation status (issue #1003)

Implemented in `internal/adaptermux` (see `raw_symbols.go`) and covered by the
deterministic regressions in `raw_symbol_continuity_test.go`:

- Raw view at the upstream boundary. When the upstream supplies the optional
  `ReadEventWithRawSymbol` capability, every observed RECEIVED wire symbol is
  enqueued for every external session in upstream order, one
  RECEIVED per wire symbol: `A9 00` and `A9 01` reach sessions as two
  symbols, the lead before its suffix exists, and a structural `AA` as one.
  The logical item a symbol completed (with `WasEscaped`) is then processed
  for phase tracking, passive reconstruction and the gateway active path,
  without a second session delivery. Raw symbols are never regenerated from
  logical bytes; an upstream without the capability keeps the previous
  logical session delivery. The v8 classifier judges the logical item and,
  in enforce mode, drops its raw symbol from the session view with it.
- Independent write normalization. External sessions pass raw SEND symbols
  through unchanged. The gateway active path advertises logical writes only
  when the upstream SEND contract is raw-symbol (ENH/ENS) and its receive side
  is logical; the mux then encodes a logical payload `A9` as `A9 00` and a
  payload `AA` as `A9 01`, sends a structural `AA` only when explicitly
  marked, and verifies every symbol: the first eligible raw echo must be the
  lead (an unsolicited structural SYN is not eligible), the symbol
  immediately after it must be the suffix, and the pair is then verified
  through the escaped logical echo. A mismatched lead or a malformed suffix
  fails the write as a collision; a later well-formed pair never
  acknowledges it. The pair's two SEND requests go out back to back,
  because waiting a round trip for the lead's echo could exceed the upstream
  decoder's 32 ms escape-pending cap and split a correct pair. Lead and
  suffix are each committed under the arbitrator lock, which every ownership
  transition takes, after checking the reset, reconnect or close fence: a
  commit that checks after a boundary sends nothing and the write fails
  once, and a boundary's ownership release waits until an in-flight write
  returned. Close and reconnect close the upstream before that teardown, so
  a stalled write fails instead of holding it. The pair verifier sees every
  observed raw symbol in wire order before the v8 session-view filter, so
  an `AA` that filter drops between lead and suffix still fails the pair.
- Grant visibility. The external winner's STARTED is queued on the goroutine
  that observed the grant, behind older RECEIVED frames. All session frames
  retain FIFO order with or without pacing. STARTED and FAILED reserve no
  pacing slots but cannot overtake older symbols. The F-34/F-35 abandon
  watchdog starts after successful STARTED delivery, bound to its grant
  generation; wire-idle SYN grace and maximum ownership still bound a grant
  whose STARTED never reaches the client.
- Session output pacing reserves one slot per RECEIVED wire symbol, independent
  of encoded ENH TCP length. Control frames reserve none. A three-frame FIFO
  lookahead containing more than two RECEIVED frames triggers a snapshot flush
  without pacing sleeps. The snapshot includes frames already in the send
  channel; later arrivals retain FIFO order and are evaluated after it finishes.
  Writer lookahead uses a fixed three-frame array; the send channel reserves
  that space within the existing 8,192-frame session budget. Overflow retains
  disconnect backpressure. Each successful flush Write rebases cadence at
  completion, so the next ordinary symbol waits one period after recovery.
  Flush counts use `adaptermux_session_backlog_flushes_total`, separately from
  `adaptermux_session_frame_latency_us_bucket_total` latency buckets.
  These are host egress scheduling rules; native symbol timing is Unknown.
  The required public companion is
  [session output delivery](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/adaptermux/session-output-delivery.md);
  it must be published before or with this change.

Open items: the raw view requires the matching helianthus-ebusgo transport
capability at the pinned dependency; a raw-symbol client that waits for each
echo adds its own round trip between an escape lead and its suffix, and when
that gap exceeds the upstream decoder's 32 ms escape-pending cap the logical
view splits the pair (the raw view stays exact), which needs a decision on
that cap; the public native ENH document
([`protocols/enh.md`](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/protocols/enh.md))
assigns TX escaping to the host and defines each SEND as one physical wire
byte, which is the transparent single-symbol SEND this slice relies on, so no
escape encoding is expected from the adapter;
bounded-backpressure loss reporting, native timing, the close/replacement
race (F01/J21) and independent review remain outside this slice.

## Authority and evidence

This page specifies a gateway boundary, not new native eBUS meaning. Native
protocol documentation must establish wire encoding, arbitration and timing
before implementation is accepted. No native ENH protocol document is
provisioned in this worktree. The pair mappings below are the supplied parent
issue's examples and the current gateway's documented assumptions; they are
not independently validated native protocol facts. A matching public native
contract remains an acceptance requirement for implementation. No native
documentation link is invented here.

Proven means directly supported by the inspected public repository source;
Hypothesis means a proposed behavior or an issue-reported explanation awaiting
implementation/native validation; Unknown means unavailable evidence, notably
native symbol times. These grades are independent of acquisition, admission,
qualification and promotion. A preserved byte or matched echo grants none of
those states. All interfaces remain pre-stable before Helianthus 1.0.

The following public source paths were checked against this worktree. Symbol
names identify claims without implying that comments or test names prove more
than the implementation and assertions actually show.

| Source | Proven current behavior / limit |
| --- | --- |
| [Mux](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/adaptermux/mux.go) | `readUpstream` prefers `StreamEventReader`; its byte-reader fallback has no escape evidence. `onReceived(symbol, wasEscaped)` handles a logical byte, classifies unescaped `SymbolSyn` separately, and calls `deliverToSessions` with the byte only. `PassiveEvent` has `WasEscaped` and host `ObservedAt`. |
| [Active path](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/adaptermux/active_path.go) | `ReadByteWithEscape` and `ReadEvent` preserve the queued flag; `ReadByte` loses it. `SignalNextWriteIsStructuralSyn` marks one next write. `BytesAreUnescaped` delegates receive representation to upstream; its current comments also couple this to write behavior. |
| [Sessions](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/adaptermux/session.go) | `sessionFrame` carries kind, payload and host enqueue measurement, no raw pair or escape flag. `writeFrame` emits one ENH RECEIVED message per payload, using short/long ENH encoding. ENH message encoding is not eBUS escape-pair restoration. `handleSend` requires current ownership. |
| [Echo tracker](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/adaptermux/echo_tracker.go) | `expectedStructural` accompanies gateway expectations. `matchEchoWithTime` initially matches byte equality; it uses received provenance when updating the drained-queue sentinel. Thus the tracker alone does not prove the proposed full eligibility rule below. Its header's per-session description is stale relative to current session code. |
| [Echo assertions](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/adaptermux/echo_passthrough_test.go) | Own-echo, burst-order and full-frame tests assert logical-byte delivery. `TestExternalSessionCRC_0xAA_Handling` feeds `deliverToSessions` directly; it does not prove raw escape restoration. `TestExternalSession_MidFrameDisconnect_PhaseReset` exercises a tracker, not actual session replacement. |
| [Passive bridge](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/adaptermux/passive_transport.go) and [assertions](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/adaptermux/passive_transport_test.go) | `deliver` preserves logical-byte provenance and maps reset/disconnect/connect to reset events. Tests cover the reconnect pair and bounded reset timeout/drop; overflow assertions show a retained symbol followed by a reset, not complete loss accounting. |

## Distinct representations and fields

The following is a conceptual contract, not an available public event API.
Preserve upstream native evidence alongside the logical projection. Never
regenerate raw evidence from logical values and call it observed evidence.

| Field / distinction | Required meaning | Current gap |
| --- | --- | --- |
| Upstream continuity identity and event order | Distinguish connection lifetimes; order raw symbols and control boundaries within one lifetime. A replacement cannot inherit pending decode or echo state. | No unified raw-symbol continuity record is exposed by this mux. Existing arbitration generations are narrower safeguards. |
| Raw symbol | One observed eBUS wire symbol, distinct from the bytes encoding its ENH message. Preserve each escape lead/suffix separately and in order. | Logical `onReceived` entry cannot expose a lead before decoding completes. |
| Logical byte and escape provenance | Project a complete valid pair once, recording its observed escaped origin. Do not infer origin from value alone. Incomplete/invalid pairs stay native evidence and cannot become fabricated payload. | Gateway/passive retain `WasEscaped`; external `sessionFrame` does not. The byte-reader fallback cannot prove an unescaped origin merely because its boolean defaults to false. |
| Structural SYN | Under the source's encoding assumption, `AA` with known unescaped provenance is a boundary; escaped logical `AA` is payload, including when it occupies a CRC position. | Existing flagged paths distinguish these; a byte-only projection cannot. |
| STARTED initiator | A grant/control result carries the observed initiator and belongs to the originating live request, session and upstream lifetime. It is separate from post-arbitration SEND echo. | Existing grant/mismatch/absorb paths and session result notification must be preserved, not replaced with value-only routing. |
| Terminal/reset kind | Transaction terminal SYN, failed arbitration, session close, upstream reset and reconnect are distinct causes. A terminal ACK is not itself the trailing structural SYN. | Passive reset projection intentionally collapses several causes; loss details are absent there. |
| Native time / host time | Native time requires upstream evidence and a declared clock domain. Host receive, enqueue, write and echo RTT measurements describe host operations only. | `onReceived` uses `time.Now`; session enqueue latency and pacing do not recover wire pair intervals. Missing native time/interval is Unknown, never zero or host time relabeled. |
| Echo eligibility | A pending write must belong to the current upstream lifetime, live session/grant and next expected symbol/representation, including structural versus payload origin. Equal value is insufficient. | Gateway matching plus mux guards are distributed; external sessions have no per-session echo tracker. End-to-end raw eligibility is not established. |
| Loss boundary | Bounded delivery must report discontinuity or terminate the affected stream; report known loss scope/count, otherwise Unknown. No contiguous-frame claim may cross an unresolved gap. | Reset markers can be dropped; there is no complete public loss report. |

## Receive, write and echo obligations

External ENH sessions require the raw symbol view: emit each observed lead's
RECEIVED immediately in stream order without waiting for a suffix. Internal
gateway and passive reconstruction use a separately decoded logical view with
provenance. Apply existing gateway ownership and passive self-echo filtering;
raw preservation does not widen consumer admission. An external owner still
receives its post-arbitration SEND echoes, as do other admitted observers.

Write normalization must be specified independently of receive normalization.
A raw external SEND lead is one wire symbol; do not buffer it awaiting another
SEND or escape it again because a receive stream is logical. A gateway logical
reserved byte requires the native write contract's raw encoding and verification
of every resulting symbol. A structural write is explicitly marked, not guessed
from `AA`. This page does not certify current reserved-byte gateway writes.

An echo observation can be visible to observers without being eligible to
satisfy a pending write. An unsolicited structural SYN must not satisfy a
pending payload `AA`; an escaped payload `AA` must not satisfy a structural
terminator. Only the next eligible expectation is consumed, once. Missing origin
or continuity evidence must not be treated as a positive match. No new receipt,
idempotency ledger or authorization mechanism is specified.

## Boundaries and ordering

These are proposed continuity obligations; the current column prevents treating
them as implemented guarantees. They constrain ordering, not scheduler design.

| Transition | Required output/state boundary | Current behavior / remaining proof |
| --- | --- | --- |
| Matched STARTED for external owner | Winner receives `STARTED(initiator)` once, no duplicate RECEIVED initiator. Other external sessions and passive receive the initiator once; gateway active receives no external transaction bytes. | `readLoop`, `handleArbitrationResponse`, `completeArbitrationGrant` and `handleStart` implement this routing. STARTED is enqueued at grant observation; delivery preserves its FIFO position behind older RECEIVED frames. The abandon watchdog starts after successful STARTED egress. |
| Matched STARTED for gateway | Gateway arbitration call completes; no fabricated duplicate source echo. External observers receive the initiator; passive self-echo suppression remains. | Current routing in `readLoop` and source preloading in `completeArbitrationGrant`; raw reserved-byte follow-through remains parent work. |
| Final ACK then structural SYN | Retain transaction ownership through final ACK; process trailing structural SYN as terminal before next grant, preserving observer order. Payload `AA` does not terminate. | Full-frame echo assertions exercise trailing SYN release; they do not establish all raw/provenance cases. |
| External close / replacement | Retire old pending START, writes, decode fragments, output and timers before any successor can claim them. Replacement is a new session even with the same initiator. Old echoes may remain bus observations, never successor acknowledgments. Other consumers continue unless upstream itself breaks. | `RemoveSession` removes arbitration state, cancels pending START/watchdogs and closes the old session. `AddSession` allocates a new ID. This is not proof of every queued-send/close race; F01/J21 own that fix. |
| Upstream RESETTED | Invalidate pending decode/echo/grant state; place reset/discontinuity before post-reset symbols in each affected consumer's view, or close that view. Do not join fragments across the boundary. | `handleReset` resets phase/echo/arbitration, drains active queue then enqueues reset, emits passive reset and broadcasts external RESETTED. Notification/drop/concurrent-queue limits prevent claiming unconditional delivery. |
| Disconnect / reconnect | End the old stream, cancel pending work; connection success starts a new decoding/echo lifetime. Gateway sees reset, passive sees disconnect and connect boundaries, external retained sessions receive RESETTED before new data or are closed. No old suffix completes a new pair. | `reconnect` clears gateway echo/phase, fails pending arbitration and drains active queue. Callback-free recovery emits passive disconnected/connected and broadcasts RESETTED after connect; managed recovery delegates replacement. No new recovery design is implied. |
| Bounded backpressure | Keep queues bounded. Record consumer-specific discontinuity with cause and known/Unknown loss extent; reset reconstruction before accepting a complete frame. If a boundary cannot be delivered within the existing bound, terminate/fence that stream rather than claim continuity. | External receive overflow schedules removal; reset enqueue waits on space or close, broadcast has a deadline. Passive reset waits at most 100 ms; overflow attempts marker insertion but concurrent refill can lose it. Active enqueue can log/drop. Reliable loss reporting is proposed, not present. |

Maintain already delivered order; a reset may invalidate queued old logical
bytes as current active draining does, but the loss must not be described as
successful delivery. Session delivery preserves cross-kind enqueue order,
including STARTED/FAILED, RECEIVED and control frames, in every pacing mode.
Grant abandonment is timed from successful STARTED egress. A RECEIVED frame consumes one pacing slot irrespective of ENH
encoding length; arbitration notifications consume none. A backlog flush rebases
at successful Write completion. None of these host operations proves native
symbol timing. Do not erase retained valid last-known fields on a partial
transaction failure; invalidate only the affected in-flight reconstruction/currentness.

## Synthetic traces: required consumer outputs

All symbols and session labels below are synthetic, with no timestamps. `R(x)`
means one ENH RECEIVED message whose payload is raw symbol x, not literal TCP
bytes. `L(x,e)` means one logical byte with escaped flag e; `B` means a reset or
discontinuity boundary. Assume admitted consumers, available queue space, and
known raw provenance. Native time and inter-symbol delay are Unknown in every
row. These are reasoned contract examples, not executed tests or protocol proof.

| Input / context | Gateway active | Passive reconstruction | External sessions | Current gap / discriminating error |
| --- | --- | --- | --- | --- |
| External owner S; upstream raw `A9 00` | No external-owned bytes. | One `L(A9,true)` after suffix. | S and monitor M: `R(A9)`, then `R(00)`; lead available before suffix. | Current logical fanout emits one RECEIVED A9; value-only re-expansion cannot prove original lead timing. |
| External owner S; raw `A9 01`, then raw `AA` | No external-owned payload; structural SYN follows existing active gating. | `L(AA,true)` payload, then `L(AA,false)` structural boundary. | S and M: `R(A9), R(01), R(AA)` in that order. | Current session path collapses the first pair to one RECEIVED AA. Dropping provenance merges equal values with different meanings. |
| Gateway owns; pending logical payload `AA`; raw `AA`, then `A9 01` | First structural SYN cannot satisfy payload echo; decoded `L(AA,true)` is eligible only for the live expectation. | Payload self-echo filtered; structural SYN follows existing boundary filtering. | Every external observer: `R(AA), R(A9), R(01)`. | Byte equality alone consumes the wrong echo. Raw gateway reserved-symbol verification still needs implementation tests. |
| STARTED carries initiator `31` for external S; later SEND `42`, echo raw `42` | No external-owned data. | Initiator `L(31,false)`, then `L(42,false)`. | S: `STARTED(31), R(42)`; M: `R(31), R(42)`. | Do not synthesize duplicate `R(31)` for S or suppress S's post-arbitration echo. |
| S SENDs raw `A9` and waits for echo before SEND `00`; upstream echoes each | No external-owned data. | No logical byte after lead alone; `L(A9,true)` only after suffix echo. | S and M receive `R(A9)` before S sends suffix, then `R(00)`. | Waiting for logical decode creates the parent issue's dependency cycle. Re-expansion after decoding cannot fix it. |
| S has sent raw lead `A9`, with echo pending; S closes; successor T requests the same initiator `31`; old echo arrives | No acknowledgment for T or unrelated gateway write. | Old symbol only if admitted as observation; no fabricated complete logical byte. | S receives nothing after close. M can receive observed `R(A9)`; T, if already admitted, can observe it but cannot count it as its SEND echo or grant. T needs its own STARTED and write. | Session identity and grant/lifetime identity must discriminate even with equal initiator/byte. Close-race proof remains F01/J21-owned. |
| Old upstream has partial lead `A9`; disconnect, reconnect, new raw `00` | Reset/error boundary before newly eligible logical data; old expectation retired. | `B(disconnect), B(connect)`, then new-stream `L(00,false)`. Never decoded A9 across reconnect. | Retained S/M see RESETTED before new `R(00)`, or connection close; no replayed old lead. | Managed replacement may close sessions; current bounded/drop paths do not prove guaranteed boundary visibility. |
| Passive queue capacity two contains `01,02`; incoming `03` overflows | Unaffected unless its own queue loses data. | Current single-producer example yields `L(02,false), B`; both 01 and 03 lost. Discard partial assembly; no complete-frame success. | S/M unaffected if their queues have space. | This matches the existing overflow assertion only; racing producers can lose B. Contract requires observable loss or stream termination, with count Unknown if not measured. |
| Final ACK `00`, structural `AA`, upstream reset, fresh `42` | Eligible ACK, terminal SYN per active gating, reset before fresh eligible data; no old echo reused. | For external-owned traffic: `L(00,false), L(AA,false), B, L(42,false)`; gateway self-echo filtering still applies. | Connected S/M: `R(00), R(AA), RESETTED, R(42)`, or close at failed boundary delivery. | Do not interchange terminal SYN and reset, release at ACK, or merge fresh data into an old frame. |

## Acceptance still outside this slice

Review of these fields, transitions and examples is documentation evidence only.
No artificial prose tests are added. The existing local CI and CI workflow
discover Go tests with `go test -race -count=1 ./...`; neither registers this
page's focused readability command or a semantic contract checker.

Parent implementation still requires raw upstream availability, independent
read/write normalization, per-symbol SEND echo progress, reserved-byte active
requests, grant visibility under queued passive output, deterministic regression
coverage, applicable transport/local gates and independent review. Native
protocol validation, full CI, hardware matrix and live smoke are not established
by this page. No predecessor implementation or open PR is needed to review this
contract. F01/J21 retain close-fix ownership; G5 D1-D5/O8 remain J01-J17-owned.
No scheduler, recovery, principal boundary, receipts or ledger is redesigned.
Preserve existing admission/trust, native evidence and partial last-known-good
state. MCP envelopes, deterministic snapshots and GraphQL parity remain intact;
historical v1 names do not require a new major or experimental namespace. No
authorization gates are introduced before 0.9.
