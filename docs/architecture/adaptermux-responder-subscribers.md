# AdapterMux registered target-address subscribers

Status: design for
[#991](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/991).
No code implements it yet. Every rule below is a software decision for the
implementation slices listed at the end; none of it is a protocol fact or a
hardware qualification. Wire timing of the responder path is **Unknown**.

## Purpose

A session of the AdapterMux can register an eBUS target address. When a
transaction on the bus names that address as its destination, the mux gives
that session the responder slot of the transaction. The session does not
arbitrate: the initiator keeps ownership of its transaction, and the slot only
lets the session supply the target's part of it (command ACK and response).

## Sources

The protocol contract comes first and this page cannot redefine it:

- [eBUS overview](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/protocols/ebus-services/ebus-overview.md):
  frame layout, ACK `0x00` and NACK `0xFF`, CRC coverage, escaping, SYN while
  waiting as a timeout boundary, command NACK and response NACK repetition.
- [ENH](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/protocols/enh.md):
  command IDs, the unknown-command contract (explicit `ERROR_HOST`), SEND as
  one physical wire byte, RECEIVED as one physical wire byte.

Scheduling and timing rules come from two accepted pages:

- [Local target emulation timing contract](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/local-target-emulation-timing-contract.md):
  echo-driven visibility (rule 1), an explicit and bounded responder window
  that does not transfer initiator ownership (rule 2), and rejection of
  detached late responses with a counter (rule 3).
- [Proxy wire-semantics decisions](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/proxy-wire-semantics.md):
  SYN while waiting closes the exchange, and a child-backed local target is not
  timing-faithful by default.

The proposed
[local responder registration contract](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/api/ebus-local-responder.md)
(Hypothesis) supplies the identity discipline used here: one admitted
registration per address, exact-key cleanup, provisional selection that emits
nothing, and one wire authority that cannot grant initiator ownership and
target output at the same time.

The code read for this design is `internal/adaptermux` (`doc.go`, `session.go`
with `handleMessage`, `handleSend`, `handleStart`, the session writer and its
pacer, and the mux send loop) and the `transport.ResponderTransport`
interface of the eBUS transport module, whose `SendResponderBytes` writes
physical wire bytes through the same SEND path as an ordinary write without
any START.

## 1. Registration and unregistration

### Wire form

ENH defines host commands `0x0`..`0x3` and adapter replies `0x0`..`0x3`,
`0xA`..`0xC`. The session protocol of the mux adds a Helianthus extension in
unused command nibbles. Each frame uses the ENH encoded two-byte form
(`0xC0 | cmd << 2 | data >> 6`, `0x80 | data & 0x3F`); the short form is never
used for them.

Session to mux:

| Nibble | Name | Data byte | Meaning |
| --- | --- | --- | --- |
| `0x4` | `TARGET` | target address | register the address for this session |
| `0x5` | `UNTARGET` | target address | unregister the address for this session |
| `0x6` | `BEGIN` | slot tag | open staging for the slot with this tag |
| `0x7` | `STAGE` | physical wire byte | append one byte to the open staging |
| `0x8` | `COMMIT` | staged byte count | bind the staged bytes to the open slot |

Mux to session:

| Nibble | Name | Data byte | Meaning |
| --- | --- | --- | --- |
| `0x4` | `TARGETED` | target address | the registration is active |
| `0x5` | `UNTARGETED` | target address | the session does not hold the address |
| `0x6` | `SLOT` | slot tag | a slot was reserved for the transaction whose destination symbol precedes this frame |
| `0x7` | `SLOT_END` | end code | the current slot ended (codes in section 6) |
| `0xD` | `TARGET_ERROR` | reason code | a refusal, a revocation or a rejected staging command |

A session that never sent `TARGET` never receives these replies, so ordinary
ENH clients see an unchanged stream. A subscriber that sends `TARGET` straight
to an adapter receives `ERROR_HOST` under the ENH unknown-command contract and
learns that the extension is absent. Alternative: a separate control endpoint
for registration; rejected because the `SLOT` notice must be ordered against
the RECEIVED symbols of the same stream.

### Lifecycle

- Admissible addresses: any target address except broadcast `0xFE`, `0xA9`,
  `0xAA`, and initiator-capable addresses (both nibbles in
  `{0x0, 0x1, 0x3, 0x7, 0xF}`). Alternative: admit initiator-capable addresses
  for initiator/initiator commands; deferred because that phase ends after the
  command ACK and needs its own tests.
- One accountable subscriber per address across all sessions. A registration
  is the key `(session, address, registration epoch)`; the epoch advances on
  every accepted registration and is never reused.
- `TARGET` for an address the session already holds answers `TARGETED` again
  and changes nothing. `TARGET` for an address held by another session is
  refused (`duplicate`) and leaves the incumbent untouched. There is no
  last-writer-wins replacement.
- Every `TARGET` gets exactly one terminal answer: `TARGETED <address>`, or
  `TARGET_ERROR <reason>` followed by `UNTARGETED <address>`.
- `UNTARGET` removes only the calling session's registration and always
  answers `UNTARGETED <address>`; repeating it is harmless and it can never
  remove another session's registration.
- A fixed per-session limit bounds the addresses one session holds; beyond it
  the answer is `capacity`. The registry is a fixed table indexed by address.
- Disconnect and session close remove every registration of the session.
  Adapter reset, a RESETTED from upstream, reconnect and mux close remove every
  registration of every session; each live session receives
  `TARGET_ERROR reset` and `UNTARGETED` per address and must register again.
  Alternative: keep registrations across a reset and drop only slots; rejected
  because the mux already tells sessions to restart on RESETTED.
- Registration needs an upstream that implements `ResponderTransport` and a
  connected mux; otherwise the answer is `unavailable`.
- Only external sessions register. An in-process registration for the gateway
  session is not part of this design.
- Registration is not initiator arbitration: it creates no START bid, changes
  no bid priority, grants no bus ownership and does not make `SEND` legal.
  `START` and `SEND` keep their current rules for every session.

Reason codes of `TARGET_ERROR`:

| Code | Name | Followed by `UNTARGETED` |
| --- | --- | --- |
| `0x01` | `duplicate`: another session holds the address | yes |
| `0x02` | `inadmissible` address | yes |
| `0x03` | `capacity` | yes |
| `0x04` | `conflict`: a foreign responder answered for the address in this stream generation | yes |
| `0x05` | `unavailable`: no responder capability upstream, or not connected | yes |
| `0x06` | `subscriber_failure` (section 5) | yes, for every held address |
| `0x07` | `reset`: the stream generation ended | yes, for every held address |
| `0x08` | `stale`: `BEGIN` or `COMMIT` for a slot that is not current | no |
| `0x09` | `late`: `COMMIT` after the request CRC was processed | no |
| `0x0A` | `invalid_response`: committed bytes fail validation | no |

## 2. Destination-time reservation

- The mux tracks each request from the first non-SYN symbol after a SYN
  boundary. The transaction ordinal advances there. Only echoed RECEIVED
  symbols drive this tracking, never a session's `SEND` intent (rule 1), so a
  local initiator and a local target see the same request.
- When the destination symbol is received and it matches a registration, the
  mux reserves the slot in the same read-loop step, before it processes the
  next symbol. The slot key is
  `(session, registration epoch, stream generation, transaction, slot tag)`.
  The slot tag is a per-session counter over `1..255` that skips zero.
- Delivery to the subscriber is the session's ordinary RECEIVED stream: the
  source symbol and the destination symbol were already queued in wire order,
  and the mux queues `SLOT <tag>` directly behind the destination symbol. Every
  later symbol follows in wire order, exactly once, as the raw symbol
  (both symbols of an escape pair). No whole-frame or passive-reconstruction
  notification is awaited.
- Broadcast, an unregistered destination, and a destination that is not
  admissible reserve nothing. Observers keep their delivery unchanged.

## 3. Provisional selection and request validation

- A reserved slot emits nothing. It becomes emission-eligible only when the
  request is complete: source, destination, PB, SB, length, that many data
  bytes and the CRC, collected without a gap, with well-formed escape pairs,
  a length within the fixed request bound, and the CRC computed over the
  logical bytes as the protocol defines.
- Valid CRC and a committed response: the slot enters the response window
  (section 4).
- Valid CRC and no commit, or a commit of zero bytes: nothing is emitted. The
  target stays silent; the initiator sees its own timeout.
- Well-formed framing with an invalid CRC: the mux emits one NACK `0xFF`
  through the responder path, because the protocol assigns the command NACK
  to the target. The slot then ends. Collection of the initiator's single
  repetition is left to a later slice, because the symbol layout of the
  repetition is not stated by the protocol page; until then the repetition
  gets no answer. Alternative: stay silent on an invalid CRC, as the minimal
  fixture of the local responder registration contract does.
- Malformed framing (bad escape, gap, length over the bound, SYN inside the
  request, lost continuity): nothing is emitted and the slot ends. The
  protocol defines no target answer for an unrecognizable command.
- Every emitted symbol is checked at its output boundary against the slot key,
  the phase and the registration, not only when the slot is selected.

### Staging

- `SLOT <tag>` clears the session's staging. The subscriber sends
  `BEGIN <tag>`, the bytes as `STAGE <byte>`, then `COMMIT <count>`.
- The staged bytes are the target part after the command CRC as physical wire
  bytes: ACK `0x00`, then length, data and CRC of the response, escaped by the
  subscriber as for SEND. The mux validates them before it accepts the commit:
  first byte ACK, consistent length, well-formed escapes, response CRC over
  length and data, total within the bound. A failing commit is refused with
  `invalid_response` and leaves the slot without a commit. `COMMIT 0` with no
  staged bytes declines the transaction: the target stays silent.
- `BEGIN` or `COMMIT` naming a tag that is not the current slot is refused with
  `stale` and discards staging. `STAGE` without an open staging is discarded
  and counted. A `COMMIT` processed after the request CRC is refused with
  `late` and counted.
- A response may therefore be prepared before the CRC arrives. At the CRC the
  mux uses what is committed and never waits to accumulate more data.

## 4. Responder emission and the response window

- Emission uses `ResponderTransport.SendResponderBytes`. It never sends START,
  never bids and never changes bus ownership. The upstream echo of every
  emitted symbol is verified before the next symbol is submitted, with the
  same bounded fallback the mux uses between the two symbols of an escape
  pair. Alternative: submit the whole committed response in one write; kept
  open for hardware measurement because some adapters may not accept a burst.
- The response window opens when the request CRC is validated and closes when
  the echo of the last committed byte is observed, when the NACK echo is
  observed, or at an invalidation that stops emission (section 6). It reopens
  once for a single retransmission of the response after the initiator's
  response NACK, as the protocol assigns that repetition to the target; a
  second NACK ends the slot.
- Exactly one emitter exists in the mux, with a single in-flight emission. It
  commits each symbol under the same lock as every ownership transition, after
  checking the reset, reconnect and close fence.
- While the window is open no other local traffic reaches the adapter:
  `SEND` from any session, including the owner of a local initiator
  transaction, is answered with `ERROR_HOST` and counted; a queued owner or
  gateway write that reaches its commit point fails without writing; staging
  commands of other sessions are `stale`. START bids keep waiting for the next
  SYN as today. The owner keeps its ownership and may send again in the
  initiator phases (response ACK or NACK, final SYN).

## 5. Latency and queueing contract

- The responder path is not observer presentation. A session that holds a
  registration is written without pacing from its first `TARGETED` until it
  holds none.
- Its pending output is bounded by a fixed responder budget, far below the
  general session buffer. If the budget is exceeded when a slot would be
  reserved, no slot is offered and the session's registrations are revoked
  with `subscriber_failure`. A write error on the session closes it as today.
  The budget is smaller than 255 slot notices, so a slot tag cannot alias a
  stale tag of the same session.
- The emitter has no queue. The read-loop step that validates the CRC hands
  the committed bytes to the emitter directly. If the first symbol cannot be
  submitted within a fixed emission bound, measured on an injected monotonic
  clock, nothing is emitted, the slot ends `late` and a counter advances
  (rule 3).
- Buffer capacity is not a timing guarantee. These bounds limit host work
  only; the on-wire delay between the request CRC and the ACK is Unknown until
  measured (section 9).

## 6. Invalidation

A slot never survives its transaction or its stream generation. The stream
generation advances on reconnect, on RESETTED, on mux reset, on raw-symbol
continuity loss and on close.

| Cause | Detection | Effect |
| --- | --- | --- |
| Duplicate physical address | an echoed symbol in a target position differs from the symbol emitted, or a target symbol arrives before the first emission | stop at once, `SLOT_END competing`, revoke the registration with `conflict` |
| Competing responder | a foreign target answer for a registered address while no local slot emits | revoke with `conflict`; the address refuses registration for the rest of the stream generation |
| Invalid CRC | request CRC check | one NACK, `SLOT_END crc` |
| Malformed request | framing check | silence, `SLOT_END malformed` |
| Missed deadline | no commit at the CRC, or the emission bound exceeded | silence, `SLOT_END not_ready` or `SLOT_END late`; a later commit is `late` and counted |
| Subscriber failure | responder budget exceeded or session error | revoke all registrations of the session; a reserved slot ends without emission |
| Unregister or disconnect | `UNTARGET`, close | a reserved slot ends without emission; an emission already started completes its committed bytes, because a truncated response corrupts the bus more than a finished one |
| Reset or reconnect | lifecycle boundary | the stream generation ends; an emission in progress stops; registrations are removed (section 1) |
| SYN while waiting | SYN in a request or response wait | `SLOT_END boundary` |
| Initiator ownership released | a local initiator's grant ends during the slot (idle gap, ownership cap, cancel) | `SLOT_END boundary` |
| Stale queued response | `BEGIN` or `COMMIT` with a tag that is not current, staging after `SLOT_END` | refused with `stale` or discarded, counted |

End codes of `SLOT_END`: `0x00` completed (initiator ACK of the response),
`0x01` crc, `0x02` malformed, `0x03` not_ready, `0x04` declined (commit of
zero bytes), `0x05` late, `0x06` competing, `0x07` boundary, `0x08` revoked,
`0x09` stream_ended, `0x0A` response_rejected (second response NACK).

## 7. Deterministic tests

All tests are offline: a synthetic upstream that implements
`ResponderTransport`, synthetic ENH sessions and an injected clock. One test
per acceptance criterion of the issue, each with sub-tests:

| Criterion | Test |
| --- | --- |
| 1 registration lifecycle | `TestTargetRegistrationLifecycle`: accept, exact retry, duplicate across sessions, capacity, inadmissible addresses, idempotent unregister, cleanup on disconnect, cleanup on reset, no START bid created |
| 2 destination-time reservation | `TestDestinationSymbolReservesSlotWithOrderedPrefix`: `SLOT` directly behind the destination symbol, raw escape pairs delivered once, no whole-frame wait |
| 3 provisional selection | `TestProvisionalSlotEmitsNothingBeforeValidatedRequest`: commit before CRC emits nothing early, single NACK on invalid CRC, silence on malformed framing, emissions only for the slot's address, transaction and phase |
| 4 emission without START | `TestResponderEmissionWithoutStartFencesLocalTraffic`: no START written, owner `SEND` rejected in the window, queued write fails at commit, other sessions cannot stage, refusal without responder capability |
| 5 latency and queueing | `TestResponderPathIsUnpacedAndBounded`: no pacing for a registered session, budget overflow revokes, prepared response emitted in full at the CRC, late commit refused and counted, emission bound exceeded emits nothing |
| 6 invalidation | `TestResponderSlotInvalidation`: one table row per cause in section 6 |
| 7 determinism | `TestResponderSlotReplayIsDeterministic`: every scenario replayed twice with identical session frames, upstream writes and counters |
| 8 hardware boundary | `TestResponderCellsStayPlannedWithoutWireEvidence` in the matrix runner: offline results never mark PX09 to PX11 hardware cells as passed |

Identity coverage: for the slot key, each test varies one field while the
others stay valid, with a valid positive control. The production mutation
each row catches:

| Field | Mutation caught |
| --- | --- |
| session | a commit from another session is accepted |
| registration epoch | a commit after unregister and re-register binds to the old slot |
| stream generation | a slot reserved before a reset emits after it |
| transaction | a commit for an ended transaction is used for the next one |
| slot tag | a stale `BEGIN` or `COMMIT` is accepted |

## 8. Slice plan

The slices follow the transition refactor of
[#1482](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1482),
which moves the mux state, read loop, received-symbol, SYN-boundary and grant
handlers into `mux_state.go`, `read_loop.go`, `received_symbol.go`,
`syn_boundary.go` and `grant.go`; they edit those files rather than the
handlers they replace. Each slice adds its own
changelog fragment and updates this page's status. Paths are under
`internal/adaptermux/` unless shown.

| Order | Title | Files | Size (files) |
| --- | --- | --- | --- |
| 0 | Session extension and responder slot contract in the docs repository | docs-ebus pages only (doc gate for slices 1 to 4) | docs |
| 1 | Register and unregister target addresses | `session_protocol.go` (new), `session_protocol_test.go` (new), `target_registry.go` (new), `target_registry_test.go` (new), `session.go`, `mux_state.go`, `doc.go`, this page, a changelog fragment | 9 |
| 2 | Reserve the slot at the destination symbol and validate the request | `responder_slot.go` (new), `responder_slot_test.go` (new), `responder_request.go` (new), `responder_request_test.go` (new), `responder_reservation_test.go` (new), `received_symbol.go`, `read_loop.go`, `syn_boundary.go`, `mux_state.go`, `session.go`, `doc.go`, this page, a changelog fragment | 13 |
| 3 | Emit the committed response without START and fence local traffic | `responder_emit.go` (new), `responder_emit_test.go` (new), `responder_isolation_test.go` (new), `responder_latency_test.go` (new), `grant.go`, `received_symbol.go`, `mux_state.go`, `session.go`, `mux.go` (only the send-loop commit check), `diag.go`, `docs/metrics-inventory.json`, `doc.go`, this page, a changelog fragment | 14 |
| 4 | Complete invalidation, replay determinism and the matrix cells | `responder_invalidation_test.go` (new), `responder_replay_test.go` (new), `responder_slot.go`, `responder_emit.go`, `cmd/matrix-runner/main.go`, `cmd/matrix-runner/main_test.go`, `cmd/matrix-runner/README.md`, `matrix/M7-adaptermux-conformance-matrix.md`, this page, a changelog fragment | 10 |

Slice 1 already changes shipped behaviour (new session frames) and needs the
transport gate; slices 3 and 4 add a new real-bus transmit path and need the
transport gate and an explicit operator decision before any live use.

## 9. What only hardware can show

The delay from the request CRC to the ACK, the gap between response symbols,
the response timing under load and the behaviour of each adapter on a
responder burst depend on the adapter, its firmware and the transport (serial,
TCP or UDP to the adapter) together with host scheduling. Symbol-rate
throughput or a link speed proves none of them. They are measured per adapter
and transport combination before any claim of hardware suitability, and the
PX09 to PX11 hardware cells stay unqualified until then. This design authorizes
no live test, no device write and no deployment.
