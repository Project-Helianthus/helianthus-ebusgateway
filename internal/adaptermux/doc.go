// Package adaptermux shares one native eBUS adapter among the gateway and
// external ENH sessions. The native START/STARTED/FAILED contract is documented
// at https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/protocols/enh.md.
//
// START bids accumulate until an idle SYN is observed. At that boundary exactly
// one pending bid is forwarded: the smallest low address nibble wins, followed
// by the smallest high nibble. Equal addresses use session ID as a deterministic
// local tie break; deployments should assign distinct initiator addresses. Session
// 0 has no special priority. Every losing bidder receives FAILED with the
// selected winner's address in that SYN interval and must submit a new bid.
// Only waiters present while processing that SYN are eligible; bids submitted
// during subsequent callbacks wait for the next boundary. Replacement withdraws
// the old waiter and admits the new waiter in the next window.
// A session's first SEND waits for the grant's ownership commit. The mux arms
// parser resets before publishing STARTED or local FAILED; the result waiter
// does not repeat those resets. Local FAILED supplies the winner's source only
// until the next observed SYN. After that boundary, STARTED synthesizes the
// source again for losing sessions so their passive frame remains complete.
// Physical adapter arbitration can still lose to an initiator outside this mux.
// On physical loss, local losers receive a synthetic session SYN before the
// actual winner source, unless an observed SYN already reset reconstruction.
// The local-loser reconstruction state survives cancellation, disconnect, or
// deadline expiry of the selected bidder until the physical response or an
// observed SYN. This session-only boundary is never emitted as passive wire
// evidence.
// A foreign STARTED still supplies its actual source once to every monitor and
// the passive stream after the selected waiter retires, even after SYN clears
// the local-loser list or the absorb budget expires. Only a session actually
// notified with FAILED-with-winner is omitted from source synthesis; a silently
// cancelled/replaced waiter receives the source as an observer instead.
// Gateway bids waiting for SYN expire after StartDeadline; once selected, a
// separate StartDeadline bounds the adapter response without admission expiry
// cancelling a deliverable wire win. A gateway local loser waits for that START
// outcome, bounded by admission expiry, the START deadline and lifecycle failure.
// A read timeout or ownership release alone never opens a new arbitration window.
//
// Arbitration lifecycle (see arbitration_recovery.go and the table-driven
// TestT02_ArbitrationExitStateMachine):
//
//   - Idle -> selected -> wire arbitration: freeze bid eligibility before raw
//     SYN delivery, capture local losers, publish their FAILED-with-winner,
//     and send one native START. Only this selected waiter may be resolved.
//   - Wire result -> attribution -> ownership: a matching STARTED commits
//     only for a live waiter/session in the same upstream lifecycle epoch.
//   - Cancel/replacement/expiry before the result: retire the waiter, retain
//     loser reconstruction and absorb the outstanding result. No cancelled
//     waiter receives STARTED or FAILED. A definite START-send failure ends
//     this uncertainty without waiting for a response that cannot arrive.
//   - Cancel after STARTED, undeliverable commit, mismatch, foreign FAILED,
//     or phantom FAILED: fence admission until a logical structural SYN.
//     A mismatched result still leaves the selected result to absorb. Retired
//     selection identity survives absorb timeout and SYN until the next selection
//     or lifecycle boundary, so still-later results also recover at SYN.
//     Local losers receive their reconstruction SYN and actual source when
//     needed; a suppressed phantom still resets their advertised source.
//     Phantom suppression applies to session source synthesis; the native
//     FAILED byte remains visible as passive evidence, as before.
//     An abandon-watchdog release of an unused grant fences admission like a
//     cancel. The external idle gap and the ownership cap also apply to a SYN
//     the classifier withholds; such a release fences admission as well.
//   - Fence -> idle: before the classifier can filter that SYN, reset its
//     abandoned header and the legacy phase tracker. Process and deliver the
//     SYN normally, then clear the fence. Raw-only AA and escaped payload AA
//     never clear it. Absorb and blocking-call barriers remain independent.
//   - Reset/disconnect/close: retire selection, loser state, absorb budget,
//     timers and fences. Request classifier reset on its read-loop producer
//     before processing the next stream item. A result from an older lifecycle
//     cannot restore ownership, restore the fence, or synthesize a source.
//
// Recovery resets are local state changes, never fabricated wire RESETTED
// observations. Ordinary AA-injection filtering for unrelated passive traffic
// is unchanged. Once the outstanding response is consumed, the recovery SYN
// clears the fence and a new bid can enter at the following SYN.
//
// The gateway's initiator address is its priority control. Its default 0x7f is in
// the lowest priority class: a continuously bidding higher-priority scanner can
// legitimately starve the gateway. This is standard address-priority behavior.
// There is no software fairness rotation or request-age suppression. Deliverable
// STARTED responses always reach their session. A wire win abandoned through
// cancellation or disconnect is held until the next SYN and counted by
// ActiveTxnSnapshot.UndeliverableStarted; upstream reconnection is unnecessary.
// Once STARTED has been consumed, cancellation never arms a stale-response
// barrier; the next SYN releases the abandoned win for queued bids.
//
// The gateway flags --adapter-mux-idle-release-grace (200ms) and
// --adapter-mux-max-ownership-duration (10s) configure the retained ownership
// limits. --adapter-mux-read-timeout (50ms) configures adapter reads separately
// from the generic transport --read-timeout flag, preserving the mux default.
// The separate
// gateway ownership guard from issue #1401 remains 1500ms. Add-on option wiring
// is a separate companion change.
//
// Out-of-step adapter (adapter_step.go). The enhanced protocol does not tag
// answers, so an adapter that runs one host command behind answers each START
// only when the next host write arrives, and the START deadline, the
// stale-answer barrier and the mismatch rule never converge with it. A START
// cycle runs from one RequestStart write to the next; it is bad when, before
// the next START write, a STARTED names another initiator than the pending
// one, a barrier armed for "deadline" or "started-mismatch" is cleared without
// its answer, or a STARTED arrives with nothing pending, no barrier and no
// retired selection. Each cycle counts once. Three consecutive bad cycles are a
// detection; the run ends at a completed transaction (a gateway transaction
// ending with syn_terminator or transaction_done, an external owner released
// at its terminal SYN), at an answer the barrier consumed, and at every
// lifecycle boundary. The first detection of an episode resets the adapter
// link once: the mux closes its upstream connection, so the read loop's
// reconnect reopens it and repeats INIT (a lifecycle owner replaces the
// generation instead). A further detection, while that reset is spent, holds
// active arbitration with reason adapter_out_of_step: selection is skipped, no
// START is written, queued external bids get no frame and a gateway bid ends at
// its admission bound with an error naming the hold. Passive and session
// delivery, ownership releases and lifecycle handling continue. The hold ends
// only at an adapter RESETTED, a reconnect or Close; the reset becomes
// available again after the next completed transaction, in any generation, as
// AdapterStepMemory carries it across generations. Until a detection the
// detector only observes, and it never adds a write: the link reset closes a
// connection and the hold only skips selection.
//
// Proven: synthetic unit fixtures exercise selection, loser notification,
// re-bidding and abandoned wins. Unknown: live scanner latency and physical
// transport qualification; these require the declared transport gate and smoke
// evidence and are not established by unit fixtures.
package adaptermux
