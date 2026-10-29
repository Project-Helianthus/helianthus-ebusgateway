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
// Gateway bids waiting for SYN expire after StartDeadline; once selected, a
// separate StartDeadline bounds the adapter response without admission expiry
// cancelling a deliverable wire win.
// A read timeout or ownership release alone never opens a new arbitration window.
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
// Proven: synthetic unit fixtures exercise selection, loser notification,
// re-bidding and abandoned wins. Unknown: live scanner latency and physical
// transport qualification; these require the declared transport gate and smoke
// evidence and are not established by unit fixtures.
package adaptermux
