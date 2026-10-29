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
// Physical adapter arbitration can still lose to a initiator outside this mux.
// A read timeout or ownership release alone never opens a new arbitration window.
//
// The gateway's initiator address is its priority control. Its default 0x7f is in
// the lowest priority class: a continuously bidding higher-priority scanner can
// legitimately starve the gateway. This is standard address-priority behavior.
// There is no software fairness rotation or request-age suppression. Deliverable
// STARTED responses always reach their session. A wire win abandoned through
// cancellation or disconnect is held until the next SYN and counted by
// ActiveTxnSnapshot.UndeliverableStarted; upstream reconnection is unnecessary.
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
