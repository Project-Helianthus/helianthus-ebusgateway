# Adapter arbitration selection

SYN bid selection is a pure decision over copied state: owner presence, bus
idleness, and the ordered gateway and external bids with their eligibility.
The gateway, when present, precedes external bids in pending-list order.
The decision returns a winner index, ordered eligible loser indices, and
ordered retained indices. It neither mutates its snapshot nor performs I/O.

The arbitrator holds its existing mutex while copying state, deciding, and
applying the result. Request pointers and notification channels stay in the
arbitrator. A nil SYN admission snapshot admits every current request. A
non-nil snapshot admits only a matching request pointer and notification
channel. Same-address replacement retains the request pointer but changes its
channel; a stale boundary cannot admit the new waiter. Later arrivals and
cancelled or replaced requests cannot cross that boundary.

An owner or a busy bus prevents selection and retains all pending bids.
Otherwise the lowest initiator low nibble wins, followed by the lowest high
nibble, then lowest session identity. Equal priority keeps the first bid.
Eligible losers keep gateway-first and external-list order. Ineligible bids
keep their pending order for a later boundary.

Selection does not confirm ownership or send results. Deferred gateway loser
tracking, cancellation, replacement, ownership tokens, and exactly-once result
delivery remain responsibilities of the arbitrator and existing callers.
No new asynchronous work is introduced.

The maintained tests compare the selection with the original algorithm across
small synthetic bid sets, both admission-snapshot forms, owner and idle states,
and address/session ties. Boundary cases cover request and channel replacement,
cancellation, late arrivals, retained waiters, and deferred gateway losers.
Copied-state tests check explicit output ordering and input immutability.
These are software characterization results, not native protocol qualification.

This structural change affects gateway build inputs and potentially all gateway
binaries containing AdapterMux. It does not establish binary neutrality.
Release artifact evidence and applicable security and hardware validation must
be renewed. Full repository CI, independent review, and applicable transport and
conformance gates remain shipping requirements. Offline fixtures do not prove
physical qualification or deployed acceptance. The broader Mux lifecycle,
recovery, and transition state machine remain outside this selection boundary.
