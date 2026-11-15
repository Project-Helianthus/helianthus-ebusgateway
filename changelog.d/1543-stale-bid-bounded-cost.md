## Bounded cost of a stale external START bid stated and pinned by tests

Tests and documentation only. A waiting external START bid has no age limit and is withdrawn only by its client: a SYN cancel, a newer request of the same session, or the session closing. A bid whose client stopped waiting costs one START request and one grant held for at most the abandon deadline (75 ms after STARTED reached the client); arbitration resumes at the next SYN, and while active arbitration is held for an out-of-step adapter no START request is written for a waiting bid. New offline tests in `internal/adaptermux/stale_external_bid_test.go` pin this bound, and the package documentation and `docs/architecture/adapter-arbitration-decision.md` state it.

No bid expiry, age field, option or timer is added; timings, counters, log lines and configuration are unchanged. The gateway binary is unchanged apart from comment text.
