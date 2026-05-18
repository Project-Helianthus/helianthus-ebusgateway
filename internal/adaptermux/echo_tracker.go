package adaptermux

// echoTracker tracks bytes sent by a session and suppresses the
// corresponding echoes from the adapter. Each session (gateway internal
// or external ENH client) has its own tracker.
//
// Design: when a session sends a byte to the adapter via SEND, the byte
// is recorded in the expectedEchoes queue. When the adapter echoes it
// back as ENHResReceived, the tracker matches and suppresses it from
// that session's observer stream. Other sessions see the byte.
//
// Unlike the proxy's byte-level ownerObserverSeen accumulation, this
// tracker operates on logical bytes (post-ENH-decode). No escape
// sequences (0xA9) are stored — the ENH protocol delivers logical bytes.
// This eliminates the proxy's escape encoding bug by design.
const (
	// maxPendingEchoes caps the expectedEchoes queue to prevent unbounded
	// growth if echo matching falls behind (AM16).
	maxPendingEchoes = 256

	// maxSeenEchoes caps the seenEchoes accumulator to prevent unbounded
	// growth between SYN boundaries (AM41).
	maxSeenEchoes = 256
)

type echoTracker struct {
	// expectedEchoes is a FIFO queue of bytes that we expect the adapter
	// to echo back. Populated by recordSent(), consumed by matchEcho().
	expectedEchoes []byte

	// seenEchoes accumulates matched echo bytes since the last SYN
	// boundary. Used for observer frame assembly.
	seenEchoes []byte

	// atRequestStart tracks whether the next echo sequence begins a new
	// request (includes initiator byte in observer frames).
	atRequestStart bool

	// Stats.
	totalSuppressed uint64
	totalMismatches uint64

	// totalOverflowResets counts how many times recordSent reset the
	// expectedEchoes queue because it would have exceeded
	// maxPendingEchoes. Pre-P10.2 the queue silently dropped its oldest
	// entry on overflow (FIFO); under sustained mid-write SYN noise
	// (P10.2 keeps the queue across suppressed SYNs) that silent drop
	// would desync subsequent matchEcho calls — every comparison would
	// be against a wrong head, producing a cascading echo_mismatch
	// storm hard to diagnose. Reset semantics make the failure noisy
	// (next received byte won't match an empty queue → echoMatchNone)
	// and operator-visible via this counter (P10.2.1).
	totalOverflowResets uint64

	// preOverflowEchoes captures the queue contents IMMEDIATELY BEFORE
	// the most recent overflow-reset-triggering recordSent call.
	// rollbackSent restores from this snapshot to guarantee the cap
	// behavior is transactional with the adapter Write that follows
	// recordSent (Codex PR #603 P2). nil if the most recent recordSent
	// did NOT trigger an overflow reset.
	//
	// Invariant: at most one snapshot is retained; subsequent successful
	// recordSent calls (no overflow) clear it. This matches the doSend
	// call sequence (recordSent → Write → maybe rollbackSent) where the
	// snapshot is only valid for the duration of a single doSend.
	preOverflowEchoes []byte

	// queueJustDrained (batch-26 round-7 — Attack 3 closure) marks the
	// brief inter-write window between the gateway's matchEcho
	// consumption of echo K and the sendLoop's recordSent of byte K+1.
	// During that window expectedEchoes is empty (peekNextExpected
	// returns hasPending=false), so the peek-based P10.2 gate cannot
	// suppress a wire SYN even when one CANNOT be a legitimate
	// terminator (the gateway is mid-frame between two body writes,
	// not at end-of-message).
	//
	// Set true by matchEcho when a non-SYN match transitions the queue
	// from len==1 to len==0 (the typical "echo of body byte K consumed,
	// next write K+1 still pending" shape). Cleared:
	//   - by recordSent: a new write means we are no longer between
	//     writes; the queue is armed again.
	//   - by markRequestStart: a fresh transaction boundary invalidates
	//     prior inter-write state.
	//   - by flushOnSYN: a wire SYN observed legitimately closing the
	//     txn (terminator gate fired); the flag's lifetime ends with
	//     the txn.
	//   - by matchEcho when consuming a SymbolSyn (terminator echo); the
	//     gateway just observed legitimate end-of-frame, no longer
	//     between body writes.
	//
	// The flag is bounded by recordSent / markRequestStart / flushOnSYN
	// — it CANNOT carry over to a new transaction. This bounding is the
	// key distinction from the round-6 bytesDeliveredToActive approach
	// (which was cleared only by recordGatewayGrant and thus leaked
	// across txn boundaries → over-suppression of legitimate echoes →
	// throughput collapse, batch-25 revert).
	queueJustDrained bool
}

// newEchoTracker creates a fresh echo tracker.
func newEchoTracker() *echoTracker {
	return &echoTracker{
		expectedEchoes: make([]byte, 0, 32),
		seenEchoes:     make([]byte, 0, 32),
	}
}

// recordSent records a byte that the session is sending to the adapter.
// We expect the adapter to echo this byte back as ENHResReceived.
//
// Overflow handling (P10.2.1 — replaces the AM16 silent FIFO drop):
// at the cap (maxPendingEchoes=256) the queue is RESET, NOT shifted.
// FIFO drop preserved length but silently rotated the head, which
// post-P10.2 (mid-write SYN now keeps the queue rather than flushing
// it on SYN) would desynchronize subsequent matchEcho calls — every
// comparison would be against the wrong head, producing a cascading
// echo_mismatch storm hard to diagnose. Reset semantics make the
// failure noisy (echoMatchNone on the next byte; bus.Send will see
// the lost-echo timeout and retry the txn) and the
// `totalOverflowResets` counter exposes the regression for
// operators. Real eBUS frames are <30 bytes; reaching 256 implies a
// pathological condition (TCP backpressure, paused readLoop, runaway
// gateway write loop) where loud failure is preferable to drift.
//
// Codex PR #603 P2 follow-up: the reset is now TRANSACTIONAL with the
// adapter Write that follows in doSend. preOverflowEchoes snapshots
// the prior queue so rollbackSent (called when Write fails) can
// restore the 256 prior expectations and revert the counter
// increment. Without this, a Write-fails-at-cap path would lose all
// prior echo expectations even though the adapter never accepted the
// new byte — subsequent real echoes would all return echoMatchNone.
func (t *echoTracker) recordSent(data byte) {
	// batch-26 round-7: any write means we are definitely no longer
	// between writes. Clear queueJustDrained BEFORE arming the queue so
	// the sentinel reflects exactly the inter-write window — the
	// recordSent here is what closes that window.
	t.queueJustDrained = false
	if len(t.expectedEchoes) >= maxPendingEchoes {
		t.preOverflowEchoes = make([]byte, len(t.expectedEchoes))
		copy(t.preOverflowEchoes, t.expectedEchoes)
		t.expectedEchoes = t.expectedEchoes[:0]
		t.totalOverflowResets++
	} else {
		// Successful append without overflow: clear any stale
		// snapshot from a prior overflow that was never rolled back
		// (the adapter Write succeeded; the reset is now committed).
		t.preOverflowEchoes = nil
	}
	t.expectedEchoes = append(t.expectedEchoes, data)
}

// rollbackSent removes the last recorded sent byte (e.g., on SEND error).
//
// Codex PR #603 P2 follow-up: when the prior recordSent triggered an
// overflow reset, rollbackSent restores the pre-overflow queue and
// reverts the counter increment. The adapter Write failed so byte 257
// never reached the wire AND the 256 prior expectations are still
// awaiting their echoes — restore them.
func (t *echoTracker) rollbackSent() {
	if t.preOverflowEchoes != nil {
		// Overflow path: restore prior 256 + drop counter increment.
		// The new byte was appended after the reset; it never reached
		// the adapter, so we fully revert.
		t.expectedEchoes = t.preOverflowEchoes
		t.preOverflowEchoes = nil
		if t.totalOverflowResets > 0 {
			t.totalOverflowResets--
		}
		return
	}
	if len(t.expectedEchoes) > 0 {
		t.expectedEchoes = t.expectedEchoes[:len(t.expectedEchoes)-1]
	}
}

// echoMatchResult describes the outcome of matching a received byte.
type echoMatchResult uint8

const (
	// echoMatchNone means the byte is not an echo of this session's
	// sent data. Deliver to this session as observer traffic.
	echoMatchNone echoMatchResult = iota

	// echoMatchSuppressed means the byte is an echo of this session's
	// sent data. Suppress from this session's observer stream.
	echoMatchSuppressed

	// echoMatchFlushed means a mismatch was detected. Previously matched
	// echo bytes have been flushed (returned as observer frames) and the
	// current byte should be delivered normally.
	echoMatchFlushed
)

// matchEcho checks if a received byte matches the next expected echo.
//
// Returns:
//   - echoMatchSuppressed: byte is this session's echo, suppress it
//   - echoMatchNone: no echo expected, byte is third-party traffic
//   - echoMatchFlushed: mismatch detected, accumulated echoes flushed
//
// flushedBytes contains any accumulated echo bytes that should be
// delivered as observer frames before the current byte.
func (t *echoTracker) matchEcho(received byte) (result echoMatchResult, flushedBytes []byte) {
	if len(t.expectedEchoes) == 0 {
		return echoMatchNone, nil
	}

	// Codex PR #603 P2 invariant: any matchEcho commits the prior
	// recordSent (the adapter actually replied to it), so the
	// preOverflowEchoes snapshot is no longer eligible for rollback.
	t.preOverflowEchoes = nil

	if received == t.expectedEchoes[0] {
		// Match: consume from expected, accumulate in seen.
		preLen := len(t.expectedEchoes)
		t.expectedEchoes = t.expectedEchoes[1:]
		// AM41: drop oldest if seenEchoes is at capacity.
		if len(t.seenEchoes) >= maxSeenEchoes {
			t.seenEchoes = t.seenEchoes[1:]
		}
		t.seenEchoes = append(t.seenEchoes, received)
		t.totalSuppressed++
		// batch-26 round-7 — Attack 3 closure. When this match transitions
		// the queue from non-empty to empty AND the consumed echo is NOT
		// a terminator SYN, we have entered the inter-write window: the
		// gateway has consumed echo K of a body byte and the sendLoop
		// has not yet armed echo K+1. Any wire SYN in this window
		// CANNOT be a legitimate terminator (the body is not done) and
		// CANNOT be a mid-write race against a queue head (the queue is
		// empty). queueJustDrained signals the mux to suppress such
		// SYNs without needing a queue head to compare against.
		//
		// SymbolSyn match: a legitimate terminator echo just consumed.
		// Clearing the flag (rather than setting it) preserves the
		// terminator branch's ability to fire — this match is the
		// "end of frame" signal, not "between body writes".
		if preLen == 1 {
			// symbolSyn = 0xAA per eBUS protocol (canonical
			// protocol.SymbolSyn — duplicated as a literal here to keep
			// echo_tracker import-free; the value is a wire-protocol
			// constant and will not drift).
			const symbolSyn byte = 0xAA
			if received == symbolSyn {
				t.queueJustDrained = false
			} else {
				t.queueJustDrained = true
			}
		}
		return echoMatchSuppressed, nil
	}

	// Mismatch: flush any accumulated echoes as observer frames.
	t.totalMismatches++
	var flushed []byte
	if len(t.seenEchoes) > 0 {
		flushed = make([]byte, len(t.seenEchoes))
		copy(flushed, t.seenEchoes)
		t.seenEchoes = t.seenEchoes[:0]
	}

	// Clear expected echoes on mismatch — the echo sequence is broken.
	t.expectedEchoes = t.expectedEchoes[:0]
	t.atRequestStart = false

	return echoMatchFlushed, flushed
}

// flushOnSYN flushes accumulated echo bytes at a SYN boundary.
// Returns any accumulated bytes that should be delivered as observer
// frames (they are confirmed echoes of the owning session's traffic).
//
// After flush, the tracker is ready for the next request cycle.
func (t *echoTracker) flushOnSYN() (flushedBytes []byte, wasAtStart bool) {
	wasAtStart = t.atRequestStart

	if len(t.seenEchoes) > 0 {
		flushedBytes = make([]byte, len(t.seenEchoes))
		copy(flushedBytes, t.seenEchoes)
		t.seenEchoes = t.seenEchoes[:0]
	}

	// Clear expected echoes at SYN boundary.
	t.expectedEchoes = t.expectedEchoes[:0]
	// Codex PR #603 P2 invariant: a SYN boundary commits any
	// in-flight overflow reset. preOverflowEchoes no longer eligible
	// for rollback.
	t.preOverflowEchoes = nil
	// batch-26 round-7: a wire SYN observed legitimately closing the
	// txn ends the inter-write window — the gateway is between
	// transactions now, not between body writes within one. Without
	// this clear, the flag from the prior inter-write window would
	// carry through to the next grant and over-suppress legitimate
	// post-grant SYNs.
	t.queueJustDrained = false

	return flushedBytes, wasAtStart
}

// markRequestStart marks that the next echo sequence is the start
// of a new request (used for observer frame assembly with initiator).
func (t *echoTracker) markRequestStart() {
	t.atRequestStart = true
	t.seenEchoes = t.seenEchoes[:0]
	t.expectedEchoes = t.expectedEchoes[:0]
	// Codex PR #603 P2 invariant: a new request boundary invalidates
	// any pending overflow snapshot.
	t.preOverflowEchoes = nil
	// batch-26 round-7: a fresh transaction invalidates any inter-write
	// flag from a prior txn. Same boundary discipline as
	// preOverflowEchoes — never carry inter-write state across grant
	// boundaries (the over-suppression failure mode of round-6).
	t.queueJustDrained = false
}

// hasPendingEchoes reports whether there are expected echoes that
// haven't been matched yet.
func (t *echoTracker) hasPendingEchoes() bool {
	return len(t.expectedEchoes) > 0
}

// peekNextExpected returns the byte at the head of the expected-echo
// queue and a boolean indicating whether the queue is non-empty. Used by
// the mux to discriminate legitimate terminator-SYN echoes (next expected
// is a SYN that the gateway just wrote) from mid-frame SYN noise (next
// expected is a non-SYN data byte) and from response-phase SYN noise
// (queue empty — gateway is reading the target's response and has no
// pending writes). Read-only; does NOT consume the queue head. P10.2.
func (t *echoTracker) peekNextExpected() (byte, bool) {
	if len(t.expectedEchoes) == 0 {
		return 0, false
	}
	return t.expectedEchoes[0], true
}

// reset clears all tracking state.
func (t *echoTracker) reset() {
	t.expectedEchoes = t.expectedEchoes[:0]
	t.seenEchoes = t.seenEchoes[:0]
	t.atRequestStart = false
	t.preOverflowEchoes = nil
	t.queueJustDrained = false
}

// IsQueueJustDrained reports whether the gateway is currently in the
// inter-write window — i.e., matchEcho consumed a non-SYN echo that
// emptied the expectedEchoes queue, AND no subsequent recordSent /
// flushOnSYN / markRequestStart has fired. batch-26 round-7 — the mux
// SYN gate uses this signal to suppress wire SYNs that arrive after
// matchEcho but before the next recordSent (Attack 3 — the leak the
// peek-based P10.2 gate cannot see because the queue head it inspects
// is gone).
//
// Exported via PascalCase even though the tracker is unexported within
// the package so the SYN-handling site reads as a deliberate
// inter-package contract; all other accessors on echoTracker are also
// safe to read from the mux while holding stateMu.
func (t *echoTracker) IsQueueJustDrained() bool {
	return t.queueJustDrained
}

// stats returns echo tracking statistics.
func (t *echoTracker) stats() (suppressed, mismatches uint64) {
	return t.totalSuppressed, t.totalMismatches
}

// overflowResets returns the number of times recordSent reset the
// expectedEchoes queue because it was about to exceed
// maxPendingEchoes. Non-zero means the gateway-write loop produced
// more than 256 unacknowledged writes — a pathological condition
// that matters as a tripwire for operator visibility (P10.2.1).
func (t *echoTracker) overflowResets() uint64 {
	return t.totalOverflowResets
}
