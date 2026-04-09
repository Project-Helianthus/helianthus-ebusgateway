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
func (t *echoTracker) recordSent(data byte) {
	t.expectedEchoes = append(t.expectedEchoes, data)
}

// rollbackSent removes the last recorded sent byte (e.g., on SEND error).
func (t *echoTracker) rollbackSent() {
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

	if received == t.expectedEchoes[0] {
		// Match: consume from expected, accumulate in seen.
		t.expectedEchoes = t.expectedEchoes[1:]
		t.seenEchoes = append(t.seenEchoes, received)
		t.totalSuppressed++
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

	return flushedBytes, wasAtStart
}

// markRequestStart marks that the next echo sequence is the start
// of a new request (used for observer frame assembly with initiator).
func (t *echoTracker) markRequestStart() {
	t.atRequestStart = true
	t.seenEchoes = t.seenEchoes[:0]
	t.expectedEchoes = t.expectedEchoes[:0]
}

// hasPendingEchoes reports whether there are expected echoes that
// haven't been matched yet.
func (t *echoTracker) hasPendingEchoes() bool {
	return len(t.expectedEchoes) > 0
}

// reset clears all tracking state.
func (t *echoTracker) reset() {
	t.expectedEchoes = t.expectedEchoes[:0]
	t.seenEchoes = t.seenEchoes[:0]
	t.atRequestStart = false
}

// stats returns echo tracking statistics.
func (t *echoTracker) stats() (suppressed, mismatches uint64) {
	return t.totalSuppressed, t.totalMismatches
}
