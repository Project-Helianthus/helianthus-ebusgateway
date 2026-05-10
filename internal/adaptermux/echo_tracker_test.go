package adaptermux

import "testing"

func TestEchoTracker_BasicSuppression(t *testing.T) {
	tracker := newEchoTracker()

	// Record sent bytes.
	tracker.recordSent(0x71)
	tracker.recordSent(0x08)

	// Match echoes.
	result, _ := tracker.matchEcho(0x71)
	if result != echoMatchSuppressed {
		t.Fatalf("first echo: got %d, want Suppressed", result)
	}

	result, _ = tracker.matchEcho(0x08)
	if result != echoMatchSuppressed {
		t.Fatalf("second echo: got %d, want Suppressed", result)
	}

	// No more expected echoes.
	result, _ = tracker.matchEcho(0xFF)
	if result != echoMatchNone {
		t.Fatalf("third-party byte: got %d, want None", result)
	}
}

func TestEchoTracker_Mismatch(t *testing.T) {
	tracker := newEchoTracker()

	tracker.recordSent(0x71)
	tracker.recordSent(0x08)

	// First byte matches.
	result, _ := tracker.matchEcho(0x71)
	if result != echoMatchSuppressed {
		t.Fatalf("first echo: got %d, want Suppressed", result)
	}

	// Second byte mismatches.
	result, flushed := tracker.matchEcho(0xFF)
	if result != echoMatchFlushed {
		t.Fatalf("mismatch: got %d, want Flushed", result)
	}
	if len(flushed) != 1 || flushed[0] != 0x71 {
		t.Fatalf("flushed = %v, want [0x71]", flushed)
	}
}

func TestEchoTracker_FlushOnSYN(t *testing.T) {
	tracker := newEchoTracker()

	tracker.recordSent(0x71)
	tracker.recordSent(0x08)

	// Match both echoes.
	tracker.matchEcho(0x71)
	tracker.matchEcho(0x08)

	// Flush at SYN boundary.
	flushed, _ := tracker.flushOnSYN()
	if len(flushed) != 2 {
		t.Fatalf("flushed len = %d, want 2", len(flushed))
	}
	if flushed[0] != 0x71 || flushed[1] != 0x08 {
		t.Fatalf("flushed = %v, want [0x71, 0x08]", flushed)
	}

	// After flush, no pending echoes.
	if tracker.hasPendingEchoes() {
		t.Fatal("expected no pending echoes after flush")
	}
}

func TestEchoTracker_Rollback(t *testing.T) {
	tracker := newEchoTracker()

	tracker.recordSent(0x71)
	tracker.recordSent(0x08)
	tracker.rollbackSent()

	// Only one echo expected now.
	result, _ := tracker.matchEcho(0x71)
	if result != echoMatchSuppressed {
		t.Fatalf("first echo: got %d, want Suppressed", result)
	}

	result, _ = tracker.matchEcho(0x08)
	if result != echoMatchNone {
		t.Fatalf("after rollback: got %d, want None", result)
	}
}

func TestEchoTracker_EscapeBytePayload(t *testing.T) {
	// Verify that 0xA9 (SymbolEscape) in payload is handled correctly.
	// The echo tracker works on logical bytes (post-ENH-decode), so
	// 0xA9 is just a regular data byte — no escape encoding involved.
	tracker := newEchoTracker()

	tracker.recordSent(0xA9) // escape byte as data
	tracker.recordSent(0xAA) // SYN byte as data

	// Both should match as regular bytes.
	result, _ := tracker.matchEcho(0xA9)
	if result != echoMatchSuppressed {
		t.Fatalf("0xA9 echo: got %d, want Suppressed", result)
	}

	result, _ = tracker.matchEcho(0xAA)
	if result != echoMatchSuppressed {
		t.Fatalf("0xAA echo: got %d, want Suppressed", result)
	}

	suppressed, mismatches := tracker.stats()
	if suppressed != 2 {
		t.Fatalf("suppressed = %d, want 2", suppressed)
	}
	if mismatches != 0 {
		t.Fatalf("mismatches = %d, want 0", mismatches)
	}
}

func TestEchoTracker_Reset(t *testing.T) {
	tracker := newEchoTracker()

	tracker.recordSent(0x71)
	tracker.matchEcho(0x71)
	tracker.reset()

	if tracker.hasPendingEchoes() {
		t.Fatal("expected no pending echoes after reset")
	}

	// After reset, any byte is non-echo.
	result, _ := tracker.matchEcho(0x71)
	if result != echoMatchNone {
		t.Fatalf("after reset: got %d, want None", result)
	}
}

func TestEchoTracker_EmptyTracker(t *testing.T) {
	tracker := newEchoTracker()

	// No sent bytes — everything is third-party.
	result, _ := tracker.matchEcho(0x71)
	if result != echoMatchNone {
		t.Fatalf("empty tracker: got %d, want None", result)
	}

	flushed, _ := tracker.flushOnSYN()
	if len(flushed) != 0 {
		t.Fatalf("empty flush: len = %d, want 0", len(flushed))
	}
}

// TestEchoTracker_OverflowReset (P10.2.1) — exceeding maxPendingEchoes
// must RESET the queue rather than silently FIFO-drop the head. P10.2's
// flushOnSYN-skip-on-mid-write would compound the silent FIFO drop into
// a cascading echo_mismatch storm; reset semantics make the failure
// noisy (next byte returns echoMatchNone, bus.Send observes timeout +
// retry) and the totalOverflowResets counter exposes the regression.
func TestEchoTracker_OverflowReset(t *testing.T) {
	tracker := newEchoTracker()

	// Fill queue to capacity with sentinel byte 0x71.
	for i := 0; i < maxPendingEchoes; i++ {
		tracker.recordSent(0x71)
	}
	if got := len(tracker.expectedEchoes); got != maxPendingEchoes {
		t.Fatalf("queue len at cap = %d; want %d", got, maxPendingEchoes)
	}
	if got := tracker.overflowResets(); got != 0 {
		t.Fatalf("overflowResets pre-overflow = %d; want 0", got)
	}

	// One more recordSent triggers the overflow path: queue must be
	// reset to a single entry with the new byte (NOT FIFO-shifted).
	tracker.recordSent(0x08)
	if got := len(tracker.expectedEchoes); got != 1 {
		t.Fatalf("queue len after overflow = %d; want 1 (reset semantics)", got)
	}
	if got := tracker.expectedEchoes[0]; got != 0x08 {
		t.Errorf("queue head after overflow = 0x%02X; want 0x08 (latest write)", got)
	}
	if got := tracker.overflowResets(); got != 1 {
		t.Errorf("overflowResets after overflow = %d; want 1", got)
	}

	// matchEcho against the latest write 0x08 succeeds — the post-reset
	// queue head IS 0x08 (the only entry).
	result, _ := tracker.matchEcho(0x08)
	if result != echoMatchSuppressed {
		t.Errorf("matchEcho(0x08) = %v; want Suppressed (post-reset head)", result)
	}

	// All later stale echoes (the original 256 sentinels still buffered
	// upstream) now fail to match. Pre-P10.2.1 (FIFO drop) the queue
	// would have kept [byte_2 ... byte_257] — first mismatch flushes
	// the queue; downstream echoes are then treated as observer
	// traffic. Reset semantics produce the same downstream behavior
	// but expose the regression via overflowResets, which is the
	// operator tripwire. Verify queue is empty and subsequent stale
	// echoes return echoMatchNone (NOT echoMatchSuppressed — the
	// gateway no longer claims them).
	if got := len(tracker.expectedEchoes); got != 0 {
		t.Errorf("queue len after consuming 0x08 = %d; want 0", got)
	}
	result, _ = tracker.matchEcho(0x71)
	if result != echoMatchNone {
		t.Errorf("stale echo of 0x71 post-overflow = %v; want echoMatchNone (queue empty after consume)", result)
	}
}
