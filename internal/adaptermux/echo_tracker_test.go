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
