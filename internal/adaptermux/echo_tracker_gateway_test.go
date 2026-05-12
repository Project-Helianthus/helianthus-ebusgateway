package adaptermux

// Echo-tracker unit tests — GATEWAY SCOPE ONLY.
//
// F-18 (_work_adaptermux_audit/EBUSD-VERIFICATION-2026-05-12-batch13.md)
// removed per-external-session echo suppression. The echoTracker struct
// remains in use only for the gateway path (m.gatewayEcho); these
// existing unit tests continue to validate that struct as a unit in its
// remaining gateway-side capacity (matchEcho on adapter-mirrored bytes,
// recordSent in sendLoop, rollbackSent on adapter-write failure,
// flushOnSYN on transaction boundaries, etc.).
//
// External ENH sessions must receive their own post-arbitration echoes
// per john30/ebusd's enhanced_proto.md and no longer carry an echo
// tracker. The integration tests for external echo passthrough live in
// `echo_passthrough_test.go`.

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

// TestEchoTracker_OverflowRollbackRestoresQueue (Codex PR #603 P2) —
// when recordSent triggers an overflow reset and the subsequent
// adapter Write fails, rollbackSent must restore the prior 256
// expectations and revert the counter increment. Pre-fix the prior
// queue was lost forever and incoming real echoes mismatched.
func TestEchoTracker_OverflowRollbackRestoresQueue(t *testing.T) {
	tracker := newEchoTracker()

	// Fill queue to capacity with distinct sentinels so we can verify
	// the exact pre-overflow contents are restored.
	for i := 0; i < maxPendingEchoes; i++ {
		tracker.recordSent(byte(i % 256))
	}

	// Trigger overflow + immediate rollback (simulates Write failure).
	tracker.recordSent(0xFE)
	if got := tracker.overflowResets(); got != 1 {
		t.Fatalf("overflowResets after overflow = %d; want 1", got)
	}
	if got := len(tracker.expectedEchoes); got != 1 {
		t.Fatalf("queue len after overflow = %d; want 1", got)
	}

	tracker.rollbackSent()

	// Counter reverted.
	if got := tracker.overflowResets(); got != 0 {
		t.Errorf("overflowResets after rollback = %d; want 0 (must revert)", got)
	}
	// Queue restored to the original 256 expectations.
	if got := len(tracker.expectedEchoes); got != maxPendingEchoes {
		t.Fatalf("queue len after rollback = %d; want %d (must restore)", got, maxPendingEchoes)
	}
	for i := 0; i < maxPendingEchoes; i++ {
		want := byte(i % 256)
		if got := tracker.expectedEchoes[i]; got != want {
			t.Errorf("expectedEchoes[%d] post-rollback = 0x%02X; want 0x%02X", i, got, want)
		}
	}

	// Real echoes (e.g. of byte 0) now match correctly — pre-fix they
	// would have all returned echoMatchNone because the prior queue
	// was destroyed.
	result, _ := tracker.matchEcho(0)
	if result != echoMatchSuppressed {
		t.Errorf("matchEcho(0) post-rollback = %v; want Suppressed (restored queue head)", result)
	}
}

// TestEchoTracker_OverflowSnapshotInvalidatedByMatch (Codex PR #603 P2
// invariant) — once any echo matches, the prior recordSent is
// considered committed; a subsequent rollbackSent must NOT restore the
// snapshot. Otherwise: overflow recordSent → match → rollback would
// resurrect a stale 256-element queue + drop the counter, both wrong.
func TestEchoTracker_OverflowSnapshotInvalidatedByMatch(t *testing.T) {
	tracker := newEchoTracker()

	for i := 0; i < maxPendingEchoes; i++ {
		tracker.recordSent(0x71)
	}
	tracker.recordSent(0x08) // overflow → snapshot stored

	// matchEcho commits → snapshot must be cleared.
	tracker.matchEcho(0x08)

	// Now rollback must NOT restore the prior 256 entries (we already
	// committed the overflow by consuming the post-reset head).
	tracker.rollbackSent()
	if got := len(tracker.expectedEchoes); got != 0 {
		t.Errorf("queue len after match+rollback = %d; want 0 (snapshot invalidated)", got)
	}
	if got := tracker.overflowResets(); got != 1 {
		t.Errorf("overflowResets after match+rollback = %d; want 1 (counter NOT reverted)", got)
	}
}
