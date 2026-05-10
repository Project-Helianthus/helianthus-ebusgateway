package adaptermux

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// P10.2 — verify that a wire SYN arriving while the gateway is mid-write
// (waiting for the echo of a non-SYN body byte) is suppressed and NOT
// delivered to activeCh, NOT classified as terminator, and does NOT
// abort the live transaction via idle release.
//
// Production motivation: 671 echo_mismatch events with subclass
// `pre_echo_syn` were observed in the live HA gateway soak. Live byte:
// 0xAA (SYN). Path: bus.Send.sendRawWithEcho writes byte N, gates on
// echo, the mux's terminator branch (pre-P10.2) delivered an
// interleaved buffered/colliding SYN to activeCh as if it were the
// terminator, and bus.Send saw 0xAA in echo position.
//
// Scenario covered:
//
//  1. Grant gateway (initiator 0x71).
//  2. Gateway writes byte 0x71 (the source) — gatewayEcho.expectedEchoes
//     = [0x71]. Echo delivered + consumed → bytesDeliveredToActive=1,
//     expectedEchoes empty.
//  3. Gateway writes byte 0x08 (target) — expectedEchoes = [0x08].
//     Echo NOT yet fed.
//  4. A noise wire SYN arrives BEFORE the echo of 0x08.
//
// Pre-P10.2 result: terminator gate fires (bytesDeliveredToActive>0 &&
// gatewayTxnActive=true) → SYN delivered to activeCh → bus.Send
// reading echo expects 0x08, gets 0xAA → echo_mismatch, subclass
// pre_echo_syn.
//
// P10.2 result: peekNextExpected returns (0x08, true). nextExpected !=
// SymbolSyn → midWriteSyn=true → preEchoMidFrameSuppress=true:
//   - terminator gate does NOT fire (gated on `!midWriteSyn`)
//   - flushOnSYN does NOT fire (queue [0x08] preserved)
//   - idle release does NOT fire (bytesDeliveredToActive>0)
//   - synSuppressedPreEcho counter increments
//   - the txn stays live; the next echo (0x08) still matches.
func TestPreEchoSyn_MidWriteSyn_Suppressed(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()

	// Step 2: write source byte and let it echo+consume so we leave the
	// pre-first-echo window (bytesDeliveredToActive>=1).
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("Write(0x71) err=%v", err)
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x71}
	if b, err := at.ReadByte(); err != nil || b != 0x71 {
		t.Fatalf("ReadByte (echo of 0x71) = (0x%02X, %v), want (0x71, nil)", b, err)
	}

	mid := mux.ActiveTxnSnapshot()
	if mid.BytesDeliveredToActive == 0 {
		t.Fatalf("BytesDeliveredToActive=0 after first echo, want >=1 — test precondition")
	}
	suppressedBefore := mid.SynSuppressedPreEcho

	// Step 3: write the target byte 0x08 — DO NOT feed its echo. The
	// gateway is now mid-write with expectedEchoes head = 0x08.
	if _, err := at.Write([]byte{0x08}); err != nil {
		t.Fatalf("Write(0x08) err=%v", err)
	}
	// Allow sendLoop's recordSent(0x08) to populate the queue before the
	// noise SYN arrives (sendLoop holds stateMu so the inbound SYN's
	// onSYNLocked critical section will serialize after recordSent;
	// nevertheless yield so the goroutine is scheduled).
	time.Sleep(20 * time.Millisecond)

	// Step 4: inject the noise SYN.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	snap := mux.ActiveTxnSnapshot()

	// Assertion (a): synSuppressedPreEcho incremented (P10.2 mid-write
	// branch fired).
	if snap.SynSuppressedPreEcho <= suppressedBefore {
		t.Errorf("SynSuppressedPreEcho=%d, suppressedBefore=%d, want increment — mid-write SYN must be classified as noise (P10.2)",
			snap.SynSuppressedPreEcho, suppressedBefore)
	}

	// Assertion (b): txn is STILL active. The mid-write SYN must not
	// have aborted the transaction via terminator OR idle release.
	if !snap.Active {
		t.Errorf("ActiveTxnSnapshot.Active=false, want true — mid-write SYN must not abort live txn")
	}
	if snap.InactiveReason == ReasonSYNTerminator {
		t.Errorf("InactiveReason=ReasonSYNTerminator — pre-P10.2 bug: terminator path fired on mid-write SYN")
	}
	if snap.InactiveReason == ReasonSYNIdle {
		t.Errorf("InactiveReason=ReasonSYNIdle — idle release fired on mid-write SYN despite bytesDeliveredToActive>0")
	}

	// Assertion (c): the echo of 0x08 still matches after the noise SYN
	// (queue [0x08] was preserved by skipping flushOnSYN). Feeding the
	// echo now must succeed without echo_mismatch and ReadByte returns
	// 0x08 to the bus.Send consumer.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x08}
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		b, err := at.ReadByte()
		if err == nil && b == 0x08 {
			return // success
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("did not receive 0x08 echo after noise SYN — gatewayEcho queue corrupted by flushOnSYN")
}

// P10.2 negative control — verify that the legitimate frame terminator
// (bus.Send wrote SYN itself, queue head IS SYN) still fires the
// terminator path. This guards against an over-aggressive suppression
// regression.
func TestPreEchoSyn_LegitimateTerminator_Delivered(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()

	// Write source byte, feed echo, consume.
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("Write err=%v", err)
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x71}
	if _, err := at.ReadByte(); err != nil {
		t.Fatalf("ReadByte err=%v", err)
	}

	// Gateway writes the terminator SYN itself (sendEndOfMessage path).
	// expectedEchoes head is now SYN.
	go func() { _, _ = at.Write([]byte{protocol.SymbolSyn}) }()
	time.Sleep(20 * time.Millisecond)

	// Wire echoes the SYN. The gate must let it through as terminator.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}

	deadline := time.Now().Add(1 * time.Second)
	delivered := false
	for time.Now().Before(deadline) {
		b, err := at.ReadByte()
		if err == nil && b == protocol.SymbolSyn {
			delivered = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !delivered {
		t.Fatalf("legitimate terminator SYN was not delivered to activeCh — over-aggressive P10.2 suppression regression")
	}

	time.Sleep(20 * time.Millisecond)
	snap := mux.ActiveTxnSnapshot()
	if snap.Active {
		t.Errorf("Active=true after legitimate terminator, want false")
	}
	if snap.InactiveReason != ReasonSYNTerminator {
		t.Errorf("InactiveReason=%q, want %q", snap.InactiveReason, ReasonSYNTerminator)
	}
}

// P10.2 — verify the response-phase / between-writes case (queue
// empty, bytesDeliveredToActive>0) still fires terminator path. This
// preserves the pre-P10.2 lifecycle contract for tests that simulate
// "gateway wrote, got echo, then SYN" without the explicit
// sendEndOfMessage step (legacy contract per PR #502).
func TestPreEchoSyn_QueueEmpty_TerminatorFiresAsBefore(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()

	// Write+echo+consume so queue is empty AND bytesDeliveredToActive>=1.
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("Write err=%v", err)
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x71}
	if _, err := at.ReadByte(); err != nil {
		t.Fatalf("ReadByte err=%v", err)
	}

	// Inject SYN — queue is empty, so peekNextExpected returns
	// (0, false). midWriteSyn=false. Terminator gate fires (legacy
	// PR #502 semantics retained).
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}

	deadline := time.Now().Add(1 * time.Second)
	delivered := false
	for time.Now().Before(deadline) {
		b, err := at.ReadByte()
		if err == nil && b == protocol.SymbolSyn {
			delivered = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !delivered {
		t.Fatalf("legacy terminator (queue empty) was not delivered — backward-compat regression")
	}
}

// P10.2 — verify that an unresponsive-target stuck-write scenario still
// terminates via idle release. This guards the
// `!suppressIdleRelease` carve-out: when bytesDeliveredToActive == 0
// and the gateway has pending writes that never echoed, idle release
// MUST fire so the transaction does not pin ownership forever.
func TestPreEchoSyn_StuckWrite_IdleReleaseStillFires(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()

	// Write 7 bytes; do NOT feed any echoes (simulates unresponsive
	// target — TestRegression_StartedButNoResponse production shape).
	go func() { _, _ = at.Write([]byte{0x08, 0xB5, 0x04, 0x01, 0x00, 0x00, 0xCD}) }()
	time.Sleep(20 * time.Millisecond)

	// Wait past IdleReleaseGrace and inject an idle SYN.
	time.Sleep(220 * time.Millisecond)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	snap := mux.ActiveTxnSnapshot()
	if snap.Active {
		t.Errorf("Active=true after idle-grace expiry — idle release was suppressed by P10.2 mid-write gate (regression)")
	}
	if snap.InactiveReason != ReasonSYNIdle {
		// SYN-terminator is acceptable too if the test fixture happened
		// to deliver the SYN as terminator, but for this scenario the
		// canonical reason is idle release.
		if snap.InactiveReason != ReasonSYNTerminator {
			t.Errorf("InactiveReason=%q, want %q (or ReasonSYNTerminator)", snap.InactiveReason, ReasonSYNIdle)
		}
	}
}
