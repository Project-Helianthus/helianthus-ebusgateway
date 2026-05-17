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

// P10.2 round-6 (batch-24) — verify the inter-write race window
// (gateway txn active, queue currently empty, bytesDeliveredToActive>0
// because at least one prior echo was delivered) is now classified as a
// wire-SYN intrusion and NOT as a frame terminator.
//
// Prior semantics (pre-round-6, PR #502 legacy "queue empty → assume
// terminator"): a SYN here was delivered to activeCh and gatewayTxnActive
// flipped to false. This was the source of the residual ~3.3/min leak
// observed after round-5 closed the transport-deadline path:
// synSeenWhileInterWriteEmpty counter was firing at ~1.27/min and a
// further share of grantWindow events (Attack 1, ~127/min) was leaking
// through. Under round-6, every legitimate terminator goes through
// bus.Bus.sendEndOfMessage → sendRawWithEcho(SymbolSyn), so by the time
// the wire echoes the terminator, recordSent(SymbolSyn) has populated
// the echo queue and the terminator gate fires through the
// `hasPendingEcho && nextExpected==SymbolSyn` path (see
// TestPreEchoSyn_LegitimateTerminator_Delivered for the positive case).
// A SYN with queue empty is therefore unambiguously a wire intrusion.
//
// Round-6 behavior:
//   - synSuppressedPreEcho counter increments (preEchoMidFrameSuppress
//     fires through the interWriteSyn branch).
//   - txn stays active (terminator gate gated on !preEchoMidFrameSuppress).
//   - the next echo of an upcoming write byte still matches (queue
//     not flushed).
func TestPreEchoSyn_InterWriteSyn_Suppress(t *testing.T) {
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

	mid := mux.ActiveTxnSnapshot()
	if mid.BytesDeliveredToActive == 0 {
		t.Fatalf("BytesDeliveredToActive=0 after first echo, want >=1 — test precondition")
	}
	if !mid.Active {
		t.Fatalf("Active=false after first write+echo, want true — test precondition")
	}
	suppressedBefore := mid.SynSuppressedPreEcho

	// Inject wire SYN — queue empty (hasPendingEcho=false), txn active,
	// bytesDelivered>0 → interWriteSyn=true → preEchoMidFrameSuppress=true
	// under round-6. Terminator gate must NOT fire.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	snap := mux.ActiveTxnSnapshot()

	// Assertion (a): synSuppressedPreEcho incremented (round-6 inter-write
	// branch fired).
	if snap.SynSuppressedPreEcho <= suppressedBefore {
		t.Errorf("SynSuppressedPreEcho=%d, suppressedBefore=%d, want increment — inter-write SYN must be classified as noise (round-6)",
			snap.SynSuppressedPreEcho, suppressedBefore)
	}

	// Assertion (b): txn is STILL active. The inter-write SYN must not
	// have aborted the transaction via terminator path.
	if !snap.Active {
		t.Errorf("ActiveTxnSnapshot.Active=false, want true — inter-write SYN must not abort live txn under round-6")
	}
	if snap.InactiveReason == ReasonSYNTerminator {
		t.Errorf("InactiveReason=ReasonSYNTerminator — pre-round-6 bug: terminator path fired on inter-write SYN")
	}

	// Assertion (c): a subsequent legitimate echo of a new write still
	// matches (queue was not flushed by the suppressed SYN).
	if _, err := at.Write([]byte{0x08}); err != nil {
		t.Fatalf("Write(0x08) err=%v", err)
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x08}
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		b, err := at.ReadByte()
		if err == nil && b == 0x08 {
			return // success
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("did not receive 0x08 echo after suppressed inter-write SYN — gatewayEcho queue corrupted")
}

// P10.2 round-6 (batch-24) — verify the grant-window race
// (wasGatewayOwned=true, gatewayTxnActive=false because the first
// activeTransport.Write has not yet flipped the flag, hasPendingEcho=false)
// is classified as a wire-SYN intrusion and suppressed.
//
// Production motivation: Attack 1 counter (synSeenDuringGrantWindow)
// fires at ~127/min in live HA gateway soak. Pre-round-6, P10.2's
// `m.gatewayTxnActive` precondition bypassed this entire population.
// Even a 2-3% translation rate from Attack 1 events to
// echo_mismatch:pre_echo_syn_raw at the next Write echo position
// accounts for ~3/min of the residual leak after round-5.
//
// Round-6 behavior:
//   - synSuppressedPreEcho counter increments (preEchoMidFrameSuppress
//     fires through the grantSyn branch).
//   - gateway echo queue is NOT flushed.
//   - SYN is NOT delivered to activeCh (caller's deliverToActive is
//     skipped because preEchoSuppressed=true forces activeExpects=false
//     in onReceived).
func TestPreEchoSyn_GrantSyn_Suppress(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	// Note: grantGateway returns after the grant is confirmed. At this
	// point: wasGatewayOwned=true, gatewayTxnActive=false (set false in
	// completeArbitrationGrant, not flipped to true until first Write
	// hits sendLoop's recordSent path).

	pre := mux.ActiveTxnSnapshot()
	if pre.Active {
		t.Fatalf("Active=true immediately after grant, want false — sendLoop has not yet recordSent (test precondition)")
	}
	if pre.BytesDeliveredToActive != 0 {
		t.Fatalf("BytesDeliveredToActive=%d after grant, want 0 — test precondition", pre.BytesDeliveredToActive)
	}
	suppressedBefore := pre.SynSuppressedPreEcho
	grantWindowBefore := pre.SynSeenDuringGrantWindow

	// Inject wire SYN before any Write happens.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	snap := mux.ActiveTxnSnapshot()

	// Assertion (a): the diagnostic Attack 1 counter saw the SYN.
	if snap.SynSeenDuringGrantWindow <= grantWindowBefore {
		t.Errorf("SynSeenDuringGrantWindow=%d, before=%d, want increment — Attack 1 diag must observe grant-window SYN",
			snap.SynSeenDuringGrantWindow, grantWindowBefore)
	}

	// Assertion (b): synSuppressedPreEcho incremented (round-6 grant-window
	// branch fired through grantSyn condition).
	if snap.SynSuppressedPreEcho <= suppressedBefore {
		t.Errorf("SynSuppressedPreEcho=%d, suppressedBefore=%d, want increment — grant-window SYN must be suppressed under round-6",
			snap.SynSuppressedPreEcho, suppressedBefore)
	}

	// Assertion (c): the gateway echo queue is intact — a subsequent
	// Write/echo cycle still matches without echo_mismatch.
	at := mux.ActiveTransport()
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("Write(0x71) err=%v", err)
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x71}
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		b, err := at.ReadByte()
		if err == nil && b == 0x71 {
			return // success — round-6 fix preserved first-write echo path
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("did not receive 0x71 echo after suppressed grant-window SYN — gatewayEcho queue corrupted by flushOnSYN despite round-6 suppression")
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
	// Codex P10.2 review LOW: require ReasonSYNIdle ONLY. Accepting
	// ReasonSYNTerminator here would hide the regression this test is
	// meant to catch — in this scenario bytesDeliveredToActive==0 and
	// the queue head is non-SYN (mid-write), so a terminator
	// classification would mean we falsely treated the wire SYN as a
	// frame terminator instead of a stuck-write idle bail-out.
	if snap.InactiveReason != ReasonSYNIdle {
		t.Errorf("InactiveReason=%q, want %q (stuck-write must terminate via idle release, NOT terminator)", snap.InactiveReason, ReasonSYNIdle)
	}
}
