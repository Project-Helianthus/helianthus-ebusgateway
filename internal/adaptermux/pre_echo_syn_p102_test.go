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


// TestPreEchoSyn_BetweenWritesSyn_Suppress (batch-26 round-7 — Attack 3
// closure). Verifies that a wire SYN arriving in the inter-write window
// (matchEcho consumed echo K, recordSent of K+1 not yet armed) is
// suppressed via the queueJustDrained sentinel, NOT delivered to
// activeCh, and the txn stays alive.
//
// Sequence modeled (matches the consultant timeline that confirmed
// Attack 3 dominance):
//  1. grantGateway → gatewayTxnActive=true, bytesDeliveredToActive=0.
//  2. at.Write(0x08) → recordSent(0x08), queue=[0x08].
//  3. Wire echo 0x08 → matchEcho consumes head, queue=[], preLen==1 +
//     received != SymbolSyn → queueJustDrained=true,
//     bytesDeliveredToActive=1.
//  4. at.ReadByte() drains 0x08 to the bus.Send consumer.
//  5. NOW (BEFORE the next Write) a noise wire SYN arrives.
//     onSYNLocked: hasPendingEcho=false, queueJustDrained=true,
//     gatewayTxnActive=true, bytesDeliveredToActive>0
//     → betweenWritesSyn=true, preEchoMidFrameSuppress=true
//     → terminator branch does NOT fire (gated on
//       !preEchoMidFrameSuppress)
//     → synSuppressedBetweenWrites counter increments
//     → flushOnSYN does NOT fire (queueJustDrained preserved)
//     → idle release does NOT fire (suppressIdleRelease gates).
//  6. at.Write(0xB5) → recordSent(0xB5) clears queueJustDrained and
//     arms queue=[0xB5]. Subsequent wire echo 0xB5 still matches
//     (sentinel did not corrupt downstream echo flow).
//
// This is the test that GATES round-7: pre-fix the SYN at step 5 races
// the real echo of 0xB5 and produces echo_mismatch with subclass
// pre_echo_syn_raw; post-fix the SYN is suppressed and no mismatch fires.
func TestPreEchoSyn_BetweenWritesSyn_Suppress(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()

	// Step 2 + 3 + 4: write a non-SYN body byte, feed its echo, consume.
	// After this, gatewayEcho is in the inter-write state
	// (queueJustDrained=true).
	if _, err := at.Write([]byte{0x08}); err != nil {
		t.Fatalf("Write(0x08) err=%v", err)
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x08}
	if b, err := at.ReadByte(); err != nil || b != 0x08 {
		t.Fatalf("ReadByte (echo of 0x08) = (0x%02X, %v), want (0x08, nil)", b, err)
	}

	mid := mux.ActiveTxnSnapshot()
	if mid.BytesDeliveredToActive == 0 {
		t.Fatalf("BytesDeliveredToActive=0 after first echo, want >=1 — test precondition")
	}
	suppressedBefore := mid.SynSuppressedPreEcho
	betweenWritesBefore := mid.SynSuppressedBetweenWrites

	// Step 5: noise SYN arrives in the inter-write window — BEFORE the
	// next Write fires recordSent. queueJustDrained should still be true
	// at this point because no recordSent/markRequestStart/flushOnSYN
	// has cleared it.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	snap := mux.ActiveTxnSnapshot()

	// Assertion (a): the round-7 subset counter incremented.
	if snap.SynSuppressedBetweenWrites <= betweenWritesBefore {
		t.Errorf("SynSuppressedBetweenWrites=%d, before=%d, want increment — Attack 3 suppression must fire on inter-write SYN",
			snap.SynSuppressedBetweenWrites, betweenWritesBefore)
	}

	// Assertion (b): umbrella SynSuppressedPreEcho also incremented
	// (the between-writes branch is part of preEchoMidFrameSuppress, so
	// the OR chain raises both).
	if snap.SynSuppressedPreEcho <= suppressedBefore {
		t.Errorf("SynSuppressedPreEcho=%d, before=%d, want increment — umbrella suppression counter must rise",
			snap.SynSuppressedPreEcho, suppressedBefore)
	}

	// Assertion (c): the txn stays active. The inter-write SYN must NOT
	// abort the transaction via terminator or idle release.
	if !snap.Active {
		t.Errorf("ActiveTxnSnapshot.Active=false, want true — inter-write SYN must not abort live txn")
	}
	if snap.InactiveReason == ReasonSYNTerminator {
		t.Errorf("InactiveReason=ReasonSYNTerminator — pre-round-7 bug: terminator path fired on inter-write SYN")
	}
	if snap.InactiveReason == ReasonSYNIdle {
		t.Errorf("InactiveReason=ReasonSYNIdle — idle release fired on inter-write SYN despite bytesDeliveredToActive>0")
	}

	// Step 6: write the next body byte, feed its echo, confirm no
	// downstream corruption (queueJustDrained must NOT carry forward
	// past recordSent).
	if _, err := at.Write([]byte{0xB5}); err != nil {
		t.Fatalf("Write(0xB5) err=%v", err)
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xB5}
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		b, err := at.ReadByte()
		if err == nil && b == 0xB5 {
			return // success
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("did not receive 0xB5 echo after inter-write SYN — gatewayEcho queue corrupted by flushOnSYN or queueJustDrained leaked")
}

// TestPreEchoSyn_InterWriteSyn_Suppress (batch-26 round-7) — reintroduces
// the round-6-shaped test that was removed in batch-25 hotfix, but with
// the NEW semantic. The original round-6 test asserted against the
// over-broad bytesDeliveredToActive gate (which caused throughput
// collapse). This version asserts against synSuppressedBetweenWrites
// (the tightly-bounded queueJustDrained sentinel that does NOT carry
// over across transactions).
//
// Distinguishing assertion: the prior round-6 test would have failed
// after a SYN terminator legitimately closed the txn AND a subsequent
// fresh-txn SYN arrived — it would still suppress because the broader
// "active flag + bytesDelivered" gate was never reset. Round-7's
// queueJustDrained IS reset by flushOnSYN, so the second txn's SYNs
// are NOT spuriously suppressed.
func TestPreEchoSyn_InterWriteSyn_Suppress(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()

	// Set up the inter-write state.
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("Write(0x71) err=%v", err)
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x71}
	if _, err := at.ReadByte(); err != nil {
		t.Fatalf("ReadByte err=%v", err)
	}

	betweenWritesBefore := mux.ActiveTxnSnapshot().SynSuppressedBetweenWrites

	// Inter-write SYN.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	snap := mux.ActiveTxnSnapshot()
	if snap.SynSuppressedBetweenWrites <= betweenWritesBefore {
		t.Errorf("SynSuppressedBetweenWrites=%d, before=%d, want increment via round-7 queueJustDrained gate (NOT the round-6 bytesDeliveredToActive gate)",
			snap.SynSuppressedBetweenWrites, betweenWritesBefore)
	}
	if !snap.Active {
		t.Errorf("Active=false, want true — round-7 must keep txn alive across the inter-write SYN")
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

// TestPreEchoSyn_QueueJustDrained_ClearedOnIdleRelease (batch-26
// round-7 — Codex r2 defect 3, MEDIUM) verifies that the
// queueJustDrained sentinel is bounded by IdleReleaseGrace, not by
// queueJustDrained itself.
//
// Failure mode pre-fix: the gateway consumed echo K (queueJustDrained
// set, bytesDeliveredToActive>0), then stalled before sendLoop
// recorded byte K+1. queueJustDrained stayed true; the inter-write
// suppression chain
// (betweenWritesSyn → preEchoMidFrameSuppress → suppressIdleRelease)
// blocked the IdleReleaseGrace path, so ownership persisted up to
// MaxOwnershipDuration (10s).
//
// Post-fix:
//   - suppressIdleRelease no longer reads queueJustDrained — it gates
//     only on midWriteSyn (queue head non-SYN), which is false once
//     the queue is empty.
//   - When idle release fires for the gateway owner, the branch
//     additionally calls gatewayEcho.ClearQueueJustDrained() so the
//     sentinel can't persist into the next grant.
//
// Scenario:
//  1. grantGateway (initiator 0x71) → busOwned=now, gatewayTxnActive
//     = true.
//  2. Write byte 0x71 — gatewayEcho.expectedEchoes=[0x71].
//  3. Inject echo 0x71 on wire → consumed via ReadByte →
//     expectedEchoes empty, queueJustDrained=true,
//     bytesDeliveredToActive=1.
//  4. Sleep > IdleReleaseGrace (200ms), then inject wire SYN.
//  5. Assert: gatewayTxnActive=false, ReasonSYNIdle,
//     queueJustDrained=false.
func TestPreEchoSyn_QueueJustDrained_ClearedOnIdleRelease(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()

	// Step 2: write a non-SYN byte and let it echo+consume so we leave
	// the pre-first-echo window (bytesDeliveredToActive=1) and the
	// expectedEchoes queue drains to empty (queueJustDrained=true).
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("Write(0x71) err=%v", err)
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x71}
	if b, err := at.ReadByte(); err != nil || b != 0x71 {
		t.Fatalf("ReadByte (echo of 0x71) = (0x%02X, %v), want (0x71, nil)", b, err)
	}

	// Precondition: the inter-write sentinel must be set. Read directly
	// from gatewayEcho — the public ActiveTxnSnapshot does not surface
	// this internal flag, and the test asserts on its lifecycle.
	mux.stateMu.Lock()
	preIdleDrained := mux.gatewayEcho.IsQueueJustDrained()
	bytesPre := mux.activeTxn.bytesDeliveredToActive.Load()
	mux.stateMu.Unlock()
	if !preIdleDrained {
		t.Fatalf("test precondition broken: queueJustDrained=false after consuming non-SYN echo; expected true")
	}
	if bytesPre == 0 {
		t.Fatalf("test precondition broken: bytesDeliveredToActive=0 after consuming echo; expected >=1")
	}

	// Step 4: wait past IdleReleaseGrace (200ms default) then inject the
	// wire SYN that should fire the idle-release branch.
	time.Sleep(220 * time.Millisecond)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	snap := mux.ActiveTxnSnapshot()

	// Assertion (a): the txn is no longer active. Pre-defect-3 the
	// stale sentinel would have left snap.Active=true here (since the
	// betweenWritesSyn-derived suppressIdleRelease blocked the grace
	// branch).
	if snap.Active {
		t.Errorf("ActiveTxnSnapshot.Active=true after IdleReleaseGrace expiry — idle release was suppressed by the queueJustDrained sentinel (regression)")
	}

	// Assertion (b): the teardown reason must be SYNIdle (NOT
	// SYNTerminator). Terminator classification would mean we mistook
	// the wire SYN for a frame terminator while bytesDeliveredToActive
	// was non-zero — that path is gated on `!preEchoMidFrameSuppress`
	// and pre-fix would not have fired here either.
	if snap.InactiveReason != ReasonSYNIdle {
		t.Errorf("InactiveReason=%q, want %q — stale queueJustDrained must be cleared via idle-grace path, not terminator", snap.InactiveReason, ReasonSYNIdle)
	}

	// Assertion (c): the sentinel was cleared by the idle-release
	// branch. Without ClearQueueJustDrained() the flag would persist
	// into the next grant and re-arm the suppression chain for an
	// unrelated future txn.
	mux.stateMu.Lock()
	postIdleDrained := mux.gatewayEcho.IsQueueJustDrained()
	mux.stateMu.Unlock()
	if postIdleDrained {
		t.Errorf("queueJustDrained=true after idle release; want false (ClearQueueJustDrained must fire in the gateway-owner idle-release branch)")
	}
}
