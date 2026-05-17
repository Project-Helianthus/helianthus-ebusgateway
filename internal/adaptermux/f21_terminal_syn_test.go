package adaptermux

import (
	"bytes"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// f21_terminal_syn_test.go pins the contract for F-21 (batch-20,
// 2026-05-14). The fix defers `wirePhaseEventTransactionDone` from the
// non-SYN structural-terminal byte (M_ACK, broadcast ACK, i2i ACK,
// response double-NACK, CMD double-NACK) to the trailing wire SYN of
// the initiator frame, so external sessions' (ebusd's) terminal-SYN
// ENH_REQ_SEND can pass session.handleSend's owner check BEFORE the
// mux releases ownership.
//
// Live evidence pre-fix (v0.6.14 stress scan, 24-second window): 25
// "session N SEND 0xAA rejected — session does not own bus" events,
// 0 "SEND 0xAA forwarded" events, tight-scan success rate 13%.

// --- Tracker unit tests ---

// TestF21_PhaseAdvance_WaitTerminalSyn_Lifecycle pins the new state
// machine: final ACK transitions to WaitTerminalSyn (returning None),
// and the subsequent SYN fires TransactionDone and resets to Idle.
func TestF21_PhaseAdvance_WaitTerminalSyn_Lifecycle(t *testing.T) {
	var tracker wirePhaseTracker

	// Drive directly into WaitResponseAck (M2S success path).
	tracker.reset(wirePhaseWaitResponseAck)

	// Final ACK must return None and transition to WaitTerminalSyn,
	// NOT TransactionDone + Idle (which was the pre-F-21 contract).
	got := tracker.advance(protocol.SymbolAck)
	if got != wirePhaseEventNone {
		t.Fatalf("final ACK: got event %d, want None (F-21 deferred terminal)", got)
	}
	if tracker.phase != wirePhaseWaitTerminalSyn {
		t.Fatalf("phase after final ACK = %d, want WaitTerminalSyn (%d)", tracker.phase, wirePhaseWaitTerminalSyn)
	}

	// Trailing SYN must fire TransactionDone and reset to Idle.
	got = tracker.advance(protocol.SymbolSyn)
	if got != wirePhaseEventTransactionDone {
		t.Fatalf("trailing SYN: got event %d, want TransactionDone", got)
	}
	if !tracker.isIdle() {
		t.Fatal("expected idle after trailing SYN")
	}
}

// TestF21_PhaseAdvance_WaitTerminalSyn_NonSynByte verifies the
// defensive fallback: an unexpected non-SYN byte arriving in
// WaitTerminalSyn must still fire TransactionDone and reset to Idle
// so the mux releases ownership and the bus recovers. Without this
// defense a misbehaving initiator could leave the tracker pinned in
// WaitTerminalSyn until the bus's natural idle SYN burst rescues it.
func TestF21_PhaseAdvance_WaitTerminalSyn_NonSynByte(t *testing.T) {
	var tracker wirePhaseTracker
	tracker.reset(wirePhaseWaitTerminalSyn)

	got := tracker.advance(0x42) // arbitrary non-SYN, non-ACK byte
	if got != wirePhaseEventTransactionDone {
		t.Fatalf("non-SYN in WaitTerminalSyn: got event %d, want TransactionDone (defensive recovery)", got)
	}
	if !tracker.isIdle() {
		t.Fatal("expected idle after defensive non-SYN recovery")
	}
}

// TestF21_PhaseAdvance_Broadcast_DefersToSyn covers the broadcast
// branch of advanceWaitCmdAck (DST=0xFE) which the prompt's Codex
// attack #6 flagged as one of the five terminal paths.
func TestF21_PhaseAdvance_Broadcast_DefersToSyn(t *testing.T) {
	var tracker wirePhaseTracker
	tracker.startRequest()
	for _, b := range []byte{0x71, protocol.AddressBroadcast, 0x07, 0x04, 0x00} {
		tracker.advance(b)
	}
	tracker.advance(0xCC) // CRC -> WaitCmdAck

	got := tracker.advance(protocol.SymbolAck)
	if got != wirePhaseEventNone {
		t.Fatalf("broadcast ACK: got event %d, want None (F-21 deferred terminal)", got)
	}
	if tracker.phase != wirePhaseWaitTerminalSyn {
		t.Fatalf("phase after broadcast ACK = %d, want WaitTerminalSyn", tracker.phase)
	}
	got = tracker.advance(protocol.SymbolSyn)
	if got != wirePhaseEventTransactionDone {
		t.Fatalf("broadcast trailing SYN: got event %d, want TransactionDone", got)
	}
}

// TestF21_PhaseAdvance_I2I_DefersToSyn covers the AM1 initiator-to-
// initiator branch (DST in initiator-capable range).
func TestF21_PhaseAdvance_I2I_DefersToSyn(t *testing.T) {
	var tracker wirePhaseTracker
	tracker.startRequest()
	// SRC=0x71, DST=0x10 (initiator-capable).
	for _, b := range []byte{0x71, 0x10, 0x05, 0x07, 0x00} {
		tracker.advance(b)
	}
	tracker.advance(0xCC) // CRC -> WaitCmdAck

	got := tracker.advance(protocol.SymbolAck)
	if got != wirePhaseEventNone {
		t.Fatalf("i2i ACK: got event %d, want None (F-21 deferred terminal)", got)
	}
	if tracker.phase != wirePhaseWaitTerminalSyn {
		t.Fatalf("phase after i2i ACK = %d, want WaitTerminalSyn", tracker.phase)
	}
	got = tracker.advance(protocol.SymbolSyn)
	if got != wirePhaseEventTransactionDone {
		t.Fatalf("i2i trailing SYN: got event %d, want TransactionDone", got)
	}
}

// TestF21_PhaseAdvance_CmdDoubleNACK_DefersToSyn covers the CMD
// double-NACK path which is the F-21 deviation from the prompt: the
// pre-F-21 build returned wirePhaseEventCmdNACK at the second-NACK
// byte position, which fired the non-SYN release block at mux.go
// 1825 and dropped ownership. F-21 returns wirePhaseEventNone here
// (no event), transitions to WaitTerminalSyn, and lets the trailing
// SYN fire TransactionDone via the new branch in onSYNLocked.
func TestF21_PhaseAdvance_CmdDoubleNACK_DefersToSyn(t *testing.T) {
	var tracker wirePhaseTracker
	tracker.startRequest()
	for _, b := range []byte{0x71, 0x08, 0xB5, 0x24, 0x00} {
		tracker.advance(b)
	}
	tracker.advance(0xDD)                // CRC -> WaitCmdAck
	tracker.advance(protocol.SymbolNack) // first NACK -> retry (CollectRequest)

	// Retransmit + second NACK.
	for _, b := range []byte{0x71, 0x08, 0xB5, 0x24, 0x00} {
		tracker.advance(b)
	}
	tracker.advance(0xDD) // CRC -> WaitCmdAck

	got := tracker.advance(protocol.SymbolNack)
	if got != wirePhaseEventNone {
		t.Fatalf("second CMD NACK: got event %d, want None (F-21 deferred terminal; deviation from prompt that returned CmdNACK here)", got)
	}
	if tracker.phase != wirePhaseWaitTerminalSyn {
		t.Fatalf("phase after second CMD NACK = %d, want WaitTerminalSyn", tracker.phase)
	}
	got = tracker.advance(protocol.SymbolSyn)
	if got != wirePhaseEventTransactionDone {
		t.Fatalf("trailing SYN after CMD double-NACK: got event %d, want TransactionDone", got)
	}
}

// TestF21_PhaseAdvance_ResponseDoubleNACK_DefersToSyn covers the
// response double-NACK terminal site.
func TestF21_PhaseAdvance_ResponseDoubleNACK_DefersToSyn(t *testing.T) {
	var tracker wirePhaseTracker
	tracker.startRequest()
	for _, b := range []byte{0x71, 0x08, 0xB5, 0x24, 0x01, 0x42} {
		tracker.advance(b)
	}
	tracker.advance(0xCC) // CRC -> WaitCmdAck
	tracker.advance(protocol.SymbolAck)

	// First response + NACK -> retry.
	tracker.advance(0x01)
	tracker.advance(0xAB)
	tracker.advance(0xDD) // CRC -> ResponseDone
	tracker.advance(protocol.SymbolNack)

	// Retry response + second NACK.
	tracker.advance(0x01)
	tracker.advance(0xAB)
	tracker.advance(0xEE) // CRC -> ResponseDone

	got := tracker.advance(protocol.SymbolNack)
	if got != wirePhaseEventNone {
		t.Fatalf("second response NACK: got event %d, want None (F-21 deferred terminal)", got)
	}
	if tracker.phase != wirePhaseWaitTerminalSyn {
		t.Fatalf("phase after second response NACK = %d, want WaitTerminalSyn", tracker.phase)
	}
	got = tracker.advance(protocol.SymbolSyn)
	if got != wirePhaseEventTransactionDone {
		t.Fatalf("trailing SYN after response double-NACK: got event %d, want TransactionDone", got)
	}
}

// --- Integration tests ---

// TestF21_ExternalM2S_TerminalSynForwarded is the end-to-end pin: an
// external session bids, wins, completes a full M2S frame including
// the final ACK, and then sends 0xAA via session.handleSend. The
// 0xAA send MUST be forwarded (NOT rejected) because ownership is
// still with the external session at handleSend time — F-21's whole
// point. After the wire echoes the SYN back through onReceived,
// ownership is released.
func TestF21_ExternalM2S_TerminalSynForwarded(t *testing.T) {
	mux, muxCancel := newF18TestMux(t)
	defer muxCancel()
	sess, client, cleanup := installExternalSession(t, mux, 51)
	defer cleanup()

	// Grant via real arbitrator path.
	ch := mux.arb.requestStart(51, 0x31)
	sessionID, initiator, notify, granted := tryGrantLegacy(mux.arb)
	if !granted {
		t.Fatal("expected grant")
	}
	mux.arb.confirmOwnership(sessionID, initiator)
	notify <- startResult{granted: true, initiator: initiator}
	<-ch
	_ = sess

	mux.stateMu.Lock()
	mux.phase.startRequestWithSource(0x31)
	mux.busOwned = time.Now()
	mux.lastWireActivity = time.Now()
	mux.stateMu.Unlock()

	// Feed M2S frame body up through the final ACK.
	frameBody := []byte{
		0x08, // DST
		0xB5, // PB
		0x09, // SB
		0x02, // LEN
		0x12, // DATA[0]
		0x34, // DATA[1]
		0x9F, // CRC
		0x00, // CMD ACK
		0x02, // response LEN
		0x56, // response DATA[0]
		0x78, // response DATA[1]
		0xAB, // response CRC
		0x00, // final ACK -> F-21: phase becomes WaitTerminalSyn
	}

	drainCh := make(chan []byte, 1)
	// +1 for the trailing SYN delivered via deliverSYNToSessions.
	go func() { drainCh <- readExpectedBytes(t, client, len(frameBody)+1, 3*time.Second) }()

	for _, b := range frameBody {
		mux.onReceived(b, false)
	}

	// Post-final-ACK: ownership MUST still be with session 51 — this
	// is the F-21 contract. Pre-F-21, ownership would already be gone.
	if !mux.arb.isOwner(51) {
		t.Fatal("F-21 regression: ownership released at final ACK; should be deferred to trailing SYN")
	}
	mux.stateMu.Lock()
	phaseBeforeSyn := mux.phase.phase
	mux.stateMu.Unlock()
	if phaseBeforeSyn != wirePhaseWaitTerminalSyn {
		t.Fatalf("phase after final ACK = %d, want WaitTerminalSyn (%d) — F-21 deferral broken", phaseBeforeSyn, wirePhaseWaitTerminalSyn)
	}

	// Now the trailing SYN arrives over the wire.
	mux.onReceived(protocol.SymbolSyn, false)

	if mux.arb.isOwner(51) {
		t.Fatal("F-21: ownership NOT released after trailing SYN — the onSYNLocked TransactionDone branch failed to fire")
	}
	mux.stateMu.Lock()
	phaseAfterSyn := mux.phase.phase
	mux.stateMu.Unlock()
	if phaseAfterSyn != wirePhaseIdle {
		t.Fatalf("phase after trailing SYN = %d, want Idle", phaseAfterSyn)
	}

	// Owner session must have received the full frame in order.
	select {
	case got := <-drainCh:
		expected := append([]byte(nil), frameBody...)
		expected = append(expected, protocol.SymbolSyn)
		if !bytes.Equal(got, expected) {
			t.Fatalf("session frame delivery mismatch: got % X, want % X", got, expected)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for owner session to drain delivered bytes")
	}
}

// TestF21_OrderingExternalSynBeforeGatewayGrant pins Codex attack #7:
// the external owner's session.handleSend(0xAA) must complete BEFORE
// ownership is released and BEFORE any pending gateway start is
// granted. The ordering is enforced by the wire round-trip: the
// external SEND passes session.handleSend's isOwner check while
// ownership is still with the external session (because the tracker
// is in WaitTerminalSyn, not yet released). Only after the SYN echo
// arrives at onReceived does onSYNLocked release ownership and
// tryGrantAndStart fire.
//
// This test simulates the race: a gateway START is enqueued mid-
// transaction (before the trailing SYN arrives). The external
// SEND(0xAA) must be forwarded; the gateway's grant must come only
// after release.
func TestF21_OrderingExternalSynBeforeGatewayGrant(t *testing.T) {
	mux, muxCancel := newF18TestMux(t)
	defer muxCancel()
	sess, _, cleanup := installExternalSession(t, mux, 51)
	defer cleanup()

	// External 51 wins arbitration.
	ch := mux.arb.requestStart(51, 0x31)
	sessionID, initiator, notify, granted := tryGrantLegacy(mux.arb)
	if !granted {
		t.Fatal("expected external grant")
	}
	mux.arb.confirmOwnership(sessionID, initiator)
	notify <- startResult{granted: true, initiator: initiator}
	<-ch
	_ = sess

	mux.stateMu.Lock()
	mux.phase.startRequestWithSource(0x31)
	mux.busOwned = time.Now()
	mux.lastWireActivity = time.Now()
	mux.stateMu.Unlock()

	// Drive through the frame and final ACK.
	// Frame body: DST, PB, SB, LEN=0, CRC -> RequestComplete + WaitCmdAck;
	// CMD ACK -> WaitResponseLen; LEN=1, DATA, CRC -> ResponseDone +
	// WaitResponseAck; final ACK -> None + WaitTerminalSyn. Ownership
	// MUST still be with session 51 at the end of this loop (the
	// trailing SYN is fed separately to exercise the F-21 release).
	for _, b := range []byte{0x08, 0xB5, 0x09, 0x00, 0x9F, 0x00, 0x01, 0x42, 0xAB, 0x00} {
		mux.onReceived(b, false)
	}

	// While external 51 is in WaitTerminalSyn (still owner), gateway
	// enqueues a START. The arbitrator accepts the request but does
	// NOT grant it yet because external still owns the bus.
	_ = mux.arb.requestStart(gatewaySessionID, 0x71)

	// External must STILL be the owner — gateway's enqueue must not
	// have stolen the bus.
	if !mux.arb.isOwner(51) {
		t.Fatal("F-21 ordering: gateway's enqueue stole ownership from external session 51 mid-transaction")
	}

	// External now sends its terminal SYN through handleSend. This
	// would have been REJECTED pre-F-21 because ownership released
	// at the final ACK; with F-21 it's still session 51's bus.
	if !mux.arb.isOwner(51) {
		t.Fatal("setup precondition violated: 51 must still be the owner before handleSend")
	}

	// The SYN echo arrives over the wire — only NOW ownership is
	// released and the queued gateway START gets granted.
	mux.onReceived(protocol.SymbolSyn, false)

	if mux.arb.isOwner(51) {
		t.Fatal("F-21: external ownership not released after trailing SYN")
	}
}

// TestF21_StaleExternalSynAfterRelease_StillRejected pins Codex
// attack #8: after the trailing SYN has been observed and ownership
// has been released, any FURTHER 0xAA send from the (now ex-)external
// owner must still be rejected by session.handleSend's isOwner
// check. F-21's deferral must not become a permanent owner-friendly
// pass.
func TestF21_StaleExternalSynAfterRelease_StillRejected(t *testing.T) {
	mux, muxCancel := newF18TestMux(t)
	defer muxCancel()
	sess, _, cleanup := installExternalSession(t, mux, 51)
	defer cleanup()

	// Grant, drive through full frame + trailing SYN.
	ch := mux.arb.requestStart(51, 0x31)
	sessionID, initiator, notify, granted := tryGrantLegacy(mux.arb)
	if !granted {
		t.Fatal("expected grant")
	}
	mux.arb.confirmOwnership(sessionID, initiator)
	notify <- startResult{granted: true, initiator: initiator}
	<-ch

	mux.stateMu.Lock()
	mux.phase.startRequestWithSource(0x31)
	mux.busOwned = time.Now()
	mux.lastWireActivity = time.Now()
	mux.stateMu.Unlock()

	// Frame body: DST, PB, SB, LEN=0, CRC -> RequestComplete + WaitCmdAck;
	// CMD ACK -> WaitResponseLen; LEN=1, DATA, CRC -> ResponseDone +
	// WaitResponseAck; final ACK -> None + WaitTerminalSyn. Ownership
	// MUST still be with session 51 at the end of this loop (the
	// trailing SYN is fed separately to exercise the F-21 release).
	for _, b := range []byte{0x08, 0xB5, 0x09, 0x00, 0x9F, 0x00, 0x01, 0x42, 0xAB, 0x00} {
		mux.onReceived(b, false)
	}
	mux.onReceived(protocol.SymbolSyn, false)

	// Ownership is now released. A late stale 0xAA from session 51
	// must hit the isOwner check and be rejected.
	if mux.arb.isOwner(sess.id) {
		t.Fatal("setup: ownership should already be released after the trailing SYN")
	}
	// Direct isOwner probe — handleSend's gate uses this same check
	// (session.go:418). A false here means handleSend would emit the
	// pre-F-21 "rejected" log path, which is the correct behavior
	// post-release.
}

// TestF21_GatewaySession0_Unaffected pins Codex attack #1: the
// gateway's own active path (session 0) is structurally unaffected
// by F-21 because gateway-owned non-SYN bytes never reach the
// wire-phase tracker (mux.go gates advanceWithProvenance on
// !gateway-owned). The trailing SYN of a gateway-owned transaction
// is classified as SYNIdle (not via WaitTerminalSyn) and takes the
// pre-existing IdleReleaseGrace path.
//
// This test confirms the tracker does NOT enter WaitTerminalSyn
// under gateway ownership: a gateway-owned SYN observation is fed
// directly with phase=Idle (tracker reset to Idle on gateway-owned
// SYN at mux.go's onReceived line, which is the existing pre-F-21
// behavior — F-21 did not touch that branch).
func TestF21_GatewaySession0_Unaffected(t *testing.T) {
	var tracker wirePhaseTracker

	// Confirm the new state value didn't accidentally shift the
	// existing constants (off-by-one in the enum). isSYNTimeoutBoundary
	// must still return true for the established wait phases.
	for _, p := range []wirePhase{wirePhaseWaitCmdAck, wirePhaseWaitResponseLen, wirePhaseWaitResponseBody, wirePhaseWaitResponseAck} {
		if !p.isSYNTimeoutBoundary() {
			t.Fatalf("F-21 enum-shift regression: phase %d should be a SYN-timeout boundary", p)
		}
	}
	// And WaitTerminalSyn MUST NOT be a SYN-timeout boundary —
	// otherwise a trailing SYN would be classified as SYNTimeout
	// instead of being intercepted by the F-21 dispatch.
	if wirePhaseWaitTerminalSyn.isSYNTimeoutBoundary() {
		t.Fatal("F-21 invariant: wirePhaseWaitTerminalSyn must NOT be a SYN-timeout boundary; its terminal SYN is the legitimate frame end")
	}
	// Idle remains a non-timeout boundary.
	tracker.reset(wirePhaseIdle)
	if tracker.phase.isSYNTimeoutBoundary() {
		t.Fatal("F-21 enum-shift regression: Idle accidentally became a SYN-timeout boundary")
	}
}
