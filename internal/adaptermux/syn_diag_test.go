package adaptermux

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// TestSynDiag_RecordsOwnershipTransition verifies that the SYN
// diagnostics ring records a single entry capturing gwActiveBefore,
// gwActiveAfter, lastWrittenByte, bytesRead, and synDeliveredToActive
// when a SYN arrives while the gateway owns the bus.
//
// Sequence:
//  1. grant gateway ownership (initiator 0x71)
//  2. feed one write byte + its echo so writePrefix has a last byte and
//     bytesRead > 0 (so the SYN-before-read guard lets the SYN-terminator
//     branch clear gatewayTxnActive)
//  3. inject a SYN
//  4. assert ring has exactly 1 entry with the expected transition
//
// The critical field under test is synDeliveredToActive: with the
// PR #502 E2E fix, when onSYNLocked's bytesRead>0 branch fires it
// delivers the SYN byte to activeCh BEFORE clearing gatewayTxnActive,
// so the bus.Send consumer DOES see the terminator. The diag entry
// records synDeliveredToActive=true and the reason is ReasonSYNTerminator
// (distinct from ReasonSYNIdle which is reserved for abandoned-grant
// idle-release).
func TestSynDiag_RecordsOwnershipTransition(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)

	// Write one byte so writePrefix has a last entry.
	at := mux.ActiveTransport()
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("Write err=%v", err)
	}

	// Feed the echo so bytesRead becomes > 0 (required for the SYN-
	// before-read guard to LET the SYN-terminator branch clear
	// gatewayTxnActive — otherwise the SYN is treated as pre-grant stale
	// and gatewayTxnActive stays true).
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x71}
	if _, err := at.ReadByte(); err != nil {
		t.Fatalf("ReadByte (echo) err=%v", err)
	}

	// Inject the trailing SYN.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	entries := mux.SynDiagSnapshot()
	if len(entries) != 1 {
		t.Fatalf("SynDiagSnapshot len=%d, want 1: %+v", len(entries), entries)
	}
	e := entries[0]

	if !e.GatewayOwned {
		t.Errorf("GatewayOwned=false, want true (gateway should own at SYN arrival)")
	}
	if !e.GwActiveBefore {
		t.Errorf("GwActiveBefore=false, want true (txn was active at SYN entry)")
	}
	if e.GwActiveAfter {
		t.Errorf("GwActiveAfter=true, want false (bytesRead>0 → SYN-idle branch clears)")
	}
	if !e.HasLastWrittenByte || e.LastWrittenByte != 0x71 {
		t.Errorf("LastWrittenByte=(has=%v,val=0x%02X), want has=true val=0x71", e.HasLastWrittenByte, e.LastWrittenByte)
	}
	if e.BytesRead == 0 {
		t.Errorf("BytesRead=0, want >0 (echo was consumed before SYN)")
	}
	if !e.SynDeliveredToActive {
		t.Errorf("SynDeliveredToActive=false, want true — PR #502 E2E fix: the bytesRead>0 branch MUST deliver the SYN terminator to activeCh before clearing gatewayTxnActive so bus.Send sees the frame-terminating SYN.")
	}
	if e.InactiveReason != ReasonSYNTerminator {
		t.Errorf("InactiveReason=%q, want %q (PR #502: terminator path is distinct from SYN-idle abandoned-grant path)", e.InactiveReason, ReasonSYNTerminator)
	}
}

// TestSynDiag_RingBoundedToCap verifies that the SYN ring never grows
// past synDiagRingCap even under many SYN events.
func TestSynDiag_RingBoundedToCap(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)

	// Feed many SYNs; each one that arrives while gatewayTxnActive is
	// still true (first one) or during ownership (subsequent) gets
	// recorded — but capped at synDiagRingCap. We also feed a write+echo
	// so bytesRead>0 for at least the first SYN.
	at := mux.ActiveTransport()
	_, _ = at.Write([]byte{0x71})
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x71}
	_, _ = at.ReadByte()

	for i := 0; i < synDiagRingCap*3; i++ {
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	}
	time.Sleep(100 * time.Millisecond)

	entries := mux.SynDiagSnapshot()
	if len(entries) > synDiagRingCap {
		t.Fatalf("SynDiagSnapshot len=%d, want <= %d (bounded ring)", len(entries), synDiagRingCap)
	}
}

// TestSynDiag_SkipsNonGatewayOwnership verifies that SYNs arriving
// while the gateway does NOT own the bus are NOT recorded — the
// diagnostic is scoped to the hypothesis-relevant window only.
func TestSynDiag_SkipsNonGatewayOwnership(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// No grant — no gateway ownership. Feed SYNs; none should record.
	for i := 0; i < 4; i++ {
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	}
	time.Sleep(30 * time.Millisecond)

	entries := mux.SynDiagSnapshot()
	if len(entries) != 0 {
		t.Fatalf("SynDiagSnapshot len=%d, want 0 when gateway does not own the bus: %+v", len(entries), entries)
	}
}

// TestOnSYNLocked_DeliversTerminatorToActive_WhenBytesReadPositive verifies
// the PR #502 E2E fix: when a SYN arrives mid-transaction (the initiator
// has left the pre-echo window, i.e. bytesWritten>0), onSYNLocked MUST
// deliver the SYN byte to activeCh BEFORE clearing gatewayTxnActive, so
// bus.Send observes the frame terminator on its FIFO activeCh reader.
//
// Without this delivery, the last byte of a transaction is consumed as
// a lifecycle signal inside the mux and never reaches the protocol.Bus
// consumer; bus.Send then hangs until its read deadline fires (ok=0
// timeouts in soak), which is the exact symptom documented by
// TestE2E_ScanB504_ToTarget0x08_SuccessFullFlow.
//
// Note: the name retains "BytesReadPositive" for historical continuity
// with PR #502, but the actual gate is bytesWritten>0 (see comment on
// preEchoSuppressed in onSYNLocked — gating on bytesRead would lag
// behind activeCh delivery whenever the consumer is slower than
// readLoop, over-suppressing legitimate terminator SYNs).
func TestOnSYNLocked_DeliversTerminatorToActive_WhenBytesReadPositive(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)

	at := mux.ActiveTransport()

	// Write first so bytesWritten>=1 — this mimics bus.Send having begun
	// transmission, which is what moves us out of the pre-echo window.
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("at.Write err=%v", err)
	}

	// Feed the echo back + consume it so bytesRead>=1.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x71}
	if b, err := at.ReadByte(); err != nil || b != 0x71 {
		t.Fatalf("ReadByte=(0x%02X,%v), want (0x71,nil)", b, err)
	}

	// Inject the trailing SYN — this should be delivered to activeCh.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}

	// Read it back via the same ActiveTransport path bus.Send uses. This
	// will fail with a timeout if the terminator never reached activeCh.
	readDone := make(chan struct {
		b   byte
		err error
	}, 1)
	go func() {
		b, err := at.ReadByte()
		readDone <- struct {
			b   byte
			err error
		}{b, err}
	}()

	select {
	case r := <-readDone:
		if r.err != nil {
			t.Fatalf("ReadByte err=%v — terminator SYN missing from activeCh (regression)", r.err)
		}
		if r.b != protocol.SymbolSyn {
			t.Fatalf("ReadByte=0x%02X, want 0xAA (SYN terminator)", r.b)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("ReadByte timed out — terminator SYN was consumed by onSYNLocked and never delivered to activeCh (PR #502 regression)")
	}

	// Give the mux a beat to finish updating state.
	time.Sleep(20 * time.Millisecond)

	// Verify lifecycle state transitioned off and the diag confirms delivery.
	snap := mux.ActiveTxnSnapshot()
	if snap.Active {
		t.Errorf("ActiveTxnSnapshot.Active=true, want false after SYN-terminator clear")
	}
	if snap.InactiveReason != ReasonSYNTerminator {
		t.Errorf("InactiveReason=%q, want %q", snap.InactiveReason, ReasonSYNTerminator)
	}

	entries := mux.SynDiagSnapshot()
	if len(entries) == 0 {
		t.Fatalf("SynDiagSnapshot empty, want >=1 entry for terminator")
	}
	last := entries[len(entries)-1]
	if !last.SynDeliveredToActive {
		t.Errorf("last SynDiagEntry.SynDeliveredToActive=false, want true (PR #502 terminator delivered inline)")
	}
	if last.GwActiveBefore != true || last.GwActiveAfter != false {
		t.Errorf("SynDiagEntry gwActive transition = before=%v after=%v, want before=true after=false",
			last.GwActiveBefore, last.GwActiveAfter)
	}
	if last.InactiveReason != ReasonSYNTerminator {
		t.Errorf("SynDiagEntry.InactiveReason=%q, want %q", last.InactiveReason, ReasonSYNTerminator)
	}
}

// TestOnSYNLocked_NoTerminatorDeliveryWhenBytesReadZero documents the
// pre-write stale-SYN semantics: a SYN arriving after grant but before the
// active caller has started Write must not be treated as a terminator and
// must not be delivered to activeCh. The active transaction is armed by
// Write, so this SYN is ignored rather than counted as active pre-echo
// suppression.
func TestOnSYNLocked_NoTerminatorDeliveryWhenBytesReadZero(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)

	before := mux.ActiveTxnSnapshot().SynSuppressedPreEcho

	// Do NOT consume any bytes via ReadByte — bytesRead stays 0.
	// Inject a SYN (simulating a pre-grant stale SYN that arrived in the
	// TCP buffer before STARTED).
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	// gatewayTxnActive has not been armed yet because no Write started.
	snap := mux.ActiveTxnSnapshot()
	if snap.Active {
		t.Errorf("ActiveTxnSnapshot.Active=true, want false before first Write")
	}
	if snap.InactiveReason == ReasonSYNTerminator {
		t.Errorf("InactiveReason=%q — terminator path must NOT fire when bytesRead==0", snap.InactiveReason)
	}

	// The SYN is ignored before Write arms the active transaction.
	if snap.SynSuppressedPreEcho != before {
		t.Errorf("SynSuppressedPreEcho=%d before=%d, want unchanged before first Write", snap.SynSuppressedPreEcho, before)
	}

	// SYN diag entry: no active transaction was armed, and nothing reached
	// activeCh.
	entries := mux.SynDiagSnapshot()
	if len(entries) == 0 {
		t.Fatalf("SynDiagSnapshot empty, want >=1 entry")
	}
	last := entries[len(entries)-1]
	if last.GwActiveAfter {
		t.Errorf("SynDiagEntry.GwActiveAfter=true, want false before first Write")
	}
	if last.SynDeliveredToActive {
		t.Errorf("SynDiagEntry.SynDeliveredToActive=true, want false — pre-echo SYN must be suppressed from activeCh (echo_mismatch fix)")
	}
}

// TestEchoMismatch_SYNAfterGrantBeforeFirstEcho is the end-to-end
// regression test for the echo_mismatch root cause. Sequence:
//
//  1. Gateway obtains bus ownership via completeArbitrationGrant.
//  2. readLoop reads a stale SYN from the TCP/ENH buffer (this is bus
//     idle traffic buffered before bus.Send's first Write reached the
//     adapter — the structural race is real: readLoop's tight loop is
//     much faster than bus.Send's notify→sendRawWithEcho→Write→
//     sendLoop→upstream chain).
//  3. bus.Send writes the first real byte (the request source 0x15).
//  4. readLoop reads the echo of that byte (0x15).
//
// Pre-fix: ReadByte returns the stale SYN (0xAA) first, sendRawWithEcho
// sees 0xAA in place of 0x15, emits echo_mismatch.
//
// Post-fix: the stale SYN is suppressed from activeCh (counted in
// synSuppressedPreEcho); ReadByte returns 0x15 — the real echo — first.
// No echo_mismatch.
func TestEchoMismatch_SYNAfterGrantBeforeFirstEcho(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()

	before := mux.ActiveTxnSnapshot().SynSuppressedPreEcho

	// Inject a stale SYN BEFORE any Write — bytesRead is still 0.
	// This models the echo_mismatch production sequence: bus idle chatter
	// buffered in the TCP socket between STARTED and the first gateway
	// byte, surfacing via readLoop before bus.Send reaches the adapter.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(20 * time.Millisecond)

	if _, err := at.Write([]byte{0x15}); err != nil {
		t.Fatalf("Write err=%v", err)
	}

	// Then inject the real echo byte that bus.Send would expect to read
	// back (0x15 — source of a scan from 0x15 targeting 0x08).
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x15}

	// ReadByte must return 0x15 — the real echo — NOT the stale 0xAA SYN.
	// Pre-fix this would return 0xAA and bus.Send would emit echo_mismatch.
	b, err := at.ReadByte()
	if err != nil {
		t.Fatalf("ReadByte err=%v", err)
	}
	if b == protocol.SymbolSyn {
		t.Fatalf("ReadByte returned stale SYN 0xAA — pre-echo suppression failed; this is the echo_mismatch regression (13,904 events in soak)")
	}
	if b != 0x15 {
		t.Fatalf("ReadByte=0x%02X, want 0x15 (the real echo)", b)
	}

	// The stale SYN arrived before Write armed the active transaction, so it
	// is ignored rather than counted as an active pre-echo suppression.
	after := mux.ActiveTxnSnapshot().SynSuppressedPreEcho
	if after != before {
		t.Errorf("SynSuppressedPreEcho=%d before=%d, want unchanged for pre-write SYN", after, before)
	}

	// Gateway still owns the bus and the txn is still active — the stale
	// SYN did not terminate the nascent transaction.
	snap := mux.ActiveTxnSnapshot()
	if !snap.Active {
		t.Errorf("Active=false, want true — stale SYN must not end the txn")
	}
	if snap.InactiveReason != ReasonNone {
		t.Errorf("InactiveReason=%q, want empty — no inactive transition on pre-echo SYN", snap.InactiveReason)
	}
}

// TestEchoMismatch_SYNAfterWriteBeforeFirstEcho is the exact scenario
// Codex flagged on PR #502 (commit 30c3dcd / finding at mux.go:1210):
// the terminator gate keying off bytesWritten>0 allows a queued idle SYN
// to be treated as an end-of-transaction marker after the first Write but
// BEFORE the first echoed/response byte has been enqueued on activeCh.
//
// Sequence modeled:
//  1. grantGateway → gatewayTxnActive=true, bytesDeliveredToActive=0.
//  2. at.Write(0x71) — bytesWritten flips to 1 immediately (the Write
//     method increments the counter before the byte even reaches the
//     adapter). bytesDeliveredToActive is still 0 because readLoop has
//     not yet seen the echo come back.
//  3. Inject idle-chatter SYN (the buffered TCP SYN that sits in the
//     adapter pipeline on busy links). Under the pre-fix gate this SYN
//     would pass "bytesWritten>0" and be delivered to activeCh as a
//     terminator, which races the real echo.
//  4. Inject the real echo byte (0x71).
//
// Post-fix invariant: because bytesDeliveredToActive is still 0 at step 3,
// the SYN is classified as pre-echo noise (synSuppressedPreEcho++,
// gatewayTxnActive stays true, NOT delivered to activeCh). Then step 4's
// echo becomes the FIRST thing ReadByte sees.
func TestEchoMismatch_SYNAfterWriteBeforeFirstEcho(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()

	// Step 2: Write the first request byte. bytesWritten becomes >=1
	// BEFORE any adapter echo has come back (this is the race window
	// Codex flagged — bytesWritten leads, bytesDeliveredToActive does
	// not yet because readLoop has not seen the echo).
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("Write err=%v", err)
	}

	// Confirm the pre-echo invariant before injecting the adversarial SYN.
	pre := mux.ActiveTxnSnapshot()
	if pre.BytesDeliveredToActive != 0 {
		t.Fatalf("BytesDeliveredToActive=%d, want 0 — test setup invariant broken; no adapter byte should have reached activeCh yet", pre.BytesDeliveredToActive)
	}
	if pre.BytesWritten == 0 {
		t.Fatalf("BytesWritten=0, want >=1 — Write must have incremented bytesWritten for the scenario to model the Codex finding")
	}
	suppressedBefore := pre.SynSuppressedPreEcho

	// Step 3: Inject idle-chatter SYN. Under the pre-fix gate (bytesWritten>0)
	// this would falsely pass as a terminator and land on activeCh.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(20 * time.Millisecond)

	// Step 4: Inject the real echo byte bus.Send is waiting for.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x71}

	// The first byte ReadByte sees MUST be the real echo (0x71), not the
	// adversarial SYN. Pre-fix this would return 0xAA and sendRawWithEcho
	// would emit echo_mismatch.
	b, err := at.ReadByte()
	if err != nil {
		t.Fatalf("ReadByte err=%v", err)
	}
	if b == protocol.SymbolSyn {
		t.Fatalf("ReadByte returned SYN 0xAA — Codex finding reproduced: terminator gate keyed off bytesWritten wrongly terminates before first real byte reaches activeCh")
	}
	if b != 0x71 {
		t.Fatalf("ReadByte=0x%02X, want 0x71 (real echo)", b)
	}

	// Invariants Codex specifically called out:
	//   (a) synSuppressedPreEcho must have incremented.
	//   (b) gatewayTxnActive must stay true throughout (no terminator fired).
	snap := mux.ActiveTxnSnapshot()
	if snap.SynSuppressedPreEcho <= suppressedBefore {
		t.Errorf("SynSuppressedPreEcho=%d suppressedBefore=%d, want increment — adversarial SYN must be classified as pre-echo noise, NOT terminator (Codex PR #502 P1)",
			snap.SynSuppressedPreEcho, suppressedBefore)
	}
	if !snap.Active {
		t.Errorf("ActiveTxnSnapshot.Active=false, want true — adversarial SYN must not terminate the txn before a real adapter byte has reached activeCh")
	}
	if snap.InactiveReason == ReasonSYNTerminator {
		t.Errorf("InactiveReason=ReasonSYNTerminator — pre-fix Codex bug: terminator path fired before first echo byte reached activeCh")
	}
}

// TestTerminatorSYN_AfterFirstDelivery verifies the complementary half
// of the gate: once a real adapter byte has been enqueued on activeCh
// (bytesDeliveredToActive>=1), a subsequent SYN IS the legitimate frame
// terminator and MUST fire the terminator path.
func TestTerminatorSYN_AfterFirstDelivery(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()

	// Write the request source, then inject its echo and consume it so
	// readLoop enqueued a real adapter byte onto activeCh (this moves
	// bytesDeliveredToActive from 0 to >=1).
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("Write err=%v", err)
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x71}
	if b, err := at.ReadByte(); err != nil || b != 0x71 {
		t.Fatalf("ReadByte (echo) = (0x%02X, %v), want (0x71, nil)", b, err)
	}

	// Verify the gate precondition: bytesDeliveredToActive is now >=1.
	mid := mux.ActiveTxnSnapshot()
	if mid.BytesDeliveredToActive == 0 {
		t.Fatalf("BytesDeliveredToActive=0 after echo enqueued+consumed, want >=1 — test precondition for terminator gate")
	}

	// Inject trailing SYN. Because bytesDeliveredToActive>=1 the terminator
	// path MUST fire: SYN is delivered to activeCh, gatewayTxnActive
	// clears with ReasonSYNTerminator, and the next ReadByte returns the
	// SYN to the bus.Send consumer.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}

	readDone := make(chan struct {
		b   byte
		err error
	}, 1)
	go func() {
		b, err := at.ReadByte()
		readDone <- struct {
			b   byte
			err error
		}{b, err}
	}()

	select {
	case r := <-readDone:
		if r.err != nil {
			t.Fatalf("ReadByte err=%v — terminator SYN missing from activeCh", r.err)
		}
		if r.b != protocol.SymbolSyn {
			t.Fatalf("ReadByte=0x%02X, want 0xAA (SYN terminator)", r.b)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("ReadByte timed out — terminator SYN was NOT delivered to activeCh after bytesDeliveredToActive>=1 gate opened")
	}

	// Small beat for state propagation.
	time.Sleep(20 * time.Millisecond)

	snap := mux.ActiveTxnSnapshot()
	if snap.Active {
		t.Errorf("Active=true, want false — terminator SYN must clear gatewayTxnActive")
	}
	if snap.InactiveReason != ReasonSYNTerminator {
		t.Errorf("InactiveReason=%q, want %q — post-delivery SYN is the legitimate terminator", snap.InactiveReason, ReasonSYNTerminator)
	}
}
