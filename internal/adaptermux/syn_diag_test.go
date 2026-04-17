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
// the PR #502 E2E fix: when a SYN arrives mid-transaction (bytesRead>0),
// onSYNLocked MUST deliver the SYN byte to activeCh BEFORE clearing
// gatewayTxnActive, so bus.Send observes the frame terminator on its
// FIFO activeCh reader.
//
// Without this delivery, the last byte of a transaction is consumed as
// a lifecycle signal inside the mux and never reaches the protocol.Bus
// consumer; bus.Send then hangs until its read deadline fires (ok=0
// timeouts in soak), which is the exact symptom documented by
// TestE2E_ScanB504_ToTarget0x08_SuccessFullFlow.
func TestOnSYNLocked_DeliversTerminatorToActive_WhenBytesReadPositive(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)

	// Feed one non-SYN byte through, consume it so bytesRead>=1.
	at := mux.ActiveTransport()
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x42}
	if b, err := at.ReadByte(); err != nil || b != 0x42 {
		t.Fatalf("ReadByte=(0x%02X,%v), want (0x42,nil)", b, err)
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
// pre-grant-stale-SYN semantics we intentionally preserve: a SYN arriving
// BEFORE any read byte has been consumed must NOT be treated as a
// terminator (it is almost certainly a TCP-buffered byte from before the
// grant). gatewayTxnActive stays true; the SYN IS delivered to activeCh
// via the caller's normal deliverToActive(symbol) path (activeExpects is
// true because gatewayTxnActive was not cleared).
//
// The SYN diag entry records this as synDelivered=true AND gwAfter=true
// (the SYN-idle-terminator inline delivery did NOT fire), which is the
// crucial counter-case to the terminator path.
func TestOnSYNLocked_NoTerminatorDeliveryWhenBytesReadZero(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)

	// Do NOT consume any bytes via ReadByte — bytesRead stays 0.
	// Inject a SYN (simulating a pre-grant stale SYN that arrived in the
	// TCP buffer before STARTED).
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(50 * time.Millisecond)

	// gatewayTxnActive should still be true (we did NOT clear on this SYN).
	snap := mux.ActiveTxnSnapshot()
	if !snap.Active {
		t.Errorf("ActiveTxnSnapshot.Active=false, want true — pre-grant-stale SYN (bytesRead==0) must NOT clear gatewayTxnActive")
	}
	if snap.InactiveReason == ReasonSYNTerminator {
		t.Errorf("InactiveReason=%q — terminator path must NOT fire when bytesRead==0", snap.InactiveReason)
	}

	// SYN diag entry: gwAfter=true, synDelivered=true (normal deliver path).
	entries := mux.SynDiagSnapshot()
	if len(entries) == 0 {
		t.Fatalf("SynDiagSnapshot empty, want >=1 entry")
	}
	last := entries[len(entries)-1]
	if !last.GwActiveAfter {
		t.Errorf("SynDiagEntry.GwActiveAfter=false, want true (pre-grant-stale SYN preserves active state)")
	}
	if !last.SynDeliveredToActive {
		t.Errorf("SynDiagEntry.SynDeliveredToActive=false, want true (caller's deliverToActive path delivers this SYN normally)")
	}
}
