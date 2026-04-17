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
// The critical field under test is synDeliveredToActive: if onSYNLocked
// cleared gatewayTxnActive (because bytesRead>0), the SYN will NOT be
// delivered to activeCh (activePathExpectsBytes() returns false right
// after). In that case the Send consumer reading activeCh does NOT see
// the terminator — which is the hypothesis for ok=0 timeouts in soak.
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
	if e.SynDeliveredToActive {
		t.Errorf("SynDeliveredToActive=true, want false — after gatewayTxnActive cleared, activePathExpectsBytes()==false so SYN is NOT enqueued to activeCh. This is the evidence of the hypothesis: Send consumer never sees the terminator.")
	}
	if e.InactiveReason != ReasonSYNIdle {
		t.Errorf("InactiveReason=%q, want %q", e.InactiveReason, ReasonSYNIdle)
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
