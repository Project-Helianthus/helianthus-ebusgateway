package adaptermux

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// TestDeliver_AllEventKinds verifies that every PassiveEventKind is
// mapped to the correct StreamEvent. In particular, PassiveEventConnected
// must NOT be dropped — it must surface as StreamEventReset so the
// passive reconstructor sees the reconnect boundary.
func TestDeliver_AllEventKinds(t *testing.T) {
	tests := []struct {
		name     string
		input    PassiveEvent
		wantKind transport.StreamEventKind
		wantByte byte
		wantDrop bool // true if the event should be silently dropped
	}{
		{
			name:     "symbol",
			input:    PassiveEvent{Kind: PassiveEventSymbol, Symbol: 0x42},
			wantKind: transport.StreamEventByte,
			wantByte: 0x42,
		},
		{
			name:     "reset",
			input:    PassiveEvent{Kind: PassiveEventReset},
			wantKind: transport.StreamEventReset,
		},
		{
			name:     "connected maps to reset boundary",
			input:    PassiveEvent{Kind: PassiveEventConnected},
			wantKind: transport.StreamEventReset,
		},
		{
			name:     "disconnected maps to reset boundary",
			input:    PassiveEvent{Kind: PassiveEventDisconnected},
			wantKind: transport.StreamEventReset,
		},
		{
			name:     "unknown kind is dropped",
			input:    PassiveEvent{Kind: PassiveEventKind(99)},
			wantDrop: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pt := newPassiveTransport()
			defer closeOrLog(t, pt, "pt")

			pt.deliver(tt.input)

			if tt.wantDrop {
				// Channel must be empty.
				select {
				case ev := <-pt.events:
					t.Fatalf("expected event to be dropped, got %+v", ev)
				default:
					// Good — nothing delivered.
				}
				return
			}

			select {
			case ev := <-pt.events:
				if ev.Kind != tt.wantKind {
					t.Errorf("Kind = %d, want %d", ev.Kind, tt.wantKind)
				}
				if tt.wantKind == transport.StreamEventByte && ev.Byte != tt.wantByte {
					t.Errorf("Byte = 0x%02X, want 0x%02X", ev.Byte, tt.wantByte)
				}
			default:
				t.Fatal("expected event to be delivered, channel empty")
			}
		})
	}
}

// TestDeliver_ConnectDisconnectCycle verifies that a full adapter
// reconnect cycle (disconnect + connect) produces two reset events,
// so the passive reconstructor sees both boundaries.
func TestDeliver_ConnectDisconnectCycle(t *testing.T) {
	pt := newPassiveTransport()
	defer closeOrLog(t, pt, "pt")

	// Simulate: symbols -> disconnect -> connect -> symbols
	pt.deliver(PassiveEvent{Kind: PassiveEventSymbol, Symbol: 0xAA})
	pt.deliver(PassiveEvent{Kind: PassiveEventDisconnected})
	pt.deliver(PassiveEvent{Kind: PassiveEventConnected})
	pt.deliver(PassiveEvent{Kind: PassiveEventSymbol, Symbol: 0xBB})

	expected := []struct {
		kind transport.StreamEventKind
		b    byte
	}{
		{transport.StreamEventByte, 0xAA},
		{transport.StreamEventReset, 0},
		{transport.StreamEventReset, 0},
		{transport.StreamEventByte, 0xBB},
	}

	for i, want := range expected {
		select {
		case ev := <-pt.events:
			if ev.Kind != want.kind {
				t.Errorf("event[%d]: Kind = %d, want %d", i, ev.Kind, want.kind)
			}
			if want.kind == transport.StreamEventByte && ev.Byte != want.b {
				t.Errorf("event[%d]: Byte = 0x%02X, want 0x%02X", i, ev.Byte, want.b)
			}
		default:
			t.Fatalf("event[%d]: expected event, channel empty", i)
		}
	}
}

// TestDeliver_ClosedTransportDropsEvents verifies that deliver() is a
// no-op after Close().
func TestDeliver_ClosedTransportDropsEvents(t *testing.T) {
	pt := newPassiveTransport()
	closeOrLog(t, pt, "pt")

	pt.deliver(PassiveEvent{Kind: PassiveEventSymbol, Symbol: 0xFF})
	pt.deliver(PassiveEvent{Kind: PassiveEventConnected})
	pt.deliver(PassiveEvent{Kind: PassiveEventDisconnected})

	select {
	case ev := <-pt.events:
		t.Fatalf("expected no events after close, got %+v", ev)
	default:
		// Good.
	}
}

// TestReadEvent_ReturnsResetForConnectDisconnect verifies that ReadEvent
// surfaces connect/disconnect as StreamEventReset (end-to-end through
// the ReadEvent interface, not just the channel).
func TestReadEvent_ReturnsResetForConnectDisconnect(t *testing.T) {
	pt := newPassiveTransport()
	defer closeOrLog(t, pt, "pt")

	pt.deliver(PassiveEvent{Kind: PassiveEventDisconnected})
	pt.deliver(PassiveEvent{Kind: PassiveEventConnected})

	for i, label := range []string{"disconnect", "connect"} {
		ev, err := pt.ReadEvent()
		if err != nil {
			t.Fatalf("ReadEvent[%d/%s]: unexpected error: %v", i, label, err)
		}
		if ev.Kind != transport.StreamEventReset {
			t.Errorf("ReadEvent[%d/%s]: Kind = %d, want StreamEventReset(%d)",
				i, label, ev.Kind, transport.StreamEventReset)
		}
	}
}

// --- Passive reset blocking safety ---

// TestPassiveTransport_ResetBoundedBlockOnFullBuffer verifies Codex-R6:
// delivering a reset to a full buffer blocks briefly (100ms bounded)
// then drops to avoid stalling readLoop/reconnect. If drained in time,
// the reset is delivered.
func TestPassiveTransport_ResetBoundedBlockOnFullBuffer(t *testing.T) {
	pt := &passiveTransport{
		events: make(chan transport.StreamEvent, 1),
		done:   make(chan struct{}),
	}
	defer closeOrLog(t, pt, "pt")

	// Fill the single slot.
	pt.events <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x01}

	// Deliver a reset — bounded-blocking (100ms timeout).
	delivered := make(chan struct{})
	go func() {
		pt.deliver(PassiveEvent{Kind: PassiveEventReset})
		close(delivered)
	}()

	// Verify it blocks briefly (doesn't return immediately).
	select {
	case <-delivered:
		t.Fatal("deliver(Reset) returned immediately on full buffer -- should block briefly")
	case <-time.After(20 * time.Millisecond):
		// Good -- still blocking after 20ms.
	}

	// Drain before the 100ms timeout to receive the reset.
	<-pt.events

	select {
	case <-delivered:
		// Good -- unblocked after drain.
	case <-time.After(2 * time.Second):
		t.Fatal("deliver(Reset) did not unblock after draining buffer")
	}

	// The reset should now be in the buffer.
	ev := <-pt.events
	if ev.Kind != transport.StreamEventReset {
		t.Fatalf("expected reset, got %+v", ev)
	}
}

// TestPassiveTransport_ResetDroppedAfterTimeout verifies Codex-R6:
// if the consumer is too slow, the reset is dropped after 100ms
// to unblock the mux recovery path.
func TestPassiveTransport_ResetDroppedAfterTimeout(t *testing.T) {
	pt := &passiveTransport{
		events: make(chan transport.StreamEvent, 1),
		done:   make(chan struct{}),
	}
	defer closeOrLog(t, pt, "pt")

	// Fill the single slot.
	pt.events <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x01}

	// Deliver a reset — will timeout after 100ms and return.
	delivered := make(chan struct{})
	go func() {
		pt.deliver(PassiveEvent{Kind: PassiveEventReset})
		close(delivered)
	}()

	// Don't drain — let the 100ms timeout fire.
	select {
	case <-delivered:
		// Good — returned after timeout, reset was dropped.
	case <-time.After(500 * time.Millisecond):
		t.Fatal("deliver(Reset) did not return after timeout -- still blocking")
	}

	// Buffer should still contain only the original symbol.
	ev := <-pt.events
	if ev.Kind != transport.StreamEventByte || ev.Byte != 0x01 {
		t.Fatalf("expected original symbol 0x01, got %+v", ev)
	}
}

// TestPassiveTransport_ResetBoundedBlock_TwoResets verifies Codex-R6:
// first reset succeeds (slot available), second reset bounded-blocks.
// If drained within 100ms, second reset is delivered.
func TestPassiveTransport_ResetBoundedBlock_TwoResets(t *testing.T) {
	pt := &passiveTransport{
		events: make(chan transport.StreamEvent, 2),
		done:   make(chan struct{}),
	}
	defer closeOrLog(t, pt, "pt")

	// Fill buffer to capacity-1 (1 slot left).
	pt.deliver(PassiveEvent{Kind: PassiveEventSymbol, Symbol: 0x01})

	// First reset succeeds immediately (1 slot available).
	pt.deliver(PassiveEvent{Kind: PassiveEventReset})

	// Buffer full: [symbol(0x01), reset]. Second reset bounded-blocks.
	delivered := make(chan struct{})
	go func() {
		pt.deliver(PassiveEvent{Kind: PassiveEventReset})
		close(delivered)
	}()

	// Verify it doesn't return immediately.
	select {
	case <-delivered:
		t.Fatal("second reset returned immediately on full buffer")
	case <-time.After(20 * time.Millisecond):
		// Good — still blocking after 20ms.
	}

	// Drain one slot before the 100ms timeout.
	ev1 := <-pt.events
	if ev1.Kind != transport.StreamEventByte || ev1.Byte != 0x01 {
		t.Fatalf("event[0]: expected symbol(0x01), got %+v", ev1)
	}

	select {
	case <-delivered:
		// Good — unblocked after drain.
	case <-time.After(2 * time.Second):
		t.Fatal("second reset did not unblock after draining one slot")
	}

	// Both resets should be in buffer.
	ev2 := <-pt.events
	if ev2.Kind != transport.StreamEventReset {
		t.Fatalf("event[1]: expected reset, got %+v", ev2)
	}
	ev3 := <-pt.events
	if ev3.Kind != transport.StreamEventReset {
		t.Fatalf("event[2]: expected reset, got %+v", ev3)
	}
}

// TestDeliver_BufferOverflowInjectsReset verifies the backpressure
// strategy: when the buffer is full and a symbol overflows, the oldest
// event is evicted and a reset marker (AM28) is injected to signal the
// data loss boundary to the consumer.
func TestDeliver_BufferOverflowInjectsReset(t *testing.T) {
	pt := &passiveTransport{
		events: make(chan transport.StreamEvent, 2), // tiny buffer
		done:   make(chan struct{}),
	}
	defer closeOrLog(t, pt, "pt")

	// Fill buffer.
	pt.deliver(PassiveEvent{Kind: PassiveEventSymbol, Symbol: 0x01})
	pt.deliver(PassiveEvent{Kind: PassiveEventSymbol, Symbol: 0x02})

	// Overflow -- oldest (0x01) is evicted, reset marker injected (AM28).
	pt.deliver(PassiveEvent{Kind: PassiveEventSymbol, Symbol: 0x03})

	// Drain: expect 0x02 (survived) then reset (loss boundary).
	// The new symbol 0x03 is dropped -- the reset replaces it.
	timeout := time.After(100 * time.Millisecond)
	var events []transport.StreamEvent
	for i := 0; i < 2; i++ {
		select {
		case ev := <-pt.events:
			events = append(events, ev)
		case <-timeout:
			t.Fatalf("timed out waiting for event %d", i)
		}
	}

	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].Kind != transport.StreamEventByte || events[0].Byte != 0x02 {
		t.Errorf("event[0]: got %+v, want symbol 0x02", events[0])
	}
	if events[1].Kind != transport.StreamEventReset {
		t.Errorf("event[1]: got %+v, want reset (AM28 loss boundary)", events[1])
	}
}

// TestDeliver_ResetUnblocksViaDone verifies that reset delivery on a
// full buffer unblocks when done is closed (transport shutdown).
func TestDeliver_ResetUnblocksViaDone(t *testing.T) {
	pt := &passiveTransport{
		events: make(chan transport.StreamEvent, 1),
		done:   make(chan struct{}),
	}

	// Fill the single slot.
	pt.deliver(PassiveEvent{Kind: PassiveEventSymbol, Symbol: 0x01})

	// Deliver a reset — bounded-blocking.
	delivered := make(chan struct{})
	go func() {
		pt.deliver(PassiveEvent{Kind: PassiveEventReset})
		close(delivered)
	}()

	// Verify it doesn't return immediately.
	select {
	case <-delivered:
		t.Fatal("deliver(Reset) returned immediately on full buffer")
	case <-time.After(20 * time.Millisecond):
		// Good — still blocking after 20ms.
	}

	// Close the transport to unblock via done channel.
	closeOrLog(t, pt, "pt")

	select {
	case <-delivered:
		// Good — unblocked via done.
	case <-time.After(2 * time.Second):
		t.Fatal("deliver(Reset) did not unblock after Close()")
	}
}
