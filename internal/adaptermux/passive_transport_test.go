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
			defer pt.Close()

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
	defer pt.Close()

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
	pt.Close()

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
	defer pt.Close()

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

// TestPassiveTransport_ResetDoesNotBlockReadLoopRecovery verifies that
// closing the passive transport unblocks a blocked deliver(Reset) call,
// allowing the mux to proceed with reconnect (create new passive transport).
func TestPassiveTransport_ResetDoesNotBlockReadLoopRecovery(t *testing.T) {
	// Create a passive transport with a FULL buffer.
	pt := &passiveTransport{
		events: make(chan transport.StreamEvent, passiveTransportBuffer),
		done:   make(chan struct{}),
	}

	// Fill all 512 slots with symbols.
	for i := 0; i < passiveTransportBuffer; i++ {
		pt.events <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: byte(i % 256)}
	}

	// Buffer is now full. Deliver a reset in a goroutine — it will block.
	delivered := make(chan struct{})
	go func() {
		pt.deliver(PassiveEvent{Kind: PassiveEventReset})
		close(delivered)
	}()

	// Verify it is blocked.
	select {
	case <-delivered:
		t.Fatal("reset deliver should block on full buffer")
	case <-time.After(50 * time.Millisecond):
		// Good — blocked as expected.
	}

	// Close the transport — this should unblock the blocked deliver.
	pt.Close()

	select {
	case <-delivered:
		// Good — unblocked by Close.
	case <-time.After(2 * time.Second):
		t.Fatal("deliver(Reset) did not unblock after Close — readLoop recovery blocked")
	}

	// Verify that after unblocking, a new passive transport can be created
	// and used (simulating mux reconnect creating a fresh passive transport).
	pt2 := newPassiveTransport()
	defer pt2.Close()

	pt2.deliver(PassiveEvent{Kind: PassiveEventSymbol, Symbol: 0xBB})
	select {
	case ev := <-pt2.events:
		if ev.Kind != transport.StreamEventByte || ev.Byte != 0xBB {
			t.Fatalf("new transport: expected symbol 0xBB, got %+v", ev)
		}
	default:
		t.Fatal("new passive transport should work after old one was closed")
	}
}

// TestPassiveTransport_ResetBlocksUntilConsumed_TwoResets verifies the
// blocking semantics with two consecutive resets: the first succeeds
// (using the last slot), the second blocks until the consumer drains.
// Neither reset is dropped.
func TestPassiveTransport_ResetBlocksUntilConsumed_TwoResets(t *testing.T) {
	pt := &passiveTransport{
		events: make(chan transport.StreamEvent, 2),
		done:   make(chan struct{}),
	}
	defer pt.Close()

	// Fill buffer to capacity-1 (1 slot left).
	pt.deliver(PassiveEvent{Kind: PassiveEventSymbol, Symbol: 0x01})

	// First reset should succeed immediately (1 slot available).
	delivered1 := make(chan struct{})
	go func() {
		pt.deliver(PassiveEvent{Kind: PassiveEventReset})
		close(delivered1)
	}()
	select {
	case <-delivered1:
		// Good — delivered using the last slot.
	case <-time.After(2 * time.Second):
		t.Fatal("first reset should succeed with 1 slot remaining")
	}

	// Buffer now has: [symbol(0x01), reset]. Second reset should block.
	delivered2 := make(chan struct{})
	go func() {
		pt.deliver(PassiveEvent{Kind: PassiveEventReset})
		close(delivered2)
	}()

	// Verify blocked.
	select {
	case <-delivered2:
		t.Fatal("second reset should block on full buffer")
	case <-time.After(50 * time.Millisecond):
		// Good — blocking as expected.
	}

	// Read one event from the consumer side — should unblock the blocked deliver.
	ev := <-pt.events
	if ev.Kind != transport.StreamEventByte || ev.Byte != 0x01 {
		t.Fatalf("expected symbol(0x01), got %+v", ev)
	}

	select {
	case <-delivered2:
		// Good — unblocked after consumer drained a slot.
	case <-time.After(2 * time.Second):
		t.Fatal("second reset did not unblock after consumer drained buffer")
	}

	// Verify the reset was NOT dropped — drain remaining events.
	var resetCount int
	for i := 0; i < 2; i++ {
		ev := <-pt.events
		if ev.Kind == transport.StreamEventReset {
			resetCount++
		}
	}
	if resetCount != 2 {
		t.Fatalf("expected 2 resets in output, got %d — reset was dropped", resetCount)
	}
}

// TestDeliver_BufferOverflowDropsOldest verifies the backpressure
// strategy: when the buffer is full, the oldest event is dropped to
// make room for the new one.
func TestDeliver_BufferOverflowDropsOldest(t *testing.T) {
	pt := &passiveTransport{
		events: make(chan transport.StreamEvent, 2), // tiny buffer
		done:   make(chan struct{}),
	}
	defer pt.Close()

	// Fill buffer.
	pt.deliver(PassiveEvent{Kind: PassiveEventSymbol, Symbol: 0x01})
	pt.deliver(PassiveEvent{Kind: PassiveEventSymbol, Symbol: 0x02})

	// Overflow — oldest (0x01) should be dropped.
	pt.deliver(PassiveEvent{Kind: PassiveEventSymbol, Symbol: 0x03})

	// Drain: expect 0x02 then 0x03.
	timeout := time.After(100 * time.Millisecond)
	var got []byte
	for i := 0; i < 2; i++ {
		select {
		case ev := <-pt.events:
			got = append(got, ev.Byte)
		case <-timeout:
			t.Fatalf("timed out waiting for event %d", i)
		}
	}

	if len(got) != 2 || got[0] != 0x02 || got[1] != 0x03 {
		t.Errorf("got %v, want [0x02, 0x03]", got)
	}
}
