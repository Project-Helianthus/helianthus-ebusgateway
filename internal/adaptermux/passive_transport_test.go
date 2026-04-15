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

// TestPassiveTransport_ResetBlocksOnFullBuffer verifies AM-fix5:
// delivering a reset to a full buffer BLOCKS until space is available
// (or done is closed). Resets are non-droppable stream boundaries.
func TestPassiveTransport_ResetNonBlockingOnFullBuffer(t *testing.T) {
	pt := &passiveTransport{
		events: make(chan transport.StreamEvent, 1),
		done:   make(chan struct{}),
	}
	defer pt.Close()

	// Fill the single slot.
	pt.events <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x01}

	// Deliver a reset -- must block (AM-fix5: resets are non-droppable).
	delivered := make(chan struct{})
	go func() {
		pt.deliver(PassiveEvent{Kind: PassiveEventReset})
		close(delivered)
	}()

	// Verify it blocks.
	select {
	case <-delivered:
		t.Fatal("deliver(Reset) returned immediately on full buffer -- should block")
	case <-time.After(100 * time.Millisecond):
		// Good -- blocking as expected.
	}

	// Drain one slot to unblock.
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

// TestPassiveTransport_ResetBlocksOnFull_TwoResets verifies AM-fix5:
// when the buffer is full, a second reset blocks until space is
// available. Both resets must be delivered (non-droppable).
func TestPassiveTransport_ResetNonBlockingOnFull_TwoResets(t *testing.T) {
	pt := &passiveTransport{
		events: make(chan transport.StreamEvent, 2),
		done:   make(chan struct{}),
	}
	defer pt.Close()

	// Fill buffer to capacity-1 (1 slot left).
	pt.deliver(PassiveEvent{Kind: PassiveEventSymbol, Symbol: 0x01})

	// First reset should succeed immediately (1 slot available).
	pt.deliver(PassiveEvent{Kind: PassiveEventReset})

	// Buffer now has: [symbol(0x01), reset]. Second reset blocks (AM-fix5).
	delivered := make(chan struct{})
	go func() {
		pt.deliver(PassiveEvent{Kind: PassiveEventReset})
		close(delivered)
	}()

	// Verify it blocks.
	select {
	case <-delivered:
		t.Fatal("second reset returned immediately on full buffer -- should block")
	case <-time.After(100 * time.Millisecond):
		// Good -- blocking as expected.
	}

	// Drain one slot to unblock.
	ev1 := <-pt.events
	if ev1.Kind != transport.StreamEventByte || ev1.Byte != 0x01 {
		t.Fatalf("event[0]: expected symbol(0x01), got %+v", ev1)
	}

	select {
	case <-delivered:
		// Good -- unblocked.
	case <-time.After(2 * time.Second):
		t.Fatal("second reset did not unblock after draining one slot")
	}

	// Drain remaining: both resets should be present.
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
	defer pt.Close()

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

// TestDeliver_ResetBlocksOnFull verifies AM-fix5: reset delivery
// blocks when the channel is full, and unblocks via done channel.
func TestDeliver_ResetNonBlockingOnFull(t *testing.T) {
	pt := &passiveTransport{
		events: make(chan transport.StreamEvent, 1), // tiny buffer
		done:   make(chan struct{}),
	}
	defer pt.Close()

	// Fill the single slot.
	pt.deliver(PassiveEvent{Kind: PassiveEventSymbol, Symbol: 0x01})

	// Deliver a reset -- must block (AM-fix5: resets are non-droppable).
	delivered := make(chan struct{})
	go func() {
		pt.deliver(PassiveEvent{Kind: PassiveEventReset})
		close(delivered)
	}()

	// Verify it blocks.
	select {
	case <-delivered:
		t.Fatal("deliver(Reset) returned immediately on full buffer -- should block")
	case <-time.After(100 * time.Millisecond):
		// Good -- blocking.
	}

	// Close the transport to unblock via done channel.
	pt.Close()

	select {
	case <-delivered:
		// Good -- unblocked via done.
	case <-time.After(2 * time.Second):
		t.Fatal("deliver(Reset) did not unblock after Close()")
	}
}
