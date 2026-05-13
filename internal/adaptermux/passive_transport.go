package adaptermux

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// passiveTransport implements transport.RawTransport and
// transport.StreamEventReader for the passive observation path.
// It reads third-party bus symbols from the multiplexer's passive
// callback via a buffered channel.
//
// The passive tap can use this as a drop-in replacement for the
// TCP-dialed observer transport — no escape decoding needed since
// symbols are already logical (post-ENH-decode).
type passiveTransport struct {
	events chan transport.StreamEvent
	done   chan struct{}
	closed bool
	mu     sync.Mutex
}

// Compile-time interface checks.
var (
	_ transport.RawTransport      = (*passiveTransport)(nil)
	_ transport.StreamEventReader = (*passiveTransport)(nil)
)

const passiveTransportBuffer = 512 // capacity: bus symbols for passive path

// newPassiveTransport creates a passive transport backed by the mux.
func newPassiveTransport() *passiveTransport {
	return &passiveTransport{
		events: make(chan transport.StreamEvent, passiveTransportBuffer),
		done:   make(chan struct{}),
	}
}

// deliver is called by the mux's passive callback to feed events.
func (t *passiveTransport) deliver(event PassiveEvent) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.mu.Unlock()

	var se transport.StreamEvent
	switch event.Kind {
	case PassiveEventSymbol:
		// F-23 (batch-19, Codex bot review on PR-2): propagate the
		// upstream WasEscaped flag through the mux's passive-bridge
		// so adapter-direct consumers see the same provenance the
		// raw-TCP path delivers via the local escape decoder.
		se = transport.StreamEvent{
			Kind:       transport.StreamEventByte,
			Byte:       event.Symbol,
			WasEscaped: event.WasEscaped,
		}
	case PassiveEventReset:
		se = transport.StreamEvent{Kind: transport.StreamEventReset}
	case PassiveEventConnected:
		// Deliver as reset: a reconnect establishes a new stream boundary.
		// The passive reconstructor must discard any partial frame state
		// accumulated before the disconnect. Consecutive resets (disconnect
		// followed by connect) are idempotent for stream parsing.
		//
		// In adapter-direct mode the PassiveBusTap does not re-call connect()
		// on mux reconnection — this event is the only signal that the adapter
		// link was re-established, so it must reach the consumer.
		se = transport.StreamEvent{Kind: transport.StreamEventReset}
	case PassiveEventDisconnected:
		// Deliver as reset: a disconnect is a stream boundary. Any partial
		// frame in flight is now incomplete and must be discarded by the
		// consumer's reconstructor.
		se = transport.StreamEvent{Kind: transport.StreamEventReset}
	default:
		return
	}

	// AM52/AM-fix5/Codex-R6/XR_PASSIVE_RESET_BACKPRESSURE:
	//
	// Reset boundaries use a bounded-blocking send (100ms). The trade-off:
	//
	//   - Dropping a reset can leave the consumer's frame reconstructor
	//     with stale partial-frame state until the next SYN boundary or
	//     a subsequent reset arrives. This is acceptable because:
	//     (a) passive transport is read-only — stale state does not affect
	//         bus ownership or the mux's own parser state;
	//     (b) the next SYN boundary (typically <50ms on a live bus)
	//         provides an implicit resynchronisation point.
	//
	//   - Blocking indefinitely stalls readLoop, reconnect, or handleReset
	//     — the mux's critical recovery path. A stalled recovery path
	//     affects ALL consumers (active + passive + external sessions),
	//     not just the slow passive consumer.
	//
	//   The 100ms bound is chosen to be long enough for a healthy consumer
	//   to drain at least one event (typical: <1ms) while short enough to
	//   avoid visible recovery delay. If the consumer is genuinely stuck,
	//   the data discontinuity on the next SYN boundary is the correct
	//   fallback — the reconstructor handles SYN as a frame boundary.
	if se.Kind == transport.StreamEventReset {
		select {
		case t.events <- se:
		case <-t.done:
		case <-time.After(100 * time.Millisecond):
			// Consumer too slow — drop reset to unblock mux recovery.
		}
		return
	}

	// Regular symbols: non-blocking with overflow drop.
	select {
	case t.events <- se:
	case <-t.done:
		return
	default:
		// Buffer full -- drop oldest symbol to prevent backpressure.
		// AM48: never evict a reset event -- resets are non-droppable
		// boundaries (Codex P1 #3060335791). If the oldest is a reset,
		// put it back and drop the new symbol instead.
		select {
		case oldest := <-t.events:
			if oldest.Kind == transport.StreamEventReset {
				// AM48: reset boundary is sacred, put it back.
				select {
				case t.events <- oldest:
				case <-t.done:
					return
				}
				return // drop the new symbol
			}
			// AM28: data was lost — inject a reset marker to signal
			// the loss boundary to the consumer instead of silently
			// continuing with a gap. The new symbol is also dropped;
			// the consumer must re-sync after seeing the reset.
			// Non-blocking: a concurrent deliver() from another goroutine
			// (e.g., Start emitting PassiveEventConnected while readLoop
			// emits symbols) may have refilled the slot between eviction
			// and this send. In that case, drop both the marker and the
			// symbol — the consumer will see a data discontinuity on the
			// next reset or SYN boundary.
			select {
			case t.events <- transport.StreamEvent{Kind: transport.StreamEventReset}:
			case <-t.done:
				return
			default:
				// Slot refilled by concurrent producer — cannot inject
				// reset marker. Data loss occurred but is unmarkable.
			}
		default:
		}
	}
}

// ReadByte blocks until a symbol is available. Implements transport.RawTransport.
func (t *passiveTransport) ReadByte() (byte, error) {
	select {
	case ev := <-t.events:
		if ev.Kind == transport.StreamEventReset {
			return 0, fmt.Errorf("adaptermux: passive transport reset")
		}
		return ev.Byte, nil
	case <-t.done:
		return 0, errors.New("adaptermux: passive transport closed")
	}
}

// ReadEvent returns stream events including resets. Implements
// transport.StreamEventReader for RESETTED boundary detection.
func (t *passiveTransport) ReadEvent() (transport.StreamEvent, error) {
	select {
	case ev := <-t.events:
		return ev, nil
	case <-t.done:
		return transport.StreamEvent{}, errors.New("adaptermux: passive transport closed")
	}
}

// Write is a no-op — passive path is read-only.
func (t *passiveTransport) Write(p []byte) (int, error) {
	return 0, errors.New("adaptermux: passive transport is read-only")
}

// Close shuts down the passive transport.
func (t *passiveTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.closed {
		t.closed = true
		close(t.done)
	}
	return nil
}

// PassiveTransport returns a transport.RawTransport for the passive
// observation path. The returned transport reads only third-party bus
// symbols (gateway self-echo filtered). The stream is already logical
// (no escape decoding needed).
//
// Must be called before Start(). The returned transport is wired to
// the mux's passive callback automatically.
func (m *Mux) PassiveTransport() transport.RawTransport {
	pt := newPassiveTransport()
	m.SetPassiveCallback(pt.deliver)
	return pt
}
