package adaptermux

import (
	"errors"
	"fmt"
	"sync"

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
		se = transport.StreamEvent{Kind: transport.StreamEventByte, Byte: event.Symbol}
	case PassiveEventReset:
		se = transport.StreamEvent{Kind: transport.StreamEventReset}
	case PassiveEventConnected:
		// No StreamEvent equivalent — skip.
		return
	case PassiveEventDisconnected:
		// Deliver as reset so passive tap sees the boundary.
		se = transport.StreamEvent{Kind: transport.StreamEventReset}
	default:
		return
	}

	select {
	case t.events <- se:
	case <-t.done:
		return
	default:
		// Buffer full — drop oldest to prevent backpressure.
		select {
		case <-t.events:
		default:
		}
		select {
		case t.events <- se:
		case <-t.done:
			return
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
