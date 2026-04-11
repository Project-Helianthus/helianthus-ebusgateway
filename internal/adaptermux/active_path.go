package adaptermux

import (
	"errors"
	"fmt"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// activeTransport wraps the multiplexer to provide a RawTransport
// interface for the gateway's active path (gateway.Bus).
//
// It implements transport.RawTransport and optionally
// transport.StreamEventReader for RESETTED detection.
type activeTransport struct {
	mux *Mux
}

// Compile-time interface checks.
var (
	_ transport.RawTransport      = (*activeTransport)(nil)
	_ transport.StreamEventReader = (*activeTransport)(nil)
	_ transport.InfoRequester     = (*activeTransport)(nil)
)

// NOTE: activeTransport intentionally does NOT implement
// transport.Reconnectable. The upstream transport's Reconnect() acquires
// readMu, and calling it from the active path (which blocks in ReadByte
// on readLoop's output) would deadlock. The mux handles reconnection
// internally via reconnect().

// ReadByte blocks until a byte is received from the adapter or an
// error occurs. This receives ALL bytes from the adapter (including
// the gateway's own echoes), which is what gateway.Bus expects for
// echo matching.
//
// activeCh is a single FIFO channel carrying both bytes and errors.
// handleReset drains the channel and enqueues the reset event before
// readLoop resumes, so the consumer sees events in exact enqueue
// order — no priority select needed.
func (t *activeTransport) ReadByte() (byte, error) {
	select {
	case ev := <-t.mux.activeCh:
		if ev.kind == activeEventError {
			if ev.err != nil {
				return 0, ev.err
			}
			return 0, errors.New("adaptermux: unexpected nil error")
		}
		return ev.b, nil
	case <-t.mux.ctx.Done():
		return 0, fmt.Errorf("adaptermux: %w", t.mux.ctx.Err())
	}
}

// ReadEvent returns stream events including RESETTED boundaries.
// Satisfies transport.StreamEventReader for passive tap integration.
//
// Same FIFO guarantee as ReadByte — see comment there.
func (t *activeTransport) ReadEvent() (transport.StreamEvent, error) {
	select {
	case ev := <-t.mux.activeCh:
		if ev.kind == activeEventError {
			if errors.Is(ev.err, ebuserrors.ErrAdapterReset) {
				return transport.StreamEvent{
					Kind: transport.StreamEventReset,
				}, nil
			}
			if ev.err != nil {
				return transport.StreamEvent{}, ev.err
			}
			return transport.StreamEvent{}, errors.New("adaptermux: unexpected nil error")
		}
		return transport.StreamEvent{
			Kind: transport.StreamEventByte,
			Byte: ev.b,
		}, nil
	case <-t.mux.ctx.Done():
		return transport.StreamEvent{}, fmt.Errorf("adaptermux: %w", t.mux.ctx.Err())
	}
}

// Write sends bytes to the adapter through the multiplexer.
// The gateway must hold bus ownership (via StartArbitration) before
// calling Write.
//
// Sends ALL bytes to the upstream transport in a single Write call
// (batched). This ensures the ENH transport encodes all bytes into
// one TCP segment, matching how the separate proxy forwarded frames.
// Byte-by-byte writes via doSend/sendLoop introduced inter-byte
// pauses that caused the wire phase tracker to detect idle states
// during the request transmission.
func (t *activeTransport) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	// Ownership check.
	if !t.mux.arb.isOwner(gatewaySessionID) {
		curOwner, _, hasOwner := t.mux.arb.owner()
		t.mux.logger.Printf("TRACE Write not-owner: len=%d hasOwner=%v curOwner=%d", len(p), hasOwner, curOwner)
		return 0, errNotBusOwner
	}
	t.mux.logger.Printf("TRACE Write: len=%d bytes=[% X]", len(p), p)

	// Record echo expectations for all bytes.
	t.mux.stateMu.Lock()
	for _, b := range p {
		t.mux.gatewayEcho.recordSent(b)
	}
	t.mux.busDirty = true
	t.mux.stateMu.Unlock()

	// Write all bytes to upstream in one call (batched ENH encoding).
	t.mux.connMu.Lock()
	tr := t.mux.upstream
	t.mux.connMu.Unlock()
	if tr == nil {
		return 0, errNotConnected
	}
	return tr.Write(p)
}

// Close shuts down the multiplexer.
func (t *activeTransport) Close() error {
	return t.mux.Close()
}

// StartArbitration requests bus ownership for the gateway.
// Blocks until the request is granted at a SYN boundary or the
// context is cancelled.
//
// Reconnect safety: during adapter reconnect, readLoop is blocked in
// reconnect() and cannot process SYN boundaries. However, reconnect()
// calls arb.failAllPending() BEFORE entering the reconnection backoff
// loop (mux.go line ~344), which sends startResult{granted:false} on
// the notify channel. This select therefore unblocks promptly on
// disconnect — pending START requests do not hang for the duration of
// the reconnection backoff.
func (t *activeTransport) StartArbitration(initiator byte) error {
	ch := t.mux.arb.requestStart(gatewaySessionID, initiator)

	select {
	case result := <-ch:
		if !result.granted {
			if result.err != nil {
				return result.err
			}
			return errors.New("adaptermux: START not granted")
		}
		return nil
	case <-t.mux.ctx.Done():
		t.mux.arb.cancelStart(gatewaySessionID)
		return fmt.Errorf("adaptermux: %w", t.mux.ctx.Err())
	}
}

// RequestInfo returns cached INFO data for the given ID.
// Delegates to the mux-level cache (populated at connect time) instead
// of querying the upstream transport, avoiding readMu contention with
// the readLoop.
func (t *activeTransport) RequestInfo(id transport.AdapterInfoID) ([]byte, error) {
	return t.mux.CachedInfo(id)
}

// ArbitrationSendsSource reports whether the upstream adapter's START
// arbitration already places the source byte on the wire.
// Both wire phase tracker and bus.sendTransaction use the same value.
func (t *activeTransport) ArbitrationSendsSource() bool {
	return t.mux.arbitrationSendsSource()
}
