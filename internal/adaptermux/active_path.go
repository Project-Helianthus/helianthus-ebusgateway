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
func (t *activeTransport) Write(p []byte) (int, error) {
	if len(p) > 0 {
		t.mux.logger.Printf("adaptermux: activeTransport.Write len=%d first=0x%02X", len(p), p[0])
	}
	for i, b := range p {
		result := make(chan error, 1)
		select {
		case t.mux.activeSendCh <- sendRequest{
			sessionID: gatewaySessionID,
			data:      b,
			result:    result,
		}:
		case <-t.mux.ctx.Done():
			return i, fmt.Errorf("adaptermux: %w", t.mux.ctx.Err())
		}

		select {
		case err := <-result:
			if err != nil {
				return i, err
			}
		case <-t.mux.ctx.Done():
			return i, fmt.Errorf("adaptermux: %w", t.mux.ctx.Err())
		}
	}
	return len(p), nil
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
// arbitration already places the source byte on the wire. Delegates to
// the upstream transport if it implements the interface; returns false
// otherwise (conservative default — caller will send the source byte).
func (t *activeTransport) ArbitrationSendsSource() bool {
	t.mux.connMu.Lock()
	tr := t.mux.upstream
	t.mux.connMu.Unlock()

	if tr == nil {
		return false
	}
	if checker, ok := tr.(interface{ ArbitrationSendsSource() bool }); ok {
		return checker.ArbitrationSendsSource()
	}
	return false
}
