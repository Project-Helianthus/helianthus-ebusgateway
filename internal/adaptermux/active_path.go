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
	_ transport.Reconnectable     = (*activeTransport)(nil)
)

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

// RequestInfo delegates INFO queries to the upstream transport.
// The gateway uses this (via Bus.RawTransportOp) to query adapter
// hardware telemetry (firmware version, temperature, voltages).
// Without this delegation, adapter-direct mode reports
// "INFO Supported: No" and "Connection Type: Unknown".
func (t *activeTransport) RequestInfo(id transport.AdapterInfoID) ([]byte, error) {
	t.mux.connMu.Lock()
	tr := t.mux.upstream
	t.mux.connMu.Unlock()
	if tr == nil {
		return nil, errors.New("adaptermux: not connected")
	}
	infoReq, ok := tr.(transport.InfoRequester)
	if !ok {
		return nil, errors.New("adaptermux: upstream does not support INFO")
	}
	return infoReq.RequestInfo(id)
}

// ArbitrationSendsSource reports whether the upstream adapter's START
// arbitration already placed the source byte on the wire. ENH/ENS
// transports return true, meaning the caller must NOT include the
// source byte in the outgoing telegram payload.
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

// Reconnect delegates reconnection to the upstream transport.
// The mux already handles reconnection internally (via its reconnect
// loop), so this is a pass-through for callers that check the
// Reconnectable interface on the gateway transport.
func (t *activeTransport) Reconnect() error {
	t.mux.connMu.Lock()
	tr := t.mux.upstream
	t.mux.connMu.Unlock()
	if tr == nil {
		return errors.New("adaptermux: not connected")
	}
	if reconnectable, ok := tr.(transport.Reconnectable); ok {
		return reconnectable.Reconnect()
	}
	return errors.New("adaptermux: upstream does not support reconnect")
}
