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
	_ transport.RawTransport    = (*activeTransport)(nil)
	_ transport.StreamEventReader = (*activeTransport)(nil)
)

// ReadByte blocks until a byte is received from the adapter or an
// error occurs. This receives ALL bytes from the adapter (including
// the gateway's own echoes), which is what gateway.Bus expects for
// echo matching.
func (t *activeTransport) ReadByte() (byte, error) {
	// Priority: check for reset/error before data.
	// After handleReset enqueues ErrAdapterReset on activeErrCh and
	// readLoop enqueues post-reset symbols on activeRecvCh, both
	// channels may be ready simultaneously.  Go select picks randomly,
	// so a bare select could deliver a post-reset byte before the
	// reset error, breaking transaction state.  The non-blocking drain
	// here guarantees the consumer sees the reset first.
	select {
	case err := <-t.mux.activeErrCh:
		if err != nil {
			return 0, err
		}
		return 0, errors.New("adaptermux: unexpected nil error")
	default:
	}

	select {
	case b := <-t.mux.activeRecvCh:
		return b, nil
	case err := <-t.mux.activeErrCh:
		if err != nil {
			return 0, err
		}
		return 0, errors.New("adaptermux: unexpected nil error")
	case <-t.mux.ctx.Done():
		return 0, fmt.Errorf("adaptermux: %w", t.mux.ctx.Err())
	}
}

// ReadEvent returns stream events including RESETTED boundaries.
// Satisfies transport.StreamEventReader for passive tap integration.
func (t *activeTransport) ReadEvent() (transport.StreamEvent, error) {
	// Priority: check for reset/error before data (same rationale as
	// ReadByte — see comment there).
	select {
	case err := <-t.mux.activeErrCh:
		if errors.Is(err, ebuserrors.ErrAdapterReset) {
			return transport.StreamEvent{
				Kind: transport.StreamEventReset,
			}, nil
		}
		if err != nil {
			return transport.StreamEvent{}, err
		}
		return transport.StreamEvent{}, errors.New("adaptermux: unexpected nil error")
	default:
	}

	select {
	case b := <-t.mux.activeRecvCh:
		return transport.StreamEvent{
			Kind: transport.StreamEventByte,
			Byte: b,
		}, nil
	case err := <-t.mux.activeErrCh:
		if errors.Is(err, ebuserrors.ErrAdapterReset) {
			return transport.StreamEvent{
				Kind: transport.StreamEventReset,
			}, nil
		}
		if err != nil {
			return transport.StreamEvent{}, err
		}
		return transport.StreamEvent{}, errors.New("adaptermux: unexpected nil error")
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
