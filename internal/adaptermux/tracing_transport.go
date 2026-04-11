package adaptermux

import (
	"log"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// tracingTransport wraps a RawTransport to log every Write and ReadByte
// call. This provides a full byte-level trace of what the mux sends to
// the adapter and what it receives back.
type tracingTransport struct {
	inner  transport.RawTransport
	logger *log.Logger
}

func (t *tracingTransport) ReadByte() (byte, error) {
	b, err := t.inner.ReadByte()
	if err == nil {
		t.logger.Printf("TRACE RX: 0x%02X", b)
	}
	return b, err
}

func (t *tracingTransport) Write(p []byte) (int, error) {
	t.logger.Printf("TRACE TX: [% X] len=%d", p, len(p))
	return t.inner.Write(p)
}

func (t *tracingTransport) Close() error {
	return t.inner.Close()
}

// Forward StartArbitration if supported.
func (t *tracingTransport) StartArbitration(initiator byte) error {
	t.logger.Printf("TRACE START_ARB: initiator=0x%02X", initiator)
	arb, ok := t.inner.(interface{ StartArbitration(byte) error })
	if !ok {
		return errNotConnected
	}
	return arb.StartArbitration(initiator)
}

// Forward ReadEvent if supported (StreamEventReader).
func (t *tracingTransport) ReadEvent() (transport.StreamEvent, error) {
	reader, ok := t.inner.(transport.StreamEventReader)
	if !ok {
		b, err := t.ReadByte()
		return transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}, err
	}
	event, err := reader.ReadEvent()
	if err == nil {
		switch event.Kind {
		case transport.StreamEventByte:
			t.logger.Printf("TRACE EVENT: byte=0x%02X", event.Byte)
		case transport.StreamEventStarted:
			t.logger.Printf("TRACE EVENT: STARTED data=0x%02X", event.Data)
		case transport.StreamEventFailed:
			t.logger.Printf("TRACE EVENT: FAILED data=0x%02X", event.Data)
		case transport.StreamEventReset:
			t.logger.Printf("TRACE EVENT: RESET")
		}
	}
	return event, err
}

// Forward ArbitrationSendsSource if supported.
func (t *tracingTransport) ArbitrationSendsSource() bool {
	if checker, ok := t.inner.(interface{ ArbitrationSendsSource() bool }); ok {
		return checker.ArbitrationSendsSource()
	}
	return false
}

// Forward RequestInfo if supported.
func (t *tracingTransport) RequestInfo(id transport.AdapterInfoID) ([]byte, error) {
	if req, ok := t.inner.(transport.InfoRequester); ok {
		return req.RequestInfo(id)
	}
	return nil, errNotConnected
}

// Interface assertions.
var (
	_ transport.RawTransport      = (*tracingTransport)(nil)
	_ transport.StreamEventReader = (*tracingTransport)(nil)
)
