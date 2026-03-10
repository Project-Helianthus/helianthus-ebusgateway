package ebusgateway

import (
	"context"
	"fmt"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
)

type BroadcastListener struct {
	router *router.BusEventRouter
	parser *broadcastFrameParser
	tap    *PassiveBusTap
}

func StartBroadcastListener(ctx context.Context, cfg Config, router *router.BusEventRouter) (*BroadcastListener, error) {
	return StartBroadcastListenerWithTransport(ctx, cfg, router, nil)
}

func StartBroadcastListenerWithTransport(ctx context.Context, cfg Config, router *router.BusEventRouter, wrap func(transport.RawTransport) transport.RawTransport) (*BroadcastListener, error) {
	if router == nil {
		return nil, fmt.Errorf("broadcast listener missing router: %w", ebuserrors.ErrInvalidPayload)
	}

	listener := &BroadcastListener{
		router: router,
		parser: &broadcastFrameParser{},
	}
	tap, err := StartPassiveBusTapWithTransport(ctx, cfg, listener, wrap)
	if err != nil {
		return nil, err
	}
	listener.tap = tap
	return listener, nil
}

func (listener *BroadcastListener) Start(ctx context.Context) {
	_ = ctx
}

func (listener *BroadcastListener) Close() error {
	if listener == nil || listener.tap == nil {
		return nil
	}
	return listener.tap.Close()
}

func (listener *BroadcastListener) OnPassiveTapEvent(event PassiveTapEvent) {
	if listener == nil || listener.router == nil || listener.parser == nil {
		return
	}

	switch event.Kind {
	case PassiveTapEventSymbol:
		frame, ok := listener.parser.push(event.Symbol)
		if !ok {
			return
		}
		if frame.Target == protocol.AddressBroadcast {
			_ = listener.router.HandleBroadcast(frame)
		}
	default:
		listener.parser.reset()
	}
}

type broadcastFrameParser struct {
	buffer []byte
}

func (parser *broadcastFrameParser) push(symbol byte) (protocol.Frame, bool) {
	if parser == nil {
		return protocol.Frame{}, false
	}
	if symbol == protocol.SymbolSyn {
		if len(parser.buffer) == 0 {
			return protocol.Frame{}, false
		}
		frame, ok := parseFrame(parser.buffer)
		parser.buffer = parser.buffer[:0]
		return frame, ok
	}
	parser.buffer = append(parser.buffer, symbol)
	return protocol.Frame{}, false
}

func (parser *broadcastFrameParser) reset() {
	if parser == nil {
		return
	}
	parser.buffer = parser.buffer[:0]
}
