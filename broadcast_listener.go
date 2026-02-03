package ebusgateway

import (
	"context"
	"errors"
	"fmt"

	ebuserrors "github.com/d3vi1/helianthus-ebusgo/errors"
	"github.com/d3vi1/helianthus-ebusgo/protocol"
	"github.com/d3vi1/helianthus-ebusgo/transport"
	"github.com/d3vi1/helianthus-ebusreg/router"
)

type BroadcastListener struct {
	transport transport.RawTransport
	closeFn   func() error
	router    *router.BusEventRouter
	reader    *frameReader
}

func StartBroadcastListener(ctx context.Context, cfg Config, router *router.BusEventRouter) (*BroadcastListener, error) {
	if router == nil {
		return nil, fmt.Errorf("broadcast listener missing router: %w", ebuserrors.ErrInvalidPayload)
	}

	// Always use a fresh transport connection to avoid interfering with bus I/O.
	cfg.Transport = nil
	tr, closeFn, err := resolveBroadcastTransport(ctx, cfg)
	if err != nil {
		return nil, err
	}

	listener := &BroadcastListener{
		transport: tr,
		closeFn:   closeFn,
		router:    router,
		reader:    newFrameReader(tr),
	}
	listener.Start(ctx)
	return listener, nil
}

func resolveBroadcastTransport(ctx context.Context, cfg Config) (transport.RawTransport, func() error, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	config := cfg.TransportConfig
	if config.Network == "" {
		return nil, nil, fmt.Errorf("broadcast transport missing network: %w", ebuserrors.ErrInvalidPayload)
	}
	if config.Address == "" {
		return nil, nil, fmt.Errorf("broadcast transport missing address: %w", ebuserrors.ErrInvalidPayload)
	}

	dial := config.Dial
	if dial == nil {
		dial = dialContext
	}

	conn, err := dial(ctx, config.Network, config.Address, config.DialTimeout)
	if err != nil {
		return nil, nil, fmt.Errorf("broadcast transport dial failed: %w", err)
	}

	transportLayer, err := transportFromConn(config.Protocol, conn, config.ReadTimeout, config.WriteTimeout)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}

	return transportLayer, conn.Close, nil
}

func (listener *BroadcastListener) Start(ctx context.Context) {
	if listener == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	go listener.run(ctx)
}

func (listener *BroadcastListener) Close() error {
	if listener == nil || listener.closeFn == nil {
		return nil
	}
	return listener.closeFn()
}

func (listener *BroadcastListener) run(ctx context.Context) {
	for {
		frame, ok, err := listener.reader.ReadFrame(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, ebuserrors.ErrTransportClosed) {
				return
			}
			continue
		}
		if !ok {
			continue
		}
		if frame.Target != protocol.AddressBroadcast {
			continue
		}
		_ = listener.router.HandleBroadcast(frame)
	}
}

type frameReader struct {
	transport transport.RawTransport
	escape    bool
	buffer    []byte
}

func newFrameReader(tr transport.RawTransport) *frameReader {
	return &frameReader{transport: tr}
}

func (reader *frameReader) ReadFrame(ctx context.Context) (protocol.Frame, bool, error) {
	for {
		if ctx != nil && ctx.Err() != nil {
			return protocol.Frame{}, false, ctx.Err()
		}

		symbol, err := reader.transport.ReadByte()
		if err != nil {
			if errors.Is(err, ebuserrors.ErrTimeout) {
				reader.escape = false
				continue
			}
			return protocol.Frame{}, false, err
		}

		if reader.escape {
			reader.escape = false
			switch symbol {
			case 0x00:
				reader.buffer = append(reader.buffer, protocol.SymbolEscape)
			case 0x01:
				reader.buffer = append(reader.buffer, protocol.SymbolSyn)
			default:
				reader.buffer = reader.buffer[:0]
			}
			continue
		}

		switch symbol {
		case protocol.SymbolEscape:
			reader.escape = true
		case protocol.SymbolSyn:
			if len(reader.buffer) == 0 {
				continue
			}
			frame, ok := parseFrame(reader.buffer)
			reader.buffer = reader.buffer[:0]
			if ok {
				return frame, true, nil
			}
		default:
			reader.buffer = append(reader.buffer, symbol)
		}
	}
}

func parseFrame(raw []byte) (protocol.Frame, bool) {
	if len(raw) < 6 {
		return protocol.Frame{}, false
	}
	length := int(raw[4])
	expected := 6 + length
	if len(raw) != expected {
		return protocol.Frame{}, false
	}
	crc := protocol.CRC(raw[:len(raw)-1])
	if crc != raw[len(raw)-1] {
		return protocol.Frame{}, false
	}
	data := append([]byte(nil), raw[5:5+length]...)
	return protocol.Frame{
		Source:    raw[0],
		Target:    raw[1],
		Primary:   raw[2],
		Secondary: raw[3],
		Data:      data,
	}, true
}
