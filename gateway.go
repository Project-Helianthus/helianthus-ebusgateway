package ebusgateway

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	ebuserrors "github.com/d3vi1/helianthus-ebusgo/errors"
	"github.com/d3vi1/helianthus-ebusgo/protocol"
	"github.com/d3vi1/helianthus-ebusgo/transport"
	"github.com/d3vi1/helianthus-ebusreg/registry"
	"github.com/d3vi1/helianthus-ebusreg/router"
)

type Gateway struct {
	Transport transport.RawTransport
	Bus       *protocol.Bus
	Registry  *registry.DeviceRegistry
	Router    *router.BusEventRouter
	closeFn   func() error
}

func New(ctx context.Context, cfg Config) (*Gateway, error) {
	cfg = applyDefaults(cfg)

	transportLayer, closeFn, err := resolveTransport(ctx, cfg)
	if err != nil {
		return nil, err
	}

	bus := protocol.NewBus(transportLayer, cfg.BusConfig, cfg.QueueCapacity)
	deviceRegistry := registry.NewDeviceRegistry(cfg.Providers)
	eventRouter := router.NewBusEventRouter(bus)

	return &Gateway{
		Transport: transportLayer,
		Bus:       bus,
		Registry:  deviceRegistry,
		Router:    eventRouter,
		closeFn:   closeFn,
	}, nil
}

func (g *Gateway) Start(ctx context.Context) {
	if g == nil || g.Bus == nil {
		return
	}
	g.Bus.Run(ctx)
}

func (g *Gateway) Close() error {
	if g == nil || g.closeFn == nil {
		return nil
	}
	return g.closeFn()
}

func (g *Gateway) RefreshRouterPlanes() int {
	if g == nil || g.Registry == nil || g.Router == nil {
		return 0
	}

	planes := make([]router.Plane, 0)
	g.Registry.Iterate(func(entry registry.DeviceEntry) bool {
		for _, plane := range entry.Planes() {
			if routerPlane, ok := plane.(router.Plane); ok {
				planes = append(planes, routerPlane)
			}
		}
		return true
	})

	g.Router.SetPlanes(planes)
	return len(planes)
}

func resolveTransport(ctx context.Context, cfg Config) (transport.RawTransport, func() error, error) {
	if cfg.Transport != nil {
		return cfg.Transport, cfg.Transport.Close, nil
	}

	if ctx == nil {
		ctx = context.Background()
	}

	config := cfg.TransportConfig
	if config.Network == "" {
		return nil, nil, fmt.Errorf("gateway transport missing network: %w", ebuserrors.ErrInvalidPayload)
	}
	if config.Address == "" {
		return nil, nil, fmt.Errorf("gateway transport missing address: %w", ebuserrors.ErrInvalidPayload)
	}

	dial := config.Dial
	if dial == nil {
		dial = dialContext
	}

	conn, err := dial(ctx, config.Network, config.Address, config.DialTimeout)
	if err != nil {
		return nil, nil, fmt.Errorf("gateway transport dial failed: %w", err)
	}

	transportLayer, err := transportFromConn(config.Protocol, conn, config.ReadTimeout, config.WriteTimeout)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}

	return transportLayer, conn.Close, nil
}

func transportFromConn(protocolName TransportProtocol, conn net.Conn, readTimeout, writeTimeout time.Duration) (transport.RawTransport, error) {
	normalized := strings.ToLower(string(protocolName))
	switch TransportProtocol(normalized) {
	case TransportENH, "":
		return transport.NewENHTransport(conn, readTimeout, writeTimeout), nil
	case TransportENS:
		return transport.NewENSTransport(conn, readTimeout, writeTimeout), nil
	default:
		return nil, fmt.Errorf("gateway transport unsupported protocol %q: %w", protocolName, ebuserrors.ErrInvalidPayload)
	}
}

func dialContext(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
	dialer := net.Dialer{Timeout: timeout}
	return dialer.DialContext(ctx, network, address)
}
