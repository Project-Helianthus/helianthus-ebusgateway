package ebusgateway

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
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
	extraMu   sync.RWMutex
	extra     []router.Plane
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

	g.extraMu.RLock()
	extra := append([]router.Plane(nil), g.extra...)
	g.extraMu.RUnlock()

	planes := make([]router.Plane, 0, len(extra))
	g.Registry.Iterate(func(entry registry.DeviceEntry) bool {
		for _, plane := range entry.Planes() {
			if routerPlane, ok := plane.(router.Plane); ok {
				planes = append(planes, routerPlane)
			}
		}
		return true
	})

	planes = append(planes, extra...)
	g.Router.SetPlanes(planes)
	return len(planes)
}

func (g *Gateway) AddRouterPlane(plane router.Plane) {
	if g == nil || plane == nil {
		return
	}
	g.extraMu.Lock()
	g.extra = append(g.extra, plane)
	g.extraMu.Unlock()
}

func resolveTransport(ctx context.Context, cfg Config) (transport.RawTransport, func() error, error) {
	if cfg.Transport != nil {
		if err := initTransportIfSupported(cfg.Transport); err != nil {
			return nil, nil, err
		}
		return cfg.Transport, cfg.Transport.Close, nil
	}

	if ctx == nil {
		ctx = context.Background()
	}

	config := cfg.TransportConfig
	config, err := normalizeTransportConfig(config)
	if err != nil {
		return nil, nil, err
	}
	// ebusd command port responses include the bus roundtrip, so short socket
	// deadlines can desync the stream (late "ERR:" lines from a previous command
	// appear as the next command's response). Clamp to at least the per-request
	// scan timeout to keep scans stable under the default add-on config.
	if config.Protocol == TransportEbusdTCP {
		minTimeout := cfg.ScanRequestTimeout
		if minTimeout <= 0 {
			minTimeout = 400 * time.Millisecond
		}
		if config.ReadTimeout > 0 && config.ReadTimeout < minTimeout {
			config.ReadTimeout = minTimeout
		}
		if config.WriteTimeout > 0 && config.WriteTimeout < minTimeout {
			config.WriteTimeout = minTimeout
		}
	}
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
	if err := initTransportIfSupported(transportLayer); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}

	return transportLayer, conn.Close, nil
}

func normalizeTransportConfig(config TransportConfig) (TransportConfig, error) {
	config.Protocol = TransportProtocol(strings.ToLower(strings.TrimSpace(string(config.Protocol))))
	config.Network = strings.ToLower(strings.TrimSpace(config.Network))
	config.Address = strings.TrimSpace(config.Address)

	if !strings.Contains(config.Address, "://") {
		if config.Protocol == "" {
			config.Protocol = TransportENH
		}
		return config, nil
	}

	protocol, network, address, err := parseTransportEndpoint(config.Address, config.Protocol)
	if err != nil {
		return TransportConfig{}, err
	}
	config.Protocol = protocol
	config.Network = network
	config.Address = address
	return config, nil
}

func parseTransportEndpoint(endpoint string, fallbackProtocol TransportProtocol) (TransportProtocol, string, string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", "", "", fmt.Errorf("gateway transport endpoint parse failed %q: %w", endpoint, err)
	}

	scheme := strings.ToLower(strings.TrimSpace(parsed.Scheme))
	if scheme == "" {
		return "", "", "", fmt.Errorf("gateway transport endpoint missing scheme %q", endpoint)
	}

	protocol := fallbackProtocol
	switch scheme {
	case "enh":
		protocol = TransportENH
	case "ens":
		protocol = TransportENS
	case "ebusd", "ebusd-tcp":
		protocol = TransportEbusdTCP
	case "tcp", "unix":
	default:
		return "", "", "", fmt.Errorf("gateway transport endpoint unsupported scheme %q", scheme)
	}

	network := ""
	address := ""
	switch {
	case parsed.Host != "":
		network = "tcp"
		address = strings.TrimSpace(parsed.Host)
	case parsed.Path != "" && parsed.Path != "/":
		network = "unix"
		address = strings.TrimSpace(parsed.Path)
	default:
		return "", "", "", fmt.Errorf("gateway transport endpoint %q missing host or unix path", endpoint)
	}

	if scheme == "tcp" && network != "tcp" {
		return "", "", "", fmt.Errorf("gateway transport endpoint %q missing tcp host", endpoint)
	}
	if scheme == "unix" && network != "unix" {
		return "", "", "", fmt.Errorf("gateway transport endpoint %q missing unix path", endpoint)
	}
	if protocol == "" {
		protocol = TransportENH
	}

	return protocol, network, address, nil
}

func transportFromConn(protocolName TransportProtocol, conn net.Conn, readTimeout, writeTimeout time.Duration) (transport.RawTransport, error) {
	normalized := strings.ToLower(string(protocolName))
	switch TransportProtocol(normalized) {
	case TransportENH, "":
		return transport.NewENHTransport(conn, readTimeout, writeTimeout), nil
	case TransportENS:
		return transport.NewENSTransport(conn, readTimeout, writeTimeout), nil
	case TransportEbusdTCP, TransportProtocol("ebusd"):
		return transport.NewEbusdTCPTransport(conn, readTimeout, writeTimeout), nil
	default:
		return nil, fmt.Errorf("gateway transport unsupported protocol %q: %w", protocolName, ebuserrors.ErrInvalidPayload)
	}
}

type initTransport interface {
	Init(features byte) error
}

func initTransportIfSupported(tr transport.RawTransport) error {
	initializer, ok := tr.(initTransport)
	if !ok {
		return nil
	}
	// Match ebusd behavior: request additional infos (bit0) during INIT.
	// Some adapters gate optional capabilities behind this feature flag.
	const defaultInitFeatures = byte(0x01)
	if err := initializer.Init(defaultInitFeatures); err != nil {
		return fmt.Errorf("gateway transport init failed: %w", err)
	}
	return nil
}

func dialContext(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error) {
	dialer := net.Dialer{Timeout: timeout}
	return dialer.DialContext(ctx, network, address)
}
