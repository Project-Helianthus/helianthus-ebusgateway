package ebusgateway

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
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
	config = clampEbusdTCPTimeouts(config, cfg.ScanRequestTimeout)
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

func clampEbusdTCPTimeouts(config TransportConfig, scanRequestTimeout time.Duration) TransportConfig {
	minTimeout := scanRequestTimeout
	switch config.Protocol {
	case TransportEbusdTCP, TransportENH, TransportENS, TransportUDPPlain, TransportTCPPlain:
	default:
		return config
	}

	if minTimeout <= 0 {
		if config.Protocol != TransportEbusdTCP {
			return config
		}
		minTimeout = 400 * time.Millisecond
	}
	if config.Protocol == TransportEbusdTCP && minTimeout < 400*time.Millisecond {
		minTimeout = 400 * time.Millisecond
	}

	if config.ReadTimeout <= 0 || config.ReadTimeout < minTimeout {
		config.ReadTimeout = minTimeout
	}
	if config.WriteTimeout <= 0 || config.WriteTimeout < minTimeout {
		config.WriteTimeout = minTimeout
	}
	return config
}

func normalizeTransportConfig(config TransportConfig) (TransportConfig, error) {
	config.Protocol = canonicalTransportProtocol(config.Protocol)
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

func canonicalTransportProtocol(protocol TransportProtocol) TransportProtocol {
	switch normalized := TransportProtocol(strings.ToLower(strings.TrimSpace(string(protocol)))); normalized {
	case TransportProtocol("ebusd"):
		return TransportEbusdTCP
	default:
		return normalized
	}
}

func PassiveTransportSupported(cfg Config) bool {
	config, err := normalizeTransportConfig(cfg.TransportConfig)
	if err == nil {
		return config.Protocol != TransportEbusdTCP
	}
	return canonicalTransportProtocol(cfg.TransportConfig.Protocol) != TransportEbusdTCP
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
		// ebusd semantics: ENS is an ENH variant (serial speed selector).
		// For TCP/UDP endpoints, ENS behaves as ENH.
		protocol = TransportENH
	case "udp-plain":
		protocol = TransportUDPPlain
	case "tcp-plain":
		protocol = TransportTCPPlain
	case "ebusd", "ebusd-tcp":
		protocol = TransportEbusdTCP
	case "tcp":
		if protocol == "" {
			protocol = TransportTCPPlain
		}
	case "unix":
	default:
		return "", "", "", fmt.Errorf("gateway transport endpoint unsupported scheme %q", scheme)
	}

	network := ""
	address := ""
	switch {
	case parsed.Host != "":
		network = "tcp"
		if scheme == "udp-plain" {
			network = "udp"
		}
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
	if scheme == "udp-plain" && network != "udp" {
		return "", "", "", fmt.Errorf("gateway transport endpoint %q missing udp host", endpoint)
	}
	if scheme == "tcp-plain" && network != "tcp" {
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
	switch canonicalTransportProtocol(protocolName) {
	case TransportENH, "":
		return transport.NewENHTransport(conn, readTimeout, writeTimeout), nil
	case TransportENS:
		return transport.NewENSTransport(conn, readTimeout, writeTimeout), nil
	case TransportUDPPlain:
		udpConn, ok := conn.(*net.UDPConn)
		if !ok {
			return nil, fmt.Errorf("gateway transport %q requires udp connection: %w", protocolName, ebuserrors.ErrInvalidPayload)
		}
		return transport.NewUDPPlainTransport(udpConn, readTimeout, writeTimeout), nil
	case TransportTCPPlain:
		return transport.NewTCPPlainTransport(conn, readTimeout, writeTimeout), nil
	case TransportEbusdTCP:
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
