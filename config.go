package ebusgateway

import (
	"context"
	"net"
	"time"

	"github.com/d3vi1/helianthus-ebusgo/protocol"
	"github.com/d3vi1/helianthus-ebusgo/transport"
	vaillantproviders "github.com/d3vi1/helianthus-ebusreg/providers/vaillant"
	"github.com/d3vi1/helianthus-ebusreg/registry"
)

type TransportProtocol string

const (
	TransportENH      TransportProtocol = "enh"
	TransportENS      TransportProtocol = "ens"
	TransportUDPPlain TransportProtocol = "udp-plain"
	TransportTCPPlain TransportProtocol = "tcp-plain"
	TransportEbusdTCP TransportProtocol = "ebusd-tcp"

	DefaultSemanticZonePresenceMissThreshold = 3
	DefaultSemanticZonePresenceHitThreshold  = 2
	DefaultSemanticDHWStaleTTL               = 15 * time.Minute
)

type TransportConfig struct {
	Protocol     TransportProtocol
	Network      string
	Address      string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	DialTimeout  time.Duration
	Dial         func(ctx context.Context, network, address string, timeout time.Duration) (net.Conn, error)
}

type Config struct {
	Transport          transport.RawTransport
	TransportConfig    TransportConfig
	BusConfig          protocol.BusConfig
	QueueCapacity      int
	Providers          []registry.PlaneProvider
	ScanOnStart        bool
	ScanSource         byte
	ScanSourceAuto     bool
	ScanTimeout        time.Duration
	ScanRequestTimeout time.Duration
	ScanInterval       time.Duration
	BootLiveTimeout    time.Duration
	// SemanticInterval is a legacy single-interval semantic polling configuration.
	// Prefer SemanticDiscoveryInterval / SemanticConfigInterval / SemanticStateInterval.
	SemanticInterval                      time.Duration
	SemanticDiscoveryInterval             time.Duration
	SemanticConfigInterval                time.Duration
	SemanticStateInterval                 time.Duration
	SemanticRequestTimeout                time.Duration
	SemanticReadBreakerFailureBudget      int
	SemanticReadBreakerFailureBudgetSet   bool
	SemanticReadBreakerOpenCooldown       time.Duration
	SemanticReadBreakerHalfOpenProbeLimit int
	SemanticZonePresenceMissThreshold     int
	SemanticZonePresenceHitThreshold      int
	SemanticDHWStaleTTL                   time.Duration
	SemanticCachePath                     string
	BroadcastListen                       bool
	HTTPAddr                              string
	GraphQLPath                           string
	SnapshotPath                          string
	SubscriptionPath                      string
	MCPPath                               string
	UIPath                                string
	PortalPath                            string
	MDNSAdvertise                         bool
	MDNSInstance                          string
	DumpOutputDir                         string
	DumpUploadPath                        string
	DumpUploadURL                         string
	DumpIncludePII                        bool
}

func DefaultConfig() Config {
	return Config{
		TransportConfig: TransportConfig{
			Protocol:     TransportENH,
			Network:      "unix",
			Address:      "/var/run/ebusd/ebusd.socket",
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 5 * time.Second,
			DialTimeout:  5 * time.Second,
		},
		BusConfig:   protocol.DefaultBusConfig(),
		Providers:   vaillantproviders.Default(),
		ScanOnStart: true,
		// Use a non-ebusd default to allow running alongside ebusd without address conflicts.
		ScanSource:  0xF0,
		ScanTimeout: 3 * time.Minute,
		// Per-request scan timeout must accommodate bus/transport latency.
		// 150ms is too aggressive for real-world ENH over TCP setups.
		ScanRequestTimeout:                    400 * time.Millisecond,
		ScanInterval:                          30 * time.Second,
		BootLiveTimeout:                       2 * time.Minute,
		SemanticInterval:                      1 * time.Minute,
		SemanticDiscoveryInterval:             10 * time.Minute,
		SemanticConfigInterval:                5 * time.Minute,
		SemanticStateInterval:                 1 * time.Minute,
		SemanticRequestTimeout:                2 * time.Second,
		SemanticReadBreakerFailureBudget:      DefaultSemanticReadFailureBudget,
		SemanticReadBreakerOpenCooldown:       DefaultSemanticReadOpenCooldown,
		SemanticReadBreakerHalfOpenProbeLimit: DefaultSemanticReadHalfOpenProbeLimit,
		SemanticZonePresenceMissThreshold:     DefaultSemanticZonePresenceMissThreshold,
		SemanticZonePresenceHitThreshold:      DefaultSemanticZonePresenceHitThreshold,
		SemanticDHWStaleTTL:                   DefaultSemanticDHWStaleTTL,
		SemanticCachePath:                     "./semantic_cache.json",
		HTTPAddr:                              ":8080",
		GraphQLPath:                           "/graphql",
		SnapshotPath:                          "/snapshot",
		SubscriptionPath:                      "/graphql/subscriptions",
		MCPPath:                               "/mcp",
		UIPath:                                "/ui",
		PortalPath:                            "/portal",
		MDNSAdvertise:                         true,
		MDNSInstance:                          "helianthus",
		DumpOutputDir:                         "./dumps",
	}
}

func applyDefaults(cfg Config) Config {
	if cfg.BusConfig == (protocol.BusConfig{}) {
		cfg.BusConfig = protocol.DefaultBusConfig()
	}
	if cfg.Providers == nil {
		cfg.Providers = vaillantproviders.Default()
	}
	if cfg.ScanSource == 0 && !cfg.ScanSourceAuto {
		cfg.ScanSource = 0xF0
	}
	if cfg.ScanTimeout == 0 {
		cfg.ScanTimeout = 3 * time.Minute
	}
	if cfg.ScanRequestTimeout == 0 {
		cfg.ScanRequestTimeout = 150 * time.Millisecond
	}
	if cfg.ScanInterval == 0 {
		cfg.ScanInterval = 30 * time.Second
	}
	if cfg.BootLiveTimeout == 0 {
		cfg.BootLiveTimeout = 2 * time.Minute
	}
	if cfg.SemanticInterval == 0 {
		cfg.SemanticInterval = 1 * time.Minute
	}
	if cfg.SemanticStateInterval == 0 {
		cfg.SemanticStateInterval = cfg.SemanticInterval
	}
	if cfg.SemanticConfigInterval == 0 {
		cfg.SemanticConfigInterval = 5 * time.Minute
	}
	if cfg.SemanticDiscoveryInterval == 0 {
		cfg.SemanticDiscoveryInterval = 10 * time.Minute
	}
	if cfg.SemanticRequestTimeout == 0 {
		cfg.SemanticRequestTimeout = 2 * time.Second
	}
	if !cfg.SemanticReadBreakerFailureBudgetSet && cfg.SemanticReadBreakerFailureBudget == 0 {
		cfg.SemanticReadBreakerFailureBudget = DefaultSemanticReadFailureBudget
	}
	if cfg.SemanticReadBreakerOpenCooldown == 0 {
		cfg.SemanticReadBreakerOpenCooldown = DefaultSemanticReadOpenCooldown
	}
	if cfg.SemanticReadBreakerHalfOpenProbeLimit == 0 {
		cfg.SemanticReadBreakerHalfOpenProbeLimit = DefaultSemanticReadHalfOpenProbeLimit
	}
	if cfg.SemanticZonePresenceMissThreshold <= 0 {
		cfg.SemanticZonePresenceMissThreshold = DefaultSemanticZonePresenceMissThreshold
	}
	if cfg.SemanticZonePresenceHitThreshold <= 0 {
		cfg.SemanticZonePresenceHitThreshold = DefaultSemanticZonePresenceHitThreshold
	}
	if cfg.SemanticDHWStaleTTL <= 0 {
		cfg.SemanticDHWStaleTTL = DefaultSemanticDHWStaleTTL
	}
	if cfg.SemanticCachePath == "" {
		cfg.SemanticCachePath = "./semantic_cache.json"
	}
	if cfg.Transport == nil {
		if cfg.TransportConfig.Protocol == "" {
			cfg.TransportConfig.Protocol = TransportENH
		}
		if cfg.TransportConfig.Network == "" {
			cfg.TransportConfig.Network = "unix"
		}
		if cfg.TransportConfig.Address == "" {
			cfg.TransportConfig.Address = "/var/run/ebusd/ebusd.socket"
		}
		if cfg.TransportConfig.ReadTimeout == 0 {
			cfg.TransportConfig.ReadTimeout = 5 * time.Second
		}
		if cfg.TransportConfig.WriteTimeout == 0 {
			cfg.TransportConfig.WriteTimeout = 5 * time.Second
		}
		if cfg.TransportConfig.DialTimeout == 0 {
			cfg.TransportConfig.DialTimeout = 5 * time.Second
		}
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = ":8080"
	}
	if cfg.GraphQLPath == "" {
		cfg.GraphQLPath = "/graphql"
	}
	if cfg.SnapshotPath == "" {
		cfg.SnapshotPath = "/snapshot"
	}
	if cfg.SubscriptionPath == "" {
		cfg.SubscriptionPath = "/graphql/subscriptions"
	}
	if cfg.MCPPath == "" {
		cfg.MCPPath = "/mcp"
	}
	if cfg.UIPath == "" {
		cfg.UIPath = "/ui"
	}
	if cfg.PortalPath == "" {
		cfg.PortalPath = "/portal"
	}
	if cfg.MDNSInstance == "" {
		cfg.MDNSInstance = "helianthus"
	}
	if cfg.DumpOutputDir == "" {
		cfg.DumpOutputDir = "./dumps"
	}
	return cfg
}
