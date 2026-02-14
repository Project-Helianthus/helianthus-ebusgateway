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
	TransportEbusdTCP TransportProtocol = "ebusd-tcp"
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
	ScanTimeout        time.Duration
	ScanRequestTimeout time.Duration
	ScanInterval       time.Duration
	SemanticInterval   time.Duration
	BroadcastListen    bool
	HTTPAddr           string
	GraphQLPath        string
	SnapshotPath       string
	SubscriptionPath   string
	MCPPath            string
	UIPath             string
	MDNSAdvertise      bool
	MDNSInstance       string
	DumpOutputDir      string
	DumpUploadPath     string
	DumpUploadURL      string
	DumpIncludePII     bool
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
		BusConfig:          protocol.DefaultBusConfig(),
		Providers:          vaillantproviders.Default(),
		ScanOnStart:        true,
		ScanSource:         0x30,
		ScanTimeout:        3 * time.Minute,
		ScanRequestTimeout: 150 * time.Millisecond,
		ScanInterval:     30 * time.Second,
		SemanticInterval: 10 * time.Second,
		HTTPAddr:         ":8080",
		GraphQLPath:      "/graphql",
		SnapshotPath:     "/snapshot",
		SubscriptionPath: "/graphql/subscriptions",
		MCPPath:          "/mcp",
		UIPath:           "/ui",
		MDNSAdvertise:    true,
		MDNSInstance:     "helianthus",
		DumpOutputDir:    "./dumps",
	}
}

func applyDefaults(cfg Config) Config {
	if cfg.BusConfig == (protocol.BusConfig{}) {
		cfg.BusConfig = protocol.DefaultBusConfig()
	}
	if cfg.Providers == nil {
		cfg.Providers = vaillantproviders.Default()
	}
	if cfg.ScanSource == 0 {
		cfg.ScanSource = 0x30
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
	if cfg.SemanticInterval == 0 {
		cfg.SemanticInterval = 10 * time.Second
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
	if cfg.MDNSInstance == "" {
		cfg.MDNSInstance = "helianthus"
	}
	if cfg.DumpOutputDir == "" {
		cfg.DumpOutputDir = "./dumps"
	}
	return cfg
}
