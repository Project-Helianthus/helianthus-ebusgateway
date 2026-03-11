package ebusgateway

import (
	"context"
	"net"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	vaillantproviders "github.com/Project-Helianthus/helianthus-ebusreg/providers/vaillant"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
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
	DefaultSemanticEnergyInterval            = 5 * time.Minute
	DefaultSemanticRegulatorRecheckInterval  = 60 * time.Second
	DefaultSemanticRegulatorAbsenceGrace     = 5 * time.Minute

	DefaultMetricsPath                             = "/metrics"
	DefaultObserveFirstRecentMessageCapacity       = 1024
	DefaultObserveFirstPeriodicityCapacity         = 256
	DefaultObserveFirstPeriodicityStaleTTL         = time.Hour
	DefaultObserveFirstSeriesBudget                = 1024
	DefaultObserveFirstWarmupConnectedWindow       = 30 * time.Second
	DefaultObserveFirstWarmupCompletedTransactions = 20
	DefaultObserveFirstWarmupPostResetWindow       = 5 * time.Second
	DefaultObserveFirstWarmupPostResetTransactions = 10
	DefaultObserveFirstWarmupOuterWindow           = 5 * time.Minute
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

type PassiveObserveFirstSupport string

const (
	PassiveObserveFirstSupportAuto                       PassiveObserveFirstSupport = "auto"
	PassiveObserveFirstSupportUnsupportedOrMisconfigured PassiveObserveFirstSupport = "unsupported_or_misconfigured"
)

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
	SemanticInterval                        time.Duration
	SemanticDiscoveryInterval               time.Duration
	SemanticConfigInterval                  time.Duration
	SemanticStateInterval                   time.Duration
	SemanticEnergyInterval                  time.Duration
	SemanticRequestTimeout                  time.Duration
	SemanticReadBreakerFailureBudget        int
	SemanticReadBreakerFailureBudgetSet     bool
	SemanticReadBreakerOpenCooldown         time.Duration
	SemanticReadBreakerHalfOpenProbeLimit   int
	SemanticZonePresenceMissThreshold       int
	SemanticZonePresenceHitThreshold        int
	SemanticDHWStaleTTL                     time.Duration
	SemanticRegulatorRecheckInterval        time.Duration
	SemanticRegulatorAbsenceGrace           time.Duration
	SemanticCachePath                       string
	BroadcastListen                         bool
	PassiveObserveFirstSupport              PassiveObserveFirstSupport
	PassiveAbsenceThreshold                 time.Duration
	PassiveTransactionWatchdog              time.Duration
	PassiveReconnectInitialDelay            time.Duration
	PassiveReconnectMaxDelay                time.Duration
	PassiveDedupActivePublishBudget         time.Duration
	PassiveDedupPassiveDeliveryBudget       time.Duration
	PassiveDedupPendingGraceTimeout         time.Duration
	PassiveDedupActiveFingerprintRetention  time.Duration
	PassiveDedupPendingCapacity             int
	PassiveDedupRecoveryHysteresis          time.Duration
	PassiveDedupRecoveryEventThreshold      int
	LocalAddressSnapshotter                 LocalBusAddressSnapshotter
	HTTPAddr                                string
	MetricsPath                             string
	GraphQLPath                             string
	SnapshotPath                            string
	SubscriptionPath                        string
	MCPPath                                 string
	UIPath                                  string
	PortalPath                              string
	MDNSAdvertise                           bool
	MDNSInstance                            string
	DumpOutputDir                           string
	DumpUploadPath                          string
	DumpUploadURL                           string
	DumpIncludePII                          bool
	ObserveFirstRecentMessageCapacity       int
	ObserveFirstPeriodicityCapacity         int
	ObserveFirstPeriodicityStaleTTL         time.Duration
	ObserveFirstSeriesBudget                int
	ObserveFirstWarmupConnectedWindow       time.Duration
	ObserveFirstWarmupCompletedTransactions int
	ObserveFirstWarmupPostResetWindow       time.Duration
	ObserveFirstWarmupPostResetTransactions int
	ObserveFirstWarmupOuterWindow           time.Duration
}

func DefaultConfig() Config {
	dedupDefaults := defaultPassiveDedupBudgets(protocol.DefaultBusConfig().RetryEnvelope())
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
		ScanRequestTimeout:                      400 * time.Millisecond,
		ScanInterval:                            30 * time.Second,
		BootLiveTimeout:                         2 * time.Minute,
		SemanticInterval:                        1 * time.Minute,
		SemanticDiscoveryInterval:               10 * time.Minute,
		SemanticConfigInterval:                  5 * time.Minute,
		SemanticStateInterval:                   1 * time.Minute,
		SemanticEnergyInterval:                  DefaultSemanticEnergyInterval,
		SemanticRequestTimeout:                  2 * time.Second,
		SemanticReadBreakerFailureBudget:        DefaultSemanticReadFailureBudget,
		SemanticReadBreakerOpenCooldown:         DefaultSemanticReadOpenCooldown,
		SemanticReadBreakerHalfOpenProbeLimit:   DefaultSemanticReadHalfOpenProbeLimit,
		SemanticZonePresenceMissThreshold:       DefaultSemanticZonePresenceMissThreshold,
		SemanticZonePresenceHitThreshold:        DefaultSemanticZonePresenceHitThreshold,
		SemanticDHWStaleTTL:                     DefaultSemanticDHWStaleTTL,
		SemanticRegulatorRecheckInterval:        DefaultSemanticRegulatorRecheckInterval,
		SemanticRegulatorAbsenceGrace:           DefaultSemanticRegulatorAbsenceGrace,
		SemanticCachePath:                       "./semantic_cache.json",
		PassiveObserveFirstSupport:              PassiveObserveFirstSupportAuto,
		PassiveAbsenceThreshold:                 10 * time.Second,
		PassiveTransactionWatchdog:              1 * time.Second,
		PassiveReconnectInitialDelay:            1 * time.Second,
		PassiveReconnectMaxDelay:                30 * time.Second,
		PassiveDedupActivePublishBudget:         dedupDefaults.ActivePublishBudget,
		PassiveDedupPassiveDeliveryBudget:       dedupDefaults.PassiveDeliveryBudget,
		PassiveDedupPendingGraceTimeout:         dedupDefaults.PendingGraceTimeout,
		PassiveDedupActiveFingerprintRetention:  dedupDefaults.ActiveFingerprintRetention,
		PassiveDedupPendingCapacity:             dedupDefaults.PendingCapacity,
		PassiveDedupRecoveryHysteresis:          dedupDefaults.RecoveryHysteresis,
		PassiveDedupRecoveryEventThreshold:      dedupDefaults.RecoveryEventThreshold,
		HTTPAddr:                                ":8080",
		MetricsPath:                             DefaultMetricsPath,
		GraphQLPath:                             "/graphql",
		SnapshotPath:                            "/snapshot",
		SubscriptionPath:                        "/graphql/subscriptions",
		MCPPath:                                 "/mcp",
		UIPath:                                  "/ui",
		PortalPath:                              "/portal",
		MDNSAdvertise:                           true,
		MDNSInstance:                            "helianthus",
		DumpOutputDir:                           "./dumps",
		ObserveFirstRecentMessageCapacity:       DefaultObserveFirstRecentMessageCapacity,
		ObserveFirstPeriodicityCapacity:         DefaultObserveFirstPeriodicityCapacity,
		ObserveFirstPeriodicityStaleTTL:         DefaultObserveFirstPeriodicityStaleTTL,
		ObserveFirstSeriesBudget:                DefaultObserveFirstSeriesBudget,
		ObserveFirstWarmupConnectedWindow:       DefaultObserveFirstWarmupConnectedWindow,
		ObserveFirstWarmupCompletedTransactions: DefaultObserveFirstWarmupCompletedTransactions,
		ObserveFirstWarmupPostResetWindow:       DefaultObserveFirstWarmupPostResetWindow,
		ObserveFirstWarmupPostResetTransactions: DefaultObserveFirstWarmupPostResetTransactions,
		ObserveFirstWarmupOuterWindow:           DefaultObserveFirstWarmupOuterWindow,
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
	if cfg.PassiveObserveFirstSupport == "" {
		cfg.PassiveObserveFirstSupport = PassiveObserveFirstSupportAuto
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
	if cfg.SemanticEnergyInterval <= 0 {
		cfg.SemanticEnergyInterval = DefaultSemanticEnergyInterval
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
	if cfg.SemanticRegulatorRecheckInterval <= 0 {
		cfg.SemanticRegulatorRecheckInterval = DefaultSemanticRegulatorRecheckInterval
	}
	if cfg.SemanticRegulatorAbsenceGrace <= 0 {
		cfg.SemanticRegulatorAbsenceGrace = DefaultSemanticRegulatorAbsenceGrace
	}
	if cfg.SemanticCachePath == "" {
		cfg.SemanticCachePath = "./semantic_cache.json"
	}
	if cfg.PassiveAbsenceThreshold <= 0 {
		cfg.PassiveAbsenceThreshold = 10 * time.Second
	}
	if cfg.PassiveTransactionWatchdog <= 0 {
		cfg.PassiveTransactionWatchdog = 1 * time.Second
	}
	if cfg.PassiveReconnectInitialDelay <= 0 {
		cfg.PassiveReconnectInitialDelay = 1 * time.Second
	}
	if cfg.PassiveReconnectMaxDelay <= 0 {
		cfg.PassiveReconnectMaxDelay = 30 * time.Second
	}
	dedupDefaults := defaultPassiveDedupBudgets(cfg.BusConfig.RetryEnvelope())
	if cfg.PassiveDedupActivePublishBudget <= 0 {
		cfg.PassiveDedupActivePublishBudget = dedupDefaults.ActivePublishBudget
	}
	if cfg.PassiveDedupPassiveDeliveryBudget <= 0 {
		cfg.PassiveDedupPassiveDeliveryBudget = dedupDefaults.PassiveDeliveryBudget
	}
	if cfg.PassiveDedupPendingGraceTimeout <= 0 {
		cfg.PassiveDedupPendingGraceTimeout = dedupDefaults.PendingGraceTimeout
	}
	if cfg.PassiveDedupActiveFingerprintRetention <= 0 {
		cfg.PassiveDedupActiveFingerprintRetention = dedupDefaults.ActiveFingerprintRetention
	}
	if cfg.PassiveDedupPendingCapacity <= 0 {
		cfg.PassiveDedupPendingCapacity = dedupDefaults.PendingCapacity
	}
	if cfg.PassiveDedupRecoveryHysteresis <= 0 {
		cfg.PassiveDedupRecoveryHysteresis = dedupDefaults.RecoveryHysteresis
	}
	if cfg.PassiveDedupRecoveryEventThreshold <= 0 {
		cfg.PassiveDedupRecoveryEventThreshold = dedupDefaults.RecoveryEventThreshold
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
	if cfg.MetricsPath == "" {
		cfg.MetricsPath = DefaultMetricsPath
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
	if cfg.ObserveFirstRecentMessageCapacity <= 0 {
		cfg.ObserveFirstRecentMessageCapacity = DefaultObserveFirstRecentMessageCapacity
	}
	if cfg.ObserveFirstPeriodicityCapacity <= 0 {
		cfg.ObserveFirstPeriodicityCapacity = DefaultObserveFirstPeriodicityCapacity
	}
	if cfg.ObserveFirstPeriodicityStaleTTL <= 0 {
		cfg.ObserveFirstPeriodicityStaleTTL = DefaultObserveFirstPeriodicityStaleTTL
	}
	if cfg.ObserveFirstSeriesBudget <= 0 {
		cfg.ObserveFirstSeriesBudget = DefaultObserveFirstSeriesBudget
	}
	if cfg.ObserveFirstWarmupConnectedWindow <= 0 {
		cfg.ObserveFirstWarmupConnectedWindow = DefaultObserveFirstWarmupConnectedWindow
	}
	if cfg.ObserveFirstWarmupCompletedTransactions <= 0 {
		cfg.ObserveFirstWarmupCompletedTransactions = DefaultObserveFirstWarmupCompletedTransactions
	}
	if cfg.ObserveFirstWarmupPostResetWindow <= 0 {
		cfg.ObserveFirstWarmupPostResetWindow = DefaultObserveFirstWarmupPostResetWindow
	}
	if cfg.ObserveFirstWarmupPostResetTransactions <= 0 {
		cfg.ObserveFirstWarmupPostResetTransactions = DefaultObserveFirstWarmupPostResetTransactions
	}
	if cfg.ObserveFirstWarmupOuterWindow <= 0 {
		cfg.ObserveFirstWarmupOuterWindow = DefaultObserveFirstWarmupOuterWindow
	}
	return cfg
}
