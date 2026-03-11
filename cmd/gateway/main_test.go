package main

import (
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

type fixedLocalSnapshotter struct {
	snapshot ebusgateway.LocalAddressSnapshot
}

func (snapshotter fixedLocalSnapshotter) LocalAddressSnapshot() ebusgateway.LocalAddressSnapshot {
	return snapshotter.snapshot
}

func TestBindFlags_SourceAddrAuto(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{"-source-addr", "auto"}); err != nil {
		t.Fatalf("parse source-addr auto: %v", err)
	}
	if cfg.ScanSource != 0x00 {
		t.Fatalf("ScanSource = 0x%02x; want 0x00", cfg.ScanSource)
	}
	if !cfg.ScanSourceAuto {
		t.Fatal("ScanSourceAuto = false; want true")
	}
}

func TestBindFlags_SourceAddrExplicitZeroEnablesAuto(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{"-source-addr", "0x00"}); err != nil {
		t.Fatalf("parse source-addr 0x00: %v", err)
	}
	if cfg.ScanSource != 0x00 {
		t.Fatalf("ScanSource = 0x%02x; want 0x00", cfg.ScanSource)
	}
	if !cfg.ScanSourceAuto {
		t.Fatal("ScanSourceAuto = false; want true")
	}
}

func TestBindFlags_SourceAddrExplicitDisablesAuto(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{"-source-addr", "0xF0"}); err != nil {
		t.Fatalf("parse source-addr 0xF0: %v", err)
	}
	if cfg.ScanSource != 0xF0 {
		t.Fatalf("ScanSource = 0x%02x; want 0xF0", cfg.ScanSource)
	}
	if cfg.ScanSourceAuto {
		t.Fatal("ScanSourceAuto = true; want false")
	}
}

func TestApplyTransportSourcePolicy_EbusdTCPAutoUsesEbusdSource(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.TransportConfig.Protocol = ebusgateway.TransportEbusdTCP
	cfg.ScanSource = 0x00
	cfg.ScanSourceAuto = true

	applyTransportSourcePolicy(&cfg)

	if cfg.ScanSource != 0x31 {
		t.Fatalf("ScanSource = 0x%02x; want 0x31", cfg.ScanSource)
	}
}

func TestApplyTransportSourcePolicy_NonEbusdAutoRemainsDynamic(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.TransportConfig.Protocol = ebusgateway.TransportENH
	cfg.ScanSource = 0x00
	cfg.ScanSourceAuto = true

	applyTransportSourcePolicy(&cfg)

	if cfg.ScanSource != 0x00 {
		t.Fatalf("ScanSource = 0x%02x; want 0x00", cfg.ScanSource)
	}
}

func TestApplyTransportSourcePolicy_EbusdTCPDefaultF0PromotesToEbusdSource(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.TransportConfig.Protocol = ebusgateway.TransportEbusdTCP
	cfg.ScanSource = 0xF0
	cfg.ScanSourceAuto = false

	applyTransportSourcePolicy(&cfg)

	if cfg.ScanSource != 0x31 {
		t.Fatalf("ScanSource = 0x%02x; want 0x31", cfg.ScanSource)
	}
}

func TestWireObserveFirstObserversWiresDedupSnapshotterIntoObservabilityStore(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.BroadcastListen = true
	cfg.LocalAddressSnapshotter = fixedLocalSnapshotter{
		snapshot: ebusgateway.LocalAddressSnapshot{
			Address: 0x31,
			Known:   true,
			Epoch:   1,
		},
	}

	busObservability, deduplicator, err := wireObserveFirstObservers(&cfg)
	if err != nil {
		t.Fatalf("wireObserveFirstObservers error = %v", err)
	}
	if busObservability == nil {
		t.Fatal("busObservability = nil")
	}
	if deduplicator == nil {
		t.Fatal("deduplicator = nil")
	}

	local := deduplicator.LocalAddressSnapshot()
	if !local.Known || local.Address != 0x31 {
		t.Fatalf("LocalAddressSnapshot = %+v; want known 0x31", local)
	}

	request := protocol.Frame{
		Source:    0x10,
		Target:    0x31,
		Primary:   0x01,
		Secondary: 0x02,
		Data:      []byte{0x03},
	}
	if got := request.Type(); got != protocol.FrameTypeInitiatorInitiator {
		t.Fatalf("request.Type() = %v; want %v", got, protocol.FrameTypeInitiatorInitiator)
	}

	if err := busObservability.OnBusEvent(protocol.BusEvent{
		Kind:       protocol.BusEventAttemptComplete,
		Request:    request,
		HasRequest: true,
	}); err != nil {
		t.Fatalf("OnBusEvent error = %v", err)
	}

	metrics := busObservability.RenderPrometheus()
	if !strings.Contains(metrics, `frame_type="local_participant_inbound"`) {
		t.Fatalf("RenderPrometheus missing local_participant_inbound classification:\n%s", metrics)
	}
}

func TestShouldStartPassiveObserveFirst(t *testing.T) {
	t.Parallel()

	cfg := ebusgateway.DefaultConfig()
	cfg.BroadcastListen = true
	cfg.TransportConfig.Protocol = ebusgateway.TransportEbusdTCP
	if shouldStartPassiveObserveFirst(cfg) {
		t.Fatal("shouldStartPassiveObserveFirst() = true; want false for ebusd-tcp")
	}

	cfg.TransportConfig.Protocol = ebusgateway.TransportENH
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "127.0.0.1:19001"
	if !shouldStartPassiveObserveFirst(cfg) {
		t.Fatal("shouldStartPassiveObserveFirst() = false; want true for loopback proxy endpoint")
	}

	cfg.TransportConfig.Address = "192.168.100.4:19001"
	if !shouldStartPassiveObserveFirst(cfg) {
		t.Fatal("shouldStartPassiveObserveFirst() = false; want true for remote proxy-like endpoint")
	}

	cfg.TransportConfig.Address = "192.168.100.2:9999"
	if shouldStartPassiveObserveFirst(cfg) {
		t.Fatal("shouldStartPassiveObserveFirst() = true; want false for direct remote endpoint")
	}

	cfg.TransportConfig.Address = "adapter.local:9999"
	if shouldStartPassiveObserveFirst(cfg) {
		t.Fatal("shouldStartPassiveObserveFirst() = true; want false for hostname direct remote endpoint")
	}

	cfg.TransportConfig.Address = "proxy.local:19001"
	if !shouldStartPassiveObserveFirst(cfg) {
		t.Fatal("shouldStartPassiveObserveFirst() = false; want true for hostname proxy-like endpoint")
	}
}

func TestBindFlags_PortalPath(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{"-portal-path", "/portal-v2"}); err != nil {
		t.Fatalf("parse portal-path: %v", err)
	}
	if cfg.PortalPath != "/portal-v2" {
		t.Fatalf("PortalPath = %q; want /portal-v2", cfg.PortalPath)
	}
}

func TestBindFlags_BootLiveTimeout(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{"-boot-live-timeout", "45s"}); err != nil {
		t.Fatalf("parse boot-live-timeout: %v", err)
	}
	if cfg.BootLiveTimeout != 45*time.Second {
		t.Fatalf("BootLiveTimeout = %s; want 45s", cfg.BootLiveTimeout)
	}
}

func TestBindFlags_SemanticCachePath(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{"-semantic-cache-path", "/tmp/semantic-cache.json"}); err != nil {
		t.Fatalf("parse semantic-cache-path: %v", err)
	}
	if cfg.SemanticCachePath != "/tmp/semantic-cache.json" {
		t.Fatalf("SemanticCachePath = %q; want /tmp/semantic-cache.json", cfg.SemanticCachePath)
	}
}

func TestBindFlags_SemanticReadBreakerConfig(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{
		"-semantic-read-breaker-failure-budget", "3",
		"-semantic-read-breaker-open-cooldown", "20s",
		"-semantic-read-breaker-half-open-probe-limit", "2",
	}); err != nil {
		t.Fatalf("parse semantic read breaker flags: %v", err)
	}
	if cfg.SemanticReadBreakerFailureBudget != 3 {
		t.Fatalf("SemanticReadBreakerFailureBudget = %d; want 3", cfg.SemanticReadBreakerFailureBudget)
	}
	if !cfg.SemanticReadBreakerFailureBudgetSet {
		t.Fatal("SemanticReadBreakerFailureBudgetSet = false; want true after explicit flag parse")
	}
	if cfg.SemanticReadBreakerOpenCooldown != 20*time.Second {
		t.Fatalf("SemanticReadBreakerOpenCooldown = %s; want 20s", cfg.SemanticReadBreakerOpenCooldown)
	}
	if cfg.SemanticReadBreakerHalfOpenProbeLimit != 2 {
		t.Fatalf("SemanticReadBreakerHalfOpenProbeLimit = %d; want 2", cfg.SemanticReadBreakerHalfOpenProbeLimit)
	}
}

func TestBindFlags_SemanticReadBreakerDisableWithZeroBudget(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{"-semantic-read-breaker-failure-budget", "0"}); err != nil {
		t.Fatalf("parse semantic read breaker disable flag: %v", err)
	}
	if cfg.SemanticReadBreakerFailureBudget != 0 {
		t.Fatalf("SemanticReadBreakerFailureBudget = %d; want 0 (disabled)", cfg.SemanticReadBreakerFailureBudget)
	}
	if !cfg.SemanticReadBreakerFailureBudgetSet {
		t.Fatal("SemanticReadBreakerFailureBudgetSet = false; want true when disable flag is provided")
	}
}

func TestBindFlags_SemanticZonePresenceThresholds(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{
		"-semantic-zone-presence-miss-threshold", "4",
		"-semantic-zone-presence-hit-threshold", "3",
	}); err != nil {
		t.Fatalf("parse semantic zone presence threshold flags: %v", err)
	}
	if cfg.SemanticZonePresenceMissThreshold != 4 {
		t.Fatalf("SemanticZonePresenceMissThreshold = %d; want 4", cfg.SemanticZonePresenceMissThreshold)
	}
	if cfg.SemanticZonePresenceHitThreshold != 3 {
		t.Fatalf("SemanticZonePresenceHitThreshold = %d; want 3", cfg.SemanticZonePresenceHitThreshold)
	}
}

func TestBindFlags_SemanticDHWStaleTTL(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{"-semantic-dhw-stale-ttl", "25m"}); err != nil {
		t.Fatalf("parse semantic-dhw-stale-ttl: %v", err)
	}
	if cfg.SemanticDHWStaleTTL != 25*time.Minute {
		t.Fatalf("SemanticDHWStaleTTL = %s; want 25m", cfg.SemanticDHWStaleTTL)
	}
}

func TestBindFlags_SemanticEnergyInterval(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{"-semantic-energy-interval", "7m"}); err != nil {
		t.Fatalf("parse semantic-energy-interval: %v", err)
	}
	if cfg.SemanticEnergyInterval != 7*time.Minute {
		t.Fatalf("SemanticEnergyInterval = %s; want 7m", cfg.SemanticEnergyInterval)
	}
}

func TestNormalizeMountPath(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		fallback string
		want     string
	}{
		{name: "already_clean", input: "/portal", fallback: "/portal", want: "/portal"},
		{name: "without_leading_slash", input: "portal", fallback: "/portal", want: "/portal"},
		{name: "trailing_slash", input: "/portal/", fallback: "/portal", want: "/portal"},
		{name: "root_becomes_fallback", input: "/", fallback: "/portal", want: "/portal"},
		{name: "empty_becomes_fallback", input: "", fallback: "/portal", want: "/portal"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeMountPath(tc.input, tc.fallback); got != tc.want {
				t.Fatalf("normalizeMountPath(%q,%q)=%q; want %q", tc.input, tc.fallback, got, tc.want)
			}
		})
	}
}
