package main

import (
	"flag"
	"testing"
	"time"

	"github.com/d3vi1/helianthus-ebusgateway"
)

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
