package main

import (
	"flag"
	"testing"

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
