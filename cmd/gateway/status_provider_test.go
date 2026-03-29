package main

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
)

func TestFormatConfiguredInitiator(t *testing.T) {
	tests := []struct {
		name   string
		source byte
		auto   bool
		want   string
	}{
		{name: "explicit_source", source: 0xF0, auto: false, want: "0xF0"},
		{name: "auto_unresolved", source: 0x00, auto: true, want: "auto"},
		{name: "auto_policy_ebusd", source: 0x31, auto: true, want: "0x31"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := formatConfiguredInitiator(test.source, test.auto)
			if got != test.want {
				t.Fatalf("formatConfiguredInitiator(0x%02x, auto=%v) = %q; want %q", test.source, test.auto, got, test.want)
			}
		})
	}
}

func TestRuntimeStatusProviderIncludesInitiatorAddress(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.ScanSource = 0xF7
	cfg.ScanSourceAuto = false

	provider := newRuntimeStatusProvider(cfg, nil)
	daemon := provider.DaemonStatus()
	adapter := provider.AdapterStatus()
	if daemon.InitiatorAddress != "0xF7" {
		t.Fatalf("daemon initiatorAddress = %q; want 0xF7", daemon.InitiatorAddress)
	}
	if adapter.InitiatorAddress != "" {
		t.Fatalf("adapter initiatorAddress = %q; want empty", adapter.InitiatorAddress)
	}
}

func TestRuntimeStatusProviderReflectsAdapterFirmwareVersion(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	semantic := graphql.NewLiveSemanticProvider()
	semantic.SetAdapterHardwareInfo(&graphql.AdapterHardwareInfo{
		FirmwareVersion:    "0x31",
		InfoSupported:      true,
		VersionResponseLen: 5,
	})

	provider := newRuntimeStatusProvider(cfg, semantic)
	adapter := provider.AdapterStatus()
	if adapter.Status != "running" {
		t.Fatalf("adapter status = %q; want running", adapter.Status)
	}
	if adapter.FirmwareVersion != "0x31" {
		t.Fatalf("adapter firmwareVersion = %q; want 0x31", adapter.FirmwareVersion)
	}
}

func TestRuntimeGatewayIdentityProviderExposesConfiguredGUID(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.InstanceGUID = "4d9336aa-f125-4f12-8b07-fcd18dbfcb10"

	provider := newRuntimeGatewayIdentityProvider(cfg)
	identity := provider.GatewayIdentity()
	if identity.InstanceGUID != "4d9336aa-f125-4f12-8b07-fcd18dbfcb10" {
		t.Fatalf("gateway identity GUID = %q; want configured GUID", identity.InstanceGUID)
	}
}

func TestRuntimeStatusProviderKeepsUnknownWithoutAdapterIdentity(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	semantic := graphql.NewLiveSemanticProvider()
	semantic.SetAdapterHardwareInfo(&graphql.AdapterHardwareInfo{})

	provider := newRuntimeStatusProvider(cfg, semantic)
	adapter := provider.AdapterStatus()
	if adapter.Status != "unknown" {
		t.Fatalf("adapter status = %q; want unknown", adapter.Status)
	}
	if adapter.FirmwareVersion != "" {
		t.Fatalf("adapter firmwareVersion = %q; want empty", adapter.FirmwareVersion)
	}
}
