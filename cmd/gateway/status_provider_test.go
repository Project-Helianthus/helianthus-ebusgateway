package main

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
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

	provider := newRuntimeStatusProvider(cfg)
	daemon := provider.DaemonStatus()
	adapter := provider.AdapterStatus()
	if daemon.InitiatorAddress != "0xF7" {
		t.Fatalf("daemon initiatorAddress = %q; want 0xF7", daemon.InitiatorAddress)
	}
	if adapter.InitiatorAddress != "" {
		t.Fatalf("adapter initiatorAddress = %q; want empty", adapter.InitiatorAddress)
	}
}
