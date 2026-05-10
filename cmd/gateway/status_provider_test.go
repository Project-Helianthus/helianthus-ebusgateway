package main

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
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

// -----------------------------------------------------------------------------
// AD24 invariant test (runtime-state-w19-26.locked):
//
// "ebus.self is HISTORICAL HINT ONLY. The current admitted source is
// exclusively the in-memory SourceAddressSelection.Source from the current
// session, AFTER SourceAddressSelector validation succeeds. No surface
// (loader, GraphQL, MCP, metrics) may expose runtime_state.ebus.self as the
// current admitted source until the current session's SourceAddressSelector
// validation passes."
//
// These tests pin the gateway-side surface contract: even when a
// runtime-state cache holds ebus.self.last_admitted_source = X, BEFORE the
// selector validates a candidate the surfaces (daemon InitiatorAddress,
// gatewayIdentity, MCP runtime status) MUST report the configured/auto
// state — never the cached X.
// -----------------------------------------------------------------------------

func TestAD24_DaemonInitiatorAddressDoesNotLeakCachedHint(t *testing.T) {
	// Simulate the boot sequence on a source-selection-capable transport:
	// runtime-state cache has a prior admitted source, but the live
	// session has not yet completed SourceAddressSelector validation, so
	// cfg.ScanSource is still in its "auto" pre-selection state.
	cachedHint, ok := runtimestate.HintFromState(&runtimestate.State{
		SchemaVersion: 1,
		EBus: &runtimestate.EBusNamespace{
			SchemaVersion: 1,
			Self: &runtimestate.Self{
				LastAdmittedSource: 0x77,
				SelectionMethod:    runtimestate.SelectionMethodWarmup,
			},
		},
	})
	if !ok || cachedHint != 0x77 {
		t.Fatalf("HintFromState fixture broken: got hint=0x%02x ok=%v", cachedHint, ok)
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.ScanSource = 0x00     // pre-selection auto
	cfg.ScanSourceAuto = true // pre-selection auto

	provider := newRuntimeStatusProvider(cfg, nil)
	daemon := provider.DaemonStatus()
	if daemon.InitiatorAddress != "auto" {
		t.Errorf("AD24 violation: daemon InitiatorAddress = %q; want \"auto\" "+
			"(cached hint 0x%02x must NOT leak into the daemon-status surface "+
			"before SourceAddressSelector validation completes)",
			daemon.InitiatorAddress, cachedHint)
	}

	// Belt-and-suspenders: the MCP daemon-status surface and the GraphQL
	// gateway-identity surface are independent constructions; verify them
	// too. None of them takes a runtimestate.State / Manager as input —
	// the package-level coupling enforces AD24 by construction (Codex
	// reviewers should flag any change that introduces a runtimestate
	// import here without an explicit hint-only audit).
	mcpProvider := newMCPRuntimeStatusProvider(cfg, nil)
	mcpDaemon := mcpProvider.DaemonStatus()
	if mcpDaemon.InitiatorAddress != "auto" {
		t.Errorf("AD24 violation: MCP daemon InitiatorAddress = %q; want \"auto\"",
			mcpDaemon.InitiatorAddress)
	}

	identityProvider := newRuntimeGatewayIdentityProvider(cfg)
	identity := identityProvider.GatewayIdentity()
	// gatewayIdentity has no source-address field today — the AD24 rule
	// is partly enforced by the type itself. If a future PR adds a
	// "currentInitiator" or "admittedSource" field to graphql.GatewayIdentity,
	// this test should be expanded to verify the field reflects builder.AdmittedMutationSource(),
	// NOT a runtimestate cached value.
	if identity.InstanceGUID != cfg.InstanceGUID {
		t.Errorf("gatewayIdentity GUID = %q; want %q (sanity)", identity.InstanceGUID, cfg.InstanceGUID)
	}
}

func TestAD24_DaemonInitiatorAddressReflectsValidatedSourcePostSelection(t *testing.T) {
	// After SourceAddressSelector validation succeeds (or operator pinned
	// an explicit source), cfg.ScanSource is mutated to the validated
	// byte and cfg.ScanSourceAuto is cleared. The daemon-status surface
	// then reports the validated source — but the runtime-state cache
	// is irrelevant to this transition: the surface reads from cfg, not
	// from any State / Manager.
	cfg := ebusgateway.DefaultConfig()
	cfg.ScanSource = 0x77
	cfg.ScanSourceAuto = false

	provider := newRuntimeStatusProvider(cfg, nil)
	daemon := provider.DaemonStatus()
	if daemon.InitiatorAddress != "0x77" {
		t.Errorf("post-selection daemon InitiatorAddress = %q; want \"0x77\"",
			daemon.InitiatorAddress)
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
