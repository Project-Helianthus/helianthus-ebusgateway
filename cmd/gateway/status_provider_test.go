package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/b503session"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

type issue851LiveSourceExplorerBus struct {
	mu      sync.Mutex
	sources []byte
}

func (bus *issue851LiveSourceExplorerBus) Send(_ context.Context, frame protocol.Frame) (*protocol.Frame, error) {
	bus.mu.Lock()
	bus.sources = append(bus.sources, frame.Source)
	bus.mu.Unlock()
	return &protocol.Frame{
		Source:    frame.Target,
		Target:    frame.Source,
		Primary:   frame.Primary,
		Secondary: frame.Secondary,
		Data:      []byte{0x01, 0x02, 0x03, 0x04},
	}, nil
}

func (bus *issue851LiveSourceExplorerBus) snapshotSources() []byte {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	return append([]byte(nil), bus.sources...)
}

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
	provider := newRuntimeStatusProvider(nil, func() (byte, bool) { return 0xF7, true })
	daemon := provider.DaemonStatus()
	adapter := provider.AdapterStatus()
	if daemon.InitiatorAddress != "0xF7" {
		t.Fatalf("daemon initiatorAddress = %q; want 0xF7", daemon.InitiatorAddress)
	}
	if adapter.InitiatorAddress != "" {
		t.Fatalf("adapter initiatorAddress = %q; want empty", adapter.InitiatorAddress)
	}
}

func TestRuntimeStatusProviderRequiresLiveGraphQLAdmission(t *testing.T) {
	builder := graphql.NewBuilder(nil, nil)
	provider := newRuntimeStatusProvider(nil, builder.AdmittedMutationSource)
	if got := provider.DaemonStatus().InitiatorAddress; got != "" {
		t.Fatalf("pre-admission daemon initiatorAddress = %q, want unavailable", got)
	}
	builder.SetAdmittedMutationSource(0x77)
	if got := provider.DaemonStatus().InitiatorAddress; got != "0x77" {
		t.Fatalf("admitted daemon initiatorAddress = %q, want 0x77", got)
	}
	builder.ClearAdmittedMutationSource()
	if got := provider.DaemonStatus().InitiatorAddress; got != "" {
		t.Fatalf("withdrawn daemon initiatorAddress = %q, want unavailable", got)
	}
}

func TestIssue851GraphQLAndExplorerRequireCurrentDriverWriteAdmission(t *testing.T) {
	startEntered := make(chan struct{})
	startRelease := make(chan struct{})
	var factoryCalls atomic.Int32
	runtimeSeam := transport.NewDriverRuntime(func(ctx context.Context) (*transport.ManagedRawTransport, error) {
		call := factoryCalls.Add(1)
		if call == 1 {
			close(startEntered)
			select {
			case <-startRelease:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if call == 2 {
			return nil, errors.New("retry construction unavailable")
		}
		raw := newIssue851BlockingRawTransport()
		return newManagedEBusGeneration(ctx, raw, raw.Close), nil
	}, transport.DriverRuntimeConfig{DrainTimeout: 100 * time.Millisecond})
	runtime := &ebusDriverRuntime{runtime: runtimeSeam}
	manager, err := drivermanager.New(drivermanager.Config{
		RetryJitter: func(time.Duration) time.Duration { return 0 },
		Drivers: []drivermanager.DriverConfig{{
			ID:           primaryEBusDriverID,
			Enabled:      true,
			Runtime:      runtime,
			Capabilities: []drivermanager.Capability{drivermanager.CapabilityRead, drivermanager.CapabilityWrite},
			ClassifyError: func(error) drivermanager.Failure {
				return drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonDependencyUnavailable, Retryable: true}}
			},
			Retry: drivermanager.RetryPolicy{Budget: 1, InitialDelay: 25 * time.Millisecond, MaxDelay: 25 * time.Millisecond},
		}},
	})
	if err != nil {
		t.Fatalf("drivermanager.New() error = %v", err)
	}
	defer func() { _ = manager.Shutdown(context.Background()) }()
	controller := &ebusDriverController{manager: manager}

	// ScanOnStart=false still lets the static ebusd policy select a builder
	// source synchronously. That selection is not runtime admission authority.
	staticCfg := ebusgateway.DefaultConfig()
	staticCfg.ScanOnStart = false
	staticCfg.TransportConfig.Protocol = ebusgateway.TransportEbusdTCP
	staticCfg.ScanSource = 0
	staticCfg.ScanSourceAuto = true
	selected, selectedOK := admittedMutationSourceForGateway(staticCfg, ebusgateway.TransportAdmissionStaticFallback, false)
	if !selectedOK || selected == 0 {
		t.Fatalf("static source fixture = 0x%02X/%v", selected, selectedOK)
	}
	builder := graphql.NewBuilder(nil, nil)
	builder.SetAdmittedMutationSource(selected)
	liveSource := newManagedEBusSourceProvider(controller, builder.AdmittedMutationSource)
	status := newRuntimeStatusProvider(nil, liveSource)
	mcpStatus := newMCPRuntimeStatusProvider(nil, liveSource)
	explorerBus := &issue851LiveSourceExplorerBus{}
	underlyingWriter := &recordingSemanticWriter{}
	graphWriter := admittedGraphQLSemanticWriter{
		boiler: underlyingWriter, system: underlyingWriter, schedule: underlyingWriter, admitted: liveSource,
	}
	mcpConfig := admittedMCPConfigWriter{writer: admittedMCPConfigAdapter{writer: underlyingWriter}, admitted: liveSource}
	mcpSchedule := admittedMCPScheduleWriter{writer: underlyingWriter, admitted: liveSource}
	b503Manager := b503session.New(b503session.TransportKey{}, 30*time.Second, nil)
	b503Dispatcher := newRawFrameDispatcherWithSourceProvider(explorerBus, liveSource, &sync.Mutex{}, b503Manager, 100*time.Millisecond)
	explorer := portal.NewHandler(portal.Options{
		GatewayVersion:         "test",
		BuildID:                "test",
		ExplorerBus:            explorerBus,
		ExplorerSourceProvider: liveSource,
	})

	assertUnavailable := func(label string) {
		t.Helper()
		if got := status.DaemonStatus().InitiatorAddress; got != "" {
			t.Fatalf("%s GraphQL source = %q, want unavailable", label, got)
		}
		if got := mcpStatus.DaemonStatus().InitiatorAddress; got == formatConfiguredInitiator(selected, false) {
			t.Fatalf("%s MCP source retained selected authority = %q", label, got)
		}
		writerCalls := underlyingWriter.calls
		if result := graphWriter.SetSystemConfig(context.Background(), "field", "value"); result.Success {
			t.Fatalf("%s GraphQL writer delegated while unavailable: %#v", label, result)
		}
		if result := mcpConfig.SetBoilerConfig(context.Background(), "field", "value"); result.Success {
			t.Fatalf("%s MCP config writer delegated while unavailable: %#v", label, result)
		}
		if result, err := mcpSchedule.SetZoneTimeProgram(context.Background(), 0, 0, nil); err != nil || result.Success {
			t.Fatalf("%s MCP schedule writer = %#v/%v, want unavailable", label, result, err)
		}
		if underlyingWriter.calls != writerCalls {
			t.Fatalf("%s writer delegates = %d->%d, want unchanged", label, writerCalls, underlyingWriter.calls)
		}
		before := len(explorerBus.snapshotSources())
		if _, err := b503Dispatcher.Invoke(context.Background(), 0x15, []byte{0x01}); !errors.Is(err, b503session.ErrTransportDown) {
			t.Fatalf("%s B503 error = %v, want transport unavailable", label, err)
		}
		if after := len(explorerBus.snapshotSources()); after != before {
			t.Fatalf("%s B503 Bus.Send calls = %d->%d", label, before, after)
		}
		recorder := httptest.NewRecorder()
		explorer.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/explorer/read/b509?target=15&addr=0028", nil))
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s Explorer status = %d, want %d", label, recorder.Code, http.StatusServiceUnavailable)
		}
		if after := len(explorerBus.snapshotSources()); after != before {
			t.Fatalf("%s Explorer Bus.Send calls = %d->%d", label, before, after)
		}
	}
	assertAvailable := func(label string) {
		t.Helper()
		if got, want := status.DaemonStatus().InitiatorAddress, formatConfiguredInitiator(selected, false); got != want {
			t.Fatalf("%s GraphQL source = %q, want %q", label, got, want)
		}
		if got, want := mcpStatus.DaemonStatus().InitiatorAddress, formatConfiguredInitiator(selected, false); got != want {
			t.Fatalf("%s MCP source = %q, want %q", label, got, want)
		}
		writerCalls := underlyingWriter.calls
		if result := graphWriter.SetSystemConfig(context.Background(), "field", "value"); !result.Success {
			t.Fatalf("%s GraphQL writer = %#v, want success", label, result)
		}
		if result := mcpConfig.SetBoilerConfig(context.Background(), "field", "value"); !result.Success {
			t.Fatalf("%s MCP config writer = %#v, want success", label, result)
		}
		if result, err := mcpSchedule.SetZoneTimeProgram(context.Background(), 0, 0, nil); err != nil || !result.Success {
			t.Fatalf("%s MCP schedule writer = %#v/%v, want success", label, result, err)
		}
		if underlyingWriter.calls != writerCalls+3 {
			t.Fatalf("%s writer delegates = %d->%d, want +3", label, writerCalls, underlyingWriter.calls)
		}
		beforeB503 := len(explorerBus.snapshotSources())
		if _, err := b503Dispatcher.Invoke(context.Background(), 0x15, []byte{0x01}); err != nil {
			t.Fatalf("%s B503 Invoke() error = %v", label, err)
		}
		b503Sources := explorerBus.snapshotSources()
		if len(b503Sources) != beforeB503+1 || b503Sources[len(b503Sources)-1] != selected {
			t.Fatalf("%s B503 sources = %v, want one exact 0x%02X", label, b503Sources, selected)
		}
		recorder := httptest.NewRecorder()
		explorer.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/explorer/read/b509?target=15&addr=0028", nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("%s Explorer status = %d, want %d body=%s", label, recorder.Code, http.StatusOK, recorder.Body.String())
		}
		sources := explorerBus.snapshotSources()
		if len(sources) == 0 || sources[len(sources)-1] != selected {
			t.Fatalf("%s Explorer sources = %v, want last 0x%02X", label, sources, selected)
		}
	}

	assertUnavailable("pending static source")
	if err := manager.StartAsync(context.Background(), primaryEBusDriverID); err != nil {
		t.Fatalf("StartAsync() error = %v", err)
	}
	select {
	case <-startEntered:
	case <-time.After(time.Second):
		t.Fatal("initial construction did not enter")
	}
	requireEBusDriverObservedState(t, manager, drivermanager.ObservedStarting, time.Second)
	assertUnavailable("STARTING")
	close(startRelease)
	first := requireEBusDriverObservedState(t, manager, drivermanager.ObservedRunning, time.Second)
	selectionReads := 0
	changingSelection := newManagedEBusSourceProvider(controller, func() (byte, bool) {
		selectionReads++
		if selectionReads == 1 {
			return selected, true
		}
		return 0, false
	})
	if source, admitted := changingSelection(); admitted || source != 0 {
		t.Fatalf("selection withdrawn during driver admission = 0x%02X/%v, want unavailable", source, admitted)
	}
	assertAvailable("RUNNING generation 1")

	if accepted := manager.ReportFailure(
		primaryEBusDriverID,
		drivermanager.Correlation{Generation: first.Generation},
		drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonDependencyUnavailable, Retryable: true}},
	); !accepted {
		t.Fatal("ReportFailure() rejected current generation")
	}
	requireEBusDriverObservedState(t, manager, drivermanager.ObservedBackoff, time.Second)
	assertUnavailable("BACKOFF")
	requireEBusDriverObservedState(t, manager, drivermanager.ObservedFailed, time.Second)
	assertUnavailable("FAILED")

	if err := manager.Start(context.Background(), primaryEBusDriverID); err != nil {
		t.Fatalf("recovery Start() error = %v", err)
	}
	second := requireEBusDriverObservedState(t, manager, drivermanager.ObservedRunning, time.Second)
	if second.Generation != first.Generation+1 {
		t.Fatalf("recovery generation = %d, want %d", second.Generation, first.Generation+1)
	}
	assertAvailable("RUNNING recovery")
	if err := manager.Stop(context.Background(), primaryEBusDriverID); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	requireEBusDriverObservedState(t, manager, drivermanager.ObservedStopped, time.Second)
	assertUnavailable("withdrawn STOPPED")
}

func TestIssue851MainWiresOneManagedSourceAuthorityToEveryEBusSurface(t *testing.T) {
	source, err := os.ReadFile("gateway_run_lifecycle.go")
	if err != nil {
		t.Fatalf("read gateway_run_lifecycle.go: %v", err)
	}
	text := string(source)
	staleWiring := []string{
		"admitted: builder.AdmittedMutationSource",
		"SetAdmittedRPCSourceProvider(builder.AdmittedMutationSource)",
		"newMCPRuntimeStatusProvider(semanticProvider, builder.AdmittedMutationSource)",
		"installVaillantB503(mcpServer, gateway, &cfg, builder.AdmittedMutationSource)",
	}
	for _, stale := range staleWiring {
		if strings.Contains(text, stale) {
			t.Errorf("source-dependent surface bypasses live DriverManager authority: %s", stale)
		}
	}
}

func TestRuntimeStatusProviderReflectsAdapterFirmwareVersion(t *testing.T) {
	semantic := graphql.NewLiveSemanticProvider()
	semantic.SetAdapterHardwareInfo(&graphql.AdapterHardwareInfo{
		FirmwareVersion:    "0x31",
		InfoSupported:      true,
		VersionResponseLen: 5,
	})

	provider := newRuntimeStatusProvider(semantic, nil)
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
// selector validates a candidate, GraphQL daemon status MUST keep the source
// unavailable, MCP may report its documented "auto" state, and neither may
// expose the cached X.
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

	provider := newRuntimeStatusProvider(nil, nil)
	daemon := provider.DaemonStatus()
	if daemon.InitiatorAddress != "" {
		t.Errorf("AD24 violation: daemon InitiatorAddress = %q; want unavailable "+
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
	mcpProvider := newMCPRuntimeStatusProvider(nil, func() (byte, bool) { return 0, false })
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
	// After validation succeeds (or an explicit source is admitted), GraphQL
	// reports only the live builder authority. Runtime-state and copied config
	// remain irrelevant to the transition.
	builder := graphql.NewBuilder(nil, nil)
	builder.SetAdmittedMutationSource(0x77)
	provider := newRuntimeStatusProvider(nil, builder.AdmittedMutationSource)
	daemon := provider.DaemonStatus()
	if daemon.InitiatorAddress != "0x77" {
		t.Errorf("post-selection daemon InitiatorAddress = %q; want \"0x77\"",
			daemon.InitiatorAddress)
	}
}

func TestMCPRuntimeStatusProviderReadsLiveAdmissionThroughEarlyHTTPHandler(t *testing.T) {
	var admitted atomic.Uint32
	provider := newMCPRuntimeStatusProvider(nil, func() (byte, bool) {
		source := admitted.Load()
		return byte(source), source != 0
	})
	server, err := mcp.NewServer(emptyMCPRegistry{}, nil)
	if err != nil {
		t.Fatalf("mcp.NewServer() error = %v", err)
	}
	server.SetStatusProvider(provider)

	initiatorAddress := func() string {
		envelope := mcpCallToolEnvelope(t, server.Handler(), "ebus.v1.runtime.status.get", `{}`)
		data, ok := envelope["data"].(map[string]any)
		if !ok {
			t.Fatalf("runtime status data = %T, want object", envelope["data"])
		}
		daemon, ok := data["daemon_status"].(map[string]any)
		if !ok {
			t.Fatalf("daemon_status = %T, want object", data["daemon_status"])
		}
		value, _ := daemon["initiator_address"].(string)
		return value
	}

	if got := initiatorAddress(); got != "auto" {
		t.Fatalf("pre-admission MCP initiator_address = %q, want auto", got)
	}
	admitted.Store(0x77)
	if got := initiatorAddress(); got != "0x77" {
		t.Fatalf("post-admission MCP initiator_address = %q, want 0x77", got)
	}
}

func TestRuntimeStatusProviderKeepsUnknownWithoutAdapterIdentity(t *testing.T) {
	semantic := graphql.NewLiveSemanticProvider()
	semantic.SetAdapterHardwareInfo(&graphql.AdapterHardwareInfo{})

	provider := newRuntimeStatusProvider(semantic, nil)
	adapter := provider.AdapterStatus()
	if adapter.Status != "unknown" {
		t.Fatalf("adapter status = %q; want unknown", adapter.Status)
	}
	if adapter.FirmwareVersion != "" {
		t.Fatalf("adapter firmwareVersion = %q; want empty", adapter.FirmwareVersion)
	}
}
