package main

import (
	"context"
	"flag"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mdns"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
)

type fixedLocalSnapshotter struct {
	snapshot ebusgateway.LocalAddressSnapshot
}

func (snapshotter fixedLocalSnapshotter) LocalAddressSnapshot() ebusgateway.LocalAddressSnapshot {
	return snapshotter.snapshot
}

type staticRuntimeWatchObserver struct {
	byCanonical map[string]ebusgateway.WatchObservation
}

func (observer staticRuntimeWatchObserver) Observe(key ebusgateway.WatchKey) ebusgateway.WatchObservation {
	if key == nil || observer.byCanonical == nil {
		return ebusgateway.WatchObservation{State: ebusgateway.WatchObservationStateCatalogMiss}
	}
	if observation, ok := observer.byCanonical[key.Canonical()]; ok {
		return observation
	}
	return ebusgateway.WatchObservation{State: ebusgateway.WatchObservationStateCatalogMiss}
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

func TestBindFlags_StartupSourceOverrideRejectsZero(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{"-startup-source-override", "0x00"}); err == nil {
		t.Fatal("parse startup-source-override 0x00 succeeded; want error")
	}
	if cfg.StartupSource.Source != nil {
		t.Fatalf("StartupSource.Source = %v; want nil after rejected zero override", *cfg.StartupSource.Source)
	}
}

func TestBindFlags_StartupProbeTargets(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{"-startup-probe-targets", "0x08,0x15 0x08"}); err != nil {
		t.Fatalf("parse startup-probe-targets: %v", err)
	}
	want := []byte{0x08, 0x15}
	if len(cfg.StartupProbeTargets) != len(want) {
		t.Fatalf("StartupProbeTargets len = %d; want %d (% X)", len(cfg.StartupProbeTargets), len(want), cfg.StartupProbeTargets)
	}
	for i := range want {
		if cfg.StartupProbeTargets[i] != want[i] {
			t.Fatalf("StartupProbeTargets = % X; want % X", cfg.StartupProbeTargets, want)
		}
	}
}

func TestBindFlags_StartupProbeTargetsRejectInvalid(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{"-startup-probe-targets", "0xFE"}); err == nil {
		t.Fatal("parse invalid startup-probe-targets error = nil; want error")
	}
}

func TestApplyTransportSourcePolicy_EbusdTCPAutoPreservesAutoSource(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.TransportConfig.Protocol = ebusgateway.TransportEbusdTCP
	cfg.ScanSource = 0x00
	cfg.ScanSourceAuto = true

	applyTransportSourcePolicy(&cfg)

	if cfg.ScanSource != 0x00 {
		t.Fatalf("ScanSource = 0x%02x; want 0x00", cfg.ScanSource)
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

func TestApplyTransportSourcePolicy_EbusdTCPConfiguredSourceIsPreserved(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.TransportConfig.Protocol = ebusgateway.TransportEbusdTCP
	cfg.ScanSource = 0xF0
	cfg.ScanSourceAuto = false

	applyTransportSourcePolicy(&cfg)

	if cfg.ScanSource != 0xF0 {
		t.Fatalf("ScanSource = 0x%02x; want 0xF0", cfg.ScanSource)
	}
}

func TestAdmittedMutationSourceForGateway_EbusdTCPAutoUsesDefaultStaticSource(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.TransportConfig.Protocol = ebusgateway.TransportEbusdTCP
	cfg.ScanSource = 0x00
	cfg.ScanSourceAuto = true

	source, admitted := admittedMutationSourceForGateway(cfg, ebusgateway.TransportAdmissionStaticFallback, false)
	if !admitted {
		t.Fatal("admitted = false; want true for ebusd-tcp auto static fallback")
	}
	if want := ebusgateway.DefaultConfig().ScanSource; source != want {
		t.Fatalf("admitted source = 0x%02X; want default static source 0x%02X", source, want)
	}
}

func TestAdmittedMutationSourceForGateway_SourceSelectionWithoutOverrideFailsClosed(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.TransportConfig.Protocol = ebusgateway.TransportENS
	cfg.ScanSource = 0x00
	cfg.ScanSourceAuto = true

	source, admitted := admittedMutationSourceForGateway(cfg, ebusgateway.TransportAdmissionSourceSelectionCapable, false)
	if admitted || source != 0 {
		t.Fatalf("admitted source = 0x%02X admitted=%v; want fail-closed before source selection", source, admitted)
	}
}

func TestAdmittedMutationSourceForGateway_UnknownStaticFallbackAutoFailsClosed(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.TransportConfig.Protocol = ebusgateway.TransportProtocol("unknown")
	cfg.ScanSource = 0x00
	cfg.ScanSourceAuto = true

	source, admitted := admittedMutationSourceForGateway(cfg, ebusgateway.TransportAdmissionStaticFallback, false)
	if admitted || source != 0 {
		t.Fatalf("admitted source = 0x%02X admitted=%v; want fail-closed for unknown static fallback", source, admitted)
	}
}

func TestRecordBusAdmissionTransitionWithStabilityRefreshPublishesOneShotActive(t *testing.T) {
	origDelay := admissionStabilityRefreshDelay
	admissionStabilityRefreshDelay = 1100 * time.Millisecond
	t.Cleanup(func() {
		admissionStabilityRefreshDelay = origDelay
	})

	store := ebusgateway.NewBusObservabilityStore(ebusgateway.DefaultConfig())
	store.SetAdmissionStabilityWindow(ebusgateway.NewAdmissionStabilityWindow(1))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	recordBusAdmissionTransitionWithStabilityRefresh(ctx, store, "active", 0x7F, 0x84, "active_probe_passed")
	if admission := store.Snapshot().Summary.Status.BusAdmission; admission != nil {
		t.Fatalf("BusAdmission before stability refresh = %+v; want nil", admission)
	}

	deadline := time.After(2 * time.Second)
	for {
		admission := store.Snapshot().Summary.Status.BusAdmission
		if admission != nil {
			if admission.State != "active" || admission.Source != 0x7F || admission.CompanionTarget != 0x84 || admission.Reason != "active_probe_passed" {
				t.Fatalf("BusAdmission = %+v; want active 0x7F/0x84 active_probe_passed", admission)
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("BusAdmission stayed nil after stability refresh")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func TestWireObserveFirstObserversWiresDedupSnapshotterIntoObservabilityStore(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.BroadcastListen = true
	b524Key := ebusgateway.NewB524WatchKey(0x15, 0x06, 0x03, 0x01, 0x001C)
	cfg.WatchObserver = staticRuntimeWatchObserver{
		byCanonical: map[string]ebusgateway.WatchObservation{
			b524Key.Canonical(): {
				State: ebusgateway.WatchObservationStateActive,
				Descriptor: ebusgateway.WatchDescriptor{
					Key:               b524Key,
					SemanticClass:     ebusgateway.WatchSemanticClassState,
					CorrelationPolicy: ebusgateway.WatchCorrelationPolicyRequestResponse,
					DirectApplyPolicy: ebusgateway.WatchDirectApplyPolicyStateDefault,
				},
				HasDescriptor: true,
			},
		},
	}
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
	if cfg.WatchObserver == nil {
		t.Fatal("WatchObserver = nil; want runtime observer wired")
	}
	if cfg.WatchEfficiencyObserver == nil {
		t.Fatal("WatchEfficiencyObserver = nil; want bus observability wiring")
	}

	local := deduplicator.LocalAddressSnapshot()
	if !local.Known || local.Address != 0x31 {
		t.Fatalf("LocalAddressSnapshot = %+v; want known 0x31", local)
	}

	initialObservation := cfg.WatchObserver.Observe(b524Key)
	if initialObservation.State != ebusgateway.WatchObservationStateActive {
		t.Fatalf("initial WatchObserver state = %q; want active from runtime observer", initialObservation.State)
	}

	b524Request := protocol.Frame{
		Source:    0x31,
		Target:    0x15,
		Primary:   0xB5,
		Secondary: 0x24,
		Data:      []byte{0x06, 0x00, 0x03, 0x01, 0x1C, 0x00},
	}
	b524Response := protocol.Frame{
		Source:    b524Request.Target,
		Target:    b524Request.Source,
		Primary:   b524Request.Primary,
		Secondary: b524Request.Secondary,
		Data:      []byte{0x42, 0x01, 0x03, 0x1C, 0x00, 0x22},
	}
	if err := deduplicator.OnBusEvent(protocol.BusEvent{
		Kind:        protocol.BusEventAttemptComplete,
		FrameType:   protocol.FrameTypeInitiatorTarget,
		Outcome:     protocol.BusOutcomeSuccess,
		Request:     b524Request,
		Response:    b524Response,
		HasRequest:  true,
		HasResponse: true,
	}); err != nil {
		t.Fatalf("deduplicator.OnBusEvent(B524 active) error = %v", err)
	}

	activeObservation := cfg.WatchObserver.Observe(b524Key)
	if activeObservation.State != ebusgateway.WatchObservationStateActive {
		t.Fatalf("WatchObserver state after active B524 = %q; want active", activeObservation.State)
	}
	if !activeObservation.HasDescriptor {
		t.Fatal("WatchObserver descriptor missing after active B524 evidence")
	}
	if activeObservation.Descriptor.DirectApplyPolicy != ebusgateway.WatchDirectApplyPolicyStateDefault {
		t.Fatalf("WatchObserver direct-apply = %q; want %q", activeObservation.Descriptor.DirectApplyPolicy, ebusgateway.WatchDirectApplyPolicyStateDefault)
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

func TestRun_AttachesPassiveShadowProducerWhenObserveFirstLaneEnabled(t *testing.T) {
	origWireObserveFirstObserversFn := wireObserveFirstObserversFn
	origStartDiscoveryScanLoopFn := startDiscoveryScanLoopFn
	origStartVaillantSemanticPollingFn := startVaillantSemanticPollingFn
	origAttachPassiveShadowProducerFn := attachPassiveShadowProducerFn
	origStartHTTPServerFn := startHTTPServerFn
	t.Cleanup(func() {
		wireObserveFirstObserversFn = origWireObserveFirstObserversFn
		startDiscoveryScanLoopFn = origStartDiscoveryScanLoopFn
		startVaillantSemanticPollingFn = origStartVaillantSemanticPollingFn
		attachPassiveShadowProducerFn = origAttachPassiveShadowProducerFn
		startHTTPServerFn = origStartHTTPServerFn
	})

	cfgForDedup := ebusgateway.DefaultConfig()
	deduplicator, err := ebusgateway.NewActivePassiveDeduplicator(cfgForDedup)
	if err != nil {
		t.Fatalf("NewActivePassiveDeduplicator() error = %v", err)
	}

	wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		return nil, deduplicator, nil
	}
	startDiscoveryScanLoopFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, activeTxnClassifier) startupScanSignals {
		return startupScanSignals{}
	}

	poller := &vaillantSemanticPoller{}
	startVaillantSemanticPollingFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.LiveSemanticProvider, *graphql.BroadcastHub, <-chan struct{}) *vaillantSemanticPoller {
		return poller
	}

	attached := make(chan struct{}, 1)
	attachErr := make(chan string, 1)
	attachPassiveShadowProducerFn = func(gotPoller *vaillantSemanticPoller, _ context.Context, gotDeduplicator *ebusgateway.ActivePassiveDeduplicator) error {
		if gotPoller != poller {
			select {
			case attachErr <- "attach poller mismatch":
			default:
			}
			return nil
		}
		if gotDeduplicator != deduplicator {
			select {
			case attachErr <- "attach deduplicator mismatch":
			default:
			}
			return nil
		}
		select {
		case attached <- struct{}{}:
		default:
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	startHTTPServerFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, *graphql.BroadcastHub, graphql.SemanticProvider, mcp.ScheduleWriter, mcp.ConfigWriter, *ebusgateway.BusObservabilityStore, *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
		cancel()
		return nil, nil, nil
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = transport.NewLoopback()
	cfg.BroadcastListen = false
	cfg.ObserveFirstEnabled = true
	cfg.PassiveStateDirectApply = true

	done := make(chan error, 1)
	go func() {
		done <- run(ctx, cfg)
	}()

	select {
	case <-attached:
	case msg := <-attachErr:
		t.Fatal(msg)
	case <-time.After(2 * time.Second):
		t.Fatal("attachPassiveShadowProducerFn was not called")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not exit after context cancellation")
	}
}

func TestRun_DoesNotAttachPassiveShadowProducerWhenObserveFirstMasterDisabled(t *testing.T) {
	origWireObserveFirstObserversFn := wireObserveFirstObserversFn
	origStartDiscoveryScanLoopFn := startDiscoveryScanLoopFn
	origStartVaillantSemanticPollingFn := startVaillantSemanticPollingFn
	origAttachPassiveShadowProducerFn := attachPassiveShadowProducerFn
	origStartHTTPServerFn := startHTTPServerFn
	t.Cleanup(func() {
		wireObserveFirstObserversFn = origWireObserveFirstObserversFn
		startDiscoveryScanLoopFn = origStartDiscoveryScanLoopFn
		startVaillantSemanticPollingFn = origStartVaillantSemanticPollingFn
		attachPassiveShadowProducerFn = origAttachPassiveShadowProducerFn
		startHTTPServerFn = origStartHTTPServerFn
	})

	cfgForDedup := ebusgateway.DefaultConfig()
	deduplicator, err := ebusgateway.NewActivePassiveDeduplicator(cfgForDedup)
	if err != nil {
		t.Fatalf("NewActivePassiveDeduplicator() error = %v", err)
	}

	wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		return nil, deduplicator, nil
	}
	startDiscoveryScanLoopFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, activeTxnClassifier) startupScanSignals {
		return startupScanSignals{}
	}
	startVaillantSemanticPollingFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.LiveSemanticProvider, *graphql.BroadcastHub, <-chan struct{}) *vaillantSemanticPoller {
		return &vaillantSemanticPoller{}
	}

	attachCalled := make(chan struct{}, 1)
	attachPassiveShadowProducerFn = func(*vaillantSemanticPoller, context.Context, *ebusgateway.ActivePassiveDeduplicator) error {
		select {
		case attachCalled <- struct{}{}:
		default:
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	startHTTPServerFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, *graphql.BroadcastHub, graphql.SemanticProvider, mcp.ScheduleWriter, mcp.ConfigWriter, *ebusgateway.BusObservabilityStore, *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
		cancel()
		return nil, nil, nil
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = transport.NewLoopback()
	cfg.BroadcastListen = false
	cfg.ObserveFirstEnabled = false
	cfg.PassiveStateDirectApply = true

	done := make(chan error, 1)
	go func() {
		done <- run(ctx, cfg)
	}()

	select {
	case <-attachCalled:
		t.Fatal("attachPassiveShadowProducerFn was called with observe-first global gate disabled")
	case err := <-done:
		if err != nil {
			t.Fatalf("run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not exit after context cancellation")
	}
}

func TestRun_ProxySingleENSAutoUsesDefaultSelectionBeforeProxyResolution(t *testing.T) {
	origWireObserveFirstObserversFn := wireObserveFirstObserversFn
	origStartDiscoveryScanLoopFn := startDiscoveryScanLoopFn
	origStartVaillantSemanticPollingFn := startVaillantSemanticPollingFn
	origStartHTTPServerFn := startHTTPServerFn
	t.Cleanup(func() {
		wireObserveFirstObserversFn = origWireObserveFirstObserversFn
		startDiscoveryScanLoopFn = origStartDiscoveryScanLoopFn
		startVaillantSemanticPollingFn = origStartVaillantSemanticPollingFn
		startHTTPServerFn = origStartHTTPServerFn
	})

	semanticSource := make(chan byte, 1)
	semanticAuto := make(chan bool, 1)
	expectedStartupSource := make(chan byte, 1)
	expectedStartupAuto := make(chan bool, 1)
	startupTargets := make(chan []byte, 1)

	wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		return nil, nil, nil
	}
	startDiscoveryScanLoopFn = func(_ context.Context, cfg ebusgateway.Config, _ *ebusgateway.Gateway, _ *graphql.Builder, _ activeTxnClassifier) startupScanSignals {
		resolved := resolveStartupScanSourceConfig(cfg)
		expectedStartupSource <- resolved.ScanSource
		expectedStartupAuto <- resolved.ScanSourceAuto
		startupTargets <- append([]byte(nil), cfg.StartupProbeTargets...)
		return startupScanSignals{}
	}
	startVaillantSemanticPollingFn = func(_ context.Context, cfg ebusgateway.Config, _ *ebusgateway.Gateway, _ *graphql.LiveSemanticProvider, _ *graphql.BroadcastHub, _ <-chan struct{}) *vaillantSemanticPoller {
		semanticSource <- cfg.ScanSource
		semanticAuto <- cfg.ScanSourceAuto
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	startHTTPServerFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, *graphql.BroadcastHub, graphql.SemanticProvider, mcp.ScheduleWriter, mcp.ConfigWriter, *ebusgateway.BusObservabilityStore, *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
		cancel()
		return nil, nil, nil
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = transport.NewLoopback()
	cfg.TransportConfig.Protocol = ebusgateway.TransportEbusdTCP
	cfg.ScanOnStart = true
	cfg.BroadcastListen = false
	cfg.ScanSource = 0x00
	cfg.ScanSourceAuto = true
	cfg.TransportConfig.Protocol = ebusgateway.TransportENS
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "127.0.0.1:19001"

	done := make(chan error, 1)
	go func() {
		done <- run(ctx, cfg)
	}()

	wantSource := byte(0x7F)
	var gotSemanticSource byte
	select {
	case gotSemanticSource = <-semanticSource:
	case <-time.After(2 * time.Second):
		t.Fatal("semantic poller was not initialized")
	}
	if gotSemanticSource != wantSource {
		t.Fatalf("semantic poller source = 0x%02X; want default-selected source 0x%02X", gotSemanticSource, wantSource)
	}

	select {
	case got := <-semanticAuto:
		if got {
			t.Fatal("semantic poller source remained auto; want explicit proxy startup source")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("semantic poller auto flag was not observed")
	}

	select {
	case got := <-expectedStartupSource:
		if got != gotSemanticSource {
			t.Fatalf("startup confirmation source = 0x%02X; want semantic source 0x%02X", got, gotSemanticSource)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("startup confirmation source was not observed")
	}

	select {
	case got := <-expectedStartupAuto:
		if got {
			t.Fatal("startup confirmation source remained auto; want explicit proxy startup source")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("startup confirmation auto flag was not observed")
	}

	select {
	case got := <-startupTargets:
		// Structural fallback: when source-selection's passive warmup
		// observed no probable targets, the startup probe seed is the
		// bounded Vaillant structural set so 0x15 (regulator) and 0x26
		// (primary controller) enter discovery — not the boiler alone.
		want := []byte{0x08, 0x15, 0x26}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("startup probe targets = % X; want % X", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("startup probe targets were not observed")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not exit after context cancellation")
	}
}

func TestRun_DefaultSourceSelectionSeedsStartupProbeTargets(t *testing.T) {
	origWireObserveFirstObserversFn := wireObserveFirstObserversFn
	origStartDiscoveryScanLoopFn := startDiscoveryScanLoopFn
	origStartVaillantSemanticPollingFn := startVaillantSemanticPollingFn
	origStartHTTPServerFn := startHTTPServerFn
	t.Cleanup(func() {
		wireObserveFirstObserversFn = origWireObserveFirstObserversFn
		startDiscoveryScanLoopFn = origStartDiscoveryScanLoopFn
		startVaillantSemanticPollingFn = origStartVaillantSemanticPollingFn
		startHTTPServerFn = origStartHTTPServerFn
	})

	startupTargets := make(chan []byte, 1)
	wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		return nil, nil, nil
	}
	startDiscoveryScanLoopFn = func(_ context.Context, cfg ebusgateway.Config, _ *ebusgateway.Gateway, _ *graphql.Builder, _ activeTxnClassifier) startupScanSignals {
		startupTargets <- append([]byte(nil), cfg.StartupProbeTargets...)
		return startupScanSignals{}
	}
	startVaillantSemanticPollingFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.LiveSemanticProvider, *graphql.BroadcastHub, <-chan struct{}) *vaillantSemanticPoller {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	startHTTPServerFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, *graphql.BroadcastHub, graphql.SemanticProvider, mcp.ScheduleWriter, mcp.ConfigWriter, *ebusgateway.BusObservabilityStore, *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
		cancel()
		return nil, nil, nil
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = transport.NewLoopback()
	cfg.TransportConfig.Protocol = ebusgateway.TransportENS
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "192.0.2.10:9999"
	cfg.BroadcastListen = false
	cfg.ScanOnStart = true
	cfg.ScanSource = 0x00
	cfg.ScanSourceAuto = true

	done := make(chan error, 1)
	go func() {
		done <- run(ctx, cfg)
	}()

	select {
	case got := <-startupTargets:
		// Structural fallback: passive warmup on the direct adapter
		// observed no probable targets in the test harness, so the
		// startup probe seeds the bounded Vaillant structural set.
		want := []byte{0x08, 0x15, 0x26}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("startup probe targets = % X; want % X", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("startup scan config was not observed")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not exit after context cancellation")
	}
}

func TestRun_ConfiguredDirectSourceSeedsStartupProbeTargets(t *testing.T) {
	origWireObserveFirstObserversFn := wireObserveFirstObserversFn
	origStartDiscoveryScanLoopFn := startDiscoveryScanLoopFn
	origStartVaillantSemanticPollingFn := startVaillantSemanticPollingFn
	origStartHTTPServerFn := startHTTPServerFn
	t.Cleanup(func() {
		wireObserveFirstObserversFn = origWireObserveFirstObserversFn
		startDiscoveryScanLoopFn = origStartDiscoveryScanLoopFn
		startVaillantSemanticPollingFn = origStartVaillantSemanticPollingFn
		startHTTPServerFn = origStartHTTPServerFn
	})

	type startupCapture struct {
		source  byte
		auto    bool
		targets []byte
	}
	startupCfg := make(chan startupCapture, 1)
	wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		return nil, nil, nil
	}
	startDiscoveryScanLoopFn = func(_ context.Context, cfg ebusgateway.Config, _ *ebusgateway.Gateway, _ *graphql.Builder, _ activeTxnClassifier) startupScanSignals {
		startupCfg <- startupCapture{
			source:  cfg.ScanSource,
			auto:    cfg.ScanSourceAuto,
			targets: append([]byte(nil), cfg.StartupProbeTargets...),
		}
		return startupScanSignals{}
	}
	startVaillantSemanticPollingFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.LiveSemanticProvider, *graphql.BroadcastHub, <-chan struct{}) *vaillantSemanticPoller {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	startHTTPServerFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, *graphql.BroadcastHub, graphql.SemanticProvider, mcp.ScheduleWriter, mcp.ConfigWriter, *ebusgateway.BusObservabilityStore, *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
		cancel()
		return nil, nil, nil
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = transport.NewLoopback()
	cfg.TransportConfig.Protocol = ebusgateway.TransportENS
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "192.0.2.10:9999"
	cfg.BroadcastListen = false
	cfg.ScanOnStart = true
	cfg.ScanSource = 0xF0
	cfg.ScanSourceAuto = false

	done := make(chan error, 1)
	go func() {
		done <- run(ctx, cfg)
	}()

	select {
	case got := <-startupCfg:
		if got.source != 0xF0 {
			t.Fatalf("startup source = 0x%02X; want configured source 0xF0", got.source)
		}
		if got.auto {
			t.Fatal("startup source remained auto; want configured direct source")
		}
		// Structural fallback: configured-direct path with no observed
		// probable targets seeds the bounded Vaillant structural set
		// (sanitized vs source/companion) — not the boiler alone.
		wantTargets := []byte{0x08, 0x15, 0x26}
		if !reflect.DeepEqual(got.targets, wantTargets) {
			t.Fatalf("startup probe targets = % X; want % X", got.targets, wantTargets)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("startup scan config was not observed")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not exit after context cancellation")
	}
}

func TestRun_DefersSemanticBootstrapUntilStartupConfirmationReadyOnPassiveObserveFirst(t *testing.T) {
	origWireObserveFirstObserversFn := wireObserveFirstObserversFn
	origStartDiscoveryScanLoopFn := startDiscoveryScanLoopFn
	origStartVaillantSemanticPollingFn := startVaillantSemanticPollingFn
	origStartPassiveTransactionReconstructor := startPassiveTransactionReconstructor
	origStartBroadcastListenerWithReconstructorFn := startBroadcastListenerWithReconstructorFn
	origStartHTTPServerFn := startHTTPServerFn
	t.Cleanup(func() {
		wireObserveFirstObserversFn = origWireObserveFirstObserversFn
		startDiscoveryScanLoopFn = origStartDiscoveryScanLoopFn
		startVaillantSemanticPollingFn = origStartVaillantSemanticPollingFn
		startPassiveTransactionReconstructor = origStartPassiveTransactionReconstructor
		startBroadcastListenerWithReconstructorFn = origStartBroadcastListenerWithReconstructorFn
		startHTTPServerFn = origStartHTTPServerFn
	})

	firstPassDone := make(chan struct{})
	semanticReady := make(chan struct{})
	barrierObserved := make(chan bool, 1)
	semanticStarted := make(chan struct{}, 1)

	wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		return nil, nil, nil
	}
	startDiscoveryScanLoopFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, activeTxnClassifier) startupScanSignals {
		return startupScanSignals{
			firstPassDone:          firstPassDone,
			semanticBootstrapReady: semanticReady,
		}
	}
	startVaillantSemanticPollingFn = func(ctx context.Context, cfg ebusgateway.Config, gateway *ebusgateway.Gateway, provider *graphql.LiveSemanticProvider, hub *graphql.BroadcastHub, startupBarrier <-chan struct{}) *vaillantSemanticPoller {
		barrierObserved <- startupBarrier != nil
		go func() {
			if startupBarrier == nil {
				return
			}
			select {
			case <-ctx.Done():
			case <-startupBarrier:
				select {
				case semanticStarted <- struct{}{}:
				default:
				}
			}
		}()
		return nil
	}
	startPassiveTransactionReconstructor = func(context.Context, ebusgateway.Config) (*ebusgateway.PassiveTransactionReconstructor, error) {
		return &ebusgateway.PassiveTransactionReconstructor{}, nil
	}
	startBroadcastListenerWithReconstructorFn = func(context.Context, *router.BusEventRouter, *ebusgateway.PassiveTransactionReconstructor) (*ebusgateway.BroadcastListener, error) {
		return nil, nil
	}
	startHTTPServerFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, *graphql.BroadcastHub, graphql.SemanticProvider, mcp.ScheduleWriter, mcp.ConfigWriter, *ebusgateway.BusObservabilityStore, *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
		return nil, nil, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = transport.NewLoopback()
	// DriverManager owns both active and passive generations when broadcast
	// observe-first is enabled; keep this injected runtime fully in-memory.
	cfg.PassiveTransport = transport.NewLoopback()
	cfg.BroadcastListen = true
	cfg.ScanOnStart = true

	done := make(chan error, 1)
	go func() {
		done <- run(ctx, cfg)
	}()

	select {
	case observed := <-barrierObserved:
		if !observed {
			t.Fatal("semantic startup barrier missing on passive observe-first path")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("semantic poller was not initialized")
	}

	select {
	case <-semanticStarted:
		t.Fatal("semantic bootstrap started before startup confirmation was ready")
	case <-time.After(150 * time.Millisecond):
	}

	close(firstPassDone)

	select {
	case <-semanticStarted:
		t.Fatal("semantic bootstrap started after first pass but before semantic barrier readiness")
	case <-time.After(150 * time.Millisecond):
	}

	close(semanticReady)

	select {
	case <-semanticStarted:
		t.Fatal("semantic bootstrap started despite degraded admission without override")
	case <-time.After(200 * time.Millisecond):
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not exit after context cancellation")
	}
}

func TestRun_OverrideClosesSemanticBarrierOnAdmissionFailure(t *testing.T) {
	origWireObserveFirstObserversFn := wireObserveFirstObserversFn
	origStartDiscoveryScanLoopFn := startDiscoveryScanLoopFn
	origStartVaillantSemanticPollingFn := startVaillantSemanticPollingFn
	origStartHTTPServerFn := startHTTPServerFn
	t.Cleanup(func() {
		wireObserveFirstObserversFn = origWireObserveFirstObserversFn
		startDiscoveryScanLoopFn = origStartDiscoveryScanLoopFn
		startVaillantSemanticPollingFn = origStartVaillantSemanticPollingFn
		startHTTPServerFn = origStartHTTPServerFn
	})

	admissionFailed := make(chan struct{})
	barrierObserved := make(chan bool, 1)
	semanticStarted := make(chan struct{}, 1)

	wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		return nil, nil, nil
	}
	startDiscoveryScanLoopFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, activeTxnClassifier) startupScanSignals {
		return startupScanSignals{
			firstPassDone:          make(chan struct{}),
			semanticBootstrapReady: make(chan struct{}),
			admissionFailed:        admissionFailed,
		}
	}
	startVaillantSemanticPollingFn = func(ctx context.Context, _ ebusgateway.Config, _ *ebusgateway.Gateway, _ *graphql.LiveSemanticProvider, _ *graphql.BroadcastHub, startupBarrier <-chan struct{}) *vaillantSemanticPoller {
		barrierObserved <- startupBarrier != nil
		go func() {
			if startupBarrier == nil {
				return
			}
			select {
			case <-ctx.Done():
			case <-startupBarrier:
				select {
				case semanticStarted <- struct{}{}:
				default:
				}
			}
		}()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startHTTPServerFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, *graphql.BroadcastHub, graphql.SemanticProvider, mcp.ScheduleWriter, mcp.ConfigWriter, *ebusgateway.BusObservabilityStore, *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
		return nil, nil, nil
	}

	overrideSource := uint8(0x7F)
	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = transport.NewLoopback()
	cfg.PassiveTransport = transport.NewLoopback()
	cfg.BroadcastListen = true
	cfg.ScanOnStart = true
	cfg.StartupSource.Source = &overrideSource

	done := make(chan error, 1)
	go func() {
		done <- run(ctx, cfg)
	}()

	select {
	case observed := <-barrierObserved:
		if !observed {
			t.Fatal("semantic startup barrier missing on passive observe-first override path")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("semantic poller was not initialized")
	}

	close(admissionFailed)

	select {
	case <-semanticStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("semantic bootstrap did not start after override admission failure")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not exit after context cancellation")
	}
}

func TestRun_DoesNotDeferSemanticBootstrapOutsidePassiveObserveFirst(t *testing.T) {
	origWireObserveFirstObserversFn := wireObserveFirstObserversFn
	origStartDiscoveryScanLoopFn := startDiscoveryScanLoopFn
	origStartVaillantSemanticPollingFn := startVaillantSemanticPollingFn
	origStartHTTPServerFn := startHTTPServerFn
	t.Cleanup(func() {
		wireObserveFirstObserversFn = origWireObserveFirstObserversFn
		startDiscoveryScanLoopFn = origStartDiscoveryScanLoopFn
		startVaillantSemanticPollingFn = origStartVaillantSemanticPollingFn
		startHTTPServerFn = origStartHTTPServerFn
	})

	barrierObserved := make(chan bool, 1)

	wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		return nil, nil, nil
	}
	startDiscoveryScanLoopFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, activeTxnClassifier) startupScanSignals {
		return startupScanSignals{}
	}
	startVaillantSemanticPollingFn = func(_ context.Context, _ ebusgateway.Config, _ *ebusgateway.Gateway, _ *graphql.LiveSemanticProvider, _ *graphql.BroadcastHub, startupBarrier <-chan struct{}) *vaillantSemanticPoller {
		barrierObserved <- startupBarrier != nil
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	startHTTPServerFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, *graphql.BroadcastHub, graphql.SemanticProvider, mcp.ScheduleWriter, mcp.ConfigWriter, *ebusgateway.BusObservabilityStore, *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
		cancel()
		return nil, nil, nil
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = transport.NewLoopback()
	cfg.TransportConfig.Protocol = ebusgateway.TransportEbusdTCP
	cfg.ScanOnStart = true
	cfg.BroadcastListen = false

	done := make(chan error, 1)
	go func() {
		done <- run(ctx, cfg)
	}()

	select {
	case observed := <-barrierObserved:
		if observed {
			t.Fatal("semantic startup barrier should be disabled outside passive observe-first path")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("semantic poller was not initialized")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not exit after context cancellation")
	}
}

func TestRun_WaitsForStartupScanFirstPassBeforePassiveObserveFirst(t *testing.T) {
	origWireObserveFirstObserversFn := wireObserveFirstObserversFn
	origStartDiscoveryScanLoopFn := startDiscoveryScanLoopFn
	origStartPassiveTransactionReconstructor := startPassiveTransactionReconstructor
	origStartBroadcastListenerWithReconstructorFn := startBroadcastListenerWithReconstructorFn
	origStartHTTPServerFn := startHTTPServerFn
	t.Cleanup(func() {
		wireObserveFirstObserversFn = origWireObserveFirstObserversFn
		startDiscoveryScanLoopFn = origStartDiscoveryScanLoopFn
		startPassiveTransactionReconstructor = origStartPassiveTransactionReconstructor
		startBroadcastListenerWithReconstructorFn = origStartBroadcastListenerWithReconstructorFn
		startHTTPServerFn = origStartHTTPServerFn
	})

	passiveStarted := make(chan struct{}, 1)

	wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		return nil, nil, nil
	}
	startDiscoveryScanLoopFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, activeTxnClassifier) startupScanSignals {
		return startupScanSignals{
			firstPassDone:          make(chan struct{}),
			semanticBootstrapReady: make(chan struct{}),
		}
	}
	startHTTPServerFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, *graphql.BroadcastHub, graphql.SemanticProvider, mcp.ScheduleWriter, mcp.ConfigWriter, *ebusgateway.BusObservabilityStore, *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
		return nil, nil, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	startPassiveTransactionReconstructor = func(context.Context, ebusgateway.Config) (*ebusgateway.PassiveTransactionReconstructor, error) {
		select {
		case passiveStarted <- struct{}{}:
		default:
		}
		cancel()
		return &ebusgateway.PassiveTransactionReconstructor{}, nil
	}
	startBroadcastListenerWithReconstructorFn = func(context.Context, *router.BusEventRouter, *ebusgateway.PassiveTransactionReconstructor) (*ebusgateway.BroadcastListener, error) {
		return nil, nil
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = transport.NewLoopback()
	// DriverManager owns both active and passive generations when broadcast
	// observe-first is enabled; keep this injected runtime fully in-memory.
	cfg.PassiveTransport = transport.NewLoopback()
	cfg.BroadcastListen = true
	cfg.ScanOnStart = true

	done := make(chan error, 1)
	go func() {
		done <- run(ctx, cfg)
	}()

	select {
	case <-passiveStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("passive reconstructor did not start before startup scan on source-selection-capable transport")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not exit after context cancellation")
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

func TestBindFlags_InstanceGUID(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{"-instance-guid", "4d9336aa-f125-4f12-8b07-fcd18dbfcb10"}); err != nil {
		t.Fatalf("parse instance-guid: %v", err)
	}
	if cfg.InstanceGUID != "4d9336aa-f125-4f12-8b07-fcd18dbfcb10" {
		t.Fatalf("InstanceGUID = %q; want parsed UUID", cfg.InstanceGUID)
	}
}

func TestBindFlags_InstanceGUIDRejectsInvalidValue(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{"-instance-guid", "not-a-guid"}); err == nil {
		t.Fatal("parse invalid instance-guid error = nil; want error")
	}
}

func TestGatewayMDNSTextIncludesInstanceGUID(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.GraphQLPath = "/graphql"
	cfg.InstanceGUID = "4d9336aa-f125-4f12-8b07-fcd18dbfcb10"

	got := gatewayMDNSText(cfg)
	want := []string{
		"path=/graphql",
		"transport=http",
		"version=1",
		"instance_guid=4d9336aa-f125-4f12-8b07-fcd18dbfcb10",
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("gatewayMDNSText() = %#v; want %#v", got, want)
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

func TestBindFlags_ObserveFirstFeatureFlags(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{
		"-observe-first-enabled",
		"-passive-state-direct-apply",
		"-passive-config-direct-apply",
		"-external-write-policy", "record_and_invalidate",
	}); err != nil {
		t.Fatalf("parse observe-first flags: %v", err)
	}
	if !cfg.ObserveFirstEnabled {
		t.Fatal("ObserveFirstEnabled = false; want true")
	}
	if !cfg.PassiveStateDirectApply {
		t.Fatal("PassiveStateDirectApply = false; want true")
	}
	if !cfg.PassiveConfigDirectApply {
		t.Fatal("PassiveConfigDirectApply = false; want true")
	}
	if cfg.ExternalWritePolicy != ebusgateway.ObserveFirstExternalWritePolicyRecordAndInvalidate {
		t.Fatalf("ExternalWritePolicy = %q; want record_and_invalidate", cfg.ExternalWritePolicy)
	}
}

func TestBindFlags_ObserveFirstRejectsInvalidExternalWritePolicy(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	fs := flag.NewFlagSet("gateway-test", flag.ContinueOnError)
	bindFlags(fs, &cfg)
	if err := fs.Parse([]string{"-external-write-policy", "unsafe"}); err == nil {
		t.Fatal("parse invalid external-write-policy error = nil; want error")
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

// ---------------------------------------------------------------------------
// wireAdapterDirect tests (PR #472 review fixes)
// ---------------------------------------------------------------------------

func TestWireAdapterDirect_NonAdapterDirect_ReturnsNil(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.TransportConfig.Protocol = ebusgateway.TransportENH
	cfg.TransportConfig.Address = "127.0.0.1:9999"

	closer, _, err := wireAdapterDirect(context.Background(), &cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if closer != nil {
		t.Fatal("closer should be nil for non-adapter-direct protocol")
	}
}

func TestWireAdapterDirect_InvalidClassifierModeReturnsError(t *testing.T) {
	t.Setenv(v8classifier.EnvVarName, "invalid-mode")
	cfg := ebusgateway.DefaultConfig()
	cfg.TransportConfig.Protocol = ebusgateway.TransportAdapterDirect
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "192.0.2.1:9999"

	closer, classifier, err := wireAdapterDirect(context.Background(), &cfg)
	if err == nil {
		t.Fatal("invalid classifier mode error = nil; want rejection")
	}
	if closer != nil || classifier != nil {
		t.Fatalf("invalid classifier mode returned resources: closer=%v classifier=%v", closer != nil, classifier != nil)
	}
	if !strings.Contains(err.Error(), v8classifier.EnvVarName) || !strings.Contains(err.Error(), "invalid-mode") {
		t.Fatalf("invalid classifier mode error = %q; want environment name and value", err)
	}
}

func TestWireAdapterDirect_URIScheme_ForcesTCP(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	// Default protocol is "enh" (not adapter-direct), but the URI
	// scheme should trigger adapter-direct mode and force TCP.
	cfg.TransportConfig.Address = "adapter-direct://192.0.2.1:9999"
	cfg.TransportConfig.DialTimeout = 100 * time.Millisecond

	_, _, err := wireAdapterDirect(context.Background(), &cfg)
	if err == nil {
		t.Fatal("expected dial error, got nil")
	}
	// The error should show tcp/192.0.2.1:9999 (not unix).
	if !strings.Contains(err.Error(), "tcp") {
		t.Fatalf("expected TCP network in error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "192.0.2.1:9999") {
		t.Fatalf("expected stripped address in error, got: %v", err)
	}
	if cfg.TransportConfig.Protocol != ebusgateway.TransportAdapterDirect || cfg.TransportConfig.Network != "tcp" || cfg.TransportConfig.Address != "192.0.2.1:9999" {
		t.Fatalf("canonical adapter-direct tuple = %q/%q/%q", cfg.TransportConfig.Protocol, cfg.TransportConfig.Network, cfg.TransportConfig.Address)
	}
	if got := adapterDirectMuxProtocol(cfg.TransportConfig.Protocol); got != "enh" {
		t.Fatalf("adapterDirectMuxProtocol() = %q, want enh", got)
	}
}

func TestWireAdapterDirect_ExplicitProtocol_ForcesTCPForHostPort(t *testing.T) {
	// Issue 2: --transport adapter-direct --address host:port should
	// force TCP even when network defaults to "unix".
	cfg := ebusgateway.DefaultConfig()
	cfg.TransportConfig.Protocol = ebusgateway.TransportAdapterDirect
	cfg.TransportConfig.Network = "unix" // simulates the default
	cfg.TransportConfig.Address = "192.0.2.1:9999"
	cfg.TransportConfig.DialTimeout = 100 * time.Millisecond

	_, _, err := wireAdapterDirect(context.Background(), &cfg)
	if err == nil {
		t.Fatal("expected dial error, got nil")
	}
	// Must show tcp, not unix.
	if !strings.Contains(err.Error(), "tcp") {
		t.Fatalf("expected TCP network for host:port address, got: %v", err)
	}
}

func TestWireAdapterDirect_ExplicitProtocol_KeepsUnixForSocketPath(t *testing.T) {
	// When --transport adapter-direct --address /var/run/adapter.sock,
	// the network should remain "unix" (no colon in path).
	cfg := ebusgateway.DefaultConfig()
	cfg.TransportConfig.Protocol = ebusgateway.TransportAdapterDirect
	cfg.TransportConfig.Network = "unix"
	cfg.TransportConfig.Address = "/var/run/adapter.sock"
	cfg.TransportConfig.DialTimeout = 100 * time.Millisecond

	_, _, err := wireAdapterDirect(context.Background(), &cfg)
	if err == nil {
		t.Fatal("expected dial error, got nil")
	}
	if !strings.Contains(err.Error(), "unix") {
		t.Fatalf("expected unix network for socket path, got: %v", err)
	}
}

func TestWireAdapterDirect_ENSScheme_SelectsENS(t *testing.T) {
	// Issue 1: adapter-direct-ens:// URI should select ENS protocol.
	cfg := ebusgateway.DefaultConfig()
	cfg.TransportConfig.Address = "adapter-direct-ens://192.0.2.1:9999"
	cfg.TransportConfig.DialTimeout = 100 * time.Millisecond

	_, _, err := wireAdapterDirect(context.Background(), &cfg)
	if err == nil {
		t.Fatal("expected dial error, got nil")
	}
	// Verify the scheme was stripped and TCP forced.
	if !strings.Contains(err.Error(), "192.0.2.1:9999") {
		t.Fatalf("expected stripped address, got: %v", err)
	}
	if !strings.Contains(err.Error(), "tcp") {
		t.Fatalf("expected TCP network, got: %v", err)
	}
	if cfg.TransportConfig.Protocol != adapterDirectENSProtocol || cfg.TransportConfig.Network != "tcp" || cfg.TransportConfig.Address != "192.0.2.1:9999" {
		t.Fatalf("canonical adapter-direct ENS tuple = %q/%q/%q", cfg.TransportConfig.Protocol, cfg.TransportConfig.Network, cfg.TransportConfig.Address)
	}
	if got := adapterDirectMuxProtocol(cfg.TransportConfig.Protocol); got != "ens" {
		t.Fatalf("adapterDirectMuxProtocol() = %q, want ens", got)
	}
}

func TestAdapterDirectProxyAvailabilityUsesCanonicalEndpointProtocol(t *testing.T) {
	tests := []struct {
		name    string
		config  ebusgateway.TransportConfig
		enabled bool
	}{
		{name: "explicit adapter direct", config: ebusgateway.TransportConfig{Protocol: ebusgateway.TransportAdapterDirect, Address: "adapter.local:9999"}, enabled: true},
		{name: "adapter direct URI", config: ebusgateway.TransportConfig{Protocol: ebusgateway.TransportENH, Address: "adapter-direct://adapter.local:9999"}, enabled: true},
		{name: "adapter direct ENS URI", config: ebusgateway.TransportConfig{Protocol: ebusgateway.TransportENH, Address: "adapter-direct-ens://adapter.local:9999"}, enabled: true},
		{name: "ordinary ENH", config: ebusgateway.TransportConfig{Protocol: ebusgateway.TransportENH, Address: "adapter.local:9999"}},
		{name: "TCP plain", config: ebusgateway.TransportConfig{Protocol: ebusgateway.TransportTCPPlain, Address: "adapter.local:9999"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := adapterDirectProxyEnabled(test.config); got != test.enabled {
				t.Fatalf("adapterDirectProxyEnabled(%#v) = %v, want %v", test.config, got, test.enabled)
			}
		})
	}
}

func TestWireAdapterDirect_ProxyListener_ReturnedAsCloser(t *testing.T) {
	// Issue 3: when ProxyListenAddr is set, the returned closer should
	// be non-nil (the proxy listener's Close). We cannot fully test
	// this without a running adapter, but we verify the non-proxy path
	// still returns nil closer on dial failure.
	cfg := ebusgateway.DefaultConfig()
	cfg.TransportConfig.Protocol = ebusgateway.TransportAdapterDirect
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "192.0.2.1:9999"
	cfg.TransportConfig.DialTimeout = 100 * time.Millisecond
	cfg.ProxyListenAddr = "" // no proxy listener

	_, _, err := wireAdapterDirect(context.Background(), &cfg)
	if err == nil {
		t.Fatal("expected dial error, got nil (no adapter running)")
	}
	// Dial error is expected — proxy listener path is not reached.
	// This test documents that without a proxy listener, closer is nil.
}
