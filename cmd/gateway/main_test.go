package main

import (
	"context"
	"flag"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
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

func TestRun_ProxySingleENSAutoUsesSameSemanticSourceAsStartupConfirmation(t *testing.T) {
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

	wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		return nil, nil, nil
	}
	startDiscoveryScanLoopFn = func(_ context.Context, cfg ebusgateway.Config, _ *ebusgateway.Gateway, _ *graphql.Builder) startupScanSignals {
		resolved := resolveStartupScanSourceConfig(cfg)
		expectedStartupSource <- resolved.ScanSource
		expectedStartupAuto <- resolved.ScanSourceAuto
		return startupScanSignals{}
	}
	startVaillantSemanticPollingFn = func(_ context.Context, cfg ebusgateway.Config, _ *ebusgateway.Gateway, _ *graphql.LiveSemanticProvider, _ *graphql.BroadcastHub, _ <-chan struct{}) *vaillantSemanticPoller {
		semanticSource <- cfg.ScanSource
		semanticAuto <- cfg.ScanSourceAuto
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	startHTTPServerFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, *graphql.BroadcastHub, graphql.SemanticProvider, mcp.ScheduleWriter, *ebusgateway.BusObservabilityStore) (*http.Server, mdns.Advertiser, error) {
		cancel()
		return nil, nil, nil
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = transport.NewLoopback()
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

	var gotSemanticSource byte
	select {
	case gotSemanticSource = <-semanticSource:
	case <-time.After(2 * time.Second):
		t.Fatal("semantic poller was not initialized")
	}
	if gotSemanticSource != proxyObserveFirstStartupSource {
		t.Fatalf("semantic poller source = 0x%02X; want 0x%02X", gotSemanticSource, proxyObserveFirstStartupSource)
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
	startDiscoveryScanLoopFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder) startupScanSignals {
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
		return nil, nil
	}
	startBroadcastListenerWithReconstructorFn = func(context.Context, *router.BusEventRouter, *ebusgateway.PassiveTransactionReconstructor) (*ebusgateway.BroadcastListener, error) {
		return nil, nil
	}
	startHTTPServerFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, *graphql.BroadcastHub, graphql.SemanticProvider, mcp.ScheduleWriter, *ebusgateway.BusObservabilityStore) (*http.Server, mdns.Advertiser, error) {
		return nil, nil, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = transport.NewLoopback()
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
	case <-time.After(2 * time.Second):
		t.Fatal("semantic bootstrap did not start after semantic barrier readiness")
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
	startDiscoveryScanLoopFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder) startupScanSignals {
		return startupScanSignals{}
	}
	startVaillantSemanticPollingFn = func(_ context.Context, _ ebusgateway.Config, _ *ebusgateway.Gateway, _ *graphql.LiveSemanticProvider, _ *graphql.BroadcastHub, startupBarrier <-chan struct{}) *vaillantSemanticPoller {
		barrierObserved <- startupBarrier != nil
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	startHTTPServerFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, *graphql.BroadcastHub, graphql.SemanticProvider, mcp.ScheduleWriter, *ebusgateway.BusObservabilityStore) (*http.Server, mdns.Advertiser, error) {
		cancel()
		return nil, nil, nil
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = transport.NewLoopback()
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

	firstPassDone := make(chan struct{})
	passiveStarted := make(chan struct{}, 1)

	wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		return nil, nil, nil
	}
	startDiscoveryScanLoopFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder) startupScanSignals {
		return startupScanSignals{
			firstPassDone:          firstPassDone,
			semanticBootstrapReady: make(chan struct{}),
		}
	}
	startHTTPServerFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, *graphql.BroadcastHub, graphql.SemanticProvider, mcp.ScheduleWriter, *ebusgateway.BusObservabilityStore) (*http.Server, mdns.Advertiser, error) {
		return nil, nil, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	startPassiveTransactionReconstructor = func(context.Context, ebusgateway.Config) (*ebusgateway.PassiveTransactionReconstructor, error) {
		select {
		case passiveStarted <- struct{}{}:
		default:
		}
		cancel()
		return nil, nil
	}
	startBroadcastListenerWithReconstructorFn = func(context.Context, *router.BusEventRouter, *ebusgateway.PassiveTransactionReconstructor) (*ebusgateway.BroadcastListener, error) {
		return nil, nil
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = transport.NewLoopback()
	cfg.BroadcastListen = true
	cfg.ScanOnStart = true

	done := make(chan error, 1)
	go func() {
		done <- run(ctx, cfg)
	}()

	select {
	case <-passiveStarted:
		t.Fatal("passive observe-first started before startup scan first pass completed")
	case <-time.After(150 * time.Millisecond):
	}

	close(firstPassDone)

	select {
	case <-passiveStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("passive observe-first did not start after startup scan first pass completed")
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
