package main

import (
	"context"
	"errors"
	"net/http"
	"net/netip"
	"path/filepath"
	"sync"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mdns"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
)

type issue809AdminStub struct{}

func (issue809AdminStub) Snapshot(context.Context, eebusruntime.AdminSnapshotRequestV1) (eebusruntime.AdminSnapshotV1, *eebusruntime.AdminErrorV1) {
	panic("unexpected Snapshot")
}
func (issue809AdminStub) OpenPairingWindow(context.Context, eebusruntime.OpenPairingWindowRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	panic("unexpected OpenPairingWindow")
}
func (issue809AdminStub) ClosePairingWindow(context.Context, eebusruntime.ClosePairingWindowRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	panic("unexpected ClosePairingWindow")
}
func (issue809AdminStub) Select(context.Context, eebusruntime.SelectRequestV1) (eebusruntime.AdminSelectionResultV1, *eebusruntime.AdminErrorV1) {
	panic("unexpected Select")
}
func (issue809AdminStub) Connect(context.Context, eebusruntime.ConnectRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	panic("unexpected Connect")
}
func (issue809AdminStub) Confirm(context.Context, eebusruntime.ConfirmRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	panic("unexpected Confirm")
}
func (issue809AdminStub) Cancel(context.Context, eebusruntime.CancelRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	panic("unexpected Cancel")
}
func (issue809AdminStub) RetryTrusted(context.Context, eebusruntime.RetryTrustedRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	panic("unexpected RetryTrusted")
}
func (issue809AdminStub) Untrust(context.Context, eebusruntime.UntrustRequestV1) (eebusruntime.AdminMutationResultV1, *eebusruntime.AdminErrorV1) {
	panic("unexpected Untrust")
}

func TestIssue809OperatorRuntimeReturnsSeparateCapability(t *testing.T) {
	runtime := &msp05bRuntime{}
	admin := issue809AdminStub{}
	adapter, gotAdmin, err := startEEBusOperatorRuntime(
		context.Background(),
		msp05bEnabledConfig(),
		func(string) ([]netip.Addr, error) { return []netip.Addr{netip.MustParseAddr("192.0.2.42")}, nil },
		func(eebusruntime.Config) (eebusruntime.Runtime, eebusruntime.AdminV1, error) {
			return runtime, admin, nil
		},
	)
	if err != nil || adapter == nil || gotAdmin == nil {
		t.Fatalf("operator runtime=(%v,%v,%v), want complete capability pair", adapter, gotAdmin, err)
	}
	if adapter.runtime != runtime || runtime.startCalls != 1 {
		t.Fatalf("adapter/runtime start=%v/%d", adapter.runtime, runtime.startCalls)
	}
	if err := adapter.Shutdown(); err != nil || runtime.stopCalls != 1 {
		t.Fatalf("shutdown=(%v,%d)", err, runtime.stopCalls)
	}
}

func TestIssue809OperatorRuntimeFailsClosedOnMissingAdminCapability(t *testing.T) {
	runtime := &msp05bRuntime{}
	adapter, admin, err := startEEBusOperatorRuntime(
		context.Background(),
		msp05bEnabledConfig(),
		func(string) ([]netip.Addr, error) { return []netip.Addr{netip.MustParseAddr("192.0.2.42")}, nil },
		func(eebusruntime.Config) (eebusruntime.Runtime, eebusruntime.AdminV1, error) {
			return runtime, nil, nil
		},
	)
	if err == nil || adapter != nil || admin != nil {
		t.Fatalf("incomplete factory=(%v,%v,%v), want closed failure", adapter, admin, err)
	}
	if runtime.startCalls != 0 || runtime.stopCalls != 1 {
		t.Fatalf("incomplete factory calls start/stop=%d/%d, want 0/1", runtime.startCalls, runtime.stopCalls)
	}
}

type msp05bRuntime struct {
	startErr    error
	shutdownErr error
	startCalls  int
	stopCalls   int
}

func (runtime *msp05bRuntime) Start(context.Context) error {
	runtime.startCalls++
	return runtime.startErr
}

func (runtime *msp05bRuntime) Shutdown() error {
	runtime.stopCalls++
	return runtime.shutdownErr
}

func (*msp05bRuntime) Snapshot() (eebusruntime.SnapshotV1, error) {
	return eebusruntime.SnapshotV1{}, nil
}

func (*msp05bRuntime) PairingState() ([]eebusruntime.PairingObservationV1, error) {
	return nil, nil
}

func (*msp05bRuntime) FeaturesGet(
	context.Context,
	eebusraw.ReadAuthorizationV1,
	eebusraw.FeaturesGetRequestV1,
) (eebusraw.FeaturesGetDataV1, *eebusraw.ErrorV1) {
	return eebusraw.FeaturesGetDataV1{}, nil
}

func (*msp05bRuntime) FeaturesDataGet(
	context.Context,
	eebusraw.ReadAuthorizationV1,
	eebusraw.FeatureDataGetRequestV1,
) (eebusraw.FeatureDataGetDataV1, *eebusraw.ErrorV1) {
	return eebusraw.FeatureDataGetDataV1{}, nil
}

func msp05bEnabledConfig() ebusgateway.EEBusConfig {
	return ebusgateway.EEBusConfig{
		Enabled:            true,
		ListenPort:         4712,
		Interfaces:         []string{"fixture0"},
		Subnets:            []string{"192.0.2.0/24"},
		StateRoot:          "/var/lib/helianthus/eebus",
		RemoteSKIAllowlist: []string{},
		PairingWindowMode:  ebusgateway.EEBusPairingWindowClosed,
	}
}

func TestMSP05BStartEEBusRuntimeDisabledMakesZeroCalls(t *testing.T) {
	resolverCalls := 0
	factoryCalls := 0
	runtime := &msp05bRuntime{}

	adapter, err := startEEBusRuntime(
		context.Background(),
		ebusgateway.DefaultEEBusConfig(),
		func(string) ([]netip.Addr, error) {
			resolverCalls++
			return []netip.Addr{netip.MustParseAddr("192.0.2.42")}, nil
		},
		func(eebusruntime.Config) (eebusruntime.Runtime, error) {
			factoryCalls++
			return runtime, nil
		},
	)
	if err != nil || adapter != nil {
		t.Fatalf("disabled start = (%v, %v); want (nil, nil)", adapter, err)
	}
	if resolverCalls != 0 || factoryCalls != 0 || runtime.startCalls != 0 || runtime.stopCalls != 0 {
		t.Fatalf("disabled calls resolver=%d factory=%d start=%d shutdown=%d; want all zero",
			resolverCalls, factoryCalls, runtime.startCalls, runtime.stopCalls)
	}
}

func TestMSP05BStartEEBusRuntimeFailureShutsConstructedRuntimeOnce(t *testing.T) {
	resolver := func(string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("192.0.2.42")}, nil
	}
	shutdownErr := errors.New("sidecar shutdown failed")
	tests := []struct {
		name       string
		factoryErr error
		startErr   error
	}{
		{name: "factory failure", factoryErr: errors.New("runtime factory failed")},
		{name: "start failure", startErr: errors.New("runtime start failed")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime := &msp05bRuntime{startErr: test.startErr, shutdownErr: shutdownErr}
			adapter, err := startEEBusRuntime(
				context.Background(),
				msp05bEnabledConfig(),
				resolver,
				func(eebusruntime.Config) (eebusruntime.Runtime, error) {
					return runtime, test.factoryErr
				},
			)
			primaryErr := test.factoryErr
			wantStartCalls := 0
			if primaryErr == nil {
				primaryErr = test.startErr
				wantStartCalls = 1
			}
			if adapter != nil {
				t.Fatalf("adapter = %v; want nil after failure", adapter)
			}
			if !errors.Is(err, primaryErr) || !errors.Is(err, shutdownErr) {
				t.Fatalf("error = %v; want primary and shutdown causes", err)
			}
			if runtime.startCalls != wantStartCalls || runtime.stopCalls != 1 {
				t.Fatalf("calls start=%d shutdown=%d; want start=%d shutdown=1",
					runtime.startCalls, runtime.stopCalls, wantStartCalls)
			}
		})
	}
}

func TestMSP05BStartEEBusRuntimeRejectsMissingFactoryAndNilRuntime(t *testing.T) {
	resolver := func(string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("192.0.2.42")}, nil
	}
	factoryErr := errors.New("runtime factory failed")
	tests := []struct {
		name    string
		factory eebusRuntimeFactory
		wantErr error
	}{
		{name: "nil factory", factory: nil},
		{
			name: "nil runtime with error",
			factory: func(eebusruntime.Config) (eebusruntime.Runtime, error) {
				return nil, factoryErr
			},
			wantErr: factoryErr,
		},
		{
			name: "nil runtime without error",
			factory: func(eebusruntime.Config) (eebusruntime.Runtime, error) {
				return nil, nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adapter, err := startEEBusRuntime(context.Background(), msp05bEnabledConfig(), resolver, test.factory)
			if adapter != nil || err == nil {
				t.Fatalf("start result = (%v, %v); want nil adapter and error", adapter, err)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("start error = %v; want factory cause", err)
			}
		})
	}
}

func TestMSP05BStartedAdapterShutdownIsIdempotent(t *testing.T) {
	runtime := &msp05bRuntime{shutdownErr: errors.New("sidecar shutdown failed")}
	adapter, err := startEEBusRuntime(
		context.Background(),
		msp05bEnabledConfig(),
		func(string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr("192.0.2.42")}, nil
		},
		func(eebusruntime.Config) (eebusruntime.Runtime, error) {
			return runtime, nil
		},
	)
	if err != nil || adapter == nil {
		t.Fatalf("start adapter = (%v, %v); want non-nil and nil error", adapter, err)
	}
	firstErr := adapter.Shutdown()
	secondErr := adapter.Shutdown()
	if !errors.Is(firstErr, runtime.shutdownErr) || !errors.Is(secondErr, runtime.shutdownErr) {
		t.Fatalf("shutdown errors = (%v, %v); want retained shutdown cause", firstErr, secondErr)
	}
	if runtime.startCalls != 1 || runtime.stopCalls != 1 {
		t.Fatalf("calls start=%d shutdown=%d; want exactly one each", runtime.startCalls, runtime.stopCalls)
	}
}

func TestMSP05BRunJoinsLaterErrorWithSidecarShutdown(t *testing.T) {
	originalResolver := resolveEEBusInterfaceAddressesFn
	originalFactory := newEEBusRuntimeFn
	originalWire := wireObserveFirstObserversFn
	t.Cleanup(func() {
		resolveEEBusInterfaceAddressesFn = originalResolver
		newEEBusRuntimeFn = originalFactory
		wireObserveFirstObserversFn = originalWire
	})

	runtime := &msp05bRuntime{shutdownErr: errors.New("sidecar shutdown failed")}
	laterErr := errors.New("later gateway startup failed")
	resolveEEBusInterfaceAddressesFn = func(string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("192.0.2.42")}, nil
	}
	newEEBusRuntimeFn = func(eebusruntime.Config) (eebusruntime.Runtime, error) {
		return runtime, nil
	}
	wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		return nil, nil, laterErr
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.EEBusConfig = msp05bEnabledConfig()
	err := run(context.Background(), cfg)
	if !errors.Is(err, laterErr) || !errors.Is(err, runtime.shutdownErr) {
		t.Fatalf("run error = %v; want later gateway and sidecar shutdown causes", err)
	}
	if runtime.startCalls != 1 || runtime.stopCalls != 1 {
		t.Fatalf("calls start=%d shutdown=%d; want exactly one each", runtime.startCalls, runtime.stopCalls)
	}
}

func TestMSP05BRunJoinsHTTPStartupErrorWithSidecarShutdown(t *testing.T) {
	originalResolver := resolveEEBusInterfaceAddressesFn
	originalFactory := newEEBusRuntimeFn
	originalWire := wireObserveFirstObserversFn
	originalDiscovery := startDiscoveryScanLoopFn
	originalSemantic := startVaillantSemanticPollingFn
	originalHTTP := startHTTPServerFn
	t.Cleanup(func() {
		resolveEEBusInterfaceAddressesFn = originalResolver
		newEEBusRuntimeFn = originalFactory
		wireObserveFirstObserversFn = originalWire
		startDiscoveryScanLoopFn = originalDiscovery
		startVaillantSemanticPollingFn = originalSemantic
		startHTTPServerFn = originalHTTP
	})

	runtime := &msp05bRuntime{shutdownErr: errors.New("sidecar shutdown failed")}
	httpErr := errors.New("HTTP startup failed")
	installMSP05BRunDependencies(runtime)
	startHTTPServerFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, *graphql.BroadcastHub, graphql.SemanticProvider, mcp.ScheduleWriter, mcp.ConfigWriter, *ebusgateway.BusObservabilityStore, *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
		return nil, nil, httpErr
	}

	err := run(context.Background(), msp05bGatewayRunConfig(t))
	if !errors.Is(err, httpErr) || !errors.Is(err, runtime.shutdownErr) {
		t.Fatalf("run error = %v; want HTTP and sidecar shutdown causes", err)
	}
	if runtime.startCalls != 1 || runtime.stopCalls != 1 {
		t.Fatalf("calls start=%d shutdown=%d; want exactly one each", runtime.startCalls, runtime.stopCalls)
	}
}

func TestMSP05BRunCleanCancellationReturnsSidecarShutdownError(t *testing.T) {
	originalResolver := resolveEEBusInterfaceAddressesFn
	originalFactory := newEEBusRuntimeFn
	originalWire := wireObserveFirstObserversFn
	originalDiscovery := startDiscoveryScanLoopFn
	originalSemantic := startVaillantSemanticPollingFn
	originalHTTP := startHTTPServerFn
	t.Cleanup(func() {
		resolveEEBusInterfaceAddressesFn = originalResolver
		newEEBusRuntimeFn = originalFactory
		wireObserveFirstObserversFn = originalWire
		startDiscoveryScanLoopFn = originalDiscovery
		startVaillantSemanticPollingFn = originalSemantic
		startHTTPServerFn = originalHTTP
	})

	runtime := &msp05bRuntime{shutdownErr: errors.New("sidecar shutdown failed")}
	installMSP05BRunDependencies(runtime)
	ctx, cancel := context.WithCancel(context.Background())
	startHTTPServerFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, *graphql.BroadcastHub, graphql.SemanticProvider, mcp.ScheduleWriter, mcp.ConfigWriter, *ebusgateway.BusObservabilityStore, *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
		cancel()
		return nil, nil, nil
	}

	err := run(ctx, msp05bGatewayRunConfig(t))
	if !errors.Is(err, runtime.shutdownErr) {
		t.Fatalf("run error = %v; want sidecar shutdown cause after clean cancellation", err)
	}
	if runtime.startCalls != 1 || runtime.stopCalls != 1 {
		t.Fatalf("calls start=%d shutdown=%d; want exactly one each", runtime.startCalls, runtime.stopCalls)
	}
}

func TestIssue846RunKeepsGatewayAvailableWhenEEBusStartupFails(t *testing.T) {
	originalResolver := resolveEEBusInterfaceAddressesFn
	originalOperatorFactory := newEEBusOperatorRuntimeFn
	originalRuntimeFactory := newEEBusRuntimeFn
	originalWire := wireObserveFirstObserversFn
	originalDiscovery := startDiscoveryScanLoopFn
	originalSemantic := startVaillantSemanticPollingFn
	originalHTTP := startHTTPServerFn
	t.Cleanup(func() {
		resolveEEBusInterfaceAddressesFn = originalResolver
		newEEBusOperatorRuntimeFn = originalOperatorFactory
		newEEBusRuntimeFn = originalRuntimeFactory
		wireObserveFirstObserversFn = originalWire
		startDiscoveryScanLoopFn = originalDiscovery
		startVaillantSemanticPollingFn = originalSemantic
		startHTTPServerFn = originalHTTP
	})

	installMSP05BRunDependencies(&msp05bRuntime{})
	newEEBusOperatorRuntimeFn = func(eebusruntime.Config) (eebusruntime.Runtime, eebusruntime.AdminV1, error) {
		return nil, nil, errors.New("operator listener unavailable")
	}
	newEEBusRuntimeFn = func(eebusruntime.Config) (eebusruntime.Runtime, error) {
		return nil, errors.New("public listener unavailable")
	}

	ctx, cancel := context.WithCancel(context.Background())
	httpStarted := false
	startHTTPServerFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, *graphql.BroadcastHub, graphql.SemanticProvider, mcp.ScheduleWriter, mcp.ConfigWriter, *ebusgateway.BusObservabilityStore, *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
		httpStarted = true
		cancel()
		return nil, nil, nil
	}

	err := run(ctx, msp05bGatewayRunConfig(t))
	if err != nil {
		t.Fatalf("run returned fatal eeBUS startup error: %v", err)
	}
	if !httpStarted {
		t.Fatal("HTTP gateway did not start after isolated eeBUS failure")
	}
}

type issue846BlockingRuntime struct {
	msp05bRuntime
	snapshotEntered chan struct{}
	snapshotRelease chan struct{}
	shutdownCalled  chan struct{}
	enterOnce       sync.Once
	shutdownOnce    sync.Once
}

func (runtime *issue846BlockingRuntime) Snapshot() (eebusruntime.SnapshotV1, error) {
	runtime.enterOnce.Do(func() { close(runtime.snapshotEntered) })
	<-runtime.snapshotRelease
	return eebusruntime.SnapshotV1{}, nil
}

func (runtime *issue846BlockingRuntime) Shutdown() error {
	runtime.shutdownOnce.Do(func() { close(runtime.shutdownCalled) })
	return runtime.msp05bRuntime.Shutdown()
}

func TestIssue846RuntimeSlotWaitsForInFlightReadersBeforeSwapAndShutdown(t *testing.T) {
	oldRuntime := &issue846BlockingRuntime{
		snapshotEntered: make(chan struct{}),
		snapshotRelease: make(chan struct{}),
		shutdownCalled:  make(chan struct{}),
	}
	nextRuntime := &msp05bRuntime{}
	slot := newEEBusRuntimeSlot(oldRuntime)
	adapter := &eebusRuntimeAdapter{runtime: slot}

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		_, _ = adapter.Snapshot()
	}()
	<-oldRuntime.snapshotEntered

	swapDone := make(chan bool, 1)
	go func() { swapDone <- slot.Replace(nextRuntime) }()
	select {
	case <-oldRuntime.shutdownCalled:
		t.Fatal("old runtime shut down while a snapshot read was still in flight")
	default:
	}

	close(oldRuntime.snapshotRelease)
	<-readDone
	if replaced := <-swapDone; !replaced {
		t.Fatal("runtime slot rejected a live replacement")
	}
	select {
	case <-oldRuntime.shutdownCalled:
	default:
		t.Fatal("old runtime was not shut down after the safe swap")
	}
	if oldRuntime.stopCalls != 1 {
		t.Fatalf("old runtime shutdown calls = %d; want one", oldRuntime.stopCalls)
	}
}

func installMSP05BRunDependencies(runtime eebusruntime.Runtime) {
	resolveEEBusInterfaceAddressesFn = func(string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("192.0.2.42")}, nil
	}
	newEEBusRuntimeFn = func(eebusruntime.Config) (eebusruntime.Runtime, error) {
		return runtime, nil
	}
	wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		return nil, nil, nil
	}
	startDiscoveryScanLoopFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, activeTxnClassifier) startupScanSignals {
		return startupScanSignals{}
	}
	startVaillantSemanticPollingFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.LiveSemanticProvider, *graphql.BroadcastHub, <-chan struct{}) *vaillantSemanticPoller {
		return nil
	}
}

func msp05bGatewayRunConfig(t *testing.T) ebusgateway.Config {
	t.Helper()
	cfg := ebusgateway.DefaultConfig()
	cfg.EEBusConfig = msp05bEnabledConfig()
	cfg.Transport = transport.NewLoopback()
	cfg.BroadcastListen = false
	cfg.RuntimeStatePath = filepath.Join(t.TempDir(), "runtime-state.json")
	return cfg
}
