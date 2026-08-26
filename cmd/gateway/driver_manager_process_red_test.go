package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mdns"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

func TestIssue851RunKeepsHTTPShellAliveWhenEBusConstructionFails(t *testing.T) {
	originalWire := wireObserveFirstObserversFn
	originalDiscovery := startDiscoveryScanLoopFn
	originalSemantic := startVaillantSemanticPollingFn
	originalHTTP := startHTTPServerFn
	t.Cleanup(func() {
		wireObserveFirstObserversFn = originalWire
		startDiscoveryScanLoopFn = originalDiscovery
		startVaillantSemanticPollingFn = originalSemantic
		startHTTPServerFn = originalHTTP
	})

	wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		return nil, nil, nil
	}
	startDiscoveryScanLoopFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, activeTxnClassifier) startupScanSignals {
		return startupScanSignals{}
	}
	startVaillantSemanticPollingFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.LiveSemanticProvider, *graphql.BroadcastHub, <-chan struct{}) *vaillantSemanticPoller {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	httpStarted := make(chan struct{}, 1)
	var surfaceErr error
	startHTTPServerFn = func(_ context.Context, _ ebusgateway.Config, gateway *ebusgateway.Gateway, _ *graphql.Builder, _ *graphql.BroadcastHub, _ graphql.SemanticProvider, _ mcp.ScheduleWriter, _ mcp.ConfigWriter, _ *ebusgateway.BusObservabilityStore, _ *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
		if gateway == nil || gateway.Registry == nil || gateway.Router == nil || gateway.Transport == nil {
			surfaceErr = errors.New("HTTP shell received an incomplete stable gateway")
		} else if _, err := gateway.Transport.ReadByte(); !errors.Is(err, transport.ErrDriverUnavailable) {
			surfaceErr = errors.New("stable eBUS provider did not return typed unavailable state")
		}
		httpStarted <- struct{}{}
		cancel()
		return nil, nil, nil
	}

	var dialCalls atomic.Int32
	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = nil
	cfg.TransportConfig.Protocol = ebusgateway.TransportTCPPlain
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "secret-user:secret-password@example.invalid:9999"
	cfg.TransportConfig.DialTimeout = 5 * time.Millisecond
	cfg.TransportConfig.Dial = func(context.Context, string, string, time.Duration) (net.Conn, error) {
		dialCalls.Add(1)
		return nil, errors.New("secret endpoint unavailable")
	}
	cfg.BroadcastListen = false
	cfg.ScanOnStart = false
	cfg.RuntimeStatePath = filepath.Join(t.TempDir(), "runtime-state.json")

	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	select {
	case <-httpStarted:
	case err := <-done:
		t.Fatalf("run exited before HTTP shell startup: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP shell did not start after eBUS construction failure")
	}
	if surfaceErr != nil {
		t.Fatal(surfaceErr)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned fatal eBUS construction/shutdown error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not exit after context cancellation")
	}
	if dialCalls.Load() == 0 {
		t.Fatal("test did not exercise eBUS construction")
	}
}

func TestIssue851HTTPShellStartsWhileEBusConstructionIsBlocked(t *testing.T) {
	originalWire := wireObserveFirstObserversFn
	originalDiscovery := startDiscoveryScanLoopFn
	originalSemantic := startVaillantSemanticPollingFn
	originalHTTP := startHTTPServerFn
	t.Cleanup(func() {
		wireObserveFirstObserversFn = originalWire
		startDiscoveryScanLoopFn = originalDiscovery
		startVaillantSemanticPollingFn = originalSemantic
		startHTTPServerFn = originalHTTP
	})
	wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		return nil, nil, nil
	}
	startDiscoveryScanLoopFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, activeTxnClassifier) startupScanSignals {
		return startupScanSignals{}
	}
	startVaillantSemanticPollingFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.LiveSemanticProvider, *graphql.BroadcastHub, <-chan struct{}) *vaillantSemanticPoller {
		return nil
	}

	dialStarted := make(chan struct{}, 1)
	dialRelease := make(chan struct{})
	httpStarted := make(chan struct{}, 1)
	startHTTPServerFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, *graphql.BroadcastHub, graphql.SemanticProvider, mcp.ScheduleWriter, mcp.ConfigWriter, *ebusgateway.BusObservabilityStore, *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
		httpStarted <- struct{}{}
		return nil, nil, nil
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = nil
	cfg.TransportConfig.Protocol = ebusgateway.TransportTCPPlain
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "blocked.invalid:9999"
	cfg.TransportConfig.Dial = func(context.Context, string, string, time.Duration) (net.Conn, error) {
		select {
		case dialStarted <- struct{}{}:
		default:
		}
		<-dialRelease // deliberately ignores context
		return nil, errors.New("offline")
	}
	cfg.BroadcastListen = false
	cfg.ScanOnStart = false
	cfg.RuntimeStatePath = filepath.Join(t.TempDir(), "runtime-state.json")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	select {
	case <-dialStarted:
	case <-time.After(time.Second):
		t.Fatal("eBUS construction did not start")
	}
	select {
	case <-httpStarted:
	case err := <-done:
		t.Fatalf("run exited while eBUS construction was blocked: %v", err)
	case <-time.After(time.Second):
		t.Fatal("HTTP shell waited for blocked eBUS construction")
	}
	close(dialRelease)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run did not shut down after blocked construction was released")
	}
}

func TestIssue851DelayedHealthyDriverWakesOneImmediateStartupScan(t *testing.T) {
	originalWire := wireObserveFirstObserversFn
	originalDiscovery := startDiscoveryScanLoopFn
	originalSemantic := startVaillantSemanticPollingFn
	originalHTTP := startHTTPServerFn
	t.Cleanup(func() {
		wireObserveFirstObserversFn = originalWire
		startDiscoveryScanLoopFn = originalDiscovery
		startVaillantSemanticPollingFn = originalSemantic
		startHTTPServerFn = originalHTTP
	})
	wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		return nil, nil, nil
	}
	startVaillantSemanticPollingFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.LiveSemanticProvider, *graphql.BroadcastHub, <-chan struct{}) *vaillantSemanticPoller {
		return nil
	}

	dialStarted := make(chan struct{}, 1)
	dialRelease := make(chan struct{})
	defer func() {
		select {
		case <-dialRelease:
		default:
			close(dialRelease)
		}
	}()
	httpStarted := make(chan struct{}, 1)
	scanStarted := make(chan struct{}, 1)
	var scanCalls atomic.Int32
	startHTTPServerFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, *graphql.BroadcastHub, graphql.SemanticProvider, mcp.ScheduleWriter, mcp.ConfigWriter, *ebusgateway.BusObservabilityStore, *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
		httpStarted <- struct{}{}
		return nil, nil, nil
	}
	startDiscoveryScanLoopFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, activeTxnClassifier) startupScanSignals {
		scanCalls.Add(1)
		select {
		case scanStarted <- struct{}{}:
		default:
		}
		return startupScanSignals{}
	}

	client, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = nil
	cfg.TransportConfig.Protocol = ebusgateway.TransportTCPPlain
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "delayed.invalid:9999"
	cfg.TransportConfig.Dial = func(context.Context, string, string, time.Duration) (net.Conn, error) {
		select {
		case dialStarted <- struct{}{}:
		default:
		}
		<-dialRelease
		return client, nil
	}
	cfg.BroadcastListen = false
	cfg.ScanOnStart = true
	cfg.ScanInterval = time.Minute
	cfg.RuntimeStatePath = filepath.Join(t.TempDir(), "runtime-state.json")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	select {
	case <-dialStarted:
	case <-time.After(time.Second):
		t.Fatal("delayed eBUS construction did not start")
	}
	select {
	case <-httpStarted:
	case err := <-done:
		t.Fatalf("run() exited before HTTP shell startup: %v", err)
	case <-time.After(time.Second):
		t.Fatal("HTTP shell waited for delayed healthy eBUS construction")
	}
	select {
	case <-scanStarted:
		t.Fatal("startup scan ran before the first correlated RUNNING generation")
	case <-time.After(25 * time.Millisecond):
	}

	close(dialRelease)
	select {
	case <-scanStarted:
	case err := <-done:
		t.Fatalf("run() exited before delayed healthy startup scan: %v", err)
	case <-time.After(time.Second):
		t.Fatal("RUNNING notification did not wake the initial scan immediately")
	}
	time.Sleep(25 * time.Millisecond)
	if got := scanCalls.Load(); got != 1 {
		t.Fatalf("startup scan calls = %d, want exactly one", got)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run() did not shut down after delayed healthy scan")
	}
}

func TestIssue851HTTPShellStartsBeforeENHSourceSelectionWarmup(t *testing.T) {
	originalWire := wireObserveFirstObserversFn
	originalDiscovery := startDiscoveryScanLoopFn
	originalSemantic := startVaillantSemanticPollingFn
	originalHTTP := startHTTPServerFn
	t.Cleanup(func() {
		wireObserveFirstObserversFn = originalWire
		startDiscoveryScanLoopFn = originalDiscovery
		startVaillantSemanticPollingFn = originalSemantic
		startHTTPServerFn = originalHTTP
	})
	wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		return nil, nil, nil
	}
	startDiscoveryScanLoopFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, activeTxnClassifier) startupScanSignals {
		return startupScanSignals{}
	}
	startVaillantSemanticPollingFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.LiveSemanticProvider, *graphql.BroadcastHub, <-chan struct{}) *vaillantSemanticPoller {
		return nil
	}

	dialStarted := make(chan struct{}, 1)
	dialRelease := make(chan struct{})
	httpStarted := make(chan struct{}, 1)
	var surfaceErr error
	startHTTPServerFn = func(_ context.Context, _ ebusgateway.Config, gateway *ebusgateway.Gateway, builder *graphql.Builder, _ *graphql.BroadcastHub, _ graphql.SemanticProvider, schedule mcp.ScheduleWriter, config mcp.ConfigWriter, _ *ebusgateway.BusObservabilityStore, _ *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
		if gateway == nil || gateway.Transport == nil || builder == nil || schedule == nil || config == nil {
			surfaceErr = errors.New("early HTTP shell received nil runtime binding")
		} else if _, admitted := builder.AdmittedMutationSource(); admitted {
			surfaceErr = errors.New("early HTTP shell exposed an admitted write source")
		} else if result, err := schedule.SetZoneTimeProgram(context.Background(), 0, 0, nil); err != nil || result.Success || result.Error != semanticWriterSourceNotAdmittedError {
			surfaceErr = errors.New("early HTTP schedule binding did not fail closed")
		} else if result := config.SetSystemConfig(context.Background(), "field", "value"); result.Success || result.Error != semanticWriterSourceNotAdmittedError {
			surfaceErr = errors.New("early HTTP config binding did not fail closed")
		}
		httpStarted <- struct{}{}
		return nil, nil, nil
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = nil
	cfg.TransportConfig.Protocol = ebusgateway.TransportENH
	cfg.TransportConfig.Network = "tcp"
	cfg.TransportConfig.Address = "blocked.invalid:8888"
	cfg.TransportConfig.Dial = func(context.Context, string, string, time.Duration) (net.Conn, error) {
		select {
		case dialStarted <- struct{}{}:
		default:
		}
		<-dialRelease // deliberately ignores context
		return nil, errors.New("offline")
	}
	cfg.BroadcastListen = true
	cfg.ScanOnStart = false
	cfg.RuntimeStatePath = filepath.Join(t.TempDir(), "runtime-state.json")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	select {
	case <-dialStarted:
	case <-time.After(time.Second):
		t.Fatal("eBUS ENH construction did not start")
	}
	select {
	case <-httpStarted:
	case err := <-done:
		t.Fatalf("run exited before early HTTP shell: %v", err)
	case <-time.After(time.Second):
		t.Fatal("HTTP shell waited for ENH passive source-selection warmup")
	}
	if surfaceErr != nil {
		t.Fatal(surfaceErr)
	}
	close(dialRelease)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run did not shut down after early HTTP test cancellation")
	}
}

func TestIssue851RunCanonicalizesURIOverrideForAllRuntimePolicies(t *testing.T) {
	originalWire := wireObserveFirstObserversFn
	originalDiscovery := startDiscoveryScanLoopFn
	originalSemantic := startVaillantSemanticPollingFn
	originalHTTP := startHTTPServerFn
	t.Cleanup(func() {
		wireObserveFirstObserversFn = originalWire
		startDiscoveryScanLoopFn = originalDiscovery
		startVaillantSemanticPollingFn = originalSemantic
		startHTTPServerFn = originalHTTP
	})

	tests := []struct {
		name          string
		uri           string
		wantProtocol  ebusgateway.TransportProtocol
		wantNetwork   string
		wantAddress   string
		wantAdmission bool
		wantSource    byte
	}{
		{name: "ebusd", uri: "ebusd://127.0.0.1:8888", wantProtocol: ebusgateway.TransportEbusdTCP, wantNetwork: "tcp", wantAddress: "127.0.0.1:8888", wantAdmission: true, wantSource: ebusgateway.DefaultConfig().ScanSource},
		{name: "tcp plain", uri: "tcp-plain://127.0.0.1:9999", wantProtocol: ebusgateway.TransportTCPPlain, wantNetwork: "tcp", wantAddress: "127.0.0.1:9999"},
		{name: "udp plain", uri: "udp-plain://127.0.0.1:9999", wantProtocol: ebusgateway.TransportUDPPlain, wantNetwork: "udp", wantAddress: "127.0.0.1:9999"},
		{name: "adapter direct enh", uri: "adapter-direct://127.0.0.1:1", wantProtocol: ebusgateway.TransportAdapterDirect, wantNetwork: "tcp", wantAddress: "127.0.0.1:1"},
		{name: "adapter direct ens", uri: "adapter-direct-ens://127.0.0.1:1", wantProtocol: adapterDirectENSProtocol, wantNetwork: "tcp", wantAddress: "127.0.0.1:1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			type runtimeObservation struct {
				cfg      ebusgateway.Config
				admitted bool
				source   byte
			}
			wireObserved := make(chan ebusgateway.Config, 1)
			discoveryObserved := make(chan runtimeObservation, 1)
			httpObserved := make(chan runtimeObservation, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			wireObserveFirstObserversFn = func(cfg *ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
				wireObserved <- *cfg
				return nil, nil, nil
			}
			startDiscoveryScanLoopFn = func(_ context.Context, cfg ebusgateway.Config, _ *ebusgateway.Gateway, builder *graphql.Builder, _ activeTxnClassifier) startupScanSignals {
				source, admitted := builder.AdmittedMutationSource()
				discoveryObserved <- runtimeObservation{cfg: cfg, admitted: admitted, source: source}
				cancel()
				return startupScanSignals{}
			}
			startVaillantSemanticPollingFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.LiveSemanticProvider, *graphql.BroadcastHub, <-chan struct{}) *vaillantSemanticPoller {
				return nil
			}

			startHTTPServerFn = func(_ context.Context, cfg ebusgateway.Config, _ *ebusgateway.Gateway, builder *graphql.Builder, _ *graphql.BroadcastHub, _ graphql.SemanticProvider, _ mcp.ScheduleWriter, _ mcp.ConfigWriter, _ *ebusgateway.BusObservabilityStore, _ *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
				source, admitted := builder.AdmittedMutationSource()
				httpObserved <- runtimeObservation{cfg: cfg, admitted: admitted, source: source}
				return nil, nil, nil
			}

			cfg := ebusgateway.DefaultConfig()
			cfg.Transport = nil
			cfg.TransportConfig.Protocol = ebusgateway.TransportENH
			cfg.TransportConfig.Network = "unix"
			cfg.TransportConfig.Address = test.uri
			cfg.TransportConfig.DialTimeout = time.Millisecond
			cfg.TransportConfig.Dial = func(context.Context, string, string, time.Duration) (net.Conn, error) {
				return nil, errors.New("test transport unavailable")
			}
			cfg.BroadcastListen = false
			cfg.ScanOnStart = false
			cfg.ScanSource = 0
			cfg.ScanSourceAuto = true
			cfg.RuntimeStatePath = filepath.Join(t.TempDir(), "runtime-state.json")

			done := make(chan error, 1)
			go func() { done <- run(ctx, cfg) }()
			var httpResult runtimeObservation
			select {
			case httpResult = <-httpObserved:
			case err := <-done:
				t.Fatalf("run() exited before HTTP observation: %v", err)
			case <-time.After(2 * time.Second):
				t.Fatal("HTTP runtime did not start")
			}
			assertCanonical := func(label string, observed ebusgateway.Config) {
				t.Helper()
				if observed.TransportConfig.Protocol != test.wantProtocol || observed.TransportConfig.Network != test.wantNetwork || observed.TransportConfig.Address != test.wantAddress {
					t.Fatalf("%s transport = protocol:%q network:%q address:%q, want %q/%q/%q", label, observed.TransportConfig.Protocol, observed.TransportConfig.Network, observed.TransportConfig.Address, test.wantProtocol, test.wantNetwork, test.wantAddress)
				}
			}
			assertCanonical("HTTP", httpResult.cfg)
			select {
			case observed := <-wireObserved:
				assertCanonical("observer wiring", observed)
			default:
				t.Fatal("observer wiring did not receive runtime config")
			}
			select {
			case discoveryResult := <-discoveryObserved:
				assertCanonical("discovery", discoveryResult.cfg)
				if discoveryResult.admitted != test.wantAdmission || discoveryResult.source != test.wantSource {
					t.Fatalf("discovery admitted source = 0x%02X admitted=%v, want 0x%02X/%v", discoveryResult.source, discoveryResult.admitted, test.wantSource, test.wantAdmission)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("discovery did not receive runtime config")
			}
			if httpResult.admitted || httpResult.source != 0 {
				t.Fatalf("early HTTP admitted source = 0x%02X admitted=%v, want fail-closed 0x00/false", httpResult.source, httpResult.admitted)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("run() error = %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("run() did not stop after discovery observation")
			}
		})
	}
}
