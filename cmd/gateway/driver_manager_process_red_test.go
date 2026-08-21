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
