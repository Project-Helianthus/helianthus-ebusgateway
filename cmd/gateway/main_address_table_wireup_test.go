package main

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mdns"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
)

func TestRun_AddressTableInserterWiresThroughPassiveReconstructor(t *testing.T) {
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

	wireObserveFirstObserversFn = func(*ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		return nil, nil, nil
	}
	startDiscoveryScanLoopFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, activeTxnClassifier) startupScanSignals {
		return startupScanSignals{}
	}
	startVaillantSemanticPollingFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.LiveSemanticProvider, *graphql.BroadcastHub, <-chan struct{}) *vaillantSemanticPoller {
		return nil
	}
	startBroadcastListenerWithReconstructorFn = func(context.Context, *router.BusEventRouter, *ebusgateway.PassiveTransactionReconstructor) (*ebusgateway.BroadcastListener, error) {
		return nil, nil
	}

	reconstructorStarted := make(chan *ebusgateway.PassiveTransactionReconstructor, 1)
	startPassiveTransactionReconstructor = func(ctx context.Context, cfg ebusgateway.Config) (*ebusgateway.PassiveTransactionReconstructor, error) {
		reconstructor, err := ebusgateway.StartPassiveTransactionReconstructor(ctx, cfg)
		if err != nil {
			return nil, err
		}
		select {
		case reconstructorStarted <- reconstructor:
		default:
		}
		return reconstructor, nil
	}

	gatewayReady := make(chan *ebusgateway.Gateway, 1)
	startHTTPServerFn = func(_ context.Context, _ ebusgateway.Config, gateway *ebusgateway.Gateway, _ *graphql.Builder, _ *graphql.BroadcastHub, _ graphql.SemanticProvider, _ mcp.ScheduleWriter, _ mcp.ConfigWriter, _ *ebusgateway.BusObservabilityStore, _ *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
		select {
		case gatewayReady <- gateway:
		default:
		}
		return nil, nil, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = transport.NewLoopback()
	cfg.PassiveTransport = transport.NewLoopback()
	cfg.BroadcastListen = true
	cfg.ScanOnStart = false
	cfg.ScanSource = 0xF0
	cfg.ScanSourceAuto = false

	done := make(chan error, 1)
	go func() {
		done <- run(ctx, cfg)
	}()

	var reconstructor *ebusgateway.PassiveTransactionReconstructor
	select {
	case reconstructor = <-reconstructorStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("passive reconstructor did not start")
	}

	feedCtx, stopFeed := context.WithCancel(ctx)
	defer stopFeed()
	go feedGatewayAddressTablePassiveObservation(feedCtx, reconstructor)

	var gateway *ebusgateway.Gateway
	select {
	case gateway = <-gatewayReady:
	case <-time.After(8 * time.Second):
		t.Fatal("gateway runtime did not reach HTTP startup")
	}

	assertPassiveObservedRegistrySlot(t, gateway.Registry, 0xF6)

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

func feedGatewayAddressTablePassiveObservation(ctx context.Context, reconstructor *ebusgateway.PassiveTransactionReconstructor) {
	request := protocol.Frame{
		Source:    0xF1,
		Target:    0xF6,
		Primary:   0xB5,
		Secondary: 0x24,
		Data:      []byte{0x01},
	}
	symbols := append(gatewayWireupFrameBytes(request), protocol.SymbolAck, protocol.SymbolSyn)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		feedGatewayWireupSymbols(reconstructor, time.Now(), symbols)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func assertPassiveObservedRegistrySlot(t *testing.T, reg *registry.DeviceRegistry, address byte) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if slot, ok := reg.LookupSlot(address); ok && slot != nil {
			if slot.DiscoverySource != registry.DiscoverySourcePassiveObserved {
				t.Fatalf("slot[0x%02X].DiscoverySource = %v; want passive_observed", address, slot.DiscoverySource)
			}
			if slot.VerificationState != registry.VerificationStateCorroborated {
				t.Fatalf("slot[0x%02X].VerificationState = %v; want corroborated_pending", address, slot.VerificationState)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("AddressTableInserter did not insert passive-observed slot 0x%02X into runtime registry", address)
		}
		time.Sleep(time.Millisecond)
	}
}

func feedGatewayWireupSymbols(reconstructor *ebusgateway.PassiveTransactionReconstructor, start time.Time, symbols []byte) {
	for index, symbol := range symbols {
		reconstructor.OnPassiveTapEvent(ebusgateway.PassiveTapEvent{
			Kind:       ebusgateway.PassiveTapEventSymbol,
			Symbol:     symbol,
			ObservedAt: start.Add(time.Duration(index) * time.Millisecond),
		})
	}
}

func gatewayWireupFrameBytes(frame protocol.Frame) []byte {
	raw := make([]byte, 0, 7+len(frame.Data))
	raw = append(raw, frame.Source, frame.Target, frame.Primary, frame.Secondary, byte(len(frame.Data)))
	raw = append(raw, frame.Data...)
	raw = append(raw, protocol.CRC(raw), protocol.SymbolSyn)
	return raw
}
