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
	// Provide a real activeProbePassed channel so the async admission
	// goroutine inside run() fires SetAdmittedMutationSource — which the
	// inserter now gates its subscription on (Phase A.5 round 3 fix).
	activeProbePassed := make(chan struct{})
	startDiscoveryScanLoopFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, activeTxnClassifier) startupScanSignals {
		return startupScanSignals{
			activeProbePassed: activeProbePassed,
		}
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

	// Wait for gateway-ready BEFORE starting the feeder so we don't race
	// against the source-selection bus subscription being torn down at
	// selector.Select() return — that bus has its own subscriber on the
	// reconstructor and its Close() path reads from the same shared
	// channels the feeder writes to.
	var gateway *ebusgateway.Gateway
	select {
	case gateway = <-gatewayReady:
	case <-time.After(8 * time.Second):
		t.Fatal("gateway runtime did not reach HTTP startup")
	}

	// Signal active-probe success so the async goroutine in run() calls
	// builder.SetAdmittedMutationSource(sourceSelection.Source). The
	// AddressTableInserter subscription is gated on AdmittedMutationSource
	// returning ok=true (Phase A.5 round 3 — Codex bot P2). Without this
	// signal the inserter never binds.
	close(activeProbePassed)

	feedCtx, stopFeed := context.WithCancel(ctx)
	feedDone := make(chan struct{})
	go func() {
		defer close(feedDone)
		feedGatewayAddressTablePassiveObservation(feedCtx, reconstructor)
	}()

	// Assert insertion via the registry's lock-safe DeviceEntry API
	// (interface-method getters synchronize internally). Avoid reading
	// AddressSlot fields directly — those are mutated by the inserter
	// without an external lock and would race with the read here. The
	// AD05/AD06 metadata fields (DiscoverySource, VerificationState)
	// are already covered by the inserter's unit tests in
	// address_table_insertion_test.go; this runtime test only proves
	// the wire-up plumbs events from reconstructor → inserter →
	// registry.
	assertPassiveObservedDeviceEntry(t, gateway.Registry, 0xF6)

	// Stop the feeder and wait for it to drain BEFORE cancelling ctx so
	// run()'s reconstructor.Close() does not race with in-flight
	// OnPassiveTapEvent calls (race detector finding 2026-05-06).
	stopFeed()
	<-feedDone

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
	// Post-Phase-C P1: the inserter now requires a fully-completed
	// M-T transaction (PassiveClassifiedEventTransaction). Build the
	// proper wire flow:  request bytes  + ACK + response segment
	//   + ACK + SYN   so the reconstructor classifies as Transaction
	// (was previously: request + ACK + SYN, which produced a phase-3
	// no_response abandon — the inserter accepted those pre-P1, but
	// that was the bug that produced phantom registry entries from
	// NETX3's identity scans).
	symbols := append(gatewayWireupFrameBytes(request), protocol.SymbolAck)
	symbols = append(symbols, gatewayWireupResponseSegmentBytes([]byte{0x11, 0x55})...)
	symbols = append(symbols, protocol.SymbolAck, protocol.SymbolSyn)

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

func gatewayWireupResponseSegmentBytes(data []byte) []byte {
	raw := make([]byte, 0, 2+len(data))
	raw = append(raw, byte(len(data)))
	raw = append(raw, data...)
	raw = append(raw, protocol.CRC(raw))
	return raw
}

func assertPassiveObservedDeviceEntry(t *testing.T, reg *registry.DeviceRegistry, address byte) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if entry, ok := reg.Lookup(address); ok && entry != nil && entry.PrimaryDisplayAddress() == address {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("AddressTableInserter did not insert passive-observed device 0x%02X into runtime registry", address)
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
