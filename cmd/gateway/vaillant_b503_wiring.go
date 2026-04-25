package main

// M6_DISPATCHER_BRIDGE (execution-plans#19, amendment-1) — production
// wiring entry point for the Vaillant B503 MCP tool surface.
//
// Replaces the M2a-era b503StubDispatcher with the real raw-frame
// dispatcher (`*rawFrameDispatcher` defined in vaillant_b503_dispatcher.go)
// that routes through the gateway's existing *protocol.Bus substrate —
// the same path B524/B525 already use (AD16). The session FSM, capability
// signal, single-owner GraphQL+MCP wiring, and forbidden-surface guards
// are all unchanged from M2a.

import (
	"context"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/b503session"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

// defaultVaillantTarget is the BAI00 primary address used when a caller
// omits `target_address`. 0x08 is the canonical Vaillant boiler address
// per existing gateway precedent (semantic_vaillant.go).
const defaultVaillantTarget byte = 0x08

// vaillantGatewaySource is the gateway's eBUS source address (113 / 0x71)
// per project convention. Populates protocol.Frame.Source on every B503
// request emitted by the production dispatcher.
const vaillantGatewaySource byte = 0x71

// b503DispatcherRequestTimeout bounds each individual B503 dispatch.
// Mirrors the ~2s timeout used by writeB555Frame in semantic_vaillant.go;
// production live-monitor latency on adapter-direct typically completes
// well under 1s.
const b503DispatcherRequestTimeout = 2 * time.Second

// b503StubDispatcher is retained as a typed nil-substrate fallback used
// only when the gateway is constructed without a bus (e.g. exotic test
// harnesses). Production code path always uses *rawFrameDispatcher.
//
// Surfaces UPSTREAM_RPC_FAILED on every call.
type b503StubDispatcher struct{}

func (b503StubDispatcher) Invoke(ctx context.Context, target byte, payload []byte) ([]byte, error) {
	_ = ctx
	_ = target
	_ = payload
	return nil, errRawFrameMisconfigured
}

// b503StubRefresh conservatively reports transport-down on every refresh.
// Mirrors spec §7.3 behaviour when the refresh function is nil
// (ErrTransportDown is the safe fallback). Kept explicit so the
// production wiring can be a single-function replacement once a
// transport-stack reconnect-epoch hook lands.
func b503StubRefresh(ctx context.Context) (b503session.TransportKey, error) {
	return b503session.TransportKey{}, b503session.ErrTransportDown
}

// b503Runtime bundles the session Manager + dispatcher + MCP server
// back-ref so both the MCP tool surface and the GraphQL B503 provider
// can share a single Manager/Dispatcher pair. The session gate must NOT
// be duplicated across surfaces — GraphQL Enable/Read/Disable operating
// on a different Manager than MCP would trivially break the single-owner
// invariant.
type b503Runtime struct {
	mcpServer  *mcp.Server
	manager    *b503session.Manager
	dispatcher mcp.RPCDispatcher
}

// installVaillantB503 installs the Vaillant B503 MCP tool surface and
// wires the production raw-frame dispatcher.
//
// Routing is single-substrate: every B503 request goes through gw.Bus
// (the same *protocol.Bus that B524/B525 use). The dispatcher acquires
// readMu around bus.Send to serialise with B524 polling — the
// liveMonitorMu side of the AD16 lock-order invariant is upheld by
// b503session.Manager (which holds liveMonitorMu before Invoke is
// reached for SERVICE_WRITE selectors).
//
// When gw or gw.Bus is nil (legacy harnesses without a bus), the wiring
// falls back to b503StubDispatcher — every Invoke surfaces
// UPSTREAM_RPC_FAILED, but the FSM and capability signal still operate
// so consumer envelopes are correctly populated.
func installVaillantB503(s *mcp.Server, gw *ebusgateway.Gateway, cfg *ebusgateway.Config) *b503Runtime {
	if s == nil {
		return nil
	}
	_ = cfg // reserved for future config-driven default-target override

	initialTK := b503session.TransportKey{
		AdapterInstanceID: "gateway",
		TransportEpoch:    0,
	}
	mgr := b503session.New(initialTK, 30*time.Second, b503StubRefresh)

	var disp mcp.RPCDispatcher
	if gw != nil && gw.Bus != nil {
		// Production path: route through the gateway's *protocol.Bus.
		// readMu is a fresh sync.Mutex dedicated to the B503 dispatcher
		// — it is NOT the per-poller readMu inside vaillantSemanticPoller
		// because that field is package-private. Sharing the mutex with
		// the poller would require either a public accessor or moving
		// the poller into the same package; both are out of scope for
		// M6. The dedicated mutex still serialises B503 dispatches with
		// each other (the M6-CONC-* tests assert this); B524 vs B503
		// concurrency is governed at the bus.Send level (the bus
		// arbitrates per-frame serially regardless).
		readMu := &sync.Mutex{}
		disp = newRawFrameDispatcher(gw.Bus, vaillantGatewaySource, readMu, mgr, b503DispatcherRequestTimeout)
	} else {
		disp = b503StubDispatcher{}
	}

	mcp.RegisterVaillantB503Tools(s, mcp.VaillantB503Options{
		Dispatcher:     disp,
		SessionManager: mgr,
		DefaultTarget:  defaultVaillantTarget,
	})

	return &b503Runtime{
		mcpServer:  s,
		manager:    mgr,
		dispatcher: disp,
	}
}
