package main

// M2a_GATEWAY_MCP (execution-plans#19) — production wiring entry point for
// the Vaillant B503 MCP tool surface.
//
// This file installs the tools on the MCP server with:
//
//   - a *deferred dispatcher stub* for raw (family, selector) frame
//     dispatch — production routing is scheduled as a follow-up under
//     M2b/M3, at which point the stub will be replaced by a real bridge
//     into the existing adaptermux / router substrate;
//
//   - a real b503session.Manager with conservative defaults: idle timeout
//     30s (spec §7.6), and a refresh function that conservatively
//     signals ErrTransportDown until the gateway transport stack offers
//     a reconnect-epoch hook. Concurrency semantics (owner-conditional
//     release, generation-guarded idle timer, EXPIRED-never-leaks) are
//     fully active and will carry over unchanged when the real
//     dispatcher lands.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/b503session"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

// defaultVaillantTarget is the BAI00 primary address used when a caller
// omits `target_address`. 0x08 is the canonical Vaillant boiler address
// per existing gateway precedent (semantic_vaillant.go).
const defaultVaillantTarget byte = 0x08

// errB503DispatcherNotWired is surfaced by the stub dispatcher until
// production B503 raw-frame wiring is implemented. It classifies as
// UPSTREAM_RPC_FAILED in the MCP envelope (mcp/vaillant_b503.go
// errUpstreamRPCFailed path).
var errB503DispatcherNotWired = errors.New("b503: production raw-frame dispatch not yet wired — see execution-plans#19 M2b/M3 follow-up")

// b503StubDispatcher is a placeholder that surfaces a clear error on
// every call. The session FSM, envelope shape, capability signal, and
// forbidden-surface guards are all live; only the actual bus dispatch
// is deferred.
type b503StubDispatcher struct{}

func (b503StubDispatcher) Invoke(ctx context.Context, target byte, payload []byte) ([]byte, error) {
	return nil, fmt.Errorf("%w (target=%#x, payload=%d bytes)", errB503DispatcherNotWired, target, len(payload))
}

// b503StubRefresh conservatively reports transport-down on every
// refresh. This mirrors the spec §7.3 behaviour when the refresh function
// is nil (ErrTransportDown is the safe fallback), but is explicit here so
// the production wiring swap can be a single-function replacement.
func b503StubRefresh(ctx context.Context) (b503session.TransportKey, error) {
	return b503session.TransportKey{}, b503session.ErrTransportDown
}

func installVaillantB503(s *mcp.Server, gw *ebusgateway.Gateway, cfg *ebusgateway.Config) {
	if s == nil {
		return
	}
	_ = gw  // reserved for future real-dispatcher bridge
	_ = cfg // reserved for future config-driven default-target override

	initialTK := b503session.TransportKey{
		AdapterInstanceID: "gateway",
		TransportEpoch:    0,
	}
	mgr := b503session.New(initialTK, 30*time.Second, b503StubRefresh)

	mcp.RegisterVaillantB503Tools(s, mcp.VaillantB503Options{
		Dispatcher:     b503StubDispatcher{},
		SessionManager: mgr,
		DefaultTarget:  defaultVaillantTarget,
	})
}
