package main

// M6_DISPATCHER_BRIDGE (execution-plans#19, amendment-1) — production
// raw-frame dispatcher for the Vaillant B503 selector family.
//
// RED-PHASE PLACEHOLDER. This file currently exposes the production
// dispatcher TYPE and SENTINELS so the M6 RED test-suite can compile and
// run; every Invoke returns errRawFrameNotImplemented so the suite is
// FAIL until the IMPL commit fills in the routing through *protocol.Bus.
// Once IMPL lands:
//   - Invoke routes through bus.Send with the (Source, Target, PB=B5,
//     SB=03, payload) frame shape (same substrate as B524/B525).
//   - readMu is acquired around bus.Send (lock order liveMonitorMu → readMu
//     per §12.6 / AD16; Manager already holds liveMonitorMu when the
//     SERVICE_WRITE 00 03 selector flows through here).
//   - Epoch is captured at issue-time and re-checked at completion-time
//     to enforce AD18 row 8 stale-frame discipline.
//   - Transport-down errors fire mgr.OnTransportDisconnect() so AD04
//     quiesce-release runs.

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/b503session"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// b503Bus is the minimal *protocol.Bus surface the dispatcher uses. The
// production gateway hands in a real *protocol.Bus; tests inject a mock
// satisfying this interface.
type b503Bus interface {
	Send(ctx context.Context, frame protocol.Frame) (*protocol.Frame, error)
}

// Public B503 protocol header constants. PB=0xB5/SB=0x03 are populated by
// the dispatcher into the protocol.Frame envelope; they are NOT part of
// the caller-supplied payload (§12.2).
const (
	b503PrimaryByte   byte = 0xB5
	b503SecondaryByte byte = 0x03
)

// Sentinel errors surfaced by the production raw-frame dispatcher. Tests
// classify via errors.Is. Each maps to a public surface per
// helianthus-docs-ebus B503.md §12.4.
var (
	// errRawFrameTransportDown indicates the bus / adaptermux is
	// disconnected. Maps to public TRANSPORT_DOWN.
	errRawFrameTransportDown = errors.New("b503dispatcher: transport down")
	// errRawFrameUpstreamTimeout indicates ctx.Done fired before the bus
	// produced a reply. Maps to public UPSTREAM_TIMEOUT.
	errRawFrameUpstreamTimeout = errors.New("b503dispatcher: ctx canceled before bus turnaround")
	// errRawFrameUpstreamRPCFailed indicates a NAK / CRC / generic protocol
	// error from the bus. Maps to public UPSTREAM_RPC_FAILED.
	errRawFrameUpstreamRPCFailed = errors.New("b503dispatcher: upstream rpc failed")
	// errRawFrameStaleEpoch indicates the request's captured epoch no
	// longer matches the Manager's current epoch — a transport rollover
	// happened between issue and completion. The reply is discarded and
	// never surfaced to consumers as a successful payload (§12.7).
	errRawFrameStaleEpoch = errors.New("b503dispatcher: stale-epoch reply discarded")
	// errRawFrameMalformedPayload indicates the caller-supplied payload
	// erroneously begins with the namespace bytes b5 03. Per §12.2 the
	// payload starts with the (family, selector) prefix; PB/SB live in
	// the Frame envelope.
	errRawFrameMalformedPayload = errors.New("b503dispatcher: payload must not start with b5 03 (PB/SB live in Frame envelope, not payload)")
	// errRawFrameMisconfigured indicates the dispatcher was constructed
	// with a nil bus or nil manager.
	errRawFrameMisconfigured = errors.New("b503dispatcher: misconfigured (nil bus or manager)")
	// errRawFrameNotImplemented is returned by every Invoke during the
	// M6 RED phase; the IMPL commit replaces it with real routing.
	errRawFrameNotImplemented = errors.New("b503dispatcher: production routing not implemented (M6 RED phase)")
)

// rawFrameDispatcher is the production B503 frame routing component.
// It implements mcp.RPCDispatcher.
type rawFrameDispatcher struct {
	bus            b503Bus
	source         byte
	readMu         *sync.Mutex
	mgr            *b503session.Manager
	requestTimeout time.Duration
}

// newRawFrameDispatcher constructs a production dispatcher. See file-level
// comment for parameter semantics.
func newRawFrameDispatcher(bus b503Bus, source byte, readMu *sync.Mutex, mgr *b503session.Manager, requestTimeout time.Duration) *rawFrameDispatcher {
	return &rawFrameDispatcher{
		bus:            bus,
		source:         source,
		readMu:         readMu,
		mgr:            mgr,
		requestTimeout: requestTimeout,
	}
}

// Invoke implements mcp.RPCDispatcher. RED-PHASE: returns
// errRawFrameNotImplemented unconditionally. The IMPL commit replaces
// this body with real bus.Send-based routing.
func (d *rawFrameDispatcher) Invoke(ctx context.Context, target byte, payload []byte) ([]byte, error) {
	_ = target
	_ = payload
	if ctx == nil {
		ctx = context.Background()
	}
	_ = ctx
	return nil, errRawFrameNotImplemented
}
