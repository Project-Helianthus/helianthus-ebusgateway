package main

// M6_DISPATCHER_BRIDGE (execution-plans#19, amendment-1) — production
// raw-frame dispatcher for the Vaillant B503 selector family.
//
// This file is the production replacement for `b503StubDispatcher{}`
// previously injected in `installVaillantB503`. Routing is single-substrate:
// every B503 request goes through the gateway's existing `*protocol.Bus.Send`
// path (the same substrate used for B524/B525 — see
// `cmd/gateway/semantic_vaillant.go::writeB555Frame`). No parallel transport
// is opened (AD16).
//
// Doc-gate companion: helianthus-docs-ebus protocols/vaillant/ebus-vaillant-B503.md
// §12 (Production dispatcher contract). Plan canonical SHA
// `86495340799be9340dc191c371a49a958f65c357c76a1e0a2974502c8489b508`.
//
// Lock acquisition order is INVARIANT: liveMonitorMu → readMu (§12.6).
// `b503session.Manager` already holds liveMonitorMu when the SERVICE_WRITE
// (00 03) selector flows through here. The dispatcher only acquires readMu
// (the B524 poll mutex) — never liveMonitorMu — to preserve the order.
//
// Epoch-tagged in-flight requests (§12.7 / AD18 row 8): each Invoke captures
// the Manager's transport epoch at issue-time and re-checks it against the
// current epoch at completion. A stale-epoch reply (epoch advanced between
// issue and completion) is silently discarded — the late frame MUST NOT
// satisfy any post-reconnect waiter or mutate capability state.

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	// disconnected. Maps to public TRANSPORT_DOWN. Wraps b503session.
	// ErrTransportDown so the existing capability-probe code path
	// (mcp/vaillant_b503.go::VaillantB503AvailabilityCtx) which uses
	// `errors.Is(_, b503session.ErrTransportDown)` keeps working
	// unchanged.
	errRawFrameTransportDown = errors.New("b503dispatcher: transport down")
	// errRawFrameUpstreamTimeout indicates ctx.Done fired before the bus
	// produced a reply. Maps to public UPSTREAM_TIMEOUT.
	errRawFrameUpstreamTimeout = errors.New("b503dispatcher: ctx canceled before bus turnaround")
	// errRawFrameUpstreamRPCFailed indicates a NAK / CRC / generic
	// protocol error from the bus. Maps to public UPSTREAM_RPC_FAILED.
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
	// errRawFrameSourceNotAdmitted indicates startup admission has not yet
	// made a usable source available for gateway-owned B503 traffic.
	errRawFrameSourceNotAdmitted = errors.New("b503dispatcher: source not admitted")
)

// rawFrameDispatcher is the production B503 frame routing component.
// It implements mcp.RPCDispatcher.
//
// `readMu` is held as a `sync.Locker` (interface) so the M6-CONC
// concurrency tests can substitute a build-tagged tracing implementation
// that records every Lock/Unlock event the dispatcher actually performs
// (R1 P2 fix). Production callers pass a `*sync.Mutex`, which satisfies
// `sync.Locker` natively — no production behaviour change.
type rawFrameDispatcher struct {
	bus            b503Bus
	source         byte
	sourceProvider func() (byte, bool)
	readMu         sync.Locker
	mgr            *b503session.Manager
	requestTimeout time.Duration
}

// newRawFrameDispatcher constructs a production dispatcher.
//
//   - bus: the gateway's *protocol.Bus (or a test mock). The single
//     bus-access primitive — §12.3 / AD16.
//   - source: the gateway's admitted eBUS initiator address. Populates
//     Frame.Source.
//   - readMu: the B524 read mutex. Acquired around bus.Send to serialise
//     B503 frames with B524 polling. May be nil for tests that don't
//     care about B524 contention. The Manager has already grabbed
//     liveMonitorMu for SERVICE_WRITE selectors; the dispatcher only
//     acquires readMu, preserving the AD16 lock-order invariant.
//   - mgr: the b503session.Manager — needed to capture the epoch at
//     issue-time (AD18) and to call OnTransportDisconnect on
//     transport-loss (AD04 quiesce-release fires).
//   - requestTimeout: the per-Invoke timeout. Zero or negative means
//     "use ctx as-is"; a positive value bounds bus.Send via a derived
//     context.
func newRawFrameDispatcher(bus b503Bus, source byte, readMu sync.Locker, mgr *b503session.Manager, requestTimeout time.Duration) *rawFrameDispatcher {
	return newRawFrameDispatcherWithSourceProvider(bus, func() (byte, bool) {
		return source, source != 0
	}, readMu, mgr, requestTimeout)
}

func newRawFrameDispatcherWithSourceProvider(bus b503Bus, sourceProvider func() (byte, bool), readMu sync.Locker, mgr *b503session.Manager, requestTimeout time.Duration) *rawFrameDispatcher {
	return &rawFrameDispatcher{
		bus:            bus,
		sourceProvider: sourceProvider,
		readMu:         readMu,
		mgr:            mgr,
		requestTimeout: requestTimeout,
	}
}

// Invoke implements mcp.RPCDispatcher.
//
// Contract (helianthus-docs-ebus B503.md §12.2):
//
//   - target is the destination device address (e.g. 0x08 BAI00).
//   - payload is the L7 request body whose first 2 bytes are the
//     (family, selector) prefix. The dispatcher MUST reject a payload
//     that begins with `b5 03` (those bytes belong in the Frame envelope,
//     not in the payload).
//   - On success returns the response data exactly as delivered by
//     bus.Send (no B503-specific stripping).
//   - On ctx.Done before bus turnaround → wraps errRawFrameUpstreamTimeout.
//   - On bus / transport disconnect → wraps errRawFrameTransportDown
//     AND fires mgr.OnTransportDisconnect() to release any held session.
//     The wrapped error chain ALSO matches errors.Is(_,
//     b503session.ErrTransportDown) so the existing capability probe in
//     mcp/vaillant_b503.go classifies it correctly without modification.
//   - On NAK / CRC / generic protocol failure → wraps
//     errRawFrameUpstreamRPCFailed.
//   - On stale-epoch completion → wraps errRawFrameStaleEpoch (caller
//     never sees data; capability stays last-known per AD18 row 8).
func (d *rawFrameDispatcher) Invoke(ctx context.Context, target byte, payload []byte) ([]byte, error) {
	if d == nil || d.bus == nil || d.mgr == nil {
		return nil, errRawFrameMisconfigured
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// §12.2 explicit reject: payload must not include PB/SB. A naive
	// caller that prepended `b5 03` would otherwise produce a wire frame
	// with `b5 03 b5 03 …` after the Frame envelope adds its own primary/
	// secondary bytes — silent corruption.
	if len(payload) >= 2 && payload[0] == b503PrimaryByte && payload[1] == b503SecondaryByte {
		return nil, errRawFrameMalformedPayload
	}
	source, admitted := d.admittedSource()
	if !admitted || source == 0 {
		return nil, fmt.Errorf("%w: %w", errRawFrameSourceNotAdmitted, b503session.ErrTransportDown)
	}

	// Capture the current epoch at issue-time. AD18 + R3 A2 fix: epoch
	// comparison is request-side metadata, populated when the request
	// leaves Invoke, NOT re-derived from Manager at receive time. A
	// reply from epoch N arriving after a rollover to N+1 must be
	// rejected without the Manager being consulted at completion-time
	// in a way that could "rescue" the stale frame.
	startEpoch := d.mgr.TransportKey().TransportEpoch

	// Optional internal timeout. The caller's ctx still bounds the call
	// regardless — whichever fires first wins.
	bsCtx := ctx
	var cancel context.CancelFunc
	if d.requestTimeout > 0 {
		bsCtx, cancel = context.WithTimeout(ctx, d.requestTimeout)
		defer cancel()
	}

	// Acquire readMu to serialise with B524 polling. AD16 + §12.6: the
	// dispatcher only takes readMu here. liveMonitorMu was already
	// acquired by Manager.Enable for the SERVICE_WRITE 00 03 selector,
	// preserving the lock order liveMonitorMu → readMu.
	if d.readMu != nil {
		d.readMu.Lock()
	}
	frame := protocol.Frame{
		FrameType: protocol.FrameTypeInitiatorTarget,
		Source:    source,
		Target:    target,
		Primary:   b503PrimaryByte,
		Secondary: b503SecondaryByte,
		Data:      payload,
	}
	resp, sendErr := d.bus.Send(bsCtx, frame)
	if d.readMu != nil {
		d.readMu.Unlock()
	}

	// Stale-epoch discipline (§12.7): re-check the Manager's current
	// epoch against the captured startEpoch BEFORE classifying or
	// returning. A rollover during the Send window means this reply
	// belongs to a dead incarnation and must be discarded. We
	// deliberately do not consult the Manager's other state for this
	// decision — the stored epoch alone is sufficient.
	endEpoch := d.mgr.TransportKey().TransportEpoch
	if endEpoch != startEpoch {
		// Discard regardless of success/error. The waiter (Invoke
		// caller) sees errRawFrameStaleEpoch; consumer code path treats
		// this as a non-mutating completion (capability stays last-known
		// per AD18 row 8). Do NOT call OnTransportDisconnect here — by
		// the time the epoch advanced, the Manager has already been
		// notified by whatever drove the rollover.
		return nil, errRawFrameStaleEpoch
	}

	if sendErr != nil {
		return nil, d.classifySendErr(ctx, bsCtx, sendErr)
	}
	if resp == nil {
		// Defensive: bus.Send returned (nil, nil). Treat as protocol
		// failure rather than panic on response.Data dereference.
		return nil, fmt.Errorf("%w: nil response frame", errRawFrameUpstreamRPCFailed)
	}
	return resp.Data, nil
}

func (d *rawFrameDispatcher) admittedSource() (byte, bool) {
	if d == nil {
		return 0, false
	}
	if d.sourceProvider != nil {
		return d.sourceProvider()
	}
	return d.source, d.source != 0
}

// classifySendErr maps bus.Send errors to dispatcher sentinels per
// §12.4. The mapping is conservative:
//
//   - transport-closed / queue-full → TRANSPORT_DOWN AND fire
//     OnTransportDisconnect so AD04 quiesce-release runs. The returned
//     error chain matches errors.Is(_, b503session.ErrTransportDown)
//     so the existing capability probe classifies it correctly. This
//     check runs FIRST (R1 P1 fix) — a sendErr that signals transport
//     loss MUST be classified as TRANSPORT_DOWN even when ctx.Done()
//     also fired in the same window, otherwise the live-monitor session
//     gate stays held until idle expiry and consumers see SESSION_BUSY
//     instead of TRANSPORT_DOWN.
//   - ctx.Done before bus turnaround (with no transport-down signal in
//     sendErr) → UPSTREAM_TIMEOUT.
//   - everything else → UPSTREAM_RPC_FAILED (NAK / CRC / generic).
//
// We err on the side of TRANSPORT_DOWN ONLY when bus.Send tells us
// explicitly the transport is closed; ambiguous bus errors remain
// UPSTREAM_RPC_FAILED so legitimate B503 protocol failures are not
// collapsed into transport errors (§12.4 discriminator rules).
func (d *rawFrameDispatcher) classifySendErr(callerCtx, bsCtx context.Context, sendErr error) error {
	// Transport-down signal in sendErr takes precedence over ctx-cancel
	// classification. A timed-out call whose underlying transport was
	// dropped MUST trigger AD04 quiesce-release; otherwise the
	// live-monitor session gate would stay held and surface SESSION_BUSY
	// instead of TRANSPORT_DOWN to the next caller (R1 P1 fix).
	if isTransportDownErr(sendErr) {
		if d.mgr != nil {
			d.mgr.OnTransportDisconnect()
		}
		// errors.Join lets the resulting error chain match BOTH
		// errRawFrameTransportDown (M6 sentinel) AND
		// b503session.ErrTransportDown (capability-probe sentinel).
		return errors.Join(
			fmt.Errorf("%w: %v", errRawFrameTransportDown, sendErr),
			b503session.ErrTransportDown,
		)
	}

	// No transport-down signal in sendErr: ctx-cancel classification
	// runs second. If the caller's context is done, the operation timed
	// out / was canceled.
	if cerr := callerCtx.Err(); cerr != nil {
		return fmt.Errorf("%w: %v (phase=ctx_canceled)", errRawFrameUpstreamTimeout, cerr)
	}
	if cerr := bsCtx.Err(); cerr != nil {
		return fmt.Errorf("%w: %v (phase=ctx_canceled)", errRawFrameUpstreamTimeout, cerr)
	}

	// Everything else: NAK, CRC, generic protocol failure. Preserves the
	// underlying error chain via %w so tests can assert via errors.Is.
	return fmt.Errorf("%w: %v", errRawFrameUpstreamRPCFailed, sendErr)
}

// isTransportDownErr reports whether the bus.Send error indicates the
// transport itself is closed/unreachable (as distinct from a per-frame
// protocol failure). Detection sources:
//
//   - errors.Is(err, b503session.ErrTransportDown) — explicit signal
//     from a test harness or a refresh-driven path.
//   - text match on "transport closed" / "send queue full" — the public
//     ebusgo errors package surfaces these via fmt.Errorf wrappers and
//     does not re-export the sentinel symbols. String-matching the
//     stable text is the same approach used elsewhere in the gateway
//     (see main.go transport-handling code paths).
//
// Bus arbitration timeouts and other in-flight protocol failures are
// NOT classified as transport-down.
func isTransportDownErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, b503session.ErrTransportDown) {
		return true
	}
	if errors.Is(err, errRawFrameTransportDown) {
		return true
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "transport closed"):
		return true
	case strings.Contains(msg, "send queue full"):
		return true
	}
	return false
}
