package main

// The B503 raw-frame dispatcher sends through the b503Bus interface.
// Production supplies the gateway bus backed by the generation-bound substrate;
// wiring falls back to a stub when the bus or source provider is unavailable.
// The source provider must admit a nonzero source before dispatch and is
// checked again after acquiring the dispatcher lock, before emission.
// A pending operation also requires the Manager's emission admission.
// Entering bus.Send is the conservative may-have-emitted boundary.
// The transport handles per-frame arbitration with other bus traffic.
//
// The Manager owns liveMonitorMu for the applicable SERVICE_WRITE operation.
// The dispatcher acquires readMu, which production dedicates to serializing
// B503 dispatches; it does not acquire liveMonitorMu or the poller's mutex.
//
// Each request captures its transport epoch before admission. A completion
// from a stale epoch is discarded before its response or error is classified;
// it cannot become a successful payload or replace last-known capability state.
//
// Source admission and the transport epoch are distinct checks: admission
// controls whether this request may proceed, while epoch comparison prevents
// a completion from an earlier transport generation being accepted.
// Context cancellation is also checked before the send boundary.
// Source withdrawal or bus-reported transport loss requests a disconnect
// correlated to the captured generation. An epoch-advance cancellation
// (b503session.ErrTransportDown from the Manager) is returned without a
// disconnect: an existing owner is retained for bounded refresh.
// Successful responses preserve the data delivered by the bus.
// The dispatcher does not open a separate transport.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/b503session"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// b503Bus is the send interface used by the dispatcher. Production supplies
// the gateway bus; tests may supply another implementation.
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
	bus                 b503Bus
	source              byte
	sourceProvider      func() (byte, bool)
	readMu              sync.Locker
	mgr                 *b503session.Manager
	requestTimeout      time.Duration
	disconnectIfCurrent func(b503session.TransportKey)
	// afterPendingEmission is a deterministic test seam. Production leaves it
	// nil; it runs while the Manager's emission fence is held and before
	// bus.Send enters the generation-bound substrate.
	afterPendingEmission func()
}

// newRawFrameDispatcher constructs a production dispatcher.
//
//   - bus: the gateway bus or another b503Bus implementation.
//     Send is the dispatcher's bus-access primitive.
//   - source: the gateway's admitted eBUS initiator address. Populates
//     Frame.Source.
//   - readMu: an optional lock acquired around bus.Send. Production uses
//     a dedicated mutex to serialize B503 dispatches. The Manager owns
//     liveMonitorMu for the applicable SERVICE_WRITE operation; the
//     dispatcher does not acquire that lock.
//   - mgr: the b503session.Manager captures the epoch at issue time and
//     participates in a generation-correlated disconnect on transport loss.
//     It also controls pending-operation emission admission.
//   - requestTimeout: the per-Invoke timeout. Zero or negative means
//     "use ctx as-is"; a positive value bounds bus.Send via a derived
//     context.
func newRawFrameDispatcher(bus b503Bus, source byte, readMu sync.Locker, mgr *b503session.Manager, requestTimeout time.Duration) *rawFrameDispatcher {
	return newRawFrameDispatcherWithSourceProvider(bus, func() (byte, bool) {
		return source, source != 0
	}, readMu, mgr, requestTimeout)
}

func newRawFrameDispatcherWithSourceProvider(bus b503Bus, sourceProvider func() (byte, bool), readMu sync.Locker, mgr *b503session.Manager, requestTimeout time.Duration) *rawFrameDispatcher {
	dispatcher := &rawFrameDispatcher{
		bus:            bus,
		sourceProvider: sourceProvider,
		readMu:         readMu,
		mgr:            mgr,
		requestTimeout: requestTimeout,
	}
	if mgr != nil {
		// Standalone/test dispatchers still require the same serialized,
		// generation-correlated disconnect boundary as production wiring.
		// installVaillantB503 replaces this private gate with the process-owned
		// b503Runtime shared by DriverManager lifecycle notifications.
		lifecycle := &b503Runtime{manager: mgr}
		dispatcher.disconnectIfCurrent = lifecycle.disconnectIfCurrent
	}
	return dispatcher
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
//     and requests a generation-correlated disconnect for an owned session.
//     The wrapped error chain ALSO matches errors.Is(_,
//     b503session.ErrTransportDown) so the existing capability probe in
//     mcp/vaillant_b503.go classifies it correctly without modification.
//   - On NAK / CRC / generic protocol failure → wraps
//     errRawFrameUpstreamRPCFailed.
//   - On stale-epoch completion → wraps errRawFrameStaleEpoch (caller
//     never sees data; capability stays last-known per AD18 row 8).
func (d *rawFrameDispatcher) Invoke(ctx context.Context, target byte, payload []byte) ([]byte, error) {
	outcome := d.invokeB503Outcome(ctx, target, payload)
	return outcome.Response, outcome.Err
}

func (d *rawFrameDispatcher) invokeB503Outcome(ctx context.Context, target byte, payload []byte) mcp.B503DispatchOutcome {
	if d == nil || d.bus == nil || d.mgr == nil {
		return mcp.B503DispatchOutcome{Err: errRawFrameMisconfigured}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	transportAtIssue := d.mgr.TransportKey()
	if err := ctx.Err(); err != nil {
		return mcp.B503DispatchOutcome{
			Err:       classifyB503ContextErr(ctx, err),
			Transport: transportAtIssue,
		}
	}
	// §12.2 explicit reject: payload must not include PB/SB. A naive
	// caller that prepended `b5 03` would otherwise produce a wire frame
	// with `b5 03 b5 03 …` after the Frame envelope adds its own primary/
	// secondary bytes — silent corruption.
	if len(payload) >= 2 && payload[0] == b503PrimaryByte && payload[1] == b503SecondaryByte {
		return mcp.B503DispatchOutcome{Err: errRawFrameMalformedPayload, Transport: transportAtIssue}
	}
	source, admitted := d.admittedSource()
	if !admitted || source == 0 {
		// Source admission is intersected with DriverManager capability. A
		// source disappearing therefore means the issuer's generation has
		// withdrawn even when no transport error reached bus.Send. Release the
		// owner here as an idempotent fallback; the correlated lifecycle event
		// may already have performed the same disconnect and never advances the
		// B503 transport epoch.
		if d.disconnectIfCurrent != nil && d.mgr.IsOwned() {
			d.disconnectIfCurrent(transportAtIssue)
		}
		return mcp.B503DispatchOutcome{
			Err:       fmt.Errorf("%w: %w", errRawFrameSourceNotAdmitted, b503session.ErrTransportDown),
			Transport: transportAtIssue,
		}
	}

	// Capture the current epoch at issue-time. AD18 + R3 A2 fix: epoch
	// comparison is request-side metadata, populated when the request
	// leaves Invoke, NOT re-derived from Manager at receive time. A
	// reply from epoch N arriving after a rollover to N+1 must be
	// rejected without the Manager being consulted at completion-time
	// in a way that could "rescue" the stale frame.
	startEpoch := transportAtIssue.TransportEpoch

	// Optional internal timeout. The caller's ctx still bounds the call
	// regardless — whichever fires first wins.
	bsCtx := ctx
	var cancel context.CancelFunc
	if d.requestTimeout > 0 {
		bsCtx, cancel = context.WithTimeout(ctx, d.requestTimeout)
		defer cancel()
	}

	// Acquire readMu to serialize dispatcher sends. For the applicable
	// SERVICE_WRITE operation the Manager owns liveMonitorMu; the dispatcher
	// does not acquire it. Production uses a dedicated dispatcher mutex.
	if d.readMu != nil {
		d.readMu.Lock()
	}
	unlockReadMu := func() {
		if d.readMu != nil {
			d.readMu.Unlock()
		}
	}
	// A context or admitted source can become invalid while this operation
	// waits for the dispatcher lock. Revalidate both at the final
	// pre-emission boundary so terminal disconnect never drains a stale queued
	// B503 write after readMu becomes available. Check the caller first: a
	// derived request-timeout context can observe parent cancellation slightly
	// later, and that gap must not admit a B503 write.
	if err := ctx.Err(); err != nil {
		unlockReadMu()
		return mcp.B503DispatchOutcome{
			Err:       classifyB503ContextErr(ctx, err),
			Transport: transportAtIssue,
		}
	}
	if err := bsCtx.Err(); err != nil {
		unlockReadMu()
		return mcp.B503DispatchOutcome{
			Err:       classifyB503ContextErr(bsCtx, err),
			Transport: transportAtIssue,
		}
	}
	currentSource, currentAdmitted := d.admittedSource()
	if !currentAdmitted || currentSource == 0 || currentSource != source {
		unlockReadMu()
		if d.disconnectIfCurrent != nil && d.mgr.IsOwned() {
			d.disconnectIfCurrent(transportAtIssue)
		}
		return mcp.B503DispatchOutcome{
			Err:       fmt.Errorf("%w: %w", errRawFrameSourceNotAdmitted, b503session.ErrTransportDown),
			Transport: transportAtIssue,
		}
	}
	frame := protocol.Frame{
		FrameType: protocol.FrameTypeInitiatorTarget,
		Source:    source,
		Target:    target,
		Primary:   b503PrimaryByte,
		Secondary: b503SecondaryByte,
		Data:      payload,
	}
	// From this point onward every outcome is conservatively emitted. The
	// transport API offers no finer acknowledgement boundary than entering
	// bus.Send.
	var releaseEmission func()
	if operationID, ok := b503session.PendingOperationIDFromContext(ctx); ok {
		var admitted bool
		releaseEmission, admitted = d.mgr.BeginPendingEmission(operationID, target)
		if !admitted {
			unlockReadMu()
			return mcp.B503DispatchOutcome{
				Err:       b503session.ErrTransportDown,
				Transport: transportAtIssue,
			}
		}
		if d.afterPendingEmission != nil {
			d.afterPendingEmission()
		}
	}
	var resp *protocol.Frame
	var sendErr error
	if releaseEmission != nil {
		func() {
			defer releaseEmission()
			resp, sendErr = d.bus.Send(bsCtx, frame)
		}()
	} else {
		resp, sendErr = d.bus.Send(bsCtx, frame)
	}
	unlockReadMu()

	// Stale-epoch discipline (§12.7): re-check the Manager's current
	// epoch against the captured startEpoch BEFORE classifying or
	// returning. A rollover during the Send window means this reply
	// belongs to a dead incarnation and must be discarded. We
	// deliberately do not consult the Manager's other state for this
	// decision — the stored epoch alone is sufficient.
	endEpoch := d.mgr.DispatchEpoch()
	if endEpoch != startEpoch {
		// Discard regardless of success/error. The waiter (Invoke
		// caller) sees errRawFrameStaleEpoch; consumer code path treats
		// this as a non-mutating completion (capability stays last-known
		// per AD18 row 8). Do NOT call OnTransportDisconnect here — by
		// the time the epoch advanced, the Manager has already been
		// notified by whatever drove the rollover.
		return mcp.B503DispatchOutcome{
			Emitted: true, Native: b503session.NativeAmbiguous,
			Err: errRawFrameStaleEpoch, Transport: transportAtIssue,
		}
	}

	if sendErr != nil {
		native := b503session.NativeAmbiguous
		if errors.Is(sendErr, ebuserrors.ErrNACK) {
			native = b503session.NativeNAK
		}
		return mcp.B503DispatchOutcome{
			Emitted: true, Native: native,
			Err: d.classifySendErr(ctx, bsCtx, transportAtIssue, sendErr), Transport: transportAtIssue,
		}
	}
	if resp == nil {
		// Defensive: bus.Send returned (nil, nil). Treat as protocol
		// failure rather than panic on response.Data dereference.
		return mcp.B503DispatchOutcome{
			Emitted: true, Native: b503session.NativeAmbiguous,
			Err: fmt.Errorf("%w: nil response frame", errRawFrameUpstreamRPCFailed), Transport: transportAtIssue,
		}
	}
	return mcp.B503DispatchOutcome{
		Response: resp.Data, Emitted: true, Native: b503session.NativeACK, Transport: transportAtIssue,
	}
}

// InvokeB503Outcome is the B503-only lifecycle extension. The generic Invoke
// contract remains unchanged. A pre-cancelled or lifecycle-invalidated request
// cannot reach bus.Send; after admission all other errors are conservatively
// may-have-emitted until the transport API grows a stronger acknowledgement
// boundary.
func (d *rawFrameDispatcher) InvokeB503Outcome(ctx context.Context, target byte, payload []byte) mcp.B503DispatchOutcome {
	return d.invokeB503Outcome(ctx, target, payload)
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
//   - transport-closed / queue-full → TRANSPORT_DOWN AND run the
//     generation-correlated disconnect so AD04 quiesce-release runs. The returned
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
func (d *rawFrameDispatcher) classifySendErr(callerCtx, bsCtx context.Context, transportAtIssue b503session.TransportKey, sendErr error) error {
	// Transport-down signal in sendErr takes precedence over ctx-cancel
	// classification. A timed-out call whose underlying transport was
	// dropped MUST trigger AD04 quiesce-release; otherwise the
	// live-monitor session gate would stay held and surface SESSION_BUSY
	// instead of TRANSPORT_DOWN to the next caller (R1 P1 fix). Correlation
	// to transportAtIssue prevents a stale generation-N completion from
	// disconnecting the issuer admitted on generation N+1.
	if isTransportDownErr(sendErr) {
		if d.disconnectIfCurrent != nil && d.mgr.IsOwned() {
			d.disconnectIfCurrent(transportAtIssue)
		}
		// errors.Join lets the resulting error chain match BOTH
		// errRawFrameTransportDown (M6 sentinel) AND
		// b503session.ErrTransportDown (capability-probe sentinel).
		return errors.Join(
			fmt.Errorf("%w: %v", errRawFrameTransportDown, sendErr),
			b503session.ErrTransportDown,
		)
	}
	if cause := context.Cause(callerCtx); errors.Is(cause, b503session.ErrTransportDown) {
		return b503session.ErrTransportDown
	}
	if cause := context.Cause(bsCtx); errors.Is(cause, b503session.ErrTransportDown) {
		return b503session.ErrTransportDown
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

// classifyB503ContextErr distinguishes a caller deadline/cancellation from a
// Manager lifecycle invalidation. The latter is a transport withdrawal, so it
// must remain visible as TRANSPORT_DOWN while ordinary caller cancellation
// preserves the upstream-timeout public mapping.
func classifyB503ContextErr(ctx context.Context, err error) error {
	if errors.Is(context.Cause(ctx), b503session.ErrTransportDown) {
		return b503session.ErrTransportDown
	}
	return fmt.Errorf("%w: %v", errRawFrameUpstreamTimeout, err)
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
