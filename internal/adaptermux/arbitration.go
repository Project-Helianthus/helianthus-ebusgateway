package adaptermux

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// errStaleStartRequest is delivered to the notify channel of an
// external pending START whose enqueued-at age exceeds
// Config.PendingStartTTL by the time tryGrant pops it. ebusd (and
// other external clients) typically have a ~50ms local arbitration
// deadline; delivering a STARTED past that window produces "won in
// invalid state" errors on their side. Returning a clean failure
// instead lets them re-request when they actually want the bus.
//
// Proxy-bug C3 (R3).
var errStaleStartRequest = errors.New("adaptermux: pending START rejected — exceeded PendingStartTTL")

// arbitrator manages bus ownership between the gateway (internal) and
// external sessions (ebusd) using a two-class priority model with an
// adaptive fairness window.
//
// XR_GATEWAY_PRIORITY (default behavior, no external sessions):
//
//	At each SYN boundary (or idle tick), tryGrant checks pendingGateway
//	BEFORE pendingExternal. The gateway always gets first pick for bus
//	ownership.
//
//	The gateway's semantic poller drives periodic register scans that
//	must complete within their polling window; treating it as the
//	priority class minimises poll-window jitter when no external
//	clients are connected.
//
// XR_EXTERNAL_FAIRNESS_WINDOW (when ≥1 external session has a pending
// START AND fairnessCounter has reached FairnessRatio — F-4 fix per
// EBUSD-VERIFICATION-2026-05-10.md):
//
//	Every FairnessRatio'th tryGrant rotation with both classes pending,
//	the external FIFO is serviced before pendingGateway. This bounds
//	the worst-case external START latency to ~FairnessRatio gateway-
//	transaction windows even when the semantic poller is continuously
//	busy, which restores ebusd's ability to complete its initial scan
//	(its per-START retry budget is 3× ~1.5 s = 4.5 s; the gateway's
//	typical 200–300 ms grants comfortably fit into 1-in-4 fairness).
//
//	When no external sessions are pending, fairness reduces to a no-op
//	and gateway-priority is unchanged — zero overhead in the standalone
//	helianthus deployment.
//
// The arbitrator is informed by the proxy's boundary-based arbitration
// (proxy M2) but simplified for a two-class model:
//   - Gateway (sessionID = gatewaySessionID): priority class
//   - External sessions: FIFO with adaptive fairness window
type arbitrator struct {
	mu sync.Mutex

	// pendingGateway holds the gateway's pending START request, if any.
	// Only one gateway request can be pending at a time.
	pendingGateway *startRequest

	// pendingExternal holds external sessions' pending START requests
	// in FIFO order. Each session can have at most one pending request.
	pendingExternal []*startRequest

	// hasOwner tracks whether the bus is currently owned.
	hasOwner bool

	// currentOwner is the session ID of the current bus owner.
	// Only valid when hasOwner is true.
	currentOwner uint64

	// currentInitiator is the eBUS source address of the current
	// owner's START request.
	currentInitiator byte

	// fairnessCounter tracks tryGrant rotations where BOTH gateway and
	// external have pending requests. When it reaches FairnessRatio,
	// the next such rotation grants to external FIFO instead of
	// gateway, then resets. F-4 anti-starvation. Only advances when
	// both classes have pending work — otherwise gateway-priority is
	// unchanged and the counter holds at its current value (no-op
	// when standalone).
	fairnessCounter int

	// nowFn is the time source the arbitrator uses for enqueuedAt
	// timestamps and PendingStartTTL comparisons. Defaults to
	// time.Now; tests override to run TTL drains deterministically.
	nowFn func() time.Time

	// pendingStartTTL is the maximum dwell time of an external pending
	// START before tryGrant drops + rejects it with errStaleStartRequest.
	// Zero disables the policy. Configured via Mux.cfg.PendingStartTTL.
	pendingStartTTL time.Duration
}

// FairnessRatio is the every-Nth grant that goes to external FIFO when
// both gateway and ≥1 external session have pending START requests.
// Default 4 = ~25% of grants under contention go to external,
// bounding worst-case external START latency to ~4 gateway-transaction
// windows (typical 200–300 ms each → ~1 s worst case, well within
// ebusd's ~4.5 s per-scan-iteration deadline).
//
// Constant for now; can be promoted to a config field if operators
// need to tune the balance between gateway poll-window jitter and
// external client throughput.
const FairnessRatio = 4

// startRequest represents a pending START arbitration request.
type startRequest struct {
	sessionID uint64
	initiator byte
	notify    chan startResult // receives exactly one result

	// enqueuedAt is the monotonic time the request was admitted to
	// the arbitrator. Used by tryGrant for PendingStartTTL drop-and-
	// reject on stale heads (proxy-bug C3 / R3).
	enqueuedAt time.Time

	// cancelled is set when a session re-submits a START for the
	// same session ID — the old request's notify channel is already
	// closed-out with granted=false in requestStart, but the adapter
	// may still return STARTED/FAILED for an in-flight grant that
	// was popped from pendingExternal before the replace. The mux
	// checks this flag on the delivery path and converts a late
	// STARTED into a FAILED to avoid handing the bus to a session
	// that already gave up. (Proxy-bug C4 / R4.)
	cancelled atomic.Bool
}

// startResult is the outcome of a START arbitration request.
type startResult struct {
	granted   bool
	cancelled bool // AM55: true when client-initiated SYN cancel — no FAILED delivery
	initiator byte // initiator byte for STARTED/FAILED payload fidelity
	err       error
}

// gatewaySessionID is the reserved session ID for the gateway's
// internal active path. External sessions use IDs > 0.
const gatewaySessionID uint64 = 0

func newArbitrator() *arbitrator {
	return &arbitrator{
		pendingExternal: make([]*startRequest, 0, 4),
		nowFn:           time.Now,
	}
}

// requestStart submits a START request for the given session.
// Returns a channel that will receive the result when arbitration
// completes (at the next SYN boundary or immediately if bus is idle).
//
// If the bus is currently idle, the request may be granted immediately
// via a call to tryGrant() by the mux loop.
func (a *arbitrator) requestStart(sessionID uint64, initiator byte) <-chan startResult {
	ch := make(chan startResult, 1)
	a.mu.Lock()
	defer a.mu.Unlock()
	req := &startRequest{
		sessionID:  sessionID,
		initiator:  initiator,
		notify:     ch,
		enqueuedAt: a.now(),
	}

	if sessionID == gatewaySessionID {
		// Cancel any existing gateway request. The old request is
		// also flagged cancelled so that — if it has already been
		// popped by tryGrant and is now in flight at the adapter —
		// the mux's delivery path can convert a late STARTED into
		// a FAILED instead of handing the bus to a session that
		// abandoned the grant. (Proxy-bug C4 / R4.)
		if a.pendingGateway != nil {
			a.pendingGateway.cancelled.Store(true)
			// F-17 (operator hand-off, batch-9): MUST set
			// startResult.cancelled = true so the waiter's
			// handleStart goroutine takes the silent-suppression
			// branch in session.go (around line 524 — same path
			// that AM55 SYN-cancel uses) instead of the
			// deliverFailed(initiator) branch that produces a
			// spurious ENHResFailed on the wire. Without this,
			// ebusd reads the FAILED as "lost arbitration to its
			// own initiator byte," retries within ~50 ms, and
			// triggers the positive-feedback loop where no
			// request ever reaches the adapter. pcap-confirmed
			// root cause of "ebusd never lands a frame."
			//
			// startRequest.cancelled (struct field) is separate
			// from startResult.cancelled (channel value):
			//   - The struct flag is checked by the mux's late-
			//     STARTED suppression path (C4/R4) when the
			//     request has already been popped into
			//     pendingStart.
			//   - The result flag is checked by the session's
			//     handleStart goroutine when the request is
			//     still in pendingExternal and gets a
			//     not-granted result.
			// Both flags must be set on a cancellation.
			a.pendingGateway.notify <- startResult{
				granted:   false,
				cancelled: true,
				initiator: a.pendingGateway.initiator,
			}
		}
		a.pendingGateway = req
	} else {
		// Remove any existing request from this session.
		for i, existing := range a.pendingExternal {
			if existing.sessionID == sessionID {
				existing.cancelled.Store(true)
				// F-17 (operator hand-off, batch-9): see the
				// gateway-path comment above. Without
				// `cancelled: true` here, ebusd's handleStart
				// receives a startResult{granted: false}
				// (default cancelled=false), takes the
				// deliverFailed branch in session.go, and
				// emits ENHResFailed(0x31) on the wire within
				// ~0.3 ms — far faster than the bus can
				// possibly arbitrate (eBUS bit-time ~4 ms).
				// ebusd reads it as "lost arbitration to my
				// own initiator byte 0x31" and retries within
				// ~50 ms, producing a same-session-replace
				// that cancels the new request, and so on.
				// Positive-feedback loop; no bid ever
				// reaches the adapter for real arbitration.
				existing.notify <- startResult{
					granted:   false,
					cancelled: true,
					initiator: existing.initiator,
				}
				a.pendingExternal = append(a.pendingExternal[:i], a.pendingExternal[i+1:]...)
				break
			}
		}
		a.pendingExternal = append(a.pendingExternal, req)
	}

	return ch
}

// markInFlightCancelled is called by the mux when a new requestStart
// arrives for a session whose previous grant has already been popped
// from pendingExternal/pendingGateway and is in flight at the adapter.
// The pendingStartState that the mux holds carries the *startRequest;
// flagging cancelled here is what handleArbitrationResponse checks to
// short-circuit a late STARTED into a FAILED. Returns the previous
// value of the flag so the caller can detect double-cancels.
// (Proxy-bug C4 / R4.)
func (a *arbitrator) markInFlightCancelled(req *startRequest) bool {
	if req == nil {
		return false
	}
	return req.cancelled.Swap(true)
}

// now returns the arbitrator's clock. Indirected for tests that need
// deterministic PendingStartTTL drains.
func (a *arbitrator) now() time.Time {
	if a.nowFn == nil {
		return time.Now()
	}
	return a.nowFn()
}

// setPolicy injects per-cycle policy fields the mux derives from
// Config. Called once on mux construction.
func (a *arbitrator) setPolicy(pendingStartTTL time.Duration) {
	a.mu.Lock()
	a.pendingStartTTL = pendingStartTTL
	a.mu.Unlock()
}

// cancelStart cancels a pending START request for the given session.
// Returns true if a request was found and cancelled.
//
// AM55 / F-17 follow-up (PR #626 review round-1, angry-tester finding
// F-2): callers are session.handleStart on a client-initiated SYN
// cancel (session.go:493). The session has signalled it no longer
// wants the bid, so the silent-return branch in handleStart
// (session.go:524 — gated on `result.cancelled`) is the correct
// resolution. Without `cancelled: true` on the notify, the old wait
// goroutine falls through to deliverFailed → ENHResFailed on the
// wire — exactly the failure mode F-17 was filed to fix. Pre-existing
// latent before this PR (no observed-wild repro) but trivially racy
// with the SYN-cancel-before-tryGrant window; fixed for symmetry with
// every other cancellation path in the arbitrator.
func (a *arbitrator) cancelStart(sessionID uint64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	if sessionID == gatewaySessionID {
		if a.pendingGateway != nil {
			a.pendingGateway.cancelled.Store(true)
			a.pendingGateway.notify <- startResult{granted: false, cancelled: true, initiator: a.pendingGateway.initiator}
			a.pendingGateway = nil
			return true
		}
		return false
	}

	for i, req := range a.pendingExternal {
		if req.sessionID == sessionID {
			req.cancelled.Store(true)
			req.notify <- startResult{granted: false, cancelled: true, initiator: req.initiator}
			a.pendingExternal = append(a.pendingExternal[:i], a.pendingExternal[i+1:]...)
			return true
		}
	}
	return false
}

// tryGrant attempts to select the highest-priority pending request
// for bus ownership. Called at SYN boundaries and when the bus becomes
// idle.
//
// busIdle is the caller's snapshot of "the wire has been quiet for at
// least one SYN interval AND the arbitrator does not believe it has
// granted ownership yet." On an idle bus the fairness rotation is
// counter-productive — there is no real contention to balance, so
// holding an external pending START in the queue until a fairness
// quantum elapses only wastes wall-clock that ebusd's local
// arbitration deadline counts against the gateway. When busIdle is
// true and external is pending, this function grants the external
// FIFO head immediately and does NOT advance the fairness counter.
// (Proxy-bug C1 / R1.)
//
// Ownership is NOT set here — the caller MUST call confirmOwnership
// after the adapter's StartArbitration succeeds (Codex P1 #3060199707:
// defer ownership until adapter START confirms). The caller MUST also
// send a startResult on the notify channel after success or failure.
//
// Returns (nil, false) if no requests are pending or the bus is
// already owned.
func (a *arbitrator) tryGrant(busIdle bool) (req *startRequest, granted bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.hasOwner {
		return nil, false
	}

	// Drain stale entries from the head of pendingExternal:
	// requests whose enqueuedAt has aged past PendingStartTTL get a
	// clean failure with errStaleStartRequest. ebusd's local
	// arbitration deadline is ~50 ms; delivering a STARTED past that
	// window produces "won in invalid state" errors on its side.
	// (Proxy-bug C3 / R3.) Drain ALWAYS before any grant decision so
	// stale heads can't claim grants the gateway might otherwise
	// have served.
	a.drainStalePendingExternalLocked()

	gatewayPending := a.pendingGateway != nil
	externalPending := len(a.pendingExternal) > 0

	// Bus-idle fast path: grant external immediately and skip the
	// fairness rotation entirely. The bus is wide open, so there's
	// no reason to defer external for a fairness quantum. Gateway
	// would also be granted on the next tryGrant invocation if it
	// has any pending work (caller invokes tryGrantAndStart on every
	// SYN boundary). (Proxy-bug C1 / R1.)
	if busIdle && externalPending {
		ext := a.pendingExternal[0]
		a.pendingExternal = a.pendingExternal[1:]
		return ext, true
	}

	// F-4 fairness window: when BOTH classes are pending AND the
	// fairness counter has reached the ratio, prefer external FIFO
	// this rotation. Reset the counter and fall through to the
	// external-FIFO branch below. When only one class is pending the
	// counter doesn't advance — gateway-priority is unchanged in the
	// no-external-clients case.
	preferExternalThisGrant := false
	if gatewayPending && externalPending {
		a.fairnessCounter++
		if a.fairnessCounter >= FairnessRatio {
			a.fairnessCounter = 0
			preferExternalThisGrant = true
		}
	}

	if gatewayPending && !preferExternalThisGrant {
		gw := a.pendingGateway
		a.pendingGateway = nil
		return gw, true
	}

	// External FIFO: grant to first external request. Reached when
	// (a) gateway has no pending request, or (b) fairness window
	// rotated this grant to external.
	if externalPending {
		ext := a.pendingExternal[0]
		a.pendingExternal = a.pendingExternal[1:]
		return ext, true
	}

	return nil, false
}

// drainStalePendingExternalLocked rejects any external pending START
// whose enqueuedAt is older than pendingStartTTL. Each stale entry's
// notify channel receives granted=false with errStaleStartRequest so
// the client (ebusd) can retry cleanly when it actually wants the bus
// again. (Proxy-bug C3 / R3.) Must be called with a.mu held.
func (a *arbitrator) drainStalePendingExternalLocked() {
	if a.pendingStartTTL <= 0 || len(a.pendingExternal) == 0 {
		return
	}
	now := a.now()
	keep := a.pendingExternal[:0]
	for _, req := range a.pendingExternal {
		if now.Sub(req.enqueuedAt) > a.pendingStartTTL {
			req.cancelled.Store(true)
			// Best-effort notify — buffer is 1, but a session that
			// has already drained the channel (replaced this
			// request meanwhile) would have left it empty, so the
			// send succeeds. Use non-blocking send for safety.
			select {
			case req.notify <- startResult{granted: false, initiator: req.initiator, err: errStaleStartRequest}:
			default:
			}
			continue
		}
		keep = append(keep, req)
	}
	// In-place rebuild to preserve capacity.
	for i := len(keep); i < len(a.pendingExternal); i++ {
		a.pendingExternal[i] = nil
	}
	a.pendingExternal = keep
}

// confirmOwnership sets bus ownership after the adapter's
// StartArbitration has succeeded. Must be called by tryGrantAndStart
// only on the success path (Codex P1 #3060199707).
func (a *arbitrator) confirmOwnership(sessionID uint64, initiator byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.hasOwner = true
	a.currentOwner = sessionID
	a.currentInitiator = initiator
}

// releaseOwnership releases bus ownership. Called when a transaction
// completes, times out, or the owner disconnects.
func (a *arbitrator) releaseOwnership(sessionID uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.hasOwner && a.currentOwner == sessionID {
		a.hasOwner = false
		a.currentOwner = 0
		a.currentInitiator = 0
	}
}

// forceRelease releases bus ownership regardless of which session
// holds it. Used on adapter RESETTED or timeout.
func (a *arbitrator) forceRelease() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.hasOwner = false
	a.currentOwner = 0
	a.currentInitiator = 0
}

// owner returns the current bus owner session ID and initiator.
// Returns (0, 0) if no session owns the bus.
func (a *arbitrator) owner() (sessionID uint64, initiator byte, owned bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.hasOwner {
		return 0, 0, false
	}
	return a.currentOwner, a.currentInitiator, true
}

// isOwner reports whether the given session currently owns the bus.
func (a *arbitrator) isOwner(sessionID uint64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.hasOwner && a.currentOwner == sessionID
}

// hasPending reports whether any START requests are pending.
func (a *arbitrator) hasPending() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.pendingGateway != nil || len(a.pendingExternal) > 0
}

// failAllPending fails all pending START requests (e.g., on shutdown
// or adapter RESETTED).
//
// F-17 follow-up (PR #626 review round-2, angry-tester finding F-NEW-6
// documentation): callers pass a non-nil `err` that
// `isResetOrDisconnectError` matches (adapter disconnect, adapter
// reset, mux closed). Session.go's handleStart routes such results to
// `deliverReset(...)`, which is the correct boundary-event notification
// for the client. This function MUST NOT set `cancelled: true` on the
// startResult — even when `req.cancelled.Load()` is true — because
// session.go's branch order (`granted → cancelled → err(reset) →
// deliverFailed`) favors `cancelled` and would silent-return on a
// boundary event where the client legitimately needs RESETTED.
// The current code accidentally relies on this NOT setting cancelled;
// the comment makes the precedence explicit so future "consistency"
// edits don't regress reset delivery.
func (a *arbitrator) failAllPending(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.pendingGateway != nil {
		a.pendingGateway.notify <- startResult{granted: false, initiator: a.pendingGateway.initiator, err: err}
		a.pendingGateway = nil
	}

	for _, req := range a.pendingExternal {
		req.notify <- startResult{granted: false, initiator: req.initiator, err: err}
	}
	a.pendingExternal = a.pendingExternal[:0]
}

// removeSession removes all pending requests and releases ownership
// for a disconnecting session.
func (a *arbitrator) removeSession(sessionID uint64) {
	a.cancelStart(sessionID)
	a.releaseOwnership(sessionID)
}
