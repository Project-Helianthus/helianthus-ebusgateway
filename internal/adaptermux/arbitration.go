package adaptermux

import (
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
)

// arbitrator collects bids until the mux observes an idle SYN. Every session,
// including session 0, competes by initiator-address priority.
type arbitrator struct {
	mu sync.Mutex

	// ownershipGen advances on every ownership transition (confirm,
	// release, forceRelease) and lifecycleEpoch on every lifecycle fence
	// (adapter reset, reconnect, close). ownershipGen is guarded by mu, the
	// lock commitWhileOwnershipCurrent holds across its check and its commit;
	// lifecycleEpoch is atomic so a fence never waits behind a stalled commit
	// write (a Close must stay able to reach the upstream close that unblocks
	// it) (issue #1003).
	ownershipGen   uint64
	lifecycleEpoch atomic.Uint64
	// ownerEpoch is the lifecycleEpoch observed when the current ownership
	// was confirmed. Tokens carry it rather than the live epoch, so a fence
	// that lands before the reset handler releases the old grant still ends
	// that grant's period.
	ownerEpoch uint64

	// pendingGateway holds the gateway's pending START request, if any.
	// Only one gateway request can be pending at a time.
	pendingGateway *startRequest

	// deferredGatewayLoser holds a gateway bid that lost local selection
	// while the selected START is outstanding. It stays subject to the same
	// expiry, replacement, cancellation and lifecycle failure as
	// pendingGateway; whichever path removes it sends its one result.
	deferredGatewayLoser *startRequest

	// pendingExternal holds external sessions' pending START requests
	// as an unordered bid set. Each session can have at most one pending request.
	pendingExternal []*startRequest

	// hasOwner tracks whether the bus is currently owned.
	hasOwner bool

	// currentOwner is the session ID of the current bus owner.
	// Only valid when hasOwner is true.
	currentOwner uint64

	// currentInitiator is the eBUS source address of the current
	// owner's START request.
	currentInitiator byte

	// onExternalBidQueued is called, without mu, after requestStart has
	// queued an external bid and released mu. The session start wrapper
	// holds the mux state lock across that call, so the callback binds
	// the INFO answer before a grant can take the same lock. Nil when
	// no session is attached. The callback must not call back into the
	// arbitrator.
	onExternalBidQueued func(*startRequest)

	// onExternalBidReleased delivers the INFO answer held for a bid that
	// left the queue because the client cancelled it. Nil when no session
	// is attached. The callback must not call back into the arbitrator.
	onExternalBidReleased func(*startRequest)

	// onExternalBidDropped discards the INFO answer held for a bid that
	// left the queue because the adapter reset, disconnected, or closed.
	// Nil when no session is attached. The callback must not call back
	// into the arbitrator.
	onExternalBidDropped func(*startRequest)

	// onGrantSettled delivers the INFO answers for the bid handed to the
	// transport and for the external bids that lost that selection. It
	// runs without mu, and only after the transport call has returned.
	// The callback must not call back into the arbitrator.
	onGrantSettled func()

	// handed is the bid the latest selection passed toward the transport.
	// handedLosers are the external bids that selection rejected. Both are
	// guarded by mu and cleared when the transport call returns.
	handed       *startRequest
	handedLosers []*startRequest
}

// startRequest represents a pending START arbitration request.
type startRequest struct {
	sessionID uint64
	initiator byte
	notify    chan startResult // receives exactly one result

	// cancelled is set when a session re-submits a START for the
	// same session ID — the old request's notify channel is already
	// closed-out with granted=false in requestStart, but the adapter
	// may still return STARTED/FAILED for an in-flight grant that
	// was popped from pendingExternal before the replace. The mux
	// checks this flag before publishing STARTED and retires a cancelled
	// wire win at the next SYN.
	cancelled atomic.Bool

	// infoSession is the external session whose INFO answer is held for
	// this bid. Set when the hold is bound, so a later drop can reach the
	// session after it has left the session map.
	infoSession atomic.Pointer[session]
}

// startResult is the outcome of a START arbitration request.
type startResult struct {
	granted   bool
	cancelled bool // AM55: true when client-initiated SYN cancel — no FAILED delivery
	initiator byte // initiator byte for STARTED/FAILED payload fidelity
	// startedQueued reports that the mux already queued the granted
	// external session's STARTED at its stream position (issue #1003);
	// the session must not deliver a second STARTED.
	startedQueued bool
	failedQueued  bool
	err           error
}

// gatewaySessionID is the reserved session ID for the gateway's
// internal active path. External sessions use IDs > 0.
const gatewaySessionID uint64 = 0

func newArbitrator() *arbitrator {
	return &arbitrator{}
}

// requestStart replaces this session's bid. A repeated external address retains
// its request identity while moving notification to the newest waiter.
func (a *arbitrator) requestStart(sessionID uint64, initiator byte) <-chan startResult {
	ch := make(chan startResult, 1)
	var queued *startRequest
	// Unlock runs before this, so the bind sees the queued bid and still
	// returns to the session wrapper while that wrapper holds the mux
	// state lock.
	defer func() { a.bindQueuedExternalBid(queued) }()
	a.mu.Lock()
	defer a.mu.Unlock()
	req := &startRequest{sessionID: sessionID, initiator: initiator, notify: ch}
	if sessionID == gatewaySessionID {
		if old := a.pendingGateway; old != nil {
			old.cancelled.Store(true)
			old.notify <- startResult{cancelled: true, initiator: old.initiator}
		}
		if old := a.deferredGatewayLoser; old != nil {
			a.deferredGatewayLoser = nil
			old.cancelled.Store(true)
			old.notify <- startResult{cancelled: true, initiator: old.initiator}
		}
		a.pendingGateway = req
		return ch
	}
	for i, old := range a.pendingExternal {
		if old.sessionID != sessionID {
			continue
		}
		old.notify <- startResult{cancelled: true, initiator: old.initiator}
		if old.initiator == initiator {
			old.notify = ch
			queued = old
		} else {
			old.cancelled.Store(true)
			a.pendingExternal[i] = req
			queued = req
		}
		return ch
	}
	a.pendingExternal = append(a.pendingExternal, req)
	queued = req
	return ch
}

// markInFlightCancelled is called by the mux when a new requestStart
// arrives for a session whose previous grant has already been popped
// from pendingExternal/pendingGateway and is in flight at the adapter.
// The pendingStartState that the mux holds carries the *startRequest;
// flagging cancelled here is what handleArbitrationResponse checks to
// retire a cancelled wire win at the next SYN. Returns the previous
// value of the flag so the caller can detect double-cancels.
// (Proxy-bug C4 / R4.)
func (a *arbitrator) markInFlightCancelled(req *startRequest) bool {
	if req == nil {
		return false
	}
	return req.cancelled.Swap(true)
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
	cancelled, dropped := a.cancelStartLocked(sessionID)
	if dropped != nil {
		a.notifyExternalBidReleased(dropped)
	}
	return cancelled
}

// cancelStartLocked cancels one queued bid and reports the external request
// that left pendingExternal, or nil for a gateway bid or a miss. Caller must
// not hold mu.
func (a *arbitrator) cancelStartLocked(sessionID uint64) (bool, *startRequest) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if sessionID == gatewaySessionID {
		if a.pendingGateway != nil {
			a.pendingGateway.cancelled.Store(true)
			a.pendingGateway.notify <- startResult{granted: false, cancelled: true, initiator: a.pendingGateway.initiator}
			a.pendingGateway = nil
			return true, nil
		}
		if old := a.deferredGatewayLoser; old != nil {
			a.deferredGatewayLoser = nil
			old.cancelled.Store(true)
			old.notify <- startResult{granted: false, cancelled: true, initiator: old.initiator}
			return true, nil
		}
		return false, nil
	}

	for i, req := range a.pendingExternal {
		if req.sessionID == sessionID {
			req.cancelled.Store(true)
			req.notify <- startResult{granted: false, cancelled: true, initiator: req.initiator}
			a.pendingExternal = append(a.pendingExternal[:i], a.pendingExternal[i+1:]...)
			return true, req
		}
	}
	return false, nil
}

// setInfoHoldHooks installs the callbacks that bind, deliver, and discard
// a session's held INFO answer. Safe to replace with the same functions.
// Each callback runs without mu and must not call back into the arbitrator.
func (a *arbitrator) setInfoHoldHooks(queued func(*startRequest), settled func(), released func(*startRequest), dropped func(*startRequest)) {
	a.mu.Lock()
	a.onExternalBidQueued = queued
	a.onGrantSettled = settled
	a.onExternalBidReleased = released
	a.onExternalBidDropped = dropped
	a.mu.Unlock()
}

func (a *arbitrator) notifyExternalBidReleased(req *startRequest) {
	a.notifyInfoHold(req, false)
}

func (a *arbitrator) notifyExternalBidDropped(req *startRequest) {
	a.notifyInfoHold(req, true)
}

func (a *arbitrator) notifyInfoHold(req *startRequest, drop bool) {
	if req == nil {
		return
	}
	a.mu.Lock()
	var hook func(*startRequest)
	if drop {
		hook = a.onExternalBidDropped
	} else {
		hook = a.onExternalBidReleased
	}
	a.mu.Unlock()
	if hook != nil {
		hook(req)
	}
}

// bindQueuedExternalBid binds the INFO answer for a bid just queued by the
// session start wrapper. Direct arbitrator callers are not that wrapper and
// do not bind: a grant can already have passed them to the transport.
func (a *arbitrator) bindQueuedExternalBid(req *startRequest) {
	if req == nil || !stackContains("(*Mux).requestStartForSession") {
		return
	}
	a.mu.Lock()
	hook := a.onExternalBidQueued
	a.mu.Unlock()
	if hook != nil {
		hook(req)
	}
}

// tryGrant selects one bidder at an idle SYN and resolves every other bid.
// The low nibble decides priority first; a smaller high nibble breaks ties.
// Equal addresses use the session ID for deterministic local selection.
// The caller confirms ownership only after the adapter reports STARTED.
func (a *arbitrator) tryGrant(busIdle bool) (*startRequest, bool) {
	winner, losers := a.selectAtSYN(busIdle)
	for _, req := range losers {
		a.takeDeferredGatewayLoser(req)
		req.notify <- startResult{initiator: winner.initiator}
	}
	return winner, winner != nil
}

// externalBid returns the session's queued external bid, or nil when it has
// none. The pointer is the request a later grant snapshot includes.
func (a *arbitrator) externalBid(sessionID uint64) *startRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, req := range a.pendingExternal {
		if req.sessionID == sessionID {
			return req
		}
	}
	return nil
}

// snapshotBids identifies the waiters admitted before the SYN state lock is
// released. Including notify distinguishes a same-address re-bid that coalesces
// onto the same request after the boundary.
func (a *arbitrator) snapshotBids() map[*startRequest]chan startResult {
	a.mu.Lock()
	defer a.mu.Unlock()
	bids := make(map[*startRequest]chan startResult, len(a.pendingExternal)+1)
	if a.pendingGateway != nil {
		bids[a.pendingGateway] = a.pendingGateway.notify
	}
	for _, req := range a.pendingExternal {
		bids[req] = req.notify
	}
	return bids
}

func (a *arbitrator) selectAtSYN(busIdle bool) (*startRequest, []*startRequest) {
	return a.selectBidsAtSYN(busIdle, nil)
}

// A nil snapshot admits all current bids; a non-nil snapshot excludes waiters
// submitted after the observed SYN, retaining them for the next boundary.
func (a *arbitrator) selectBidsAtSYN(busIdle bool, bids map[*startRequest]chan startResult) (*startRequest, []*startRequest) {
	a.mu.Lock()
	defer a.mu.Unlock()
	requests := make([]*startRequest, 0, len(a.pendingExternal)+1)
	if a.pendingGateway != nil {
		requests = append(requests, a.pendingGateway)
	}
	requests = append(requests, a.pendingExternal...)
	snapshot := arbitrationSnapshot{hasOwner: a.hasOwner, busIdle: busIdle, bids: make([]arbitrationBid, len(requests))}
	for i, req := range requests {
		if req != nil {
			snapshot.bids[i] = arbitrationBid{sessionID: req.sessionID, initiator: req.initiator,
				eligible: bids == nil || bids[req] == req.notify}
		}
	}
	decision := decideArbitration(snapshot)
	if decision.winner < 0 {
		return nil, nil
	}
	var losers []*startRequest
	for _, i := range decision.losers {
		req := requests[i]
		losers = append(losers, req)
		if req.sessionID == gatewaySessionID {
			a.deferredGatewayLoser = req
		}
	}
	gateway := a.pendingGateway
	a.pendingGateway = nil
	remaining := a.pendingExternal[:0]
	for _, i := range decision.retained {
		req := requests[i]
		if gateway != nil && i == 0 {
			a.pendingGateway = req
		} else {
			remaining = append(remaining, req)
		}
	}
	clear(a.pendingExternal[len(remaining):])
	a.pendingExternal = remaining
	winner := requests[decision.winner]
	a.noteHandedGrant(winner, losers)
	return winner, losers
}

// takeDeferredGatewayLoser removes req from deferred tracking. It reports true
// when the caller now owns sending req's one result. The grant step calls
// this after the transport call returns; that return delivers the held INFO
// answers for the handed bid and the external losers.
func (a *arbitrator) takeDeferredGatewayLoser(req *startRequest) bool {
	a.mu.Lock()
	owned := req != nil && a.deferredGatewayLoser == req
	if owned {
		a.deferredGatewayLoser = nil
	}
	hook := a.onGrantSettled
	a.mu.Unlock()
	if hook != nil && stackHasGrantReturn() {
		hook()
	}
	return owned
}

// noteHandedGrant records the bid a selection is about to hand to the
// transport, and the external bids that selection rejected. Caller holds mu.
func (a *arbitrator) noteHandedGrant(winner *startRequest, losers []*startRequest) {
	a.handed = winner
	a.handedLosers = a.handedLosers[:0]
	for _, req := range losers {
		if req != nil && req.sessionID != gatewaySessionID {
			a.handedLosers = append(a.handedLosers, req)
		}
	}
}

// takeHandedGrant returns the bid and external losers recorded by the latest
// selection and clears them. A second call is empty.
func (a *arbitrator) takeHandedGrant() (*startRequest, []*startRequest) {
	a.mu.Lock()
	defer a.mu.Unlock()
	req, losers := a.handed, a.handedLosers
	a.handed = nil
	a.handedLosers = nil
	return req, losers
}

// confirmOwnership sets bus ownership after the adapter's
// StartArbitration has succeeded. Must be called by tryGrantAndStart
// only on the success path (Codex P1 #3060199707).
func (a *arbitrator) confirmOwnership(sessionID uint64, initiator byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ownershipGen++
	a.ownerEpoch = a.lifecycleEpoch.Load()
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
		a.ownershipGen++
		a.hasOwner = false
		a.currentOwner = 0
		a.currentInitiator = 0
	}
}

// ownershipToken identifies one ownership period of one session within one
// lifecycle epoch (issue #1003).
type ownershipToken struct {
	sessionID uint64
	gen       uint64
	epoch     uint64
}

var (
	// errOwnershipLifecycleFenced: an adapter reset, reconnect or close
	// fenced the ownership period the commit belonged to.
	errOwnershipLifecycleFenced = errors.New("adaptermux: ownership period ended at a lifecycle boundary")
	// errOwnershipChanged: ownership was released or re-granted since the
	// token was taken.
	errOwnershipChanged = errors.New("adaptermux: ownership period ended")
)

// currentOwnershipToken returns the token of sessionID's current ownership
// period, or false when sessionID does not own the bus.
func (a *arbitrator) currentOwnershipToken(sessionID uint64) (ownershipToken, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.hasOwner || a.currentOwner != sessionID {
		return ownershipToken{}, false
	}
	return ownershipToken{sessionID: sessionID, gen: a.ownershipGen, epoch: a.ownerEpoch}, true
}

// commitWhileOwnershipCurrent runs commit while holding mu, only if tok still
// identifies the current ownership period. Every ownership transition and
// every lifecycle fence takes mu, so it is ordered deterministically against
// the commit: either it happened first and commit does not run (the returned
// error wraps errOwnershipLifecycleFenced or errOwnershipChanged), or it waits
// until commit has returned. commit must not call back into the arbitrator.
func (a *arbitrator) commitWhileOwnershipCurrent(tok ownershipToken, commit func() error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lifecycleEpoch.Load() != tok.epoch {
		return errOwnershipLifecycleFenced
	}
	if !a.hasOwner || a.currentOwner != tok.sessionID || a.ownershipGen != tok.gen {
		return errOwnershipChanged
	}
	return commit()
}

// fenceLifecycle ends every ownership period taken before it for the purpose
// of commitWhileOwnershipCurrent. Called at adapter reset, reconnect and
// close.
//
// The fence does not take mu: a commit already past its epoch check may still
// be inside its write, and a fence that waited for it would block Close (and
// reconnect) before they reach the upstream close that unblocks that write.
// Any commit that checks after the fence sees the new epoch and writes nothing.
func (a *arbitrator) fenceLifecycle() {
	a.lifecycleEpoch.Add(1)
}

// forceRelease releases bus ownership regardless of which session
// holds it. Used on adapter RESETTED or timeout.
func (a *arbitrator) forceRelease() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ownershipGen++
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

// hasExternalPending reports whether at least one external-session START
// request is queued. F-26 (batch-23, iter3, 2026-05-14): used by the
// SYN-idle release path to skip the gateway's IdleReleaseGrace when an
// external bidder (e.g. ebusd) is already waiting. Without this gate,
// the gateway holds bus ownership for the full 200 ms grace period
// after its own transaction completes, even though the bus has been
// physically idle and an external START is queued. The wire-byte
// trace measured median 596 ms / p95 4.2 s latency between ebusd's
// ENH_REQ_START and the gateway's ENH_RES_STARTED for this exact
// reason, with 100/260 paired events landing in the 1-5 s bucket —
// far past ebusd's arbitration-wait timeout, producing the
// `arbitration won in invalid state` cascade.
func (a *arbitrator) hasExternalPending() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.pendingExternal) > 0
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
	dropped := a.failAllPendingLocked(err)
	for _, req := range dropped {
		a.notifyExternalBidDropped(req)
	}
}

// failAllPendingLocked fails every queued bid and returns the external
// requests that left pendingExternal. The drop callback runs after mu is
// released. Caller must not hold mu.
func (a *arbitrator) failAllPendingLocked(err error) []*startRequest {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.pendingGateway != nil {
		a.pendingGateway.notify <- startResult{granted: false, initiator: a.pendingGateway.initiator, err: err}
		a.pendingGateway = nil
	}
	if a.deferredGatewayLoser != nil {
		a.deferredGatewayLoser.notify <- startResult{granted: false, initiator: a.deferredGatewayLoser.initiator, err: err}
		a.deferredGatewayLoser = nil
	}

	dropped := append([]*startRequest(nil), a.pendingExternal...)
	for _, req := range dropped {
		req.notify <- startResult{granted: false, initiator: req.initiator, err: err}
	}
	a.pendingExternal = a.pendingExternal[:0]
	return dropped
}

// removeSession removes all pending requests and releases ownership
// for a disconnecting session.
func (a *arbitrator) removeSession(sessionID uint64) {
	a.cancelStart(sessionID)
	a.releaseOwnership(sessionID)
}

// stackContains reports whether any frame above the caller ends with suffix.
// The session wrapper and the grant step are the frames that already hold
// the mux state lock, or that run only after RequestStart has returned.
func stackContains(suffix string) bool {
	return stackHasAny(suffix)
}

// stackHasGrantReturn reports whether this call is the grant step's
// takeDeferredGatewayLoser after the transport call returned. The deadline
// path calls the same method and does not carry these frames, so a parked
// answer stays parked until the call itself returns.
func stackHasGrantReturn() bool {
	return stackHasAny(
		"(*Mux).settleLocalLosses",
		"(*Mux).tryGrantAndStartForBids",
		"(*Mux).requestGrantStart",
		"(*Mux).runBlockingStart",
		"(*Mux).sendGrantStart",
		"(*Mux).startBlockingGrant",
		"(*Mux).failBlockingStart",
	)
}

func stackHasAny(suffixes ...string) bool {
	var pcs [32]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		for _, suffix := range suffixes {
			if strings.HasSuffix(frame.Function, suffix) {
				return true
			}
		}
		if !more {
			return false
		}
	}
}
