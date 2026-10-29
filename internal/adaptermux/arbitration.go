package adaptermux

import (
	"errors"
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
	a.mu.Lock()
	defer a.mu.Unlock()
	req := &startRequest{sessionID: sessionID, initiator: initiator, notify: ch}
	if sessionID == gatewaySessionID {
		if old := a.pendingGateway; old != nil {
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
		} else {
			old.cancelled.Store(true)
			a.pendingExternal[i] = req
		}
		return ch
	}
	a.pendingExternal = append(a.pendingExternal, req)
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

// tryGrant selects one bidder at an idle SYN and resolves every other bid.
// The low nibble decides priority first; a smaller high nibble breaks ties.
// Equal addresses use the session ID for deterministic local selection.
// The caller confirms ownership only after the adapter reports STARTED.
func (a *arbitrator) tryGrant(busIdle bool) (*startRequest, bool) {
	winner, losers := a.selectAtSYN(busIdle)
	for _, req := range losers {
		req.notify <- startResult{initiator: winner.initiator}
	}
	return winner, winner != nil
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
	if a.hasOwner || !busIdle {
		return nil, nil
	}
	eligible := func(req *startRequest) bool {
		return req != nil && (bids == nil || bids[req] == req.notify)
	}
	var winner *startRequest
	if eligible(a.pendingGateway) {
		winner = a.pendingGateway
	}
	for _, req := range a.pendingExternal {
		if eligible(req) && (winner == nil || higherPriority(req, winner)) {
			winner = req
		}
	}
	if winner == nil {
		return nil, nil
	}
	var losers []*startRequest
	lose := func(req *startRequest) {
		if eligible(req) && req != winner {
			losers = append(losers, req)
		}
	}
	lose(a.pendingGateway)
	for _, req := range a.pendingExternal {
		lose(req)
	}
	if eligible(a.pendingGateway) {
		a.pendingGateway = nil
	}
	remaining := a.pendingExternal[:0]
	for _, req := range a.pendingExternal {
		if !eligible(req) {
			remaining = append(remaining, req)
		}
	}
	clear(a.pendingExternal[len(remaining):])
	a.pendingExternal = remaining
	return winner, losers
}

func higherPriority(a, b *startRequest) bool {
	if a.initiator&0x0f != b.initiator&0x0f {
		return a.initiator&0x0f < b.initiator&0x0f
	}
	if a.initiator != b.initiator {
		return a.initiator>>4 < b.initiator>>4
	}
	return a.sessionID < b.sessionID
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
