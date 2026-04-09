package adaptermux

import "sync"

// arbitrator manages bus ownership between the gateway (internal) and
// external sessions (ebusd). Gateway-priority scheduling: at each SYN
// boundary, the gateway gets first pick. External requests are serviced
// at the next boundary when the gateway has no pending work.
//
// The arbitrator is informed by the proxy's boundary-based arbitration
// (proxy M2) but simplified for a two-class model:
//   - Gateway (sessionID = gatewaySessionID): always has priority
//   - External sessions: queued FIFO, serviced when gateway is idle
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
}

// startRequest represents a pending START arbitration request.
type startRequest struct {
	sessionID uint64
	initiator byte
	notify    chan startResult // receives exactly one result
}

// startResult is the outcome of a START arbitration request.
type startResult struct {
	granted   bool
	initiator byte // initiator byte for STARTED/FAILED payload fidelity
	err       error
}

// gatewaySessionID is the reserved session ID for the gateway's
// internal active path. External sessions use IDs > 0.
const gatewaySessionID uint64 = 0

func newArbitrator() *arbitrator {
	return &arbitrator{
		pendingExternal: make([]*startRequest, 0, 4),
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
	req := &startRequest{
		sessionID: sessionID,
		initiator: initiator,
		notify:    ch,
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if sessionID == gatewaySessionID {
		// Cancel any existing gateway request.
		if a.pendingGateway != nil {
			a.pendingGateway.notify <- startResult{granted: false, initiator: a.pendingGateway.initiator}
		}
		a.pendingGateway = req
	} else {
		// Remove any existing request from this session.
		for i, existing := range a.pendingExternal {
			if existing.sessionID == sessionID {
				existing.notify <- startResult{granted: false, initiator: existing.initiator}
				a.pendingExternal = append(a.pendingExternal[:i], a.pendingExternal[i+1:]...)
				break
			}
		}
		a.pendingExternal = append(a.pendingExternal, req)
	}

	return ch
}

// cancelStart cancels a pending START request for the given session.
// Returns true if a request was found and cancelled.
func (a *arbitrator) cancelStart(sessionID uint64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	if sessionID == gatewaySessionID {
		if a.pendingGateway != nil {
			a.pendingGateway.notify <- startResult{granted: false, initiator: a.pendingGateway.initiator}
			a.pendingGateway = nil
			return true
		}
		return false
	}

	for i, req := range a.pendingExternal {
		if req.sessionID == sessionID {
			req.notify <- startResult{granted: false, initiator: req.initiator}
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
// Ownership is NOT set here — the caller MUST call confirmOwnership
// after the adapter's StartArbitration succeeds (Codex P1 #3060199707:
// defer ownership until adapter START confirms). The caller MUST also
// send a startResult on the notify channel after success or failure.
//
// Returns (0, 0, nil, false) if no requests are pending or the bus
// is already owned.
func (a *arbitrator) tryGrant() (sessionID uint64, initiator byte, notify chan startResult, granted bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.hasOwner {
		return 0, 0, nil, false
	}

	// Gateway-priority: check gateway first.
	if a.pendingGateway != nil {
		req := a.pendingGateway
		a.pendingGateway = nil
		return gatewaySessionID, req.initiator, req.notify, true
	}

	// External FIFO: grant to first external request.
	if len(a.pendingExternal) > 0 {
		req := a.pendingExternal[0]
		a.pendingExternal = a.pendingExternal[1:]
		return req.sessionID, req.initiator, req.notify, true
	}

	return 0, 0, nil, false
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
