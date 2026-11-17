package adaptermux

import (
	"errors"
	"fmt"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
)

// A grant attempt that follows an idle SYN runs through named steps once the
// managed connection gate has admitted it (that gate refuses a delegated or
// shut-down connection). Under stateMu the admission step applies the gates
// (a START in flight, the absorb barrier, an outstanding blocking call, the
// regrant fence and the arbitration hold); the selection step runs
// decideArbitration under the arbiter lock, which is also where the current
// owner refuses a winner; the loser step publishes the
// external losers' FAILED (under sessionsMu, then on their notify channel);
// and the deadline step records the selection and arms its START deadline.
// stateMu is released before the START step writes the START: the
// non-blocking RequestStart from the caller's goroutine, or the blocking
// StartArbitration from its own goroutine started under closeMu. The admission
// gates take their decision from decideGrantAdmission, which starts from
// decideMuxTransition. The outcome of the START write comes back under stateMu
// as a second input, at the point the handler always re-locked: the blocking
// call's return or failure is the START call event of decideMuxTransition,
// and decideStartWrite decides every outcome over the re-derived typed state.
// The steps apply their effects in the order the handler always used.

// grantEvent is the transition event a grant attempt follows: the wire SYN
// at which the queued bids are selected.
var grantEvent = muxEvent{kind: muxEventSYN, data: protocol.SymbolSyn}

// startDeadlineEvent is the event of a START deadline that fires.
var startDeadlineEvent = muxEvent{kind: muxEventStartDeadline}

// blockingArbitrationStarter is the blocking START of a transport that offers
// no RequestStart.
type blockingArbitrationStarter interface {
	StartArbitration(byte) error
}

// grantGate is the admission gate that skips a grant attempt.
type grantGate uint8

const (
	// grantAdmitted: no gate skips the attempt.
	grantAdmitted grantGate = iota
	// grantSkipStartInFlight: a START is already in flight.
	grantSkipStartInFlight
	// grantSkipAbsorb: a stale arbitration result is still to be absorbed.
	grantSkipAbsorb
	// grantSkipBlockingCall: a blocking StartArbitration call is outstanding.
	grantSkipBlockingCall
	// grantSkipFence: an undeliverable STARTED fences selection until SYN.
	grantSkipFence
	// grantSkipHeld: active arbitration is held for an out-of-step adapter.
	grantSkipHeld
	// grantSkipInvalid: the transition function rules the event out.
	grantSkipInvalid
)

// decideGrantAdmission is the admission part of the transition function:
// decideMuxTransition decides whether the event can occur in the state, and
// the gates are checked in the order the handler always checked them. The
// state is the grant view (grantStateLocked), so a fence is the fenced phase.
func decideGrantAdmission(s muxState, ev muxEvent) grantGate {
	if decideMuxTransition(s, ev).invalid != "" {
		return grantSkipInvalid
	}
	switch {
	case s.start.pending:
		return grantSkipStartInFlight
	case s.absorb > 0:
		return grantSkipAbsorb
	case s.blocking:
		return grantSkipBlockingCall
	case s.phase == muxFenced:
		return grantSkipFence
	case s.held:
		return grantSkipHeld
	}
	return grantAdmitted
}

// startWrite is the kind of START write whose outcome comes back.
type startWrite uint8

const (
	// startWriteRequest: the non-blocking RequestStart write failed.
	startWriteRequest startWrite = iota
	// startWriteBlocking: the blocking StartArbitration call returned.
	startWriteBlocking
	// startWriteRefused: shutdown refused the blocking call's goroutine.
	startWriteRefused
	// startWriteUnsupported: the transport offers no START at all.
	startWriteUnsupported
)

// startWriteInput is what the START outcome observes under stateMu besides
// the typed state.
type startWriteInput struct {
	write startWrite
	// failed: the write or call returned an error.
	failed bool
	// ownsSelection: the START in flight is still this attempt's (the same
	// notify channel), so no cancel, deadline, reset or close resolved it.
	ownsSelection bool
	// cancelledInFlight: the selected request was cancelled by a re-submit;
	// resetOrDisconnect: the error is a reset or disconnect boundary. Read
	// only when the selection is still owned, and the second only when the
	// first holds.
	cancelledInFlight bool
	resetOrDisconnect bool
	// currentCall: the blocking call's generation is still the current one.
	currentCall bool
}

// startWriteDecision is the decision of decideStartWrite.
type startWriteDecision struct {
	// callCurrent: the blocking call is the outstanding one.
	callCurrent bool
	// clearBlocking: the call clears the blocking-call barrier now.
	clearBlocking bool
	// withdraw: the selection's deadline stops and the selection is cleared;
	// cancelled: its waiter is told it was cancelled instead of failed.
	withdraw  bool
	cancelled bool
	// grant: the blocking call won; the selection completes its grant.
	grant bool
	// fence: selection is fenced until the next SYN.
	fence bool
	// countUndeliverable: a won blocking call found nobody to deliver to.
	countUndeliverable bool
	// consumeAbsorb: one stale result is consumed from the absorb barrier;
	// clearSkipLog: that drains it, so the skip-log state is cleared.
	consumeAbsorb bool
	clearSkipLog  bool
	// reopen: the drained barrier clears the retired selection and the
	// fence; clearAtSYNRecovery: and the at-SYN recovery.
	reopen             bool
	clearAtSYNRecovery bool
}

// startCallEvent is the transition event of a blocking call's outcome.
func startCallEvent(failed bool) muxEvent {
	if failed {
		return muxEvent{kind: muxEventStartCallFails}
	}
	return muxEvent{kind: muxEventStartCallReturns}
}

// decideStartWrite decides the outcome of a START write over the state
// re-derived after the write. The blocking call's outcome is the START call
// event of decideMuxTransition: it is the outstanding call only when the
// transition function says a blocking call can return and the call's
// generation is current. The non-blocking write, the refusal and a transport
// without START have no event of their own; they are decided over the state
// and the input alone.
func decideStartWrite(s muxState, in startWriteInput) startWriteDecision {
	var d startWriteDecision
	switch in.write {
	case startWriteRequest:
		if in.ownsSelection {
			d.withdraw = true
			d.cancelled = in.cancelledInFlight && !in.resetOrDisconnect
			return d
		}
		d.consumeAbsorb = s.absorb > 0
		d.clearSkipLog = s.absorb == 1
		d.reopen = s.absorb <= 1
		d.clearAtSYNRecovery = d.reopen && s.recovery == arbitrationRecoveryAtSYN
	case startWriteBlocking:
		d.callCurrent = decideMuxTransition(s, startCallEvent(in.failed)).invalid == "" && in.currentCall
		switch {
		case in.failed && in.ownsSelection:
			d.clearBlocking = d.callCurrent
			d.withdraw = true
			d.cancelled = in.cancelledInFlight && !in.resetOrDisconnect
		case in.ownsSelection:
			d.grant = true
		default:
			d.clearBlocking = d.callCurrent
			d.fence = d.callCurrent && !in.failed
			d.countUndeliverable = d.fence
			d.consumeAbsorb = d.callCurrent && s.absorb > 0
			d.clearSkipLog = d.consumeAbsorb && s.absorb == 1
		}
	case startWriteRefused:
		d.withdraw = in.ownsSelection
	case startWriteUnsupported:
		d.withdraw = in.ownsSelection
		d.fence = in.ownsSelection
	}
	return d
}

// grantAttempt carries one selection through the steps of
// tryGrantAndStartForBids.
type grantAttempt struct {
	bids map[*startRequest]chan startResult
	// tr is the transport snapshot; blockingPath: it offers only the
	// blocking StartArbitration.
	tr           transport.RawTransport
	blockingPath bool
	// The selection: the selected request with its session, source and
	// notify channel, every loser and the gateway's deferred loss.
	req          *startRequest
	losers       []*startRequest
	gatewayLoser *startRequest
	sessionID    uint64
	initiator    byte
	notify       chan startResult
}

// grantStateLocked derives the typed state the grant decides on. The grant
// reads neither the owner nor the queued bids before the selection: the
// selection reads both under the arbiter lock, so this view takes neither
// that lock nor the echo tracker, and an owned bus is not a phase here.
// Caller holds stateMu.
func (m *Mux) grantStateLocked() muxState {
	return m.synBoundaryStateLocked(0, false)
}

// Guard: only one pending START at a time. If pendingStart is non-nil,
// this method is a no-op — the next tryGrantAndStart will fire after
// the current one resolves.
func (m *Mux) tryGrantAndStart() {
	m.tryGrantAndStartForBids(nil)
}

// tryGrantAndStartForBids runs the grant steps for the bids admitted before
// the SYN (nil admits every current bid).
func (m *Mux) tryGrantAndStartForBids(bids map[*startRequest]chan startResult) {
	managedGateHeld, admitted := m.beginManagedConnectionUse()
	if !admitted {
		return
	}
	if managedGateHeld {
		defer m.endManagedConnectionUse(true)
	}
	g := &grantAttempt{bids: bids}
	m.snapshotGrantTransport(g)

	// P1 fix (#3062924968): serialize the pendingStart guard, the
	// tryGrantLegacy(arb) dequeue, and the pendingStart assignment in a
	// single stateMu critical section.  Without this, two concurrent
	// callers (readLoop + RemoveSession goroutine) can both pass the
	// nil-guard, dequeue different requests, and one overwrites the
	// other's pendingStart — leaving the first waiter without a
	// terminal result.
	//
	// Lock order: stateMu → arb.mu (tryGrant acquires arb.mu
	// internally).  No path holds arb.mu then acquires stateMu,
	// so this is ABBA-safe.
	m.stateMu.Lock()
	if !m.admitGrantLocked() {
		return
	}
	if !m.selectGrantLocked(g) {
		m.stateMu.Unlock()
		return
	}
	m.notifyGrantLosersLocked(g)
	m.armStartDeadlineLocked(g)
	m.stateMu.Unlock()

	m.sendGrantStart(g)
}

// snapshotGrantTransport captures the transport and its START capability.
func (m *Mux) snapshotGrantTransport(g *grantAttempt) {
	// Snapshot transport BEFORE acquiring stateMu to avoid stateMu → connMu
	// lock nesting. doSend uses connMu → (release) → stateMu, so while not
	// strictly ABBA, keeping consistent ordering is defensive best practice.
	m.connMu.Lock()
	g.tr = m.upstream
	m.connMu.Unlock()

	_, hasRequestStart := g.tr.(arbitrationRequester)
	_, hasBlockingStart := g.tr.(blockingArbitrationStarter)
	g.blockingPath = !hasRequestStart && hasBlockingStart
}

// admitGrantLocked is the admission step. Caller holds stateMu; when a gate
// skips the attempt it releases stateMu, logs as the gate always did and
// returns false.
func (m *Mux) admitGrantLocked() bool {
	switch decideGrantAdmission(m.grantStateLocked(), grantEvent) {
	case grantSkipStartInFlight:
		m.logger.Printf("adaptermux: tryGrantAndStart skipped — pendingStart already set for session %d", m.pendingStart.sessionID)
	case grantSkipAbsorb:
		// Do not launch a new START while we still expect at least one stale
		// STARTED/FAILED from a cancelled or deadline-expired RequestStart.
		// A client often reuses the same initiator, so once we have a new
		// pending request there is no reliable way to distinguish the old
		// STARTED from the new one. Treat pendingStartAbsorb as a real regrant
		// barrier, not just a best-effort diagnostic counter.
		m.logAbsorbSkipLocked(time.Now())
	case grantSkipBlockingCall:
		// Codex-R5: block regrant while a blocking StartArbitration goroutine
		// is still in-flight. The deadline may have cleared pendingStart, but
		// the goroutine is still running on the transport. Starting another
		// arbitration would overlap STARTs on the same transport.
		m.logger.Printf("adaptermux: tryGrantAndStart skipped — blocking StartArbitration gen %d still in-flight", m.blockingArbGen)
	case grantSkipFence:
		m.logger.Printf("adaptermux: tryGrantAndStart skipped — undeliverable STARTED awaiting next SYN")
	case grantSkipHeld:
		_, line := m.stepHoldSkipLocked(time.Now())
		m.stateMu.Unlock()
		if line != "" {
			m.logger.Print(line)
		}
		return false
	case grantSkipInvalid:
	default:
		return true
	}
	m.stateMu.Unlock()
	return false
}

// logAbsorbSkipLocked logs an absorb-barrier skip at most once per second.
// Caller holds stateMu.
func (m *Mux) logAbsorbSkipLocked(now time.Time) {
	if !m.absorbSkipLogLastEmit.IsZero() && now.Sub(m.absorbSkipLogLastEmit) < time.Second {
		m.absorbSkipLogSuppressedSince++
		return
	}
	suppressed := m.absorbSkipLogSuppressedSince
	m.absorbSkipLogSuppressedSince = 0
	m.absorbSkipLogLastEmit = now
	if suppressed > 0 {
		m.logger.Printf("adaptermux: tryGrantAndStart skipped — waiting to absorb %d stale arbitration response(s) (%d suppressed)", m.pendingStartAbsorb, suppressed)
	} else {
		m.logger.Printf("adaptermux: tryGrantAndStart skipped — waiting to absorb %d stale arbitration response(s)", m.pendingStartAbsorb)
	}
}

// selectGrantLocked is the selection step: decideArbitration selects among
// the admitted bids under the arbiter lock and refuses a winner while the bus
// has an owner. It reports whether a request was selected. Caller holds
// stateMu.
func (m *Mux) selectGrantLocked(g *grantAttempt) bool {
	req, losers := m.arb.selectBidsAtSYN(true, g.bids)
	if req == nil {
		return false
	}
	g.req, g.losers = req, losers
	g.sessionID = req.sessionID
	g.initiator = req.initiator
	g.notify = req.notify
	return true
}

// notifyGrantLosersLocked is the loser step. Caller holds stateMu.
func (m *Mux) notifyGrantLosersLocked(g *grantAttempt) {
	// An external loser's FAILED must precede the next wire byte, so it is
	// published now and undone if the START never reaches the adapter. A
	// collision told to protocol.Bus cannot be undone (it would wait for SYNs
	// that nobody owes it), so the gateway's loss waits for the START outcome.
	for _, loser := range g.losers {
		if loser.sessionID == gatewaySessionID {
			g.gatewayLoser = loser
			continue
		}
		queued := false
		m.sessionsMu.Lock()
		if sess := m.sessions[loser.sessionID]; sess != nil {
			sess.parserResetNeeded.Store(true)
			sess.dropInfoHold(loser)
			sess.deliverFailed(g.req.initiator)
			queued = true
		}
		m.sessionsMu.Unlock()
		loser.notify <- startResult{initiator: g.req.initiator, failedQueued: queued}
	}
}

// settleLocalLosses runs once per selection, when the transport call that
// carries the START has returned. A nil startErr means the source reached
// the adapter and the local losses stand.
func (m *Mux) settleLocalLosses(g *grantAttempt, startErr error) {
	if startErr != nil {
		m.undoLocalLosses(g.losers)
	}
	m.stateMu.Lock()
	owned := m.arb.takeDeferredGatewayLoser(g.gatewayLoser)
	if owned && startErr == nil {
		m.collisionResyncOwed = protocol.CollisionRetryResyncSYNCount
	}
	m.stateMu.Unlock()
	if !owned {
		return // expiry, reset, disconnect or close already resolved it
	}
	if startErr != nil {
		g.gatewayLoser.notify <- startResult{initiator: g.gatewayLoser.initiator, err: startErr}
		return
	}
	g.gatewayLoser.notify <- startResult{initiator: g.initiator}
}

// armStartDeadlineLocked is the deadline step: it records the selection and
// arms its START deadline. Caller holds stateMu.
func (m *Mux) armStartDeadlineLocked(g *grantAttempt) {
	m.retiredSelection = false
	m.localLosers = g.losers
	m.localLoserSource = g.initiator
	m.pendingStart = &pendingStartState{
		lifecycleEpoch: m.arb.lifecycleEpoch.Load(),
		sessionID:      g.sessionID,
		initiator:      g.initiator,
		notify:         g.notify,
		blockingArb:    g.blockingPath,
		req:            g.req,
	}
	// AM8: start a deadline timer so pendingStart cannot block indefinitely
	// if the adapter never responds with STARTED/FAILED.
	notify, gatewayLoser := g.notify, g.gatewayLoser
	m.pendingStart.deadline = time.AfterFunc(m.cfg.StartDeadline, func() {
		m.expireStartDeadline(notify, gatewayLoser)
	})
	// Every START path opens a cycle: the blocking StartArbitration fallback
	// (adapter-direct ENS) is detected like the non-blocking RequestStart.
	m.stepStartCycleLocked()
}

// expireStartDeadline is the START deadline of the selection identified by
// notify. decideMuxTransition decides whether a deadline can fire (a START
// is in flight); it expires only that selection, and only before its result.
func (m *Mux) expireStartDeadline(notify chan startResult, gatewayLoser *startRequest) {
	m.stateMu.Lock()
	if decideMuxTransition(m.grantStateLocked(), startDeadlineEvent).invalid != "" ||
		m.pendingStart.notify != notify || m.pendingStart.responseReceived {
		m.stateMu.Unlock()
		return
	}
	pending := m.pendingStart
	m.pendingStart = nil
	m.fenceArbitrationUntilSYNLocked()
	m.armPendingStartAbsorbLocked("deadline")
	// F-15 (operator hand-off, batch-9/11): gate the reconnect by
	// transport type, mirroring `cancelPendingStart`'s existing
	// `wasBlocking := pending.blockingArb` pattern.
	//
	// PRIOR BUG (`needReconnect := true` unconditionally): every
	// deadline expiry forced an upstream reconnect — including on
	// the non-blocking ENH RequestStart path where the adapter is
	// only slow, not hung. F-17's retry storm amplified this:
	// adapter backlog → slow STARTED → AM8 trips → forced
	// reconnect → drops the next ReqStart → loop. Even without
	// F-17 the asymmetric reconnect is a design defect on the
	// non-blocking path.
	//
	// LIVE EVIDENCE: 0 absorb-timeout reconnects across the entire
	// 30k-line batch-9 log. The absorb safety-net inside
	// `armPendingStartAbsorbLocked` drained naturally on every
	// late-STARTED in production. The unconditional reconnect
	// here was therefore never needed for the non-blocking
	// transport in observed runs.
	//
	// F-22 (batch-19, 2026-05-13): the absorb safety-net's own
	// timeout side effect is now a counter reset ONLY — it does
	// NOT close the transport. Covered by
	// `TestF22_AbsorbTimerResetsCounterWithoutReconnect`. The
	// truly-hung scenario the original code feared is recovered
	// via the bus poll loop's next RequestStart on the still-
	// open connection; the prior reconnect-on-absorb-timeout
	// was load-bearing only on the legacy blocking transport.
	//
	// BLOCKING TRANSPORT (legacy StartArbitration): the goroutine
	// may still be hung in the transport call after the deadline,
	// so the reconnect is still required to unstick it — same
	// shape as `cancelPendingStart`.
	wasBlocking := pending.blockingArb
	// F-17 follow-up (PR #626 review round-3, angry-tester
	// finding M1): the AM8 deadline path resolves the same
	// pending startRequest that the three branches of
	// handleArbitrationResponse resolve, and it can fire
	// PRECISELY when the adapter is slow — the exact
	// production condition that motivated this PR cycle.
	// Without the cancelled-check, a same-session re-submit
	// that flipped `pending.req.cancelled` via
	// markInFlightCancelled will see the old wait goroutine
	// fall through to deliverFailed("…deadline expired…")
	// → ENHResFailed on the wire. Same retry-feedback-loop
	// class as F-17 / F-NEW-1, just gated on AM8 instead of
	// the normal arrival.
	cancelledInFlight := pending.req != nil && pending.req.cancelled.Load()
	m.stateMu.Unlock()
	m.logger.Printf("adaptermux: pendingStart deadline expired for session %d (wasBlocking=%v, cancelledInFlight=%v)", pending.sessionID, wasBlocking, cancelledInFlight)
	// The hung START may never return, so its deferred loser ends here.
	if m.arb.takeDeferredGatewayLoser(gatewayLoser) {
		gatewayLoser.notify <- startResult{initiator: gatewayLoser.initiator, err: fmt.Errorf("adaptermux: START deadline expired: %w", ebuserrors.ErrTimeout)}
	}
	// AM8: guard the send — if another path already delivered a
	// result, the channel is full and this send would block forever.
	var result startResult
	if cancelledInFlight {
		result = startResult{
			granted:   false,
			cancelled: true,
			initiator: pending.initiator,
			err:       errors.New("adaptermux: START deadline expired (superseded by client re-submit)"),
		}
	} else {
		result = startResult{
			granted:   false,
			initiator: pending.initiator,
			err:       errors.New("adaptermux: START deadline expired"),
		}
	}
	select {
	case pending.notify <- result:
	default:
		m.logger.Printf("adaptermux: pendingStart deadline: notify channel full for session %d, result already delivered", pending.sessionID)
	}
	if wasBlocking {
		// Close the current conn to force the hung blocking
		// StartArbitration call to return with an I/O error.
		// readLoop's error handler invokes reconnect() which
		// advances blockingArbGen and clears blockingArbActive
		// atomically under stateMu.
		m.connMu.Lock()
		c := m.conn
		m.connMu.Unlock()
		if c != nil {
			if err := c.Close(); err != nil {
				m.logger.Printf("adaptermux: deadline-triggered conn close: %v", err)
			} else {
				m.logger.Printf("adaptermux: deadline triggered transport reconnect to unstick hung StartArbitration")
			}
		}
	}
}

// sendGrantStart is the START step: it writes the START with the transport's
// capability outside stateMu.
func (m *Mux) sendGrantStart(g *grantAttempt) {
	// Forward START to adapter via non-blocking RequestStart.
	// tr was already captured above (before pendingStart creation).
	if requester, ok := g.tr.(arbitrationRequester); ok {
		m.requestGrantStart(g, requester)
	} else if starter, ok := g.tr.(blockingArbitrationStarter); ok {
		m.startBlockingGrant(g, starter)
	} else {
		m.logger.Printf("adaptermux: transport does not support arbitration")
		m.dropSessionInfoHold(g.sessionID, g.req)
		m.settleLocalLosses(g, errors.New("adaptermux: transport does not support arbitration"))
		m.stateMu.Lock()
		m.applyStartWriteLocked(m.decideStartWriteLocked(g, startWriteUnsupported, nil, 0))
		m.stateMu.Unlock()
		g.notify <- startResult{granted: false, initiator: g.initiator, err: errors.New("adaptermux: transport does not support arbitration")}
	}
}

// decideStartWriteLocked observes the START outcome under stateMu and takes
// its decision over the re-derived typed state. arbGen is the blocking call's
// generation. Caller holds stateMu.
func (m *Mux) decideStartWriteLocked(g *grantAttempt, write startWrite, err error, arbGen uint64) startWriteDecision {
	in := startWriteInput{write: write, failed: err != nil, currentCall: m.blockingArbGen == arbGen}
	if p := m.pendingStart; p != nil && p.notify == g.notify {
		in.ownsSelection = true
		in.cancelledInFlight = p.req != nil && p.req.cancelled.Load()
		in.resetOrDisconnect = in.cancelledInFlight && isResetOrDisconnectError(err)
	}
	return decideStartWrite(m.grantStateLocked(), in)
}

// applyStartWriteLocked applies the state part of a START outcome in the
// order the handler always used. Caller holds stateMu.
func (m *Mux) applyStartWriteLocked(d startWriteDecision) {
	if d.clearBlocking {
		// Only our generation may clear the active flag.
		// A stale gen (prior reconnect/handleReset) won't match.
		m.blockingArbActive = false
	}
	if d.withdraw {
		if m.pendingStart.deadline != nil {
			m.pendingStart.deadline.Stop() // AM8: cancel deadline timer
		}
		m.pendingStart = nil
	}
	if d.fence {
		m.fenceArbitrationUntilSYNLocked()
	}
	if d.countUndeliverable {
		m.undeliverableStarted.Add(1)
	}
	if d.consumeAbsorb {
		m.pendingStartAbsorb--
	}
	if d.clearSkipLog {
		m.clearAbsorbSkipLogStateLocked()
	}
	if d.reopen {
		m.retiredSelection = false
		m.deferRegrantUntilNextSyn = false
	}
	if d.clearAtSYNRecovery {
		m.arbitrationRecovery = arbitrationRecoveryNone
	}
}

// failedStartResult is the waiter's result of a failed START write.
func failedStartResult(d startWriteDecision, initiator byte, err error) startResult {
	if d.cancelled {
		return startResult{granted: false, cancelled: true, initiator: initiator, err: err}
	}
	return startResult{granted: false, initiator: initiator, err: err}
}

// requestGrantStart writes the START through the non-blocking RequestStart.
func (m *Mux) requestGrantStart(g *grantAttempt, requester arbitrationRequester) {
	m.logger.Printf("adaptermux: RequestStart(0x%02X) sent for session %d", g.initiator, g.sessionID)
	err := requester.RequestStart(g.initiator)
	if err == nil {
		m.settleLocalLosses(g, nil)
		m.releaseSessionInfoHold(g.sessionID, g.req)
		return
	}
	m.dropSessionInfoHold(g.sessionID, g.req)
	m.logger.Printf("adaptermux: RequestStart failed for session %d: %v", g.sessionID, err)
	m.settleLocalLosses(g, err)
	// P1 fix: only send failure if we still own the pending slot.
	// cancelPendingStart (session disconnect / cancel on another
	// goroutine) may have already cleared m.pendingStart and sent
	// on notify while RequestStart was in progress. A second send
	// on the cap-1 channel would block forever, pinning readLoop.
	//
	// F-17 follow-up (PR #626 review round-3, angry-tester
	// finding M2): a same-session re-submit could race
	// with the transport-call returning an error. If
	// cancelled-in-flight, suppress with `cancelled: true`
	// so the old wait goroutine silent-returns rather than
	// emitting ENHResFailed on the wire (same class as M1).
	//
	// F-17 follow-up (PR #626 Codex bot P2, 2026-05-11):
	// EXCEPTION — when the transport err is a reset/
	// disconnect boundary event (isResetOrDisconnectError),
	// the cancelled flag MUST NOT be set, even if the
	// in-flight req is cancelled. Session.go's branch order
	// (`granted → cancelled → err(reset) → deliverFailed`)
	// would silent-return on `cancelled: true` and the
	// client would miss the RESETTED delivery it needs to
	// observe the boundary event. This is the same
	// precedence rule documented in
	// `arbitration.failAllPending`'s contract block.
	//
	// Otherwise RequestStart failed AND pending was already cancelled.
	// Since the adapter never received the START, no stale
	// response will arrive — decrement absorb counter so the
	// next real arbitration response is not incorrectly consumed.
	m.stateMu.Lock()
	d := m.decideStartWriteLocked(g, startWriteRequest, err, 0)
	m.applyStartWriteLocked(d)
	m.stateMu.Unlock()
	if d.withdraw {
		g.notify <- failedStartResult(d, g.initiator, err)
	}
}

// startBlockingGrant starts the blocking StartArbitration call in its own
// goroutine, under the closeMu guard.
func (m *Mux) startBlockingGrant(g *grantAttempt, starter blockingArbitrationStarter) {
	// Fallback for transports that only implement the blocking
	// StartArbitration (e.g. ENS or test mocks without RequestStart).
	// blockingArb was already set in the pendingStart struct literal
	// above, so the deadline callback sees it from the start.
	// The AM8 deadline timer stays active to preserve liveness —
	// if StartArbitration hangs, the deadline clears pendingStart
	// and notifies the session. The deadline path triggers a
	// transport reconnect (via m.conn.Close) to force the hung
	// StartArbitration call to return with an I/O error;
	// reconnect() will then bump blockingArbGen and clear
	// blockingArbActive so the hung goroutine's late return is
	// observed with a stale gen and skips state mutation.
	//
	// PR502-Fix1: Run blocking StartArbitration in a goroutine so
	// readLoop is not stalled. The AM8 deadline timer handles
	// liveness if the call hangs indefinitely.
	// Codex-R5/R6: set blockingArbGen to prevent tryGrantAndStart
	// from starting another arbitration while this goroutine runs.
	// Generation-scoped: goroutine only clears if gen matches,
	// so a reconnect+relaunch won't be cleared by an old goroutine.
	// C1 / PR #502 P2: refuse to spawn if shutdown is in progress.
	// Without this gate, m.wg.Add(1) below can race with m.wg.Wait()
	// in Close() and panic with "sync: WaitGroup misuse". Emit FAILED
	// to the pending session so it is not left orphaned.
	//
	// PR #502 P2 (Codex): the closing-check and wg.Add MUST be
	// serialized under closeMu with Close()'s closing.Store(true).
	// A bare `if m.closing.Load()` check followed by a later
	// `m.wg.Add(1)` is a TOCTOU race: Close can flip closing to true
	// and reach m.wg.Wait() between the two statements. Holding
	// closeMu across the check and the Add (paired with Close
	// holding closeMu across the Store) guarantees the Add cannot
	// run after closing has been observed as true by Close.
	m.closeMu.Lock()
	if m.closing.Load() {
		m.closeMu.Unlock()
		m.dropSessionInfoHold(g.sessionID, g.req)
		m.settleLocalLosses(g, errors.New("adaptermux: closed"))
		m.stateMu.Lock()
		m.applyStartWriteLocked(m.decideStartWriteLocked(g, startWriteRefused, nil, 0))
		m.stateMu.Unlock()
		select {
		case g.notify <- startResult{granted: false, initiator: g.initiator, err: errors.New("adaptermux: closed")}:
		default:
		}
		return
	}
	m.stateMu.Lock()
	m.blockingArbGen++
	arbGen := m.blockingArbGen
	m.blockingArbActive = true
	m.stateMu.Unlock()
	// Codex-R9: track the blocking goroutine in mux.wg so Close()
	// waits for it. Without this, Close() can return while a hung
	// StartArbitration is still running, causing post-close state
	// mutations and goroutine leaks across reconnect/restart cycles.
	m.wg.Add(1)
	m.closeMu.Unlock()
	go m.runBlockingStart(g, starter, arbGen)
}

// runBlockingStart is the blocking call's goroutine.
func (m *Mux) runBlockingStart(g *grantAttempt, starter blockingArbitrationStarter, arbGen uint64) {
	defer m.wg.Done()
	blockingGateHeld, blockingAdmitted := m.beginManagedConnectionUse()
	var startErr error
	if !blockingAdmitted {
		startErr = errNotConnected
	} else {
		startErr = starter.StartArbitration(g.initiator)
		m.endManagedConnectionUse(blockingGateHeld)
	}
	if startErr != nil {
		m.dropSessionInfoHold(g.sessionID, g.req)
		m.failBlockingStart(g, arbGen, startErr)
		return
	}
	m.settleLocalLosses(g, nil)
	m.releaseSessionInfoHold(g.sessionID, g.req)
	m.completeBlockingStart(g, arbGen)
}

// failBlockingStart handles a failed blocking call.
func (m *Mux) failBlockingStart(g *grantAttempt, arbGen uint64, err error) {
	m.logger.Printf("adaptermux: START arbitration failed for session %d: %v", g.sessionID, err)
	m.settleLocalLosses(g, err)
	// P1 fix (#3063005909): only send failure if we still own
	// the pending slot.  cancelPendingStart may have cleared
	// m.pendingStart and already sent a failure on notify while
	// StartArbitration was blocking.  A second send on the
	// cap-1 channel would block the caller indefinitely.
	//
	// F-17 follow-up (PR #626 review round-3, angry-tester
	// finding M2, blocking-half): same as the non-blocking
	// RequestStart-err path — a same-session re-submit
	// could race with the transport-call returning an
	// error. Suppress with `cancelled: true` when the
	// in-flight req is cancelled.
	//
	// F-17 follow-up (PR #626 Codex bot P2, 2026-05-11):
	// EXCEPTION — when the transport err is a reset/
	// disconnect boundary event, the cancelled flag MUST
	// NOT be set so session.go's err-routing path drives
	// deliverReset(...) instead of silent-returning. Same
	// precedence as the non-blocking RequestStart-err
	// path above.
	//
	// Otherwise StartArbitration failed AND pending was already cancelled.
	// Codex-R8: scope absorb decrement + queue advance to our
	// generation. A stale goroutine (from before reconnect/
	// handleReset) must not consume absorb budget or advance
	// the queue on behalf of a newer request.
	m.stateMu.Lock()
	d := m.decideStartWriteLocked(g, startWriteBlocking, err, arbGen)
	m.applyStartWriteLocked(d)
	m.stateMu.Unlock()
	if d.withdraw {
		g.notify <- failedStartResult(d, g.initiator, err)
	}
}

// completeBlockingStart handles a blocking call that won.
func (m *Mux) completeBlockingStart(g *grantAttempt, arbGen uint64) {
	// Blocking path: adapter already confirmed — handle inline.
	// P1 fix (#3063005909): verify ownership before completing.
	// cancelPendingStart may have run while StartArbitration was
	// blocking, clearing pendingStart and sending a failure on
	// notify.  Completing here would double-send on the cap-1
	// channel and re-grant the bus to a cancelled session.
	//
	// Codex-R10: do NOT clear blockingArbActive in the success
	// branch until AFTER completeArbitrationGrant confirms
	// ownership. Clearing earlier opens a race window where
	// tryGrantAndStart (from readLoop SYN/idle or RemoveSession)
	// observes pendingStart==nil + blockingArbActive==false +
	// no owner and launches a second overlapping arbitration.
	//
	// Already cancelled (deadline or session disconnect) — don't
	// double-send. Safe to clear blockingArbActive here: no
	// completeArbitrationGrant will follow.
	// Codex-R8: scope absorb + queue advance to our generation.
	m.stateMu.Lock()
	d := m.decideStartWriteLocked(g, startWriteBlocking, nil, arbGen)
	if !d.grant {
		m.applyStartWriteLocked(d)
		m.stateMu.Unlock()
		return
	}
	if m.pendingStart.deadline != nil {
		m.pendingStart.deadline.Stop() // AM8: cancel deadline timer
	}
	pending := m.pendingStart
	pending.responseReceived = true
	m.stateMu.Unlock()
	m.completeArbitrationGrant(pending)
	// Clear blockingArbActive AFTER ownership is confirmed.
	// Codex-R11: re-check generation under the lock at the clear
	// site. The cached isCurrentGen could be stale if reconnect()
	// or handleReset() bumped blockingArbGen while
	// completeArbitrationGrant was running — a stale stale-gen
	// goroutine must not clear the guard for a newer in-flight
	// START.
	m.stateMu.Lock()
	if m.blockingArbGen == arbGen {
		m.blockingArbActive = false
	}
	m.stateMu.Unlock()
}
