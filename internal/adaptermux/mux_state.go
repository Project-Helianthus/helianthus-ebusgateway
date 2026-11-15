package adaptermux

import "github.com/Project-Helianthus/helianthus-ebusgo/protocol"

// The typed mux state is a view over the fields the transition handlers read
// and write; it is derived on demand and never stored. muxEvent names the
// inputs the handlers react to, and decideMuxTransition is the transition
// function: it holds the rule saying which events can occur in which state and
// the decisions routed through it. The handlers translate I/O into events,
// derive the state where they take a decision and apply the result in the
// order they always have. doc.go describes the states and events.

// muxPhase is the arbitration phase. When several phase conditions hold at
// once the first in this order wins: owner, START in flight, fence, queued
// bids, idle.
type muxPhase uint8

const (
	// muxIdle: no owner, no START in flight, no fence, no bid.
	muxIdle muxPhase = iota
	// muxBidsQueued: at least one bid waits for an idle SYN.
	muxBidsQueued
	// muxStartAsync: a selected START waits for its adapter result through
	// the stream (RequestStart).
	muxStartAsync
	// muxStartBlocking: a selected START uses the blocking StartArbitration
	// call.
	muxStartBlocking
	// muxGatewayTransaction: the gateway owns the bus with an active
	// transaction.
	muxGatewayTransaction
	// muxGatewayGrace: the gateway owns the bus, its transaction ended and
	// the idle release grace still runs.
	muxGatewayGrace
	// muxSessionOwned: an external session owns the bus.
	muxSessionOwned
	// muxFenced: no selection before the next SYN; the state carries the
	// recovery kind.
	muxFenced
)

// owned reports whether the phase has a bus owner.
func (p muxPhase) owned() bool {
	return p == muxGatewayTransaction || p == muxGatewayGrace || p == muxSessionOwned
}

// muxSelection is the START in flight: the selected party (gatewaySessionID
// for the gateway) and the source it bid. The zero value is no START in flight.
type muxSelection struct {
	pending   bool
	party     uint64
	initiator byte
}

// muxBidSetSize is the number of queued external bidders muxBidSet names.
const muxBidSetSize = 4

// muxBidSet is the set of queued bids by party: whether the gateway bid waits
// and the waiting external sessions in ascending order. Session IDs start at
// one, so a zero entry ends the list; bidders beyond muxBidSetSize are only
// counted.
type muxBidSet struct {
	gateway  bool
	sessions [muxBidSetSize]uint64
	more     int
}

// add records the bid of party.
func (b *muxBidSet) add(party uint64) {
	if party == gatewaySessionID {
		b.gateway = true
		return
	}
	for i := range b.sessions {
		switch {
		case b.sessions[i] == party:
			return
		case b.sessions[i] == 0:
			b.sessions[i] = party
			return
		case b.sessions[i] > party:
			if b.sessions[len(b.sessions)-1] != 0 {
				b.more++
			}
			copy(b.sessions[i+1:], b.sessions[i:len(b.sessions)-1])
			b.sessions[i] = party
			return
		}
	}
	b.more++
}

// empty reports whether no bid waits.
func (b muxBidSet) empty() bool {
	return !b.gateway && b.sessions[0] == 0 && b.more == 0
}

// muxState is the typed mux state.
type muxState struct {
	phase muxPhase
	// recovery is the arbitration recovery kind: the kind of the fence in
	// muxFenced, and the pending lifecycle recovery after a reset.
	recovery arbitrationRecovery
	// absorb is the number of stale arbitration results still to be
	// absorbed; each STARTED or FAILED absorbs one.
	absorb int
	// blocking: a blocking StartArbitration call is outstanding.
	blocking bool
	// held: active arbitration is held for an out-of-step adapter, so
	// selection is skipped until a lifecycle boundary.
	held bool
	// resyncOwed is the number of wire SYNs still owed to the gateway active
	// path.
	resyncOwed uint8
	// start is the START in flight, whatever the phase.
	start muxSelection
	// owner is the session that owns the bus in muxSessionOwned.
	owner uint64
	// bids are the queued bids; a deferred gateway loser is no bid.
	bids muxBidSet
	// deferred: the loss of the gateway to the selected START is deferred
	// until the blocking call returns.
	deferred bool
	// retired: late results of a retired selection are recognized;
	// retiredSource is the source that selection bid.
	retired       bool
	retiredSource byte
	// echoPending: the gateway wrote a byte of its transaction whose echo has
	// not arrived; echoTerminator: that echo is the structural SYN that ends
	// the gateway's telegram. Both only in muxGatewayTransaction.
	echoPending    bool
	echoTerminator bool
	// telegramLive: an owner's own telegram is on the wire, past its source
	// byte, including after the owner was released.
	telegramLive bool
}

// resultBidder is the selection an arbitration result is matched against:
// the START in flight, unless a stale result is still to be absorbed.
func (s muxState) resultBidder() muxSelection {
	if s.absorb != 0 {
		return muxSelection{}
	}
	return s.start
}

// muxStateLocked derives the typed state from the mux fields. Callers hold
// stateMu; the arbiter fields are read under the arbiter lock, which nests
// inside stateMu.
func (m *Mux) muxStateLocked() muxState {
	s := muxState{
		recovery:   m.arbitrationRecovery,
		absorb:     m.pendingStartAbsorb,
		blocking:   m.blockingArbActive,
		held:       m.step.held,
		resyncOwed: m.collisionResyncOwed,
		retired:    m.retiredSelection,
	}
	if s.retired {
		s.retiredSource = m.localLoserSource
	}
	if p := m.pendingStart; p != nil {
		s.start = muxSelection{pending: true, party: p.sessionID, initiator: p.initiator}
	}
	m.arb.mu.Lock()
	hasOwner, owner := m.arb.hasOwner, m.arb.currentOwner
	if req := m.arb.pendingGateway; req != nil {
		s.bids.add(req.sessionID)
	}
	for _, req := range m.arb.pendingExternal {
		s.bids.add(req.sessionID)
	}
	s.deferred = m.arb.deferredGatewayLoser != nil
	m.arb.mu.Unlock()
	switch {
	case hasOwner && owner == gatewaySessionID && m.gatewayTxnActive:
		s.phase = muxGatewayTransaction
		next, structural, pending := m.gatewayEcho.peekNextExpectedWithFlags()
		s.echoPending = pending
		s.echoTerminator = pending && structural && next == protocol.SymbolSyn
	case hasOwner && owner == gatewaySessionID:
		s.phase = muxGatewayGrace
	case hasOwner:
		s.phase = muxSessionOwned
		s.owner = owner
	case s.start.pending && m.pendingStart.blockingArb:
		s.phase = muxStartBlocking
	case s.start.pending:
		s.phase = muxStartAsync
	case m.deferRegrantUntilNextSyn:
		s.phase = muxFenced
	case !s.bids.empty():
		s.phase = muxBidsQueued
	default:
		s.phase = muxIdle
	}
	s.telegramLive = m.phase.telegramLive()
	return s
}

// muxEventKind is the kind of one transition input.
type muxEventKind uint8

const (
	// muxEventSYN: a wire SYN.
	muxEventSYN muxEventKind = iota
	// muxEventSymbol: a received symbol other than a wire SYN.
	muxEventSymbol
	// muxEventBid: a party bids for the bus.
	muxEventBid
	// muxEventStarted: the adapter reports STARTED naming a source.
	muxEventStarted
	// muxEventFailed: the adapter reports FAILED naming a winner.
	muxEventFailed
	// muxEventStartedApplied and muxEventFailedApplied: the arbitration
	// result was applied to the selection; they carry whether that failed
	// the notified bidder and whether a lifecycle boundary passed meanwhile.
	muxEventStartedApplied
	muxEventFailedApplied
	// muxEventStartDeadline: the START deadline of the selection fires.
	muxEventStartDeadline
	// muxEventAbsorbDebounce: the absorb debounce deadline elapses.
	muxEventAbsorbDebounce
	// muxEventIdleGrace: a wire SYN after the idle release grace elapsed.
	muxEventIdleGrace
	// muxEventOwnershipLimit: a symbol after the ownership limit elapsed.
	muxEventOwnershipLimit
	// muxEventAbandonWatchdog: the abandon watchdog of the owning session's
	// grant fires with no SEND seen.
	muxEventAbandonWatchdog
	// muxEventSessionClose: a session disconnects.
	muxEventSessionClose
	// muxEventReset: the adapter reports RESETTED.
	muxEventReset
	// muxEventStartCallReturns and muxEventStartCallFails: the outstanding
	// blocking StartArbitration call returns, successfully or with an error.
	muxEventStartCallReturns
	muxEventStartCallFails
	// muxEventReadFailure: reading the upstream failed (timeout, parser or
	// connection error).
	muxEventReadFailure
)

// muxEvent is one transition input.
type muxEvent struct {
	kind muxEventKind
	// data is the symbol of muxEventSYN and muxEventSymbol and the source a
	// STARTED or FAILED names; escaped is the escape provenance of a symbol.
	data    byte
	escaped bool
	// phantom: the initiator predicate rejects the winner a FAILED names.
	phantom bool
	// bidderFailed and epochChanged are the outcome of applying a result.
	bidderFailed bool
	epochChanged bool
}

// Reasons of the events that cannot occur.
const (
	muxNoStartDeadline = "no START is in flight, so no START deadline is armed"
	muxNoAbsorb        = "no stale result is to be absorbed, so the debounce clears nothing"
	muxNoOwnerTimer    = "nobody owns the bus, so no ownership timer runs"
	muxNoSessionGrant  = "no external session owns the bus, so no abandon watchdog runs"
	muxNoBlockingCall  = "no blocking START call is outstanding, so none can return"
)

// muxDelivery says which sessions receive the winner byte of a result.
type muxDelivery uint8

const (
	muxDeliverNone muxDelivery = iota
	// muxDeliverAll: every external session.
	muxDeliverAll
	// muxDeliverOthers: every external session except the bidder.
	muxDeliverOthers
	// muxDeliverOthersNotLosers: every external session except the bidder
	// and the local losers, which already received the winner by FAILED.
	muxDeliverOthersNotLosers
)

// muxTransition is the decision of decideMuxTransition.
type muxTransition struct {
	// invalid is the reason why the event cannot occur in the state; empty
	// when it can.
	invalid string
	// bidder is the selection a STARTED or FAILED result is matched against.
	bidder muxSelection
	// notify is the source a FAILED result reports to that bidder.
	notify byte
	// The winner byte of an applied result: whether the local losers'
	// advertised source is reset, which sessions receive it (except names
	// the skipped bidder) and whether the passive observers receive it.
	resetLosers bool
	deliver     muxDelivery
	except      uint64
	passive     bool
}

// muxEventCannotOccur is the validity rule: it returns the reason why the
// event cannot occur in the state, or "" when it can.
func muxEventCannotOccur(s muxState, kind muxEventKind) string {
	switch kind {
	case muxEventStartDeadline:
		if !s.start.pending {
			return muxNoStartDeadline
		}
	case muxEventAbsorbDebounce:
		if s.absorb == 0 {
			return muxNoAbsorb
		}
	case muxEventIdleGrace, muxEventOwnershipLimit:
		if !s.phase.owned() {
			return muxNoOwnerTimer
		}
	case muxEventAbandonWatchdog:
		if s.phase != muxSessionOwned {
			return muxNoSessionGrant
		}
	case muxEventStartCallReturns, muxEventStartCallFails:
		if !s.blocking {
			return muxNoBlockingCall
		}
	}
	return ""
}

// decideMuxTransition is the transition function. For every event it decides
// whether the event can occur in the state; for the arbitration results it
// decides which bidder the result is matched against, the source FAILED
// reports to it and, once the result is applied, where the winner byte goes.
func decideMuxTransition(s muxState, ev muxEvent) muxTransition {
	if reason := muxEventCannotOccur(s, ev.kind); reason != "" {
		return muxTransition{invalid: reason}
	}
	var t muxTransition
	switch ev.kind {
	case muxEventStarted:
		t.bidder = s.resultBidder()
		t.notify = ev.data
	case muxEventFailed:
		t.bidder = s.resultBidder()
		t.notify = ev.data
		// Substitute the phantom byte by the bidder's own initiator, from
		// the same state as the bidder snapshot, so a concurrent re-submit
		// cannot move the selection between read and use.
		if ev.phantom && t.bidder.pending {
			t.notify = t.bidder.initiator
		}
	case muxEventStartedApplied:
		t.bidder = s.resultBidder()
		if !ev.epochChanged {
			decideStartedWinnerByte(&t, s, ev)
		}
	case muxEventFailedApplied:
		t.bidder = s.resultBidder()
		if !ev.epochChanged {
			decideFailedWinnerByte(&t, ev)
		}
	}
	return t
}

// decideStartedWinnerByte routes the source a STARTED named, given the
// bidder snapshot taken before the result was applied.
func decideStartedWinnerByte(t *muxTransition, s muxState, ev muxEvent) {
	bidder := t.bidder
	if !bidder.pending {
		if s.retired && ev.data != s.retiredSource {
			// A retired bidder has no result frame conveying the foreign
			// source. Reset local losers that still hold the advertised
			// source, then deliver the actual source once to every session
			// and passive observer. An intervening SYN clears the loser
			// list, but does not remove this wire-observation obligation.
			t.resetLosers, t.deliver, t.passive = true, muxDeliverAll, true
		}
		return
	}
	// Synthesize the arbitration-winner byte as a passive
	// observation for external sessions ONLY when the gateway
	// is the winner. The wire DID carry this byte (e.g. 0x7F)
	// at this point, but the ENH adapter consumes it as a
	// STARTED control event instead of echoing it via
	// ResReceived. Without this synthesis, external sessions
	// like ebusd see the gateway's transaction stream as
	// `... SYN, 0x15 (target), 0xB5, 0x24, ...` with no
	// initiator-byte boundary. Their bus parser discards the
	// frame as malformed (0x15 isn't a valid initiator byte
	// — low nibble 5 isn't a valid arbitration nibble) and
	// the gateway's traffic vanishes from their grab buffer
	// / initiator enumeration. Worse, their bus state
	// machine thinks the bus is idle when it isn't, leading
	// to mis-timed bid attempts and the "won in invalid
	// state" + read-timeout cascade seen in
	// EBUSD-VERIFICATION-2026-05-10.md F-9 / F-10.
	//
	// We restrict synthesis to gateway-wins because:
	//   - When an external session wins, it receives
	//     ENHResStarted instead (queued at its stream
	//     position by completeArbitrationGrant since issue
	//     #1003; previously by its handleStart goroutine).
	//     A synthesized ENHResReceived(winner_byte) would
	//     be a duplicate initiator for that session (Codex
	//     P2 follow-up on PR #620).
	//   - For gateway wins, there is no competing per-
	//     session control event — the gateway consumes
	//     STARTED synchronously, so no goroutine race
	//     exists.
	//
	// If hasOwner is false after handleArbitrationResponse,
	// the STARTED was absorbed as stale (cancelled bid).
	// Don't synthesize — nobody is using the bus from this
	// stale win.
	// Use the snapshotted bidder identity (NOT a fresh
	// owner() read) so a winner-disconnect-then-release
	// race doesn't skip synthesis for the remaining
	// monitors. The wire byte was already consumed by the
	// adapter; synthesis is required regardless of whether
	// the winning session is still attached.
	if ev.data == bidder.initiator {
		// Matched STARTED — handleArbitrationResponse
		// granted ownership to the bidder, so event.Data
		// is the bidder's own initiator byte.
		t.deliver, t.except = muxDeliverOthersNotLosers, bidder.party
		if bidder.party == gatewaySessionID {
			// Gateway won — every external session
			// needs the byte; no per-session control
			// event competes. Passive observers are
			// intentionally NOT notified (gateway-
			// owned bytes are suppressed in
			// onReceived too — consistent).
			return
		}
		// External session won — skip the winner
		// (completeArbitrationGrant already queued
		// its ENHResStarted; a synthesized
		// RECEIVED would duplicate it). Other
		// external sessions (multi-monitor
		// deployments) still need the byte to
		// keep their bus state synced — without
		// it they'd misread the next target/PB
		// byte as the frame source. (Codex P2
		// round 3.)
		//
		// Passive observer pipeline also needs
		// the non-gateway winner byte — the
		// PassiveTransport / bus reconstructor
		// otherwise sees the next target/PB byte
		// as the frame source for externally-
		// owned frames. onReceived's normal
		// PassiveEventSymbol path is bypassed for
		// StreamEventStarted, so synthesize here.
		// (Codex P2 round 5 on PR #620.)
		t.passive = true
		return
	}
	t.resetLosers = true
	// Mismatched STARTED (AM56): the wire byte
	// belongs to some third party, not the bidder.
	// handleArbitrationResponse has already failed
	// the bidder session and armed absorb, so the
	// bidder is no longer the winner. The third-
	// party winner byte still needs to flow to the
	// non-bidder sessions and passive observers so
	// their bus state stays synced with physical
	// reality. (Codex P2 round 6 on PR #620.)
	if bidder.party == gatewaySessionID || !ev.bidderFailed {
		// Gateway was bidder but lost to a third
		// party, or cancellation suppressed FAILED:
		// no session result conveys the actual source.
		t.deliver = muxDeliverAll
	} else {
		// External session was bidder but lost.
		// Skip the loser: handleArbitrationResponse's
		// mismatch path now notifies the bidder with
		// initiator=event.Data (the third-party
		// winner) — see Codex P2 round 7 fix. The
		// bidder's handleStart goroutine therefore
		// delivers ENHResFailed(winner_byte), which
		// conveys the actual wire owner and keeps the
		// bidder's bus reconstructor in sync. A
		// synthesized RECEIVED(winner) here would
		// race that frame on the same session.
		t.deliver, t.except = muxDeliverOthers, bidder.party
	}
	// Third-party winner byte is a true passive
	// observation regardless of who the bidder was
	// — always emit it for the passive pipeline.
	t.passive = true
}

// decideFailedWinnerByte routes the winner a FAILED named, given the bidder
// snapshot taken before the result was applied.
func decideFailedWinnerByte(t *muxTransition, ev muxEvent) {
	// Synthesize the winner byte:
	//   - No notified bidder (retired, replaced, or no pending):
	//     deliver to all external sessions. The wire byte
	//     was consumed; the rest of the third-party frame
	//     will continue through onReceived. Without this,
	//     external monitors and the passive reconstructor
	//     would parse the next target/PB byte as the frame
	//     source and desynchronize.
	//   - Gateway-lost: deliver to all external sessions
	//     (gateway has no session in m.sessions, no skip).
	//   - External-lost: deliver to all OTHER external
	//     sessions (skip the losing bidder; their session's
	//     handleStart goroutine delivers ENHResFailed via
	//     deliverFailed, which would race a synthesized
	//     RECEIVED(winner)). Multi-monitor deployments need
	//     the byte on the non-bidder sessions to keep their
	//     bus state synced. (Codex P2 round 3 on PR #620.)
	// Local FAILED advertised a source even if the physical result
	// is a suppressed phantom. Reset affected clients before the
	// next wire byte so they can reconstruct the actual source.
	t.resetLosers = true
	if !ev.phantom {
		// Without a pending bidder the snapshot names the gateway's
		// session ID, as the bidder ID does when none was captured.
		switch {
		case !ev.bidderFailed:
			t.deliver = muxDeliverAll
		case t.bidder.party == gatewaySessionID:
			t.deliver = muxDeliverAll
		default:
			t.deliver, t.except = muxDeliverOthers, t.bidder.party
		}
	}
	// FAILED always means a non-gateway initiator won the
	// arbitration (gateway-win produces STARTED, not FAILED).
	// Emit the winner byte to passive observers so the bus
	// reconstructor sees the correct frame source for the
	// third-party transaction that's about to flow through
	// onReceived. Emit unconditionally — the wire byte
	// happened irrespective of whether a gateway bid was
	// pending. (Codex P2 round 5 + round 8 on PR #620.)
	t.passive = true
}
