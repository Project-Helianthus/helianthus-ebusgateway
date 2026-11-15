package adaptermux

import (
	"errors"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// readHealth is the read loop's blackhole detection state.
//
// AM27: duration-based TCP blackhole detection.
// Track when the first timeout in the current streak started and
// when we last received actual data. A bus that has NEVER sent
// data is legitimately quiet — don't treat consecutive timeouts
// as a TCP blackhole in that case.
//
// XR_BLACKHOLE_DURATION: uses cfg.BlackholeThreshold (default 30s)
// instead of counting timeout iterations. This decouples the
// detection threshold from ReadTimeout — changing ReadTimeout no
// longer silently changes when blackhole reconnect fires.
type readHealth struct {
	firstTimeoutTime time.Time // zero until a streak starts
	lastDataTime     time.Time
}

// streamItemEvent translates one upstream read into a transition event. ok
// is false for a raw-only item: an escape lead, a 0xAA absorbed inside a
// pending pair, or a symbol discarded by a decode fault, which has no logical
// consequence.
func streamItemEvent(item upstreamItem, err error) (ev muxEvent, ok bool) {
	if err != nil {
		return muxEvent{kind: muxEventReadFailure}, true
	}
	event := item.event
	switch event.Kind {
	case 0:
		return muxEvent{}, false
	case transport.StreamEventStarted:
		return muxEvent{kind: muxEventStarted, data: event.Data}, true
	case transport.StreamEventFailed:
		return muxEvent{kind: muxEventFailed, data: event.Data}, true
	case transport.StreamEventReset:
		return muxEvent{kind: muxEventReset}, true
	case transport.StreamEventWireSyn:
		// F-38-fix (PR #155 P1, 2026-05-15): pre-grant SYN passive
		// marker emitted by enh_transport during awaitingStart.
		// Identical downstream effect to a wire SYN byte (route to
		// onReceived as 0xAA with wasEscaped=false), but the new
		// kind keeps the active sender's ReadByte queue clean.
		return muxEvent{kind: muxEventSYN, data: event.Byte}, true
	}
	// F-23 (batch-19, Codex bot on PR-2): thread the upstream
	// WasEscaped flag through onReceived so adapter-direct
	// passive consumers see the same provenance the raw-TCP
	// passive observers get via their local escape decoder.
	kind := muxEventSymbol
	if event.Kind == transport.StreamEventByte && event.Byte == protocol.SymbolSyn && !event.WasEscaped {
		kind = muxEventSYN
	}
	return muxEvent{kind: kind, data: event.Byte, escaped: event.WasEscaped}, true
}

// onReadFailure handles a failed upstream read: shutdown, adapter reset,
// quiet-bus timeout with blackhole detection, soft parser error or a
// connection error. It reports whether the read loop continues.
func (m *Mux) onReadFailure(err error, health *readHealth) bool {
	if m.ctx.Err() != nil {
		return false // context cancelled, shutting down
	}
	if errors.Is(err, ebuserrors.ErrAdapterReset) {
		m.handleReset()
		return true
	}
	// Read timeouts on a quiet bus are normal (no data within
	// ReadTimeout window) — treat as idle, not disconnect.
	// Matches passive_bus_tap.go behavior (Codex P2 #3059211395).
	if errors.Is(err, ebuserrors.ErrTimeout) || isNetTimeout(err) {
		// AM27: only treat as TCP blackhole if we were actively
		// receiving data before the timeout streak. A bus that
		// has NEVER sent data within this session is legitimately
		// quiet — not a blackhole.
		if !health.lastDataTime.IsZero() && health.firstTimeoutTime.IsZero() {
			health.firstTimeoutTime = time.Now()
		}

		// AM27/XR_BLACKHOLE_DURATION: detect TCP blackhole after
		// cfg.BlackholeThreshold (default 30s) of consecutive
		// timeouts since the bus was last active.
		if !health.firstTimeoutTime.IsZero() && time.Since(health.firstTimeoutTime) > m.cfg.BlackholeThreshold {
			m.logger.Printf("adaptermux: consecutive timeouts for %v (threshold %v), triggering reconnect", time.Since(health.firstTimeoutTime).Round(time.Millisecond), m.cfg.BlackholeThreshold)
			health.firstTimeoutTime = time.Time{}
			health.lastDataTime = time.Time{} // reset after reconnect so quiet bus doesn't re-trigger
			return m.reconnectFromReadLoop()
		}

		m.enforceOwnershipLimitOnQuietBus()
		return true
	}
	if errors.Is(err, ebuserrors.ErrInvalidPayload) {
		m.logger.Printf("adaptermux: soft parser error (no reconnect): %v", err)
		health.firstTimeoutTime = time.Time{}
		health.lastDataTime = time.Now()
		return true
	}
	m.logger.Printf("adaptermux: read error: %v", err)
	health.firstTimeoutTime = time.Time{}
	health.lastDataTime = time.Time{} // reset after reconnect so quiet bus doesn't trigger blackhole
	return m.reconnectFromReadLoop()
}

// reconnectFromReadLoop reconnects the upstream and reports whether this
// read loop keeps reading: it stops when reconnecting gave up or another
// read loop took over.
func (m *Mux) reconnectFromReadLoop() bool {
	delegated, reconnErr := m.reconnect()
	if reconnErr != nil {
		m.logger.Printf("adaptermux: reconnect gave up: %v", reconnErr)
		return false
	}
	return !delegated
}

// enforceOwnershipLimitOnQuietBus enforces the ownership timeout on a quiet
// bus — onReceived won't run to check MaxOwnershipDuration.
func (m *Mux) enforceOwnershipLimitOnQuietBus() {
	quietBusTimedOut := false
	var quietBusOwnerWasGateway bool
	m.stateMu.Lock()
	ownerID, _, hasOwner := m.arb.owner()
	if hasOwner && !m.busOwned.IsZero() &&
		time.Since(m.busOwned) > m.ownershipDurationLimit(ownerID) {
		m.arb.releaseOwnership(ownerID)
		m.gatewayEcho.reset()
		if ownerID == gatewaySessionID && m.gatewayTxnActive {
			m.gatewayTxnActive = false
			m.recordGatewayInactive(ReasonMaxOwnership)
			m.discardStaleActiveBytesLocked()
			quietBusOwnerWasGateway = true
		}
		quietBusTimedOut = true
	}
	m.stateMu.Unlock()
	// Codex round-2 MUST FIX on PR #647: if the
	// gateway lost ownership via max-duration timeout
	// while a watchdog was armed, the echo will never
	// arrive on this transport instance — cancel so
	// no stale soft/hard fire emits an admin event
	// for a write whose context is gone.
	if quietBusOwnerWasGateway {
		if pacer := m.SessionPacer(gatewaySessionID); pacer != nil {
			pacer.CancelEchoWatchdog()
		}
	}

	// F-18: per-session echo trackers removed; nothing to
	// reset for external sessions on ownership timeout.
	_ = quietBusTimedOut
}

// admitStreamItem runs the classifier and the raw view over one read item
// before its logical dispatch. It returns the bids the raw view froze and
// whether the item is admitted to the logical dispatch.
func (m *Mux) admitStreamItem(item upstreamItem) (map[*startRequest]chan startResult, bool) {
	// Phase 3 Step B3.2 / B3.6b: route every StreamEvent through
	// the v8 classifier BEFORE the dispatch switch. The
	// nil-guard short-circuits BOTH the method call AND the
	// time.Now() evaluation when the classifier is off
	// (production default), keeping the hot-path overhead at
	// zero.
	//
	// B3.6b: in ModeEnforce the classifier may return drop=true
	// for AA-injection bytes (FSM DecisionDropAaInjection).
	// When that happens we SKIP the legacy dispatch entirely
	// for THIS event — the byte is filtered out of session
	// streams, classifier counters still record it via
	// EnforceDropsAppliedTotal + fsmDropAaInjectionTotal.
	//
	// ModeShadow always returns drop=false even when the FSM
	// recommends a drop — operators run Shadow first to
	// validate the classifier against the wire before
	// promoting to Enforce.
	//
	// Issue #1003: the classifier judges the logical item. When it
	// drops that item, the raw symbol that completed it is dropped
	// from the session view too, so enforce mode filters both views
	// consistently. A raw-only item (no logical item) is not
	// classified.
	event := item.event
	m.reconcileArbitrationClassifier(event)
	// Issue #1003: the gateway escape-pair verifier sees every
	// observed raw symbol in true wire order, before any session-view
	// filtering below. The v8 AA-injection filter only shapes what
	// sessions (and the logical consumers) receive; an 0xAA it drops
	// between an escape lead and its suffix still decides the pair
	// (A9 AA 00 fails it as a malformed echo).
	if item.hasRaw {
		m.observeGatewayLeadEcho(item.rawSymbol, event)
	}
	if m.v8 != nil && event.Kind != 0 {
		if m.v8.Observe(event, time.Now()) {
			// A withheld wire SYN still counts toward the ownership
			// limits; it stays withheld from sessions and observers.
			if event.Kind == transport.StreamEventWireSyn ||
				(event.Kind == transport.StreamEventByte && event.Byte == protocol.SymbolSyn && !event.WasEscaped) {
				m.enforceOwnershipLimitsOnWithheldSYN()
			}
			return nil, false
		}
	} else if m.v8 != nil && item.hasRaw && item.rawSymbol == protocol.SymbolSyn {
		// A raw-only 0xAA (absorbed inside a pending escape pair) has
		// no logical item but is still a wire SYN the AA-injection
		// filter must judge; a drop withholds it from sessions.
		if m.v8.Observe(transport.StreamEvent{Kind: transport.StreamEventWireSyn, Byte: protocol.SymbolSyn}, time.Now()) {
			m.enforceOwnershipLimitsOnWithheldSYN()
			return nil, false
		}
	}

	// Issue #1003: raw view first. External sessions receive the
	// observed wire symbol immediately and in upstream order (an
	// escape lead before its suffix exists); the logical item it
	// completed, if any, is then processed below for phase tracking,
	// passive reconstruction and the gateway active path, without a
	// second session delivery.
	var bids map[*startRequest]chan startResult
	if item.hasRaw {
		bids = m.onRawSymbol(item.rawSymbol, event)
	}
	return bids, true
}

// onStreamStarted handles a STARTED item.
func (m *Mux) onStreamStarted(ev muxEvent) {
	m.logger.Printf("adaptermux: readLoop got StreamEventStarted data=0x%02X", ev.data)
	// Peek the pending bidder's session ID BEFORE
	// handleArbitrationResponse fires. Without this, an
	// external winner can call RemoveSession immediately
	// after receiving STARTED (e.g. ebusd disconnects on
	// transaction completion) which releases ownership;
	// our subsequent m.arb.owner() would return owned=false
	// and we'd miss the chance to synthesize for non-winner
	// sessions. Codex P2 round 4 on PR #620.
	//
	// pendingStartAbsorb > 0 means the response is for a
	// cancelled bid. Local loser reconstruction is snapshotted
	// independently because it can outlive that bidder.
	m.stateMu.Lock()
	state := m.muxStateLocked()
	startedEpoch := m.arb.lifecycleEpoch.Load()
	startedLosers := m.localLosers
	m.lastWireActivity = time.Now()
	m.stateMu.Unlock()
	bidderFailed := m.handleArbitrationResponse(true, ev.data)
	m.expectForeignTelegram(ev.data)
	applied := ev
	applied.kind = muxEventStartedApplied
	applied.bidderFailed = bidderFailed
	applied.epochChanged = startedEpoch != m.arb.lifecycleEpoch.Load()
	m.applyWinnerByte(decideMuxTransition(state, applied), ev.data, startedLosers)
}

// onStreamFailed handles a FAILED item.
func (m *Mux) onStreamFailed(ev muxEvent) {
	m.logger.Printf("adaptermux: readLoop got StreamEventFailed data=0x%02X", ev.data)
	// Peek the *active* bidder (if any) BEFORE
	// handleArbitrationResponse clears pendingStart. The
	// presence and identity of the bidder dictate how we
	// route the synthesized winner byte; the wire byte
	// itself always needs to flow to observers regardless.
	//
	// `pendingStartAbsorb > 0` indicates this FAILED is the
	// stale response to a cancelled bid. The bid is gone, so
	// no per-session control event races us — but the byte
	// was still consumed from the wire and the third-party
	// transaction continues, so observers still need it.
	// (Codex P2 round 8 on PR #620.)
	//
	// `pendingStart == nil` (no bid at all) is similarly a
	// pure passive observation: deliver to all sessions +
	// emit passive.
	// Phantom-collision filter (proxy-bug C5 / R5): compute
	// the predicate result BEFORE we take stateMu so the
	// (potentially operator-supplied) callback never runs
	// under lock. The IsKnownInitiatorByte predicate is
	// pure / read-only by contract, so this snapshot is
	// safe regardless of any concurrent state change.
	if m.cfg.IsKnownInitiatorByte != nil && !m.cfg.IsKnownInitiatorByte(ev.data) {
		ev.phantom = true
		m.logger.Printf("adaptermux: phantom FAILED data=0x%02X (not a known bus initiator) — substituting bidder's own initiator on notify", ev.data)
	}

	// Single stateMu critical section that fuses three
	// previously-separate steps and closes the round-6
	// unlock/relock race (Codex P2 on PR #624). Order matters:
	//
	//   - Bump lastWireActivity FIRST so any concurrent
	//     requestStartForSession that grabs stateMu after
	//     this section observes the fresh timestamp and does
	//     NOT take the idle-kick path. (Codex P2 round 6
	//     on PR #623 + Codex P2 on PR #624.)
	//
	//   - Snapshot the active bidder so the FAILED routing
	//     below can decide where to deliver the winner byte
	//     and whether to suppress the bidder's own notify.
	//     `pendingStartAbsorb > 0` indicates a stale FAILED
	//     for a cancelled bid; `pendingStart == nil` is a
	//     pure passive observation.
	//
	//   - Substitute the phantom-byte → bidder's own
	//     initiator atomically with the snapshot so a
	//     concurrent re-submit cannot move the pointer
	//     between read and use.
	//
	// All three reads happen under one acquire/release of
	// stateMu — no goroutine can interleave between the
	// bump and the snapshot.
	m.stateMu.Lock()
	m.lastWireActivity = time.Now()
	failedEpoch := m.arb.lifecycleEpoch.Load()
	failedLosers := m.localLosers
	state := m.muxStateLocked()
	decision := decideMuxTransition(state, ev)
	m.stateMu.Unlock()

	bidderFailed := m.handleArbitrationResponse(false, decision.notify)
	m.expectForeignTelegram(ev.data)
	applied := ev
	applied.kind = muxEventFailedApplied
	applied.bidderFailed = bidderFailed
	applied.epochChanged = failedEpoch != m.arb.lifecycleEpoch.Load()
	m.applyWinnerByte(decideMuxTransition(state, applied), ev.data, failedLosers)
}

// applyWinnerByte applies the winner-byte decision of an applied result in
// the order the read loop always used: loser reset, session delivery, passive
// observation.
func (m *Mux) applyWinnerByte(t muxTransition, winner byte, losers []*startRequest) {
	if t.resetLosers {
		m.resetLocalLoserSources(losers)
	}
	switch t.deliver {
	case muxDeliverAll:
		m.deliverToSessions(winner, 0, false, time.Now())
	case muxDeliverOthers:
		m.deliverWinnerByteToOtherSessions(winner, t.except)
	case muxDeliverOthersNotLosers:
		m.deliverWinnerByteToOtherSessions(winner, t.except, losers...)
	}
	if t.passive {
		m.emitPassive(PassiveEvent{
			Kind:       PassiveEventSymbol,
			Symbol:     winner,
			ObservedAt: time.Now(),
		})
	}
}

// onStreamSymbol handles a received symbol or wire SYN. deliverToSessions is
// false when the raw view already delivered the symbol to the sessions.
func (m *Mux) onStreamSymbol(ev muxEvent, deliverToSessions bool, bids map[*startRequest]chan startResult) {
	m.onLogicalReceivedForBids(ev.data, ev.escaped, deliverToSessions, bids)
}
