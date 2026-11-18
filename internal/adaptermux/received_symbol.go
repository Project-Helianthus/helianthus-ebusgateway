package adaptermux

import (
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// A received symbol runs through named steps. The entry step derives the
// typed state under stateMu, takes the owner from it, tracks the wire phase
// and releases an owner past its ownership limit when decideMuxTransition
// says that event can occur. A wire SYN then takes the wire-SYN path; any
// other symbol takes the data-symbol decision (owner, pending echo, foreign
// initiator first byte). Each path releases stateMu in onLogicalReceivedForBids
// and applies its effects afterwards in the order the handler always used;
// the data path ends with the terminal release. An effect whose outcome the
// next decision reads (an ownership release, a foreign initiator release)
// comes back as a second input under the same lock: the released owner is
// cleared from the step's view and a foreign initiator byte re-enters the
// wire-phase tracker as third-party traffic.

// receivedSymbol carries one received symbol through the steps of its
// handler.
type receivedSymbol struct {
	ev                muxEvent
	now               time.Time
	deliverToSessions bool
	// bids is the eligibility boundary frozen before raw SYN delivery, or
	// nil for logical-only input, which takes its snapshot under the state
	// lock.
	bids map[*startRequest]chan startResult
	// ownerID and hasOwner are the owner when the symbol arrived; hasOwner
	// is cleared when the ownership limit released that owner.
	ownerID  uint64
	hasOwner bool
	// phaseEvent is the wire-phase event of the symbol.
	phaseEvent wirePhaseEvent
	// AM11: track ownership timeout so we can reset external session
	// echo trackers after releasing stateMu (ABBA avoidance).
	ownershipTimedOut        bool
	ownershipTimedOutGateway bool
	// Collect passive events under lock, emit after unlock (Issue#3 fix).
	passiveEvents []PassiveEvent
}

// receivedSymbolEvent translates one logical symbol into its transition
// event.
func receivedSymbolEvent(symbol byte, wasEscaped bool) muxEvent {
	// F-23 (batch-19, Codex bot on PR-2): require wasEscaped=false
	// for the SYN classification. An escape-decoded payload 0xAA
	// (wasEscaped=true) is user data from a third-party frame, NOT
	// a wire SYN — treating it as SYN would falsely release
	// ownership and corrupt the wire-phase tracker. Same
	// distinction the passive reconstructor already enforces (F-19d).
	if symbol == protocol.SymbolSyn && !wasEscaped {
		return muxEvent{kind: muxEventSYN, data: symbol}
	}
	return muxEvent{kind: muxEventSymbol, data: symbol, escaped: wasEscaped}
}

// ownerParty returns the bus owner of the state: the gateway in the gateway
// phases, the owning session in muxSessionOwned, none otherwise.
func (s muxState) ownerParty() (uint64, bool) {
	switch s.phase {
	case muxGatewayTransaction, muxGatewayGrace:
		return gatewaySessionID, true
	case muxSessionOwned:
		return s.owner, true
	}
	return 0, false
}

// onLogicalReceivedForBids retains the eligibility boundary frozen before raw
// SYN delivery. Logical-only input takes its snapshot under the state lock below.
func (m *Mux) onLogicalReceivedForBids(symbol byte, wasEscaped bool, deliverToSessions bool, bids map[*startRequest]chan startResult) {
	rs := receivedSymbol{
		ev:                receivedSymbolEvent(symbol, wasEscaped),
		now:               time.Now(),
		deliverToSessions: deliverToSessions,
		bids:              bids,
	}

	// --- Phase 1: state update under stateMu ---
	m.stateMu.Lock()
	m.enterReceivedSymbolLocked(&rs)
	// F-23 (batch-19, Codex bot on PR-2): use the wire-SYN
	// classification computed above so escape-decoded payload 0xAA
	// (wasEscaped=true) takes the non-SYN branch.
	if rs.ev.kind == muxEventSYN {
		syn := m.decideReceivedWireSYNLocked(&rs)
		m.stateMu.Unlock()
		m.applyReceivedWireSYN(&rs, syn)
		return
	}
	data := m.decideReceivedDataSymbolLocked(&rs)
	m.stateMu.Unlock()
	m.applyReceivedDataSymbol(&rs, data)
	m.releaseOnTerminalPhaseEvent(&rs)
}

// enterReceivedSymbolLocked is the entry step of every received symbol: the
// owner from the typed state, the wire activity, the wire phase and the
// ownership limit. Caller holds stateMu.
func (m *Mux) enterReceivedSymbolLocked(rs *receivedSymbol) {
	state := m.muxStateLocked()
	rs.ownerID, rs.hasOwner = state.ownerParty()

	if rs.ev.kind != muxEventSYN {
		m.lastWireActivity = rs.now
	}

	// Skip wire phase tracking for non-SYN bytes during gateway ownership.
	// The gateway's bus.Send handles the transaction directly via echo
	// matching. Skipping advance() for data bytes prevents premature
	// WaitCmdAck → CmdNACK → idle from off-by-one byte counting.
	//
	// SYN is always processed: ownership release depends on the SYN
	// handler (SYNIdle + IdleReleaseGrace or SYNTimeout). During gateway
	// ownership, treat SYN as SYNIdle since the data-phase tracking is
	// skipped.
	if rs.ev.kind == muxEventSYN {
		// FAILED supplied the source only until this stream boundary.
		// A later STARTED must supply it again for loser reconstruction.
		m.localLosers = nil
		if rs.hasOwner && rs.ownerID == gatewaySessionID {
			// Gateway owns bus, phase tracking skipped for data bytes.
			// Treat SYN as idle so IdleReleaseGrace controls release.
			rs.phaseEvent = wirePhaseEventSYNIdle
			m.phase.reset(wirePhaseIdle)
		} else {
			// F-23 (Codex bot on PR-2): provenance-aware advance —
			// isWireSyn=true tells the tracker this byte IS a real
			// wire SYN. Equivalent to the value-only contract here
			// but propagates the classification consistently.
			rs.phaseEvent = m.phase.advanceWithProvenance(rs.ev.data, true)
		}
	} else if !rs.hasOwner || rs.ownerID != gatewaySessionID {
		// F-23 (Codex bot on PR-2): non-SYN branch. Pass
		// isWireSyn=false explicitly so the tracker does NOT
		// internally fall back to its value-only `symbol ==
		// SymbolSyn` check — an escape-decoded payload 0xAA
		// (wasEscaped=true here) must be treated as data, not as
		// a SYN frame terminator.
		rs.phaseEvent = m.phase.advanceWithProvenance(rs.ev.data, false)
	}
	m.releaseOnOwnershipLimitLocked(rs, state)
}

// releaseOnOwnershipLimitLocked releases the owner once its ownership limit
// elapsed. The ownership-limit event can occur only while the bus is owned,
// which decideMuxTransition decides over the state the entry step derived.
// Caller holds stateMu.
func (m *Mux) releaseOnOwnershipLimitLocked(rs *receivedSymbol, state muxState) {
	limit := muxEvent{kind: muxEventOwnershipLimit, data: rs.ev.data}
	if decideMuxTransition(state, limit).invalid != "" || m.busOwned.IsZero() ||
		time.Since(m.busOwned) <= m.ownershipDurationLimit(rs.ownerID) {
		return
	}
	m.arb.releaseOwnership(rs.ownerID)
	m.gatewayEcho.reset()
	if rs.ownerID == gatewaySessionID && m.gatewayTxnActive {
		m.gatewayTxnActive = false
		m.recordGatewayInactive(ReasonMaxOwnership)
		m.discardStaleActiveBytesLocked()
		rs.ownershipTimedOutGateway = true
	}
	rs.hasOwner = false
	rs.ownershipTimedOut = true
}

// receivedWireSYN is the decision of the wire-SYN path.
type receivedWireSYN struct {
	activeExpects  bool
	shouldTryGrant bool
	cancelWatchdog bool
}

// decideReceivedWireSYNLocked is the wire-SYN path under stateMu: the
// shape diagnostics, onSYNLocked, the active-path decision and the bid
// snapshot. Caller holds stateMu.
func (m *Mux) decideReceivedWireSYNLocked(rs *receivedSymbol) receivedWireSYN {
	symbol := rs.ev.data
	// Shape diag: count SYN markers seen during gateway ownership
	// while a gateway transaction is actually active. Pre-write or
	// post-inactive idle chatter must not contaminate the current txn.
	// Must run BEFORE onSYNLocked so the counter reflects the SYN that
	// triggers a possible inactive transition.
	if rs.hasOwner && rs.ownerID == gatewaySessionID && m.gatewayTxnActive {
		m.recordReadPrefixAndClassify(symbol)
	}
	// Runtime-soak: bus.Send returns BEFORE the trailing SYN (reads
	// are count-based, not SYN-terminated). So onSYNLocked's
	// gatewayTxnActive=false is the correct state for delivery —
	// capture AFTER to skip the trailing SYN that has no consumer.
	var syn receivedWireSYN
	var preEchoSuppressed bool
	rs.passiveEvents, syn.shouldTryGrant, preEchoSuppressed, syn.cancelWatchdog = m.onSYNLocked(rs.phaseEvent, rs.ownerID, rs.hasOwner, rs.now)
	syn.activeExpects = m.activePathExpectsByte(symbol)
	if preEchoSuppressed {
		// Pre-echo SYN suppression (echo_mismatch root cause fix).
		// After completeArbitrationGrant, readLoop can read a SYN from
		// the TCP/ENH buffer BEFORE bus.Send's first Write reaches the
		// adapter. Delivering this SYN to activeCh races the real echo
		// byte — the consumer (sendRawWithEcho) then reads 0xAA in
		// place of the expected echo and emits echo_mismatch (13,904
		// events observed in production soak). When onSYNLocked sees
		// gatewayTxnActive=true && bytesRead==0, it signals us to
		// suppress the activeCh delivery; gatewayTxnActive stays true
		// and the next real echo byte completes the handshake normally.
		syn.activeExpects = false
	}
	if syn.shouldTryGrant && rs.bids == nil {
		rs.bids = m.arb.snapshotBids()
	}
	return syn
}

// applyReceivedWireSYN applies the effects of the wire-SYN path after
// stateMu was released: the watchdog cancel, the active-path delivery, the
// passive events, the session SYN and the grant attempt.
func (m *Mux) applyReceivedWireSYN(rs *receivedSymbol, syn receivedWireSYN) {
	symbol := rs.ev.data
	// F-18: per-session echo trackers removed; nothing to reset
	// for external sessions on ownership timeout, and no SYN-flush
	// to perform. Gateway-side echo handling continues unchanged
	// via m.gatewayEcho.
	_ = rs.ownershipTimedOut
	// Codex round-2 MUST FIX on PR #647: if the gateway lost
	// ownership via max-duration timeout while a watchdog was
	// armed, the echo will never arrive — cancel.
	// Codex round-3 MUST FIX #1: same for SYN-timeout /
	// SYN-idle teardown branches inside onSYNLocked.
	if rs.ownershipTimedOutGateway || syn.cancelWatchdog {
		m.cancelGatewayWatchdog()
	}

	// --- Phase 2: deliver outside all locks ---

	if syn.activeExpects {
		// activeExpects was decided under stateMu above and implies
		// the gateway owns the bus with gatewayTxnActive=true — count
		// this enqueue so bytesDeliveredToActive reflects real
		// adapter-originated bytes delivered during this grant.
		//
		// P12 pass-2 (Codex P1): re-acquire stateMu and re-validate
		// gatewayTxnActive before delivering. Mirrors the non-SYN
		// path at line ~1220. Pre-pass-2 sendLoop could drain+arm
		// for byte K+1 between stateMu.Unlock above and this
		// deliverToActive call — landing the SYN in activeCh AFTER
		// the drain, where bus.Send.sendRawWithEcho on byte K+1
		// reads it as 0xAA → echo_mismatch (pre_echo_syn). With
		// re-acquisition, P12's drain-then-arm under stateMu is
		// genuinely atomic with this enqueue.
		m.stateMu.Lock()
		if m.gatewayTxnActive {
			select {
			case m.activeCh <- activeEvent{kind: activeEventByte, b: symbol}:
				m.activeTxn.bytesDeliveredToActive.Add(1)
			default:
				m.activeTxn.terminatorDropOnFullCh.Add(1)
				m.logger.Printf("adaptermux: active channel full, dropping SYN 0x%02X", symbol)
			}
		} else if rs.hasOwner && rs.ownerID == gatewaySessionID {
			m.activeTxn.afterInactive.Add(1)
		}
		m.stateMu.Unlock()
	}
	for _, pe := range rs.passiveEvents {
		m.emitPassive(pe)
	}
	if rs.deliverToSessions {
		m.deliverSYNToSessions(rs.now)
	}
	if syn.shouldTryGrant {
		m.grantAtSYN(rs.bids)
	}
}

// grantAtSYN runs the grant step for the bids admitted before the SYN, then
// delivers the held INFO answer of a bid whose RequestStart was accepted.
func (m *Mux) grantAtSYN(bids map[*startRequest]chan startResult) {
	m.tryGrantAndStartForBids(bids)
	m.releaseAcceptedInfoHold()
}

// foreignInitiatorRelease is how a foreign initiator byte ends the gateway's
// ownership.
type foreignInitiatorRelease uint8

const (
	// foreignInitiatorKept: the byte releases nothing.
	foreignInitiatorKept foreignInitiatorRelease = iota
	// foreignInitiatorFirstEcho: the byte arrived as the first echo of the
	// live gateway transaction, so its arbitration was lost.
	foreignInitiatorFirstEcho
	// foreignInitiatorAfterTxn: the byte arrived after the gateway's
	// transaction ended, inside its ownership tail.
	foreignInitiatorAfterTxn
)

// dataSymbolView is what the data-symbol decision reads under stateMu,
// after the entry step.
type dataSymbolView struct {
	// gatewayOwned: the gateway still owns the bus.
	gatewayOwned bool
	// txnActive: the gateway transaction is live.
	txnActive bool
	// echoHead and echoPending are the gateway's expected-echo queue head
	// before this symbol is matched.
	echoHead    byte
	echoPending bool
	// matchCount and writeCount are the echoes matched and the bytes
	// written in the current gateway transaction.
	matchCount uint64
	writeCount uint64
	// suspectArbLoss: the first-byte arbitration loss was already taken.
	suspectArbLoss bool
	// txnEnded: the gateway transaction recorded its inactive reason.
	txnEnded bool
}

// dataSymbolDecision is the data-symbol decision: whether the symbol is
// matched against the gateway's expected echo and whether a foreign
// initiator byte releases the gateway's ownership.
type dataSymbolDecision struct {
	matchEcho bool
	release   foreignInitiatorRelease
}

// decideDataSymbol takes the data-symbol decision. Matching the echo only
// consumes the queue head when the symbol is that head (or the queue is
// empty), which rules out a first-echo foreign initiator, so both decisions
// read the same pre-match view.
func decideDataSymbol(v dataSymbolView, ev muxEvent) dataSymbolDecision {
	symbol := ev.data
	d := dataSymbolDecision{release: foreignInitiatorKept}
	// P11 round-2 cascade fix: only call matchEcho when it will MATCH
	// (or when the queue is empty — then it's a no-op). matchEcho's
	// mismatch branch CLEARS the queue, which would destroy our filter
	// state for cascading stale bytes (a single mid-write stale byte
	// would clear the queue and the second stale byte would slip
	// through as response-phase). By gating the call on a match check
	// we preserve the queue across stale bytes.
	matchWouldHit := !v.echoPending || (v.echoPending && symbol == v.echoHead)
	d.matchEcho = v.gatewayOwned && matchWouldHit
	foreignInitiatorByte := protocol.AddressClassOf(symbol) == protocol.AddressClassMaster
	if v.gatewayOwned && v.txnActive && v.echoPending &&
		v.matchCount == 0 &&
		v.writeCount == 1 &&
		symbol != v.echoHead &&
		foreignInitiatorByte &&
		!v.suspectArbLoss {
		d.release = foreignInitiatorFirstEcho
	} else if v.gatewayOwned && !v.txnActive && v.txnEnded && foreignInitiatorByte {
		d.release = foreignInitiatorAfterTxn
	}
	return d
}

// receivedDataSymbol is what the data-symbol decision leaves for the effect
// application.
type receivedDataSymbol struct {
	// gatewayOwned: the symbol belongs to the gateway's transaction; false
	// once a foreign initiator byte released the gateway.
	gatewayOwned    bool
	foreignToActive bool
	activeExpects   bool
	cancelWatchdog  bool
}

// decideReceivedDataSymbolLocked is the data-symbol decision under stateMu:
// the echo match, the foreign initiator release, the active-path decision
// and the shape diagnostics. Caller holds stateMu.
func (m *Mux) decideReceivedDataSymbolLocked(rs *receivedSymbol) receivedDataSymbol {
	symbol := rs.ev.data
	// Non-SYN byte: suppress from passive when gateway owns the bus.
	//
	// When the gateway owns the bus, ALL received bytes belong to the
	// gateway's transaction: echoed request bytes AND the target's
	// response bytes (ACK, LEN, DATA, CRC, final ACK). Without full
	// suppression, orphaned response bytes leak to the passive path
	// and the reconstructor parses them as garbage frames (Source=0x00,
	// fake protocol IDs).
	//
	// The echo tracker still runs for internal state tracking, but its
	// result is not used for passive filtering — we suppress everything.
	view := dataSymbolView{
		gatewayOwned:   rs.hasOwner && rs.ownerID == gatewaySessionID,
		txnActive:      m.gatewayTxnActive,
		matchCount:     m.gatewayEcho.matchCount(),
		writeCount:     m.gatewayEcho.writeCount(),
		suspectArbLoss: m.activeTxn.firstByteSuspectArbLoss.Load(),
		txnEnded:       m.activeTxn.inactiveReas != ReasonNone,
	}
	// P11 round-2 (Codex P1 findings on PR #606) — capture the echo
	// queue head BEFORE matchEcho consumes a matching byte. The filter
	// uses the PRE-MATCH queue head as the protocol-accurate "what byte
	// is bus.Send currently waiting for" signal.
	//
	// Why not echoCursor / writePrefix:
	//   - writePrefix is capped at txnPrefixCap=8 (diagnostic). Frames
	//     longer than 8 bytes would have echoCursor==writePrefixLen
	//     forever after byte 8, opening response phase prematurely.
	//   - recordReadPrefixAndClassify advances echoCursor on a NON-match
	//     too (diagnostic side effect). A single mid-write stale byte
	//     would advance the cursor past the missing echo, and a second
	//     stale byte before the real echo would slip through.
	//
	// gatewayEcho.expectedEchoes is the authoritative protocol state:
	// every byte bus.Send writes is recorded; matchEcho only consumes
	// on actual match. Uncapped, mismatch-resistant.
	view.echoHead, view.echoPending = m.gatewayEcho.peekNextExpected()
	decision := decideDataSymbol(view, rs.ev)
	if decision.matchEcho {
		m.matchGatewayEchoLocked(rs)
	}
	out := receivedDataSymbol{gatewayOwned: view.gatewayOwned}
	m.releaseOnForeignInitiatorLocked(rs, decision.release, &out)

	out.activeExpects = m.gatewayTxnActive
	if out.activeExpects && view.echoPending {
		// Mid-write: only the next expected echo passes.
		out.activeExpects = symbol == view.echoHead
	}
	// hadPendingEcho == false: response phase open, activeExpects
	// stays as gatewayTxnActive (true) — any byte delivered.
	// Codex P2: regression signal — if a non-SYN byte arrives while
	// gateway still owns the bus but gatewayTxnActive is already false
	// (post-SYN window before ownership is released), we skipped
	// delivery. Count those events so the snapshot can surface any
	// post-inactive delivery pressure.
	if out.gatewayOwned && !out.activeExpects {
		m.activeTxn.afterInactive.Add(1)
	}

	// Shape diag: capture non-SYN read prefix + echo/non-echo class
	// while stateMu is held (prefix is a struct field, not atomic).
	// Restrict to the live gateway transaction window so pre-write or
	// post-inactive ownership periods do not pollute the per-txn prefix.
	if out.gatewayOwned && m.gatewayTxnActive {
		m.recordReadPrefixAndClassify(symbol)
	}
	return out
}

// matchGatewayEchoLocked matches the symbol against the gateway's expected
// echo. Caller holds stateMu.
func (m *Mux) matchGatewayEchoLocked(rs *receivedSymbol) {
	// Phase 3 Step B3.6d (Codex round-1 MAJOR on PR #646):
	// gate the time-tracking match path on m.v8 != nil. In
	// ModeOff (m.v8 == nil) we call the legacy matchEcho and
	// skip both time.Now() at the match site AND the writeAt
	// pop in matchEchoWithTime — restoring the documented
	// "zero overhead in ModeOff" contract. When v8 is on, the
	// echo timestamp from matchEchoWithTime feeds RTT into the
	// gateway pacer's L_rtt EMA.
	//
	// Phase 3 Step B3.6e: on a suppressed (matched) echo,
	// cancel the echo-watchdog timer that doSend armed —
	// otherwise a delayed soft/hard fire would spuriously
	// emit an admin event for a write whose echo already
	// landed. CancelEchoWatchdog is idempotent and lock-free
	// from the caller's perspective.
	if m.v8 != nil {
		echoNow := time.Now()
		result, _, writeAt, hasWriteAt := m.gatewayEcho.matchEchoWithTime(rs.ev.data, rs.ev.escaped)
		if result == echoMatchSuppressed {
			if pacer := m.SessionPacer(gatewaySessionID); pacer != nil {
				pacer.CancelEchoWatchdog()
				if hasWriteAt {
					pacer.RecordEcho(echoNow.Sub(writeAt), echoNow)
				}
			}
		}
	} else {
		m.gatewayEcho.matchEcho(rs.ev.data, rs.ev.escaped)
	}
}

// releaseOnForeignInitiatorLocked applies the foreign initiator release the
// data-symbol decision took. The released byte comes back to the wire-phase
// tracker as third-party traffic. Caller holds stateMu.
//
// P11 — gate activeCh delivery: mid-write requires queue-head match;
// response phase (no pending writes) accepts any byte.
//
// Pre-P11 the non-SYN path used the bulk filter
// (activePathExpectsBytes — just gatewayTxnActive), so any byte
// during ownership flowed through and post_grant_ack accounted for
// ~52% of all echo_mismatch. P11 round-1 used echoCursor /
// writePrefix as the gate but those are capped diagnostic state;
// P11 round-2 uses gatewayEcho's pre-match queue head, which is
// protocol-accurate and uncapped.
//
// F-NEW-29 (2026-05-23, plan fix-p11-midwrite-byte-routing-w21-26):
// First-byte arbitration revalidation. When the gateway has just been
// granted arbitration (matchCount==0 && writeCount==1) and the very
// first wire byte we observe is a foreign initiator-class address that
// doesn't match our expected echo, the arbitration grant was a
// false-positive: another initiator actually has the bus. Forward the
// byte to activeCh (do NOT drop) so bus.go's existing first-byte-
// after-arbitration classifier (helianthus-ebusgo bus.go:1328-1378)
// can raise ErrBusCollision and sendWithRetries can retry naturally.
//
// Issue #1401: the loss also releases the gateway's ownership at once
// (releaseGatewayOnArbitrationLossLocked) instead of holding the grant
// through a recovery envelope; holding it only blocked external
// bidders, up to MaxOwnershipDuration when no lifecycle callback ended
// the envelope. The stale-byte discard runs before the foreign byte is
// queued (phase 2, after the abandoned write's watchdog is cancelled),
// so protocol.Bus still reads it as the first-byte echo, and the resync
// SYNs the bus waits for stay owed to activeCh.
//
// Strict P11 mid-write drop remains active for ALL other mismatches:
// mid-frame stale bytes, non-initiator-class noise, post-first-echo wire
// intrusions. P11 round-2's design intent is preserved after ownership
// is validated by at least one echo.
func (m *Mux) releaseOnForeignInitiatorLocked(rs *receivedSymbol, release foreignInitiatorRelease, out *receivedDataSymbol) {
	symbol := rs.ev.data
	switch release {
	case foreignInitiatorFirstEcho:
		m.activeTxn.firstByteSuspectArbLoss.Store(true)
		// Classify while the transaction is still live: the terminal
		// class is computed from the read prefix at release.
		m.recordReadPrefixAndClassify(symbol)
		// The outstanding write can no longer receive its echo: cancel the
		// watchdog after stateMu unlock, before the byte reaches activeCh.
		out.cancelWatchdog = m.releaseGatewayOnArbitrationLossLocked()
		out.foreignToActive = true
		m.logger.Printf("adaptermux: ownership released for session %d (foreign initiator 0x%02X as first echo of txn id=%d: arbitration lost)",
			gatewaySessionID, symbol, m.activeTxn.id)
	case foreignInitiatorAfterTxn:
		// Issue #1401: a foreign initiator byte after the gateway's
		// transaction already ended (normally inside the IdleReleaseGrace
		// tail) is another initiator's telegram on the wire. The gateway lost
		// the bus; holding the grant until the SYN after that telegram
		// would only block external bidders. The transaction's own reason
		// is already recorded and its activeCh content (the terminator
		// echo protocol.Bus may still read) is left untouched. The grant
		// window before the first write (no reason yet) is excluded: stale
		// pre-grant bytes must not abort a fresh grant.
		m.releaseGatewayOwnershipLocked()
		m.deferRegrantUntilNextSyn = true
		m.logger.Printf("adaptermux: ownership released for session %d (foreign initiator 0x%02X after txn id=%d ended reason=%s)",
			gatewaySessionID, symbol, m.activeTxn.id, m.activeTxn.inactiveReas)
	default:
		return
	}
	m.activeTxn.foreignInitiatorReleases.Add(1)
	// From here on the byte is third-party traffic: the passive path
	// and the wire-phase tracker see the foreign telegram from its
	// source byte (the tracker was reset to idle at the last
	// gateway-owned SYN).
	out.gatewayOwned = false
	rs.phaseEvent = m.phase.advanceWithProvenance(symbol, false)
}

// applyReceivedDataSymbol applies the effects of the data-symbol decision
// after stateMu was released: the watchdog cancel, the active-path
// deliveries, the passive events and the session delivery.
func (m *Mux) applyReceivedDataSymbol(rs *receivedSymbol, data receivedDataSymbol) {
	symbol, wasEscaped := rs.ev.data, rs.ev.escaped
	// F-18: per-session echo trackers removed; nothing to reset on
	// ownership timeout.
	_ = rs.ownershipTimedOut
	// Codex round-2 MUST FIX on PR #647: cancel the gateway
	// watchdog when ownership was forcibly released. Mirror of
	// the same cancel at the SYN-branch unlock above.
	if rs.ownershipTimedOutGateway || data.cancelWatchdog {
		if pacer := m.SessionPacer(gatewaySessionID); pacer != nil {
			pacer.CancelEchoWatchdog()
		}
	}

	// --- Phase 2: deliver outside all locks ---
	// Codex PR #502 P2: revalidate active-path gating atomically with
	// the enqueue. `activeExpects` was decided under stateMu above, but
	// between that unlock and the send below, gatewayTxnActive may have
	// flipped to false (e.g. active read-timeout / write-error /
	// context-cancel paths). Without a re-check, stale bytes can leak
	// onto activeCh and afterInactive misses them (it was computed from
	// the earlier snapshot). Re-acquire stateMu for a short critical
	// section: check + non-blocking send. activeCh is capacity-4096 and
	// deliverToActive already uses a non-blocking select, so holding
	// stateMu across the send cannot deadlock.
	if data.foreignToActive {
		// Issue #1401: the arbitration-loss release above already ended the
		// transaction, so this bypasses the gatewayTxnActive gate on
		// purpose. Only this goroutine produces activeCh bytes and the bus
		// is blocked on the first echo, so nothing interleaves with the
		// stale discard done under the earlier critical section.
		m.stateMu.Lock()
		select {
		case m.activeCh <- activeEvent{kind: activeEventByte, b: symbol, wasEscaped: wasEscaped}:
			m.activeTxn.bytesDeliveredToActive.Add(1)
		default:
			m.logger.Printf("adaptermux: active channel full, dropping foreign initiator byte 0x%02X", symbol)
		}
		m.stateMu.Unlock()
	}
	if data.activeExpects {
		m.stateMu.Lock()
		if m.gatewayTxnActive {
			select {
			// F-23 (Codex bot on PR-2): thread the upstream
			// WasEscaped flag into activeCh so the gateway's
			// protocol.Bus consumer (via
			// activeTransport.ReadByteWithEscape) can distinguish
			// real wire SYN from escape-decoded payload 0xAA in
			// waitForSyn / sendRawWithEcho.
			case m.activeCh <- activeEvent{kind: activeEventByte, b: symbol, wasEscaped: wasEscaped}:
				// Codex PR #502 P1: count real adapter-originated byte
				// enqueued on activeCh during gateway-owned active txn.
				// Decision was re-validated under stateMu on the line
				// above, so gatewayTxnActive is confirmed true here.
				m.activeTxn.bytesDeliveredToActive.Add(1)
			default:
				m.logger.Printf("adaptermux: active channel full, dropping byte 0x%02X", symbol)
			}
		} else if data.gatewayOwned {
			// Gating flipped between snapshot and delivery — the byte
			// arrived while gateway owned the bus but the active path
			// no longer expects bytes. Record the post-inactive event
			// for diagnostics instead of enqueuing a stale byte.
			m.activeTxn.afterInactive.Add(1)
		}
		m.stateMu.Unlock()
	}

	if !data.gatewayOwned {
		rs.passiveEvents = append(rs.passiveEvents, PassiveEvent{
			Kind: PassiveEventSymbol, Symbol: symbol, WasEscaped: wasEscaped, ObservedAt: rs.now,
		})
	}
	for _, pe := range rs.passiveEvents {
		m.emitPassive(pe)
	}

	if rs.deliverToSessions {
		m.deliverToSessions(symbol, rs.ownerID, rs.hasOwner, rs.now)
	}
}

// releaseOnTerminalPhaseEvent is the terminal release of a data symbol whose
// wire-phase event ends the owner's transaction.
func (m *Mux) releaseOnTerminalPhaseEvent(rs *receivedSymbol) {
	if rs.phaseEvent != wirePhaseEventTransactionDone &&
		rs.phaseEvent != wirePhaseEventCmdNACK {
		return
	}
	// F-21 (batch-20): under F-21 this non-SYN release block is
	// reachable ONLY via the protocol-error garbled-byte fallback
	// in advanceWaitCmdAck (which still resets to Idle and returns
	// wirePhaseEventCmdNACK at the non-SYN byte position — see the
	// "Neither ACK nor NACK" branch comment in wire_phase.go for
	// the rationale). All five normal structural-terminal sites
	// (broadcast ACK, i2i ACK, CMD double-NACK, response double-
	// NACK, response success ACK) now transition to
	// wirePhaseWaitTerminalSyn and defer TransactionDone to the
	// trailing-SYN observation; that release happens in
	// onSYNLocked's F-21 branch. Keeping this block intact closes
	// the rare protocol-error case where the bus is too degraded
	// to emit a clean trailing SYN.
	//
	// Codex-R9: atomic release + gatewayTxnActive clear.
	// Must re-verify ownership under stateMu — between the phase
	// event and this point another goroutine may have granted a
	// new session. Only release + clear if ownerID still matches.
	reason := ReasonTransactionDone
	if rs.phaseEvent == wirePhaseEventCmdNACK {
		reason = ReasonCmdNACK
	}
	ownerID := rs.ownerID
	m.stateMu.Lock()
	curOwner, _, hasCurOwner := m.arb.owner()
	if hasCurOwner && curOwner == ownerID {
		m.arb.releaseOwnership(ownerID)
		if ownerID == gatewaySessionID && m.gatewayTxnActive {
			m.gatewayTxnActive = false
			m.recordGatewayInactive(reason)
		}
	}
	m.stateMu.Unlock()
}
