package adaptermux

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// The state model specifies the AdapterMux transition behaviour over explicit
// states and events, in test code only:
//
//   - muxModelState is a typed state derived from the fields the transition
//     handlers read and write: the arbitration phase, the recovery kind of the
//     arbitration fence and three barriers (stale-response absorb pending,
//     blocking START call active, resynchronization SYNs owed to the gateway
//     active path), plus the provenance that changes how an event is handled
//     (a deferred gateway loser, a gateway echo still awaited, an owner other
//     than the session the close event removes);
//   - muxModelEvent is the set of inputs the handlers react to;
//   - muxModelRows is the transition table: for every named model state and
//     every event either the next state and the ordered effect kinds, or an
//     explicit row stating that the event cannot occur in that state.
//
// The table pins today's behaviour, defects included. The conformance test
// drives every valid row through the current handlers with the transition
// harness (transition_harness_test.go) and compares the derived state and the
// ordered effect kinds with the row.

// muxModelPhase is the arbitration phase. When several phase conditions hold
// at once the first in this order wins: owner, START in flight, fence, hold,
// queued bids, idle. A hold under a higher phase is not representable and is
// reported by muxModelDerive.
type muxModelPhase uint8

const (
	// muxModelIdle: no owner, no START in flight, no fence, no hold, no bid.
	muxModelIdle muxModelPhase = iota
	// muxModelBidsQueued: at least one bid waits for an idle SYN.
	muxModelBidsQueued
	// muxModelStartAsync: a selected START waits for its adapter result
	// through the stream (RequestStart).
	muxModelStartAsync
	// muxModelStartBlocking: a selected START uses the blocking
	// StartArbitration call.
	muxModelStartBlocking
	// muxModelGatewayTransaction: the gateway owns the bus with an active
	// transaction.
	muxModelGatewayTransaction
	// muxModelGatewayGrace: the gateway owns the bus, its transaction ended
	// and the idle release grace still runs.
	muxModelGatewayGrace
	// muxModelSessionOwned: an external session owns the bus.
	muxModelSessionOwned
	// muxModelFenced: no selection before the next SYN; the state carries
	// the recovery kind.
	muxModelFenced
	// muxModelHeld: active arbitration is held for an out-of-step adapter;
	// selection is skipped until a lifecycle boundary.
	muxModelHeld
)

var muxModelPhaseNames = [...]string{
	muxModelIdle:               "muxModelIdle",
	muxModelBidsQueued:         "muxModelBidsQueued",
	muxModelStartAsync:         "muxModelStartAsync",
	muxModelStartBlocking:      "muxModelStartBlocking",
	muxModelGatewayTransaction: "muxModelGatewayTransaction",
	muxModelGatewayGrace:       "muxModelGatewayGrace",
	muxModelSessionOwned:       "muxModelSessionOwned",
	muxModelFenced:             "muxModelFenced",
	muxModelHeld:               "muxModelHeld",
}

func (p muxModelPhase) String() string {
	if int(p) < len(muxModelPhaseNames) {
		return muxModelPhaseNames[p]
	}
	return fmt.Sprintf("muxModelPhase(%d)", p)
}

func (p muxModelPhase) owned() bool {
	return p == muxModelGatewayTransaction || p == muxModelGatewayGrace || p == muxModelSessionOwned
}

var muxModelRecoveryNames = map[arbitrationRecovery]string{
	arbitrationRecoveryNone:           "arbitrationRecoveryNone",
	arbitrationRecoveryAtSYN:          "arbitrationRecoveryAtSYN",
	arbitrationRecoveryReset:          "arbitrationRecoveryReset",
	arbitrationRecoveryForeign:        "arbitrationRecoveryForeign",
	arbitrationRecoveryForeignAfterAA: "arbitrationRecoveryForeignAfterAA",
}

// muxModelState is the typed mux state of the model.
type muxModelState struct {
	phase muxModelPhase
	// recovery is the arbitration recovery kind: the kind of the fence in
	// muxModelFenced, and the pending lifecycle recovery after a reset.
	recovery arbitrationRecovery
	// absorb: a stale arbitration result is still to be absorbed.
	absorb bool
	// blocking: a blocking StartArbitration call is outstanding.
	blocking bool
	// resyncOwed: wire SYNs still owed to the gateway active path.
	resyncOwed uint8
	// gateway: the selected party of a START in flight is the gateway rather
	// than an external session.
	gateway bool
	// deferred: the loss of the gateway to the selected START is deferred
	// until the blocking call returns.
	deferred bool
	// echoPending: the gateway wrote a byte of its transaction whose echo has
	// not arrived; only meaningful in muxModelGatewayTransaction.
	echoPending bool
	// otherOwner: the external session that owns the bus is not session 1;
	// only meaningful in muxModelSessionOwned.
	otherOwner bool
}

// literal prints the state as the Go literal used in muxModelRows.
func (s muxModelState) literal() string {
	parts := []string{"phase: " + s.phase.String()}
	if s.recovery != arbitrationRecoveryNone {
		name, ok := muxModelRecoveryNames[s.recovery]
		if !ok {
			name = fmt.Sprintf("arbitrationRecovery(%d)", s.recovery)
		}
		parts = append(parts, "recovery: "+name)
	}
	if s.absorb {
		parts = append(parts, "absorb: true")
	}
	if s.blocking {
		parts = append(parts, "blocking: true")
	}
	if s.resyncOwed != 0 {
		parts = append(parts, fmt.Sprintf("resyncOwed: %d", s.resyncOwed))
	}
	if s.gateway {
		parts = append(parts, "gateway: true")
	}
	if s.deferred {
		parts = append(parts, "deferred: true")
	}
	if s.echoPending {
		parts = append(parts, "echoPending: true")
	}
	if s.otherOwner {
		parts = append(parts, "otherOwner: true")
	}
	return "muxModelState{" + strings.Join(parts, ", ") + "}"
}

// muxModelDerive derives the model state from the harness state, the
// out-of-step hold and whether the gateway echo queue still waits for an echo.
// It fails when the hold coexists with a phase that hides it, since the model
// has no state for that combination.
func muxModelDerive(s transitionState, held, echoPending bool) (muxModelState, error) {
	out := muxModelState{
		recovery:   arbitrationRecoveryNone,
		absorb:     s.absorb > 0,
		blocking:   s.blockingCall,
		resyncOwed: s.resyncOwed,
	}
	if r, ok := muxModelRecoveryFromText(s.recovery); ok {
		out.recovery = r
	} else {
		return out, fmt.Errorf("unknown recovery %q", s.recovery)
	}
	switch {
	case s.owner == "gateway" && s.gatewayTxn:
		out.phase = muxModelGatewayTransaction
		out.echoPending = echoPending
	case s.owner == "gateway":
		out.phase = muxModelGatewayGrace
	case s.owner != "":
		out.phase = muxModelSessionOwned
		out.otherOwner = s.owner != "session 1"
	case s.start != "" && s.startBlocking:
		out.phase = muxModelStartBlocking
	case s.start != "":
		out.phase = muxModelStartAsync
	case s.regrantFence:
		out.phase = muxModelFenced
	case held:
		out.phase = muxModelHeld
	case muxModelHasQueuedBid(s.bids):
		out.phase = muxModelBidsQueued
	default:
		out.phase = muxModelIdle
	}
	if (out.phase == muxModelStartAsync || out.phase == muxModelStartBlocking) && strings.HasPrefix(s.start, "gateway ") {
		out.gateway = true
	}
	out.deferred = strings.Contains(s.bids, "deferred ")
	if held && out.phase != muxModelHeld {
		return out, fmt.Errorf("out-of-step hold under phase %s is not representable", out.phase)
	}
	return out, nil
}

func muxModelRecoveryFromText(text string) (arbitrationRecovery, bool) {
	for _, r := range []arbitrationRecovery{
		arbitrationRecoveryNone,
		arbitrationRecoveryAtSYN,
		arbitrationRecoveryReset,
		arbitrationRecoveryForeign,
		arbitrationRecoveryForeignAfterAA,
	} {
		if transitionRecoveryText(r) == text {
			return r, true
		}
	}
	return arbitrationRecoveryNone, false
}

// muxModelHasQueuedBid reports whether the harness bid list names a bid that
// waits for selection; a deferred gateway loser waits for a START outcome and
// is no bid.
func muxModelHasQueuedBid(bids string) bool {
	if bids == "" {
		return false
	}
	for _, entry := range strings.Split(bids, ", ") {
		if !strings.HasPrefix(entry, "deferred ") {
			return true
		}
	}
	return false
}

// muxModelEvent is one input of the transition handlers.
type muxModelEvent uint8

const (
	// muxModelEventSYN: a wire SYN.
	muxModelEventSYN muxModelEvent = iota
	// muxModelEventSymbol: a received data symbol.
	muxModelEventSymbol
	// muxModelEventBid: session 2 bids 0x71.
	muxModelEventBid
	// muxModelEventStartedMatching: STARTED naming the selected source
	// (0x31 when none is selected).
	muxModelEventStartedMatching
	// muxModelEventStartedOther: STARTED naming a foreign source.
	muxModelEventStartedOther
	// muxModelEventFailed: FAILED naming a foreign winner.
	muxModelEventFailed
	// muxModelEventStartDeadline: the START deadline of the selection fires.
	muxModelEventStartDeadline
	// muxModelEventAbsorbDebounce: the absorb debounce deadline elapses.
	muxModelEventAbsorbDebounce
	// muxModelEventIdleGrace: a wire SYN after the idle release grace (the
	// gateway grace since the grant, the external grace since the last wire
	// activity) elapsed.
	muxModelEventIdleGrace
	// muxModelEventOwnershipLimit: a data symbol after the ownership limit
	// elapsed.
	muxModelEventOwnershipLimit
	// muxModelEventAbandonWatchdog: the abandon watchdog of the owning
	// session's grant fires with no SEND seen.
	muxModelEventAbandonWatchdog
	// muxModelEventSessionClose: session 1 disconnects.
	muxModelEventSessionClose
	// muxModelEventReset: the adapter reports RESETTED.
	muxModelEventReset
	// muxModelEventStartCallReturns: the outstanding blocking
	// StartArbitration call returns successfully.
	muxModelEventStartCallReturns
	// muxModelEventStartCallFails: the outstanding blocking StartArbitration
	// call returns an error.
	muxModelEventStartCallFails
	muxModelEventCount
)

var muxModelEventNames = [...]string{
	muxModelEventSYN:              "muxModelEventSYN",
	muxModelEventSymbol:           "muxModelEventSymbol",
	muxModelEventBid:              "muxModelEventBid",
	muxModelEventStartedMatching:  "muxModelEventStartedMatching",
	muxModelEventStartedOther:     "muxModelEventStartedOther",
	muxModelEventFailed:           "muxModelEventFailed",
	muxModelEventStartDeadline:    "muxModelEventStartDeadline",
	muxModelEventAbsorbDebounce:   "muxModelEventAbsorbDebounce",
	muxModelEventIdleGrace:        "muxModelEventIdleGrace",
	muxModelEventOwnershipLimit:   "muxModelEventOwnershipLimit",
	muxModelEventAbandonWatchdog:  "muxModelEventAbandonWatchdog",
	muxModelEventSessionClose:     "muxModelEventSessionClose",
	muxModelEventReset:            "muxModelEventReset",
	muxModelEventStartCallReturns: "muxModelEventStartCallReturns",
	muxModelEventStartCallFails:   "muxModelEventStartCallFails",
}

func (e muxModelEvent) String() string {
	if int(e) < len(muxModelEventNames) {
		return muxModelEventNames[e]
	}
	return fmt.Sprintf("muxModelEvent(%d)", e)
}

// Reasons of the pairs that cannot occur.
const (
	muxModelNoStartDeadline = "no START is in flight, so no START deadline is armed"
	muxModelNoAbsorb        = "no stale result is to be absorbed, so the debounce clears nothing"
	muxModelNoOwnerTimer    = "nobody owns the bus, so no ownership timer runs"
	muxModelNoSessionGrant  = "no external session owns the bus, so no abandon watchdog runs"
	muxModelNoBlockingCall  = "no blocking START call is outstanding, so none can return"
)

// muxModelEventCannotOccur is the validity rule of the table: it returns the
// reason why the event cannot occur in the state, or "" when it can.
func muxModelEventCannotOccur(s muxModelState, e muxModelEvent) string {
	switch e {
	case muxModelEventStartDeadline:
		if s.phase != muxModelStartAsync && s.phase != muxModelStartBlocking {
			return muxModelNoStartDeadline
		}
	case muxModelEventAbsorbDebounce:
		if !s.absorb {
			return muxModelNoAbsorb
		}
	case muxModelEventIdleGrace, muxModelEventOwnershipLimit:
		if !s.phase.owned() {
			return muxModelNoOwnerTimer
		}
	case muxModelEventAbandonWatchdog:
		if s.phase != muxModelSessionOwned {
			return muxModelNoSessionGrant
		}
	case muxModelEventStartCallReturns, muxModelEventStartCallFails:
		if !s.blocking {
			return muxModelNoBlockingCall
		}
	}
	return ""
}

// muxModelEffect is the kind of one handler effect.
type muxModelEffect uint8

const (
	muxModelEffectPassiveSymbol muxModelEffect = iota
	muxModelEffectPassiveReset
	muxModelEffectPassiveLifecycle
	muxModelEffectAdapterStart
	muxModelEffectAdapterStartBlocking
	muxModelEffectAdapterWrite
	muxModelEffectWatchdogArmed
	muxModelEffectActiveByte
	muxModelEffectActiveError
	muxModelEffectSessionReceived
	muxModelEffectSessionStarted
	muxModelEffectSessionFailed
	muxModelEffectSessionReset
	muxModelEffectSessionError
	muxModelEffectSessionInfo
	muxModelEffectResultGranted
	muxModelEffectResultLost
	muxModelEffectResultCancelled
	muxModelEffectResultError
)

var muxModelEffectNames = [...]string{
	muxModelEffectPassiveSymbol:        "muxModelEffectPassiveSymbol",
	muxModelEffectPassiveReset:         "muxModelEffectPassiveReset",
	muxModelEffectPassiveLifecycle:     "muxModelEffectPassiveLifecycle",
	muxModelEffectAdapterStart:         "muxModelEffectAdapterStart",
	muxModelEffectAdapterStartBlocking: "muxModelEffectAdapterStartBlocking",
	muxModelEffectAdapterWrite:         "muxModelEffectAdapterWrite",
	muxModelEffectWatchdogArmed:        "muxModelEffectWatchdogArmed",
	muxModelEffectActiveByte:           "muxModelEffectActiveByte",
	muxModelEffectActiveError:          "muxModelEffectActiveError",
	muxModelEffectSessionReceived:      "muxModelEffectSessionReceived",
	muxModelEffectSessionStarted:       "muxModelEffectSessionStarted",
	muxModelEffectSessionFailed:        "muxModelEffectSessionFailed",
	muxModelEffectSessionReset:         "muxModelEffectSessionReset",
	muxModelEffectSessionError:         "muxModelEffectSessionError",
	muxModelEffectSessionInfo:          "muxModelEffectSessionInfo",
	muxModelEffectResultGranted:        "muxModelEffectResultGranted",
	muxModelEffectResultLost:           "muxModelEffectResultLost",
	muxModelEffectResultCancelled:      "muxModelEffectResultCancelled",
	muxModelEffectResultError:          "muxModelEffectResultError",
}

func (e muxModelEffect) String() string {
	if int(e) < len(muxModelEffectNames) {
		return muxModelEffectNames[e]
	}
	return fmt.Sprintf("muxModelEffect(%d)", e)
}

// muxModelEffectKind classifies one entry of the harness effect log.
func muxModelEffectKind(entry string) (muxModelEffect, error) {
	switch {
	case strings.HasPrefix(entry, "passive: symbol "):
		return muxModelEffectPassiveSymbol, nil
	case entry == "passive: reset":
		return muxModelEffectPassiveReset, nil
	case strings.HasPrefix(entry, "passive: "):
		return muxModelEffectPassiveLifecycle, nil
	case strings.HasPrefix(entry, "adapter: RequestStart "):
		return muxModelEffectAdapterStart, nil
	case strings.HasPrefix(entry, "adapter: StartArbitration "):
		return muxModelEffectAdapterStartBlocking, nil
	case strings.HasPrefix(entry, "adapter: Write "):
		return muxModelEffectAdapterWrite, nil
	case strings.HasPrefix(entry, "abandon watchdog armed"):
		return muxModelEffectWatchdogArmed, nil
	case strings.HasPrefix(entry, "active: error "):
		return muxModelEffectActiveError, nil
	case strings.HasPrefix(entry, "active: "):
		return muxModelEffectActiveByte, nil
	case strings.HasPrefix(entry, "session "):
		_, frame, ok := strings.Cut(entry, ": ")
		if !ok {
			break
		}
		switch {
		case strings.HasPrefix(frame, "RECEIVED "):
			return muxModelEffectSessionReceived, nil
		case strings.HasPrefix(frame, "STARTED "):
			return muxModelEffectSessionStarted, nil
		case strings.HasPrefix(frame, "FAILED "):
			return muxModelEffectSessionFailed, nil
		case strings.HasPrefix(frame, "RESETTED "):
			return muxModelEffectSessionReset, nil
		case strings.HasPrefix(frame, "ERROR_"):
			return muxModelEffectSessionError, nil
		case strings.HasPrefix(frame, "INFO "):
			return muxModelEffectSessionInfo, nil
		}
	case strings.HasPrefix(entry, "result "):
		_, result, ok := strings.Cut(entry, ": ")
		if !ok {
			break
		}
		switch {
		case strings.HasPrefix(result, "granted"):
			return muxModelEffectResultGranted, nil
		case strings.HasPrefix(result, "cancelled"):
			return muxModelEffectResultCancelled, nil
		case strings.Contains(result, ", error "):
			return muxModelEffectResultError, nil
		default:
			return muxModelEffectResultLost, nil
		}
	}
	return 0, fmt.Errorf("unclassified effect %q", entry)
}

// muxModelEffectKinds classifies an effect log in order; consecutive effects
// of the same kind count once.
func muxModelEffectKinds(entries []string) ([]muxModelEffect, error) {
	out := []muxModelEffect{}
	for _, entry := range entries {
		kind, err := muxModelEffectKind(entry)
		if err != nil {
			return nil, err
		}
		if len(out) > 0 && out[len(out)-1] == kind {
			continue
		}
		out = append(out, kind)
	}
	return out, nil
}

func muxModelEffectsLiteral(effects []muxModelEffect) string {
	names := make([]string, 0, len(effects))
	for _, e := range effects {
		names = append(names, e.String())
	}
	return strings.Join(names, ", ")
}

// muxModelNamedState is one state of the model and how the harness reaches it
// through the real handlers.
type muxModelNamedState struct {
	mode  transitionUpstreamMode
	build func(h *transitionHarness)
	state muxModelState
}

func muxModelFixture(name string) func(h *transitionHarness) {
	return func(h *transitionHarness) {
		fixture, ok := transitionFixtures[name]
		if !ok {
			h.t.Fatalf("unknown transition fixture %s", name)
		}
		fixture.build(h)
	}
}

// muxModelBuildHeld holds active arbitration through the out-of-step detector
// with the episode's link reset already spent, then queues the bid of session
// 1 so a SYN shows the skipped selection.
func muxModelBuildHeld(h *transitionHarness) {
	h.t.Helper()
	m := h.m
	m.stateMu.Lock()
	m.stepMemory.spendReset()
	action := adapterStepNone
	for i := 0; i < outOfStepBadCycles; i++ {
		m.stepStartCycleLocked()
		action = m.stepBadCycleLocked()
	}
	m.stateMu.Unlock()
	if action != adapterStepHoldSet {
		h.t.Fatalf("fixture: out-of-step detector action %d, want the hold", action)
	}
	h.bid(1, transitionSessionSource)
}

var muxModelStates = map[string]muxModelNamedState{
	"idle": {mode: transitionUpstreamAsync, build: muxModelFixture("idle"),
		state: muxModelState{phase: muxModelIdle}},
	"bids_queued": {mode: transitionUpstreamAsync, build: muxModelFixture("bids_queued"),
		state: muxModelState{phase: muxModelBidsQueued}},
	"start_async": {mode: transitionUpstreamAsync, build: muxModelFixture("start_async"),
		state: muxModelState{phase: muxModelStartAsync}},
	// start_async_resync_owed: the gateway lost locally to the selected
	// session, so two resynchronization SYNs are owed to its active path.
	"start_async_resync_owed": {mode: transitionUpstreamAsync, build: muxModelFixture("start_async_local_losers"),
		state: muxModelState{phase: muxModelStartAsync, resyncOwed: 2}},
	// start_gateway: the gateway won the selection; the START waits for its
	// adapter result through the stream.
	"start_gateway": {mode: transitionUpstreamAsync, build: muxModelFixture("start_async_gateway"),
		state: muxModelState{phase: muxModelStartAsync, gateway: true}},
	"start_blocking": {mode: transitionUpstreamBlocking, build: muxModelFixture("start_blocking"),
		state: muxModelState{phase: muxModelStartBlocking, blocking: true}},
	// gateway_awaiting_echo: the gateway wrote its first request byte and the
	// echo has not arrived.
	"gateway_awaiting_echo": {mode: transitionUpstreamAsync, build: muxModelFixture("gateway_awaiting_echo"),
		state: muxModelState{phase: muxModelGatewayTransaction, echoPending: true}},
	"gateway_transaction": {mode: transitionUpstreamAsync, build: muxModelFixture("gateway_transaction"),
		state: muxModelState{phase: muxModelGatewayTransaction}},
	// start_blocking_deferred_loser: session 1 was selected over session 2 and
	// the gateway; the gateway's loss stays deferred until the blocking call
	// returns.
	"start_blocking_deferred_loser": {mode: transitionUpstreamBlocking, build: muxModelFixture("start_blocking_local_losers"),
		state: muxModelState{phase: muxModelStartBlocking, blocking: true, deferred: true}},
	"gateway_grace": {mode: transitionUpstreamAsync, build: muxModelFixture("gateway_grace"),
		state: muxModelState{phase: muxModelGatewayGrace}},
	"session_owned": {mode: transitionUpstreamAsync, build: muxModelFixture("session_owned"),
		state: muxModelState{phase: muxModelSessionOwned}},
	// session_owned_other: STARTED granted the bus to session 2, so session 1
	// is not the owner.
	"session_owned_other": {mode: transitionUpstreamAsync, build: func(h *transitionHarness) {
		h.bid(2, transitionOtherSource)
		h.selected = "session 2 0x71"
		h.source = transitionOtherSource
		h.syn()
		h.captureDeadline()
		h.feedStarted(transitionOtherSource)
	}, state: muxModelState{phase: muxModelSessionOwned, otherOwner: true}},
	"fenced": {mode: transitionUpstreamAsync, build: muxModelFixture("fenced"),
		state: muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN}},
	// fenced_absorb_pending: the selected session cancelled its START; the
	// fence and the stale-result barrier both hold.
	"fenced_absorb_pending": {mode: transitionUpstreamAsync, build: muxModelFixture("absorb_pending"),
		state: muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN, absorb: true}},
	// absorb_pending: the SYN after the cancel cleared the fence; the
	// stale-result barrier still holds.
	"absorb_pending": {mode: transitionUpstreamAsync, build: func(h *transitionHarness) {
		muxModelFixture("absorb_pending")(h)
		h.syn()
	}, state: muxModelState{phase: muxModelIdle, absorb: true}},
	"held": {mode: transitionUpstreamAsync, build: muxModelBuildHeld,
		state: muxModelState{phase: muxModelHeld}},
	// reset_recovery: the adapter reported RESETTED; the lifecycle recovery
	// waits for the next stream item.
	"reset_recovery": {mode: transitionUpstreamAsync, build: func(h *transitionHarness) {
		h.feedReset()
	}, state: muxModelState{phase: muxModelIdle, recovery: arbitrationRecoveryReset}},
}

// muxModelObserve reads the model state of the harness mux.
func muxModelObserve(h *transitionHarness) (muxModelState, error) {
	s := h.state()
	h.m.stateMu.Lock()
	held := h.m.step.held
	echoPending := h.m.gatewayEcho.hasPendingEchoes()
	h.m.stateMu.Unlock()
	return muxModelDerive(s, held, echoPending)
}

// muxModelFeedSymbol hands one received data symbol to the read loop.
func muxModelFeedSymbol(h *transitionHarness) {
	h.t.Helper()
	h.feed(transitionItem{event: transport.StreamEvent{Kind: transport.StreamEventByte, Byte: transitionDataSymbol}})
}

// muxModelDrivers apply one event to the harness mux through the handlers.
var muxModelDrivers = map[muxModelEvent]func(h *transitionHarness){
	muxModelEventSYN:             func(h *transitionHarness) { h.feedWireSyn() },
	muxModelEventSymbol:          func(h *transitionHarness) { muxModelFeedSymbol(h) },
	muxModelEventBid:             func(h *transitionHarness) { h.bid(2, transitionOtherSource) },
	muxModelEventStartedMatching: func(h *transitionHarness) { h.feedStarted(h.source) },
	muxModelEventStartedOther:    func(h *transitionHarness) { h.feedStarted(transitionForeignSource) },
	muxModelEventFailed:          func(h *transitionHarness) { h.feedFailed(transitionForeignSource) },
	muxModelEventStartDeadline:   func(h *transitionHarness) { h.fireDeadline(h.selected) },
	// The debounce timer runs for the harness horizon; clearing its due-at
	// and running its callback is the deadline elapsing now.
	muxModelEventAbsorbDebounce: func(h *transitionHarness) {
		h.m.stateMu.Lock()
		h.m.pendingAbsorbResetDueAt = time.Time{}
		h.m.stateMu.Unlock()
		h.m.fireAbsorbReset()
	},
	// The gateway grace runs from the grant, as the ownership limit does, so
	// the grace is shortened instead of moving the grant back; the external
	// grace runs from the last wire activity, which is moved back.
	muxModelEventIdleGrace: func(h *transitionHarness) {
		h.m.stateMu.Lock()
		h.m.cfg.IdleReleaseGrace = 0
		h.m.lastWireActivity = time.Now().Add(-2 * transitionTimerHorizon)
		h.m.stateMu.Unlock()
		h.feedWireSyn()
	},
	muxModelEventOwnershipLimit: func(h *transitionHarness) {
		h.m.stateMu.Lock()
		h.m.busOwned = h.m.busOwned.Add(-2 * transitionTimerHorizon)
		h.m.stateMu.Unlock()
		muxModelFeedSymbol(h)
	},
	// The session writer arms the watchdog once STARTED reached the client;
	// the harness runs no writer, so the driver arms it for the current grant
	// and runs the callback at once.
	muxModelEventAbandonWatchdog: func(h *transitionHarness) {
		h.t.Helper()
		h.m.stateMu.Lock()
		ownerID, _, _ := h.m.arb.owner()
		h.m.stateMu.Unlock()
		if ownerID == 0 || int(ownerID) > len(h.sessions) {
			h.t.Fatalf("abandon watchdog: owner %d is no harness session", ownerID)
		}
		sess := h.sessions[ownerID-1]
		var fire func()
		h.m.externalAbandonAfter = func(_ time.Duration, f func()) { fire = f }
		h.m.armExternalAbandonWatchdog(sess, uint32(sess.grantGeneration.Load()))
		if fire == nil {
			h.t.Fatalf("abandon watchdog was not armed")
		}
		fire()
	},
	muxModelEventSessionClose:     func(h *transitionHarness) { h.m.RemoveSession(1) },
	muxModelEventReset:            func(h *transitionHarness) { h.feedReset() },
	muxModelEventStartCallReturns: func(h *transitionHarness) { h.releaseStartCall(nil, h.selected) },
	muxModelEventStartCallFails: func(h *transitionHarness) {
		h.releaseStartCall(errors.New("scripted START failure"), h.selected)
	},
}

// muxModelOutcome is the defined outcome of a table row.
type muxModelOutcome uint8

const (
	// muxModelTransition: the event moves the state to next with effects.
	muxModelTransition muxModelOutcome = iota
	// muxModelCannotOccur: the event has no source in the state; the state
	// stays and no effect is defined.
	muxModelCannotOccur
)

type muxModelRow struct {
	state   string
	event   muxModelEvent
	outcome muxModelOutcome
	next    muxModelState
	effects []muxModelEffect
	reason  string
}

func muxModelOn(state string, event muxModelEvent, next muxModelState, effects ...muxModelEffect) muxModelRow {
	if effects == nil {
		effects = []muxModelEffect{}
	}
	return muxModelRow{state: state, event: event, outcome: muxModelTransition, next: next, effects: effects}
}

func muxModelNever(state string, event muxModelEvent, reason string) muxModelRow {
	return muxModelRow{state: state, event: event, outcome: muxModelCannotOccur, reason: reason}
}

func muxModelRowLiteral(state string, event muxModelEvent, next muxModelState, effects []muxModelEffect) string {
	args := []string{fmt.Sprintf("%q", state), event.String(), next.literal()}
	if len(effects) > 0 {
		args = append(args, muxModelEffectsLiteral(effects))
	}
	return "muxModelOn(" + strings.Join(args, ", ") + "),"
}

func muxModelStateNames() []string {
	names := make([]string, 0, len(muxModelStates))
	for name := range muxModelStates {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// muxModelRows is the transition table: one row per named state and event.
var muxModelRows = []muxModelRow{
	muxModelOn("absorb_pending", muxModelEventSYN, muxModelState{phase: muxModelIdle, absorb: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("absorb_pending", muxModelEventSymbol, muxModelState{phase: muxModelIdle, absorb: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("absorb_pending", muxModelEventBid, muxModelState{phase: muxModelBidsQueued, absorb: true}),
	muxModelOn("absorb_pending", muxModelEventStartedMatching, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN}),
	muxModelOn("absorb_pending", muxModelEventStartedOther, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("absorb_pending", muxModelEventFailed, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("absorb_pending", muxModelEventStartDeadline, muxModelNoStartDeadline),
	muxModelOn("absorb_pending", muxModelEventAbsorbDebounce, muxModelState{phase: muxModelIdle}),
	muxModelNever("absorb_pending", muxModelEventIdleGrace, muxModelNoOwnerTimer),
	muxModelNever("absorb_pending", muxModelEventOwnershipLimit, muxModelNoOwnerTimer),
	muxModelNever("absorb_pending", muxModelEventAbandonWatchdog, muxModelNoSessionGrant),
	muxModelOn("absorb_pending", muxModelEventSessionClose, muxModelState{phase: muxModelIdle, absorb: true}),
	muxModelOn("absorb_pending", muxModelEventReset, muxModelState{phase: muxModelIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),

	muxModelOn("bids_queued", muxModelEventSYN, muxModelState{phase: muxModelStartAsync}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("bids_queued", muxModelEventSymbol, muxModelState{phase: muxModelBidsQueued}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued", muxModelEventBid, muxModelState{phase: muxModelBidsQueued}),
	muxModelOn("bids_queued", muxModelEventStartedMatching, muxModelState{phase: muxModelBidsQueued}),
	muxModelOn("bids_queued", muxModelEventStartedOther, muxModelState{phase: muxModelBidsQueued}),
	muxModelOn("bids_queued", muxModelEventFailed, muxModelState{phase: muxModelBidsQueued}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("bids_queued", muxModelEventStartDeadline, muxModelNoStartDeadline),
	muxModelNever("bids_queued", muxModelEventAbsorbDebounce, muxModelNoAbsorb),
	muxModelNever("bids_queued", muxModelEventIdleGrace, muxModelNoOwnerTimer),
	muxModelNever("bids_queued", muxModelEventOwnershipLimit, muxModelNoOwnerTimer),
	muxModelNever("bids_queued", muxModelEventAbandonWatchdog, muxModelNoSessionGrant),
	muxModelOn("bids_queued", muxModelEventSessionClose, muxModelState{phase: muxModelIdle}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued", muxModelEventReset, muxModelState{phase: muxModelIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),

	muxModelOn("fenced", muxModelEventSYN, muxModelState{phase: muxModelIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced", muxModelEventSymbol, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced", muxModelEventBid, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN}),
	muxModelOn("fenced", muxModelEventStartedMatching, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN}),
	muxModelOn("fenced", muxModelEventStartedOther, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced", muxModelEventFailed, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("fenced", muxModelEventStartDeadline, muxModelNoStartDeadline),
	muxModelNever("fenced", muxModelEventAbsorbDebounce, muxModelNoAbsorb),
	muxModelNever("fenced", muxModelEventIdleGrace, muxModelNoOwnerTimer),
	muxModelNever("fenced", muxModelEventOwnershipLimit, muxModelNoOwnerTimer),
	muxModelNever("fenced", muxModelEventAbandonWatchdog, muxModelNoSessionGrant),
	muxModelOn("fenced", muxModelEventSessionClose, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN}),
	muxModelOn("fenced", muxModelEventReset, muxModelState{phase: muxModelIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),

	muxModelOn("fenced_absorb_pending", muxModelEventSYN, muxModelState{phase: muxModelIdle, absorb: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_absorb_pending", muxModelEventSymbol, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN, absorb: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_absorb_pending", muxModelEventBid, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN, absorb: true}),
	muxModelOn("fenced_absorb_pending", muxModelEventStartedMatching, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN}),
	muxModelOn("fenced_absorb_pending", muxModelEventStartedOther, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_absorb_pending", muxModelEventFailed, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("fenced_absorb_pending", muxModelEventStartDeadline, muxModelNoStartDeadline),
	muxModelOn("fenced_absorb_pending", muxModelEventAbsorbDebounce, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN}),
	muxModelNever("fenced_absorb_pending", muxModelEventIdleGrace, muxModelNoOwnerTimer),
	muxModelNever("fenced_absorb_pending", muxModelEventOwnershipLimit, muxModelNoOwnerTimer),
	muxModelNever("fenced_absorb_pending", muxModelEventAbandonWatchdog, muxModelNoSessionGrant),
	muxModelOn("fenced_absorb_pending", muxModelEventSessionClose, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN, absorb: true}),
	muxModelOn("fenced_absorb_pending", muxModelEventReset, muxModelState{phase: muxModelIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),

	muxModelOn("gateway_grace", muxModelEventSYN, muxModelState{phase: muxModelGatewayGrace}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("gateway_grace", muxModelEventSymbol, muxModelState{phase: muxModelGatewayGrace}, muxModelEffectSessionReceived),
	muxModelOn("gateway_grace", muxModelEventBid, muxModelState{phase: muxModelGatewayGrace}),
	muxModelOn("gateway_grace", muxModelEventStartedMatching, muxModelState{phase: muxModelGatewayGrace}),
	muxModelOn("gateway_grace", muxModelEventStartedOther, muxModelState{phase: muxModelGatewayGrace}),
	muxModelOn("gateway_grace", muxModelEventFailed, muxModelState{phase: muxModelGatewayGrace}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("gateway_grace", muxModelEventStartDeadline, muxModelNoStartDeadline),
	muxModelNever("gateway_grace", muxModelEventAbsorbDebounce, muxModelNoAbsorb),
	muxModelOn("gateway_grace", muxModelEventIdleGrace, muxModelState{phase: muxModelIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("gateway_grace", muxModelEventOwnershipLimit, muxModelState{phase: muxModelIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("gateway_grace", muxModelEventAbandonWatchdog, muxModelNoSessionGrant),
	muxModelOn("gateway_grace", muxModelEventSessionClose, muxModelState{phase: muxModelGatewayGrace}),
	muxModelOn("gateway_grace", muxModelEventReset, muxModelState{phase: muxModelIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),

	muxModelOn("gateway_transaction", muxModelEventSYN, muxModelState{phase: muxModelGatewayTransaction}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("gateway_transaction", muxModelEventSymbol, muxModelState{phase: muxModelGatewayTransaction}, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("gateway_transaction", muxModelEventBid, muxModelState{phase: muxModelGatewayTransaction}),
	muxModelOn("gateway_transaction", muxModelEventStartedMatching, muxModelState{phase: muxModelGatewayTransaction}),
	muxModelOn("gateway_transaction", muxModelEventStartedOther, muxModelState{phase: muxModelGatewayTransaction}),
	muxModelOn("gateway_transaction", muxModelEventFailed, muxModelState{phase: muxModelGatewayTransaction}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("gateway_transaction", muxModelEventStartDeadline, muxModelNoStartDeadline),
	muxModelNever("gateway_transaction", muxModelEventAbsorbDebounce, muxModelNoAbsorb),
	muxModelOn("gateway_transaction", muxModelEventIdleGrace, muxModelState{phase: muxModelIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("gateway_transaction", muxModelEventOwnershipLimit, muxModelState{phase: muxModelIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("gateway_transaction", muxModelEventAbandonWatchdog, muxModelNoSessionGrant),
	muxModelOn("gateway_transaction", muxModelEventSessionClose, muxModelState{phase: muxModelGatewayTransaction}),
	muxModelOn("gateway_transaction", muxModelEventReset, muxModelState{phase: muxModelIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),

	muxModelOn("held", muxModelEventSYN, muxModelState{phase: muxModelHeld}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("held", muxModelEventSymbol, muxModelState{phase: muxModelHeld}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("held", muxModelEventBid, muxModelState{phase: muxModelHeld}),
	muxModelOn("held", muxModelEventStartedMatching, muxModelState{phase: muxModelHeld}),
	muxModelOn("held", muxModelEventStartedOther, muxModelState{phase: muxModelHeld}),
	muxModelOn("held", muxModelEventFailed, muxModelState{phase: muxModelHeld}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("held", muxModelEventStartDeadline, muxModelNoStartDeadline),
	muxModelNever("held", muxModelEventAbsorbDebounce, muxModelNoAbsorb),
	muxModelNever("held", muxModelEventIdleGrace, muxModelNoOwnerTimer),
	muxModelNever("held", muxModelEventOwnershipLimit, muxModelNoOwnerTimer),
	muxModelNever("held", muxModelEventAbandonWatchdog, muxModelNoSessionGrant),
	muxModelOn("held", muxModelEventSessionClose, muxModelState{phase: muxModelHeld}, muxModelEffectResultCancelled),
	muxModelOn("held", muxModelEventReset, muxModelState{phase: muxModelIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),

	muxModelOn("idle", muxModelEventSYN, muxModelState{phase: muxModelIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle", muxModelEventSymbol, muxModelState{phase: muxModelIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle", muxModelEventBid, muxModelState{phase: muxModelBidsQueued}),
	muxModelOn("idle", muxModelEventStartedMatching, muxModelState{phase: muxModelIdle}),
	muxModelOn("idle", muxModelEventStartedOther, muxModelState{phase: muxModelIdle}),
	muxModelOn("idle", muxModelEventFailed, muxModelState{phase: muxModelIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("idle", muxModelEventStartDeadline, muxModelNoStartDeadline),
	muxModelNever("idle", muxModelEventAbsorbDebounce, muxModelNoAbsorb),
	muxModelNever("idle", muxModelEventIdleGrace, muxModelNoOwnerTimer),
	muxModelNever("idle", muxModelEventOwnershipLimit, muxModelNoOwnerTimer),
	muxModelNever("idle", muxModelEventAbandonWatchdog, muxModelNoSessionGrant),
	muxModelOn("idle", muxModelEventSessionClose, muxModelState{phase: muxModelIdle}),
	muxModelOn("idle", muxModelEventReset, muxModelState{phase: muxModelIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),

	muxModelOn("reset_recovery", muxModelEventSYN, muxModelState{phase: muxModelIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("reset_recovery", muxModelEventSymbol, muxModelState{phase: muxModelIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("reset_recovery", muxModelEventBid, muxModelState{phase: muxModelBidsQueued, recovery: arbitrationRecoveryReset}),
	muxModelOn("reset_recovery", muxModelEventStartedMatching, muxModelState{phase: muxModelIdle}),
	muxModelOn("reset_recovery", muxModelEventStartedOther, muxModelState{phase: muxModelIdle}),
	muxModelOn("reset_recovery", muxModelEventFailed, muxModelState{phase: muxModelIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("reset_recovery", muxModelEventStartDeadline, muxModelNoStartDeadline),
	muxModelNever("reset_recovery", muxModelEventAbsorbDebounce, muxModelNoAbsorb),
	muxModelNever("reset_recovery", muxModelEventIdleGrace, muxModelNoOwnerTimer),
	muxModelNever("reset_recovery", muxModelEventOwnershipLimit, muxModelNoOwnerTimer),
	muxModelNever("reset_recovery", muxModelEventAbandonWatchdog, muxModelNoSessionGrant),
	muxModelOn("reset_recovery", muxModelEventSessionClose, muxModelState{phase: muxModelIdle, recovery: arbitrationRecoveryReset}),
	muxModelOn("reset_recovery", muxModelEventReset, muxModelState{phase: muxModelIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),

	muxModelOn("session_owned", muxModelEventSYN, muxModelState{phase: muxModelSessionOwned}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned", muxModelEventSymbol, muxModelState{phase: muxModelSessionOwned}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned", muxModelEventBid, muxModelState{phase: muxModelSessionOwned}),
	muxModelOn("session_owned", muxModelEventStartedMatching, muxModelState{phase: muxModelSessionOwned}),
	muxModelOn("session_owned", muxModelEventStartedOther, muxModelState{phase: muxModelSessionOwned}),
	muxModelOn("session_owned", muxModelEventFailed, muxModelState{phase: muxModelSessionOwned}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("session_owned", muxModelEventStartDeadline, muxModelNoStartDeadline),
	muxModelNever("session_owned", muxModelEventAbsorbDebounce, muxModelNoAbsorb),
	muxModelOn("session_owned", muxModelEventIdleGrace, muxModelState{phase: muxModelIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned", muxModelEventOwnershipLimit, muxModelState{phase: muxModelIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned", muxModelEventAbandonWatchdog, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN}),
	muxModelOn("session_owned", muxModelEventSessionClose, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN}),
	muxModelOn("session_owned", muxModelEventReset, muxModelState{phase: muxModelIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),

	muxModelOn("start_async", muxModelEventSYN, muxModelState{phase: muxModelStartAsync}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_async", muxModelEventSymbol, muxModelState{phase: muxModelStartAsync}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_async", muxModelEventBid, muxModelState{phase: muxModelStartAsync}),
	muxModelOn("start_async", muxModelEventStartedMatching, muxModelState{phase: muxModelSessionOwned}, muxModelEffectPassiveSymbol, muxModelEffectSessionStarted, muxModelEffectSessionReceived, muxModelEffectResultGranted),
	muxModelOn("start_async", muxModelEventStartedOther, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN, absorb: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async", muxModelEventFailed, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async", muxModelEventStartDeadline, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN, absorb: true}, muxModelEffectResultError),
	muxModelNever("start_async", muxModelEventAbsorbDebounce, muxModelNoAbsorb),
	muxModelNever("start_async", muxModelEventIdleGrace, muxModelNoOwnerTimer),
	muxModelNever("start_async", muxModelEventOwnershipLimit, muxModelNoOwnerTimer),
	muxModelNever("start_async", muxModelEventAbandonWatchdog, muxModelNoSessionGrant),
	muxModelOn("start_async", muxModelEventSessionClose, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN, absorb: true}, muxModelEffectResultCancelled),
	muxModelOn("start_async", muxModelEventReset, muxModelState{phase: muxModelIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),

	muxModelOn("start_async_resync_owed", muxModelEventSYN, muxModelState{phase: muxModelStartAsync, resyncOwed: 1}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("start_async_resync_owed", muxModelEventSymbol, muxModelState{phase: muxModelStartAsync, resyncOwed: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_async_resync_owed", muxModelEventBid, muxModelState{phase: muxModelStartAsync, resyncOwed: 2}),
	muxModelOn("start_async_resync_owed", muxModelEventStartedMatching, muxModelState{phase: muxModelSessionOwned, resyncOwed: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionStarted, muxModelEffectResultGranted),
	muxModelOn("start_async_resync_owed", muxModelEventStartedOther, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN, absorb: true, resyncOwed: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_resync_owed", muxModelEventFailed, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_resync_owed", muxModelEventStartDeadline, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN, absorb: true, resyncOwed: 2}, muxModelEffectResultError),
	muxModelNever("start_async_resync_owed", muxModelEventAbsorbDebounce, muxModelNoAbsorb),
	muxModelNever("start_async_resync_owed", muxModelEventIdleGrace, muxModelNoOwnerTimer),
	muxModelNever("start_async_resync_owed", muxModelEventOwnershipLimit, muxModelNoOwnerTimer),
	muxModelNever("start_async_resync_owed", muxModelEventAbandonWatchdog, muxModelNoSessionGrant),
	muxModelOn("start_async_resync_owed", muxModelEventSessionClose, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN, absorb: true, resyncOwed: 2}, muxModelEffectResultCancelled),
	muxModelOn("start_async_resync_owed", muxModelEventReset, muxModelState{phase: muxModelIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),

	muxModelOn("start_blocking", muxModelEventSYN, muxModelState{phase: muxModelStartBlocking, blocking: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_blocking", muxModelEventSymbol, muxModelState{phase: muxModelStartBlocking, blocking: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_blocking", muxModelEventBid, muxModelState{phase: muxModelStartBlocking, blocking: true}),
	muxModelOn("start_blocking", muxModelEventStartedMatching, muxModelState{phase: muxModelSessionOwned, blocking: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionStarted, muxModelEffectSessionReceived, muxModelEffectResultGranted),
	muxModelOn("start_blocking", muxModelEventStartedOther, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN, absorb: true, blocking: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_blocking", muxModelEventFailed, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN, blocking: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_blocking", muxModelEventStartDeadline, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN, absorb: true, blocking: true}, muxModelEffectResultError),
	muxModelNever("start_blocking", muxModelEventAbsorbDebounce, muxModelNoAbsorb),
	muxModelNever("start_blocking", muxModelEventIdleGrace, muxModelNoOwnerTimer),
	muxModelNever("start_blocking", muxModelEventOwnershipLimit, muxModelNoOwnerTimer),
	muxModelNever("start_blocking", muxModelEventAbandonWatchdog, muxModelNoSessionGrant),
	muxModelOn("start_blocking", muxModelEventSessionClose, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN, absorb: true, blocking: true}, muxModelEffectResultCancelled),
	muxModelOn("start_blocking", muxModelEventReset, muxModelState{phase: muxModelIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),

	muxModelNever("absorb_pending", muxModelEventStartCallReturns, muxModelNoBlockingCall),
	muxModelNever("absorb_pending", muxModelEventStartCallFails, muxModelNoBlockingCall),
	muxModelNever("bids_queued", muxModelEventStartCallReturns, muxModelNoBlockingCall),
	muxModelNever("bids_queued", muxModelEventStartCallFails, muxModelNoBlockingCall),
	muxModelNever("fenced", muxModelEventStartCallReturns, muxModelNoBlockingCall),
	muxModelNever("fenced", muxModelEventStartCallFails, muxModelNoBlockingCall),
	muxModelNever("fenced_absorb_pending", muxModelEventStartCallReturns, muxModelNoBlockingCall),
	muxModelNever("fenced_absorb_pending", muxModelEventStartCallFails, muxModelNoBlockingCall),
	muxModelNever("gateway_grace", muxModelEventStartCallReturns, muxModelNoBlockingCall),
	muxModelNever("gateway_grace", muxModelEventStartCallFails, muxModelNoBlockingCall),
	muxModelNever("gateway_transaction", muxModelEventStartCallReturns, muxModelNoBlockingCall),
	muxModelNever("gateway_transaction", muxModelEventStartCallFails, muxModelNoBlockingCall),
	muxModelNever("held", muxModelEventStartCallReturns, muxModelNoBlockingCall),
	muxModelNever("held", muxModelEventStartCallFails, muxModelNoBlockingCall),
	muxModelNever("idle", muxModelEventStartCallReturns, muxModelNoBlockingCall),
	muxModelNever("idle", muxModelEventStartCallFails, muxModelNoBlockingCall),
	muxModelNever("reset_recovery", muxModelEventStartCallReturns, muxModelNoBlockingCall),
	muxModelNever("reset_recovery", muxModelEventStartCallFails, muxModelNoBlockingCall),
	muxModelNever("session_owned", muxModelEventStartCallReturns, muxModelNoBlockingCall),
	muxModelNever("session_owned", muxModelEventStartCallFails, muxModelNoBlockingCall),
	muxModelNever("start_async", muxModelEventStartCallReturns, muxModelNoBlockingCall),
	muxModelNever("start_async", muxModelEventStartCallFails, muxModelNoBlockingCall),
	muxModelNever("start_async_resync_owed", muxModelEventStartCallReturns, muxModelNoBlockingCall),
	muxModelNever("start_async_resync_owed", muxModelEventStartCallFails, muxModelNoBlockingCall),
	muxModelOn("start_blocking", muxModelEventStartCallReturns, muxModelState{phase: muxModelSessionOwned}, muxModelEffectSessionStarted, muxModelEffectResultGranted),
	muxModelOn("start_blocking", muxModelEventStartCallFails, muxModelState{phase: muxModelIdle}, muxModelEffectResultError),
	muxModelOn("start_gateway", muxModelEventSYN, muxModelState{phase: muxModelStartAsync, gateway: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_gateway", muxModelEventSymbol, muxModelState{phase: muxModelStartAsync, gateway: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_gateway", muxModelEventBid, muxModelState{phase: muxModelStartAsync, gateway: true}),
	muxModelOn("start_gateway", muxModelEventStartedMatching, muxModelState{phase: muxModelGatewayGrace}, muxModelEffectSessionReceived, muxModelEffectResultGranted),
	muxModelOn("start_gateway", muxModelEventStartedOther, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN, absorb: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_gateway", muxModelEventFailed, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_gateway", muxModelEventStartDeadline, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN, absorb: true}, muxModelEffectResultError),
	muxModelNever("start_gateway", muxModelEventAbsorbDebounce, muxModelNoAbsorb),
	muxModelNever("start_gateway", muxModelEventIdleGrace, muxModelNoOwnerTimer),
	muxModelNever("start_gateway", muxModelEventOwnershipLimit, muxModelNoOwnerTimer),
	muxModelNever("start_gateway", muxModelEventAbandonWatchdog, muxModelNoSessionGrant),
	muxModelOn("start_gateway", muxModelEventSessionClose, muxModelState{phase: muxModelStartAsync, gateway: true}),
	muxModelOn("start_gateway", muxModelEventReset, muxModelState{phase: muxModelIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("start_gateway", muxModelEventStartCallReturns, muxModelNoBlockingCall),
	muxModelNever("start_gateway", muxModelEventStartCallFails, muxModelNoBlockingCall),
	muxModelOn("gateway_awaiting_echo", muxModelEventSYN, muxModelState{phase: muxModelGatewayTransaction, echoPending: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("gateway_awaiting_echo", muxModelEventSymbol, muxModelState{phase: muxModelGatewayTransaction, echoPending: true}, muxModelEffectSessionReceived),
	muxModelOn("gateway_awaiting_echo", muxModelEventBid, muxModelState{phase: muxModelGatewayTransaction, echoPending: true}),
	muxModelOn("gateway_awaiting_echo", muxModelEventStartedMatching, muxModelState{phase: muxModelGatewayTransaction, echoPending: true}),
	muxModelOn("gateway_awaiting_echo", muxModelEventStartedOther, muxModelState{phase: muxModelGatewayTransaction, echoPending: true}),
	muxModelOn("gateway_awaiting_echo", muxModelEventFailed, muxModelState{phase: muxModelGatewayTransaction, echoPending: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("gateway_awaiting_echo", muxModelEventStartDeadline, muxModelNoStartDeadline),
	muxModelNever("gateway_awaiting_echo", muxModelEventAbsorbDebounce, muxModelNoAbsorb),
	muxModelOn("gateway_awaiting_echo", muxModelEventIdleGrace, muxModelState{phase: muxModelIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("gateway_awaiting_echo", muxModelEventOwnershipLimit, muxModelState{phase: muxModelIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("gateway_awaiting_echo", muxModelEventAbandonWatchdog, muxModelNoSessionGrant),
	muxModelOn("gateway_awaiting_echo", muxModelEventSessionClose, muxModelState{phase: muxModelGatewayTransaction, echoPending: true}),
	muxModelOn("gateway_awaiting_echo", muxModelEventReset, muxModelState{phase: muxModelIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("gateway_awaiting_echo", muxModelEventStartCallReturns, muxModelNoBlockingCall),
	muxModelNever("gateway_awaiting_echo", muxModelEventStartCallFails, muxModelNoBlockingCall),
	muxModelOn("start_blocking_deferred_loser", muxModelEventSYN, muxModelState{phase: muxModelStartBlocking, blocking: true, deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_blocking_deferred_loser", muxModelEventSymbol, muxModelState{phase: muxModelStartBlocking, blocking: true, deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_blocking_deferred_loser", muxModelEventBid, muxModelState{phase: muxModelStartBlocking, blocking: true, deferred: true}),
	muxModelOn("start_blocking_deferred_loser", muxModelEventStartedMatching, muxModelState{phase: muxModelSessionOwned, blocking: true, deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionStarted, muxModelEffectResultGranted),
	muxModelOn("start_blocking_deferred_loser", muxModelEventStartedOther, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN, absorb: true, blocking: true, deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_blocking_deferred_loser", muxModelEventFailed, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_blocking_deferred_loser", muxModelEventStartDeadline, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN, absorb: true, blocking: true}, muxModelEffectResultError),
	muxModelNever("start_blocking_deferred_loser", muxModelEventAbsorbDebounce, muxModelNoAbsorb),
	muxModelNever("start_blocking_deferred_loser", muxModelEventIdleGrace, muxModelNoOwnerTimer),
	muxModelNever("start_blocking_deferred_loser", muxModelEventOwnershipLimit, muxModelNoOwnerTimer),
	muxModelNever("start_blocking_deferred_loser", muxModelEventAbandonWatchdog, muxModelNoSessionGrant),
	muxModelOn("start_blocking_deferred_loser", muxModelEventSessionClose, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN, absorb: true, blocking: true, deferred: true}, muxModelEffectResultCancelled),
	muxModelOn("start_blocking_deferred_loser", muxModelEventReset, muxModelState{phase: muxModelIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("start_blocking_deferred_loser", muxModelEventStartCallReturns, muxModelState{phase: muxModelSessionOwned, resyncOwed: 2}, muxModelEffectSessionStarted, muxModelEffectResultGranted, muxModelEffectResultLost),
	muxModelOn("start_blocking_deferred_loser", muxModelEventStartCallFails, muxModelState{phase: muxModelIdle}, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("session_owned_other", muxModelEventSYN, muxModelState{phase: muxModelSessionOwned, otherOwner: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_other", muxModelEventSymbol, muxModelState{phase: muxModelSessionOwned, otherOwner: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_other", muxModelEventBid, muxModelState{phase: muxModelSessionOwned, otherOwner: true}),
	muxModelOn("session_owned_other", muxModelEventStartedMatching, muxModelState{phase: muxModelSessionOwned, otherOwner: true}),
	muxModelOn("session_owned_other", muxModelEventStartedOther, muxModelState{phase: muxModelSessionOwned, otherOwner: true}),
	muxModelOn("session_owned_other", muxModelEventFailed, muxModelState{phase: muxModelSessionOwned, otherOwner: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("session_owned_other", muxModelEventStartDeadline, muxModelNoStartDeadline),
	muxModelNever("session_owned_other", muxModelEventAbsorbDebounce, muxModelNoAbsorb),
	muxModelOn("session_owned_other", muxModelEventIdleGrace, muxModelState{phase: muxModelIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_other", muxModelEventOwnershipLimit, muxModelState{phase: muxModelIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_other", muxModelEventAbandonWatchdog, muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN}),
	muxModelOn("session_owned_other", muxModelEventSessionClose, muxModelState{phase: muxModelSessionOwned, otherOwner: true}),
	muxModelOn("session_owned_other", muxModelEventReset, muxModelState{phase: muxModelIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("session_owned_other", muxModelEventStartCallReturns, muxModelNoBlockingCall),
	muxModelNever("session_owned_other", muxModelEventStartCallFails, muxModelNoBlockingCall),
}

// TestMuxStateModelTableIsComplete requires exactly one row per named state
// and event, agreement of every row with the validity rule, distinct named
// states and a driver for every event that occurs somewhere.
func TestMuxStateModelTableIsComplete(t *testing.T) {
	seenState := map[muxModelState]string{}
	for _, name := range muxModelStateNames() {
		named := muxModelStates[name]
		if named.build == nil {
			t.Errorf("state %s has no fixture", name)
		}
		if other, dup := seenState[named.state]; dup {
			t.Errorf("states %s and %s are the same model state %s", other, name, named.state.literal())
		}
		seenState[named.state] = name
	}
	seen := map[string]bool{}
	validEvents := map[muxModelEvent]bool{}
	valid, invalid := 0, 0
	for _, row := range muxModelRows {
		key := row.state + "/" + row.event.String()
		if seen[key] {
			t.Errorf("duplicate row %s", key)
		}
		seen[key] = true
		named, ok := muxModelStates[row.state]
		if !ok {
			t.Errorf("row %s names no model state", key)
			continue
		}
		if row.event >= muxModelEventCount {
			t.Errorf("row %s names no model event", key)
			continue
		}
		reason := muxModelEventCannotOccur(named.state, row.event)
		switch row.outcome {
		case muxModelTransition:
			valid++
			validEvents[row.event] = true
			if reason != "" {
				t.Errorf("row %s is a transition, but the event cannot occur: %s", key, reason)
			}
			if row.reason != "" || row.effects == nil {
				t.Errorf("transition row %s: reason %q, effects %v", key, row.reason, row.effects)
			}
		case muxModelCannotOccur:
			invalid++
			if reason == "" || row.reason != reason {
				t.Errorf("row %s: cannot occur because %q, the rule says %q", key, row.reason, reason)
			}
			if row.next != (muxModelState{}) || row.effects != nil {
				t.Errorf("row %s cannot occur but defines next %s and effects %v", key, row.next.literal(), row.effects)
			}
		default:
			t.Errorf("row %s has outcome %d", key, row.outcome)
		}
	}
	var missing []string
	for _, name := range muxModelStateNames() {
		for e := muxModelEvent(0); e < muxModelEventCount; e++ {
			if !seen[name+"/"+e.String()] {
				missing = append(missing, name+"/"+e.String())
			}
		}
	}
	if len(missing) > 0 {
		t.Errorf("missing rows:\n%s", strings.Join(missing, "\n"))
	}
	for e := muxModelEvent(0); e < muxModelEventCount; e++ {
		if !validEvents[e] {
			t.Errorf("event %s occurs in no state", e)
		}
		if muxModelDrivers[e] == nil {
			t.Errorf("event %s has no driver", e)
		}
	}
	if want := len(muxModelStates) * int(muxModelEventCount); valid+invalid != want {
		t.Errorf("%d valid and %d invalid rows, want %d in total", valid, invalid, want)
	}
}

// TestMuxStateModelDerivation varies every field the model state is derived
// from and requires the derived state to follow.
func TestMuxStateModelDerivation(t *testing.T) {
	cases := []struct {
		name string
		in   transitionState
		held bool
		echo bool
		want muxModelState
		fail bool
	}{
		{name: "zero", want: muxModelState{phase: muxModelIdle}},
		{name: "owner gateway with transaction", in: transitionState{owner: "gateway", gatewayTxn: true},
			want: muxModelState{phase: muxModelGatewayTransaction}},
		{name: "owner gateway", in: transitionState{owner: "gateway"},
			want: muxModelState{phase: muxModelGatewayGrace}},
		{name: "owner session", in: transitionState{owner: "session 1"},
			want: muxModelState{phase: muxModelSessionOwned}},
		{name: "owner before start", in: transitionState{owner: "session 1", start: "session 2 0x71"},
			want: muxModelState{phase: muxModelSessionOwned}},
		{name: "start", in: transitionState{start: "session 1 0x31"},
			want: muxModelState{phase: muxModelStartAsync}},
		{name: "start blocking", in: transitionState{start: "session 1 0x31", startBlocking: true},
			want: muxModelState{phase: muxModelStartBlocking}},
		{name: "start response is not a phase", in: transitionState{start: "session 1 0x31", startResponse: true},
			want: muxModelState{phase: muxModelStartAsync}},
		{name: "start gateway", in: transitionState{start: "gateway 0x7F"},
			want: muxModelState{phase: muxModelStartAsync, gateway: true}},
		{name: "start gateway blocking", in: transitionState{start: "gateway 0x7F", startBlocking: true},
			want: muxModelState{phase: muxModelStartBlocking, gateway: true}},
		{name: "gateway deferred loser is no selected party", in: transitionState{start: "session 1 0x31", bids: "deferred gateway 0x7F"},
			want: muxModelState{phase: muxModelStartAsync, deferred: true}},
		{name: "start before fence", in: transitionState{start: "session 1 0x31", regrantFence: true},
			want: muxModelState{phase: muxModelStartAsync}},
		{name: "fence", in: transitionState{regrantFence: true, recovery: "at_syn"},
			want: muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryAtSYN}},
		{name: "fence foreign", in: transitionState{regrantFence: true, recovery: "foreign"},
			want: muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryForeign}},
		{name: "fence foreign after syn symbol", in: transitionState{regrantFence: true, recovery: "foreign_after_syn_symbol"},
			want: muxModelState{phase: muxModelFenced, recovery: arbitrationRecoveryForeignAfterAA}},
		{name: "fence before bids", in: transitionState{regrantFence: true, bids: "session 1 0x31"},
			want: muxModelState{phase: muxModelFenced}},
		{name: "reset recovery", in: transitionState{recovery: "reset"},
			want: muxModelState{phase: muxModelIdle, recovery: arbitrationRecoveryReset}},
		{name: "unknown recovery", in: transitionState{recovery: "other"}, fail: true},
		{name: "held", held: true, want: muxModelState{phase: muxModelHeld}},
		{name: "held before bids", in: transitionState{bids: "session 1 0x31"}, held: true,
			want: muxModelState{phase: muxModelHeld}},
		{name: "held under owner", in: transitionState{owner: "session 1"}, held: true, fail: true},
		{name: "held under start", in: transitionState{start: "session 1 0x31"}, held: true, fail: true},
		{name: "held under fence", in: transitionState{regrantFence: true}, held: true, fail: true},
		{name: "bids", in: transitionState{bids: "session 1 0x31"},
			want: muxModelState{phase: muxModelBidsQueued}},
		{name: "deferred gateway loser is no bid", in: transitionState{bids: "deferred gateway 0x7F"},
			want: muxModelState{phase: muxModelIdle, deferred: true}},
		{name: "bid and deferred gateway loser", in: transitionState{bids: "session 2 0x71, deferred gateway 0x7F"},
			want: muxModelState{phase: muxModelBidsQueued, deferred: true}},
		{name: "deferred loser under a blocking start", in: transitionState{start: "session 1 0x31", startBlocking: true, blockingCall: true, bids: "deferred gateway 0x7F"},
			want: muxModelState{phase: muxModelStartBlocking, blocking: true, deferred: true}},
		{name: "owner session 2", in: transitionState{owner: "session 2"},
			want: muxModelState{phase: muxModelSessionOwned, otherOwner: true}},
		{name: "echo pending in a gateway transaction", in: transitionState{owner: "gateway", gatewayTxn: true}, echo: true,
			want: muxModelState{phase: muxModelGatewayTransaction, echoPending: true}},
		{name: "echo pending outside a gateway transaction is not modelled", in: transitionState{owner: "gateway"}, echo: true,
			want: muxModelState{phase: muxModelGatewayGrace}},
		{name: "absorb", in: transitionState{absorb: 1},
			want: muxModelState{phase: muxModelIdle, absorb: true}},
		{name: "absorb counts", in: transitionState{absorb: 3},
			want: muxModelState{phase: muxModelIdle, absorb: true}},
		{name: "blocking call", in: transitionState{blockingCall: true},
			want: muxModelState{phase: muxModelIdle, blocking: true}},
		{name: "resync owed", in: transitionState{resyncOwed: 2},
			want: muxModelState{phase: muxModelIdle, resyncOwed: 2}},
		{name: "fields outside the model", in: transitionState{retiredSelection: true, phase: "collect_request", localLosers: "session 2"},
			want: muxModelState{phase: muxModelIdle}},
	}
	for _, tc := range cases {
		got, err := muxModelDerive(tc.in, tc.held, tc.echo)
		if tc.fail {
			if err == nil {
				t.Errorf("%s: derived %s, want an error", tc.name, got.literal())
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("%s: derived %s (%v), want %s", tc.name, got.literal(), err, tc.want.literal())
		}
	}
}

// TestMuxStateModelEffectKinds classifies every effect form the harness logs
// and counts consecutive effects of one kind once.
func TestMuxStateModelEffectKinds(t *testing.T) {
	cases := map[string]muxModelEffect{
		"passive: symbol 0xAA":                                          muxModelEffectPassiveSymbol,
		"passive: symbol 0xAA escaped":                                  muxModelEffectPassiveSymbol,
		"passive: reset":                                                muxModelEffectPassiveReset,
		"passive: connected":                                            muxModelEffectPassiveLifecycle,
		"adapter: RequestStart 0x31":                                    muxModelEffectAdapterStart,
		"adapter: StartArbitration 0x31":                                muxModelEffectAdapterStartBlocking,
		"adapter: Write 15":                                             muxModelEffectAdapterWrite,
		"abandon watchdog armed for 1s":                                 muxModelEffectWatchdogArmed,
		"active: error ebus: x":                                         muxModelEffectActiveError,
		"active: 0x52":                                                  muxModelEffectActiveByte,
		"session 1: RECEIVED 0x52":                                      muxModelEffectSessionReceived,
		"session 1: STARTED 0x31":                                       muxModelEffectSessionStarted,
		"session 2: FAILED 0x31":                                        muxModelEffectSessionFailed,
		"session 2: RESETTED 0x00":                                      muxModelEffectSessionReset,
		"session 2: ERROR_EBUS":                                         muxModelEffectSessionError,
		"session 2: INFO 0x00":                                          muxModelEffectSessionInfo,
		"result session 1 0x31: granted, source 0x31":                   muxModelEffectResultGranted,
		"result gateway 0x7F: source 0x31":                              muxModelEffectResultLost,
		"result session 1 0x31: cancelled, source 0x31":                 muxModelEffectResultCancelled,
		"result session 1 0x31: source 0x31, error adaptermux: expired": muxModelEffectResultError,
	}
	for entry, want := range cases {
		got, err := muxModelEffectKind(entry)
		if err != nil || got != want {
			t.Errorf("%q: kind %s (%v), want %s", entry, got, err, want)
		}
	}
	for _, entry := range []string{"session 1 RECEIVED", "session 1: OTHER", "result x", "other"} {
		if got, err := muxModelEffectKind(entry); err == nil {
			t.Errorf("%q: kind %s, want an error", entry, got)
		}
	}
	got, err := muxModelEffectKinds([]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA", "passive: symbol 0x10"})
	want := []muxModelEffect{muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectPassiveSymbol}
	if err != nil || muxModelEffectsLiteral(got) != muxModelEffectsLiteral(want) {
		t.Errorf("kinds %v (%v), want %v", got, err, want)
	}
	if got, err := muxModelEffectKinds(nil); err != nil || got == nil || len(got) != 0 {
		t.Errorf("kinds of no effect %v (%v), want an empty list", got, err)
	}
}

// TestMuxStateModelConformance drives every transition row through the
// current handlers: the fixture must reach the row's state, and the event must
// lead to the row's next state with the row's ordered effect kinds.
func TestMuxStateModelConformance(t *testing.T) {
	for _, row := range muxModelRows {
		if row.outcome != muxModelTransition {
			continue
		}
		row := row
		t.Run(row.state+"/"+row.event.String(), func(t *testing.T) {
			named, ok := muxModelStates[row.state]
			if !ok {
				t.Fatalf("unknown model state %s", row.state)
			}
			drive, ok := muxModelDrivers[row.event]
			if !ok {
				t.Fatalf("no driver for %s", row.event)
			}
			h := newTransitionHarness(t, named.mode)
			named.build(h)
			h.discardEffects()
			from, err := muxModelObserve(h)
			if err != nil || from != named.state {
				t.Fatalf("fixture reaches %s (%v), want %s", from.literal(), err, named.state.literal())
			}
			drive(h)
			got, err := muxModelObserve(h)
			raw := h.effects()
			h.stop()
			if err != nil {
				t.Fatalf("derive next state: %v", err)
			}
			effects, err := muxModelEffectKinds(raw)
			if err != nil {
				t.Fatalf("classify effects %#v: %v", raw, err)
			}
			if got != row.next || muxModelEffectsLiteral(effects) != muxModelEffectsLiteral(row.effects) {
				t.Errorf("transition differs from the table\n got: %s\nwant: %s\neffect log: %#v",
					muxModelRowLiteral(row.state, row.event, got, effects),
					muxModelRowLiteral(row.state, row.event, row.next, row.effects), raw)
			}
		})
	}
}
