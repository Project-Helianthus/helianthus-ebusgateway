package adaptermux

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// The state model specifies the AdapterMux transition behaviour over the
// typed state, events and transition function of mux_state.go:
//
//   - a model state is the mux configuration together with the typed
//     muxState the mux derives from its fields;
//   - muxModelEvent is the set of table inputs; each is one production
//     muxEvent applied through the real handlers by its driver;
//   - muxModelRows is the transition table: for every named model state and
//     every event either the next state and the ordered effect kinds, or an
//     explicit row stating that the event cannot occur in that state, for
//     the reason decideMuxTransition gives. The table is closed: every next
//     state is itself a named state with rows.
//
// The configuration that changes how an event is handled is part of the
// model state: whether the adapter offers only the blocking StartArbitration
// call, and whether the classifier runs in enforce mode (the foreign recovery
// kinds only arise there). No event changes it, so each configuration has its
// own closed set of states, built from root fixtures in that configuration.
// The initiator predicate is an input of FAILED instead: the phantom FAILED
// event runs with a predicate that rejects the winner it names.
//
// The table pins today's behaviour, defects included. The conformance test
// drives every valid row through the handlers with the transition harness
// (transition_harness_test.go) and compares the derived state and the
// ordered effect kinds with the row; the result test checks the read loop's
// STARTED and FAILED decisions of decideMuxTransition against the rows.

// muxModelConfig is the configuration part of the model state: it is fixed
// when the mux is built and no event changes it.
type muxModelConfig struct {
	blockingUpstream bool
	enforce          bool
}

var muxModelPhaseNames = [...]string{
	muxIdle:               "muxIdle",
	muxBidsQueued:         "muxBidsQueued",
	muxStartAsync:         "muxStartAsync",
	muxStartBlocking:      "muxStartBlocking",
	muxGatewayTransaction: "muxGatewayTransaction",
	muxGatewayGrace:       "muxGatewayGrace",
	muxSessionOwned:       "muxSessionOwned",
	muxFenced:             "muxFenced",
}

func muxModelPhaseName(p muxPhase) string {
	if int(p) < len(muxModelPhaseNames) {
		return muxModelPhaseNames[p]
	}
	return fmt.Sprintf("muxPhase(%d)", p)
}

var muxModelRecoveryNames = map[arbitrationRecovery]string{
	arbitrationRecoveryNone:           "arbitrationRecoveryNone",
	arbitrationRecoveryAtSYN:          "arbitrationRecoveryAtSYN",
	arbitrationRecoveryReset:          "arbitrationRecoveryReset",
	arbitrationRecoveryForeign:        "arbitrationRecoveryForeign",
	arbitrationRecoveryForeignAfterAA: "arbitrationRecoveryForeignAfterAA",
}

// muxModelSelected is the START in flight of party with source initiator.
func muxModelSelected(party uint64, initiator byte) muxSelection {
	return muxSelection{pending: true, party: party, initiator: initiator}
}

// muxModelBids is the bid set of the parties.
func muxModelBids(parties ...uint64) muxBidSet {
	var b muxBidSet
	for _, p := range parties {
		b.add(p)
	}
	return b
}

func muxModelPartyLiteral(party uint64) string {
	if party == gatewaySessionID {
		return "gatewaySessionID"
	}
	return fmt.Sprintf("%d", party)
}

func muxModelBidsLiteral(b muxBidSet) string {
	if b.more != 0 {
		return fmt.Sprintf("%#v", b)
	}
	var parties []string
	if b.gateway {
		parties = append(parties, muxModelPartyLiteral(gatewaySessionID))
	}
	for _, id := range b.sessions {
		if id != 0 {
			parties = append(parties, muxModelPartyLiteral(id))
		}
	}
	return "muxModelBids(" + strings.Join(parties, ", ") + ")"
}

// muxModelStateLiteral prints the state as the Go literal used in the table.
func muxModelStateLiteral(s muxState) string {
	parts := []string{"phase: " + muxModelPhaseName(s.phase)}
	if s.recovery != arbitrationRecoveryNone {
		name, ok := muxModelRecoveryNames[s.recovery]
		if !ok {
			name = fmt.Sprintf("arbitrationRecovery(%d)", s.recovery)
		}
		parts = append(parts, "recovery: "+name)
	}
	if s.absorb != 0 {
		parts = append(parts, fmt.Sprintf("absorb: %d", s.absorb))
	}
	if s.blocking {
		parts = append(parts, "blocking: true")
	}
	if s.held {
		parts = append(parts, "held: true")
	}
	if s.resyncOwed != 0 {
		parts = append(parts, fmt.Sprintf("resyncOwed: %d", s.resyncOwed))
	}
	switch {
	case s.start == muxSelection{}:
	case s.start.pending:
		parts = append(parts, fmt.Sprintf("start: muxModelSelected(%s, 0x%02X)", muxModelPartyLiteral(s.start.party), s.start.initiator))
	default:
		parts = append(parts, fmt.Sprintf("start: %#v", s.start))
	}
	if s.owner != 0 {
		parts = append(parts, fmt.Sprintf("owner: %d", s.owner))
	}
	if s.bids != (muxBidSet{}) {
		parts = append(parts, "bids: "+muxModelBidsLiteral(s.bids))
	}
	if s.deferred {
		parts = append(parts, "deferred: true")
	}
	if s.retired {
		parts = append(parts, "retired: true")
	}
	if s.retiredSource != 0 {
		parts = append(parts, fmt.Sprintf("retiredSource: 0x%02X", s.retiredSource))
	}
	if s.echoPending {
		parts = append(parts, "echoPending: true")
	}
	if s.echoTerminator {
		parts = append(parts, "echoTerminator: true")
	}
	if s.telegramLive {
		parts = append(parts, "telegramLive: true")
	}
	return "muxState{" + strings.Join(parts, ", ") + "}"
}

// muxModelEvent is one column of the table: a production event applied by
// its driver.
type muxModelEvent uint8

const (
	// muxModelEventSYN: a wire SYN.
	muxModelEventSYN muxModelEvent = iota
	// muxModelEventSymbol: a received data symbol.
	muxModelEventSymbol
	// muxModelEventBid: session 2 bids 0x71.
	muxModelEventBid
	// muxModelEventStartedMatching: STARTED naming the source of the last
	// selection (0x31 when none was made).
	muxModelEventStartedMatching
	// muxModelEventStartedOther: STARTED naming a foreign source.
	muxModelEventStartedOther
	// muxModelEventFailed: FAILED naming a foreign winner the initiator
	// predicate accepts.
	muxModelEventFailed
	// muxModelEventFailedPhantom: FAILED naming a winner the initiator
	// predicate rejects.
	muxModelEventFailedPhantom
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
	// muxModelEventInitiatorSymbol: a received unescaped symbol that is the
	// initiator address of a bus participant outside the mux (0x10).
	muxModelEventInitiatorSymbol
	muxModelEventCount
)

var muxModelEventNames = [...]string{
	muxModelEventSYN:              "muxModelEventSYN",
	muxModelEventSymbol:           "muxModelEventSymbol",
	muxModelEventBid:              "muxModelEventBid",
	muxModelEventStartedMatching:  "muxModelEventStartedMatching",
	muxModelEventStartedOther:     "muxModelEventStartedOther",
	muxModelEventFailed:           "muxModelEventFailed",
	muxModelEventFailedPhantom:    "muxModelEventFailedPhantom",
	muxModelEventStartDeadline:    "muxModelEventStartDeadline",
	muxModelEventAbsorbDebounce:   "muxModelEventAbsorbDebounce",
	muxModelEventIdleGrace:        "muxModelEventIdleGrace",
	muxModelEventOwnershipLimit:   "muxModelEventOwnershipLimit",
	muxModelEventAbandonWatchdog:  "muxModelEventAbandonWatchdog",
	muxModelEventSessionClose:     "muxModelEventSessionClose",
	muxModelEventReset:            "muxModelEventReset",
	muxModelEventStartCallReturns: "muxModelEventStartCallReturns",
	muxModelEventStartCallFails:   "muxModelEventStartCallFails",
	muxModelEventInitiatorSymbol:  "muxModelEventInitiatorSymbol",
}

func (e muxModelEvent) String() string {
	if int(e) < len(muxModelEventNames) {
		return muxModelEventNames[e]
	}
	return fmt.Sprintf("muxModelEvent(%d)", e)
}

// muxModelEventInput is the production event a driver applies in state s.
// STARTED naming the last selection's source names the START in flight, or
// the retired selection when none is in flight.
func muxModelEventInput(e muxModelEvent, s muxState) muxEvent {
	switch e {
	case muxModelEventSYN, muxModelEventIdleGrace:
		kind := muxEventSYN
		if e == muxModelEventIdleGrace {
			kind = muxEventIdleGrace
		}
		return muxEvent{kind: kind, data: 0xAA}
	case muxModelEventSymbol:
		return muxEvent{kind: muxEventSymbol, data: transitionDataSymbol}
	case muxModelEventOwnershipLimit:
		return muxEvent{kind: muxEventOwnershipLimit, data: transitionDataSymbol}
	case muxModelEventInitiatorSymbol:
		return muxEvent{kind: muxEventSymbol, data: transitionForeignSource}
	case muxModelEventBid:
		return muxEvent{kind: muxEventBid, data: transitionOtherSource}
	case muxModelEventStartedMatching:
		source := transitionSessionSource
		switch {
		case s.start.pending:
			source = s.start.initiator
		case s.retired:
			source = s.retiredSource
		}
		return muxEvent{kind: muxEventStarted, data: source}
	case muxModelEventStartedOther:
		return muxEvent{kind: muxEventStarted, data: transitionForeignSource}
	case muxModelEventFailed:
		return muxEvent{kind: muxEventFailed, data: transitionForeignSource}
	case muxModelEventFailedPhantom:
		return muxEvent{kind: muxEventFailed, data: transitionForeignSource, phantom: true}
	case muxModelEventStartDeadline:
		return muxEvent{kind: muxEventStartDeadline}
	case muxModelEventAbsorbDebounce:
		return muxEvent{kind: muxEventAbsorbDebounce}
	case muxModelEventAbandonWatchdog:
		return muxEvent{kind: muxEventAbandonWatchdog}
	case muxModelEventSessionClose:
		return muxEvent{kind: muxEventSessionClose}
	case muxModelEventReset:
		return muxEvent{kind: muxEventReset}
	case muxModelEventStartCallReturns:
		return muxEvent{kind: muxEventStartCallReturns}
	case muxModelEventStartCallFails:
		return muxEvent{kind: muxEventStartCallFails}
	}
	panic(fmt.Sprintf("no production event for %s", e))
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
// through the real handlers: either a root fixture (build, on a harness in the
// configuration cfg) or the named state from followed by the event via,
// applied by its driver, in the configuration of from.
type muxModelNamedState struct {
	build func(h *transitionHarness)
	cfg   muxModelConfig
	from  string
	via   muxModelEvent
	state muxState
}

// muxModelReach names a state reached from the state from by the event via.
func muxModelReach(from string, via muxModelEvent, state muxState) muxModelNamedState {
	return muxModelNamedState{from: from, via: via, state: state}
}

// muxModelRoot returns the root fixture state a named state is reached from.
func muxModelRoot(states map[string]muxModelNamedState, name string) (muxModelNamedState, error) {
	seen := map[string]bool{}
	for {
		named, ok := states[name]
		if !ok {
			return muxModelNamedState{}, fmt.Errorf("unknown model state %s", name)
		}
		if named.from == "" {
			return named, nil
		}
		if seen[name] {
			return muxModelNamedState{}, fmt.Errorf("model state %s reaches itself", name)
		}
		seen[name] = true
		name = named.from
	}
}

// muxModelStateConfig is the configuration of a named state.
func muxModelStateConfig(states map[string]muxModelNamedState, name string) muxModelConfig {
	root, err := muxModelRoot(states, name)
	if err != nil {
		return muxModelConfig{}
	}
	return root.cfg
}

// muxModelHarness builds a harness and drives it into the named state.
func muxModelHarness(t testing.TB, states map[string]muxModelNamedState, name string) *transitionHarness {
	t.Helper()
	root, err := muxModelRoot(states, name)
	if err != nil {
		t.Fatalf("%v", err)
	}
	mode := transitionUpstreamAsync
	if root.cfg.blockingUpstream {
		mode = transitionUpstreamBlocking
	}
	h := newTransitionHarness(t, mode)
	if root.cfg.enforce {
		// The read loop waits for the next item, so the classifier is in
		// place before it handles anything.
		h.m.v8 = v8classifier.New(v8classifier.ModeEnforce)
	}
	muxModelBuild(h, states, name)
	return h
}

func muxModelBuild(h *transitionHarness, states map[string]muxModelNamedState, name string) {
	h.t.Helper()
	named := states[name]
	if named.from == "" {
		named.build(h)
		return
	}
	muxModelBuild(h, states, named.from)
	muxModelDrivers[named.via](h)
	muxModelTrackSelection(h)
}

// muxModelTrackSelection points the harness at the START in flight when an
// event of a reached state selected a new one, so the drivers of the START
// result, the deadline and the blocking call address it. On a blocking
// upstream the new selection's StartArbitration call is accepted, as the root
// fixtures accept theirs, so the call waits for its result. A new selection
// of the party selected before has the same label, so a selection is known by
// its START deadline timer.
func muxModelTrackSelection(h *transitionHarness) {
	h.t.Helper()
	h.m.stateMu.Lock()
	p := h.m.pendingStart
	if p == nil {
		h.m.stateMu.Unlock()
		return
	}
	label := fmt.Sprintf("%s 0x%02X", transitionParty(p.sessionID), p.initiator)
	if label == h.selected && p.deadline == h.deadline {
		h.m.stateMu.Unlock()
		return
	}
	h.selected, h.source, h.deadline = label, p.initiator, p.deadline
	h.m.stateMu.Unlock()
	if h.mode == transitionUpstreamBlocking {
		h.awaitStartCall()
	}
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

// muxModelBuildTerminatorEcho: the gateway transaction wrote its structural
// terminator SYN after a delivered request byte; that echo has not arrived.
func muxModelBuildTerminatorEcho(h *transitionHarness) {
	h.t.Helper()
	transitionGatewayTransaction(h)
	h.gatewayWrite(0xAA, true)
}

// muxModelBuildLiveTelegram: session 1 owns the bus, wrote its request and
// the responder is sending its response body, so the owner's telegram is on
// the wire. The wire symbols are target, PB, SB, NN=1, one data byte, CRC,
// the ACK and the response length 2.
func muxModelBuildLiveTelegram(h *transitionHarness) {
	h.t.Helper()
	transitionSelectSession(h)
	h.feedStarted(transitionSessionSource)
	h.m.stateMu.Lock()
	h.m.phase.markOwnerWrite()
	h.m.stateMu.Unlock()
	for _, b := range []byte{0x08, 0xB5, 0x04, 0x01, 0x55, 0x66, 0x00, 0x02} {
		h.feed(transitionItem{event: transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}})
	}
}

var (
	muxModelAsync    = muxModelConfig{}
	muxModelBlocking = muxModelConfig{blockingUpstream: true}
	muxModelEnforce  = muxModelConfig{enforce: true}
)

// muxModelRootStates are the root fixtures of the model.
var muxModelRootStates = map[string]muxModelNamedState{
	"idle": {build: muxModelFixture("idle"), cfg: muxModelAsync,
		state: muxState{phase: muxIdle}},
	"bids_queued": {build: muxModelFixture("bids_queued"), cfg: muxModelAsync,
		state: muxState{phase: muxBidsQueued, bids: muxModelBids(1)}},
	"start_async": {build: muxModelFixture("start_async"), cfg: muxModelAsync,
		state: muxState{phase: muxStartAsync, start: muxModelSelected(1, 0x31)}},
	// start_async_resync_owed: the gateway lost locally to the selected
	// session, so two resynchronization SYNs are owed to its active path.
	"start_async_resync_owed": {build: muxModelFixture("start_async_local_losers"), cfg: muxModelAsync,
		state: muxState{phase: muxStartAsync, resyncOwed: 2, start: muxModelSelected(1, 0x31)}},
	// start_gateway: the gateway won the selection; the START waits for its
	// adapter result through the stream.
	"start_gateway": {build: muxModelFixture("start_async_gateway"), cfg: muxModelAsync,
		state: muxState{phase: muxStartAsync, start: muxModelSelected(gatewaySessionID, 0x7F)}},
	"start_blocking": {build: muxModelFixture("start_blocking"), cfg: muxModelBlocking,
		state: muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(1, 0x31)}},
	// gateway_awaiting_echo: the gateway wrote its first request byte and the
	// echo has not arrived.
	"gateway_awaiting_echo": {build: muxModelFixture("gateway_awaiting_echo"), cfg: muxModelAsync,
		state: muxState{phase: muxGatewayTransaction, echoPending: true}},
	// gateway_awaiting_terminator_echo: the gateway's request byte was
	// delivered and it wrote its structural terminator SYN, whose echo has
	// not arrived.
	"gateway_awaiting_terminator_echo": {build: muxModelBuildTerminatorEcho, cfg: muxModelAsync,
		state: muxState{phase: muxGatewayTransaction, echoPending: true, echoTerminator: true}},
	"gateway_transaction": {build: muxModelFixture("gateway_transaction"), cfg: muxModelAsync,
		state: muxState{phase: muxGatewayTransaction}},
	// start_blocking_deferred_loser: session 1 was selected over session 2 and
	// the gateway; the gateway's loss stays deferred until the blocking call
	// returns.
	"start_blocking_deferred_loser": {build: muxModelFixture("start_blocking_local_losers"), cfg: muxModelBlocking,
		state: muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(1, 0x31), deferred: true}},
	"gateway_grace": {build: muxModelFixture("gateway_grace"), cfg: muxModelAsync,
		state: muxState{phase: muxGatewayGrace}},
	"session_owned": {build: muxModelFixture("session_owned"), cfg: muxModelAsync,
		state: muxState{phase: muxSessionOwned, owner: 1}},
	// session_owned_other: STARTED granted the bus to session 2, so session 1
	// is not the owner.
	"session_owned_other": {build: func(h *transitionHarness) {
		h.bid(2, transitionOtherSource)
		h.selected = "session 2 0x71"
		h.source = transitionOtherSource
		h.syn()
		h.captureDeadline()
		h.feedStarted(transitionOtherSource)
	}, cfg: muxModelAsync, state: muxState{phase: muxSessionOwned, owner: 2}},
	// session_telegram_live: with the classifier in enforce mode, session 1
	// owns the bus and its telegram is on the wire.
	"session_telegram_live": {build: muxModelBuildLiveTelegram, cfg: muxModelEnforce,
		state: muxState{phase: muxSessionOwned, owner: 1, telegramLive: true}},
	"fenced": {build: muxModelFixture("fenced"), cfg: muxModelAsync,
		state: muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}},
	// fenced_foreign: with the classifier in enforce mode, FAILED naming a
	// real foreign winner makes the fence wait for that winner's telegram.
	"fenced_foreign": {build: muxModelFixture("fenced"), cfg: muxModelEnforce,
		state: muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}},
	// fenced_foreign_absorb_pending: with the classifier in enforce mode,
	// STARTED naming a real foreign winner fences for that winner's telegram
	// and leaves the selected START's own result to absorb.
	"fenced_foreign_absorb_pending": {build: func(h *transitionHarness) {
		muxModelFixture("start_async")(h)
		h.feedStarted(transitionForeignSource)
	}, cfg: muxModelEnforce, state: muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, retired: true, retiredSource: 0x31}},
	// fenced_absorb_pending: the selected session cancelled its START; the
	// fence and the stale-result barrier both hold.
	"fenced_absorb_pending": {build: muxModelFixture("absorb_pending"), cfg: muxModelAsync,
		state: muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x31}},
	// absorb_pending: the SYN after the cancel cleared the fence; the
	// stale-result barrier still holds.
	"absorb_pending": {build: func(h *transitionHarness) {
		muxModelFixture("absorb_pending")(h)
		h.syn()
	}, cfg: muxModelAsync, state: muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x31}},
	"held": {build: muxModelBuildHeld, cfg: muxModelAsync,
		state: muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(1)}},
	// reset_recovery: the adapter reported RESETTED; the lifecycle recovery
	// waits for the next stream item.
	"reset_recovery": {build: func(h *transitionHarness) {
		h.feedReset()
	}, cfg: muxModelAsync, state: muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}},
}

// muxModelStates are all named states: the roots and the states the table's
// transitions reach, each from a named state by one event through its driver.
var muxModelStates = muxModelMergeStates(muxModelRootStates, muxModelReachedStates)

func muxModelMergeStates(maps ...map[string]muxModelNamedState) map[string]muxModelNamedState {
	out := map[string]muxModelNamedState{}
	for _, m := range maps {
		for name, named := range m {
			out[name] = named
		}
	}
	return out
}

// muxModelObserve reads the typed state of the harness mux.
func muxModelObserve(h *transitionHarness) muxState {
	h.m.stateMu.Lock()
	defer h.m.stateMu.Unlock()
	return h.m.muxStateLocked()
}

// muxModelFeedSymbol hands one received data symbol to the read loop.
func muxModelFeedSymbol(h *transitionHarness) {
	h.t.Helper()
	h.feed(transitionItem{event: transport.StreamEvent{Kind: transport.StreamEventByte, Byte: transitionDataSymbol}})
}

// muxModelReleaseStartCall returns err from the held StartArbitration call.
// While the selected START still waits for it, the call delivers the selected
// waiter's result, which is awaited; once that START ended (granted through the
// stream, timed out or cancelled), the call delivers no result and only its
// blocking-call barrier is awaited.
func muxModelReleaseStartCall(h *transitionHarness, err error) {
	h.t.Helper()
	h.m.stateMu.Lock()
	p := h.m.pendingStart
	waiting := p != nil && fmt.Sprintf("%s 0x%02X", transitionParty(p.sessionID), p.initiator) == h.selected
	h.m.stateMu.Unlock()
	if waiting {
		h.releaseStartCall(err, h.selected)
		return
	}
	select {
	case h.blocking.release <- err:
	case <-time.After(transitionWaitBudget):
		h.t.Fatalf("StartArbitration was not waiting for its result")
	}
	deadline := time.Now().Add(transitionWaitBudget)
	for {
		h.m.stateMu.Lock()
		active := h.m.blockingArbActive
		h.m.stateMu.Unlock()
		if !active {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("blocking START call did not finish within %v", transitionWaitBudget)
		}
		time.Sleep(time.Millisecond)
	}
}

// muxModelFeedPhantomFailed hands the read loop a FAILED whose winner the
// initiator predicate rejects. The predicate is read on the read loop only,
// which waits for the item while it is installed and after it is removed.
func muxModelFeedPhantomFailed(h *transitionHarness) {
	h.t.Helper()
	h.m.cfg.IsKnownInitiatorByte = func(b byte) bool { return b != transitionForeignSource }
	h.feedFailed(transitionForeignSource)
	h.m.cfg.IsKnownInitiatorByte = nil
}

// muxModelDrivers apply one event to the harness mux through the handlers.
var muxModelDrivers = map[muxModelEvent]func(h *transitionHarness){
	muxModelEventSYN:             func(h *transitionHarness) { h.feedWireSyn() },
	muxModelEventSymbol:          func(h *transitionHarness) { muxModelFeedSymbol(h) },
	muxModelEventBid:             func(h *transitionHarness) { h.bid(2, transitionOtherSource) },
	muxModelEventStartedMatching: func(h *transitionHarness) { h.feedStarted(h.source) },
	muxModelEventStartedOther:    func(h *transitionHarness) { h.feedStarted(transitionForeignSource) },
	muxModelEventFailed:          func(h *transitionHarness) { h.feedFailed(transitionForeignSource) },
	muxModelEventFailedPhantom:   muxModelFeedPhantomFailed,
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
	muxModelEventStartCallReturns: func(h *transitionHarness) { muxModelReleaseStartCall(h, nil) },
	muxModelEventStartCallFails: func(h *transitionHarness) {
		muxModelReleaseStartCall(h, errors.New("scripted START failure"))
	},
	muxModelEventInitiatorSymbol: func(h *transitionHarness) {
		h.feed(transitionItem{event: transport.StreamEvent{Kind: transport.StreamEventByte, Byte: transitionForeignSource}})
	},
}

// muxModelRun drives the named state's fixture, applies the event and
// returns the state before and after it and the ordered effect kinds.
func muxModelRun(t *testing.T, states map[string]muxModelNamedState, name string, event muxModelEvent) (from, next muxState, effects []muxModelEffect, raw []string) {
	t.Helper()
	drive, ok := muxModelDrivers[event]
	if !ok {
		t.Fatalf("no driver for %s", event)
	}
	h := muxModelHarness(t, states, name)
	h.discardEffects()
	from = muxModelObserve(h)
	drive(h)
	// A selection the event made starts its adapter call on its own
	// goroutine; settling it orders the call's effect into the log.
	muxModelTrackSelection(h)
	next = muxModelObserve(h)
	raw = h.effects()
	h.stop()
	effects, err := muxModelEffectKinds(raw)
	if err != nil {
		t.Fatalf("classify effects %#v: %v", raw, err)
	}
	return from, next, effects, raw
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
	next    muxState
	effects []muxModelEffect
	reason  string
}

func muxModelOn(state string, event muxModelEvent, next muxState, effects ...muxModelEffect) muxModelRow {
	if effects == nil {
		effects = []muxModelEffect{}
	}
	return muxModelRow{state: state, event: event, outcome: muxModelTransition, next: next, effects: effects}
}

func muxModelNever(state string, event muxModelEvent, reason string) muxModelRow {
	return muxModelRow{state: state, event: event, outcome: muxModelCannotOccur, reason: reason}
}

func muxModelRowLiteral(state string, event muxModelEvent, next muxState, effects []muxModelEffect) string {
	args := []string{fmt.Sprintf("%q", state), event.String(), muxModelStateLiteral(next)}
	if len(effects) > 0 {
		args = append(args, muxModelEffectsLiteral(effects))
	}
	return "muxModelOn(" + strings.Join(args, ", ") + "),"
}

func muxModelStateNames(states map[string]muxModelNamedState) []string {
	names := make([]string, 0, len(states))
	for name := range states {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// muxModelKey identifies a model state: the configuration and the typed state.
type muxModelKey struct {
	cfg   muxModelConfig
	state muxState
}

// TestMuxStateModelTableIsComplete requires exactly one row per named state
// and event, agreement of every row with the validity rule of
// decideMuxTransition, distinct named states and a driver for every event
// that occurs somewhere.
func TestMuxStateModelTableIsComplete(t *testing.T) {
	seenState := map[muxModelKey]string{}
	for _, name := range muxModelStateNames(muxModelStates) {
		named := muxModelStates[name]
		if named.from == "" && named.build == nil {
			t.Errorf("state %s has no fixture", name)
		}
		if named.from != "" && (named.build != nil || named.cfg != (muxModelConfig{})) {
			t.Errorf("state %s has a fixture or configuration and is reached from %s", name, named.from)
		}
		if _, err := muxModelRoot(muxModelStates, name); err != nil {
			t.Errorf("state %s: %v", name, err)
		}
		key := muxModelKey{muxModelStateConfig(muxModelStates, name), named.state}
		if other, dup := seenState[key]; dup {
			t.Errorf("states %s and %s are the same model state %s", other, name, muxModelStateLiteral(named.state))
		}
		seenState[key] = name
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
		reason := decideMuxTransition(named.state, muxModelEventInput(row.event, named.state)).invalid
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
				t.Errorf("row %s: cannot occur because %q, the transition function says %q", key, row.reason, reason)
			}
			if row.next != (muxState{}) || row.effects != nil {
				t.Errorf("row %s cannot occur but defines next %s and effects %v", key, muxModelStateLiteral(row.next), row.effects)
			}
		default:
			t.Errorf("row %s has outcome %d", key, row.outcome)
		}
	}
	var missing []string
	for _, name := range muxModelStateNames(muxModelStates) {
		for e := muxModelEvent(0); e < muxModelEventCount; e++ {
			if !seen[name+"/"+e.String()] {
				missing = append(missing, name+"/"+e.String())
			}
		}
	}
	if len(missing) > 0 {
		t.Errorf("missing rows:\n%s", strings.Join(missing, "\n"))
	}
	// The table is closed: every next state is a named state of the same
	// configuration, so its own rows specify what follows. A state reached
	// by an event is the next state of that event's row.
	rows := map[string]muxModelRow{}
	for _, row := range muxModelRows {
		rows[row.state+"/"+row.event.String()] = row
		if row.outcome != muxModelTransition {
			continue
		}
		key := muxModelKey{muxModelStateConfig(muxModelStates, row.state), row.next}
		if _, ok := seenState[key]; !ok {
			t.Errorf("row %s/%s leads to %s in %+v, which is no named state", row.state, row.event, muxModelStateLiteral(row.next), key.cfg)
		}
	}
	for _, name := range muxModelStateNames(muxModelStates) {
		named := muxModelStates[name]
		if named.from == "" {
			continue
		}
		row, ok := rows[named.from+"/"+named.via.String()]
		if !ok || row.outcome != muxModelTransition || row.next != named.state {
			t.Errorf("state %s is reached from %s by %s, but that row leads to %s", name, named.from, named.via, muxModelStateLiteral(row.next))
		}
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
			from, got, effects, raw := muxModelRun(t, muxModelStates, row.state, row.event)
			if from != named.state {
				t.Fatalf("fixture reaches %s, want %s", muxModelStateLiteral(from), muxModelStateLiteral(named.state))
			}
			if got != row.next || muxModelEffectsLiteral(effects) != muxModelEffectsLiteral(row.effects) {
				t.Errorf("transition differs from the table\n got: %s\nwant: %s\neffect log: %#v",
					muxModelRowLiteral(row.state, row.event, got, effects),
					muxModelRowLiteral(row.state, row.event, row.next, row.effects), raw)
			}
		})
	}
}

// TestMuxStateModelResultDecisions checks the read loop's STARTED and FAILED
// decisions of decideMuxTransition against every result row of the table:
// the winner byte reaches the passive observers exactly when the row emits a
// passive symbol, no session receives anything when the decision neither
// delivers the winner nor resets the local losers (whose reset delivers a
// session SYN), and the sessions receive it when it is delivered to all.
// Whether applying the result failed the bidder does not change the passive
// decision, so both outcomes must agree on it.
func TestMuxStateModelResultDecisions(t *testing.T) {
	checked := 0
	for _, row := range muxModelRows {
		if row.outcome != muxModelTransition {
			continue
		}
		switch row.event {
		case muxModelEventStartedMatching, muxModelEventStartedOther, muxModelEventFailed, muxModelEventFailedPhantom:
		default:
			continue
		}
		checked++
		state := muxModelStates[row.state].state
		in := muxModelEventInput(row.event, state)
		applied := in
		applied.kind = muxEventStartedApplied
		if in.kind == muxEventFailed {
			applied.kind = muxEventFailedApplied
		}
		var decisions [2]muxTransition
		for i, failed := range []bool{false, true} {
			applied.bidderFailed = failed
			decisions[i] = decideMuxTransition(state, applied)
		}
		key := row.state + "/" + row.event.String()
		if decisions[0].passive != decisions[1].passive {
			t.Errorf("%s: the passive decision depends on the bidder outcome", key)
			continue
		}
		has := map[muxModelEffect]bool{}
		for _, e := range row.effects {
			has[e] = true
		}
		if decisions[0].passive != has[muxModelEffectPassiveSymbol] {
			t.Errorf("%s: decision passive=%v, the row's effects are %s", key, decisions[0].passive, muxModelEffectsLiteral(row.effects))
		}
		quiet := func(d muxTransition) bool { return d.deliver == muxDeliverNone && !d.resetLosers }
		if quiet(decisions[0]) && quiet(decisions[1]) && has[muxModelEffectSessionReceived] {
			t.Errorf("%s: decision delivers the winner to no session, the row's effects are %s", key, muxModelEffectsLiteral(row.effects))
		}
		if decisions[0].deliver == muxDeliverAll && decisions[1].deliver == muxDeliverAll && !has[muxModelEffectSessionReceived] {
			t.Errorf("%s: decision delivers the winner to every session, the row's effects are %s", key, muxModelEffectsLiteral(row.effects))
		}
	}
	if checked == 0 {
		t.Fatal("no result row checked")
	}
}

// muxModelReachedStates are the states the table's transitions reach.
var muxModelReachedStates = map[string]muxModelNamedState{
	"bids_queued_absorb_pending_bids_2_retired_31":                                                muxModelReach("absorb_pending", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"bids_queued_absorb_pending_bids_2_retired_31_enforce":                                        muxModelReach("idle_absorb_pending_retired_31_enforce", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"bids_queued_absorb_pending_bids_2_retired_71":                                                muxModelReach("idle_absorb_pending_retired_71", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"bids_queued_absorb_pending_bids_2_retired_71_enforce":                                        muxModelReach("idle_absorb_pending_retired_71_enforce", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"bids_queued_absorb_pending_bids_2_retired_7f":                                                muxModelReach("fenced_at_syn_absorb_pending_bids_2_retired_7f", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}),
	"bids_queued_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream": muxModelReach("fenced_at_syn_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}),
	"bids_queued_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream":                muxModelReach("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"bids_queued_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream":                muxModelReach("idle_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"bids_queued_absorb_pending_resync_owed_1_bids_2_retired_31":                                  muxModelReach("fenced_at_syn_absorb_pending_resync_owed_2_bids_2_retired_31", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"bids_queued_bids_1_2":                                                                          muxModelReach("bids_queued", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(1, 2)}),
	"bids_queued_bids_2":                                                                            muxModelReach("idle", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	"bids_queued_bids_2_blocking_upstream":                                                          muxModelReach("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	"bids_queued_bids_2_enforce":                                                                    muxModelReach("idle_enforce", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	"bids_queued_bids_2_retired_31":                                                                 muxModelReach("bids_queued_absorb_pending_bids_2_retired_31", muxModelEventAbsorbDebounce, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"bids_queued_bids_2_retired_31_blocking_upstream":                                               muxModelReach("bids_queued_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"bids_queued_bids_2_retired_31_enforce":                                                         muxModelReach("idle_retired_31_enforce", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"bids_queued_bids_2_retired_71":                                                                 muxModelReach("idle_retired_71", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"bids_queued_bids_2_retired_71_blocking_upstream":                                               muxModelReach("idle_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"bids_queued_bids_2_retired_71_enforce":                                                         muxModelReach("idle_retired_71_enforce", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"bids_queued_bids_2_retired_7f":                                                                 muxModelReach("idle_retired_7f", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}),
	"bids_queued_bids_2_telegram_live_enforce":                                                      muxModelReach("session_owned_owner_1_bids_2_telegram_live_enforce", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, bids: muxModelBids(2), telegramLive: true}),
	"bids_queued_blocking_call_bids_2_blocking_upstream":                                            muxModelReach("session_owned_owner_1_blocking_call_bids_2_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2)}),
	"bids_queued_blocking_call_bids_2_deferred_loser_blocking_upstream":                             muxModelReach("session_owned_owner_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true}),
	"bids_queued_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream":                  muxModelReach("fenced_at_syn_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}),
	"bids_queued_blocking_call_bids_2_retired_31_blocking_upstream":                                 muxModelReach("fenced_at_syn_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"bids_queued_blocking_call_bids_2_retired_71_blocking_upstream":                                 muxModelReach("idle_blocking_call_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"bids_queued_blocking_call_resync_owed_1_bids_2_blocking_upstream":                              muxModelReach("session_owned_owner_2_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, blocking: true, resyncOwed: 1, bids: muxModelBids(2)}),
	"bids_queued_held_bids_1_2":                                                                     muxModelReach("held", muxModelEventBid, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(1, 2)}),
	"bids_queued_held_bids_2":                                                                       muxModelReach("bids_queued_held_bids_1_2", muxModelEventSessionClose, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(2)}),
	"bids_queued_reset_bids_2":                                                                      muxModelReach("reset_recovery", muxModelEventBid, muxState{phase: muxBidsQueued, recovery: arbitrationRecoveryReset, bids: muxModelBids(2)}),
	"bids_queued_reset_bids_2_blocking_upstream":                                                    muxModelReach("idle_reset_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, recovery: arbitrationRecoveryReset, bids: muxModelBids(2)}),
	"bids_queued_reset_bids_2_enforce":                                                              muxModelReach("idle_reset_enforce", muxModelEventBid, muxState{phase: muxBidsQueued, recovery: arbitrationRecoveryReset, bids: muxModelBids(2)}),
	"bids_queued_resync_owed_1_bids_2":                                                              muxModelReach("idle_resync_owed_1", muxModelEventBid, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}),
	"bids_queued_resync_owed_1_bids_2_blocking_upstream":                                            muxModelReach("idle_resync_owed_1_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}),
	"bids_queued_resync_owed_1_bids_2_retired_31":                                                   muxModelReach("idle_resync_owed_1_retired_31", muxModelEventBid, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"bids_queued_resync_owed_1_bids_2_retired_31_blocking_upstream":                                 muxModelReach("idle_resync_owed_1_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"bids_queued_resync_owed_2_bids_2":                                                              muxModelReach("session_owned_owner_1_resync_owed_2_bids_2", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}),
	"bids_queued_resync_owed_2_bids_2_blocking_upstream":                                            muxModelReach("session_owned_owner_1_resync_owed_2_bids_2_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}),
	"fenced_at_syn_absorb_pending_bids_2_retired_31":                                                muxModelReach("fenced_absorb_pending", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"fenced_at_syn_absorb_pending_bids_2_retired_71":                                                muxModelReach("fenced_at_syn_absorb_pending_retired_71", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"fenced_at_syn_absorb_pending_bids_2_retired_71_enforce":                                        muxModelReach("fenced_at_syn_absorb_pending_retired_71_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"fenced_at_syn_absorb_pending_bids_2_retired_7f":                                                muxModelReach("start_async_selected_gateway_bids_2", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}),
	"fenced_at_syn_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream": muxModelReach("start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}),
	"fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream":                muxModelReach("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream":                muxModelReach("fenced_at_syn_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"fenced_at_syn_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream":        muxModelReach("start_blocking_deferred_loser", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, deferred: true, retired: true, retiredSource: 0x31}),
	"fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream":  muxModelReach("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_retired_71_blocking_upstream":         muxModelReach("start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	"fenced_at_syn_absorb_pending_blocking_call_retired_31_blocking_upstream":                       muxModelReach("start_blocking", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, retired: true, retiredSource: 0x31}),
	"fenced_at_syn_absorb_pending_blocking_call_retired_71_blocking_upstream":                       muxModelReach("start_blocking_selected_session_2_blocking_call_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, retired: true, retiredSource: 0x71}),
	"fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_31":                                  muxModelReach("start_async_selected_session_1_resync_owed_1_bids_2", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_71":                                  muxModelReach("fenced_at_syn_absorb_pending_resync_owed_1_retired_71", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"fenced_at_syn_absorb_pending_resync_owed_1_retired_31":                                         muxModelReach("start_async_selected_session_1_resync_owed_1", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	"fenced_at_syn_absorb_pending_resync_owed_1_retired_71":                                         muxModelReach("start_async_selected_session_2_resync_owed_1", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	"fenced_at_syn_absorb_pending_resync_owed_2_bids_2_retired_31":                                  muxModelReach("start_async_selected_session_1_resync_owed_2_bids_2", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"fenced_at_syn_absorb_pending_resync_owed_2_retired_31":                                         muxModelReach("start_async_resync_owed", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 2, retired: true, retiredSource: 0x31}),
	"fenced_at_syn_absorb_pending_retired_71":                                                       muxModelReach("start_async_selected_session_2", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x71}),
	"fenced_at_syn_absorb_pending_retired_71_enforce":                                               muxModelReach("start_async_selected_session_2_enforce", muxModelEventStartDeadline, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x71}),
	"fenced_at_syn_absorb_pending_retired_7f":                                                       muxModelReach("start_gateway", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x7F}),
	"fenced_at_syn_bids_2_retired_31":                                                               muxModelReach("fenced", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"fenced_at_syn_bids_2_retired_31_blocking_upstream":                                             muxModelReach("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"fenced_at_syn_bids_2_retired_31_enforce":                                                       muxModelReach("bids_queued_bids_2_retired_31_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"fenced_at_syn_bids_2_retired_31_telegram_live_enforce":                                         muxModelReach("session_owned_owner_1_bids_2_telegram_live_enforce", muxModelEventIdleGrace, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}),
	"fenced_at_syn_bids_2_retired_71":                                                               muxModelReach("session_owned_owner_2_bids_2", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"fenced_at_syn_bids_2_retired_71_blocking_upstream":                                             muxModelReach("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"fenced_at_syn_bids_2_retired_71_enforce":                                                       muxModelReach("fenced_at_syn_retired_71_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"fenced_at_syn_bids_2_retired_7f":                                                               muxModelReach("start_async_selected_gateway_bids_2", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}),
	"fenced_at_syn_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream":                muxModelReach("start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}),
	"fenced_at_syn_blocking_call_bids_2_retired_31_blocking_upstream":                               muxModelReach("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"fenced_at_syn_blocking_call_bids_2_retired_71_blocking_upstream":                               muxModelReach("fenced_at_syn_blocking_call_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"fenced_at_syn_blocking_call_deferred_loser_retired_31_blocking_upstream":                       muxModelReach("start_blocking_deferred_loser", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}),
	"fenced_at_syn_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream":                 muxModelReach("fenced_at_syn_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"fenced_at_syn_blocking_call_resync_owed_1_retired_71_blocking_upstream":                        muxModelReach("start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	"fenced_at_syn_blocking_call_retired_31_blocking_upstream":                                      muxModelReach("start_blocking", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}),
	"fenced_at_syn_blocking_call_retired_71_blocking_upstream":                                      muxModelReach("start_blocking_selected_session_2_blocking_call_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}),
	"fenced_at_syn_resync_owed_1_bids_2_retired_31":                                                 muxModelReach("start_async_selected_session_1_resync_owed_1_bids_2", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"fenced_at_syn_resync_owed_1_bids_2_retired_31_blocking_upstream":                               muxModelReach("session_owned_owner_1_resync_owed_1_bids_2_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"fenced_at_syn_resync_owed_1_bids_2_retired_71":                                                 muxModelReach("fenced_at_syn_resync_owed_1_retired_71", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"fenced_at_syn_resync_owed_1_bids_2_retired_71_blocking_upstream":                               muxModelReach("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"fenced_at_syn_resync_owed_1_retired_31":                                                        muxModelReach("start_async_selected_session_1_resync_owed_1", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	"fenced_at_syn_resync_owed_1_retired_31_blocking_upstream":                                      muxModelReach("session_owned_owner_1_resync_owed_1_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	"fenced_at_syn_resync_owed_1_retired_71":                                                        muxModelReach("start_async_selected_session_2_resync_owed_1", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	"fenced_at_syn_resync_owed_1_retired_71_blocking_upstream":                                      muxModelReach("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	"fenced_at_syn_resync_owed_2_bids_2_retired_31":                                                 muxModelReach("start_async_selected_session_1_resync_owed_2_bids_2", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"fenced_at_syn_resync_owed_2_bids_2_retired_31_blocking_upstream":                               muxModelReach("fenced_at_syn_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"fenced_at_syn_resync_owed_2_retired_31":                                                        muxModelReach("start_async_resync_owed", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}),
	"fenced_at_syn_resync_owed_2_retired_31_blocking_upstream":                                      muxModelReach("fenced_at_syn_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}),
	"fenced_at_syn_retired_31_blocking_upstream":                                                    muxModelReach("fenced_at_syn_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	"fenced_at_syn_retired_31_enforce":                                                              muxModelReach("idle_retired_31_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	"fenced_at_syn_retired_31_telegram_live_enforce":                                                muxModelReach("session_telegram_live", muxModelEventIdleGrace, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31, telegramLive: true}),
	"fenced_at_syn_retired_71":                                                                      muxModelReach("session_owned_other", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	"fenced_at_syn_retired_71_blocking_upstream":                                                    muxModelReach("fenced_at_syn_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	"fenced_at_syn_retired_71_enforce":                                                              muxModelReach("start_async_selected_session_2_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	"fenced_at_syn_retired_7f":                                                                      muxModelReach("start_gateway", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}),
	"fenced_foreign_absorb_pending_bids_2_retired_31_enforce":                                       muxModelReach("fenced_foreign_absorb_pending", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"fenced_foreign_absorb_pending_bids_2_retired_71_enforce":                                       muxModelReach("fenced_foreign_absorb_pending_retired_71_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"fenced_foreign_absorb_pending_retired_71_enforce":                                              muxModelReach("start_async_selected_session_2_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, retired: true, retiredSource: 0x71}),
	"fenced_foreign_after_syn_absorb_pending_bids_2_retired_31_enforce":                             muxModelReach("fenced_foreign_after_syn_absorb_pending_retired_31_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"fenced_foreign_after_syn_absorb_pending_bids_2_retired_71_enforce":                             muxModelReach("fenced_foreign_after_syn_absorb_pending_retired_71_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"fenced_foreign_after_syn_absorb_pending_retired_31_enforce":                                    muxModelReach("fenced_foreign_absorb_pending", muxModelEventSYN, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, absorb: 1, retired: true, retiredSource: 0x31}),
	"fenced_foreign_after_syn_absorb_pending_retired_71_enforce":                                    muxModelReach("fenced_foreign_absorb_pending_retired_71_enforce", muxModelEventSYN, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, absorb: 1, retired: true, retiredSource: 0x71}),
	"fenced_foreign_after_syn_bids_2_retired_31_enforce":                                            muxModelReach("fenced_foreign_after_syn_retired_31_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"fenced_foreign_after_syn_bids_2_retired_31_telegram_live_enforce":                              muxModelReach("fenced_at_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventSYN, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}),
	"fenced_foreign_after_syn_bids_2_retired_71_enforce":                                            muxModelReach("fenced_foreign_after_syn_retired_71_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"fenced_foreign_after_syn_retired_31_enforce":                                                   muxModelReach("fenced_foreign", muxModelEventSYN, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, retired: true, retiredSource: 0x31}),
	"fenced_foreign_after_syn_retired_31_telegram_live_enforce":                                     muxModelReach("fenced_at_syn_retired_31_telegram_live_enforce", muxModelEventSYN, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, retired: true, retiredSource: 0x31, telegramLive: true}),
	"fenced_foreign_after_syn_retired_71_enforce":                                                   muxModelReach("fenced_foreign_retired_71_enforce", muxModelEventSYN, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, retired: true, retiredSource: 0x71}),
	"fenced_foreign_bids_2_retired_31_enforce":                                                      muxModelReach("fenced_foreign", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"fenced_foreign_bids_2_retired_31_telegram_live_enforce":                                        muxModelReach("fenced_at_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}),
	"fenced_foreign_bids_2_retired_71_enforce":                                                      muxModelReach("fenced_foreign_retired_71_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"fenced_foreign_retired_31_telegram_live_enforce":                                               muxModelReach("fenced_at_syn_retired_31_telegram_live_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31, telegramLive: true}),
	"fenced_foreign_retired_71_enforce":                                                             muxModelReach("start_async_selected_session_2_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}),
	"fenced_no_recovery":                                                                            muxModelReach("gateway_grace", muxModelEventInitiatorSymbol, muxState{phase: muxFenced}),
	"fenced_no_recovery_bids_2":                                                                     muxModelReach("gateway_grace_bids_2", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, bids: muxModelBids(2)}),
	"fenced_no_recovery_resync_owed_2":                                                              muxModelReach("gateway_awaiting_echo", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, resyncOwed: 2}),
	"fenced_no_recovery_resync_owed_2_bids_2":                                                       muxModelReach("gateway_transaction_bids_2_awaiting_echo", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, resyncOwed: 2, bids: muxModelBids(2)}),
	"gateway_grace_bids_2":                                                                          muxModelReach("gateway_grace", muxModelEventBid, muxState{phase: muxGatewayGrace, bids: muxModelBids(2)}),
	"gateway_transaction_bids_2":                                                                    muxModelReach("gateway_transaction", muxModelEventBid, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2)}),
	"gateway_transaction_bids_2_awaiting_echo":                                                      muxModelReach("gateway_awaiting_echo", muxModelEventBid, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2), echoPending: true}),
	"gateway_transaction_bids_2_awaiting_terminator_echo":                                           muxModelReach("gateway_awaiting_terminator_echo", muxModelEventBid, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2), echoPending: true, echoTerminator: true}),
	"idle_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream":                 muxModelReach("fenced_at_syn_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, blocking: true, deferred: true, retired: true, retiredSource: 0x31}),
	"idle_absorb_pending_blocking_call_retired_31_blocking_upstream":                                muxModelReach("fenced_at_syn_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, blocking: true, retired: true, retiredSource: 0x31}),
	"idle_absorb_pending_blocking_call_retired_71_blocking_upstream":                                muxModelReach("fenced_at_syn_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, blocking: true, retired: true, retiredSource: 0x71}),
	"idle_absorb_pending_resync_owed_1_retired_31":                                                  muxModelReach("fenced_at_syn_absorb_pending_resync_owed_2_retired_31", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	"idle_absorb_pending_retired_31_enforce":                                                        muxModelReach("fenced_foreign_after_syn_absorb_pending_retired_31_enforce", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x31}),
	"idle_absorb_pending_retired_71":                                                                muxModelReach("fenced_at_syn_absorb_pending_retired_71", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x71}),
	"idle_absorb_pending_retired_71_enforce":                                                        muxModelReach("fenced_at_syn_absorb_pending_retired_71_enforce", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x71}),
	"idle_absorb_pending_retired_7f":                                                                muxModelReach("fenced_at_syn_absorb_pending_retired_7f", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x7F}),
	"idle_blocking_call_blocking_upstream":                                                          muxModelReach("session_owned_owner_1_blocking_call_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle, blocking: true}),
	"idle_blocking_call_deferred_loser_blocking_upstream":                                           muxModelReach("session_owned_owner_1_blocking_call_deferred_loser_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle, blocking: true, deferred: true}),
	"idle_blocking_call_deferred_loser_retired_31_blocking_upstream":                                muxModelReach("fenced_at_syn_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, blocking: true, deferred: true, retired: true, retiredSource: 0x31}),
	"idle_blocking_call_resync_owed_1_blocking_upstream":                                            muxModelReach("session_owned_owner_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxIdle, blocking: true, resyncOwed: 1}),
	"idle_blocking_call_retired_31_blocking_upstream":                                               muxModelReach("fenced_at_syn_blocking_call_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, blocking: true, retired: true, retiredSource: 0x31}),
	"idle_blocking_call_retired_71_blocking_upstream":                                               muxModelReach("fenced_at_syn_blocking_call_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, blocking: true, retired: true, retiredSource: 0x71}),
	"idle_blocking_upstream":                                                                        muxModelReach("start_blocking", muxModelEventStartCallFails, muxState{phase: muxIdle}),
	"idle_enforce":                                                                                  muxModelReach("idle_reset_enforce", muxModelEventSYN, muxState{phase: muxIdle}),
	"idle_held":                                                                                     muxModelReach("held", muxModelEventSessionClose, muxState{phase: muxIdle, held: true}),
	"idle_reset_blocking_upstream":                                                                  muxModelReach("start_blocking", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}),
	"idle_reset_enforce":                                                                            muxModelReach("fenced_foreign", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}),
	"idle_resync_owed_1":                                                                            muxModelReach("fenced_no_recovery_resync_owed_2", muxModelEventSYN, muxState{phase: muxIdle, resyncOwed: 1}),
	"idle_resync_owed_1_blocking_upstream":                                                          muxModelReach("session_owned_owner_1_resync_owed_2_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle, resyncOwed: 1}),
	"idle_resync_owed_1_retired_31":                                                                 muxModelReach("fenced_at_syn_resync_owed_2_retired_31", muxModelEventSYN, muxState{phase: muxIdle, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	"idle_resync_owed_1_retired_31_blocking_upstream":                                               muxModelReach("session_owned_at_syn_owner_1_resync_owed_2_retired_31_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	"idle_resync_owed_2":                                                                            muxModelReach("session_owned_owner_1_resync_owed_2", muxModelEventOwnershipLimit, muxState{phase: muxIdle, resyncOwed: 2}),
	"idle_resync_owed_2_blocking_upstream":                                                          muxModelReach("session_owned_owner_1_resync_owed_2_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxIdle, resyncOwed: 2}),
	"idle_retired_31":                                                                               muxModelReach("absorb_pending", muxModelEventAbsorbDebounce, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}),
	"idle_retired_31_blocking_upstream":                                                             muxModelReach("session_owned_at_syn_owner_1_retired_31_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}),
	"idle_retired_31_enforce":                                                                       muxModelReach("fenced_foreign_after_syn_retired_31_enforce", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}),
	"idle_retired_71":                                                                               muxModelReach("fenced_at_syn_retired_71", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}),
	"idle_retired_71_blocking_upstream":                                                             muxModelReach("session_owned_at_syn_owner_2_retired_71_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}),
	"idle_retired_71_enforce":                                                                       muxModelReach("fenced_at_syn_retired_71_enforce", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}),
	"idle_retired_7f":                                                                               muxModelReach("fenced_at_syn_retired_7f", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x7F}),
	"idle_telegram_live_enforce":                                                                    muxModelReach("session_telegram_live", muxModelEventOwnershipLimit, muxState{phase: muxIdle, telegramLive: true}),
	"session_owned_at_syn_owner_1_bids_2_retired_31_blocking_upstream":                              muxModelReach("session_owned_owner_1_blocking_call_bids_2_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"session_owned_at_syn_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream":                muxModelReach("session_owned_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"session_owned_at_syn_owner_1_resync_owed_1_retired_31_blocking_upstream":                       muxModelReach("session_owned_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, retired: true, retiredSource: 0x31}),
	"session_owned_at_syn_owner_1_resync_owed_2_bids_2_retired_31_blocking_upstream":                muxModelReach("session_owned_owner_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"session_owned_at_syn_owner_1_resync_owed_2_retired_31_blocking_upstream":                       muxModelReach("session_owned_owner_1_blocking_call_deferred_loser_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, owner: 1, retired: true, retiredSource: 0x31}),
	"session_owned_at_syn_owner_1_retired_31_blocking_upstream":                                     muxModelReach("session_owned_owner_1_blocking_call_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, retired: true, retiredSource: 0x31}),
	"session_owned_at_syn_owner_2_bids_2_retired_71_blocking_upstream":                              muxModelReach("session_owned_owner_2_blocking_call_bids_2_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"session_owned_at_syn_owner_2_resync_owed_1_bids_2_retired_71_blocking_upstream":                muxModelReach("session_owned_owner_2_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"session_owned_at_syn_owner_2_resync_owed_1_retired_71_blocking_upstream":                       muxModelReach("session_owned_owner_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 2, retired: true, retiredSource: 0x71}),
	"session_owned_at_syn_owner_2_retired_71_blocking_upstream":                                     muxModelReach("session_owned_owner_2_blocking_call_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, retired: true, retiredSource: 0x71}),
	"session_owned_owner_1_bids_2":                                                                  muxModelReach("session_owned", muxModelEventBid, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}),
	"session_owned_owner_1_bids_2_blocking_upstream":                                                muxModelReach("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}),
	"session_owned_owner_1_bids_2_retired_31_blocking_upstream":                                     muxModelReach("session_owned_at_syn_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"session_owned_owner_1_bids_2_telegram_live_enforce":                                            muxModelReach("session_telegram_live", muxModelEventBid, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2), telegramLive: true}),
	"session_owned_owner_1_blocking_call_bids_2_blocking_upstream":                                  muxModelReach("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2)}),
	"session_owned_owner_1_blocking_call_bids_2_deferred_loser_blocking_upstream":                   muxModelReach("start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2), deferred: true}),
	"session_owned_owner_1_blocking_call_blocking_upstream":                                         muxModelReach("start_blocking", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, blocking: true, owner: 1}),
	"session_owned_owner_1_blocking_call_deferred_loser_blocking_upstream":                          muxModelReach("start_blocking_deferred_loser", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, blocking: true, owner: 1, deferred: true}),
	"session_owned_owner_1_blocking_upstream":                                                       muxModelReach("start_blocking", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, owner: 1}),
	"session_owned_owner_1_resync_owed_1":                                                           muxModelReach("start_async_selected_session_1_resync_owed_1", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1}),
	"session_owned_owner_1_resync_owed_1_bids_2":                                                    muxModelReach("start_async_selected_session_1_resync_owed_1_bids_2", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2)}),
	"session_owned_owner_1_resync_owed_1_bids_2_blocking_upstream":                                  muxModelReach("session_owned_owner_1_resync_owed_2_bids_2_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2)}),
	"session_owned_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream":                       muxModelReach("session_owned_at_syn_owner_1_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	"session_owned_owner_1_resync_owed_1_blocking_upstream":                                         muxModelReach("session_owned_owner_1_resync_owed_2_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1}),
	"session_owned_owner_1_resync_owed_1_retired_31_blocking_upstream":                              muxModelReach("session_owned_at_syn_owner_1_resync_owed_2_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, retired: true, retiredSource: 0x31}),
	"session_owned_owner_1_resync_owed_2":                                                           muxModelReach("start_async_resync_owed", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1}),
	"session_owned_owner_1_resync_owed_2_bids_2":                                                    muxModelReach("start_async_selected_session_1_resync_owed_2_bids_2", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1, bids: muxModelBids(2)}),
	"session_owned_owner_1_resync_owed_2_bids_2_blocking_upstream":                                  muxModelReach("start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1, bids: muxModelBids(2)}),
	"session_owned_owner_1_resync_owed_2_blocking_upstream":                                         muxModelReach("start_blocking_deferred_loser", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1}),
	"session_owned_owner_1_retired_31_blocking_upstream":                                            muxModelReach("session_owned_at_syn_owner_1_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 1, retired: true, retiredSource: 0x31}),
	"session_owned_owner_2_bids_2":                                                                  muxModelReach("session_owned_other", muxModelEventBid, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}),
	"session_owned_owner_2_bids_2_blocking_upstream":                                                muxModelReach("session_owned_owner_2_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}),
	"session_owned_owner_2_bids_2_enforce":                                                          muxModelReach("session_owned_owner_2_enforce", muxModelEventBid, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}),
	"session_owned_owner_2_bids_2_retired_71_blocking_upstream":                                     muxModelReach("session_owned_at_syn_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	"session_owned_owner_2_blocking_call_bids_2_blocking_upstream":                                  muxModelReach("session_owned_owner_2_blocking_call_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, blocking: true, owner: 2, bids: muxModelBids(2)}),
	"session_owned_owner_2_blocking_call_blocking_upstream":                                         muxModelReach("start_blocking_selected_session_2_blocking_call_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, blocking: true, owner: 2}),
	"session_owned_owner_2_blocking_call_resync_owed_1_bids_2_blocking_upstream":                    muxModelReach("session_owned_owner_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, blocking: true, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}),
	"session_owned_owner_2_blocking_call_resync_owed_1_blocking_upstream":                           muxModelReach("start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, blocking: true, resyncOwed: 1, owner: 2}),
	"session_owned_owner_2_blocking_upstream":                                                       muxModelReach("start_blocking_selected_session_2_blocking_call_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, owner: 2}),
	"session_owned_owner_2_enforce":                                                                 muxModelReach("start_async_selected_session_2_enforce", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, owner: 2}),
	"session_owned_owner_2_resync_owed_1":                                                           muxModelReach("start_async_selected_session_2_resync_owed_1", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2}),
	"session_owned_owner_2_resync_owed_1_bids_2":                                                    muxModelReach("session_owned_owner_2_resync_owed_1", muxModelEventBid, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}),
	"session_owned_owner_2_resync_owed_1_bids_2_blocking_upstream":                                  muxModelReach("session_owned_owner_2_resync_owed_1_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}),
	"session_owned_owner_2_resync_owed_1_blocking_upstream":                                         muxModelReach("start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2}),
	"session_owned_owner_2_retired_71_blocking_upstream":                                            muxModelReach("session_owned_at_syn_owner_2_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 2, retired: true, retiredSource: 0x71}),
	"start_async_selected_gateway_bids_2":                                                           muxModelReach("start_gateway", muxModelEventBid, muxState{phase: muxStartAsync, start: muxModelSelected(gatewaySessionID, 0x7F), bids: muxModelBids(2)}),
	"start_async_selected_session_1_bids_2":                                                         muxModelReach("start_async", muxModelEventBid, muxState{phase: muxStartAsync, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}),
	"start_async_selected_session_1_resync_owed_1":                                                  muxModelReach("start_async_resync_owed", muxModelEventSYN, muxState{phase: muxStartAsync, resyncOwed: 1, start: muxModelSelected(1, 0x31)}),
	"start_async_selected_session_1_resync_owed_1_bids_2":                                           muxModelReach("start_async_selected_session_1_resync_owed_1", muxModelEventBid, muxState{phase: muxStartAsync, resyncOwed: 1, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}),
	"start_async_selected_session_1_resync_owed_2_bids_2":                                           muxModelReach("start_async_resync_owed", muxModelEventBid, muxState{phase: muxStartAsync, resyncOwed: 2, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}),
	"start_async_selected_session_2":                                                                muxModelReach("fenced_at_syn_bids_2_retired_31", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}),
	"start_async_selected_session_2_enforce":                                                        muxModelReach("fenced_foreign_after_syn_bids_2_retired_31_enforce", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}),
	"start_async_selected_session_2_resync_owed_1":                                                  muxModelReach("fenced_no_recovery_resync_owed_2_bids_2", muxModelEventSYN, muxState{phase: muxStartAsync, resyncOwed: 1, start: muxModelSelected(2, 0x71)}),
	"start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream":                      muxModelReach("start_blocking", muxModelEventBid, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}),
	"start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream":       muxModelReach("start_blocking_deferred_loser", muxModelEventBid, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(1, 0x31), bids: muxModelBids(2), deferred: true}),
	"start_blocking_selected_session_2_blocking_call_blocking_upstream":                             muxModelReach("session_owned_owner_1_bids_2_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}),
	"start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream":               muxModelReach("session_owned_owner_1_resync_owed_2_bids_2_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxStartBlocking, blocking: true, resyncOwed: 1, start: muxModelSelected(2, 0x71)}),
}

// muxModelRows is the transition table: one row per named state and event.
var muxModelRows = []muxModelRow{
	muxModelOn("absorb_pending", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("absorb_pending", muxModelEventSymbol, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("absorb_pending", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("absorb_pending", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("absorb_pending", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("absorb_pending", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("absorb_pending", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("absorb_pending", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("absorb_pending", muxModelEventAbsorbDebounce, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}),
	muxModelNever("absorb_pending", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("absorb_pending", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("absorb_pending", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("absorb_pending", muxModelEventSessionClose, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("absorb_pending", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("absorb_pending", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("absorb_pending", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("absorb_pending", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(1, 0x31)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("bids_queued", muxModelEventSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(1)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(1, 2)}),
	muxModelOn("bids_queued", muxModelEventStartedMatching, muxState{phase: muxBidsQueued, bids: muxModelBids(1)}),
	muxModelOn("bids_queued", muxModelEventStartedOther, muxState{phase: muxBidsQueued, bids: muxModelBids(1)}),
	muxModelOn("bids_queued", muxModelEventFailed, muxState{phase: muxBidsQueued, bids: muxModelBids(1)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued", muxModelEventFailedPhantom, muxState{phase: muxBidsQueued, bids: muxModelBids(1)}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued", muxModelEventSessionClose, muxState{phase: muxIdle}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(1)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31", muxModelEventSymbol, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_31", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31", muxModelEventAbsorbDebounce, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_31", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_31", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_31", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31", muxModelEventSessionClose, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_31", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_31", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31_enforce", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31_enforce", muxModelEventSymbol, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31_enforce", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_31_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31_enforce", muxModelEventAbsorbDebounce, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_31_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_31_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_31_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31_enforce", muxModelEventSessionClose, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_31_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_31_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_31_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71", muxModelEventSymbol, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_71", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71", muxModelEventAbsorbDebounce, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_71", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_71", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_71", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71", muxModelEventSessionClose, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_71", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_71", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71_enforce", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71_enforce", muxModelEventSymbol, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71_enforce", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_71_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71_enforce", muxModelEventAbsorbDebounce, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_71_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_71_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_71_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71_enforce", muxModelEventSessionClose, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_71_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_71_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_71_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_absorb_pending_bids_2_retired_7f", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_7f", muxModelEventSymbol, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_7f", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_7f", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_7f", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_7f", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_7f", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_7f", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_7f", muxModelEventAbsorbDebounce, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_7f", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_7f", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_7f", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_7f", muxModelEventSessionClose, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_7f", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_7f", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_absorb_pending_bids_2_retired_7f", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_absorb_pending_bids_2_retired_7f", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}),
	muxModelNever("bids_queued_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultLost),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultError),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelNever("bids_queued_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelNever("bids_queued_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("bids_queued_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventSymbol, muxState{phase: muxBidsQueued, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("bids_queued_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventAbsorbDebounce, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelNever("bids_queued_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventSessionClose, muxState{phase: muxBidsQueued, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_bids_1_2", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(1, 0x31)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived, muxModelEffectSessionFailed, muxModelEffectResultLost),
	muxModelOn("bids_queued_bids_1_2", muxModelEventSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(1, 2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_1_2", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(1, 2)}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_bids_1_2", muxModelEventStartedMatching, muxState{phase: muxBidsQueued, bids: muxModelBids(1, 2)}),
	muxModelOn("bids_queued_bids_1_2", muxModelEventStartedOther, muxState{phase: muxBidsQueued, bids: muxModelBids(1, 2)}),
	muxModelOn("bids_queued_bids_1_2", muxModelEventFailed, muxState{phase: muxBidsQueued, bids: muxModelBids(1, 2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_1_2", muxModelEventFailedPhantom, muxState{phase: muxBidsQueued, bids: muxModelBids(1, 2)}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_bids_1_2", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_bids_1_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_bids_1_2", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_1_2", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_1_2", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_bids_1_2", muxModelEventSessionClose, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_bids_1_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_bids_1_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_bids_1_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_bids_1_2", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(1, 2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_bids_2", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2", muxModelEventSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_bids_2", muxModelEventStartedMatching, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_bids_2", muxModelEventStartedOther, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_bids_2", muxModelEventFailed, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2", muxModelEventFailedPhantom, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_bids_2", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_bids_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_bids_2", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_bids_2", muxModelEventSessionClose, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_bids_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_bids_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_bids_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_bids_2", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_bids_2_blocking_upstream", muxModelEventSYN, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_blocking_upstream", muxModelEventSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_bids_2_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_bids_2_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_bids_2_blocking_upstream", muxModelEventFailed, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_bids_2_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_bids_2_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_bids_2_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_bids_2_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_bids_2_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_bids_2_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_bids_2_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_bids_2_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_bids_2_enforce", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_enforce", muxModelEventSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_enforce", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_bids_2_enforce", muxModelEventStartedMatching, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_bids_2_enforce", muxModelEventStartedOther, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_bids_2_enforce", muxModelEventFailed, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_enforce", muxModelEventFailedPhantom, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_bids_2_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_bids_2_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_bids_2_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_bids_2_enforce", muxModelEventSessionClose, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_bids_2_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_bids_2_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_bids_2_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_bids_2_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_bids_2_retired_31", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_31", muxModelEventSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_31", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_bids_2_retired_31", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_bids_2_retired_31", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_31", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_31", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_bids_2_retired_31", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_bids_2_retired_31", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_bids_2_retired_31", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2_retired_31", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2_retired_31", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_bids_2_retired_31", muxModelEventSessionClose, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_bids_2_retired_31", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_bids_2_retired_31", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_bids_2_retired_31", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_bids_2_retired_31", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_bids_2_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_bids_2_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_bids_2_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_bids_2_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_bids_2_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_bids_2_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_bids_2_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_bids_2_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_bids_2_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_bids_2_retired_31_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_bids_2_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_bids_2_retired_31_enforce", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_31_enforce", muxModelEventSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_31_enforce", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_bids_2_retired_31_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_bids_2_retired_31_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_31_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_31_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_bids_2_retired_31_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_bids_2_retired_31_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_bids_2_retired_31_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2_retired_31_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2_retired_31_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_bids_2_retired_31_enforce", muxModelEventSessionClose, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_bids_2_retired_31_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_bids_2_retired_31_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_bids_2_retired_31_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_bids_2_retired_31_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_bids_2_retired_71", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_71", muxModelEventSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_71", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_bids_2_retired_71", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("bids_queued_bids_2_retired_71", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_71", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_71", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_bids_2_retired_71", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_bids_2_retired_71", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_bids_2_retired_71", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2_retired_71", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2_retired_71", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_bids_2_retired_71", muxModelEventSessionClose, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("bids_queued_bids_2_retired_71", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_bids_2_retired_71", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_bids_2_retired_71", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_bids_2_retired_71", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_bids_2_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_bids_2_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("bids_queued_bids_2_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_bids_2_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_bids_2_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_bids_2_retired_71_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_bids_2_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("bids_queued_bids_2_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_bids_2_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_bids_2_retired_71_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_bids_2_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_bids_2_retired_71_enforce", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_71_enforce", muxModelEventSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_71_enforce", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_bids_2_retired_71_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("bids_queued_bids_2_retired_71_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_71_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_71_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_bids_2_retired_71_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_bids_2_retired_71_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_bids_2_retired_71_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2_retired_71_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2_retired_71_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_bids_2_retired_71_enforce", muxModelEventSessionClose, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("bids_queued_bids_2_retired_71_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_bids_2_retired_71_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_bids_2_retired_71_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_bids_2_retired_71_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_bids_2_retired_7f", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_7f", muxModelEventSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_7f", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_bids_2_retired_7f", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}),
	muxModelOn("bids_queued_bids_2_retired_7f", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_7f", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_retired_7f", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_bids_2_retired_7f", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_bids_2_retired_7f", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_bids_2_retired_7f", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2_retired_7f", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2_retired_7f", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_bids_2_retired_7f", muxModelEventSessionClose, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}),
	muxModelOn("bids_queued_bids_2_retired_7f", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_bids_2_retired_7f", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_bids_2_retired_7f", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_bids_2_retired_7f", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_bids_2_telegram_live_enforce", muxModelEventSYN, muxState{phase: muxBidsQueued, bids: muxModelBids(2), telegramLive: true}),
	muxModelOn("bids_queued_bids_2_telegram_live_enforce", muxModelEventSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2), telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_telegram_live_enforce", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2), telegramLive: true}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_bids_2_telegram_live_enforce", muxModelEventStartedMatching, muxState{phase: muxBidsQueued, bids: muxModelBids(2), telegramLive: true}),
	muxModelOn("bids_queued_bids_2_telegram_live_enforce", muxModelEventStartedOther, muxState{phase: muxBidsQueued, bids: muxModelBids(2), telegramLive: true}),
	muxModelOn("bids_queued_bids_2_telegram_live_enforce", muxModelEventFailed, muxState{phase: muxBidsQueued, bids: muxModelBids(2), telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_bids_2_telegram_live_enforce", muxModelEventFailedPhantom, muxState{phase: muxBidsQueued, bids: muxModelBids(2), telegramLive: true}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_bids_2_telegram_live_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_bids_2_telegram_live_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_bids_2_telegram_live_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2_telegram_live_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_bids_2_telegram_live_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_bids_2_telegram_live_enforce", muxModelEventSessionClose, muxState{phase: muxBidsQueued, bids: muxModelBids(2), telegramLive: true}),
	muxModelOn("bids_queued_bids_2_telegram_live_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_bids_2_telegram_live_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_bids_2_telegram_live_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_bids_2_telegram_live_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2), telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_blocking_call_bids_2_blocking_upstream", muxModelEventSYN, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_blocking_call_bids_2_blocking_upstream", muxModelEventSymbol, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_blocking_call_bids_2_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_blocking_call_bids_2_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_blocking_call_bids_2_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_blocking_call_bids_2_blocking_upstream", muxModelEventFailed, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_blocking_call_bids_2_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_blocking_call_bids_2_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_blocking_call_bids_2_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_blocking_call_bids_2_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_blocking_call_bids_2_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_blocking_call_bids_2_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_blocking_call_bids_2_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_blocking_call_bids_2_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("bids_queued_blocking_call_bids_2_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_blocking_call_bids_2_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_blocking_call_bids_2_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventSYN, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventSymbol, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true}),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true}),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventFailed, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true}),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultLost),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectResultError),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultLost),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultError),
	muxModelOn("bids_queued_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("bids_queued_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventSYN, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventSymbol, muxState{phase: muxBidsQueued, blocking: true, resyncOwed: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, blocking: true, resyncOwed: 1, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxBidsQueued, blocking: true, resyncOwed: 1, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxBidsQueued, blocking: true, resyncOwed: 1, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventFailed, muxState{phase: muxBidsQueued, blocking: true, resyncOwed: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxBidsQueued, blocking: true, resyncOwed: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxBidsQueued, blocking: true, resyncOwed: 1, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("bids_queued_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("bids_queued_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, blocking: true, resyncOwed: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_held_bids_1_2", muxModelEventSYN, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(1, 2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_held_bids_1_2", muxModelEventSymbol, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(1, 2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_held_bids_1_2", muxModelEventBid, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(1, 2)}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_held_bids_1_2", muxModelEventStartedMatching, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(1, 2)}),
	muxModelOn("bids_queued_held_bids_1_2", muxModelEventStartedOther, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(1, 2)}),
	muxModelOn("bids_queued_held_bids_1_2", muxModelEventFailed, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(1, 2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_held_bids_1_2", muxModelEventFailedPhantom, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(1, 2)}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_held_bids_1_2", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_held_bids_1_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_held_bids_1_2", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_held_bids_1_2", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_held_bids_1_2", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_held_bids_1_2", muxModelEventSessionClose, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_held_bids_1_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_held_bids_1_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_held_bids_1_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_held_bids_1_2", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(1, 2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_held_bids_2", muxModelEventSYN, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_held_bids_2", muxModelEventSymbol, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_held_bids_2", muxModelEventBid, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_held_bids_2", muxModelEventStartedMatching, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_held_bids_2", muxModelEventStartedOther, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_held_bids_2", muxModelEventFailed, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_held_bids_2", muxModelEventFailedPhantom, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_held_bids_2", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_held_bids_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_held_bids_2", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_held_bids_2", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_held_bids_2", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_held_bids_2", muxModelEventSessionClose, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_held_bids_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_held_bids_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_held_bids_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_held_bids_2", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_reset_bids_2", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_reset_bids_2", muxModelEventSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_reset_bids_2", muxModelEventBid, muxState{phase: muxBidsQueued, recovery: arbitrationRecoveryReset, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_reset_bids_2", muxModelEventStartedMatching, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_reset_bids_2", muxModelEventStartedOther, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_reset_bids_2", muxModelEventFailed, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_reset_bids_2", muxModelEventFailedPhantom, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_reset_bids_2", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_reset_bids_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_reset_bids_2", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_reset_bids_2", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_reset_bids_2", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_reset_bids_2", muxModelEventSessionClose, muxState{phase: muxBidsQueued, recovery: arbitrationRecoveryReset, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_reset_bids_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_reset_bids_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_reset_bids_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_reset_bids_2", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_reset_bids_2_blocking_upstream", muxModelEventSYN, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_reset_bids_2_blocking_upstream", muxModelEventSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_reset_bids_2_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, recovery: arbitrationRecoveryReset, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_reset_bids_2_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_reset_bids_2_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_reset_bids_2_blocking_upstream", muxModelEventFailed, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_reset_bids_2_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_reset_bids_2_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_reset_bids_2_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_reset_bids_2_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_reset_bids_2_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_reset_bids_2_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_reset_bids_2_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxBidsQueued, recovery: arbitrationRecoveryReset, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_reset_bids_2_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_reset_bids_2_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_reset_bids_2_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_reset_bids_2_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_reset_bids_2_enforce", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_reset_bids_2_enforce", muxModelEventSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_reset_bids_2_enforce", muxModelEventBid, muxState{phase: muxBidsQueued, recovery: arbitrationRecoveryReset, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_reset_bids_2_enforce", muxModelEventStartedMatching, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_reset_bids_2_enforce", muxModelEventStartedOther, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_reset_bids_2_enforce", muxModelEventFailed, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_reset_bids_2_enforce", muxModelEventFailedPhantom, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_reset_bids_2_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_reset_bids_2_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_reset_bids_2_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_reset_bids_2_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_reset_bids_2_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_reset_bids_2_enforce", muxModelEventSessionClose, muxState{phase: muxBidsQueued, recovery: arbitrationRecoveryReset, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_reset_bids_2_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_reset_bids_2_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_reset_bids_2_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_reset_bids_2_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_resync_owed_1_bids_2", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_resync_owed_1_bids_2", muxModelEventSymbol, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_resync_owed_1_bids_2", muxModelEventBid, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_resync_owed_1_bids_2", muxModelEventStartedMatching, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_resync_owed_1_bids_2", muxModelEventStartedOther, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_resync_owed_1_bids_2", muxModelEventFailed, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_resync_owed_1_bids_2", muxModelEventFailedPhantom, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_resync_owed_1_bids_2", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_resync_owed_1_bids_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_resync_owed_1_bids_2", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_resync_owed_1_bids_2", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_resync_owed_1_bids_2", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_resync_owed_1_bids_2", muxModelEventSessionClose, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_resync_owed_1_bids_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_resync_owed_1_bids_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_resync_owed_1_bids_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_resync_owed_1_bids_2", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_resync_owed_1_bids_2_blocking_upstream", muxModelEventSYN, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_resync_owed_1_bids_2_blocking_upstream", muxModelEventSymbol, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_resync_owed_1_bids_2_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_resync_owed_1_bids_2_blocking_upstream", muxModelEventFailed, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_resync_owed_1_bids_2_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_resync_owed_1_bids_2_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_resync_owed_1_bids_2_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_resync_owed_1_bids_2_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_resync_owed_1_bids_2_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_resync_owed_1_bids_2_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_resync_owed_1_bids_2_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_resync_owed_1_bids_2_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_resync_owed_1_bids_2_retired_31", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_resync_owed_1_bids_2_retired_31", muxModelEventSymbol, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_resync_owed_1_bids_2_retired_31", muxModelEventBid, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_resync_owed_1_bids_2_retired_31", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_resync_owed_1_bids_2_retired_31", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_resync_owed_1_bids_2_retired_31", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_resync_owed_1_bids_2_retired_31", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_resync_owed_1_bids_2_retired_31", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_resync_owed_1_bids_2_retired_31", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_resync_owed_1_bids_2_retired_31", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_resync_owed_1_bids_2_retired_31", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_resync_owed_1_bids_2_retired_31", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_resync_owed_1_bids_2_retired_31", muxModelEventSessionClose, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_resync_owed_1_bids_2_retired_31", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_resync_owed_1_bids_2_retired_31", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_resync_owed_1_bids_2_retired_31", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_resync_owed_1_bids_2_retired_31", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("bids_queued_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_resync_owed_2_bids_2", muxModelEventSYN, muxState{phase: muxStartAsync, resyncOwed: 1, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_resync_owed_2_bids_2", muxModelEventSymbol, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_resync_owed_2_bids_2", muxModelEventBid, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_resync_owed_2_bids_2", muxModelEventStartedMatching, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_resync_owed_2_bids_2", muxModelEventStartedOther, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_resync_owed_2_bids_2", muxModelEventFailed, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_resync_owed_2_bids_2", muxModelEventFailedPhantom, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("bids_queued_resync_owed_2_bids_2", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_resync_owed_2_bids_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_resync_owed_2_bids_2", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_resync_owed_2_bids_2", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_resync_owed_2_bids_2", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_resync_owed_2_bids_2", muxModelEventSessionClose, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_resync_owed_2_bids_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_resync_owed_2_bids_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_resync_owed_2_bids_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_resync_owed_2_bids_2", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("bids_queued_resync_owed_2_bids_2_blocking_upstream", muxModelEventSYN, muxState{phase: muxStartBlocking, blocking: true, resyncOwed: 1, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_resync_owed_2_bids_2_blocking_upstream", muxModelEventSymbol, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_resync_owed_2_bids_2_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("bids_queued_resync_owed_2_bids_2_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_resync_owed_2_bids_2_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_resync_owed_2_bids_2_blocking_upstream", muxModelEventFailed, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("bids_queued_resync_owed_2_bids_2_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("bids_queued_resync_owed_2_bids_2_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("bids_queued_resync_owed_2_bids_2_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("bids_queued_resync_owed_2_bids_2_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("bids_queued_resync_owed_2_bids_2_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("bids_queued_resync_owed_2_bids_2_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("bids_queued_resync_owed_2_bids_2_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}),
	muxModelOn("bids_queued_resync_owed_2_bids_2_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("bids_queued_resync_owed_2_bids_2_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("bids_queued_resync_owed_2_bids_2_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("bids_queued_resync_owed_2_bids_2_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_absorb_pending", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_absorb_pending", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_absorb_pending", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_absorb_pending", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_absorb_pending", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_absorb_pending", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_absorb_pending", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_absorb_pending", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_absorb_pending", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelNever("fenced_absorb_pending", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_absorb_pending", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_absorb_pending", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_absorb_pending", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_absorb_pending", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_absorb_pending", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_absorb_pending", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_absorb_pending", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_31", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_31", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_31", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_31", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_31", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_31", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_31", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_31", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_31", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_31", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_31", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_31", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_31", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_31", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_31", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_31", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_31", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_71", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_71", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_71", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_71", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_71", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_71", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_7f", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_7f", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_7f", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_7f", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_7f", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_7f", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_7f", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_7f", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_7f", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_7f", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_7f", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_7f", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_7f", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_7f", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_7f", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_absorb_pending_bids_2_retired_7f", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_absorb_pending_bids_2_retired_7f", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultLost),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultError),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectResultLost),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectResultError),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_31", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_71", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_71", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_71", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_71", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_71", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_71", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_71", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_71", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_71", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_71", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_71", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_71", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_71", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_71", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_71", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_71", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_bids_2_retired_71", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_31", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_31", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_31", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_31", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_31", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_31", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_31", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_retired_31", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_31", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_retired_31", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_retired_31", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_retired_31", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_31", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_31", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_retired_31", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_retired_31", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_31", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_71", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_71", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_71", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_71", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_71", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_71", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_71", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_retired_71", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_71", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_retired_71", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_retired_71", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_retired_71", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_71", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_71", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_retired_71", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_1_retired_71", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_1_retired_71", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_bids_2_retired_31", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_bids_2_retired_31", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_bids_2_retired_31", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_bids_2_retired_31", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_bids_2_retired_31", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_bids_2_retired_31", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_bids_2_retired_31", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_2_bids_2_retired_31", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_bids_2_retired_31", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_2_bids_2_retired_31", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_2_bids_2_retired_31", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_2_bids_2_retired_31", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_bids_2_retired_31", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_bids_2_retired_31", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_2_bids_2_retired_31", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_2_bids_2_retired_31", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_bids_2_retired_31", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_retired_31", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_retired_31", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_retired_31", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_retired_31", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_retired_31", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_retired_31", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_retired_31", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_2_retired_31", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_retired_31", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_2_retired_31", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_2_retired_31", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_2_retired_31", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_retired_31", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 2, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_retired_31", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_2_retired_31", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_absorb_pending_resync_owed_2_retired_31", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_absorb_pending_resync_owed_2_retired_31", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_absorb_pending_retired_71", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_retired_71", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_retired_71", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_retired_71", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_retired_71", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_retired_71", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_retired_71", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_absorb_pending_retired_71", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_at_syn_absorb_pending_retired_71", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelNever("fenced_at_syn_absorb_pending_retired_71", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_retired_71", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_retired_71", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_absorb_pending_retired_71", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_retired_71", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_at_syn_absorb_pending_retired_71", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_absorb_pending_retired_71", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_absorb_pending_retired_71", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_absorb_pending_retired_71_enforce", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_retired_71_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_retired_71_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_retired_71_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_retired_71_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_retired_71_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_retired_71_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_absorb_pending_retired_71_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_at_syn_absorb_pending_retired_71_enforce", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelNever("fenced_at_syn_absorb_pending_retired_71_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_retired_71_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_retired_71_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_absorb_pending_retired_71_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_absorb_pending_retired_71_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_at_syn_absorb_pending_retired_71_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_absorb_pending_retired_71_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_absorb_pending_retired_71_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_absorb_pending_retired_7f", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_retired_7f", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_retired_7f", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}),
	muxModelOn("fenced_at_syn_absorb_pending_retired_7f", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}),
	muxModelOn("fenced_at_syn_absorb_pending_retired_7f", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_retired_7f", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_absorb_pending_retired_7f", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_absorb_pending_retired_7f", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_at_syn_absorb_pending_retired_7f", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}),
	muxModelNever("fenced_at_syn_absorb_pending_retired_7f", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_retired_7f", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_absorb_pending_retired_7f", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_absorb_pending_retired_7f", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x7F}),
	muxModelOn("fenced_at_syn_absorb_pending_retired_7f", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_at_syn_absorb_pending_retired_7f", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_absorb_pending_retired_7f", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_absorb_pending_retired_7f", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_bids_2_retired_31", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_31", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_31", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_bids_2_retired_31", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_bids_2_retired_31", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_31", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_31", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_bids_2_retired_31", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_bids_2_retired_31", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_bids_2_retired_31", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_bids_2_retired_31", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_bids_2_retired_31", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_bids_2_retired_31", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_bids_2_retired_31", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_at_syn_bids_2_retired_31", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_bids_2_retired_31", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_bids_2_retired_31", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_bids_2_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_bids_2_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_bids_2_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_bids_2_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_bids_2_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_bids_2_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_bids_2_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_bids_2_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_bids_2_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_bids_2_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_at_syn_bids_2_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_bids_2_retired_31_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_bids_2_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_bids_2_retired_31_enforce", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_31_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_31_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_bids_2_retired_31_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_bids_2_retired_31_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_31_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_31_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_bids_2_retired_31_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_bids_2_retired_31_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_bids_2_retired_31_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_bids_2_retired_31_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_bids_2_retired_31_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_bids_2_retired_31_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_bids_2_retired_31_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_at_syn_bids_2_retired_31_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_bids_2_retired_31_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_bids_2_retired_31_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventSYN, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("fenced_at_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("fenced_at_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("fenced_at_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_at_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_bids_2_retired_71", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_71", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_71", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_bids_2_retired_71", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_bids_2_retired_71", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_71", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_71", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_bids_2_retired_71", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_bids_2_retired_71", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_bids_2_retired_71", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_bids_2_retired_71", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_bids_2_retired_71", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_bids_2_retired_71", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_bids_2_retired_71", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_at_syn_bids_2_retired_71", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_bids_2_retired_71", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_bids_2_retired_71", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_bids_2_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_bids_2_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_bids_2_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_bids_2_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_bids_2_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_bids_2_retired_71_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_bids_2_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_bids_2_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_bids_2_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_bids_2_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_at_syn_bids_2_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_bids_2_retired_71_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_bids_2_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_bids_2_retired_71_enforce", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_71_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_71_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_bids_2_retired_71_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_bids_2_retired_71_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_71_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_71_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_bids_2_retired_71_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_bids_2_retired_71_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_bids_2_retired_71_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_bids_2_retired_71_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_bids_2_retired_71_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_bids_2_retired_71_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_bids_2_retired_71_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_at_syn_bids_2_retired_71_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_bids_2_retired_71_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_bids_2_retired_71_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_bids_2_retired_7f", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_7f", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_7f", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_bids_2_retired_7f", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}),
	muxModelOn("fenced_at_syn_bids_2_retired_7f", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_7f", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_bids_2_retired_7f", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_bids_2_retired_7f", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_bids_2_retired_7f", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_bids_2_retired_7f", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_bids_2_retired_7f", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_bids_2_retired_7f", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_bids_2_retired_7f", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}),
	muxModelOn("fenced_at_syn_bids_2_retired_7f", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_at_syn_bids_2_retired_7f", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_bids_2_retired_7f", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_bids_2_retired_7f", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultLost),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultError),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_deferred_loser_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_blocking_call_bids_2_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("fenced_at_syn_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectResultLost),
	muxModelOn("fenced_at_syn_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectResultError),
	muxModelOn("fenced_at_syn_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_blocking_call_resync_owed_1_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_blocking_call_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_blocking_call_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_blocking_call_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_blocking_call_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_blocking_call_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_blocking_call_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_blocking_call_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_blocking_call_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_blocking_call_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_blocking_call_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelOn("fenced_at_syn_blocking_call_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_blocking_call_retired_31_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_blocking_call_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_blocking_call_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_blocking_call_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_blocking_call_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_blocking_call_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_blocking_call_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_blocking_call_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_blocking_call_retired_71_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_blocking_call_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_blocking_call_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_blocking_call_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_blocking_call_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelOn("fenced_at_syn_blocking_call_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_blocking_call_retired_71_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_blocking_call_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_31", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_31", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_31", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_31", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_31", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_31", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_31", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_31", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_31", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_31", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_31", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_31", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_31", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_31", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_31", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_31", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_31", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_71", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_71", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_71", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_71", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_71", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_71", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_71", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_71", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_71", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_71", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_71", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_71", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_71", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_71", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_71", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_71", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_71", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_resync_owed_1_retired_31", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_31", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_31", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_31", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_31", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_31", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_31", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_31", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_31", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_31", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_31", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_31", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_31", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_31", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_31", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_31", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_31", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_resync_owed_1_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_31_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_resync_owed_1_retired_71", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_71", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_71", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_71", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_71", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_71", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_71", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_71", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_71", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_71", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_71", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_71", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_71", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_71", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_71", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_71", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_71", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_resync_owed_1_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_71_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_resync_owed_1_retired_71_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_resync_owed_1_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_resync_owed_2_bids_2_retired_31", muxModelEventSYN, muxState{phase: muxStartAsync, resyncOwed: 1, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_2_bids_2_retired_31", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_2_bids_2_retired_31", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_resync_owed_2_bids_2_retired_31", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_resync_owed_2_bids_2_retired_31", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_2_bids_2_retired_31", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_2_bids_2_retired_31", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_resync_owed_2_bids_2_retired_31", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_resync_owed_2_bids_2_retired_31", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_resync_owed_2_bids_2_retired_31", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_2_bids_2_retired_31", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_2_bids_2_retired_31", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_resync_owed_2_bids_2_retired_31", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_resync_owed_2_bids_2_retired_31", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_at_syn_resync_owed_2_bids_2_retired_31", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_resync_owed_2_bids_2_retired_31", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_resync_owed_2_bids_2_retired_31", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxStartBlocking, blocking: true, resyncOwed: 1, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("fenced_at_syn_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_at_syn_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_resync_owed_2_retired_31", muxModelEventSYN, muxState{phase: muxIdle, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_2_retired_31", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_2_retired_31", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_resync_owed_2_retired_31", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_resync_owed_2_retired_31", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_2_retired_31", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_2_retired_31", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_resync_owed_2_retired_31", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_resync_owed_2_retired_31", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_resync_owed_2_retired_31", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_2_retired_31", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_2_retired_31", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_resync_owed_2_retired_31", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_resync_owed_2_retired_31", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_at_syn_resync_owed_2_retired_31", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_resync_owed_2_retired_31", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_resync_owed_2_retired_31", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_resync_owed_2_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_2_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_2_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_resync_owed_2_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_resync_owed_2_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_2_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_resync_owed_2_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_resync_owed_2_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_resync_owed_2_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_resync_owed_2_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_2_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_resync_owed_2_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_resync_owed_2_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_resync_owed_2_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_at_syn_resync_owed_2_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_resync_owed_2_retired_31_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_resync_owed_2_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_at_syn_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_retired_31_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_retired_31_enforce", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_31_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_31_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_retired_31_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_retired_31_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_31_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_31_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_retired_31_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_retired_31_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_retired_31_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_retired_31_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_retired_31_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_retired_31_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_at_syn_retired_31_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_at_syn_retired_31_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_retired_31_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_retired_31_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_retired_31_telegram_live_enforce", muxModelEventSYN, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("fenced_at_syn_retired_31_telegram_live_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_31_telegram_live_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("fenced_at_syn_retired_31_telegram_live_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("fenced_at_syn_retired_31_telegram_live_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_31_telegram_live_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_31_telegram_live_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_retired_31_telegram_live_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_retired_31_telegram_live_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_retired_31_telegram_live_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_retired_31_telegram_live_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_retired_31_telegram_live_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_retired_31_telegram_live_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("fenced_at_syn_retired_31_telegram_live_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_at_syn_retired_31_telegram_live_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_retired_31_telegram_live_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_retired_31_telegram_live_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_retired_71", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_71", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_71", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_retired_71", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_retired_71", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_71", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_71", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_retired_71", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_retired_71", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_retired_71", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_retired_71", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_retired_71", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_retired_71", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_retired_71", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_at_syn_retired_71", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_retired_71", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_retired_71", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_retired_71_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_at_syn_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_retired_71_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_retired_71_enforce", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_71_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_71_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_retired_71_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_retired_71_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_71_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_71_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_retired_71_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_retired_71_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_retired_71_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_retired_71_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_retired_71_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_retired_71_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_at_syn_retired_71_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_at_syn_retired_71_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_retired_71_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_retired_71_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_at_syn_retired_7f", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_7f", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_7f", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}),
	muxModelOn("fenced_at_syn_retired_7f", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}),
	muxModelOn("fenced_at_syn_retired_7f", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_7f", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_at_syn_retired_7f", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_at_syn_retired_7f", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_at_syn_retired_7f", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_at_syn_retired_7f", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_retired_7f", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_at_syn_retired_7f", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_at_syn_retired_7f", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}),
	muxModelOn("fenced_at_syn_retired_7f", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_at_syn_retired_7f", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_at_syn_retired_7f", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_at_syn_retired_7f", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_foreign", muxModelEventSYN, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_foreign", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_foreign", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_foreign", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_foreign", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_foreign", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_foreign", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_foreign", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_foreign", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_foreign", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_foreign_absorb_pending", muxModelEventSYN, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, absorb: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign_absorb_pending", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_absorb_pending", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign_absorb_pending", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign_absorb_pending", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_absorb_pending", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_absorb_pending", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_foreign_absorb_pending", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_foreign_absorb_pending", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}),
	muxModelNever("fenced_foreign_absorb_pending", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_absorb_pending", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_absorb_pending", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_foreign_absorb_pending", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign_absorb_pending", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_foreign_absorb_pending", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_foreign_absorb_pending", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_foreign_absorb_pending", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_31_enforce", muxModelEventSYN, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_31_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_31_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_31_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_31_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_31_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_31_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_foreign_absorb_pending_bids_2_retired_31_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_31_enforce", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelNever("fenced_foreign_absorb_pending_bids_2_retired_31_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_absorb_pending_bids_2_retired_31_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_absorb_pending_bids_2_retired_31_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_31_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_31_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_foreign_absorb_pending_bids_2_retired_31_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_foreign_absorb_pending_bids_2_retired_31_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_31_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_71_enforce", muxModelEventSYN, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_71_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_71_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_71_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_71_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_71_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_71_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_foreign_absorb_pending_bids_2_retired_71_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_71_enforce", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelNever("fenced_foreign_absorb_pending_bids_2_retired_71_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_absorb_pending_bids_2_retired_71_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_absorb_pending_bids_2_retired_71_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_71_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_71_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_foreign_absorb_pending_bids_2_retired_71_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_foreign_absorb_pending_bids_2_retired_71_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_foreign_absorb_pending_bids_2_retired_71_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_foreign_absorb_pending_retired_71_enforce", muxModelEventSYN, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, absorb: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_absorb_pending_retired_71_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_absorb_pending_retired_71_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_absorb_pending_retired_71_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_absorb_pending_retired_71_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_absorb_pending_retired_71_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_absorb_pending_retired_71_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_foreign_absorb_pending_retired_71_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_foreign_absorb_pending_retired_71_enforce", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}),
	muxModelNever("fenced_foreign_absorb_pending_retired_71_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_absorb_pending_retired_71_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_absorb_pending_retired_71_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_foreign_absorb_pending_retired_71_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_absorb_pending_retired_71_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_foreign_absorb_pending_retired_71_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_foreign_absorb_pending_retired_71_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_foreign_absorb_pending_retired_71_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_31_enforce", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_31_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_31_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_31_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_31_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_31_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_31_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_bids_2_retired_31_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_31_enforce", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_bids_2_retired_31_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_bids_2_retired_31_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_bids_2_retired_31_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_31_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_31_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_bids_2_retired_31_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_bids_2_retired_31_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_31_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventSYN, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_bids_2_retired_71_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_31_enforce", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_31_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_31_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_31_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_31_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_31_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_31_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_retired_31_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_31_enforce", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, retired: true, retiredSource: 0x31}),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_retired_31_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_retired_31_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_retired_31_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_31_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, absorb: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_31_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_retired_31_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_retired_31_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_31_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_71_enforce", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_71_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_71_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_71_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_71_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_71_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_71_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_retired_71_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_71_enforce", muxModelEventAbsorbDebounce, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, retired: true, retiredSource: 0x71}),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_retired_71_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_retired_71_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_retired_71_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_71_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, absorb: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_71_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_retired_71_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_foreign_after_syn_absorb_pending_retired_71_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_foreign_after_syn_absorb_pending_retired_71_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_foreign_after_syn_bids_2_retired_31_enforce", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_31_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_31_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_31_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_31_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_31_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_31_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_foreign_after_syn_bids_2_retired_31_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_foreign_after_syn_bids_2_retired_31_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_foreign_after_syn_bids_2_retired_31_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_after_syn_bids_2_retired_31_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_after_syn_bids_2_retired_31_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_31_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_31_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_foreign_after_syn_bids_2_retired_31_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_foreign_after_syn_bids_2_retired_31_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_31_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_foreign_after_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectResultCancelled),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_foreign_after_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_foreign_after_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_foreign_after_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_after_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_after_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_foreign_after_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_foreign_after_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_31_telegram_live_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_foreign_after_syn_bids_2_retired_71_enforce", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_71_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_71_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_71_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_71_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_71_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_71_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_foreign_after_syn_bids_2_retired_71_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_foreign_after_syn_bids_2_retired_71_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_foreign_after_syn_bids_2_retired_71_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_after_syn_bids_2_retired_71_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_after_syn_bids_2_retired_71_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_71_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_71_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_foreign_after_syn_bids_2_retired_71_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_foreign_after_syn_bids_2_retired_71_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_foreign_after_syn_bids_2_retired_71_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_foreign_after_syn_retired_31_enforce", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_retired_31_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_retired_31_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign_after_syn_retired_31_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign_after_syn_retired_31_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_retired_31_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_retired_31_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_foreign_after_syn_retired_31_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_foreign_after_syn_retired_31_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_foreign_after_syn_retired_31_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_after_syn_retired_31_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_after_syn_retired_31_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_foreign_after_syn_retired_31_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign_after_syn_retired_31_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_foreign_after_syn_retired_31_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_foreign_after_syn_retired_31_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_foreign_after_syn_retired_31_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_foreign_after_syn_retired_31_telegram_live_enforce", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_retired_31_telegram_live_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_retired_31_telegram_live_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("fenced_foreign_after_syn_retired_31_telegram_live_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("fenced_foreign_after_syn_retired_31_telegram_live_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_retired_31_telegram_live_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_retired_31_telegram_live_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_foreign_after_syn_retired_31_telegram_live_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_foreign_after_syn_retired_31_telegram_live_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_foreign_after_syn_retired_31_telegram_live_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_after_syn_retired_31_telegram_live_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_after_syn_retired_31_telegram_live_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_foreign_after_syn_retired_31_telegram_live_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("fenced_foreign_after_syn_retired_31_telegram_live_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_foreign_after_syn_retired_31_telegram_live_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_foreign_after_syn_retired_31_telegram_live_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_foreign_after_syn_retired_31_telegram_live_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_foreign_after_syn_retired_71_enforce", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_retired_71_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_retired_71_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_after_syn_retired_71_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_after_syn_retired_71_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_retired_71_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_after_syn_retired_71_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_foreign_after_syn_retired_71_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_foreign_after_syn_retired_71_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_foreign_after_syn_retired_71_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_after_syn_retired_71_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_after_syn_retired_71_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_foreign_after_syn_retired_71_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_after_syn_retired_71_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_foreign_after_syn_retired_71_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_foreign_after_syn_retired_71_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_foreign_after_syn_retired_71_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_foreign_bids_2_retired_31_enforce", muxModelEventSYN, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign_bids_2_retired_31_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_bids_2_retired_31_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("fenced_foreign_bids_2_retired_31_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign_bids_2_retired_31_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_bids_2_retired_31_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_bids_2_retired_31_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_foreign_bids_2_retired_31_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_foreign_bids_2_retired_31_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_foreign_bids_2_retired_31_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_bids_2_retired_31_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_bids_2_retired_31_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_foreign_bids_2_retired_31_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("fenced_foreign_bids_2_retired_31_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_foreign_bids_2_retired_31_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_foreign_bids_2_retired_31_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_foreign_bids_2_retired_31_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_foreign_bids_2_retired_31_telegram_live_enforce", muxModelEventSYN, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("fenced_foreign_bids_2_retired_31_telegram_live_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_bids_2_retired_31_telegram_live_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectResultCancelled),
	muxModelOn("fenced_foreign_bids_2_retired_31_telegram_live_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("fenced_foreign_bids_2_retired_31_telegram_live_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_bids_2_retired_31_telegram_live_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_bids_2_retired_31_telegram_live_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_foreign_bids_2_retired_31_telegram_live_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_foreign_bids_2_retired_31_telegram_live_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_foreign_bids_2_retired_31_telegram_live_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_bids_2_retired_31_telegram_live_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_bids_2_retired_31_telegram_live_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_foreign_bids_2_retired_31_telegram_live_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("fenced_foreign_bids_2_retired_31_telegram_live_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_foreign_bids_2_retired_31_telegram_live_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_foreign_bids_2_retired_31_telegram_live_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_foreign_bids_2_retired_31_telegram_live_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_foreign_bids_2_retired_71_enforce", muxModelEventSYN, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_bids_2_retired_71_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_bids_2_retired_71_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("fenced_foreign_bids_2_retired_71_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_bids_2_retired_71_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_bids_2_retired_71_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_bids_2_retired_71_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_foreign_bids_2_retired_71_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_foreign_bids_2_retired_71_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_foreign_bids_2_retired_71_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_bids_2_retired_71_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_bids_2_retired_71_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_foreign_bids_2_retired_71_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_bids_2_retired_71_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_foreign_bids_2_retired_71_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_foreign_bids_2_retired_71_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_foreign_bids_2_retired_71_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_foreign_retired_31_telegram_live_enforce", muxModelEventSYN, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("fenced_foreign_retired_31_telegram_live_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_retired_31_telegram_live_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("fenced_foreign_retired_31_telegram_live_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("fenced_foreign_retired_31_telegram_live_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_retired_31_telegram_live_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_retired_31_telegram_live_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_foreign_retired_31_telegram_live_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_foreign_retired_31_telegram_live_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_foreign_retired_31_telegram_live_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_retired_31_telegram_live_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_retired_31_telegram_live_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_foreign_retired_31_telegram_live_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("fenced_foreign_retired_31_telegram_live_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_foreign_retired_31_telegram_live_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_foreign_retired_31_telegram_live_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_foreign_retired_31_telegram_live_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_foreign_retired_71_enforce", muxModelEventSYN, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeignAfterAA, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_retired_71_enforce", muxModelEventSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_retired_71_enforce", muxModelEventBid, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_retired_71_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_retired_71_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_retired_71_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_foreign_retired_71_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_foreign_retired_71_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_foreign_retired_71_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_foreign_retired_71_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_retired_71_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_foreign_retired_71_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_foreign_retired_71_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}),
	muxModelOn("fenced_foreign_retired_71_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_foreign_retired_71_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_foreign_retired_71_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_foreign_retired_71_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_no_recovery", muxModelEventSYN, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_no_recovery", muxModelEventSymbol, muxState{phase: muxFenced}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_no_recovery", muxModelEventBid, muxState{phase: muxFenced, bids: muxModelBids(2)}),
	muxModelOn("fenced_no_recovery", muxModelEventStartedMatching, muxState{phase: muxFenced}),
	muxModelOn("fenced_no_recovery", muxModelEventStartedOther, muxState{phase: muxFenced}),
	muxModelOn("fenced_no_recovery", muxModelEventFailed, muxState{phase: muxFenced}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_no_recovery", muxModelEventFailedPhantom, muxState{phase: muxFenced}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_no_recovery", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_no_recovery", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_no_recovery", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_no_recovery", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_no_recovery", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_no_recovery", muxModelEventSessionClose, muxState{phase: muxFenced}),
	muxModelOn("fenced_no_recovery", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_no_recovery", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_no_recovery", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_no_recovery", muxModelEventInitiatorSymbol, muxState{phase: muxFenced}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_no_recovery_bids_2", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("fenced_no_recovery_bids_2", muxModelEventSymbol, muxState{phase: muxFenced, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_no_recovery_bids_2", muxModelEventBid, muxState{phase: muxFenced, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("fenced_no_recovery_bids_2", muxModelEventStartedMatching, muxState{phase: muxFenced, bids: muxModelBids(2)}),
	muxModelOn("fenced_no_recovery_bids_2", muxModelEventStartedOther, muxState{phase: muxFenced, bids: muxModelBids(2)}),
	muxModelOn("fenced_no_recovery_bids_2", muxModelEventFailed, muxState{phase: muxFenced, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_no_recovery_bids_2", muxModelEventFailedPhantom, muxState{phase: muxFenced, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_no_recovery_bids_2", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_no_recovery_bids_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_no_recovery_bids_2", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_no_recovery_bids_2", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_no_recovery_bids_2", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_no_recovery_bids_2", muxModelEventSessionClose, muxState{phase: muxFenced, bids: muxModelBids(2)}),
	muxModelOn("fenced_no_recovery_bids_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_no_recovery_bids_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_no_recovery_bids_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_no_recovery_bids_2", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_no_recovery_resync_owed_2", muxModelEventSYN, muxState{phase: muxIdle, resyncOwed: 1}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_no_recovery_resync_owed_2", muxModelEventSymbol, muxState{phase: muxFenced, resyncOwed: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_no_recovery_resync_owed_2", muxModelEventBid, muxState{phase: muxFenced, resyncOwed: 2, bids: muxModelBids(2)}),
	muxModelOn("fenced_no_recovery_resync_owed_2", muxModelEventStartedMatching, muxState{phase: muxFenced, resyncOwed: 2}),
	muxModelOn("fenced_no_recovery_resync_owed_2", muxModelEventStartedOther, muxState{phase: muxFenced, resyncOwed: 2}),
	muxModelOn("fenced_no_recovery_resync_owed_2", muxModelEventFailed, muxState{phase: muxFenced, resyncOwed: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_no_recovery_resync_owed_2", muxModelEventFailedPhantom, muxState{phase: muxFenced, resyncOwed: 2}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_no_recovery_resync_owed_2", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_no_recovery_resync_owed_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_no_recovery_resync_owed_2", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_no_recovery_resync_owed_2", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_no_recovery_resync_owed_2", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_no_recovery_resync_owed_2", muxModelEventSessionClose, muxState{phase: muxFenced, resyncOwed: 2}),
	muxModelOn("fenced_no_recovery_resync_owed_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("fenced_no_recovery_resync_owed_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_no_recovery_resync_owed_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_no_recovery_resync_owed_2", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, resyncOwed: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("fenced_no_recovery_resync_owed_2_bids_2", muxModelEventSYN, muxState{phase: muxStartAsync, resyncOwed: 1, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("fenced_no_recovery_resync_owed_2_bids_2", muxModelEventSymbol, muxState{phase: muxFenced, resyncOwed: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_no_recovery_resync_owed_2_bids_2", muxModelEventBid, muxState{phase: muxFenced, resyncOwed: 2, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("fenced_no_recovery_resync_owed_2_bids_2", muxModelEventStartedMatching, muxState{phase: muxFenced, resyncOwed: 2, bids: muxModelBids(2)}),
	muxModelOn("fenced_no_recovery_resync_owed_2_bids_2", muxModelEventStartedOther, muxState{phase: muxFenced, resyncOwed: 2, bids: muxModelBids(2)}),
	muxModelOn("fenced_no_recovery_resync_owed_2_bids_2", muxModelEventFailed, muxState{phase: muxFenced, resyncOwed: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("fenced_no_recovery_resync_owed_2_bids_2", muxModelEventFailedPhantom, muxState{phase: muxFenced, resyncOwed: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("fenced_no_recovery_resync_owed_2_bids_2", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("fenced_no_recovery_resync_owed_2_bids_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("fenced_no_recovery_resync_owed_2_bids_2", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("fenced_no_recovery_resync_owed_2_bids_2", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("fenced_no_recovery_resync_owed_2_bids_2", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("fenced_no_recovery_resync_owed_2_bids_2", muxModelEventSessionClose, muxState{phase: muxFenced, resyncOwed: 2, bids: muxModelBids(2)}),
	muxModelOn("fenced_no_recovery_resync_owed_2_bids_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("fenced_no_recovery_resync_owed_2_bids_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("fenced_no_recovery_resync_owed_2_bids_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("fenced_no_recovery_resync_owed_2_bids_2", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, resyncOwed: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("gateway_awaiting_echo", muxModelEventSYN, muxState{phase: muxGatewayTransaction, echoPending: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("gateway_awaiting_echo", muxModelEventSymbol, muxState{phase: muxGatewayTransaction, echoPending: true}, muxModelEffectSessionReceived),
	muxModelOn("gateway_awaiting_echo", muxModelEventBid, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2), echoPending: true}),
	muxModelOn("gateway_awaiting_echo", muxModelEventStartedMatching, muxState{phase: muxGatewayTransaction, echoPending: true}),
	muxModelOn("gateway_awaiting_echo", muxModelEventStartedOther, muxState{phase: muxGatewayTransaction, echoPending: true}),
	muxModelOn("gateway_awaiting_echo", muxModelEventFailed, muxState{phase: muxGatewayTransaction, echoPending: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("gateway_awaiting_echo", muxModelEventFailedPhantom, muxState{phase: muxGatewayTransaction, echoPending: true}, muxModelEffectPassiveSymbol),
	muxModelNever("gateway_awaiting_echo", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("gateway_awaiting_echo", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("gateway_awaiting_echo", muxModelEventIdleGrace, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("gateway_awaiting_echo", muxModelEventOwnershipLimit, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("gateway_awaiting_echo", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("gateway_awaiting_echo", muxModelEventSessionClose, muxState{phase: muxGatewayTransaction, echoPending: true}),
	muxModelOn("gateway_awaiting_echo", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("gateway_awaiting_echo", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("gateway_awaiting_echo", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("gateway_awaiting_echo", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, resyncOwed: 2}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),

	muxModelOn("gateway_awaiting_terminator_echo", muxModelEventSYN, muxState{phase: muxGatewayGrace}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("gateway_awaiting_terminator_echo", muxModelEventSymbol, muxState{phase: muxGatewayTransaction, echoPending: true, echoTerminator: true}, muxModelEffectSessionReceived),
	muxModelOn("gateway_awaiting_terminator_echo", muxModelEventBid, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2), echoPending: true, echoTerminator: true}),
	muxModelOn("gateway_awaiting_terminator_echo", muxModelEventStartedMatching, muxState{phase: muxGatewayTransaction, echoPending: true, echoTerminator: true}),
	muxModelOn("gateway_awaiting_terminator_echo", muxModelEventStartedOther, muxState{phase: muxGatewayTransaction, echoPending: true, echoTerminator: true}),
	muxModelOn("gateway_awaiting_terminator_echo", muxModelEventFailed, muxState{phase: muxGatewayTransaction, echoPending: true, echoTerminator: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("gateway_awaiting_terminator_echo", muxModelEventFailedPhantom, muxState{phase: muxGatewayTransaction, echoPending: true, echoTerminator: true}, muxModelEffectPassiveSymbol),
	muxModelNever("gateway_awaiting_terminator_echo", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("gateway_awaiting_terminator_echo", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("gateway_awaiting_terminator_echo", muxModelEventIdleGrace, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("gateway_awaiting_terminator_echo", muxModelEventOwnershipLimit, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("gateway_awaiting_terminator_echo", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("gateway_awaiting_terminator_echo", muxModelEventSessionClose, muxState{phase: muxGatewayTransaction, echoPending: true, echoTerminator: true}),
	muxModelOn("gateway_awaiting_terminator_echo", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("gateway_awaiting_terminator_echo", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("gateway_awaiting_terminator_echo", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("gateway_awaiting_terminator_echo", muxModelEventInitiatorSymbol, muxState{phase: muxGatewayTransaction, echoPending: true, echoTerminator: true}, muxModelEffectSessionReceived),

	muxModelOn("gateway_grace", muxModelEventSYN, muxState{phase: muxGatewayGrace}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("gateway_grace", muxModelEventSymbol, muxState{phase: muxGatewayGrace}, muxModelEffectSessionReceived),
	muxModelOn("gateway_grace", muxModelEventBid, muxState{phase: muxGatewayGrace, bids: muxModelBids(2)}),
	muxModelOn("gateway_grace", muxModelEventStartedMatching, muxState{phase: muxGatewayGrace}),
	muxModelOn("gateway_grace", muxModelEventStartedOther, muxState{phase: muxGatewayGrace}),
	muxModelOn("gateway_grace", muxModelEventFailed, muxState{phase: muxGatewayGrace}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("gateway_grace", muxModelEventFailedPhantom, muxState{phase: muxGatewayGrace}, muxModelEffectPassiveSymbol),
	muxModelNever("gateway_grace", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("gateway_grace", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("gateway_grace", muxModelEventIdleGrace, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("gateway_grace", muxModelEventOwnershipLimit, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("gateway_grace", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("gateway_grace", muxModelEventSessionClose, muxState{phase: muxGatewayGrace}),
	muxModelOn("gateway_grace", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("gateway_grace", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("gateway_grace", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("gateway_grace", muxModelEventInitiatorSymbol, muxState{phase: muxFenced}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("gateway_grace_bids_2", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("gateway_grace_bids_2", muxModelEventSymbol, muxState{phase: muxGatewayGrace, bids: muxModelBids(2)}, muxModelEffectSessionReceived),
	muxModelOn("gateway_grace_bids_2", muxModelEventBid, muxState{phase: muxGatewayGrace, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("gateway_grace_bids_2", muxModelEventStartedMatching, muxState{phase: muxGatewayGrace, bids: muxModelBids(2)}),
	muxModelOn("gateway_grace_bids_2", muxModelEventStartedOther, muxState{phase: muxGatewayGrace, bids: muxModelBids(2)}),
	muxModelOn("gateway_grace_bids_2", muxModelEventFailed, muxState{phase: muxGatewayGrace, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("gateway_grace_bids_2", muxModelEventFailedPhantom, muxState{phase: muxGatewayGrace, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("gateway_grace_bids_2", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("gateway_grace_bids_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("gateway_grace_bids_2", muxModelEventIdleGrace, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("gateway_grace_bids_2", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("gateway_grace_bids_2", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("gateway_grace_bids_2", muxModelEventSessionClose, muxState{phase: muxGatewayGrace, bids: muxModelBids(2)}),
	muxModelOn("gateway_grace_bids_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("gateway_grace_bids_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("gateway_grace_bids_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("gateway_grace_bids_2", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("gateway_transaction", muxModelEventSYN, muxState{phase: muxGatewayTransaction}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("gateway_transaction", muxModelEventSymbol, muxState{phase: muxGatewayTransaction}, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("gateway_transaction", muxModelEventBid, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2)}),
	muxModelOn("gateway_transaction", muxModelEventStartedMatching, muxState{phase: muxGatewayTransaction}),
	muxModelOn("gateway_transaction", muxModelEventStartedOther, muxState{phase: muxGatewayTransaction}),
	muxModelOn("gateway_transaction", muxModelEventFailed, muxState{phase: muxGatewayTransaction}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("gateway_transaction", muxModelEventFailedPhantom, muxState{phase: muxGatewayTransaction}, muxModelEffectPassiveSymbol),
	muxModelNever("gateway_transaction", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("gateway_transaction", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("gateway_transaction", muxModelEventIdleGrace, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("gateway_transaction", muxModelEventOwnershipLimit, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("gateway_transaction", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("gateway_transaction", muxModelEventSessionClose, muxState{phase: muxGatewayTransaction}),
	muxModelOn("gateway_transaction", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("gateway_transaction", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("gateway_transaction", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("gateway_transaction", muxModelEventInitiatorSymbol, muxState{phase: muxGatewayTransaction}, muxModelEffectActiveByte, muxModelEffectSessionReceived),

	muxModelOn("gateway_transaction_bids_2", muxModelEventSYN, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("gateway_transaction_bids_2", muxModelEventSymbol, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2)}, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("gateway_transaction_bids_2", muxModelEventBid, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("gateway_transaction_bids_2", muxModelEventStartedMatching, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2)}),
	muxModelOn("gateway_transaction_bids_2", muxModelEventStartedOther, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2)}),
	muxModelOn("gateway_transaction_bids_2", muxModelEventFailed, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("gateway_transaction_bids_2", muxModelEventFailedPhantom, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("gateway_transaction_bids_2", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("gateway_transaction_bids_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("gateway_transaction_bids_2", muxModelEventIdleGrace, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("gateway_transaction_bids_2", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("gateway_transaction_bids_2", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("gateway_transaction_bids_2", muxModelEventSessionClose, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2)}),
	muxModelOn("gateway_transaction_bids_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("gateway_transaction_bids_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("gateway_transaction_bids_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("gateway_transaction_bids_2", muxModelEventInitiatorSymbol, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2)}, muxModelEffectActiveByte, muxModelEffectSessionReceived),

	muxModelOn("gateway_transaction_bids_2_awaiting_echo", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("gateway_transaction_bids_2_awaiting_echo", muxModelEventSymbol, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2), echoPending: true}, muxModelEffectSessionReceived),
	muxModelOn("gateway_transaction_bids_2_awaiting_echo", muxModelEventBid, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2), echoPending: true}, muxModelEffectResultCancelled),
	muxModelOn("gateway_transaction_bids_2_awaiting_echo", muxModelEventStartedMatching, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2), echoPending: true}),
	muxModelOn("gateway_transaction_bids_2_awaiting_echo", muxModelEventStartedOther, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2), echoPending: true}),
	muxModelOn("gateway_transaction_bids_2_awaiting_echo", muxModelEventFailed, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2), echoPending: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("gateway_transaction_bids_2_awaiting_echo", muxModelEventFailedPhantom, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2), echoPending: true}, muxModelEffectPassiveSymbol),
	muxModelNever("gateway_transaction_bids_2_awaiting_echo", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("gateway_transaction_bids_2_awaiting_echo", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("gateway_transaction_bids_2_awaiting_echo", muxModelEventIdleGrace, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("gateway_transaction_bids_2_awaiting_echo", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("gateway_transaction_bids_2_awaiting_echo", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("gateway_transaction_bids_2_awaiting_echo", muxModelEventSessionClose, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2), echoPending: true}),
	muxModelOn("gateway_transaction_bids_2_awaiting_echo", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("gateway_transaction_bids_2_awaiting_echo", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("gateway_transaction_bids_2_awaiting_echo", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("gateway_transaction_bids_2_awaiting_echo", muxModelEventInitiatorSymbol, muxState{phase: muxFenced, resyncOwed: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),

	muxModelOn("gateway_transaction_bids_2_awaiting_terminator_echo", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("gateway_transaction_bids_2_awaiting_terminator_echo", muxModelEventSymbol, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2), echoPending: true, echoTerminator: true}, muxModelEffectSessionReceived),
	muxModelOn("gateway_transaction_bids_2_awaiting_terminator_echo", muxModelEventBid, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2), echoPending: true, echoTerminator: true}, muxModelEffectResultCancelled),
	muxModelOn("gateway_transaction_bids_2_awaiting_terminator_echo", muxModelEventStartedMatching, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2), echoPending: true, echoTerminator: true}),
	muxModelOn("gateway_transaction_bids_2_awaiting_terminator_echo", muxModelEventStartedOther, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2), echoPending: true, echoTerminator: true}),
	muxModelOn("gateway_transaction_bids_2_awaiting_terminator_echo", muxModelEventFailed, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2), echoPending: true, echoTerminator: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("gateway_transaction_bids_2_awaiting_terminator_echo", muxModelEventFailedPhantom, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2), echoPending: true, echoTerminator: true}, muxModelEffectPassiveSymbol),
	muxModelNever("gateway_transaction_bids_2_awaiting_terminator_echo", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("gateway_transaction_bids_2_awaiting_terminator_echo", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("gateway_transaction_bids_2_awaiting_terminator_echo", muxModelEventIdleGrace, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("gateway_transaction_bids_2_awaiting_terminator_echo", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("gateway_transaction_bids_2_awaiting_terminator_echo", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("gateway_transaction_bids_2_awaiting_terminator_echo", muxModelEventSessionClose, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2), echoPending: true, echoTerminator: true}),
	muxModelOn("gateway_transaction_bids_2_awaiting_terminator_echo", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("gateway_transaction_bids_2_awaiting_terminator_echo", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("gateway_transaction_bids_2_awaiting_terminator_echo", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("gateway_transaction_bids_2_awaiting_terminator_echo", muxModelEventInitiatorSymbol, muxState{phase: muxGatewayTransaction, bids: muxModelBids(2), echoPending: true, echoTerminator: true}, muxModelEffectSessionReceived),

	muxModelOn("held", muxModelEventSYN, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(1)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("held", muxModelEventSymbol, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(1)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("held", muxModelEventBid, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(1, 2)}),
	muxModelOn("held", muxModelEventStartedMatching, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(1)}),
	muxModelOn("held", muxModelEventStartedOther, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(1)}),
	muxModelOn("held", muxModelEventFailed, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(1)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("held", muxModelEventFailedPhantom, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(1)}, muxModelEffectPassiveSymbol),
	muxModelNever("held", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("held", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("held", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("held", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("held", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("held", muxModelEventSessionClose, muxState{phase: muxIdle, held: true}, muxModelEffectResultCancelled),
	muxModelOn("held", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("held", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("held", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("held", muxModelEventInitiatorSymbol, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(1)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle", muxModelEventSYN, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle", muxModelEventSymbol, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	muxModelOn("idle", muxModelEventStartedMatching, muxState{phase: muxIdle}),
	muxModelOn("idle", muxModelEventStartedOther, muxState{phase: muxIdle}),
	muxModelOn("idle", muxModelEventFailed, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle", muxModelEventFailedPhantom, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol),
	muxModelNever("idle", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle", muxModelEventSessionClose, muxState{phase: muxIdle}),
	muxModelOn("idle", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle", muxModelEventInitiatorSymbol, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxIdle, absorb: 1, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("idle_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxState{phase: muxIdle, blocking: true, deferred: true, retired: true, retiredSource: 0x31}),
	muxModelNever("idle_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxIdle, absorb: 1, blocking: true, deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("idle_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectResultLost),
	muxModelOn("idle_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectResultError),
	muxModelOn("idle_absorb_pending_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, absorb: 1, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxIdle, absorb: 1, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("idle_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("idle_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxState{phase: muxIdle, blocking: true, retired: true, retiredSource: 0x31}),
	muxModelNever("idle_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxIdle, absorb: 1, blocking: true, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelOn("idle_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_absorb_pending_blocking_call_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, absorb: 1, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxIdle, absorb: 1, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("idle_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}),
	muxModelOn("idle_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("idle_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxState{phase: muxIdle, blocking: true, retired: true, retiredSource: 0x71}),
	muxModelNever("idle_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxIdle, absorb: 1, blocking: true, retired: true, retiredSource: 0x71}),
	muxModelOn("idle_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelOn("idle_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("idle_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}),
	muxModelOn("idle_absorb_pending_blocking_call_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, absorb: 1, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_absorb_pending_resync_owed_1_retired_31", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_resync_owed_1_retired_31", muxModelEventSymbol, muxState{phase: muxIdle, absorb: 1, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_resync_owed_1_retired_31", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("idle_absorb_pending_resync_owed_1_retired_31", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_absorb_pending_resync_owed_1_retired_31", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_resync_owed_1_retired_31", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_resync_owed_1_retired_31", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_absorb_pending_resync_owed_1_retired_31", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("idle_absorb_pending_resync_owed_1_retired_31", muxModelEventAbsorbDebounce, muxState{phase: muxIdle, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelNever("idle_absorb_pending_resync_owed_1_retired_31", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_absorb_pending_resync_owed_1_retired_31", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_absorb_pending_resync_owed_1_retired_31", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_absorb_pending_resync_owed_1_retired_31", muxModelEventSessionClose, muxState{phase: muxIdle, absorb: 1, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_absorb_pending_resync_owed_1_retired_31", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_absorb_pending_resync_owed_1_retired_31", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_absorb_pending_resync_owed_1_retired_31", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_absorb_pending_resync_owed_1_retired_31", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, absorb: 1, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_absorb_pending_retired_31_enforce", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_retired_31_enforce", muxModelEventSymbol, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_retired_31_enforce", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("idle_absorb_pending_retired_31_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_absorb_pending_retired_31_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_retired_31_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_retired_31_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_absorb_pending_retired_31_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("idle_absorb_pending_retired_31_enforce", muxModelEventAbsorbDebounce, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}),
	muxModelNever("idle_absorb_pending_retired_31_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_absorb_pending_retired_31_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_absorb_pending_retired_31_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_absorb_pending_retired_31_enforce", muxModelEventSessionClose, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_absorb_pending_retired_31_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_absorb_pending_retired_31_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_absorb_pending_retired_31_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_absorb_pending_retired_31_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_absorb_pending_retired_71", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_retired_71", muxModelEventSymbol, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_retired_71", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("idle_absorb_pending_retired_71", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("idle_absorb_pending_retired_71", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_retired_71", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_retired_71", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_absorb_pending_retired_71", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("idle_absorb_pending_retired_71", muxModelEventAbsorbDebounce, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}),
	muxModelNever("idle_absorb_pending_retired_71", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_absorb_pending_retired_71", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_absorb_pending_retired_71", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_absorb_pending_retired_71", muxModelEventSessionClose, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("idle_absorb_pending_retired_71", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_absorb_pending_retired_71", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_absorb_pending_retired_71", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_absorb_pending_retired_71", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_absorb_pending_retired_71_enforce", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_retired_71_enforce", muxModelEventSymbol, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_retired_71_enforce", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("idle_absorb_pending_retired_71_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("idle_absorb_pending_retired_71_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_retired_71_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_retired_71_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_absorb_pending_retired_71_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("idle_absorb_pending_retired_71_enforce", muxModelEventAbsorbDebounce, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}),
	muxModelNever("idle_absorb_pending_retired_71_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_absorb_pending_retired_71_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_absorb_pending_retired_71_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_absorb_pending_retired_71_enforce", muxModelEventSessionClose, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("idle_absorb_pending_retired_71_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_absorb_pending_retired_71_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_absorb_pending_retired_71_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_absorb_pending_retired_71_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_absorb_pending_retired_7f", muxModelEventSYN, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_retired_7f", muxModelEventSymbol, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_retired_7f", muxModelEventBid, muxState{phase: muxBidsQueued, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}),
	muxModelOn("idle_absorb_pending_retired_7f", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}),
	muxModelOn("idle_absorb_pending_retired_7f", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_retired_7f", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_absorb_pending_retired_7f", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_absorb_pending_retired_7f", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelOn("idle_absorb_pending_retired_7f", muxModelEventAbsorbDebounce, muxState{phase: muxIdle, retired: true, retiredSource: 0x7F}),
	muxModelNever("idle_absorb_pending_retired_7f", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_absorb_pending_retired_7f", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_absorb_pending_retired_7f", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_absorb_pending_retired_7f", muxModelEventSessionClose, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x7F}),
	muxModelOn("idle_absorb_pending_retired_7f", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_absorb_pending_retired_7f", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_absorb_pending_retired_7f", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_absorb_pending_retired_7f", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, absorb: 1, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_blocking_call_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, blocking: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_call_blocking_upstream", muxModelEventSymbol, muxState{phase: muxIdle, blocking: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_call_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2)}),
	muxModelOn("idle_blocking_call_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxIdle, blocking: true}),
	muxModelOn("idle_blocking_call_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxIdle, blocking: true}),
	muxModelOn("idle_blocking_call_blocking_upstream", muxModelEventFailed, muxState{phase: muxIdle, blocking: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_call_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxIdle, blocking: true}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_blocking_call_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_blocking_call_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_blocking_call_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_blocking_call_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_blocking_call_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_blocking_call_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxIdle, blocking: true}),
	muxModelOn("idle_blocking_call_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelOn("idle_blocking_call_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_blocking_call_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxIdle}),
	muxModelOn("idle_blocking_call_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, blocking: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_blocking_call_deferred_loser_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, blocking: true, deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_call_deferred_loser_blocking_upstream", muxModelEventSymbol, muxState{phase: muxIdle, blocking: true, deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_call_deferred_loser_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true}),
	muxModelOn("idle_blocking_call_deferred_loser_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxIdle, blocking: true, deferred: true}),
	muxModelOn("idle_blocking_call_deferred_loser_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxIdle, blocking: true, deferred: true}),
	muxModelOn("idle_blocking_call_deferred_loser_blocking_upstream", muxModelEventFailed, muxState{phase: muxIdle, blocking: true, deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_call_deferred_loser_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxIdle, blocking: true, deferred: true}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_blocking_call_deferred_loser_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_blocking_call_deferred_loser_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_blocking_call_deferred_loser_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_blocking_call_deferred_loser_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_blocking_call_deferred_loser_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_blocking_call_deferred_loser_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxIdle, blocking: true, deferred: true}),
	muxModelOn("idle_blocking_call_deferred_loser_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("idle_blocking_call_deferred_loser_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectResultLost),
	muxModelOn("idle_blocking_call_deferred_loser_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxIdle}, muxModelEffectResultError),
	muxModelOn("idle_blocking_call_deferred_loser_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, blocking: true, deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxIdle, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxIdle, blocking: true, deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("idle_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectResultLost),
	muxModelOn("idle_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectResultError),
	muxModelOn("idle_blocking_call_deferred_loser_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_blocking_call_resync_owed_1_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, blocking: true}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_call_resync_owed_1_blocking_upstream", muxModelEventSymbol, muxState{phase: muxIdle, blocking: true, resyncOwed: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_call_resync_owed_1_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, blocking: true, resyncOwed: 1, bids: muxModelBids(2)}),
	muxModelOn("idle_blocking_call_resync_owed_1_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxIdle, blocking: true, resyncOwed: 1}),
	muxModelOn("idle_blocking_call_resync_owed_1_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxIdle, blocking: true, resyncOwed: 1}),
	muxModelOn("idle_blocking_call_resync_owed_1_blocking_upstream", muxModelEventFailed, muxState{phase: muxIdle, blocking: true, resyncOwed: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_call_resync_owed_1_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxIdle, blocking: true, resyncOwed: 1}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_blocking_call_resync_owed_1_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_blocking_call_resync_owed_1_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_blocking_call_resync_owed_1_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_blocking_call_resync_owed_1_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_blocking_call_resync_owed_1_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_blocking_call_resync_owed_1_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxIdle, blocking: true, resyncOwed: 1}),
	muxModelOn("idle_blocking_call_resync_owed_1_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelOn("idle_blocking_call_resync_owed_1_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("idle_blocking_call_resync_owed_1_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxIdle, resyncOwed: 1}),
	muxModelOn("idle_blocking_call_resync_owed_1_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, blocking: true, resyncOwed: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_blocking_call_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_call_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxIdle, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_call_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("idle_blocking_call_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_blocking_call_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_call_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_call_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_blocking_call_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_blocking_call_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_blocking_call_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_blocking_call_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_blocking_call_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_blocking_call_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxIdle, blocking: true, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_blocking_call_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelOn("idle_blocking_call_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_blocking_call_retired_31_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_blocking_call_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_blocking_call_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_call_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxIdle, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_call_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("idle_blocking_call_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}),
	muxModelOn("idle_blocking_call_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_call_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_call_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_blocking_call_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_blocking_call_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_blocking_call_retired_71_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_blocking_call_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_blocking_call_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_blocking_call_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxIdle, blocking: true, retired: true, retiredSource: 0x71}),
	muxModelOn("idle_blocking_call_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelOn("idle_blocking_call_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("idle_blocking_call_retired_71_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}),
	muxModelOn("idle_blocking_call_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_upstream", muxModelEventSymbol, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	muxModelOn("idle_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxIdle}),
	muxModelOn("idle_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxIdle}),
	muxModelOn("idle_blocking_upstream", muxModelEventFailed, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxIdle}),
	muxModelOn("idle_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_enforce", muxModelEventSYN, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_enforce", muxModelEventSymbol, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_enforce", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}),
	muxModelOn("idle_enforce", muxModelEventStartedMatching, muxState{phase: muxIdle}),
	muxModelOn("idle_enforce", muxModelEventStartedOther, muxState{phase: muxIdle}),
	muxModelOn("idle_enforce", muxModelEventFailed, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_enforce", muxModelEventFailedPhantom, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_enforce", muxModelEventSessionClose, muxState{phase: muxIdle}),
	muxModelOn("idle_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_held", muxModelEventSYN, muxState{phase: muxIdle, held: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_held", muxModelEventSymbol, muxState{phase: muxIdle, held: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_held", muxModelEventBid, muxState{phase: muxBidsQueued, held: true, bids: muxModelBids(2)}),
	muxModelOn("idle_held", muxModelEventStartedMatching, muxState{phase: muxIdle, held: true}),
	muxModelOn("idle_held", muxModelEventStartedOther, muxState{phase: muxIdle, held: true}),
	muxModelOn("idle_held", muxModelEventFailed, muxState{phase: muxIdle, held: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_held", muxModelEventFailedPhantom, muxState{phase: muxIdle, held: true}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_held", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_held", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_held", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_held", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_held", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_held", muxModelEventSessionClose, muxState{phase: muxIdle, held: true}),
	muxModelOn("idle_held", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_held", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_held", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_held", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, held: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_reset_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_reset_blocking_upstream", muxModelEventSymbol, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_reset_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, recovery: arbitrationRecoveryReset, bids: muxModelBids(2)}),
	muxModelOn("idle_reset_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxIdle}),
	muxModelOn("idle_reset_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxIdle}),
	muxModelOn("idle_reset_blocking_upstream", muxModelEventFailed, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_reset_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_reset_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_reset_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_reset_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_reset_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_reset_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_reset_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}),
	muxModelOn("idle_reset_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_reset_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_reset_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_reset_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_reset_enforce", muxModelEventSYN, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_reset_enforce", muxModelEventSymbol, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_reset_enforce", muxModelEventBid, muxState{phase: muxBidsQueued, recovery: arbitrationRecoveryReset, bids: muxModelBids(2)}),
	muxModelOn("idle_reset_enforce", muxModelEventStartedMatching, muxState{phase: muxIdle}),
	muxModelOn("idle_reset_enforce", muxModelEventStartedOther, muxState{phase: muxIdle}),
	muxModelOn("idle_reset_enforce", muxModelEventFailed, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_reset_enforce", muxModelEventFailedPhantom, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_reset_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_reset_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_reset_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_reset_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_reset_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_reset_enforce", muxModelEventSessionClose, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}),
	muxModelOn("idle_reset_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_reset_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_reset_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_reset_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_resync_owed_1", muxModelEventSYN, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("idle_resync_owed_1", muxModelEventSymbol, muxState{phase: muxIdle, resyncOwed: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_resync_owed_1", muxModelEventBid, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}),
	muxModelOn("idle_resync_owed_1", muxModelEventStartedMatching, muxState{phase: muxIdle, resyncOwed: 1}),
	muxModelOn("idle_resync_owed_1", muxModelEventStartedOther, muxState{phase: muxIdle, resyncOwed: 1}),
	muxModelOn("idle_resync_owed_1", muxModelEventFailed, muxState{phase: muxIdle, resyncOwed: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_resync_owed_1", muxModelEventFailedPhantom, muxState{phase: muxIdle, resyncOwed: 1}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_resync_owed_1", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_resync_owed_1", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_resync_owed_1", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_resync_owed_1", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_resync_owed_1", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_resync_owed_1", muxModelEventSessionClose, muxState{phase: muxIdle, resyncOwed: 1}),
	muxModelOn("idle_resync_owed_1", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_resync_owed_1", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_resync_owed_1", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_resync_owed_1", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, resyncOwed: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_resync_owed_1_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("idle_resync_owed_1_blocking_upstream", muxModelEventSymbol, muxState{phase: muxIdle, resyncOwed: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_resync_owed_1_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}),
	muxModelOn("idle_resync_owed_1_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxIdle, resyncOwed: 1}),
	muxModelOn("idle_resync_owed_1_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxIdle, resyncOwed: 1}),
	muxModelOn("idle_resync_owed_1_blocking_upstream", muxModelEventFailed, muxState{phase: muxIdle, resyncOwed: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_resync_owed_1_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxIdle, resyncOwed: 1}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_resync_owed_1_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_resync_owed_1_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_resync_owed_1_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_resync_owed_1_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_resync_owed_1_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_resync_owed_1_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxIdle, resyncOwed: 1}),
	muxModelOn("idle_resync_owed_1_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_resync_owed_1_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_resync_owed_1_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_resync_owed_1_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, resyncOwed: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_resync_owed_1_retired_31", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("idle_resync_owed_1_retired_31", muxModelEventSymbol, muxState{phase: muxIdle, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_resync_owed_1_retired_31", muxModelEventBid, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("idle_resync_owed_1_retired_31", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_resync_owed_1_retired_31", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_resync_owed_1_retired_31", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_resync_owed_1_retired_31", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_resync_owed_1_retired_31", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_resync_owed_1_retired_31", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_resync_owed_1_retired_31", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_resync_owed_1_retired_31", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_resync_owed_1_retired_31", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_resync_owed_1_retired_31", muxModelEventSessionClose, muxState{phase: muxIdle, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_resync_owed_1_retired_31", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_resync_owed_1_retired_31", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_resync_owed_1_retired_31", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_resync_owed_1_retired_31", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_resync_owed_1_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("idle_resync_owed_1_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxIdle, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_resync_owed_1_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("idle_resync_owed_1_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_resync_owed_1_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_resync_owed_1_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_resync_owed_1_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_resync_owed_1_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_resync_owed_1_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_resync_owed_1_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_resync_owed_1_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_resync_owed_1_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_resync_owed_1_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxIdle, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_resync_owed_1_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_resync_owed_1_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_resync_owed_1_retired_31_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_resync_owed_1_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_resync_owed_2", muxModelEventSYN, muxState{phase: muxIdle, resyncOwed: 1}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("idle_resync_owed_2", muxModelEventSymbol, muxState{phase: muxIdle, resyncOwed: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_resync_owed_2", muxModelEventBid, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}),
	muxModelOn("idle_resync_owed_2", muxModelEventStartedMatching, muxState{phase: muxIdle, resyncOwed: 2}),
	muxModelOn("idle_resync_owed_2", muxModelEventStartedOther, muxState{phase: muxIdle, resyncOwed: 2}),
	muxModelOn("idle_resync_owed_2", muxModelEventFailed, muxState{phase: muxIdle, resyncOwed: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_resync_owed_2", muxModelEventFailedPhantom, muxState{phase: muxIdle, resyncOwed: 2}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_resync_owed_2", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_resync_owed_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_resync_owed_2", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_resync_owed_2", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_resync_owed_2", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_resync_owed_2", muxModelEventSessionClose, muxState{phase: muxIdle, resyncOwed: 2}),
	muxModelOn("idle_resync_owed_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_resync_owed_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_resync_owed_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_resync_owed_2", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, resyncOwed: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_resync_owed_2_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, resyncOwed: 1}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("idle_resync_owed_2_blocking_upstream", muxModelEventSymbol, muxState{phase: muxIdle, resyncOwed: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_resync_owed_2_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}),
	muxModelOn("idle_resync_owed_2_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxIdle, resyncOwed: 2}),
	muxModelOn("idle_resync_owed_2_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxIdle, resyncOwed: 2}),
	muxModelOn("idle_resync_owed_2_blocking_upstream", muxModelEventFailed, muxState{phase: muxIdle, resyncOwed: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_resync_owed_2_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxIdle, resyncOwed: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("idle_resync_owed_2_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_resync_owed_2_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_resync_owed_2_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_resync_owed_2_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_resync_owed_2_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_resync_owed_2_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxIdle, resyncOwed: 2}),
	muxModelOn("idle_resync_owed_2_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_resync_owed_2_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_resync_owed_2_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_resync_owed_2_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, resyncOwed: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_retired_31", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_31", muxModelEventSymbol, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_31", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("idle_retired_31", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_retired_31", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_31", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_31", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_retired_31", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_retired_31", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_retired_31", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_retired_31", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_retired_31", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_retired_31", muxModelEventSessionClose, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_retired_31", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_retired_31", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_retired_31", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_retired_31", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("idle_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_retired_31_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_retired_31_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_retired_31_enforce", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_31_enforce", muxModelEventSymbol, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_31_enforce", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("idle_retired_31_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_retired_31_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_31_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_31_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_retired_31_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_retired_31_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_retired_31_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_retired_31_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_retired_31_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_retired_31_enforce", muxModelEventSessionClose, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}),
	muxModelOn("idle_retired_31_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_retired_31_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_retired_31_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_retired_31_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_retired_71", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_71", muxModelEventSymbol, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_71", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("idle_retired_71", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("idle_retired_71", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_71", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_71", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_retired_71", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_retired_71", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_retired_71", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_retired_71", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_retired_71", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_retired_71", muxModelEventSessionClose, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}),
	muxModelOn("idle_retired_71", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_retired_71", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_retired_71", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_retired_71", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("idle_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("idle_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_retired_71_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}),
	muxModelOn("idle_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_retired_71_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_retired_71_enforce", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_71_enforce", muxModelEventSymbol, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_71_enforce", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("idle_retired_71_enforce", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("idle_retired_71_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_71_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_71_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_retired_71_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_retired_71_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_retired_71_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_retired_71_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_retired_71_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_retired_71_enforce", muxModelEventSessionClose, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}),
	muxModelOn("idle_retired_71_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_retired_71_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_retired_71_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_retired_71_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_retired_7f", muxModelEventSYN, muxState{phase: muxIdle, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_7f", muxModelEventSymbol, muxState{phase: muxIdle, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_7f", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}),
	muxModelOn("idle_retired_7f", muxModelEventStartedMatching, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}),
	muxModelOn("idle_retired_7f", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_7f", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_retired_7f", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_retired_7f", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_retired_7f", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_retired_7f", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_retired_7f", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_retired_7f", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_retired_7f", muxModelEventSessionClose, muxState{phase: muxIdle, retired: true, retiredSource: 0x7F}),
	muxModelOn("idle_retired_7f", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_retired_7f", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_retired_7f", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_retired_7f", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("idle_telegram_live_enforce", muxModelEventSYN, muxState{phase: muxIdle, telegramLive: true}),
	muxModelOn("idle_telegram_live_enforce", muxModelEventSymbol, muxState{phase: muxIdle, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_telegram_live_enforce", muxModelEventBid, muxState{phase: muxBidsQueued, bids: muxModelBids(2), telegramLive: true}),
	muxModelOn("idle_telegram_live_enforce", muxModelEventStartedMatching, muxState{phase: muxIdle, telegramLive: true}),
	muxModelOn("idle_telegram_live_enforce", muxModelEventStartedOther, muxState{phase: muxIdle, telegramLive: true}),
	muxModelOn("idle_telegram_live_enforce", muxModelEventFailed, muxState{phase: muxIdle, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("idle_telegram_live_enforce", muxModelEventFailedPhantom, muxState{phase: muxIdle, telegramLive: true}, muxModelEffectPassiveSymbol),
	muxModelNever("idle_telegram_live_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("idle_telegram_live_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("idle_telegram_live_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("idle_telegram_live_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("idle_telegram_live_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("idle_telegram_live_enforce", muxModelEventSessionClose, muxState{phase: muxIdle, telegramLive: true}),
	muxModelOn("idle_telegram_live_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("idle_telegram_live_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("idle_telegram_live_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("idle_telegram_live_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxIdle, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("reset_recovery", muxModelEventSYN, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("reset_recovery", muxModelEventSymbol, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("reset_recovery", muxModelEventBid, muxState{phase: muxBidsQueued, recovery: arbitrationRecoveryReset, bids: muxModelBids(2)}),
	muxModelOn("reset_recovery", muxModelEventStartedMatching, muxState{phase: muxIdle}),
	muxModelOn("reset_recovery", muxModelEventStartedOther, muxState{phase: muxIdle}),
	muxModelOn("reset_recovery", muxModelEventFailed, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("reset_recovery", muxModelEventFailedPhantom, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol),
	muxModelNever("reset_recovery", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("reset_recovery", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("reset_recovery", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("reset_recovery", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("reset_recovery", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("reset_recovery", muxModelEventSessionClose, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}),
	muxModelOn("reset_recovery", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("reset_recovery", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("reset_recovery", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("reset_recovery", muxModelEventInitiatorSymbol, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned", muxModelEventSymbol, muxState{phase: muxSessionOwned, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned", muxModelEventBid, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, owner: 1}),
	muxModelOn("session_owned", muxModelEventStartedOther, muxState{phase: muxSessionOwned, owner: 1}),
	muxModelOn("session_owned", muxModelEventFailed, muxState{phase: muxSessionOwned, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, owner: 1}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned", muxModelEventIdleGrace, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned", muxModelEventOwnershipLimit, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("session_owned", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_at_syn_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_at_syn_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_at_syn_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_at_syn_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_at_syn_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_at_syn_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_at_syn_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_at_syn_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("session_owned_at_syn_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_at_syn_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_at_syn_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_at_syn_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_at_syn_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("session_owned_at_syn_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_at_syn_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_at_syn_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_at_syn_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("session_owned_at_syn_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_at_syn_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_at_syn_owner_1_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_at_syn_owner_1_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxStartBlocking, blocking: true, resyncOwed: 1, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("session_owned_at_syn_owner_1_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_at_syn_owner_1_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_bids_2_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, owner: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_at_syn_owner_1_resync_owed_2_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_at_syn_owner_1_resync_owed_2_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_retired_31_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("session_owned_at_syn_owner_1_resync_owed_2_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_at_syn_owner_1_resync_owed_2_retired_31_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_at_syn_owner_1_resync_owed_2_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_at_syn_owner_1_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_at_syn_owner_1_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_at_syn_owner_1_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_at_syn_owner_1_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_at_syn_owner_1_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_at_syn_owner_1_retired_31_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_1_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_at_syn_owner_1_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_at_syn_owner_1_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("session_owned_at_syn_owner_1_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_at_syn_owner_1_retired_31_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_at_syn_owner_1_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_at_syn_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_at_syn_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_at_syn_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_at_syn_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_at_syn_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_at_syn_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_at_syn_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_at_syn_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("session_owned_at_syn_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_at_syn_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_at_syn_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_at_syn_owner_2_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_at_syn_owner_2_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("session_owned_at_syn_owner_2_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_at_syn_owner_2_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_bids_2_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 2, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 2, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 2, retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 2, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 2, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 2, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_at_syn_owner_2_resync_owed_1_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_at_syn_owner_2_resync_owed_1_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_retired_71_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 2, retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("session_owned_at_syn_owner_2_resync_owed_1_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_at_syn_owner_2_resync_owed_1_retired_71_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_at_syn_owner_2_resync_owed_1_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 2, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_at_syn_owner_2_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 2, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_at_syn_owner_2_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_at_syn_owner_2_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_at_syn_owner_2_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_at_syn_owner_2_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_at_syn_owner_2_retired_71_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_at_syn_owner_2_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_at_syn_owner_2_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_at_syn_owner_2_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("session_owned_at_syn_owner_2_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_at_syn_owner_2_retired_71_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_at_syn_owner_2_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_other", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_other", muxModelEventSymbol, muxState{phase: muxSessionOwned, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_other", muxModelEventBid, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_other", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, owner: 2}),
	muxModelOn("session_owned_other", muxModelEventStartedOther, muxState{phase: muxSessionOwned, owner: 2}),
	muxModelOn("session_owned_other", muxModelEventFailed, muxState{phase: muxSessionOwned, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_other", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, owner: 2}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_other", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_other", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_other", muxModelEventIdleGrace, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_other", muxModelEventOwnershipLimit, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_other", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_other", muxModelEventSessionClose, muxState{phase: muxSessionOwned, owner: 2}),
	muxModelOn("session_owned_other", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("session_owned_other", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_other", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_other", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_1_bids_2", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_bids_2", muxModelEventSymbol, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_bids_2", muxModelEventBid, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_owner_1_bids_2", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_1_bids_2", muxModelEventStartedOther, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_1_bids_2", muxModelEventFailed, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_bids_2", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_1_bids_2", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_1_bids_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_1_bids_2", muxModelEventIdleGrace, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_bids_2", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_bids_2", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_bids_2", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_bids_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("session_owned_owner_1_bids_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_1_bids_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_1_bids_2", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_1_bids_2_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_bids_2_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_bids_2_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_owner_1_bids_2_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_1_bids_2_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_1_bids_2_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_bids_2_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_1_bids_2_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_1_bids_2_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_1_bids_2_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_bids_2_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_bids_2_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_bids_2_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_bids_2_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("session_owned_owner_1_bids_2_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_1_bids_2_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_1_bids_2_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("session_owned_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_1_bids_2_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_1_bids_2_telegram_live_enforce", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2), telegramLive: true}),
	muxModelOn("session_owned_owner_1_bids_2_telegram_live_enforce", muxModelEventSymbol, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2), telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_bids_2_telegram_live_enforce", muxModelEventBid, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2), telegramLive: true}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_owner_1_bids_2_telegram_live_enforce", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2), telegramLive: true}),
	muxModelOn("session_owned_owner_1_bids_2_telegram_live_enforce", muxModelEventStartedOther, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2), telegramLive: true}),
	muxModelOn("session_owned_owner_1_bids_2_telegram_live_enforce", muxModelEventFailed, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2), telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_bids_2_telegram_live_enforce", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2), telegramLive: true}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_1_bids_2_telegram_live_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_1_bids_2_telegram_live_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_1_bids_2_telegram_live_enforce", muxModelEventIdleGrace, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("session_owned_owner_1_bids_2_telegram_live_enforce", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, bids: muxModelBids(2), telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_bids_2_telegram_live_enforce", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("session_owned_owner_1_bids_2_telegram_live_enforce", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("session_owned_owner_1_bids_2_telegram_live_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("session_owned_owner_1_bids_2_telegram_live_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_1_bids_2_telegram_live_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_1_bids_2_telegram_live_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2), telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_1_blocking_call_bids_2_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_1_blocking_call_bids_2_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_1_blocking_call_bids_2_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2), deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2), deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2), deferred: true}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2), deferred: true}),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2), deferred: true}),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2), deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2), deferred: true}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2), deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultLost),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}, muxModelEffectResultError),
	muxModelOn("session_owned_owner_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2), deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_1_blocking_call_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, blocking: true, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_call_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, blocking: true, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_call_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_1_blocking_call_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, blocking: true, owner: 1}),
	muxModelOn("session_owned_owner_1_blocking_call_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, blocking: true, owner: 1}),
	muxModelOn("session_owned_owner_1_blocking_call_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, blocking: true, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_call_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, blocking: true, owner: 1}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_1_blocking_call_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_1_blocking_call_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_1_blocking_call_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle, blocking: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_call_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxIdle, blocking: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_call_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_blocking_call_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_blocking_call_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelOn("session_owned_owner_1_blocking_call_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_blocking_call_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxSessionOwned, owner: 1}),
	muxModelOn("session_owned_owner_1_blocking_call_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, blocking: true, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_1_blocking_call_deferred_loser_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, blocking: true, owner: 1, deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_call_deferred_loser_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, blocking: true, owner: 1, deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_call_deferred_loser_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2), deferred: true}),
	muxModelOn("session_owned_owner_1_blocking_call_deferred_loser_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, blocking: true, owner: 1, deferred: true}),
	muxModelOn("session_owned_owner_1_blocking_call_deferred_loser_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, blocking: true, owner: 1, deferred: true}),
	muxModelOn("session_owned_owner_1_blocking_call_deferred_loser_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, blocking: true, owner: 1, deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_call_deferred_loser_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, blocking: true, owner: 1, deferred: true}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_1_blocking_call_deferred_loser_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_1_blocking_call_deferred_loser_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_1_blocking_call_deferred_loser_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle, blocking: true, deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_call_deferred_loser_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxIdle, blocking: true, deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_call_deferred_loser_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_blocking_call_deferred_loser_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_blocking_call_deferred_loser_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("session_owned_owner_1_blocking_call_deferred_loser_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectResultLost),
	muxModelOn("session_owned_owner_1_blocking_call_deferred_loser_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxSessionOwned, owner: 1}, muxModelEffectResultError),
	muxModelOn("session_owned_owner_1_blocking_call_deferred_loser_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, blocking: true, owner: 1, deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_1_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_1_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, owner: 1}),
	muxModelOn("session_owned_owner_1_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, owner: 1}),
	muxModelOn("session_owned_owner_1_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, owner: 1}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_1_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_1_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_1_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("session_owned_owner_1_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_1_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_1_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_1_resync_owed_1", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1", muxModelEventSymbol, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1", muxModelEventBid, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_1_resync_owed_1", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1}),
	muxModelOn("session_owned_owner_1_resync_owed_1", muxModelEventStartedOther, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1}),
	muxModelOn("session_owned_owner_1_resync_owed_1", muxModelEventFailed, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_1_resync_owed_1", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_1_resync_owed_1", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_1_resync_owed_1", muxModelEventIdleGrace, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1", muxModelEventOwnershipLimit, muxState{phase: muxIdle, resyncOwed: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_1", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_1", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("session_owned_owner_1_resync_owed_1", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_1_resync_owed_1", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_1_resync_owed_1", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2", muxModelEventSymbol, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2", muxModelEventBid, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2", muxModelEventStartedOther, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2", muxModelEventFailed, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_1_resync_owed_1_bids_2", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_1_resync_owed_1_bids_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2", muxModelEventIdleGrace, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("session_owned_owner_1_resync_owed_1_bids_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_1_resync_owed_1_bids_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_1_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_1_resync_owed_1_bids_2_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("session_owned_owner_1_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_1_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("session_owned_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_1_resync_owed_1_bids_2_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_1_resync_owed_1_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_1_resync_owed_1_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1}),
	muxModelOn("session_owned_owner_1_resync_owed_1_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1}),
	muxModelOn("session_owned_owner_1_resync_owed_1_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_1_resync_owed_1_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_1_resync_owed_1_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_1_resync_owed_1_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxIdle, resyncOwed: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_1_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_1_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("session_owned_owner_1_resync_owed_1_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_1_resync_owed_1_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_1_resync_owed_1_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxIdle, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("session_owned_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_1_resync_owed_1_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_1_resync_owed_2", muxModelEventSYN, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_2", muxModelEventSymbol, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_2", muxModelEventBid, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_1_resync_owed_2", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1}),
	muxModelOn("session_owned_owner_1_resync_owed_2", muxModelEventStartedOther, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1}),
	muxModelOn("session_owned_owner_1_resync_owed_2", muxModelEventFailed, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_2", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_1_resync_owed_2", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_1_resync_owed_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_1_resync_owed_2", muxModelEventIdleGrace, muxState{phase: muxIdle, resyncOwed: 1}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_2", muxModelEventOwnershipLimit, muxState{phase: muxIdle, resyncOwed: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_2", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_2", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("session_owned_owner_1_resync_owed_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_1_resync_owed_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_1_resync_owed_2", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2", muxModelEventSYN, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2", muxModelEventSymbol, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2", muxModelEventBid, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2", muxModelEventStartedOther, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2", muxModelEventFailed, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_1_resync_owed_2_bids_2", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_1_resync_owed_2_bids_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2", muxModelEventIdleGrace, muxState{phase: muxStartAsync, resyncOwed: 1, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("session_owned_owner_1_resync_owed_2_bids_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_1_resync_owed_2_bids_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("session_owned_owner_1_resync_owed_2_bids_2_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_1_resync_owed_2_bids_2_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxStartBlocking, blocking: true, resyncOwed: 1, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, resyncOwed: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("session_owned_owner_1_resync_owed_2_bids_2_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_1_resync_owed_2_bids_2_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_1_resync_owed_2_bids_2_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_1_resync_owed_2_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_2_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_2_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_1_resync_owed_2_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1}),
	muxModelOn("session_owned_owner_1_resync_owed_2_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1}),
	muxModelOn("session_owned_owner_1_resync_owed_2_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_2_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelNever("session_owned_owner_1_resync_owed_2_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_1_resync_owed_2_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_1_resync_owed_2_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle, resyncOwed: 1}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_2_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxIdle, resyncOwed: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_resync_owed_2_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_2_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_resync_owed_2_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("session_owned_owner_1_resync_owed_2_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_1_resync_owed_2_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_1_resync_owed_2_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_1_retired_31_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_retired_31_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_retired_31_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_retired_31_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_retired_31_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_retired_31_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_retired_31_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_1_retired_31_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_1_retired_31_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_1_retired_31_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_retired_31_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxIdle, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_1_retired_31_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_retired_31_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}),
	muxModelOn("session_owned_owner_1_retired_31_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("session_owned_owner_1_retired_31_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_1_retired_31_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_1_retired_31_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, owner: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_2_bids_2", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_bids_2", muxModelEventSymbol, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_bids_2", muxModelEventBid, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_owner_2_bids_2", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_bids_2", muxModelEventStartedOther, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_bids_2", muxModelEventFailed, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_bids_2", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_2_bids_2", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_2_bids_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_2_bids_2", muxModelEventIdleGrace, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_bids_2", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_bids_2", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_bids_2", muxModelEventSessionClose, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_bids_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("session_owned_owner_2_bids_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_2_bids_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_2_bids_2", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_2_bids_2_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_bids_2_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_bids_2_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_owner_2_bids_2_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_bids_2_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_bids_2_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_bids_2_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_2_bids_2_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_2_bids_2_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_2_bids_2_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_bids_2_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_bids_2_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_bids_2_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_bids_2_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("session_owned_owner_2_bids_2_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_2_bids_2_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_2_bids_2_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_2_bids_2_enforce", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_bids_2_enforce", muxModelEventSymbol, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_bids_2_enforce", muxModelEventBid, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_owner_2_bids_2_enforce", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_bids_2_enforce", muxModelEventStartedOther, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_bids_2_enforce", muxModelEventFailed, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_bids_2_enforce", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_2_bids_2_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_2_bids_2_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_2_bids_2_enforce", muxModelEventIdleGrace, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_bids_2_enforce", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_bids_2_enforce", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_bids_2_enforce", muxModelEventSessionClose, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_bids_2_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("session_owned_owner_2_bids_2_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_2_bids_2_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_2_bids_2_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("session_owned_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_2_bids_2_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_2_blocking_call_bids_2_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, blocking: true, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_call_bids_2_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, blocking: true, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_call_bids_2_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, blocking: true, owner: 2, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_owner_2_blocking_call_bids_2_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, blocking: true, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_blocking_call_bids_2_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, blocking: true, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_blocking_call_bids_2_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, blocking: true, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_call_bids_2_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, blocking: true, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_2_blocking_call_bids_2_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_2_blocking_call_bids_2_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_2_blocking_call_bids_2_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_call_bids_2_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_call_bids_2_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_blocking_call_bids_2_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxSessionOwned, blocking: true, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_blocking_call_bids_2_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("session_owned_owner_2_blocking_call_bids_2_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_blocking_call_bids_2_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_blocking_call_bids_2_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, blocking: true, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_2_blocking_call_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, blocking: true, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_call_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, blocking: true, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_call_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, blocking: true, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_blocking_call_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, blocking: true, owner: 2}),
	muxModelOn("session_owned_owner_2_blocking_call_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, blocking: true, owner: 2}),
	muxModelOn("session_owned_owner_2_blocking_call_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, blocking: true, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_call_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, blocking: true, owner: 2}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_2_blocking_call_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_2_blocking_call_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_2_blocking_call_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle, blocking: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_call_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxIdle, blocking: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_call_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_blocking_call_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxSessionOwned, blocking: true, owner: 2}),
	muxModelOn("session_owned_owner_2_blocking_call_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelOn("session_owned_owner_2_blocking_call_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_blocking_call_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxSessionOwned, owner: 2}),
	muxModelOn("session_owned_owner_2_blocking_call_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, blocking: true, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, blocking: true, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, blocking: true, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, blocking: true, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, blocking: true, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, blocking: true, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, blocking: true, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, blocking: true, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_2_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_2_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxBidsQueued, blocking: true, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, blocking: true, resyncOwed: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxSessionOwned, blocking: true, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_bids_2_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, blocking: true, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, blocking: true, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, blocking: true, resyncOwed: 1, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, blocking: true, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, blocking: true, resyncOwed: 1, owner: 2}),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, blocking: true, resyncOwed: 1, owner: 2}),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, blocking: true, resyncOwed: 1, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, blocking: true, resyncOwed: 1, owner: 2}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle, blocking: true}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxIdle, blocking: true, resyncOwed: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxSessionOwned, blocking: true, resyncOwed: 1, owner: 2}),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, owner: 2, retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2}),
	muxModelOn("session_owned_owner_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, blocking: true, resyncOwed: 1, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_2_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, owner: 2}),
	muxModelOn("session_owned_owner_2_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, owner: 2}),
	muxModelOn("session_owned_owner_2_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, owner: 2}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_2_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_2_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_2_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxSessionOwned, owner: 2}),
	muxModelOn("session_owned_owner_2_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("session_owned_owner_2_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_2_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_2_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_2_enforce", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 2}),
	muxModelOn("session_owned_owner_2_enforce", muxModelEventSymbol, muxState{phase: muxSessionOwned, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_enforce", muxModelEventBid, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_enforce", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, owner: 2}),
	muxModelOn("session_owned_owner_2_enforce", muxModelEventStartedOther, muxState{phase: muxSessionOwned, owner: 2}),
	muxModelOn("session_owned_owner_2_enforce", muxModelEventFailed, muxState{phase: muxSessionOwned, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_enforce", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, owner: 2}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_2_enforce", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_2_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_2_enforce", muxModelEventIdleGrace, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_enforce", muxModelEventOwnershipLimit, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_enforce", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_enforce", muxModelEventSessionClose, muxState{phase: muxSessionOwned, owner: 2}),
	muxModelOn("session_owned_owner_2_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("session_owned_owner_2_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_2_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_2_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_2_resync_owed_1", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_resync_owed_1", muxModelEventSymbol, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_resync_owed_1", muxModelEventBid, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_resync_owed_1", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2}),
	muxModelOn("session_owned_owner_2_resync_owed_1", muxModelEventStartedOther, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2}),
	muxModelOn("session_owned_owner_2_resync_owed_1", muxModelEventFailed, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_resync_owed_1", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_2_resync_owed_1", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_2_resync_owed_1", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_2_resync_owed_1", muxModelEventIdleGrace, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_resync_owed_1", muxModelEventOwnershipLimit, muxState{phase: muxIdle, resyncOwed: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_resync_owed_1", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_resync_owed_1", muxModelEventSessionClose, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2}),
	muxModelOn("session_owned_owner_2_resync_owed_1", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("session_owned_owner_2_resync_owed_1", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_2_resync_owed_1", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_2_resync_owed_1", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2", muxModelEventSymbol, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2", muxModelEventBid, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2", muxModelEventStartedOther, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2", muxModelEventFailed, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_2_resync_owed_1_bids_2", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_2_resync_owed_1_bids_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2", muxModelEventIdleGrace, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStart, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2", muxModelEventSessionClose, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("session_owned_owner_2_resync_owed_1_bids_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_2_resync_owed_1_bids_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_2_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_2_resync_owed_1_bids_2_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectAdapterStartBlocking, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxBidsQueued, resyncOwed: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("session_owned_owner_2_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_2_resync_owed_1_bids_2_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_2_resync_owed_1_bids_2_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_2_resync_owed_1_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_resync_owed_1_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_resync_owed_1_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2, bids: muxModelBids(2)}),
	muxModelOn("session_owned_owner_2_resync_owed_1_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2}),
	muxModelOn("session_owned_owner_2_resync_owed_1_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2}),
	muxModelOn("session_owned_owner_2_resync_owed_1_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_resync_owed_1_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_2_resync_owed_1_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_2_resync_owed_1_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_2_resync_owed_1_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_resync_owed_1_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxIdle, resyncOwed: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_resync_owed_1_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_resync_owed_1_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2}),
	muxModelOn("session_owned_owner_2_resync_owed_1_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("session_owned_owner_2_resync_owed_1_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_2_resync_owed_1_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_2_resync_owed_1_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_owned_owner_2_retired_71_blocking_upstream", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 2, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_retired_71_blocking_upstream", muxModelEventSymbol, muxState{phase: muxSessionOwned, owner: 2, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_retired_71_blocking_upstream", muxModelEventBid, muxState{phase: muxSessionOwned, owner: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_retired_71_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_retired_71_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_retired_71_blocking_upstream", muxModelEventFailed, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_retired_71_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, recovery: arbitrationRecoveryAtSYN, owner: 2, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol),
	muxModelNever("session_owned_owner_2_retired_71_blocking_upstream", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_owned_owner_2_retired_71_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_owned_owner_2_retired_71_blocking_upstream", muxModelEventIdleGrace, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_retired_71_blocking_upstream", muxModelEventOwnershipLimit, muxState{phase: muxIdle, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_owned_owner_2_retired_71_blocking_upstream", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_retired_71_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxSessionOwned, owner: 2, retired: true, retiredSource: 0x71}),
	muxModelOn("session_owned_owner_2_retired_71_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("session_owned_owner_2_retired_71_blocking_upstream", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_owned_owner_2_retired_71_blocking_upstream", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_owned_owner_2_retired_71_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, owner: 2, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("session_telegram_live", muxModelEventSYN, muxState{phase: muxSessionOwned, owner: 1, telegramLive: true}),
	muxModelOn("session_telegram_live", muxModelEventSymbol, muxState{phase: muxSessionOwned, owner: 1, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_telegram_live", muxModelEventBid, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2), telegramLive: true}),
	muxModelOn("session_telegram_live", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, owner: 1, telegramLive: true}),
	muxModelOn("session_telegram_live", muxModelEventStartedOther, muxState{phase: muxSessionOwned, owner: 1, telegramLive: true}),
	muxModelOn("session_telegram_live", muxModelEventFailed, muxState{phase: muxSessionOwned, owner: 1, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_telegram_live", muxModelEventFailedPhantom, muxState{phase: muxSessionOwned, owner: 1, telegramLive: true}, muxModelEffectPassiveSymbol),
	muxModelNever("session_telegram_live", muxModelEventStartDeadline, muxNoStartDeadline),
	muxModelNever("session_telegram_live", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelOn("session_telegram_live", muxModelEventIdleGrace, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("session_telegram_live", muxModelEventOwnershipLimit, muxState{phase: muxIdle, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("session_telegram_live", muxModelEventAbandonWatchdog, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("session_telegram_live", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31, telegramLive: true}),
	muxModelOn("session_telegram_live", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset),
	muxModelNever("session_telegram_live", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("session_telegram_live", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("session_telegram_live", muxModelEventInitiatorSymbol, muxState{phase: muxSessionOwned, owner: 1, telegramLive: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("start_async", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(1, 0x31)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_async", muxModelEventSymbol, muxState{phase: muxStartAsync, start: muxModelSelected(1, 0x31)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_async", muxModelEventBid, muxState{phase: muxStartAsync, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}),
	muxModelOn("start_async", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionStarted, muxModelEffectSessionReceived, muxModelEffectResultGranted),
	muxModelOn("start_async", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectResultError),
	muxModelOn("start_async", muxModelEventStartDeadline, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x31}, muxModelEffectResultError),
	muxModelNever("start_async", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("start_async", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("start_async", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("start_async", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("start_async", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("start_async", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("start_async", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("start_async", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("start_async", muxModelEventInitiatorSymbol, muxState{phase: muxStartAsync, start: muxModelSelected(1, 0x31)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("start_async_resync_owed", muxModelEventSYN, muxState{phase: muxStartAsync, resyncOwed: 1, start: muxModelSelected(1, 0x31)}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("start_async_resync_owed", muxModelEventSymbol, muxState{phase: muxStartAsync, resyncOwed: 2, start: muxModelSelected(1, 0x31)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_async_resync_owed", muxModelEventBid, muxState{phase: muxStartAsync, resyncOwed: 2, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}),
	muxModelOn("start_async_resync_owed", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionStarted, muxModelEffectResultGranted),
	muxModelOn("start_async_resync_owed", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_resync_owed", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_resync_owed", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_resync_owed", muxModelEventStartDeadline, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectResultError),
	muxModelNever("start_async_resync_owed", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("start_async_resync_owed", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("start_async_resync_owed", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("start_async_resync_owed", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("start_async_resync_owed", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 2, retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("start_async_resync_owed", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("start_async_resync_owed", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("start_async_resync_owed", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("start_async_resync_owed", muxModelEventInitiatorSymbol, muxState{phase: muxStartAsync, resyncOwed: 2, start: muxModelSelected(1, 0x31)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("start_async_selected_gateway_bids_2", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(gatewaySessionID, 0x7F), bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_async_selected_gateway_bids_2", muxModelEventSymbol, muxState{phase: muxStartAsync, start: muxModelSelected(gatewaySessionID, 0x7F), bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_async_selected_gateway_bids_2", muxModelEventBid, muxState{phase: muxStartAsync, start: muxModelSelected(gatewaySessionID, 0x7F), bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("start_async_selected_gateway_bids_2", muxModelEventStartedMatching, muxState{phase: muxGatewayGrace, bids: muxModelBids(2)}, muxModelEffectSessionReceived, muxModelEffectResultGranted),
	muxModelOn("start_async_selected_gateway_bids_2", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_selected_gateway_bids_2", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_selected_gateway_bids_2", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectResultError),
	muxModelOn("start_async_selected_gateway_bids_2", muxModelEventStartDeadline, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x7F}, muxModelEffectResultError),
	muxModelNever("start_async_selected_gateway_bids_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("start_async_selected_gateway_bids_2", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("start_async_selected_gateway_bids_2", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("start_async_selected_gateway_bids_2", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("start_async_selected_gateway_bids_2", muxModelEventSessionClose, muxState{phase: muxStartAsync, start: muxModelSelected(gatewaySessionID, 0x7F), bids: muxModelBids(2)}),
	muxModelOn("start_async_selected_gateway_bids_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("start_async_selected_gateway_bids_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("start_async_selected_gateway_bids_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("start_async_selected_gateway_bids_2", muxModelEventInitiatorSymbol, muxState{phase: muxStartAsync, start: muxModelSelected(gatewaySessionID, 0x7F), bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("start_async_selected_session_1_bids_2", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_async_selected_session_1_bids_2", muxModelEventSymbol, muxState{phase: muxStartAsync, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_async_selected_session_1_bids_2", muxModelEventBid, muxState{phase: muxStartAsync, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("start_async_selected_session_1_bids_2", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionStarted, muxModelEffectSessionReceived, muxModelEffectResultGranted),
	muxModelOn("start_async_selected_session_1_bids_2", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_selected_session_1_bids_2", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_selected_session_1_bids_2", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectResultError),
	muxModelOn("start_async_selected_session_1_bids_2", muxModelEventStartDeadline, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultError),
	muxModelNever("start_async_selected_session_1_bids_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("start_async_selected_session_1_bids_2", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("start_async_selected_session_1_bids_2", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("start_async_selected_session_1_bids_2", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("start_async_selected_session_1_bids_2", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("start_async_selected_session_1_bids_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("start_async_selected_session_1_bids_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("start_async_selected_session_1_bids_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("start_async_selected_session_1_bids_2", muxModelEventInitiatorSymbol, muxState{phase: muxStartAsync, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("start_async_selected_session_1_resync_owed_1", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(1, 0x31)}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("start_async_selected_session_1_resync_owed_1", muxModelEventSymbol, muxState{phase: muxStartAsync, resyncOwed: 1, start: muxModelSelected(1, 0x31)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_async_selected_session_1_resync_owed_1", muxModelEventBid, muxState{phase: muxStartAsync, resyncOwed: 1, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}),
	muxModelOn("start_async_selected_session_1_resync_owed_1", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionStarted, muxModelEffectSessionReceived, muxModelEffectResultGranted),
	muxModelOn("start_async_selected_session_1_resync_owed_1", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_selected_session_1_resync_owed_1", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_selected_session_1_resync_owed_1", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectResultError),
	muxModelOn("start_async_selected_session_1_resync_owed_1", muxModelEventStartDeadline, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectResultError),
	muxModelNever("start_async_selected_session_1_resync_owed_1", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("start_async_selected_session_1_resync_owed_1", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("start_async_selected_session_1_resync_owed_1", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("start_async_selected_session_1_resync_owed_1", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("start_async_selected_session_1_resync_owed_1", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("start_async_selected_session_1_resync_owed_1", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("start_async_selected_session_1_resync_owed_1", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("start_async_selected_session_1_resync_owed_1", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("start_async_selected_session_1_resync_owed_1", muxModelEventInitiatorSymbol, muxState{phase: muxStartAsync, resyncOwed: 1, start: muxModelSelected(1, 0x31)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("start_async_selected_session_1_resync_owed_1_bids_2", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("start_async_selected_session_1_resync_owed_1_bids_2", muxModelEventSymbol, muxState{phase: muxStartAsync, resyncOwed: 1, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_async_selected_session_1_resync_owed_1_bids_2", muxModelEventBid, muxState{phase: muxStartAsync, resyncOwed: 1, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("start_async_selected_session_1_resync_owed_1_bids_2", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionStarted, muxModelEffectSessionReceived, muxModelEffectResultGranted),
	muxModelOn("start_async_selected_session_1_resync_owed_1_bids_2", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_selected_session_1_resync_owed_1_bids_2", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_selected_session_1_resync_owed_1_bids_2", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectResultError),
	muxModelOn("start_async_selected_session_1_resync_owed_1_bids_2", muxModelEventStartDeadline, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultError),
	muxModelNever("start_async_selected_session_1_resync_owed_1_bids_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("start_async_selected_session_1_resync_owed_1_bids_2", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("start_async_selected_session_1_resync_owed_1_bids_2", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("start_async_selected_session_1_resync_owed_1_bids_2", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("start_async_selected_session_1_resync_owed_1_bids_2", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("start_async_selected_session_1_resync_owed_1_bids_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("start_async_selected_session_1_resync_owed_1_bids_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("start_async_selected_session_1_resync_owed_1_bids_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("start_async_selected_session_1_resync_owed_1_bids_2", muxModelEventInitiatorSymbol, muxState{phase: muxStartAsync, resyncOwed: 1, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("start_async_selected_session_1_resync_owed_2_bids_2", muxModelEventSYN, muxState{phase: muxStartAsync, resyncOwed: 1, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("start_async_selected_session_1_resync_owed_2_bids_2", muxModelEventSymbol, muxState{phase: muxStartAsync, resyncOwed: 2, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_async_selected_session_1_resync_owed_2_bids_2", muxModelEventBid, muxState{phase: muxStartAsync, resyncOwed: 2, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("start_async_selected_session_1_resync_owed_2_bids_2", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionStarted, muxModelEffectResultGranted),
	muxModelOn("start_async_selected_session_1_resync_owed_2_bids_2", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_selected_session_1_resync_owed_2_bids_2", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_selected_session_1_resync_owed_2_bids_2", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_selected_session_1_resync_owed_2_bids_2", muxModelEventStartDeadline, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultError),
	muxModelNever("start_async_selected_session_1_resync_owed_2_bids_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("start_async_selected_session_1_resync_owed_2_bids_2", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("start_async_selected_session_1_resync_owed_2_bids_2", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("start_async_selected_session_1_resync_owed_2_bids_2", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("start_async_selected_session_1_resync_owed_2_bids_2", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 2, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("start_async_selected_session_1_resync_owed_2_bids_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("start_async_selected_session_1_resync_owed_2_bids_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("start_async_selected_session_1_resync_owed_2_bids_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("start_async_selected_session_1_resync_owed_2_bids_2", muxModelEventInitiatorSymbol, muxState{phase: muxStartAsync, resyncOwed: 2, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("start_async_selected_session_2", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_async_selected_session_2", muxModelEventSymbol, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_async_selected_session_2", muxModelEventBid, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectResultCancelled),
	muxModelOn("start_async_selected_session_2", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectSessionStarted, muxModelEffectResultGranted),
	muxModelOn("start_async_selected_session_2", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_selected_session_2", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_selected_session_2", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectResultError),
	muxModelOn("start_async_selected_session_2", muxModelEventStartDeadline, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectResultError),
	muxModelNever("start_async_selected_session_2", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("start_async_selected_session_2", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("start_async_selected_session_2", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("start_async_selected_session_2", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("start_async_selected_session_2", muxModelEventSessionClose, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}),
	muxModelOn("start_async_selected_session_2", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("start_async_selected_session_2", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("start_async_selected_session_2", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("start_async_selected_session_2", muxModelEventInitiatorSymbol, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("start_async_selected_session_2_enforce", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_async_selected_session_2_enforce", muxModelEventSymbol, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_async_selected_session_2_enforce", muxModelEventBid, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectResultCancelled),
	muxModelOn("start_async_selected_session_2_enforce", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectSessionStarted, muxModelEffectResultGranted),
	muxModelOn("start_async_selected_session_2_enforce", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_selected_session_2_enforce", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryForeign, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_selected_session_2_enforce", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectResultError),
	muxModelOn("start_async_selected_session_2_enforce", muxModelEventStartDeadline, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x71}, muxModelEffectResultError),
	muxModelNever("start_async_selected_session_2_enforce", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("start_async_selected_session_2_enforce", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("start_async_selected_session_2_enforce", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("start_async_selected_session_2_enforce", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("start_async_selected_session_2_enforce", muxModelEventSessionClose, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}),
	muxModelOn("start_async_selected_session_2_enforce", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("start_async_selected_session_2_enforce", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("start_async_selected_session_2_enforce", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("start_async_selected_session_2_enforce", muxModelEventInitiatorSymbol, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("start_async_selected_session_2_resync_owed_1", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("start_async_selected_session_2_resync_owed_1", muxModelEventSymbol, muxState{phase: muxStartAsync, resyncOwed: 1, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_async_selected_session_2_resync_owed_1", muxModelEventBid, muxState{phase: muxStartAsync, resyncOwed: 1, start: muxModelSelected(2, 0x71)}, muxModelEffectResultCancelled),
	muxModelOn("start_async_selected_session_2_resync_owed_1", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectSessionStarted, muxModelEffectResultGranted),
	muxModelOn("start_async_selected_session_2_resync_owed_1", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_selected_session_2_resync_owed_1", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_async_selected_session_2_resync_owed_1", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectResultError),
	muxModelOn("start_async_selected_session_2_resync_owed_1", muxModelEventStartDeadline, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectResultError),
	muxModelNever("start_async_selected_session_2_resync_owed_1", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("start_async_selected_session_2_resync_owed_1", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("start_async_selected_session_2_resync_owed_1", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("start_async_selected_session_2_resync_owed_1", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("start_async_selected_session_2_resync_owed_1", muxModelEventSessionClose, muxState{phase: muxStartAsync, resyncOwed: 1, start: muxModelSelected(2, 0x71)}),
	muxModelOn("start_async_selected_session_2_resync_owed_1", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("start_async_selected_session_2_resync_owed_1", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("start_async_selected_session_2_resync_owed_1", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("start_async_selected_session_2_resync_owed_1", muxModelEventInitiatorSymbol, muxState{phase: muxStartAsync, resyncOwed: 1, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("start_blocking", muxModelEventSYN, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(1, 0x31)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_blocking", muxModelEventSymbol, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(1, 0x31)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_blocking", muxModelEventBid, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}),
	muxModelOn("start_blocking", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, blocking: true, owner: 1}, muxModelEffectPassiveSymbol, muxModelEffectSessionStarted, muxModelEffectSessionReceived, muxModelEffectResultGranted),
	muxModelOn("start_blocking", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_blocking", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_blocking", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectResultError),
	muxModelOn("start_blocking", muxModelEventStartDeadline, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectResultError),
	muxModelNever("start_blocking", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("start_blocking", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("start_blocking", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("start_blocking", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("start_blocking", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("start_blocking", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("start_blocking", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, owner: 1}, muxModelEffectSessionStarted, muxModelEffectResultGranted),
	muxModelOn("start_blocking", muxModelEventStartCallFails, muxState{phase: muxIdle}, muxModelEffectResultError),
	muxModelOn("start_blocking", muxModelEventInitiatorSymbol, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(1, 0x31)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("start_blocking_deferred_loser", muxModelEventSYN, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(1, 0x31), deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_blocking_deferred_loser", muxModelEventSymbol, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(1, 0x31), deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_blocking_deferred_loser", muxModelEventBid, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(1, 0x31), bids: muxModelBids(2), deferred: true}),
	muxModelOn("start_blocking_deferred_loser", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, blocking: true, owner: 1, deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionStarted, muxModelEffectResultGranted),
	muxModelOn("start_blocking_deferred_loser", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_blocking_deferred_loser", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_blocking_deferred_loser", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_blocking_deferred_loser", muxModelEventStartDeadline, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, retired: true, retiredSource: 0x31}, muxModelEffectResultError),
	muxModelNever("start_blocking_deferred_loser", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("start_blocking_deferred_loser", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("start_blocking_deferred_loser", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("start_blocking_deferred_loser", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("start_blocking_deferred_loser", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("start_blocking_deferred_loser", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("start_blocking_deferred_loser", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1}, muxModelEffectSessionStarted, muxModelEffectResultGranted, muxModelEffectResultLost),
	muxModelOn("start_blocking_deferred_loser", muxModelEventStartCallFails, muxState{phase: muxIdle}, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_blocking_deferred_loser", muxModelEventInitiatorSymbol, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(1, 0x31), deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventSYN, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventSymbol, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventBid, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}, muxModelEffectResultCancelled),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionStarted, muxModelEffectSessionReceived, muxModelEffectResultGranted),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectResultError),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventStartDeadline, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultError),
	muxModelNever("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, owner: 1, bids: muxModelBids(2)}, muxModelEffectSessionStarted, muxModelEffectResultGranted),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectResultError),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(1, 0x31), bids: muxModelBids(2)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventSYN, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(1, 0x31), bids: muxModelBids(2), deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventSymbol, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(1, 0x31), bids: muxModelBids(2), deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventBid, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(1, 0x31), bids: muxModelBids(2), deferred: true}, muxModelEffectResultCancelled),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, blocking: true, owner: 1, bids: muxModelBids(2), deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionStarted, muxModelEffectResultGranted),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventStartDeadline, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), retired: true, retiredSource: 0x31}, muxModelEffectResultError),
	muxModelNever("start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, bids: muxModelBids(2), deferred: true, retired: true, retiredSource: 0x31}, muxModelEffectResultCancelled),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, resyncOwed: 2, owner: 1, bids: muxModelBids(2)}, muxModelEffectSessionStarted, muxModelEffectResultGranted, muxModelEffectResultLost),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxBidsQueued, bids: muxModelBids(2)}, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_blocking_selected_session_1_blocking_call_bids_2_deferred_loser_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(1, 0x31), bids: muxModelBids(2), deferred: true}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("start_blocking_selected_session_2_blocking_call_blocking_upstream", muxModelEventSYN, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_blocking_selected_session_2_blocking_call_blocking_upstream", muxModelEventSymbol, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_blocking_selected_session_2_blocking_call_blocking_upstream", muxModelEventBid, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectResultCancelled),
	muxModelOn("start_blocking_selected_session_2_blocking_call_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, blocking: true, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectSessionStarted, muxModelEffectResultGranted),
	muxModelOn("start_blocking_selected_session_2_blocking_call_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_blocking_selected_session_2_blocking_call_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_blocking_selected_session_2_blocking_call_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectResultError),
	muxModelOn("start_blocking_selected_session_2_blocking_call_blocking_upstream", muxModelEventStartDeadline, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, retired: true, retiredSource: 0x71}, muxModelEffectResultError),
	muxModelNever("start_blocking_selected_session_2_blocking_call_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("start_blocking_selected_session_2_blocking_call_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("start_blocking_selected_session_2_blocking_call_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("start_blocking_selected_session_2_blocking_call_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("start_blocking_selected_session_2_blocking_call_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}),
	muxModelOn("start_blocking_selected_session_2_blocking_call_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("start_blocking_selected_session_2_blocking_call_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, owner: 2}, muxModelEffectSessionStarted, muxModelEffectResultGranted),
	muxModelOn("start_blocking_selected_session_2_blocking_call_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxIdle}, muxModelEffectResultError),
	muxModelOn("start_blocking_selected_session_2_blocking_call_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventSYN, muxState{phase: muxStartBlocking, blocking: true, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectActiveByte, muxModelEffectSessionReceived),
	muxModelOn("start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventSymbol, muxState{phase: muxStartBlocking, blocking: true, resyncOwed: 1, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventBid, muxState{phase: muxStartBlocking, blocking: true, resyncOwed: 1, start: muxModelSelected(2, 0x71)}, muxModelEffectResultCancelled),
	muxModelOn("start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventStartedMatching, muxState{phase: muxSessionOwned, blocking: true, resyncOwed: 1, owner: 2}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectSessionStarted, muxModelEffectResultGranted),
	muxModelOn("start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectPassiveSymbol, muxModelEffectResultError),
	muxModelOn("start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventStartDeadline, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, blocking: true, resyncOwed: 1, retired: true, retiredSource: 0x71}, muxModelEffectResultError),
	muxModelNever("start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventSessionClose, muxState{phase: muxStartBlocking, blocking: true, resyncOwed: 1, start: muxModelSelected(2, 0x71)}),
	muxModelOn("start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelOn("start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventStartCallReturns, muxState{phase: muxSessionOwned, resyncOwed: 1, owner: 2}, muxModelEffectSessionStarted, muxModelEffectResultGranted),
	muxModelOn("start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventStartCallFails, muxState{phase: muxIdle, resyncOwed: 1}, muxModelEffectResultError),
	muxModelOn("start_blocking_selected_session_2_blocking_call_resync_owed_1_blocking_upstream", muxModelEventInitiatorSymbol, muxState{phase: muxStartBlocking, blocking: true, resyncOwed: 1, start: muxModelSelected(2, 0x71)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),

	muxModelOn("start_gateway", muxModelEventSYN, muxState{phase: muxStartAsync, start: muxModelSelected(gatewaySessionID, 0x7F)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_gateway", muxModelEventSymbol, muxState{phase: muxStartAsync, start: muxModelSelected(gatewaySessionID, 0x7F)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
	muxModelOn("start_gateway", muxModelEventBid, muxState{phase: muxStartAsync, start: muxModelSelected(gatewaySessionID, 0x7F), bids: muxModelBids(2)}),
	muxModelOn("start_gateway", muxModelEventStartedMatching, muxState{phase: muxGatewayGrace}, muxModelEffectSessionReceived, muxModelEffectResultGranted),
	muxModelOn("start_gateway", muxModelEventStartedOther, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_gateway", muxModelEventFailed, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived, muxModelEffectResultError),
	muxModelOn("start_gateway", muxModelEventFailedPhantom, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, retired: true, retiredSource: 0x7F}, muxModelEffectPassiveSymbol, muxModelEffectResultError),
	muxModelOn("start_gateway", muxModelEventStartDeadline, muxState{phase: muxFenced, recovery: arbitrationRecoveryAtSYN, absorb: 1, retired: true, retiredSource: 0x7F}, muxModelEffectResultError),
	muxModelNever("start_gateway", muxModelEventAbsorbDebounce, muxNoAbsorb),
	muxModelNever("start_gateway", muxModelEventIdleGrace, muxNoOwnerTimer),
	muxModelNever("start_gateway", muxModelEventOwnershipLimit, muxNoOwnerTimer),
	muxModelNever("start_gateway", muxModelEventAbandonWatchdog, muxNoSessionGrant),
	muxModelOn("start_gateway", muxModelEventSessionClose, muxState{phase: muxStartAsync, start: muxModelSelected(gatewaySessionID, 0x7F)}),
	muxModelOn("start_gateway", muxModelEventReset, muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}, muxModelEffectPassiveReset, muxModelEffectActiveError, muxModelEffectSessionReset, muxModelEffectResultError),
	muxModelNever("start_gateway", muxModelEventStartCallReturns, muxNoBlockingCall),
	muxModelNever("start_gateway", muxModelEventStartCallFails, muxNoBlockingCall),
	muxModelOn("start_gateway", muxModelEventInitiatorSymbol, muxState{phase: muxStartAsync, start: muxModelSelected(gatewaySessionID, 0x7F)}, muxModelEffectPassiveSymbol, muxModelEffectSessionReceived),
}

// TestMuxStateModelDistinguishesReachableCases pins pairs of model states the
// typed state keeps apart because the same event leads to different
// transitions from them:
//
//   - the queued bidder: closing session 1 cancels its own queued bid, but
//     leaves the bid of session 2 that the idle state's Bid queues;
//   - the expected echo: a SYN while the gateway waits for a data echo after a
//     delivered byte is suppressed, while its structural terminator echo ends
//     the transaction into the gateway grace;
//   - the initiator predicate: in enforce mode a FAILED naming a winner the
//     predicate accepts upgrades the fence to foreign recovery and publishes
//     the winner, while a phantom FAILED keeps the SYN fence and withholds it;
//   - the live telegram: in enforce mode a SYN fence raised while the owner's
//     telegram is on the wire absorbs the next SYN as foreign recovery, while
//     a fence without a live telegram clears at that SYN.
func TestMuxStateModelDistinguishesReachableCases(t *testing.T) {
	rows := map[string]muxModelRow{}
	for _, row := range muxModelRows {
		rows[row.state+"/"+row.event.String()] = row
	}
	row := func(state string, event muxModelEvent) muxModelRow {
		t.Helper()
		r, ok := rows[state+"/"+event.String()]
		if !ok || r.outcome != muxModelTransition {
			t.Fatalf("no transition row %s/%s", state, event)
		}
		return r
	}
	has := func(r muxModelRow, effect muxModelEffect) bool {
		for _, e := range r.effects {
			if e == effect {
				return true
			}
		}
		return false
	}

	queued := row("idle", muxModelEventBid).next
	if queued.bids != muxModelBids(2) || queued == muxModelStates["bids_queued"].state {
		t.Errorf("the idle state's bid derives %s, want the bid of session 2 apart from bids_queued", muxModelStateLiteral(queued))
	}
	if r := row("bids_queued", muxModelEventSessionClose); r.next.phase != muxIdle || !has(r, muxModelEffectResultCancelled) {
		t.Errorf("closing session 1 with its bid queued: %s", muxModelRowLiteral(r.state, r.event, r.next, r.effects))
	}
	for name, named := range muxModelStates {
		if named.state != queued || muxModelStateConfig(muxModelStates, name) != muxModelAsync {
			continue
		}
		if r := row(name, muxModelEventSessionClose); r.next != queued || has(r, muxModelEffectResultCancelled) {
			t.Errorf("closing session 1 with session 2's bid queued: %s", muxModelRowLiteral(r.state, r.event, r.next, r.effects))
		}
	}

	data, terminator := muxModelStates["gateway_awaiting_echo"].state, muxModelStates["gateway_awaiting_terminator_echo"].state
	if data.echoTerminator || !terminator.echoTerminator || !data.echoPending || !terminator.echoPending {
		t.Errorf("expected echoes derive %s and %s", muxModelStateLiteral(data), muxModelStateLiteral(terminator))
	}
	if r := row("gateway_awaiting_echo", muxModelEventSYN); r.next != data {
		t.Errorf("SYN while a data echo is due: %s", muxModelRowLiteral(r.state, r.event, r.next, r.effects))
	}
	if r := row("gateway_awaiting_terminator_echo", muxModelEventSYN); r.next.phase != muxGatewayGrace {
		t.Errorf("SYN while the terminator echo is due: %s", muxModelRowLiteral(r.state, r.event, r.next, r.effects))
	}

	compared := 0
	for _, name := range muxModelStateNames(muxModelStates) {
		named := muxModelStates[name]
		if muxModelStateConfig(muxModelStates, name) != muxModelEnforce || !named.state.start.pending || named.state.absorb != 0 || named.state.phase != muxStartAsync {
			continue
		}
		accepted, phantom := row(name, muxModelEventFailed), row(name, muxModelEventFailedPhantom)
		if accepted.next.recovery != arbitrationRecoveryForeign || !has(accepted, muxModelEffectSessionReceived) ||
			phantom.next.recovery != arbitrationRecoveryAtSYN || has(phantom, muxModelEffectSessionReceived) {
			t.Errorf("%s: FAILED %s, phantom FAILED %s", name,
				muxModelRowLiteral(accepted.state, accepted.event, accepted.next, accepted.effects),
				muxModelRowLiteral(phantom.state, phantom.event, phantom.next, phantom.effects))
		}
		compared++
	}
	if compared == 0 {
		t.Error("no enforce-mode START state compares FAILED with phantom FAILED")
	}

	live := row("session_telegram_live", muxModelEventSessionClose).next
	if live.phase != muxFenced || live.recovery != arbitrationRecoveryAtSYN || !live.telegramLive {
		t.Fatalf("closing the owner of a live telegram derives %s, want a SYN fence over the live telegram", muxModelStateLiteral(live))
	}
	quiet := live
	quiet.telegramLive = false
	var liveName, quietName string
	for name, named := range muxModelStates {
		if muxModelStateConfig(muxModelStates, name) != muxModelEnforce {
			continue
		}
		switch named.state {
		case live:
			liveName = name
		case quiet:
			quietName = name
		}
	}
	if liveName == "" || quietName == "" {
		t.Fatalf("enforce-mode SYN fences with and without a live telegram: %q, %q", liveName, quietName)
	}
	if r := row(liveName, muxModelEventSYN); r.next.recovery != arbitrationRecoveryForeignAfterAA || len(r.effects) != 0 {
		t.Errorf("SYN over a live telegram: %s", muxModelRowLiteral(r.state, r.event, r.next, r.effects))
	}
	if r := row(quietName, muxModelEventSYN); r.next.phase == muxFenced || !has(r, muxModelEffectPassiveSymbol) {
		t.Errorf("SYN with no live telegram: %s", muxModelRowLiteral(r.state, r.event, r.next, r.effects))
	}
}
