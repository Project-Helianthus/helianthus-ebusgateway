package adaptermux

import (
	"reflect"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// The SYN boundary tests cover the pure decision of every step and each named
// step of onSYNLocked through the harness: the opening step (fence, recovery,
// owed resynchronization SYN), the echo step, the terminator step, the three
// release steps, the pre-echo step and the diagnostics step.

const (
	synTestIdleGrace    = time.Second
	synTestSessionGrace = 2 * time.Second
)

// TestSYNBoundaryDecisionVariesEveryInput covers decideSYNBoundary. Each row
// varies one input against a base case; the production mutation each field
// catches:
//
//   - state recovery: clearing the reset recovery at a SYN;
//   - state resyncOwed: delivering a SYN nobody is owed;
//   - state phase and owner: releasing or delivering a terminator for an
//     unowned bus, or applying the gateway's grace to a session;
//   - event kind: deciding for an event that is not a SYN, or releasing on
//     an idle grace that cannot occur;
//   - phaseEvent: releasing on the wrong wire-phase event;
//   - arrivedGatewayOwned: counting the diagnostics of a SYN the gateway did
//     not own, or keeping the echo queue for it;
//   - txnActiveBefore: delivering the owed SYN into a live transaction, or
//     classifying the grant window after the terminator cleared the flag;
//   - deliveredBefore: classifying the inter-write window against the count
//     the terminator just raised;
//   - echoPending and echoTerminator: delivering a SYN mid-write, or
//     suppressing the structural terminator echo;
//   - queueJustDrained: delivering a SYN between two writes;
//   - txnActive and delivered: delivering a terminator without a live
//     transaction or before its first echo, or suppressing the idle release
//     of a stuck write;
//   - terminatorDelivered: reporting a terminator delivery as withheld;
//   - wireGap and sessionGrace: releasing a session mid-scan, or comparing
//     with > instead of >=;
//   - grantAge: holding a between-writes gateway past its grace;
//   - idleElapsed and idleGrace: releasing the gateway inside its grace, or
//     comparing with >= instead of >;
//   - externalPendingRead and externalPending: reading the arbiter when not
//     needed, or bypassing the grace without an external START.
func TestSYNBoundaryDecisionVariesEveryInput(t *testing.T) {
	gwTxn := muxState{phase: muxGatewayTransaction}
	gwGrace := muxState{phase: muxGatewayGrace}
	sess := muxState{phase: muxSessionOwned, owner: 1}
	vary := func(in synBoundaryInput, f func(*synBoundaryInput)) synBoundaryInput {
		f(&in)
		return in
	}
	// terminator is a gateway transaction whose structural terminator SYN
	// arrives after its echoes were delivered.
	terminator := synBoundaryInput{
		phaseEvent: wirePhaseEventSYNIdle, arrivedGatewayOwned: true,
		txnActiveBefore: true, deliveredBefore: true, txnActive: true, delivered: true,
		idleGrace: synTestIdleGrace, sessionGrace: synTestSessionGrace,
	}
	terminatorWant := synBoundaryDecision{
		clearRecovery: true, deliverTerminator: true, askExternalPending: true, graceUsed: synTestIdleGrace,
		interWriteEmptySYN: true, checkTransportWindow: true, recordDiag: true, synDelivered: true,
	}
	with := func(d synBoundaryDecision, f func(*synBoundaryDecision)) synBoundaryDecision {
		f(&d)
		return d
	}
	unowned := synBoundaryInput{phaseEvent: wirePhaseEventSYNIdle, idleGrace: synTestIdleGrace, sessionGrace: synTestSessionGrace}
	timeout := vary(unowned, func(in *synBoundaryInput) { in.phaseEvent = wirePhaseEventSYNTimeout })
	done := vary(unowned, func(in *synBoundaryInput) { in.phaseEvent = wirePhaseEventTransactionDone })

	cases := []struct {
		name  string
		state muxState
		kind  muxEventKind
		in    synBoundaryInput
		want  synBoundaryDecision
	}{
		{"terminator", gwTxn, muxEventSYN, terminator, terminatorWant},

		// Opening step.
		{"reset recovery survives", muxState{phase: muxGatewayTransaction, recovery: arbitrationRecoveryReset}, muxEventSYN, terminator,
			with(terminatorWant, func(d *synBoundaryDecision) { d.clearRecovery = false })},
		{"at-SYN recovery ends", muxState{phase: muxGatewayTransaction, recovery: arbitrationRecoveryAtSYN}, muxEventSYN, terminator, terminatorWant},
		{"owed SYN goes to the active path", muxState{resyncOwed: 1}, muxEventSYN, unowned,
			synBoundaryDecision{clearRecovery: true, deliverOwedResync: true}},
		{"owed SYN waits for a live transaction", muxState{resyncOwed: 1}, muxEventSYN,
			vary(unowned, func(in *synBoundaryInput) { in.txnActiveBefore = true }),
			synBoundaryDecision{clearRecovery: true, recordDiag: true}},
		{"nothing owed", muxState{}, muxEventSYN, unowned, synBoundaryDecision{clearRecovery: true}},

		// Echo step and terminator step.
		{"SYN the gateway did not own at arrival", gwTxn, muxEventSYN,
			vary(terminator, func(in *synBoundaryInput) { in.arrivedGatewayOwned = false }),
			with(terminatorWant, func(d *synBoundaryDecision) { d.interWriteEmptySYN, d.checkTransportWindow = false, false })},
		{"mid-write SYN", gwTxn, muxEventSYN,
			vary(terminator, func(in *synBoundaryInput) { in.echoPending = true }),
			synBoundaryDecision{clearRecovery: true, midWrite: true, keepEchoQueue: true, preEchoSuppressed: true,
				checkTransportWindow: true, recordDiag: true}},
		{"mid-write SYN the gateway did not own keeps nothing", gwTxn, muxEventSYN,
			vary(terminator, func(in *synBoundaryInput) { in.echoPending, in.arrivedGatewayOwned = true, false }),
			synBoundaryDecision{clearRecovery: true, midWrite: true, deliverTerminator: true, askExternalPending: true,
				graceUsed: synTestIdleGrace, recordDiag: true, synDelivered: true}},
		{"mid-write SYN before the transaction keeps nothing", gwTxn, muxEventSYN,
			vary(terminator, func(in *synBoundaryInput) { in.echoPending, in.txnActiveBefore = true, false }),
			synBoundaryDecision{clearRecovery: true, midWrite: true, deliverTerminator: true,
				grantWindowSYN: true, checkTransportWindow: true, recordDiag: true, synDelivered: true}},
		{"structural terminator echo pending", gwTxn, muxEventSYN,
			vary(terminator, func(in *synBoundaryInput) { in.echoPending, in.echoTerminator = true, true }),
			with(terminatorWant, func(d *synBoundaryDecision) { d.interWriteEmptySYN = false })},
		{"SYN between two writes", gwTxn, muxEventSYN,
			vary(terminator, func(in *synBoundaryInput) { in.queueJustDrained = true }),
			synBoundaryDecision{clearRecovery: true, betweenWrites: true, keepEchoQueue: true, preEchoSuppressed: true,
				betweenWritesSuppressed: true, interWriteEmptySYN: true, checkTransportWindow: true, recordDiag: true}},
		{"SYN between two writes past the grace", gwTxn, muxEventSYN,
			vary(terminator, func(in *synBoundaryInput) { in.queueJustDrained, in.grantAge = true, 2*synTestIdleGrace }),
			synBoundaryDecision{clearRecovery: true, betweenWrites: true, keepEchoQueue: true, askExternalPending: true,
				graceUsed: synTestIdleGrace, preEchoSuppressed: true, betweenWritesSuppressed: true,
				interWriteEmptySYN: true, checkTransportWindow: true, recordDiag: true}},
		{"drained queue with a pending echo is mid-write", gwTxn, muxEventSYN,
			vary(terminator, func(in *synBoundaryInput) { in.queueJustDrained, in.echoPending = true, true }),
			synBoundaryDecision{clearRecovery: true, midWrite: true, keepEchoQueue: true, preEchoSuppressed: true,
				checkTransportWindow: true, recordDiag: true}},
		{"no live transaction", gwGrace, muxEventSYN,
			vary(terminator, func(in *synBoundaryInput) { in.txnActive = false }),
			with(terminatorWant, func(d *synBoundaryDecision) { d.deliverTerminator, d.synDelivered = false, false })},
		{"terminator delivered", gwGrace, muxEventSYN,
			vary(terminator, func(in *synBoundaryInput) { in.txnActive, in.terminatorDelivered = false, true }),
			with(terminatorWant, func(d *synBoundaryDecision) { d.deliverTerminator = false })},
		{"before the first echo", gwTxn, muxEventSYN,
			vary(terminator, func(in *synBoundaryInput) { in.delivered = false }),
			with(terminatorWant, func(d *synBoundaryDecision) {
				d.deliverTerminator, d.preEchoSuppressed, d.synDelivered = false, true, false
			})},
		{"no delivery before the arrival", gwTxn, muxEventSYN,
			vary(terminator, func(in *synBoundaryInput) { in.deliveredBefore = false }),
			with(terminatorWant, func(d *synBoundaryDecision) { d.interWriteEmptySYN = false })},
		{"grant window", gwGrace, muxEventSYN,
			vary(terminator, func(in *synBoundaryInput) { in.txnActiveBefore, in.txnActive = false, false }),
			with(terminatorWant, func(d *synBoundaryDecision) {
				d.deliverTerminator, d.interWriteEmptySYN, d.grantWindowSYN, d.synDelivered = false, false, true, false
			})},
		{"unowned bus", muxState{}, muxEventSYN, terminator,
			with(terminatorWant, func(d *synBoundaryDecision) {
				d.deliverTerminator, d.askExternalPending, d.graceUsed = false, false, 0
			})},

		// Idle release.
		{"gateway past its grace", gwTxn, muxEventSYN,
			vary(terminator, func(in *synBoundaryInput) { in.idleElapsed = synTestIdleGrace + 1 }),
			with(terminatorWant, func(d *synBoundaryDecision) { d.askExternalPending, d.release = false, synReleaseIdleGrace })},
		{"gateway at its grace", gwTxn, muxEventSYN,
			vary(terminator, func(in *synBoundaryInput) { in.idleElapsed = synTestIdleGrace }), terminatorWant},
		{"gateway with a longer grace", gwTxn, muxEventSYN,
			vary(terminator, func(in *synBoundaryInput) { in.idleElapsed, in.idleGrace = synTestIdleGrace+1, synTestSessionGrace }),
			with(terminatorWant, func(d *synBoundaryDecision) { d.graceUsed = synTestSessionGrace })},
		{"gateway without an external START", gwTxn, muxEventSYN,
			vary(terminator, func(in *synBoundaryInput) { in.externalPendingRead = true }),
			with(terminatorWant, func(d *synBoundaryDecision) { d.askExternalPending = false })},
		{"gateway with an external START", gwTxn, muxEventSYN,
			vary(terminator, func(in *synBoundaryInput) { in.externalPendingRead, in.externalPending = true, true }),
			with(terminatorWant, func(d *synBoundaryDecision) {
				d.askExternalPending, d.release, d.externalBypass = false, synReleaseIdleGrace, true
			})},
		{"external START not read yet", gwTxn, muxEventSYN,
			vary(terminator, func(in *synBoundaryInput) { in.externalPending = true }), terminatorWant},
		{"session inside its gap", sess, muxEventSYN,
			vary(unowned, func(in *synBoundaryInput) { in.idleElapsed = synTestSessionGrace - 1 }),
			synBoundaryDecision{clearRecovery: true, graceUsed: synTestSessionGrace}},
		{"session at its gap", sess, muxEventSYN,
			vary(unowned, func(in *synBoundaryInput) { in.idleElapsed = synTestSessionGrace }),
			synBoundaryDecision{clearRecovery: true, graceUsed: synTestSessionGrace, release: synReleaseIdleGrace}},

		// SYN timeout release.
		{"gateway SYN timeout", gwGrace, muxEventSYN, timeout,
			synBoundaryDecision{clearRecovery: true, release: synReleaseTimeout}},
		{"gateway SYN timeout ends its transaction", gwTxn, muxEventSYN,
			vary(timeout, func(in *synBoundaryInput) { in.txnActive, in.delivered = true, true }),
			synBoundaryDecision{clearRecovery: true, deliverTerminator: true, release: synReleaseTimeout,
				endTxn: ReasonSYNTimeout, cancelWatchdog: true, synDelivered: true}},
		{"session SYN timeout inside its gap", sess, muxEventSYN,
			vary(timeout, func(in *synBoundaryInput) { in.wireGap = synTestSessionGrace - 1 }),
			synBoundaryDecision{clearRecovery: true}},
		{"session SYN timeout at its gap", sess, muxEventSYN,
			vary(timeout, func(in *synBoundaryInput) { in.wireGap = synTestSessionGrace }),
			synBoundaryDecision{clearRecovery: true, release: synReleaseTimeout, logGap: true}},
		{"session SYN timeout with a longer grace", sess, muxEventSYN,
			vary(timeout, func(in *synBoundaryInput) { in.wireGap, in.sessionGrace = synTestSessionGrace, 2*synTestSessionGrace }),
			synBoundaryDecision{clearRecovery: true}},
		{"SYN timeout of an unowned bus", muxState{}, muxEventSYN,
			vary(timeout, func(in *synBoundaryInput) { in.wireGap = synTestSessionGrace }),
			synBoundaryDecision{clearRecovery: true}},

		// Transaction done release.
		{"session transaction done", sess, muxEventSYN, done,
			synBoundaryDecision{clearRecovery: true, release: synReleaseTransactionDone, stepCompleted: true}},
		{"gateway transaction done", gwTxn, muxEventSYN,
			vary(done, func(in *synBoundaryInput) { in.txnActive, in.delivered = true, true }),
			synBoundaryDecision{clearRecovery: true, deliverTerminator: true, release: synReleaseTransactionDone,
				endTxn: ReasonTransactionDone, synDelivered: true}},
		{"gateway transaction done after its transaction", gwGrace, muxEventSYN, done,
			synBoundaryDecision{clearRecovery: true, release: synReleaseTransactionDone}},
		{"transaction done of an unowned bus", muxState{}, muxEventSYN, done,
			synBoundaryDecision{clearRecovery: true}},

		// The idle-grace event.
		{"idle grace ends the gateway transaction", gwTxn, muxEventIdleGrace,
			vary(unowned, func(in *synBoundaryInput) { in.txnActive = true }),
			synBoundaryDecision{release: synReleaseIdleGrace, endTxn: ReasonSYNIdle, clearSentinel: true, cancelWatchdog: true}},
		{"idle grace after the gateway transaction", gwGrace, muxEventIdleGrace, unowned,
			synBoundaryDecision{release: synReleaseIdleGrace}},
		{"idle grace of a session", sess, muxEventIdleGrace,
			vary(unowned, func(in *synBoundaryInput) { in.txnActive = true }),
			synBoundaryDecision{release: synReleaseIdleGrace}},
		{"idle grace of an unowned bus cannot occur", muxState{phase: muxFenced}, muxEventIdleGrace,
			vary(unowned, func(in *synBoundaryInput) { in.txnActive = true }), synBoundaryDecision{}},
		{"a symbol is no SYN boundary", gwTxn, muxEventSymbol, terminator, synBoundaryDecision{}},
	}
	for _, tc := range cases {
		ev := muxEvent{kind: tc.kind, data: protocol.SymbolSyn}
		if got := decideSYNBoundary(tc.state, ev, tc.in); got != tc.want {
			t.Errorf("%s:\n got %+v\nwant %+v", tc.name, got, tc.want)
		}
	}
}

// TestSYNBoundaryStateFollowsThePinnedOwner covers the typed state the steps
// decide on: the owner is the one the caller pinned, the gateway phase
// follows the transaction flag, and an unowned bus shows the START in
// flight, the fence or idle.
func TestSYNBoundaryStateFollowsThePinnedOwner(t *testing.T) {
	h := newTransitionHarness(t, transitionUpstreamAsync)
	m := h.m
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	m.arbitrationRecovery = arbitrationRecoveryAtSYN
	m.collisionResyncOwed = 2
	cases := []struct {
		name     string
		owner    uint64
		owned    bool
		txn      bool
		fence    bool
		start    *pendingStartState
		phase    muxPhase
		ownerOut uint64
	}{
		{name: "gateway transaction", owner: gatewaySessionID, owned: true, txn: true, phase: muxGatewayTransaction},
		{name: "gateway grace", owner: gatewaySessionID, owned: true, phase: muxGatewayGrace},
		{name: "session", owner: 2, owned: true, txn: true, phase: muxSessionOwned, ownerOut: 2},
		{name: "released owner", owner: 2, txn: true, phase: muxIdle},
		{name: "START in flight", start: &pendingStartState{sessionID: 1, initiator: transitionSessionSource}, phase: muxStartAsync},
		{name: "blocking START", start: &pendingStartState{sessionID: 1, initiator: transitionSessionSource, blockingArb: true}, phase: muxStartBlocking},
		{name: "fenced", fence: true, phase: muxFenced},
	}
	for _, tc := range cases {
		m.gatewayTxnActive = tc.txn
		m.deferRegrantUntilNextSyn = tc.fence
		m.pendingStart = tc.start
		s := m.synBoundaryStateLocked(tc.owner, tc.owned)
		if s.phase != tc.phase || s.owner != tc.ownerOut {
			t.Errorf("%s: phase %d owner %d, want phase %d owner %d", tc.name, s.phase, s.owner, tc.phase, tc.ownerOut)
		}
		if s.recovery != arbitrationRecoveryAtSYN || s.resyncOwed != 2 {
			t.Errorf("%s: recovery %d resync %d", tc.name, s.recovery, s.resyncOwed)
		}
		if (tc.start != nil) != s.start.pending {
			t.Errorf("%s: START in flight %v", tc.name, s.start.pending)
		}
	}
	m.gatewayTxnActive, m.deferRegrantUntilNextSyn, m.pendingStart = false, false, nil
	m.arbitrationRecovery, m.collisionResyncOwed = arbitrationRecoveryNone, 0
}

// synBoundaryCounters are the diagnostics the SYN boundary writes.
type synBoundaryCounters struct {
	reason          ActiveTxnInactiveReason
	delivered       uint64
	dropped         uint64
	preEcho         uint64
	betweenWrites   uint64
	grantWindow     uint64
	interWriteEmpty uint64
	ring            []SynDiagEntry
	stepRun         int
}

func synBoundaryReadCounters(h *transitionHarness) synBoundaryCounters {
	h.m.stateMu.Lock()
	defer h.m.stateMu.Unlock()
	a := &h.m.activeTxn
	return synBoundaryCounters{
		reason:          a.inactiveReas,
		delivered:       a.bytesDeliveredToActive.Load(),
		dropped:         a.terminatorDropOnFullCh.Load(),
		preEcho:         a.synSuppressedPreEcho.Load(),
		betweenWrites:   a.synSuppressedBetweenWrites.Load(),
		grantWindow:     a.synSeenDuringGrantWindow.Load(),
		interWriteEmpty: a.synSeenWhileInterWriteEmpty.Load(),
		ring:            h.m.synDiag.snapshot(),
		stepRun:         h.m.step.run,
	}
}

// synBoundaryReturn is what onSYNLocked returned to a direct call.
type synBoundaryReturn struct {
	passive        int
	tryGrant       bool
	preEcho        bool
	cancelWatchdog bool
}

// synBoundaryDirect runs onSYNLocked directly under stateMu.
func synBoundaryDirect(h *transitionHarness, ev wirePhaseEvent, owner uint64, owned bool) synBoundaryReturn {
	h.m.stateMu.Lock()
	defer h.m.stateMu.Unlock()
	passive, tryGrant, preEcho, cancel := h.m.onSYNLocked(ev, owner, owned, time.Now())
	return synBoundaryReturn{passive: len(passive), tryGrant: tryGrant, preEcho: preEcho, cancelWatchdog: cancel}
}

// synBoundaryRow is one harness row: a fixture, one input and the state,
// effects and diagnostics afterwards.
type synBoundaryRow struct {
	name    string
	fixture func(h *transitionHarness)
	// input runs the SYN; a direct call returns what onSYNLocked returned.
	input   func(h *transitionHarness) *synBoundaryReturn
	want    transitionState
	effects []string
	// ret is what a direct call returned.
	ret *synBoundaryReturn
	// reason is the inactive reason afterwards; the counts are increments.
	reason          ActiveTxnInactiveReason
	delivered       uint64
	dropped         uint64
	preEcho         uint64
	betweenWrites   uint64
	grantWindow     uint64
	interWriteEmpty uint64
	// ring is the SYN ring entry the input added: "" none, "delivered" or
	// "withheld".
	ring string
	// stepRun is the out-of-step run afterwards, -1 when not checked.
	stepRun int
}

func runSYNBoundaryRows(t *testing.T, rows []synBoundaryRow) {
	t.Helper()
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			h := newTransitionHarness(t, transitionUpstreamAsync)
			row.fixture(h)
			h.discardEffects()
			before := synBoundaryReadCounters(h)
			ret := row.input(h)
			got, effects := h.state(), h.effects()
			after := synBoundaryReadCounters(h)
			if got != row.want {
				t.Errorf("state\n got %s\nwant %s", got, row.want)
			}
			if !reflect.DeepEqual(effects, row.effects) {
				t.Errorf("effects\n got %q\nwant %q", effects, row.effects)
			}
			if (ret == nil) != (row.ret == nil) || (ret != nil && *ret != *row.ret) {
				t.Errorf("returned %+v, want %+v", ret, row.ret)
			}
			if after.reason != row.reason {
				t.Errorf("inactive reason %q, want %q", after.reason, row.reason)
			}
			counts := []struct {
				name          string
				before, after uint64
				want          uint64
			}{
				{"active deliveries", before.delivered, after.delivered, row.delivered},
				{"dropped SYNs", before.dropped, after.dropped, row.dropped},
				{"pre-echo suppressions", before.preEcho, after.preEcho, row.preEcho},
				{"between-writes suppressions", before.betweenWrites, after.betweenWrites, row.betweenWrites},
				{"grant-window SYNs", before.grantWindow, after.grantWindow, row.grantWindow},
				{"inter-write-empty SYNs", before.interWriteEmpty, after.interWriteEmpty, row.interWriteEmpty},
			}
			for _, c := range counts {
				if d := c.after - c.before; d != c.want {
					t.Errorf("%s +%d, want +%d", c.name, d, c.want)
				}
			}
			ring := ""
			if len(after.ring) == len(before.ring)+1 {
				ring = "withheld"
				if after.ring[len(after.ring)-1].SynDeliveredToActive {
					ring = "delivered"
				}
			} else if len(after.ring) != len(before.ring) {
				ring = "several"
			}
			if ring != row.ring {
				t.Errorf("SYN ring %q, want %q", ring, row.ring)
			}
			if row.stepRun >= 0 && after.stepRun != row.stepRun {
				t.Errorf("out-of-step run %d, want %d", after.stepRun, row.stepRun)
			}
		})
	}
}

func synBoundarySYN(h *transitionHarness) *synBoundaryReturn {
	h.syn()
	return nil
}

func synBoundaryDirectInput(ev wirePhaseEvent, owner uint64, owned bool) func(h *transitionHarness) *synBoundaryReturn {
	return func(h *transitionHarness) *synBoundaryReturn {
		ret := synBoundaryDirect(h, ev, owner, owned)
		return &ret
	}
}

func synBoundaryLocked(f func(m *Mux)) func(h *transitionHarness) {
	return func(h *transitionHarness) {
		h.m.stateMu.Lock()
		defer h.m.stateMu.Unlock()
		f(h.m)
	}
}

var synBoundaryPassive = []string{"passive: symbol 0xAA"}

func synBoundaryEffects(groups ...[]string) []string {
	out := []string{}
	for _, g := range groups {
		out = append(out, g...)
	}
	return out
}

// TestSYNBoundaryOpeningStep covers the opening step: the SYN clears the
// fence and a recovery other than the reset recovery, and delivers an owed
// resynchronization SYN unless the gateway transaction is live.
func TestSYNBoundaryOpeningStep(t *testing.T) {
	runSYNBoundaryRows(t, []synBoundaryRow{
		{
			name: "SYN clears the fence and an at-SYN recovery",
			fixture: synBoundaryLocked(func(m *Mux) {
				m.deferRegrantUntilNextSyn = true
				m.arbitrationRecovery = arbitrationRecoveryAtSYN
			}),
			input:   synBoundarySYN,
			want:    transitionState{},
			effects: synBoundaryEffects(synBoundaryPassive, receivedSymbolBothSessions("0xAA")),
			stepRun: -1,
		},
		{
			name:    "the reset recovery survives the SYN",
			fixture: synBoundaryLocked(func(m *Mux) { m.arbitrationRecovery = arbitrationRecoveryReset }),
			input:   synBoundarySYN,
			want:    transitionState{recovery: "reset"},
			effects: synBoundaryEffects(synBoundaryPassive, receivedSymbolBothSessions("0xAA")),
			stepRun: -1,
		},
		{
			name:    "an owed SYN goes to the active path",
			fixture: synBoundaryLocked(func(m *Mux) { m.collisionResyncOwed = 2 }),
			input:   synBoundarySYN,
			want:    transitionState{resyncOwed: 1},
			effects: synBoundaryEffects(synBoundaryPassive, []string{"active: 0xAA"}, receivedSymbolBothSessions("0xAA")),
			stepRun: -1,
		},
		{
			name: "an owed SYN waits while the gateway transaction is live",
			fixture: func(h *transitionHarness) {
				transitionGatewayAwaitingEcho(h)
				synBoundaryLocked(func(m *Mux) { m.collisionResyncOwed = 1 })(h)
			},
			input:   synBoundarySYN,
			want:    transitionState{owner: "gateway", gatewayTxn: true, resyncOwed: 1},
			effects: synBoundaryEffects(synBoundaryPassive, receivedSymbolBothSessions("0xAA")),
			preEcho: 1,
			ring:    "withheld",
			stepRun: -1,
		},
	})
}

// TestSYNBoundaryOwedSYNOnAFullActivePath covers the opening step's drop: an
// owed SYN that finds the active path full stays owed and is counted.
func TestSYNBoundaryOwedSYNOnAFullActivePath(t *testing.T) {
	h := newTransitionHarness(t, transitionUpstreamAsync)
	h.m.stateMu.Lock()
	h.m.collisionResyncOwed = 1
	h.m.stateMu.Unlock()
	for len(h.m.activeCh) < cap(h.m.activeCh) {
		h.m.activeCh <- activeEvent{kind: activeEventByte, b: transitionDataSymbol}
	}
	before := synBoundaryReadCounters(h)
	h.syn()
	after := synBoundaryReadCounters(h)
	for len(h.m.activeCh) > 0 {
		<-h.m.activeCh
	}
	if got := h.state(); got != (transitionState{resyncOwed: 1}) {
		t.Errorf("state %s", got)
	}
	if d := after.dropped - before.dropped; d != 1 {
		t.Errorf("dropped SYNs +%d, want +1", d)
	}
}

// TestSYNBoundaryEchoAndTerminatorSteps covers the echo step and the
// terminator step: a SYN mid-write keeps the echo queue so the awaited echo
// still reaches the active path, a SYN before a payload 0xAA echo and a SYN
// between two writes are withheld, and the structural terminator echo
// reaches the active path and ends the transaction.
func TestSYNBoundaryEchoAndTerminatorSteps(t *testing.T) {
	runSYNBoundaryRows(t, []synBoundaryRow{
		{
			name:    "a mid-write SYN keeps the echo queue",
			fixture: transitionGatewayAwaitingEcho,
			input: func(h *transitionHarness) *synBoundaryReturn {
				h.syn()
				h.symbol(transitionGatewayData)
				return nil
			},
			want: transitionState{owner: "gateway", gatewayTxn: true},
			effects: synBoundaryEffects(synBoundaryPassive, []string{"active: 0x15",
				"session 1: RECEIVED 0xAA", "session 1: RECEIVED 0x15", "session 2: RECEIVED 0xAA", "session 2: RECEIVED 0x15"}),
			delivered: 1,
			preEcho:   1,
			ring:      "withheld",
			stepRun:   -1,
		},
		{
			name: "a SYN before a payload 0xAA echo is withheld",
			fixture: func(h *transitionHarness) {
				transitionGatewayTransaction(h)
				h.gatewayWrite(protocol.SymbolSyn, false)
			},
			input:   synBoundarySYN,
			want:    transitionState{owner: "gateway", gatewayTxn: true},
			effects: synBoundaryEffects(synBoundaryPassive, receivedSymbolBothSessions("0xAA")),
			preEcho: 1,
			ring:    "withheld",
			stepRun: -1,
		},
		{
			name:            "a SYN between two writes is withheld",
			fixture:         transitionGatewayTransaction,
			input:           synBoundarySYN,
			want:            transitionState{owner: "gateway", gatewayTxn: true},
			effects:         synBoundaryEffects(synBoundaryPassive, receivedSymbolBothSessions("0xAA")),
			preEcho:         1,
			betweenWrites:   1,
			interWriteEmpty: 1,
			ring:            "withheld",
			stepRun:         -1,
		},
		{
			name: "the terminator SYN reaches the active path",
			fixture: func(h *transitionHarness) {
				transitionGatewayTransaction(h)
				h.gatewayWrite(protocol.SymbolSyn, true)
			},
			input:     synBoundarySYN,
			want:      transitionState{owner: "gateway"},
			effects:   synBoundaryEffects(synBoundaryPassive, []string{"active: 0xAA"}, receivedSymbolBothSessions("0xAA")),
			reason:    ReasonSYNTerminator,
			delivered: 1,
			ring:      "delivered",
			stepRun:   -1,
		},
		{
			name: "the terminator SYN through a direct call cancels the watchdog",
			fixture: func(h *transitionHarness) {
				transitionGatewayTransaction(h)
				h.gatewayWrite(protocol.SymbolSyn, true)
			},
			input:     synBoundaryDirectInput(wirePhaseEventSYNIdle, gatewaySessionID, true),
			want:      transitionState{owner: "gateway", phase: "collect_request"},
			effects:   []string{"active: 0xAA"},
			ret:       &synBoundaryReturn{passive: 1, cancelWatchdog: true},
			reason:    ReasonSYNTerminator,
			delivered: 1,
			ring:      "delivered",
			stepRun:   -1,
		},
	})
}

// TestSYNBoundaryTerminatorOnAFullActivePath covers the terminator step's
// drop: the transaction still ends and the SYN ring records no delivery.
func TestSYNBoundaryTerminatorOnAFullActivePath(t *testing.T) {
	h := newTransitionHarness(t, transitionUpstreamAsync)
	transitionGatewayTransaction(h)
	h.gatewayWrite(protocol.SymbolSyn, true)
	h.discardEffects()
	for len(h.m.activeCh) < cap(h.m.activeCh) {
		h.m.activeCh <- activeEvent{kind: activeEventByte, b: transitionDataSymbol}
	}
	before := synBoundaryReadCounters(h)
	h.syn()
	after := synBoundaryReadCounters(h)
	for len(h.m.activeCh) > 0 {
		<-h.m.activeCh
	}
	if got := h.state(); got != (transitionState{owner: "gateway"}) {
		t.Errorf("state %s", got)
	}
	if d := after.dropped - before.dropped; d != 1 {
		t.Errorf("dropped SYNs +%d, want +1", d)
	}
	if d := after.delivered - before.delivered; d != 0 {
		t.Errorf("active deliveries +%d, want +0", d)
	}
	if after.reason != ReasonSYNTerminator {
		t.Errorf("inactive reason %q", after.reason)
	}
	if n := len(after.ring); n != len(before.ring)+1 || after.ring[n-1].SynDeliveredToActive {
		t.Errorf("SYN ring %+v", after.ring)
	}
}

// TestSYNBoundaryReleaseSteps covers the three release steps through direct
// calls and the handler: a completed transaction releases the owner (and
// completes a session's out-of-step run), a SYN timeout releases the gateway
// at once and a session only past its gap, and an idle SYN releases the
// gateway past its grace or for an external START, a session past its gap,
// and never a gateway mid-write with live echoes.
func TestSYNBoundaryReleaseSteps(t *testing.T) {
	// The gateway's grace runs from the grant, which also starts the
	// ownership limit, so the gateway's rows shorten the grace instead of
	// moving the grant; a session's gap runs from the last wire activity.
	shortGrace := synBoundaryLocked(func(m *Mux) { m.cfg.IdleReleaseGrace = time.Nanosecond })
	wireGap := synBoundaryLocked(func(m *Mux) { m.lastWireActivity = time.Now().Add(-2 * transitionTimerHorizon) })
	then := func(fs ...func(h *transitionHarness)) func(h *transitionHarness) {
		return func(h *transitionHarness) {
			for _, f := range fs {
				f(h)
			}
		}
	}
	setRun := synBoundaryLocked(func(m *Mux) { m.step.run = 2 })
	sessionPhase := transitionState{phase: "collect_request"}
	runSYNBoundaryRows(t, []synBoundaryRow{
		{
			name:    "a completed transaction releases the session",
			fixture: then(receivedSymbolSessionOwned, setRun),
			input:   synBoundaryDirectInput(wirePhaseEventTransactionDone, 1, true),
			want:    sessionPhase,
			effects: []string{},
			ret:     &synBoundaryReturn{passive: 1},
			stepRun: 0,
		},
		{
			name:    "a completed transaction ends the gateway transaction",
			fixture: then(transitionGatewayTransaction, setRun),
			input:   synBoundaryDirectInput(wirePhaseEventTransactionDone, gatewaySessionID, true),
			want:    sessionPhase,
			effects: []string{},
			ret:     &synBoundaryReturn{passive: 1, preEcho: true},
			reason:  ReasonTransactionDone,
			preEcho: 1, betweenWrites: 1, interWriteEmpty: 1,
			ring:    "withheld",
			stepRun: 0,
		},
		{
			name:        "a completed transaction after the gateway transaction keeps the run",
			fixture:     then(receivedSymbolGatewayGrace, setRun),
			input:       synBoundaryDirectInput(wirePhaseEventTransactionDone, gatewaySessionID, true),
			want:        transitionState{},
			effects:     []string{},
			ret:         &synBoundaryReturn{passive: 1},
			reason:      ReasonSYNTerminator,
			grantWindow: 1,
			ring:        "withheld",
			stepRun:     2,
		},
		{
			name:    "a SYN timeout releases the gateway and cancels the watchdog",
			fixture: transitionGatewayAwaitingEcho,
			input:   synBoundaryDirectInput(wirePhaseEventSYNTimeout, gatewaySessionID, true),
			want:    transitionState{phase: "collect_request"},
			effects: []string{},
			ret:     &synBoundaryReturn{passive: 1, preEcho: true, cancelWatchdog: true},
			reason:  ReasonSYNTimeout,
			preEcho: 1,
			ring:    "withheld",
			stepRun: -1,
		},
		{
			name:    "a SYN timeout inside the session's gap keeps the session",
			fixture: receivedSymbolSessionOwned,
			input:   synBoundaryDirectInput(wirePhaseEventSYNTimeout, 1, true),
			want:    transitionState{owner: "session 1", phase: "collect_request"},
			effects: []string{},
			ret:     &synBoundaryReturn{passive: 1},
			stepRun: -1,
		},
		{
			name:    "a SYN timeout past the session's gap releases it",
			fixture: then(receivedSymbolSessionOwned, wireGap),
			input:   synBoundaryDirectInput(wirePhaseEventSYNTimeout, 1, true),
			want:    sessionPhase,
			effects: []string{},
			ret:     &synBoundaryReturn{passive: 1},
			stepRun: -1,
		},
		{
			name:        "an idle SYN inside the gateway's grace keeps the gateway",
			fixture:     receivedSymbolGatewayGrace,
			input:       synBoundarySYN,
			want:        transitionState{owner: "gateway"},
			effects:     synBoundaryEffects(synBoundaryPassive, receivedSymbolBothSessions("0xAA")),
			reason:      ReasonSYNTerminator,
			grantWindow: 1,
			ring:        "withheld",
			stepRun:     -1,
		},
		{
			name:        "an idle SYN past the gateway's grace releases it",
			fixture:     then(receivedSymbolGatewayGrace, shortGrace),
			input:       synBoundarySYN,
			want:        transitionState{},
			effects:     synBoundaryEffects(synBoundaryPassive, receivedSymbolBothSessions("0xAA")),
			reason:      ReasonSYNTerminator,
			grantWindow: 1,
			ring:        "withheld",
			stepRun:     -1,
		},
		{
			name: "an external START releases the gateway inside its grace",
			fixture: func(h *transitionHarness) {
				receivedSymbolGatewayGrace(h)
				h.bid(1, transitionSessionSource)
			},
			input:       synBoundarySYN,
			want:        transitionState{start: "session 1 0x31"},
			effects:     synBoundaryEffects(synBoundaryPassive, []string{"adapter: RequestStart 0x31"}, receivedSymbolBothSessions("0xAA")),
			reason:      ReasonSYNTerminator,
			grantWindow: 1,
			ring:        "withheld",
			stepRun:     -1,
		},
		{
			name:    "an idle SYN past the grace ends a gateway transaction without echoes",
			fixture: then(transitionGatewayAwaitingEcho, shortGrace),
			input:   synBoundaryDirectInput(wirePhaseEventSYNIdle, gatewaySessionID, true),
			want:    transitionState{phase: "collect_request"},
			effects: []string{},
			ret:     &synBoundaryReturn{passive: 1, preEcho: true, cancelWatchdog: true},
			reason:  ReasonSYNIdle,
			preEcho: 1,
			ring:    "withheld",
			stepRun: -1,
		},
		{
			name: "an idle SYN past the grace keeps a gateway mid-write with live echoes",
			fixture: func(h *transitionHarness) {
				transitionGatewayTransaction(h)
				h.gatewayWrite(transitionDataSymbol, false)
				shortGrace(h)
			},
			input:   synBoundarySYN,
			want:    transitionState{owner: "gateway", gatewayTxn: true},
			effects: synBoundaryEffects(synBoundaryPassive, receivedSymbolBothSessions("0xAA")),
			preEcho: 1,
			ring:    "withheld",
			stepRun: -1,
		},
		{
			name:    "an idle SYN past the session's gap releases it",
			fixture: then(receivedSymbolSessionOwned, wireGap),
			input:   synBoundaryDirectInput(wirePhaseEventSYNIdle, 1, true),
			want:    sessionPhase,
			effects: []string{},
			ret:     &synBoundaryReturn{passive: 1},
			stepRun: -1,
		},
		{
			name:    "an idle SYN inside the session's gap keeps it",
			fixture: receivedSymbolSessionOwned,
			input:   synBoundaryDirectInput(wirePhaseEventSYNIdle, 1, true),
			want:    transitionState{owner: "session 1", phase: "collect_request"},
			effects: []string{},
			ret:     &synBoundaryReturn{passive: 1},
			stepRun: -1,
		},
		{
			name:    "an unpinned owner is not released",
			fixture: then(receivedSymbolSessionOwned, wireGap),
			input:   synBoundaryDirectInput(wirePhaseEventSYNIdle, 1, false),
			want:    transitionState{owner: "session 1", phase: "collect_request"},
			effects: []string{},
			ret:     &synBoundaryReturn{passive: 1},
			stepRun: -1,
		},
	})
}

// TestSYNBoundaryPreEchoAndDiagnosticsSteps covers the pre-echo step and the
// diagnostics step: every SYN reaches the passive path, a SYN before the
// first echo is withheld and counted, a queued bid asks for a grant attempt,
// and an unowned SYN records no diagnostics.
func TestSYNBoundaryPreEchoAndDiagnosticsSteps(t *testing.T) {
	runSYNBoundaryRows(t, []synBoundaryRow{
		{
			name:    "a SYN before the first echo is withheld",
			fixture: transitionGatewayAwaitingEcho,
			input:   synBoundaryDirectInput(wirePhaseEventSYNIdle, gatewaySessionID, true),
			want:    transitionState{owner: "gateway", gatewayTxn: true, phase: "collect_request"},
			effects: []string{},
			ret:     &synBoundaryReturn{passive: 1, preEcho: true},
			preEcho: 1,
			ring:    "withheld",
			stepRun: -1,
		},
		{
			name:    "a queued bid asks for a grant attempt",
			fixture: func(h *transitionHarness) { h.bid(1, transitionSessionSource) },
			input:   synBoundaryDirectInput(wirePhaseEventSYNIdle, gatewaySessionID, false),
			want:    transitionState{bids: "session 1 0x31"},
			effects: []string{},
			ret:     &synBoundaryReturn{passive: 1, tryGrant: true},
			stepRun: -1,
		},
		{
			name:    "an unowned SYN records no diagnostics",
			fixture: func(*transitionHarness) {},
			input:   synBoundarySYN,
			want:    transitionState{},
			effects: synBoundaryEffects(synBoundaryPassive, receivedSymbolBothSessions("0xAA")),
			stepRun: -1,
		},
	})
}
