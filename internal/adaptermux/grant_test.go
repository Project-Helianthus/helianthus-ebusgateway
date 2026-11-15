package adaptermux

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// The grant tests cover the pure decisions of the admission gates and of the
// START outcome, and each named step of tryGrantAndStartForBids through the
// harness: admission, selection, loser notification, the deadline step with
// its expiry, and the START step on the non-blocking write, the blocking call,
// the refusal at shutdown and a transport without START.

// TestGrantAdmissionDecisionVariesEveryInput covers decideGrantAdmission.
// Each row varies one input against the idle state; the production mutation
// each input catches:
//
//   - event kind: dropping the transition function's validity check;
//   - start.pending: dropping the START-in-flight gate, or checking it after
//     the absorb barrier (the in-flight log would be lost);
//   - absorb: dropping the absorb barrier, or checking it after the blocking
//     call;
//   - blocking: dropping the blocking-call gate, or checking it after the
//     fence;
//   - phase muxFenced: dropping the fence gate, or checking it after the hold;
//   - held: dropping the hold gate;
//   - phase owned, recovery, resyncOwed, retired and queued bids: gating
//     admission on a field it does not read (the current owner is the
//     selection's gate).
func TestGrantAdmissionDecisionVariesEveryInput(t *testing.T) {
	selected := muxSelection{pending: true, party: 2, initiator: 0x71}
	cases := []struct {
		name  string
		state muxState
		ev    muxEvent
		want  grantGate
	}{
		{"idle", muxState{}, grantEvent, grantAdmitted},
		{"bids queued", muxState{phase: muxBidsQueued, bids: muxModelBids(1)}, grantEvent, grantAdmitted},
		{"an event that cannot occur", muxState{}, startDeadlineEvent, grantSkipInvalid},
		{"START in flight", muxState{phase: muxStartAsync, start: selected}, grantEvent, grantSkipStartInFlight},
		{"START in flight before absorb", muxState{phase: muxStartAsync, start: selected, absorb: 1}, grantEvent, grantSkipStartInFlight},
		{"absorb", muxState{absorb: 1}, grantEvent, grantSkipAbsorb},
		{"absorb before blocking", muxState{absorb: 2, blocking: true}, grantEvent, grantSkipAbsorb},
		{"blocking call", muxState{blocking: true}, grantEvent, grantSkipBlockingCall},
		{"blocking before fence", muxState{phase: muxFenced, blocking: true}, grantEvent, grantSkipBlockingCall},
		{"fence", muxState{phase: muxFenced}, grantEvent, grantSkipFence},
		{"fence before hold", muxState{phase: muxFenced, held: true}, grantEvent, grantSkipFence},
		{"hold", muxState{held: true}, grantEvent, grantSkipHeld},
		{"owner is not an admission gate", muxState{phase: muxSessionOwned, owner: 2}, grantEvent, grantAdmitted},
		{"recovery is not read", muxState{recovery: arbitrationRecoveryReset}, grantEvent, grantAdmitted},
		{"owed SYNs are not read", muxState{resyncOwed: 2}, grantEvent, grantAdmitted},
		{"retired selection is not read", muxState{retired: true, retiredSource: 0x31}, grantEvent, grantAdmitted},
	}
	for _, tc := range cases {
		if got := decideGrantAdmission(tc.state, tc.ev); got != tc.want {
			t.Errorf("%s: gate %d, want %d", tc.name, got, tc.want)
		}
	}
}

// TestGrantStartWriteDecisionVariesEveryInput covers decideStartWrite. The
// production mutation each input catches:
//
//   - write: applying the non-blocking write's barrier rules to the blocking
//     call, or fencing on a refusal;
//   - failed: granting a failed call, or failing a won one;
//   - ownsSelection: notifying or clearing a selection another path resolved,
//     or granting it;
//   - cancelledInFlight and resetOrDisconnect: failing a superseded waiter on
//     the wire, or hiding a reset boundary behind the cancelled flag;
//   - currentCall: letting a stale call clear the barrier of a newer one or
//     consume its absorb budget;
//   - state blocking: deciding a call the transition function says is not
//     outstanding;
//   - state absorb: consuming a result nobody owes, clearing the skip log of
//     a barrier that stays, or reopening selection before it drains;
//   - state recovery: clearing a reset recovery, or keeping the at-SYN one.
func TestGrantStartWriteDecisionVariesEveryInput(t *testing.T) {
	blocking := muxState{blocking: true}
	withAbsorb := func(s muxState, n int) muxState {
		s.absorb = n
		return s
	}
	cases := []struct {
		name  string
		state muxState
		in    startWriteInput
		want  startWriteDecision
	}{
		{"write failed, owned", muxState{}, startWriteInput{write: startWriteRequest, failed: true, ownsSelection: true},
			startWriteDecision{withdraw: true}},
		{"write failed, owned, cancelled", muxState{}, startWriteInput{write: startWriteRequest, failed: true, ownsSelection: true, cancelledInFlight: true},
			startWriteDecision{withdraw: true, cancelled: true}},
		{"write failed, owned, cancelled by a reset", muxState{}, startWriteInput{write: startWriteRequest, failed: true, ownsSelection: true, cancelledInFlight: true, resetOrDisconnect: true},
			startWriteDecision{withdraw: true}},
		{"write failed, resolved, nothing to absorb", muxState{}, startWriteInput{write: startWriteRequest, failed: true},
			startWriteDecision{reopen: true}},
		{"write failed, resolved, last result to absorb", muxState{absorb: 1}, startWriteInput{write: startWriteRequest, failed: true},
			startWriteDecision{consumeAbsorb: true, clearSkipLog: true, reopen: true}},
		{"write failed, resolved, two results to absorb", muxState{absorb: 2}, startWriteInput{write: startWriteRequest, failed: true},
			startWriteDecision{consumeAbsorb: true}},
		{"write failed, resolved, at-SYN recovery", muxState{absorb: 1, recovery: arbitrationRecoveryAtSYN}, startWriteInput{write: startWriteRequest, failed: true},
			startWriteDecision{consumeAbsorb: true, clearSkipLog: true, reopen: true, clearAtSYNRecovery: true}},
		{"write failed, resolved, at-SYN recovery kept", muxState{absorb: 2, recovery: arbitrationRecoveryAtSYN}, startWriteInput{write: startWriteRequest, failed: true},
			startWriteDecision{consumeAbsorb: true}},
		{"write failed, resolved, reset recovery kept", muxState{recovery: arbitrationRecoveryReset}, startWriteInput{write: startWriteRequest, failed: true},
			startWriteDecision{reopen: true}},
		{"write failed ignores the call generation", muxState{}, startWriteInput{write: startWriteRequest, failed: true, ownsSelection: true, currentCall: true},
			startWriteDecision{withdraw: true}},

		{"call failed, owned, current", blocking, startWriteInput{write: startWriteBlocking, failed: true, ownsSelection: true, currentCall: true},
			startWriteDecision{callCurrent: true, clearBlocking: true, withdraw: true}},
		{"call failed, owned, cancelled", blocking, startWriteInput{write: startWriteBlocking, failed: true, ownsSelection: true, currentCall: true, cancelledInFlight: true},
			startWriteDecision{callCurrent: true, clearBlocking: true, withdraw: true, cancelled: true}},
		{"call failed, owned, cancelled by a reset", blocking, startWriteInput{write: startWriteBlocking, failed: true, ownsSelection: true, currentCall: true, cancelledInFlight: true, resetOrDisconnect: true},
			startWriteDecision{callCurrent: true, clearBlocking: true, withdraw: true}},
		{"call failed, owned, stale generation", blocking, startWriteInput{write: startWriteBlocking, failed: true, ownsSelection: true},
			startWriteDecision{withdraw: true}},
		{"call failed, resolved, last result to absorb", withAbsorb(blocking, 1), startWriteInput{write: startWriteBlocking, failed: true, currentCall: true},
			startWriteDecision{callCurrent: true, clearBlocking: true, consumeAbsorb: true, clearSkipLog: true}},
		{"call failed, resolved, two results to absorb", withAbsorb(blocking, 2), startWriteInput{write: startWriteBlocking, failed: true, currentCall: true},
			startWriteDecision{callCurrent: true, clearBlocking: true, consumeAbsorb: true}},
		{"call failed, resolved, stale generation", withAbsorb(blocking, 1), startWriteInput{write: startWriteBlocking, failed: true},
			startWriteDecision{}},
		{"call failed, no blocking call in the state", muxState{absorb: 1}, startWriteInput{write: startWriteBlocking, failed: true, currentCall: true},
			startWriteDecision{}},
		{"call returned, owned", blocking, startWriteInput{write: startWriteBlocking, ownsSelection: true, currentCall: true},
			startWriteDecision{callCurrent: true, grant: true}},
		{"call returned, owned, stale generation", blocking, startWriteInput{write: startWriteBlocking, ownsSelection: true},
			startWriteDecision{grant: true}},
		{"call returned, resolved", blocking, startWriteInput{write: startWriteBlocking, currentCall: true},
			startWriteDecision{callCurrent: true, clearBlocking: true, fence: true, countUndeliverable: true}},
		{"call returned, resolved, result to absorb", withAbsorb(blocking, 1), startWriteInput{write: startWriteBlocking, currentCall: true},
			startWriteDecision{callCurrent: true, clearBlocking: true, fence: true, countUndeliverable: true, consumeAbsorb: true, clearSkipLog: true}},
		{"call returned, resolved, stale generation", withAbsorb(blocking, 1), startWriteInput{write: startWriteBlocking},
			startWriteDecision{}},
		{"call returned, no blocking call in the state", muxState{}, startWriteInput{write: startWriteBlocking, currentCall: true},
			startWriteDecision{}},

		{"refused, owned", muxState{}, startWriteInput{write: startWriteRefused, ownsSelection: true}, startWriteDecision{withdraw: true}},
		{"refused, resolved", muxState{absorb: 1}, startWriteInput{write: startWriteRefused}, startWriteDecision{}},
		{"unsupported, owned", muxState{}, startWriteInput{write: startWriteUnsupported, ownsSelection: true}, startWriteDecision{withdraw: true, fence: true}},
		{"unsupported, resolved", muxState{absorb: 1}, startWriteInput{write: startWriteUnsupported}, startWriteDecision{}},
	}
	for _, tc := range cases {
		if got := decideStartWrite(tc.state, tc.in); got != tc.want {
			t.Errorf("%s:\n got %+v\nwant %+v", tc.name, got, tc.want)
		}
	}
}

// grantLog captures the mux log lines of one harness.
type grantLog struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (l *grantLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

// take returns the captured text and clears it.
func (l *grantLog) take() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := l.buf.String()
	l.buf.Reset()
	return out
}

func captureGrantLog(h *transitionHarness) *grantLog {
	l := &grantLog{}
	h.m.logger.SetOutput(l)
	return l
}

// grantLocked runs f under stateMu.
func grantLocked(h *transitionHarness, f func(m *Mux)) {
	h.m.stateMu.Lock()
	defer h.m.stateMu.Unlock()
	f(h.m)
}

// grantEffectsEqual compares two effect logs.
func grantEffectsEqual(t *testing.T, name string, got, want []string) {
	t.Helper()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("%s: effects\n got %q\nwant %q", name, got, want)
	}
}

// TestGrantAdmissionStep covers the admission step: each gate skips the
// attempt with its log line and leaves the bid queued, the gates keep their
// order, and the current owner refuses a winner in the selection step.
func TestGrantAdmissionStep(t *testing.T) {
	foreignStart := func(m *Mux) {
		m.pendingStart = &pendingStartState{sessionID: 2, initiator: transitionOtherSource, notify: make(chan startResult, 1)}
	}
	cases := []struct {
		name    string
		set     func(m *Mux)
		log     string
		want    transitionState
		effects []string
	}{
		{name: "admitted", log: "RequestStart(0x31) sent for session 1",
			want: transitionState{start: "session 1 0x31"}, effects: []string{"adapter: RequestStart 0x31"}},
		{name: "START in flight", set: foreignStart, log: "pendingStart already set for session 2",
			want: transitionState{start: "session 2 0x71", bids: "session 1 0x31"}},
		{name: "START in flight before absorb", set: func(m *Mux) { foreignStart(m); m.pendingStartAbsorb = 1 },
			log:  "pendingStart already set for session 2",
			want: transitionState{start: "session 2 0x71", absorb: 1, bids: "session 1 0x31"}},
		{name: "absorb", set: func(m *Mux) { m.pendingStartAbsorb = 1 }, log: "waiting to absorb 1 stale arbitration response(s)",
			want: transitionState{absorb: 1, bids: "session 1 0x31"}},
		{name: "absorb before blocking call", set: func(m *Mux) { m.pendingStartAbsorb = 2; m.blockingArbActive = true },
			log:  "waiting to absorb 2 stale arbitration response(s)",
			want: transitionState{absorb: 2, blockingCall: true, bids: "session 1 0x31"}},
		{name: "blocking call", set: func(m *Mux) { m.blockingArbActive = true; m.blockingArbGen = 3 },
			log: "blocking StartArbitration gen 3 still in-flight", want: transitionState{blockingCall: true, bids: "session 1 0x31"}},
		{name: "blocking call before fence", set: func(m *Mux) { m.blockingArbActive = true; m.deferRegrantUntilNextSyn = true },
			log: "blocking StartArbitration gen 0 still in-flight", want: transitionState{blockingCall: true, regrantFence: true, bids: "session 1 0x31"}},
		{name: "fence", set: func(m *Mux) { m.deferRegrantUntilNextSyn = true },
			log: "undeliverable STARTED awaiting next SYN", want: transitionState{regrantFence: true, bids: "session 1 0x31"}},
		{name: "fence before hold", set: func(m *Mux) { m.deferRegrantUntilNextSyn = true; m.step.held = true },
			log: "undeliverable STARTED awaiting next SYN", want: transitionState{regrantFence: true, bids: "session 1 0x31"}},
		{name: "hold", set: func(m *Mux) { m.step.held = true },
			log: "active arbitration held (" + holdReasonAdapterOutOfStep + ")", want: transitionState{bids: "session 1 0x31"}},
		{name: "current owner", set: func(m *Mux) { m.arb.confirmOwnership(2, transitionOtherSource) },
			want: transitionState{owner: "session 2", bids: "session 1 0x31"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTransitionHarness(t, transitionUpstreamAsync)
			logs := captureGrantLog(h)
			h.bid(1, transitionSessionSource)
			if tc.set != nil {
				grantLocked(h, tc.set)
			}
			h.discardEffects()
			logs.take()
			h.m.tryGrantAndStart()
			if got := h.state(); got != tc.want {
				t.Errorf("state %s, want %s", got, tc.want)
			}
			effects := tc.effects
			if effects == nil {
				effects = []string{}
			}
			grantEffectsEqual(t, tc.name, h.effects(), effects)
			got := logs.take()
			if tc.log == "" {
				if strings.Contains(got, "skipped") || strings.Contains(got, "RequestStart") {
					t.Errorf("log %q, want no skip and no START", got)
				}
			} else if strings.Count(got, "adaptermux: ") != 1 || !strings.Contains(got, tc.log) {
				t.Errorf("log %q, want one line with %q", got, tc.log)
			}
		})
	}
}

// TestGrantAbsorbSkipLogRateLimit covers the absorb gate's log: a skip within
// a second of the last line is only counted, and the next line reports and
// clears the count.
func TestGrantAbsorbSkipLogRateLimit(t *testing.T) {
	h := newTransitionHarness(t, transitionUpstreamAsync)
	logs := captureGrantLog(h)
	recent := time.Now()
	grantLocked(h, func(m *Mux) {
		m.pendingStartAbsorb = 1
		m.absorbSkipLogLastEmit = recent
		m.absorbSkipLogSuppressedSince = 2
	})
	h.m.tryGrantAndStart()
	if got := logs.take(); got != "" {
		t.Errorf("log %q within the second, want none", got)
	}
	grantLocked(h, func(m *Mux) {
		if m.absorbSkipLogSuppressedSince != 3 || !m.absorbSkipLogLastEmit.Equal(recent) {
			t.Errorf("suppressed %d last %v, want 3 and unchanged", m.absorbSkipLogSuppressedSince, m.absorbSkipLogLastEmit)
		}
		m.absorbSkipLogLastEmit = recent.Add(-2 * time.Second)
	})
	h.m.tryGrantAndStart()
	if got := logs.take(); !strings.Contains(got, "waiting to absorb 1 stale arbitration response(s) (3 suppressed)") {
		t.Errorf("log %q, want the suppressed count", got)
	}
	grantLocked(h, func(m *Mux) {
		if m.absorbSkipLogSuppressedSince != 0 || !m.absorbSkipLogLastEmit.After(recent) {
			t.Errorf("suppressed %d last %v, want 0 and a new emission", m.absorbSkipLogSuppressedSince, m.absorbSkipLogLastEmit)
		}
	})
}

// TestGrantSelectionAndLoserSteps covers the selection step with a bid
// snapshot and the loser step: an external loser gets FAILED naming the
// winner before the START and its result, a departed loser only its result,
// and the gateway's loss waits for the START outcome, which owes the
// collision resynchronization SYNs.
func TestGrantSelectionAndLoserSteps(t *testing.T) {
	t.Run("external loser", func(t *testing.T) {
		h := newTransitionHarness(t, transitionUpstreamAsync)
		h.bid(1, transitionSessionSource)
		h.bid(2, transitionOtherSource)
		h.discardEffects()
		h.m.tryGrantAndStart()
		if got, want := h.state(), (transitionState{start: "session 1 0x31", localLosers: "session 2"}); got != want {
			t.Errorf("state %s, want %s", got, want)
		}
		grantEffectsEqual(t, "external loser", h.effects(), []string{
			"adapter: RequestStart 0x31",
			"session 2: FAILED 0x31",
			"result session 2 0x71: source 0x31, failed queued",
		})
		if !h.sessions[1].parserResetNeeded.Load() {
			t.Error("loser parser reset not requested")
		}
	})
	t.Run("departed loser", func(t *testing.T) {
		h := newTransitionHarness(t, transitionUpstreamAsync)
		h.bid(1, transitionSessionSource)
		h.bid(2, transitionOtherSource)
		h.m.sessionsMu.Lock()
		delete(h.m.sessions, 2)
		h.m.sessionsMu.Unlock()
		h.discardEffects()
		h.m.tryGrantAndStart()
		grantEffectsEqual(t, "departed loser", h.effects(), []string{
			"adapter: RequestStart 0x31",
			"result session 2 0x71: source 0x31",
		})
	})
	t.Run("gateway loser", func(t *testing.T) {
		h := newTransitionHarness(t, transitionUpstreamAsync)
		h.bid(gatewaySessionID, transitionGatewaySource)
		h.bid(1, transitionSessionSource)
		h.discardEffects()
		h.m.tryGrantAndStart()
		want := transitionState{start: "session 1 0x31", resyncOwed: protocol.CollisionRetryResyncSYNCount, localLosers: "gateway"}
		if got := h.state(); got != want {
			t.Errorf("state %s, want %s", got, want)
		}
		grantEffectsEqual(t, "gateway loser", h.effects(), []string{
			"adapter: RequestStart 0x31",
			"result gateway 0x7F: source 0x31",
		})
	})
	t.Run("bid snapshot", func(t *testing.T) {
		h := newTransitionHarness(t, transitionUpstreamAsync)
		h.bid(1, transitionSessionSource)
		bids := h.m.arb.snapshotBids()
		h.bid(2, 0x21) // higher priority, but after the observed SYN
		h.discardEffects()
		h.m.tryGrantAndStartForBids(bids)
		if got, want := h.state(), (transitionState{start: "session 1 0x31", bids: "session 2 0x21"}); got != want {
			t.Errorf("state %s, want %s", got, want)
		}
		grantEffectsEqual(t, "bid snapshot", h.effects(), []string{"adapter: RequestStart 0x31"})
	})
}

// TestGrantDeadlineStep covers the deadline step: it records the selection,
// opens a START cycle and arms the deadline, whose expiry fails the waiter,
// fences and arms the absorb barrier; a superseded waiter is told it was
// cancelled.
func TestGrantDeadlineStep(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		h := newTransitionHarness(t, transitionUpstreamAsync)
		h.bid(1, transitionSessionSource)
		var cycle uint64
		grantLocked(h, func(m *Mux) {
			m.retiredSelection = true
			m.step.cycleBad = true
			cycle = m.step.cycle
		})
		h.discardEffects()
		h.m.tryGrantAndStart()
		grantLocked(h, func(m *Mux) {
			p := m.pendingStart
			if p == nil || p.deadline == nil || p.req == nil || p.blockingArb || p.responseReceived ||
				p.lifecycleEpoch != m.arb.lifecycleEpoch.Load() || p.sessionID != 1 || p.initiator != transitionSessionSource {
				t.Fatalf("selection %+v", p)
			}
			if m.retiredSelection || m.localLoserSource != transitionSessionSource || m.step.cycle != cycle+1 || m.step.cycleBad {
				t.Errorf("retired %v source 0x%02X cycle %d bad %v", m.retiredSelection, m.localLoserSource, m.step.cycle-cycle, m.step.cycleBad)
			}
			if cancelled {
				p.req.cancelled.Store(true)
			}
		})
		h.discardEffects()
		h.captureDeadline()
		h.fireDeadline("session 1 0x31")
		want := transitionState{absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn"}
		if got := h.state(); got != want {
			t.Errorf("cancelled %v: state %s, want %s", cancelled, got, want)
		}
		result := "result session 1 0x31: source 0x31, error adaptermux: START deadline expired"
		if cancelled {
			result = "result session 1 0x31: cancelled, source 0x31, error adaptermux: START deadline expired (superseded by client re-submit)"
		}
		grantEffectsEqual(t, "deadline", h.effects(), []string{result})
	}
}

// TestGrantDeadlineExpiryIsScoped covers the deadline callback directly: it
// expires only the selection it was armed for, only before its result, and
// ends the gateway's deferred loss.
func TestGrantDeadlineExpiryIsScoped(t *testing.T) {
	h := newTransitionHarness(t, transitionUpstreamAsync)
	notify := make(chan startResult, 1)
	other := make(chan startResult, 1)
	selection := func(m *Mux) {
		m.pendingStart = &pendingStartState{sessionID: 1, initiator: transitionSessionSource, notify: notify}
	}
	// No START in flight: the transition function rules the deadline out.
	h.m.expireStartDeadline(notify, nil)
	// Another selection's deadline.
	grantLocked(h, selection)
	h.m.expireStartDeadline(other, nil)
	// The result already arrived.
	grantLocked(h, func(m *Mux) { m.pendingStart.responseReceived = true })
	h.m.expireStartDeadline(notify, nil)
	if got, want := h.state(), (transitionState{start: "session 1 0x31", startResponse: true}); got != want {
		t.Errorf("state %s, want %s", got, want)
	}
	if len(notify) != 0 || len(other) != 0 {
		t.Fatal("a scoped-out deadline delivered a result")
	}

	loser := &startRequest{sessionID: gatewaySessionID, initiator: transitionGatewaySource, notify: make(chan startResult, 1)}
	grantLocked(h, func(m *Mux) {
		selection(m)
		m.arb.mu.Lock()
		m.arb.deferredGatewayLoser = loser
		m.arb.mu.Unlock()
	})
	h.m.expireStartDeadline(notify, loser)
	if res := <-loser.notify; !errors.Is(res.err, ebuserrors.ErrTimeout) || res.initiator != transitionGatewaySource {
		t.Errorf("gateway loser result %+v", res)
	}
	if res := <-notify; res.err == nil || res.err.Error() != "adaptermux: START deadline expired" {
		t.Errorf("waiter result %+v", res)
	}
	if got, want := h.state(), (transitionState{absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn"}); got != want {
		t.Errorf("state %s, want %s", got, want)
	}
}

// grantFailingUpstream answers RequestStart with err after running during.
type grantFailingUpstream struct {
	*transitionUpstream
	during func()
	err    error
}

func (u grantFailingUpstream) RequestStart(initiator byte) error {
	u.rec.add("adapter: RequestStart 0x%02X", initiator)
	if u.during != nil {
		u.during()
	}
	return u.err
}

// grantSetUpstream replaces the harness upstream.
func grantSetUpstream(h *transitionHarness, upstream transport.RawTransport) {
	h.m.connMu.Lock()
	defer h.m.connMu.Unlock()
	h.m.upstream = upstream
}

// grantPlainUpstream offers neither RequestStart nor StartArbitration.
type grantPlainUpstream struct {
	*transitionUpstream
}

// TestGrantRequestStartFailure covers the START step's non-blocking write
// failing: an owned selection is withdrawn (its deadline stopped) and its
// waiter failed, cancelled unless the error is a reset boundary; the local
// losses are undone and the gateway's deferred loss gets the error; a
// selection another path resolved during the write consumes the result it
// left to absorb and reopens selection once that barrier drains.
func TestGrantRequestStartFailure(t *testing.T) {
	errWrite := errors.New("write failed")
	type row struct {
		name    string
		err     error
		during  func(h *transitionHarness)
		want    transitionState
		effects []string
	}
	cancel := func(h *transitionHarness) { h.m.cancelPendingStart(1) }
	rows := []row{
		{name: "owned", err: errWrite,
			effects: []string{"adapter: RequestStart 0x31", "result session 1 0x31: source 0x31, error write failed"}},
		{name: "owned, cancelled", err: errWrite,
			during:  func(h *transitionHarness) { grantLocked(h, func(m *Mux) { m.pendingStart.req.cancelled.Store(true) }) },
			effects: []string{"adapter: RequestStart 0x31", "result session 1 0x31: cancelled, source 0x31, error write failed"}},
		{name: "owned, cancelled, reset", err: ebuserrors.ErrAdapterReset,
			during:  func(h *transitionHarness) { grantLocked(h, func(m *Mux) { m.pendingStart.req.cancelled.Store(true) }) },
			effects: []string{"adapter: RequestStart 0x31", "result session 1 0x31: source 0x31, error " + ebuserrors.ErrAdapterReset.Error()}},
		{name: "resolved, barrier drains", err: errWrite, during: cancel,
			effects: []string{"adapter: RequestStart 0x31", "result session 1 0x31: cancelled, source 0x31"}},
		{name: "resolved, barrier stays", err: errWrite,
			during:  func(h *transitionHarness) { cancel(h); grantLocked(h, func(m *Mux) { m.pendingStartAbsorb++ }) },
			want:    transitionState{absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn"},
			effects: []string{"adapter: RequestStart 0x31", "result session 1 0x31: cancelled, source 0x31"}},
		{name: "resolved, reset recovery kept", err: errWrite,
			during: func(h *transitionHarness) {
				cancel(h)
				grantLocked(h, func(m *Mux) { m.arbitrationRecovery = arbitrationRecoveryReset })
			},
			want:    transitionState{recovery: "reset"},
			effects: []string{"adapter: RequestStart 0x31", "result session 1 0x31: cancelled, source 0x31"}},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			h := newTransitionHarness(t, transitionUpstreamAsync)
			var deadline *time.Timer
			grantSetUpstream(h, grantFailingUpstream{transitionUpstream: h.up, err: tc.err, during: func() {
				grantLocked(h, func(m *Mux) { deadline = m.pendingStart.deadline })
				if tc.during != nil {
					tc.during(h)
				}
			}})
			h.bid(1, transitionSessionSource)
			h.discardEffects()
			h.m.tryGrantAndStart()
			if got := h.state(); got != tc.want {
				t.Errorf("state %s, want %s", got, tc.want)
			}
			grantEffectsEqual(t, tc.name, h.effects(), tc.effects)
			if deadline == nil || deadline.Stop() {
				t.Error("the START deadline was not stopped")
			}
		})
	}
	t.Run("losers", func(t *testing.T) {
		h := newTransitionHarness(t, transitionUpstreamAsync)
		grantSetUpstream(h, grantFailingUpstream{transitionUpstream: h.up, err: errWrite})
		h.bid(gatewaySessionID, transitionGatewaySource)
		h.bid(1, transitionSessionSource)
		h.bid(2, transitionOtherSource)
		h.discardEffects()
		h.m.tryGrantAndStart()
		if got := h.state(); got != (transitionState{}) {
			t.Errorf("state %s, want idle", got)
		}
		grantEffectsEqual(t, "losers", h.effects(), []string{
			"adapter: RequestStart 0x31",
			"session 2: FAILED 0x31",
			"session 2: RECEIVED 0xAA",
			"result gateway 0x7F: source 0x7F, error write failed",
			"result session 1 0x31: source 0x31, error write failed",
			"result session 2 0x71: source 0x31, failed queued",
		})
	})
}

// TestGrantBlockingStart covers the START step's blocking call through the
// harness: a won call grants the bus and clears the barrier after the grant,
// a failed call withdraws the selection, and shutdown refuses the call's
// goroutine and withdraws the selection without a call.
func TestGrantBlockingStart(t *testing.T) {
	t.Run("won", func(t *testing.T) {
		h := newTransitionHarness(t, transitionUpstreamBlocking)
		h.bid(1, transitionSessionSource)
		h.discardEffects()
		h.m.tryGrantAndStart()
		h.awaitStartCall()
		if got, want := h.state(), (transitionState{start: "session 1 0x31", startBlocking: true, blockingCall: true}); got != want {
			t.Errorf("state during the call %s, want %s", got, want)
		}
		h.releaseStartCall(nil, "session 1 0x31")
		if got := h.state(); got.owner != "session 1" || got.start != "" || got.blockingCall {
			t.Errorf("state %s, want session 1 owning, no START and no blocking call", got)
		}
		effects := h.effects()
		if len(effects) == 0 || effects[0] != "adapter: StartArbitration 0x31" || !strings.HasPrefix(effects[len(effects)-1], "result session 1 0x31: granted") {
			t.Errorf("effects %q", effects)
		}
	})
	t.Run("failed", func(t *testing.T) {
		h := newTransitionHarness(t, transitionUpstreamBlocking)
		h.bid(1, transitionSessionSource)
		h.discardEffects()
		h.m.tryGrantAndStart()
		h.awaitStartCall()
		h.releaseStartCall(errors.New("call failed"), "session 1 0x31")
		if got := h.state(); got != (transitionState{}) {
			t.Errorf("state %s, want idle", got)
		}
		grantEffectsEqual(t, "failed", h.effects(), []string{
			"adapter: StartArbitration 0x31",
			"result session 1 0x31: source 0x31, error call failed",
		})
	})
	t.Run("refused at shutdown", func(t *testing.T) {
		h := newTransitionHarness(t, transitionUpstreamBlocking)
		h.bid(1, transitionSessionSource)
		h.discardEffects()
		h.m.closing.Store(true)
		h.m.tryGrantAndStart()
		h.m.closing.Store(false)
		if got := h.state(); got != (transitionState{}) {
			t.Errorf("state %s, want idle", got)
		}
		grantEffectsEqual(t, "refused", h.effects(), []string{"result session 1 0x31: source 0x31, error adaptermux: closed"})
	})
}

// TestGrantBlockingStartOutcomeSteps covers the outcome steps of the blocking
// call directly: a call whose selection another path resolved clears its own
// barrier, consumes the result it left and, when it won, fences and counts an
// undeliverable STARTED; a stale generation changes nothing; an owned
// selection is withdrawn even by a stale call.
func TestGrantBlockingStartOutcomeSteps(t *testing.T) {
	const gen = 7
	errCall := errors.New("call failed")
	type row struct {
		name      string
		owned     bool
		current   bool
		err       error
		want      transitionState
		result    string
		undeliver uint64
	}
	rows := []row{
		{name: "won, resolved", current: true,
			want: transitionState{regrantFence: true, retiredSelection: true, recovery: "at_syn"}, undeliver: 1},
		{name: "won, resolved, stale", want: transitionState{blockingCall: true, absorb: 1}},
		{name: "failed, resolved", current: true, err: errCall, want: transitionState{}},
		{name: "failed, resolved, stale", err: errCall, want: transitionState{blockingCall: true, absorb: 1}},
		{name: "failed, owned, stale", owned: true, err: errCall,
			want: transitionState{blockingCall: true, absorb: 1}, result: "source 0x31, error call failed"},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			h := newTransitionHarness(t, transitionUpstreamBlocking)
			g := &grantAttempt{sessionID: 1, initiator: transitionSessionSource, notify: make(chan startResult, 1)}
			grantLocked(h, func(m *Mux) {
				m.blockingArbActive = true
				m.blockingArbGen = gen
				if !tc.current {
					m.blockingArbGen = gen + 1
				}
				m.pendingStartAbsorb = 1
				if tc.owned {
					m.pendingStart = &pendingStartState{sessionID: 1, initiator: transitionSessionSource, notify: g.notify, blockingArb: true}
				}
			})
			before := h.m.undeliverableStarted.Load()
			if tc.err != nil {
				h.m.failBlockingStart(g, gen, tc.err)
			} else {
				h.m.completeBlockingStart(g, gen)
			}
			if got := h.state(); got != tc.want {
				t.Errorf("state %s, want %s", got, tc.want)
			}
			if d := h.m.undeliverableStarted.Load() - before; d != tc.undeliver {
				t.Errorf("undeliverable STARTED +%d, want +%d", d, tc.undeliver)
			}
			var got string
			select {
			case res := <-g.notify:
				got = transitionResultText(res)
			default:
			}
			if got != tc.result {
				t.Errorf("result %q, want %q", got, tc.result)
			}
		})
	}
}

// TestGrantUnsupportedTransport covers the START step on a transport without
// START: the selection is withdrawn and fenced and the waiter failed.
func TestGrantUnsupportedTransport(t *testing.T) {
	h := newTransitionHarness(t, transitionUpstreamAsync)
	grantSetUpstream(h, grantPlainUpstream{h.up})
	h.bid(1, transitionSessionSource)
	h.discardEffects()
	h.m.tryGrantAndStart()
	if got, want := h.state(), (transitionState{regrantFence: true, retiredSelection: true, recovery: "at_syn"}); got != want {
		t.Errorf("state %s, want %s", got, want)
	}
	grantEffectsEqual(t, "unsupported", h.effects(), []string{
		"result session 1 0x31: source 0x31, error adaptermux: transport does not support arbitration",
	})
}
