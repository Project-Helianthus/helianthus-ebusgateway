package adaptermux

import (
	"errors"
	"testing"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// TestMuxStateDerivesEveryField varies every mux field the typed state is
// derived from, one at a time over an idle mux, and requires the derived
// state to follow; the precedence cases require the phase order owner, START
// in flight, fence, queued bids, idle.
func TestMuxStateDerivesEveryField(t *testing.T) {
	cases := []struct {
		name string
		set  func(m *Mux)
		want muxState
	}{
		{name: "idle", set: func(*Mux) {}, want: muxState{phase: muxIdle}},
		{name: "recovery", set: func(m *Mux) { m.arbitrationRecovery = arbitrationRecoveryReset },
			want: muxState{phase: muxIdle, recovery: arbitrationRecoveryReset}},
		{name: "absorb", set: func(m *Mux) { m.pendingStartAbsorb = 2 },
			want: muxState{phase: muxIdle, absorb: 2}},
		{name: "blocking call", set: func(m *Mux) { m.blockingArbActive = true },
			want: muxState{phase: muxIdle, blocking: true}},
		{name: "held", set: func(m *Mux) { m.step.held = true },
			want: muxState{phase: muxIdle, held: true}},
		{name: "resync owed", set: func(m *Mux) { m.collisionResyncOwed = 2 },
			want: muxState{phase: muxIdle, resyncOwed: 2}},
		{name: "retired selection", set: func(m *Mux) { m.retiredSelection, m.localLoserSource = true, 0x31 },
			want: muxState{phase: muxIdle, retired: true, retiredSource: 0x31}},
		{name: "loser source without a retired selection", set: func(m *Mux) { m.localLoserSource = 0x31 },
			want: muxState{phase: muxIdle}},
		{name: "start", set: func(m *Mux) { m.pendingStart = &pendingStartState{sessionID: 3, initiator: 0x33} },
			want: muxState{phase: muxStartAsync, start: muxSelection{pending: true, party: 3, initiator: 0x33}}},
		{name: "start blocking", set: func(m *Mux) {
			m.pendingStart = &pendingStartState{sessionID: 3, initiator: 0x33, blockingArb: true}
		}, want: muxState{phase: muxStartBlocking, start: muxSelection{pending: true, party: 3, initiator: 0x33}}},
		{name: "start gateway", set: func(m *Mux) {
			m.pendingStart = &pendingStartState{sessionID: gatewaySessionID, initiator: 0x7F}
		}, want: muxState{phase: muxStartAsync, start: muxSelection{pending: true, party: gatewaySessionID, initiator: 0x7F}}},
		{name: "owner session", set: func(m *Mux) { m.arb.hasOwner, m.arb.currentOwner = true, 4 },
			want: muxState{phase: muxSessionOwned, owner: 4}},
		{name: "owner gateway", set: func(m *Mux) { m.arb.hasOwner, m.arb.currentOwner = true, gatewaySessionID },
			want: muxState{phase: muxGatewayGrace}},
		{name: "owner gateway with transaction", set: func(m *Mux) {
			m.arb.hasOwner, m.arb.currentOwner, m.gatewayTxnActive = true, gatewaySessionID, true
		}, want: muxState{phase: muxGatewayTransaction}},
		{name: "transaction flag without the gateway owner", set: func(m *Mux) { m.gatewayTxnActive = true },
			want: muxState{phase: muxIdle}},
		{name: "data echo due", set: func(m *Mux) {
			m.arb.hasOwner, m.arb.currentOwner, m.gatewayTxnActive = true, gatewaySessionID, true
			m.gatewayEcho.recordSentStructural(0x15, false)
		}, want: muxState{phase: muxGatewayTransaction, echoPending: true}},
		{name: "payload 0xAA echo due", set: func(m *Mux) {
			m.arb.hasOwner, m.arb.currentOwner, m.gatewayTxnActive = true, gatewaySessionID, true
			m.gatewayEcho.recordSentStructural(0xAA, false)
		}, want: muxState{phase: muxGatewayTransaction, echoPending: true}},
		{name: "structural non-SYN echo due", set: func(m *Mux) {
			m.arb.hasOwner, m.arb.currentOwner, m.gatewayTxnActive = true, gatewaySessionID, true
			m.gatewayEcho.recordSentStructural(0x15, true)
		}, want: muxState{phase: muxGatewayTransaction, echoPending: true}},
		{name: "terminator echo due", set: func(m *Mux) {
			m.arb.hasOwner, m.arb.currentOwner, m.gatewayTxnActive = true, gatewaySessionID, true
			m.gatewayEcho.recordSentStructural(0xAA, true)
		}, want: muxState{phase: muxGatewayTransaction, echoPending: true, echoTerminator: true}},
		{name: "echo outside a gateway transaction", set: func(m *Mux) {
			m.arb.hasOwner, m.arb.currentOwner = true, gatewaySessionID
			m.gatewayEcho.recordSentStructural(0xAA, true)
		}, want: muxState{phase: muxGatewayGrace}},
		{name: "fence", set: func(m *Mux) { m.deferRegrantUntilNextSyn = true },
			want: muxState{phase: muxFenced}},
		{name: "gateway bid", set: func(m *Mux) { m.arb.pendingGateway = &startRequest{sessionID: gatewaySessionID} },
			want: muxState{phase: muxBidsQueued, bids: muxBidSet{gateway: true}}},
		{name: "external bids", set: func(m *Mux) {
			m.arb.pendingExternal = []*startRequest{{sessionID: 5}, {sessionID: 2}}
		}, want: muxState{phase: muxBidsQueued, bids: muxBidSet{sessions: [muxBidSetSize]uint64{2, 5}}}},
		{name: "deferred gateway loser is no bid", set: func(m *Mux) {
			m.arb.deferredGatewayLoser = &startRequest{sessionID: gatewaySessionID}
		}, want: muxState{phase: muxIdle, deferred: true}},
		{name: "telegram live", set: func(m *Mux) {
			m.phase.markOwnerWrite()
			m.phase.phase = wirePhaseWaitCmdAck
		}, want: muxState{phase: muxIdle, telegramLive: true}},
		{name: "owner write without a live phase", set: func(m *Mux) { m.phase.markOwnerWrite() },
			want: muxState{phase: muxIdle}},
		{name: "owner before start", set: func(m *Mux) {
			m.arb.hasOwner, m.arb.currentOwner = true, 4
			m.pendingStart = &pendingStartState{sessionID: 3, initiator: 0x33}
		}, want: muxState{phase: muxSessionOwned, owner: 4, start: muxSelection{pending: true, party: 3, initiator: 0x33}}},
		{name: "start before fence", set: func(m *Mux) {
			m.deferRegrantUntilNextSyn = true
			m.pendingStart = &pendingStartState{sessionID: 3, initiator: 0x33}
		}, want: muxState{phase: muxStartAsync, start: muxSelection{pending: true, party: 3, initiator: 0x33}}},
		{name: "fence before bids", set: func(m *Mux) {
			m.deferRegrantUntilNextSyn = true
			m.arb.pendingExternal = []*startRequest{{sessionID: 2}}
		}, want: muxState{phase: muxFenced, bids: muxBidSet{sessions: [muxBidSetSize]uint64{2}}}},
	}
	for _, tc := range cases {
		m := New(Config{})
		m.stateMu.Lock()
		tc.set(m)
		got := m.muxStateLocked()
		m.stateMu.Unlock()
		if got != tc.want {
			t.Errorf("%s: derived %s, want %s", tc.name, muxModelStateLiteral(got), muxModelStateLiteral(tc.want))
		}
	}
}

// TestMuxBidSetKeepsPartiesInOrder adds bidders in any order and requires
// the gateway flag, the ascending session list and the count beyond it.
func TestMuxBidSetKeepsPartiesInOrder(t *testing.T) {
	cases := []struct {
		name    string
		parties []uint64
		want    muxBidSet
	}{
		{name: "none", want: muxBidSet{}},
		{name: "gateway", parties: []uint64{gatewaySessionID}, want: muxBidSet{gateway: true}},
		{name: "ascending", parties: []uint64{7, 3, 5}, want: muxBidSet{sessions: [muxBidSetSize]uint64{3, 5, 7}}},
		{name: "repeat", parties: []uint64{3, 3}, want: muxBidSet{sessions: [muxBidSetSize]uint64{3}}},
		{name: "more appended", parties: []uint64{1, 2, 3, 4, 9}, want: muxBidSet{sessions: [muxBidSetSize]uint64{1, 2, 3, 4}, more: 1}},
		{name: "more displaced", parties: []uint64{2, 3, 4, 5, 1}, want: muxBidSet{sessions: [muxBidSetSize]uint64{1, 2, 3, 4}, more: 1}},
		{name: "gateway and sessions", parties: []uint64{2, gatewaySessionID}, want: muxBidSet{gateway: true, sessions: [muxBidSetSize]uint64{2}}},
	}
	for _, tc := range cases {
		got := muxModelBids(tc.parties...)
		if got != tc.want {
			t.Errorf("%s: %#v, want %#v", tc.name, got, tc.want)
		}
		if got.empty() != (len(tc.parties) == 0) {
			t.Errorf("%s: empty %v", tc.name, got.empty())
		}
	}
}

// TestDecideMuxTransitionValidity requires the reason of every event that
// needs a running timer, barrier or owner, and none where it runs. Each case
// varies the one state field the rule reads.
func TestDecideMuxTransitionValidity(t *testing.T) {
	selected := muxSelection{pending: true, party: 1, initiator: 0x31}
	cases := []struct {
		kind   muxEventKind
		absent muxState
		reason string
		ok     muxState
	}{
		{muxEventStartDeadline, muxState{phase: muxSessionOwned, owner: 1}, muxNoStartDeadline, muxState{phase: muxSessionOwned, owner: 1, start: selected}},
		{muxEventAbsorbDebounce, muxState{phase: muxIdle}, muxNoAbsorb, muxState{phase: muxIdle, absorb: 1}},
		{muxEventIdleGrace, muxState{phase: muxFenced}, muxNoOwnerTimer, muxState{phase: muxGatewayGrace}},
		{muxEventOwnershipLimit, muxState{phase: muxStartAsync, start: selected}, muxNoOwnerTimer, muxState{phase: muxGatewayTransaction}},
		{muxEventAbandonWatchdog, muxState{phase: muxGatewayGrace}, muxNoSessionGrant, muxState{phase: muxSessionOwned, owner: 1}},
		{muxEventStartCallReturns, muxState{phase: muxStartBlocking, start: selected}, muxNoBlockingCall, muxState{phase: muxIdle, blocking: true}},
		{muxEventStartCallFails, muxState{phase: muxIdle}, muxNoBlockingCall, muxState{phase: muxIdle, blocking: true}},
	}
	for _, tc := range cases {
		if got := decideMuxTransition(tc.absent, muxEvent{kind: tc.kind}); got.invalid != tc.reason || got != (muxTransition{invalid: tc.reason}) {
			t.Errorf("kind %d in %s: %+v, want reason %q", tc.kind, muxModelStateLiteral(tc.absent), got, tc.reason)
		}
		if got := decideMuxTransition(tc.ok, muxEvent{kind: tc.kind}); got.invalid != "" {
			t.Errorf("kind %d in %s: invalid %q", tc.kind, muxModelStateLiteral(tc.ok), got.invalid)
		}
	}
	for _, kind := range []muxEventKind{muxEventSYN, muxEventSymbol, muxEventBid, muxEventStarted, muxEventFailed,
		muxEventStartedApplied, muxEventFailedApplied, muxEventSessionClose, muxEventReset, muxEventReadFailure} {
		if got := decideMuxTransition(muxState{}, muxEvent{kind: kind}); got.invalid != "" {
			t.Errorf("kind %d is invalid in the idle state: %q", kind, got.invalid)
		}
	}
}

// TestDecideMuxTransitionResults covers the read loop's STARTED and FAILED
// decisions. The cases vary every input they read: the START in flight
// (pending, party, initiator), the absorb count, the retired selection and
// its source, and the event's kind, data, phantom flag, bidder outcome and
// lifecycle change.
func TestDecideMuxTransitionResults(t *testing.T) {
	session := muxSelection{pending: true, party: 1, initiator: 0x31}
	gateway := muxSelection{pending: true, party: gatewaySessionID, initiator: 0x7F}
	cases := []struct {
		name  string
		state muxState
		ev    muxEvent
		want  muxTransition
	}{
		{name: "STARTED snapshots the bidder", state: muxState{start: session},
			ev:   muxEvent{kind: muxEventStarted, data: 0x31},
			want: muxTransition{bidder: session, notify: 0x31}},
		{name: "STARTED during absorb has no bidder", state: muxState{start: session, absorb: 1},
			ev:   muxEvent{kind: muxEventStarted, data: 0x31},
			want: muxTransition{notify: 0x31}},
		{name: "FAILED notifies the winner", state: muxState{start: session},
			ev:   muxEvent{kind: muxEventFailed, data: 0x10},
			want: muxTransition{bidder: session, notify: 0x10}},
		{name: "phantom FAILED notifies the bidder's own initiator", state: muxState{start: session},
			ev:   muxEvent{kind: muxEventFailed, data: 0x10, phantom: true},
			want: muxTransition{bidder: session, notify: 0x31}},
		{name: "phantom FAILED without a bidder notifies the byte", state: muxState{},
			ev:   muxEvent{kind: muxEventFailed, data: 0x10, phantom: true},
			want: muxTransition{notify: 0x10}},
		{name: "phantom FAILED during absorb notifies the byte", state: muxState{start: session, absorb: 1},
			ev:   muxEvent{kind: muxEventFailed, data: 0x10, phantom: true},
			want: muxTransition{notify: 0x10}},

		{name: "applied STARTED after a lifecycle change routes nothing", state: muxState{start: session},
			ev:   muxEvent{kind: muxEventStartedApplied, data: 0x10, bidderFailed: true, epochChanged: true},
			want: muxTransition{bidder: session}},
		{name: "applied STARTED without bidder or retired selection", state: muxState{},
			ev:   muxEvent{kind: muxEventStartedApplied, data: 0x10},
			want: muxTransition{}},
		{name: "applied STARTED naming the retired source", state: muxState{retired: true, retiredSource: 0x31},
			ev:   muxEvent{kind: muxEventStartedApplied, data: 0x31},
			want: muxTransition{}},
		{name: "applied STARTED naming a foreign source after retirement", state: muxState{retired: true, retiredSource: 0x31},
			ev:   muxEvent{kind: muxEventStartedApplied, data: 0x10},
			want: muxTransition{resetLosers: true, deliver: muxDeliverAll, passive: true}},
		{name: "applied STARTED during absorb follows the retired selection", state: muxState{start: session, absorb: 1, retired: true, retiredSource: 0x31},
			ev:   muxEvent{kind: muxEventStartedApplied, data: 0x10},
			want: muxTransition{resetLosers: true, deliver: muxDeliverAll, passive: true}},
		{name: "gateway wins", state: muxState{start: gateway},
			ev:   muxEvent{kind: muxEventStartedApplied, data: 0x7F},
			want: muxTransition{bidder: gateway, deliver: muxDeliverOthersNotLosers, except: gatewaySessionID}},
		{name: "session wins", state: muxState{start: session},
			ev:   muxEvent{kind: muxEventStartedApplied, data: 0x31},
			want: muxTransition{bidder: session, deliver: muxDeliverOthersNotLosers, except: 1, passive: true}},
		{name: "session 2 wins", state: muxState{start: muxSelection{pending: true, party: 2, initiator: 0x71}},
			ev:   muxEvent{kind: muxEventStartedApplied, data: 0x71},
			want: muxTransition{bidder: muxSelection{pending: true, party: 2, initiator: 0x71}, deliver: muxDeliverOthersNotLosers, except: 2, passive: true}},
		{name: "gateway loses to a third party", state: muxState{start: gateway},
			ev:   muxEvent{kind: muxEventStartedApplied, data: 0x10, bidderFailed: true},
			want: muxTransition{bidder: gateway, resetLosers: true, deliver: muxDeliverAll, passive: true}},
		{name: "session loses to a third party", state: muxState{start: session},
			ev:   muxEvent{kind: muxEventStartedApplied, data: 0x10, bidderFailed: true},
			want: muxTransition{bidder: session, resetLosers: true, deliver: muxDeliverOthers, except: 1, passive: true}},
		{name: "session's FAILED suppressed on a mismatch", state: muxState{start: session},
			ev:   muxEvent{kind: muxEventStartedApplied, data: 0x10},
			want: muxTransition{bidder: session, resetLosers: true, deliver: muxDeliverAll, passive: true}},

		{name: "applied FAILED after a lifecycle change routes nothing", state: muxState{start: session},
			ev:   muxEvent{kind: muxEventFailedApplied, data: 0x10, bidderFailed: true, epochChanged: true},
			want: muxTransition{bidder: session}},
		{name: "applied FAILED without a failed bidder", state: muxState{start: session},
			ev:   muxEvent{kind: muxEventFailedApplied, data: 0x10},
			want: muxTransition{bidder: session, resetLosers: true, deliver: muxDeliverAll, passive: true}},
		{name: "applied FAILED of the gateway", state: muxState{start: gateway},
			ev:   muxEvent{kind: muxEventFailedApplied, data: 0x10, bidderFailed: true},
			want: muxTransition{bidder: gateway, resetLosers: true, deliver: muxDeliverAll, passive: true}},
		{name: "applied FAILED of a session", state: muxState{start: session},
			ev:   muxEvent{kind: muxEventFailedApplied, data: 0x10, bidderFailed: true},
			want: muxTransition{bidder: session, resetLosers: true, deliver: muxDeliverOthers, except: 1, passive: true}},
		{name: "applied FAILED without a bidder", state: muxState{},
			ev:   muxEvent{kind: muxEventFailedApplied, data: 0x10, bidderFailed: true},
			want: muxTransition{resetLosers: true, deliver: muxDeliverAll, passive: true}},
		{name: "applied phantom FAILED delivers no session", state: muxState{start: session},
			ev:   muxEvent{kind: muxEventFailedApplied, data: 0x10, phantom: true, bidderFailed: true},
			want: muxTransition{bidder: session, resetLosers: true, passive: true}},
	}
	for _, tc := range cases {
		if got := decideMuxTransition(tc.state, tc.ev); got != tc.want {
			t.Errorf("%s: %+v, want %+v", tc.name, got, tc.want)
		}
	}
}

// TestStreamItemEventTranslatesEveryKind requires the read loop's event for
// every stream item kind, the escape provenance of symbols and the read
// failure.
func TestStreamItemEventTranslatesEveryKind(t *testing.T) {
	cases := []struct {
		name    string
		item    upstreamItem
		err     error
		want    muxEvent
		logical bool
	}{
		{name: "read failure", err: ebuserrors.ErrTimeout, want: muxEvent{kind: muxEventReadFailure}, logical: true},
		{name: "other read failure", item: upstreamItem{event: transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x52}},
			err: errors.New("x"), want: muxEvent{kind: muxEventReadFailure}, logical: true},
		{name: "raw only", item: upstreamItem{rawSymbol: 0xA9, hasRaw: true}, want: muxEvent{}},
		{name: "started", item: upstreamItem{event: transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x31}},
			want: muxEvent{kind: muxEventStarted, data: 0x31}, logical: true},
		{name: "failed", item: upstreamItem{event: transport.StreamEvent{Kind: transport.StreamEventFailed, Data: 0x10}},
			want: muxEvent{kind: muxEventFailed, data: 0x10}, logical: true},
		{name: "reset", item: upstreamItem{event: transport.StreamEvent{Kind: transport.StreamEventReset}},
			want: muxEvent{kind: muxEventReset}, logical: true},
		{name: "wire SYN", item: upstreamItem{event: transport.StreamEvent{Kind: transport.StreamEventWireSyn, Byte: 0xAA, WasEscaped: true}},
			want: muxEvent{kind: muxEventSYN, data: 0xAA}, logical: true},
		{name: "SYN byte", item: upstreamItem{event: transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xAA}},
			want: muxEvent{kind: muxEventSYN, data: 0xAA}, logical: true},
		{name: "escaped 0xAA", item: upstreamItem{event: transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xAA, WasEscaped: true}},
			want: muxEvent{kind: muxEventSymbol, data: 0xAA, escaped: true}, logical: true},
		{name: "data byte", item: upstreamItem{event: transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x52}},
			want: muxEvent{kind: muxEventSymbol, data: 0x52}, logical: true},
		{name: "escaped data byte", item: upstreamItem{event: transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xA9, WasEscaped: true}},
			want: muxEvent{kind: muxEventSymbol, data: 0xA9, escaped: true}, logical: true},
		{name: "other kind", item: upstreamItem{event: transport.StreamEvent{Kind: transport.StreamEventKind(99), Byte: 0xAA}},
			want: muxEvent{kind: muxEventSymbol, data: 0xAA}, logical: true},
		{name: "escaped other kind", item: upstreamItem{event: transport.StreamEvent{Kind: transport.StreamEventKind(99), Byte: 0x52, WasEscaped: true}},
			want: muxEvent{kind: muxEventSymbol, data: 0x52, escaped: true}, logical: true},
	}
	for _, tc := range cases {
		got, logical := streamItemEvent(tc.item, tc.err)
		if got != tc.want || logical != tc.logical {
			t.Errorf("%s: %+v logical=%v, want %+v logical=%v", tc.name, got, logical, tc.want, tc.logical)
		}
	}
}
