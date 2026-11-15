package adaptermux

import (
	"reflect"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// The received-symbol tests cover each named step of the symbol handler: the
// event translation, the owner taken from the typed state, the entry step
// with its ownership limit, the wire-SYN path, the data-symbol decision and
// its effects, and the terminal release. The harness rows drive the real
// handler through onLogicalReceived and pin the state and effects afterwards.

// receivedSymbolExpireOwnership moves the ownership start past every
// ownership limit.
func receivedSymbolExpireOwnership(h *transitionHarness) {
	h.m.stateMu.Lock()
	h.m.busOwned = time.Now().Add(-2 * transitionTimerHorizon)
	h.m.stateMu.Unlock()
}

// receivedSymbolCounters are the gateway transaction diagnostics the steps
// write.
type receivedSymbolCounters struct {
	reason        ActiveTxnInactiveReason
	afterInactive uint64
	delivered     uint64
	releases      uint64
}

func receivedSymbolReadCounters(h *transitionHarness) receivedSymbolCounters {
	h.m.stateMu.Lock()
	defer h.m.stateMu.Unlock()
	return receivedSymbolCounters{
		reason:        h.m.activeTxn.inactiveReas,
		afterInactive: h.m.activeTxn.afterInactive.Load(),
		delivered:     h.m.activeTxn.bytesDeliveredToActive.Load(),
		releases:      h.m.activeTxn.foreignInitiatorReleases.Load(),
	}
}

// receivedSymbolRow is one harness row: a fixture, one input and the state,
// effects and counter changes afterwards.
type receivedSymbolRow struct {
	name    string
	fixture func(h *transitionHarness)
	input   func(h *transitionHarness)
	want    transitionState
	effects []string
	// reason is the inactive reason afterwards; the other fields are the
	// counter increments the input caused.
	reason        ActiveTxnInactiveReason
	afterInactive uint64
	delivered     uint64
	releases      uint64
}

func runReceivedSymbolRows(t *testing.T, rows []receivedSymbolRow) {
	t.Helper()
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			h := newTransitionHarness(t, transitionUpstreamAsync)
			row.fixture(h)
			h.discardEffects()
			before := receivedSymbolReadCounters(h)
			row.input(h)
			got, effects := h.state(), h.effects()
			after := receivedSymbolReadCounters(h)
			if got != row.want {
				t.Errorf("state\n got %s\nwant %s", got, row.want)
			}
			if !reflect.DeepEqual(effects, row.effects) {
				t.Errorf("effects\n got %q\nwant %q", effects, row.effects)
			}
			if after.reason != row.reason {
				t.Errorf("inactive reason %q, want %q", after.reason, row.reason)
			}
			if d := after.afterInactive - before.afterInactive; d != row.afterInactive {
				t.Errorf("after-inactive count +%d, want +%d", d, row.afterInactive)
			}
			if d := after.delivered - before.delivered; d != row.delivered {
				t.Errorf("active deliveries +%d, want +%d", d, row.delivered)
			}
			if d := after.releases - before.releases; d != row.releases {
				t.Errorf("foreign initiator releases +%d, want +%d", d, row.releases)
			}
		})
	}
}

func receivedSymbolSessionOwned(h *transitionHarness) {
	transitionSelectSession(h)
	h.feedStarted(transitionSessionSource)
}

func receivedSymbolGatewayGrace(h *transitionHarness) {
	transitionGatewayTransaction(h)
	h.gatewayWrite(protocol.SymbolSyn, true)
	h.syn()
}

var receivedSymbolBothSessions = func(b string) []string {
	return []string{"session 1: RECEIVED " + b, "session 2: RECEIVED " + b}
}

// TestReceivedSymbolEventClassifiesTheWireSYN covers the event translation:
// only an unescaped 0xAA is a wire SYN; the symbol and its escape provenance
// are kept.
func TestReceivedSymbolEventClassifiesTheWireSYN(t *testing.T) {
	cases := []struct {
		symbol  byte
		escaped bool
		want    muxEvent
	}{
		{protocol.SymbolSyn, false, muxEvent{kind: muxEventSYN, data: protocol.SymbolSyn}},
		{protocol.SymbolSyn, true, muxEvent{kind: muxEventSymbol, data: protocol.SymbolSyn, escaped: true}},
		{transitionDataSymbol, false, muxEvent{kind: muxEventSymbol, data: transitionDataSymbol}},
		{protocol.SymbolEscape, true, muxEvent{kind: muxEventSymbol, data: protocol.SymbolEscape, escaped: true}},
	}
	for _, tc := range cases {
		if got := receivedSymbolEvent(tc.symbol, tc.escaped); got != tc.want {
			t.Errorf("0x%02X escaped=%v: %+v, want %+v", tc.symbol, tc.escaped, got, tc.want)
		}
	}
}

// TestReceivedSymbolOwnerPartyFollowsThePhase covers the owner the entry step
// takes from the typed state: the gateway in both gateway phases, the owning
// session only in muxSessionOwned, and none in every other phase even when
// the owner field is set.
func TestReceivedSymbolOwnerPartyFollowsThePhase(t *testing.T) {
	cases := []struct {
		phase muxPhase
		id    uint64
		owned bool
	}{
		{muxIdle, 0, false},
		{muxBidsQueued, 0, false},
		{muxStartAsync, 0, false},
		{muxStartBlocking, 0, false},
		{muxGatewayTransaction, gatewaySessionID, true},
		{muxGatewayGrace, gatewaySessionID, true},
		{muxSessionOwned, 3, true},
		{muxFenced, 0, false},
	}
	for _, tc := range cases {
		id, owned := muxState{phase: tc.phase, owner: 3}.ownerParty()
		if id != tc.id || owned != tc.owned {
			t.Errorf("phase %d: (%d, %v), want (%d, %v)", tc.phase, id, owned, tc.id, tc.owned)
		}
		if owned != tc.phase.owned() {
			t.Errorf("phase %d: owned %v disagrees with the phase", tc.phase, owned)
		}
	}
}

// TestReceivedSymbolEntryReleasesPastTheOwnershipLimit covers the entry
// step: an owner past its ownership limit is released by the symbol that
// finds it, the gateway transaction ends with the ownership reason, and an
// owner within its limit or an unowned bus keeps its state.
func TestReceivedSymbolEntryReleasesPastTheOwnershipLimit(t *testing.T) {
	runReceivedSymbolRows(t, []receivedSymbolRow{
		{
			name:    "session past the limit is released by a data symbol",
			fixture: receivedSymbolSessionOwned,
			input: func(h *transitionHarness) {
				receivedSymbolExpireOwnership(h)
				h.symbol(transitionDataSymbol)
			},
			want:    transitionState{phase: "collect_request"},
			effects: append([]string{"passive: symbol 0x52"}, receivedSymbolBothSessions("0x52")...),
		},
		{
			name:    "session within the limit keeps the bus",
			fixture: receivedSymbolSessionOwned,
			input:   func(h *transitionHarness) { h.symbol(transitionDataSymbol) },
			want:    transitionState{owner: "session 1", phase: "collect_request"},
			effects: append([]string{"passive: symbol 0x52"}, receivedSymbolBothSessions("0x52")...),
		},
		{
			name:    "gateway past the limit ends its transaction at a data symbol",
			fixture: transitionGatewayTransaction,
			input: func(h *transitionHarness) {
				receivedSymbolExpireOwnership(h)
				h.symbol(transitionDataSymbol)
			},
			want:    transitionState{phase: "collect_request"},
			effects: append([]string{"passive: symbol 0x52"}, receivedSymbolBothSessions("0x52")...),
			reason:  ReasonMaxOwnership,
		},
		{
			name:    "gateway past the limit ends its transaction at a wire SYN",
			fixture: transitionGatewayTransaction,
			input: func(h *transitionHarness) {
				receivedSymbolExpireOwnership(h)
				h.syn()
			},
			want:    transitionState{},
			effects: append([]string{"passive: symbol 0xAA"}, receivedSymbolBothSessions("0xAA")...),
			reason:  ReasonMaxOwnership,
		},
		{
			name:    "an unowned bus has no ownership limit",
			fixture: func(*transitionHarness) {},
			input: func(h *transitionHarness) {
				receivedSymbolExpireOwnership(h)
				h.symbol(transitionDataSymbol)
			},
			want:    transitionState{},
			effects: append([]string{"passive: symbol 0x52"}, receivedSymbolBothSessions("0x52")...),
		},
	})
}

// TestReceivedSymbolWireSYNPath covers the wire-SYN path: the pre-echo SYN is
// withheld from the active path, the terminator SYN reaches it and ends the
// transaction into the idle grace, a SYN selects the queued bid of a
// logical-only input, and a frozen eligibility boundary is retained.
func TestReceivedSymbolWireSYNPath(t *testing.T) {
	runReceivedSymbolRows(t, []receivedSymbolRow{
		{
			name:    "pre-echo SYN is withheld from the active path",
			fixture: transitionGatewayAwaitingEcho,
			input:   func(h *transitionHarness) { h.syn() },
			want:    transitionState{owner: "gateway", gatewayTxn: true},
			effects: append([]string{"passive: symbol 0xAA"}, receivedSymbolBothSessions("0xAA")...),
		},
		{
			name: "terminator SYN reaches the active path and ends the transaction",
			fixture: func(h *transitionHarness) {
				transitionGatewayTransaction(h)
				h.gatewayWrite(protocol.SymbolSyn, true)
			},
			input:     func(h *transitionHarness) { h.syn() },
			want:      transitionState{owner: "gateway"},
			effects:   append([]string{"passive: symbol 0xAA", "active: 0xAA"}, receivedSymbolBothSessions("0xAA")...),
			reason:    ReasonSYNTerminator,
			delivered: 1,
		},
		{
			name:    "logical-only SYN snapshots the queued bids and selects one",
			fixture: func(h *transitionHarness) { h.bid(1, transitionSessionSource) },
			input:   func(h *transitionHarness) { h.syn() },
			want:    transitionState{start: "session 1 0x31"},
			effects: append([]string{"passive: symbol 0xAA", "adapter: RequestStart 0x31"}, receivedSymbolBothSessions("0xAA")...),
		},
		{
			name:    "a frozen empty eligibility boundary selects nothing",
			fixture: func(h *transitionHarness) { h.bid(1, transitionSessionSource) },
			input: func(h *transitionHarness) {
				h.m.onLogicalReceivedForBids(protocol.SymbolSyn, false, true, map[*startRequest]chan startResult{})
			},
			want:    transitionState{bids: "session 1 0x31"},
			effects: append([]string{"passive: symbol 0xAA"}, receivedSymbolBothSessions("0xAA")...),
		},
		{
			name:    "a raw-view SYN is not delivered to the sessions again",
			fixture: func(*transitionHarness) {},
			input: func(h *transitionHarness) {
				h.m.onLogicalReceived(protocol.SymbolSyn, false, false)
			},
			want:    transitionState{},
			effects: []string{"passive: symbol 0xAA"},
		},
	})
}

// TestReceivedSymbolDataSymbolDecision covers the pure data-symbol decision.
// Each row varies one field of the view or the symbol against a base case:
//
//   - gatewayOwned: dropping the owner check matches echoes and releases the
//     gateway for an external owner's bytes;
//   - txnActive: inverting it swaps the first-echo and after-transaction
//     releases;
//   - echoHead: comparing against another byte matches stale bytes or
//     releases on the gateway's own echo;
//   - echoPending: ignoring it opens the response phase mid-write or
//     releases without a pending write;
//   - matchCount and writeCount: dropping either releases after the first
//     echo or after a later write;
//   - suspectArbLoss: dropping it releases twice in one transaction;
//   - txnEnded: dropping it aborts a fresh grant before its first write;
//   - the symbol: a non-initiator byte never releases.
func TestReceivedSymbolDataSymbolDecision(t *testing.T) {
	firstEcho := dataSymbolView{gatewayOwned: true, txnActive: true, echoHead: transitionGatewayData, echoPending: true, writeCount: 1}
	afterTxn := dataSymbolView{gatewayOwned: true, txnEnded: true}
	vary := func(v dataSymbolView, f func(*dataSymbolView)) dataSymbolView {
		f(&v)
		return v
	}
	foreign := muxEvent{kind: muxEventSymbol, data: transitionForeignSource}
	data := muxEvent{kind: muxEventSymbol, data: transitionDataSymbol}
	echo := muxEvent{kind: muxEventSymbol, data: transitionGatewayData}
	cases := []struct {
		name string
		view dataSymbolView
		ev   muxEvent
		want dataSymbolDecision
	}{
		{"foreign first echo", firstEcho, foreign, dataSymbolDecision{release: foreignInitiatorFirstEcho}},
		{"not gateway owned", vary(firstEcho, func(v *dataSymbolView) { v.gatewayOwned = false }), foreign, dataSymbolDecision{}},
		{"transaction not live", vary(firstEcho, func(v *dataSymbolView) { v.txnActive = false }), foreign, dataSymbolDecision{}},
		{"transaction not live after its end", vary(firstEcho, func(v *dataSymbolView) { v.txnActive, v.txnEnded = false, true }), foreign,
			dataSymbolDecision{release: foreignInitiatorAfterTxn}},
		{"foreign byte is the expected echo", vary(firstEcho, func(v *dataSymbolView) { v.echoHead = transitionForeignSource }), foreign,
			dataSymbolDecision{matchEcho: true}},
		{"gateway echo matches", firstEcho, echo, dataSymbolDecision{matchEcho: true}},
		{"no pending echo", vary(firstEcho, func(v *dataSymbolView) { v.echoPending = false }), foreign, dataSymbolDecision{matchEcho: true}},
		{"an echo already matched", vary(firstEcho, func(v *dataSymbolView) { v.matchCount = 1 }), foreign, dataSymbolDecision{}},
		{"a later write", vary(firstEcho, func(v *dataSymbolView) { v.writeCount = 2 }), foreign, dataSymbolDecision{}},
		{"loss already taken", vary(firstEcho, func(v *dataSymbolView) { v.suspectArbLoss = true }), foreign, dataSymbolDecision{}},
		{"mid-write non-initiator byte", firstEcho, data, dataSymbolDecision{}},
		{"foreign byte after the transaction", afterTxn, foreign, dataSymbolDecision{matchEcho: true, release: foreignInitiatorAfterTxn}},
		{"before the first write", vary(afterTxn, func(v *dataSymbolView) { v.txnEnded = false }), foreign, dataSymbolDecision{matchEcho: true}},
		{"non-initiator byte after the transaction", afterTxn, data, dataSymbolDecision{matchEcho: true}},
		{"after the transaction, not gateway owned", vary(afterTxn, func(v *dataSymbolView) { v.gatewayOwned = false }), foreign, dataSymbolDecision{}},
		{"stale byte after the transaction keeps the queue", vary(afterTxn, func(v *dataSymbolView) {
			v.echoHead, v.echoPending = transitionGatewayData, true
		}), data, dataSymbolDecision{}},
	}
	for _, tc := range cases {
		if got := decideDataSymbol(tc.view, tc.ev); got != tc.want {
			t.Errorf("%s: %+v, want %+v", tc.name, got, tc.want)
		}
	}
}

// TestReceivedSymbolDataSymbolPath covers the data-symbol decision and its
// effects through the handler: a mid-write stale byte is dropped from the
// active and passive paths, the expected echo and response bytes reach the
// active path, a foreign initiator byte releases the gateway (as its first
// echo with the collision fence and resync SYNs, after its transaction with
// the fence) and becomes passive traffic, and an escaped symbol keeps its
// provenance.
func TestReceivedSymbolDataSymbolPath(t *testing.T) {
	runReceivedSymbolRows(t, []receivedSymbolRow{
		{
			name:          "mid-write stale byte is dropped",
			fixture:       transitionGatewayAwaitingEcho,
			input:         func(h *transitionHarness) { h.symbol(transitionDataSymbol) },
			want:          transitionState{owner: "gateway", gatewayTxn: true, phase: "collect_request"},
			effects:       receivedSymbolBothSessions("0x52"),
			afterInactive: 1,
		},
		{
			name:      "expected echo reaches the active path",
			fixture:   transitionGatewayAwaitingEcho,
			input:     func(h *transitionHarness) { h.symbol(transitionGatewayData) },
			want:      transitionState{owner: "gateway", gatewayTxn: true, phase: "collect_request"},
			effects:   append([]string{"active: 0x15"}, receivedSymbolBothSessions("0x15")...),
			delivered: 1,
		},
		{
			name:      "response byte reaches the active path",
			fixture:   transitionGatewayTransaction,
			input:     func(h *transitionHarness) { h.symbol(transitionDataSymbol) },
			want:      transitionState{owner: "gateway", gatewayTxn: true, phase: "collect_request"},
			effects:   append([]string{"active: 0x52"}, receivedSymbolBothSessions("0x52")...),
			delivered: 1,
		},
		{
			name:      "foreign initiator as first echo releases the gateway",
			fixture:   transitionGatewayAwaitingEcho,
			input:     func(h *transitionHarness) { h.symbol(transitionForeignSource) },
			want:      transitionState{regrantFence: true, resyncOwed: protocol.CollisionRetryResyncSYNCount, phase: "collect_request"},
			effects:   append([]string{"passive: symbol 0x10", "active: 0x10"}, receivedSymbolBothSessions("0x10")...),
			reason:    ReasonArbitrationCollision,
			delivered: 1,
			releases:  1,
		},
		{
			name:     "foreign initiator after the transaction releases the gateway",
			fixture:  receivedSymbolGatewayGrace,
			input:    func(h *transitionHarness) { h.symbol(transitionForeignSource) },
			want:     transitionState{regrantFence: true},
			effects:  append([]string{"passive: symbol 0x10"}, receivedSymbolBothSessions("0x10")...),
			reason:   ReasonSYNTerminator,
			releases: 1,
		},
		{
			name:          "non-initiator byte after the transaction keeps the gateway",
			fixture:       receivedSymbolGatewayGrace,
			input:         func(h *transitionHarness) { h.symbol(transitionDataSymbol) },
			want:          transitionState{owner: "gateway"},
			effects:       receivedSymbolBothSessions("0x52"),
			reason:        ReasonSYNTerminator,
			afterInactive: 1,
		},
		{
			name:    "escaped symbol keeps its provenance and is no SYN",
			fixture: func(*transitionHarness) {},
			input:   func(h *transitionHarness) { h.m.onLogicalReceived(protocol.SymbolSyn, true, true) },
			want:    transitionState{},
			effects: append([]string{"passive: symbol 0xAA escaped"}, receivedSymbolBothSessions("0xAA")...),
		},
		{
			name:    "a raw-view data symbol is not delivered to the sessions again",
			fixture: receivedSymbolSessionOwned,
			input:   func(h *transitionHarness) { h.m.onLogicalReceived(transitionDataSymbol, false, false) },
			want:    transitionState{owner: "session 1", phase: "collect_request"},
			effects: []string{"passive: symbol 0x52"},
		},
	})
}

// TestReceivedSymbolTerminalRelease covers the terminal release through the
// handler (a garbled command acknowledgement of a session's telegram) and
// directly: only a terminal phase event of the owner that still holds the
// bus releases it, with the reason the event names for the gateway.
func TestReceivedSymbolTerminalRelease(t *testing.T) {
	telegram := []byte{transitionSessionSource, transitionGatewayData, 0xB5, 0x04, 0x00, 0x33}
	runReceivedSymbolRows(t, []receivedSymbolRow{
		{
			name: "garbled command acknowledgement releases the session",
			fixture: func(h *transitionHarness) {
				receivedSymbolSessionOwned(h)
				for _, b := range telegram {
					h.symbol(b)
				}
			},
			input:   func(h *transitionHarness) { h.symbol(0x99) },
			want:    transitionState{},
			effects: append([]string{"passive: symbol 0x99"}, receivedSymbolBothSessions("0x99")...),
		},
		{
			name: "command acknowledgement keeps the session",
			fixture: func(h *transitionHarness) {
				receivedSymbolSessionOwned(h)
				for _, b := range telegram {
					h.symbol(b)
				}
			},
			input:   func(h *transitionHarness) { h.symbol(protocol.SymbolAck) },
			want:    transitionState{owner: "session 1", phase: "wait_response_length"},
			effects: append([]string{"passive: symbol 0x00"}, receivedSymbolBothSessions("0x00")...),
		},
	})

	direct := []struct {
		name    string
		fixture func(h *transitionHarness)
		rs      receivedSymbol
		want    transitionState
		reason  ActiveTxnInactiveReason
	}{
		{"session command NACK", receivedSymbolSessionOwned,
			receivedSymbol{phaseEvent: wirePhaseEventCmdNACK, ownerID: 1}, transitionState{phase: "collect_request"}, ReasonNone},
		{"session transaction done", receivedSymbolSessionOwned,
			receivedSymbol{phaseEvent: wirePhaseEventTransactionDone, ownerID: 1}, transitionState{phase: "collect_request"}, ReasonNone},
		{"another owner holds the bus", receivedSymbolSessionOwned,
			receivedSymbol{phaseEvent: wirePhaseEventCmdNACK, ownerID: 2}, transitionState{owner: "session 1", phase: "collect_request"}, ReasonNone},
		{"no terminal event", receivedSymbolSessionOwned,
			receivedSymbol{phaseEvent: wirePhaseEventRequestComplete, ownerID: 1}, transitionState{owner: "session 1", phase: "collect_request"}, ReasonNone},
		{"gateway command NACK", transitionGatewayTransaction,
			receivedSymbol{phaseEvent: wirePhaseEventCmdNACK, ownerID: gatewaySessionID}, transitionState{phase: "collect_request"}, ReasonCmdNACK},
		{"gateway transaction done", transitionGatewayTransaction,
			receivedSymbol{phaseEvent: wirePhaseEventTransactionDone, ownerID: gatewaySessionID}, transitionState{phase: "collect_request"}, ReasonTransactionDone},
	}
	for _, tc := range direct {
		t.Run(tc.name, func(t *testing.T) {
			h := newTransitionHarness(t, transitionUpstreamAsync)
			tc.fixture(h)
			h.discardEffects()
			rs := tc.rs
			h.m.releaseOnTerminalPhaseEvent(&rs)
			if got := h.state(); got != tc.want {
				t.Errorf("state\n got %s\nwant %s", got, tc.want)
			}
			if got := receivedSymbolReadCounters(h).reason; got != tc.reason {
				t.Errorf("inactive reason %q, want %q", got, tc.reason)
			}
			if effects := h.effects(); len(effects) != 0 {
				t.Errorf("effects %q, want none", effects)
			}
		})
	}
}
