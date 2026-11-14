package adaptermux

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// The characterization table pins how the AdapterMux transition handlers
// behave today: for each fixture state and each input it records the state
// afterwards and the ordered effect log (see transition_harness_test.go). It
// describes observed behavior, defects included; a row that changes means the
// behavior changed.

// transitionFixture builds one mux state through the real handlers.
type transitionFixture struct {
	mode  transitionUpstreamMode
	build func(h *transitionHarness)
	// inputs lists the fixture-specific inputs on top of the common ones.
	inputs []string
}

// transitionCommonInputs apply in every non-raw-view fixture.
var transitionCommonInputs = []string{
	"wire_syn",
	"stream_wire_syn",
	"received_symbol",
	"received_initiator_symbol",
	"bid",
	"grant",
	"started_selected_source",
	"started_other_source",
	"failed",
	"session_close",
	"reset",
}

func transitionSelectSession(h *transitionHarness) {
	h.bid(1, transitionSessionSource)
	h.selected = "session 1 0x31"
	h.syn()
	h.captureDeadline()
}

func transitionSelectWithLocalLosers(h *transitionHarness) {
	h.bid(1, transitionSessionSource)
	h.bid(2, transitionOtherSource)
	h.bid(gatewaySessionID, transitionGatewaySource)
	h.selected = "session 1 0x31"
	h.syn()
	h.captureDeadline()
}

func transitionSelectGateway(h *transitionHarness) {
	h.source = transitionGatewaySource
	h.bid(gatewaySessionID, transitionGatewaySource)
	h.selected = "gateway 0x7F"
	h.syn()
	h.captureDeadline()
}

func transitionGatewayAwaitingEcho(h *transitionHarness) {
	transitionSelectGateway(h)
	h.feedStarted(transitionGatewaySource)
	h.gatewayWrite(transitionGatewayData, false)
}

func transitionGatewayTransaction(h *transitionHarness) {
	transitionGatewayAwaitingEcho(h)
	h.symbol(transitionGatewayData)
}

var transitionFixtures = map[string]transitionFixture{
	// idle: two sessions attached, no bid, no owner.
	"idle": {mode: transitionUpstreamAsync, build: func(*transitionHarness) {}},
	// bids_queued: session 1 bid 0x31 waits for an idle SYN.
	"bids_queued": {mode: transitionUpstreamAsync, build: func(h *transitionHarness) {
		h.bid(1, transitionSessionSource)
	}},
	// start_async: session 1 selected at SYN, RequestStart sent, no result yet.
	"start_async": {mode: transitionUpstreamAsync, build: transitionSelectSession, inputs: []string{"start_deadline"}},
	// start_async_local_losers: session 1 selected over session 2 and the
	// gateway, which were told they lost.
	"start_async_local_losers": {mode: transitionUpstreamAsync, build: transitionSelectWithLocalLosers, inputs: []string{"start_deadline"}},
	// start_async_gateway: the gateway selected at SYN, RequestStart sent.
	"start_async_gateway": {mode: transitionUpstreamAsync, build: transitionSelectGateway, inputs: []string{"start_deadline"}},
	// start_blocking: session 1 selected, the blocking StartArbitration held.
	"start_blocking": {mode: transitionUpstreamBlocking, build: func(h *transitionHarness) {
		h.bid(1, transitionSessionSource)
		h.selected = "session 1 0x31"
		h.syn()
		h.awaitStartCall()
		h.captureDeadline()
	}, inputs: []string{"start_deadline", "start_call_returns", "start_call_fails"}},
	// start_blocking_local_losers: session 1 selected over session 2 and the
	// gateway with the blocking StartArbitration held; the gateway's loss
	// stays deferred until the call returns.
	"start_blocking_local_losers": {mode: transitionUpstreamBlocking, build: func(h *transitionHarness) {
		h.bid(1, transitionSessionSource)
		h.bid(2, transitionOtherSource)
		h.bid(gatewaySessionID, transitionGatewaySource)
		h.selected = "session 1 0x31"
		h.syn()
		h.awaitStartCall()
		h.captureDeadline()
	}, inputs: []string{"start_deadline", "start_call_returns", "start_call_fails"}},
	// gateway_awaiting_echo: the gateway owns the bus and wrote 0x15; its
	// echo has not arrived.
	"gateway_awaiting_echo": {mode: transitionUpstreamAsync, build: transitionGatewayAwaitingEcho},
	// gateway_transaction: the gateway owns the bus, wrote 0x15 and its echo
	// reached the active path.
	"gateway_transaction": {mode: transitionUpstreamAsync, build: transitionGatewayTransaction},
	// gateway_grace: the gateway transaction ended at its terminator SYN and
	// the gateway still owns the bus inside the idle release grace.
	"gateway_grace": {mode: transitionUpstreamAsync, build: func(h *transitionHarness) {
		transitionGatewayTransaction(h)
		h.gatewayWrite(0xAA, true)
		h.syn()
	}},
	// session_owned: STARTED granted the bus to session 1.
	"session_owned": {mode: transitionUpstreamAsync, build: func(h *transitionHarness) {
		transitionSelectSession(h)
		h.feedStarted(transitionSessionSource)
	}},
	// fenced: the selection of session 1 lost to a foreign source; no
	// selection before the next SYN.
	"fenced": {mode: transitionUpstreamAsync, build: func(h *transitionHarness) {
		transitionSelectSession(h)
		h.feedFailed(transitionForeignSource)
	}},
	// absorb_pending: session 1 cancelled its in-flight START; its adapter
	// result is still expected.
	"absorb_pending": {mode: transitionUpstreamAsync, build: func(h *transitionHarness) {
		transitionSelectSession(h)
		h.m.cancelPendingStart(1)
	}},
	// raw_view_idle and raw_view_bids_queued use an upstream that also
	// supplies the raw wire symbols.
	"raw_view_idle": {mode: transitionUpstreamRawView, build: func(*transitionHarness) {},
		inputs: []string{"stream_wire_syn", "raw_escape_lead"}},
	"raw_view_bids_queued": {mode: transitionUpstreamRawView, build: func(h *transitionHarness) {
		h.bid(1, transitionSessionSource)
	}, inputs: []string{"stream_wire_syn", "raw_escape_lead"}},
}

var transitionInputs = map[string]func(h *transitionHarness){
	// wire_syn: a wire SYN through onLogicalReceived and onSYNLocked.
	"wire_syn": func(h *transitionHarness) { h.syn() },
	// stream_wire_syn: a wire SYN item through the read loop.
	"stream_wire_syn": func(h *transitionHarness) { h.feedWireSyn() },
	// received_symbol: a data symbol through onLogicalReceived.
	"received_symbol": func(h *transitionHarness) { h.symbol(transitionDataSymbol) },
	// received_initiator_symbol: a foreign initiator address through
	// onLogicalReceived.
	"received_initiator_symbol": func(h *transitionHarness) { h.symbol(transitionForeignSource) },
	// bid: session 2 bids 0x71.
	"bid": func(h *transitionHarness) { h.bid(2, transitionOtherSource) },
	// grant: tryGrantAndStart over the queued bids.
	"grant": func(h *transitionHarness) { h.m.tryGrantAndStart() },
	// started_selected_source: STARTED naming the source the fixture selected
	// (0x31 when it selected none).
	"started_selected_source": func(h *transitionHarness) { h.feedStarted(h.source) },
	// started_other_source: STARTED naming a foreign source.
	"started_other_source": func(h *transitionHarness) { h.feedStarted(transitionForeignSource) },
	// failed: FAILED naming a foreign winner.
	"failed": func(h *transitionHarness) { h.feedFailed(transitionForeignSource) },
	// session_close: session 1 disconnects.
	"session_close": func(h *transitionHarness) { h.m.RemoveSession(1) },
	// reset: the adapter reports RESETTED.
	"reset": func(h *transitionHarness) { h.feedReset() },
	// start_deadline: the START deadline of the fixture's selection fires.
	"start_deadline": func(h *transitionHarness) { h.fireDeadline(h.selected) },
	// start_call_returns: the held StartArbitration call succeeds.
	"start_call_returns": func(h *transitionHarness) { h.releaseStartCall(nil, h.selected) },
	// start_call_fails: the held StartArbitration call fails.
	"start_call_fails": func(h *transitionHarness) {
		h.releaseStartCall(errors.New("scripted START failure"), h.selected)
	},
	// raw_escape_lead: a raw escape lead that completes no logical item.
	"raw_escape_lead": func(h *transitionHarness) {
		h.feed(transitionItem{raw: 0xA9, hasRaw: true})
	},
}

// transitionApplicableInputs lists the inputs of one fixture.
func transitionApplicableInputs(fixture transitionFixture) []string {
	if fixture.mode == transitionUpstreamRawView {
		return fixture.inputs
	}
	return append(append([]string(nil), transitionCommonInputs...), fixture.inputs...)
}

type transitionRow struct {
	fixture string
	input   string
	want    transitionState
	effects []string
}

func (r transitionRow) literal(got transitionState, effects []string) string {
	return fmt.Sprintf("{%q, %q, %s, %#v},", r.fixture, r.input, got, effects)
}

var transitionRows = []transitionRow{
	{"absorb_pending", "wire_syn",
		transitionState{absorb: 1, retiredSelection: true},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"absorb_pending", "stream_wire_syn",
		transitionState{absorb: 1, retiredSelection: true},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"absorb_pending", "received_symbol",
		transitionState{absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{"passive: symbol 0x52", "session 1: RECEIVED 0x52", "session 2: RECEIVED 0x52"}},
	{"absorb_pending", "received_initiator_symbol",
		transitionState{absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"absorb_pending", "bid",
		transitionState{absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn", bids: "session 2 0x71"},
		[]string{}},
	{"absorb_pending", "grant",
		transitionState{absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{}},
	{"absorb_pending", "started_selected_source",
		transitionState{regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{}},
	{"absorb_pending", "started_other_source",
		transitionState{regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"absorb_pending", "failed",
		transitionState{regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"absorb_pending", "session_close",
		transitionState{absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{}},
	{"absorb_pending", "reset",
		transitionState{recovery: "reset"},
		[]string{"passive: reset", "active: error ebus: adapter reset during operation", "session 1: RESETTED 0x00", "session 2: RESETTED 0x00"}},

	{"bids_queued", "wire_syn",
		transitionState{start: "session 1 0x31"},
		[]string{"passive: symbol 0xAA", "adapter: RequestStart 0x31", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"bids_queued", "stream_wire_syn",
		transitionState{start: "session 1 0x31"},
		[]string{"passive: symbol 0xAA", "adapter: RequestStart 0x31", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"bids_queued", "received_symbol",
		transitionState{bids: "session 1 0x31"},
		[]string{"passive: symbol 0x52", "session 1: RECEIVED 0x52", "session 2: RECEIVED 0x52"}},
	{"bids_queued", "received_initiator_symbol",
		transitionState{bids: "session 1 0x31"},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"bids_queued", "bid",
		transitionState{bids: "session 1 0x31, session 2 0x71"},
		[]string{}},
	{"bids_queued", "grant",
		transitionState{start: "session 1 0x31"},
		[]string{"adapter: RequestStart 0x31"}},
	{"bids_queued", "started_selected_source",
		transitionState{bids: "session 1 0x31"},
		[]string{}},
	{"bids_queued", "started_other_source",
		transitionState{bids: "session 1 0x31"},
		[]string{}},
	{"bids_queued", "failed",
		transitionState{bids: "session 1 0x31"},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"bids_queued", "session_close",
		transitionState{},
		[]string{"result session 1 0x31: cancelled, source 0x31"}},
	{"bids_queued", "reset",
		transitionState{recovery: "reset"},
		[]string{"passive: reset", "active: error ebus: adapter reset during operation", "session 1: RESETTED 0x00", "session 2: RESETTED 0x00", "result session 1 0x31: source 0x31, error adaptermux: ebus: adapter reset during operation"}},

	{"fenced", "wire_syn",
		transitionState{retiredSelection: true},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"fenced", "stream_wire_syn",
		transitionState{retiredSelection: true},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"fenced", "received_symbol",
		transitionState{regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{"passive: symbol 0x52", "session 1: RECEIVED 0x52", "session 2: RECEIVED 0x52"}},
	{"fenced", "received_initiator_symbol",
		transitionState{regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"fenced", "bid",
		transitionState{regrantFence: true, retiredSelection: true, recovery: "at_syn", bids: "session 2 0x71"},
		[]string{}},
	{"fenced", "grant",
		transitionState{regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{}},
	{"fenced", "started_selected_source",
		transitionState{regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{}},
	{"fenced", "started_other_source",
		transitionState{regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"fenced", "failed",
		transitionState{regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"fenced", "session_close",
		transitionState{regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{}},
	{"fenced", "reset",
		transitionState{recovery: "reset"},
		[]string{"passive: reset", "active: error ebus: adapter reset during operation", "session 1: RESETTED 0x00", "session 2: RESETTED 0x00"}},

	{"gateway_awaiting_echo", "wire_syn",
		transitionState{owner: "gateway", gatewayTxn: true},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"gateway_awaiting_echo", "stream_wire_syn",
		transitionState{owner: "gateway", gatewayTxn: true},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"gateway_awaiting_echo", "received_symbol",
		transitionState{owner: "gateway", gatewayTxn: true, phase: "collect_request"},
		[]string{"session 1: RECEIVED 0x52", "session 2: RECEIVED 0x52"}},
	{"gateway_awaiting_echo", "received_initiator_symbol",
		transitionState{regrantFence: true, resyncOwed: 2, phase: "collect_request"},
		[]string{"passive: symbol 0x10", "active: 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"gateway_awaiting_echo", "bid",
		transitionState{owner: "gateway", gatewayTxn: true, phase: "collect_request", bids: "session 2 0x71"},
		[]string{}},
	{"gateway_awaiting_echo", "grant",
		transitionState{owner: "gateway", gatewayTxn: true, phase: "collect_request"},
		[]string{}},
	{"gateway_awaiting_echo", "started_selected_source",
		transitionState{owner: "gateway", gatewayTxn: true, phase: "collect_request"},
		[]string{}},
	{"gateway_awaiting_echo", "started_other_source",
		transitionState{owner: "gateway", gatewayTxn: true, phase: "collect_request"},
		[]string{}},
	{"gateway_awaiting_echo", "failed",
		transitionState{owner: "gateway", gatewayTxn: true, phase: "collect_request"},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"gateway_awaiting_echo", "session_close",
		transitionState{owner: "gateway", gatewayTxn: true, phase: "collect_request"},
		[]string{}},
	{"gateway_awaiting_echo", "reset",
		transitionState{recovery: "reset"},
		[]string{"passive: reset", "active: error ebus: adapter reset during operation", "session 1: RESETTED 0x00", "session 2: RESETTED 0x00"}},

	{"gateway_grace", "wire_syn",
		transitionState{owner: "gateway"},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"gateway_grace", "stream_wire_syn",
		transitionState{owner: "gateway"},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"gateway_grace", "received_symbol",
		transitionState{owner: "gateway"},
		[]string{"session 1: RECEIVED 0x52", "session 2: RECEIVED 0x52"}},
	{"gateway_grace", "received_initiator_symbol",
		transitionState{regrantFence: true},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"gateway_grace", "bid",
		transitionState{owner: "gateway", bids: "session 2 0x71"},
		[]string{}},
	{"gateway_grace", "grant",
		transitionState{owner: "gateway"},
		[]string{}},
	{"gateway_grace", "started_selected_source",
		transitionState{owner: "gateway"},
		[]string{}},
	{"gateway_grace", "started_other_source",
		transitionState{owner: "gateway"},
		[]string{}},
	{"gateway_grace", "failed",
		transitionState{owner: "gateway"},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"gateway_grace", "session_close",
		transitionState{owner: "gateway"},
		[]string{}},
	{"gateway_grace", "reset",
		transitionState{recovery: "reset"},
		[]string{"passive: reset", "active: error ebus: adapter reset during operation", "session 1: RESETTED 0x00", "session 2: RESETTED 0x00"}},

	{"gateway_transaction", "wire_syn",
		transitionState{owner: "gateway", gatewayTxn: true},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"gateway_transaction", "stream_wire_syn",
		transitionState{owner: "gateway", gatewayTxn: true},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"gateway_transaction", "received_symbol",
		transitionState{owner: "gateway", gatewayTxn: true, phase: "collect_request"},
		[]string{"active: 0x52", "session 1: RECEIVED 0x52", "session 2: RECEIVED 0x52"}},
	{"gateway_transaction", "received_initiator_symbol",
		transitionState{owner: "gateway", gatewayTxn: true, phase: "collect_request"},
		[]string{"active: 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"gateway_transaction", "bid",
		transitionState{owner: "gateway", gatewayTxn: true, phase: "collect_request", bids: "session 2 0x71"},
		[]string{}},
	{"gateway_transaction", "grant",
		transitionState{owner: "gateway", gatewayTxn: true, phase: "collect_request"},
		[]string{}},
	{"gateway_transaction", "started_selected_source",
		transitionState{owner: "gateway", gatewayTxn: true, phase: "collect_request"},
		[]string{}},
	{"gateway_transaction", "started_other_source",
		transitionState{owner: "gateway", gatewayTxn: true, phase: "collect_request"},
		[]string{}},
	{"gateway_transaction", "failed",
		transitionState{owner: "gateway", gatewayTxn: true, phase: "collect_request"},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"gateway_transaction", "session_close",
		transitionState{owner: "gateway", gatewayTxn: true, phase: "collect_request"},
		[]string{}},
	{"gateway_transaction", "reset",
		transitionState{recovery: "reset"},
		[]string{"passive: reset", "active: error ebus: adapter reset during operation", "session 1: RESETTED 0x00", "session 2: RESETTED 0x00"}},

	{"idle", "wire_syn",
		transitionState{},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"idle", "stream_wire_syn",
		transitionState{},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"idle", "received_symbol",
		transitionState{},
		[]string{"passive: symbol 0x52", "session 1: RECEIVED 0x52", "session 2: RECEIVED 0x52"}},
	{"idle", "received_initiator_symbol",
		transitionState{},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"idle", "bid",
		transitionState{bids: "session 2 0x71"},
		[]string{}},
	{"idle", "grant",
		transitionState{},
		[]string{}},
	{"idle", "started_selected_source",
		transitionState{},
		[]string{}},
	{"idle", "started_other_source",
		transitionState{},
		[]string{}},
	{"idle", "failed",
		transitionState{},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"idle", "session_close",
		transitionState{},
		[]string{}},
	{"idle", "reset",
		transitionState{recovery: "reset"},
		[]string{"passive: reset", "active: error ebus: adapter reset during operation", "session 1: RESETTED 0x00", "session 2: RESETTED 0x00"}},

	{"raw_view_bids_queued", "stream_wire_syn",
		transitionState{start: "session 1 0x31"},
		[]string{"passive: symbol 0xAA", "adapter: RequestStart 0x31", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"raw_view_bids_queued", "raw_escape_lead",
		transitionState{bids: "session 1 0x31"},
		[]string{"session 1: RECEIVED 0xA9", "session 2: RECEIVED 0xA9"}},

	{"raw_view_idle", "stream_wire_syn",
		transitionState{},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"raw_view_idle", "raw_escape_lead",
		transitionState{},
		[]string{"session 1: RECEIVED 0xA9", "session 2: RECEIVED 0xA9"}},

	{"session_owned", "wire_syn",
		transitionState{owner: "session 1"},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"session_owned", "stream_wire_syn",
		transitionState{owner: "session 1"},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"session_owned", "received_symbol",
		transitionState{owner: "session 1", phase: "collect_request"},
		[]string{"passive: symbol 0x52", "session 1: RECEIVED 0x52", "session 2: RECEIVED 0x52"}},
	{"session_owned", "received_initiator_symbol",
		transitionState{owner: "session 1", phase: "collect_request"},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"session_owned", "bid",
		transitionState{owner: "session 1", phase: "collect_request", bids: "session 2 0x71"},
		[]string{}},
	{"session_owned", "grant",
		transitionState{owner: "session 1", phase: "collect_request"},
		[]string{}},
	{"session_owned", "started_selected_source",
		transitionState{owner: "session 1", phase: "collect_request"},
		[]string{}},
	{"session_owned", "started_other_source",
		transitionState{owner: "session 1", phase: "collect_request"},
		[]string{}},
	{"session_owned", "failed",
		transitionState{owner: "session 1", phase: "collect_request"},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"session_owned", "session_close",
		transitionState{regrantFence: true, retiredSelection: true, recovery: "at_syn", phase: "collect_request"},
		[]string{}},
	{"session_owned", "reset",
		transitionState{recovery: "reset"},
		[]string{"passive: reset", "active: error ebus: adapter reset during operation", "session 1: RESETTED 0x00", "session 2: RESETTED 0x00"}},

	{"start_async", "wire_syn",
		transitionState{start: "session 1 0x31"},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"start_async", "stream_wire_syn",
		transitionState{start: "session 1 0x31"},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"start_async", "received_symbol",
		transitionState{start: "session 1 0x31"},
		[]string{"passive: symbol 0x52", "session 1: RECEIVED 0x52", "session 2: RECEIVED 0x52"}},
	{"start_async", "received_initiator_symbol",
		transitionState{start: "session 1 0x31"},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"start_async", "bid",
		transitionState{start: "session 1 0x31", bids: "session 2 0x71"},
		[]string{}},
	{"start_async", "grant",
		transitionState{start: "session 1 0x31"},
		[]string{}},
	{"start_async", "started_selected_source",
		transitionState{owner: "session 1", phase: "collect_request"},
		[]string{"passive: symbol 0x31", "session 1: STARTED 0x31", "session 2: RECEIVED 0x31", "result session 1 0x31: granted, source 0x31, started queued"}},
	{"start_async", "started_other_source",
		transitionState{absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{"passive: symbol 0x10", "session 2: RECEIVED 0x10", "result session 1 0x31: source 0x10, error adaptermux: STARTED mismatch (got 0x10, want 0x31)"}},
	{"start_async", "failed",
		transitionState{regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{"passive: symbol 0x10", "session 2: RECEIVED 0x10", "result session 1 0x31: source 0x10, error adaptermux: arbitration lost to 0x10: ebus: bus collision during arbitration"}},
	{"start_async", "session_close",
		transitionState{absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{"result session 1 0x31: cancelled, source 0x31"}},
	{"start_async", "reset",
		transitionState{recovery: "reset"},
		[]string{"passive: reset", "active: error ebus: adapter reset during operation", "session 1: RESETTED 0x00", "session 2: RESETTED 0x00", "result session 1 0x31: source 0x31, error adaptermux: ebus: adapter reset during operation"}},
	{"start_async", "start_deadline",
		transitionState{absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{"result session 1 0x31: source 0x31, error adaptermux: START deadline expired"}},

	{"start_async_gateway", "wire_syn",
		transitionState{start: "gateway 0x7F"},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"start_async_gateway", "stream_wire_syn",
		transitionState{start: "gateway 0x7F"},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"start_async_gateway", "received_symbol",
		transitionState{start: "gateway 0x7F"},
		[]string{"passive: symbol 0x52", "session 1: RECEIVED 0x52", "session 2: RECEIVED 0x52"}},
	{"start_async_gateway", "received_initiator_symbol",
		transitionState{start: "gateway 0x7F"},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"start_async_gateway", "bid",
		transitionState{start: "gateway 0x7F", bids: "session 2 0x71"},
		[]string{}},
	{"start_async_gateway", "grant",
		transitionState{start: "gateway 0x7F"},
		[]string{}},
	{"start_async_gateway", "started_selected_source",
		transitionState{owner: "gateway", phase: "collect_request"},
		[]string{"session 1: RECEIVED 0x7F", "session 2: RECEIVED 0x7F", "result gateway 0x7F: granted, source 0x7F"}},
	{"start_async_gateway", "started_other_source",
		transitionState{absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10", "result gateway 0x7F: source 0x10, error adaptermux: STARTED mismatch (got 0x10, want 0x7F)"}},
	{"start_async_gateway", "failed",
		transitionState{regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10", "result gateway 0x7F: source 0x10, error adaptermux: arbitration lost to 0x10: ebus: bus collision during arbitration"}},
	{"start_async_gateway", "session_close",
		transitionState{start: "gateway 0x7F"},
		[]string{}},
	{"start_async_gateway", "reset",
		transitionState{recovery: "reset"},
		[]string{"passive: reset", "active: error ebus: adapter reset during operation", "session 1: RESETTED 0x00", "session 2: RESETTED 0x00", "result gateway 0x7F: source 0x7F, error adaptermux: ebus: adapter reset during operation"}},
	{"start_async_gateway", "start_deadline",
		transitionState{absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{"result gateway 0x7F: source 0x7F, error adaptermux: START deadline expired"}},

	{"start_async_local_losers", "wire_syn",
		transitionState{start: "session 1 0x31", resyncOwed: 1},
		[]string{"passive: symbol 0xAA", "active: 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"start_async_local_losers", "stream_wire_syn",
		transitionState{start: "session 1 0x31", resyncOwed: 1},
		[]string{"passive: symbol 0xAA", "active: 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"start_async_local_losers", "received_symbol",
		transitionState{start: "session 1 0x31", resyncOwed: 2, localLosers: "gateway, session 2"},
		[]string{"passive: symbol 0x52", "session 1: RECEIVED 0x52", "session 2: RECEIVED 0x52"}},
	{"start_async_local_losers", "received_initiator_symbol",
		transitionState{start: "session 1 0x31", resyncOwed: 2, localLosers: "gateway, session 2"},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"start_async_local_losers", "bid",
		transitionState{start: "session 1 0x31", resyncOwed: 2, localLosers: "gateway, session 2", bids: "session 2 0x71"},
		[]string{}},
	{"start_async_local_losers", "grant",
		transitionState{start: "session 1 0x31", resyncOwed: 2, localLosers: "gateway, session 2"},
		[]string{}},
	{"start_async_local_losers", "started_selected_source",
		transitionState{owner: "session 1", resyncOwed: 2, phase: "collect_request"},
		[]string{"passive: symbol 0x31", "session 1: STARTED 0x31", "result session 1 0x31: granted, source 0x31, started queued"}},
	{"start_async_local_losers", "started_other_source",
		transitionState{absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn", resyncOwed: 2},
		[]string{"passive: symbol 0x10", "session 2: RECEIVED 0xAA", "session 2: RECEIVED 0x10", "result session 1 0x31: source 0x10, error adaptermux: STARTED mismatch (got 0x10, want 0x31)"}},
	{"start_async_local_losers", "failed",
		transitionState{regrantFence: true, retiredSelection: true, recovery: "at_syn", resyncOwed: 2},
		[]string{"passive: symbol 0x10", "session 2: RECEIVED 0xAA", "session 2: RECEIVED 0x10", "result session 1 0x31: source 0x10, error adaptermux: arbitration lost to 0x10: ebus: bus collision during arbitration"}},
	{"start_async_local_losers", "session_close",
		transitionState{absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn", resyncOwed: 2, localLosers: "gateway, session 2"},
		[]string{"result session 1 0x31: cancelled, source 0x31"}},
	{"start_async_local_losers", "reset",
		transitionState{recovery: "reset"},
		[]string{"passive: reset", "active: error ebus: adapter reset during operation", "session 1: RESETTED 0x00", "session 2: RESETTED 0x00", "result session 1 0x31: source 0x31, error adaptermux: ebus: adapter reset during operation"}},
	{"start_async_local_losers", "start_deadline",
		transitionState{absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn", resyncOwed: 2, localLosers: "gateway, session 2"},
		[]string{"result session 1 0x31: source 0x31, error adaptermux: START deadline expired"}},

	{"start_blocking", "wire_syn",
		transitionState{start: "session 1 0x31", startBlocking: true, blockingCall: true},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"start_blocking", "stream_wire_syn",
		transitionState{start: "session 1 0x31", startBlocking: true, blockingCall: true},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"start_blocking", "received_symbol",
		transitionState{start: "session 1 0x31", startBlocking: true, blockingCall: true},
		[]string{"passive: symbol 0x52", "session 1: RECEIVED 0x52", "session 2: RECEIVED 0x52"}},
	{"start_blocking", "received_initiator_symbol",
		transitionState{start: "session 1 0x31", startBlocking: true, blockingCall: true},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"start_blocking", "bid",
		transitionState{start: "session 1 0x31", startBlocking: true, blockingCall: true, bids: "session 2 0x71"},
		[]string{}},
	{"start_blocking", "grant",
		transitionState{start: "session 1 0x31", startBlocking: true, blockingCall: true},
		[]string{}},
	{"start_blocking", "started_selected_source",
		transitionState{owner: "session 1", blockingCall: true, phase: "collect_request"},
		[]string{"passive: symbol 0x31", "session 1: STARTED 0x31", "session 2: RECEIVED 0x31", "result session 1 0x31: granted, source 0x31, started queued"}},
	{"start_blocking", "started_other_source",
		transitionState{blockingCall: true, absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{"passive: symbol 0x10", "session 2: RECEIVED 0x10", "result session 1 0x31: source 0x10, error adaptermux: STARTED mismatch (got 0x10, want 0x31)"}},
	{"start_blocking", "failed",
		transitionState{blockingCall: true, regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{"passive: symbol 0x10", "session 2: RECEIVED 0x10", "result session 1 0x31: source 0x10, error adaptermux: arbitration lost to 0x10: ebus: bus collision during arbitration"}},
	{"start_blocking", "session_close",
		transitionState{blockingCall: true, absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{"result session 1 0x31: cancelled, source 0x31"}},
	{"start_blocking", "reset",
		transitionState{recovery: "reset"},
		[]string{"passive: reset", "active: error ebus: adapter reset during operation", "session 1: RESETTED 0x00", "session 2: RESETTED 0x00", "result session 1 0x31: source 0x31, error adaptermux: ebus: adapter reset during operation"}},
	{"start_blocking", "start_deadline",
		transitionState{blockingCall: true, absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn"},
		[]string{"result session 1 0x31: source 0x31, error adaptermux: START deadline expired"}},
	{"start_blocking", "start_call_returns",
		transitionState{owner: "session 1", phase: "collect_request"},
		[]string{"session 1: STARTED 0x31", "result session 1 0x31: granted, source 0x31, started queued"}},
	{"start_blocking", "start_call_fails",
		transitionState{},
		[]string{"result session 1 0x31: source 0x31, error scripted START failure"}},

	{"start_blocking_local_losers", "wire_syn",
		transitionState{start: "session 1 0x31", startBlocking: true, blockingCall: true, bids: "deferred gateway 0x7F"},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"start_blocking_local_losers", "stream_wire_syn",
		transitionState{start: "session 1 0x31", startBlocking: true, blockingCall: true, bids: "deferred gateway 0x7F"},
		[]string{"passive: symbol 0xAA", "session 1: RECEIVED 0xAA", "session 2: RECEIVED 0xAA"}},
	{"start_blocking_local_losers", "received_symbol",
		transitionState{start: "session 1 0x31", startBlocking: true, blockingCall: true, localLosers: "gateway, session 2", bids: "deferred gateway 0x7F"},
		[]string{"passive: symbol 0x52", "session 1: RECEIVED 0x52", "session 2: RECEIVED 0x52"}},
	{"start_blocking_local_losers", "received_initiator_symbol",
		transitionState{start: "session 1 0x31", startBlocking: true, blockingCall: true, localLosers: "gateway, session 2", bids: "deferred gateway 0x7F"},
		[]string{"passive: symbol 0x10", "session 1: RECEIVED 0x10", "session 2: RECEIVED 0x10"}},
	{"start_blocking_local_losers", "bid",
		transitionState{start: "session 1 0x31", startBlocking: true, blockingCall: true, localLosers: "gateway, session 2", bids: "session 2 0x71, deferred gateway 0x7F"},
		[]string{}},
	{"start_blocking_local_losers", "grant",
		transitionState{start: "session 1 0x31", startBlocking: true, blockingCall: true, localLosers: "gateway, session 2", bids: "deferred gateway 0x7F"},
		[]string{}},
	{"start_blocking_local_losers", "started_selected_source",
		transitionState{owner: "session 1", blockingCall: true, phase: "collect_request", bids: "deferred gateway 0x7F"},
		[]string{"passive: symbol 0x31", "session 1: STARTED 0x31", "result session 1 0x31: granted, source 0x31, started queued"}},
	{"start_blocking_local_losers", "started_other_source",
		transitionState{blockingCall: true, absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn", bids: "deferred gateway 0x7F"},
		[]string{"passive: symbol 0x10", "session 2: RECEIVED 0xAA", "session 2: RECEIVED 0x10", "result session 1 0x31: source 0x10, error adaptermux: STARTED mismatch (got 0x10, want 0x31)"}},
	{"start_blocking_local_losers", "failed",
		transitionState{blockingCall: true, regrantFence: true, retiredSelection: true, recovery: "at_syn", bids: "deferred gateway 0x7F"},
		[]string{"passive: symbol 0x10", "session 2: RECEIVED 0xAA", "session 2: RECEIVED 0x10", "result session 1 0x31: source 0x10, error adaptermux: arbitration lost to 0x10: ebus: bus collision during arbitration"}},
	{"start_blocking_local_losers", "session_close",
		transitionState{blockingCall: true, absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn", localLosers: "gateway, session 2", bids: "deferred gateway 0x7F"},
		[]string{"result session 1 0x31: cancelled, source 0x31"}},
	{"start_blocking_local_losers", "reset",
		transitionState{recovery: "reset"},
		[]string{"passive: reset", "active: error ebus: adapter reset during operation", "session 1: RESETTED 0x00", "session 2: RESETTED 0x00", "result session 1 0x31: source 0x31, error adaptermux: ebus: adapter reset during operation", "result gateway 0x7F: source 0x7F, error adaptermux: ebus: adapter reset during operation"}},
	{"start_blocking_local_losers", "start_deadline",
		transitionState{blockingCall: true, absorb: 1, regrantFence: true, retiredSelection: true, recovery: "at_syn", localLosers: "gateway, session 2"},
		[]string{"result session 1 0x31: source 0x31, error adaptermux: START deadline expired", "result gateway 0x7F: source 0x7F, error adaptermux: START deadline expired: ebus: no response within timeout window"}},
	{"start_blocking_local_losers", "start_call_returns",
		transitionState{owner: "session 1", resyncOwed: 2, phase: "collect_request", localLosers: "gateway, session 2"},
		[]string{"session 1: STARTED 0x31", "result session 1 0x31: granted, source 0x31, started queued", "result gateway 0x7F: source 0x31"}},
	{"start_blocking_local_losers", "start_call_fails",
		transitionState{},
		[]string{"session 2: RECEIVED 0xAA", "result session 1 0x31: source 0x31, error scripted START failure", "result gateway 0x7F: source 0x7F, error scripted START failure"}},
}

// TestMuxTransitionCharacterizationTableIsComplete requires exactly one row
// per fixture and applicable input, and no row for anything else.
func TestMuxTransitionCharacterizationTableIsComplete(t *testing.T) {
	want := map[string]bool{}
	for name, fixture := range transitionFixtures {
		for _, input := range transitionApplicableInputs(fixture) {
			if _, ok := transitionInputs[input]; !ok {
				t.Errorf("fixture %s names unknown input %s", name, input)
			}
			want[name+"/"+input] = true
		}
	}
	seen := map[string]bool{}
	for _, row := range transitionRows {
		key := row.fixture + "/" + row.input
		if seen[key] {
			t.Errorf("duplicate row %s", key)
		}
		seen[key] = true
		if !want[key] {
			t.Errorf("row %s has no applicable fixture and input", key)
		}
	}
	var missing []string
	for key := range want {
		if !seen[key] {
			missing = append(missing, key)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("missing rows:\n%s", strings.Join(missing, "\n"))
	}
}

// TestMuxTransitionCharacterization applies each row's input to its fixture
// through the real handlers and compares the state and the effect log.
func TestMuxTransitionCharacterization(t *testing.T) {
	for _, row := range transitionRows {
		row := row
		t.Run(row.fixture+"/"+row.input, func(t *testing.T) {
			fixture, ok := transitionFixtures[row.fixture]
			if !ok {
				t.Fatalf("unknown fixture %s", row.fixture)
			}
			apply, ok := transitionInputs[row.input]
			if !ok {
				t.Fatalf("unknown input %s", row.input)
			}
			h := newTransitionHarness(t, fixture.mode)
			fixture.build(h)
			h.discardEffects()
			apply(h)
			got := h.state()
			effects := h.effects()
			h.stop()
			if got != row.want || !reflect.DeepEqual(effects, row.effects) {
				t.Errorf("transition differs from the pinned row\n got: %s\nwant: %s", row.literal(got, effects), row.literal(row.want, row.effects))
			}
		})
	}
}
