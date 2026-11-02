package adaptermux

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol/telegram_fsm"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// An acknowledgement is closed on the next read, after readLoop has finished
// dispatching the previous item. This checks the real classifier/filter/raw/
// logical ordering without sleeps or racing a raw session's early delivery.
type t02ExitStep struct {
	item      upstreamItem
	err       error
	processed chan struct{}
}

type t02ExitTransport struct {
	*p3MockTransport
	steps        chan t02ExitStep
	processed    chan struct{}
	beforeCommit func()
}

func (tr *t02ExitTransport) read() (upstreamItem, error) {
	if tr.processed != nil {
		close(tr.processed)
		tr.processed = nil
	}
	select {
	case step := <-tr.steps:
		tr.processed = step.processed
		return step.item, step.err
	case <-tr.readDone:
		return upstreamItem{}, io.EOF
	}
}

func (tr *t02ExitTransport) ReadEvent() (transport.StreamEvent, error) {
	item, err := tr.read()
	return item.event, err
}

func (tr *t02ExitTransport) ArbitrationSendsSource() bool {
	if tr.beforeCommit != nil {
		tr.beforeCommit()
	}
	return true
}

type t02RawExitTransport struct{ *t02ExitTransport }

func (tr *t02RawExitTransport) ReadEventWithRawSymbol() (transport.StreamEvent, byte, bool, error) {
	item, err := tr.read()
	return item.event, item.rawSymbol, item.hasRaw, err
}

// The logger seam pauses after readLoop snapshots attribution, before the
// response handler takes stateMu. It lets cancellation/replacement win that
// exact interleaving without adding a production hook or timing sleeps.
type t02ExitLogWriter struct{ beforeResponse func() }

func (w *t02ExitLogWriter) Write(p []byte) (int, error) {
	if w.beforeResponse != nil && bytes.Contains(p, []byte("adaptermux: arbitration response ")) {
		fn := w.beforeResponse
		w.beforeResponse = nil
		fn()
	}
	return len(p), nil
}

type t02ExitCase struct {
	name     string
	retire   string
	result   transport.StreamEventKind
	address  byte
	phantom  bool
	boundary bool
	timeout  bool

	run func(*testing.T)
}

func TestT02_ArbitrationExitStateMachine(t *testing.T) {
	cases := []t02ExitCase{
		{name: "cancel_before_STARTED", retire: "cancel", result: transport.StreamEventStarted, address: 0x7f},
		{name: "cancel_confirmed_STARTED", retire: "owned", result: transport.StreamEventStarted, address: 0x7f},
		{name: "cancel_after_STARTED", retire: "commit", result: transport.StreamEventStarted, address: 0x7f},
		{name: "replace_before_STARTED", retire: "replace", result: transport.StreamEventStarted, address: 0x7f},
		{name: "expiry_before_STARTED", retire: "deadline", result: transport.StreamEventStarted, address: 0x7f},
		{name: "expired_STARTED_selected_after_SYN", retire: "deadline", result: transport.StreamEventStarted, address: 0x7f, boundary: true},
		{name: "expired_STARTED_selected_after_absorb_timeout", retire: "deadline", timeout: true, result: transport.StreamEventStarted, address: 0x7f, boundary: true},
		{name: "late_STARTED_selected", retire: "cancel", result: transport.StreamEventStarted, address: 0x7f, boundary: true},
		{name: "late_STARTED_after_absorb_timeout", retire: "cancel", timeout: true, result: transport.StreamEventStarted, address: 0x7f, boundary: true},
		{name: "late_STARTED_other", retire: "cancel", result: transport.StreamEventStarted, address: 0x10},
		{name: "expired_STARTED_other", retire: "deadline", result: transport.StreamEventStarted, address: 0x10},
		{name: "late_STARTED_other_after_SYN", retire: "cancel", result: transport.StreamEventStarted, address: 0x10, boundary: true},
		{name: "expired_STARTED_other_after_SYN", retire: "deadline", result: transport.StreamEventStarted, address: 0x10, boundary: true},
		{name: "late_STARTED_other_after_absorb_timeout", retire: "cancel", timeout: true, result: transport.StreamEventStarted, address: 0x10},
		{name: "late_STARTED_other_after_absorb_timeout_and_SYN", retire: "cancel", timeout: true, result: transport.StreamEventStarted, address: 0x10, boundary: true},
		{name: "expired_STARTED_other_after_absorb_timeout", retire: "deadline", timeout: true, result: transport.StreamEventStarted, address: 0x10, boundary: true},
		{name: "cancel_during_mismatched_STARTED", retire: "cancel_response", result: transport.StreamEventStarted, address: 0x10},
		{name: "replace_during_mismatched_STARTED", retire: "replace_response", result: transport.StreamEventStarted, address: 0x10},
		{name: "mismatched_STARTED", result: transport.StreamEventStarted, address: 0x10},
		{name: "replaced_mismatched_STARTED", retire: "replace", result: transport.StreamEventStarted, address: 0x10},
		{name: "phantom_FAILED", result: transport.StreamEventFailed, address: 0x71, phantom: true},
		{name: "cancelled_phantom_FAILED", retire: "cancel", result: transport.StreamEventFailed, address: 0x71, phantom: true},
		{name: "expired_phantom_FAILED", retire: "deadline", result: transport.StreamEventFailed, address: 0x71, phantom: true},
		{name: "replaced_phantom_FAILED", retire: "replace", result: transport.StreamEventFailed, address: 0x71, phantom: true},
		{name: "cancelled_phantom_FAILED_after_SYN", retire: "cancel", result: transport.StreamEventFailed, address: 0x71, phantom: true, boundary: true},
		{name: "expired_phantom_FAILED_after_SYN", retire: "deadline", result: transport.StreamEventFailed, address: 0x71, phantom: true, boundary: true},
		{name: "FAILED_foreign_winner", result: transport.StreamEventFailed, address: 0x10},
		{name: "cancelled_FAILED_foreign", retire: "cancel", result: transport.StreamEventFailed, address: 0x10},
		{name: "expired_FAILED_foreign", retire: "deadline", result: transport.StreamEventFailed, address: 0x10},
		{name: "replaced_FAILED_foreign", retire: "replace", result: transport.StreamEventFailed, address: 0x10},
		{name: "cancelled_FAILED_foreign_after_SYN", retire: "cancel", result: transport.StreamEventFailed, address: 0x10, boundary: true},
		{name: "expired_FAILED_foreign_after_SYN", retire: "deadline", result: transport.StreamEventFailed, address: 0x10, boundary: true},
		{name: "cancel_during_FAILED_foreign", retire: "cancel_response", result: transport.StreamEventFailed, address: 0x10},
		{name: "replace_during_FAILED_foreign", retire: "replace_response", result: transport.StreamEventFailed, address: 0x10},
		{name: "reset_after_STARTED_before_commit", retire: "reset_commit", result: transport.StreamEventStarted, address: 0x7f},
		{name: "reset_during_arbitration", retire: "reset"},
		{name: "disconnect_during_arbitration", retire: "disconnect"},
		{name: "raw_SYN_bid_eligibility", run: t02RawSYNFreezesPreSYNBidBeforeClientResponse},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.run != nil {
				tc.run(t)
				return
			}
			for _, mode := range []v8classifier.Mode{v8classifier.ModeOff, v8classifier.ModeShadow, v8classifier.ModeEnforce} {
				for _, raw := range []bool{false, true} {
					for _, synKind := range []transport.StreamEventKind{transport.StreamEventByte, transport.StreamEventWireSyn} {
						for _, selectedID := range []uint64{0, 1} {
							if tc.retire == "owned" && selectedID == gatewaySessionID {
								continue
							}
							t.Run(fmt.Sprintf("mode%d/raw%v/syn%d/session%d", mode, raw, synKind, selectedID), func(t *testing.T) {
								t02CheckArbitrationExit(t, tc, mode, raw, synKind, selectedID)
							})
						}
					}
				}
			}
		})
	}
}

func t02CheckArbitrationExit(t *testing.T, tc t02ExitCase, mode v8classifier.Mode, raw bool, synKind transport.StreamEventKind, selectedID uint64) {
	m, mock := newT02Mux(t)
	// An independent monitor is needed even when session 1 is selected:
	// sessions 2 and 3 already learned a source from local FAILED.
	m.sessions[4] = &session{id: 4, mux: m, sendCh: make(chan sessionFrame, 64), done: make(chan struct{})}
	type observation struct {
		kind PassiveEventKind
		b    byte
		esc  bool
	}
	var passive []observation
	recordPassive := func(ev PassiveEvent) {
		passive = append(passive, observation{ev.Kind, ev.Symbol, ev.WasEscaped})
	}
	m.SetPassiveCallback(recordPassive)
	wantPassive := []observation{{kind: PassiveEventSymbol, b: protocol.SymbolSyn}}
	wantSymbol := func(b byte) { wantPassive = append(wantPassive, observation{kind: PassiveEventSymbol, b: b}) }
	logWriter := &t02ExitLogWriter{}
	m.logger = log.New(logWriter, "", 0)
	m.v8 = v8classifier.New(mode)
	m.cfg.StartDeadline = 10 * time.Second
	if tc.retire == "deadline" || tc.timeout {
		m.cfg.StartDeadline = 30 * time.Millisecond
	}
	m.cfg.IsKnownInitiatorByte = func(b byte) bool { return b != 0x71 }
	tr := &t02ExitTransport{p3MockTransport: mock, steps: make(chan t02ExitStep)}
	install := func() {
		mock.readDone = m.ctx.Done()
		m.upstream = tr
		if raw {
			m.upstream = &t02RawExitTransport{tr}
		}
	}
	install()
	var loopDone chan struct{}
	start := func() {
		loopDone = make(chan struct{})
		m.wg.Add(1)
		go func() { m.readLoop(); close(loopDone) }()
	}
	start()
	defer func() { m.cancel(); m.wg.Wait() }()
	send := func(ev transport.StreamEvent) {
		t.Helper()
		step := t02ExitStep{item: upstreamItem{event: ev}, processed: make(chan struct{})}
		if raw && (ev.Kind == transport.StreamEventByte || ev.Kind == transport.StreamEventWireSyn) {
			step.item.hasRaw, step.item.rawSymbol = true, ev.Byte
		}
		select {
		case tr.steps <- step:
		case <-time.After(time.Second):
			t.Fatal("readLoop not reading")
		}
		select {
		case <-step.processed:
		case <-time.After(time.Second):
			t.Fatal("readLoop did not finish dispatch")
		}
	}
	syn := transport.StreamEvent{Kind: synKind, Byte: protocol.SymbolSyn}
	var selected <-chan startResult
	var selectedDone chan struct{}
	if selectedID == gatewaySessionID {
		selected = m.requestStartForSession(selectedID, 0x7f)
	} else {
		m.sessions[selectedID].handleStart(0x7f)
		selectedDone = make(chan struct{})
		go func() { m.sessions[selectedID].wg.Wait(); close(selectedDone) }()
	}
	waitSelected := func() {
		t.Helper()
		select {
		case <-selectedDone:
		case <-time.After(time.Second):
			t.Fatal("session result waiter did not finish")
		}
	}
	loser := m.requestStartForSession(2, 0xef)
	otherLoser := m.requestStartForSession(3, 0xff)
	send(syn)
	t02Result(t, loser, 0x7f, false)
	t02Result(t, otherLoser, 0x7f, false)
	// Compare complete ordered session views, including all result
	// frames. No session writer races the deterministic fixture.
	type frame struct {
		kind    sessionFrameKind
		payload byte
	}
	r := func(b byte) frame { return frame{sessionFrameReceived, b} }
	want := map[uint64][]frame{1: {r(protocol.SymbolSyn)}, 2: {r(protocol.SymbolSyn), {sessionFrameFailed, 0x7f}}, 3: {r(protocol.SymbolSyn), {sessionFrameFailed, 0x7f}}, 4: {r(protocol.SymbolSyn)}}
	all := func(f frame) {
		for id := range want {
			want[id] = append(want[id], f)
		}
	}
	switch tc.retire {
	case "cancel":
		m.cancelPendingStart(selectedID)
	case "cancel_response":
		logWriter.beforeResponse = func() { m.cancelPendingStart(selectedID) }
	case "replace_response":
		logWriter.beforeResponse = func() { m.requestStartForSession(selectedID, 0x13) }
	case "replace":
		m.requestStartForSession(selectedID, 0x13)
	case "reset_commit":
		tr.beforeCommit = m.handleReset
	case "commit":
		tr.beforeCommit = func() { m.cancelPendingStart(selectedID) }
	case "deadline":
		if selectedID != gatewaySessionID {
			waitSelected()
			want[selectedID] = append(want[selectedID], frame{sessionFrameFailed, 0x7f})
			break
		}
		select {
		case result := <-selected:
			if result.granted || result.err == nil {
				t.Fatalf("expiry=%+v", result)
			}
		case <-time.After(time.Second):
			t.Fatal("bid did not expire")
		}
	}
	if tc.timeout {
		deadline := time.Now().Add(time.Second)
		for {
			m.stateMu.Lock()
			absorbing := m.pendingStartAbsorb > 0
			m.stateMu.Unlock()
			if !absorbing {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("absorb timeout did not retire the stale-response budget")
			}
			time.Sleep(time.Millisecond)
		}
	}
	if tc.boundary {
		send(syn)
		all(r(protocol.SymbolSyn))
		wantSymbol(protocol.SymbolSyn)
	}
	switch tc.retire {
	case "reset", "disconnect":
		if tc.retire == "reset" {
			send(transport.StreamEvent{Kind: transport.StreamEventReset})
			all(frame{sessionFrameResetted, 0})
			wantPassive = append(wantPassive, observation{kind: PassiveEventReset})
		} else {
			// Cancel the retry delay at the disconnect boundary: exercise
			// callback-free teardown and replace only the in-memory stream.
			cancel := m.cancel
			m.SetPassiveCallback(func(ev PassiveEvent) {
				recordPassive(ev)
				if ev.Kind == PassiveEventDisconnected {
					cancel()
				}
			})
			tr.steps <- t02ExitStep{err: io.EOF}
			select {
			case <-loopDone:
			case <-time.After(time.Second):
				t.Fatal("disconnect teardown blocked")
			}
			if selectedID != gatewaySessionID {
				waitSelected()
			}
			wantPassive = append(wantPassive, observation{kind: PassiveEventDisconnected})
			m.SetPassiveCallback(recordPassive)
			m.ctx, m.cancel = context.WithCancel(context.Background())
			install()
			start()
		}
	case "reset_commit":
		send(transport.StreamEvent{Kind: tc.result, Data: tc.address})
		tr.beforeCommit = nil
		all(frame{sessionFrameResetted, 0})
		wantPassive = append(wantPassive, observation{kind: PassiveEventReset})
	default:
		send(transport.StreamEvent{Kind: tc.result, Data: tc.address})
		tr.beforeCommit = nil
		if selectedID != gatewaySessionID {
			waitSelected()
			switch tc.retire {
			case "owned":
				want[selectedID] = append(want[selectedID], frame{sessionFrameStarted, 0x7f})
				m.sessions[selectedID].handleStart(protocol.SymbolSyn)
			case "":
				winner := tc.address
				if tc.phantom {
					winner = 0x7f
				}
				want[selectedID] = append(want[selectedID], frame{sessionFrameFailed, winner})
			}
		} else if tc.retire != "deadline" {
			select {
			case result := <-selected:
				if result.granted || (tc.retire != "" && !result.cancelled) {
					t.Fatalf("aborted result=%+v", result)
				}

				if tc.retire == "" {
					winner := tc.address
					if tc.phantom {
						winner = 0x7f
					}
					if result.initiator != winner {
						t.Fatalf("FAILED winner=%02x want %02x", result.initiator, winner)
					}
				}
			default:
				t.Fatal("selected waiter unresolved")
			}
		}
		if !tc.boundary && (tc.result == transport.StreamEventFailed || tc.address != 0x7f) {
			for _, id := range []uint64{2, 3} {
				want[id] = append(want[id], r(protocol.SymbolSyn))
			}
		}
		if tc.result == transport.StreamEventFailed && !tc.phantom {
			for id := range want {
				if tc.retire != "" || id != selectedID {
					want[id] = append(want[id], r(tc.address))
				}
			}
		} else if tc.result == transport.StreamEventStarted && tc.address != 0x7f {
			for id := range want {
				if tc.retire == "" && id == selectedID {
					continue
				}
				want[id] = append(want[id], r(tc.address))
			}
		} else if tc.result == transport.StreamEventStarted && (tc.retire == "replace" || tc.retire == "commit" || tc.retire == "owned") {
			// A matching STARTED seen with the waiter still pending supplies
			// QQ to monitors. Local losers already have QQ until a wire SYN.
			for id := range want {
				if id != selectedID && (tc.boundary || (id != 2 && id != 3)) {
					want[id] = append(want[id], r(0x7f))
				}
			}
		}
		// STARTED for a foreign address and every FAILED carries native
		// source evidence. Session-only loser SYNs must never enter this
		// stream, and a retired bidder must not hide the actual source.
		if tc.result == transport.StreamEventFailed || tc.address != 0x7f ||
			(selectedID != gatewaySessionID && (tc.retire == "owned" || tc.retire == "commit" || tc.retire == "replace")) {
			wantSymbol(tc.address)
		}
		// Preserve the earlier reconstruction checks: the phantom is
		// suppressed, but the next real source and target still flow.
		if tc.phantom {
			send(transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xf1})
			all(r(0xf1))
			wantSymbol(0xf1)
		}
		send(transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x08})
		all(r(0x08))
		wantSymbol(0x08)
	}
	if selectedID != gatewaySessionID && (tc.retire == "reset" || tc.retire == "reset_commit" || tc.retire == "disconnect") {
		waitSelected()
		want[selectedID] = append(want[selectedID], frame{sessionFrameResetted, 0})
	}
	// A mismatched STARTED leaves one physical result outstanding;
	// consume it before checking the final idle recovery boundary.
	m.stateMu.Lock()
	absorb := m.pendingStartAbsorb
	m.stateMu.Unlock()
	if absorb > 0 {
		send(transport.StreamEvent{Kind: transport.StreamEventFailed, Data: 0x71})
		wantSymbol(0x71)
	}
	if tc.retire == "replace" || tc.retire == "replace_response" {
		m.arb.cancelStart(selectedID)
	}
	m.stateMu.Lock()
	pending, fencedBeforeSYN := m.pendingStart != nil, m.deferRegrantUntilNextSyn
	m.stateMu.Unlock()
	_, _, owned := m.arb.owner()
	lifecycleReset := tc.retire == "reset" || tc.retire == "reset_commit" || tc.retire == "disconnect"
	if pending || owned || fencedBeforeSYN == lifecycleReset {
		t.Fatalf("after abort: pending=%v owner=%v fence=%v lifecycleReset=%v", pending, owned, fencedBeforeSYN, lifecycleReset)
	}
	// In enforce mode a real foreign winner's telegram stays filtered:
	// this first AA after its bytes reaches nobody and keeps the fence.
	// Idle repeats SYN, so the next one is the recovery boundary.
	if mode == v8classifier.ModeEnforce && !lifecycleReset && !tc.phantom && tc.address != 0x7f {
		send(syn)
		m.stateMu.Lock()
		stillFenced := m.deferRegrantUntilNextSyn
		m.stateMu.Unlock()
		if !stillFenced {
			t.Fatal("first AA after a foreign winner released the fence")
		}
	}
	// No new bid exists at this boundary. It must clear recovery
	// in both trackers; the following SYN admits a fresh bid.
	send(syn)
	all(r(protocol.SymbolSyn))
	wantSymbol(protocol.SymbolSyn)
	if !reflect.DeepEqual(passive, wantPassive) {
		t.Fatalf("passive observations=%v want %v", passive, wantPassive)
	}
	if state := m.v8.FSMInternalState(); state != telegram_fsm.StateIdle {
		t.Fatalf("classifier after idle SYN=%v", state)
	}
	m.stateMu.Lock()
	fenced, losersLeft, absorbLeft, phase, recovery := m.deferRegrantUntilNextSyn, len(m.localLosers), m.pendingStartAbsorb, m.phase.phase, m.arbitrationRecovery
	m.stateMu.Unlock()
	if fenced || losersLeft != 0 || absorbLeft != 0 || phase != wirePhaseIdle || recovery != arbitrationRecoveryNone {
		t.Fatalf("idle SYN: fence=%v losers=%d absorb=%d phase=%v recovery=%v", fenced, losersLeft, absorbLeft, phase, recovery)
	}
	for id, expected := range want {
		var got []frame
		for len(m.sessions[id].sendCh) > 0 {
			f := <-m.sessions[id].sendCh
			got = append(got, frame{f.kind, f.payload})
		}
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("session %d frames=%v want %v", id, got, expected)
		}
	}
	next := m.requestStartForSession(1, 0x31)
	if got := mock.getStartRequests(); !reflect.DeepEqual(got, []byte{0x7f}) {
		t.Fatalf("premature STARTs=%x", got)
	}
	send(syn)
	if got := mock.getStartRequests(); !reflect.DeepEqual(got, []byte{0x7f, 0x31}) {
		t.Fatalf("next SYN STARTs=%x", got)
	}
	send(transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x31})
	t02Result(t, next, 0x31, true)
	wantSymbol(protocol.SymbolSyn)
	wantSymbol(0x31)
	if !reflect.DeepEqual(passive, wantPassive) {
		t.Fatalf("next grant passive observations=%v want %v", passive, wantPassive)
	}
	for id, expected := range map[uint64][]frame{1: {r(protocol.SymbolSyn), {sessionFrameStarted, 0x31}}, 2: {r(protocol.SymbolSyn), r(0x31)}, 3: {r(protocol.SymbolSyn), r(0x31)}, 4: {r(protocol.SymbolSyn), r(0x31)}} {
		var got []frame
		for len(m.sessions[id].sendCh) > 0 {
			f := <-m.sessions[id].sendCh
			got = append(got, frame{f.kind, f.payload})
		}
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("next grant session %d frames=%v want %v", id, got, expected)
		}
	}
}

func t02RawSYNFreezesPreSYNBidBeforeClientResponse(t *testing.T) {
	for _, kind := range []transport.StreamEventKind{transport.StreamEventByte, transport.StreamEventWireSyn} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			m, tr := newT02Mux(t)
			winner := m.requestStartForSession(1, 0x73)
			loser := m.requestStartForSession(3, 0x7f)
			event := transport.StreamEvent{Kind: kind, Byte: protocol.SymbolSyn}

			// Split the raw readLoop dispatch at the client-visible boundary:
			// a higher-priority client responds after RECEIVED(SYN), before
			// the paired logical item is processed. Eligibility must already
			// be frozen, rather than first captured by logical processing.
			bids := m.onRawSymbol(protocol.SymbolSyn, event)
			for _, id := range []uint64{1, 2, 3} {
				select {
				case frame := <-m.sessions[id].sendCh:
					if frame.kind != sessionFrameReceived || frame.payload != protocol.SymbolSyn {
						t.Fatalf("session %d raw SYN=%+v", id, frame)
					}
				default:
					t.Fatalf("session %d did not receive raw SYN", id)
				}
			}
			late := m.requestStartForSession(2, 0x31)
			m.onLogicalReceivedForBids(event.Byte, event.WasEscaped, false, bids)

			if got := tr.getStartRequests(); !reflect.DeepEqual(got, []byte{0x73}) {
				t.Fatalf("raw SYN window STARTs=%x, want 73", got)
			}
			t02Result(t, loser, 0x73, false)
			t02Failed(t, m, 3, 0x73)
			select {
			case result := <-late:
				t.Fatalf("post-SYN bid resolved in prior window: %+v", result)
			default:
			}
			m.handleArbitrationResponse(true, 0x73)
			t02Result(t, winner, 0x73, true)
			m.arb.releaseOwnership(1)
			bids = m.onRawSymbol(protocol.SymbolSyn, event)
			m.onLogicalReceivedForBids(event.Byte, event.WasEscaped, false, bids)
			if got := tr.getStartRequests(); !reflect.DeepEqual(got, []byte{0x73, 0x31}) {
				t.Fatalf("next raw SYN STARTs=%x, want 7331", got)
			}
			m.handleArbitrationResponse(true, 0x31)
			t02Result(t, late, 0x31, true)
		})
	}
}

// t02ForeignTelegramFixture drives the real readLoop in enforce mode through a
// selection that ended with a response naming a foreign winner.
func t02ForeignTelegramFixture(t *testing.T, raw bool, response transport.StreamEventKind, retire string) (m *Mux, mock *p3MockTransport, send func(transport.StreamEvent), passive func() []byte) {
	t.Helper()
	m, mock = newT02Mux(t)
	var seen []byte
	m.SetPassiveCallback(func(ev PassiveEvent) { seen = append(seen, ev.Symbol) })
	m.logger = log.New(io.Discard, "", 0)
	m.v8 = v8classifier.New(v8classifier.ModeEnforce)
	m.cfg.IsKnownInitiatorByte = func(b byte) bool { return b != 0x71 }
	tr := &t02ExitTransport{p3MockTransport: mock, steps: make(chan t02ExitStep)}
	mock.readDone = m.ctx.Done()
	m.upstream = tr
	if raw {
		m.upstream = &t02RawExitTransport{tr}
	}
	m.wg.Add(1)
	go m.readLoop()
	t.Cleanup(func() { m.cancel(); m.wg.Wait() })
	send = func(ev transport.StreamEvent) {
		t.Helper()
		step := t02ExitStep{item: upstreamItem{event: ev}, processed: make(chan struct{})}
		if raw && (ev.Kind == transport.StreamEventByte || ev.Kind == transport.StreamEventWireSyn) {
			step.item.hasRaw, step.item.rawSymbol = true, ev.Byte
		}
		select {
		case tr.steps <- step:
		case <-time.After(time.Second):
			t.Fatal("readLoop not reading")
		}
		select {
		case <-step.processed:
		case <-time.After(time.Second):
			t.Fatal("readLoop did not finish dispatch")
		}
	}
	m.requestStartForSession(1, 0x7f)
	send(transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn})
	switch retire {
	case "cancel":
		m.cancelPendingStart(1)
	case "absorbTimeout":
		m.cancelPendingStart(1)
		m.stateMu.Lock()
		m.pendingStartAbsorb = 0
		m.stateMu.Unlock()
	}
	send(transport.StreamEvent{Kind: response, Data: 0x10})
	for len(m.sessions[3].sendCh) > 0 {
		<-m.sessions[3].sendCh
	}
	seen = nil
	return m, mock, send, func() []byte { return seen }
}

func TestT02_ForeignWinnerTelegramKeepsAAFilter(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response transport.StreamEventKind
		retire   string
	}{
		{"FAILED_pending", transport.StreamEventFailed, ""},
		{"FAILED_cancelled", transport.StreamEventFailed, "cancel"},
		{"FAILED_absorbTimeout", transport.StreamEventFailed, "absorbTimeout"},
		{"STARTED_mismatch", transport.StreamEventStarted, ""},
		{"STARTED_cancelled", transport.StreamEventStarted, "cancel"},
		{"STARTED_absorbTimeout", transport.StreamEventStarted, "absorbTimeout"},
	} {
		for _, raw := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/raw%v", tc.name, raw), func(t *testing.T) {
				m, mock, send, passive := t02ForeignTelegramFixture(t, raw, tc.response, tc.retire)
				plain := func(bs ...byte) {
					t.Helper()
					for _, b := range bs {
						send(transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b})
					}
				}
				next := m.requestStartForSession(2, 0x31)
				plain(0x08, 0xb5) // ZZ PB of the winner's header
				// The injected AA inside the header must reach nobody and must
				// not open an arbitration window for the waiting bid.
				plain(protocol.SymbolSyn)
				if got := passive(); !reflect.DeepEqual(got, []byte{0x08, 0xb5}) {
					t.Errorf("passive=%x, injected AA reached observers", got)
				}
				for len(m.sessions[3].sendCh) > 0 {
					if f := <-m.sessions[3].sendCh; f.payload == protocol.SymbolSyn {
						t.Errorf("session observed injected AA: %+v", f)
					}
				}
				if got := mock.getStartRequests(); !reflect.DeepEqual(got, []byte{0x7f}) {
					t.Fatalf("injected AA admitted START: %x", got)
				}
				// The rest of the telegram is still tracked as the same telegram.
				plain(0x04, 0x01, 0x55, 0x66, 0x00, 0x01, 0x22, 0x33, 0x00)
				if got := mock.getStartRequests(); !reflect.DeepEqual(got, []byte{0x7f}) {
					t.Fatalf("telegram body admitted START: %x", got)
				}
				// Its real terminator is a genuine idle SYN: recovery releases.
				plain(protocol.SymbolSyn)
				if state := m.v8.FSMInternalState(); state != telegram_fsm.StateIdle {
					t.Fatalf("classifier after terminator=%v", state)
				}
				m.stateMu.Lock()
				fenced, recovery := m.deferRegrantUntilNextSyn, m.arbitrationRecovery
				m.stateMu.Unlock()
				if fenced || recovery != arbitrationRecoveryNone {
					t.Fatalf("after terminator: fence=%v recovery=%v", fenced, recovery)
				}
				// A mismatched STARTED leaves its own physical result outstanding.
				m.stateMu.Lock()
				absorb := m.pendingStartAbsorb
				m.stateMu.Unlock()
				if absorb > 0 {
					send(transport.StreamEvent{Kind: transport.StreamEventFailed, Data: 0x71})
					plain(protocol.SymbolSyn)
				}
				plain(protocol.SymbolSyn)
				if got := mock.getStartRequests(); !reflect.DeepEqual(got, []byte{0x7f, 0x31}) {
					t.Fatalf("recovered STARTs=%x", got)
				}
				_ = next
			})
		}
	}
}

// An abandoned foreign telegram must not strand the fence: consecutive AAs are
// idle, while a phantom FAILED or the selected address has no telegram to protect.
func TestT02_ForeignRecoveryReleasesAtGenuineIdle(t *testing.T) {
	t.Run("abandoned_telegram", func(t *testing.T) {
		m, mock, send, passive := t02ForeignTelegramFixture(t, false, transport.StreamEventFailed, "")
		syn := transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
		m.requestStartForSession(2, 0x31)
		send(transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x08})
		send(syn)
		if got := mock.getStartRequests(); !reflect.DeepEqual(got, []byte{0x7f}) {
			t.Fatalf("first AA admitted START: %x", got)
		}
		send(syn)
		if got := passive(); !reflect.DeepEqual(got, []byte{0x08, protocol.SymbolSyn}) {
			t.Fatalf("passive=%x", got)
		}
		if state := m.v8.FSMInternalState(); state != telegram_fsm.StateIdle {
			t.Fatalf("classifier=%v", state)
		}
		if got := mock.getStartRequests(); !reflect.DeepEqual(got, []byte{0x7f, 0x31}) {
			t.Fatalf("idle STARTs=%x", got)
		}
	})
	t.Run("telegram_byte_between_AAs_keeps_filtering", func(t *testing.T) {
		m, mock, send, _ := t02ForeignTelegramFixture(t, false, transport.StreamEventFailed, "")
		syn := transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
		m.requestStartForSession(2, 0x31)
		send(syn)
		send(transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x08})
		send(syn)
		if got := mock.getStartRequests(); !reflect.DeepEqual(got, []byte{0x7f}) {
			t.Fatalf("separated AAs admitted START: %x", got)
		}
	})
	t.Run("aborted_classifier_recovers_at_first_SYN", func(t *testing.T) {
		m, mock, send, _ := t02ForeignTelegramFixture(t, false, transport.StreamEventFailed, "")
		m.requestStartForSession(2, 0x31)
		for _, b := range []byte{0x08, 0xb5, 0x04, 0x20} { // NN > 16 aborts
			send(transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b})
		}
		if state := m.v8.FSMInternalState(); state != telegram_fsm.StateAborted {
			t.Fatalf("classifier=%v, want aborted", state)
		}
		send(transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn})
		if state := m.v8.FSMInternalState(); state != telegram_fsm.StateIdle {
			t.Fatalf("classifier=%v", state)
		}
		if got := mock.getStartRequests(); !reflect.DeepEqual(got, []byte{0x7f, 0x31}) {
			t.Fatalf("STARTs=%x", got)
		}
	})
}
