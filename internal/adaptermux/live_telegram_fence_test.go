package adaptermux

import (
	"io"
	"log"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol/telegram_fsm"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// t02LiveTelegramFixture drives the real readLoop in enforce mode with external
// session 1 granted the bus (source 0x31) and session 2 waiting with a bid.
// The returned send helper feeds one wire byte and waits until it is dispatched.
func t02LiveTelegramFixture(t *testing.T, raw, wrote bool) (m *Mux, mock *p3MockTransport, plain func(...byte), passive func() []byte) {
	t.Helper()
	m, mock = newT02Mux(t)
	var seen []byte
	m.SetPassiveCallback(func(ev PassiveEvent) { seen = append(seen, ev.Symbol) })
	m.logger = log.New(io.Discard, "", 0)
	m.v8 = v8classifier.New(v8classifier.ModeEnforce)
	tr := &t02ExitTransport{p3MockTransport: mock, steps: make(chan t02ExitStep)}
	mock.readDone = m.ctx.Done()
	m.upstream = tr
	if raw {
		m.upstream = &t02RawExitTransport{tr}
	}
	m.wg.Add(1)
	go m.readLoop()
	t.Cleanup(func() { m.cancel(); m.wg.Wait() })
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
	plain = func(bs ...byte) {
		t.Helper()
		for _, b := range bs {
			send(transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b})
		}
	}
	// Session 1 needs a real connection so RemoveSession can close it.
	srv, cli := net.Pipe()
	t.Cleanup(func() { _ = srv.Close(); _ = cli.Close() })
	m.sessions[1].conn = srv

	m.requestStartForSession(1, 0x31)
	plain(protocol.SymbolSyn)
	send(transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x31})
	if !m.arb.isOwner(1) {
		t.Fatal("session 1 did not become the owner")
	}
	m.sessions[1].grantDelivered.Store(true)
	// The owner has written (a SEND was accepted) by the time its telegram is
	// on the wire; the test feeds the resulting wire bytes itself.
	if wrote {
		m.stateMu.Lock()
		m.phase.markOwnerWrite()
		m.stateMu.Unlock()
	}
	for _, id := range []uint64{1, 2, 3} {
		for len(m.sessions[id].sendCh) > 0 {
			<-m.sessions[id].sendCh
		}
	}
	seen = nil
	return m, mock, plain, func() []byte { return seen }
}

// An external owner that already wrote its request leaves a telegram on the
// wire when it goes away. Whichever way its ownership ends, the response that
// is still arriving stays subject to the mid-telegram AA filter.
func TestT02_LiveTelegramSurvivesOwnerRelease(t *testing.T) {
	for _, tc := range []struct {
		name string
		end  func(*Mux)
	}{
		{"cancel", func(m *Mux) { m.sessions[1].handleStart(protocol.SymbolSyn) }},
		{"disconnect", func(m *Mux) { m.RemoveSession(1) }},
		// Other fence raisers that can run while a telegram is on the wire:
		// a result absorbed after its request was cancelled, and a fence
		// requested by any other path.
		{"absorbed_result", func(m *Mux) {
			m.stateMu.Lock()
			m.pendingStartAbsorb = 1
			m.stateMu.Unlock()
			m.handleArbitrationResponse(false, 0x71)
		}},
		{"direct_fence", func(m *Mux) {
			m.stateMu.Lock()
			m.fenceArbitrationUntilSYNLocked()
			m.stateMu.Unlock()
		}},
	} {
		for _, raw := range []bool{false, true} {
			name := tc.name
			if raw {
				name += "/raw"
			}
			t.Run(name, func(t *testing.T) {
				m, mock, plain, passive := t02LiveTelegramFixture(t, raw, true)
				// Request: QQ ZZ PB SB NN D CRC, then ACK and the slave's NN.
				plain(0x31, 0x08, 0xb5, 0x04, 0x01, 0x55, 0x66, 0x00, 0x02)
				m.requestStartForSession(2, 0x03)
				tc.end(m)
				if tc.name == "cancel" || tc.name == "disconnect" {
					if m.arb.isOwner(1) {
						t.Fatal("owner was not released")
					}
				} else {
					m.arb.releaseOwnership(1)
				}
				before := mock.getStartRequests()
				// An AA inside the response is an injected symbol, not idle.
				plain(protocol.SymbolSyn)
				if got := mock.getStartRequests(); !reflect.DeepEqual(got, before) {
					t.Fatalf("injected AA admitted START: %x -> %x", before, got)
				}
				for _, b := range passive() {
					if b == protocol.SymbolSyn {
						t.Fatalf("injected AA reached observers: %x", passive())
					}
				}
				if state := m.v8.FSMInternalState(); state == telegram_fsm.StateIdle {
					t.Fatalf("classifier reset mid-telegram: %v", state)
				}
				// Rest of the response (two data bytes and CRC), ACK, then the real idle SYN.
				plain(0xa1, 0xa2, 0x77, 0x00)
				if got := mock.getStartRequests(); !reflect.DeepEqual(got, before) {
					t.Fatalf("response body admitted START: %x -> %x", before, got)
				}
				plain(protocol.SymbolSyn)
				if state := m.v8.FSMInternalState(); state != telegram_fsm.StateIdle {
					t.Fatalf("classifier after terminator=%v", state)
				}
				plain(protocol.SymbolSyn)
				if got := mock.getStartRequests(); len(got) != len(before)+1 || got[len(got)-1] != 0x03 {
					t.Fatalf("waiting bid not admitted at idle: %x", got)
				}
			})
		}
	}
}

// If the abandoned telegram never continues, idle repeats SYN: the second
// consecutive AA is genuine and releases the fence and the waiting bid.
func TestT02_LiveTelegramAbandonedByOwnerReleasesAtIdle(t *testing.T) {
	m, mock, plain, _ := t02LiveTelegramFixture(t, false, true)
	plain(0x31, 0x08, 0xb5, 0x04, 0x01, 0x55, 0x66)
	m.requestStartForSession(2, 0x03)
	m.sessions[1].handleStart(protocol.SymbolSyn)
	before := mock.getStartRequests()
	plain(protocol.SymbolSyn)
	if got := mock.getStartRequests(); !reflect.DeepEqual(got, before) {
		t.Fatalf("first AA admitted START: %x", got)
	}
	plain(protocol.SymbolSyn)
	if state := m.v8.FSMInternalState(); state != telegram_fsm.StateIdle {
		t.Fatalf("classifier=%v", state)
	}
	plain(protocol.SymbolSyn)
	if got := mock.getStartRequests(); len(got) != len(before)+1 || got[len(got)-1] != 0x03 {
		t.Fatalf("waiting bid not admitted: %x", got)
	}
}

// A grant that wrote nothing has no telegram of its own to protect, even when
// stray bytes follow: the first SYN after its release recovers immediately.
func TestT02_UnusedGrantReleaseRecoversAtFirstSYN(t *testing.T) {
	for _, stray := range [][]byte{nil, {0x08}, {0x08, 0xb5, 0x04}} {
		m, mock, plain, _ := t02LiveTelegramFixture(t, false, false)
		plain(stray...)
		m.requestStartForSession(2, 0x03)
		m.sessions[1].handleStart(protocol.SymbolSyn)
		before := mock.getStartRequests()
		plain(protocol.SymbolSyn)
		plain(protocol.SymbolSyn)
		if got := mock.getStartRequests(); len(got) != len(before)+1 || got[len(got)-1] != 0x03 {
			t.Fatalf("stray=%x: waiting bid not admitted: %x", stray, got)
		}
	}
}

// An accepted SEND is what marks the owner's telegram as its own.
func TestT02_AcceptedSendMarksOwnerWrite(t *testing.T) {
	m, _, _, _ := t02LiveTelegramFixture(t, false, false)
	done := make(chan struct{})
	go func() { m.sessions[1].handleSend(0x08); close(done) }()
	select {
	case req := <-m.activeSendCh:
		req.result <- nil
	case <-time.After(time.Second):
		t.Fatal("SEND did not reach the active path")
	}
	<-done
	m.stateMu.Lock()
	wrote := m.phase.ownerWrote
	m.stateMu.Unlock()
	if !wrote {
		t.Fatal("accepted SEND did not mark the owner's write")
	}
}
