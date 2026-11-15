package adaptermux

import (
	"errors"
	"io"
	"log"
	"net"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// A waiting external START bid has no age limit: only its client withdraws it.
// A bid whose client stopped waiting therefore costs one START request and one
// grant held for at most the abandon deadline, and arbitration resumes at the
// next SYN. These tests pin that bound on an idle bus with the classifier
// enforcing; no owner is granted at setup.

// staleBidAdmissionDeadline is the START deadline while a bid waits. It is
// short so that a gateway bid placed after the external bid reaches its
// admission bound quickly, which proves the external bid is older than it.
const staleBidAdmissionDeadline = 20 * time.Millisecond

type staleBidFixture struct {
	m     *Mux
	mock  *p3MockTransport
	plain func(...byte)
	event func(transport.StreamEvent)
}

// newStaleBidFixture starts the real readLoop in enforce mode over the
// stepped transport, with three external sessions and no owner. The external
// idle grace is an hour and the START deadline is staleBidAdmissionDeadline,
// both set before readLoop starts.
func newStaleBidFixture(t *testing.T, raw bool) *staleBidFixture {
	t.Helper()
	m, mock := newT02Mux(t)
	m.logger = log.New(io.Discard, "", 0)
	m.cfg.ExternalSessionSYNGrace = time.Hour
	m.cfg.StartDeadline = staleBidAdmissionDeadline
	m.v8 = v8classifier.New(v8classifier.ModeEnforce)
	tr := &t02ExitTransport{p3MockTransport: mock, steps: make(chan t02ExitStep)}
	mock.readDone = m.ctx.Done()
	m.upstream = tr
	if raw {
		m.upstream = &t02RawExitTransport{tr}
	}
	for _, id := range []uint64{1, 2, 3} {
		srv, cli := net.Pipe()
		t.Cleanup(func() { _ = srv.Close(); _ = cli.Close() })
		m.sessions[id].conn = srv
	}
	m.wg.Add(1)
	go m.readLoop()
	t.Cleanup(func() { m.cancel(); m.wg.Wait() })
	f := &staleBidFixture{m: m, mock: mock}
	f.event = func(ev transport.StreamEvent) {
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
	f.plain = func(bs ...byte) {
		t.Helper()
		for _, b := range bs {
			f.event(transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b})
		}
	}
	return f
}

// ageExternalBid queues the external bid of session id and makes it older
// than the START deadline: a gateway bid placed after it must first reach its
// admission bound. The START deadline is then raised to an hour, before the
// next item reaches readLoop, so that the selected START cannot expire while
// the test feeds its answer. The external bid is still queued and no START
// request has been written.
func (f *staleBidFixture) ageExternalBid(t *testing.T, id uint64, initiator byte) <-chan startResult {
	t.Helper()
	ch := f.m.requestStartForSession(id, initiator)
	queued := time.Now()
	gateway := f.m.requestStartForSession(gatewaySessionID, 0x7F)
	select {
	case res := <-gateway:
		if res.granted || !errors.Is(res.err, ebuserrors.ErrTimeout) {
			t.Fatalf("gateway bid result %+v, want its admission bound", res)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("gateway bid did not reach its admission bound")
	}
	if age := time.Since(queued); age < staleBidAdmissionDeadline {
		t.Fatalf("external bid is %v old, want at least %v", age, staleBidAdmissionDeadline)
	}
	f.m.cfg.StartDeadline = time.Hour
	if !staleBidQueued(f.m, id, initiator) {
		t.Fatalf("external bid of session %d was dropped while waiting", id)
	}
	if got := f.mock.getStartRequests(); len(got) != 0 {
		t.Fatalf("START written before any SYN: %x", got)
	}
	return ch
}

// staleBidQueued reports whether session id has a waiting bid for initiator.
func staleBidQueued(m *Mux, id uint64, initiator byte) bool {
	m.arb.mu.Lock()
	defer m.arb.mu.Unlock()
	for _, req := range m.arb.pendingExternal {
		if req.sessionID == id && req.initiator == initiator {
			return true
		}
	}
	return false
}

// staleBidAnyQueued reports whether session id has any waiting bid.
func staleBidAnyQueued(m *Mux, id uint64) bool {
	m.arb.mu.Lock()
	defer m.arb.mu.Unlock()
	for _, req := range m.arb.pendingExternal {
		if req.sessionID == id {
			return true
		}
	}
	return false
}

// staleBidDrainFrames empties a session's queue and returns its frames.
func staleBidDrainFrames(s *session) []sessionFrame {
	var got []sessionFrame
	for {
		select {
		case fr := <-s.sendCh:
			got = append(got, fr)
		default:
			return got
		}
	}
}

func staleBidCountReceived(frames []sessionFrame, symbol byte) int {
	n := 0
	for _, fr := range frames {
		if fr.kind == sessionFrameReceived && fr.payload == symbol {
			n++
		}
	}
	return n
}

// (a) An external bid older than the START deadline is not expired: it is
// selected at its SYN boundary and exactly one START request is written for
// it, also across the SYNs that follow while its answer is outstanding.
func TestStaleExternalBidSelectedAtSYNAfterStartDeadline(t *testing.T) {
	for _, raw := range externalGrantRecoveryViews() {
		t.Run(externalGrantRecoveryViewName(raw), func(t *testing.T) {
			f := newStaleBidFixture(t, raw)
			f.ageExternalBid(t, 2, 0x03)
			f.plain(protocol.SymbolSyn)
			if got := f.mock.getStartRequests(); len(got) != 1 || got[0] != 0x03 {
				t.Fatalf("START requests at the first SYN %x, want [03]", got)
			}
			f.plain(protocol.SymbolSyn, protocol.SymbolSyn)
			if got := f.mock.getStartRequests(); len(got) != 1 {
				t.Fatalf("START requests %x, want exactly one for the stale bid", got)
			}
		})
	}
}

// (b) The stale bid's STARTED reaches its session. No SEND follows, so the
// abandon watchdog releases the grant at its deadline; the bid of a second
// session that waited during the grant gets its START request at the next
// SYN, and SYN delivery to both sessions continues. The stale bid cost one
// START request and one grant.
func TestStaleExternalBidAbandonedGrantRecoversAtNextSYN(t *testing.T) {
	for _, raw := range externalGrantRecoveryViews() {
		t.Run(externalGrantRecoveryViewName(raw), func(t *testing.T) {
			f := newStaleBidFixture(t, raw)
			m := f.m
			f.ageExternalBid(t, 2, 0x03)
			f.plain(protocol.SymbolSyn)
			f.event(transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x03})
			if !m.arb.isOwner(2) {
				t.Fatal("the stale bid's session did not become the owner")
			}
			var started []sessionFrame
			for _, fr := range staleBidDrainFrames(m.sessions[2]) {
				if fr.kind == sessionFrameStarted {
					started = append(started, fr)
				}
			}
			if len(started) != 1 || started[0].payload != 0x03 || started[0].grantGen == 0 {
				t.Fatalf("STARTED frames for the stale bid %+v, want one for 0x03", started)
			}
			staleBidDrainFrames(m.sessions[3])

			// The session writer arms the watchdog once STARTED is written to
			// the client; the test arms it the same way and fires it.
			var fire func()
			m.externalAbandonAfter = func(d time.Duration, fn func()) {
				if d != externalAbandonDeadline {
					t.Errorf("watchdog armed for %v, want %v", d, externalAbandonDeadline)
				}
				fire = fn
			}
			m.sessions[2].grantDelivered.Store(true)
			m.armExternalAbandonWatchdog(m.sessions[2], started[0].grantGen)
			if fire == nil {
				t.Fatal("abandon watchdog was not armed")
			}

			m.requestStartForSession(3, 0x13)
			fire()
			if m.arb.isOwner(2) {
				t.Fatal("watchdog did not release the unused grant")
			}
			if got := f.mock.getStartRequests(); len(got) != 1 {
				t.Fatalf("START requests before the next SYN %x, want only the stale bid's", got)
			}

			f.plain(protocol.SymbolSyn)
			if got := f.mock.getStartRequests(); len(got) != 2 || got[0] != 0x03 || got[1] != 0x13 {
				t.Fatalf("START requests after the next SYN %x, want [03 13]", got)
			}
			for _, id := range []uint64{2, 3} {
				if n := staleBidCountReceived(staleBidDrainFrames(m.sessions[id]), protocol.SymbolSyn); n != 1 {
					t.Fatalf("session %d received %d SYN at the next boundary, want 1", id, n)
				}
			}
			f.plain(protocol.SymbolSyn)
			for _, id := range []uint64{2, 3} {
				if n := staleBidCountReceived(staleBidDrainFrames(m.sessions[id]), protocol.SymbolSyn); n != 1 {
					t.Fatalf("session %d received %d SYN at the following boundary, want 1", id, n)
				}
			}
			if got := f.mock.getStartRequests(); len(got) != 2 {
				t.Fatalf("START requests %x, want one per bid", got)
			}
		})
	}
}

// (c) A client withdraws its own waiting bid: SYN cancel, closing the session,
// or a newer request of the same session. The withdrawn bid leaves nothing
// behind and no START request is written for it; a replacing request is the
// only one of that session started. A bystander bid of session 3 (address
// 0x33, queued first) is untouched by the withdrawal of session 2 and is
// selected when nothing of session 2 outranks it.
//
// Fields of the bid identity varied: the session (a withdrawal of session 2
// must not remove the earlier bid of session 3) and the initiator (a newer
// request of session 2 at another address withdraws the old one; at the same
// address it keeps the queued bid and only moves its waiter).
func TestStaleExternalBidWithdrawnOnlyByItsClient(t *testing.T) {
	for _, tc := range []struct {
		name     string
		withdraw func(*Mux)
		// kept is the address session 2 still bids at after the withdrawal.
		kept byte
		want byte // the single START request at the next SYN
	}{
		{"cancel", func(m *Mux) { m.sessions[2].handleStart(protocol.SymbolSyn) }, 0, 0x33},
		{"close", func(m *Mux) { m.RemoveSession(2) }, 0, 0x33},
		{"newer_request_other_address", func(m *Mux) { m.requestStartForSession(2, 0x13) }, 0x13, 0x13},
		{"newer_request_same_address", func(m *Mux) { m.requestStartForSession(2, 0x03) }, 0x03, 0x03},
	} {
		for _, raw := range externalGrantRecoveryViews() {
			t.Run(tc.name+"/"+externalGrantRecoveryViewName(raw), func(t *testing.T) {
				f := newStaleBidFixture(t, raw)
				f.m.requestStartForSession(3, 0x33)
				ch := f.ageExternalBid(t, 2, 0x03)
				tc.withdraw(f.m)
				select {
				case res := <-ch:
					if !res.cancelled || res.granted {
						t.Fatalf("withdrawn waiter result %+v, want cancelled", res)
					}
				default:
					t.Fatal("withdrawn waiter was not resolved")
				}
				if tc.kept != 0x03 && staleBidQueued(f.m, 2, 0x03) {
					t.Fatal("withdrawn bid is still queued")
				}
				if tc.kept == 0 && staleBidAnyQueued(f.m, 2) {
					t.Fatal("withdrawal left a bid of the session behind")
				}
				if tc.kept != 0 && !staleBidQueued(f.m, 2, tc.kept) {
					t.Fatalf("session 2 has no bid at 0x%02X after its newer request", tc.kept)
				}
				if !staleBidQueued(f.m, 3, 0x33) {
					t.Fatal("withdrawal of session 2 removed the bid of session 3")
				}
				f.plain(protocol.SymbolSyn, protocol.SymbolSyn)
				if got := f.mock.getStartRequests(); len(got) != 1 || got[0] != tc.want {
					t.Fatalf("START requests %x, want [%02x]", got, tc.want)
				}
			})
		}
	}
}

// (d) While active arbitration is held for an out-of-step adapter, no START
// request is written for a waiting external bid, at any SYN; the bid stays
// queued for its client to withdraw.
func TestStaleExternalBidNotStartedDuringHold(t *testing.T) {
	for _, raw := range externalGrantRecoveryViews() {
		t.Run(externalGrantRecoveryViewName(raw), func(t *testing.T) {
			f := newStaleBidFixture(t, raw)
			if !adapterStepForceHold(f.m) {
				t.Fatal("active arbitration was not held")
			}
			f.ageExternalBid(t, 2, 0x03)
			f.plain(protocol.SymbolSyn, protocol.SymbolSyn, protocol.SymbolSyn)
			if got := f.mock.getStartRequests(); len(got) != 0 {
				t.Fatalf("START written during the hold: %x", got)
			}
			if !staleBidQueued(f.m, 2, 0x03) {
				t.Fatal("waiting bid was dropped during the hold")
			}
			if n := staleBidCountReceived(staleBidDrainFrames(f.m.sessions[2]), protocol.SymbolSyn); n != 3 {
				t.Fatalf("waiting session received %d SYN during the hold, want 3", n)
			}
		})
	}
}
