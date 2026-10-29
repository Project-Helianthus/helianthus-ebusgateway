package adaptermux

import (
	"context"
	"errors"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	"io"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

func newT02Mux(t *testing.T) (*Mux, *p3MockTransport) {
	t.Helper()
	m := New(Config{Logger: log.New(io.Discard, "", 0)})
	m.ctx, m.cancel = context.WithCancel(context.Background())
	tr := newP3MockTransport()
	m.upstream = tr
	for _, id := range []uint64{1, 2, 3} {
		m.sessions[id] = &session{id: id, mux: m, sendCh: make(chan sessionFrame, 64), done: make(chan struct{})}
	}
	t.Cleanup(func() {
		m.cancel()
		m.stateMu.Lock()
		if m.pendingStart != nil && m.pendingStart.deadline != nil {
			m.pendingStart.deadline.Stop()
		}
		if m.pendingAbsorbResetTimer != nil {
			m.pendingAbsorbResetTimer.Stop()
		}
		m.stateMu.Unlock()
	})
	return m, tr
}

func t02SYN(m *Mux) { m.onLogicalReceived(protocol.SymbolSyn, false, false) }

func t02Result(t *testing.T, ch <-chan startResult, winner byte, granted bool) {
	t.Helper()
	select {
	case got := <-ch:
		if got.granted != granted || got.cancelled || got.initiator != winner || got.err != nil {
			t.Fatalf("result=%+v, want winner=%02x granted=%v", got, winner, granted)
		}
	default:
		t.Fatal("result missing at SYN boundary")
	}
}

func t02Failed(t *testing.T, m *Mux, id uint64, winner byte) {
	t.Helper()
	select {
	case frame := <-m.sessions[id].sendCh:
		if frame.kind != sessionFrameFailed || frame.payload != winner {
			t.Fatalf("loser frame=%+v, winner=%02x", frame, winner)
		}
	default:
		t.Fatal("FAILED was not queued within the SYN interval")
	}
}

func TestT02_ThreeBiddersAndRebidOrder(t *testing.T) {
	for _, order := range [][]uint64{{0, 1, 2}, {2, 0, 1}, {1, 2, 0}} {
		t.Run(string(rune('a'+order[0])), func(t *testing.T) {
			m, tr := newT02Mux(t)
			addresses := map[uint64]byte{0: 0x7f, 1: 0x13, 2: 0x71}
			channels := map[uint64]<-chan startResult{}
			m.arb.confirmOwnership(3, 0x31)
			m.busOwned = time.Now()
			m.lastWireActivity = m.busOwned
			for _, id := range order {
				channels[id] = m.requestStartForSession(id, addresses[id])
			}
			t02SYN(m)
			if got := tr.getStartRequests(); len(got) != 0 {
				t.Fatalf("busy bus STARTs=%x", got)
			}
			m.arb.releaseOwnership(3)
			if got := tr.getStartRequests(); len(got) != 0 {
				t.Fatal("release dispatched before SYN")
			}
			t02SYN(m)
			if got := tr.getStartRequests(); !reflect.DeepEqual(got, []byte{0x71}) {
				t.Fatalf("STARTs=%x", got)
			}
			t02Result(t, channels[0], 0x71, false)
			t02Result(t, channels[1], 0x71, false)
			t02Failed(t, m, 1, 0x71)
			if m.arb.hasPending() {
				t.Fatal("losing bids remained queued")
			}
			m.handleArbitrationResponse(true, 0x71)
			t02Result(t, channels[2], 0x71, true)
			m.arb.releaseOwnership(2)
			channels[0] = m.requestStartForSession(0, 0x7f)
			channels[1] = m.requestStartForSession(1, 0x13)
			if len(tr.getStartRequests()) != 1 {
				t.Fatal("rebid dispatched before SYN")
			}
			t02SYN(m)
			t02Result(t, channels[0], 0x13, false)
			m.handleArbitrationResponse(true, 0x13)
			t02Result(t, channels[1], 0x13, true)
			m.arb.releaseOwnership(1)
			channels[0] = m.requestStartForSession(0, 0x7f)
			t02SYN(m)
			m.handleArbitrationResponse(true, 0x7f)
			t02Result(t, channels[0], 0x7f, true)
			if got := tr.getStartRequests(); !reflect.DeepEqual(got, []byte{0x71, 0x13, 0x7f}) {
				t.Fatalf("rebid order=%x", got)
			}
		})
	}
}

func TestT02_PriorityFields(t *testing.T) {
	cases := []struct {
		name                      string
		gateway, external, winner byte
	}{
		{"low nibble before high", 0x13, 0x71, 0x71},
		{"low nibble swapped", 0x11, 0x73, 0x11},
		{"high nibble tie break", 0x71, 0x31, 0x31},
		{"high nibble swapped", 0x31, 0x71, 0x31},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, tr := newT02Mux(t)
			m.requestStartForSession(0, tc.gateway)
			m.requestStartForSession(1, tc.external)
			t02SYN(m)
			if got := tr.getStartRequests(); !reflect.DeepEqual(got, []byte{tc.winner}) {
				t.Fatalf("STARTs=%x want %02x", got, tc.winner)
			}
		})
	}
}

func TestT02_GatewayStarvationIsAddressPriority(t *testing.T) {
	m, tr := newT02Mux(t)
	for i := 0; i < 8; i++ {
		gateway := m.requestStartForSession(0, 0x7f)
		external := m.requestStartForSession(1, 0x31)
		t02SYN(m)
		t02Result(t, gateway, 0x31, false)
		m.handleArbitrationResponse(true, 0x31)
		t02Result(t, external, 0x31, true)
		m.arb.releaseOwnership(1)
	}
	for _, b := range tr.getStartRequests() {
		if b != 0x31 {
			t.Fatalf("lower-priority gateway won: %x", b)
		}
	}
}

func TestT02_CancelBetweenBidAndGrant(t *testing.T) {
	for _, id := range []uint64{0, 1} {
		t.Run(string(rune('a'+id)), func(t *testing.T) {
			m, tr := newT02Mux(t)
			ch := m.requestStartForSession(id, 0x31)
			t02SYN(m)
			m.cancelPendingStart(id)
			m.handleArbitrationResponse(true, 0x31)
			if got := m.ActiveTxnSnapshot().UndeliverableStarted; got != 1 {
				t.Fatalf("undeliverable=%d", got)
			}
			if _, _, owned := m.arb.owner(); owned {
				t.Fatal("cancelled winner owns bus")
			}
			m.requestStartForSession(2, 0x71)
			if len(tr.getStartRequests()) != 1 {
				t.Fatal("cancelled win regranted before SYN")
			}
			t02SYN(m)
			if len(tr.getStartRequests()) != 2 || m.deferRegrantUntilNextSyn {
				t.Fatal("next SYN did not release cancelled win")
			}
			select {
			case result := <-ch:
				if result.granted {
					t.Fatal("cancelled bid granted")
				}
			default:
				t.Fatal("cancel did not resolve bid")
			}
		})
	}
}

func TestT02_CancelBeforeSYNWithdrawsBid(t *testing.T) {
	m, tr := newT02Mux(t)
	ch := m.requestStartForSession(1, 0x31)
	m.arb.cancelStart(1)
	t02SYN(m)
	if len(tr.getStartRequests()) != 0 {
		t.Fatal("cancelled pending bid forwarded")
	}
	if result := <-ch; !result.cancelled {
		t.Fatalf("result=%+v", result)
	}
}

func TestT02_DelayedDeliverableGrantIsNeverDropped(t *testing.T) {
	m, _ := newT02Mux(t)
	ch := m.requestStartForSession(1, 0x31)
	t02SYN(m)
	time.Sleep(310 * time.Millisecond)
	m.handleArbitrationResponse(true, 0x31)
	t02Result(t, ch, 0x31, true)
	if m.ActiveTxnSnapshot().UndeliverableStarted != 0 {
		t.Fatal("deliverable grant counted as abandoned")
	}
}

func TestT02_CommitBoundaryRetiresUndeliverableWin(t *testing.T) {
	for _, boundary := range []string{"cancel", "disconnect", "closed"} {
		t.Run(boundary, func(t *testing.T) {
			m, _ := newT02Mux(t)
			m.requestStartForSession(1, 0x31)
			t02SYN(m)
			pending := m.pendingStart
			switch boundary {
			case "cancel":
				m.cancelPendingStart(1)
			case "disconnect":
				delete(m.sessions, 1)
			case "closed":
				m.sessions[1].closed.Store(true)
			}
			m.completeArbitrationGrant(pending)
			if got := m.ActiveTxnSnapshot().UndeliverableStarted; got != 1 {
				t.Fatalf("undeliverable=%d", got)
			}
			if !m.deferRegrantUntilNextSyn {
				t.Fatal("wire win was not fenced until SYN")
			}
			if _, _, owned := m.arb.owner(); owned {
				t.Fatal("undeliverable win retained an owner")
			}
			t02SYN(m)
			if m.deferRegrantUntilNextSyn {
				t.Fatal("SYN did not release abandoned win")
			}
		})
	}
}

func TestT02_LocalLoserConsumesSourceOnce(t *testing.T) {
	for _, winner := range []uint64{0, 1} {
		t.Run(string(rune('a'+winner)), func(t *testing.T) {
			m, tr := newT02Mux(t)
			loser := uint64(2)
			m.requestStartForSession(winner, 0x31)
			m.requestStartForSession(loser, 0x73)
			t02SYN(m)
			t02Failed(t, m, loser, 0x31)
			tr.readDone = m.ctx.Done()
			m.wg.Add(1)
			go m.readLoop()
			defer func() { m.cancel(); m.wg.Wait() }()
			tr.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x31}
			tr.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x08}
			select {
			case frame := <-m.sessions[loser].sendCh:
				if frame.kind != sessionFrameReceived || frame.payload != 0x08 {
					t.Fatalf("duplicate source after FAILED: %+v", frame)
				}
			case <-time.After(time.Second):
				t.Fatal("payload did not follow loser FAILED")
			}
		})
	}
}

func TestT02_GatewayLoserReturnsCollisionAndReceivesResyncSYNs(t *testing.T) {
	m, _ := newT02Mux(t)
	done := make(chan error, 1)
	go func() { done <- m.ActiveTransport().(*activeTransport).StartArbitration(0x7f) }()
	deadline := time.Now().Add(time.Second)
	for !m.arb.hasPending() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !m.arb.hasPending() {
		t.Fatal("gateway bid missing")
	}
	m.requestStartForSession(1, 0x31)
	t02SYN(m)
	select {
	case err := <-done:
		if !errors.Is(err, ebuserrors.ErrBusCollision) {
			t.Fatalf("gateway loss=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway loser was not notified")
	}
	m.handleArbitrationResponse(true, 0x31)
	t02SYN(m)
	t02SYN(m)
	for range 2 {
		select {
		case ev := <-m.activeCh:
			if ev.kind != activeEventByte || ev.b != protocol.SymbolSyn {
				t.Fatalf("resync=%+v", ev)
			}
		default:
			t.Fatal("gateway arbitration loser did not receive resync SYN")
		}
	}
}

func TestT02_BidIdentityFields(t *testing.T) {
	t.Run("session ID distinguishes equal-address bids", func(t *testing.T) {
		for _, ids := range [][2]uint64{{1, 2}, {2, 1}} {
			a := newArbitrator()
			channels := map[uint64]<-chan startResult{}
			for _, id := range ids {
				channels[id] = a.requestStart(id, 0x31)
			}
			req, ok := a.tryGrant(true)
			if !ok || req.sessionID != 1 {
				t.Fatalf("equal-address winner=%+v", req)
			}
			t02Result(t, channels[2], 0x31, false)
			req.notify <- startResult{granted: true, initiator: 0x31}
			t02Result(t, channels[1], 0x31, true)
		}
	})
	t.Run("initiator distinguishes replacement from coalesce", func(t *testing.T) {
		for _, next := range []byte{0x31, 0x71} {
			a := newArbitrator()
			old := a.requestStart(1, 0x31)
			original := a.pendingExternal[0]
			newest := a.requestStart(1, next)
			if result := <-old; !result.cancelled {
				t.Fatalf("old waiter=%+v", result)
			}
			req, ok := a.tryGrant(true)
			if !ok || req.initiator != next || req.sessionID != 1 || req.notify != newest {
				t.Fatalf("new bid=%+v", req)
			}
			if (req == original) != (next == 0x31) {
				t.Fatal("address replacement/coalesce identity incorrect")
			}
			if original.cancelled.Load() != (next != 0x31) {
				t.Fatal("replacement cancellation flag incorrect")
			}
		}
	})
}
