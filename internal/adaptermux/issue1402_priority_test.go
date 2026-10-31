package adaptermux

import (
	"context"
	"errors"
	"fmt"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	"io"
	"log"
	"net"
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

func TestT02_FirstSENDWaitsForGrantCommit(t *testing.T) {
	m, _ := newT02Mux(t)
	sess := m.sessions[1]
	// Pause at the commit boundary after STARTED is published. The client
	// may issue SEND immediately, while completeArbitrationGrant still
	// holds stateMu and has not yet confirmed ownership.
	m.stateMu.Lock()
	locked := true
	defer func() {
		if locked {
			m.stateMu.Unlock()
		}
	}()
	if !sess.deliverStartedForGrant(0x31, 1) {
		t.Fatal("STARTED was not queued")
	}
	if frame := <-sess.sendCh; frame.kind != sessionFrameStarted {
		t.Fatalf("grant frame=%+v", frame)
	}
	done := make(chan struct{})
	go func() { sess.handleSend(0x08); close(done) }()
	select {
	case frame := <-sess.sendCh:
		t.Fatalf("first SEND rejected before ownership commit: %+v", frame)
	case <-time.After(30 * time.Millisecond):
	}
	m.arb.confirmOwnership(1, 0x31)
	m.stateMu.Unlock()
	locked = false
	select {
	case req := <-m.activeSendCh:
		if req.sessionID != 1 || req.data != 0x08 {
			t.Fatalf("first SEND=%+v", req)
		}
		req.result <- nil
	case <-time.After(time.Second):
		t.Fatal("first SEND did not reach the active path")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SEND did not complete")
	}
}

func TestT02_LocalFAILEDWaiterPreservesSplitRebid(t *testing.T) {
	m, _ := newT02Mux(t)
	sess := m.sessions[2]
	sess.handleStart(0x73)
	m.requestStartForSession(1, 0x31)
	// Retain the result so the waiter cannot run until the first half of
	// the client's re-bid has arrived. The mux normally queues FAILED
	// before notifying this waiter.
	m.arb.mu.Lock()
	var loser *startRequest
	for _, req := range m.arb.pendingExternal {
		if req.sessionID == sess.id {
			loser = req
		}
	}
	m.arb.mu.Unlock()
	if loser == nil {
		t.Fatal("loser bid missing")
	}
	resultCh := loser.notify
	loser.notify = make(chan startResult, 1)
	t02SYN(m)
	t02Failed(t, m, sess.id, 0x31)
	result := <-loser.notify

	var parser transport.ENHParser
	rebid := transport.EncodeENH(transport.ENHReqStart, 0x73)
	if !sess.parserResetNeeded.CompareAndSwap(true, false) {
		t.Fatal("FAILED did not arm its parser reset")
	}
	parser.Reset()
	if msgs, err := parser.Parse(rebid[:1]); err != nil || len(msgs) != 0 {
		t.Fatalf("split START prefix: messages=%v err=%v", msgs, err)
	}
	resultCh <- result
	sess.wg.Wait() // the delayed handleStart waiter has now finished
	if sess.parserResetNeeded.Load() {
		t.Fatal("FAILED waiter re-armed reset in the middle of a split re-bid")
	}
	msgs, err := parser.Parse(rebid[1:])
	if err != nil || len(msgs) != 1 || msgs[0].Command != transport.ENHReqStart || msgs[0].Data != 0x73 {
		t.Fatalf("split re-bid lost: messages=%v err=%v", msgs, err)
	}
	sess.handleMessage(msgs[0])
	m.handleArbitrationResponse(true, 0x31)
	m.arb.releaseOwnership(1)
	t02SYN(m)
	m.handleArbitrationResponse(true, 0x73)
	sess.wg.Wait()
	if !m.arb.isOwner(sess.id) {
		t.Fatal("split re-bid did not win its next SYN window")
	}
}

func TestT02_LocalLoserNeedsSourceAfterPreGrantSYN(t *testing.T) {
	for _, winner := range []uint64{0, 1} {
		for _, rawOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("winner%d/rawOnly%v", winner, rawOnly), func(t *testing.T) {
				m, tr := newT02Mux(t)
				m.requestStartForSession(winner, 0x31)
				m.requestStartForSession(2, 0x73)
				t02SYN(m)
				t02Failed(t, m, 2, 0x31)
				tr.readDone = m.ctx.Done()
				m.wg.Add(1)
				go m.readLoop()
				defer func() { m.cancel(); m.wg.Wait() }()
				if rawOnly {
					// An absorbed SYN still appears in the raw session view.
					m.onRawSymbol(protocol.SymbolSyn, transport.StreamEvent{})
				} else {
					tr.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
				}
				tr.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x31}
				tr.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x08}
				for _, want := range []byte{protocol.SymbolSyn, 0x31, 0x08} {
					select {
					case frame := <-m.sessions[2].sendCh:
						if frame.kind != sessionFrameReceived || frame.payload != want {
							t.Fatalf("loser reconstruction=%+v, want RECEIVED %02x", frame, want)
						}
					case <-time.After(time.Second):
						t.Fatalf("missing RECEIVED %02x", want)
					}
				}
			})
		}
	}
}

func TestT02_GatewayAdmissionDeadlineWithoutSYN(t *testing.T) {
	m, tr := newT02Mux(t)
	m.cfg.StartDeadline = 20 * time.Millisecond
	done := make(chan error, 1)
	go func() { done <- m.ActiveTransport().(*activeTransport).StartArbitration(0x7f) }()
	select {
	case err := <-done:
		if !errors.Is(err, ebuserrors.ErrTimeout) {
			t.Fatalf("admission error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway admission blocked without SYN")
	}
	if m.arb.hasPending() {
		t.Fatal("expired gateway bid remained queued")
	}
	t02SYN(m)
	if got := tr.getStartRequests(); len(got) != 0 {
		t.Fatalf("expired bid forwarded: %x", got)
	}
	// The serialized worker can now proceed to a new bid.
	next := m.requestStartForSession(0, 0x31)
	t02SYN(m)
	m.handleArbitrationResponse(true, 0x31)
	t02Result(t, next, 0x31, true)
}

func TestT02_GatewayAdmissionDeadlinePreservesSelectedWin(t *testing.T) {
	m, _ := newT02Mux(t)
	m.cfg.StartDeadline = 40 * time.Millisecond
	ch := m.requestStartForSession(0, 0x31)
	// Exercise the admission callback after selection without scheduling races.
	t02SYN(m)
	m.expireGatewayAdmission(ch)
	m.handleArbitrationResponse(true, 0x31)
	t02Result(t, ch, 0x31, true)
}

func TestT02_LocalLoserPhysicalLossReconstruction(t *testing.T) {
	for _, winner := range []uint64{0, 1} {
		for _, started := range []bool{false, true} {
			for _, preGrantSYN := range []bool{false, true} {
				t.Run(fmt.Sprintf("winner%d/started%v/syn%v", winner, started, preGrantSYN), func(t *testing.T) {
					m, tr := newT02Mux(t)
					m.requestStartForSession(winner, 0x31)
					m.requestStartForSession(2, 0x73)
					t02SYN(m)
					t02Failed(t, m, 2, 0x31)
					tr.readDone = m.ctx.Done()
					m.wg.Add(1)
					go m.readLoop()
					defer func() { m.cancel(); m.wg.Wait() }()
					if preGrantSYN {
						tr.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
					}
					kind := transport.StreamEventFailed
					if started {
						kind = transport.StreamEventStarted
					}
					tr.eventCh <- transport.StreamEvent{Kind: kind, Data: 0x10}
					tr.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x08}
					for _, want := range []byte{protocol.SymbolSyn, 0x10, 0x08} {
						select {
						case frame := <-m.sessions[2].sendCh:
							if frame.kind != sessionFrameReceived || frame.payload != want {
								t.Fatalf("reconstruction=%+v, want RECEIVED %02x", frame, want)
							}
						case <-time.After(time.Second):
							t.Fatalf("missing RECEIVED %02x", want)
						}
					}
				})
			}
		}
	}
}

func TestT02_LocalLoserPhysicalLossAfterSelectedBidRetires(t *testing.T) {
	for _, retire := range []string{"cancel", "disconnect", "deadline"} {
		for _, boundary := range []string{"none", "logicalSYN", "rawSYN"} {
			t.Run(retire+"/"+boundary, func(t *testing.T) {
				m, tr := newT02Mux(t)
				if retire == "deadline" {
					m.cfg.StartDeadline = 100 * time.Millisecond
				}
				selected := m.requestStartForSession(1, 0x31)
				m.requestStartForSession(2, 0x73)
				t02SYN(m)
				t02Failed(t, m, 2, 0x31)
				switch retire {
				case "cancel":
					m.cancelPendingStart(1)
				case "disconnect":
					client, server := net.Pipe()
					defer func() { _ = client.Close() }()
					m.sessions[1].conn = server
					m.RemoveSession(1)
				}
				select {
				case result := <-selected:
					if result.granted || (retire == "deadline" && result.err == nil) {
						t.Fatalf("retired bid result=%+v", result)
					}
				case <-time.After(time.Second):
					t.Fatal("selected bid did not retire")
				}
				m.stateMu.Lock()
				pending, absorb := m.pendingStart, m.pendingStartAbsorb
				m.stateMu.Unlock()
				if pending != nil || absorb != 1 {
					t.Fatalf("retired bid: pending=%v absorb=%d", pending, absorb)
				}
				if boundary == "rawSYN" {
					m.onRawSymbol(protocol.SymbolSyn, transport.StreamEvent{})
				}
				tr.readDone = m.ctx.Done()
				m.wg.Add(1)
				go m.readLoop()
				defer func() { m.cancel(); m.wg.Wait() }()
				if boundary == "logicalSYN" {
					tr.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
				}
				tr.eventCh <- transport.StreamEvent{Kind: transport.StreamEventFailed, Data: 0x10}
				tr.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x08}
				// Exactly one boundary must precede the actual source, whether
				// observed on the wire or synthesized for this local loser.
				for _, want := range []byte{protocol.SymbolSyn, 0x10, 0x08} {
					select {
					case frame := <-m.sessions[2].sendCh:
						if frame.kind != sessionFrameReceived || frame.payload != want {
							t.Fatalf("reconstruction=%+v, want RECEIVED %02x", frame, want)
						}
					case <-time.After(time.Second):
						t.Fatalf("missing RECEIVED %02x", want)
					}
				}
			})
		}
	}
}

func TestT02_GatewayAdmissionExpiryDoesNotCancelReplacement(t *testing.T) {
	m, _ := newT02Mux(t)
	old := m.requestStartForSession(0, 0x7f)
	newest := m.requestStartForSession(0, 0x31)
	m.expireGatewayAdmission(old)
	if result := <-old; !result.cancelled {
		t.Fatalf("old bid=%+v", result)
	}
	select {
	case result := <-newest:
		t.Fatalf("old admission expiry resolved replacement: %+v", result)
	default:
	}
	t02SYN(m)
	m.handleArbitrationResponse(true, 0x31)
	t02Result(t, newest, 0x31, true)
}

// t02CommitTransport pauses at the real transport capability check between
// receiving STARTED and taking stateMu for the ownership commit.
type t02CommitTransport struct {
	*p3MockTransport
	beforeCommit func()
}

func (tr *t02CommitTransport) ArbitrationSendsSource() bool {
	if tr.beforeCommit != nil {
		tr.beforeCommit()
	}
	return false
}

func TestT02_CancelAfterResponseBeforeCommit(t *testing.T) {
	for _, id := range []uint64{0, 1} {
		t.Run(fmt.Sprintf("session%d", id), func(t *testing.T) {
			m, tr := newT02Mux(t)
			commitTr := &t02CommitTransport{p3MockTransport: tr}
			m.upstream = commitTr
			ch := m.requestStartForSession(id, 0x31)
			t02SYN(m)
			commitTr.beforeCommit = func() { m.cancelPendingStart(id) }
			m.handleArbitrationResponse(true, 0x31)
			commitTr.beforeCommit = nil
			if result := <-ch; result.granted || !result.cancelled {
				t.Fatalf("cancelled bid result=%+v", result)
			}
			if got := m.ActiveTxnSnapshot().UndeliverableStarted; got != 1 {
				t.Fatalf("undeliverable=%d", got)
			}
			if _, _, owned := m.arb.owner(); owned {
				t.Fatal("cancelled bid owns the bus")
			}
			m.requestStartForSession(2, 0x71)
			if len(tr.getStartRequests()) != 1 {
				t.Fatal("new bid dispatched before next SYN")
			}
			t02SYN(m)
			if got := tr.getStartRequests(); !reflect.DeepEqual(got, []byte{0x31, 0x71}) {
				t.Fatalf("next SYN STARTs=%x, want 3171", got)
			}
			m.stateMu.Lock()
			absorb := m.pendingStartAbsorb
			m.stateMu.Unlock()
			if absorb != 0 {
				t.Fatalf("already consumed STARTED left absorb=%d", absorb)
			}
		})
	}
}

func TestT02_PostSYNBidWaitsForNextWindow(t *testing.T) {
	for _, lateID := range []uint64{0, 2} {
		t.Run(fmt.Sprintf("lateSession%d", lateID), func(t *testing.T) {
			m, tr := newT02Mux(t)
			winner := m.requestStartForSession(1, 0x73)
			loser := m.requestStartForSession(3, 0x7f)
			var late <-chan startResult
			m.SetPassiveCallback(func(ev PassiveEvent) {
				if ev.Kind == PassiveEventSymbol && ev.Symbol == protocol.SymbolSyn && late == nil {
					late = m.requestStartForSession(lateID, 0x31)
				}
			})
			t02SYN(m)
			if late == nil {
				t.Fatal("SYN callback did not submit late bid")
			}
			if got := tr.getStartRequests(); !reflect.DeepEqual(got, []byte{0x73}) {
				t.Fatalf("completed window STARTs=%x, want 73", got)
			}
			t02Result(t, loser, 0x73, false)
			t02Failed(t, m, 3, 0x73)
			select {
			case result := <-late:
				t.Fatalf("late bid resolved in completed window: %+v", result)
			default:
			}
			m.handleArbitrationResponse(true, 0x73)
			t02Result(t, winner, 0x73, true)
			m.arb.releaseOwnership(1)
			t02SYN(m)
			if got := tr.getStartRequests(); !reflect.DeepEqual(got, []byte{0x73, 0x31}) {
				t.Fatalf("next window STARTs=%x, want 7331", got)
			}
			m.handleArbitrationResponse(true, 0x31)
			t02Result(t, late, 0x31, true)
		})
	}
}

func TestT02_PostSYNReplacementWaitsForNextWindow(t *testing.T) {
	for _, address := range []byte{0x31, 0x71} {
		t.Run(fmt.Sprintf("replacement%02x", address), func(t *testing.T) {
			m, tr := newT02Mux(t)
			old := m.requestStartForSession(1, 0x31)
			winner := m.requestStartForSession(2, 0x73)
			var replacement <-chan startResult
			m.SetPassiveCallback(func(ev PassiveEvent) {
				if ev.Kind == PassiveEventSymbol && ev.Symbol == protocol.SymbolSyn && replacement == nil {
					replacement = m.requestStartForSession(1, address)
				}
			})
			t02SYN(m)
			if replacement == nil {
				t.Fatal("SYN callback did not replace bid")
			}
			if result := <-old; !result.cancelled {
				t.Fatalf("replaced bid=%+v", result)
			}
			if got := tr.getStartRequests(); !reflect.DeepEqual(got, []byte{0x73}) {
				t.Fatalf("completed window STARTs=%x, want 73", got)
			}
			m.handleArbitrationResponse(true, 0x73)
			t02Result(t, winner, 0x73, true)
			m.arb.releaseOwnership(2)
			t02SYN(m)
			m.handleArbitrationResponse(true, address)
			t02Result(t, replacement, address, true)
		})
	}
}

// t02CheckStartFailure selects 0x31 over a local loser and the gateway while
// the transport holds the START, then releases it with startErr.
func t02CheckStartFailure(t *testing.T, m *Mux, release func(), startErr error, cancelSelected bool) {
	t.Helper()
	selected := m.requestStartForSession(1, 0x31)
	gateway := m.requestStartForSession(gatewaySessionID, 0x7f)
	m.requestStartForSession(2, 0x73)
	synDone := make(chan struct{})
	go func() { t02SYN(m); close(synDone) }()
	deadline := time.Now().Add(time.Second)
	for {
		m.stateMu.Lock()
		pending := m.pendingStart != nil
		m.stateMu.Unlock()
		if pending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("selection did not reach the transport")
		}
		time.Sleep(time.Millisecond)
	}
	t02Failed(t, m, 2, 0x31)
	select {
	case got := <-gateway:
		t.Fatalf("gateway resolved before the START outcome: %+v", got)
	default:
	}
	if cancelSelected {
		m.cancelPendingStart(1)
	}
	release()
	select {
	case got := <-gateway:
		if !errors.Is(got.err, startErr) || errors.Is(got.err, ebuserrors.ErrBusCollision) {
			t.Fatalf("gateway result=%+v, want the transport error", got)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway loser was not resolved")
	}
	select {
	case got := <-selected:
		if got.granted || (!cancelSelected && !errors.Is(got.err, startErr)) {
			t.Fatalf("selected result=%+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("selected bid was not failed")
	}
	<-synDone
	m.wg.Wait() // the blocking START goroutine has finished its error path
	select {
	case frame := <-m.sessions[2].sendCh:
		if frame.kind != sessionFrameReceived || frame.payload != protocol.SymbolSyn {
			t.Fatalf("loser boundary=%+v", frame)
		}
	default:
		t.Fatal("loser kept a source that never reached the wire")
	}
	m.stateMu.Lock()
	owed, losersLeft := m.collisionResyncOwed, len(m.localLosers)
	m.stateMu.Unlock()
	if owed != 0 || losersLeft != 0 {
		t.Fatalf("owed SYNs/local losers = %d/%d, want 0/0", owed, losersLeft)
	}
	t02SYN(m)
	select {
	case ev := <-m.activeCh:
		t.Fatalf("gateway received an unneeded resync event: %+v", ev)
	default:
	}
	select {
	case frame := <-m.sessions[2].sendCh:
		t.Fatalf("second loser boundary: %+v", frame)
	default:
	}
}

var t02StartFailures = []struct {
	name   string
	err    error
	cancel bool
}{
	{"writeError", errors.New("adapter write error"), false},
	{"adapterReset", fmt.Errorf("adaptermux: %w", ebuserrors.ErrAdapterReset), false},
	{"selectedCancelled", errors.New("adapter write error"), true},
}

func TestT02_RequestStartFailureUndoesLocalLosses(t *testing.T) {
	for _, tc := range t02StartFailures {
		t.Run(tc.name, func(t *testing.T) {
			m, _ := newT02Mux(t)
			tr := &failingStartTransport{p3MockTransport: newP3MockTransport(), startGate: make(chan struct{}), startErr: tc.err}
			m.upstream = tr
			t02CheckStartFailure(t, m, func() { close(tr.startGate) }, tc.err, tc.cancel)
		})
	}
}

func TestT02_StartArbitrationFailureUndoesLocalLosses(t *testing.T) {
	for _, tc := range t02StartFailures {
		t.Run(tc.name, func(t *testing.T) {
			m, _ := newT02Mux(t)
			tr := &gatedBlockingStartTransport{readCh: make(chan byte, 1), startGate: make(chan struct{}), startErr: tc.err}
			m.upstream = tr
			t02CheckStartFailure(t, m, func() { close(tr.startGate) }, tc.err, tc.cancel)
		})
	}
}

func TestT02_StartArbitrationAcceptedPublishesGatewayLoss(t *testing.T) {
	m, _ := newT02Mux(t)
	tr := &gatedBlockingStartTransport{readCh: make(chan byte, 1), startGate: make(chan struct{})}
	m.upstream = tr
	gateway := m.requestStartForSession(gatewaySessionID, 0x7f)
	m.requestStartForSession(1, 0x31)
	t02SYN(m)
	close(tr.startGate)
	select {
	case got := <-gateway:
		if got.granted || got.err != nil || got.initiator != 0x31 {
			t.Fatalf("gateway result=%+v, want loss to 0x31", got)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway loser was not resolved")
	}
	m.stateMu.Lock()
	owed := m.collisionResyncOwed
	m.stateMu.Unlock()
	if owed != protocol.CollisionRetryResyncSYNCount {
		t.Fatalf("owed SYNs=%d", owed)
	}
	m.wg.Wait()
}
