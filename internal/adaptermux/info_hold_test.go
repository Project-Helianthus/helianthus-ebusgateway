package adaptermux

import (
	"net"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// gatingTransport blocks inside RequestStart after recording the initiator,
// so a test can observe the session before and after the call returns.
type gatingTransport struct {
	*p3MockTransport
	entered chan struct{}
	gate    chan struct{}
}

func (g *gatingTransport) RequestStart(initiator byte) error {
	g.mu.Lock()
	g.startRequests = append(g.startRequests, initiator)
	g.mu.Unlock()
	select {
	case <-g.entered:
	default:
		close(g.entered)
	}
	<-g.gate
	return nil
}

// TestInfoAnswerWaitsForTheIncludingGrant checks the session observable
// the contender uses: an INFO answer stays queued until the grant step
// whose snapshot included this START returns from RequestStart.
//
// Fields, and the production mutation each row catches:
//   - held request: absent, the answer is immediate. Parking every INFO
//     leaves this row without an answer.
//   - snapshot membership: a non-nil snapshot taken before the bid.
//     Releasing on any grant step delivers the answer here.
//   - RequestStart progress: the snapshot includes the bid and the call
//     has not returned. Releasing when the bid is selected, before
//     RequestStart returns, delivers the answer during the block.
//   - RequestStart return: the same call has returned. Never releasing
//     leaves the answer parked.
//   - request identity: another session's included bid returns from
//     RequestStart. Releasing every held session delivers this answer.
func TestInfoAnswerWaitsForTheIncludingGrant(t *testing.T) {
	t.Parallel()

	t.Run("no held request", func(t *testing.T) {
		t.Parallel()
		_, client, _, cleanup := newInfoHoldFixture(t)
		defer cleanup()
		writeENH(client, transport.ENHReqInfo, byte(transport.AdapterInfoVersion))
		msgs := readEnhUntil(t, client, time.Second)
		if got, ok := infoPayload(msgs); !ok || string(got) != "\x11\x22" {
			t.Fatalf("INFO answer %x (ok %v); an idle session must answer immediately", got, ok)
		}
	})

	t.Run("snapshot excludes the bid", func(t *testing.T) {
		t.Parallel()
		mux, client, id, cleanup := newInfoHoldFixture(t)
		defer cleanup()
		before := map[*startRequest]chan startResult{}
		queueInfoBid(t, mux, client, id, 0x31)
		mux.tryGrantAndStartForBids(before)
		if starts := mux.upstream.(*p3MockTransport).getStartRequests(); len(starts) != 0 {
			t.Fatalf("RequestStart calls %v; the excluded bid was handed to the adapter", starts)
		}
		if mux.arb.externalBid(id) == nil {
			t.Fatal("the excluded bid left the queue")
		}
		if !infoStillParked(mux, id) {
			t.Fatal("the answer was released for a grant step whose snapshot excluded the bid")
		}
		if msgs := readEnhUntil(t, client, 100*time.Millisecond); infoOrHost(msgs) {
			t.Fatalf("session saw %v while the bid stayed outside the snapshot", msgs)
		}
	})

	t.Run("included bid", func(t *testing.T) {
		t.Parallel()
		mux, client, id, cleanup := newInfoHoldFixture(t)
		defer cleanup()
		queueInfoBid(t, mux, client, id, 0x31)
		gate, entered := armGatedStart(t, mux)
		snapshot := mux.arb.snapshotBids()
		done := make(chan struct{})
		go func() {
			mux.tryGrantAndStartForBids(snapshot)
			close(done)
		}()
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("RequestStart was not entered")
		}
		if !infoStillParked(mux, id) {
			t.Fatal("the answer was released before RequestStart returned")
		}
		if msgs := readEnhUntil(t, client, 100*time.Millisecond); infoOrHost(msgs) {
			t.Fatalf("session saw %v while RequestStart was blocked", msgs)
		}
		close(gate)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("the grant step did not return")
		}
		msgs := readEnhUntil(t, client, time.Second)
		if got, ok := infoPayload(msgs); !ok || string(got) != "\x11\x22" {
			t.Fatalf("INFO answer %x (ok %v); the answer stayed parked after RequestStart returned", got, ok)
		}
		if infoStillParked(mux, id) {
			t.Fatal("the hold remained after the included RequestStart returned")
		}
		if starts := gatedStarts(mux); len(starts) != 1 || starts[0] != 0x31 {
			t.Fatalf("RequestStart calls %v; want the included initiator only", starts)
		}
	})

	t.Run("another session is handed the adapter", func(t *testing.T) {
		t.Parallel()
		mux, client, id, cleanup := newInfoHoldFixture(t)
		defer cleanup()
		winnerClient, winnerServer := net.Pipe()
		defer closeOrLog(t, winnerClient, "winner client")
		winnerID := mux.AddSession(winnerServer)
		if winnerID == 0 {
			t.Fatal("winner session was rejected")
		}
		defer mux.RemoveSession(winnerID)
		writeENH(winnerClient, transport.ENHReqStart, 0x10)
		waitExternalBid(t, mux, winnerID)
		snapshot := mux.arb.snapshotBids()

		queueInfoBid(t, mux, client, id, 0x31)
		gate, entered := armGatedStart(t, mux)
		done := make(chan struct{})
		go func() {
			mux.tryGrantAndStartForBids(snapshot)
			close(done)
		}()
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("RequestStart was not entered")
		}
		close(gate)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("the other session's grant step did not return")
		}
		if mux.arb.externalBid(id) == nil {
			t.Fatal("this session's bid was removed by the other session's grant")
		}
		if !infoStillParked(mux, id) {
			t.Fatal("this session's answer was released when another session's RequestStart returned")
		}
		if msgs := readEnhUntil(t, client, 100*time.Millisecond); infoOrHost(msgs) {
			t.Fatalf("session saw %v after another session was handed to the adapter", msgs)
		}
		if starts := gatedStarts(mux); len(starts) != 1 || starts[0] != 0x10 {
			t.Fatalf("RequestStart calls %v; want the other session's initiator 0x10", starts)
		}
	})
}

func newInfoHoldFixture(t *testing.T) (*Mux, net.Conn, uint64, func()) {
	t.Helper()
	mux, _, _, cleanup := newP3TestMux(t)
	mux.cfg.StartDeadline = time.Minute
	mux.infoCacheMu.Lock()
	mux.infoCache[transport.AdapterInfoVersion] = []byte{0x11, 0x22}
	mux.infoCacheMu.Unlock()
	client, server := net.Pipe()
	id := mux.AddSession(server)
	if id == 0 {
		closeOrLog(t, client, "rejected client")
		t.Fatal("AddSession returned zero")
	}
	return mux, client, id, func() {
		closeOrLog(t, client, "info hold client")
		cleanup()
	}
}

func queueInfoBid(t *testing.T, mux *Mux, client net.Conn, id uint64, initiator byte) {
	t.Helper()
	writeENH(client, transport.ENHReqStart, initiator)
	waitExternalBid(t, mux, id)
	writeENH(client, transport.ENHReqInfo, byte(transport.AdapterInfoVersion))
	waitParkedInfo(t, mux, id)
}

func waitExternalBid(t *testing.T, mux *Mux, id uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if mux.arb.externalBid(id) != nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("session %d did not queue a bid", id)
}

func waitParkedInfo(t *testing.T, mux *Mux, id uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if infoStillParked(mux, id) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("session %d did not park the INFO answer", id)
}

func infoStillParked(mux *Mux, id uint64) bool {
	mux.sessionsMu.Lock()
	sess := mux.sessions[id]
	mux.sessionsMu.Unlock()
	if sess == nil {
		return false
	}
	sess.infoHold.Lock()
	defer sess.infoHold.Unlock()
	return sess.heldReq != nil && len(sess.parkedInfo) > 0
}

func armGatedStart(t *testing.T, mux *Mux) (chan struct{}, <-chan struct{}) {
	t.Helper()
	mux.connMu.Lock()
	base, ok := mux.upstream.(*p3MockTransport)
	mux.connMu.Unlock()
	if !ok || base == nil {
		t.Fatal("fixture transport is not the p3 mock")
	}
	gate := make(chan struct{})
	entered := make(chan struct{})
	gated := &gatingTransport{p3MockTransport: base, entered: entered, gate: gate}
	mux.connMu.Lock()
	mux.upstream = gated
	mux.connMu.Unlock()
	return gate, entered
}

func gatedStarts(mux *Mux) []byte {
	mux.connMu.Lock()
	gated, _ := mux.upstream.(*gatingTransport)
	mux.connMu.Unlock()
	if gated == nil {
		return nil
	}
	return gated.getStartRequests()
}

func readEnhUntil(t *testing.T, conn net.Conn, wait time.Duration) []transport.ENHMessage {
	t.Helper()
	var parser transport.ENHParser
	var out []transport.ENHMessage
	buf := make([]byte, 128)
	deadline := time.Now().Add(wait)
	for {
		remain := time.Until(deadline)
		if remain <= 0 {
			return out
		}
		_ = conn.SetReadDeadline(time.Now().Add(remain))
		n, err := conn.Read(buf)
		if n > 0 {
			msgs, perr := parser.Parse(buf[:n])
			if perr != nil {
				t.Fatalf("parse session frames: %v", perr)
			}
			out = append(out, msgs...)
		}
		if err != nil {
			return out
		}
	}
}

func infoPayload(msgs []transport.ENHMessage) ([]byte, bool) {
	for i := 0; i < len(msgs); i++ {
		if msgs[i].Command != transport.ENHResInfo {
			continue
		}
		n := int(msgs[i].Data)
		if i+1+n > len(msgs) {
			return nil, false
		}
		payload := make([]byte, 0, n)
		for _, msg := range msgs[i+1 : i+1+n] {
			if msg.Command != transport.ENHResInfo {
				return nil, false
			}
			payload = append(payload, msg.Data)
		}
		return payload, true
	}
	return nil, false
}

func infoOrHost(msgs []transport.ENHMessage) bool {
	for _, msg := range msgs {
		if msg.Command == transport.ENHResInfo || msg.Command == transport.ENHResErrorHost {
			return true
		}
	}
	return false
}
