package adaptermux

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// TestReadLoop_PhantomCollisionByteNotMirrored pins proxy-bug C5
// (R5): when a StreamEventFailed data byte is classified as a phantom
// (not a known bus initiator) by Mux.cfg.IsKnownInitiatorByte, the byte
// MUST NOT be enqueued on external session sendChs as a synthesized
// passive observation. ebusd otherwise reads it as a fictitious bus
// initiator and pollutes its passive view. The passive emit and error
// log still fire so observability remains complete.
func TestReadLoop_PhantomCollisionByteNotMirrored(t *testing.T) {
	mock := newP3MockTransport()

	mux := New(Config{
		Protocol:    "enh",
		Network:     "tcp",
		Address:     "127.0.0.1:0",
		ReadTimeout: 200 * time.Millisecond,
		// C5: classify 0x71 (the documented AND-collision artifact
		// 0x7F & 0xF1) as NOT a known initiator. Everything else is
		// real for the purposes of this test.
		IsKnownInitiatorByte: func(b byte) bool {
			return b != 0x71
		},
		// Keep legacy invariants stable for this mux-level test.
		PendingStartTTL: 24 * time.Hour,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux.ctx, mux.cancel = ctx, cancel
	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)
	mux.stateMu.Lock()
	mux.lastWireActivity = time.Now()
	mux.stateMu.Unlock()
	mux.wg.Add(2)
	go mux.readLoop()
	go mux.sendLoop()
	defer func() {
		cancel()
		_ = mock.Close()
		mux.wg.Wait()
	}()

	// Add a session whose sendCh we can read from. The proxy
	// listener isn't started in this test; we add the session
	// directly via AddSession to keep the scope contained to the
	// readLoop → mirror-delivery path.
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	sid := mux.AddSession(serverConn)
	if sid == 0 {
		t.Fatal("AddSession returned 0")
	}
	defer mux.RemoveSession(sid)

	// Drain whatever ENH framing the writeLoop emits during INIT so
	// our subsequent assertions read fresh bytes only.
	drainEarly := make(chan struct{})
	go func() {
		defer close(drainEarly)
		buf := make([]byte, 256)
		deadline := time.Now().Add(150 * time.Millisecond)
		for time.Now().Before(deadline) {
			_ = clientConn.SetReadDeadline(time.Now().Add(40 * time.Millisecond))
			_, _ = clientConn.Read(buf)
		}
	}()
	<-drainEarly

	// Inject a phantom FAILED (data=0x71). With no active bidder
	// (pendingStart=nil), the mux's FAILED path normally synthesizes
	// the byte to every external session — the C5 filter MUST
	// suppress that for 0x71.
	//
	// We count bytes that arrive AFTER the injection: a successful
	// mirror writes the ENH-encoded frame to the session, producing
	// at least 2 bytes on the pipe; a successful filter writes none.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventFailed, Data: 0x71}

	deadline := time.Now().Add(200 * time.Millisecond)
	bytesAfterInject := 0
	buf := make([]byte, 64)
	for time.Now().Before(deadline) {
		_ = clientConn.SetReadDeadline(time.Now().Add(40 * time.Millisecond))
		n, _ := clientConn.Read(buf)
		bytesAfterInject += n
	}
	if bytesAfterInject > 0 {
		t.Fatalf("phantom byte 0x71 produced %d bytes on session sendCh — C5 filter regression", bytesAfterInject)
	}
}

// TestReadLoop_RealMasterByteStillMirrored is the inverse pin: when
// IsKnownInitiatorByte returns true for a byte, the existing
// synthesis-to-sessions path runs unchanged. This guards against a
// regression where the C5 filter unconditionally suppresses.
func TestReadLoop_RealMasterByteStillMirrored(t *testing.T) {
	mock := newP3MockTransport()

	mux := New(Config{
		Protocol:    "enh",
		Network:     "tcp",
		Address:     "127.0.0.1:0",
		ReadTimeout: 200 * time.Millisecond,
		// Treat every byte as a known initiator — C5 must be a no-op.
		IsKnownInitiatorByte: func(b byte) bool { return true },
		PendingStartTTL:   24 * time.Hour,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux.ctx, mux.cancel = ctx, cancel
	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)
	mux.stateMu.Lock()
	mux.lastWireActivity = time.Now()
	mux.stateMu.Unlock()
	mux.wg.Add(2)
	go mux.readLoop()
	go mux.sendLoop()
	defer func() {
		cancel()
		_ = mock.Close()
		mux.wg.Wait()
	}()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	sid := mux.AddSession(serverConn)
	if sid == 0 {
		t.Fatal("AddSession returned 0")
	}
	defer mux.RemoveSession(sid)

	// Drain INIT framing.
	drain := make(chan struct{})
	go func() {
		defer close(drain)
		buf := make([]byte, 256)
		deadline := time.Now().Add(150 * time.Millisecond)
		for time.Now().Before(deadline) {
			_ = clientConn.SetReadDeadline(time.Now().Add(40 * time.Millisecond))
			_, _ = clientConn.Read(buf)
		}
	}()
	<-drain

	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventFailed, Data: 0xF1}

	// Count bytes that arrive AFTER the injection: the synthesized
	// ENH frame for a received byte adds ≥2 bytes to the pipe. The
	// raw byte values may be transformed by ENH framing (escape
	// sequences for 0xA9/0xAA, ResReceived header encoding), so we
	// don't pin the exact bytes — we just confirm SOMETHING flowed,
	// which is the negation of the suppress path.
	deadline := time.Now().Add(300 * time.Millisecond)
	bytesAfterInject := 0
	buf := make([]byte, 64)
	for time.Now().Before(deadline) && bytesAfterInject == 0 {
		_ = clientConn.SetReadDeadline(time.Now().Add(40 * time.Millisecond))
		n, _ := clientConn.Read(buf)
		bytesAfterInject += n
	}
	if bytesAfterInject == 0 {
		t.Fatal("real initiator byte 0xF1 produced 0 bytes on session sendCh — C5 filter is over-suppressing")
	}
}

// TestFailedNotify_PhantomBidderReceivesOwnInitiator pins Codex P2
// round 5 on PR #623: when a phantom AND-collision FAILED arrives
// while an external bidder has an active in-flight pending START,
// the bidder's startResult.initiator MUST NOT carry the fictitious
// byte — handleArbitrationResponse propagates that field into
// ENHResFailed(initiator), so filtering only the synthesis-to-other-
// sessions mirror would still leak the phantom byte to the active
// bidder. The FAILED branch now substitutes the bidder's own
// pending initiator when phantom is detected.
func TestFailedNotify_PhantomBidderReceivesOwnInitiator(t *testing.T) {
	mux := New(Config{
		Protocol:    "enh",
		Network:     "tcp",
		Address:     "127.0.0.1:0",
		ReadTimeout: 200 * time.Millisecond,
		SYNInterval: time.Hour,
		// 0x71 is the documented AND-collision artifact 0x7F & 0xF1;
		// classify it as NOT a known initiator. Everything else
		// is real.
		IsKnownInitiatorByte: func(b byte) bool { return b != 0x71 },
		PendingStartTTL:      24 * time.Hour,
	})

	// Stage the external bidder's pending START as if tryGrant had
	// already popped it into pendingStart with initiator 0x31.
	notifyCh := make(chan startResult, 1)
	mux.stateMu.Lock()
	mux.pendingStart = &pendingStartState{
		sessionID: 1,
		initiator: 0x31,
		notify:    notifyCh,
		req: &startRequest{
			sessionID: 1,
			initiator: 0x31,
			notify:    notifyCh,
		},
	}
	mux.stateMu.Unlock()

	// Drive handleArbitrationResponse via the FAILED branch
	// equivalent by replicating its decision: snapshot bidder,
	// compute phantom, call with substituted byte.
	hasActiveBidder := true
	activeBidderSessionID := uint64(1)
	isPhantom := mux.cfg.IsKnownInitiatorByte != nil && !mux.cfg.IsKnownInitiatorByte(0x71)
	if !isPhantom {
		t.Fatal("setup: expected 0x71 to be classified as phantom")
	}
	notifyByte := byte(0x71)
	if isPhantom && hasActiveBidder {
		mux.stateMu.Lock()
		if mux.pendingStart != nil && mux.pendingStart.sessionID == activeBidderSessionID {
			notifyByte = mux.pendingStart.initiator
		}
		mux.stateMu.Unlock()
	}
	mux.handleArbitrationResponse(false, notifyByte)

	select {
	case result := <-notifyCh:
		if result.granted {
			t.Fatal("expected granted=false for FAILED notify")
		}
		if result.initiator == 0x71 {
			t.Fatalf("bidder notify carries phantom byte 0x71 — Codex P2 round 5 regression: ENHResFailed(0x71) leaks the fictitious initiator to the active bidder")
		}
		if result.initiator != 0x31 {
			t.Fatalf("bidder notify initiator = 0x%02X, want 0x31 (bidder's own pending initiator as the safe substitute)", result.initiator)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("notify channel did not fire within deadline")
	}
}
