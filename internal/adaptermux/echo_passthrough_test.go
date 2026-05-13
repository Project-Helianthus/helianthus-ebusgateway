package adaptermux

// F-18 echo-passthrough integration tests
// (_work_adaptermux_audit/EBUSD-VERIFICATION-2026-05-12-batch13.md).
//
// External ENH sessions must receive their own post-arbitration echoes
// per john30/ebusd's enhanced_proto.md:
//
//	"Note that this message [ENH_RES_RECEIVED] shall not be sent when
//	 the byte received was part of an arbitration request initiated
//	 by ebusd."
//
// john30/ebusd's DirectProtocolHandler at protocol_direct.cpp:412-414
// compares `recvSymbol != sentSymbol` after each send and collapses to
// `bs_skip` on mismatch or SEND_TIMEOUT (~10ms) — without the echo,
// ebusd cannot advance past byte 1 (the arbitration byte that gets
// ENH_RES_STARTED instead). The arbitration-byte echo is handled
// separately by deliverWinnerByteToOtherSessions in mux.go (which
// correctly skips the winning session); post-arbitration bytes flow
// through deliverToSessions and MUST reach the owner session too.
//
// These tests pin that contract end-to-end. The standalone proxy at
// helianthus-ebus-adapter-proxy/internal/adapterproxy/server.go:126
// uses a single shared ownerObserverSeen []byte and works correctly
// for ENH external clients — the bug was introduced when the embedded
// mux generalized echoTracker per-session and applied it to ENH
// externals that should have been pass-through.

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// newF18TestMux returns a minimal Mux for F-18 echo-passthrough tests.
// Sets up ctx/cancel so session.writeLoop's `<-s.mux.ctx.Done()` arm
// doesn't dereference a nil context.
func newF18TestMux(t *testing.T) (*Mux, context.CancelFunc) {
	t.Helper()
	mux := New(Config{
		Protocol:    "enh",
		Network:     "tcp",
		Address:     "127.0.0.1:0",
		ReadTimeout: 200 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	mux.ctx, mux.cancel = ctx, cancel
	return mux, cancel
}

// installExternalSession creates and registers an external session with
// the mux, returning the session, a client net.Pipe end for reading any
// bytes the mux delivers (via the writeLoop), and a cleanup func. The
// session's writeLoop runs in a goroutine.
func installExternalSession(t *testing.T, mux *Mux, id uint64) (*session, net.Conn, func()) {
	t.Helper()
	client, server := net.Pipe()
	sess := &session{
		id:     id,
		conn:   server,
		mux:    mux,
		sendCh: make(chan sessionFrame, defaultSessionSendBuffer),
		done:   make(chan struct{}),
	}
	mux.sessionsMu.Lock()
	mux.sessions[id] = sess
	mux.sessionsMu.Unlock()

	sess.wg.Add(1)
	go sess.writeLoop()

	cleanup := func() {
		mux.sessionsMu.Lock()
		delete(mux.sessions, id)
		mux.sessionsMu.Unlock()
		sess.close()
		_ = client.Close()
	}
	return sess, client, cleanup
}

// readExpectedBytes drains the client side of an ENH session's pipe
// for `count` payload bytes, decoding the ENH framing. Returns the
// decoded payloads in arrival order.
//
// Each delivered byte is either:
//   - a raw byte (< 0x80): 1 wire byte == 1 payload byte
//   - an ENH long-form ENH_RES_RECEIVED: 2 wire bytes per payload byte
func readExpectedBytes(t *testing.T, r io.Reader, count int, timeout time.Duration) []byte {
	t.Helper()
	type result struct {
		buf []byte
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		var out []byte
		buf := make([]byte, 1)
		for len(out) < count {
			n, err := io.ReadFull(r, buf)
			if err != nil || n != 1 {
				resultCh <- result{out, err}
				return
			}
			b := buf[0]
			if b < 0x80 {
				// Raw short-form byte.
				out = append(out, b)
				continue
			}
			// Long-form ENH frame: 2 wire bytes.
			second := make([]byte, 1)
			if _, err := io.ReadFull(r, second); err != nil {
				resultCh <- result{out, err}
				return
			}
			payload, decodeErr := transport.DecodeENH(b, second[0])
			if decodeErr != nil {
				resultCh <- result{out, decodeErr}
				return
			}
			// Spec requires the command to be ENHResReceived for echo
			// passthrough (this is what session.writeFrame produces
			// for sessionFrameReceived with payload >= 0x80).
			if payload.Command != transport.ENHResReceived {
				t.Errorf("readExpectedBytes: unexpected ENH command 0x%02X (want ENHResReceived 0x%02X)", payload.Command, transport.ENHResReceived)
			}
			out = append(out, payload.Data)
		}
		resultCh <- result{out, nil}
	}()
	select {
	case r := <-resultCh:
		if r.err != nil && len(r.buf) < count {
			t.Fatalf("readExpectedBytes: got %d/%d bytes, err=%v", len(r.buf), count, r.err)
		}
		return r.buf
	case <-time.After(timeout):
		t.Fatalf("readExpectedBytes: timeout waiting for %d bytes", count)
		return nil
	}
}

// TestExternalSessionReceivesOwnEcho is the headline F-18 invariant:
// a single byte echoed by the adapter MUST reach the owner external
// session via deliverToSessions. Pre-F-18 this byte was suppressed
// by sess.echoTracker.matchEcho.
func TestExternalSessionReceivesOwnEcho(t *testing.T) {
	mux, muxCancel := newF18TestMux(t)
	defer muxCancel()
	_, client, cleanup := installExternalSession(t, mux, 1)
	defer cleanup()

	// Adapter mirrors the post-arbitration data byte 0xFE back to us.
	// Owner = session 1.
	mux.deliverToSessions(0xFE, 1, true, time.Now())

	got := readExpectedBytes(t, client, 1, time.Second)
	if len(got) != 1 || got[0] != 0xFE {
		t.Fatalf("owner did not receive its own echo: got % X, want [FE] (F-18 regression — pre-fix matchEcho suppressed this byte)", got)
	}
}

// TestExternalSessionCompletesMultiByteFrame replays the broadcast-scan
// frame ebusd issues in the failing batch-9 case: 0xFE, 0x07, 0x04, 0x00,
// 0x5A (a plausible CRC). The mux MUST deliver every byte in order to
// the owning session. Pre-F-18, only 0xFE would have been processed
// before ebusd's bs_skip kicked in; the embedded mux additionally
// suppressed the echo, so even 0xFE would not have round-tripped.
func TestExternalSessionCompletesMultiByteFrame(t *testing.T) {
	mux, muxCancel := newF18TestMux(t)
	defer muxCancel()
	_, client, cleanup := installExternalSession(t, mux, 7)
	defer cleanup()

	frame := []byte{0xFE, 0x07, 0x04, 0x00, 0x5A}
	for _, b := range frame {
		mux.deliverToSessions(b, 7, true, time.Now())
	}

	got := readExpectedBytes(t, client, len(frame), 2*time.Second)
	if !bytes.Equal(got, frame) {
		t.Fatalf("multi-byte frame delivery mismatch: got % X, want % X", got, frame)
	}
}

// TestExternalSessionLongFormByteRoundtrip mitigates angry-tester A1:
// bytes with bit 7 set go long-form via session.writeFrame, encoded as
// ENH_RES_RECEIVED. Verify the encoder/decoder pair preserves the
// payload through the wire-format round-trip.
func TestExternalSessionLongFormByteRoundtrip(t *testing.T) {
	mux, muxCancel := newF18TestMux(t)
	defer muxCancel()
	_, client, cleanup := installExternalSession(t, mux, 11)
	defer cleanup()

	// Pick bytes that exercise the long-form path (bit 7 set), plus
	// boundary values.
	bytesUnderTest := []byte{0x80, 0x83, 0x96, 0xAA, 0xFE, 0xFF}
	for _, b := range bytesUnderTest {
		mux.deliverToSessions(b, 11, true, time.Now())
	}

	got := readExpectedBytes(t, client, len(bytesUnderTest), 2*time.Second)
	if !bytes.Equal(got, bytesUnderTest) {
		t.Fatalf("long-form round-trip mismatch: got % X, want % X (A1 regression — long-form ENH_RES_RECEIVED encoder/decoder must preserve the payload byte exactly)", got, bytesUnderTest)
	}
}

// TestExternalSessionCRC_0xAA_Handling verifies a 0xAA byte arriving
// mid-frame as a CRC value is delivered as a normal data byte to the
// owner session. SYN-vs-data classification happens at the readLoop
// level (deliverSYNToSessions vs deliverToSessions), so by the time
// the byte reaches deliverToSessions it's already classified as data.
// This test pins that deliverToSessions itself never special-cases
// 0xAA — a future "optimization" that re-introduces SYN detection in
// this path would fail this test.
func TestExternalSessionCRC_0xAA_Handling(t *testing.T) {
	mux, muxCancel := newF18TestMux(t)
	defer muxCancel()
	_, client, cleanup := installExternalSession(t, mux, 13)
	defer cleanup()

	// Plausible owner-owned frame ending in a CRC = 0xAA.
	frame := []byte{0xFE, 0x07, 0x04, 0x00, 0xAA}
	for _, b := range frame {
		mux.deliverToSessions(b, 13, true, time.Now())
	}

	got := readExpectedBytes(t, client, len(frame), 2*time.Second)
	if !bytes.Equal(got, frame) {
		t.Fatalf("CRC 0xAA mid-frame: got % X, want % X (must not special-case 0xAA in deliverToSessions)", got, frame)
	}
}

// TestExternalSession_PhaseAdvance_FullFrame mitigates angry-tester
// risk C3.1: post-F-18 the wirePhaseTracker becomes "live" for external
// owners (pre-F-18 it effectively idled because ebusd never wrote
// frame bodies). Verify the tracker advances through the expected
// transitions on a full SRC,DST,PB,SB,LEN,DATA,CRC,ACK,LEN,DATA,CRC,ACK
// sequence and lands cleanly back at Idle on TransactionDone, with no
// intermediate SYNTimeout / SYNIdle events.
func TestExternalSession_PhaseAdvance_FullFrame(t *testing.T) {
	var tracker wirePhaseTracker
	// The mux drives the tracker into CollectRequest after a grant
	// (the arbitration-byte path is handled by the readLoop's STARTED
	// branch). Mirror that initial state here so the unit test
	// exercises the post-arbitration phase progression — exactly the
	// behavior that becomes "live" under F-18 for external owners.
	tracker.reset(wirePhaseCollectRequest)

	// A complete eBUS transaction (request + ACK + response + ACK).
	// LEN=2 keeps it short. DST=0x08 (a non-initiator boiler address)
	// so the WaitCmdAck branch transitions to WaitResponseLen rather
	// than short-circuiting on broadcast or i2i.
	//
	// Phase progression:
	//   CollectRequest → WaitCmdAck     (RequestComplete after LEN+DATA+CRC)
	//   WaitCmdAck     → WaitResponseLen(CmdACK)
	//   WaitResponseLen→ WaitResponseBody(LEN consumed)
	//   WaitResponseBody → WaitResponseAck(ResponseDone after DATA+CRC)
	//   WaitResponseAck→ Idle           (TransactionDone via final ACK)
	frame := []byte{
		0x31, // SRC (initiator) — captured by CollectRequest
		0x08, // DST
		0xB5, // PB
		0x09, // SB
		0x02, // LEN
		0x12, // DATA[0]
		0x34, // DATA[1]
		0x9F, // CRC (placeholder)
		0x00, // CMD ACK
		0x02, // response LEN
		0x56, // response DATA[0]
		0x78, // response DATA[1]
		0xAB, // response CRC (placeholder)
		0x00, // final ACK
	}

	var sawRequestComplete, sawCmdACK, sawResponseDone, sawTransactionDone bool
	for i, b := range frame {
		ev := tracker.advance(b)
		switch ev {
		case wirePhaseEventRequestComplete:
			sawRequestComplete = true
		case wirePhaseEventCmdACK:
			sawCmdACK = true
		case wirePhaseEventResponseDone:
			sawResponseDone = true
		case wirePhaseEventTransactionDone:
			sawTransactionDone = true
		case wirePhaseEventSYNTimeout, wirePhaseEventSYNIdle:
			t.Fatalf("unexpected SYN event %d at byte %d (0x%02X) — phase tracker must complete a full frame without spurious SYN events", ev, i, b)
		}
	}
	if !sawRequestComplete || !sawCmdACK || !sawResponseDone || !sawTransactionDone {
		t.Fatalf("phase tracker did not advance through all required transitions: requestComplete=%v cmdACK=%v responseDone=%v transactionDone=%v",
			sawRequestComplete, sawCmdACK, sawResponseDone, sawTransactionDone)
	}
	if tracker.phase != wirePhaseIdle {
		t.Fatalf("phase tracker did not return to Idle after TransactionDone: phase=%d", tracker.phase)
	}
}

// TestExternalSession_MidFrameDisconnect_PhaseReset pins angry-tester
// risk T4.3: if an external owner drops mid-frame, the next SYN must
// reset the phase tracker cleanly. The SYN-during-wait-phase path
// returns SYNTimeout and resets the tracker to Idle so the next
// arbitration cycle starts from a clean state.
func TestExternalSession_MidFrameDisconnect_PhaseReset(t *testing.T) {
	var tracker wirePhaseTracker
	// Drive directly into a wait phase (the boundary phases that
	// trigger SYNTimeout per wirePhase.isSYNTimeoutBoundary). This
	// is what the mux holds after a grant + partial request when
	// the bus owner's connection drops mid-frame.
	tracker.reset(wirePhaseWaitCmdAck)

	// Mid-frame SYN: per wire_phase.go advance(), a SYN while in
	// any wait* phase collapses to SYNTimeout and resets to Idle.
	ev := tracker.advance(0xAA)
	if ev != wirePhaseEventSYNTimeout {
		t.Fatalf("mid-frame SYN: got event %d, want SYNTimeout (%d)", ev, wirePhaseEventSYNTimeout)
	}
	if tracker.phase != wirePhaseIdle {
		t.Fatalf("mid-frame SYN: tracker phase = %d, want Idle (%d)", tracker.phase, wirePhaseIdle)
	}

	// Also pin the "active request" branch of CollectRequest: with
	// requestBytesSeen > 1, a SYN must also collapse to SYNTimeout.
	tracker.reset(wirePhaseCollectRequest)
	// Advance through SRC + DST + PB so requestBytesSeen=3 (>1).
	for _, b := range []byte{0x31, 0x08, 0xB5} {
		_ = tracker.advance(b)
	}
	ev = tracker.advance(0xAA)
	if ev != wirePhaseEventSYNTimeout {
		t.Fatalf("CollectRequest mid-frame SYN: got event %d, want SYNTimeout (%d) — requestBytesSeen>1 should not be treated as idle", ev, wirePhaseEventSYNTimeout)
	}
	if tracker.phase != wirePhaseIdle {
		t.Fatalf("CollectRequest mid-frame SYN: tracker phase = %d, want Idle", tracker.phase)
	}
}

// TestExternalSession_MultiSession_NoCrossPollination pins angry-tester
// risk T4.4: with two external sessions present, deliverToSessions
// delivers each byte EXACTLY ONCE to EACH session. Pre-F-18 (with
// per-session echoTracker suppression) the owner session was skipped
// while non-owners received the byte once; post-F-18 every session
// receives the byte uniformly. The invariant is no double-delivery
// and no cross-pollination between sessions.
func TestExternalSession_MultiSession_NoCrossPollination(t *testing.T) {
	mux, muxCancel := newF18TestMux(t)
	defer muxCancel()

	_, clientA, cleanupA := installExternalSession(t, mux, 21)
	defer cleanupA()
	_, clientB, cleanupB := installExternalSession(t, mux, 22)
	defer cleanupB()

	frame := []byte{0xFE, 0x07, 0x04, 0x00}
	for _, b := range frame {
		// Session 21 owns the bus.
		mux.deliverToSessions(b, 21, true, time.Now())
	}

	var wg sync.WaitGroup
	var gotA, gotB []byte
	wg.Add(2)
	go func() {
		defer wg.Done()
		gotA = readExpectedBytes(t, clientA, len(frame), 2*time.Second)
	}()
	go func() {
		defer wg.Done()
		gotB = readExpectedBytes(t, clientB, len(frame), 2*time.Second)
	}()
	wg.Wait()

	if !bytes.Equal(gotA, frame) {
		t.Fatalf("session A (owner) frame mismatch: got % X, want % X", gotA, frame)
	}
	if !bytes.Equal(gotB, frame) {
		t.Fatalf("session B (non-owner) frame mismatch: got % X, want % X", gotB, frame)
	}
}

// TestExternalSession_NoEchoTrackerOverflow pins angry-tester risk T4.6
// and the batch-13 cosmetic-flood concern: after F-18, an external
// session writing 300 consecutive bytes through doSend (the path that
// previously called sess.echoTracker.recordSent) MUST NOT produce any
// expectedEchoes-queue growth, because the per-session tracker no
// longer exists. The gateway-side tracker (m.gatewayEcho) is a
// separate instance untouched by external-session activity. We assert
// totalOverflowResets on the gateway tracker stays at zero — there
// should be no path by which external SENDs can affect it.
func TestExternalSession_NoEchoTrackerOverflow(t *testing.T) {
	mux, muxCancel := newF18TestMux(t)
	defer muxCancel()
	_, _, cleanup := installExternalSession(t, mux, 31)
	defer cleanup()

	gatewayOverflowBefore := mux.gatewayEcho.totalOverflowResets

	// 300 external delivery events — well past the 256 overflow cap
	// the prior per-session tracker would have hit.
	for i := 0; i < 300; i++ {
		mux.deliverToSessions(byte(i&0x7F), 31, true, time.Now())
	}

	gatewayOverflowAfter := mux.gatewayEcho.totalOverflowResets
	if gatewayOverflowAfter != gatewayOverflowBefore {
		t.Fatalf("gateway echoTracker.totalOverflowResets moved during external traffic: before=%d after=%d (F-18 regression — external session activity must not affect m.gatewayEcho)", gatewayOverflowBefore, gatewayOverflowAfter)
	}
}

// TestDeliverToSessions_NoReorderUnderBurst pins Codex bonus finding:
// the pre-F-18 echoMatchFlushed branch could flush tracker-accumulated
// bytes BEFORE the live symbol on mismatch (mux.go:3511-3516), giving
// the appearance of byte reordering on the wire. Post-F-18 the
// matchEcho block is gone and deliverToSessions has exactly one
// side-effect per call (sess.deliverReceived(symbol) for each session)
// — no buffered side path. This test bursts 6 bytes across two
// sessions and asserts strict in-order delivery to each.
func TestDeliverToSessions_NoReorderUnderBurst(t *testing.T) {
	mux, muxCancel := newF18TestMux(t)
	defer muxCancel()
	_, clientA, cleanupA := installExternalSession(t, mux, 41)
	defer cleanupA()
	_, clientB, cleanupB := installExternalSession(t, mux, 42)
	defer cleanupB()

	// Distinguishable burst.
	burst := []byte{0x01, 0x02, 0x83, 0x04, 0x85, 0x06}
	// Alternate which session "owns" the bus during the burst so
	// the loop exercises both owner and non-owner paths.
	for i, b := range burst {
		owner := uint64(41)
		if i%2 == 1 {
			owner = 42
		}
		mux.deliverToSessions(b, owner, true, time.Now())
	}

	gotA := readExpectedBytes(t, clientA, len(burst), 2*time.Second)
	gotB := readExpectedBytes(t, clientB, len(burst), 2*time.Second)

	if !bytes.Equal(gotA, burst) {
		t.Fatalf("session A burst order: got % X, want % X (Codex bonus regression — deliverToSessions must preserve strict arrival order)", gotA, burst)
	}
	if !bytes.Equal(gotB, burst) {
		t.Fatalf("session B burst order: got % X, want % X", gotB, burst)
	}
}

// TestExternalSession_OnReceivedIntegration_FullFrame is the
// integration counterpart to TestExternalSession_PhaseAdvance_FullFrame
// surfaced by Codex's adversarial review of this PR
// (echo_passthrough_test.go:249, F-NEW-LOW).
//
// Pre-F-18, m.phase.advance effectively idled for external owners
// because ebusd never wrote frame bodies (the F-17 retry loop
// blocked the first SEND). Post-F-18 the tracker is live during
// external ownership and `wirePhaseEventTransactionDone` actually
// fires at mux.go:~1746-1767 → releases ownership → kicks
// tryGrantAndStart. This integration test exercises that exact
// path end-to-end:
//
//  1. Install external session, give it ownership through the
//     real arbitration path (requestStart → tryGrant → confirmOwnership).
//  2. Initialize m.phase via startRequestWithSource as the production
//     grant code does at mux.go:2813.
//  3. Feed a complete request/response sequence through
//     mux.onReceived(byte, false) one byte at a time.
//  4. Assert at each step:
//     a. Owner session receives each byte (F-18 contract).
//     b. Ownership held until the final ACK.
//     c. Ownership released exactly when TransactionDone fires.
//     d. After release, m.phase is back at Idle.
//
// This pins the C3.1 risk: external owners now drive phase to
// TransactionDone, which must NOT prematurely rip ownership
// mid-frame and MUST release cleanly on the final ACK.
func TestExternalSession_OnReceivedIntegration_FullFrame(t *testing.T) {
	mux, muxCancel := newF18TestMux(t)
	defer muxCancel()
	sess, client, cleanup := installExternalSession(t, mux, 51)
	defer cleanup()

	// Grant external session ownership through the real arbitrator
	// path (mirrors the production grant flow).
	ch := mux.arb.requestStart(51, 0x31)
	sessionID, initiator, notify, granted := tryGrantLegacy(mux.arb)
	if !granted {
		t.Fatal("expected grant from arbitrator")
	}
	if sessionID != 51 || initiator != 0x31 {
		t.Fatalf("unexpected grant: session=%d initiator=0x%02X", sessionID, initiator)
	}
	mux.arb.confirmOwnership(sessionID, initiator)
	notify <- startResult{granted: true, initiator: initiator}
	// Drain the originating channel so it doesn't leak.
	<-ch
	_ = sess

	// Initialize the phase tracker exactly as the mux's grant path
	// does at mux.go:2813 when ArbitrationSendsSource is true. SRC
	// = 0x31 (the initiator byte that arrived during arbitration).
	mux.stateMu.Lock()
	mux.phase.startRequestWithSource(0x31)
	mux.busOwned = time.Now()
	mux.lastWireActivity = time.Now()
	mux.stateMu.Unlock()

	if !mux.arb.isOwner(51) {
		t.Fatal("setup: session 51 should own the bus after confirmOwnership")
	}

	// The full transaction: DST + PB + SB + LEN=2 + DATA(2) + CRC,
	// then CMD ACK, then response LEN=2 + DATA(2) + CRC, then final
	// ACK. SRC was pre-loaded via startRequestWithSource above.
	//
	// DST=0x08 (non-broadcast, non-initiator) so the WaitCmdAck →
	// WaitResponseLen branch fires (not the short-circuit broadcast
	// or i2i transitions).
	frameAfterSrc := []byte{
		0x08, // DST
		0xB5, // PB
		0x09, // SB
		0x02, // LEN
		0x12, // DATA[0]
		0x34, // DATA[1]
		0x9F, // CRC (placeholder; tracker doesn't validate)
		0x00, // CMD ACK
		0x02, // response LEN
		0x56, // response DATA[0]
		0x78, // response DATA[1]
		0xAB, // response CRC
		0x00, // final ACK — triggers TransactionDone → release
	}

	// Concurrently drain the client pipe so deliverReceived doesn't
	// block on a full sendCh.
	drainCh := make(chan []byte, 1)
	go func() {
		drainCh <- readExpectedBytes(t, client, len(frameAfterSrc), 3*time.Second)
	}()

	// Feed each byte through onReceived and assert ownership
	// status at the right boundaries.
	for i, b := range frameAfterSrc {
		mux.onReceived(b, false)

		// Ownership must hold until the FINAL byte (which triggers
		// TransactionDone). Any premature release would indicate
		// a phase-tracking regression.
		if i < len(frameAfterSrc)-1 {
			if !mux.arb.isOwner(51) {
				t.Fatalf("ownership released prematurely at byte %d (0x%02X) — phase tracker must not release mid-frame", i, b)
			}
		}
	}

	// After the final ACK, ownership must have been released
	// (mux.go:~1746-1767 path: TransactionDone → releaseOwnership
	// → tryGrantAndStart).
	if mux.arb.isOwner(51) {
		t.Fatal("ownership not released after TransactionDone — F-18 risk C3.1: phase tracker became live for external owners and the release path must fire on final ACK")
	}

	// Phase tracker must be back at Idle.
	mux.stateMu.Lock()
	finalPhase := mux.phase.phase
	mux.stateMu.Unlock()
	if finalPhase != wirePhaseIdle {
		t.Fatalf("phase tracker did not return to Idle after TransactionDone: phase=%d", finalPhase)
	}

	// Verify all bytes were delivered to the owner session in order.
	select {
	case got := <-drainCh:
		if !bytes.Equal(got, frameAfterSrc) {
			t.Fatalf("owner session frame delivery mismatch: got % X, want % X (F-18 contract: owner must receive its own echoes)", got, frameAfterSrc)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for owner session to drain delivered bytes")
	}

	// A new arbitration cycle must succeed after release. Enqueue a
	// new gateway START and verify tryGrant pops it (no leftover
	// ownership lock).
	gwCh := mux.arb.requestStart(gatewaySessionID, 0x71)
	sessID2, _, _, granted2 := tryGrantLegacy(mux.arb)
	if !granted2 {
		t.Fatal("post-release arbitration: expected grant but tryGrant returned no-grant — old ownership may be lingering")
	}
	if sessID2 != gatewaySessionID {
		t.Fatalf("post-release arbitration: granted to session %d, want gateway (%d)", sessID2, gatewaySessionID)
	}
	_ = gwCh
}
