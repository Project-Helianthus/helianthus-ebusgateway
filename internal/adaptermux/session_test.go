package adaptermux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// readENHFrame reads a 2-byte ENH frame from conn with deadline.
func readENHFrame(t *testing.T, conn net.Conn, timeout time.Duration) [2]byte {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	var buf [2]byte
	_, err := io.ReadFull(conn, buf[:])
	if err != nil {
		t.Fatalf("readENHFrame: %v", err)
	}
	return buf
}

// --- Fix 1: START payload fidelity (session-level) ---

func TestSession_StartedCarriesInitiator(t *testing.T) {
	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	// Create a pipe to act as the session connection.
	client, server := net.Pipe()
	defer client.Close()

	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	// Send an ENH START request with initiator 0x31.
	startReq := transport.EncodeENH(transport.ENHReqStart, 0x31)
	if _, err := client.Write(startReq[:]); err != nil {
		t.Fatalf("write START: %v", err)
	}

	// The mux does not actually forward to an adapter that grants,
	// so we need to grant via the arbitrator directly. The handleStart
	// goroutine has called requestStart — wait a bit, then grant.
	time.Sleep(50 * time.Millisecond)

	// tryGrant pulls the pending request (ownership not yet set).
	sessionID, initiator, notify, granted := mux.arb.tryGrant()
	if !granted {
		t.Fatal("expected grant from arbitrator")
	}
	if initiator != 0x31 {
		t.Fatalf("tryGrant initiator = 0x%02x, want 0x31", initiator)
	}

	// Confirm ownership after adapter START success.
	mux.arb.confirmOwnership(sessionID, initiator)

	// Simulate successful adapter START.
	notify <- startResult{granted: true, initiator: initiator}

	// Read the STARTED response from the client side.
	frame := readENHFrame(t, client, 2*time.Second)
	expected := transport.EncodeENH(transport.ENHResStarted, 0x31)
	if frame != expected {
		t.Fatalf("STARTED frame = %x, want %x", frame, expected)
	}
}

func TestSession_FailedCarriesInitiator(t *testing.T) {
	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	client, server := net.Pipe()
	defer client.Close()

	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	// Send START.
	startReq := transport.EncodeENH(transport.ENHReqStart, 0x42)
	if _, err := client.Write(startReq[:]); err != nil {
		t.Fatalf("write START: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	_, initiator, notify, granted := mux.arb.tryGrant()
	if !granted {
		t.Fatal("expected grant")
	}

	// Simulate failed adapter START.
	notify <- startResult{granted: false, initiator: initiator}

	// No releaseOwnership needed — tryGrant no longer sets ownership
	// (Codex P1 #3060199707). Call is a harmless no-op.

	frame := readENHFrame(t, client, 2*time.Second)
	expected := transport.EncodeENH(transport.ENHResFailed, 0x42)
	if frame != expected {
		t.Fatalf("FAILED frame = %x, want %x", frame, expected)
	}
}

// --- Fix 2: START-cancel (0xAA) ---

func TestSession_StartCancelSYN(t *testing.T) {
	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	client, server := net.Pipe()
	defer client.Close()

	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	// First, send a normal START so there's something to cancel.
	startReq := transport.EncodeENH(transport.ENHReqStart, 0x31)
	if _, err := client.Write(startReq[:]); err != nil {
		t.Fatalf("write START: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Verify request is pending.
	if !mux.arb.hasPending() {
		t.Fatal("expected pending request after START")
	}

	// Send START with SYN (0xAA) to cancel.
	cancelReq := transport.EncodeENH(transport.ENHReqStart, protocol.SymbolSyn)
	if _, err := client.Write(cancelReq[:]); err != nil {
		t.Fatalf("write START cancel: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Pending request should be cancelled.
	if mux.arb.hasPending() {
		t.Fatal("expected no pending requests after START cancel with SYN")
	}
}

// --- Fix 3: INIT feature negotiation ---

func TestSession_InitReturnsUpstreamFeatures(t *testing.T) {
	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	// The fake adapter responds to INIT with features. The mux stored
	// requestedFeatures=0x01 after connect(). Verify session INIT reply.
	client, server := net.Pipe()
	defer client.Close()

	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	// Send INIT with features=0x03 (client requesting more features).
	initReq := transport.EncodeENH(transport.ENHReqInit, 0x03)
	if _, err := client.Write(initReq[:]); err != nil {
		t.Fatalf("write INIT: %v", err)
	}

	// Read the RESETTED response.
	frame := readENHFrame(t, client, 2*time.Second)

	// Should carry upstream features (0x01), not the client's 0x03.
	expected := transport.EncodeENH(transport.ENHResResetted, 0x01)
	if frame != expected {
		t.Fatalf("INIT reply = %x, want %x (upstream features 0x01)", frame, expected)
	}
}

func TestSession_InitEchoesWhenUpstreamUnknown(t *testing.T) {
	// Create a mux with upstreamFeatures=0 (e.g. ENS transport).
	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	// Force upstream features to 0 to simulate unknown.
	mux.upstreamFeatures.Store(0)

	client, server := net.Pipe()
	defer client.Close()

	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	// Send INIT with features=0x05.
	initReq := transport.EncodeENH(transport.ENHReqInit, 0x05)
	if _, err := client.Write(initReq[:]); err != nil {
		t.Fatalf("write INIT: %v", err)
	}

	frame := readENHFrame(t, client, 2*time.Second)

	// Should echo back client's requested features.
	expected := transport.EncodeENH(transport.ENHResResetted, 0x05)
	if frame != expected {
		t.Fatalf("INIT reply = %x, want %x (echoed client features)", frame, expected)
	}
}

// --- Fix 4: ErrorHost vs ErrorEBUS ---

func TestSession_SendWithoutOwnershipReturnsErrorHost(t *testing.T) {
	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	client, server := net.Pipe()
	defer client.Close()

	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	// Send a SEND command without owning the bus.
	sendReq := transport.EncodeENH(transport.ENHReqSend, 0x42)
	if _, err := client.Write(sendReq[:]); err != nil {
		t.Fatalf("write SEND: %v", err)
	}

	frame := readENHFrame(t, client, 2*time.Second)

	// Should be ErrorHost (0x0C), not ErrorEBUS (0x0B).
	expected := transport.EncodeENH(transport.ENHResErrorHost, 0x00)
	if frame != expected {
		t.Fatalf("SEND error frame = %x, want %x (ErrorHost)", frame, expected)
	}

	// Verify it is NOT ErrorEBUS.
	notExpected := transport.EncodeENH(transport.ENHResErrorEBUS, 0x00)
	if frame == notExpected {
		t.Fatal("SEND without ownership should return ErrorHost, not ErrorEBUS")
	}
}

func TestSession_WriteFrameErrorHost(t *testing.T) {
	// Unit test: writeFrame encodes ErrorHost correctly.
	// net.Pipe is synchronous — read concurrently to avoid blocking.
	client, server := net.Pipe()
	defer closeOrLog(t, client, "client")
	defer closeOrLog(t, server, "server")

	s := &session{
		conn:   server,
		sendCh: make(chan sessionFrame, 1),
		done:   make(chan struct{}),
	}

	// Start reader goroutine before write to unblock the pipe.
	readCh := make(chan [2]byte, 1)
	go func() {
		var buf [2]byte
		_, _ = io.ReadFull(client, buf[:])
		readCh <- buf
	}()

	frame := sessionFrame{kind: sessionFrameErrorHost}
	err := s.writeFrame(frame)
	if err != nil {
		t.Fatalf("writeFrame: %v", err)
	}

	select {
	case buf := <-readCh:
		expected := transport.EncodeENH(transport.ENHResErrorHost, 0x00)
		if buf != expected {
			t.Fatalf("ErrorHost encoding = %x, want %x", buf, expected)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout reading ErrorHost frame")
	}
}

// --- Fix 5: Close idempotency ---

func TestMux_CloseIdempotent(t *testing.T) {
	mux, _, _ := newTestMux(t)

	// First close should succeed.
	err1 := mux.Close()

	// Second close should also succeed (not panic or error differently).
	err2 := mux.Close()

	// Both should return the same result.
	if err1 != err2 {
		t.Fatalf("Close() not idempotent: first=%v, second=%v", err1, err2)
	}
}

func TestMux_CloseAfterContextCancel(t *testing.T) {
	mux, cancel, _ := newTestMux(t)

	// Cancel context first.
	cancel()
	time.Sleep(50 * time.Millisecond)

	// Close should be safe after context cancel.
	err := mux.Close()
	// The close should not panic; error is acceptable (connection may
	// already be closed).
	_ = err

	// Second close should also be safe.
	err2 := mux.Close()
	_ = err2
}

// --- writeFrame payload fidelity unit tests ---

// writeFrameWithReader is a helper that writes a frame to a net.Pipe
// session and reads the result concurrently to avoid blocking.
func writeFrameWithReader(t *testing.T, frame sessionFrame) [2]byte {
	t.Helper()
	client, server := net.Pipe()
	defer closeOrLog(t, client, "client")
	defer closeOrLog(t, server, "server")

	s := &session{conn: server, sendCh: make(chan sessionFrame, 1), done: make(chan struct{})}

	readCh := make(chan [2]byte, 1)
	go func() {
		var buf [2]byte
		_, _ = io.ReadFull(client, buf[:])
		readCh <- buf
	}()

	if err := s.writeFrame(frame); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}

	select {
	case buf := <-readCh:
		return buf
	case <-time.After(2 * time.Second):
		t.Fatal("timeout reading frame")
		return [2]byte{}
	}
}

func TestWriteFrame_StartedPayload(t *testing.T) {
	buf := writeFrameWithReader(t, sessionFrame{kind: sessionFrameStarted, payload: 0x31})
	expected := transport.EncodeENH(transport.ENHResStarted, 0x31)
	if !bytes.Equal(buf[:], expected[:]) {
		t.Fatalf("STARTED encoding = %x, want %x", buf, expected)
	}
}

func TestWriteFrame_FailedPayload(t *testing.T) {
	buf := writeFrameWithReader(t, sessionFrame{kind: sessionFrameFailed, payload: 0x42})
	expected := transport.EncodeENH(transport.ENHResFailed, 0x42)
	if !bytes.Equal(buf[:], expected[:]) {
		t.Fatalf("FAILED encoding = %x, want %x", buf, expected)
	}
}

func TestWriteFrame_ResettedPayload(t *testing.T) {
	buf := writeFrameWithReader(t, sessionFrame{kind: sessionFrameResetted, payload: 0x01})
	expected := transport.EncodeENH(transport.ENHResResetted, 0x01)
	if !bytes.Equal(buf[:], expected[:]) {
		t.Fatalf("RESETTED encoding = %x, want %x", buf, expected)
	}
}

// --- Fix 2: cancelStart for non-existent session ---

func TestArbitrator_CancelStartNoOp(t *testing.T) {
	arb := newArbitrator()

	// Cancel a session that never requested START.
	cancelled := arb.cancelStart(42)
	if cancelled {
		t.Fatal("cancelStart should return false for non-existent session")
	}
}

// --- Fix 5: Close idempotency on fresh mux (never started) ---

func TestMux_CloseWithoutStart(t *testing.T) {
	cfg := Config{
		Protocol:    "enh",
		Network:     "tcp",
		Address:     "127.0.0.1:0",
		DialTimeout: 100 * time.Millisecond,
	}
	mux := New(cfg)

	// Close without Start — should not panic.
	err := mux.Close()
	_ = err

	// Double close also safe.
	err2 := mux.Close()
	_ = err2
}

// --- P1 fix: reset/disconnect errors deliver RESETTED not FAILED ---

func TestIsResetOrDisconnectError(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errors.New("arbitration collision"), false},
		{errors.New("adapter refused"), false},
		{errors.New("adaptermux: adapter reset"), true},
		{errors.New("adaptermux: adapter disconnected"), true},
		{errors.New("adaptermux: closed"), true},
		{fmt.Errorf("during arbitration: %w", ebuserrors.ErrAdapterReset), true},
		// AM18/AM47: broad substrings no longer match — prevents false
		// positives like "budget reset" or "connection disconnect".
		{errors.New("RESET during arbitration"), false},
		{errors.New("connection disconnect"), false},
		{errors.New("budget reset complete"), false},
	}
	for _, tt := range tests {
		got := isResetOrDisconnectError(tt.err)
		label := "<nil>"
		if tt.err != nil {
			label = tt.err.Error()
		}
		if got != tt.want {
			t.Errorf("isResetOrDisconnectError(%q) = %v, want %v", label, got, tt.want)
		}
	}
}

func TestSession_StartFailedByResetDeliversResetted(t *testing.T) {
	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	client, server := net.Pipe()
	defer client.Close()

	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	// Send START.
	startReq := transport.EncodeENH(transport.ENHReqStart, 0x31)
	if _, err := client.Write(startReq[:]); err != nil {
		t.Fatalf("write START: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	_, _, notify, granted := mux.arb.tryGrant()
	if !granted {
		t.Fatal("expected grant")
	}

	// Simulate failure caused by adapter reset (not collision).
	notify <- startResult{granted: false, initiator: 0x31, err: errors.New("adaptermux: adapter reset")}

	frame := readENHFrame(t, client, 2*time.Second)

	// Should be RESETTED (not FAILED) since the cause was a reset.
	features := byte(mux.upstreamFeatures.Load())
	expected := transport.EncodeENH(transport.ENHResResetted, features)
	if frame != expected {
		t.Fatalf("reset-caused START failure: got %x, want %x (RESETTED)", frame, expected)
	}

	// Verify it is NOT FAILED.
	notExpected := transport.EncodeENH(transport.ENHResFailed, 0x31)
	if frame == notExpected {
		t.Fatal("reset-caused START failure should deliver RESETTED, not FAILED")
	}
}

func TestSession_StartFailedByCollisionDeliversFailed(t *testing.T) {
	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	client, server := net.Pipe()
	defer client.Close()

	id := mux.AddSession(server)
	defer mux.RemoveSession(id)

	// Send START.
	startReq := transport.EncodeENH(transport.ENHReqStart, 0x42)
	if _, err := client.Write(startReq[:]); err != nil {
		t.Fatalf("write START: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	_, initiator, notify, granted := mux.arb.tryGrant()
	if !granted {
		t.Fatal("expected grant")
	}

	// Simulate collision failure (not reset).
	notify <- startResult{granted: false, initiator: initiator, err: errors.New("adapter refused")}

	frame := readENHFrame(t, client, 2*time.Second)

	// Should be FAILED (collision), not RESETTED.
	expected := transport.EncodeENH(transport.ENHResFailed, 0x42)
	if frame != expected {
		t.Fatalf("collision START failure: got %x, want %x (FAILED)", frame, expected)
	}
}

// --- Fix 6: STARTED arrives before RECEIVED bytes ---

// TestSession_StartedArrivesBeforeReceivedBytes verifies the FIFO ordering
// guarantee of the session's sendCh: when STARTED is enqueued before bus
// data bytes, the client reads STARTED first. This tests the channel
// semantics directly — deliverStarted + deliverReceived into the same
// sendCh produce deterministic FIFO output.
func TestSession_StartedArrivesBeforeReceivedBytes(t *testing.T) {
	// Test the channel FIFO ordering directly at the session level.
	// This avoids goroutine scheduling non-determinism from the full
	// mux path (readLoop -> handleArbitrationResponse -> handleStart
	// goroutine) and focuses on the invariant: once STARTED is enqueued
	// before RECEIVED, the client sees them in that order.
	client, server := net.Pipe()
	defer closeOrLog(t, client, "client")
	defer closeOrLog(t, server, "server")

	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	sess := &session{
		id:          100,
		conn:        server,
		mux:         mux,
		echoTracker: newEchoTracker(),
		sendCh:      make(chan sessionFrame, defaultSessionSendBuffer),
		done:        make(chan struct{}),
	}

	// Start the writeLoop goroutine to drain sendCh -> conn.
	sess.wg.Add(1)
	go sess.writeLoop()

	// Enqueue STARTED, then three RECEIVED bytes — same order as the
	// mux would produce when STARTED is processed before bus data.
	sess.deliverStarted(0x55)
	sess.deliverReceived(0x10)
	sess.deliverReceived(0x08)
	sess.deliverReceived(0x42)

	// Read from client: STARTED (2-byte ENH frame) must come first.
	frame0 := readENHFrame(t, client, 2*time.Second)
	expectedStarted := transport.EncodeENH(transport.ENHResStarted, 0x55)
	if frame0 != expectedStarted {
		t.Fatalf("first frame = %x, want %x (STARTED) — FIFO ordering violated", frame0, expectedStarted)
	}

	// Next 3 frames: RECEIVED bytes 0x10, 0x08, 0x42 (short-form, 1 byte each).
	for i, wantByte := range []byte{0x10, 0x08, 0x42} {
		var buf [1]byte
		_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, err := io.ReadFull(client, buf[:])
		if err != nil {
			t.Fatalf("reading RECEIVED byte %d: %v", i, err)
		}
		if buf[0] != wantByte {
			t.Fatalf("RECEIVED byte %d = 0x%02X, want 0x%02X — FIFO ordering violated", i, buf[0], wantByte)
		}
	}

	close(sess.done)
	sess.wg.Wait()
}

// --- Fix 7: Reset delivery is lossless (blocking, not dropped) ---

func TestSession_ResetDeliveryIsLossless(t *testing.T) {
	// Create a session with a tiny sendCh to force buffer pressure.
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	sess := &session{
		id:          999,
		conn:        server,
		mux:         mux,
		echoTracker: newEchoTracker(),
		sendCh:      make(chan sessionFrame, 2), // tiny buffer
		done:        make(chan struct{}),
	}

	// Fill the buffer completely with received bytes.
	sess.sendCh <- sessionFrame{kind: sessionFrameReceived, payload: 0x01}
	sess.sendCh <- sessionFrame{kind: sessionFrameReceived, payload: 0x02}

	// deliverReset should block (buffer full). Run in a goroutine.
	delivered := make(chan struct{})
	go func() {
		sess.deliverReset(0x07)
		close(delivered)
	}()

	// Verify it is blocked (not dropped, not delivered immediately).
	select {
	case <-delivered:
		t.Fatal("reset was delivered immediately to a full buffer — should block (not drop)")
	case <-time.After(50 * time.Millisecond):
		// Good — blocking as expected.
	}

	// Drain one slot from the buffer.
	frame := <-sess.sendCh
	if frame.kind != sessionFrameReceived || frame.payload != 0x01 {
		t.Fatalf("expected received(0x01), got %+v", frame)
	}

	// Now the reset should unblock and be delivered.
	select {
	case <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("reset deliver did not unblock after draining one buffer slot")
	}

	// Drain remaining frames: received(0x02), then resetted(0x07).
	frame = <-sess.sendCh
	if frame.kind != sessionFrameReceived || frame.payload != 0x02 {
		t.Fatalf("expected received(0x02), got %+v", frame)
	}
	frame = <-sess.sendCh
	if frame.kind != sessionFrameResetted || frame.payload != 0x07 {
		t.Fatalf("expected resetted(0x07), got %+v", frame)
	}

	// Part 2: Verify that closing the session unblocks a blocked deliverReset.
	sess2 := &session{
		id:          998,
		conn:        server,
		mux:         mux,
		echoTracker: newEchoTracker(),
		sendCh:      make(chan sessionFrame, 1),
		done:        make(chan struct{}),
	}
	// Fill buffer.
	sess2.sendCh <- sessionFrame{kind: sessionFrameReceived, payload: 0xAA}

	delivered2 := make(chan struct{})
	go func() {
		sess2.deliverReset(0x01)
		close(delivered2)
	}()

	// Verify blocked.
	select {
	case <-delivered2:
		t.Fatal("reset was delivered to full buffer — should block")
	case <-time.After(50 * time.Millisecond):
	}

	// Close the session — should unblock deliverReset via done channel.
	close(sess2.done)

	select {
	case <-delivered2:
		// Good — unblocked by session close.
	case <-time.After(2 * time.Second):
		t.Fatal("deliverReset did not unblock after session close")
	}
}

// --- AM42: writeFrame 0x7F/0x80 boundary ---

// TestWriteFrame_BoundaryBytes verifies that writeFrame correctly
// encodes payloads at the short-form/ENH-encoded boundary (0x80).
// Payloads < 0x80 use short form (1 byte), >= 0x80 use ENH (2 bytes).
func TestWriteFrame_BoundaryBytes(t *testing.T) {
	tests := []struct {
		payload byte
		wantLen int
	}{
		{0x00, 1}, // short form: minimum
		{0x7E, 1}, // short form: max - 1
		{0x7F, 1}, // short form: last short byte
		{0x80, 2}, // ENH encoded: first long byte
		{0x81, 2}, // ENH encoded
		{0xFF, 2}, // ENH encoded: maximum byte
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("payload_0x%02X", tt.payload), func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()

			s := &session{conn: server, sendCh: make(chan sessionFrame, 1), done: make(chan struct{})}

			// AM-fix4: use io.ReadFull with deadline to avoid partial reads
			// on net.Pipe and prevent hangs on test failure.
			readCh := make(chan int, 1)
			go func() {
				_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
				buf := make([]byte, tt.wantLen)
				n, err := io.ReadFull(client, buf)
				if err != nil {
					// Signal error via negative length.
					readCh <- -1
					return
				}
				readCh <- n
			}()

			if err := s.writeFrame(sessionFrame{kind: sessionFrameReceived, payload: tt.payload}); err != nil {
				t.Fatalf("writeFrame: %v", err)
			}

			select {
			case n := <-readCh:
				if n < 0 {
					t.Fatalf("payload 0x%02X: ReadFull error", tt.payload)
				}
				if n != tt.wantLen {
					t.Errorf("payload 0x%02X: wrote %d bytes, want %d", tt.payload, n, tt.wantLen)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("timeout reading frame")
			}
		})
	}
}

// Verify Close is safe from concurrent goroutines.
func TestMux_CloseConcurrent(t *testing.T) {
	mux, _, _ := newTestMux(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = ctx

	done := make(chan struct{})
	for i := 0; i < 5; i++ {
		go func() {
			mux.Close()
			done <- struct{}{}
		}()
	}

	for i := 0; i < 5; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent Close timed out")
		}
	}
}
