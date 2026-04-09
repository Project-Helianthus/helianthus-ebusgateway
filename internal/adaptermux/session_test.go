package adaptermux

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

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

	// tryGrant pulls the pending request and grants ownership.
	_, initiator, notify, granted := mux.arb.tryGrant()
	if !granted {
		t.Fatal("expected grant from arbitrator")
	}
	if initiator != 0x31 {
		t.Fatalf("tryGrant initiator = 0x%02x, want 0x31", initiator)
	}

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

	// Release ownership since tryGrant set it.
	mux.arb.releaseOwnership(id)

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
	defer client.Close()
	defer server.Close()

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
	defer client.Close()
	defer server.Close()

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
