package adaptermux

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestManagedConnectionLossDelegatesRecoveryAndFencesOldMux(t *testing.T) {
	reconnectTarget, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer func() {
		if closeErr := reconnectTarget.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			t.Errorf("listener Close() error = %v", closeErr)
		}
	}()

	mock := newP3MockTransport()
	mock.readTimeout = 2 * time.Millisecond
	mux := New(Config{
		Protocol:              "enh",
		Network:               "tcp",
		Address:               reconnectTarget.Addr().String(),
		DialTimeout:           20 * time.Millisecond,
		ReadTimeout:           2 * time.Millisecond,
		BlackholeThreshold:    time.Hour,
		ReconnectInitialDelay: 5 * time.Millisecond,
		ReconnectMaxDelay:     5 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		mux.wg.Wait()
	}()
	mux.ctx, mux.cancel = ctx, cancel
	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()

	lost := make(chan struct{}, 1)
	var reports atomic.Int32
	mux.SetConnectionLostCallback(func() {
		reports.Add(1)
		select {
		case lost <- struct{}{}:
		default:
		}
	})
	mux.wg.Add(1)
	readDone := make(chan struct{})
	go func() {
		mux.readLoop()
		close(readDone)
	}()

	select {
	case <-lost:
		t.Fatal("ordinary idle read timeout emitted connection loss")
	case <-time.After(25 * time.Millisecond):
	}

	if err := mock.Close(); err != nil {
		t.Fatalf("mock Close() error = %v", err)
	}
	select {
	case <-lost:
	case <-time.After(time.Second):
		t.Fatal("terminal read failure did not emit connection loss")
	}
	if got := reports.Load(); got != 1 {
		t.Fatalf("connection loss reports = %d, want 1", got)
	}

	select {
	case <-readDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("managed read loop did not retire after delegating recovery")
	}

	// A managed generation must not run the mux's legacy reconnect loop. A
	// listening endpoint makes any old-generation dial directly observable.
	tcpTarget, ok := reconnectTarget.(*net.TCPListener)
	if !ok {
		t.Fatalf("listener type = %T, want *net.TCPListener", reconnectTarget)
	}
	if err := tcpTarget.SetDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatalf("SetDeadline() error = %v", err)
	}
	conn, acceptErr := reconnectTarget.Accept()
	if acceptErr == nil {
		_ = conn.Close()
		t.Fatal("retired managed mux dialed the adapter during manager-owned recovery")
	}
	var netErr net.Error
	if !errors.As(acceptErr, &netErr) || !netErr.Timeout() {
		t.Fatalf("Accept() error = %v, want timeout proving no reconnect dial", acceptErr)
	}

	// The generation-local proxy surface remains fenced while the manager is
	// in BACKOFF: new sessions are rejected and START cannot reach upstream.
	server, client := net.Pipe()
	if id := mux.AddSession(server); id != 0 {
		t.Fatalf("AddSession() id = %d after delegated loss, want 0", id)
	}
	_ = client.Close()
	result := <-mux.requestStartForSession(41, 0x31)
	if !errors.Is(result.err, errNotConnected) {
		t.Fatalf("old-generation START error = %v, want errNotConnected", result.err)
	}
	if starts := mock.getStartRequests(); len(starts) != 0 {
		t.Fatalf("old-generation START reached upstream after loss: %v", starts)
	}

	// Existing proxy sessions are fenced too. Simulate a previously granted
	// external owner and prove its SEND cannot reach the retired transport.
	mux.arb.mu.Lock()
	mux.arb.hasOwner = true
	mux.arb.currentOwner = 41
	mux.arb.mu.Unlock()
	if err := mux.doSend(41, 0x55); !errors.Is(err, errNotConnected) {
		t.Fatalf("old-generation SEND error = %v, want errNotConnected", err)
	}
	mock.mu.Lock()
	written := append([]byte(nil), mock.writtenBytes...)
	mock.mu.Unlock()
	if len(written) != 0 {
		t.Fatalf("old-generation SEND reached upstream after loss: % X", written)
	}

	// Retirement remains normally closable; manager replacement will call the
	// same Close path after its bounded backoff.
	closeDone := make(chan error, 1)
	go func() { closeDone <- mux.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Close() did not remain bounded after managed connection loss")
	}
}
