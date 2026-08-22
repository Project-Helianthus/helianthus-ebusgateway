package adaptermux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// fakeAdapterServer starts a TCP server that simulates a minimal ENH
// adapter. It accepts one connection, reads the 2-byte INIT request,
// and responds with ENHResResetted. The connection stays open until
// the returned closer is called.
func fakeAdapterServer(t *testing.T) (addr string, closer func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fakeAdapterServer: listen: %v", err)
	}

	done := make(chan struct{})
	var adapterConn net.Conn

	go func() { // goroutine: fake adapter accept loop
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		adapterConn = conn

		// Read INIT request (2 bytes).
		buf := make([]byte, 2)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return
		}

		// Respond with RESETTED carrying upstream features=0x01.
		resetted := transport.EncodeENH(transport.ENHResResetted, 0x01)
		if _, err := conn.Write(resetted[:]); err != nil {
			return
		}

		// Keep connection open until done is closed externally
		// (simulated by reading until error).
		hold := make([]byte, 1)
		for {
			_, err := conn.Read(hold)
			if err != nil {
				return
			}
		}
	}()

	return ln.Addr().String(), func() {
		closeOrLog(t, ln, "fakeAdapterServer listener")
		if adapterConn != nil {
			closeOrLog(t, adapterConn, "fakeAdapterServer conn")
		}
	}
}

// newTestMux creates a Mux connected to a fake adapter and starts it.
// Returns the mux, a cancel function, and a cleanup function.
func newTestMux(t *testing.T) (*Mux, context.CancelFunc, func()) {
	t.Helper()

	adapterAddr, adapterClose := fakeAdapterServer(t)

	cfg := Config{
		Protocol:        "enh",
		Network:         "tcp",
		Address:         adapterAddr,
		DialTimeout:     2 * time.Second,
		ReadTimeout:     200 * time.Millisecond,
		PendingStartTTL: 24 * time.Hour, // disable C3 TTL drain in legacy tests
		SYNInterval:     time.Hour,      // disable C1 idle fast path in legacy tests
	}

	mux := New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	if err := mux.Start(ctx); err != nil {
		adapterClose()
		cancel()
		t.Fatalf("mux.Start: %v", err)
	}

	// Default to contended-bus arbitration so legacy tests that
	// assume gateway-priority continue to pass under the C1 fast
	// path. Tests that explicitly want to exercise the bus-idle
	// fast path should override this in their setup.
	mux.stateMu.Lock()
	mux.lastWireActivity = time.Now()
	mux.stateMu.Unlock()

	cleanup := func() {
		cancel()
		closeOrLog(t, mux, "newTestMux mux")
		adapterClose()
	}

	return mux, cancel, cleanup
}

func TestProxyListenerAcceptsConnection(t *testing.T) {
	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	ctx := context.Background()
	pl, err := NewProxyListener(ctx, mux, "127.0.0.1:0", log.Default())
	if err != nil {
		t.Fatalf("NewProxyListener: %v", err)
	}
	defer closeOrLog(t, pl, "pl")

	// Dial the proxy listener.
	conn, err := net.DialTimeout("tcp", pl.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer closeOrLog(t, conn, "conn")

	// Give the accept loop time to register the session.
	time.Sleep(50 * time.Millisecond)

	// Verify at least one session was registered.
	mux.sessionsMu.Lock()
	count := len(mux.sessions)
	mux.sessionsMu.Unlock()

	if count < 1 {
		t.Fatalf("expected at least 1 session, got %d", count)
	}
}

func TestProxyListenerCloseStopsAccepting(t *testing.T) {
	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	ctx := context.Background()
	pl, err := NewProxyListener(ctx, mux, "127.0.0.1:0", log.Default())
	if err != nil {
		t.Fatalf("NewProxyListener: %v", err)
	}

	addr := pl.Addr().String()

	// Close the listener.
	if err := pl.Close(); err != nil {
		t.Fatalf("pl.Close: %v", err)
	}

	// New connections should be refused.
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err == nil {
		closeOrLog(t, conn, "unexpected-open conn")
		t.Fatal("expected dial to fail after listener close, but it succeeded")
	}
}

func TestProxyListenerContextCancel(t *testing.T) {
	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	pl, err := NewProxyListener(ctx, mux, "127.0.0.1:0", log.Default())
	if err != nil {
		t.Fatalf("NewProxyListener: %v", err)
	}

	addr := pl.Addr().String()

	// Cancel the context — should close the listener.
	cancel()

	// Wait for the listener to shut down.
	time.Sleep(50 * time.Millisecond)

	// New connections should be refused.
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err == nil {
		closeOrLog(t, conn, "unexpected-open conn")
		t.Fatal("expected dial to fail after context cancel, but it succeeded")
	}
}

func TestProxyListenerFatalAcceptDropsReadinessAndReportsOnce(t *testing.T) {
	mux, _, cleanup := newTestMux(t)
	defer cleanup()
	// Managed readiness is enabled only when a lifecycle owner exists.
	mux.SetConnectionLostCallback(func() {})
	fatal := make(chan error, 1)
	var reports atomic.Int32
	pl, err := NewProxyListenerWithFatalCallback(context.Background(), mux, "127.0.0.1:0", log.Default(), func(err error) {
		mux.FenceManagedConnection()
		reports.Add(1)
		fatal <- err
	})
	if err != nil {
		t.Fatalf("NewProxyListenerWithFatalCallback: %v", err)
	}
	if !pl.Ready() || !mux.ManagedConnectionReady() {
		t.Fatal("successfully bound listener was not live")
	}

	// Close the underlying listener without canceling the owner context. This
	// is the real accept-loop permanent-error path, not normal lifecycle Close.
	if err := pl.listener.Close(); err != nil {
		t.Fatalf("fatal listener close: %v", err)
	}
	select {
	case acceptErr := <-fatal:
		if !errors.Is(acceptErr, net.ErrClosed) {
			t.Fatalf("fatal callback error = %v, want net.ErrClosed", acceptErr)
		}
	case <-time.After(time.Second):
		t.Fatal("permanent Accept failure was not reported")
	}
	if pl.Ready() || mux.ManagedConnectionReady() {
		t.Fatal("fatal Accept failure retained proxy readiness")
	}
	if err := pl.Close(); err != nil {
		t.Fatalf("Close() after fatal Accept error = %v", err)
	}
	if got := reports.Load(); got != 1 {
		t.Fatalf("fatal reports = %d, want exactly 1", got)
	}
}

func TestProxyListenerNormalRetirementDoesNotReportFatal(t *testing.T) {
	for _, action := range []string{"close", "cancel"} {
		t.Run(action, func(t *testing.T) {
			mux := New(Config{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var reports atomic.Int32
			pl, err := NewProxyListenerWithFatalCallback(ctx, mux, "127.0.0.1:0", log.Default(), func(error) {
				reports.Add(1)
			})
			if err != nil {
				t.Fatalf("NewProxyListenerWithFatalCallback: %v", err)
			}
			switch action {
			case "close":
				if err := pl.Close(); err != nil {
					t.Fatalf("Close() error = %v", err)
				}
				cancel()
			case "cancel":
				cancel()
				if err := pl.Close(); err != nil {
					t.Fatalf("Close() after cancel error = %v", err)
				}
			}
			if pl.Ready() || reports.Load() != 0 {
				t.Fatalf("normal %s readiness=%v fatal_reports=%d", action, pl.Ready(), reports.Load())
			}
		})
	}
}

func TestProxyListenerBindError(t *testing.T) {
	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	ctx := context.Background()

	// Bind first listener.
	pl1, err := NewProxyListener(ctx, mux, "127.0.0.1:0", log.Default())
	if err != nil {
		t.Fatalf("first NewProxyListener: %v", err)
	}
	defer closeOrLog(t, pl1, "pl1")

	// Try to bind a second listener on the same port — should fail.
	_, err = NewProxyListener(ctx, mux, pl1.Addr().String(), log.Default())
	if err == nil {
		t.Fatal("expected bind error for double-bind, got nil")
	}
}

func TestProxyListenerNilMux(t *testing.T) {
	ctx := context.Background()
	_, err := NewProxyListener(ctx, nil, "127.0.0.1:0", log.Default())
	if err == nil {
		t.Fatal("expected error for nil mux, got nil")
	}
}

func TestProxyListenerMultipleConnections(t *testing.T) {
	mux, _, cleanup := newTestMux(t)
	defer cleanup()

	ctx := context.Background()
	pl, err := NewProxyListener(ctx, mux, "127.0.0.1:0", log.Default())
	if err != nil {
		t.Fatalf("NewProxyListener: %v", err)
	}
	defer closeOrLog(t, pl, "pl")

	// Open 3 connections.
	var conns []net.Conn
	for i := 0; i < 3; i++ {
		conn, err := net.DialTimeout("tcp", pl.Addr().String(), 2*time.Second)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		conns = append(conns, conn)
	}
	defer func() {
		for i, c := range conns {
			closeOrLog(t, c, fmt.Sprintf("conns[%d]", i))
		}
	}()

	// Give accept loop time to register all sessions.
	time.Sleep(100 * time.Millisecond)

	mux.sessionsMu.Lock()
	count := len(mux.sessions)
	mux.sessionsMu.Unlock()

	if count < 3 {
		t.Fatalf("expected at least 3 sessions, got %d", count)
	}
}

// fakeAdapterServerINITRetry starts a TCP server that returns
// features=0x00 on the first INIT attempt, then features=0x01 on the
// second. This verifies the connect() INIT retry logic.
func fakeAdapterServerINITRetry(t *testing.T) (addr string, closer func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fakeAdapterServerINITRetry: listen: %v", err)
	}

	done := make(chan struct{})
	var adapterConn net.Conn
	var attemptCount atomic.Int32

	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		adapterConn = conn

		for {
			buf := make([]byte, 2)
			if _, err := io.ReadFull(conn, buf); err != nil {
				return
			}

			attempt := attemptCount.Add(1)
			var features byte
			if attempt == 1 {
				features = 0x00 // not ready
			} else {
				features = 0x01 // ready
			}
			resp := transport.EncodeENH(transport.ENHResResetted, features)
			if _, err := conn.Write(resp[:]); err != nil {
				return
			}

			if features != 0x00 {
				break
			}
		}

		// Keep connection open.
		hold := make([]byte, 1)
		for {
			_, err := conn.Read(hold)
			if err != nil {
				return
			}
		}
	}()

	return ln.Addr().String(), func() {
		closeOrLog(t, ln, "fakeAdapterServerINITRetry listener")
		if adapterConn != nil {
			closeOrLog(t, adapterConn, "fakeAdapterServerINITRetry conn")
		}
	}
}

func TestConnect_INITRetrySucceeds(t *testing.T) {
	adapterAddr, adapterClose := fakeAdapterServerINITRetry(t)
	defer adapterClose()

	cfg := Config{
		Protocol:        "enh",
		Network:         "tcp",
		Address:         adapterAddr,
		DialTimeout:     2 * time.Second,
		ReadTimeout:     200 * time.Millisecond,
		PendingStartTTL: 24 * time.Hour, // disable C3 TTL drain in legacy tests
		SYNInterval:     time.Hour,      // disable C1 idle fast path in legacy tests
	}

	mux := New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := mux.Start(ctx); err != nil {
		t.Fatalf("mux.Start: %v (expected INIT retry to succeed on second attempt)", err)
	}
	defer func() {
		cancel()
		closeOrLog(t, mux, "INITRetry mux")
	}()

	// Verify the upstream features were stored correctly.
	features := byte(mux.upstreamFeatures.Load())
	if features != 0x01 {
		t.Fatalf("upstreamFeatures = 0x%02X, want 0x01", features)
	}
}
