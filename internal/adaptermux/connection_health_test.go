package adaptermux

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestConnectionLostCallbackIgnoresIdleTimeoutAndReportsReconnectBoundary(t *testing.T) {
	mock := newP3MockTransport()
	mock.readTimeout = 2 * time.Millisecond
	mux := New(Config{
		Protocol:              "enh",
		Network:               "tcp",
		Address:               "127.0.0.1:0",
		ReadTimeout:           2 * time.Millisecond,
		BlackholeThreshold:    time.Hour,
		ReconnectInitialDelay: time.Hour,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
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
	go mux.readLoop()

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
	cancel()
	mux.wg.Wait()
}
