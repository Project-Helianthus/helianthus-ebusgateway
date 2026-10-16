package adaptermux

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

func waitIssue1068(t *testing.T, done <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func TestIssue1068_SessionCleanupJoinsSender(t *testing.T) {
	for _, abort := range []bool{false, true} {
		name := "return"
		if abort {
			name = "early abort"
		}
		t.Run(name, func(t *testing.T) {
			mux, mock, _, cleanup := newP3TestMux(t)
			_, closeExternal := newP3TestSession(t, mux)
			mux.stateMu.Lock()
			mux.lastWireActivity = time.Time{}
			mux.stateMu.Unlock()
			mux.arb.requestStart(gatewaySessionID, 0x10)

			entered, release, sent := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			releaseHook := func() { once.Do(func() { close(release) }) }
			mock.setRequestStartHook(func(initiator byte) {
				close(entered)
				<-release
				mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: initiator}
				close(sent)
			})
			// Keep cleanup safe even if a controller assertion aborts.
			defer cleanup()
			defer closeExternal()
			defer releaseHook()
			scopeDone := make(chan struct{})
			go func() {
				defer close(scopeDone)
				defer cleanup()
				defer closeExternal()
				if abort {
					// Fatal uses Goexit too; exercise its deferred cleanup without
					// intentionally failing the parent test.
					runtime.Goexit()
				}
			}()
			waitIssue1068(t, entered, "session-removal START hook")
			mock.mu.Lock()
			closed := mock.closed
			mock.mu.Unlock()
			if closed {
				t.Fatal("fixture closed its event channel while session sender was paused")
			}
			releaseHook()
			waitIssue1068(t, scopeDone, "session and fixture cleanup")
			select {
			case <-sent:
			default:
				t.Fatal("session cleanup returned before its sender finished")
			}
		})
	}
}

func TestIssue1068_PausedTestSenderJoinedOnAbort(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	entered, release, sent := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	releaseHook := func() { once.Do(func() { close(release) }) }
	var senders p3TestSenders
	defer cleanup()
	defer senders.releaseAndWait(releaseHook)
	mock.setRequestStartHook(func(initiator byte) {
		close(entered)
		<-release
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: initiator}
		close(sent)
	})
	mux.arb.requestStart(gatewaySessionID, 0x10)
	scopeDone := make(chan struct{})
	go func() {
		defer close(scopeDone)
		defer cleanup()
		defer senders.releaseAndWait(releaseHook)
		senders.start(mux.tryGrantAndStart)
		<-entered
		runtime.Goexit()
	}()
	waitIssue1068(t, scopeDone, "aborted sender scope cleanup")
	select {
	case <-sent:
	default:
		t.Fatal("abort cleanup did not join the test-owned sender")
	}
}

func TestIssue1068_MuxCloseRetiresPendingSessionGrant(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()
	_, closeExternal := newP3TestSession(t, mux)
	defer closeExternal()
	mux.arb.requestStart(gatewaySessionID, 0x10)
	for range 2 {
		if err := mux.Close(); err != nil {
			t.Fatalf("Mux.Close = %v", err)
		}
	}
	closeExternal()
	if starts := mock.getStartRequests(); len(starts) != 0 {
		t.Fatalf("production close started pending arbitration: %v", starts)
	}
}
