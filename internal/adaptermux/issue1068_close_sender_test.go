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
	for _, retire := range []bool{false, true} {
		name := "grant without close"
		if retire {
			name = "close retires grant"
		}
		t.Run(name, func(t *testing.T) {
			mux, mock, _, cleanup := newP3TestMux(t)
			defer cleanup()
			_, closeExternal := newP3TestSession(t, mux)
			defer closeExternal()
			mux.stateMu.Lock()
			mux.lastWireActivity = time.Time{}
			mux.stateMu.Unlock()
			notify := mux.arb.requestStart(gatewaySessionID, 0x10)
			mock.setRequestStartHook(func(initiator byte) {
				mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: initiator}
			})

			// Session removal is the grant trigger. Hold it at the same
			// queued, idle-bus boundary in both cases; the positive case
			// proves this request really reaches RequestStart without Close.
			ready, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			releaseRemoval := func() { once.Do(func() { close(release) }) }
			var senders p3TestSenders
			defer senders.releaseAndWait(releaseRemoval)
			senders.start(func() {
				defer close(done)
				close(ready)
				<-release
				closeExternal()
			})
			waitIssue1068(t, ready, "pending grant boundary")
			if !mux.arb.hasPending() || len(mock.getStartRequests()) != 0 {
				t.Fatal("expected a queued START before session-removal trigger")
			}
			if retire {
				for range 2 {
					if err := mux.Close(); err != nil {
						t.Fatalf("Mux.Close = %v", err)
					}
				}
				// Checking the terminal result makes this sensitive to removal
				// of Close's failAllPending retirement, even though Close also
				// detaches sessions and the upstream transport.
				select {
				case result := <-notify:
					if result.granted || result.err == nil {
						t.Fatalf("retired START result = %+v", result)
					}
				default:
					t.Fatal("Close did not retire queued START")
				}
				if mux.arb.hasPending() {
					t.Fatal("Close retained pending arbitration")
				}
			}
			releaseRemoval()
			waitIssue1068(t, done, "session-removal grant trigger")
			starts := mock.getStartRequests()
			if retire {
				if len(starts) != 0 {
					t.Fatalf("production close started retired arbitration: %v", starts)
				}
			} else {
				if len(starts) != 1 || starts[0] != 0x10 {
					t.Fatalf("idle pending START did not proceed: %v", starts)
				}
				select {
				case result := <-notify:
					if !result.granted || result.err != nil || result.initiator != 0x10 {
						t.Fatalf("positive START result = %+v", result)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("positive START did not grant")
				}
			}
		})
	}
}

func TestIssue1068_FixtureCleanupJoinsMuxSenderOnAbort(t *testing.T) {
	for _, abort := range []bool{false, true} {
		name := "return"
		if abort {
			name = "early abort"
		}
		t.Run(name, func(t *testing.T) {
			mux, mock, _, cleanup := newP3TestMux(t)
			entered, release, sent := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			releaseHook := func() { once.Do(func() { close(release) }) }
			defer cleanup()
			defer releaseHook()
			mux.stateMu.Lock()
			mux.lastWireActivity = time.Time{}
			mux.stateMu.Unlock()
			mux.arb.requestStart(gatewaySessionID, 0x10)
			mock.setRequestStartHook(func(initiator byte) {
				close(entered)
				<-release
				mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: initiator}
				close(sent)
			})
			// readLoop owns this sender, so fixture cleanup must join it.
			mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xAA}
			waitIssue1068(t, entered, "mux-owned START hook")
			scopeDone := make(chan struct{})
			go func() {
				defer close(scopeDone)
				defer cleanup()
				if abort {
					runtime.Goexit()
				}
			}()
			waitIssue1068(t, mux.ctx.Done(), "fixture cancellation")
			mock.mu.Lock()
			closedBeforeJoin := mock.closed
			mock.mu.Unlock()
			if closedBeforeJoin {
				t.Fatal("fixture closed mock while mux-owned sender was paused")
			}
			releaseHook()
			waitIssue1068(t, scopeDone, "mux-owned sender cleanup")
			select {
			case <-sent:
			default:
				t.Fatal("fixture cleanup returned before mux sender finished")
			}
			mock.mu.Lock()
			closed := mock.closed
			mock.mu.Unlock()
			if !closed {
				t.Fatal("fixture cleanup did not close mock after joining")
			}
		})
	}
}
