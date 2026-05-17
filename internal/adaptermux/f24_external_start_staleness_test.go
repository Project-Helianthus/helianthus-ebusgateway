package adaptermux

import (
	"context"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// f24_external_start_staleness_test.go pins the F-24 (batch-21,
// 2026-05-14) staleness guard. The fix suppresses ENH_RES_STARTED
// delivery to external sessions when the in-flight START has aged
// beyond ExternalStartStaleness, preventing ebusd from receiving a
// grant after its internal arbitration-wait timeout has already
// elapsed (which would trigger the `arbitration won in invalid state`
// silent-drop in ebusd's bus state machine).
//
// Live evidence pre-fix (2026-05-14 17:03-17:24): 31
// `arbitration won in invalid state` events / 27 min, tight-scan-08
// success rate 13-20% vs 95% target.

// TestF24_StaleExternalSTARTSuppressed_DefaultBudget pins the
// default-budget path: an external START's pendingStart.req with an
// `enqueuedAt` older than ExternalStartStaleness must result in a
// suppressed grant — log line emitted, ownership NOT confirmed,
// notify receives cancelled:true.
func TestF24_StaleExternalSTARTSuppressed_DefaultBudget(t *testing.T) {
	mock := newP3MockTransport()
	var logBuf cancelledStartedLogBuffer
	mux := newF24TestMux(t, mock, &logBuf, 100*time.Millisecond)
	defer mux.shutdown()

	const externalSID = uint64(42)
	notify := mux.injectStaleExternalPendingStart(externalSID, 0x31, 500*time.Millisecond)

	// Adapter emits matched STARTED.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x31}

	select {
	case result := <-notify:
		if result.granted {
			t.Fatal("F-24 regression: stale external STARTED was granted; should be suppressed")
		}
		if !result.cancelled {
			t.Fatalf("F-24 regression: stale STARTED result should set cancelled=true, got %+v", result)
		}
		if result.err == nil || !strings.Contains(result.err.Error(), "F-24") {
			t.Fatalf("F-24 regression: error message should reference F-24, got %v", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("F-24: notify never received result for stale STARTED")
	}

	if mux.arb.isOwner(externalSID) {
		t.Fatal("F-24 regression: arbitrator confirmed ownership for a stale external grant; should remain un-owned")
	}

	logStr := logBuf.String()
	if !strings.Contains(logStr, "suppressing stale STARTED") {
		t.Fatalf("F-24: expected suppression log line; got:\n%s", logStr)
	}
	if !strings.Contains(logStr, "(F-24)") {
		t.Fatalf("F-24: log should tag the suppression with (F-24); got:\n%s", logStr)
	}
}

// TestF24_FreshExternalSTARTGranted_BelowBudget asserts the
// HAPPY-PATH non-regression: an external START whose enqueuedAt is
// well within the budget passes the staleness guard and grants
// normally.
func TestF24_FreshExternalSTARTGranted_BelowBudget(t *testing.T) {
	mock := newP3MockTransport()
	var logBuf cancelledStartedLogBuffer
	mux := newF24TestMux(t, mock, &logBuf, 500*time.Millisecond)
	defer mux.shutdown()

	const externalSID = uint64(43)
	// Age = 50ms is well below the 500ms budget.
	notify := mux.injectStaleExternalPendingStart(externalSID, 0x31, 50*time.Millisecond)

	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x31}

	select {
	case result := <-notify:
		if !result.granted {
			t.Fatalf("F-24 over-correction: fresh external STARTED rejected; got %+v", result)
		}
		if result.cancelled {
			t.Fatal("F-24 over-correction: fresh external STARTED set cancelled=true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("F-24: notify never received result for fresh STARTED")
	}

	if !mux.arb.isOwner(externalSID) {
		t.Fatal("F-24 over-correction: arbitrator failed to confirm ownership for a fresh external grant")
	}

	if strings.Contains(logBuf.String(), "suppressing stale STARTED") {
		t.Fatalf("F-24 over-correction: suppression log emitted for fresh grant:\n%s", logBuf.String())
	}
}

// TestF24_GatewaySession0_NotSubjectToStalenessGuard pins that the
// gateway's own session (session 0) bypasses the F-24 guard
// regardless of the request's age. The gateway's send path doesn't
// fall through ebusd's bus state machine, so the failure mode F-24
// protects against doesn't apply.
func TestF24_GatewaySession0_NotSubjectToStalenessGuard(t *testing.T) {
	mock := newP3MockTransport()
	var logBuf cancelledStartedLogBuffer
	mux := newF24TestMux(t, mock, &logBuf, 100*time.Millisecond)
	defer mux.shutdown()

	// Inject a gateway pendingStart whose req.enqueuedAt is far past
	// the budget. The gateway path MUST still grant.
	notify := mux.injectStaleExternalPendingStart(gatewaySessionID, 0x71, 500*time.Millisecond)

	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x71}

	select {
	case result := <-notify:
		if !result.granted {
			t.Fatalf("F-24 over-correction: gateway STARTED was rejected; gateway path must bypass the staleness guard. got %+v", result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("F-24: gateway-path notify never received result")
	}

	if !mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("F-24 over-correction: gateway grant did not confirm ownership; the staleness guard is incorrectly gating session 0")
	}

	if strings.Contains(logBuf.String(), "suppressing stale STARTED") {
		t.Fatalf("F-24 over-correction: gateway-path STARTED triggered the staleness suppression log:\n%s", logBuf.String())
	}
}

// TestF24_StaleSuppression_DefersRegrantUntilNextSyn pins the F-24-fix
// (Codex PR #634 P1 thread "Wait for idle after suppressing stale
// STARTED", 2026-05-17 batch-23). When the F-24 staleness branch
// suppresses a stale external STARTED, the external initiator has
// already won wire arbitration but `arb.hasOwner` is never confirmed.
// A concurrent gateway (or other external) START issued before the
// abandoned external's bus slot terminates would collide with bytes
// already on the wire. The fix: set `deferRegrantUntilNextSyn=true`
// in the suppression branch; `tryGrantAndStart` rejects until the
// next SYN observation clears the flag in `onSYNLocked`.
//
// This test verifies the two invariants directly on the Mux:
//  1. Immediately after F-24 suppression, deferRegrantUntilNextSyn=true
//     and tryGrantAndStart is a no-op even with a fresh pending request.
//  2. After observing any SYN (any SYN observation marks bus-idle), the
//     flag clears and the next tryGrantAndStart proceeds normally.
func TestF24_StaleSuppression_DefersRegrantUntilNextSyn(t *testing.T) {
	mock := newP3MockTransport()
	var logBuf cancelledStartedLogBuffer
	mux := newF24TestMux(t, mock, &logBuf, 100*time.Millisecond)
	defer mux.shutdown()

	const externalSID = uint64(81)
	notify := mux.injectStaleExternalPendingStart(externalSID, 0x31, 500*time.Millisecond)

	// Trigger F-24 suppression.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x31}

	select {
	case result := <-notify:
		if result.granted {
			t.Fatal("setup: F-24 suppression branch did not fire")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("setup: notify never received result")
	}

	// Invariant 1: deferral flag set.
	mux.stateMu.Lock()
	deferred := mux.deferRegrantUntilNextSyn
	mux.stateMu.Unlock()
	if !deferred {
		t.Fatal("F-24-fix regression: deferRegrantUntilNextSyn should be true after stale-STARTED suppression — the abandoned external initiator's bus slot has not yet terminated")
	}

	// Enqueue a fresh gateway request and confirm tryGrantAndStart is
	// a no-op while the flag is set.
	gwReqCh := mux.arb.requestStart(gatewaySessionID, 0x71)
	mux.tryGrantAndStart()
	mux.stateMu.Lock()
	stillPending := mux.pendingStart == nil
	mux.stateMu.Unlock()
	if !stillPending {
		t.Fatal("F-24-fix regression: tryGrantAndStart proceeded while deferRegrantUntilNextSyn=true — gateway START would race the abandoned external initiator's bus slot")
	}
	if !strings.Contains(logBuf.String(), "F-24 stale-STARTED suppression pending bus-idle") {
		t.Fatalf("F-24-fix: expected deferral log line; got:\n%s", logBuf.String())
	}

	// Invariant 2: any SYN observation clears the flag.
	mux.stateMu.Lock()
	mux.onSYNLocked(wirePhaseEventNone, 0, false, time.Now())
	cleared := !mux.deferRegrantUntilNextSyn
	mux.stateMu.Unlock()
	if !cleared {
		t.Fatal("F-24-fix regression: SYN observation did not clear deferRegrantUntilNextSyn — gateway grant would remain deferred indefinitely after F-24 suppression")
	}

	// After clearing, a fresh tryGrantAndStart must pick up the
	// pending gateway request.
	mux.tryGrantAndStart()
	mux.stateMu.Lock()
	grantPicked := mux.pendingStart != nil && mux.pendingStart.sessionID == gatewaySessionID
	mux.stateMu.Unlock()
	if !grantPicked {
		t.Fatal("F-24-fix regression: post-SYN tryGrantAndStart failed to grant gateway request — deferral cleared but follow-up grant path is broken")
	}

	_ = gwReqCh
}

// TestF24_NegativeBudgetDisablesGuard verifies the legacy-behavior
// escape hatch — passing a negative ExternalStartStaleness disables
// the guard entirely, preserving pre-F-24 grant semantics for
// regression testing.
func TestF24_NegativeBudgetDisablesGuard(t *testing.T) {
	mock := newP3MockTransport()
	var logBuf cancelledStartedLogBuffer
	mux := newF24TestMux(t, mock, &logBuf, -1)
	defer mux.shutdown()

	const externalSID = uint64(44)
	notify := mux.injectStaleExternalPendingStart(externalSID, 0x31, 5*time.Second)

	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x31}

	select {
	case result := <-notify:
		if !result.granted {
			t.Fatalf("F-24 escape hatch: negative budget should disable the guard; STARTED was suppressed: %+v", result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("F-24: notify never received result with disabled guard")
	}

	if strings.Contains(logBuf.String(), "suppressing stale STARTED") {
		t.Fatalf("F-24 escape hatch: negative budget should not log suppression:\n%s", logBuf.String())
	}
}

// --- Test harness ---

// f24TestMux is a minimal Mux scaffold for F-24 tests. It wires up
// the arbitrator, the mock transport, readLoop/sendLoop, and a
// helper that injects a synthetic pendingStart with a controllable
// enqueuedAt offset so the staleness guard can be exercised
// deterministically without depending on real wall-clock queue
// delay.
type f24TestMux struct {
	*Mux
	cancelFn func()
	mock     *p3MockTransport
}

func newF24TestMux(t *testing.T, mock *p3MockTransport, logBuf *cancelledStartedLogBuffer, externalStartStaleness time.Duration) *f24TestMux {
	t.Helper()
	logger := log.New(logBuf, "", 0)
	mux := New(Config{
		Protocol:               "enh",
		Network:                "tcp",
		Address:                "127.0.0.1:0",
		ReadTimeout:            200 * time.Millisecond,
		StartDeadline:          5 * time.Second,
		PendingStartTTL:        24 * time.Hour,
		SYNInterval:            time.Hour,
		ExternalStartStaleness: externalStartStaleness,
		Logger:                 logger,
	})

	ctx, cancel := context.WithCancel(context.Background())
	mux.ctx, mux.cancel = ctx, cancel
	mux.connMu.Lock()
	mux.upstream = mock
	mux.conn = newCancelledStartedConnMock()
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	mux.wg.Add(2)
	go mux.readLoop()
	go mux.sendLoop()

	return &f24TestMux{
		Mux:      mux,
		cancelFn: cancel,
		mock:     mock,
	}
}

func (m *f24TestMux) shutdown() {
	m.cancelFn()
	_ = m.mock.Close()
	done := make(chan struct{})
	go func() { m.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		// best-effort drain; if this leaks the test goroutine, downstream
		// tests will surface it.
	}
}

// injectStaleExternalPendingStart synthesizes an in-flight pendingStart
// for the given session with `req.enqueuedAt = now - age`, mimicking a
// queue+arbitration delay of `age`. The returned channel is the
// session's notify channel; the test awaits the result on it after
// feeding StreamEventStarted to the mock transport.
//
// For external sessions, the session must exist in m.sessions for
// completeArbitrationGrant's liveness check to pass. We register a
// stub session for that purpose. The gateway session bypasses the
// liveness check.
func (m *f24TestMux) injectStaleExternalPendingStart(sessionID uint64, initiator byte, age time.Duration) chan startResult {
	notify := make(chan startResult, 1)
	req := &startRequest{
		sessionID:  sessionID,
		initiator:  initiator,
		notify:     notify,
		enqueuedAt: time.Now().Add(-age),
	}

	if sessionID != gatewaySessionID {
		m.sessionsMu.Lock()
		if m.sessions[sessionID] == nil {
			m.sessions[sessionID] = &session{
				id:     sessionID,
				mux:    m.Mux,
				sendCh: make(chan sessionFrame, defaultSessionSendBuffer),
				done:   make(chan struct{}),
			}
		}
		m.sessionsMu.Unlock()
	}

	m.stateMu.Lock()
	m.pendingStart = &pendingStartState{
		sessionID: sessionID,
		initiator: initiator,
		notify:    notify,
		req:       req,
	}
	m.arb.mu.Lock()
	if sessionID == gatewaySessionID {
		m.arb.pendingGateway = req
	}
	m.arb.mu.Unlock()
	m.stateMu.Unlock()

	return notify
}
