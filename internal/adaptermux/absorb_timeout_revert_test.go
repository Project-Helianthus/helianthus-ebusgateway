package adaptermux

import (
	"context"
	"log"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// absorb_timeout_revert_test.go pins the contract for the v0.6.13 →
// v0.6.14 revert of the over-extended "drop-on-late-response" window
// that shipped on top of the F-22 absorb-timeout fix.
//
// (NOTE: the literal forbidden-prefix tokens used by
// TestF22_NoStaleAbsorbWindowFieldsRemain are built via string
// concatenation lower in this file so a global grep for those tokens
// over internal/adaptermux/*.go does NOT false-positive on this
// guard test. The reflection check still matches by full field name
// because the runtime value is identical to the concatenated source
// literal.)
//
// Background (live evidence, 2026-05-13 21:17:30 cycle):
//
//   absorb timeout reset reason=deadline (was waiting for 1) (F-22)
//   RequestStart(0x7F) sent for session 0           ← fresh bid
//   readLoop got StreamEventStarted data=0x7F       ← adapter GRANTS
//   readLoop dropping late-response STARTED (F-22)  ← BUG: drops grant
//   pendingStart deadline expired (AM8 ...)
//   absorb timeout reset reason=deadline ...        ← LOOPS every 10s
//
// The drop-on-late-response mechanism shipped in v0.6.13 was supposed
// to absorb late STARTED/FAILED responses from cancelled bids (the
// original reconnect-replacement Codex P1 raised on PR #632). The
// implementation over-classified: when the absorb safety-net fired and
// immediately after the bus poll loop issued a NEW RequestStart, the
// mux's readLoop dropped the FRESH grant as if it belonged to the
// cancelled bid. The gateway then AM8-timed out the new pendingStart
// 5 s later and the cycle restarted — a permanent livelock on the
// active path while the bus stayed otherwise healthy (ebusctl
// signal=acquired, other masters operating).
//
// The revert removes:
//   - the post-reset deadline field + timer on Mux
//   - readLoop drop guard for StreamEventStarted
//   - readLoop drop guard for StreamEventFailed
//   - snapshot gating in StreamEventStarted/Failed handlers
//   - the in-handleArbitrationResponse window-absorb helper
//   - the deadline.Store(windowEnd) call at armPendingStartAbsorbLocked
//
// The revert KEEPS (legitimate F-22):
//   - absorbResetTotal counter
//   - the absorb-timeout reset path (log + counter reset, NO transport
//     reconnect)
//   - the existing pendingStartAbsorb-counter-decrement path in
//     handleArbitrationResponse (handles legitimate late-responses to
//     ACTIVE pending requests — untouched by v0.6.13's over-extension)
//
// All other fixes (F-15, F-17, F-18, F-19a/c/d/e, F-23 consumer side)
// are unaffected by this revert. The helianthus-ebusgo F-23 escape
// decoder is in a separate repo and unchanged.

// forbiddenAbsorbFieldPrefixes is the list of struct-field-name prefixes
// that, if reintroduced, would indicate the v0.6.13 over-extension came
// back. Built from concatenated parts so a grep over
// internal/adaptermux/*.go does not surface this test file as a
// false-positive match. Compile-time string concatenation yields the
// same runtime value as the literal prefix.
func forbiddenAbsorbFieldPrefixes() []string {
	return []string{
		"stale" + "Absorb",
		"stale" + "Window",
	}
}

// droppedLogToken / windowAbsorbedLogToken are the log substrings the
// pre-revert build would emit. Constructed from parts for the same
// grep-hygiene reason as forbiddenAbsorbFieldPrefixes.
func droppedLogToken() string {
	return "dropping " + "stale" + "-window"
}
func windowAbsorbedLogToken() string {
	return "stale" + "-absorb window absorbed"
}

// TestF22_FreshRequestStartGrantNotDroppedAsStale exercises the
// livelock-reproducing sequence on a fresh mux: the gateway issues
// RequestStart, the adapter responds with StreamEventStarted, and the
// readLoop must NOT drop it as a late response to a cancelled bid.
// With the v0.6.13 over-extension this test would observe the
// pre-revert drop log line (see droppedLogToken below for the token
// pattern) and the pendingStart would not be granted.
func TestF22_FreshRequestStartGrantNotDroppedAsStale(t *testing.T) {
	mock := newP3MockTransport()

	var logBuf cancelledStartedLogBuffer
	logger := log.New(&logBuf, "", 0)

	mux := New(Config{
		Protocol:        "enh",
		Network:         "tcp",
		Address:         "127.0.0.1:0",
		ReadTimeout:     200 * time.Millisecond,
		StartDeadline:   200 * time.Millisecond,
		PendingStartTTL: 24 * time.Hour,
		SYNInterval:     time.Hour,
		Logger:          logger,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux.ctx, mux.cancel = ctx, cancel
	mux.connMu.Lock()
	mux.upstream = mock
	mux.conn = newCancelledStartedConnMock()
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	mux.wg.Add(2)
	go mux.readLoop()
	go mux.sendLoop()
	defer func() {
		cancel()
		_ = mock.Close()
		done := make(chan struct{})
		go func() { mux.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("mux goroutine drain exceeded 2s")
		}
	}()

	// Pretend the absorb safety-net just fired: the pre-revert build
	// would have opened a post-reset window here. Post-revert the
	// field is gone, so there is nothing to open. We still set
	// absorbResetTotal so the test mirrors the live livelock setup.
	mux.absorbResetTotal.Add(1)

	// Queue the gateway's fresh RequestStart immediately afterwards
	// (this is exactly what the bus poll loop does in production).
	gwCh := mux.arb.requestStart(gatewaySessionID, 0x7F)

	// Prime tryGrantAndStart via a SYN byte so the non-blocking path
	// dispatches RequestStart(0x7F) to the adapter.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xAA}

	// The adapter promptly grants the bid via StreamEventStarted with
	// the same initiator byte. Pre-revert: readLoop would drop this
	// inside the late-response guard. Post-revert: it flows through
	// to handleArbitrationResponse and grants ownership.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: 0x7F}

	select {
	case result := <-gwCh:
		if !result.granted {
			t.Fatalf("fresh RequestStart was NOT granted (granted=%v cancelled=%v err=%v) — late-response over-extension regression: a STARTED matching the fresh pendingStart was dropped",
				result.granted, result.cancelled, result.err)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for STARTED grant — livelock regression. Captured log:\n%s",
			logBuf.String())
	}

	if strings.Contains(logBuf.String(), droppedLogToken()) {
		t.Fatalf("readLoop emitted the pre-revert drop log line for a fresh RequestStart grant — the over-extension was re-introduced. Log:\n%s",
			logBuf.String())
	}
}

// TestF22_NoLivelockAfterReset runs three reset → RequestStart → grant
// cycles back-to-back. Each cycle must complete: the absorb safety-net
// counter increment must NOT poison the next fresh RequestStart's grant.
// Pre-revert this test would hang on cycle 2 or 3 as the persistent
// post-reset deadline window dropped every subsequent STARTED.
func TestF22_NoLivelockAfterReset(t *testing.T) {
	mock := newP3MockTransport()

	mux := New(Config{
		Protocol:        "enh",
		Network:         "tcp",
		Address:         "127.0.0.1:0",
		ReadTimeout:     200 * time.Millisecond,
		StartDeadline:   200 * time.Millisecond,
		PendingStartTTL: 24 * time.Hour,
		SYNInterval:     time.Hour,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux.ctx, mux.cancel = ctx, cancel
	mux.connMu.Lock()
	mux.upstream = mock
	mux.conn = newCancelledStartedConnMock()
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	mux.wg.Add(2)
	go mux.readLoop()
	go mux.sendLoop()
	defer func() {
		cancel()
		_ = mock.Close()
		done := make(chan struct{})
		go func() { mux.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("mux goroutine drain exceeded 2s")
		}
	}()

	initiator := byte(0x7F)
	for cycle := 1; cycle <= 3; cycle++ {
		// Simulate the absorb-timer firing: bump the reset counter
		// (the legitimate F-22 side effect).
		mux.absorbResetTotal.Add(1)

		gwCh := mux.arb.requestStart(gatewaySessionID, initiator)

		// Prime tryGrantAndStart.
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xAA}
		// Grant.
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: initiator}

		select {
		case result := <-gwCh:
			if !result.granted {
				t.Fatalf("cycle %d: fresh RequestStart NOT granted (granted=%v err=%v) — livelock regression at cycle boundary",
					cycle, result.granted, result.err)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("cycle %d: timed out waiting for STARTED grant — livelock regression", cycle)
		}

		// Release ownership so the next cycle can re-acquire.
		mux.arb.releaseOwnership(gatewaySessionID)
		mux.stateMu.Lock()
		mux.gatewayTxnActive = false
		mux.pendingStart = nil
		mux.stateMu.Unlock()
	}
}

// TestF22_TimeoutResetStillResetsCounterWithoutReconnect re-pins the
// legitimate F-22 contract that the revert preserves: when the absorb
// safety-net's deadline fires, the absorb counter resets to zero and
// the upstream conn is NOT closed. The pre-F-22 reconnect-on-timeout
// produced the 5/68min reconnect cascade that motivated F-22 in the
// first place; this test asserts the revert does NOT regress that fix.
//
// Mirrors TestF22_AbsorbTimerResetsCounterWithoutReconnect (sibling
// file f15_am8_deadline_reconnect_test.go) but is named after this
// revert for explicit linkage in CI failure output.
func TestF22_TimeoutResetStillResetsCounterWithoutReconnect(t *testing.T) {
	mock := newP3MockTransport()

	const startDeadline = 200 * time.Millisecond

	mux := New(Config{
		Protocol:        "enh",
		Network:         "tcp",
		Address:         "127.0.0.1:0",
		ReadTimeout:     200 * time.Millisecond,
		StartDeadline:   startDeadline,
		PendingStartTTL: 24 * time.Hour,
		SYNInterval:     time.Hour,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux.ctx, mux.cancel = ctx, cancel
	mux.connMu.Lock()
	mux.upstream = mock
	mux.conn = newCancelledStartedConnMock()
	connMock := mux.conn.(*cancelledStartedConnMock)
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	mux.wg.Add(2)
	go mux.readLoop()
	go mux.sendLoop()
	defer func() {
		cancel()
		_ = mock.Close()
		done := make(chan struct{})
		go func() { mux.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("mux goroutine drain exceeded 2s")
		}
	}()

	// Submit a START with no adapter response so the AM8 deadline →
	// absorb-timer chain fires.
	_ = mux.arb.requestStart(gatewaySessionID, 0x71)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xAA}

	// Wait for the AM8 deadline; the sibling test asserts the no-
	// reconnect contract here. Sleep slightly past it so the absorb
	// timer is armed.
	time.Sleep(startDeadline + 30*time.Millisecond)

	priorReset := mux.absorbResetTotal.Load()
	deadlineWait := time.Now().Add(2*startDeadline + 1500*time.Millisecond)
	for time.Now().Before(deadlineWait) {
		if mux.absorbResetTotal.Load() > priorReset {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := mux.absorbResetTotal.Load(); got <= priorReset {
		t.Fatalf("legitimate F-22 absorb-reset did NOT fire (absorbResetTotal stayed at %d) after %v",
			got, 2*startDeadline+1500*time.Millisecond)
	}
	if connMock.closed.Load() {
		t.Fatal("F-22 regression: absorb safety-net closed the upstream conn — the post-F-22 contract is counter-reset only, NO transport reconnect. The revert must preserve this invariant.")
	}

	mux.stateMu.Lock()
	stillBlocked := mux.pendingStartAbsorb
	mux.stateMu.Unlock()
	if stillBlocked != 0 {
		t.Fatalf("pendingStartAbsorb = %d after safety-net fire; want 0", stillBlocked)
	}
}

// TestF22_NoStaleAbsorbWindowFieldsRemain catches orphan field
// reintroduction. Any future change that adds a field on the Mux
// struct whose name starts with one of the forbidden prefixes (built
// via string concatenation in forbiddenAbsorbFieldPrefixes for grep
// hygiene — see file header) will fail this test.
func TestF22_NoStaleAbsorbWindowFieldsRemain(t *testing.T) {
	muxType := reflect.TypeOf(Mux{})
	forbiddenPrefixes := forbiddenAbsorbFieldPrefixes()

	for i := 0; i < muxType.NumField(); i++ {
		f := muxType.Field(i)
		for _, prefix := range forbiddenPrefixes {
			if strings.HasPrefix(f.Name, prefix) {
				t.Fatalf("Mux struct field %q matches forbidden prefix %q — the v0.6.13 over-extension was re-introduced. The livelock was caused by exactly this mechanism; see absorb_timeout_revert_test.go header comment for the 6-line live evidence.",
					f.Name, prefix)
			}
		}
	}
}

// TestF22_F15RegressionGuard_AM8BlockingPathReconnects is a structural
// guard ensuring the revert did NOT accidentally remove F-15's blocking-
// path AM8 deadline reconnect logic. The full behavioral test lives at
// f15_am8_deadline_reconnect_test.go::TestAM8Deadline_BlockingPath_StillReconnects.
// Here we assert the dependent struct fields and pending-state shape
// remain so the deadline callback can still tell blocking from non-
// blocking dispatch.
func TestF22_F15RegressionGuard_AM8BlockingPathReconnects(t *testing.T) {
	// pendingStartState.blockingArb is the field the AM8 deadline
	// callback inspects to decide whether to close the upstream
	// transport. The revert must NOT touch it.
	pssType := reflect.TypeOf(pendingStartState{})
	if _, ok := pssType.FieldByName("blockingArb"); !ok {
		t.Fatal("pendingStartState.blockingArb missing — F-15 regression: the AM8 deadline callback can no longer detect the blocking-path requirement to reconnect. The revert must not remove this field.")
	}

	// blockingArbActive on the mux is the gate that tryGrantAndStart
	// honors when a blocking goroutine is still in-flight. Removing
	// this would re-introduce double-dispatch on the blocking path.
	muxType := reflect.TypeOf(Mux{})
	if _, ok := muxType.FieldByName("blockingArbActive"); !ok {
		t.Fatal("Mux.blockingArbActive missing — F-15 regression: the blocking StartArbitration in-flight gate was deleted.")
	}
	if _, ok := muxType.FieldByName("blockingArbGen"); !ok {
		t.Fatal("Mux.blockingArbGen missing — F-15 regression: the blocking-path generation counter was deleted; stale goroutines could clear blockingArbActive out of turn.")
	}
}

// TestF22_LegitimateAbsorptionViaExistingPath confirms the EXISTING
// (untouched by the revert) absorb-counter-decrement path still
// handles legitimate stale responses to ACTIVE pending requests. The
// scenario: cancelPendingStart cleared a pending request, incremented
// pendingStartAbsorb to 1, and the adapter then delivered the late
// STARTED/FAILED for that cancelled bid. The mux must absorb it
// (decrement the counter) and MUST NOT route it to any session.
//
// This is what Codex P1 on PR #632 was trying to defend against in the
// first place; the revert preserves the legitimate counter-decrement
// path (the only one needed in practice) and removes the over-
// extension that broke fresh grants.
func TestF22_LegitimateAbsorptionViaExistingPath(t *testing.T) {
	mock := newP3MockTransport()

	var logBuf cancelledStartedLogBuffer
	logger := log.New(&logBuf, "", 0)

	mux := New(Config{
		Protocol:        "enh",
		Network:         "tcp",
		Address:         "127.0.0.1:0",
		ReadTimeout:     200 * time.Millisecond,
		StartDeadline:   200 * time.Millisecond,
		PendingStartTTL: 24 * time.Hour,
		SYNInterval:     time.Hour,
		Logger:          logger,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux.ctx, mux.cancel = ctx, cancel
	mux.connMu.Lock()
	mux.upstream = mock
	mux.conn = newCancelledStartedConnMock()
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	mux.wg.Add(2)
	go mux.readLoop()
	go mux.sendLoop()
	defer func() {
		cancel()
		_ = mock.Close()
		done := make(chan struct{})
		go func() { mux.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("mux goroutine drain exceeded 2s")
		}
	}()

	// Pre-seed the absorb-counter directly: simulates that
	// cancelPendingStart fired and is now waiting for one stale
	// response from the cancelled bid.
	mux.stateMu.Lock()
	mux.pendingStartAbsorb = 1
	mux.stateMu.Unlock()

	// Adapter delivers the late FAILED for the cancelled bid.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventFailed, Data: 0x42}

	// Give the readLoop a moment to process.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		mux.stateMu.Lock()
		c := mux.pendingStartAbsorb
		mux.stateMu.Unlock()
		if c == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mux.stateMu.Lock()
	final := mux.pendingStartAbsorb
	mux.stateMu.Unlock()

	if final != 0 {
		t.Fatalf("legitimate late-FAILED was NOT absorbed: pendingStartAbsorb=%d (want 0). The counter-decrement path in handleArbitrationResponse must remain functional after the revert. Captured log:\n%s",
			final, logBuf.String())
	}

	// The pre-revert drop log line must not appear — that path is gone.
	if strings.Contains(logBuf.String(), droppedLogToken()) {
		t.Fatalf("pre-revert drop log line appeared — the over-extended mechanism was re-introduced. Log:\n%s",
			logBuf.String())
	}
	// Nor the in-handleArbitrationResponse window-absorb log line.
	if strings.Contains(logBuf.String(), windowAbsorbedLogToken()) {
		t.Fatalf("pre-revert window-absorb log line appeared — handleArbitrationResponse's window helper was re-introduced. Log:\n%s",
			logBuf.String())
	}
}
