package adaptermux

import (
	"net"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
)

// Phase 3 Step B3.6c: tests for the pacer wiring at the
// adapter-write hot path (mux.doSend's BeforeActiveWrite hook).
// Scope contract: pacer storage lifecycle + BeforeActiveWrite
// call on every gateway-internal and external-session active
// write. NO RecordEcho yet (B3.6d), NO watchdog (B3.6e), NO
// output pacer (B3.6f).

// TestSessionPacer_OffMode_AlwaysNil pins the production-default
// invariant: V8ClassifierMode == Off → SessionPacer returns nil
// regardless of sessionID. No per-session pacer allocation, no
// adapter-write hot path overhead beyond the nil-check.
func TestSessionPacer_OffMode_AlwaysNil(t *testing.T) {
	t.Parallel()
	mux, _, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeOff)
	defer cleanup()

	for _, sid := range []uint64{gatewaySessionID, 1, 42, 12345} {
		if got := mux.SessionPacer(sid); got != nil {
			t.Errorf("SessionPacer(%d) = %v in ModeOff; want nil", sid, got)
		}
	}
}

// TestSessionPacer_ShadowMode_GatewayPreCreated pins that the
// gateway-internal session pacer (sessionID = gatewaySessionID = 0)
// is pre-created in Mux.New when V8ClassifierMode != Off, so the
// first active write doesn't race on lazy-create.
func TestSessionPacer_ShadowMode_GatewayPreCreated(t *testing.T) {
	t.Parallel()
	mux, _, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	defer cleanup()

	pacer := mux.SessionPacer(gatewaySessionID)
	if pacer == nil {
		t.Fatal("SessionPacer(gatewaySessionID) = nil in ModeShadow; want pre-created Pacer")
	}
	// Verify it's a fresh Pacer (default bootstrap state).
	if got := pacer.Lrtt(); got != v8classifier.LrttBootstrapInitial {
		t.Errorf("gateway Pacer.Lrtt() = %v; want LrttBootstrapInitial %v (fresh state)",
			got, v8classifier.LrttBootstrapInitial)
	}
	if pacer.InGraceBootstrap() {
		t.Error("gateway Pacer.InGraceBootstrap() = true; want false (no prior history)")
	}
}

// TestSessionPacer_AddSession_CreatesPacer pins that adding an
// external session in ModeShadow / ModeEnforce pre-creates its
// pacer entry.
func TestSessionPacer_AddSession_CreatesPacer(t *testing.T) {
	t.Parallel()
	mux, _, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	defer cleanup()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	sid := mux.AddSession(serverConn)
	if sid == 0 {
		t.Fatal("AddSession returned 0")
	}
	defer mux.RemoveSession(sid)

	pacer := mux.SessionPacer(sid)
	if pacer == nil {
		t.Fatalf("SessionPacer(%d) = nil after AddSession in ModeShadow; want non-nil", sid)
	}
}

// TestSessionPacer_RemoveSession_DropsEntry pins that RemoveSession
// cleans up the per-session pacer entry AND that the load-only
// SessionPacer accessor returns nil afterward (NO lazy-recreate).
//
// Per Codex round-1 MEDIUM on PR #645: the prior version of this
// test couldn't actually prove deletion — it observed
// SessionPacer's lazy-create returning a fresh Pacer, which is
// indistinguishable from the deletion never having fired. The
// round-1 fix removed lazy-create from SessionPacer; this test
// is the load-only assertion that proves deletion.
func TestSessionPacer_RemoveSession_DropsEntry(t *testing.T) {
	t.Parallel()
	mux, _, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	defer cleanup()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	sid := mux.AddSession(serverConn)
	if sid == 0 {
		t.Fatal("AddSession returned 0")
	}

	pacerBefore := mux.SessionPacer(sid)
	if pacerBefore == nil {
		t.Fatalf("precondition: SessionPacer(%d) nil before Remove", sid)
	}
	// Bump the counter on the pre-Remove pacer so the load-only
	// post-Remove probe would clearly distinguish the OLD pacer
	// from a hypothetical lazy-recreate (if lazy-create ever
	// resurges as a regression). With load-only, the second
	// SessionPacer call MUST return nil — no Pacer to inspect.
	pacerBefore.BeforeActiveWrite(time.Now())
	if got := pacerBefore.BeforeActiveWriteTotal(); got != 1 {
		t.Fatalf("precondition: pacerBefore counter = %d; want 1", got)
	}

	mux.RemoveSession(sid)

	// PRIMARY ASSERTION: SessionPacer is load-only post-Remove.
	// Returns nil — proves the Delete actually fired AND proves
	// no resurrection-on-hot-path race.
	if got := mux.SessionPacer(sid); got != nil {
		t.Errorf("SessionPacer(%d) = %v after Remove; want nil (load-only contract — no lazy-recreate)", sid, got)
	}
}

// TestSessionPacer_DoSend_GatewayInvokesBeforeActiveWrite is the
// LOAD-BEARING test for B3.6c: a gateway-internal active write
// MUST invoke BeforeActiveWrite on the gateway session's pacer.
// We exercise this via grantGateway → active path so doSend fires.
func TestSessionPacer_DoSend_GatewayInvokesBeforeActiveWrite(t *testing.T) {
	mux, mock, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	defer cleanup()

	pacer := mux.SessionPacer(gatewaySessionID)
	if pacer == nil {
		t.Fatal("gateway Pacer nil in ModeShadow")
	}
	if got := pacer.BeforeActiveWriteTotal(); got != 0 {
		t.Fatalf("precondition: BeforeActiveWriteTotal = %d; want 0", got)
	}

	// Grant the gateway and do an active write.
	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()
	req := []byte{0x71, 0x08, 0x07, 0x04, 0x00}
	n, err := at.Write(req)
	if err != nil || n != len(req) {
		t.Fatalf("ActiveTransport.Write returned (%d, %v); want (%d, nil)", n, err, len(req))
	}

	// Poll BeforeActiveWriteTotal until it reaches the byte count
	// (each byte triggers one BeforeActiveWrite via doSend).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if pacer.BeforeActiveWriteTotal() >= uint64(len(req)) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := pacer.BeforeActiveWriteTotal(); got != uint64(len(req)) {
		t.Errorf("BeforeActiveWriteTotal() = %d; want %d (one per byte written)", got, len(req))
	}
}

// TestSessionPacer_RecordEcho_OnGatewayEchoMatch pins the Phase 3
// Step B3.6d behavioral contract: when the gateway writes a byte
// AND the adapter echoes it back AND mode != Off, the matched
// echo's RTT MUST be fed to the gateway pacer's L_rtt EMA via
// RecordEcho. The L_rtt EMA moves off its bootstrap value
// (LrttBootstrapInitial = 100ms) only when this wiring is correct.
func TestSessionPacer_RecordEcho_OnGatewayEchoMatch(t *testing.T) {
	mux, mock, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	defer cleanup()

	pacer := mux.SessionPacer(gatewaySessionID)
	if pacer == nil {
		t.Fatal("gateway pacer nil in ModeShadow")
	}
	if got := pacer.RecordedSamples(); got != 0 {
		t.Fatalf("precondition: RecordedSamples = %d; want 0", got)
	}

	// Grant the gateway, write a byte, then inject the echo on
	// the upstream mock — this fires matchEchoWithTime →
	// pacer.RecordEcho.
	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("Write err: %v", err)
	}

	// Echo back. The mock's StreamEventByte arrives at readLoop
	// which calls matchEchoWithTime.
	mock.eventCh <- transport.StreamEvent{
		Kind: transport.StreamEventByte, Byte: 0x71, WasEscaped: false,
	}

	// Poll until RecordedSamples reaches 1.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if pacer.RecordedSamples() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := pacer.RecordedSamples(); got != 1 {
		t.Errorf("RecordedSamples() = %d; want 1 (echo match should feed RecordEcho)", got)
	}
}

// TestSessionPacer_RecordEcho_OffMode_NoSamples pins the
// production-default invariant: in ModeOff, no L_rtt sample fires
// even when the gateway echoes are matched, AND no per-byte
// time.Time slot is allocated in echoTracker.expectedWriteTimes
// (Codex round-1 MAJOR on PR #646 — ModeOff must be true zero
// overhead, not "harmless time.Time{} append per byte").
//
// Postcondition checked: after a full grant + write + echo round
// trip, the gateway pacer slot is STILL nil (no lazy allocation
// on the hot path) and the gateway echoTracker's
// expectedWriteTimes queue is STILL empty (proves the legacy
// recordSent path was taken, not recordSentWithTime). Either
// failure would indicate a regression where the v8 != nil
// short-circuit at doSend / matchEcho leaked back into the
// ModeOff configuration.
func TestSessionPacer_RecordEcho_OffMode_NoSamples(t *testing.T) {
	mux, mock, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeOff)
	defer cleanup()

	if got := mux.SessionPacer(gatewaySessionID); got != nil {
		t.Fatalf("precondition: SessionPacer in ModeOff = %v; want nil", got)
	}

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("Write err: %v", err)
	}
	mock.eventCh <- transport.StreamEvent{
		Kind: transport.StreamEventByte, Byte: 0x71, WasEscaped: false,
	}

	// Brief settle so the echo-match path completes on the read
	// goroutine before we inspect tracker state.
	time.Sleep(100 * time.Millisecond)

	// PRIMARY assertion: ModeOff still has no gateway pacer. Any
	// drift toward lazy-allocate on the hot path would surface
	// here as a non-nil pointer.
	if got := mux.SessionPacer(gatewaySessionID); got != nil {
		t.Errorf("post-echo: SessionPacer in ModeOff = %v; want nil (no lazy-allocate on hot path)", got)
	}

	// SECONDARY assertion: the gateway echoTracker took the
	// legacy recordSent path (no timestamp). expectedWriteTimes
	// must be empty AFTER the write+match round trip — proves
	// the per-byte time.Time slot allocation did NOT happen.
	mux.stateMu.Lock()
	wtLen := len(mux.gatewayEcho.expectedWriteTimes)
	mux.stateMu.Unlock()
	if wtLen != 0 {
		t.Errorf("post-echo: gatewayEcho.expectedWriteTimes length = %d; want 0 (ModeOff must skip timestamp allocation)", wtLen)
	}
}

// TestEchoTracker_LegacyRecordSent_NoWriteAtSlot pins the
// echo_tracker.go side of the conditional lockstep invariant
// (Codex round-1 MAJOR on PR #646): the legacy recordSent path
// MUST NOT touch expectedWriteTimes. A focused unit test on the
// tracker alone — no Mux setup required.
func TestEchoTracker_LegacyRecordSent_NoWriteAtSlot(t *testing.T) {
	t.Parallel()
	tr := newEchoTracker()
	for _, b := range []byte{0x71, 0x08, 0x07, 0x04, 0x00} {
		tr.recordSent(b)
	}
	if got := len(tr.expectedEchoes); got != 5 {
		t.Errorf("expectedEchoes length = %d; want 5", got)
	}
	if got := len(tr.expectedWriteTimes); got != 0 {
		t.Errorf("expectedWriteTimes length = %d; want 0 (legacy recordSent must NOT append timestamp slots)", got)
	}
	// matchEchoWithTime on a legacy-populated tracker MUST
	// report hasWriteAt=false for every match — no L_rtt sample
	// possible without a recorded writeAt.
	result, _, writeAt, hasWriteAt := tr.matchEchoWithTime(0x71, false)
	if result != echoMatchSuppressed {
		t.Errorf("matchEchoWithTime result = %v; want echoMatchSuppressed", result)
	}
	if hasWriteAt {
		t.Error("hasWriteAt = true on legacy-recorded byte; want false (no timestamp captured)")
	}
	if !writeAt.IsZero() {
		t.Errorf("writeAt = %v on legacy-recorded byte; want zero", writeAt)
	}
}

// TestSessionPacer_OffMode_DoSendDoesNotCallPacer pins the
// performance contract: ModeOff means SessionPacer returns nil
// at doSend's hook site, so no pacer overhead per byte.
//
// This is asserted indirectly: in ModeOff, no Pacer is allocated
// for any sessionID, so even if doSend tried to call
// BeforeActiveWrite, the nil-guard prevents the call. We verify
// by attempting a gateway write and confirming no pacer was
// ever created.
func TestSessionPacer_OffMode_DoSendDoesNotCallPacer(t *testing.T) {
	mux, mock, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeOff)
	defer cleanup()

	// Sanity: no gateway pacer pre-created in Off.
	if got := mux.SessionPacer(gatewaySessionID); got != nil {
		t.Fatalf("SessionPacer(gateway) = %v in ModeOff; want nil", got)
	}

	// Do the active write.
	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()
	if _, err := at.Write([]byte{0x71, 0x08}); err != nil {
		t.Fatalf("Write err: %v", err)
	}

	// After the write, SessionPacer(gateway) should STILL be nil
	// — no lazy-creation in Off mode.
	if got := mux.SessionPacer(gatewaySessionID); got != nil {
		t.Errorf("SessionPacer(gateway) = %v after write in ModeOff; want nil (no lazy-create)", got)
	}
}

// TestSessionPacer_Watchdog_ArmedOnDoSend pins the Phase 3 Step
// B3.6e wiring at the mux.doSend hot path: when V8ClassifierMode
// != Off, a gateway-internal active write MUST arm the gateway
// pacer's echo watchdog. We verify by checking WatchdogArmed
// immediately after the write — before any echo arrives, the
// watchdog must still be running.
func TestSessionPacer_Watchdog_ArmedOnDoSend(t *testing.T) {
	mux, mock, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	defer cleanup()

	pacer := mux.SessionPacer(gatewaySessionID)
	if pacer == nil {
		t.Fatal("gateway Pacer nil in ModeShadow")
	}
	if pacer.WatchdogArmed() {
		t.Fatal("precondition: WatchdogArmed=true before any write")
	}

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()
	req := []byte{0x71}
	if _, err := at.Write(req); err != nil {
		t.Fatalf("Write err: %v", err)
	}

	// Codex round-1 MEDIUM #1 on PR #647: the previous 500ms
	// poll window exceeded the normal-mode hard deadline (~400ms),
	// so an unrelated CI stall could let the hard timer fire and
	// clear WatchdogArmed before the assertion checked it. Switch
	// to a tight poll window (50ms — well under the soft deadline
	// of ~200ms) and cancel IMMEDIATELY after the first observed
	// armed=true.
	deadline := time.Now().Add(50 * time.Millisecond)
	armed := false
	for time.Now().Before(deadline) {
		if pacer.WatchdogArmed() {
			armed = true
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	if !armed {
		t.Errorf("WatchdogArmed() = false after gateway write; want true (B3.6e arm at sendLoop)")
	}
	// Cancel immediately so the watchdog cannot fire during
	// cleanup and emit a spurious admin event that could confuse
	// downstream tests.
	pacer.CancelEchoWatchdog()
}

// TestSessionPacer_Watchdog_CancelledOnEchoMatch pins that the
// match-site cancellation (B3.6e) actually fires: once the
// adapter echoes the gateway's write back, the watchdog must
// disarm so a delayed timer fire cannot emit a stale admin
// event for an already-completed transaction.
func TestSessionPacer_Watchdog_CancelledOnEchoMatch(t *testing.T) {
	mux, mock, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	defer cleanup()

	pacer := mux.SessionPacer(gatewaySessionID)
	if pacer == nil {
		t.Fatal("gateway Pacer nil in ModeShadow")
	}

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("Write err: %v", err)
	}

	// Inject the echo so matchEchoWithTime fires → CancelEchoWatchdog.
	mock.eventCh <- transport.StreamEvent{
		Kind: transport.StreamEventByte, Byte: 0x71, WasEscaped: false,
	}

	// Poll until the cancel propagates.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !pacer.WatchdogArmed() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if pacer.WatchdogArmed() {
		t.Error("WatchdogArmed() = true after echo match; want false (B3.6e cancel at match site)")
	}
	// And confirm no spurious timeout fired during the round trip.
	if got := pacer.SoftTimeoutTotal(); got != 0 {
		t.Errorf("SoftTimeoutTotal() = %d after fast echo; want 0 (echo arrived before soft deadline)", got)
	}
}

// TestSessionPacer_Watchdog_OffMode_NoArm pins the ModeOff
// zero-overhead contract on the watchdog side: in ModeOff there
// is no pacer, so there is no watchdog to arm. The test exercises
// the doSend path and confirms (a) SessionPacer remains nil and
// (b) no panic / no goroutine leak.
func TestSessionPacer_Watchdog_OffMode_NoArm(t *testing.T) {
	mux, mock, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeOff)
	defer cleanup()

	if got := mux.SessionPacer(gatewaySessionID); got != nil {
		t.Fatalf("precondition: SessionPacer in ModeOff = %v; want nil", got)
	}

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("Write err: %v", err)
	}

	// Post-write: still nil. (No lazy-create on the watchdog path.)
	if got := mux.SessionPacer(gatewaySessionID); got != nil {
		t.Errorf("post-write SessionPacer in ModeOff = %v; want nil (no watchdog lazy-create)", got)
	}
}

// TestSessionPacer_OutputPacer_SchedulesFrameEgress pins the
// Phase 3 Step B3.6f wiring: in V8ClassifierMode != Off, each
// frame written by session.writeFrame consults the per-session
// pacer's Schedule. The cadence anchor advances by τ_wire_byte
// per byte (4.17ms per byte). We exercise this by writing two
// 1-byte frames in quick succession on the same session and
// confirming the LastScheduledEmit advances by at least one τ
// between them.
func TestSessionPacer_OutputPacer_SchedulesFrameEgress(t *testing.T) {
	mux, _, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	defer cleanup()

	// Add an external session via the same plumbing that
	// production AddSession exercises.
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	sid := mux.AddSession(serverConn)
	if sid == 0 {
		t.Fatal("AddSession returned 0")
	}
	defer mux.RemoveSession(sid)

	pacer := mux.SessionPacer(sid)
	if pacer == nil {
		t.Fatalf("SessionPacer(%d) = nil; want non-nil", sid)
	}
	// Precondition: LastScheduledEmit is zero (no Schedule fired).
	if got := pacer.LastScheduledEmit(); !got.IsZero() {
		t.Fatalf("precondition: LastScheduledEmit = %v; want zero", got)
	}

	// Drive a frame egress: enqueue a single-byte ENH frame on
	// the session's sendCh. The writeLoop drains it and calls
	// writeFrame, which fires our new Schedule wiring.
	// Use a payload byte < 0x80 so writeFrame takes the "short
	// form" path (1-byte buf).
	mux.sessions[sid].sendCh <- sessionFrame{
		kind:    sessionFrameReceived,
		payload: 0x42,
	}
	// Read from the pipe so writeLoop's Write doesn't block.
	go func() {
		buf := make([]byte, 32)
		_, _ = clientConn.Read(buf)
	}()

	// Poll LastScheduledEmit until it advances (post-Schedule call).
	deadline := time.Now().Add(2 * time.Second)
	var emit1 time.Time
	for time.Now().Before(deadline) {
		emit1 = pacer.LastScheduledEmit()
		if !emit1.IsZero() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if emit1.IsZero() {
		t.Fatal("LastScheduledEmit stayed zero after frame egress — Schedule was not called")
	}

	// Enqueue a SECOND frame and confirm the anchor advances by
	// at least one τ.
	mux.sessions[sid].sendCh <- sessionFrame{
		kind:    sessionFrameReceived,
		payload: 0x43,
	}
	go func() {
		buf := make([]byte, 32)
		_, _ = clientConn.Read(buf)
	}()

	deadline = time.Now().Add(2 * time.Second)
	var emit2 time.Time
	for time.Now().Before(deadline) {
		emit2 = pacer.LastScheduledEmit()
		if emit2.After(emit1) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !emit2.After(emit1) {
		t.Errorf("LastScheduledEmit did not advance between frames: emit1=%v emit2=%v", emit1, emit2)
	}
	// Verify the advance is AT LEAST τ_wire_byte (4.17ms). It
	// can be MORE if wall clock advanced past the prior emit
	// (writeLoop scheduling latency, GC, etc).
	advance := emit2.Sub(emit1)
	if advance < v8classifier.TauWireByte {
		t.Errorf("cadence advance %v < TauWireByte %v; pacer enforcement broken", advance, v8classifier.TauWireByte)
	}
}

// TestSessionPacer_OutputPacer_OffMode_NoSchedule pins the
// ModeOff zero-overhead contract on the output-pacer side: when
// V8ClassifierMode == Off, writeFrame must NOT call Schedule
// (and obviously must not allocate a pacer). The pipe still
// receives the frame promptly.
func TestSessionPacer_OutputPacer_OffMode_NoSchedule(t *testing.T) {
	mux, _, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeOff)
	defer cleanup()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	sid := mux.AddSession(serverConn)
	if sid == 0 {
		t.Fatal("AddSession returned 0")
	}
	defer mux.RemoveSession(sid)

	// SessionPacer is nil in ModeOff.
	if got := mux.SessionPacer(sid); got != nil {
		t.Fatalf("SessionPacer in ModeOff = %v; want nil", got)
	}

	mux.sessions[sid].sendCh <- sessionFrame{
		kind:    sessionFrameReceived,
		payload: 0x42,
	}
	// Read promptly — confirms the frame egress did NOT block on
	// any pacer Schedule (which doesn't exist in ModeOff).
	if err := serverConn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	buf := make([]byte, 32)
	n, err := clientConn.Read(buf)
	if err != nil {
		t.Fatalf("clientConn.Read err: %v", err)
	}
	if n < 1 {
		t.Errorf("clientConn.Read returned %d bytes; want >= 1", n)
	}
	// Post-write: SessionPacer STILL nil — no lazy-create.
	if got := mux.SessionPacer(sid); got != nil {
		t.Errorf("post-write SessionPacer in ModeOff = %v; want nil (no lazy-create)", got)
	}
}
