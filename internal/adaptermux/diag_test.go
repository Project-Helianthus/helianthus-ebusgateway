package adaptermux

import (
	"context"
	"io"
	"testing"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// ---------------------------------------------------------------------
// Active transaction diagnostics tests
// ---------------------------------------------------------------------

// TestActiveTxnDiag_GrantRecorded proves that completing a gateway
// arbitration grant updates the diagnostic snapshot: grant id, initiator,
// grantedAt, and grantsTotal counter.
func TestActiveTxnDiag_GrantRecorded(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	preSnap := mux.ActiveTxnSnapshot()

	grantGateway(t, mux, mock, 0x71)

	snap := mux.ActiveTxnSnapshot()
	if snap.ID == 0 {
		t.Fatal("ActiveTxnSnapshot.ID must be >0 after grant")
	}
	if snap.Initiator != 0x71 {
		t.Fatalf("Initiator = 0x%02X, want 0x71", snap.Initiator)
	}
	if snap.GrantedAt.IsZero() {
		t.Fatal("GrantedAt must be set")
	}
	if snap.GrantsTotal != preSnap.GrantsTotal+1 {
		t.Fatalf("GrantsTotal = %d, want %d", snap.GrantsTotal, preSnap.GrantsTotal+1)
	}
	if snap.Active {
		t.Fatal("Active must remain false after grant until the gateway performs its first write")
	}
	if snap.InactiveReason != ReasonNone {
		t.Fatalf("InactiveReason must be empty while active, got %q", snap.InactiveReason)
	}
}

// TestActiveTxnDiag_WriteCounterIncrements proves that writes through
// the active transport increment bytesWritten.
func TestActiveTxnDiag_WriteCounterIncrements(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)

	at := mux.ActiveTransport()
	n, err := at.Write([]byte{0x08, 0xB5, 0x04})
	if err != nil {
		t.Fatalf("Write returned err=%v", err)
	}
	if n != 3 {
		t.Fatalf("Write returned n=%d, want 3", n)
	}

	snap := mux.ActiveTxnSnapshot()
	if snap.BytesWritten != 3 {
		t.Fatalf("BytesWritten = %d, want 3", snap.BytesWritten)
	}
}

// TestActiveTxnDiag_ReadCounterIncrements proves that bytes delivered
// through readLoop → activeCh → ReadByte increment bytesRead.
func TestActiveTxnDiag_ReadCounterIncrements(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("Write err=%v", err)
	}

	// P11 — feed the source-byte echo first to leave mid-write phase
	// (echoCursor catches up to writePrefixLen). Without this, the new
	// per-byte filter rejects subsequent bytes that don't match
	// writePrefix[echoCursor]. Pre-P11 the bulk filter let any byte
	// flow through during ownership; the new filter requires
	// protocol-correctness (echo first, then response bytes).
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x71}
	if _, err := at.ReadByte(); err != nil {
		t.Fatalf("ReadByte echo err=%v", err)
	}

	// Feed response bytes via mock. Now in response phase
	// (echoCursor == writePrefixLen), so any byte is delivered.
	bytes := []byte{0x10, 0x08, 0xB5}
	for _, b := range bytes {
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}
	}
	time.Sleep(30 * time.Millisecond)

	for range bytes {
		if _, err := at.ReadByte(); err != nil {
			t.Fatalf("ReadByte err=%v", err)
		}
	}

	snap := mux.ActiveTxnSnapshot()
	if snap.BytesRead < 4 {
		t.Fatalf("BytesRead = %d, want >= 4 (1 echo + 3 response)", snap.BytesRead)
	}
}

// TestActiveTxnDiag_InactiveReason_SYNIdle proves that after the
// gateway has read at least one response byte, the trailing SYN marks
// the txn inactive with ReasonSYNTerminator (PR #502 E2E fix —
// previously ReasonSYNIdle, now distinguished from abandoned-grant
// idle-release).
//
// Lifecycle correctness: SYN before any read must NOT clear (grant
// handoff can leave pre-grant stale SYN bytes in the TCP buffer).
func TestActiveTxnDiag_InactiveReason_SYNIdle(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()

	// Simulate a bus.Send initiating transmission (bytesWritten > 0 is
	// the post-fix signal that the pre-echo window has closed; gating
	// SYN terminator on bytesWritten avoids over-suppressing legitimate
	// terminators when the consumer is slower than readLoop).
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("at.Write err=%v", err)
	}

	// P11 — feed echo of 0x71 first so we leave mid-write phase.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x71}
	time.Sleep(20 * time.Millisecond)
	if _, err := at.ReadByte(); err != nil {
		t.Fatalf("ReadByte echo err=%v", err)
	}

	// Simulate a response read (response phase open, any byte ok).
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x10}
	time.Sleep(20 * time.Millisecond)
	if _, err := at.ReadByte(); err != nil {
		t.Fatalf("ReadByte err=%v", err)
	}

	// Round-6: emulate sendEndOfMessage so the terminator gate fires.
	gatewayEndOfMessage(t, at)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)

	snap := mux.ActiveTxnSnapshot()
	if snap.Active {
		t.Fatal("Active must be false after SYN (with bytesRead > 0)")
	}
	if snap.InactiveReason != ReasonSYNTerminator {
		t.Fatalf("InactiveReason = %q, want %q", snap.InactiveReason, ReasonSYNTerminator)
	}
	if snap.InactiveAt.IsZero() {
		t.Fatal("InactiveAt must be set")
	}
}

// TestActiveTxnDiag_SYNBeforeRead_DoesNotClear proves that a SYN arriving
// during gateway ownership BEFORE any Write must not terminate the
// nascent transaction. The transaction has not started yet, so the SYN
// must not terminate it. (batch-25 hotfix: the round-6 grantSyn branch
// that counted these via SynSuppressedPreEcho was reverted; the SYN now
// passes through the original midWriteSyn=false path and no counter
// increments.)
func TestActiveTxnDiag_SYNBeforeRead_DoesNotClear(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)

	// NOTE: no Write yet — simulating the grant-to-first-write window.

	// SYN arrives BEFORE any Write. Must NOT clear the nascent active
	// state.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)

	snap := mux.ActiveTxnSnapshot()
	if snap.Active {
		t.Fatalf("Active must remain false before first write; got inactive reason=%q writes=%d reads=%d",
			snap.InactiveReason, snap.BytesWritten, snap.BytesRead)
	}
	if snap.InactiveReason != ReasonNone {
		t.Fatalf("InactiveReason must be empty, got %q", snap.InactiveReason)
	}
}

// TestActiveTxnDiag_PreWriteBytesDoNotPolluteTxn proves that bus noise
// observed while the gateway owns arbitration but has not written the
// first request byte yet does not contaminate the current txn prefix or
// non-echo counters.
func TestActiveTxnDiag_PreWriteBytesDoNotPolluteTxn(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)

	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xF1}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)

	snap := mux.ActiveTxnSnapshot()
	if snap.NonEcho != 0 || snap.EchoLike != 0 || snap.SynMarkers != 0 {
		t.Fatalf("pre-write noise polluted txn: echo=%d nonEcho=%d syn=%d",
			snap.EchoLike, snap.NonEcho, snap.SynMarkers)
	}
	if len(snap.ReadPrefix) != 0 {
		t.Fatalf("ReadPrefix = % X, want empty before first write", snap.ReadPrefix)
	}
}

// TestActiveTxnDiag_InactiveReason_Reset proves handleReset marks
// the txn inactive with ReasonReset.
func TestActiveTxnDiag_InactiveReason_Reset(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("Write err=%v", err)
	}

	mux.handleReset()

	snap := mux.ActiveTxnSnapshot()
	if snap.Active {
		t.Fatal("Active must be false after reset")
	}
	if snap.InactiveReason != ReasonReset {
		t.Fatalf("InactiveReason = %q, want %q", snap.InactiveReason, ReasonReset)
	}
}

// TestActiveTxnDiag_InactiveReason_Idempotent proves that the first
// reason sticks (subsequent cleanup paths do not override).
func TestActiveTxnDiag_InactiveReason_Idempotent(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()

	// Move out of the pre-echo window (bytesWritten > 0) so the next SYN
	// is treated as the frame terminator, not pre-echo suppressed.
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("Write err=%v", err)
	}
	// P11 — feed echo of 0x71 first to leave mid-write phase.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x71}
	time.Sleep(20 * time.Millisecond)
	if _, err := at.ReadByte(); err != nil {
		t.Fatalf("ReadByte echo err=%v", err)
	}
	// Also feed + consume a response byte for realism.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x10}
	time.Sleep(20 * time.Millisecond)
	if _, err := at.ReadByte(); err != nil {
		t.Fatalf("ReadByte err=%v", err)
	}

	// Round-6: emulate sendEndOfMessage so the terminator gate fires.
	gatewayEndOfMessage(t, at)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)

	// Then a handleReset — should NOT override the original reason.
	mux.handleReset()

	snap := mux.ActiveTxnSnapshot()
	if snap.InactiveReason != ReasonSYNTerminator {
		t.Fatalf("first reason should stick: got %q, want %q", snap.InactiveReason, ReasonSYNTerminator)
	}
}

// ---------------------------------------------------------------------
// Regression test modeling the observed runtime failure shape
// ---------------------------------------------------------------------

// mockWriteOnlyTransport simulates the observed production failure:
// START arbitration succeeds, the mux writes request bytes, but NO
// response bytes ever arrive. At the bus.Send layer, these become
// timeouts (not collisions/nacks/crc/other).
type mockWriteOnlyTransport struct {
	*p3MockTransport
	writtenBytesCount int
}

func newMockWriteOnlyTransport() *mockWriteOnlyTransport {
	return &mockWriteOnlyTransport{
		p3MockTransport: newP3MockTransport(),
	}
}

func (m *mockWriteOnlyTransport) Write(p []byte) (int, error) {
	m.mu.Lock()
	m.writtenBytesCount += len(p)
	m.mu.Unlock()
	// Do NOT echo back — mimics adapter accepting writes but bus
	// producing no response (unresponsive target / wiring / partition).
	return len(p), nil
}

// TestRegression_StartedButNoResponse models the exact failure shape
// observed on the RPi deploy:
//   - RequestStart succeeds
//   - StreamEventStarted arrives
//   - gateway writes request bytes
//   - NO response bytes arrive
//   - scan/request layer classifies this as a timeout
//
// Diagnostics must clearly show started > 0, writes > 0, reads == 0.
// This test does not fix runtime — it ensures the next deploy's
// diagnostics will identify whether production is failing at
// write (writes == 0), response (writes > 0, reads == 0), or delivery
// (reads > 0 but protocol layer times out).
func TestRegression_StartedButNoResponse(t *testing.T) {
	mock := newMockWriteOnlyTransport()

	mux := New(Config{
		Protocol:             "enh",
		Network:              "tcp",
		Address:              "127.0.0.1:0",
		ReadTimeout:          200 * time.Millisecond,
		MaxOwnershipDuration: 10 * time.Second,
		IdleReleaseGrace:     200 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux.ctx, mux.cancel = ctx, cancel

	mux.connMu.Lock()
	mux.upstream = mock
	mux.connMu.Unlock()
	mux.upstreamFeatures.Store(0x01)

	mux.wg.Add(2)
	go mux.readLoop()
	go mux.sendLoop()

	defer func() {
		cancel()
		closeOrLog(t, mock, "mock")
		mux.wg.Wait()
	}()

	// Simulate the observed production pattern: 3 successful grants,
	// each writes 7 request bytes, no response arrives, transaction
	// times out via SYN idle release.
	for i := 0; i < 3; i++ {
		// Grant
		grantGateway(t, mux, mock.p3MockTransport, 0x71)

		// Write request bytes via the active transport (like bus.Send).
		at := mux.ActiveTransport()
		request := []byte{0x08, 0xB5, 0x04, 0x01, 0x00, 0x00, 0xCD}
		n, err := at.Write(request)
		if err != nil || n != len(request) {
			t.Fatalf("cycle %d: write n=%d err=%v", i, n, err)
		}

		// NO response bytes arrive — simulate unresponsive target.
		// Wait for idle grace, then SYN to release ownership.
		time.Sleep(220 * time.Millisecond)
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
		time.Sleep(30 * time.Millisecond)
	}

	snap := mux.ActiveTxnSnapshot()

	// Diagnostic assertions proving the failure SHAPE:
	//   grants > 0
	//   writes > 0  (each request wrote 7 bytes)
	//   reads == 0  (no response ever arrived)
	//   txn eventually inactive with a SYN-based reason
	if snap.GrantsTotal != 3 {
		t.Fatalf("GrantsTotal = %d, want 3", snap.GrantsTotal)
	}
	// bytesWritten is per-txn, not cumulative — last txn should have 7.
	if snap.BytesWritten != uint64(7) {
		t.Fatalf("last txn BytesWritten = %d, want 7", snap.BytesWritten)
	}
	if snap.BytesRead != 0 {
		t.Fatalf("last txn BytesRead = %d, want 0 (no response arrived)", snap.BytesRead)
	}
	// Write+no-read transactions are terminated by ONE of:
	//   - idle-grace release (syn_idle) after IdleReleaseGrace with an
	//     abandoned grant (no reads ever)
	//   - active_read_timeout when bus.Send hits its read timeout
	//   - max_ownership after MaxOwnershipDuration
	//   - context_cancel / reset / reconnect
	// All of these preserve the diagnostic signal: writes>0 reads=0.
	// Pre-grant stale SYN (SYN with bytesRead==0 BEFORE idle-grace
	// expired) must NOT be the cause — that was the production bug.
	if snap.Active {
		t.Fatal("last txn should be inactive by now (idle-grace already fired)")
	}
	allowed := map[ActiveTxnInactiveReason]bool{
		ReasonSYNIdle:           true,
		ReasonSYNTerminator:     true, // echo_mismatch-fix gate is bytesWritten>0; writes=7 here ⇒ first SYN treated as terminator
		ReasonActiveReadTimeout: true,
		ReasonMaxOwnership:      true,
		ReasonContextCancel:     true,
	}
	if !allowed[snap.InactiveReason] {
		t.Fatalf("InactiveReason = %q, want one of syn_idle/syn_terminator/active_read_timeout/max_ownership/context_cancel (lifecycle-valid reasons)", snap.InactiveReason)
	}
}

// TestActiveTxnDiag_DrainedOnGrant records the drain count from the
// previous cycle's leftovers. Under the lifecycle-correct policy,
// drain should be zero in steady-state; a non-zero value here signals
// a regression (the "drained N bytes from activeCh on grant" runtime
// log spam).
func TestActiveTxnDiag_DrainedOnGrant_ZeroInSteadyState(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	// First grant — should drain zero.
	grantGateway(t, mux, mock, 0x71)
	snap1 := mux.ActiveTxnSnapshot()
	if snap1.DrainedOnGrant != 0 {
		t.Fatalf("first grant DrainedOnGrant = %d, want 0", snap1.DrainedOnGrant)
	}

	at := mux.ActiveTransport()
	// Move past the pre-echo window (bytesWritten > 0) so the terminator
	// SYN below clears cleanly under the new echo_mismatch-fix gate.
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("Write err=%v", err)
	}
	// P11 — feed echo of 0x71 first to leave mid-write phase.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x71}
	time.Sleep(20 * time.Millisecond)
	if _, err := at.ReadByte(); err != nil {
		t.Fatalf("ReadByte echo err=%v", err)
	}
	// Feed response byte + consume via activeTransport so bytesRead > 0
	// (response phase is open after first echo).
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x10}
	time.Sleep(20 * time.Millisecond)
	if _, err := at.ReadByte(); err != nil {
		t.Fatalf("ReadByte err=%v", err)
	}
	// Round-6: emulate sendEndOfMessage so the terminator gate fires.
	// The terminator SYN is delivered to activeCh before clearing, so
	// the test's consumer MUST drain it; otherwise the next grant would
	// drain it as a stale byte and break the steady-state
	// DrainedOnGrant==0 invariant.
	gatewayEndOfMessage(t, at)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)
	// Consume the terminator echo (recordSent put SymbolSyn on the queue,
	// the wire echo matches and is delivered to activeCh).
	if b, err := at.ReadByte(); err != nil || b != protocol.SymbolSyn {
		t.Fatalf("ReadByte terminator=(0x%02X,%v), want (0xAA,nil)", b, err)
	}

	// Third-party bytes during idle grace — must NOT land in activeCh
	// under the lifecycle-correct policy.
	for _, b := range []byte{0xFE, 0xBA, 0x55} {
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}
	}
	time.Sleep(10 * time.Millisecond)

	// Wait past idle grace, release, and re-grant.
	time.Sleep(220 * time.Millisecond)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(20 * time.Millisecond)

	grantGateway(t, mux, mock, 0x71)
	snap2 := mux.ActiveTxnSnapshot()
	if snap2.DrainedOnGrant != 0 {
		t.Errorf("second grant DrainedOnGrant = %d, want 0 (steady-state invariant broken)", snap2.DrainedOnGrant)
	}
}

// ---------------------------------------------------------------------
// Bounded-diagnostics test for the p3MockTransport timeout hook
// (pre-existing infrastructure; verify we didn't break it)
// ---------------------------------------------------------------------

// TestActiveTxnDiag_MockReadTimeout proves that the readTimeout hook
// on the mock transport produces ErrTimeout that the readLoop tolerates.
func TestActiveTxnDiag_MockReadTimeout(t *testing.T) {
	mock := newP3MockTransport()
	mock.readTimeout = 30 * time.Millisecond
	ev, err := mock.ReadEvent()
	if err == nil {
		t.Fatalf("expected timeout error, got event %+v", ev)
	}
	if err != ebuserrors.ErrTimeout {
		t.Fatalf("err = %v, want ErrTimeout", err)
	}
}

// Ensure mockWriteOnlyTransport is usable (io.EOF handling).
var _ io.Closer = (*mockWriteOnlyTransport)(nil)

// TestActiveTxnDiag_AfterInactive_Counter proves that afterInactive
// increments when a non-SYN byte arrives while gateway owns the bus
// but gatewayTxnActive is false (post-SYN window before ownership is
// released). This is the regression signal for post-inactive delivery
// pressure.
func TestActiveTxnDiag_AfterInactive_Counter(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)
	at := mux.ActiveTransport()

	// Move out of the pre-echo window (bytesWritten > 0) so the trailing
	// SYN is treated as terminator, not pre-echo suppressed.
	if _, err := at.Write([]byte{0x71}); err != nil {
		t.Fatalf("Write err=%v", err)
	}
	// P11 — feed echo of 0x71 first to leave mid-write phase.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x71}
	time.Sleep(20 * time.Millisecond)
	if _, err := at.ReadByte(); err != nil {
		t.Fatalf("ReadByte echo err=%v", err)
	}
	// Simulate a completed transaction read.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x10}
	time.Sleep(20 * time.Millisecond)
	if _, err := at.ReadByte(); err != nil {
		t.Fatalf("ReadByte err=%v", err)
	}

	// Round-6: emulate sendEndOfMessage so the terminator gate fires.
	gatewayEndOfMessage(t, at)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)
	// Drain the terminator byte delivered to activeCh.
	_, _ = at.ReadByte()

	// Confirm ownership still held and txn inactive.
	snap := mux.ActiveTxnSnapshot()
	if snap.Active {
		t.Fatal("gatewayTxnActive must be false after SYN")
	}
	if !mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("ownership should still be held")
	}
	preAfter := snap.AfterInactive

	// Feed 3 non-SYN bytes during the inactive window.
	for _, b := range []byte{0xFE, 0x10, 0xBA} {
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}
	}
	time.Sleep(30 * time.Millisecond)

	snap = mux.ActiveTxnSnapshot()
	if snap.AfterInactive != preAfter+3 {
		t.Fatalf("AfterInactive = %d, want %d (3 bytes while owned+inactive)", snap.AfterInactive, preAfter+3)
	}
}
