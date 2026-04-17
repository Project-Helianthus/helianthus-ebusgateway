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
	if !snap.Active {
		t.Fatal("Active must be true after grant (before end-of-txn SYN)")
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

	// Feed response bytes via mock. While gateway txn is active these
	// are delivered to activeCh and consumed by ReadByte.
	bytes := []byte{0x10, 0x08, 0xB5}
	for _, b := range bytes {
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: b}
	}
	time.Sleep(30 * time.Millisecond)

	at := mux.ActiveTransport()
	for range bytes {
		if _, err := at.ReadByte(); err != nil {
			t.Fatalf("ReadByte err=%v", err)
		}
	}

	snap := mux.ActiveTxnSnapshot()
	if snap.BytesRead < 3 {
		t.Fatalf("BytesRead = %d, want >= 3", snap.BytesRead)
	}
}

// TestActiveTxnDiag_InactiveReason_SYNIdle proves that the first SYN
// during gateway ownership marks the txn inactive with ReasonSYNIdle.
func TestActiveTxnDiag_InactiveReason_SYNIdle(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)

	// Feed the end-of-transaction SYN.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)

	snap := mux.ActiveTxnSnapshot()
	if snap.Active {
		t.Fatal("Active must be false after SYN")
	}
	if snap.InactiveReason != ReasonSYNIdle {
		t.Fatalf("InactiveReason = %q, want %q", snap.InactiveReason, ReasonSYNIdle)
	}
	if snap.InactiveAt.IsZero() {
		t.Fatal("InactiveAt must be set")
	}
}

// TestActiveTxnDiag_InactiveReason_Reset proves handleReset marks
// the txn inactive with ReasonReset.
func TestActiveTxnDiag_InactiveReason_Reset(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()

	grantGateway(t, mux, mock, 0x71)

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

	// First SYN — idle.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)

	// Then a handleReset — should NOT override the original reason.
	mux.handleReset()

	snap := mux.ActiveTxnSnapshot()
	if snap.InactiveReason != ReasonSYNIdle {
		t.Fatalf("first reason should stick: got %q, want %q", snap.InactiveReason, ReasonSYNIdle)
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
	// producing no response (broken slave / wiring / partition).
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
		ReadTimeout:           200 * time.Millisecond,
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
		mock.Close()
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

		// NO response bytes arrive — simulate dead slave.
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
	// After the final SYN, txn is inactive with a SYN-based reason.
	if snap.Active {
		t.Fatal("last txn must be inactive after final SYN")
	}
	if snap.InactiveReason != ReasonSYNIdle && snap.InactiveReason != ReasonSYNTimeout {
		t.Fatalf("InactiveReason = %q, want syn_idle or syn_timeout", snap.InactiveReason)
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

	// Feed txn bytes + end-of-txn SYN + third-party noise + release.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x10}
	time.Sleep(15 * time.Millisecond)
	drainActiveChTest(mux) // simulate bus.Send consuming
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)

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
