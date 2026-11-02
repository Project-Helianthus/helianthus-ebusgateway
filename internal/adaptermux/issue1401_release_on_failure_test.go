package adaptermux

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// Issue #1401 fault-injection coverage: every failure path of a gateway
// transaction releases the gateway's bus ownership at the failure itself
// (no SYN has to arrive, so the release is within one SYN interval), the
// MaxOwnershipDuration cap never fires on these paths, and nothing the dead
// transaction left in activeCh reaches the next grant.

const issue1401Initiator byte = 0x10

// assertGatewayReleasedBy checks the state right after a failure path: the
// gateway transaction is inactive with the expected reason, the gateway no
// longer owns the bus, the reason was counted and the cap did not fire.
func assertGatewayReleasedBy(t *testing.T, mux *Mux, reason ActiveTxnInactiveReason) {
	t.Helper()
	snap := mux.ActiveTxnSnapshot()
	if snap.Active {
		t.Fatalf("gateway transaction still active after %s", reason)
	}
	if mux.arb.isOwner(gatewaySessionID) {
		t.Fatalf("gateway still owns the bus after %s", reason)
	}
	if snap.InactiveReason != reason {
		t.Fatalf("inactive reason = %q, want %q", snap.InactiveReason, reason)
	}
	if got := snap.InactiveReasonCounts[reason]; got != 1 {
		t.Fatalf("%s endings = %d, want 1", reason, got)
	}
	if got := snap.InactiveReasonCounts[ReasonMaxOwnership]; got != 0 {
		t.Fatalf("max_ownership endings = %d, want 0 (the cap must not be the exit)", got)
	}
}

// regrantGatewayClean grants the gateway again through the arbitrator and
// asserts that the new grant found no stale bytes to drain.
func regrantGatewayClean(t *testing.T, mux *Mux, mock *p3MockTransport) {
	t.Helper()
	grantGateway(t, mux, mock, issue1401Initiator)
	if got := mux.ActiveTxnSnapshot().DrainedOnGrant; got != 0 {
		t.Fatalf("next grant drained %d stale bytes, want 0", got)
	}
}

// TestIssue1401_OneByteFailureReleasedOnRebid models the bus-detected
// failure after one byte (for example an echo mismatch): protocol.Bus gives
// up without a transport-visible terminal and bids again. The re-bid ends
// the abandoned transaction, releases the grant before any SYN, and discards
// the byte the dead transaction left in activeCh instead of handing it to
// the next grant.
func TestIssue1401_OneByteFailureReleasedOnRebid(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()
	grantGateway(t, mux, mock, issue1401Initiator)
	at := mux.ActiveTransport()
	if _, err := at.Write([]byte{0x15}); err != nil {
		t.Fatalf("Write = %v", err)
	}
	// The echo is delivered but never consumed: the bus already gave up.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x15}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && len(mux.activeCh) == 0 {
		time.Sleep(time.Millisecond)
	}
	if len(mux.activeCh) != 1 {
		t.Fatalf("activeCh holds %d events before the re-bid, want the 1 unread echo", len(mux.activeCh))
	}

	retry := mux.requestStartForSession(gatewaySessionID, issue1401Initiator)

	assertGatewayReleasedBy(t, mux, ReasonAbandonedOnRebid)
	snap := mux.ActiveTxnSnapshot()
	if snap.StaleActiveBytesDiscarded != 1 || len(mux.activeCh) != 0 {
		t.Fatalf("stale discarded/left in activeCh = %d/%d, want 1/0", snap.StaleActiveBytesDiscarded, len(mux.activeCh))
	}
	if got := snap.TxnClass; got != TxnClassEchoOnlyTimeout {
		t.Fatalf("abandoned transaction class = %q, want %q", got, TxnClassEchoOnlyTimeout)
	}

	// The queued re-bid is admitted at the next SYN and starts clean.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	time.Sleep(30 * time.Millisecond)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: issue1401Initiator}
	select {
	case r := <-retry:
		if !r.granted {
			t.Fatalf("re-bid result = %+v, want granted", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("re-bid was not granted")
	}
	if got := mux.ActiveTxnSnapshot().DrainedOnGrant; got != 0 {
		t.Fatalf("re-bid grant drained %d stale bytes, want 0", got)
	}
}

// TestIssue1401_RebidAfterNormalEndKeepsGraceTail pins the boundary of the
// re-bid rule: a grant whose transaction already ended normally keeps its
// IdleReleaseGrace tail, so healthy back-to-back gateway transactions see
// unchanged arbitration timing.
func TestIssue1401_RebidAfterNormalEndKeepsGraceTail(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()
	mux.cfg.IdleReleaseGrace = 10 * time.Second // set before the first event reaches readLoop
	grantGateway(t, mux, mock, issue1401Initiator)
	completeGatewayTxn(t, mux, mock)

	mux.requestStartForSession(gatewaySessionID, issue1401Initiator)
	if !mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("re-bid after a normal end released the grace tail")
	}
	snap := mux.ActiveTxnSnapshot()
	if snap.InactiveReason != ReasonSYNTerminator || snap.InactiveReasonCounts[ReasonAbandonedOnRebid] != 0 {
		t.Fatalf("reason/abandoned count = %q/%d, want %q/0", snap.InactiveReason, snap.InactiveReasonCounts[ReasonAbandonedOnRebid], ReasonSYNTerminator)
	}
}

// completeGatewayTxn drives one normal gateway transaction to its SYN
// terminator and consumes every byte the active path delivered.
func completeGatewayTxn(t *testing.T, mux *Mux, mock *p3MockTransport) {
	t.Helper()
	at := mux.ActiveTransport()
	if _, err := at.Write([]byte{0x15}); err != nil {
		t.Fatalf("Write = %v", err)
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x15}
	if b, err := at.ReadByte(); err != nil || b != 0x15 {
		t.Fatalf("echo = (0x%02X, %v), want (0x15, nil)", b, err)
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolAck}
	if b, err := at.ReadByte(); err != nil || b != protocol.SymbolAck {
		t.Fatalf("response = (0x%02X, %v), want (ACK, nil)", b, err)
	}
	gatewayEndOfMessage(t, at)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	if b, err := at.ReadByte(); err != nil || b != protocol.SymbolSyn {
		t.Fatalf("terminator = (0x%02X, %v), want (SYN, nil)", b, err)
	}
	snap := mux.ActiveTxnSnapshot()
	if snap.Active || snap.InactiveReason != ReasonSYNTerminator {
		t.Fatalf("transaction active/reason = %v/%q, want false/%q", snap.Active, snap.InactiveReason, ReasonSYNTerminator)
	}
}

// TestIssue1401_ForeignInitiatorDuringOwnershipReleasesAtOnce covers point 2:
// foreign initiator bytes while session 0 owns the bus are a lost
// arbitration and release the grant at once.
func TestIssue1401_ForeignInitiatorDuringOwnershipReleasesAtOnce(t *testing.T) {
	const foreignInitiator byte = 0xF1

	t.Run("first echo of a live transaction", func(t *testing.T) {
		mux, mock, _, cleanup := newP3TestMux(t)
		defer cleanup()
		grantGateway(t, mux, mock, issue1401Initiator)
		at := mux.ActiveTransport()
		if _, err := at.Write([]byte{0x15}); err != nil {
			t.Fatalf("Write = %v", err)
		}
		// A stale byte already queued when the loss is detected must not
		// survive it: protocol.Bus has to read the foreign initiator first.
		mux.activeCh <- activeEvent{kind: activeEventByte, b: 0x00}

		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: foreignInitiator}
		// Read activeCh only after readLoop processed the loss; the stale
		// byte is discarded in the same critical section as the release.
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) && mux.activeTxn.foreignInitiatorReleases.Load() == 0 {
			time.Sleep(time.Millisecond)
		}
		expectActiveByte(t, mux, foreignInitiator)

		assertGatewayReleasedBy(t, mux, ReasonArbitrationCollision)
		snap := mux.ActiveTxnSnapshot()
		if snap.ForeignInitiatorReleases != 1 || snap.StaleActiveBytesDiscarded != 1 {
			t.Fatalf("foreign releases/stale discarded = %d/%d, want 1/1", snap.ForeignInitiatorReleases, snap.StaleActiveBytesDiscarded)
		}
		if snap.LastTxnClass != TxnClassNonEchoInvalidFrame {
			t.Fatalf("lost transaction class = %q, want %q", snap.LastTxnClass, TxnClassNonEchoInvalidFrame)
		}

		// Exactly the owed resynchronization SYNs reach protocol.Bus; a
		// surplus SYN does not.
		for i := 0; i < protocol.CollisionRetryResyncSYNCount+1; i++ {
			mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
		}
		for i := 0; i < protocol.CollisionRetryResyncSYNCount; i++ {
			expectActiveByte(t, mux, protocol.SymbolSyn)
		}
		expectNoActiveByte(t, mux)
		regrantGatewayClean(t, mux, mock)
	})

	t.Run("post-transaction ownership tail", func(t *testing.T) {
		mux, mock, _, cleanup := newP3TestMux(t)
		defer cleanup()
		var passiveMu sync.Mutex
		var passive []byte
		mux.SetPassiveCallback(func(ev PassiveEvent) {
			if ev.Kind == PassiveEventSymbol {
				passiveMu.Lock()
				passive = append(passive, ev.Symbol)
				passiveMu.Unlock()
			}
		})
		mux.cfg.IdleReleaseGrace = 10 * time.Second // keep the tail open; set before the first event
		grantGateway(t, mux, mock, issue1401Initiator)
		completeGatewayTxn(t, mux, mock)
		if !mux.arb.isOwner(gatewaySessionID) {
			t.Fatal("precondition: grant released before the foreign telegram")
		}

		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: issue1401Initiator}
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) && mux.arb.isOwner(gatewaySessionID) {
			time.Sleep(time.Millisecond)
		}
		if mux.arb.isOwner(gatewaySessionID) {
			t.Fatal("foreign initiator in the ownership tail did not release the grant")
		}
		snap := mux.ActiveTxnSnapshot()
		if snap.InactiveReason != ReasonSYNTerminator || snap.ForeignInitiatorReleases != 1 {
			t.Fatalf("reason/foreign releases = %q/%d, want %q/1 (the transaction itself ended normally)",
				snap.InactiveReason, snap.ForeignInitiatorReleases, ReasonSYNTerminator)
		}
		mux.stateMu.Lock()
		deferred := mux.deferRegrantUntilNextSyn
		mux.stateMu.Unlock()
		if !deferred {
			t.Fatal("regrant was not deferred while the foreign telegram is on the wire")
		}
		expectNoActiveByte(t, mux)
		deadline = time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			passiveMu.Lock()
			seen := len(passive) > 0 && passive[len(passive)-1] == issue1401Initiator
			passiveMu.Unlock()
			if seen {
				break
			}
			time.Sleep(time.Millisecond)
		}
		passiveMu.Lock()
		got := append([]byte(nil), passive...)
		passiveMu.Unlock()
		if len(got) == 0 || got[len(got)-1] != issue1401Initiator {
			t.Fatalf("passive symbols = % X, want the foreign initiator 0x%02X as third-party traffic", got, issue1401Initiator)
		}
	})

	t.Run("stale byte in the grant window keeps the grant", func(t *testing.T) {
		mux, mock, _, cleanup := newP3TestMux(t)
		defer cleanup()
		grantGateway(t, mux, mock, issue1401Initiator)
		before := len(mux.ActiveTxnSnapshot().ReadPrefix)
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: foreignInitiator}
		// The byte is processed (afterInactive counts it) without a release.
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) && mux.ActiveTxnSnapshot().AfterInactive == 0 {
			time.Sleep(time.Millisecond)
		}
		snap := mux.ActiveTxnSnapshot()
		if snap.AfterInactive == 0 || len(snap.ReadPrefix) != before {
			t.Fatalf("afterInactive/readPrefix = %d/%d, want the byte observed outside the transaction", snap.AfterInactive, len(snap.ReadPrefix))
		}
		if !mux.arb.isOwner(gatewaySessionID) || snap.ForeignInitiatorReleases != 0 {
			t.Fatal("a stale byte before the first write released a fresh grant")
		}
	})
}

// TestIssue1401_ActiveReadTimeoutReleasesAtOnce covers the one-write,
// zero-read shape from the issue: the echo never arrives, the active read
// times out, and the grant is released at the timeout rather than at the
// next idle SYN or at the cap.
func TestIssue1401_ActiveReadTimeoutReleasesAtOnce(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()
	grantGateway(t, mux, mock, issue1401Initiator)
	at := mux.ActiveTransport()
	if _, err := at.Write([]byte{0x15}); err != nil {
		t.Fatalf("Write = %v", err)
	}
	if _, err := at.ReadByte(); !errors.Is(err, ebuserrors.ErrTimeout) {
		t.Fatalf("ReadByte error = %v, want ErrTimeout", err)
	}
	assertGatewayReleasedBy(t, mux, ReasonActiveReadTimeout)

	// The late echo of the dead write is not delivered and therefore cannot
	// be drained into the next grant.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x15}
	expectNoActiveByte(t, mux)
	regrantGatewayClean(t, mux, mock)
}

// TestIssue1401_ContextCancelReleasesAtOnce: cancelling the mux context
// while protocol.Bus waits for an echo ends the transaction and releases the
// grant in the same step.
func TestIssue1401_ContextCancelReleasesAtOnce(t *testing.T) {
	mux, mock, cancel, cleanup := newP3TestMux(t)
	defer cleanup()
	grantGateway(t, mux, mock, issue1401Initiator)
	at := mux.ActiveTransport()
	if _, err := at.Write([]byte{0x15}); err != nil {
		t.Fatalf("Write = %v", err)
	}
	readErr := make(chan error, 1)
	go func() {
		_, err := at.ReadByte()
		readErr <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("ReadByte succeeded after context cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("ReadByte did not observe context cancellation")
	}
	assertGatewayReleasedBy(t, mux, ReasonContextCancel)
}

// failingWriteP3Transport fails adapter writes on demand.
type failingWriteP3Transport struct {
	*p3MockTransport
	fail atomic.Bool
}

func (f *failingWriteP3Transport) Write(p []byte) (int, error) {
	if f.fail.Load() {
		return 0, errors.New("simulated adapter write failure")
	}
	return f.p3MockTransport.Write(p)
}

// TestIssue1401_WriteErrorReleasesAtOnce: an adapter write failure after the
// first byte releases the grant in the write path itself.
func TestIssue1401_WriteErrorReleasesAtOnce(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()
	failing := &failingWriteP3Transport{p3MockTransport: mock}
	mux.connMu.Lock()
	mux.upstream = failing
	mux.connMu.Unlock()

	grantGateway(t, mux, mock, issue1401Initiator)
	at := mux.ActiveTransport()
	if _, err := at.Write([]byte{0x15}); err != nil {
		t.Fatalf("first Write = %v", err)
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x15}
	if b, err := at.ReadByte(); err != nil || b != 0x15 {
		t.Fatalf("echo = (0x%02X, %v), want (0x15, nil)", b, err)
	}
	failing.fail.Store(true)
	if _, err := at.Write([]byte{0xB5}); !errors.Is(err, errAdapterWrite) {
		t.Fatalf("second Write error = %v, want errAdapterWrite", err)
	}
	assertGatewayReleasedBy(t, mux, ReasonActiveWriteError)
	if got := mux.ActiveTxnSnapshot().WriteErrTotal; got != 1 {
		t.Fatalf("write errors = %d, want 1", got)
	}
	failing.fail.Store(false)
	regrantGatewayClean(t, mux, mock)
}

// TestIssue1401_MaxOwnershipDefaultIsLastResortGuard pins point 3: the
// default gateway cap is 1.5 s, explicit limits are kept, and a stuck grant that no
// failure path ended is released by the cap once it is exceeded (and not
// before).
func TestIssue1401_MaxOwnershipDefaultIsLastResortGuard(t *testing.T) {
	if got := New(Config{}).ownershipDurationLimit(gatewaySessionID); got != 1500*time.Millisecond {
		t.Fatalf("default gateway ownership limit = %v, want 1.5s", got)
	}
	if got := New(Config{MaxOwnershipDuration: 7 * time.Second}).cfg.MaxOwnershipDuration; got != 7*time.Second {
		t.Fatalf("explicit MaxOwnershipDuration = %v, want 7s", got)
	}

	if got := New(Config{}).cfg.MaxOwnershipDuration; got != 10*time.Second {
		t.Fatalf("shared MaxOwnershipDuration = %v, want 10s", got)
	}
	if got := New(Config{GatewayMaxOwnershipDuration: 7 * time.Second}).ownershipDurationLimit(gatewaySessionID); got != 7*time.Second {
		t.Fatalf("explicit gateway ownership limit = %v, want 7s", got)
	}
	if got := New(Config{MaxOwnershipDuration: time.Second}).ownershipDurationLimit(gatewaySessionID); got != time.Second {
		t.Fatalf("shorter shared ownership limit = %v, want 1s", got)
	}

	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()
	grantGateway(t, mux, mock, issue1401Initiator)
	if _, err := mux.ActiveTransport().Write([]byte{0x15}); err != nil {
		t.Fatalf("Write = %v", err)
	}
	// Non-initiator noise mid-write is a strict P11 drop; below the cap it
	// must not end the grant.
	time.Sleep(time.Second)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x55}
	time.Sleep(30 * time.Millisecond)
	if !mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("grant released below MaxOwnershipDuration")
	}
	time.Sleep(600 * time.Millisecond)
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x55}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && mux.arb.isOwner(gatewaySessionID) {
		time.Sleep(time.Millisecond)
	}
	snap := mux.ActiveTxnSnapshot()
	if mux.arb.isOwner(gatewaySessionID) || snap.InactiveReason != ReasonMaxOwnership {
		t.Fatalf("owner/reason after the cap = %v/%q, want false/%q", mux.arb.isOwner(gatewaySessionID), snap.InactiveReason, ReasonMaxOwnership)
	}
	if got := snap.InactiveReasonCounts[ReasonMaxOwnership]; got != 1 {
		t.Fatalf("max_ownership endings = %d, want 1", got)
	}
}

// TestIssue1401_InactiveReasonCountsByReason pins point 5. The key is the
// reason: each known reason increments only its own bucket (catches a wrong
// or missing index mapping), a later reason for the same transaction does not
// count again (catches double counting under first-reason-wins), and the
// snapshot lists every known reason.
func TestIssue1401_InactiveReasonCountsByReason(t *testing.T) {
	mux := New(Config{})
	mux.stateMu.Lock()
	for i, reason := range inactiveReasons {
		mux.recordGatewayGrant(issue1401Initiator, 0)
		mux.recordGatewayInactive(reason)
		mux.recordGatewayInactive(ReasonReconnect) // ignored: first reason wins
		for j, other := range inactiveReasons {
			want := uint64(0)
			if j <= i {
				want = 1
			}
			if got := mux.activeTxn.inactiveByReason[j].Load(); got != want {
				mux.stateMu.Unlock()
				t.Fatalf("after %d endings, %s count = %d, want %d", i+1, other, got, want)
			}
		}
	}
	mux.stateMu.Unlock()
	counts := mux.ActiveTxnSnapshot().InactiveReasonCounts
	if len(counts) != len(inactiveReasons) {
		t.Fatalf("snapshot lists %d reasons, want %d", len(counts), len(inactiveReasons))
	}
	for _, reason := range inactiveReasons {
		if counts[reason] != 1 {
			t.Fatalf("snapshot %s = %d, want 1", reason, counts[reason])
		}
	}
}

// External grants keep their existing budget even when logical wire activity
// arrives after the gateway's shorter last-resort limit.
func TestIssue1401_ExternalOwnershipBudgetThroughLogicalReceive(t *testing.T) {
	const externalSessionID = uint64(7)
	for _, tc := range []struct {
		name      string
		age       time.Duration
		wantOwner bool
	}{
		{"active_after_gateway_cap", 5 * time.Second, true},
		{"shared_cap_still_releases", 11 * time.Second, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mux := New(Config{})
			mux.arb.confirmOwnership(externalSessionID, 0x31)
			mux.busOwned = time.Now().Add(-tc.age)
			mux.lastWireActivity = time.Now()
			// Use the full logical receive path: direct onSYNLocked calls
			// bypass the hard ownership timeout.
			mux.onLogicalReceived(0x31, false, true)
			if got := mux.arb.isOwner(externalSessionID); got != tc.wantOwner {
				t.Fatalf("external ownership after %v grant = %v, want %v", tc.age, got, tc.wantOwner)
			}
			if tc.wantOwner {
				mux.onLogicalReceived(protocol.SymbolSyn, false, true)
				if !mux.arb.isOwner(externalSessionID) {
					t.Fatal("fresh wire activity must keep the external grant through SYN")
				}
			}
		})
	}
}
