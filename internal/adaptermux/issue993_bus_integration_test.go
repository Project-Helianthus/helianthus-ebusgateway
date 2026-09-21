package adaptermux

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
)

// TestIssue993_ProtocolBusRetriesAfterFirstForeignInitiator proves the whole
// active path, rather than mirroring the mux predicate: the pinned protocol
// bus receives the first foreign initiator byte through AdapterMux, classifies it
// as collision, consumes its bounded resynchronization SYNs, suppresses one
// surplus queued SYN, then retries a broadcast transaction exactly once and
// completes it with real mux echoes.
func TestIssue993_ProtocolBusRetriesAfterFirstForeignInitiator(t *testing.T) {
	const (
		gatewaySource    byte = 0x10
		foreignInitiator byte = 0xF1
	)

	if got := protocol.AddressClassOf(foreignInitiator); got != protocol.AddressClassMaster {
		t.Fatalf("test premise: AddressClassOf(0x%02X) = %v, want initiator", foreignInitiator, got)
	}

	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()
	// Start from a confirmed idle boundary so the first protocol.Bus START
	// takes AdapterMux's normal immediate idle-grant path deterministically.
	mux.stateMu.Lock()
	mux.lastWireActivity = time.Time{}
	mux.stateMu.Unlock()

	var starts atomic.Uint32
	mock.setRequestStartHook(func(initiator byte) {
		if initiator != gatewaySource {
			t.Errorf("RequestStart initiator = 0x%02X, want 0x%02X", initiator, gatewaySource)
		}
		if starts.Load() == 1 {
			if got := mux.LastTxnClass(); got != string(TxnClassNonEchoInvalidFrame) {
				t.Errorf("collision terminal class = %q, want %q", got, TxnClassNonEchoInvalidFrame)
			}
			if mux.arb.isOwner(gatewaySessionID) {
				t.Error("collision retry START retained prior gateway ownership")
			}
		}
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: initiator}
		starts.Add(1)
	})

	var writes atomic.Uint32
	mock.setWriteHook(func(wireByte byte) {
		if writes.Add(1) == 1 {
			// The first observable byte after a fresh grant is a foreign
			// initiator, before the expected source echo. This must reach
			// protocol.Bus; strict P11 still drops every other mismatch.
			mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: foreignInitiator}
			// Queue SYN3 with the recovery envelope. It is surplus while the
			// lifecycle decision is pending and must stay out of protocol.Bus.
			for range 2 {
				mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
			}
			return
		}
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: wireByte}
	})

	var eventsMu sync.Mutex
	var events []protocol.BusEvent
	config := protocol.DefaultBusConfig()
	config.ReconnectRetries = 0
	config.Observer = protocol.BusObserverFunc(func(event protocol.BusEvent) error {
		eventsMu.Lock()
		events = append(events, event)
		eventsMu.Unlock()
		return nil
	})

	bus := protocol.NewBus(mux.ActiveTransport(), config, 8)
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	bus.Run(runCtx)

	requestCtx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := bus.Send(requestCtx, protocol.Frame{
			Source:    gatewaySource,
			Target:    protocol.AddressBroadcast,
			Primary:   0xB5,
			Secondary: 0x16,
		})
		result <- err
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mux.stateMu.Lock()
		ready := !mux.gatewayTxnActive && mux.deferRegrantUntilNextSyn
		mux.stateMu.Unlock()
		if ready {
			break
		}
		time.Sleep(time.Millisecond)
	}
	mux.stateMu.Lock()
	ready := !mux.gatewayTxnActive && mux.deferRegrantUntilNextSyn
	mux.stateMu.Unlock()
	if !ready {
		t.Fatal("collision lifecycle did not terminalize before SYN3")
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("arbitration starts before SYN3 = %d, want 1", got)
	}
	// SYN4 is a new post-terminal fairness boundary; it may admit the queued
	// retry through the ordinary arbitrator.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Send error = %v; want successful bounded collision retry", err)
		}
	case <-requestCtx.Done():
		t.Fatalf("Send did not finish within bounded request context: %v", requestCtx.Err())
	}

	if got := starts.Load(); got != 2 {
		t.Fatalf("arbitration starts = %d, want 2 (one collision retry)", got)
	}
	if got := writes.Load(); got != 8 {
		t.Fatalf("wire writes = %d, want 8 (one collided source + seven retry bytes)", got)
	}

	eventsMu.Lock()
	defer eventsMu.Unlock()
	var retries, collisions, completes, resyncSYNs int
	for _, event := range events {
		if event.Kind == protocol.BusEventRetry && event.Outcome == protocol.BusOutcomeCollision {
			retries++
		}
		if event.Kind == protocol.BusEventRequestComplete && event.Outcome == protocol.BusOutcomeCollision {
			collisions++
		}
		if event.Kind == protocol.BusEventRequestComplete && event.Outcome == protocol.BusOutcomeSuccess {
			completes++
		}
		if event.Kind == protocol.BusEventRX && event.Byte == protocol.SymbolSyn {
			resyncSYNs++
		}
	}
	if retries != 1 {
		t.Fatalf("collision retries = %d, want 1", retries)
	}
	if collisions != 0 {
		t.Fatalf("collision request completions = %d, want 0 after recovery", collisions)
	}
	if completes != 1 {
		t.Fatalf("successful request completions = %d, want 1", completes)
	}
	// Two collision-resynchronization SYNs plus the retry transaction's
	// structural terminator are visible to protocol.Bus. A fourth would
	// prove the queued surplus escaped the mux recovery boundary.
	if resyncSYNs != 3 {
		t.Fatalf("SYNs delivered to protocol.Bus = %d, want 3", resyncSYNs)
	}
}

// TestIssue993_CollisionRecoverySurvivesActiveReadTimeout proves the real
// waitForSyn behavior of protocol.Bus: its bounded active-path timeout is a
// retryable absence of a SYN, not a lifecycle terminal. The two structural
// SYNs may arrive after that timeout and still complete exactly one recovery.
func TestIssue993_CollisionRecoverySurvivesActiveReadTimeout(t *testing.T) {
	const gatewaySource, foreignInitiator byte = 0x10, 0xF1

	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()
	mux.stateMu.Lock()
	mux.lastWireActivity = time.Time{}
	mux.stateMu.Unlock()

	var starts atomic.Uint32
	mock.setRequestStartHook(func(initiator byte) {
		if initiator != gatewaySource {
			t.Errorf("RequestStart initiator = 0x%02X, want 0x%02X", initiator, gatewaySource)
		}
		starts.Add(1)
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: initiator}
	})
	var writes atomic.Uint32
	mock.setWriteHook(func(wireByte byte) {
		if writes.Add(1) == 1 {
			mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: foreignInitiator}
			return
		}
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: wireByte}
	})

	bus := protocol.NewBus(mux.ActiveTransport(), protocol.DefaultBusConfig(), 8)
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	bus.Run(runCtx)
	requestCtx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := bus.Send(requestCtx, protocol.Frame{Source: gatewaySource, Target: protocol.AddressBroadcast, Primary: 0xB5, Secondary: 0x16})
		result <- err
	}()

	deadline := time.Now().Add(activeChanTimeout + time.Second)
	for time.Now().Before(deadline) && mux.activeTxn.readTimeoutTot.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	if got := mux.activeTxn.readTimeoutTot.Load(); got == 0 {
		t.Fatal("waitForSyn did not observe an active-channel timeout")
	}
	at := mux.ActiveTransport().(*activeTransport)
	mux.stateMu.Lock()
	live := mux.gatewayTxnActive && mux.activeTxn.firstByteSuspectArbLoss.Load()
	mux.stateMu.Unlock()
	if !live || !mux.arb.isOwner(gatewaySessionID) || at.CollisionRecoveryToken() == 0 {
		t.Fatal("active-channel timeout tore down live collision recovery")
	}

	// These are the two protocol-owned recovery symbols after the timeout.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mux.stateMu.Lock()
		terminal := !mux.gatewayTxnActive && mux.deferRegrantUntilNextSyn
		mux.stateMu.Unlock()
		if terminal {
			break
		}
		time.Sleep(time.Millisecond)
	}
	mux.stateMu.Lock()
	terminal := !mux.gatewayTxnActive && mux.deferRegrantUntilNextSyn
	mux.stateMu.Unlock()
	if !terminal || mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("two recovery SYNs did not terminalize and release collision grant")
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("starts before post-terminal SYN = %d, want 1", got)
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Send error after timeout recovery = %v", err)
		}
	case <-requestCtx.Done():
		t.Fatalf("Send did not complete after timeout recovery: %v", requestCtx.Err())
	}
	if got := starts.Load(); got != 2 {
		t.Fatalf("starts = %d, want exactly 2", got)
	}
}

func TestIssue993_NonCollisionActiveReadTimeoutStillTerminates(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()
	grantGateway(t, mux, mock, 0x10)
	at := mux.ActiveTransport().(*activeTransport)
	if _, err := at.Write([]byte{0x10}); err != nil {
		t.Fatalf("Write = %v", err)
	}
	if _, err := at.ReadByte(); !errors.Is(err, ebuserrors.ErrTimeout) {
		t.Fatalf("ReadByte error = %v, want ErrTimeout", err)
	}
	mux.stateMu.Lock()
	active := mux.gatewayTxnActive
	reason := mux.activeTxn.inactiveReas
	mux.stateMu.Unlock()
	if active || reason != ReasonActiveReadTimeout || at.CollisionRecoveryToken() != 0 {
		t.Fatalf("ordinary timeout state active/reason/token = %v/%q/%d, want false/%q/0", active, reason, at.CollisionRecoveryToken(), ReasonActiveReadTimeout)
	}
}

func TestIssue993_CollisionRecoveryCancelAfterActiveReadTimeout(t *testing.T) {
	const gatewaySource, externalInitiator, foreignInitiator byte = 0x10, 0x31, 0xF1

	mux, mock, _, cleanup := newClassifiedTestMux(t, v8classifier.ModeShadow)
	defer cleanup()
	// Keep the queued external request fresh across the worker's second bounded
	// active-channel wait; F-24 staleness is a separate policy from this
	// transaction-bound cancellation proof.
	mux.cfg.ExternalStartStaleness = 3 * activeChanTimeout
	pacer := mux.SessionPacer(gatewaySessionID)
	if pacer == nil {
		t.Fatal("shadow classifier did not create gateway pacer")
	}
	mux.stateMu.Lock()
	mux.lastWireActivity = time.Time{}
	mux.stateMu.Unlock()

	client, server := net.Pipe()
	defer client.Close()
	externalID := mux.AddSession(server)
	if externalID == 0 {
		t.Fatal("AddSession returned zero external ID")
	}
	var gatewayStarts, externalStarts atomic.Uint32
	mock.setRequestStartHook(func(initiator byte) {
		switch initiator {
		case gatewaySource:
			gatewayStarts.Add(1)
		case externalInitiator:
			externalStarts.Add(1)
		default:
			t.Errorf("RequestStart initiator = 0x%02X", initiator)
		}
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: initiator}
	})
	var writes atomic.Uint32
	mock.setWriteHook(func(wireByte byte) {
		if writes.Add(1) == 1 {
			mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: foreignInitiator}
			return
		}
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: wireByte}
	})

	bus := protocol.NewBus(mux.ActiveTransport(), protocol.DefaultBusConfig(), 8)
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	bus.Run(runCtx)
	requestCtx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := bus.Send(requestCtx, protocol.Frame{Source: gatewaySource, Target: protocol.AddressBroadcast, Primary: 0xB5, Secondary: 0x16})
		result <- err
	}()

	deadline := time.Now().Add(activeChanTimeout + time.Second)
	for time.Now().Before(deadline) && mux.activeTxn.readTimeoutTot.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	if got := mux.activeTxn.readTimeoutTot.Load(); got == 0 {
		t.Fatal("waitForSyn did not reach the active-channel timeout")
	}
	at := mux.ActiveTransport().(*activeTransport)
	if at.CollisionRecoveryToken() == 0 || !mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("timeout lost live collision lifecycle before request cancellation")
	}
	if pacer.WatchdogArmed() {
		t.Fatal("collision recovery retained old watchdog across active-channel timeout")
	}
	if events, dropped := mux.V8Classifier().DrainAdminEvents(); dropped != 0 {
		t.Fatalf("collision recovery dropped admin events=%d", dropped)
	} else {
		for _, event := range events {
			if event.Kind == v8classifier.AdminEventKindEchoSoftTimeout || event.Kind == v8classifier.AdminEventKindEchoHardTimeout {
				t.Fatalf("collision recovery emitted stale watchdog event=%v", event)
			}
		}
	}
	// Queue the external bidder after the real no-SYN gap, so its normal F-24
	// freshness budget does not expire while protocol.Bus is still waiting.
	externalStart := mux.arb.requestStart(externalID, externalInitiator)
	if gatewayStarts.Load() != 1 || externalStarts.Load() != 0 {
		t.Fatalf("bidder started before cancellation boundary: gateway=%d external=%d", gatewayStarts.Load(), externalStarts.Load())
	}

	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Send error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Bus did not abandon collision recovery after request cancellation")
	}
	// Send returns promptly for its caller, while the bus worker is still
	// blocked in activeTransport.ReadByte. Its next bounded read observes the
	// cancelled request and invokes the transaction-bound Abandon decision.
	deadline = time.Now().Add(activeChanTimeout + time.Second)
	for time.Now().Before(deadline) {
		mux.stateMu.Lock()
		terminal := !mux.gatewayTxnActive && mux.deferRegrantUntilNextSyn
		mux.stateMu.Unlock()
		if terminal {
			break
		}
		time.Sleep(time.Millisecond)
	}
	mux.stateMu.Lock()
	active := mux.gatewayTxnActive
	deferred := mux.deferRegrantUntilNextSyn
	mux.stateMu.Unlock()
	if active || !deferred || mux.arb.isOwner(gatewaySessionID) || at.CollisionRecoveryToken() != 0 || pacer.WatchdogArmed() {
		t.Fatalf("Abandon state active/deferred/gateway-owner/token/watchdog = %v/%v/%v/%d/%v, want false/true/false/0/false", active, deferred, mux.arb.isOwner(gatewaySessionID), at.CollisionRecoveryToken(), pacer.WatchdogArmed())
	}

	// Recovery's terminal latch deliberately defers the already-queued external
	// bidder until a new normal SYN boundary.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	select {
	case granted := <-externalStart:
		if !granted.granted {
			t.Fatalf("external grant = %+v, want granted", granted)
		}
	case <-time.After(time.Second):
		t.Fatal("external bidder did not proceed at post-Abandon SYN boundary")
	}
	if externalStarts.Load() != 1 || !mux.arb.isOwner(externalID) {
		t.Fatal("ordinary fairness did not grant pending external bidder once")
	}
}

func TestIssue993_CollisionRecoveryLifecycleTokenGuards(t *testing.T) {
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()
	at := mux.ActiveTransport().(*activeTransport)
	if got := at.CollisionRecoveryToken(); got != 0 {
		t.Fatalf("precondition token = %d, want 0", got)
	}
	grantGateway(t, mux, mock, 0x10)
	if _, err := at.Write([]byte{0x10}); err != nil {
		t.Fatalf("Write = %v", err)
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xF1}
	expectActiveByte(t, mux, 0xF1)
	token := at.CollisionRecoveryToken()
	if token == 0 {
		t.Fatal("confirmed collision returned zero token")
	}
	at.CompleteCollisionRecovery(token, transport.CollisionRecoveryRetry)
	if !mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("early Retry released ownership")
	}
	mux.stateMu.Lock()
	mux.collisionResyncSYNs = protocol.CollisionRetryResyncSYNCount
	mux.stateMu.Unlock()
	at.CompleteCollisionRecovery(token, transport.CollisionRecoveryAbandon)
	if mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("Abandon retained gateway ownership")
	}
	mux.stateMu.Lock()
	deferred := mux.deferRegrantUntilNextSyn
	mux.stateMu.Unlock()
	if !deferred {
		t.Fatal("Abandon did not defer normal regrant until SYN boundary")
	}
	at.CompleteCollisionRecovery(token, transport.CollisionRecoveryAbandon)
	if got := at.CollisionRecoveryToken(); got != 0 {
		t.Fatalf("duplicate completion left token %d", got)
	}

	// A post-terminal SYN clears the deferral latch. A new transaction has a
	// different opaque identity, so completing the prior token cannot release
	// the fresh owner.
	mux.stateMu.Lock()
	_, shouldGrant, _, cancelWatchdog := mux.onSYNLocked(wirePhaseEventSYNIdle, 0, false, time.Now())
	mux.stateMu.Unlock()
	if cancelWatchdog {
		mux.cancelGatewayWatchdog()
	}
	if shouldGrant {
		mux.tryGrantAndStart()
	}
	grantGateway(t, mux, mock, 0x10)
	if _, err := at.Write([]byte{0x10}); err != nil {
		t.Fatalf("fresh Write = %v", err)
	}
	at.CompleteCollisionRecovery(token, transport.CollisionRecoveryAbandon)
	mux.stateMu.Lock()
	active := mux.gatewayTxnActive
	mux.stateMu.Unlock()
	if !mux.arb.isOwner(gatewaySessionID) || !active {
		t.Fatal("stale token after a new grant changed fresh gateway ownership")
	}
}

// TestIssue993_CollisionRecoveryIgnoresLaterForeignInitiator proves that the
// one first-byte exception cannot re-arm while protocol.Bus is still consuming
// its two SYN recovery symbols. A later initiator-class mismatch remains a
// strict P11 drop and must not reset the completed envelope.
func TestIssue993_CollisionRecoveryIgnoresLaterForeignInitiator(t *testing.T) {
	const gatewaySource, foreignInitiator byte = 0x10, 0xF1
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()
	mux.stateMu.Lock()
	mux.lastWireActivity = time.Time{}
	mux.stateMu.Unlock()

	var starts atomic.Uint32
	mock.setRequestStartHook(func(initiator byte) {
		if initiator != gatewaySource {
			t.Errorf("RequestStart initiator = 0x%02X", initiator)
		}
		if starts.Load() == 1 && mux.arb.isOwner(gatewaySessionID) {
			t.Error("retry START retained old collision ownership")
		}
		starts.Add(1)
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: initiator}
	})
	var writes atomic.Uint32
	mock.setWriteHook(func(wireByte byte) {
		if writes.Add(1) == 1 {
			mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: foreignInitiator}
			for range 2 {
				mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
			}
			// This is the next bus-slot initiator, before protocol.Bus has
			// re-bid. It must be strict P11 and never restart recovery.
			mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: foreignInitiator}
			return
		}
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: wireByte}
	})

	var eventsMu sync.Mutex
	var events []protocol.BusEvent
	config := protocol.DefaultBusConfig()
	config.ReconnectRetries = 0
	config.Observer = protocol.BusObserverFunc(func(event protocol.BusEvent) error {
		eventsMu.Lock()
		events = append(events, event)
		eventsMu.Unlock()
		return nil
	})
	bus := protocol.NewBus(mux.ActiveTransport(), config, 8)
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	bus.Run(runCtx)
	requestCtx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := bus.Send(requestCtx, protocol.Frame{Source: gatewaySource, Target: protocol.AddressBroadcast, Primary: 0xB5, Secondary: 0x16})
		result <- err
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mux.stateMu.Lock()
		terminal := !mux.gatewayTxnActive && mux.deferRegrantUntilNextSyn
		mux.stateMu.Unlock()
		if terminal {
			break
		}
		time.Sleep(time.Millisecond)
	}
	mux.stateMu.Lock()
	terminal := !mux.gatewayTxnActive && mux.deferRegrantUntilNextSyn
	mux.stateMu.Unlock()
	if !terminal {
		t.Fatal("collision did not terminalize before retry boundary")
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("starts before post-terminal SYN = %d, want 1", got)
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Send error = %v", err)
		}
	case <-requestCtx.Done():
		t.Fatalf("Send did not complete bounded retry: %v", requestCtx.Err())
	}
	if got := starts.Load(); got != 2 {
		t.Fatalf("arbitration starts = %d, want exactly 2", got)
	}
	if got := writes.Load(); got != 8 {
		t.Fatalf("writes = %d, want one collided and one successful request", got)
	}

	eventsMu.Lock()
	defer eventsMu.Unlock()
	var retries, foreignRX, success, collisionCompletion, synRX int
	for _, event := range events {
		if event.Kind == protocol.BusEventRetry && event.Outcome == protocol.BusOutcomeCollision {
			retries++
		}
		if event.Kind == protocol.BusEventRX && event.Byte == foreignInitiator {
			foreignRX++
		}
		if event.Kind == protocol.BusEventRX && event.Byte == protocol.SymbolSyn {
			synRX++
		}
		if event.Kind == protocol.BusEventRequestComplete && event.Outcome == protocol.BusOutcomeSuccess {
			success++
		}
		if event.Kind == protocol.BusEventRequestComplete && event.Outcome == protocol.BusOutcomeCollision {
			collisionCompletion++
		}
	}
	if retries != 1 || foreignRX != 1 || synRX != 3 || success != 1 || collisionCompletion != 0 {
		t.Fatalf("retry/foreign/SYN/success/collision = %d/%d/%d/%d/%d, want 1/1/3/1/0", retries, foreignRX, synRX, success, collisionCompletion)
	}
}

// TestIssue993_CollisionRecoveryDefersF26ForPendingExternal proves that a
// pending external bidder cannot consume the collision recovery boundary. The
// recovery SYNs belong to protocol.Bus; once its re-bid has atomically dropped
// the abandoned gateway ownership, ordinary F-26 fairness resumes.
func TestIssue993_CollisionRecoveryDefersF26ForPendingExternal(t *testing.T) {
	const gatewaySource, externalInitiator, foreignInitiator byte = 0x10, 0x31, 0xF1
	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()
	mux.stateMu.Lock()
	mux.lastWireActivity = time.Time{}
	mux.stateMu.Unlock()

	client, server := net.Pipe()
	defer client.Close()
	externalID := mux.AddSession(server)
	if externalID == 0 {
		t.Fatal("AddSession returned zero external ID")
	}

	var gatewayStarts atomic.Uint32
	var externalStarts atomic.Uint32
	mock.setRequestStartHook(func(initiator byte) {
		switch initiator {
		case gatewaySource:
			if gatewayStarts.Load() == 1 && mux.arb.isOwner(gatewaySessionID) {
				t.Error("retry START retained abandoned gateway ownership")
			}
			gatewayStarts.Add(1)
		case externalInitiator:
			externalStarts.Add(1)
		default:
			t.Errorf("RequestStart initiator = 0x%02X", initiator)
		}
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: initiator}
	})

	externalRequest := make(chan (<-chan startResult), 1)
	var writes atomic.Uint32
	mock.setWriteHook(func(wireByte byte) {
		if writes.Add(1) == 1 {
			externalRequest <- mux.arb.requestStart(externalID, externalInitiator)
			mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: foreignInitiator}
			for range 3 {
				mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
			}
			return
		}
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: wireByte}
	})

	var eventsMu sync.Mutex
	var events []protocol.BusEvent
	config := protocol.DefaultBusConfig()
	config.ReconnectRetries = 0
	config.Observer = protocol.BusObserverFunc(func(event protocol.BusEvent) error {
		eventsMu.Lock()
		events = append(events, event)
		eventsMu.Unlock()
		return nil
	})
	bus := protocol.NewBus(mux.ActiveTransport(), config, 8)
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	bus.Run(runCtx)

	requestCtx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := bus.Send(requestCtx, protocol.Frame{Source: gatewaySource, Target: protocol.AddressBroadcast, Primary: 0xB5, Secondary: 0x16})
		result <- err
	}()

	var externalStart <-chan startResult
	select {
	case externalStart = <-externalRequest:
	case <-requestCtx.Done():
		t.Fatalf("external bidder was not queued: %v", requestCtx.Err())
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mux.stateMu.Lock()
		terminal := !mux.gatewayTxnActive && mux.deferRegrantUntilNextSyn
		mux.stateMu.Unlock()
		if terminal {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if gatewayStarts.Load() != 1 || externalStarts.Load() != 0 {
		t.Fatalf("bidder started before terminal boundary: gateway=%d external=%d", gatewayStarts.Load(), externalStarts.Load())
	}
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Send error = %v", err)
		}
	case <-requestCtx.Done():
		t.Fatalf("Send did not complete: %v", requestCtx.Err())
	}
	if got := gatewayStarts.Load(); got != 2 {
		t.Fatalf("gateway starts = %d, want one retry", got)
	}

	// The retry completed and left the bus idle. A later normal SYN is the
	// fairness boundary that may grant the already-pending external session.
	mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: protocol.SymbolSyn}
	select {
	case granted := <-externalStart:
		if !granted.granted {
			t.Fatalf("external grant = %+v, want granted", granted)
		}
	case <-time.After(time.Second):
		t.Fatal("pending external bidder was not granted after retry boundary")
	}
	if got := externalStarts.Load(); got != 1 {
		t.Fatalf("external starts = %d, want 1", got)
	}
	if !mux.arb.isOwner(externalID) || mux.arb.isOwner(gatewaySessionID) {
		t.Fatal("ownership did not transfer exactly once to external bidder")
	}

	eventsMu.Lock()
	defer eventsMu.Unlock()
	var retries, syns int
	for _, event := range events {
		if event.Kind == protocol.BusEventRetry && event.Outcome == protocol.BusOutcomeCollision {
			retries++
		}
		if event.Kind == protocol.BusEventRX && event.Byte == protocol.SymbolSyn {
			syns++
		}
	}
	if retries != 1 || syns != 3 {
		t.Fatalf("retries/SYNs = %d/%d, want 1/3 (two recovery plus retry terminator)", retries, syns)
	}
}

// TestIssue993_CollisionHandoffCancelsAbandonedWatchdog keeps the collision
// handoff lifecycle-safe in both observing and enforcing V8 modes. It pauses
// the re-bid beyond the abandoned write's deadlines, then proves that the new
// retry write still arms and clears its own watchdog normally.
func TestIssue993_CollisionHandoffCancelsAbandonedWatchdog(t *testing.T) {
	for _, mode := range []v8classifier.Mode{v8classifier.ModeShadow, v8classifier.ModeEnforce} {
		t.Run(mode.String(), func(t *testing.T) {
			const gatewaySource byte = 0x10
			mux, mock, _, cleanup := newClassifiedTestMux(t, mode)
			defer cleanup()
			pacer := mux.SessionPacer(gatewaySessionID)
			classifier := mux.V8Classifier()
			if pacer == nil || classifier == nil {
				t.Fatal("classified mux did not create gateway pacer/classifier")
			}

			grantGateway(t, mux, mock, gatewaySource)
			if _, err := mux.ActiveTransport().Write([]byte{gatewaySource}); err != nil {
				t.Fatalf("initial Write = %v", err)
			}
			deadline := time.Now().Add(50 * time.Millisecond)
			for time.Now().Before(deadline) && !pacer.WatchdogArmed() {
				time.Sleep(time.Millisecond)
			}
			if !pacer.WatchdogArmed() {
				t.Fatal("precondition: abandoned write did not arm watchdog")
			}

			// The end-to-end foreign-byte predicate is covered above. This
			// seam starts after that transition and isolates terminal watchdog
			// teardown in both V8 modes.
			mux.stateMu.Lock()
			mux.activeTxn.firstByteSuspectArbLoss.Store(true)
			mux.collisionResyncSYNs = protocol.CollisionRetryResyncSYNCount
			mux.stateMu.Unlock()
			token := mux.ActiveTransport().(*activeTransport).CollisionRecoveryToken()
			if token == 0 {
				t.Fatal("live collision did not expose recovery token")
			}
			mux.ActiveTransport().(*activeTransport).CompleteCollisionRecovery(token, transport.CollisionRecoveryRetry)

			pausedRetryStart := make(chan struct{}, 1)
			allowRetryStart := make(chan struct{})
			var releaseRetryStartOnce sync.Once
			releaseRetryStart := func() {
				releaseRetryStartOnce.Do(func() { close(allowRetryStart) })
			}
			// Cleanup waits for the read loop. Every failing assertion below must
			// therefore release the deliberately paused RequestStart hook first.
			defer releaseRetryStart()
			mock.setRequestStartHook(func(initiator byte) {
				if initiator != gatewaySource {
					t.Errorf("RequestStart initiator = 0x%02X", initiator)
				}
				pausedRetryStart <- struct{}{}
				<-allowRetryStart
				mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventStarted, Data: initiator}
			})
			retryWrite := make(chan byte, 1)
			mock.setWriteHook(func(wireByte byte) {
				retryWrite <- wireByte // hold echo until the replacement watchdog is armed.
			})
			handoff := make(chan (<-chan startResult), 1)
			go func() {
				handoff <- mux.requestStartForSession(gatewaySessionID, gatewaySource)
			}()
			// Retry-ready is queued behind the lifecycle boundary until SYN3.
			deadline = time.Now().Add(time.Second)
			for time.Now().Before(deadline) && !mux.arb.hasPending() {
				time.Sleep(time.Millisecond)
			}
			if !mux.arb.hasPending() {
				t.Fatal("retry was not pending before SYN3")
			}
			mux.stateMu.Lock()
			_, shouldGrant, _, cancelWatchdog := mux.onSYNLocked(wirePhaseEventSYNIdle, 0, false, time.Now())
			mux.stateMu.Unlock()
			if cancelWatchdog {
				mux.cancelGatewayWatchdog()
			}
			if shouldGrant {
				// The test hook deliberately pauses RequestStart. Keep the
				// arbitration path asynchronous as the production read loop does.
				go mux.tryGrantAndStart()
			}

			select {
			case <-pausedRetryStart:
			case <-time.After(time.Second):
				t.Fatal("retry handoff did not reach paused START")
			}
			if pacer.WatchdogArmed() {
				t.Fatal("abandoned collision write retained its watchdog at retry handoff")
			}
			time.Sleep(450 * time.Millisecond)
			if got := pacer.SoftTimeoutTotal(); got != 0 {
				t.Fatalf("abandoned write soft timeouts = %d, want 0", got)
			}
			if got := pacer.HardTimeoutTotal(); got != 0 {
				t.Fatalf("abandoned write hard timeouts = %d, want 0", got)
			}
			if events, dropped := classifier.DrainAdminEvents(); dropped != 0 {
				t.Fatalf("abandoned write dropped admin events=%d", dropped)
			} else {
				for _, event := range events {
					if event.Kind == v8classifier.AdminEventKindEchoSoftTimeout || event.Kind == v8classifier.AdminEventKindEchoHardTimeout {
						t.Fatalf("abandoned write emitted watchdog event=%v", event)
					}
				}
			}

			releaseRetryStart()
			select {
			case result := <-handoff:
				select {
				case granted := <-result:
					if !granted.granted {
						t.Fatalf("retry grant = %+v", granted)
					}
				case <-time.After(time.Second):
					t.Fatal("retry STARTED did not resolve handoff")
				}
			case <-time.After(time.Second):
				t.Fatal("retry handoff did not return")
			}
			if _, err := mux.ActiveTransport().Write([]byte{gatewaySource}); err != nil {
				t.Fatalf("retry Write = %v", err)
			}
			var firstRetryByte byte
			select {
			case firstRetryByte = <-retryWrite:
			case <-time.After(time.Second):
				t.Fatal("retry wire write did not reach transport")
			}
			deadline = time.Now().Add(50 * time.Millisecond)
			for time.Now().Before(deadline) && !pacer.WatchdogArmed() {
				time.Sleep(time.Millisecond)
			}
			if !pacer.WatchdogArmed() {
				t.Fatal("retry write did not arm its watchdog")
			}
			mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: firstRetryByte}
			time.Sleep(10 * time.Millisecond)
			if pacer.WatchdogArmed() {
				t.Fatal("retry watchdog remained armed after matching echo")
			}
		})
	}
}

// TestIssue993_FirstByteExceptionLeavesP11Strict checks the neighboring
// shapes that must remain outside the recovery exception. These use the real
// active transport and readLoop but intentionally stop before protocol.Bus:
// their contract is byte routing, not retry policy.
func TestIssue993_FirstByteExceptionLeavesP11Strict(t *testing.T) {
	t.Run("non-initiator first noise remains dropped", func(t *testing.T) {
		mux, mock, _, cleanup := newP3TestMux(t)
		defer cleanup()
		grantGateway(t, mux, mock, 0x10)

		at := mux.ActiveTransport()
		if _, err := at.Write([]byte{0x10}); err != nil {
			t.Fatalf("Write = %v", err)
		}
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x55}
		expectNoActiveByte(t, mux)
		if mux.activeTxn.firstByteSuspectArbLoss.Load() {
			t.Fatal("non-initiator noise armed first-byte collision recovery")
		}
	})

	t.Run("initiator after one echo and later missing echo remain dropped", func(t *testing.T) {
		mux, mock, _, cleanup := newP3TestMux(t)
		defer cleanup()
		grantGateway(t, mux, mock, 0x10)

		at := mux.ActiveTransport()
		if _, err := at.Write([]byte{0x10}); err != nil {
			t.Fatalf("first Write = %v", err)
		}
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x10}
		if got, err := at.ReadByte(); err != nil || got != 0x10 {
			t.Fatalf("first echo = (0x%02X, %v), want (0x10, nil)", got, err)
		}

		if _, err := at.Write([]byte{0x15}); err != nil {
			t.Fatalf("second Write = %v", err)
		}
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xF1}
		expectNoActiveByte(t, mux)
		if mux.activeTxn.firstByteSuspectArbLoss.Load() {
			t.Fatal("post-echo initiator byte armed first-byte collision recovery")
		}

		// The later legitimate echo must still be accepted after the stale
		// initiator was filtered; P11 must not clear the expected-echo queue.
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x15}
		if got, err := at.ReadByte(); err != nil || got != 0x15 {
			t.Fatalf("later expected echo = (0x%02X, %v), want (0x15, nil)", got, err)
		}
	})

	t.Run("response phase still accepts interleaving", func(t *testing.T) {
		mux, mock, _, cleanup := newP3TestMux(t)
		defer cleanup()
		grantGateway(t, mux, mock, 0x10)

		at := mux.ActiveTransport()
		if _, err := at.Write([]byte{0x10}); err != nil {
			t.Fatalf("Write = %v", err)
		}
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x10}
		if got, err := at.ReadByte(); err != nil || got != 0x10 {
			t.Fatalf("echo = (0x%02X, %v), want (0x10, nil)", got, err)
		}

		// With no pending gateway echo, response bytes retain the normal
		// active-path behavior; F-NEW-29 must not over-filter this phase.
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0x00}
		if got, err := at.ReadByte(); err != nil || got != 0x00 {
			t.Fatalf("response interleave = (0x%02X, %v), want (0x00, nil)", got, err)
		}
	})
}

func TestIssue993_CollisionResyncStateClearsAtTerminalBoundaries(t *testing.T) {
	armCollisionRecovery := func(t *testing.T, mux *Mux, mock *p3MockTransport) (*activeTransport, uint64) {
		t.Helper()
		grantGateway(t, mux, mock, 0x10)
		at := mux.ActiveTransport().(*activeTransport)
		if _, err := at.Write([]byte{0x10}); err != nil {
			t.Fatalf("Write = %v", err)
		}
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xF1}
		expectActiveByte(t, mux, 0xF1)
		mux.stateMu.Lock()
		mux.collisionResyncSYNs = 1
		mux.stateMu.Unlock()
		token := at.CollisionRecoveryToken()
		if token == 0 {
			t.Fatal("armed recovery returned zero token")
		}
		return at, token
	}

	assertCleared := func(t *testing.T, mux *Mux) {
		t.Helper()
		mux.stateMu.Lock()
		defer mux.stateMu.Unlock()
		if got := mux.collisionResyncSYNs; got != 0 {
			t.Fatalf("collision resync SYN count = %d, want 0 after terminal boundary", got)
		}
	}

	assertInvalidated := func(t *testing.T, mux *Mux, mock *p3MockTransport, at *activeTransport, token uint64) {
		t.Helper()
		if got := at.CollisionRecoveryToken(); got != 0 {
			t.Fatalf("terminal boundary retained token %d", got)
		}
		// Force a fresh ordinary grant after the terminal boundary, then prove
		// a stale completion cannot release or reactivate that transaction.
		mux.arb.forceRelease()
		grantGateway(t, mux, mock, 0x10)
		if _, err := at.Write([]byte{0x10}); err != nil {
			t.Fatalf("fresh Write = %v", err)
		}
		at.CompleteCollisionRecovery(token, transport.CollisionRecoveryAbandon)
		mux.stateMu.Lock()
		active := mux.gatewayTxnActive
		mux.stateMu.Unlock()
		if !mux.arb.isOwner(gatewaySessionID) || !active {
			t.Fatal("stale completion modified later gateway grant")
		}
	}

	t.Run("context cancellation", func(t *testing.T) {
		mux, mock, _, cleanup := newP3TestMux(t)
		defer cleanup()
		at, token := armCollisionRecovery(t, mux, mock)
		mux.markActiveContextCancel()
		assertCleared(t, mux)
		assertInvalidated(t, mux, mock, at, token)
	})

	t.Run("adapter reset", func(t *testing.T) {
		mux, mock, _, cleanup := newP3TestMux(t)
		defer cleanup()
		at, token := armCollisionRecovery(t, mux, mock)
		mux.handleReset()
		assertCleared(t, mux)
		assertInvalidated(t, mux, mock, at, token)
	})

	t.Run("reconnect and ownership teardown share terminal cleanup", func(t *testing.T) {
		mux, mock, _, cleanup := newP3TestMux(t)
		defer cleanup()
		at, token := armCollisionRecovery(t, mux, mock)
		mux.stateMu.Lock()
		mux.gatewayTxnActive = false
		mux.recordGatewayInactive(ReasonReconnect)
		mux.stateMu.Unlock()
		assertCleared(t, mux)
		assertInvalidated(t, mux, mock, at, token)
	})

	t.Run("shutdown invalidates stale recovery token", func(t *testing.T) {
		mux, mock, _, cleanup := newP3TestMux(t)
		defer cleanup()
		at, token := armCollisionRecovery(t, mux, mock)
		if err := mux.Close(); err != nil {
			t.Fatalf("Close = %v", err)
		}
		if got := at.CollisionRecoveryToken(); got != 0 {
			t.Fatalf("Close retained token %d", got)
		}
		if got := mux.ActiveTxnSnapshot().InactiveReason; got != ReasonContextCancel {
			t.Fatalf("Close inactive reason = %q, want %q", got, ReasonContextCancel)
		}
		at.CompleteCollisionRecovery(token, transport.CollisionRecoveryAbandon)
		mux.stateMu.Lock()
		active := mux.gatewayTxnActive
		mux.stateMu.Unlock()
		if mux.arb.isOwner(gatewaySessionID) || active {
			t.Fatal("stale completion after Close resurrected gateway ownership")
		}
	})
}

// TestIssue993_InjectsBoundedOfflineContentionCoverage repeatedly injects the
// exact first-byte foreign-initiator shape through readLoop. The end-to-end
// test above proves protocol.Bus retry behavior once; this bounded loop keeps
// the mux predicate exercised across 500 deterministic offline contention
// events without making a live-bus or duration claim.
func TestIssue993_InjectsBoundedOfflineContentionCoverage(t *testing.T) {
	const events = 500

	mux, mock, _, cleanup := newP3TestMux(t)
	defer cleanup()
	at := mux.ActiveTransport()

	for i := 0; i < events; i++ {
		grantGateway(t, mux, mock, 0x10)
		if _, err := at.Write([]byte{0x10}); err != nil {
			t.Fatalf("event %d: Write = %v", i, err)
		}
		mock.eventCh <- transport.StreamEvent{Kind: transport.StreamEventByte, Byte: 0xF1}
		expectActiveByte(t, mux, 0xF1)
		if !mux.activeTxn.firstByteSuspectArbLoss.Load() {
			t.Fatalf("event %d: foreign initiator did not arm collision recovery", i)
		}

		// Each iteration is an independent offline contention episode.
		// The explicit terminal boundary mirrors a cancelled active caller;
		// the next grant resets every per-transaction predicate.
		mux.markActiveContextCancel()
		mux.arb.forceRelease()
	}
}
