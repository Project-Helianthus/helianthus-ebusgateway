package b503session_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/b503session"
)

const operationTarget byte = 0x15

type dispatchRecorder struct {
	mu       sync.Mutex
	targets  []byte
	outcomes []b503session.DispatchOutcome
}

func (recorder *dispatchRecorder) dispatch(_ context.Context, target byte) b503session.DispatchOutcome {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.targets = append(recorder.targets, target)
	if len(recorder.outcomes) == 0 {
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: newTK(testEpoch)}
	}
	outcome := recorder.outcomes[0]
	recorder.outcomes = recorder.outcomes[1:]
	return outcome
}

func (recorder *dispatchRecorder) snapshot() []byte {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]byte(nil), recorder.targets...)
}

func TestOperationEnableBecomesActiveOnlyAfterNativeACK(t *testing.T) {
	manager := b503session.New(newTK(testEpoch), time.Minute, okRefresh(testEpoch+1))
	entered := make(chan struct{})
	release := make(chan struct{})
	result := make(chan b503session.SessionKey, 1)
	go func() {
		key, _ := manager.EnableOperation(context.Background(), operationTarget, func(context.Context, byte) b503session.DispatchOutcome {
			close(entered)
			<-release
			return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: newTK(testEpoch)}
		})
		result <- key
	}()
	<-entered
	pending, ok := manager.PendingOperation()
	if !ok || pending.Kind != b503session.OperationEnable || pending.Target != operationTarget || pending.Emitted || pending.OperationID == "" {
		t.Fatalf("pending enable = %+v, present=%v", pending, ok)
	}
	if got := manager.StatusSnapshot(); got.State != b503session.Enabling || !got.Owned {
		t.Fatalf("during native enable = %+v, want Enabling owned", got)
	}
	close(release)
	key := <-result
	if key.IssuerToken == "" || key.Transport != newTK(testEpoch) {
		t.Fatalf("acknowledged key = %+v", key)
	}
	if got := manager.StatusSnapshot(); got.State != b503session.Active || !got.Owned {
		t.Fatalf("after native ACK = %+v, want Active owned", got)
	}
	obligation, ok := manager.CleanupObligation()
	if !ok || obligation.Target != operationTarget || obligation.OriginNative != b503session.NativeACK || !obligation.OriginEmitted {
		t.Fatalf("emitted enable ACK cleanup = %+v, present=%v", obligation, ok)
	}
}

func TestOperationEnablePreEmissionCancellationNeedsNoCleanupWrite(t *testing.T) {
	manager := b503session.New(newTK(testEpoch), time.Minute, nil)
	cleanup := &dispatchRecorder{}
	manager.SetCleanupDispatcher(cleanup.dispatch)
	canceled := errors.New("pre-emission cancellation")
	_, err := manager.EnableOperation(context.Background(), operationTarget, func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{Err: canceled, Emitted: false, Native: b503session.NativeAmbiguous, Transport: newTK(testEpoch)}
	})
	if !errors.Is(err, canceled) {
		t.Fatalf("enable error = %v", err)
	}
	if _, ok := manager.CleanupObligation(); ok {
		t.Fatal("pre-emission cancellation created cleanup")
	}
	if got := cleanup.snapshot(); len(got) != 0 {
		t.Fatalf("cleanup writes = %v, want none", got)
	}
}

func TestOperationEnableNAKRetainsUnsettledCleanup(t *testing.T) {
	manager := b503session.New(newTK(testEpoch), time.Minute, nil)
	nak := errors.New("native NAK")
	_, err := manager.EnableOperation(context.Background(), operationTarget, func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{Err: nak, Emitted: true, Native: b503session.NativeNAK, Transport: newTK(testEpoch)}
	})
	if !errors.Is(err, nak) {
		t.Fatalf("enable NAK = %v", err)
	}
	obligation, ok := manager.CleanupObligation()
	if !ok || obligation.Target != operationTarget || obligation.OriginNative != b503session.NativeNAK || !errors.Is(obligation.OriginErr, nak) {
		t.Fatalf("enable NAK cleanup = %+v, present=%v", obligation, ok)
	}
	if got := manager.StatusSnapshot(); got.State != b503session.Idle || got.Owned {
		t.Fatalf("enable NAK state = %+v, want public Idle without owner", got)
	}
}

func TestOperationAmbiguousEnableRunsOneTargetBoundCleanup(t *testing.T) {
	manager := b503session.New(newTK(testEpoch), time.Minute, nil)
	cleanup := &dispatchRecorder{outcomes: []b503session.DispatchOutcome{{Emitted: true, Native: b503session.NativeACK, Transport: newTK(testEpoch)}}}
	manager.SetCleanupDispatcher(cleanup.dispatch)
	ambiguous := errors.New("CRC mismatch")
	_, err := manager.EnableOperation(context.Background(), operationTarget, func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{Err: ambiguous, Emitted: true, Native: b503session.NativeAmbiguous, Transport: newTK(testEpoch)}
	})
	if !errors.Is(err, ambiguous) {
		t.Fatalf("enable error = %v", err)
	}
	if got := cleanup.snapshot(); len(got) != 1 || got[0] != operationTarget {
		t.Fatalf("cleanup targets = %v, want [%#x]", got, operationTarget)
	}
	obligation, ok := manager.CleanupObligation()
	if !ok || obligation.LastNative != b503session.NativeACK || obligation.LastAttemptEpoch != testEpoch {
		t.Fatalf("cleanup ACK must remain evidence, not settlement: %+v, present=%v", obligation, ok)
	}
}

func TestOperationEnablePostEmissionEpochAdvanceRunsAtMostOneLifecycleCleanup(t *testing.T) {
	manager := b503session.New(newTK(testEpoch), time.Minute, nil)
	cleanup := &dispatchRecorder{outcomes: []b503session.DispatchOutcome{{
		Emitted: true, Native: b503session.NativeACK, Transport: newTK(testEpoch + 1),
	}}}
	manager.SetCleanupDispatcher(cleanup.dispatch)
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	staleCompletion := errors.New("stale epoch completion")
	go func() {
		_, err := manager.EnableOperation(context.Background(), operationTarget, func(context.Context, byte) b503session.DispatchOutcome {
			close(entered)
			<-release
			return b503session.DispatchOutcome{Err: staleCompletion, Emitted: true, Native: b503session.NativeAmbiguous, Transport: newTK(testEpoch)}
		})
		done <- err
	}()
	<-entered
	manager.OnEpochAdvance(context.Background(), testEpoch+1)
	manager.OnEpochAdvance(context.Background(), testEpoch+1)
	close(release)
	if err := <-done; !errors.Is(err, staleCompletion) {
		t.Fatalf("epoch-crossed enable = %v, want exact stale completion", err)
	}
	if got := cleanup.snapshot(); len(got) != 1 || got[0] != operationTarget {
		t.Fatalf("admitted-lifecycle cleanup targets = %v, want one %#x", got, operationTarget)
	}
	obligation, ok := manager.CleanupObligation()
	if !ok || obligation.Target != operationTarget || obligation.TransportEpoch != testEpoch+1 || obligation.OriginNative != b503session.NativeAmbiguous || !errors.Is(obligation.OriginErr, staleCompletion) || obligation.LastAttemptEpoch != testEpoch+1 || obligation.LastNative != b503session.NativeACK {
		t.Fatalf("epoch-crossed cleanup = %+v, present=%v", obligation, ok)
	}
	if got := manager.StatusSnapshot(); got.State != b503session.Idle || got.Owned {
		t.Fatalf("epoch-crossed snapshot = %+v, want public Idle without owner", got)
	}
	manager.OnEpochAdvance(context.Background(), testEpoch+2)
	manager.OnEpochAdvance(context.Background(), testEpoch+3)
	if got := cleanup.snapshot(); len(got) != 1 {
		t.Fatalf("automatic reconnect cleanup writes = %v, want only admitted-lifecycle write", got)
	}
	var reenableWrites int
	if _, err := manager.EnableOperation(context.Background(), operationTarget, func(context.Context, byte) b503session.DispatchOutcome {
		reenableWrites++
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK}
	}); !errors.Is(err, b503session.ErrCleanupPending) || reenableWrites != 0 {
		t.Fatalf("post-cleanup re-enable = %v, writes=%d; want cleanup pending without dispatch", err, reenableWrites)
	}
}

func TestOperationDisableACKRetainsUnsettledCleanup(t *testing.T) {
	manager := b503session.New(newTK(testEpoch), time.Minute, nil)
	ack := func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: newTK(testEpoch)}
	}
	key, err := manager.EnableOperation(context.Background(), operationTarget, ack)
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	if err := manager.DisableOperation(context.Background(), key, operationTarget, ack); err != nil {
		t.Fatalf("disable: %v", err)
	}
	obligation, ok := manager.CleanupObligation()
	if !ok || obligation.LastNative != b503session.NativeACK || obligation.LastAttemptEpoch != testEpoch {
		t.Fatalf("disable ACK cleanup evidence = %+v, present=%v", obligation, ok)
	}
	if got := manager.StatusSnapshot(); got.State != b503session.Idle || got.Owned {
		t.Fatalf("after disable ACK = %+v", got)
	}
}

func TestOperationReadInFlightRejectsConcurrentDisable(t *testing.T) {
	manager := b503session.New(newTK(testEpoch), time.Minute, nil)
	ack := func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: newTK(testEpoch)}
	}
	key, err := manager.EnableOperation(context.Background(), operationTarget, ack)
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	readDone := make(chan error, 1)
	go func() {
		_, err := manager.ReadOperation(context.Background(), operationTarget, func(context.Context, byte) b503session.DispatchOutcome {
			close(entered)
			<-release
			return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: newTK(testEpoch)}
		})
		readDone <- err
	}()
	<-entered
	if pending, ok := manager.PendingOperation(); !ok || pending.Kind != b503session.OperationRead || pending.Target != operationTarget {
		t.Fatalf("pending read = %+v, present=%v", pending, ok)
	}
	var disableCalls int
	err = manager.DisableOperation(context.Background(), key, operationTarget, func(context.Context, byte) b503session.DispatchOutcome {
		disableCalls++
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK}
	})
	if !errors.Is(err, b503session.ErrSessionBusy) || disableCalls != 0 {
		t.Fatalf("concurrent disable = %v, dispatches=%d", err, disableCalls)
	}
	close(release)
	if err := <-readDone; err != nil {
		t.Fatalf("read completion: %v", err)
	}
}

func TestOperationRefreshedDisableUsesOldAuthenticatedKeyAndDispatchesOnce(t *testing.T) {
	manager := b503session.New(newTK(testEpoch), time.Minute, okRefresh(testEpoch+1))
	ack := func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: manager.TransportKey()}
	}
	oldKey, err := manager.EnableOperation(context.Background(), operationTarget, ack)
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	manager.OnEpochAdvance(context.Background(), testEpoch+1)
	var dispatches int
	err = manager.DisableOperation(context.Background(), oldKey, operationTarget, func(context.Context, byte) b503session.DispatchOutcome {
		dispatches++
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: newTK(testEpoch + 1)}
	})
	if err != nil {
		t.Fatalf("refreshed disable: %v", err)
	}
	if dispatches != 1 {
		t.Fatalf("refreshed disable dispatches = %d, want 1", dispatches)
	}
	if got := manager.StatusSnapshot(); got.State != b503session.Idle || got.Owned {
		t.Fatalf("refreshed disable state = %+v", got)
	}
}

func TestOperationDisableFailureReconnectEmitsNoRecoveryWrite(t *testing.T) {
	manager := b503session.New(newTK(testEpoch), time.Minute, nil)
	ack := func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: newTK(testEpoch)}
	}
	key, err := manager.EnableOperation(context.Background(), operationTarget, ack)
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	failure := errors.New("disable timeout")
	if err := manager.DisableOperation(context.Background(), key, operationTarget, func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{Err: failure, Emitted: true, Native: b503session.NativeAmbiguous, Transport: newTK(testEpoch)}
	}); !errors.Is(err, failure) {
		t.Fatalf("disable error = %v", err)
	}
	before, ok := manager.CleanupObligation()
	if !ok || before.Target != operationTarget || before.LastAttemptEpoch != testEpoch || before.GatewayCleanupAttemptID == "" {
		t.Fatalf("cleanup after disable = %+v, present=%v", before, ok)
	}
	if _, err := manager.EnableOperation(context.Background(), operationTarget+1, ack); !errors.Is(err, b503session.ErrCleanupPending) {
		t.Fatalf("enable while cleanup pending = %v, want ErrCleanupPending", err)
	}
	cleanup := &dispatchRecorder{}
	manager.SetCleanupDispatcher(cleanup.dispatch)
	manager.OnEpochAdvance(context.Background(), testEpoch+1)
	manager.OnEpochAdvance(context.Background(), testEpoch+1)
	manager.OnEpochAdvance(context.Background(), testEpoch+2)
	if got := cleanup.snapshot(); len(got) != 0 {
		t.Fatalf("reconnect recovery writes = %v, want none", got)
	}
	after, ok := manager.CleanupObligation()
	if !ok || after.GatewayCleanupAttemptID != before.GatewayCleanupAttemptID || after.Target != before.Target || after.TransportEpoch != before.TransportEpoch || after.OriginEmitted != before.OriginEmitted || after.OriginNative != before.OriginNative || after.OriginErr != before.OriginErr || after.LastAttemptEpoch != before.LastAttemptEpoch || after.LastEmitted != before.LastEmitted || after.LastNative != before.LastNative || !errors.Is(after.LastErr, failure) {
		t.Fatalf("reconnect mutated retained cleanup: before=%+v after=%+v present=%v", before, after, ok)
	}
	if got := manager.TransportKey().TransportEpoch; got != testEpoch+2 {
		t.Fatalf("transport epoch = %d, want %d", got, testEpoch+2)
	}
	var reenableWrites int
	if _, err := manager.EnableOperation(context.Background(), operationTarget, func(context.Context, byte) b503session.DispatchOutcome {
		reenableWrites++
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK}
	}); !errors.Is(err, b503session.ErrCleanupPending) || reenableWrites != 0 {
		t.Fatalf("post-reconnect enable = %v, writes=%d; want cleanup pending without write", err, reenableWrites)
	}
}

func TestOperationActiveDisconnectReleasesOwnerAndRetainsCleanup(t *testing.T) {
	manager := b503session.New(newTK(testEpoch), time.Minute, nil)
	ack := func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: newTK(testEpoch)}
	}
	if _, err := manager.EnableOperation(context.Background(), operationTarget, ack); err != nil {
		t.Fatalf("enable: %v", err)
	}
	manager.OnTransportDisconnect()
	if got := manager.StatusSnapshot(); got.State != b503session.Idle || got.Owned {
		t.Fatalf("disconnect snapshot = %+v", got)
	}
	obligation, ok := manager.CleanupObligation()
	if !ok || obligation.Target != operationTarget || obligation.TransportEpoch != testEpoch {
		t.Fatalf("disconnect cleanup = %+v, present=%v", obligation, ok)
	}
}

func TestOperationEnableDisconnectRetainsConservativeEmissionEvidence(t *testing.T) {
	manager := b503session.New(newTK(testEpoch), time.Minute, nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	enableErr := make(chan error, 1)
	staleCompletion := errors.New("stale completion")
	go func() {
		_, err := manager.EnableOperation(context.Background(), operationTarget, func(context.Context, byte) b503session.DispatchOutcome {
			close(entered)
			<-release
			return b503session.DispatchOutcome{
				Err: staleCompletion, Emitted: true,
				Native: b503session.NativeAmbiguous, Transport: newTK(testEpoch),
			}
		})
		enableErr <- err
	}()
	<-entered
	manager.OnTransportDisconnect()
	close(release)
	if err := <-enableErr; !errors.Is(err, staleCompletion) {
		t.Fatalf("enable error = %v, want exact stale completion", err)
	}
	obligation, ok := manager.CleanupObligation()
	if !ok || obligation.Target != operationTarget || !obligation.OriginEmitted || obligation.OriginNative != b503session.NativeAmbiguous || obligation.OriginErr == nil {
		t.Fatalf("disconnected enable cleanup = %+v, present=%v", obligation, ok)
	}
	if got := manager.StatusSnapshot(); got.State != b503session.Idle || got.Owned {
		t.Fatalf("disconnect snapshot = %+v, want public Idle without owner", got)
	}
}

func TestOperationRefreshedReadDisconnectReleasesOwnerAndRetainsCleanup(t *testing.T) {
	manager := b503session.New(newTK(testEpoch), time.Minute, okRefresh(testEpoch+1))
	ack := func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: newTK(testEpoch)}
	}
	if _, err := manager.EnableOperation(context.Background(), operationTarget, ack); err != nil {
		t.Fatalf("enable: %v", err)
	}
	manager.OnEpochAdvance(context.Background(), testEpoch+1)
	_, err := manager.ReadOperation(context.Background(), operationTarget, func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{
			Err: b503session.ErrTransportDown, Emitted: true,
			Native: b503session.NativeAmbiguous, Transport: newTK(testEpoch + 1),
		}
	})
	if !errors.Is(err, b503session.ErrTransportDown) {
		t.Fatalf("refreshed read error = %v, want ErrTransportDown", err)
	}
	if got := manager.StatusSnapshot(); got.State != b503session.Idle || got.Owned {
		t.Fatalf("refreshed read disconnect snapshot = %+v", got)
	}
	obligation, ok := manager.CleanupObligation()
	if !ok || obligation.Target != operationTarget || obligation.TransportEpoch != testEpoch+1 {
		t.Fatalf("refreshed read disconnect cleanup = %+v, present=%v", obligation, ok)
	}
}

func TestOperationRestartFencesQualifiedTargetWithoutAutomaticWrite(t *testing.T) {
	manager := b503session.New(newTK(testEpoch), time.Minute, nil)
	writes := &dispatchRecorder{}
	manager.SetCleanupDispatcher(writes.dispatch)
	manager.MarkQualifiedTarget(operationTarget)
	manager.ResetForRestart()
	manager.OnEpochAdvance(context.Background(), testEpoch+1)
	if got := writes.snapshot(); len(got) != 0 {
		t.Fatalf("restart emitted automatic writes to %v", got)
	}
	if !manager.TargetBlocked(operationTarget) {
		t.Fatal("qualified target lost restart UNKNOWN fence")
	}
	if _, err := manager.EnableOperation(context.Background(), operationTarget, writes.dispatch); !errors.Is(err, b503session.ErrCleanupPending) {
		t.Fatalf("restart-fenced enable = %v, want ErrCleanupPending", err)
	}
}
