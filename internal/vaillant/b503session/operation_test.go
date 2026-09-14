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

func TestOperationEnableNAKProvesNoSession(t *testing.T) {
	manager := b503session.New(newTK(testEpoch), time.Minute, nil)
	nak := errors.New("native NAK")
	_, err := manager.EnableOperation(context.Background(), operationTarget, func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{Err: nak, Emitted: true, Native: b503session.NativeNAK, Transport: newTK(testEpoch)}
	})
	if !errors.Is(err, nak) {
		t.Fatalf("enable NAK = %v", err)
	}
	if _, ok := manager.CleanupObligation(); ok {
		t.Fatal("enable NAK retained cleanup")
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
	if _, ok := manager.CleanupObligation(); ok {
		t.Fatal("valid cleanup ACK did not clear obligation")
	}
}

func TestOperationDisableACKIsOnlyCleanupSuccess(t *testing.T) {
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
	if _, ok := manager.CleanupObligation(); ok {
		t.Fatal("disable ACK retained cleanup")
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

func TestOperationDisableFailureRetriesOnceOnlyOnLaterEpoch(t *testing.T) {
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
	cleanup := &dispatchRecorder{outcomes: []b503session.DispatchOutcome{
		{Err: failure, Emitted: true, Native: b503session.NativeAmbiguous, Transport: newTK(testEpoch + 1)},
		{Emitted: true, Native: b503session.NativeACK, Transport: newTK(testEpoch + 2)},
	}}
	manager.SetCleanupDispatcher(cleanup.dispatch)
	manager.OnEpochAdvance(context.Background(), testEpoch+1)
	manager.OnEpochAdvance(context.Background(), testEpoch+1)
	if got := cleanup.snapshot(); len(got) != 1 || got[0] != operationTarget {
		t.Fatalf("same-epoch cleanup targets = %v", got)
	}
	middle, ok := manager.CleanupObligation()
	if !ok || middle.GatewayCleanupAttemptID != before.GatewayCleanupAttemptID || middle.LastAttemptEpoch != testEpoch+1 {
		t.Fatalf("retained cleanup = %+v, present=%v", middle, ok)
	}
	manager.OnEpochAdvance(context.Background(), testEpoch+2)
	if got := cleanup.snapshot(); len(got) != 2 || got[1] != operationTarget {
		t.Fatalf("later-epoch cleanup targets = %v", got)
	}
	if _, ok := manager.CleanupObligation(); ok {
		t.Fatal("later-epoch ACK did not clear cleanup")
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
