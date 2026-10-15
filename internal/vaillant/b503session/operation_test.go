package b503session_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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
		_, err := manager.EnableOperation(context.Background(), operationTarget, func(ctx context.Context, target byte) b503session.DispatchOutcome {
			operationID, ok := b503session.PendingOperationIDFromContext(ctx)
			if !ok || !manager.AdmitPendingEmission(operationID, target) {
				panic("post-emission epoch test could not admit pending enable")
			}
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

func TestOperationDefensiveCleanupAdmissionIsCanceledBeforeNewEpochEmission(t *testing.T) {
	manager := b503session.New(newTK(testEpoch), time.Minute, nil)
	cleanupEntered := make(chan struct{})
	allowAdmission := make(chan struct{})
	cleanupDone := make(chan struct{})
	var writes atomic.Int32
	manager.SetCleanupDispatcher(func(ctx context.Context, target byte) b503session.DispatchOutcome {
		defer close(cleanupDone)
		operationID, ok := b503session.PendingOperationIDFromContext(ctx)
		if !ok {
			t.Fatal("defensive cleanup must carry a typed pending operation")
		}
		close(cleanupEntered)
		<-allowAdmission
		if ctx.Err() == nil {
			t.Fatal("epoch advance must cancel the defensive cleanup context")
		}
		if manager.AdmitPendingEmission(operationID, target) {
			writes.Add(1)
			return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: newTK(testEpoch)}
		}
		return b503session.DispatchOutcome{Err: b503session.ErrTransportDown, Transport: newTK(testEpoch)}
	})

	done := make(chan error, 1)
	go func() {
		_, err := manager.EnableOperation(context.Background(), operationTarget, func(context.Context, byte) b503session.DispatchOutcome {
			return b503session.DispatchOutcome{Err: errors.New("post-emission timeout"), Emitted: true, Native: b503session.NativeAmbiguous, Transport: newTK(testEpoch)}
		})
		done <- err
	}()
	<-cleanupEntered
	manager.OnEpochAdvance(context.Background(), testEpoch+1)
	close(allowAdmission)
	<-cleanupDone
	if err := <-done; err == nil {
		t.Fatal("emitted enable failure must retain its exact failure")
	}
	if got := writes.Load(); got != 0 {
		t.Fatalf("epoch-invalidated defensive cleanup emitted %d writes, want 0", got)
	}
	if pending, ok := manager.PendingOperation(); ok {
		t.Fatalf("completed defensive cleanup retained pending operation: %+v", pending)
	}
}

func TestOperationPostFailureCleanupUsesFreshBoundedContext(t *testing.T) {
	manager := b503session.New(newTK(testEpoch), time.Minute, nil)
	var cleanupCalls atomic.Int32
	manager.SetCleanupDispatcher(func(ctx context.Context, target byte) b503session.DispatchOutcome {
		cleanupCalls.Add(1)
		if ctx.Err() != nil {
			t.Fatalf("cleanup inherited failed caller context: %v", ctx.Err())
		}
		operationID, ok := b503session.PendingOperationIDFromContext(ctx)
		if !ok || !manager.AdmitPendingEmission(operationID, target) {
			t.Fatal("fresh cleanup context must admit only its typed defensive disable")
		}
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: newTK(testEpoch)}
	})

	caller, cancel := context.WithCancel(context.Background())
	_, err := manager.EnableOperation(caller, operationTarget, func(ctx context.Context, target byte) b503session.DispatchOutcome {
		operationID, ok := b503session.PendingOperationIDFromContext(ctx)
		if !ok || !manager.AdmitPendingEmission(operationID, target) {
			t.Fatal("enable could not admit its pending emission")
		}
		cancel()
		return b503session.DispatchOutcome{Err: context.Canceled, Emitted: true, Native: b503session.NativeAmbiguous, Transport: newTK(testEpoch)}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("enable error = %v, want caller cancellation", err)
	}
	if got := cleanupCalls.Load(); got != 1 {
		t.Fatalf("fresh post-failure cleanup calls = %d, want one conservative attempt", got)
	}
}

func TestOperationEpochAdvanceCancelsAdmittedEnableBeforeEmission(t *testing.T) {
	manager := b503session.New(newTK(testEpoch), time.Minute, nil)
	admitted := make(chan struct{})
	done := make(chan error, 1)
	var emitted int
	go func() {
		_, err := manager.EnableOperation(context.Background(), operationTarget, func(ctx context.Context, target byte) b503session.DispatchOutcome {
			operationID, ok := b503session.PendingOperationIDFromContext(ctx)
			if !ok || !manager.AdmitPendingEmission(operationID, target) {
				panic("epoch-cancellation test could not admit pending enable")
			}
			close(admitted)
			<-ctx.Done()
			// This models the dispatcher's final context check after poll
			// quiescence and before entering bus.Send.
			if ctx.Err() == nil {
				emitted++
			}
			return b503session.DispatchOutcome{Err: ctx.Err(), Transport: newTK(testEpoch)}
		})
		done <- err
	}()
	<-admitted
	manager.OnEpochAdvance(context.Background(), testEpoch+1)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("epoch-invalidated enable = %v, want context cancellation", err)
	}
	if emitted != 0 {
		t.Fatalf("epoch-invalidated enable emissions = %d, want 0", emitted)
	}
	if _, ok := manager.CleanupObligation(); ok {
		t.Fatal("pre-emission epoch cancellation retained cleanup")
	}
	if got := manager.StatusSnapshot(); got.State != b503session.Idle || got.Owned {
		t.Fatalf("epoch-invalidated snapshot = %+v, want Idle without owner", got)
	}
	if got := manager.TransportKey(); got != newTK(testEpoch+1) {
		t.Fatalf("epoch-invalidated transport = %+v, want %+v", got, newTK(testEpoch+1))
	}
}

func TestOperationEpochAdvanceBeforeAdmissionEmitsNothing(t *testing.T) {
	manager := b503session.New(newTK(testEpoch), time.Minute, nil)
	waiting := make(chan struct{})
	allowAdmission := make(chan struct{})
	done := make(chan error, 1)
	var sends int
	go func() {
		_, err := manager.EnableOperation(context.Background(), operationTarget, func(ctx context.Context, target byte) b503session.DispatchOutcome {
			close(waiting)
			<-allowAdmission
			operationID, ok := b503session.PendingOperationIDFromContext(ctx)
			if !ok || manager.AdmitPendingEmission(operationID, target) {
				sends++
				return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: newTK(testEpoch)}
			}
			return b503session.DispatchOutcome{Err: b503session.ErrTransportDown, Transport: newTK(testEpoch)}
		})
		done <- err
	}()
	<-waiting
	manager.OnEpochAdvance(context.Background(), testEpoch+1)
	close(allowAdmission)
	if err := <-done; !errors.Is(err, b503session.ErrTransportDown) {
		t.Fatalf("epoch-invalidated pre-admission enable = %v, want ErrTransportDown", err)
	}
	if sends != 0 {
		t.Fatalf("post-rollover sends = %d, want 0", sends)
	}
	if _, ok := manager.CleanupObligation(); ok {
		t.Fatal("pre-admission rollover retained cleanup")
	}
}

func TestOperationEpochZeroRunsFirstCleanupAttempt(t *testing.T) {
	manager := b503session.New(newTK(0), time.Minute, nil)
	cleanup := &dispatchRecorder{outcomes: []b503session.DispatchOutcome{{
		Emitted: true, Native: b503session.NativeACK, Transport: newTK(0),
	}}}
	manager.SetCleanupDispatcher(cleanup.dispatch)
	ambiguous := errors.New("ambiguous enable")
	_, err := manager.EnableOperation(context.Background(), operationTarget, func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{Err: ambiguous, Emitted: true, Native: b503session.NativeAmbiguous, Transport: newTK(0)}
	})
	if !errors.Is(err, ambiguous) {
		t.Fatalf("epoch-zero enable error = %v, want ambiguous result", err)
	}
	if got := cleanup.snapshot(); len(got) != 1 || got[0] != operationTarget {
		t.Fatalf("epoch-zero cleanup targets = %v, want one %#x", got, operationTarget)
	}
	obligation, ok := manager.CleanupObligation()
	if !ok || !obligation.LastAttempted || obligation.LastAttemptEpoch != 0 || obligation.LastNative != b503session.NativeACK {
		t.Fatalf("epoch-zero cleanup evidence = %+v, present=%v", obligation, ok)
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

func TestOperationDisablePreEmissionFailureKeepsOwnerAndAllowsRetry(t *testing.T) {
	manager := b503session.New(newTK(testEpoch), time.Minute, nil)
	cleanup := &dispatchRecorder{}
	manager.SetCleanupDispatcher(cleanup.dispatch)
	ack := func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: newTK(testEpoch)}
	}
	key, err := manager.EnableOperation(context.Background(), operationTarget, ack)
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	before, ok := manager.CleanupObligation()
	if !ok || !before.OriginEmitted || before.LastAttempted {
		t.Fatalf("enable cleanup evidence = %+v, present=%v", before, ok)
	}
	blocked := errors.New("readMu quiesce deadline")
	if err := manager.DisableOperation(context.Background(), key, operationTarget, func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{Err: blocked, Emitted: false, Native: b503session.NativeAmbiguous, Transport: newTK(testEpoch)}
	}); !errors.Is(err, blocked) {
		t.Fatalf("non-emitted disable = %v, want exact blocked error", err)
	}
	if got := manager.StatusSnapshot(); got.State != b503session.Active || !got.Owned {
		t.Fatalf("non-emitted disable state = %+v, want owned Active", got)
	}
	after, ok := manager.CleanupObligation()
	if !ok || after.GatewayCleanupAttemptID != before.GatewayCleanupAttemptID || after.LastAttempted || after.LastEmitted {
		t.Fatalf("non-emitted disable mutated cleanup into a nonexistent wire attempt: before=%+v after=%+v present=%v", before, after, ok)
	}
	if got := cleanup.snapshot(); len(got) != 0 {
		t.Fatalf("non-emitted disable cleanup writes = %v, want none", got)
	}
	if err := manager.DisableOperation(context.Background(), key, operationTarget, ack); err != nil {
		t.Fatalf("retry disable: %v", err)
	}
	if got := manager.StatusSnapshot(); got.State != b503session.Idle || got.Owned {
		t.Fatalf("retry disable state = %+v, want ownerless Idle", got)
	}
	if cleanup, ok := manager.CleanupObligation(); !ok || !cleanup.LastEmitted || cleanup.LastAttemptEpoch != testEpoch {
		t.Fatalf("retry disable cleanup evidence = %+v, present=%v", cleanup, ok)
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

// These are real AfterFunc wiring checks. Arm the timer only after the initial
// epoch notification, so setup ordering does not depend on the idle interval.
// The 10 s observation budget tolerates runner contention during completion.
const operationIdleWiringInterval = 100 * time.Millisecond
const operationIdleWiringBudget = 10 * time.Second

func awaitOperationIdleSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	timer := time.NewTimer(operationIdleWiringBudget)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", description)
	}
}

func awaitOperationIdleCompletion(t *testing.T, manager *b503session.Manager) {
	t.Helper()
	// Rollover cancels the dispatch context before the callback finalizes, so
	// cancellation cannot signal completion there. No public completion hook
	// exists: observe the terminal owner/pending state only after dispatch returns.
	timer := time.NewTimer(operationIdleWiringBudget)
	defer timer.Stop()
	ticker := time.NewTicker(operationIdleWiringInterval / 10)
	defer ticker.Stop()
	for {
		_, pending := manager.PendingOperation()
		if !manager.IsOwned() && !pending {
			return
		}
		select {
		case <-timer.C:
			t.Fatal("idle callback did not release owner and pending operation")
		case <-ticker.C:
		}
	}
}

// Register cleanup before enabling the real timer. Closing the hook gate fences
// callbacks that have not entered yet; admitted test work is released and joined
// before reset, including when a signal wait fails.
func setOperationIdleDispatcher(t *testing.T, manager *b503session.Manager, dispatch b503session.DispatchFunc, release func()) {
	t.Helper()
	var mu sync.Mutex
	stopped, started := false, false
	returned := make(chan struct{})
	manager.SetCleanupDispatcher(func(ctx context.Context, target byte) b503session.DispatchOutcome {
		mu.Lock()
		if stopped {
			mu.Unlock()
			return b503session.DispatchOutcome{Err: b503session.ErrTransportDown}
		}
		if started {
			mu.Unlock()
			t.Error("idle timer dispatched cleanup more than once")
			return b503session.DispatchOutcome{Err: b503session.ErrTransportDown}
		}
		started = true
		mu.Unlock()
		defer close(returned)
		return dispatch(ctx, target)
	})
	t.Cleanup(func() {
		if release != nil {
			release()
		}
		defer manager.ResetForRestart()
		mu.Lock()
		stopped = true
		join := started
		mu.Unlock()
		if join {
			awaitOperationIdleSignal(t, returned, "released idle dispatcher")
			awaitOperationIdleCompletion(t, manager)
		}
	})
}

func TestOperationIdleExpiryRefreshesBeforeDefensiveDisable(t *testing.T) {
	refreshed := make(chan struct{}, 1)
	manager := b503session.New(newTK(testEpoch), 0, func(context.Context) (b503session.TransportKey, error) {
		refreshed <- struct{}{}
		return newTK(testEpoch + 1), nil
	})
	dispatched := make(chan context.Context, 1)
	var dispatches atomic.Int32
	setOperationIdleDispatcher(t, manager, func(ctx context.Context, target byte) b503session.DispatchOutcome {
		dispatches.Add(1)
		operationID, ok := b503session.PendingOperationIDFromContext(ctx)
		pending, present := manager.PendingOperation()
		if !ok || !present || operationID != pending.OperationID || pending.Kind != b503session.OperationDisable ||
			pending.Transport != newTK(testEpoch+1) || pending.Target != operationTarget || target != operationTarget ||
			!manager.AdmitPendingEmission(operationID, target) {
			t.Error("idle cleanup did not admit a target-bound disable with the refreshed transport key")
			return b503session.DispatchOutcome{Err: b503session.ErrTransportDown}
		}
		dispatched <- ctx
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: newTK(testEpoch + 1)}
	}, nil)
	if _, err := manager.EnableOperation(context.Background(), operationTarget, func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: newTK(testEpoch)}
	}); err != nil {
		t.Fatalf("enable = %v", err)
	}
	manager.OnEpochAdvance(context.Background(), testEpoch+1)
	b503session.ArmIdleTimerForTest(manager, operationIdleWiringInterval)
	awaitOperationIdleSignal(t, refreshed, "admitted idle refresh")
	timer := time.NewTimer(operationIdleWiringBudget)
	defer timer.Stop()
	select {
	case ctx := <-dispatched:
		// With no rollover, deferred context cancellation follows recording the
		// cleanup result and is a completion signal for the real timer path.
		awaitOperationIdleSignal(t, ctx.Done(), "idle cleanup completion")
	case <-timer.C:
		t.Fatal("idle expiry did not issue refreshed defensive disable")
	}
	if got := dispatches.Load(); got != 1 {
		t.Fatalf("idle cleanup dispatches = %d, want one", got)
	}
	if got := manager.TransportKey(); got != newTK(testEpoch+1) {
		t.Fatalf("idle cleanup transport = %+v, want refreshed epoch", got)
	}
	cleanup, ok := manager.CleanupObligation()
	if !ok || cleanup.Target != operationTarget || cleanup.TransportEpoch != testEpoch ||
		!cleanup.LastAttempted || !cleanup.LastEmitted || cleanup.LastAttemptEpoch != testEpoch+1 || cleanup.LastNative != b503session.NativeACK {
		t.Fatalf("idle cleanup evidence = %+v, present=%v", cleanup, ok)
	}
	if got := manager.StatusSnapshot(); got.Owned || got.State != b503session.Idle {
		t.Fatalf("idle cleanup state = %+v, want ownerless Idle", got)
	}
}

func TestOperationIdleRefreshRolloverBeforeEmissionRetainsLatestCleanup(t *testing.T) {
	refreshed := make(chan struct{}, 1)
	dispatchEntered := make(chan struct{}, 1)
	dispatchRelease := make(chan struct{})
	dispatchReturned := make(chan struct{}, 1)
	var release sync.Once
	var dispatches, emissions atomic.Int32
	manager := b503session.New(newTK(testEpoch), 0, func(context.Context) (b503session.TransportKey, error) {
		refreshed <- struct{}{}
		return newTK(testEpoch + 1), nil
	})
	setOperationIdleDispatcher(t, manager, func(ctx context.Context, target byte) b503session.DispatchOutcome {
		defer func() { dispatchReturned <- struct{}{} }()
		dispatches.Add(1)
		operationID, ok := b503session.PendingOperationIDFromContext(ctx)
		pending, present := manager.PendingOperation()
		if !ok || !present || operationID != pending.OperationID || pending.Kind != b503session.OperationDisable ||
			pending.Target != operationTarget || target != operationTarget || pending.Transport != newTK(testEpoch+1) {
			t.Error("idle rollover did not reach a target-bound disable with the refreshed transport key")
		}
		dispatchEntered <- struct{}{}
		<-dispatchRelease
		if ctx.Err() == nil || manager.AdmitPendingEmission(operationID, target) {
			t.Error("rollover did not cancel and fence stale idle emission")
			emissions.Add(1)
			return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: newTK(testEpoch + 1)}
		}
		return b503session.DispatchOutcome{Err: b503session.ErrTransportDown, Transport: newTK(testEpoch + 1)}
	}, func() { release.Do(func() { close(dispatchRelease) }) })
	if _, err := manager.EnableOperation(context.Background(), operationTarget, func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: newTK(testEpoch)}
	}); err != nil {
		t.Fatalf("enable = %v", err)
	}
	manager.OnEpochAdvance(context.Background(), testEpoch+1)
	b503session.ArmIdleTimerForTest(manager, operationIdleWiringInterval)
	awaitOperationIdleSignal(t, refreshed, "admitted idle refresh")
	awaitOperationIdleSignal(t, dispatchEntered, "refreshed idle dispatcher")
	manager.OnEpochAdvance(context.Background(), testEpoch+2)
	release.Do(func() { close(dispatchRelease) })
	awaitOperationIdleSignal(t, dispatchReturned, "stale idle dispatcher return")
	awaitOperationIdleCompletion(t, manager)
	if got := dispatches.Load(); got != 1 || emissions.Load() != 0 {
		t.Fatalf("idle rollover dispatches = %d, emissions = %d, want one dispatch without stale emission", got, emissions.Load())
	}
	cleanup, ok := manager.CleanupObligation()
	if !ok || cleanup.Target != operationTarget || cleanup.TransportEpoch != testEpoch+2 || cleanup.LastAttempted || cleanup.LastEmitted {
		t.Fatalf("idle rollover cleanup = %+v, present=%v", cleanup, ok)
	}
	if got := manager.TransportKey(); got != newTK(testEpoch+2) {
		t.Fatalf("idle rollover transport = %+v, want latest epoch", got)
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
		_, err := manager.EnableOperation(context.Background(), operationTarget, func(ctx context.Context, target byte) b503session.DispatchOutcome {
			operationID, ok := b503session.PendingOperationIDFromContext(ctx)
			if !ok || !manager.AdmitPendingEmission(operationID, target) {
				panic("post-emission disconnect test could not admit pending enable")
			}
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

func TestOperationRefreshInvalidatedByNewerEpochDoesNotRegressOwner(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	manager := b503session.New(newTK(testEpoch), time.Minute, func(context.Context) (b503session.TransportKey, error) {
		close(entered)
		<-release
		return newTK(testEpoch + 1), nil
	})
	ack := func(context.Context, byte) b503session.DispatchOutcome {
		return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK, Transport: manager.TransportKey()}
	}
	if _, err := manager.EnableOperation(context.Background(), operationTarget, ack); err != nil {
		t.Fatalf("enable = %v", err)
	}
	manager.OnEpochAdvance(context.Background(), testEpoch+1)
	done := make(chan error, 1)
	go func() { _, err := manager.ReadOperation(context.Background(), operationTarget, ack); done <- err }()
	<-entered
	manager.OnEpochAdvance(context.Background(), testEpoch+2)
	close(release)
	if err := <-done; !errors.Is(err, b503session.ErrTransportDown) {
		t.Fatalf("invalidated refresh = %v, want ErrTransportDown", err)
	}
	if got := manager.TransportKey().TransportEpoch; got != testEpoch+2 {
		t.Fatalf("transport epoch regressed to %d, want %d", got, testEpoch+2)
	}
	if got := manager.StatusSnapshot(); got.State != b503session.Idle || got.Owned {
		t.Fatalf("invalidated refresh state = %+v, want ownerless Idle", got)
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
