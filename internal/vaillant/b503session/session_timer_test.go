package b503session

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// These tests retain the real 30-second timer and deliver its callback directly.
// Generation fencing is observable this way; whether Stop() was called on the
// old timer is not. ResetForRestart stops any remaining timer at test cleanup.
func newIdleTimerTestManager(t *testing.T) *Manager {
	t.Helper()
	m := New(TransportKey{}, 30*time.Second, nil)
	t.Cleanup(func() { m.ResetForRestart() })
	return m
}

// Capture generations under the same lock as the production callback. Never
// hold stateMu while delivering idleTimerFired, which acquires it itself.
func idleTimerGeneration(m *Manager) uint64 {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return m.idleTimerGen
}

func TestSession_IdleTimeout_AutoDisable(t *testing.T) {
	m := newIdleTimerTestManager(t)
	transport := m.TransportKey()
	if _, err := m.Enable(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.idleTimerFired(idleTimerGeneration(m))
	if got := m.StatusSnapshot(); got.State != Idle || got.Owned {
		t.Fatalf("expired snapshot = %+v; want Idle without ownership", got)
	}
	if err := m.Read(transport); !errors.Is(err, ErrNotActive) {
		t.Fatalf("Read after expiry = %v; want ErrNotActive", err)
	}
	if _, ok := m.CleanupObligation(); !ok {
		t.Fatal("expiry must retain a cleanup obligation")
	}
}

func TestSession_ReadResetsIdleTimer(t *testing.T) {
	m := newIdleTimerTestManager(t)
	transport := m.TransportKey()
	if _, err := m.Enable(context.Background()); err != nil {
		t.Fatal(err)
	}
	var staleGenerations []uint64
	for i := 0; i < 5; i++ {
		old := idleTimerGeneration(m)
		staleGenerations = append(staleGenerations, old)
		if err := m.Read(transport); err != nil {
			t.Fatalf("Read iteration %d: %v", i, err)
		}
		if current := idleTimerGeneration(m); current <= old {
			t.Fatalf("Read iteration %d did not advance timer generation: old=%d current=%d", i, old, current)
		}
		// Deliver every previous generation, including callbacks stopped by
		// earlier reads, after the newest timer has been armed.
		for _, stale := range staleGenerations {
			m.idleTimerFired(stale)
		}
		if got := m.StatusSnapshot(); got.State != Active || !got.Owned {
			t.Fatalf("Read iteration %d: stale callback changed owner: %+v", i, got)
		}
		if _, ok := m.CleanupObligation(); ok {
			t.Fatal("stale callback created cleanup")
		}
	}
	m.idleTimerFired(idleTimerGeneration(m))
	if got := m.StatusSnapshot(); got.State != Idle || got.Owned {
		t.Fatalf("current expiry snapshot = %+v; want Idle without ownership", got)
	}
	if err := m.Read(transport); !errors.Is(err, ErrNotActive) {
		t.Fatalf("Read after expiry = %v; want ErrNotActive", err)
	}
	if _, ok := m.CleanupObligation(); !ok {
		t.Fatal("current expiry must retain cleanup")
	}
}

// Direct delivery after Disable observes generation fencing, not Timer.Stop.
func TestSession_IdleStaleCallbackAfterDisable(t *testing.T) {
	m := newIdleTimerTestManager(t)
	key, err := m.Enable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	generation := idleTimerGeneration(m)
	resume, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		<-resume
		m.idleTimerFired(generation)
	}()
	err = m.Disable(key)
	// Release and join the stale callback before checking the resulting state.
	close(resume)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stale callback did not complete after Disable")
	}
	if err != nil {
		t.Fatal(err)
	}
	if current := idleTimerGeneration(m); current <= generation {
		t.Fatalf("Disable did not invalidate callback: old=%d current=%d", generation, current)
	}
	if got := m.StatusSnapshot(); got.State != Idle || got.Owned {
		t.Fatalf("stale callback after Disable changed snapshot: %+v", got)
	}
	if _, ok := m.CleanupObligation(); ok {
		t.Fatal("stale callback after Disable created cleanup")
	}
}

func TestSession_IdleCallbackRacesWithStop(t *testing.T) {
	m := newIdleTimerTestManager(t)
	key, err := m.Enable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	generation := idleTimerGeneration(m)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		m.idleTimerFired(generation)
	}()
	go func() {
		defer wg.Done()
		<-start
		err = m.Disable(key)
	}()
	close(start)
	wg.Wait()
	if err != nil && !errors.Is(err, ErrNotActive) {
		t.Fatalf("Disable racing expiry: %v", err)
	}
	if current := idleTimerGeneration(m); current <= generation {
		t.Fatalf("expiry/Disable did not invalidate callback: old=%d current=%d", generation, current)
	}
	if got := m.StatusSnapshot(); got.State != Idle || got.Owned {
		t.Fatalf("snapshot after racing expiry and stop: %+v", got)
	}
	// Only an expiry that wins against Disable may leave cleanup behind.
	if _, cleanup := m.CleanupObligation(); cleanup != errors.Is(err, ErrNotActive) {
		t.Fatalf("cleanup = %v, Disable error = %v", cleanup, err)
	}
}

const idleTimerOperationTarget byte = 0x15

func TestOperationSuccessfulReadResetsIdleTimer(t *testing.T) {
	manager := newIdleTimerTestManager(t)
	ack := func(context.Context, byte) DispatchOutcome {
		return DispatchOutcome{Emitted: true, Native: NativeACK, Transport: manager.TransportKey()}
	}
	if _, err := manager.EnableOperation(context.Background(), idleTimerOperationTarget, ack); err != nil {
		t.Fatalf("enable = %v", err)
	}
	oldGeneration := idleTimerGeneration(manager)
	if _, err := manager.ReadOperation(context.Background(), idleTimerOperationTarget, ack); err != nil {
		t.Fatalf("successful read = %v", err)
	}
	if current := idleTimerGeneration(manager); current <= oldGeneration {
		t.Fatalf("successful read did not advance generation: old=%d current=%d", oldGeneration, current)
	}
	manager.idleTimerFired(oldGeneration)
	if got := manager.StatusSnapshot(); got.State != Active || !got.Owned {
		t.Fatalf("post-read idle snapshot = %+v, want owned Active", got)
	}
}

func TestOperationIdleExpiryWaitsForAdmittedReadACK(t *testing.T) {
	manager := newIdleTimerTestManager(t)
	cleanupStarted := make(chan struct{}, 1)
	manager.SetCleanupDispatcher(func(context.Context, byte) DispatchOutcome {
		cleanupStarted <- struct{}{}
		return DispatchOutcome{Emitted: true, Native: NativeACK, Transport: TransportKey{}}
	})
	ack := func(context.Context, byte) DispatchOutcome {
		return DispatchOutcome{Emitted: true, Native: NativeACK, Transport: TransportKey{}}
	}
	if _, err := manager.EnableOperation(context.Background(), idleTimerOperationTarget, ack); err != nil {
		t.Fatalf("enable: %v", err)
	}
	readEntered := make(chan struct{})
	releaseRead := make(chan struct{})
	readDone := make(chan error, 1)
	go func() {
		_, err := manager.ReadOperation(context.Background(), idleTimerOperationTarget, func(context.Context, byte) DispatchOutcome {
			close(readEntered)
			<-releaseRead
			return DispatchOutcome{Emitted: true, Native: NativeACK, Transport: TransportKey{}}
		})
		readDone <- err
	}()
	<-readEntered
	generation := idleTimerGeneration(manager)
	manager.idleTimerFired(generation)
	manager.stateMu.Lock()
	deferred := manager.idleExpiryPending
	manager.stateMu.Unlock()
	cleanupDuringRead := false
	select {
	case <-cleanupStarted:
		cleanupDuringRead = true
	default:
	}
	close(releaseRead)
	readErr := <-readDone
	if cleanupDuringRead {
		t.Fatal("idle cleanup crossed an admitted pending read")
	}
	if !deferred {
		t.Fatal("expiry did not defer to the admitted read")
	}
	if err := readErr; err != nil {
		t.Fatalf("read completion: %v", err)
	}
	manager.stateMu.Lock()
	stillDeferred := manager.idleExpiryPending
	manager.stateMu.Unlock()
	if current := idleTimerGeneration(manager); current <= generation || stillDeferred {
		t.Fatalf("ACK did not rearm and clear deferred expiry: old=%d current=%d deferred=%v", generation, current, stillDeferred)
	}
	manager.idleTimerFired(generation)
	select {
	case <-cleanupStarted:
		t.Fatal("successful read must re-arm instead of consuming its deferred idle expiry")
	default:
	}
	if got := manager.StatusSnapshot(); got.State != Active || !got.Owned {
		t.Fatalf("post-read state = %+v, want owned Active", got)
	}
}

func TestOperationIdleExpiryAfterAdmittedReadFailureRunsOneCleanup(t *testing.T) {
	manager := newIdleTimerTestManager(t)
	cleanupStarted := make(chan struct{}, 2)
	manager.SetCleanupDispatcher(func(context.Context, byte) DispatchOutcome {
		cleanupStarted <- struct{}{}
		return DispatchOutcome{Emitted: true, Native: NativeACK, Transport: TransportKey{}}
	})
	ack := func(context.Context, byte) DispatchOutcome {
		return DispatchOutcome{Emitted: true, Native: NativeACK, Transport: TransportKey{}}
	}
	if _, err := manager.EnableOperation(context.Background(), idleTimerOperationTarget, ack); err != nil {
		t.Fatalf("enable: %v", err)
	}
	readEntered := make(chan struct{})
	releaseRead := make(chan struct{})
	readDone := make(chan error, 1)
	readFailure := errors.New("read response failed")
	go func() {
		_, err := manager.ReadOperation(context.Background(), idleTimerOperationTarget, func(context.Context, byte) DispatchOutcome {
			close(readEntered)
			<-releaseRead
			return DispatchOutcome{Err: readFailure, Emitted: true, Native: NativeAmbiguous, Transport: TransportKey{}}
		})
		readDone <- err
	}()
	<-readEntered
	generation := idleTimerGeneration(manager)
	manager.idleTimerFired(generation)
	manager.stateMu.Lock()
	deferred := manager.idleExpiryPending
	manager.stateMu.Unlock()
	cleanupDuringRead := false
	select {
	case <-cleanupStarted:
		cleanupDuringRead = true
	default:
	}
	close(releaseRead)
	readErr := <-readDone
	if cleanupDuringRead {
		t.Fatal("idle cleanup crossed an admitted pending read")
	}
	if !deferred {
		t.Fatal("expiry did not defer to the admitted read")
	}
	if err := readErr; !errors.Is(err, readFailure) {
		t.Fatalf("read completion = %v, want exact read failure", err)
	}
	select {
	case <-cleanupStarted:
	default:
		t.Fatal("expired pending read failure did not run conservative cleanup")
	}
	select {
	case <-cleanupStarted:
		t.Fatal("expired pending read failure ran more than one cleanup")
	default:
	}
}
