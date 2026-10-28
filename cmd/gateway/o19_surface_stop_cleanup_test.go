package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
)

func o19Await(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("O19 obligation did not finish")
	}
}

func TestO19LateSurfaceCleanup(t *testing.T) {
	for _, failed := range []bool{false, true} {
		name := "proven-close"
		if failed {
			name = "failed-close"
		}
		t.Run(name, func(t *testing.T) {
			failures := 0
			if failed {
				failures = 1
			}
			plant := newCtlPlant(t, ctlPlantOptions{budget: 1, stopFailures: failures})
			var nativeStops atomic.Int32
			plant.ebus.desiredRunning = func() bool { nativeStops.Add(1); return false }

			retired := make(chan struct{}, 1)
			plant.hub.setHook("RetireEpoch", func() error { retired <- struct{}{}; return nil })
			var closeGate chan struct{}
			if !failed {
				closeGate = plant.gateNextClose()
			}

			var installOnce sync.Once
			entered, release, installed := make(chan struct{}), make(chan struct{}), make(chan struct{})
			released := false
			defer func() {
				if !released {
					close(release)
				}
			}()
			runner, err := newEBusGenerationTaskRunner(ebusGenerationTaskConfig{
				JoinTimeout: 20 * time.Millisecond,
				Admitted:    ebusControlGenerationAdmitted(plant.bridge),
				Tasks: []ebusGenerationTask{{Name: "o19.surface", Run: func(_ context.Context, scope *ebusGenerationScope) error {
					var err error
					installOnce.Do(func() {
						defer close(installed)
						err = installProcessSurface(scope, func() error { close(entered); <-release; return nil })
					})
					return err
				}}},
			})
			if err != nil {
				t.Fatal(err)
			}
			executor := &ebusControlExecutor{bridge: plant.bridge}
			executor.bind(runner, func(bool) {})
			plant.bridge.SetAdmitGate(executor.admitGate)
			corr := ctlCorr("o19.predecessor", 1, 1)
			if err := executor.Start(ctlCtx(t), corr); err != nil {
				t.Fatal(err)
			}
			o19Await(t, entered)
			foreign := ctlCorr("o19.unbound", 1, 1)
			if out := executor.Stop(ctlCtx(t), foreign); !out.NativeClosed || !out.SemanticCleaned {
				t.Fatalf("unbound epoch did not reach bridge: %+v", out)
			}
			if runner.Status().Phase != ebusGenerationRunning {
				t.Fatal("foreign epoch STOP fenced predecessor")
			}
			if err := plant.bridge.Start(ctlCtx(t), foreign); err == nil {
				t.Fatal("foreign epoch STOP did not tombstone its epoch")
			}
			if out := executor.Stop(ctlCtx(t), corr); out.NativeClosed || out.SemanticCleaned {
				t.Fatalf("premature proof: %+v", out)
			}
			if runner.Status().Phase != ebusGenerationDraining {
				t.Fatal("pending installation must drain")
			}
			executor.mu.Lock()
			pending := executor.o19Cleanup
			executor.mu.Unlock()
			if pending == nil {
				t.Fatal("missing generation-owned cleanup continuation")
			}
			if err := executor.Start(ctlCtx(t), ctlCorr("o19.successor", 1, 2)); err == nil {
				t.Fatal("successor admitted before proof")
			}
			if plant.constructed() != 1 {
				t.Fatal("successor constructed before proof")
			}
			close(release)
			released = true
			o19Await(t, installed)
			if !failed {
				o19Await(t, retired)
				select {
				case <-pending.done:
					t.Fatal("cleanup completed before native proof")
				default:
				}
				if err := executor.Start(ctlCtx(t), ctlCorr("o19.successor", 1, 2)); err == nil {
					t.Fatal("successor admitted while native close pending")
				}
				if runner.Status().Phase != ebusGenerationDraining {
					t.Fatal("native cleanup must remain joined work")
				}
				close(closeGate)
			}
			o19Await(t, pending.done)
			if join := runner.Withdraw(ctlCtx(t), pending.key); !join.Joined {
				t.Fatalf("cleanup not joined: %+v", join)
			}
			if pending.out.NativeClosed == failed || !pending.out.SemanticCleaned {
				t.Fatalf("close proof = %+v, failed=%v", pending.out, failed)
			}
			if plant.hub.count("RetireEpoch") != 1 {
				t.Fatal("cleanup must retire exactly once")
			}
			if failed {
				if !executor.o19CleanupBlocked() {
					t.Fatal("failed native close must remain quarantined")
				}
				if err := executor.Start(ctlCtx(t), ctlCorr("o19.successor", 1, 2)); err == nil {
					t.Fatal("successor admitted after failed close")
				}
			} else {
				if !plant.raw(0).isClosed() {
					t.Fatal("native endpoint escaped cleanup")
				}
				if executor.o19CleanupBlocked() {
					t.Fatal("proven cleanup still blocked")
				}
			}
			if out := executor.Stop(ctlCtx(t), corr); !out.NativeClosed || !out.SemanticCleaned {
				t.Fatalf("retried stop did not prove cleanup: %+v", out)
			}
			if executor.o19CleanupBlocked() {
				t.Fatal("retried cleanup still blocked")
			}
			if err := executor.Start(ctlCtx(t), ctlCorr("o19.successor", 1, 2)); err != nil {
				t.Fatalf("successor after retried cleanup: %v", err)
			}
			if plant.hub.count("RetireEpoch") != 1 {
				t.Fatal("repeated cleanup")
			}
			// The injected failure returns before the native runtime callback;
			// recovery must reach that callback exactly once in either case.
			if nativeStops.Load() != 1 {
				t.Fatalf("native STOP calls = %d; want exactly one", nativeStops.Load())
			}

		})
	}
}

func TestO19PredecessorCorrelation(t *testing.T) {
	corr := ctlCorr("o19.identity", 3, 7)
	corr.SourceID = ctlSource
	key := ebusGenerationKey{Control: ctlKey(corr), Native: 2}
	cases := []struct {
		name   string
		change func(*runtimecontrol.LifecycleCorrelation)
	}{
		{"driver", func(c *runtimecontrol.LifecycleCorrelation) { c.DriverID = "o19.other" }},
		{"epoch", func(c *runtimecontrol.LifecycleCorrelation) { c.Epoch = "o19.other" }},
		{"driver-generation", func(c *runtimecontrol.LifecycleCorrelation) { c.Generation++ }},
		{"source", func(c *runtimecontrol.LifecycleCorrelation) { c.SourceID = "source:o19-other" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !o19MatchesStop(key, corr) {
				t.Fatal("valid control refused")
			}
			changed := corr
			tc.change(&changed)
			if o19MatchesStop(key, changed) {
				t.Fatal("foreign predecessor matched")
			}
		})
	}
	corr.Sequence++
	if !o19MatchesStop(key, corr) {
		t.Fatal("STOP operation sequence is not predecessor identity")
	}
	corr.SourceID = ""
	if !o19MatchesStop(key, corr) {
		t.Fatal("service source binding refused")
	}
}

func TestO19CloseProofAdmissionGuard(t *testing.T) {
	for _, out := range []runtimecontrol.StopOutcome{{}, {NativeClosed: true}, {SemanticCleaned: true}, {NativeClosed: true, SemanticCleaned: true}} {
		pending := &o19SurfaceCleanup{done: make(chan struct{}), out: out}
		executor := &ebusControlExecutor{o19Cleanup: pending}
		if !executor.o19CleanupBlocked() {
			t.Fatal("pending cleanup admitted")
		}
		close(pending.done)
		want := !out.NativeClosed || !out.SemanticCleaned
		if executor.o19CleanupBlocked() != want {
			t.Fatalf("wrong proof guard for %+v", out)
		}
		if want && executor.admitGate(context.Background()) == nil {
			t.Fatal("replacement gate bypassed proof")
		}
	}
}

func o19ReadyExecutor(t *testing.T, plant *ctlPlant) (*ebusControlExecutor, runtimecontrol.LifecycleCorrelation) {
	t.Helper()
	ready := make(chan struct{})
	var readyOnce sync.Once
	runner, err := newEBusGenerationTaskRunner(ebusGenerationTaskConfig{
		JoinTimeout: time.Second,
		Admitted:    ebusControlGenerationAdmitted(plant.bridge),
		Tasks:       []ebusGenerationTask{{Name: "o19.ready", Run: func(context.Context, *ebusGenerationScope) error { readyOnce.Do(func() { close(ready) }); return nil }}},
	})
	if err != nil {
		t.Fatal(err)
	}
	executor := &ebusControlExecutor{bridge: plant.bridge}
	executor.bind(runner, func(bool) {})
	plant.bridge.SetAdmitGate(executor.admitGate)
	corr := ctlCorr("o19.ready", 1, 1)
	if err := executor.Start(ctlCtx(t), corr); err != nil {
		t.Fatal(err)
	}
	o19Await(t, ready)
	return executor, corr
}

func TestO19RetirementRetry(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 1})
	executor, corr := o19ReadyExecutor(t, plant)
	var nativeStops atomic.Int32
	plant.ebus.desiredRunning = func() bool { nativeStops.Add(1); return false }
	plant.hub.setHook("RetireEpoch", func() error { return errors.New("synthetic retirement failure") })
	if out := executor.Stop(ctlCtx(t), corr); !out.NativeClosed || out.SemanticCleaned {
		t.Fatalf("first stop = %+v", out)
	}
	if !executor.o19CleanupBlocked() {
		t.Fatal("missing retirement admitted successor")
	}
	plant.hub.setHook("RetireEpoch", nil)
	if out := executor.Stop(ctlCtx(t), corr); !out.NativeClosed || !out.SemanticCleaned {
		t.Fatalf("retried stop = %+v", out)
	}
	if nativeStops.Load() != 1 {
		t.Fatalf("proven native close repeated: %d", nativeStops.Load())
	}
	if err := executor.Start(ctlCtx(t), ctlCorr("o19.next", 1, 2)); err != nil {
		t.Fatalf("successor after retirement retry: %v", err)
	}
}

func TestO19NormalStopCallerContextAndSync(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 1, drain: 2 * time.Second})
	closeGate := plant.gateNextClose()
	defer close(closeGate)
	executor, corr := o19ReadyExecutor(t, plant)
	var synced atomic.Bool
	executor.bind(executor.runner, func(force bool) {
		if !force {
			synced.Store(true)
		}
	})
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	executor.Stop(ctx, corr)
	executor.mu.Lock()
	pending := executor.o19Cleanup
	executor.mu.Unlock()
	if pending == nil {
		t.Fatal("missing cleanup")
	}
	// Bridge Stop must finish its expired-context attempt while native close
	// remains held. A detached context instead waits for that close.
	select {
	case <-pending.done:
	case <-time.After(time.Second):
		t.Fatal("normal STOP replaced expired caller context")
	}
	// The initial return can precede the bridge's fence; another STOP joins
	// the completed attempt and must resync after the fence is visible.
	executor.Stop(ctx, corr)
	if !synced.Load() {
		t.Fatal("normal STOP did not resync runner")
	}
}

func TestO19NativeCloseRetry(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 1, stopFailures: 1})
	executor, corr := o19ReadyExecutor(t, plant)
	if out := executor.Stop(ctlCtx(t), corr); out.NativeClosed || !out.SemanticCleaned {
		t.Fatalf("first stop = %+v", out)
	}
	if err := executor.Start(ctlCtx(t), ctlCorr("o19.next", 1, 2)); err == nil {
		t.Fatal("successor admitted after failed native close")
	}
	if out := executor.Stop(ctlCtx(t), corr); !out.NativeClosed || !out.SemanticCleaned {
		t.Fatalf("retried stop = %+v", out)
	}
	if plant.hub.count("RetireEpoch") != 1 {
		t.Fatal("successful retirement repeated")
	}
	if err := executor.Start(ctlCtx(t), ctlCorr("o19.next", 1, 2)); err != nil {
		t.Fatalf("successor after native close retry: %v", err)
	}
}

func TestO19NormalStopSync(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 1})
	executor, corr := o19ReadyExecutor(t, plant)
	var synced atomic.Bool
	executor.bind(executor.runner, func(force bool) {
		if !force {
			synced.Store(true)
		}
	})
	if out := executor.Stop(ctlCtx(t), corr); !out.NativeClosed || !out.SemanticCleaned {
		t.Fatalf("normal stop = %+v", out)
	}
	if !synced.Load() {
		t.Fatal("normal STOP did not resync runner")
	}
}
