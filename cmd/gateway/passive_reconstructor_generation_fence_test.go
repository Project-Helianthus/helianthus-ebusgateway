package main

// The process-scoped passive reconstructor is published for reuse only by a
// generation that is still live when its constructor returns. A generation
// fenced (STOP) while the constructor runs closes the handle it created, and
// the next generation creates its own; an unfenced generation publishes once
// and every successor reuses that handle.

import (
	"context"
	"go/token"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
)

// reconstructorFenceProbe replaces the reconstructor constructor and the
// broadcast listener seam. The first constructor call can be held open; every
// call builds a real reconstructor on its own in-process loopback tap.
type reconstructorFenceProbe struct {
	holdFirst    bool
	release      chan struct{}
	releaseFirst func()
	entered      chan int32
	calls        atomic.Int32

	mu        sync.Mutex
	created   []*ebusgateway.PassiveTransactionReconstructor
	listeners []*ebusgateway.PassiveTransactionReconstructor
}

func installReconstructorFenceProbe(t *testing.T, holdFirst bool) *reconstructorFenceProbe {
	t.Helper()
	probe := &reconstructorFenceProbe{
		holdFirst: holdFirst,
		release:   make(chan struct{}),
		entered:   make(chan int32, 8),
	}
	originalStart := startPassiveTransactionReconstructor
	originalListener := startBroadcastListenerWithReconstructorFn
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(probe.release) })
		startPassiveTransactionReconstructor = originalStart
		startBroadcastListenerWithReconstructorFn = originalListener
	})
	probe.releaseFirst = func() { releaseOnce.Do(func() { close(probe.release) }) }
	startPassiveTransactionReconstructor = func(ctx context.Context, cfg ebusgateway.Config) (*ebusgateway.PassiveTransactionReconstructor, error) {
		n := probe.calls.Add(1)
		probe.entered <- n
		if n == 1 && probe.holdFirst {
			select {
			case <-probe.release:
			case <-ctx.Done():
			}
		}
		cfg.PassiveTransport = transport.NewLoopback()
		created, err := ebusgateway.StartPassiveTransactionReconstructor(ctx, cfg)
		if err != nil {
			return nil, err
		}
		probe.mu.Lock()
		probe.created = append(probe.created, created)
		probe.mu.Unlock()
		return created, nil
	}
	startBroadcastListenerWithReconstructorFn = func(_ context.Context, _ *router.BusEventRouter, active *ebusgateway.PassiveTransactionReconstructor) (*ebusgateway.BroadcastListener, error) {
		probe.mu.Lock()
		probe.listeners = append(probe.listeners, active)
		probe.mu.Unlock()
		return nil, nil
	}
	return probe
}

func (probe *reconstructorFenceProbe) createdHandles() []*ebusgateway.PassiveTransactionReconstructor {
	probe.mu.Lock()
	defer probe.mu.Unlock()
	return append([]*ebusgateway.PassiveTransactionReconstructor(nil), probe.created...)
}

func (probe *reconstructorFenceProbe) attachedHandles() []*ebusgateway.PassiveTransactionReconstructor {
	probe.mu.Lock()
	defer probe.mu.Unlock()
	return append([]*ebusgateway.PassiveTransactionReconstructor(nil), probe.listeners...)
}

func (probe *reconstructorFenceProbe) waitEntered(t *testing.T, want int32) {
	t.Helper()
	deadline := time.After(rctlBound)
	for {
		select {
		case n := <-probe.entered:
			if n == want {
				return
			}
		case <-deadline:
			t.Fatalf("reconstructor constructor call %d did not start (calls %d)", want, probe.calls.Load())
		}
	}
}

// reconstructorClosed reports whether Close ran on the handle: a closed
// reconstructor refuses new subscribers.
func reconstructorClosed(handle *ebusgateway.PassiveTransactionReconstructor) bool {
	subscription, err := handle.Subscribe("generation_fence_probe", ebusgateway.PassiveSubscriberNonCritical, 1)
	if err != nil {
		return true
	}
	subscription.Close()
	return false
}

// fenceWhileConstructorHeld issues STOP while the first constructor call is
// held, waits until the generation is fenced with that task still pending,
// releases the constructor and waits for the STOP to settle.
func fenceWhileConstructorHeld(t *testing.T, env *rctlEnv, process *rctlProcess, probe *reconstructorFenceProbe) *ebusgateway.PassiveTransactionReconstructor {
	t.Helper()
	probe.waitEntered(t, 1)
	runner, _ := process.control.executor.tasks()
	if runner == nil {
		t.Fatal("generation task runner not bound")
	}
	process.decide(runtimecontrol.MethodStop)
	env.waitFor("generation fenced while the constructor is held", func() bool {
		return runner.Status().Phase == ebusGenerationDraining
	})
	probe.releaseFirst()
	process.waitObserved(drivermanager.ObservedStopped)
	env.waitFor("fenced generation joined", func() bool {
		return runner.Status().Phase != ebusGenerationDraining
	})
	created := probe.createdHandles()
	if len(created) != 1 {
		t.Fatalf("constructed %d reconstructors while fenced; want 1", len(created))
	}
	return created[0]
}

// requireSuccessorCreatesOwn starts a following generation and requires it to
// construct its own reconstructor instead of reusing the fenced one.
func requireSuccessorCreatesOwn(t *testing.T, process *rctlProcess, probe *reconstructorFenceProbe, fenced *ebusgateway.PassiveTransactionReconstructor) {
	t.Helper()
	process.decide(runtimecontrol.MethodStart)
	probe.waitEntered(t, 2)
	process.env.waitFor("successor reconstructor constructed", func() bool { return len(probe.createdHandles()) == 2 })
	successor := probe.createdHandles()[1]
	if successor == fenced {
		t.Fatal("successor generation received the fenced generation's reconstructor")
	}
	if reconstructorClosed(successor) {
		t.Fatal("successor generation's reconstructor was closed; want it published")
	}
	for _, attached := range probe.attachedHandles() {
		if attached == fenced {
			t.Fatal("observe-first consumers attached to the fenced generation's reconstructor")
		}
	}
}

// (a) The advisory source-selection warmup of a generation that is stopped
// while its reconstructor is being constructed publishes nothing and closes
// the handle it created.
func TestPassiveReconstructorGenerationFenceAdvisoryWarmup(t *testing.T) {
	env := newRCtlEnv(t)
	env.seed(false, nil)
	probe := installReconstructorFenceProbe(t, true)
	process := env.start(rctlProcessOptions{config: func(cfg *ebusgateway.Config) {
		// Observe-first on a source-selection-capable transport without a
		// configured override: the source-admission task runs the advisory
		// warmup and creates the reconstructor there.
		cfg.BroadcastListen = true
	}})

	fenced := fenceWhileConstructorHeld(t, env, process, probe)
	if !reconstructorClosed(fenced) {
		t.Error("reconstructor of the fenced advisory warmup is still open; want it closed when publication is refused")
	}
	requireSuccessorCreatesOwn(t, process, probe, fenced)
}

// (b) The passive observe-first task of a generation that is stopped while
// its reconstructor is being constructed publishes nothing and closes the
// handle it created.
func TestPassiveReconstructorGenerationFencePassiveObserveFirst(t *testing.T) {
	env := newRCtlEnv(t)
	env.seed(false, nil)
	probe := installReconstructorFenceProbe(t, true)
	process := env.start(rctlProcessOptions{config: passiveObserveFirstOnlyConfig})

	fenced := fenceWhileConstructorHeld(t, env, process, probe)
	if !reconstructorClosed(fenced) {
		t.Error("reconstructor of the fenced passive observe-first task is still open; want it closed when publication is refused")
	}
	if attached := probe.attachedHandles(); len(attached) != 0 {
		t.Errorf("observe-first consumers attached %d times from a fenced generation; want none", len(attached))
	}
	requireSuccessorCreatesOwn(t, process, probe, fenced)
}

// (c) An unfenced generation publishes its reconstructor once and the next
// generation reuses it without constructing another.
func TestPassiveReconstructorGenerationFenceLiveGenerationPublishesOnce(t *testing.T) {
	env := newRCtlEnv(t)
	env.seed(false, nil)
	probe := installReconstructorFenceProbe(t, false)
	process := env.start(rctlProcessOptions{config: passiveObserveFirstOnlyConfig})

	env.waitFor("first generation attaches its reconstructor", func() bool { return len(probe.attachedHandles()) == 1 })
	created := probe.createdHandles()
	if len(created) != 1 {
		t.Fatalf("constructed %d reconstructors; want 1", len(created))
	}
	process.decide(runtimecontrol.MethodRestart)
	env.waitFor("successor generation attaches the reused reconstructor", func() bool { return len(probe.attachedHandles()) == 2 })
	if calls := probe.calls.Load(); calls != 1 {
		t.Fatalf("constructor called %d times; want once for both generations", calls)
	}
	for i, attached := range probe.attachedHandles() {
		if attached != created[0] {
			t.Fatalf("generation %d attached a different reconstructor than the published one", i+1)
		}
	}
	if reconstructorClosed(created[0]) {
		t.Fatal("published reconstructor was closed by a generation change")
	}
}

// passiveObserveFirstOnlyConfig enables observe-first with a configured
// source that skips the advisory warmup, so only the passive observe-first
// task creates the reconstructor.
func passiveObserveFirstOnlyConfig(cfg *ebusgateway.Config) {
	source := uint8(rctlSource)
	cfg.BroadcastListen = true
	cfg.StartupSource.Source = &source
	cfg.StartupSource.Validate = false
}

// (d) The retained owner's publication method refuses a handle from a fenced
// generation and closes it, and publishes a live generation's handle open.
func TestPassiveReconstructorGenerationFencePublishMethod(t *testing.T) {
	retained := newRetainedTestConsumers(t, context.Background(), retainedTestOptions(t))

	fencedScope, fence := retainedTestScope(t)
	fence()
	refused := startRetainedTestReconstructor(t)
	if retained.publishReconstructor(fencedScope, refused) {
		t.Fatal("fenced generation published its reconstructor")
	}
	if retained.currentReconstructor() != nil {
		t.Fatal("fenced generation's reconstructor became the retained handle")
	}
	if !reconstructorClosed(refused) {
		t.Fatal("reconstructor refused for a fenced generation is still open")
	}

	liveScope, _ := retainedTestScope(t)
	published := startRetainedTestReconstructor(t)
	if !retained.publishReconstructor(liveScope, published) {
		t.Fatal("live generation could not publish its reconstructor")
	}
	if retained.currentReconstructor() != published || reconstructorClosed(published) {
		t.Fatal("live generation's reconstructor is not the open retained handle")
	}
}

// Both generation tasks that create the reconstructor publish it only through
// the retained owner's fenced publication method.
func TestPassiveReconstructorGenerationFenceTasksPublishThroughOwner(t *testing.T) {
	files, _ := lifecycleCompositionSources(t, token.NewFileSet())
	lifecycle := lifecycleFunctionDecl(t, files, "runGatewayLifecycle")
	calls := lifecycleSelectorCallPositions(lifecycle.Body)
	if got := len(calls["publishReconstructor"]); got != 2 {
		t.Fatalf("runGatewayLifecycle publishes the reconstructor through the owner %d times; want 2", got)
	}
	publish := lifecycleMethodDecl(t, files, "retainedEBusConsumers", "publishReconstructor")
	inner := lifecycleSelectorCallPositions(publish.Body)
	if len(inner["Commit"]) != 1 || len(inner["Close"]) != 1 {
		t.Fatalf("(*retainedEBusConsumers).publishReconstructor must commit once and close the refused handle: %v", inner)
	}
}
