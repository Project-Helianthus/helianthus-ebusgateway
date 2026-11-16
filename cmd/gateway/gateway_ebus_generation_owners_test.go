package main

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
)

// ebusGenerationOwnersTestOptions returns complete wiring for the generation
// owners on a static-fallback transport with a synthetic configured source.
func ebusGenerationOwnersTestOptions(t *testing.T) (ebusGenerationOwnerOptions, *retainedEBusConsumers) {
	t.Helper()
	retainedOpts := retainedTestOptions(t)
	retained := newRetainedTestConsumers(t, context.Background(), retainedOpts)
	cfg := ebusgateway.DefaultConfig()
	cfg.TransportConfig.Protocol = ebusgateway.TransportProtocol("unrecognised")
	cfg.ScanSource = 0x31
	cfg.ScanSourceAuto = false
	selection, err := newStartupSourceSelection(cfg)
	if err != nil {
		t.Fatalf("newStartupSourceSelection() error = %v", err)
	}
	if selection.admissionPath != ebusgateway.TransportAdmissionStaticFallback {
		t.Fatalf("admission path = %v; want the static fallback", selection.admissionPath)
	}
	return ebusGenerationOwnerOptions{
		Config:              cfg,
		Selection:           selection,
		RuntimeStateManager: runtimestate.New(runtimestate.Options{Path: filepath.Join(t.TempDir(), "runtime_state.json")}),
		Gateway:             retainedOpts.Gateway,
		Builder:             retainedOpts.Builder,
		Control:             retainedOpts.Control,
		Retained:            retained,
	}, retained
}

func newEBusGenerationOwnersForTest(t *testing.T, opts ebusGenerationOwnerOptions) *ebusGenerationOwners {
	t.Helper()
	owners, err := newEBusGenerationOwners(context.Background(), opts)
	if err != nil {
		t.Fatalf("newEBusGenerationOwners() error = %v", err)
	}
	return owners
}

func TestEBusGenerationOwnersRejectsMissingWiring(t *testing.T) {
	cases := []struct {
		name   string
		noCtx  bool
		remove func(*ebusGenerationOwnerOptions)
	}{
		{name: "process context", noCtx: true},
		{name: "runtime-state manager", remove: func(o *ebusGenerationOwnerOptions) { o.RuntimeStateManager = nil }},
		{name: "gateway", remove: func(o *ebusGenerationOwnerOptions) { o.Gateway = nil }},
		{name: "builder", remove: func(o *ebusGenerationOwnerOptions) { o.Builder = nil }},
		{name: "runtime control", remove: func(o *ebusGenerationOwnerOptions) { o.Control = nil }},
		{name: "retained consumers", remove: func(o *ebusGenerationOwnerOptions) { o.Retained = nil }},
		{name: "source-selection metrics", remove: func(o *ebusGenerationOwnerOptions) { o.Selection.metrics = nil }},
		{name: "source-selection artifact builder", remove: func(o *ebusGenerationOwnerOptions) { o.Selection.artifactBuilder = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts, _ := ebusGenerationOwnersTestOptions(t)
			ctx := context.Background()
			if tc.noCtx {
				ctx = nil
			}
			if tc.remove != nil {
				tc.remove(&opts)
			}
			owners, err := newEBusGenerationOwners(ctx, opts)
			if owners != nil || !errors.Is(err, errEBusGenerationOwnersWiring) || !strings.HasSuffix(err.Error(), ": "+tc.name) {
				t.Fatalf("newEBusGenerationOwners() = %v, %v; want a wiring error naming %q", owners, err, tc.name)
			}
		})
	}

	// Bus observability, the adapter classifier and the loaded runtime state
	// are optional.
	opts, _ := ebusGenerationOwnersTestOptions(t)
	opts.BusObservability, opts.AdapterClassifier, opts.RuntimeState = nil, nil, nil
	if _, err := newEBusGenerationOwners(context.Background(), opts); err != nil {
		t.Fatalf("newEBusGenerationOwners() without optional collaborators error = %v", err)
	}
}

// The runner configuration keeps the four task names, their prerequisites
// and their start order, and each task runs its owner method.
func TestEBusGenerationOwnersTaskGraph(t *testing.T) {
	opts, _ := ebusGenerationOwnersTestOptions(t)
	owners := newEBusGenerationOwnersForTest(t, opts)
	config := owners.taskConfig()

	want := []struct {
		name     string
		requires []string
		method   string
	}{
		{name: "ebus.source-admission", method: "runSourceAdmission"},
		{name: "ebus.semantic-surface", requires: []string{"ebus.source-admission"}, method: "runSemanticSurface"},
		{name: "ebus.startup-scan", requires: []string{"ebus.semantic-surface"}, method: "runStartupScan"},
		{name: "ebus.passive-observe-first", requires: []string{"ebus.startup-scan"}, method: "runPassiveObserveFirst"},
	}
	if len(config.Tasks) != len(want) {
		t.Fatalf("task count = %d; want %d", len(config.Tasks), len(want))
	}
	for i, task := range config.Tasks {
		if task.Name != want[i].name {
			t.Fatalf("task %d = %q; want %q", i, task.Name, want[i].name)
		}
		if !reflect.DeepEqual(task.Requires, want[i].requires) {
			t.Fatalf("task %q requires %v; want %v", task.Name, task.Requires, want[i].requires)
		}
		if task.Run == nil {
			t.Fatalf("task %q has no function", task.Name)
		}
		function := runtime.FuncForPC(reflect.ValueOf(task.Run).Pointer()).Name()
		if !strings.Contains(function, "(*ebusGenerationOwners)."+want[i].method) {
			t.Fatalf("task %q runs %s; want (*ebusGenerationOwners).%s", task.Name, function, want[i].method)
		}
	}
	if config.Admitted == nil || config.Failure == nil {
		t.Fatal("the runner configuration has no admission predicate or failure log")
	}
	// Without a composed bridge the runtime control admits no generation.
	if config.Admitted(genKey("inc.owners", 1, 1, 1)) {
		t.Fatal("the admission predicate admitted a generation without a composed bridge")
	}
	if _, err := newEBusGenerationTaskRunner(config); err != nil {
		t.Fatalf("the runner rejected the generation task configuration: %v", err)
	}
}

// ebusGenerationOwnersTestScope returns a scope for key that is not bound to
// a runner; only its key is read.
func ebusGenerationOwnersTestScope(key ebusGenerationKey) *ebusGenerationScope {
	return &ebusGenerationScope{run: &ebusGenerationRun{key: key}}
}

// Startup state is keyed by the native generation alone. A request for a
// different native generation drops every older entry; a key that differs
// only in its control fields (driver, source, epoch, driver generation or
// lifecycle sequence) shares the entry of its native generation.
func TestEBusGenerationOwnersKeepsStartupStateOnlyForNewestGeneration(t *testing.T) {
	opts, _ := ebusGenerationOwnersTestOptions(t)
	owners := newEBusGenerationOwnersForTest(t, opts)

	first := genKey("inc.owners", 1, 1, 1)
	firstState := owners.startupStateFor(ebusGenerationOwnersTestScope(first))
	firstState.admitted = true
	if again := owners.startupStateFor(ebusGenerationOwnersTestScope(first)); again != firstState {
		t.Fatal("the same generation received a second startup state")
	}

	for _, vary := range []struct {
		field  string
		mutate func(*ebusGenerationKey)
	}{
		{"driver", func(k *ebusGenerationKey) { k.Control.DriverID = "ebus.secondary" }},
		{"source", func(k *ebusGenerationKey) { k.Control.SourceID = "synthetic-source" }},
		{"epoch", func(k *ebusGenerationKey) { k.Control.Epoch = runtimecontrol.SourceEpoch("inc.other") }},
		{"driver generation", func(k *ebusGenerationKey) { k.Control.Generation = 7 }},
		{"lifecycle sequence", func(k *ebusGenerationKey) { k.Control.Sequence = 9 }},
	} {
		key := first
		vary.mutate(&key)
		if key == first {
			t.Fatalf("varying the %s did not change the key", vary.field)
		}
		if got := owners.startupStateFor(ebusGenerationOwnersTestScope(key)); got != firstState {
			t.Fatalf("a key that differs only in its %s received a different startup state", vary.field)
		}
	}

	second := first
	second.Native = 2
	secondState := owners.startupStateFor(ebusGenerationOwnersTestScope(second))
	if secondState == firstState || secondState.admitted {
		t.Fatal("a newer native generation inherited the older generation's startup state")
	}
	owners.startupStateMu.Lock()
	entries := len(owners.startupStates)
	_, keptFirst := owners.startupStates[first.Native]
	owners.startupStateMu.Unlock()
	if entries != 1 || keptFirst {
		t.Fatalf("startup states = %d entries, older kept = %v; want only the newest generation", entries, keptFirst)
	}
}

// A generation withdrawn before source admission installs no admitted source,
// records no startup state and runs none of the later tasks' effects; a live
// generation installs its configured source.
func TestEBusGenerationOwnersWithdrawnGenerationInstallsNothing(t *testing.T) {
	opts, retained := ebusGenerationOwnersTestOptions(t)
	owners := newEBusGenerationOwnersForTest(t, opts)
	originalScan := startDiscoveryScanLoopFn
	t.Cleanup(func() { startDiscoveryScanLoopFn = originalScan })
	startDiscoveryScanLoopFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder, activeTxnClassifier) startupScanSignals {
		t.Error("a withdrawn generation started the startup discovery scan")
		return startupScanSignals{}
	}

	fenced, fence := retainedTestScope(t)
	fence()
	ctx := context.Background()
	if err := owners.runSourceAdmission(ctx, fenced); err != nil {
		t.Fatalf("source admission of a withdrawn generation error = %v", err)
	}
	if source, ok := opts.Builder.AdmittedMutationSource(); ok {
		t.Fatalf("a withdrawn generation installed admitted source %#x", source)
	}
	if state := owners.startupStateFor(fenced); state.admitted {
		t.Fatal("a withdrawn generation recorded an admitted startup state")
	}
	if self := opts.RuntimeStateManager.State().EBus; self != nil && self.Self != nil {
		t.Fatal("a withdrawn generation wrote ebus.self")
	}
	for name, run := range map[string]func(context.Context, *ebusGenerationScope) error{
		"semantic surface":      owners.runSemanticSurface,
		"startup scan":          owners.runStartupScan,
		"passive observe-first": owners.runPassiveObserveFirst,
	} {
		if err := run(ctx, fenced); err != nil {
			t.Fatalf("%s of a withdrawn generation error = %v", name, err)
		}
	}
	if poller, promoter := retained.pollerAndPromoter(); poller != nil || promoter != nil {
		t.Fatal("a withdrawn generation created process-scoped consumers")
	}

	live, _ := retainedTestScope(t)
	if err := owners.runSourceAdmission(ctx, live); err != nil {
		t.Fatalf("source admission of a live generation error = %v", err)
	}
	if source, ok := opts.Builder.AdmittedMutationSource(); !ok || source != 0x31 {
		t.Fatalf("admitted source = %#x/%v; want the configured source", source, ok)
	}
	state := owners.startupStateFor(live)
	if !state.admitted || !state.static || state.staticSource != 0x31 {
		t.Fatalf("live startup state = admitted %v static %v source %#x; want the configured static source", state.admitted, state.static, state.staticSource)
	}
}
