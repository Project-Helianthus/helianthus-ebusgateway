package runtimecontrol

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
)

func outputHooks() []HookDeclaration {
	return []HookDeclaration{{Name: HookOutput, Version: OutputHookVersion}}
}

func TestOutputHookRegistersOnNorthboundDescriptor(t *testing.T) {
	var calls atomic.Int64
	registry := NewRegistry(nil)
	north := fixture(&calls, "fake.output", DirectionNorthbound, "output-src")
	north.Hooks = []HookDeclaration{{Name: HookName("output"), Version: "1"}}
	if err := registry.Register(north); err != nil {
		t.Fatalf("Register northbound output descriptor: %v", err)
	}
	got, err := registry.Get("fake.output")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if want := []HookDeclaration{{Name: "output", Version: "1"}}; !reflect.DeepEqual(got.Driver.Hooks, want) {
		t.Fatalf("hooks = %+v, want %+v", got.Driver.Hooks, want)
	}
	if calls.Load() != 0 {
		t.Fatalf("factory calls = %d, want 0", calls.Load())
	}
}

func TestOutputHookSpellingAndVersion(t *testing.T) {
	if HookOutput != "output" || OutputHookVersion != "1" {
		t.Fatalf("output hook = %q version %q, want %q version %q", HookOutput, OutputHookVersion, "output", "1")
	}
	for _, reserved := range []HookName{HookMCP, HookSetup, HookPortal, HookSemantic, HookEvidence} {
		if reserved == HookOutput {
			t.Fatalf("output hook collides with reserved name %q", reserved)
		}
	}
}

func TestOutputHookRefusedOnBothDescriptor(t *testing.T) {
	var calls atomic.Int64
	registry := NewRegistry(nil)
	installed := fixture(&calls, "fake.output-installed", DirectionNorthbound, "installed-src")
	installed.Hooks = outputHooks()
	if err := registry.Register(installed); err != nil {
		t.Fatalf("Register: %v", err)
	}
	before := registry.List()
	both := fixture(&calls, "fake.output-both", DirectionBoth, "output-both-src")
	both.Hooks = outputHooks()
	err := registry.Register(both)
	var refused *RegistrationError
	if !errors.Is(err, ErrHookDirection) || !errors.As(err, &refused) || refused.Index != 0 || refused.DriverID != "fake.output-both" {
		t.Fatalf("Register both output descriptor = %v, want ErrHookDirection at descriptor 0", err)
	}
	if after := registry.List(); !reflect.DeepEqual(before, after) {
		t.Fatalf("registry changed after refusal:\nbefore %+v\nafter  %+v", before, after)
	}
	if _, err := registry.Get("fake.output-both"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("both output descriptor installed after refusal: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("factory calls = %d, want 0", calls.Load())
	}
	emptyBoth := fixture(&calls, "fake.output-both", DirectionBoth, "output-both-src")
	if err := registry.Register(emptyBoth); err != nil {
		t.Fatalf("Register both descriptor with an empty hook list: %v", err)
	}
}

func TestOutputHookEmptyHookListStillRegisters(t *testing.T) {
	for _, direction := range []Direction{DirectionSouthbound, DirectionNorthbound, DirectionBoth} {
		for _, hooks := range [][]HookDeclaration{nil, {}} {
			var calls atomic.Int64
			registry := NewRegistry(nil)
			descriptor := fixture(&calls, "fake.empty-hooks", direction, "empty-hooks-src")
			descriptor.Hooks = hooks
			if err := registry.Register(descriptor); err != nil {
				t.Fatalf("%s with %#v hooks: Register: %v", direction, hooks, err)
			}
			got, err := registry.Get("fake.empty-hooks")
			if err != nil || got.Driver.Hooks != nil {
				t.Fatalf("%s: Get = %+v, %v; want an entry with no hooks", direction, got, err)
			}
			if calls.Load() != 0 {
				t.Fatalf("factory calls = %d, want 0", calls.Load())
			}
		}
	}
}

func TestOutputHookEntryIsDetached(t *testing.T) {
	var calls atomic.Int64
	registry := NewRegistry(nil)
	descriptor := fixture(&calls, "fake.output-frozen", DirectionNorthbound, "output-frozen-src")
	descriptor.Hooks = outputHooks()
	callerHooks := descriptor.Hooks
	if err := registry.Register(descriptor); err != nil {
		t.Fatalf("Register: %v", err)
	}
	callerHooks[0] = HookDeclaration{Name: HookMCP, Version: "9"}
	listed := registry.List()
	listed.Drivers[0].Hooks[0].Version = "mutated-list"
	got, _ := registry.Get("fake.output-frozen")
	got.Driver.Hooks[0].Name = "mutated-get"

	again, err := registry.Get("fake.output-frozen")
	if err != nil || !reflect.DeepEqual(again.Driver.Hooks, outputHooks()) {
		t.Fatalf("frozen hooks = %+v, %v; want %+v", again.Driver.Hooks, err, outputHooks())
	}
	if relisted := registry.List(); !reflect.DeepEqual(relisted.Drivers[0], again.Driver) || relisted.CatalogRevision != again.CatalogRevision {
		t.Fatalf("list %+v incoherent with get %+v", relisted, again)
	}
}

func TestOutputHookRefusalKeepsLastKnownGood(t *testing.T) {
	var calls atomic.Int64
	registry := NewRegistry(nil)
	installed := fixture(&calls, "fake.output-installed", DirectionNorthbound, "installed-src")
	installed.Hooks = outputHooks()
	if err := registry.Register(installed); err != nil {
		t.Fatalf("Register: %v", err)
	}
	before := registry.List()
	sibling := fixture(&calls, "sibling.ok", DirectionNorthbound, "sibling-src")
	sibling.Hooks = outputHooks()
	south := fixture(&calls, "fake.output-south", DirectionSouthbound, "south-src")
	south.Hooks = outputHooks()
	err := registry.Register(sibling, south)
	var refused *RegistrationError
	if !errors.Is(err, ErrHookDirection) || !errors.As(err, &refused) || refused.Index != 1 || refused.DriverID != "fake.output-south" {
		t.Fatalf("Register error = %v, want ErrHookDirection at descriptor 1", err)
	}
	if after := registry.List(); !reflect.DeepEqual(before, after) {
		t.Fatalf("registry changed after refusal:\nbefore %+v\nafter  %+v", before, after)
	}
	if _, err := registry.Get("sibling.ok"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("valid sibling installed after refusal: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("factory calls = %d, want 0", calls.Load())
	}
}

func TestOutputHookLeavesResolveHookClassification(t *testing.T) {
	var calls atomic.Int64
	registry := NewRegistry(nil)
	descriptor := fixture(&calls, "fake.output-resolve", DirectionNorthbound, "output-resolve-src")
	descriptor.Hooks = outputHooks()
	if err := registry.Register(descriptor); err != nil {
		t.Fatalf("Register: %v", err)
	}
	for _, name := range []HookName{HookMCP, HookSetup, HookPortal, HookSemantic, HookEvidence} {
		if err := registry.ResolveHook("fake.output-resolve", name); !errors.Is(err, ErrHookUnimplemented) {
			t.Fatalf("ResolveHook(%s) = %v, want ErrHookUnimplemented", name, err)
		}
	}
	for _, name := range []HookName{"", "telemetry", "OUTPUT", "output.v1"} {
		if err := registry.ResolveHook("fake.output-resolve", name); !errors.Is(err, ErrUnknownHook) {
			t.Fatalf("ResolveHook(%q) = %v, want ErrUnknownHook", name, err)
		}
	}
}

// quarantiningRuntime is a synthetic northbound runtime whose stop cannot be
// proven closed and that then reports a safety quarantine.
type quarantiningRuntime struct {
	fakeRuntime
	quarantined atomic.Bool
}

func (r *quarantiningRuntime) Stop(context.Context) error {
	r.quarantined.Store(true)
	return drivermanager.ErrSafetyQuarantined
}

func (r *quarantiningRuntime) SafetyQuarantined() bool { return r.quarantined.Load() }

// managerFor builds the driver manager over one registered descriptor the way
// a composition does: the frozen binding's adapter constructs the runtime and
// the disposition selects enablement.
func managerFor(t *testing.T, registry *Registry, id DriverID) *drivermanager.Manager {
	t.Helper()
	binding, err := registry.Binding(id)
	if err != nil {
		t.Fatalf("Binding: %v", err)
	}
	entry, err := registry.Get(id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	runtime, err := binding.Adapter.Construct(context.Background(), binding.Construction)
	if err != nil {
		t.Fatalf("Construct: %v", err)
	}
	manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{
		ID:      string(id),
		Enabled: entry.Driver.Disposition == DispositionEnabled,
		Runtime: runtime,
	}}})
	if err != nil {
		t.Fatalf("drivermanager.New: %v", err)
	}
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	return manager
}

func assertHealth(t *testing.T, manager *drivermanager.Manager, id DriverID, observed drivermanager.ObservedState, reason drivermanager.ReasonCode, quarantined bool) {
	t.Helper()
	snapshot, ok := manager.Snapshot(string(id))
	if !ok {
		t.Fatalf("no manager snapshot for %s", id)
	}
	if snapshot.ObservedState != observed || snapshot.Reason.Code != reason || snapshot.SafetyQuarantined != quarantined {
		t.Fatalf("health = {%s %s %v}, want {%s %s %v}", snapshot.ObservedState, snapshot.Reason.Code, snapshot.SafetyQuarantined, observed, reason, quarantined)
	}
}

func TestOutputHookLifecycleAndHealthAreTheManagerSnapshot(t *testing.T) {
	var calls atomic.Int64
	registry := NewRegistry(nil)
	descriptor := fixture(&calls, "fake.output-life", DirectionNorthbound, "output-life-src")
	descriptor.Hooks = outputHooks()
	if err := registry.Register(descriptor); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("registration invoked the adapter %d times", calls.Load())
	}
	manager := managerFor(t, registry, "fake.output-life")
	ctx := context.Background()
	assertHealth(t, manager, "fake.output-life", drivermanager.ObservedStopped, drivermanager.ReasonNone, false)
	if err := manager.Start(ctx, "fake.output-life"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	assertHealth(t, manager, "fake.output-life", drivermanager.ObservedRunning, drivermanager.ReasonNone, false)
	if err := manager.Stop(ctx, "fake.output-life"); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	assertHealth(t, manager, "fake.output-life", drivermanager.ObservedStopped, drivermanager.ReasonOperatorStopped, false)
}

func TestOutputHookDisabledDescriptorHealthIsDisabled(t *testing.T) {
	var calls atomic.Int64
	registry := NewRegistry(nil)
	descriptor := fixture(&calls, "fake.output-off", DirectionNorthbound, "output-off-src")
	descriptor.Disposition = DispositionDisabled
	descriptor.Hooks = outputHooks()
	if err := registry.Register(descriptor); err != nil {
		t.Fatalf("Register: %v", err)
	}
	manager := managerFor(t, registry, "fake.output-off")
	assertHealth(t, manager, "fake.output-off", drivermanager.ObservedDisabled, drivermanager.ReasonConfigDisabled, false)
}

func TestOutputHookSafetyQuarantineIsTheManagerFlag(t *testing.T) {
	var calls atomic.Int64
	runtime := &quarantiningRuntime{}
	registry := NewRegistry(nil)
	descriptor := fixture(&calls, "fake.output-quarantine", DirectionNorthbound, "output-quarantine-src")
	descriptor.Hooks = outputHooks()
	descriptor.Adapter = NewAdapter(func(context.Context, Construction) (*quarantiningRuntime, error) {
		calls.Add(1)
		return runtime, nil
	})
	if err := registry.Register(descriptor); err != nil {
		t.Fatalf("Register: %v", err)
	}
	manager := managerFor(t, registry, "fake.output-quarantine")
	ctx := context.Background()
	if err := manager.Start(ctx, "fake.output-quarantine"); err != nil {
		t.Fatalf("Start: %v", err)
	}
	assertHealth(t, manager, "fake.output-quarantine", drivermanager.ObservedRunning, drivermanager.ReasonNone, false)
	if err := manager.Stop(ctx, "fake.output-quarantine"); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	assertHealth(t, manager, "fake.output-quarantine", drivermanager.ObservedFailed, drivermanager.ReasonCloseUnconfirmed, true)
	if err := manager.Start(ctx, "fake.output-quarantine"); !errors.Is(err, drivermanager.ErrSafetyQuarantined) {
		t.Fatalf("Start after quarantine = %v, want ErrSafetyQuarantined", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("factory calls = %d, want 1", calls.Load())
	}
}

// TestOutputHookProviderContractStatesException pins the companion provider
// contract to the admission this package implements, so a reader of that
// contract does not refuse an output declaration the registry admits.
func TestOutputHookProviderContractStatesException(t *testing.T) {
	raw, err := os.ReadFile("../../docs/architecture/runtime-driver-provider-contract-v1.md")
	if err != nil {
		t.Fatal(err)
	}
	page := strings.Join(strings.Fields(string(raw)), " ")
	for _, want := range []string{
		"An empty hook list is accepted on every direction.",
		"The one admitted hook declaration is the single name `output` at hook version `1` on a `northbound` descriptor",
		"`output` on a `southbound` or `both` descriptor fails with `ErrHookDirection`",
		"`output` with no version or another version with `ErrUnsupportedHookVersion`",
		"a repeated `output` with `ErrDuplicateHook`",
	} {
		if !strings.Contains(page, want) {
			t.Errorf("provider contract does not state %q", want)
		}
	}
	if strings.Contains(page, "An empty owned-source set is accepted. Every hook declaration fails:") {
		t.Error("provider contract still says every hook declaration fails")
	}
}
