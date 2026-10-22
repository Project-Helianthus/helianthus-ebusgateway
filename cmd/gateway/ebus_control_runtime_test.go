package main

// Tests for the S4 manager-to-hub bridge of the lifecycle-only ebus.primary
// driver. Every endpoint is a synthetic in-process transport; the hub is an
// in-memory semhub incarnation and the durable control store is a temporary
// runtime_control file. Nothing here dials, listens on a bus or opens a
// device, and nothing installs the bridge into process startup.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/b503session"
	ebuserrors "github.com/Project-Helianthus/helianthus-ebusgo/errors"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

const ctlSource runtimecontrol.SourceID = "source:ebus-primary-synthetic"

// --- synthetic endpoint ---

type ctlRaw struct {
	closed    chan struct{}
	closeOnce sync.Once
	closeGate <-chan struct{}
}

func (raw *ctlRaw) ReadByte() (byte, error) {
	<-raw.closed
	return 0, ebuserrors.ErrTransportClosed
}

func (*ctlRaw) Write(payload []byte) (int, error) { return len(payload), nil }

func (raw *ctlRaw) Close() error {
	if raw.closeGate != nil {
		<-raw.closeGate
	}
	raw.closeOnce.Do(func() { close(raw.closed) })
	return nil
}

func (raw *ctlRaw) isClosed() bool {
	select {
	case <-raw.closed:
		return true
	default:
		return false
	}
}

// --- hub wrapper with per-operation hooks ---

type ctlHub struct {
	*semhub.Hub
	mu    sync.Mutex
	hooks map[string]func() error
	calls map[string]int
}

func (hub *ctlHub) before(op string) error {
	hub.mu.Lock()
	hub.calls[op]++
	hook := hub.hooks[op]
	hub.mu.Unlock()
	if hook != nil {
		return hook()
	}
	return nil
}

func (hub *ctlHub) setHook(op string, hook func() error) {
	hub.mu.Lock()
	hub.hooks[op] = hook
	hub.mu.Unlock()
}

func (hub *ctlHub) count(op string) int {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	return hub.calls[op]
}

func (hub *ctlHub) AdmitSource(request semhub.Admission) (*semhub.Handle, error) {
	if err := hub.before("AdmitSource"); err != nil {
		return nil, err
	}
	return hub.Hub.AdmitSource(request)
}

func (hub *ctlHub) AdmitSuccessor(request semhub.Succession) (*semhub.Handle, error) {
	if err := hub.before("AdmitSuccessor"); err != nil {
		return nil, err
	}
	return hub.Hub.AdmitSuccessor(request)
}

func (hub *ctlHub) Fence(request semhub.FenceRequest) (semhub.ControlResult, error) {
	if err := hub.before("Fence"); err != nil {
		return semhub.ControlResult{}, err
	}
	return hub.Hub.Fence(request)
}

func (hub *ctlHub) Activate(previous *semhub.Handle, generation uint64) (*semhub.Handle, error) {
	if err := hub.before("Activate"); err != nil {
		return nil, err
	}
	return hub.Hub.Activate(previous, generation)
}

func (hub *ctlHub) RetireEpoch(request semhub.RetireRequest) (semhub.ControlResult, error) {
	if err := hub.before("RetireEpoch"); err != nil {
		return semhub.ControlResult{}, err
	}
	return hub.Hub.RetireEpoch(request)
}

func (hub *ctlHub) Source(id semreg.SourceID) (semhub.SourceInfo, bool) {
	if err := hub.before("Source"); err != nil {
		return semhub.SourceInfo{}, false
	}
	return hub.Hub.Source(id)
}

// --- runtime with a scripted Stop failure (INTERNAL_ERROR class) ---

type ctlStopOverride struct {
	*ebusDriverRuntime
	mu       sync.Mutex
	failures int
}

func (runtime *ctlStopOverride) Stop(ctx context.Context) error {
	runtime.mu.Lock()
	fail := runtime.failures > 0
	if fail {
		runtime.failures--
	}
	runtime.mu.Unlock()
	if fail {
		return errors.New("synthetic stop failure without close classification")
	}
	return runtime.ebusDriverRuntime.Stop(ctx)
}

// --- process-level plant: manager, eBUS runtime, hub and bridge ---

type ctlPlantOptions struct {
	budget       int
	initialDelay time.Duration
	drain        time.Duration
	stopFailures int
	proxy        bool
	b503         bool
	noPrepare    bool
}

type ctlPlant struct {
	t        *testing.T
	mu       sync.Mutex
	raws     []*ctlRaw
	overlap  bool
	hooks    []func(context.Context) error
	gates    []chan struct{}
	allGates []chan struct{}
	proxies  []*issue851ProxyLifecycle

	journal *semhub.MemoryJournal
	hub     *ctlHub
	ebus    *ebusDriverRuntime
	manager *drivermanager.Manager
	bridge  *ebusControlRuntime
	b503    *b503Runtime
	ctrl    *ebusDriverController
}

func newCtlPlant(t *testing.T, opts ctlPlantOptions) *ctlPlant {
	t.Helper()
	plant := &ctlPlant{t: t, journal: semhub.NewMemoryJournal()}
	hub, err := semhub.New(semhub.Options{Journal: plant.journal})
	if err != nil {
		t.Fatalf("semhub.New: %v", err)
	}
	plant.hub = &ctlHub{Hub: hub, hooks: map[string]func() error{}, calls: map[string]int{}}
	drain := opts.drain
	if drain == 0 {
		drain = 500 * time.Millisecond
	}
	initialDelay := opts.initialDelay
	if initialDelay == 0 {
		initialDelay = time.Millisecond
	}
	runtimeSeam := transport.NewDriverRuntime(plant.factory(opts.proxy), transport.DriverRuntimeConfig{DrainTimeout: drain})
	plant.ebus = &ebusDriverRuntime{runtime: runtimeSeam}
	var runtime drivermanager.Runtime = plant.ebus
	if opts.stopFailures > 0 {
		runtime = &ctlStopOverride{ebusDriverRuntime: plant.ebus, failures: opts.stopFailures}
	}
	plant.manager, err = drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{
		ID: primaryEBusDriverID, Enabled: true, Runtime: runtime,
		Capabilities: []drivermanager.Capability{
			drivermanager.CapabilityDiscovery, drivermanager.CapabilityRawEvidence, drivermanager.CapabilityRead,
			drivermanager.CapabilitySemanticProjection, drivermanager.CapabilityWrite,
		},
		ClassifyError: classifyEBusDriverError,
		Retry:         drivermanager.RetryPolicy{Budget: opts.budget, InitialDelay: initialDelay, MaxDelay: initialDelay, JitterRatio: -1},
	}}, LateRetireTimeout: time.Second})
	if err != nil {
		t.Fatalf("drivermanager.New: %v", err)
	}
	reporter := func(correlation drivermanager.Correlation, rawErr error) {
		plant.manager.ReportFailure(primaryEBusDriverID, correlation, classifyEBusDriverError(rawErr))
	}
	plant.ebus.reporter = reporter
	plant.ctrl = &ebusDriverController{manager: plant.manager, runtime: plant.ebus, proxyConfigured: opts.proxy}
	if opts.b503 {
		plant.b503 = &b503Runtime{manager: b503session.New(b503session.TransportKey{AdapterInstanceID: "gateway"}, 30*time.Second, b503StubRefresh)}
		plant.ctrl.SetLifecycleObserver(plant.b503)
	}
	plant.bridge, err = newEBusControlRuntime(ebusControlConfig{
		Manager: plant.manager, Hub: plant.hub, Source: ctlSource, ClockEpoch: "clock:ebus-control-test",
		CleanupTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("newEBusControlRuntime: %v", err)
	}
	t.Cleanup(plant.close)
	if !opts.noPrepare {
		if err := plant.bridge.Prepare(context.Background()); err != nil {
			t.Fatalf("Prepare: %v", err)
		}
	}
	return plant
}

func (plant *ctlPlant) close() {
	plant.mu.Lock()
	for _, gate := range plant.allGates {
		select {
		case <-gate:
		default:
			close(gate)
		}
	}
	plant.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = plant.manager.Shutdown(ctx)
	_ = plant.bridge.Close(ctx)
}

func (plant *ctlPlant) factory(proxy bool) transport.DriverRuntimeFactory {
	return func(ctx context.Context) (*transport.ManagedRawTransport, error) {
		plant.mu.Lock()
		for _, raw := range plant.raws {
			if !raw.isClosed() {
				plant.overlap = true
			}
		}
		var hook func(context.Context) error
		if len(plant.hooks) > 0 {
			hook, plant.hooks = plant.hooks[0], plant.hooks[1:]
		}
		var gate chan struct{}
		if len(plant.gates) > 0 {
			gate, plant.gates = plant.gates[0], plant.gates[1:]
		}
		plant.mu.Unlock()
		if hook != nil {
			if err := hook(ctx); err != nil {
				return nil, err
			}
		}
		raw := &ctlRaw{closed: make(chan struct{})}
		if gate != nil {
			raw.closeGate = gate
		}
		plant.mu.Lock()
		plant.raws = append(plant.raws, raw)
		plant.mu.Unlock()
		managed := newManagedEBusGeneration(ctx, raw, raw.Close)
		if proxy {
			generation := managed.Transport.(*managedEBusGeneration)
			lifecycle := newIssue851ProxyLifecycle()
			generation.proxyListenerBound = true
			generation.proxyLifecycle = lifecycle
			plant.mu.Lock()
			plant.proxies = append(plant.proxies, lifecycle)
			plant.mu.Unlock()
		}
		return managed, nil
	}
}

// script queues a per-construction hook (nil for a plain success).
func (plant *ctlPlant) script(hooks ...func(context.Context) error) {
	plant.mu.Lock()
	plant.hooks = append(plant.hooks, hooks...)
	plant.mu.Unlock()
}

// gateNextClose makes the next constructed endpoint's Close block on the
// returned channel.
func (plant *ctlPlant) gateNextClose() chan struct{} {
	gate := make(chan struct{})
	plant.mu.Lock()
	plant.gates = append(plant.gates, gate)
	plant.allGates = append(plant.allGates, gate)
	plant.mu.Unlock()
	return gate
}

func (plant *ctlPlant) constructed() int {
	plant.mu.Lock()
	defer plant.mu.Unlock()
	return len(plant.raws)
}

func (plant *ctlPlant) raw(index int) *ctlRaw {
	plant.mu.Lock()
	defer plant.mu.Unlock()
	return plant.raws[index]
}

func (plant *ctlPlant) overlapped() bool {
	plant.mu.Lock()
	defer plant.mu.Unlock()
	return plant.overlap
}

func (plant *ctlPlant) snapshot() drivermanager.Snapshot {
	snapshot, _ := plant.manager.Snapshot(primaryEBusDriverID)
	return snapshot
}

func (plant *ctlPlant) records() []semhub.EpochRecord { return plant.journal.Records() }

func (plant *ctlPlant) successionRecords() []semhub.EpochRecord {
	var out []semhub.EpochRecord
	for _, record := range plant.records() {
		if record.Ordinal != 0 {
			out = append(out, record)
		}
	}
	return out
}

func (plant *ctlPlant) source() semhub.SourceInfo {
	info, _ := plant.hub.Hub.Source(semreg.SourceID(ctlSource))
	return info
}

func ctlCorr(epoch string, generation, sequence uint64) runtimecontrol.LifecycleCorrelation {
	return runtimecontrol.LifecycleCorrelation{DriverID: primaryEBusDriverID, Epoch: runtimecontrol.SourceEpoch(epoch),
		Generation: runtimecontrol.DriverGeneration(generation), Sequence: runtimecontrol.LifecycleSequence(sequence)}
}

func ctlKey(corr runtimecontrol.LifecycleCorrelation) ebusControlKey {
	return ebusControlKey{DriverID: corr.DriverID, SourceID: ctlSource, Epoch: corr.Epoch, Generation: corr.Generation, Sequence: corr.Sequence}
}

func ctlWait(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func ctlCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// requireResponsive proves that manager reads are not blocked by a lock held
// across hub work: a Watch from another goroutine returns within the bound.
func requireResponsive(t *testing.T, manager *drivermanager.Manager) drivermanager.Snapshot {
	t.Helper()
	done := make(chan drivermanager.Snapshot, 1)
	go func() {
		snapshot, _, _ := manager.Watch(primaryEBusDriverID)
		done <- snapshot
	}()
	select {
	case snapshot := <-done:
		return snapshot
	case <-time.After(200 * time.Millisecond):
		t.Fatal("manager read blocked while hub work ran: a manager lock is held")
		return drivermanager.Snapshot{}
	}
}

func controlReason(err error) drivermanager.ReasonCode {
	var coded interface {
		ReasonCode() drivermanager.ReasonCode
	}
	if errors.As(err, &coded) {
		return coded.ReasonCode()
	}
	return ""
}

// --- S0/A1 registration and S4 preparation ---

func TestEBusControlDescriptorIsLifecycleOnlyAndRequiresSuppliedIdentity(t *testing.T) {
	native := runtimecontrol.NativeContractRef{Owner: "github.com/Project-Helianthus/helianthus-ebusgo", Contract: "synthetic-transport-runtime", Version: "synthetic-1"}
	adapter := newEBusPrimaryLifecycleAdapter(ebusgateway.Config{})
	for name, build := range map[string]func() (runtimecontrol.Descriptor, error){
		"no owner": func() (runtimecontrol.Descriptor, error) {
			n := native
			n.Owner = ""
			return newEBusPrimaryDescriptor(n, ctlSource, adapter)
		},
		"no contract": func() (runtimecontrol.Descriptor, error) {
			n := native
			n.Contract = ""
			return newEBusPrimaryDescriptor(n, ctlSource, adapter)
		},
		"no version": func() (runtimecontrol.Descriptor, error) {
			n := native
			n.Version = ""
			return newEBusPrimaryDescriptor(n, ctlSource, adapter)
		},
		"no source":  func() (runtimecontrol.Descriptor, error) { return newEBusPrimaryDescriptor(native, "", adapter) },
		"no adapter": func() (runtimecontrol.Descriptor, error) { return newEBusPrimaryDescriptor(native, ctlSource, nil) },
	} {
		if _, err := build(); !errors.Is(err, errEBusControlInvalidConfig) {
			t.Fatalf("%s: error = %v, want invalid configuration (no placeholder)", name, err)
		}
	}
	descriptor, err := newEBusPrimaryDescriptor(native, ctlSource, adapter)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.DriverID != primaryEBusDriverID || descriptor.Direction != runtimecontrol.DirectionSouthbound ||
		descriptor.Disposition != runtimecontrol.DispositionEnabled || len(descriptor.Hooks) != 0 ||
		!reflect.DeepEqual(descriptor.Sources, []runtimecontrol.SourceID{ctlSource}) || descriptor.Format != runtimecontrol.DescriptorFormatV1 {
		t.Fatalf("descriptor = %#v", descriptor)
	}
	registry := runtimecontrol.NewRegistry(nil)
	if err := registry.Register(descriptor); err != nil {
		t.Fatalf("Register: %v", err)
	}
	binding, err := registry.Binding(primaryEBusDriverID)
	if err != nil {
		t.Fatal(err)
	}
	built, err := binding.Adapter.Construct(context.Background(), binding.Construction)
	if err != nil {
		t.Fatalf("Construct: %v", err)
	}
	runtime, ok := built.(*ebusDriverRuntime)
	if !ok || runtime.Generation() != 0 || runtime.SafetyQuarantined() {
		t.Fatalf("constructed runtime = %#v, want an unopened eBUS runtime", built)
	}
	if _, err := binding.Adapter.Construct(context.Background(), runtimecontrol.Construction{DriverID: "fam.other"}); err == nil {
		t.Fatal("adapter constructed a runtime for another driver")
	}
}

func TestEBusControlPrepareAdmitsInertSourceBeforeAnyEndpoint(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 1, noPrepare: true})
	if err := plant.bridge.Start(ctlCtx(t), ctlCorr("e1", 1, 1)); controlReason(err) != drivermanager.ReasonRuntimeNotReady {
		t.Fatalf("Start before Prepare = %v, want RUNTIME_NOT_READY", err)
	}
	if err := plant.bridge.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := plant.bridge.Prepare(context.Background()); err != nil {
		t.Fatalf("repeated Prepare: %v", err)
	}
	if plant.constructed() != 0 || plant.snapshot().ObservedState != drivermanager.ObservedStopped {
		t.Fatalf("Prepare constructed %d endpoints, manager %s", plant.constructed(), plant.snapshot().ObservedState)
	}
	records := plant.records()
	if len(records) != 1 || records[0].Kind != semhub.EventEpochAdmitted || records[0].Generation != 1 || len(records[0].Assets) != 0 || records[0].Source != semreg.SourceID(ctlSource) {
		t.Fatalf("journal after Prepare = %#v, want one asset-free generation-1 admission", records)
	}
	if info := plant.source(); !info.Live || info.Driver != primaryEBusDriverID || info.Generation != 1 {
		t.Fatalf("source = %#v", info)
	}
	view := plant.bridge.View()
	if view.Admitted || len(view.NativeCapabilities) != 0 || len(view.SemanticCapabilities) != 0 {
		t.Fatalf("view after Prepare = %#v, want inert source with no admission", view)
	}
}

// LIFE-01 and acceptance 1: admission and observed RUNNING become visible
// together only after the correlation is current, the hub obligation held
// and readiness was proven; B503 activation still precedes RUNNING.
func TestEBusControlReadyAdmissionAndRunningBecomeVisibleTogether(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 1, b503: true})
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	plant.hub.setHook("Source", func() error {
		once.Do(func() {
			close(entered)
			<-release
		})
		return nil
	})
	corr := ctlCorr("inc.e1", 1, 4)
	done := make(chan error, 1)
	go func() { done <- plant.bridge.Start(ctlCtx(t), corr) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("admission hold did not reach the hub")
	}
	held := requireResponsive(t, plant.manager)
	if held.ObservedState != drivermanager.ObservedStarting || len(held.EffectiveCapabilities) != 0 || held.Generation != 0 {
		t.Fatalf("manager during hold = %#v", held)
	}
	if view := plant.bridge.View(); view.Admitted || len(view.NativeCapabilities) != 0 {
		t.Fatalf("view during hold = %#v", view)
	}
	if _, err := plant.manager.Invoke(context.Background(), primaryEBusDriverID, drivermanager.CapabilityRead, func(any) error { return nil }); !errors.Is(err, drivermanager.ErrUnavailable) {
		t.Fatalf("Invoke during hold = %v, want unavailable", err)
	}
	if got := plant.b503.manager.TransportKey().TransportEpoch; got != 0 {
		t.Fatalf("B503 activated at epoch %d during the hold", got)
	}
	if plant.bridge.Admitted(ctlKey(corr)) {
		t.Fatal("key admitted during the hold")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Start: %v", err)
	}
	running := plant.snapshot()
	view := plant.bridge.View()
	if running.ObservedState != drivermanager.ObservedRunning || !view.Admitted || view.Observed != drivermanager.ObservedRunning ||
		!reflect.DeepEqual(view.NativeCapabilities, running.EffectiveCapabilities) || len(view.NativeCapabilities) == 0 || len(view.SemanticCapabilities) != 0 {
		t.Fatalf("after hold: manager=%#v view=%#v", running, view)
	}
	if got := plant.b503.manager.TransportKey().TransportEpoch; got != running.Generation {
		t.Fatalf("B503 epoch = %d, want admitted native generation %d", got, running.Generation)
	}
	mappings := plant.bridge.Mappings()
	if len(mappings) != 1 || mappings[0].Key != ctlKey(corr) || uint64(mappings[0].Native) != running.Generation || mappings[0].Automatic {
		t.Fatalf("mappings = %#v", mappings)
	}
	if !plant.bridge.Admitted(ctlKey(corr)) {
		t.Fatal("exact key not admitted after RUNNING")
	}
}

// Acceptance 1: every field of the correlation is checked. Each variant
// names the production mutation it catches.
func TestEBusControlFullCorrelationRejectsLateConstructionHealthAndPublication(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 1})
	corr := ctlCorr("inc.e1", 1, 5)
	if err := plant.bridge.Start(ctlCtx(t), corr); err != nil {
		t.Fatal(err)
	}
	key := ctlKey(corr)
	variants := map[string]ebusControlKey{
		// DriverID: a gate that ignored the driver would admit another driver's key.
		"driver": func() ebusControlKey { k := key; k.DriverID = "fam.other"; return k }(),
		// SourceID: a gate keyed without the source would admit a foreign source.
		"source": func() ebusControlKey { k := key; k.SourceID = "source:other"; return k }(),
		// Epoch: dropping the epoch term would admit a prior epoch's callback.
		"epoch": func() ebusControlKey { k := key; k.Epoch = "inc.e0"; return k }(),
		// Generation: dropping it would admit a fenced generation of this epoch.
		"generation": func() ebusControlKey { k := key; k.Generation = 2; return k }(),
		// Sequence: dropping it would admit a superseded lifecycle operation.
		"sequence": func() ebusControlKey { k := key; k.Sequence = 6; return k }(),
	}
	before := plant.snapshot()
	records := len(plant.records())
	for name, variant := range variants {
		if plant.bridge.Admitted(variant) {
			t.Fatalf("%s: stale key admitted", name)
		}
		if _, err := plant.bridge.PublicationHandle(variant); err == nil {
			t.Fatalf("%s: stale key obtained a publication handle", name)
		}
		if plant.bridge.ReportFailure(variant, drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonDependencyUnavailable, Retryable: true}}) {
			t.Fatalf("%s: stale health report accepted", name)
		}
	}
	for name, late := range map[string]runtimecontrol.LifecycleCorrelation{
		"driver":     {DriverID: "fam.other", Epoch: corr.Epoch, Generation: 1, Sequence: 5},
		"source":     {DriverID: primaryEBusDriverID, SourceID: "source:other", Epoch: corr.Epoch, Generation: 1, Sequence: 5},
		"epoch":      ctlCorr("inc.e9", 1, 9),
		"generation": ctlCorr(string(corr.Epoch), 2, 5),
		"sequence":   ctlCorr(string(corr.Epoch), 1, 6),
	} {
		if err := plant.bridge.Start(ctlCtx(t), late); err == nil {
			t.Fatalf("%s: late construction accepted", name)
		}
	}
	after := plant.snapshot()
	if after.Revision != before.Revision || after.Generation != before.Generation || plant.constructed() != 1 || len(plant.records()) != records {
		t.Fatalf("stale traffic mutated state: before=%#v after=%#v constructed=%d records=%d->%d", before, after, plant.constructed(), records, len(plant.records()))
	}
	handle, err := plant.bridge.PublicationHandle(key)
	if err != nil || handle.Source() != semreg.SourceID(ctlSource) || handle.Generation() != 1 {
		t.Fatalf("exact key handle = %v, %v", handle, err)
	}
	if out := plant.bridge.Stop(ctlCtx(t), ctlCorr(string(corr.Epoch), 1, 6)); !out.NativeClosed || !out.SemanticCleaned {
		t.Fatalf("Stop = %#v", out)
	}
	if plant.bridge.Admitted(key) {
		t.Fatal("key admitted after STOP")
	}
	if _, err := plant.bridge.PublicationHandle(key); err == nil {
		t.Fatal("publication handle after STOP")
	}
	if _, err := plant.hub.Begin(handle, "asset:none"); err == nil {
		t.Fatal("retired handle still opens a hub transaction")
	}
	if err := plant.bridge.Start(ctlCtx(t), corr); err == nil {
		t.Fatal("restart of a stopped correlation accepted")
	}
	if plant.constructed() != 1 {
		t.Fatalf("late construction built an endpoint: %d", plant.constructed())
	}
}

// LIFE-08: a STOP that arrives before its START binds tombstones the
// correlation, so the late START constructs nothing.
func TestEBusControlStopBeforeBindTombstonesLateStart(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 1})
	corr := ctlCorr("inc.e1", 1, 1)
	if out := plant.bridge.Stop(ctlCtx(t), corr); !out.NativeClosed || !out.SemanticCleaned {
		t.Fatalf("Stop of an unbound correlation = %#v, want vacuous proof", out)
	}
	if err := plant.bridge.Start(ctlCtx(t), corr); err == nil {
		t.Fatal("late Start of a tombstoned correlation accepted")
	}
	if plant.constructed() != 0 || plant.hub.count("AdmitSuccessor") != 0 {
		t.Fatalf("tombstoned Start constructed %d endpoints", plant.constructed())
	}
	// The inert reservation is still unclaimed: the next operation claims it.
	if err := plant.bridge.Start(ctlCtx(t), ctlCorr("inc.e2", 1, 2)); err != nil {
		t.Fatalf("next Start: %v", err)
	}
	if len(plant.successionRecords()) != 0 {
		t.Fatalf("first admitted epoch allocated a successor: %#v", plant.successionRecords())
	}
}

// A START that paused after binding must not dispatch once STOP fenced its
// epoch: it would otherwise cancel a successor's retry and renew its budget.
func TestEBusControlFencedStartNeverDispatchesToManager(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 1})
	paused := make(chan struct{})
	resume := make(chan struct{})
	var once sync.Once
	plant.bridge.beforeDispatch = func() {
		once.Do(func() {
			close(paused)
			<-resume
		})
	}
	corr := ctlCorr("inc.e1", 1, 1)
	startDone := make(chan error, 1)
	go func() { startDone <- plant.bridge.Start(ctlCtx(t), corr) }()
	<-paused
	if out := plant.bridge.Stop(ctlCtx(t), corr); !out.NativeClosed || !out.SemanticCleaned {
		t.Fatalf("Stop = %#v", out)
	}
	close(resume)
	if err := <-startDone; err == nil {
		t.Fatal("fenced Start reported success")
	}
	if plant.constructed() != 0 || plant.snapshot().DesiredState == drivermanager.DesiredRunning {
		t.Fatalf("fenced Start reached the manager: constructed=%d snapshot=%#v", plant.constructed(), plant.snapshot())
	}
}

// LIFE-07/LIFE-09: a construction that completes after its operation was
// stopped is never admitted and is retired on its own deadline.
func TestEBusControlLateConstructionAfterStopIsNeverAdmitted(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 1})
	entered := make(chan struct{})
	release := make(chan struct{})
	plant.script(func(context.Context) error {
		close(entered)
		<-release // ignores cancellation, like a slow dial
		return nil
	})
	corr := ctlCorr("inc.e1", 1, 1)
	startDone := make(chan error, 1)
	go func() { startDone <- plant.bridge.Start(ctlCtx(t), corr) }()
	<-entered
	stopDone := make(chan runtimecontrol.StopOutcome, 1)
	go func() { stopDone <- plant.bridge.Stop(ctlCtx(t), corr) }()
	ctlWait(t, "withdrawal fence", func() bool { return plant.snapshot().ObservedState != drivermanager.ObservedStarting })
	close(release)
	if err := <-startDone; err == nil {
		t.Fatal("superseded Start reported success")
	}
	out := <-stopDone
	if !out.NativeClosed || !out.SemanticCleaned {
		t.Fatalf("Stop = %#v", out)
	}
	ctlWait(t, "late endpoint retired", func() bool { return plant.constructed() == 1 && plant.raw(0).isClosed() })
	snapshot := plant.snapshot()
	if snapshot.Generation != 0 || snapshot.ObservedState == drivermanager.ObservedRunning || plant.bridge.View().Admitted || len(plant.bridge.Mappings()) != 0 {
		t.Fatalf("late construction admitted: snapshot=%#v mappings=%#v", snapshot, plant.bridge.Mappings())
	}
}

// LIFE-10: when the hub no longer holds the claimed generation live, the
// hold fails closed; the manager retires the generation and its own retry
// cannot admit either. RUNNING and capabilities are never published.
func TestEBusControlHubProofMismatchFailsClosed(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 1})
	if _, err := plant.hub.Hub.Fence(plant.bridge.fenceRequest(plant.bridge.initial, 1)); err != nil {
		t.Fatalf("fence: %v", err)
	}
	err := plant.bridge.Start(ctlCtx(t), ctlCorr("inc.e1", 1, 1))
	if err == nil {
		t.Fatal("Start succeeded without a live hub generation")
	}
	snapshot := plant.snapshot()
	if snapshot.Generation != 0 || len(snapshot.EffectiveCapabilities) != 0 || snapshot.ObservedState == drivermanager.ObservedRunning {
		t.Fatalf("snapshot = %#v, want no published generation", snapshot)
	}
	if plant.constructed() != 2 || !plant.raw(0).isClosed() || !plant.raw(1).isClosed() {
		t.Fatalf("constructed=%d, want the attempt and one manager retry, both retired", plant.constructed())
	}
	if len(plant.bridge.Mappings()) != 0 {
		t.Fatalf("mappings = %#v", plant.bridge.Mappings())
	}
}

// --- acceptance 2: close classes and independent obligations ---

func TestEBusControlCloseClasses(t *testing.T) {
	t.Run("nil close is proven", func(t *testing.T) {
		plant := newCtlPlant(t, ctlPlantOptions{budget: 1})
		corr := ctlCorr("inc.e1", 1, 1)
		if err := plant.bridge.Start(ctlCtx(t), corr); err != nil {
			t.Fatal(err)
		}
		out := plant.bridge.Stop(ctlCtx(t), corr)
		if !out.NativeClosed || !out.SemanticCleaned || plant.bridge.View().CloseClass != drivermanager.CloseProven || plant.snapshot().ObservedState != drivermanager.ObservedStopped {
			t.Fatalf("Stop = %#v view=%#v", out, plant.bridge.View())
		}
	})
	t.Run("drain timeout with confirmed close is proven and restartable", func(t *testing.T) {
		plant := newCtlPlant(t, ctlPlantOptions{budget: 1, drain: 50 * time.Millisecond})
		corr := ctlCorr("inc.e1", 1, 1)
		if err := plant.bridge.Start(ctlCtx(t), corr); err != nil {
			t.Fatal(err)
		}
		lease, err := plant.ebus.runtime.Admit(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = lease.Release() }()
		out := plant.bridge.Stop(ctlCtx(t), corr)
		if !out.NativeClosed || !out.SemanticCleaned || plant.bridge.View().CloseClass != drivermanager.CloseProvenAfterDrainTimeout {
			t.Fatalf("Stop = %#v view=%#v", out, plant.bridge.View())
		}
		if snapshot := plant.snapshot(); snapshot.ObservedState != drivermanager.ObservedStopped || snapshot.Reason.Code != drivermanager.ReasonStopTimeout {
			t.Fatalf("snapshot = %#v", snapshot)
		}
		if err := plant.bridge.Start(ctlCtx(t), ctlCorr("inc.e2", 1, 2)); err != nil {
			t.Fatalf("successor after proven drain timeout: %v", err)
		}
	})
	t.Run("unconfirmed close quarantines and blocks the successor", func(t *testing.T) {
		plant := newCtlPlant(t, ctlPlantOptions{budget: 1, drain: 50 * time.Millisecond})
		plant.gateNextClose()
		corr := ctlCorr("inc.e1", 1, 1)
		if err := plant.bridge.Start(ctlCtx(t), corr); err != nil {
			t.Fatal(err)
		}
		out := plant.bridge.Stop(ctlCtx(t), corr)
		if out.NativeClosed || !out.SemanticCleaned {
			t.Fatalf("Stop = %#v, want unproven close with completed retirement", out)
		}
		snapshot := plant.snapshot()
		view := plant.bridge.View()
		if snapshot.ObservedState != drivermanager.ObservedFailed || snapshot.Reason.Code != drivermanager.ReasonCloseUnconfirmed || !view.Quarantined || view.CloseClass != drivermanager.CloseUnconfirmed {
			t.Fatalf("snapshot=%#v view=%#v", snapshot, view)
		}
		successions := plant.hub.count("AdmitSuccessor")
		if err := plant.bridge.Start(ctlCtx(t), ctlCorr("inc.e2", 1, 2)); err == nil {
			t.Fatal("successor started over an unconfirmed close")
		}
		if again := plant.bridge.Stop(ctlCtx(t), corr); again.NativeClosed {
			t.Fatal("repeated Stop cleared the unconfirmed close")
		}
		if plant.constructed() != 1 || plant.hub.count("AdmitSuccessor") != successions || len(plant.successionRecords()) != 0 {
			t.Fatalf("quarantine allowed replacement work: constructed=%d", plant.constructed())
		}
	})
	t.Run("internal error is never proof", func(t *testing.T) {
		plant := newCtlPlant(t, ctlPlantOptions{budget: 1, stopFailures: 1})
		corr := ctlCorr("inc.e1", 1, 1)
		if err := plant.bridge.Start(ctlCtx(t), corr); err != nil {
			t.Fatal(err)
		}
		out := plant.bridge.Stop(ctlCtx(t), corr)
		if out.NativeClosed || plant.bridge.View().CloseClass != drivermanager.CloseInternalError {
			t.Fatalf("Stop = %#v view=%#v", out, plant.bridge.View())
		}
		if err := plant.bridge.Start(ctlCtx(t), ctlCorr("inc.e2", 1, 2)); err == nil {
			t.Fatal("successor accepted INTERNAL_ERROR as close proof")
		}
		again := plant.bridge.Stop(ctlCtx(t), corr)
		if !again.NativeClosed || !again.SemanticCleaned || plant.bridge.View().CloseClass != drivermanager.CloseProven {
			t.Fatalf("retried Stop = %#v", again)
		}
		if err := plant.bridge.Start(ctlCtx(t), ctlCorr("inc.e3", 1, 3)); err != nil {
			t.Fatalf("successor after proven close: %v", err)
		}
	})
	t.Run("semantic failure cannot prevent native close", func(t *testing.T) {
		plant := newCtlPlant(t, ctlPlantOptions{budget: 1})
		corr := ctlCorr("inc.e1", 1, 1)
		if err := plant.bridge.Start(ctlCtx(t), corr); err != nil {
			t.Fatal(err)
		}
		plant.hub.setHook("RetireEpoch", func() error { return errors.New("synthetic retirement conflict") })
		out := plant.bridge.Stop(ctlCtx(t), corr)
		if !out.NativeClosed || out.SemanticCleaned || plant.snapshot().ObservedState != drivermanager.ObservedStopped || !plant.raw(0).isClosed() {
			t.Fatalf("Stop = %#v snapshot=%#v", out, plant.snapshot())
		}
		// LIFE-32: no successor while the retirement is unresolved.
		if err := plant.bridge.Start(ctlCtx(t), ctlCorr("inc.e2", 1, 2)); err == nil {
			t.Fatal("successor admitted over an unretired predecessor")
		}
		if plant.hub.count("AdmitSuccessor") != 0 || plant.constructed() != 1 {
			t.Fatalf("refused successor did hub or native work")
		}
		plant.hub.setHook("RetireEpoch", nil)
		if err := plant.bridge.Start(ctlCtx(t), ctlCorr("inc.e3", 1, 3)); err != nil {
			t.Fatalf("successor after retirement: %v", err)
		}
		if records := plant.successionRecords(); len(records) != 2 || records[0].Ordinal != 1 {
			t.Fatalf("succession records = %#v, want exactly one succession", records)
		}
	})
	t.Run("blocked semantic retirement lets native close progress", func(t *testing.T) {
		plant := newCtlPlant(t, ctlPlantOptions{budget: 1})
		corr := ctlCorr("inc.e1", 1, 1)
		if err := plant.bridge.Start(ctlCtx(t), corr); err != nil {
			t.Fatal(err)
		}
		release := make(chan struct{})
		plant.hub.setHook("RetireEpoch", func() error { <-release; return nil })
		done := make(chan runtimecontrol.StopOutcome, 1)
		go func() { done <- plant.bridge.Stop(ctlCtx(t), corr) }()
		ctlWait(t, "native close while retirement blocks", func() bool {
			return plant.snapshot().ObservedState == drivermanager.ObservedStopped && plant.raw(0).isClosed()
		})
		close(release)
		if out := <-done; !out.NativeClosed || !out.SemanticCleaned {
			t.Fatalf("Stop = %#v", out)
		}
	})
	t.Run("blocked native close lets semantic retirement progress", func(t *testing.T) {
		plant := newCtlPlant(t, ctlPlantOptions{budget: 1, drain: 3 * time.Second})
		gate := plant.gateNextClose()
		corr := ctlCorr("inc.e1", 1, 1)
		if err := plant.bridge.Start(ctlCtx(t), corr); err != nil {
			t.Fatal(err)
		}
		epoch := plant.source().Epoch
		done := make(chan runtimecontrol.StopOutcome, 1)
		go func() { done <- plant.bridge.Stop(ctlCtx(t), corr) }()
		ctlWait(t, "retirement while close blocks", func() bool {
			for _, record := range plant.records() {
				if record.Kind == semhub.EventSourceRetired && record.Epoch == epoch {
					return true
				}
			}
			return false
		})
		if plant.raw(0).isClosed() || plant.snapshot().ObservedState != drivermanager.ObservedStopping {
			t.Fatalf("close was not blocked: %#v", plant.snapshot())
		}
		close(gate)
		if out := <-done; !out.NativeClosed || !out.SemanticCleaned {
			t.Fatalf("Stop = %#v", out)
		}
	})
}

func TestEBusControlStopCancelPendingRetirement(t *testing.T) {
	for _, alreadyCancelled := range []bool{false, true} {
		for _, retirementFails := range []bool{false, true} {
			t.Run(fmt.Sprintf("already_cancelled=%t/retirement_fails=%t", alreadyCancelled, retirementFails), func(t *testing.T) {
				plant := newCtlPlant(t, ctlPlantOptions{budget: 1})
				corr := ctlCorr("inc.cancel", 1, 1)
				watchdog, stopWatchdog := context.WithTimeout(context.Background(), 5*time.Second)
				defer stopWatchdog()
				wait := func(done <-chan struct{}, what string) {
					t.Helper()
					select {
					case <-done:
					case <-watchdog.Done():
						t.Fatalf("timed out waiting for %s", what)
					}
				}
				if err := plant.bridge.Start(watchdog, corr); err != nil {
					t.Fatal(err)
				}
				plant.bridge.mu.Lock()
				epoch := plant.bridge.current
				plant.bridge.mu.Unlock()
				// Settle native close independently so even an already-cancelled
				// Stop reaches the pending hub retirement rather than its fence wait.
				wait(plant.bridge.ensureNativeStop(epoch), "native close")

				ctx, cancel := context.WithCancel(context.Background())
				release := make(chan struct{})
				entered := make(chan struct{})
				returned := make(chan struct{})
				stopDone := make(chan struct{})
				var releaseOnce sync.Once
				var retirementDone <-chan struct{}
				t.Cleanup(func() {
					cancel()
					releaseOnce.Do(func() { close(release) })
					cleanup, finishCleanup := context.WithTimeout(context.Background(), 5*time.Second)
					defer finishCleanup()
					for _, done := range []<-chan struct{}{stopDone, returned} {
						select {
						case <-done:
						case <-cleanup.Done():
							t.Error("timed out joining stop or retirement during cleanup")
						}
					}
					plant.bridge.mu.Lock()
					pending := epoch.retiring
					plant.bridge.mu.Unlock()
					if pending != nil {
						select {
						case <-pending:
						case <-cleanup.Done():
							t.Error("timed out joining retirement during cleanup")
						}
					}
				})
				plant.hub.setHook("RetireEpoch", func() error {
					defer close(returned)
					close(entered)
					<-release
					if retirementFails {
						return errors.New("synthetic retirement failure")
					}
					return nil
				})
				if alreadyCancelled {
					cancel()
				}
				var out runtimecontrol.StopOutcome
				go func() {
					defer close(stopDone)
					out = plant.bridge.Stop(ctx, corr)
				}()
				if !alreadyCancelled {
					wait(entered, "pending retirement before cancellation")
					cancel()
				}
				wait(stopDone, "Stop to return while retirement is pending")
				if !out.NativeClosed || out.SemanticCleaned {
					t.Errorf("Stop = %#v, want proven native close and pending retirement", out)
				}
				wait(entered, "retirement after cancelled Stop")
				plant.bridge.mu.Lock()
				retirementDone = epoch.retiring
				fenced := epoch.fenced
				plant.bridge.mu.Unlock()
				if retirementDone == nil || !fenced {
					t.Fatal("Stop did not fence the epoch and leave retirement pending")
				}
				releaseOnce.Do(func() { close(release) })
				wait(returned, "hub retirement hook to return")
				wait(retirementDone, "retirement goroutine to finish")
				plant.bridge.mu.Lock()
				retired := epoch.retired
				plant.bridge.mu.Unlock()
				if retired == retirementFails {
					t.Errorf("retired = %t, retirement failure = %t", retired, retirementFails)
				}
			})
		}
	}
}

// --- acceptance 4: automatic native retry keeps the epoch ---

func TestEBusControlAutomaticReplacementKeepsEpochAndMapsGeneration(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 2})
	corr := ctlCorr("inc.e1", 1, 3)
	if err := plant.bridge.Start(ctlCtx(t), corr); err != nil {
		t.Fatal(err)
	}
	first := plant.snapshot()
	epoch := plant.source().Epoch
	var fenceSnapshot, activateSnapshot drivermanager.Snapshot
	plant.hub.setHook("Fence", func() error { fenceSnapshot = requireResponsive(t, plant.manager); return nil })
	plant.hub.setHook("Activate", func() error { activateSnapshot = requireResponsive(t, plant.manager); return nil })
	if !plant.bridge.ReportFailure(ctlKey(corr), drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonDependencyUnavailable, Retryable: true}}) {
		t.Fatal("current-key failure report rejected")
	}
	ctlWait(t, "replacement admitted", func() bool {
		snapshot := plant.snapshot()
		return snapshot.ObservedState == drivermanager.ObservedRunning && snapshot.Generation == first.Generation+1 && plant.bridge.View().Admitted
	})
	for name, snapshot := range map[string]drivermanager.Snapshot{"fence": fenceSnapshot, "activate": activateSnapshot} {
		if snapshot.ObservedState != drivermanager.ObservedStarting || len(snapshot.EffectiveCapabilities) != 0 {
			t.Fatalf("%s ran with admission open: %#v", name, snapshot)
		}
	}
	if info := plant.source(); info.Epoch != epoch || info.Generation != 2 || !info.Live {
		t.Fatalf("source after replacement = %#v, want the same epoch at generation 2", info)
	}
	if len(plant.successionRecords()) != 0 || plant.hub.count("AdmitSuccessor") != 0 {
		t.Fatal("automatic replacement turned into epoch succession")
	}
	mappings := plant.bridge.Mappings()
	second := ctlKey(corr)
	second.Generation = 2
	if len(mappings) != 2 || mappings[0].Key != ctlKey(corr) || mappings[1].Key != second || !mappings[1].Automatic || mappings[1].Native <= mappings[0].Native {
		t.Fatalf("mappings = %#v", mappings)
	}
	if plant.constructed() != 2 || !plant.raw(0).isClosed() || plant.overlapped() {
		t.Fatalf("constructed=%d overlap=%v", plant.constructed(), plant.overlapped())
	}
	if plant.bridge.Admitted(ctlKey(corr)) || !plant.bridge.Admitted(second) {
		t.Fatal("old generation still admitted or new generation not admitted")
	}
	// The service still names generation 1: STOP withdraws the whole epoch.
	if out := plant.bridge.Stop(ctlCtx(t), ctlCorr(string(corr.Epoch), 1, 7)); !out.NativeClosed || !out.SemanticCleaned {
		t.Fatalf("Stop = %#v", out)
	}
	if err := plant.bridge.Start(ctlCtx(t), ctlCorr("inc.e2", 1, 8)); err != nil {
		t.Fatalf("successor: %v", err)
	}
	mappings = plant.bridge.Mappings()
	last := mappings[len(mappings)-1]
	if last.Key.Generation != 1 || last.Key.Epoch != "inc.e2" || last.Native <= mappings[1].Native {
		t.Fatalf("successor mapping = %#v, want generation 1 on a strictly later native generation", last)
	}
}

// --- acceptance 5: lifecycle-only, asset-free ---

func TestEBusControlLifecycleOnlyDeclaresNoSemanticMaterial(t *testing.T) {
	hubType := reflect.TypeOf((*ebusLifecycleHub)(nil)).Elem()
	for _, forbidden := range []string{"Declare", "DeclareClass", "Reserve", "Begin", "BeginEpoch", "BeginGeneration", "Withdraw"} {
		if _, ok := hubType.MethodByName(forbidden); ok {
			t.Fatalf("lifecycle hub seam exposes %s", forbidden)
		}
	}
	plant := newCtlPlant(t, ctlPlantOptions{budget: 1})
	if err := plant.bridge.Start(ctlCtx(t), ctlCorr("inc.e1", 1, 1)); err != nil {
		t.Fatal(err)
	}
	if view := plant.bridge.View(); len(view.SemanticCapabilities) != 0 || len(view.NativeCapabilities) == 0 {
		t.Fatalf("running view = %#v", view)
	}
	if out := plant.bridge.Stop(ctlCtx(t), ctlCorr("inc.e1", 1, 2)); !out.NativeClosed || !out.SemanticCleaned {
		t.Fatalf("Stop = %#v", out)
	}
	if view := plant.bridge.View(); len(view.NativeCapabilities) != 0 || view.Admitted {
		t.Fatalf("stopped view = %#v, want native gates withdrawn", view)
	}
	if err := plant.bridge.Start(ctlCtx(t), ctlCorr("inc.e2", 1, 3)); err != nil {
		t.Fatal(err)
	}
	for _, record := range plant.records() {
		if len(record.Assets) != 0 {
			t.Fatalf("journal record names assets: %#v", record)
		}
	}
}

// --- acceptance 6: B503 observer and proxy fencing order ---

func TestEBusControlStopPreservesB503AndProxyFencingOrder(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 1, drain: 3 * time.Second, b503: true, proxy: true})
	gate := plant.gateNextClose()
	corr := ctlCorr("inc.e1", 1, 1)
	if err := plant.bridge.Start(ctlCtx(t), corr); err != nil {
		t.Fatal(err)
	}
	running := plant.snapshot()
	if got := plant.b503.manager.TransportKey().TransportEpoch; got != running.Generation {
		t.Fatalf("B503 epoch = %d, want %d", got, running.Generation)
	}
	if _, err := plant.b503.manager.Enable(context.Background()); err != nil {
		t.Fatalf("B503 Enable: %v", err)
	}
	if got := plant.ctrl.ProxyReadiness(); got != ebusProxyReadinessReady {
		t.Fatalf("proxy readiness = %s", got)
	}
	done := make(chan runtimecontrol.StopOutcome, 1)
	go func() { done <- plant.bridge.Stop(ctlCtx(t), corr) }()
	ctlWait(t, "withdrawal", func() bool { return plant.snapshot().ObservedState == drivermanager.ObservedStopping })
	// The close is still blocked: authority was revoked first.
	if plant.raw(0).isClosed() {
		t.Fatal("close finished before the fence could be observed")
	}
	if plant.b503.manager.IsOwned() {
		t.Fatal("B503 issuer kept authority after the STOP fence")
	}
	plant.mu.Lock()
	proxy := plant.proxies[0]
	plant.mu.Unlock()
	if proxy.ready.Load() || proxy.fences.Load() == 0 || plant.ctrl.ProxyReadiness() != ebusProxyReadinessDegraded {
		t.Fatalf("proxy not fenced before close: ready=%v fences=%d", proxy.ready.Load(), proxy.fences.Load())
	}
	close(gate)
	if out := <-done; !out.NativeClosed || !out.SemanticCleaned {
		t.Fatalf("Stop = %#v", out)
	}
}

// --- service-level vectors (acceptance 3, LIFE-05/06/25/26/31) ---

type ctlStore struct{ m *runtimestate.Manager }

func ctlToRS(t *runtimecontrol.Transition) *runtimestate.ControlTransition {
	if t == nil {
		return nil
	}
	return &runtimestate.ControlTransition{
		Kind: runtimestate.TransitionKind(t.Kind), DesiredRevision: uint64(t.DesiredRevision),
		Sequence: uint64(t.Sequence), Phase: runtimestate.TransitionPhase(t.Phase), Epoch: string(t.Epoch),
		Generation: uint64(t.Generation), NativeClosePending: t.NativeClosePending,
		SemanticCleanupPending: t.SemanticCleanupPending, AttemptsUsed: t.AttemptsUsed, DeadlineUTC: t.DeadlineUTC,
	}
}

func ctlFromRS(t *runtimestate.ControlTransition) *runtimecontrol.Transition {
	if t == nil {
		return nil
	}
	return &runtimecontrol.Transition{
		Kind: runtimecontrol.TransitionKind(t.Kind), DesiredRevision: runtimecontrol.DesiredRevision(t.DesiredRevision),
		Sequence: runtimecontrol.LifecycleSequence(t.Sequence), Phase: runtimecontrol.TransitionPhase(t.Phase), Epoch: runtimecontrol.SourceEpoch(t.Epoch),
		Generation: runtimecontrol.DriverGeneration(t.Generation), NativeClosePending: t.NativeClosePending,
		SemanticCleanupPending: t.SemanticCleanupPending, AttemptsUsed: t.AttemptsUsed, DeadlineUTC: t.DeadlineUTC,
	}
}

func ctlConvertErr(err error) error {
	var refusal *runtimestate.ControlRefusal
	if errors.As(err, &refusal) {
		out := &runtimecontrol.Refusal{Code: runtimecontrol.RefusalCode(refusal.Code), Row: refusal.Row, Reason: refusal.Reason}
		if refusal.View != nil {
			out.View = &runtimecontrol.DesiredView{DesiredState: runtimecontrol.DesiredState(refusal.View.DesiredState), DesiredRevision: runtimecontrol.DesiredRevision(refusal.View.DesiredRevision)}
		}
		return out
	}
	return err
}

func ctlFromRecord(v runtimestate.ControlDriverView) runtimecontrol.ControlRecord {
	return runtimecontrol.ControlRecord{
		DriverID: runtimecontrol.DriverID(v.DriverID), Enabled: v.Enabled, Failed: v.Failed, ReadOnly: v.ReadOnly,
		HasRecord: v.HasRecord, DesiredState: runtimecontrol.DesiredState(v.DesiredState),
		DesiredRevision: runtimecontrol.DesiredRevision(v.DesiredRevision), Current: ctlFromRS(v.Current), Reserved: ctlFromRS(v.Reserved),
	}
}

func (s *ctlStore) Decide(ctx context.Context, method runtimecontrol.Method, req runtimecontrol.Request, planner runtimecontrol.Planner) (runtimecontrol.DesiredView, error) {
	view, err := s.m.DecideControl(ctx, runtimestate.ControlMethod(method),
		runtimestate.ControlRequest{DriverID: string(req.DriverID), ExpectedRevision: uint64(req.ExpectedRevision)},
		func(in runtimestate.ControlPlanInput) (runtimestate.ControlPlan, *runtimestate.ControlRefusal) {
			plan, refusal := planner(runtimecontrol.PlanInput{Method: runtimecontrol.Method(in.Method), DriverID: runtimecontrol.DriverID(in.DriverID),
				HasRecord: in.HasRecord, Current: ctlFromRS(in.Record.Current), Reserved: ctlFromRS(in.Record.Reserved)})
			if refusal != nil {
				return runtimestate.ControlPlan{}, &runtimestate.ControlRefusal{Code: runtimestate.RefusalCode(refusal.Code), Row: refusal.Row, Reason: refusal.Reason}
			}
			return runtimestate.ControlPlan{Current: ctlToRS(plan.Current), Reserved: ctlToRS(plan.Reserved)}, nil
		})
	if err != nil {
		return runtimecontrol.DesiredView{}, ctlConvertErr(err)
	}
	return runtimecontrol.DesiredView{DesiredState: runtimecontrol.DesiredState(view.DesiredState), DesiredRevision: runtimecontrol.DesiredRevision(view.DesiredRevision)}, nil
}

func (s *ctlStore) CommitWork(ctx context.Context, id runtimecontrol.DriverID, current, reserved *runtimecontrol.Transition) error {
	return ctlConvertErr(s.m.CommitControlWork(ctx, string(id), ctlToRS(current), ctlToRS(reserved)))
}

func (s *ctlStore) AdvanceCatalog(ctx context.Context) (runtimecontrol.CatalogRevision, error) {
	v, err := s.m.AdvanceControlCatalog(ctx)
	return runtimecontrol.CatalogRevision(v), ctlConvertErr(err)
}

func (s *ctlStore) CommitRecovery(ctx context.Context, m *runtimecontrol.RecoveryMaterial) error {
	if m == nil {
		return ctlConvertErr(s.m.CommitControlRecovery(ctx, nil))
	}
	ids := make([]string, 0, len(m.DriverIDs))
	for _, id := range m.DriverIDs {
		ids = append(ids, string(id))
	}
	return ctlConvertErr(s.m.CommitControlRecovery(ctx, &runtimestate.ControlRecovery{DriverIDs: ids,
		MaxDesiredRevision: uint64(m.MaxDesiredRevision), Payload: append([]byte(nil), m.Payload...)}))
}

func (s *ctlStore) Recovery() *runtimecontrol.RecoveryMaterial {
	r := s.m.ControlRecoveryPayload()
	if r == nil {
		return nil
	}
	out := &runtimecontrol.RecoveryMaterial{MaxDesiredRevision: runtimecontrol.DesiredRevision(r.MaxDesiredRevision), Payload: r.Payload}
	for _, id := range r.DriverIDs {
		out.DriverIDs = append(out.DriverIDs, runtimecontrol.DriverID(id))
	}
	return out
}

func (s *ctlStore) Catalog() (runtimecontrol.ControlCatalog, error) {
	v, err := s.m.ControlSnapshot()
	if err != nil {
		return runtimecontrol.ControlCatalog{}, ctlConvertErr(err)
	}
	out := runtimecontrol.ControlCatalog{ReadOnly: v.ReadOnly, IssuedMark: runtimecontrol.DesiredRevision(v.IssuedRevisionMark), CatalogRevision: runtimecontrol.CatalogRevision(v.CatalogRevision)}
	for _, d := range v.Drivers {
		out.Drivers = append(out.Drivers, ctlFromRecord(d))
	}
	return out, nil
}

func (s *ctlStore) Driver(id runtimecontrol.DriverID) (runtimecontrol.ControlRecord, error) {
	v, err := s.m.ControlDriver(string(id))
	if err != nil {
		return runtimecontrol.ControlRecord{}, ctlConvertErr(err)
	}
	return ctlFromRecord(v), nil
}

func (s *ctlStore) AdmissionOpen() bool { return s.m.ControlAdmissionOpen() }

func (s *ctlStore) ResolveReadback(ctx context.Context) error {
	return ctlConvertErr(s.m.ResolveControlReadback(ctx))
}

type ctlNoRecovery struct{}

func (ctlNoRecovery) Restore(context.Context, *runtimecontrol.RecoveryMaterial) error { return nil }
func (ctlNoRecovery) Secure(context.Context, runtimecontrol.DriverID, runtimecontrol.Transition) (*runtimecontrol.RecoveryMaterial, error) {
	return nil, nil
}

type ctlExec struct {
	inner  runtimecontrol.LifecycleExecutor
	mu     sync.Mutex
	starts []runtimecontrol.LifecycleCorrelation
	stops  []runtimecontrol.LifecycleCorrelation
}

func (exec *ctlExec) Start(ctx context.Context, corr runtimecontrol.LifecycleCorrelation) error {
	exec.mu.Lock()
	exec.starts = append(exec.starts, corr)
	exec.mu.Unlock()
	return exec.inner.Start(ctx, corr)
}

func (exec *ctlExec) Stop(ctx context.Context, corr runtimecontrol.LifecycleCorrelation) runtimecontrol.StopOutcome {
	exec.mu.Lock()
	exec.stops = append(exec.stops, corr)
	exec.mu.Unlock()
	return exec.inner.Stop(ctx, corr)
}

func (exec *ctlExec) counts() (int, int) {
	exec.mu.Lock()
	defer exec.mu.Unlock()
	return len(exec.starts), len(exec.stops)
}

func (exec *ctlExec) start(index int) runtimecontrol.LifecycleCorrelation {
	exec.mu.Lock()
	defer exec.mu.Unlock()
	return exec.starts[index]
}

type ctlService struct {
	t     *testing.T
	state *runtimestate.Manager
	svc   *runtimecontrol.Service
	exec  *ctlExec
	errs  []error
	mu    sync.Mutex
}

const ctlGUID = "00000000-0000-4000-8000-0000000000e5"

func ctlStatePath(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runtime_state.json")
	body := fmt.Sprintf(`{"schema_version":1,"meta":{"instance_guid":%q,"written_at":"2001-02-03T04:05:06Z"}}`, ctlGUID)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func newCtlService(t *testing.T, path string, plant *ctlPlant, incarnation string, maxAttempts int) *ctlService {
	t.Helper()
	state := runtimestate.New(runtimestate.Options{Path: path, ControlWaitBound: 2 * time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if _, err := state.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := state.BootstrapControl(context.Background(), []runtimestate.ControlDriverConfig{{DriverID: primaryEBusDriverID, Enabled: true}}); err != nil {
		t.Fatalf("BootstrapControl: %v", err)
	}
	service := &ctlService{t: t, state: state, exec: &ctlExec{inner: plant.bridge}}
	svc, err := runtimecontrol.New(runtimecontrol.Options{
		Store: &ctlStore{m: state}, Recovery: ctlNoRecovery{}, Incarnation: incarnation,
		Drivers:         []runtimecontrol.DriverConfig{{DriverID: primaryEBusDriverID, Executor: service.exec}},
		OperationBudget: 10 * time.Second, MaxAttempts: maxAttempts,
		RetryDelay: func(int) time.Duration { return 2 * time.Millisecond },
		OnError: func(err error) {
			service.mu.Lock()
			service.errs = append(service.errs, err)
			service.mu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("runtimecontrol.New: %v", err)
	}
	service.svc = svc
	t.Cleanup(service.close)
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("service Start: %v", err)
	}
	return service
}

func (service *ctlService) close() {
	service.svc.Close()
	_ = service.state.Stop(context.Background())
}

func (service *ctlService) status() runtimecontrol.DriverStatus {
	service.t.Helper()
	status, err := service.svc.Get(primaryEBusDriverID)
	if err != nil {
		service.t.Fatalf("Get: %v", err)
	}
	return status
}

func (service *ctlService) decide(method runtimecontrol.Method) (runtimecontrol.DesiredView, error) {
	service.t.Helper()
	body := []byte(fmt.Sprintf(`{"DriverID":%q,"ExpectedRevision":"%d"}`, primaryEBusDriverID, service.status().DesiredRevision))
	return service.svc.Control(ctlCtx(service.t), method, body)
}

func (service *ctlService) mustDecide(method runtimecontrol.Method) runtimecontrol.DesiredView {
	service.t.Helper()
	view, err := service.decide(method)
	if err != nil {
		service.t.Fatalf("%s: %v", method, err)
	}
	return view
}

func (service *ctlService) idle() bool {
	record, err := service.state.ControlDriver(primaryEBusDriverID)
	return err == nil && record.Current == nil && record.Reserved == nil
}

func (service *ctlService) waitSettled(state drivermanager.ObservedState) {
	service.t.Helper()
	ctlWait(service.t, fmt.Sprintf("settled %s", state), func() bool {
		return service.idle() && service.status().ObservedState == state
	})
}

// Acceptance 3, LIFE-05 and LIFE-31: every RESTART and every START after a
// completed STOP reserves exactly one successor, after retirement and proven
// close, with no overlapping native owner; repeated wakes reuse it.
func TestEBusControlServiceRestartAndStartAfterStopReserveOneSuccessor(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 1})
	service := newCtlService(t, ctlStatePath(t), plant, "inc1", 0)
	service.waitSettled(drivermanager.ObservedRunning)
	first := service.exec.start(0)
	if first.SourceID != "" || first.Generation != 1 {
		t.Fatalf("process-start correlation = %#v", first)
	}
	if len(plant.successionRecords()) != 0 {
		t.Fatal("process-start reconciliation allocated a successor instead of claiming the inert epoch")
	}
	firstHubEpoch := plant.source().Epoch

	before := service.status().DesiredRevision
	service.mustDecide(runtimecontrol.MethodRestart)
	ctlWait(t, "restart replacement", func() bool {
		starts, _ := service.exec.counts()
		return starts == 2 && service.idle() && service.status().ObservedState == drivermanager.ObservedRunning
	})
	if got := service.status().DesiredRevision; got != before+1 {
		t.Fatalf("desired revision %d -> %d, want one bump", before, got)
	}
	records := plant.successionRecords()
	if len(records) != 2 || records[0].Kind != semhub.EventSourceRetired || records[0].Epoch != firstHubEpoch || records[1].Kind != semhub.EventEpochAdmitted || records[1].Generation != 1 || records[0].Ordinal != 1 || records[1].Ordinal != 1 {
		t.Fatalf("succession records = %#v", records)
	}
	if plant.overlapped() || plant.constructed() != 2 || !plant.raw(0).isClosed() {
		t.Fatalf("overlap=%v constructed=%d", plant.overlapped(), plant.constructed())
	}
	restart := service.exec.start(1)
	if restart.Epoch == first.Epoch || restart.Generation != 1 {
		t.Fatalf("restart correlation = %#v", restart)
	}
	mappings := plant.bridge.Mappings()
	if len(mappings) != 2 || mappings[1].Key != ctlKey(restart) || mappings[1].Native <= mappings[0].Native {
		t.Fatalf("mappings = %#v", mappings)
	}
	if plant.bridge.Admitted(mappings[0].Key) || !plant.bridge.Admitted(mappings[1].Key) {
		t.Fatal("predecessor key admitted or successor key not admitted")
	}

	service.mustDecide(runtimecontrol.MethodStop)
	service.waitSettled(drivermanager.ObservedStopped)
	if view := plant.bridge.View(); view.Admitted || len(view.NativeCapabilities) != 0 {
		t.Fatalf("view after STOP = %#v", view)
	}
	service.mustDecide(runtimecontrol.MethodStart)
	ctlWait(t, "start after stop", func() bool {
		starts, _ := service.exec.counts()
		return starts == 3 && service.idle() && service.status().ObservedState == drivermanager.ObservedRunning
	})
	if records := plant.successionRecords(); len(records) != 4 || records[3].Ordinal != 2 {
		t.Fatalf("succession records = %#v, want ordinal 2 for START after STOP", records)
	}
	// Duplicate wakes of the accepted START reuse the one recorded successor.
	third := service.exec.start(2)
	for i := 0; i < 2; i++ {
		if err := plant.bridge.Start(ctlCtx(t), third); err != nil {
			t.Fatalf("duplicate wake %d: %v", i, err)
		}
	}
	if plant.constructed() != 3 || len(plant.successionRecords()) != 4 || plant.hub.count("AdmitSuccessor") != 2 {
		t.Fatalf("duplicate wakes allocated work: constructed=%d successions=%d", plant.constructed(), plant.hub.count("AdmitSuccessor"))
	}
	if plant.overlapped() {
		t.Fatal("overlapping native owners")
	}
}

// Acceptance 3 (D3): STOP/RESTART during BACKOFF, repeated same-desired
// calls and repeated wakes renew no budget and reset no retry; DriverManager
// stays the only native retry owner.
func TestEBusControlServiceBackoffRestartPreservesD3(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 3, initialDelay: time.Hour})
	plant.script(func(context.Context) error { return errors.New("synthetic dependency outage") })
	service := newCtlService(t, ctlStatePath(t), plant, "inc1", 0)
	ctlWait(t, "manager backoff", func() bool { return plant.snapshot().ObservedState == drivermanager.ObservedBackoff })
	backoff := plant.snapshot()
	revision := service.status().DesiredRevision
	// LIFE-02: a failing start leaves reads responsive.
	requireResponsive(t, plant.manager)
	viewed := make(chan ebusControlView, 1)
	go func() { viewed <- plant.bridge.View() }()
	select {
	case view := <-viewed:
		if view.Admitted || view.Observed != drivermanager.ObservedBackoff {
			t.Fatalf("view during backoff = %#v", view)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("bridge view blocked during backoff")
	}

	view := service.mustDecide(runtimecontrol.MethodStart)
	if view.DesiredRevision != revision {
		t.Fatalf("same-desired START bumped the revision: %d -> %d", revision, view.DesiredRevision)
	}
	shortCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	if err := plant.bridge.Start(shortCtx, service.exec.start(0)); err == nil {
		t.Fatal("joined wake reported success while the start is in backoff")
	}
	cancel()
	starts, _ := service.exec.counts()
	if again := plant.snapshot(); starts != 1 || again.Revision != backoff.Revision || again.Attempt != backoff.Attempt ||
		again.Retry == nil || backoff.Retry == nil || again.Retry.BudgetRemaining != backoff.Retry.BudgetRemaining {
		t.Fatalf("same-desired traffic touched native retry: before=%#v after=%#v starts=%d", backoff, again, starts)
	}
	if plant.constructed() != 0 {
		t.Fatalf("constructed = %d during backoff", plant.constructed())
	}

	service.mustDecide(runtimecontrol.MethodRestart)
	ctlWait(t, "restart from backoff", func() bool {
		return service.idle() && service.status().ObservedState == drivermanager.ObservedRunning
	})
	starts, stops := service.exec.counts()
	if starts != 2 || stops < 1 || len(plant.successionRecords()) != 2 || plant.constructed() != 1 || plant.overlapped() {
		t.Fatalf("starts=%d stops=%d successions=%#v constructed=%d", starts, stops, plant.successionRecords(), plant.constructed())
	}
	if got := service.status().DesiredRevision; got != revision+1 {
		t.Fatalf("desired revision = %d, want %d", got, revision+1)
	}
}

// Owner decision on #1183 (reserved cleanup attempt), with LIFE-02/LIFE-26:
// a start that fails with no attempt or native budget left still settles
// its cleanup once, and repeated wakes of the failed operation reuse its
// recorded result.
func TestEBusControlServiceFailedStartKeepsReservedCleanup(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 0})
	invalid := func(context.Context) error { return ebuserrors.ErrInvalidPayload }
	plant.script(invalid, invalid)
	service := newCtlService(t, ctlStatePath(t), plant, "inc1", 1)
	service.waitSettled(drivermanager.ObservedFailed)
	if err := plant.bridge.Start(ctlCtx(t), service.exec.start(0)); controlReason(err) != drivermanager.ReasonConfigInvalid {
		t.Fatalf("wake of the failed operation = %v, want its recorded result", err)
	}
	service.mustDecide(runtimecontrol.MethodRestart)
	ctlWait(t, "restart failure settled", func() bool {
		_, stops := service.exec.counts()
		return stops == 1 && service.idle() && service.status().ObservedState == drivermanager.ObservedFailed
	})
	for _, diagnostic := range service.svc.Diagnostics(primaryEBusDriverID) {
		if diagnostic.Outcome == "CLEANUP_STALLED" {
			t.Fatalf("cleanup stalled: %#v", service.svc.Diagnostics(primaryEBusDriverID))
		}
	}
	if snapshot := plant.snapshot(); snapshot.ObservedState != drivermanager.ObservedStopped {
		t.Fatalf("native state after failed start = %#v, want nothing open", snapshot)
	}
	if plant.constructed() != 0 || len(plant.successionRecords()) != 2 {
		t.Fatalf("constructed=%d successions=%#v", plant.constructed(), plant.successionRecords())
	}
	if view := plant.bridge.View(); view.Admitted || view.Quarantined {
		t.Fatalf("view = %#v", view)
	}
}

// LIFE-06 and LIFE-25: a new process with durable desired STOPPED prepares
// a fresh inert epoch with no endpoint; the first START claims it without a
// successor and the later RESTART allocates exactly one. Historical
// correlations of the earlier process admit nothing.
func TestEBusControlServiceProcessRestartClaimsFreshInertEpoch(t *testing.T) {
	path := ctlStatePath(t)
	first := newCtlPlant(t, ctlPlantOptions{budget: 1})
	firstService := newCtlService(t, path, first, "inc1", 0)
	firstService.waitSettled(drivermanager.ObservedRunning)
	historical := firstService.exec.start(0)
	firstService.mustDecide(runtimecontrol.MethodStop)
	firstService.waitSettled(drivermanager.ObservedStopped)
	firstEpoch := first.source().Epoch
	firstService.close()
	first.close()

	second := newCtlPlant(t, ctlPlantOptions{budget: 1})
	secondService := newCtlService(t, path, second, "inc2", 0)
	secondService.waitSettled(drivermanager.ObservedStopped)
	if second.constructed() != 0 || second.bridge.View().Admitted {
		t.Fatalf("desired STOPPED process constructed %d endpoints", second.constructed())
	}
	inert := second.source()
	if inert.Epoch == firstEpoch || !inert.Live || inert.Generation != 1 {
		t.Fatalf("inert source = %#v, want a fresh epoch distinct from %s", inert, firstEpoch)
	}
	if second.bridge.Admitted(ctlKey(historical)) {
		t.Fatal("historical correlation admitted")
	}
	secondService.mustDecide(runtimecontrol.MethodStart)
	secondService.waitSettled(drivermanager.ObservedRunning)
	if len(second.successionRecords()) != 0 || second.source().Epoch != inert.Epoch {
		t.Fatalf("first START did not claim the inert epoch: %#v", second.successionRecords())
	}
	secondService.mustDecide(runtimecontrol.MethodRestart)
	ctlWait(t, "restart", func() bool {
		starts, _ := secondService.exec.counts()
		return starts == 2 && secondService.idle() && secondService.status().ObservedState == drivermanager.ObservedRunning
	})
	if records := second.successionRecords(); len(records) != 2 || records[0].Ordinal != 1 {
		t.Fatalf("succession records = %#v", records)
	}
}

// A STOP can finish while the predecessor's hold is still blocked in hub
// work. The accepted successor must wait for that attempt to retire rather
// than fail permanently on the manager's unavailable answer.
func TestEBusControlSuccessorWaitsForPredecessorHoldRetirement(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 1})
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	plant.hub.setHook("Source", func() error {
		first := false
		once.Do(func() { first = true })
		if first {
			close(entered)
			<-release
		}
		return nil
	})
	first := ctlCorr("inc.e1", 1, 1)
	firstDone := make(chan error, 1)
	go func() { firstDone <- plant.bridge.Start(ctlCtx(t), first) }()
	<-entered
	if out := plant.bridge.Stop(ctlCtx(t), first); !out.NativeClosed || !out.SemanticCleaned {
		t.Fatalf("Stop = %#v", out)
	}
	successorDone := make(chan error, 1)
	go func() { successorDone <- plant.bridge.Start(ctlCtx(t), ctlCorr("inc.e2", 1, 2)) }()
	select {
	case err := <-successorDone:
		t.Fatalf("successor returned while the predecessor hold was blocked: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-successorDone; err != nil {
		t.Fatalf("successor Start = %v, want success after predecessor retirement", err)
	}
	if err := <-firstDone; err == nil {
		t.Fatal("superseded Start reported success")
	}
	if plant.overlapped() {
		t.Fatal("overlapping native owners")
	}
}

// TestEBusControlReplacementWaitsForTaskDrain: a manager-driven same-epoch
// replacement is not natively admitted while the predecessor generation's
// tasks are DRAINING; DriverManager keeps owning the retry and admits the
// replacement once the drain joined, and the runner then starts the
// replacement's tasks.
func TestEBusControlReplacementWaitsForTaskDrain(t *testing.T) {
	plant := newCtlPlant(t, ctlPlantOptions{budget: 50, initialDelay: 20 * time.Millisecond})
	stuck := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(stuck) }) }
	t.Cleanup(release)
	var runCount int32
	var runMu sync.Mutex
	running := make(chan struct{})
	runner, err := newEBusGenerationTaskRunner(ebusGenerationTaskConfig{
		Tasks: []ebusGenerationTask{{Name: "stuck", Run: func(ctx context.Context, _ *ebusGenerationScope) error {
			runMu.Lock()
			runCount++
			first := runCount == 1
			runMu.Unlock()
			if first {
				close(running)
				<-ctx.Done()
				<-stuck
			}
			return nil
		}}},
		Admitted:    ebusControlGenerationAdmitted(plant.bridge),
		JoinTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	executor := &ebusControlExecutor{bridge: plant.bridge}
	executor.bind(runner, nil)
	plant.bridge.SetAdmitGate(executor.admitGate)
	corr := ctlCorr("inc.gate.e1", 1, 1)
	if err := executor.Start(ctlCtx(t), corr); err != nil {
		t.Fatal(err)
	}
	first, ok := ebusControlGeneration(plant.bridge)
	if !ok || runner.Status().Phase != ebusGenerationRunning {
		t.Fatalf("first generation not admitted with tasks: %+v %+v", first, runner.Status())
	}
	select {
	case <-running:
	case <-time.After(5 * time.Second):
		t.Fatal("first generation task never ran")
	}
	if !plant.bridge.ReportFailure(first.Control, drivermanager.Failure{Reason: drivermanager.Reason{Code: drivermanager.ReasonDependencyUnavailable, Retryable: true}}) {
		t.Fatal("failure report refused")
	}
	ctlWait(t, "refused replacement attempts", func() bool { return plant.constructed() >= 3 })
	if snapshot := plant.snapshot(); snapshot.ObservedState == drivermanager.ObservedRunning {
		t.Fatalf("replacement admitted while the predecessor's tasks drain: %+v", snapshot)
	}
	if phase := runner.Status().Phase; phase != ebusGenerationDraining {
		t.Fatalf("runner phase = %s; want DRAINING", phase)
	}
	release()
	ctlWait(t, "replacement admitted after the drain joined", func() bool {
		executor.sync(context.Background())
		key, ok := ebusControlGeneration(plant.bridge)
		return ok && key.Native > first.Native && runner.Status().Phase == ebusGenerationRunning
	})
	if second, _ := ebusControlGeneration(plant.bridge); second.Control.Epoch != first.Control.Epoch || second.Control.Generation != first.Control.Generation+1 {
		t.Fatalf("replacement %+v; want the same epoch at the next driver generation after %+v", second, first)
	}
	count := func() int32 {
		runMu.Lock()
		defer runMu.Unlock()
		return runCount
	}
	ctlWait(t, "the replacement generation's task", func() bool { return count() == 2 })
	executor.sync(context.Background())
	if got := count(); got != 2 {
		t.Fatalf("tasks ran %d times; want once per admitted generation (2)", got)
	}
}
