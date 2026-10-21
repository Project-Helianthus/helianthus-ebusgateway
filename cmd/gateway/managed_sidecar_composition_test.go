package main

import (
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimestate"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type m06Runtime struct {
	starts     atomic.Int32
	stops      atomic.Int32
	generation atomic.Uint64
	running    atomic.Bool
	fail       bool
}

func (r *m06Runtime) Start(context.Context) (uint64, error) {
	r.starts.Add(1)
	if r.fail {
		return 0, errors.New("synthetic sidecar failure")
	}
	r.running.Store(true)
	return r.generation.Add(1), nil
}
func (r *m06Runtime) Stop(context.Context) error {
	if r.running.Swap(false) {
		r.stops.Add(1)
	}
	return nil
}
func (r *m06Runtime) Replace(ctx context.Context) (uint64, error) {
	if err := r.Stop(ctx); err != nil {
		return 0, err
	}
	return r.Start(ctx)
}
func (r *m06Runtime) Generation() uint64    { return r.generation.Load() }
func (r *m06Runtime) Revision() uint64      { return r.generation.Load() }
func (*m06Runtime) SafetyQuarantined() bool { return false }

func m06Factories(t *testing.T, failed runtimecontrol.DriverID, disabled runtimecontrol.DriverID) map[runtimecontrol.DriverID]*m06Runtime {
	t.Helper()
	original := gatewayManagedSidecarDescriptorsFor
	t.Cleanup(func() { gatewayManagedSidecarDescriptorsFor = original })
	runtimes := map[runtimecontrol.DriverID]*m06Runtime{}
	for _, id := range []runtimecontrol.DriverID{managedSunSpecID, managedGrowattID, managedEEBusID} {
		runtimes[id] = &m06Runtime{fail: id == failed}
	}
	gatewayManagedSidecarDescriptorsFor = func(descriptors []runtimecontrol.Descriptor) []runtimecontrol.Descriptor {
		for i := range descriptors {
			id := descriptors[i].DriverID
			descriptors[i].Disposition = runtimecontrol.DispositionEnabled
			if id == disabled {
				descriptors[i].Disposition = runtimecontrol.DispositionDisabled
			}
			descriptors[i].Adapter = runtimecontrol.NewAdapter(func(_ context.Context, c runtimecontrol.Construction) (*m06Runtime, error) {
				if c.DriverID == disabled {
					t.Error("disabled adapter constructed")
				}
				if c.DriverID != id || c.NativeContract != descriptors[i].NativeContract || c.Direction != descriptors[i].Direction || len(c.Sources) != 1 || c.Sources[0] != descriptors[i].Sources[0] {
					t.Error("construction did not use frozen descriptor")
				}
				return runtimes[id], nil
			})
		}
		return descriptors
	}
	return runtimes
}

func m06Wait(t *testing.T, condition func() bool) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-timer.C:
			t.Fatal("timed out waiting for M06 condition")
		case <-ticker.C:
		}
	}
}

func TestM06ProductionRegistrationFailureIsolationAndShutdown(t *testing.T) {
	natives := m06Factories(t, managedGrowattID, "")
	env := newRCtlEnv(t)
	process := env.start(rctlProcessOptions{})
	binding, err := process.control.Registry().Binding(primaryEBusDriverID)
	if err != nil {
		t.Fatal(err)
	}
	primary, err := binding.Adapter.Construct(context.Background(), binding.Construction)
	if err != nil || primary != process.control.driver.runtime {
		t.Fatal("primary binding no longer returns the shared eBUS runtime")
	}
	m06Wait(t, func() bool {
		for _, r := range natives {
			if r.starts.Load() != 1 {
				return false
			}
		}
		return true
	})
	for _, entries := range [][]runtimecontrol.DriverID{
		func() []runtimecontrol.DriverID {
			var ids []runtimecontrol.DriverID
			for _, d := range process.control.Registry().List().Drivers {
				ids = append(ids, d.DriverID)
			}
			return ids
		}(),
		func() []runtimecontrol.DriverID {
			var ids []runtimecontrol.DriverID
			for _, d := range process.control.Service().List().Drivers {
				ids = append(ids, d.DriverID)
			}
			return ids
		}(),
	} {
		counts := map[runtimecontrol.DriverID]int{}
		for _, id := range entries {
			counts[id]++
		}
		if len(counts) != 4 {
			t.Fatalf("driver list = %v", entries)
		}
		for _, id := range []runtimecontrol.DriverID{primaryEBusDriverID, managedSunSpecID, managedGrowattID, managedEEBusID} {
			if counts[id] != 1 {
				t.Fatalf("%s listed %d times", id, counts[id])
			}
		}
	}
	m06Wait(t, func() bool {
		failed, _ := process.control.Service().Get(managedGrowattID)
		healthy, _ := process.control.Service().Get(managedEEBusID)
		return failed.ObservedState == drivermanager.ObservedFailed && healthy.ObservedState == drivermanager.ObservedRunning
	})
	if process.adapterStatus() == "" {
		t.Fatal("HTTP/GraphQL shell did not expose status")
	}
	if process.control.Service() == nil {
		t.Fatal("MCP lifecycle service unavailable after sidecar failure")
	}
	server, err := mcp.NewServer(registry.NewDeviceRegistry(nil), &rctlCountingInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registerRuntimeControlMCPTools(server, process.control); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(rctlRPCBody("tools/call", rctlCallParams(mcp.RuntimeControlListTool, `{}`))))
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	result, rpcErr := rctlDecodeRPC(t, response.Body.Bytes())
	if rpcErr != nil {
		t.Fatalf("ordinary MCP failed after sidecar failure: %+v", rpcErr)
	}
	tool := rctlDecodeTool(t, result)
	var list rctlWireList
	if tool.isError || json.Unmarshal(tool.data, &list) != nil || len(list.Drivers) != 4 {
		t.Fatalf("ordinary MCP list unavailable: %+v", tool)
	}
	process.stop()
	for id, r := range natives {
		want := int32(1)
		if id == managedGrowattID {
			want = 0
		}
		if r.stops.Load() != want || r.running.Load() {
			t.Errorf("%s shutdown: closes=%d running=%t", id, r.stops.Load(), r.running.Load())
		}
	}
}

func TestM06DesiredStateSelection(t *testing.T) {
	natives := m06Factories(t, "", managedEEBusID)
	env := newRCtlEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := runtimestate.New(runtimestate.Options{Path: env.statePath})
	if _, err := store.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.EagerPersistInstanceGUID(ctx, env.guid, runtimestate.IdentitySourceRuntimeState); err != nil {
		t.Fatal(err)
	}
	if _, err := store.BootstrapControl(ctx, []runtimestate.ControlDriverConfig{{DriverID: primaryEBusDriverID, Enabled: true}, {DriverID: string(managedSunSpecID), Enabled: true}, {DriverID: string(managedGrowattID), Enabled: true}, {DriverID: string(managedEEBusID), Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	record, err := store.ControlDriver(string(managedGrowattID))
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.DecideControl(ctx, runtimestate.ControlStop, runtimestate.ControlRequest{DriverID: string(managedGrowattID), ExpectedRevision: record.DesiredRevision}, func(runtimestate.ControlPlanInput) (runtimestate.ControlPlan, *runtimestate.ControlRefusal) {
		return runtimestate.ControlPlan{Current: &runtimestate.ControlTransition{Kind: runtimestate.TransitionStop, Phase: runtimestate.PhasePending}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CommitControlWork(ctx, string(managedGrowattID), nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	process := env.start(rctlProcessOptions{})
	m06Wait(t, func() bool {
		view, _ := process.control.Service().Get(managedSunSpecID)
		return view.ObservedState == drivermanager.ObservedRunning
	})
	stopped, _ := process.control.Service().Get(managedGrowattID)
	disabled, _ := process.control.Service().Get(managedEEBusID)
	if stopped.DesiredState != runtimecontrol.DesiredStopped || stopped.ObservedState != drivermanager.ObservedStopped || disabled.Enabled || disabled.DesiredState != runtimecontrol.DesiredRunning {
		t.Fatalf("desired state changed: stopped=%+v disabled=%+v", stopped, disabled)
	}
	if natives[managedGrowattID].starts.Load() != 0 || natives[managedEEBusID].starts.Load() != 0 {
		t.Fatal("stopped or disabled sidecar started")
	}
	process.stop()
}

func TestM06ReplacementDrainsAndNeverServesRetiredGeneration(t *testing.T) {
	native := &m06Runtime{}
	runtime := &managedSidecarRuntime{Runtime: native}
	manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{ID: string(managedSunSpecID), Enabled: true, Runtime: runtime, Capabilities: []drivermanager.Capability{drivermanager.CapabilityRead}}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = manager.Shutdown(ctx)
	}()
	entry := &managedSidecar{descriptor: runtimecontrol.Descriptor{DriverID: managedSunSpecID, Sources: []runtimecontrol.SourceID{"synthetic-source"}}, runtime: runtime, manager: manager}
	first := runtimecontrol.LifecycleCorrelation{DriverID: managedSunSpecID, SourceID: "synthetic-source", Epoch: "synthetic-first", Generation: 1, Sequence: 1}
	second := first
	second.Epoch = "synthetic-next"
	second.Sequence++
	if err := entry.Start(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	entered, release, readDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		readDone <- entry.with(context.Background(), func(drivermanager.Runtime) error {
			close(entered)
			<-release
			if !native.running.Load() {
				return errors.New("retired provider served")
			}
			return nil
		})
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	outcome := entry.Stop(ctx, first)
	cancel()
	if outcome.NativeClosed || outcome.SemanticCleaned || native.stops.Load() != 0 {
		t.Fatal("shutdown claimed a join while a request held its lease")
	}
	if err := entry.with(context.Background(), func(drivermanager.Runtime) error { t.Error("withdrawn callback executed"); return nil }); err == nil {
		t.Fatal("withdrawn provider remained available")
	}
	if err := entry.Start(context.Background(), second); err == nil || native.starts.Load() != 1 {
		t.Fatal("successor overlapped draining predecessor")
	}
	close(release)
	if err := <-readDone; err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	outcome = entry.Stop(ctx, first)
	if !outcome.NativeClosed || !outcome.SemanticCleaned {
		t.Fatal("joined predecessor did not close")
	}
	if err := entry.Start(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := entry.with(ctx, func(r drivermanager.Runtime) error {
		if r.Generation() != 2 {
			t.Errorf("served generation %d", r.Generation())
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if outcome := entry.Stop(ctx, first); !outcome.NativeClosed || !outcome.SemanticCleaned || !native.running.Load() {
		t.Fatal("retired STOP affected current generation")
	}
	if err := entry.Start(ctx, first); err == nil {
		t.Fatal("retired START reopened")
	}
}

func TestM06NoDuplicateEagerStarts(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "gateway_run_lifecycle.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	forbidden := map[string]bool{"startModbusRuntime": true, "startGrowattBMSRS485Runtime": true, "startEEBusAdminAwareRuntime": true, "newGatewaySunSpecLiveSmokeWorker": true}
	ast.Inspect(file, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if id, ok := call.Fun.(*ast.Ident); ok && forbidden[id.Name] {
				t.Errorf("duplicate eager call: %s", id.Name)
			}
		}
		return true
	})
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	for name := range forbidden {
		if strings.Contains(string(data), name+"(") {
			t.Errorf("main.go fallback: %s", name)
		}
	}
	cfg := ebusgateway.DefaultConfig()
	hub := newTestSemanticHub(t)
	sidecars := prepareManagedSidecars(context.Background(), cfg, hub, rctlGUID, nil)
	for _, s := range sidecars.entries {
		if s.runtime != nil {
			t.Fatalf("disabled %s constructed", s.descriptor.DriverID)
		}
	}
	if len(sidecars.registry.List().Drivers) != 4 {
		t.Fatal("disabled descriptor batch was not registered")
	}
}

func TestM06LifecycleCorrelationRejectsEveryFieldDrift(t *testing.T) {
	mutations := map[string]func(*runtimecontrol.LifecycleCorrelation){
		"DriverID":   func(c *runtimecontrol.LifecycleCorrelation) { c.DriverID = managedEEBusID },
		"SourceID":   func(c *runtimecontrol.LifecycleCorrelation) { c.SourceID = "other-source" },
		"Epoch":      func(c *runtimecontrol.LifecycleCorrelation) { c.Epoch = "other-epoch" },
		"Generation": func(c *runtimecontrol.LifecycleCorrelation) { c.Generation++ },
		"Sequence":   func(c *runtimecontrol.LifecycleCorrelation) { c.Sequence++ },
	}
	for field, mutate := range mutations {
		t.Run(field, func(t *testing.T) {
			native := &m06Runtime{}
			runtime := &managedSidecarRuntime{Runtime: native}
			manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{ID: string(managedSunSpecID), Enabled: true, Runtime: runtime}}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			defer func() { _ = manager.Shutdown(ctx) }()
			entry := &managedSidecar{descriptor: runtimecontrol.Descriptor{DriverID: managedSunSpecID, Sources: []runtimecontrol.SourceID{"synthetic-source"}}, manager: manager, runtime: runtime}
			key := runtimecontrol.LifecycleCorrelation{DriverID: managedSunSpecID, SourceID: "synthetic-source", Epoch: "synthetic-epoch", Generation: 1, Sequence: 1}
			if err := entry.Start(ctx, key); err != nil {
				t.Fatal(err)
			}
			changed := key
			mutate(&changed)
			if err := entry.Start(ctx, changed); err == nil {
				t.Fatalf("START accepted changed %s", field)
			}
			outcome := entry.Stop(ctx, changed)
			if outcome.NativeClosed || outcome.SemanticCleaned || native.stops.Load() != 0 || !native.running.Load() {
				t.Fatalf("STOP accepted changed %s", field)
			}
			if err := entry.Start(ctx, key); err != nil || native.starts.Load() != 1 {
				t.Fatal("identical START was not idempotent")
			}
			if outcome := entry.Stop(ctx, key); !outcome.NativeClosed || !outcome.SemanticCleaned {
				t.Fatal("original correlation failed to stop")
			}
		})
	}
}

func TestM06ConstructionFailureKeepsSiblingRegistered(t *testing.T) {
	natives := m06Factories(t, "", "")
	factory := gatewayManagedSidecarDescriptorsFor
	gatewayManagedSidecarDescriptorsFor = func(d []runtimecontrol.Descriptor) []runtimecontrol.Descriptor {
		d = factory(d)
		for i := range d {
			if d[i].DriverID == managedGrowattID {
				d[i].Adapter = runtimecontrol.NewAdapter(func(context.Context, runtimecontrol.Construction) (*m06Runtime, error) {
					return nil, errors.New("synthetic construction failure")
				})
			}
		}
		return d
	}
	env := newRCtlEnv(t)
	process := env.start(rctlProcessOptions{})
	m06Wait(t, func() bool {
		failed, _ := process.control.Service().Get(managedGrowattID)
		sibling, _ := process.control.Service().Get(managedEEBusID)
		return failed.ObservedState == drivermanager.ObservedFailed && sibling.ObservedState == drivermanager.ObservedRunning
	})
	if natives[managedGrowattID].starts.Load() != 0 {
		t.Fatal("failed construction opened a native generation")
	}
	if len(process.control.Registry().List().Drivers) != 4 {
		t.Fatal("construction failure lost the configured descriptor")
	}
	process.stop()
}

func TestM06RegistrationRefusalKeepsSiblingsRunning(t *testing.T) {
	for _, disabled := range []runtimecontrol.DriverID{"", managedGrowattID} {
		for _, source := range []runtimecontrol.SourceID{"modbus.sunspec", "eebus.primary", "invalid source"} {
			t.Run(string(disabled)+"/"+string(source), func(t *testing.T) {
				natives := m06Factories(t, "", disabled)
				cfg := ebusgateway.DefaultConfig()
				cfg.ModbusTCPConfig.GrowattBMSRS485.SourceID = string(source)
				sidecars := prepareManagedSidecars(context.Background(), cfg, nil, "", nil)
				bindings := &managedSidecarBindings{sidecars: sidecars}
				if bindings.entry(managedGrowattID).withheld == nil {
					t.Fatal("bad Growatt source was admitted")
				}
				manager, err := drivermanager.New(drivermanager.Config{Drivers: sidecars.driverConfigs()})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = manager.Shutdown(context.Background()) }()
				sidecars.bind(manager, nil)
				for _, id := range []runtimecontrol.DriverID{managedSunSpecID, managedEEBusID} {
					if _, err := sidecars.registry.Binding(id); err != nil {
						t.Fatalf("healthy %s registration refused: %v", id, err)
					}
					entry := bindings.entry(id)
					key := runtimecontrol.LifecycleCorrelation{DriverID: id, Epoch: "synthetic-epoch", Generation: 1}
					if err := entry.Start(context.Background(), key); err != nil || natives[id].starts.Load() != 1 {
						t.Fatalf("healthy %s did not start: %v", id, err)
					}
				}
				if natives[managedGrowattID].starts.Load() != 0 {
					t.Fatal("refused Growatt opened a runtime")
				}
			})
		}
	}
}

type m06LeaseFailureRuntime struct {
	m06Runtime
	setupErr error
}

func (r *m06LeaseFailureRuntime) prepareManagedLease(context.Context, uint64, *managedSidecarLease) error {
	return r.setupErr
}

func TestM06LeaseSetupFailureIsReturnedAndNativeStopped(t *testing.T) {
	setupErr := errors.New("synthetic generation refused")
	native := &m06LeaseFailureRuntime{setupErr: setupErr}
	runtime := &managedSidecarRuntime{Runtime: native}
	generation, err := runtime.Start(context.Background())
	if !errors.Is(err, setupErr) || generation != 0 || native.stops.Load() != 1 || native.running.Load() || runtime.active.Load() != nil || runtime.current != nil {
		t.Fatalf("lease refusal: generation=%d err=%v stops=%d active=%v", generation, err, native.stops.Load(), runtime.active.Load())
	}
}
