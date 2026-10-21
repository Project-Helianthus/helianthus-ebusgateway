package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
	"github.com/Project-Helianthus/helianthus-eebusreg/eebusraw"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
)

type m06EEBusRuntime struct {
	msp05bRuntime
	reads   atomic.Int32
	closes  atomic.Int32
	blocked atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (r *m06EEBusRuntime) Snapshot() (eebusruntime.SnapshotV1, error) {
	r.reads.Add(1)
	if r.blocked.CompareAndSwap(true, false) {
		close(r.entered)
		<-r.release
	}
	return eebusruntime.SnapshotV1{}, errors.New("synthetic snapshot unavailable")
}
func (r *m06EEBusRuntime) Shutdown() error { r.closes.Add(1); return nil }

func TestM06EEBusRefreshJoinedBeforeReplacement(t *testing.T) {
	first := &m06EEBusRuntime{entered: make(chan struct{}), release: make(chan struct{})}
	first.blocked.Store(true)
	second := &m06EEBusRuntime{}
	var opens atomic.Int32
	options := m05Options(t, func(eebusruntime.Config) (eebusruntime.Runtime, error) {
		if opens.Add(1) == 1 {
			return first, nil
		}
		return second, nil
	})
	native := newEEBusManagedDriver(options)
	runtime := &managedSidecarRuntime{Runtime: native}
	manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{ID: string(managedEEBusID), Enabled: true, Runtime: runtime, Capabilities: []drivermanager.Capability{drivermanager.CapabilityRead}}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = manager.Shutdown(ctx)
	}()
	entry := &managedSidecar{descriptor: runtimecontrol.Descriptor{DriverID: managedEEBusID, Sources: []runtimecontrol.SourceID{"synthetic-eebus-source"}}, runtime: runtime, manager: manager}
	bindings := &managedSidecarBindings{sidecars: &managedSidecars{entries: []*managedSidecar{entry}}}
	key := runtimecontrol.LifecycleCorrelation{DriverID: managedEEBusID, SourceID: "synthetic-eebus-source", Epoch: "synthetic-initial", Generation: 1, Sequence: 1}
	if err := entry.Start(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	select {
	case <-first.entered:
	case <-time.After(time.Second):
		t.Fatal("generation refresh did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	outcome := entry.Stop(ctx, key)
	cancel()
	if outcome.NativeClosed || outcome.SemanticCleaned || first.closes.Load() != 0 {
		t.Fatal("refresh was not joined before native close")
	}
	before := first.reads.Load()
	if _, err := bindings.eebusSnapshot(); err == nil || first.reads.Load() != before {
		t.Fatal("withdrawn snapshot touched retired runtime")
	}
	next := key
	next.Epoch = "synthetic-successor"
	next.Sequence++
	if err := entry.Start(context.Background(), next); err == nil || opens.Load() != 1 {
		t.Fatal("replacement opened while refresh was draining")
	}
	close(first.release)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	outcome = entry.Stop(ctx, key)
	if !outcome.NativeClosed || !outcome.SemanticCleaned || first.closes.Load() != 1 {
		t.Fatal("completed refresh did not release native close")
	}
	if err := entry.Start(ctx, next); err != nil {
		t.Fatal(err)
	}
	m06Wait(t, func() bool { return second.reads.Load() > 0 })
	if _, err := bindings.eebusSnapshot(); err == nil {
		t.Fatal("synthetic unavailable snapshot unexpectedly succeeded")
	}
	if first.reads.Load() != before || opens.Load() != 2 {
		t.Fatal("successor snapshot served retired provider")
	}
}

func TestM06ExistingStoreDoesNotPromoteNewSidecars(t *testing.T) {
	natives := m06Factories(t, "", "")
	env := newRCtlEnv(t)
	env.seed(false, nil)
	process := env.start(rctlProcessOptions{})
	for _, id := range []runtimecontrol.DriverID{managedSunSpecID, managedGrowattID, managedEEBusID} {
		view, err := process.control.Service().Get(id)
		if err != nil || view.DesiredState != runtimecontrol.DesiredStopped || natives[id].starts.Load() != 0 {
			t.Fatalf("new configured driver %s implicitly promoted: %+v, %v", id, view, err)
		}
	}
	process.stop()
}

func TestM06GrowattWithdrawalDoesNotWaitForShellReader(t *testing.T) {
	config := growattProductionConfig()
	endpoint := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1}
	native := m04Driver(t, config, func(modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error) { return endpoint, nil })
	contributions, err := newGatewayPortalCatalogContributions()
	if err != nil {
		t.Fatal(err)
	}
	runtime := &managedSidecarRuntime{Runtime: native, contributions: contributions}
	manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{ID: string(managedGrowattID), Enabled: true, Runtime: runtime, Capabilities: []drivermanager.Capability{drivermanager.CapabilityRead}}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = manager.Shutdown(ctx)
	}()
	entry := &managedSidecar{descriptor: runtimecontrol.Descriptor{DriverID: managedGrowattID, Sources: []runtimecontrol.SourceID{runtimecontrol.SourceID(config.SourceID)}}, runtime: runtime, manager: manager}
	key := runtimecontrol.LifecycleCorrelation{DriverID: managedGrowattID, SourceID: runtimecontrol.SourceID(config.SourceID), Epoch: "synthetic-control", Generation: 1, Sequence: 1}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := entry.Start(ctx, key); err != nil {
		t.Fatal(err)
	}
	provider, ok := native.Provider()
	if !ok {
		t.Fatal("provider not admitted")
	}
	if _, err := provider.SemanticStorageCurrent(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := provider.storage.Current(config.AssetID); !ok {
		t.Fatal("initial Storage not current")
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- entry.with(ctx, func(drivermanager.Runtime) error { close(entered); <-release; return nil })
	}()
	<-entered
	stopCtx, stopCancel := context.WithTimeout(ctx, 25*time.Millisecond)
	outcome := entry.Stop(stopCtx, key)
	stopCancel()
	if outcome.NativeClosed || outcome.SemanticCleaned {
		t.Fatal("shutdown claimed the held shell reader was joined")
	}
	m06Wait(t, func() bool { _, ok := provider.storage.Current(config.AssetID); return !ok })
	if contributions.accepts("storage.primary", "portal.storage") {
		t.Fatal("withdrawn generation kept its Storage lease")
	}
	if _, ok := provider.LastObservationEvidence(); !ok {
		t.Fatal("withdrawal discarded native evidence")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if outcome := entry.Stop(ctx, key); !outcome.NativeClosed || !outcome.SemanticCleaned {
		t.Fatal("joined shell generation did not stop")
	}
}

func TestM06SunSpecBindingAndRawReadBudgetSurviveReplacement(t *testing.T) {
	var endpoints []*m03Endpoint
	native := m03Driver(t, true, func(modbus.TCPEndpointConfig) (modbusadapter.Endpoint, error) {
		endpoint := &m03Endpoint{}
		endpoints = append(endpoints, endpoint)
		return endpoint, nil
	}, nil)
	native.construction.DriverID = managedSunSpecID
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
	key := m03Correlation()
	key.DriverID = managedSunSpecID
	entry := &managedSidecar{descriptor: runtimecontrol.Descriptor{DriverID: managedSunSpecID, Sources: native.construction.Sources}, runtime: runtime, manager: manager}
	bindings := &managedSidecarBindings{sidecars: &managedSidecars{entries: []*managedSidecar{entry}}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := entry.Start(ctx, key); err != nil {
		t.Fatal(err)
	}
	var first *modbusadapter.Adapter
	if err := bindings.modbus(ctx, func(adapter *modbusadapter.Adapter) error { first = adapter; return nil }); err != nil {
		t.Fatal(err)
	}
	provider := newManagedModbusMCPProvider(bindings, ebusgateway.ModbusTCPConfig{Enabled: true}, nil)
	for i := 0; i < mcp.ModbusV1MaxRawReadsPerWindow; i++ {
		if _, err := provider.RawRead(ctx, mcp.ModbusRawReadRequest{}); errors.Is(err, mcp.ErrModbusV1ResourceExhausted) {
			t.Fatal("raw read budget exhausted early")
		}
	}
	if outcome := entry.Stop(ctx, key); !outcome.NativeClosed || !outcome.SemanticCleaned {
		t.Fatal("first SunSpec generation did not stop")
	}
	if err := bindings.modbus(ctx, func(*modbusadapter.Adapter) error { t.Error("withdrawn SunSpec callback ran"); return nil }); err == nil {
		t.Fatal("withdrawn SunSpec binding available")
	}
	next := key
	next.Epoch = "synthetic-modbus-next"
	next.Sequence++
	if err := entry.Start(ctx, next); err != nil {
		t.Fatal(err)
	}
	if err := bindings.modbus(ctx, func(adapter *modbusadapter.Adapter) error {
		if adapter == first {
			t.Error("successor retained retired adapter")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.RawRead(ctx, mcp.ModbusRawReadRequest{}); !errors.Is(err, mcp.ErrModbusV1ResourceExhausted) {
		t.Fatal("replacement reset shell raw read budget")
	}
	if len(endpoints) != 2 || endpoints[0].closes.Load() != 1 {
		t.Fatal("replacement did not close predecessor exactly once")
	}
}

func TestM06EEBusLeaseRejectsRetiredGeneration(t *testing.T) {
	native := newEEBusManagedDriver(m05Options(t, func(eebusruntime.Config) (eebusruntime.Runtime, error) {
		return &m06EEBusRuntime{}, nil
	}))
	generation, err := native.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = native.Stop(context.Background()) }()
	lease := &managedSidecarLease{}
	if err := native.prepareManagedLease(context.Background(), generation+1, lease); !errors.Is(err, drivermanager.ErrUnavailable) {
		t.Fatalf("stale generation lease error = %v", err)
	}
	if lease.worker != nil || lease.adminHandler != nil {
		t.Fatal("refused generation installed lease work")
	}
}

func TestM06EEBusCaptureRequiresAdmittedRuntime(t *testing.T) {
	var absent *managedSidecarBindings
	if absent.eebusEvidenceCapture() != nil || absent.eebusAvailable() {
		t.Fatal("disabled runtime admitted evidence or leaf capture")
	}
	native := newEEBusManagedDriver(m05Options(t, func(eebusruntime.Config) (eebusruntime.Runtime, error) {
		return &m06EEBusRuntime{}, nil
	}))
	runtime := &managedSidecarRuntime{Runtime: native}
	manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{ID: string(managedEEBusID), Enabled: true, Runtime: runtime}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = manager.Shutdown(context.Background()) }()
	bindings := &managedSidecarBindings{sidecars: &managedSidecars{entries: []*managedSidecar{{descriptor: runtimecontrol.Descriptor{DriverID: managedEEBusID}, runtime: runtime, manager: manager}}}}
	if bindings.eebusEvidenceCapture() != nil || bindings.eebusAvailable() {
		t.Fatal("unavailable runtime admitted evidence or leaf capture")
	}
	if err := manager.Start(context.Background(), string(managedEEBusID)); err != nil {
		t.Fatal(err)
	}
	if bindings.eebusEvidenceCapture() == nil || !bindings.eebusAvailable() {
		t.Fatal("running runtime withheld evidence or leaf capture")
	}
	runtime.FenceWithdrawal()
	if bindings.eebusEvidenceCapture() != nil || bindings.eebusAvailable() {
		t.Fatal("withdrawn runtime admitted evidence or leaf capture")
	}
}

func TestM06FailedEEBusWithholdsCapture(t *testing.T) {
	native := newEEBusManagedDriver(m05Options(t, func(eebusruntime.Config) (eebusruntime.Runtime, error) {
		return nil, errors.New("synthetic runtime unavailable")
	}))
	runtime := &managedSidecarRuntime{Runtime: native}
	manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{ID: string(managedEEBusID), Enabled: true, Runtime: runtime}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = manager.Shutdown(context.Background()) }()
	bindings := &managedSidecarBindings{sidecars: &managedSidecars{entries: []*managedSidecar{{descriptor: runtimecontrol.Descriptor{DriverID: managedEEBusID}, runtime: runtime, manager: manager}}}}
	if err := manager.Start(context.Background(), string(managedEEBusID)); err != nil {
		t.Fatal(err)
	}
	snapshot, ok := manager.Snapshot(string(managedEEBusID))
	if !ok || snapshot.ObservedState != drivermanager.ObservedFailed || runtime.active.Load() != nil {
		t.Fatalf("factory failure did not withhold the runtime: %+v", snapshot)
	}
	if bindings.eebusEvidenceCapture() != nil || bindings.eebusAvailable() {
		t.Fatal("failed runtime admitted evidence or leaf capture")
	}
}

func TestM06DisabledEEBusSurfaceIsDiscoverableAndFailsClosed(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.EEBusConfig.Enabled = false
	hub, err := preflightSemanticHub(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sidecars := prepareManagedSidecars(context.Background(), cfg, hub, rctlGUID, nil)
	bindings := &managedSidecarBindings{sidecars: sidecars}
	entry := bindings.entry(managedEEBusID)
	if entry == nil || entry.runtime != nil || entry.descriptor.Disposition != runtimecontrol.DispositionDisabled {
		t.Fatal("disabled eeBUS constructed a runtime or lost its descriptor")
	}
	adapter := &eebusRuntimeAdapter{managed: bindings}
	server, err := mcp.NewServer(registry.NewDeviceRegistry(nil), &rctlCountingInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.RegisterEEBusV1Provider(eebusMCPProvider(adapter)); err != nil {
		t.Fatal(err)
	}
	router := eebusMCPCommandRouter(adapter)
	if err := server.RegisterEEBusV1CommandRouter(router); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(rctlRPCBody("tools/list", "")))
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	result, rpcErr := rctlDecodeRPC(t, response.Body.Bytes())
	if rpcErr != nil {
		t.Fatalf("tools/list: %+v", rpcErr)
	}
	var listed struct {
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(result, &listed); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, tool := range listed.Tools {
		if strings.HasPrefix(tool.Name, "eebus.v1.") {
			got = append(got, tool.Name)
		}
	}
	want := []string{
		"eebus.v1.runtime.status.get", "eebus.v1.services.list", "eebus.v1.services.get",
		"eebus.v1.sessions.list", "eebus.v1.sessions.get", "eebus.v1.topology.get",
		"eebus.v1.snapshot.capture", "eebus.v1.snapshot.drop", "eebus.v1.pairing.status.get",
	}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("disabled eeBUS tools = %v; want %v", got, want)
	}
	for _, command := range []eebusraw.ToolV1{eebusraw.ToolV1FeaturesGet, eebusraw.ToolV1FeaturesDataGet, eebusraw.ToolV1FeaturesDataSet, eebusraw.ToolV1MutationsGet, eebusraw.ToolV1MutationsRollback} {
		for _, name := range got {
			if name == string(command) {
				t.Fatalf("operator tool %s leaked into public discovery", command)
			}
		}
	}
	request = httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(rctlRPCBody("tools/call", rctlCallParams("eebus.v1.services.list", `{}`))))
	response = httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	result, rpcErr = rctlDecodeRPC(t, response.Body.Bytes())
	if rpcErr != nil {
		t.Fatalf("disabled public read: %+v", rpcErr)
	}
	var call struct {
		IsError bool `json:"isError"`
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(result, &call); err != nil || !call.IsError || len(call.Content) != 1 {
		t.Fatalf("disabled read response = %s, %v", result, err)
	}
	var envelope struct {
		Error *struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(call.Content[0].Text), &envelope); err != nil || envelope.Error == nil || envelope.Error.Code != "backend_unavailable" {
		t.Fatalf("disabled read envelope = %s, %v", call.Content[0].Text, err)
	}
	if _, err := adapter.Snapshot(); !errors.Is(err, drivermanager.ErrUnavailable) {
		t.Fatalf("disabled read error = %v", err)
	}
	_, terminal := router.FeaturesDataSet(context.Background(), eebusraw.WriteAuthorizationV1{}, eebusraw.FeatureDataSetRequestV1{})
	if terminal == nil || terminal.Code != eebusraw.ErrorCodeV1Disconnected {
		t.Fatalf("disabled mutation error = %+v", terminal)
	}
	_, terminal = router.MutationsRollback(context.Background(), eebusraw.WriteAuthorizationV1{}, eebusraw.MutationRollbackRequestV1{})
	if terminal == nil || terminal.Code != eebusraw.ErrorCodeV1Disconnected {
		t.Fatalf("disabled rollback error = %+v", terminal)
	}
	if promoted := newEEBusPromotedSemanticProvider(adapter); promoted == nil || !reflect.DeepEqual(promoted.Overlay(), graphql.PromotedSemanticOverlay{}) {
		t.Fatal("disabled managed GraphQL overlay is missing or nonempty")
	}
	oneShot, err := newSynchronizedEvidenceOneShotRuntime(true, adapter, router, gatewayBuildInfo{})
	if err != nil || oneShot == nil {
		t.Fatalf("disabled one-shot registration = %v, %v", oneShot, err)
	}
	if bindings.eebusAvailable() || entry.runtime != nil {
		t.Fatal("surface discovery or a call started disabled eeBUS")
	}
}
