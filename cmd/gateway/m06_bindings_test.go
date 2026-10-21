package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	eebusruntime "github.com/Project-Helianthus/helianthus-eebusreg"
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
