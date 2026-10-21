package main

import (
	"context"
	"errors"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
)

func m04Driver(t *testing.T, config ebusgateway.GrowattBMSRS485Config, open func(modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error)) *growattManagedDriver {
	t.Helper()
	adapter := newGrowattManagedDriverFactory(config, modbusProviderGrowattBMS, newTestSemanticHub(t), open)
	runtime, err := adapter.Construct(context.Background(), runtimecontrol.Construction{})
	if err != nil {
		t.Fatal(err)
	}
	return runtime.(*growattManagedDriver)
}

func TestM04StopJoinsAndRestartRejectsRetiredProvider(t *testing.T) {
	started, release := make(chan struct{}, 1), make(chan struct{})
	first := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1, readStarted: started, releaseRead: release}
	second := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 2}
	opens := 0
	driver := m04Driver(t, growattProductionConfig(), func(modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error) {
		opens++
		if opens == 1 {
			return first, nil
		}
		return second, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	generation, err := driver.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	old, ok := driver.Provider()
	if !ok {
		t.Fatal("provider unavailable")
	}
	result := make(chan error, 1)
	go func() { _, err := old.SemanticStorageCurrent(context.Background()); result <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := driver.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("acquisition=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("acquisition did not return after Stop")
	}
	first.mu.Lock()
	closed := first.closed
	first.mu.Unlock()
	if closed != 1 {
		t.Fatalf("close count=%d", closed)
	}
	if _, ok := old.LastObservationEvidence(); ok {
		t.Fatal("cancelled acquisition retained evidence")
	}
	next, err := driver.Start(ctx)
	if err != nil || next != generation+1 {
		t.Fatalf("restart=%d/%v", next, err)
	}
	t.Cleanup(func() {
		cleanup, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := driver.Stop(cleanup); err != nil {
			t.Error(err)
		}
	})
	if _, err := old.SemanticStorageCurrent(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("retired provider accepted refresh: %v", err)
	}
	current, ok := driver.Provider()
	if !ok {
		t.Fatal("new provider unavailable")
	}
	if _, err := current.SemanticStorageCurrent(ctx); err != nil {
		t.Fatal(err)
	}
	evidence, ok := current.LastObservationEvidence()
	if !ok || evidence.DriverGeneration != next || evidence.SourceEpoch != growattProductionConfig().SourceEpoch {
		t.Fatalf("new evidence=%+v/%t", evidence, ok)
	}
}

func TestM04FactoryProfileAndObservedStatesNeverOpenHardware(t *testing.T) {
	opens := 0
	open := func(modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error) {
		opens++
		return nil, errors.New("synthetic transport unavailable")
	}
	for _, profile := range []modbusProviderProfileID{"", modbusProviderGrowattProtocolII, "unknown"} {
		adapter := newGrowattManagedDriverFactory(growattProductionConfig(), profile, newTestSemanticHub(t), open)
		if _, err := adapter.Construct(context.Background(), runtimecontrol.Construction{}); !errors.Is(err, errModbusProviderUnknown) {
			t.Fatalf("profile=%q: %v", profile, err)
		}
	}
	for _, enabled := range []bool{false, true} {
		config := ebusgateway.GrowattBMSRS485Config{}
		if enabled {
			config = growattProductionConfig()
			config.UnitID = 0
		}
		driver := m04Driver(t, config, open)
		manager, err := drivermanager.New(drivermanager.Config{Drivers: []drivermanager.DriverConfig{{ID: "growatt.primary", Enabled: enabled, Runtime: driver}}})
		if err != nil {
			t.Fatal(err)
		}
		if enabled {
			_ = manager.Start(context.Background(), "growatt.primary")
		} else if _, err := driver.Start(context.Background()); !errors.Is(err, drivermanager.ErrUnavailable) {
			t.Fatalf("disabled start=%v", err)
		}
		snapshot, _ := manager.Snapshot("growatt.primary")
		want := drivermanager.ObservedDisabled
		if enabled {
			want = drivermanager.ObservedFailed
		}
		if snapshot.ObservedState != want {
			t.Fatalf("enabled=%t observed=%s", enabled, snapshot.ObservedState)
		}
		if _, ok := driver.Provider(); ok {
			t.Fatal("failed/disabled provider exposed")
		}
	}
	if opens != 0 {
		t.Fatalf("refused profiles/configuration opened transport %d times", opens)
	}
	driver := m04Driver(t, growattProductionConfig(), open)
	if _, err := driver.Start(context.Background()); err == nil {
		t.Fatal("open failure accepted")
	}
	if _, ok := driver.Provider(); ok {
		t.Fatal("open failure exposed provider")
	}
	if info, _ := driver.hub.Source("growatt-bms-a"); info.Live {
		t.Fatal("failed open left source admitted")
	}
}

func TestM04ManagedTransportPreservesTimingAndAdmission(t *testing.T) {
	config := growattProductionConfig()
	fake := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1}
	var got modbus.RTUProductionConfig
	driver := m04Driver(t, config, func(c modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error) { got = c; return fake, nil })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := driver.Stop(ctx); err != nil {
			t.Error(err)
		}
	}()
	wantTiming, err := growattBMSRS485Timing(config)
	if err != nil {
		t.Fatal(err)
	}
	if got.Timing != wantTiming || got.ResponseTimeout != config.ResponseTimeout || got.Serial != (modbus.RTUSerialConfig{Path: config.SerialPath, Baud: config.Baud, DataBits: 8, Parity: modbus.RTUParity(config.Parity), StopBits: config.StopBits}) {
		t.Fatalf("timing or serial configuration changed")
	}
	if got.Admission != (growattBMSRTUAdmission{unitID: config.UnitID}) || !got.Enabled {
		t.Fatal("native admission changed")
	}
	provider, _ := driver.Provider()
	if _, err := provider.SemanticStorageCurrent(ctx); err != nil {
		t.Fatal(err)
	}
	before, ok := provider.storage.Current(config.AssetID)
	if !ok {
		t.Fatal("no initial publication")
	}
	fake.mu.Lock()
	fake.failAt = len(fake.calls)
	fake.mu.Unlock()
	if _, err := provider.SemanticStorageCurrent(ctx); err == nil {
		t.Fatal("partial failure accepted")
	}
	after, ok := provider.storage.Current(config.AssetID)
	if !ok || string(before) != string(after) {
		t.Fatal("partial failure replaced last-known-good")
	}
}

type m04LateEndpoint struct {
	*growattEndpointFake
	entered chan struct{}
	release chan struct{}
}

func (endpoint *m04LateEndpoint) Read(ctx context.Context, unit byte, request modbus.ReadRegistersRequest) (modbus.ReadRegistersResponse, modbus.RTUReadEvidence, error) {
	if request.Offset() == 0x010d {
		close(endpoint.entered)
		<-endpoint.release
	}
	return endpoint.growattEndpointFake.Read(context.Background(), unit, request)
}

func TestM04LateResultCannotPublishAndDrainTimeoutBlocksRestart(t *testing.T) {
	endpoint := &m04LateEndpoint{growattEndpointFake: &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1}, entered: make(chan struct{}), release: make(chan struct{})}
	driver := m04Driver(t, growattProductionConfig(), func(modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error) { return endpoint, nil })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	provider, _ := driver.Provider()
	result := make(chan error, 1)
	go func() { _, err := provider.SemanticStorageCurrent(context.Background()); result <- err }()
	select {
	case <-endpoint.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	short, stopCancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stopCancel()
	if err := driver.Stop(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain=%v", err)
	}
	if _, err := driver.Start(ctx); !errors.Is(err, drivermanager.ErrUnavailable) {
		t.Fatalf("restart while draining=%v", err)
	}
	endpoint.mu.Lock()
	closed := endpoint.closed
	endpoint.mu.Unlock()
	if closed != 0 {
		t.Fatal("endpoint closed during acquisition")
	}
	close(endpoint.release)
	if err := driver.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("late result=%v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, ok := provider.LastObservationEvidence(); ok {
		t.Fatal("late result retained as qualified evidence")
	}
	if _, ok := provider.storage.Current(growattProductionConfig().AssetID); ok {
		t.Fatal("late result published")
	}
}

func TestM04StopDuringConstructionJoinsLateOpen(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	endpoint := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1}
	driver := m04Driver(t, growattProductionConfig(), func(modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error) {
		close(entered)
		<-release
		return endpoint, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := driver.Start(ctx); result <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	short, stopCancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stopCancel()
	if err := driver.Stop(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pending open stop=%v", err)
	}
	if _, ok := driver.Provider(); ok {
		t.Fatal("constructing provider became visible")
	}
	close(release)
	if err := driver.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("late open=%v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	if endpoint.closed != 1 {
		t.Fatalf("late endpoint close=%d", endpoint.closed)
	}
	if info, _ := driver.hub.Source("growatt-bms-a"); info.Live {
		t.Fatal("late construction retained admission")
	}
}

type m04CloseEndpoint struct {
	*growattEndpointFake
	closing chan struct{}
	release chan struct{}
	err     error
}

func (endpoint *m04CloseEndpoint) Close() error {
	close(endpoint.closing)
	<-endpoint.release
	_ = endpoint.growattEndpointFake.Close()
	return endpoint.err
}

func TestM04SemanticWithdrawalPrecedesNativeCloseAndFailureQuarantines(t *testing.T) {
	endpoint := &m04CloseEndpoint{growattEndpointFake: &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1}, closing: make(chan struct{}), release: make(chan struct{}), err: errors.New("synthetic close unconfirmed")}
	driver := m04Driver(t, growattProductionConfig(), func(modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error) { return endpoint, nil })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	provider, _ := driver.Provider()
	if _, err := provider.SemanticStorageCurrent(ctx); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- driver.Stop(ctx) }()
	select {
	case <-endpoint.closing:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, ok := provider.storage.Current(growattProductionConfig().AssetID); ok {
		t.Fatal("cached current remained admitted during native close")
	}
	if _, ok := provider.LastObservationEvidence(); !ok {
		t.Fatal("withdrawal deleted native evidence")
	}
	close(endpoint.release)
	select {
	case err := <-result:
		if !errors.Is(err, endpoint.err) {
			t.Fatalf("close=%v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if !driver.SafetyQuarantined() {
		t.Fatal("unconfirmed native close not quarantined")
	}
	if _, err := driver.Start(ctx); !errors.Is(err, endpoint.err) {
		t.Fatalf("unconfirmed close allowed successor: %v", err)
	}
}
