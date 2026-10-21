package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/drivermanager"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	modbus "github.com/Project-Helianthus/helianthus-modbus"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
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
	if request.Offset() == 0x010d && endpoint.entered != nil {
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

func TestM04StopWithdrawsRetainedStorageBeforeStalledRefreshDrains(t *testing.T) {
	endpoint := &m04LateEndpoint{growattEndpointFake: &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1}}
	driver := m04Driver(t, growattProductionConfig(), func(modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error) { return endpoint, nil })
	fenced := make(chan struct{})
	hub, err := semhub.New(semhub.Options{Faults: func(stage string, _ semreg.AssetID) {
		if stage == "after-commit" {
			close(fenced)
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	driver.hub = hub
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	provider, _ := driver.Provider()
	if _, err := provider.SemanticStorageCurrent(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := provider.storage.Current(growattProductionConfig().AssetID); !ok {
		t.Fatal("initial storage not admitted")
	}
	before, ok := provider.LastObservationEvidence()
	if !ok {
		t.Fatal("initial native evidence missing")
	}
	endpoint.entered, endpoint.release = make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() { _, err := provider.SemanticStorageCurrent(context.Background()); result <- err }()
	released := false
	defer func() {
		if !released {
			close(endpoint.release)
		}
		if err := driver.Stop(ctx); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-endpoint.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Complete semantic withdrawal before timing the still-stalled native drain.
	joined := make(chan error, 1)
	go func() { joined <- driver.Stop(ctx) }()
	select {
	case <-fenced:
	case <-ctx.Done():
		t.Fatal("semantic withdrawal did not complete")
	}
	short, stopCancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stopCancel()
	stopped := make(chan error, 1)
	go func() { stopped <- driver.Stop(short) }()
	select {
	case err := <-stopped:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("stop=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("withdrawal blocked on stalled refresh")
	}
	if _, ok := provider.storage.Current(growattProductionConfig().AssetID); ok {
		t.Fatal("retained storage remained admitted after timed-out stop")
	}
	if _, ok := driver.Provider(); ok {
		t.Fatal("retired provider still exposed")
	}
	if _, err := driver.Start(ctx); !errors.Is(err, drivermanager.ErrUnavailable) {
		t.Fatalf("restart during drain=%v", err)
	}
	endpoint.mu.Lock()
	closed := endpoint.closed
	endpoint.mu.Unlock()
	if closed != 0 {
		t.Fatal("endpoint closed before refresh drained")
	}
	close(endpoint.release)
	released = true
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("late refresh=%v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := driver.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-joined:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("initial stop did not join drain")
	}
	after, ok := provider.LastObservationEvidence()
	if !ok || after.ObservationID != before.ObservationID {
		t.Fatal("withdrawal lost or replaced retained native evidence")
	}
	if _, ok := provider.storage.Current(growattProductionConfig().AssetID); ok {
		t.Fatal("late refresh restored admission")
	}
}

type m04StalledFenceJournal struct {
	*semhub.MemoryJournal
	entered chan struct{}
	release chan struct{}
}

func (journal *m04StalledFenceJournal) Append(records ...semhub.EpochRecord) error {
	for _, record := range records {
		if record.Kind == semhub.EventGenerationFenced {
			close(journal.entered)
			<-journal.release
			break
		}
	}
	return journal.MemoryJournal.Append(records...)
}

func TestM04StalledSemanticFenceHonorsStopDeadline(t *testing.T) {
	journal := &m04StalledFenceJournal{MemoryJournal: semhub.NewMemoryJournal(), entered: make(chan struct{}), release: make(chan struct{})}
	hub, err := semhub.New(semhub.Options{Journal: journal})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1}
	driver := m04Driver(t, growattProductionConfig(), func(modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error) { return endpoint, nil })
	driver.hub = hub
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	generation, err := driver.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	provider, _ := driver.Provider()
	if _, err := provider.SemanticStorageCurrent(ctx); err != nil {
		t.Fatal(err)
	}
	// Always release the fence, including on failure against a mutex-blocking Stop.
	released := false
	defer func() {
		if !released {
			close(journal.release)
		}
		cleanup, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := driver.Stop(cleanup); err != nil {
			t.Error(err)
		}
	}()
	short, stopCancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stopCancel()
	stopped := make(chan error, 1)
	go func() { stopped <- driver.Stop(short) }()
	select {
	case <-journal.entered:
	case <-ctx.Done():
		t.Fatal("semantic fence did not reach journal")
	}
	select {
	case err := <-stopped:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("stop=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("Stop deadline blocked on semantic fence")
	}
	responsive := make(chan struct{})
	go func() {
		defer close(responsive)
		if _, ok := driver.Provider(); ok {
			t.Error("draining provider exposed")
		}
		if got := driver.Generation(); got != generation {
			t.Errorf("generation=%d, want %d", got, generation)
		}
		_ = driver.Revision()
		if driver.SafetyQuarantined() {
			t.Error("pending fence reported as failed")
		}
		if _, err := driver.Start(ctx); !errors.Is(err, drivermanager.ErrUnavailable) {
			t.Errorf("restart during fence=%v", err)
		}
	}()
	select {
	case <-responsive:
	case <-ctx.Done():
		t.Fatal("provider/status/start blocked on semantic fence")
	}
	endpoint.mu.Lock()
	closed := endpoint.closed
	endpoint.mu.Unlock()
	if closed != 0 {
		t.Fatal("endpoint closed before semantic fence completed")
	}
	close(journal.release)
	released = true
	if err := driver.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	endpoint.mu.Lock()
	closed = endpoint.closed
	endpoint.mu.Unlock()
	if closed != 1 {
		t.Fatalf("endpoint close count after fence=%d", closed)
	}
	if _, ok := provider.storage.Current(growattProductionConfig().AssetID); ok {
		t.Fatal("retained storage remained admitted after completed stop")
	}
}

func TestM04WithdrawalFencesObservedRefreshBeforePublication(t *testing.T) {
	endpoint := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1}
	driver := m04Driver(t, growattProductionConfig(), func(modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error) { return endpoint, nil })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	provider, _ := driver.Provider()
	observed, release := make(chan struct{}), make(chan struct{})
	provider.beforePublish = func() {
		close(observed)
		<-release
	}
	result := make(chan error, 1)
	go func() { _, err := provider.SemanticStorageCurrent(ctx); result <- err }()
	released := false
	defer func() {
		if !released {
			close(release)
		}
		if err := driver.Stop(ctx); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-observed:
	case <-ctx.Done():
		t.Fatal("refresh did not reach observation/publication boundary")
	}
	if _, ok := provider.LastObservationEvidence(); !ok {
		t.Fatal("completed native observation missing")
	}
	driver.FenceWithdrawal()
	close(release)
	released = true
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("refresh after withdrawal=%v, want cancellation", err)
		}
	case <-ctx.Done():
		t.Fatal("refresh did not finish after withdrawal")
	}
	if _, ok := provider.storage.Current(growattProductionConfig().AssetID); ok {
		t.Fatal("observed refresh published after withdrawal returned")
	}
	if _, ok := provider.LastObservationEvidence(); !ok {
		t.Fatal("withdrawal deleted completed native evidence")
	}
}

func TestM04WithdrawalReturnsDuringPublicationAlreadyInProgress(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	hub, err := semhub.New(semhub.Options{Faults: func(stage string, _ semreg.AssetID) {
		if stage == "commit" {
			close(entered)
			<-release
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1}
	driver := m04Driver(t, growattProductionConfig(), func(modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error) { return endpoint, nil })
	driver.hub = hub
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	provider, _ := driver.Provider()
	result := make(chan error, 1)
	go func() { _, err := provider.SemanticStorageCurrent(ctx); result <- err }()
	released := false
	defer func() {
		if !released {
			close(release)
		}
		if err := driver.Stop(ctx); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("publication did not reach commit")
	}
	withdrawn := make(chan struct{})
	go func() { driver.FenceWithdrawal(); close(withdrawn) }()
	select {
	case <-provider.lifetime.Done():
	case <-ctx.Done():
		t.Fatal("withdrawal did not cancel acquisition")
	}
	select {
	case <-withdrawn:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("FenceWithdrawal waited for in-flight publication")
	}
	select {
	case err := <-result:
		t.Fatalf("publication finished before release: %v", err)
	default:
	}
	close(release)
	released = true
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("publication already in progress=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("publication did not finish")
	}
	select {
	case <-withdrawn:
	case <-ctx.Done():
		t.Fatal("withdrawal did not return promptly")
	}
}

func TestM04StopJoinsPublicationAlreadyInProgress(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	hub, err := semhub.New(semhub.Options{Faults: func(stage string, _ semreg.AssetID) {
		if stage == "commit" {
			close(entered)
			<-release
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1}
	driver := m04Driver(t, growattProductionConfig(), func(modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error) { return endpoint, nil })
	driver.hub = hub
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := driver.Start(ctx); err != nil {
		t.Fatal(err)
	}
	provider, _ := driver.Provider()
	result := make(chan error, 1)
	go func() { _, err := provider.SemanticStorageCurrent(ctx); result <- err }()
	released := false
	defer func() {
		if !released {
			close(release)
		}
		if err := driver.Stop(ctx); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("publication did not reach commit")
	}
	withdrawn := make(chan struct{})
	go func() {
		driver.FenceWithdrawal()
		if err := driver.Stop(ctx); err != nil {
			t.Error(err)
		}
		close(withdrawn)
	}()
	select {
	case <-provider.lifetime.Done():
	case <-ctx.Done():
		t.Fatal("withdrawal did not cancel acquisition")
	}
	select {
	case <-withdrawn:
		t.Fatal("Stop returned before in-flight publication finished")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	released = true
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("publication already in progress=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("publication did not finish")
	}
	select {
	case <-withdrawn:
	case <-ctx.Done():
		t.Fatal("Stop did not join publication")
	}
}

func TestM04StalledPublicationWithdrawalIsNonBlockingAndDrainTimesOut(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	hub, err := semhub.New(semhub.Options{Faults: func(stage string, _ semreg.AssetID) {
		if stage == "commit" {
			close(entered)
			<-release
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1}
	driver := m04Driver(t, growattProductionConfig(), func(modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error) { return endpoint, nil })
	driver.hub = hub
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	generation, err := driver.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	provider, _ := driver.Provider()
	result := make(chan error, 1)
	go func() { _, err := provider.SemanticStorageCurrent(ctx); result <- err }()
	defer func() {
		close(release)
		select {
		case err := <-result:
			if err != nil {
				t.Error(err)
			}
		case <-ctx.Done():
			t.Error("publication did not finish")
		}
		// A timed-out fence stays quarantined even after background cleanup completes.
		if err := driver.Stop(ctx); !errors.Is(err, errGrowattPublicationFenceTimeout) {
			t.Errorf("completed drain lost fence timeout: %v", err)
		}
		if !driver.SafetyQuarantined() {
			t.Error("completed drain cleared quarantine")
		}
		if _, err := driver.Start(ctx); !errors.Is(err, errGrowattPublicationFenceTimeout) {
			t.Errorf("restart after fence timeout=%v", err)
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("publication did not reach commit")
	}
	withdrawn := make(chan struct{})
	go func() { driver.FenceWithdrawal(); close(withdrawn) }()
	select {
	case <-withdrawn:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("FenceWithdrawal blocked on publication lock")
	}
	if provider.lifetime.Err() == nil {
		t.Fatal("withdrawal did not cancel generation")
	}
	short, stopCancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stopCancel()
	stopped := make(chan error, 1)
	go func() { stopped <- driver.Stop(short) }()
	select {
	case err := <-stopped:
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "publication fence timeout") {
			t.Fatalf("stop=%v, want publication fence timeout", err)
		}
	case <-ctx.Done():
		t.Fatal("Stop deadline blocked on publication")
	}
	responsive := make(chan struct{})
	go func() {
		defer close(responsive)
		if driver.Generation() != generation {
			t.Error("generation changed during stalled publication")
		}
		_ = driver.Revision()
		if _, ok := driver.Provider(); ok {
			t.Error("draining provider exposed")
		}
		if !driver.SafetyQuarantined() {
			t.Error("publication fence timeout did not quarantine generation")
		}
		replacement, replaceCancel := context.WithTimeout(ctx, 20*time.Millisecond)
		defer replaceCancel()
		if _, err := driver.Replace(replacement); !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "publication fence timeout") {
			t.Errorf("replace=%v", err)
		}
	}()
	select {
	case <-responsive:
	case <-ctx.Done():
		t.Fatal("status reads or Replace blocked on publication")
	}
	endpoint.mu.Lock()
	closed := endpoint.closed
	endpoint.mu.Unlock()
	if closed != 0 {
		t.Fatal("endpoint closed before publication finished")
	}
}

func TestM04CancelledStartCommitWithdrawsBeforeStalledClose(t *testing.T) {
	endpoint := &m04CloseEndpoint{growattEndpointFake: &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1}, closing: make(chan struct{}), release: make(chan struct{})}
	driver := m04Driver(t, growattProductionConfig(), func(modbus.RTUProductionConfig) (growattBMSRTUEndpoint, error) { return endpoint, nil })
	entered, release := make(chan struct{}), make(chan struct{})
	driver.beforeStartCommit = func() {
		close(entered)
		<-release
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := driver.Start(ctx); result <- err }()
	startupReleased, closeReleased := false, false
	defer func() {
		if !startupReleased {
			close(release)
		}
		if !closeReleased {
			close(endpoint.release)
		}
		if err := driver.Stop(ctx); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("startup did not reach commit boundary")
	}
	short, stopCancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stopCancel()
	if err := driver.Stop(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pending startup stop=%v", err)
	}
	close(release)
	startupReleased = true
	select {
	case <-endpoint.closing:
	case err := <-result:
		t.Fatalf("cancelled startup returned without closing endpoint: %v", err)
	case <-ctx.Done():
		t.Fatal("cancelled startup did not close endpoint")
	}
	if info, _ := driver.hub.Source(semreg.SourceID(growattProductionConfig().SourceID)); info.Live {
		t.Fatal("cancelled startup retained admission during native close")
	}
	if _, ok := driver.Provider(); ok || driver.current.Load() != nil {
		t.Fatal("cancelled startup installed a provider")
	}
	if _, err := driver.Start(ctx); !errors.Is(err, drivermanager.ErrUnavailable) {
		t.Fatalf("successor admitted before startup drain: %v", err)
	}
	select {
	case err := <-result:
		t.Fatalf("startup settled before native close: %v", err)
	default:
	}
	// Release close separately so the startup result also proves cancellation.
	close(endpoint.release)
	closeReleased = true
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled startup commit=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("startup did not settle after close")
	}
}

func TestM04WithdrawalRejectsEvidenceAtCommitBoundary(t *testing.T) {
	for _, phase := range []string{"native_evidence", "semantic_evidence", "semantic_publication"} {
		t.Run(phase, func(t *testing.T) {
			endpoint := &growattEndpointFake{words: growattBMSProductionWords(), failAt: -1, mismatch: -1, generation: 1}
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
			previous, _ := provider.LastObservationEvidence()
			entered, release := make(chan struct{}), make(chan struct{})
			commits := 0
			provider.beforeCommit = func() {
				commits++
				if phase == "semantic_publication" && commits == 1 {
					return
				}
				close(entered)
				<-release
			}
			result := make(chan error, 1)
			go func() {
				var err error
				if phase != "native_evidence" {
					_, err = provider.SemanticStorageCurrent(ctx)
				} else {
					_, err = provider.GrowattBMSRS485V202(ctx)
				}
				result <- err
			}()
			released := false
			defer func() {
				if !released {
					close(release)
				}
				if err := driver.Stop(ctx); err != nil {
					t.Error(err)
				}
			}()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("refresh did not reach evidence commit boundary")
			}
			driver.FenceWithdrawal()
			short, stopCancel := context.WithTimeout(ctx, 20*time.Millisecond)
			defer stopCancel()
			if err := driver.Stop(short); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("paused evidence stop=%v", err)
			}
			close(release)
			released = true
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("withdrawn refresh=%v", err)
				}
			case <-ctx.Done():
				t.Fatal("withdrawn refresh did not finish")
			}
			if err := driver.Stop(ctx); err != nil {
				t.Fatal(err)
			}
			last, ok := provider.LastObservationEvidence()
			if phase == "semantic_publication" {
				if !ok || last.Revision != previous.Revision+1 {
					t.Fatal("withdrawal lost evidence committed before publication admission")
				}
			} else if !ok || last.Revision != previous.Revision || last.ObservationID != previous.ObservationID {
				t.Fatal("withdrawn refresh replaced retained native evidence")
			}
			if _, ok := provider.storage.Current(growattProductionConfig().AssetID); ok {
				t.Fatal("withdrawn refresh republished current storage")
			}
		})
	}
}
