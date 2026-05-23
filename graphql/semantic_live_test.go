package graphql

import (
	"context"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/types"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
)

func TestLiveSemanticProvider_StartupPhaseAndEpochTransitions(t *testing.T) {
	provider := NewLiveSemanticProvider()
	if got := provider.StartupPhase(); got != SemanticStartupPhaseBootInit {
		t.Fatalf("phase = %s; want %s", got, SemanticStartupPhaseBootInit)
	}

	provider.SetZonesFromCache([]Zone{{ID: "zone-1", Name: "Zone 1"}})
	if got := provider.StartupPhase(); got != SemanticStartupPhaseCacheLoadedStale {
		t.Fatalf("phase after cache = %s; want %s", got, SemanticStartupPhaseCacheLoadedStale)
	}
	cacheEpoch, liveEpoch := provider.StartupEpochs()
	if cacheEpoch != 1 || liveEpoch != 0 {
		t.Fatalf("epochs after cache = (%d,%d); want (1,0)", cacheEpoch, liveEpoch)
	}

	provider.SetZones([]Zone{{ID: "zone-1", Name: "Zone 1"}})
	if got := provider.StartupPhase(); got != SemanticStartupPhaseLiveWarmup {
		t.Fatalf("phase after first live update = %s; want %s", got, SemanticStartupPhaseLiveWarmup)
	}
	cacheEpoch, liveEpoch = provider.StartupEpochs()
	if cacheEpoch != 1 || liveEpoch != 1 {
		t.Fatalf("epochs after first live = (%d,%d); want (1,1)", cacheEpoch, liveEpoch)
	}

	provider.SetDHW(&DhwStatus{Config: DhwConfig{OperatingMode: "auto"}})
	if got := provider.StartupPhase(); got != SemanticStartupPhaseLiveReady {
		t.Fatalf("phase after second live update = %s; want %s", got, SemanticStartupPhaseLiveReady)
	}
	cacheEpoch, liveEpoch = provider.StartupEpochs()
	if cacheEpoch != 1 || liveEpoch != 2 {
		t.Fatalf("epochs after second live = (%d,%d); want (1,2)", cacheEpoch, liveEpoch)
	}
}

func TestLiveSemanticProvider_LiveReadyRequiresLiveForPublishedStreams(t *testing.T) {
	provider := NewLiveSemanticProvider()

	provider.SetZonesFromCache([]Zone{{ID: "zone-1", Name: "Zone 1"}})
	provider.SetDHW(&DhwStatus{Config: DhwConfig{OperatingMode: "auto"}})
	if got := provider.StartupPhase(); got != SemanticStartupPhaseLiveWarmup {
		t.Fatalf("phase after first DHW live = %s; want %s", got, SemanticStartupPhaseLiveWarmup)
	}

	provider.SetDHW(&DhwStatus{Config: DhwConfig{OperatingMode: "auto"}})
	if got := provider.StartupPhase(); got != SemanticStartupPhaseLiveWarmup {
		t.Fatalf("phase after second DHW live with zones cache-only = %s; want %s", got, SemanticStartupPhaseLiveWarmup)
	}

	provider.SetZones([]Zone{{ID: "zone-1", Name: "Zone 1"}})
	if got := provider.StartupPhase(); got != SemanticStartupPhaseLiveReady {
		t.Fatalf("phase after zones live = %s; want %s", got, SemanticStartupPhaseLiveReady)
	}
}

func TestLiveSemanticProvider_CacheRefreshAfterLiveReadyKeepsPhase(t *testing.T) {
	provider := NewLiveSemanticProvider()

	provider.SetZonesFromCache([]Zone{{ID: "zone-1", Name: "Zone 1"}})
	provider.SetDHWFromCache(&DhwStatus{Config: DhwConfig{OperatingMode: "auto"}})
	provider.SetZones([]Zone{{ID: "zone-1", Name: "Zone 1"}})
	provider.SetDHW(&DhwStatus{Config: DhwConfig{OperatingMode: "auto"}})
	if got := provider.StartupPhase(); got != SemanticStartupPhaseLiveReady {
		t.Fatalf("phase after live warmup = %s; want %s", got, SemanticStartupPhaseLiveReady)
	}
	cacheEpochBefore, liveEpochBefore := provider.StartupEpochs()

	provider.SetZonesFromCache([]Zone{{ID: "zone-1", Name: "Zone 1", Config: ZoneConfig{OperatingMode: "heat"}}})
	provider.SetDHWFromCache(&DhwStatus{Config: DhwConfig{OperatingMode: "auto", Preset: "schedule"}})

	if got := provider.StartupPhase(); got != SemanticStartupPhaseLiveReady {
		t.Fatalf("phase after cache refresh = %s; want %s", got, SemanticStartupPhaseLiveReady)
	}
	cacheEpochAfter, liveEpochAfter := provider.StartupEpochs()
	if cacheEpochAfter != cacheEpochBefore+2 {
		t.Fatalf("cache epoch delta = %d; want +2", cacheEpochAfter-cacheEpochBefore)
	}
	if liveEpochAfter != liveEpochBefore {
		t.Fatalf("live epoch changed after cache refresh: got %d want %d", liveEpochAfter, liveEpochBefore)
	}
}

func TestLiveSemanticProvider_StartBootFSMTransitionsToDegradedOnTimeout(t *testing.T) {
	provider := NewLiveSemanticProvider()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider.StartBootFSM(ctx, 20*time.Millisecond, func(string, ...any) {})

	waitForPhase(t, provider, SemanticStartupPhaseDegraded, 500*time.Millisecond)
}

func TestLiveSemanticProvider_CacheUpdateDoesNotExitDegradedWithoutLive(t *testing.T) {
	provider := NewLiveSemanticProvider()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider.StartBootFSM(ctx, 20*time.Millisecond, func(string, ...any) {})
	waitForPhase(t, provider, SemanticStartupPhaseDegraded, 500*time.Millisecond)

	provider.SetZonesFromCache([]Zone{{ID: "zone-1", Name: "Zone 1"}})

	if got := provider.StartupPhase(); got != SemanticStartupPhaseDegraded {
		t.Fatalf("phase after cache update = %s; want %s", got, SemanticStartupPhaseDegraded)
	}
	cacheEpoch, liveEpoch := provider.StartupEpochs()
	if cacheEpoch != 1 || liveEpoch != 0 {
		t.Fatalf("epochs after degraded+cache = (%d,%d); want (1,0)", cacheEpoch, liveEpoch)
	}
}

func TestLiveSemanticProvider_StartBootFSMDoesNotDegradeAfterLiveReady(t *testing.T) {
	provider := NewLiveSemanticProvider()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider.StartBootFSM(ctx, 60*time.Millisecond, func(string, ...any) {})
	provider.SetZones([]Zone{{ID: "zone-1", Name: "Zone 1"}})
	provider.SetDHW(&DhwStatus{Config: DhwConfig{OperatingMode: "auto"}})
	time.Sleep(120 * time.Millisecond)

	if got := provider.StartupPhase(); got != SemanticStartupPhaseLiveReady {
		t.Fatalf("phase = %s; want %s", got, SemanticStartupPhaseLiveReady)
	}
}

func TestSemanticRuntime_StartWithoutRouterStillRunsBootFSM(t *testing.T) {
	provider := NewLiveSemanticProvider()
	runtime := NewSemanticRuntime(nil, provider, nil)
	runtime.SetBootLiveTimeout(20 * time.Millisecond)
	runtime.SetPhaseLogger(func(string, ...any) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime.Start(ctx)

	waitForPhase(t, provider, SemanticStartupPhaseDegraded, 500*time.Millisecond)
}

func TestLiveSemanticProvider_ApplyBroadcastDoesNotAdvanceStartupEpochs(t *testing.T) {
	provider := NewLiveSemanticProvider()

	event := router.BroadcastEvent{
		Values: map[string]types.Value{
			"wh":     {Valid: true, Value: float64(1200)},
			"source": {Valid: true, Value: "gas"},
			"usage":  {Valid: true, Value: "heating"},
			"period": {Valid: true, Value: "day"},
		},
	}

	totals, updated := provider.ApplyBroadcast(event)
	if !updated {
		t.Fatalf("ApplyBroadcast() updated = false; want true")
	}
	if totals == nil {
		t.Fatalf("ApplyBroadcast() totals = nil; want non-nil")
	}
	if got := provider.StartupPhase(); got != SemanticStartupPhaseBootInit {
		t.Fatalf("phase = %s; want %s", got, SemanticStartupPhaseBootInit)
	}
	cacheEpoch, liveEpoch := provider.StartupEpochs()
	if cacheEpoch != 0 || liveEpoch != 0 {
		t.Fatalf("epochs after energy broadcast = (%d,%d); want (0,0)", cacheEpoch, liveEpoch)
	}
}

func TestLiveSemanticProvider_PhaseLoggerCanReadProviderState(t *testing.T) {
	provider := NewLiveSemanticProvider()
	loggerEntered := make(chan struct{}, 1)
	provider.StartBootFSM(context.Background(), 0, func(string, ...any) {
		_ = provider.StartupPhase()
		_, _ = provider.StartupEpochs()
		select {
		case loggerEntered <- struct{}{}:
		default:
		}
	})

	done := make(chan struct{})
	go func() {
		provider.SetZonesFromCache([]Zone{{ID: "zone-1", Name: "Zone 1"}})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("SetZonesFromCache deadlocked with phase logger")
	}

	select {
	case <-loggerEntered:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("phase logger was not invoked")
	}
}

func TestLiveSemanticProvider_FM5SolarAndCylinders(t *testing.T) {
	provider := NewLiveSemanticProvider()

	if got := provider.FM5SemanticMode(); got != Fm5SemanticModeAbsent {
		t.Fatalf("FM5SemanticMode() = %s; want %s", got, Fm5SemanticModeAbsent)
	}
	if provider.Solar() != nil {
		t.Fatal("Solar() expected nil by default")
	}
	if provider.Cylinders() != nil {
		t.Fatalf("Cylinders() = %#v by default; want nil", provider.Cylinders())
	}

	provider.SetCylinders([]CylinderStatus{})
	if cylinders := provider.Cylinders(); cylinders == nil || len(cylinders) != 0 {
		t.Fatalf("Cylinders() after empty set = %#v; want empty non-nil slice", cylinders)
	}
	provider.SetCylinders(nil)
	if provider.Cylinders() != nil {
		t.Fatalf("Cylinders() after nil set = %#v; want nil", provider.Cylinders())
	}

	collector := 71.5
	pumpActive := true
	cylTemp := 48.0
	provider.SetFM5SemanticMode(Fm5SemanticModeInterpreted)
	provider.SetSolar(&SolarStatus{
		CollectorTemperatureC: &collector,
		PumpActive:            &pumpActive,
	})
	provider.SetCylinders([]CylinderStatus{
		{Index: 0, TemperatureC: &cylTemp},
	})

	if got := provider.FM5SemanticMode(); got != Fm5SemanticModeInterpreted {
		t.Fatalf("FM5SemanticMode() = %s; want %s", got, Fm5SemanticModeInterpreted)
	}
	solar := provider.Solar()
	if solar == nil || solar.CollectorTemperatureC == nil || *solar.CollectorTemperatureC != 71.5 {
		t.Fatalf("Solar() = %#v; want collector 71.5", solar)
	}
	if solar.PumpActive == nil || !*solar.PumpActive {
		t.Fatalf("Solar().PumpActive = %#v; want true", solar.PumpActive)
	}
	cylinders := provider.Cylinders()
	if len(cylinders) != 1 || cylinders[0].Index != 0 {
		t.Fatalf("Cylinders() = %#v; want index 0", cylinders)
	}
	if cylinders[0].TemperatureC == nil || *cylinders[0].TemperatureC != 48.0 {
		t.Fatalf("Cylinders()[0].TemperatureC = %#v; want 48.0", cylinders[0].TemperatureC)
	}

	// Ensure getters return cloned snapshots.
	*solar.CollectorTemperatureC = 10
	*cylinders[0].TemperatureC = 10
	latestSolar := provider.Solar()
	if latestSolar == nil || latestSolar.CollectorTemperatureC == nil || *latestSolar.CollectorTemperatureC != 71.5 {
		t.Fatalf("provider solar mutated through getter: %#v", latestSolar)
	}
	latestCylinders := provider.Cylinders()
	if len(latestCylinders) != 1 || latestCylinders[0].TemperatureC == nil || *latestCylinders[0].TemperatureC != 48.0 {
		t.Fatalf("provider cylinders mutated through getter: %#v", latestCylinders)
	}
}

func TestLiveSemanticProvider_RadioDevicesPreservesEmptyPublication(t *testing.T) {
	provider := NewLiveSemanticProvider()

	if provider.RadioDevices() != nil {
		t.Fatalf("RadioDevices() by default = %#v; want nil", provider.RadioDevices())
	}

	provider.SetRadioDevices([]RadioDevice{})
	if devices := provider.RadioDevices(); devices == nil || len(devices) != 0 {
		t.Fatalf("RadioDevices() after empty set = %#v; want empty non-nil slice", devices)
	}

	provider.SetRadioDevices(nil)
	if provider.RadioDevices() != nil {
		t.Fatalf("RadioDevices() after nil set = %#v; want nil", provider.RadioDevices())
	}
}

func waitForPhase(t *testing.T, provider *LiveSemanticProvider, want SemanticStartupPhase, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if provider.StartupPhase() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("phase timeout: got %s; want %s", provider.StartupPhase(), want)
}
