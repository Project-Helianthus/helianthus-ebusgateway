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

	provider.SetDHW(&DhwStatus{OperatingMode: "auto"})
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
	provider.SetDHW(&DhwStatus{OperatingMode: "auto"})
	if got := provider.StartupPhase(); got != SemanticStartupPhaseLiveWarmup {
		t.Fatalf("phase after first DHW live = %s; want %s", got, SemanticStartupPhaseLiveWarmup)
	}

	provider.SetDHW(&DhwStatus{OperatingMode: "auto"})
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
	provider.SetDHWFromCache(&DhwStatus{OperatingMode: "auto"})
	provider.SetZones([]Zone{{ID: "zone-1", Name: "Zone 1"}})
	provider.SetDHW(&DhwStatus{OperatingMode: "auto"})
	if got := provider.StartupPhase(); got != SemanticStartupPhaseLiveReady {
		t.Fatalf("phase after live warmup = %s; want %s", got, SemanticStartupPhaseLiveReady)
	}
	cacheEpochBefore, liveEpochBefore := provider.StartupEpochs()

	provider.SetZonesFromCache([]Zone{{ID: "zone-1", Name: "Zone 1", OperatingMode: "heat"}})
	provider.SetDHWFromCache(&DhwStatus{OperatingMode: "auto", Preset: "schedule"})

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
	provider.SetDHW(&DhwStatus{OperatingMode: "auto"})
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
