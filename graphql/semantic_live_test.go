package graphql

import (
	"context"
	"testing"
	"time"
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

func TestLiveSemanticProvider_StartBootFSMTransitionsToDegradedOnTimeout(t *testing.T) {
	provider := NewLiveSemanticProvider()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider.StartBootFSM(ctx, 20*time.Millisecond, func(string, ...any) {})

	waitForPhase(t, provider, SemanticStartupPhaseDegraded, 500*time.Millisecond)
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
