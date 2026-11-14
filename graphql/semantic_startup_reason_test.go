package graphql

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	graphqlgo "github.com/graphql-go/graphql"
)

// Issue #1526: the startup reason no_supported_heat_source_path is additive.
// It changes neither the phase, the epochs nor the readiness predicate.
func TestLiveSemanticProvider_StartupReasonIsAdditive(t *testing.T) {
	provider := NewLiveSemanticProvider()
	if got := provider.StartupReason(); got != "" {
		t.Fatalf("initial StartupReason() = %q; want empty", got)
	}
	before := provider.StartupUpdatedAt()
	time.Sleep(time.Millisecond)

	if !provider.SetNoSupportedHeatSourcePath(true) {
		t.Fatal("SetNoSupportedHeatSourcePath(true) = false; want a change")
	}
	if provider.SetNoSupportedHeatSourcePath(true) {
		t.Fatal("second SetNoSupportedHeatSourcePath(true) = true; want no change")
	}
	if got := provider.StartupReason(); got != SemanticStartupReasonNoSupportedHeatSourcePath {
		t.Fatalf("StartupReason() = %q; want %q", got, SemanticStartupReasonNoSupportedHeatSourcePath)
	}
	if got := provider.StartupPhase(); got != SemanticStartupPhaseBootInit {
		t.Fatalf("phase with the reason set = %s; want %s", got, SemanticStartupPhaseBootInit)
	}
	if cacheEpoch, liveEpoch := provider.StartupEpochs(); cacheEpoch != 0 || liveEpoch != 0 {
		t.Fatalf("epochs with the reason set = (%d,%d); want (0,0)", cacheEpoch, liveEpoch)
	}
	if !provider.StartupUpdatedAt().After(before) {
		t.Fatal("startup last update did not advance when the reason was set")
	}

	// Live streams still drive the phase exactly as without the reason.
	provider.SetZones([]Zone{{ID: "zone-1", Name: "zone 1"}})
	if got := provider.StartupPhase(); got != SemanticStartupPhaseLiveWarmup {
		t.Fatalf("phase after first live update = %s; want %s", got, SemanticStartupPhaseLiveWarmup)
	}
	provider.SetDHW(&DhwStatus{Config: DhwConfig{OperatingMode: "auto"}})
	if got := provider.StartupPhase(); got != SemanticStartupPhaseLiveReady {
		t.Fatalf("phase after second live update = %s; want %s", got, SemanticStartupPhaseLiveReady)
	}

	if !provider.SetNoSupportedHeatSourcePath(false) {
		t.Fatal("SetNoSupportedHeatSourcePath(false) = false; want a change")
	}
	if got := provider.StartupReason(); got != "" {
		t.Fatalf("StartupReason() after clearing = %q; want empty", got)
	}
	if provider.SetNoSupportedHeatSourcePath(false) {
		t.Fatal("second SetNoSupportedHeatSourcePath(false) = true; want no change")
	}
	var nilProvider *LiveSemanticProvider
	if nilProvider.StartupReason() != "" || nilProvider.SetNoSupportedHeatSourcePath(true) {
		t.Fatal("nil provider reports or accepts a startup reason")
	}
}

// The boot-live timer still ends warm-up with DEGRADED; with the reason set the
// transition carries it, without the reason the transition text is unchanged.
func TestLiveSemanticProvider_BootTimeoutCarriesStartupReason(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reason     bool
		wantReason string
	}{
		{name: "no startup reason", wantReason: "boot_live_timeout elapsed=20ms"},
		{name: "no supported heat-source path", reason: true, wantReason: "boot_live_timeout elapsed=20ms startup_reason=no_supported_heat_source_path"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := NewLiveSemanticProvider()
			provider.SetNoSupportedHeatSourcePath(tc.reason)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var mu sync.Mutex
			var lines []string
			provider.StartBootFSM(ctx, 20*time.Millisecond, func(format string, args ...any) {
				mu.Lock()
				defer mu.Unlock()
				lines = append(lines, fmt.Sprintf(format, args...))
			})
			waitForPhase(t, provider, SemanticStartupPhaseDegraded, 2*time.Second)
			deadline := time.Now().Add(2 * time.Second)
			var got []string
			for time.Now().Before(deadline) {
				mu.Lock()
				got = append([]string(nil), lines...)
				mu.Unlock()
				if len(got) > 0 {
					break
				}
				time.Sleep(time.Millisecond)
			}
			want := fmt.Sprintf("semantic_startup_phase_transition from=BOOT_INIT to=DEGRADED reason=%q cache_epoch=0 live_epoch=0", tc.wantReason)
			if len(got) != 1 || got[0] != want {
				t.Fatalf("phase transition log = %q; want [%q]", got, want)
			}
			if wantSet := tc.reason; (provider.StartupReason() != "") != wantSet {
				t.Fatalf("StartupReason() after DEGRADED = %q; want set = %v", provider.StartupReason(), wantSet)
			}
		})
	}
}

// The GraphQL startup block serves the reason as a nullable snake_case field.
func TestBusSummaryStartupReasonField(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reason string
		want   any
	}{
		{name: "no reason is null", reason: "", want: nil},
		{name: "reason is served", reason: SemanticStartupReasonNoSupportedHeatSourcePath, want: SemanticStartupReasonNoSupportedHeatSourcePath},
	} {
		t.Run(tc.name, func(t *testing.T) {
			at := time.Date(2001, time.January, 1, 0, 2, 0, 0, time.UTC)
			builder := NewBuilder(nil, nil)
			builder.SetBusObservabilityProvider(testBusObservabilityProvider{snapshot: BusObservabilitySnapshot{
				Summary: &BusSummary{
					LastUpdatedAt: &at,
					Status: &BusObservabilityStatus{
						LastUpdatedAt: &at,
						Startup: &BusObservabilityStartup{
							LastUpdatedAt: &at,
							Phase:         string(SemanticStartupPhaseDegraded),
							Reason:        tc.reason,
						},
					},
				},
			}})
			schema, err := NewQuerySchema(builder)
			if err != nil {
				t.Fatalf("NewQuerySchema() error = %v", err)
			}
			result := graphqlgo.Do(graphqlgo.Params{Schema: schema, RequestString: `{ bus_summary { status { startup { phase reason } } } }`})
			if len(result.Errors) != 0 {
				t.Fatalf("GraphQL errors = %v", result.Errors)
			}
			startup := result.Data.(map[string]any)["bus_summary"].(map[string]any)["status"].(map[string]any)["startup"].(map[string]any)
			if got := startup["reason"]; got != tc.want {
				t.Fatalf("bus_summary.status.startup.reason = %#v; want %#v", got, tc.want)
			}
			if got := startup["phase"]; got != string(SemanticStartupPhaseDegraded) {
				t.Fatalf("bus_summary.status.startup.phase = %#v; want DEGRADED", got)
			}
		})
	}
}
