package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
)

func TestK01CircuitZeroAloneLeavesBoilerPumpUnknown(t *testing.T) {
	for _, raw := range []uint16{0, 1} {
		name := "off"
		if raw == 1 {
			name = "on"
		}
		t.Run(name, func(t *testing.T) {
			poller := k01PumpPoller(raw, nil)
			poller.refreshBoilerStatusTier(context.Background(), boilerStatusTierFast)
			k01AssertPublishedPump(t, poller, nil)
			if got := *poller.circuits[0].PumpStatusRaw; got != raw {
				t.Errorf("native circuit pump = %d; want preserved %d", got, raw)
			}
		})
	}
}

func TestK01IndependentBoilerPumpWinsCircuitDisagreement(t *testing.T) {
	for _, boilerActive := range []bool{false, true} {
		name := "boiler_off_circuit_on"
		circuitRaw := uint16(1)
		if boilerActive {
			name = "boiler_on_circuit_off"
			circuitRaw = 0
		}
		t.Run(name, func(t *testing.T) {
			poller := k01PumpPoller(circuitRaw, &boilerActive)
			poller.refreshBoilerStatusTier(context.Background(), boilerStatusTierFast)
			k01AssertPublishedPump(t, poller, &boilerActive)
		})
	}
}

func TestK01PartialUpdatesPreserveIndependentBoilerPump(t *testing.T) {
	for _, initial := range []bool{false, true} {
		name := "initial_off"
		if initial {
			name = "initial_on"
		}
		t.Run(name, func(t *testing.T) {
			poller := k01PumpPoller(0, &initial)
			for _, raw := range []uint16{0, 1} {
				poller.circuits[0].PumpStatusRaw = &raw
				flow := 43.5 + float64(raw)
				poller.system.SystemFlowTemperature = &flow
				poller.refreshBoilerStatusTier(context.Background(), boilerStatusTierFast)
				k01AssertPublishedPump(t, poller, &initial)
				if got := poller.provider.BoilerStatus().State.FlowTemperatureC; got == nil || *got != flow {
					t.Errorf("partial flow update = %v; want %v", got, flow)
				}
			}

			// A later independent boiler update must replace either known boolean.
			next := !initial
			poller.boiler = mergeBoilerSnapshotNonDestructive(poller.boiler, &vaillantBoilerSnapshot{
				CentralHeatingPumpActive: &next,
			})
			poller.publishBoilerStatus(semanticSnapshotSourceLive)
			k01AssertPublishedPump(t, poller, &next)

			// Missing pump evidence in a later partial snapshot retains the new value.
			flow := 46.5
			poller.boiler = mergeBoilerSnapshotNonDestructive(poller.boiler, &vaillantBoilerSnapshot{
				FlowTemperatureC: &flow,
			})
			poller.publishBoilerStatus(semanticSnapshotSourceLive)
			k01AssertPublishedPump(t, poller, &next)
		})
	}
}

func TestK01CachedBoilerPumpWithoutProvenanceIsDiscarded(t *testing.T) {
	for _, cachedActive := range []bool{false, true} {
		name := "cached_off"
		if cachedActive {
			name = "cached_on"
		}
		t.Run(name, func(t *testing.T) {
			store := newSemanticCacheStore(filepath.Join(t.TempDir(), "semantic-cache.json"), t.Logf)
			flow := 39.5
			// Reproduce an existing cache, including the legacy circuit-derived field.
			if err := store.writeV3(semanticCacheV3{
				SchemaVersion: semanticCacheSchemaVersionV3,
				Boiler: &semanticCacheBoiler{
					CentralHeatingPumpActive: &cachedActive,
					FlowTemperatureC:         &flow,
				},
			}); err != nil {
				t.Fatal(err)
			}

			poller := k01PumpPoller(1, nil)
			snapshot, ok := preloadSemanticCache(poller.provider, store)
			if !ok {
				t.Fatal("legacy cache was discarded wholesale")
			}
			k01AssertPublishedPump(t, poller, nil)
			poller.hydrateFromCache(snapshot)
			if poller.boiler == nil || poller.boiler.CentralHeatingPumpActive != nil {
				t.Fatal("hydrated boiler pump must be unknown")
			}
			if got := poller.boiler.FlowTemperatureC; got == nil || *got != flow {
				t.Fatalf("cached flow = %v; want retained %v", got, flow)
			}
			poller.refreshBoilerStatusTier(context.Background(), boilerStatusTierFast)
			k01AssertPublishedPump(t, poller, nil)

			// Independent evidence can still replace Unknown after hydration.
			poller.boiler = mergeBoilerSnapshotNonDestructive(poller.boiler, &vaillantBoilerSnapshot{
				CentralHeatingPumpActive: &cachedActive,
			})
			poller.publishBoilerStatus(semanticSnapshotSourceLive)
			poller.refreshBoilerStatusTier(context.Background(), boilerStatusTierFast)
			k01AssertPublishedPump(t, poller, &cachedActive)
			if err := store.Save(semanticCacheSnapshot{Boiler: poller.provider.BoilerStatus()}); err != nil {
				t.Fatal(err)
			}
			cached, ok := store.Load()
			if !ok || cached.Boiler == nil || cached.Boiler.State.CentralHeatingPumpActive != nil {
				t.Fatal("cache must not restore pump state without provenance")
			}
		})
	}
}

func k01PumpPoller(circuitRaw uint16, boilerActive *bool) *vaillantSemanticPoller {
	provider := graphql.NewLiveSemanticProvider()
	// Independent synthetic boiler state; no decoder or bus read supplies it.
	provider.SetBoilerStatus(&graphql.BoilerStatus{
		State: graphql.BoilerState{CentralHeatingPumpActive: boilerActive},
	})
	observedAt := time.Unix(100, 0)
	flow := 42.5
	return &vaillantSemanticPoller{
		provider:   provider,
		controller: 0x15,
		nowFn:      func() time.Time { return observedAt },
		boiler:     boilerSnapshotFromGraphQL(provider.BoilerStatus()),
		system: &vaillantSystemSnapshot{
			Controller:                  0x15,
			SystemFlowTemperature:       &flow,
			SystemFlowTemperatureLiveAt: observedAt,
		},
		circuits: map[byte]*vaillantCircuitSnapshot{
			0: {
				Instance:         0,
				Active:           true,
				Controller:       0x15,
				PumpStatusRaw:    &circuitRaw,
				PumpStatusLiveAt: observedAt,
			},
		},
	}
}

func k01AssertPublishedPump(t *testing.T, poller *vaillantSemanticPoller, want *bool) {
	t.Helper()
	status := poller.provider.BoilerStatus()
	if status == nil {
		t.Fatal("boiler status missing after assembly")
	}
	got := status.State.CentralHeatingPumpActive
	if want == nil {
		if got != nil {
			t.Errorf("boiler pump = %t; want unknown without independent boiler evidence", *got)
		}
		return
	}
	if got == nil {
		t.Errorf("boiler pump = unknown; want independent boiler value %t", *want)
	} else if *got != *want {
		t.Errorf("boiler pump = %t; want independent boiler value %t", *got, *want)
	}
}
