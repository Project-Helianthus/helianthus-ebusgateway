package graphql

import (
	"expvar"
	"testing"
	"time"
)

func TestObservability_StartupPhaseTransitionIncrementsCounter(t *testing.T) {
	provider := NewLiveSemanticProvider()

	key := "BOOT_INIT->CACHE_LOADED_STALE"
	before := expvarMapInt64Value(semanticStartupPhaseTransitionsTotal, key)

	provider.SetZonesFromCache([]Zone{{ID: "zone-1", Name: "Zone 1"}})

	after := expvarMapInt64Value(semanticStartupPhaseTransitionsTotal, key)
	if after != before+1 {
		t.Fatalf("semanticStartupPhaseTransitionsTotal[%q] = %d; want %d", key, after, before+1)
	}
	if got := semanticStartupCurrentPhase.Value(); got != string(SemanticStartupPhaseCacheLoadedStale) {
		t.Fatalf("semanticStartupCurrentPhase = %q; want %q", got, SemanticStartupPhaseCacheLoadedStale)
	}
}

func TestObservability_EnergyMergeCounters(t *testing.T) {
	store := newEnergyMergeStore()
	now := time.Date(2026, time.February, 26, 12, 0, 0, 0, time.UTC)
	key := energyMergeKey{Channel: "gas", Usage: "heating", Period: "day"}

	beforeBroadcastAccept := expvarMapInt64Value(semanticEnergyMergesTotal, "broadcast")
	beforeRegisterAccept := expvarMapInt64Value(semanticEnergyMergesTotal, "register")
	beforeMonotonicReject := expvarMapInt64Value(semanticEnergyRejectionsTotal, "monotonic")
	beforeSourceDowngradeReject := expvarMapInt64Value(semanticEnergyRejectionsTotal, "source_downgrade")

	if !store.Apply(key, 1.0, EnergySourceBroadcast, now) {
		t.Fatal("first broadcast apply rejected; want accepted")
	}
	if store.Apply(key, 2.0, EnergySourceBroadcast, now) {
		t.Fatal("same-timestamp broadcast apply accepted; want monotonic reject")
	}
	if !store.Apply(key, 3.0, EnergySourceRegister, now) {
		t.Fatal("register apply over broadcast rejected; want accepted")
	}
	if store.Apply(key, 4.0, EnergySourceBroadcast, now.Add(1*time.Second)) {
		t.Fatal("broadcast apply over register accepted; want source_downgrade reject")
	}

	afterBroadcastAccept := expvarMapInt64Value(semanticEnergyMergesTotal, "broadcast")
	afterRegisterAccept := expvarMapInt64Value(semanticEnergyMergesTotal, "register")
	afterMonotonicReject := expvarMapInt64Value(semanticEnergyRejectionsTotal, "monotonic")
	afterSourceDowngradeReject := expvarMapInt64Value(semanticEnergyRejectionsTotal, "source_downgrade")

	if afterBroadcastAccept != beforeBroadcastAccept+1 {
		t.Fatalf("semanticEnergyMergesTotal[broadcast] = %d; want %d", afterBroadcastAccept, beforeBroadcastAccept+1)
	}
	if afterRegisterAccept != beforeRegisterAccept+1 {
		t.Fatalf("semanticEnergyMergesTotal[register] = %d; want %d", afterRegisterAccept, beforeRegisterAccept+1)
	}
	if afterMonotonicReject != beforeMonotonicReject+1 {
		t.Fatalf("semanticEnergyRejectionsTotal[monotonic] = %d; want %d", afterMonotonicReject, beforeMonotonicReject+1)
	}
	if afterSourceDowngradeReject != beforeSourceDowngradeReject+1 {
		t.Fatalf("semanticEnergyRejectionsTotal[source_downgrade] = %d; want %d", afterSourceDowngradeReject, beforeSourceDowngradeReject+1)
	}
}

func expvarMapInt64Value(m *expvar.Map, key string) int64 {
	if m == nil {
		return 0
	}
	variable := m.Get(key)
	if variable == nil {
		return 0
	}
	counter, ok := variable.(*expvar.Int)
	if !ok {
		return 0
	}
	return counter.Value()
}
