package ebusgateway

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func activeArbitrationHoldTestStore() *BusObservabilityStore {
	store := NewBusObservabilityStore(DefaultConfig())
	now := time.Unix(1_700_000_000, 0)
	store.now = func() time.Time { return now }
	return store
}

func activeArbitrationHeldSample(t *testing.T, store *BusObservabilityStore) string {
	t.Helper()
	for _, line := range strings.Split(store.RenderPrometheus(), "\n") {
		if value, ok := strings.CutPrefix(line, "ebus_adaptermux_active_arbitration_held "); ok {
			return value
		}
	}
	return ""
}

// An empty hold reason leaves the summary and the metrics byte-identical to
// the same store without a hold provider, and the gauge reads 0.
func TestBusObservabilityActiveArbitrationHoldEmptyReasonLeavesSummaryUnchanged(t *testing.T) {
	store := activeArbitrationHoldTestStore()
	want, err := json.Marshal(store.Snapshot().Summary)
	if err != nil {
		t.Fatal(err)
	}
	wantMetrics := store.RenderPrometheus()
	store.SetActiveArbitrationHoldProvider(func() string { return "" })
	got, err := json.Marshal(store.Snapshot().Summary)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("summary changed by an empty hold reason:\n got %s\nwant %s", got, want)
	}
	if store.RenderPrometheus() != wantMetrics {
		t.Fatal("metrics changed by an empty hold reason")
	}
	store.SetAdaptermuxDiagProvider(func() AdaptermuxDiagSnapshot { return AdaptermuxDiagSnapshot{} })
	if got := activeArbitrationHeldSample(t, store); got != "0" {
		t.Fatalf("ebus_adaptermux_active_arbitration_held = %q, want 0", got)
	}
}

// Without the adaptermux diagnostic provider the gauge is omitted like the
// rest of the family.
func TestBusObservabilityActiveArbitrationHoldGaugeOmittedWithoutProvider(t *testing.T) {
	store := activeArbitrationHoldTestStore()
	if strings.Contains(store.RenderPrometheus(), "ebus_adaptermux_active_arbitration_held") {
		t.Fatal("gauge rendered without the adaptermux diagnostic provider")
	}
}

// The reason adapter_out_of_step makes the summary degraded with that reason
// and sets the gauge to 1.
func TestBusObservabilityActiveArbitrationHoldReasonDegradesSummary(t *testing.T) {
	store := activeArbitrationHoldTestStore()
	before := store.Snapshot().Summary.Status.Degraded.Reasons
	store.SetActiveArbitrationHoldProvider(func() string { return "adapter_out_of_step" })
	store.SetAdaptermuxDiagProvider(func() AdaptermuxDiagSnapshot {
		return AdaptermuxDiagSnapshot{ActiveArbitrationHeld: true}
	})
	degraded := store.Snapshot().Summary.Status.Degraded
	want := append(append([]string(nil), before...), "adapter_out_of_step")
	if !degraded.Active || !reflect.DeepEqual(degraded.Reasons, want) {
		t.Fatalf("degraded = %+v, want active with reasons %v", degraded, want)
	}
	if got := activeArbitrationHeldSample(t, store); got != "1" {
		t.Fatalf("ebus_adaptermux_active_arbitration_held = %q, want 1", got)
	}
}
