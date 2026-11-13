package mcp

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// busSummaryStartupReasonSnapshot is a synthetic bus summary of a bus without
// a controller whose heat source is of heat-pump class: the startup phase has
// left warm-up through the boot-live timer and carries the startup reason.
func busSummaryStartupReasonSnapshot(reason string) BusObservabilitySnapshot {
	at := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	return BusObservabilitySnapshot{Summary: &BusSummary{
		LastUpdatedAt: &at,
		Status: &BusObservabilityStatus{
			LastUpdatedAt:          &at,
			TransportClass:         "synthetic",
			PublisherCadenceSec:    5,
			PublisherCadenceSource: "default",
			Capability: BusObservabilityCapability{
				ActiveSupported:  true,
				PassiveSupported: true,
				PassiveState:     "warming_up",
				EndpointState:    "connected",
			},
			Warmup:        BusObservabilityWarmup{State: "warming_up", RequiredTransactions: 3},
			TimingQuality: BusObservabilityTimingQuality{Active: "estimated", Passive: "unavailable", Busy: "unavailable", Periodicity: "unavailable"},
			Startup: &BusObservabilityStartup{
				LastUpdatedAt: &at,
				Phase:         "DEGRADED",
				CacheEpoch:    0,
				LiveEpoch:     0,
				Reason:        reason,
			},
			FeatureFlags: ObserveFirstFeatureFlagState{ObserveFirstEnabled: true, ExternalWritePolicy: "record_only"},
		},
		Messages:    BusBoundedListSummary{Count: 0, Capacity: 16},
		Periodicity: BusBoundedListSummary{Count: 0, Capacity: 16},
	}}
}

func callBusSummaryForStartupReason(t *testing.T, reason string) map[string]any {
	t.Helper()
	server, err := NewServer(registry.NewDeviceRegistry(nil), &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	server.SetBusObservabilityProvider(&testBusObservabilityProvider{snapshot: busSummaryStartupReasonSnapshot(reason)})
	res := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolBusSummaryGetName + `","arguments":{}}`),
	})
	envelope := envelopeFromResult(t, res)
	// Only the wall-clock timestamp varies across identical calls. Keep the
	// computed data hash and every other envelope field.
	meta := envelope["meta"].(map[string]any)
	if _, ok := meta["data_timestamp"].(string); !ok {
		t.Fatal("missing data_timestamp")
	}
	meta["data_timestamp"] = "2001-02-03T04:05:06Z"
	return envelope
}

func busSummaryStartupBlock(t *testing.T, envelope map[string]any) map[string]any {
	t.Helper()
	data, _ := envelope["data"].(map[string]any)
	status, _ := data["status"].(map[string]any)
	startup, ok := status["startup"].(map[string]any)
	if !ok {
		t.Fatalf("status.startup = %#v; want object", status["startup"])
	}
	return startup
}

// TestBusSummaryStartupReasonGolden pins the ebus_v1_bus_summary_get envelope,
// including its data_hash, with the startup reason no_supported_heat_source_path
// populated, and that the reason takes part in the hash while an empty reason
// is omitted from the startup block.
func TestBusSummaryStartupReasonGolden(t *testing.T) {
	const reason = "no_supported_heat_source_path"
	first := callBusSummaryForStartupReason(t, reason)
	second := callBusSummaryForStartupReason(t, reason)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("identical bus summary calls produced different envelopes")
	}
	if got := busSummaryStartupBlock(t, first)["reason"]; got != reason {
		t.Fatalf("status.startup.reason = %#v; want %q", got, reason)
	}
	compareRPCInvokeGolden(t, "bus_summary_startup_reason.golden.json", first)

	without := callBusSummaryForStartupReason(t, "")
	if got, present := busSummaryStartupBlock(t, without)["reason"]; present {
		t.Fatalf("status.startup.reason = %#v; want the member omitted without a startup reason", got)
	}
	withHash := first["meta"].(map[string]any)["data_hash"]
	withoutHash := without["meta"].(map[string]any)["data_hash"]
	if withHash == "" || withHash == withoutHash {
		t.Fatalf("data_hash with reason = %#v, without = %#v; want distinct non-empty hashes", withHash, withoutHash)
	}
}
