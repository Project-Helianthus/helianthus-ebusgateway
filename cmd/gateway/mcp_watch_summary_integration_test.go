package main

import (
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

const mcpToolWatchSummaryGet = "ebus.v1.watch.summary.get"

func TestMCPWatchSummaryProviderAdapterWiresRuntimeShadowCache(t *testing.T) {
	now := time.Date(2026, time.March, 13, 8, 0, 0, 0, time.UTC)
	key := ebusgateway.NewB524WatchKey(0x15, 0x02, 0x08, 0x00, 0x1234)
	catalog, err := ebusgateway.NewWatchCatalog([]ebusgateway.WatchDescriptor{
		{
			Key:               key,
			SemanticClass:     ebusgateway.WatchSemanticClassState,
			FreshnessProfile:  ebusgateway.WatchFreshnessProfileStateFast,
			DecoderID:         "test.watch.summary",
			CorrelationPolicy: ebusgateway.WatchCorrelationPolicyRequestResponse,
			DirectApplyPolicy: ebusgateway.WatchDirectApplyPolicyStateDefault,
		},
	})
	if err != nil {
		t.Fatalf("NewWatchCatalog error = %v", err)
	}

	activations := ebusgateway.NewWatchActivationSet(catalog)
	if err := activations.Activate(ebusgateway.WatchActivationSourcePoller, key); err != nil {
		t.Fatalf("Activate poller error = %v", err)
	}
	if err := activations.Activate(ebusgateway.WatchActivationSourceTooling, key); err != nil {
		t.Fatalf("Activate tooling error = %v", err)
	}

	shadow := ebusgateway.NewShadowCache(ebusgateway.ShadowCacheOptions{
		Catalog:      catalog,
		Activations:  activations,
		FeatureFlags: ebusgateway.NormalizeObserveFirstFeatureFlags(true, true, false, ebusgateway.ObserveFirstExternalWritePolicyRecordOnly),
		Now:          func() time.Time { return now },
	})

	writeResult := shadow.Write(ebusgateway.ShadowWrite{
		Key:        key,
		Source:     ebusgateway.ShadowWriteSourcePassive,
		Confidence: ebusgateway.ShadowConfidenceHigh,
		Value:      []byte{0x42},
		ObservedAt: now,
	})
	if !writeResult.Accepted {
		t.Fatalf("Shadow write rejected: %s", writeResult.Reason)
	}

	server, err := mcp.NewServer(emptyMCPRegistry{}, nil)
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	server.SetWatchSummaryProvider(newMCPWatchSummaryProvider(shadow))

	tools := mcpToolsList(t, server.Handler())
	if !mcpToolsContain(tools, mcpToolWatchSummaryGet) {
		t.Fatalf("tools/list missing %q after shadow wiring", mcpToolWatchSummaryGet)
	}

	envelope := mcpCallToolEnvelope(t, server.Handler(), mcpToolWatchSummaryGet, `{}`)
	data, ok := envelope["data"].(map[string]any)
	if !ok {
		t.Fatalf("watch summary data type = %T; want map", envelope["data"])
	}
	if got, _ := data["last_updated_at"].(string); got != now.Format(time.RFC3339Nano) {
		t.Fatalf("watch summary last_updated_at = %q; want %s", got, now.Format(time.RFC3339Nano))
	}

	activationCounts, ok := data["activation_counts"].(map[string]any)
	if !ok {
		t.Fatalf("watch summary activation_counts type = %T; want map", data["activation_counts"])
	}
	if got, _ := activationCounts["active_keys"].(float64); int(got) != 1 {
		t.Fatalf("watch summary activation_counts.active_keys = %v; want 1", activationCounts["active_keys"])
	}

	degraded, ok := data["degraded"].(map[string]any)
	if !ok {
		t.Fatalf("watch summary degraded type = %T; want map", data["degraded"])
	}
	if got, _ := degraded["shadowing_enabled"].(bool); !got {
		t.Fatalf("watch summary degraded.shadowing_enabled = %v; want true", degraded["shadowing_enabled"])
	}
}
