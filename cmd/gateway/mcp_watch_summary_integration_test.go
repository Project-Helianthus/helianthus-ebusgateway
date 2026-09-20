package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

const mcpToolWatchSummaryGet = "ebus.v1.watch.summary.get"

func TestMCPWatchSummaryProviderAdapter_DirectApplyPolicyGoldenAndNormalizedFlagClasses(t *testing.T) {
	base := time.Date(2026, time.March, 13, 8, 30, 0, 0, time.UTC)
	type flagsCase struct {
		name       string
		flags      ebusgateway.ObserveFirstFeatureFlags
		wantCounts map[string]int
	}
	cases := []flagsCase{
		{
			name:  "permitted paths and denied policies",
			flags: ebusgateway.NormalizeObserveFirstFeatureFlags(true, true, true, ebusgateway.ObserveFirstExternalWritePolicyRecordOnly),
			wantCounts: map[string]int{
				"state_eligible": 1,
				"not_applicable": 6,
			},
		},
		{
			name:  "global disable",
			flags: ebusgateway.NormalizeObserveFirstFeatureFlags(false, true, true, ebusgateway.ObserveFirstExternalWritePolicyRecordOnly),
			wantCounts: map[string]int{
				"state_master_off": 1,
				"not_applicable":   6,
			},
		},
		{
			name:  "normalized config disabled",
			flags: ebusgateway.NormalizeObserveFirstFeatureFlags(true, false, true, ebusgateway.ObserveFirstExternalWritePolicyRecordAndInvalidate),
			wantCounts: map[string]int{
				"state_ineligible": 1,
				"not_applicable":   6,
			},
		},
	}

	for _, test := range cases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			shadow := newWatchSummaryPolicyFixture(t, base, test.flags)
			server, err := mcp.NewServer(emptyMCPRegistry{}, nil)
			if err != nil {
				t.Fatalf("NewServer error = %v", err)
			}
			server.SetWatchSummaryProvider(newMCPWatchSummaryProvider(shadow))

			first := mcpCallToolEnvelope(t, server.Handler(), mcpToolWatchSummaryGet, `{}`)
			second := mcpCallToolEnvelope(t, server.Handler(), mcpToolWatchSummaryGet, `{}`)
			firstMeta, ok := first["meta"].(map[string]any)
			if !ok {
				t.Fatalf("first meta type = %T; want map", first["meta"])
			}
			secondMeta, ok := second["meta"].(map[string]any)
			if !ok {
				t.Fatalf("second meta type = %T; want map", second["meta"])
			}
			if firstMeta["data_hash"] != secondMeta["data_hash"] {
				t.Fatalf("data_hash differs across identical watch summaries: %v != %v", firstMeta["data_hash"], secondMeta["data_hash"])
			}
			if first["error"] != nil {
				t.Fatalf("watch summary error = %#v; want nil", first["error"])
			}

			data, ok := first["data"].(map[string]any)
			if !ok {
				t.Fatalf("watch summary data type = %T; want map", first["data"])
			}
			classes, ok := data["direct_apply_eligibility_classes"].([]any)
			if !ok {
				t.Fatalf("direct apply classes type = %T; want list", data["direct_apply_eligibility_classes"])
			}
			gotCounts := make(map[string]int, len(classes))
			for _, raw := range classes {
				item, ok := raw.(map[string]any)
				if !ok {
					t.Fatalf("direct apply class type = %T; want map", raw)
				}
				name, _ := item["class"].(string)
				count, _ := item["count"].(float64)
				gotCounts[name] = int(count)
			}
			for name, want := range test.wantCounts {
				if gotCounts[name] != want {
					t.Fatalf("direct apply class %q = %d; want %d in %#v", name, gotCounts[name], want, gotCounts)
				}
			}

			if test.name != "permitted paths and denied policies" {
				return
			}
			firstMeta["data_timestamp"] = base.Format(time.RFC3339Nano)
			actual, err := json.Marshal(first)
			if err != nil {
				t.Fatalf("marshal golden envelope: %v", err)
			}
			fixture := filepath.Join("testdata", "watch_summary_direct_apply_policy.golden.json")
			want, err := os.ReadFile(fixture)
			if err != nil {
				t.Fatalf("read watch-summary golden: %v\ngot: %s", err, actual)
			}
			if string(actual) != strings.TrimSpace(string(want)) {
				t.Fatalf("watch-summary golden mismatch\nwant: %s\ngot:  %s", strings.TrimSpace(string(want)), actual)
			}
		})
	}
}

func newWatchSummaryPolicyFixture(t *testing.T, now time.Time, flags ebusgateway.ObserveFirstFeatureFlags) *ebusgateway.ShadowCache {
	t.Helper()
	descriptors := []ebusgateway.WatchDescriptor{
		watchSummaryFixtureDescriptor(ebusgateway.NewB509WatchKey(0x15, 0x2101), ebusgateway.WatchSemanticClassState, ebusgateway.WatchFreshnessProfileStateFast, ebusgateway.WatchDirectApplyPolicyStateDefault),
		watchSummaryFixtureDescriptor(ebusgateway.NewB524WatchKey(0x15, 0x02, 0x08, 0x00, 0x2102), ebusgateway.WatchSemanticClassConfig, ebusgateway.WatchFreshnessProfileConfig, ebusgateway.WatchDirectApplyPolicyConfigOptIn),
		watchSummaryFixtureDescriptor(ebusgateway.NewB509WatchKey(0x15, 0x2103), ebusgateway.WatchSemanticClassState, ebusgateway.WatchFreshnessProfileStateFast, ebusgateway.WatchDirectApplyPolicyNever),
		watchSummaryFixtureDescriptor(ebusgateway.NewB509WatchKey(0x15, 0x2104), ebusgateway.WatchSemanticClassState, ebusgateway.WatchFreshnessProfileStateFast, ebusgateway.WatchDirectApplyPolicyEnergyMergeOnly),
		watchSummaryFixtureDescriptor(ebusgateway.NewB509WatchKey(0x15, 0x2105), ebusgateway.WatchSemanticClassState, ebusgateway.WatchFreshnessProfileStateFast, ebusgateway.WatchDirectApplyPolicy("unknown")),
		watchSummaryFixtureDescriptor(ebusgateway.NewB509WatchKey(0x15, 0x2106), ebusgateway.WatchSemanticClassState, ebusgateway.WatchFreshnessProfileStateFast, ebusgateway.WatchDirectApplyPolicyConfigOptIn),
		watchSummaryFixtureDescriptor(ebusgateway.NewB524WatchKey(0x15, 0x02, 0x08, 0x00, 0x2107), ebusgateway.WatchSemanticClassConfig, ebusgateway.WatchFreshnessProfileConfig, ebusgateway.WatchDirectApplyPolicyStateDefault),
	}
	catalog, err := ebusgateway.NewWatchCatalog(descriptors)
	if err != nil {
		t.Fatalf("NewWatchCatalog error = %v", err)
	}
	activations := ebusgateway.NewWatchActivationSet(catalog)
	keys := make([]ebusgateway.WatchKey, 0, len(descriptors))
	for _, descriptor := range descriptors {
		keys = append(keys, descriptor.Key)
	}
	if err := activations.Activate(ebusgateway.WatchActivationSourcePoller, keys...); err != nil {
		t.Fatalf("Activate error = %v", err)
	}
	return ebusgateway.NewShadowCache(ebusgateway.ShadowCacheOptions{
		Catalog:      catalog,
		Activations:  activations,
		FeatureFlags: flags,
		Now:          func() time.Time { return now },
	})
}

func watchSummaryFixtureDescriptor(key ebusgateway.WatchKey, class ebusgateway.WatchSemanticClass, freshness ebusgateway.WatchFreshnessProfile, policy ebusgateway.WatchDirectApplyPolicy) ebusgateway.WatchDescriptor {
	return ebusgateway.WatchDescriptor{
		Key:               key,
		SemanticClass:     class,
		FreshnessProfile:  freshness,
		DecoderID:         "test.watch.summary.policy",
		CorrelationPolicy: ebusgateway.WatchCorrelationPolicyRequestResponse,
		DirectApplyPolicy: policy,
	}
}

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
