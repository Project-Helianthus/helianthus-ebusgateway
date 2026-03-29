package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mdns"
	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

func TestGraphQLBusObservabilityProviderAdapter_ParityWithMCPAdapter(t *testing.T) {
	cfg := ebusgateway.DefaultConfig()
	cfg.BroadcastListen = true
	cfg.TransportConfig.Protocol = ebusgateway.TransportENS
	cfg.SemanticStateInterval = 7 * time.Minute

	store := ebusgateway.NewBusObservabilityStore(cfg)
	if store == nil {
		t.Fatal("NewBusObservabilityStore() = nil")
	}
	startupAt := time.Date(2026, time.March, 12, 17, 45, 0, 0, time.UTC)
	store.SetStartupSurfaceProvider(func() *ebusgateway.BusObservabilityStartup {
		return &ebusgateway.BusObservabilityStartup{
			LastUpdatedAt: &startupAt,
			Phase:         string(graphql.SemanticStartupPhaseLiveReady),
			CacheEpoch:    4,
			LiveEpoch:     9,
		}
	})

	if err := store.OnBusEvent(protocol.BusEvent{
		Kind: protocol.BusEventAttemptComplete,
		Request: protocol.Frame{
			Source:    0x08,
			Target:    0x15,
			Primary:   0xB5,
			Secondary: 0x09,
			Data:      []byte{0x03},
		},
		HasRequest: true,
	}); err != nil {
		t.Fatalf("OnBusEvent error = %v", err)
	}

	base := time.Date(2026, time.March, 12, 18, 0, 0, 0, time.UTC)
	store.OnPassiveClassifiedEvent(testPassiveTransactionEvent(base.Add(10*time.Second), 0x10, 0x08, 0xB5, 0x24))
	store.OnPassiveClassifiedEvent(testPassiveTransactionEvent(base.Add(20*time.Second), 0x10, 0x08, 0xB5, 0x24))

	got := graphQLSnapshotToMCPShape(t, newGraphQLBusObservabilityProvider(store).Snapshot())
	want := newMCPBusObservabilityProvider(store).Snapshot()

	if !reflect.DeepEqual(normalizeMCPBusSnapshot(got), normalizeMCPBusSnapshot(want)) {
		t.Fatalf("GraphQL/MCP bus observability parity mismatch\nwant: %#v\ngot:  %#v", want, got)
	}
}

func TestRun_WiresBusObservabilityIntoGraphQLQueries(t *testing.T) {
	origWireObserveFirstObserversFn := wireObserveFirstObserversFn
	origStartDiscoveryScanLoopFn := startDiscoveryScanLoopFn
	origStartVaillantSemanticPollingFn := startVaillantSemanticPollingFn
	origStartHTTPServerFn := startHTTPServerFn
	startupAt := time.Date(2026, time.March, 12, 17, 45, 0, 0, time.UTC)
	t.Cleanup(func() {
		wireObserveFirstObserversFn = origWireObserveFirstObserversFn
		startDiscoveryScanLoopFn = origStartDiscoveryScanLoopFn
		startVaillantSemanticPollingFn = origStartVaillantSemanticPollingFn
		startHTTPServerFn = origStartHTTPServerFn
	})

	wireObserveFirstObserversFn = func(cfg *ebusgateway.Config) (*ebusgateway.BusObservabilityStore, *ebusgateway.ActivePassiveDeduplicator, error) {
		store := ebusgateway.NewBusObservabilityStore(*cfg)
		store.SetStartupSurfaceProvider(func() *ebusgateway.BusObservabilityStartup {
			return &ebusgateway.BusObservabilityStartup{
				LastUpdatedAt: &startupAt,
				Phase:         string(graphql.SemanticStartupPhaseLiveReady),
				CacheEpoch:    1,
				LiveEpoch:     3,
			}
		})
		if err := store.OnBusEvent(protocol.BusEvent{
			Kind: protocol.BusEventAttemptComplete,
			Request: protocol.Frame{
				Source:    0x08,
				Target:    0x15,
				Primary:   0xB5,
				Secondary: 0x09,
				Data:      []byte{0x03},
			},
			HasRequest: true,
		}); err != nil {
			t.Fatalf("OnBusEvent error = %v", err)
		}
		return store, nil, nil
	}
	startDiscoveryScanLoopFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.Builder) startupScanSignals {
		return startupScanSignals{}
	}
	startVaillantSemanticPollingFn = func(context.Context, ebusgateway.Config, *ebusgateway.Gateway, *graphql.LiveSemanticProvider, *graphql.BroadcastHub, <-chan struct{}) *vaillantSemanticPoller {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startHTTPServerFn = func(_ context.Context, cfg ebusgateway.Config, gateway *ebusgateway.Gateway, builder *graphql.Builder, hub *graphql.BroadcastHub, semanticProvider graphql.SemanticProvider, scheduleWriter mcp.ScheduleWriter, busObservability *ebusgateway.BusObservabilityStore, _ *ebusgateway.ShadowCache) (*http.Server, mdns.Advertiser, error) {
		if busObservability == nil {
			t.Fatal("busObservability = nil; want runtime wiring to pass the store")
		}
		handler, err := graphql.NewInvokeHandler(builder, gateway.Registry, gateway.Router)
		if err != nil {
			t.Fatalf("NewInvokeHandler error = %v", err)
		}

		body := bytes.NewBufferString(`{"query":"{ busSummary { lastUpdatedAt messages { count capacity } status { lastUpdatedAt transportClass publisherCadenceSec publisherCadenceSource capability { activeSupported } startup { lastUpdatedAt phase cacheEpoch liveEpoch } featureFlags { lastUpdatedAt } } } busMessages(limit: 1) { count items { family sourceAddress targetAddress } } }"}`)
		req := httptest.NewRequest(http.MethodPost, cfg.GraphQLPath, body)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("graphql status = %d; want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var response struct {
			Data struct {
				BusSummary struct {
					LastUpdatedAt string `json:"lastUpdatedAt"`
					Messages      struct {
						Count    int `json:"count"`
						Capacity int `json:"capacity"`
					} `json:"messages"`
					Status struct {
						LastUpdatedAt          string  `json:"lastUpdatedAt"`
						TransportClass         string  `json:"transportClass"`
						PublisherCadenceSec    float64 `json:"publisherCadenceSec"`
						PublisherCadenceSource string  `json:"publisherCadenceSource"`
						Capability             struct {
							ActiveSupported bool `json:"activeSupported"`
						} `json:"capability"`
						Startup struct {
							LastUpdatedAt string `json:"lastUpdatedAt"`
							Phase         string `json:"phase"`
							CacheEpoch    string `json:"cacheEpoch"`
							LiveEpoch     string `json:"liveEpoch"`
						} `json:"startup"`
						FeatureFlags struct {
							LastUpdatedAt string `json:"lastUpdatedAt"`
						} `json:"featureFlags"`
					} `json:"status"`
				} `json:"busSummary"`
				BusMessages struct {
					Count int `json:"count"`
					Items []struct {
						Family        string `json:"family"`
						SourceAddress int    `json:"sourceAddress"`
						TargetAddress int    `json:"targetAddress"`
					} `json:"items"`
				} `json:"busMessages"`
			} `json:"data"`
			Errors []any `json:"errors"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatalf("graphql response unmarshal: %v body=%s", err, rec.Body.String())
		}
		if len(response.Errors) != 0 {
			t.Fatalf("graphql errors = %#v; want none", response.Errors)
		}
		if response.Data.BusSummary.Messages.Count != 1 {
			t.Fatalf("busSummary.messages.count = %d; want 1", response.Data.BusSummary.Messages.Count)
		}
		if response.Data.BusSummary.Status.TransportClass == "" {
			t.Fatal("busSummary.status.transportClass empty; want wired store summary")
		}
		if response.Data.BusSummary.Status.PublisherCadenceSec != 420 {
			t.Fatalf("busSummary.status.publisherCadenceSec = %v; want 420", response.Data.BusSummary.Status.PublisherCadenceSec)
		}
		if response.Data.BusSummary.Status.PublisherCadenceSource != "config.semantic_state_interval" {
			t.Fatalf("busSummary.status.publisherCadenceSource = %q; want config.semantic_state_interval", response.Data.BusSummary.Status.PublisherCadenceSource)
		}
		if !response.Data.BusSummary.Status.Capability.ActiveSupported {
			t.Fatal("busSummary.status.capability.activeSupported = false; want true")
		}
		if response.Data.BusSummary.Status.Startup.Phase != string(graphql.SemanticStartupPhaseBootInit) {
			t.Fatalf("busSummary.status.startup.phase = %q; want BOOT_INIT", response.Data.BusSummary.Status.Startup.Phase)
		}
		if response.Data.BusSummary.Status.Startup.CacheEpoch != "0" || response.Data.BusSummary.Status.Startup.LiveEpoch != "0" {
			t.Fatalf("busSummary.status.startup epochs = (%s,%s); want (0,0)", response.Data.BusSummary.Status.Startup.CacheEpoch, response.Data.BusSummary.Status.Startup.LiveEpoch)
		}
		if response.Data.BusSummary.Status.Startup.LastUpdatedAt == "" {
			t.Fatal("busSummary.status.startup.lastUpdatedAt empty; want timestamp")
		}
		if _, err := time.Parse(time.RFC3339Nano, response.Data.BusSummary.Status.Startup.LastUpdatedAt); err != nil {
			t.Fatalf("busSummary.status.startup.lastUpdatedAt parse: %v", err)
		}
		if response.Data.BusSummary.LastUpdatedAt == "" {
			t.Fatal("busSummary.lastUpdatedAt empty; want timestamp")
		}
		if _, err := time.Parse(time.RFC3339Nano, response.Data.BusSummary.LastUpdatedAt); err != nil {
			t.Fatalf("busSummary.lastUpdatedAt parse: %v", err)
		}
		if response.Data.BusSummary.Status.LastUpdatedAt == "" {
			t.Fatal("busSummary.status.lastUpdatedAt empty; want timestamp")
		}
		if _, err := time.Parse(time.RFC3339Nano, response.Data.BusSummary.Status.LastUpdatedAt); err != nil {
			t.Fatalf("busSummary.status.lastUpdatedAt parse: %v", err)
		}
		if response.Data.BusSummary.Status.FeatureFlags.LastUpdatedAt == "" {
			t.Fatal("busSummary.status.featureFlags.lastUpdatedAt empty; want timestamp")
		}
		if _, err := time.Parse(time.RFC3339Nano, response.Data.BusSummary.Status.FeatureFlags.LastUpdatedAt); err != nil {
			t.Fatalf("busSummary.status.featureFlags.lastUpdatedAt parse: %v", err)
		}
		if response.Data.BusMessages.Count != 1 || len(response.Data.BusMessages.Items) != 1 {
			t.Fatalf("busMessages = %+v; want one wired item", response.Data.BusMessages)
		}
		if response.Data.BusMessages.Items[0].Family != "B509" {
			t.Fatalf("busMessages.items[0].family = %q; want B509", response.Data.BusMessages.Items[0].Family)
		}

		cancel()
		return nil, nil, nil
	}

	cfg := ebusgateway.DefaultConfig()
	cfg.Transport = transport.NewLoopback()
	cfg.GraphQLPath = "/graphql"
	cfg.BroadcastListen = false
	cfg.ScanOnStart = false
	cfg.SemanticStateInterval = 7 * time.Minute

	if err := run(ctx, cfg); err != nil {
		t.Fatalf("run error = %v", err)
	}
}

func graphQLSnapshotToMCPShape(t *testing.T, snapshot graphql.BusObservabilitySnapshot) mcp.BusObservabilitySnapshot {
	t.Helper()

	out := mcp.BusObservabilitySnapshot{
		Summary: graphQLSummaryToMCP(snapshot.Summary),
	}
	if len(snapshot.Messages) > 0 {
		out.Messages = make([]mcp.BusMessage, len(snapshot.Messages))
		for i, item := range snapshot.Messages {
			out.Messages[i] = mcp.BusMessage{
				Scope:         item.Scope,
				Family:        item.Family,
				FrameType:     item.FrameType,
				Outcome:       item.Outcome,
				ObservedAt:    parseGraphQLTime(t, item.ObservedAt),
				SourceAddress: item.SourceAddress,
				TargetAddress: item.TargetAddress,
				RequestLen:    item.RequestLen,
				ResponseLen:   item.ResponseLen,
			}
		}
	}
	if len(snapshot.Periodicity) > 0 {
		out.Periodicity = make([]mcp.BusPeriodicityEntry, len(snapshot.Periodicity))
		for i, item := range snapshot.Periodicity {
			out.Periodicity[i] = mcp.BusPeriodicityEntry{
				SourceBucket: item.SourceBucket,
				TargetBucket: item.TargetBucket,
				Primary:      item.Primary,
				Secondary:    item.Secondary,
				Family:       item.Family,
				State:        item.State,
				LastSeen:     parseGraphQLTime(t, item.LastSeen),
				SampleCount:  item.SampleCount,
				LastInterval: item.LastInterval,
				MeanInterval: item.MeanInterval,
				MinInterval:  item.MinInterval,
				MaxInterval:  item.MaxInterval,
			}
		}
	}
	return out
}

func graphQLSummaryToMCP(summary *graphql.BusSummary) *mcp.BusSummary {
	if summary == nil {
		return nil
	}
	out := &mcp.BusSummary{
		LastUpdatedAt: cloneTimePtr(summary.LastUpdatedAt),
		Status:        graphQLStatusToMCP(summary.Status),
		Messages: mcp.BusBoundedListSummary{
			Count:    summary.Messages.Count,
			Capacity: summary.Messages.Capacity,
		},
		Periodicity: mcp.BusBoundedListSummary{
			Count:    summary.Periodicity.Count,
			Capacity: summary.Periodicity.Capacity,
		},
		Counters: mcp.BusObservabilityCounters{
			SeriesBudgetOverflowTotal:      summary.Counters.SeriesBudgetOverflowTotal,
			PeriodicityBudgetOverflowTotal: summary.Counters.PeriodicityBudgetOverflowTotal,
		},
	}
	if len(summary.Errors) > 0 {
		out.Errors = make([]mcp.BusErrorAggregate, len(summary.Errors))
		for i, e := range summary.Errors {
			out.Errors[i] = mcp.BusErrorAggregate{Scope: e.Scope, Class: e.Class, Phase: e.Phase, Count: e.Count}
		}
	}
	if len(summary.Frames) > 0 {
		out.Frames = make([]mcp.BusFrameAggregate, len(summary.Frames))
		for i, f := range summary.Frames {
			out.Frames[i] = mcp.BusFrameAggregate{Scope: f.Scope, Source: f.Source, Target: f.Target, Family: f.Family, FrameType: f.FrameType, Count: f.Count}
		}
	}
	if summary.Busy != nil {
		busy := &mcp.BusBusyAggregate{TotalSeconds: summary.Busy.TotalSeconds}
		for _, w := range summary.Busy.Windows {
			busy.Windows = append(busy.Windows, mcp.BusBusyWindow{Window: w.Window, Ratio: w.Ratio})
		}
		out.Busy = busy
	}
	if summary.Reconstructor != nil {
		recon := &mcp.BusReconstructorAggregate{}
		for _, r := range summary.Reconstructor.Recoveries {
			recon.Recoveries = append(recon.Recoveries, mcp.BusReconstructorRecovery{Reason: r.Reason, Count: r.Count})
		}
		out.Reconstructor = recon
	}
	return out
}

func normalizeMCPBusSnapshot(snapshot mcp.BusObservabilitySnapshot) mcp.BusObservabilitySnapshot {
	if snapshot.Summary != nil {
		normalized := *snapshot.Summary
		normalized.LastUpdatedAt = cloneTimePtr(normalized.LastUpdatedAt)
		if snapshot.Summary.Status != nil {
			status := *snapshot.Summary.Status
			status.LastUpdatedAt = cloneTimePtr(status.LastUpdatedAt)
			if status.Startup != nil {
				status.Startup.LastUpdatedAt = cloneTimePtr(status.Startup.LastUpdatedAt)
			}
			status.Degraded.Reasons = append([]string(nil), snapshot.Summary.Status.Degraded.Reasons...)
			status.FeatureFlags.LastUpdatedAt = cloneTimePtr(status.FeatureFlags.LastUpdatedAt)
			status.FeatureFlags.Normalizations = append([]string(nil), snapshot.Summary.Status.FeatureFlags.Normalizations...)
			normalized.Status = &status
		}
		snapshot.Summary = &normalized
	}
	for i := range snapshot.Messages {
		snapshot.Messages[i].ObservedAt = snapshot.Messages[i].ObservedAt.UTC()
	}
	for i := range snapshot.Periodicity {
		snapshot.Periodicity[i].LastSeen = snapshot.Periodicity[i].LastSeen.UTC()
	}
	return snapshot
}

func graphQLStatusToMCP(status *graphql.BusObservabilityStatus) *mcp.BusObservabilityStatus {
	if status == nil {
		return nil
	}
	var startup *mcp.BusObservabilityStartup
	if status.Startup != nil {
		startup = &mcp.BusObservabilityStartup{
			LastUpdatedAt: cloneTimePtr(status.Startup.LastUpdatedAt),
			Phase:         status.Startup.Phase,
			CacheEpoch:    status.Startup.CacheEpoch,
			LiveEpoch:     status.Startup.LiveEpoch,
		}
	}
	return &mcp.BusObservabilityStatus{
		LastUpdatedAt:          cloneTimePtr(status.LastUpdatedAt),
		TransportClass:         status.TransportClass,
		PublisherCadenceSec:    status.PublisherCadenceSec,
		PublisherCadenceSource: status.PublisherCadenceSource,
		Capability: mcp.BusObservabilityCapability{
			ActiveSupported:    status.Capability.ActiveSupported,
			PassiveSupported:   status.Capability.PassiveSupported,
			BroadcastSupported: status.Capability.BroadcastSupported,
			PassiveAvailable:   status.Capability.PassiveAvailable,
			PassiveState:       status.Capability.PassiveState,
			PassiveReason:      status.Capability.PassiveReason,
			EndpointState:      status.Capability.EndpointState,
			TapConnected:       status.Capability.TapConnected,
		},
		Warmup: mcp.BusObservabilityWarmup{
			State:                 status.Warmup.State,
			Blocker:               status.Warmup.Blocker,
			ElapsedSeconds:        status.Warmup.ElapsedSeconds,
			CompletedTransactions: status.Warmup.CompletedTransactions,
			RequiredTransactions:  status.Warmup.RequiredTransactions,
			CompletionMode:        status.Warmup.CompletionMode,
		},
		TimingQuality: mcp.BusObservabilityTimingQuality{
			Active:      status.TimingQuality.Active,
			Passive:     status.TimingQuality.Passive,
			Busy:        status.TimingQuality.Busy,
			Periodicity: status.TimingQuality.Periodicity,
		},
		Degraded: mcp.BusObservabilityDegraded{
			Active:  status.Degraded.Active,
			Reasons: append([]string(nil), status.Degraded.Reasons...),
		},
		Startup: startup,
		FeatureFlags: mcp.ObserveFirstFeatureFlagState{
			ObserveFirstEnabled:      status.FeatureFlags.ObserveFirstEnabled,
			PassiveStateDirectApply:  status.FeatureFlags.PassiveStateDirectApply,
			PassiveConfigDirectApply: status.FeatureFlags.PassiveConfigDirectApply,
			ExternalWritePolicy:      status.FeatureFlags.ExternalWritePolicy,
			LastUpdatedAt:            cloneTimePtr(status.FeatureFlags.LastUpdatedAt),
			Normalizations:           append([]string(nil), status.FeatureFlags.Normalizations...),
		},
	}
}

func parseGraphQLTime(t *testing.T, value string) time.Time {
	t.Helper()
	if value == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("time.Parse(%q): %v", value, err)
	}
	return parsed
}

func testPassiveTransactionEvent(observedAt time.Time, source, target, primary, secondary byte) ebusgateway.PassiveClassifiedEvent {
	return ebusgateway.PassiveClassifiedEvent{
		Kind:      ebusgateway.PassiveClassifiedEventTransaction,
		FrameType: protocol.FrameTypeInitiatorTarget,
		Request: protocol.Frame{
			Source:    source,
			Target:    target,
			Primary:   primary,
			Secondary: secondary,
			Data:      []byte{0x01},
		},
		Response: protocol.Frame{
			Source:    target,
			Target:    source,
			Primary:   primary,
			Secondary: secondary,
			Data:      []byte{0x02},
		},
		HasRequest:  true,
		HasResponse: true,
		Timing: ebusgateway.PassiveTimingMarkers{
			RequestStart:  observedAt.Add(-50 * time.Millisecond),
			RequestEnd:    observedAt.Add(-25 * time.Millisecond),
			ResponseStart: observedAt.Add(-20 * time.Millisecond),
			ResponseEnd:   observedAt.Add(-5 * time.Millisecond),
			Terminal:      observedAt,
		},
		ObservedAt: observedAt,
	}
}
