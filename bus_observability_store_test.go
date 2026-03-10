package ebusgateway

import (
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

func TestBusObservabilityStoreRecentRingEvictsOldestButCountersAdvance(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ObserveFirstRecentMessageCapacity = 2
	store := NewBusObservabilityStore(cfg)
	base := time.Now().UTC()

	request := protocol.Frame{
		Source:    0x10,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x24,
		Data:      []byte{0x01},
	}
	response := protocol.Frame{
		Source:    0x08,
		Target:    0x10,
		Primary:   0xB5,
		Secondary: 0x24,
		Data:      []byte{0x02},
	}

	for index, observedAt := range []time.Time{
		base.Add(10 * time.Second),
		base.Add(20 * time.Second),
		base.Add(30 * time.Second),
	} {
		now := observedAt
		store.now = func() time.Time { return now }
		if err := store.OnBusEvent(protocol.BusEvent{
			Kind:        protocol.BusEventAttemptComplete,
			Request:     request,
			HasRequest:  true,
			Response:    response,
			HasResponse: true,
		}); err != nil {
			t.Fatalf("OnBusEvent[%d] error = %v", index, err)
		}
	}

	messages := store.RecentMessages(10)
	if len(messages) != 2 {
		t.Fatalf("RecentMessages length = %d; want 2", len(messages))
	}
	if !messages[0].ObservedAt.Equal(base.Add(20 * time.Second)) {
		t.Fatalf("RecentMessages[0].ObservedAt = %s; want 20s entry", messages[0].ObservedAt)
	}
	if !messages[1].ObservedAt.Equal(base.Add(30 * time.Second)) {
		t.Fatalf("RecentMessages[1].ObservedAt = %s; want 30s entry", messages[1].ObservedAt)
	}

	metrics := store.RenderPrometheus()
	if !strings.Contains(metrics, `ebus_frames_observed_total{dst="0x08",family="B524",frame_type="master_target",scope="active",src="0x10"} 3`) {
		t.Fatalf("RenderPrometheus missing cumulative frame counter:\n%s", metrics)
	}
	if !strings.Contains(metrics, "ebus_observability_recent_messages 2") {
		t.Fatalf("RenderPrometheus missing recent-message occupancy:\n%s", metrics)
	}
}

func TestBusObservabilityStorePeriodicityBudgetEvictsLRU(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BroadcastListen = true
	cfg.ObserveFirstPeriodicityCapacity = 1
	store := NewBusObservabilityStore(cfg)
	base := time.Now().UTC()
	store.now = func() time.Time { return base.Add(30 * time.Second) }

	store.mu.Lock()
	store.passive.state = "available"
	store.passive.startupWindowClosed = true
	store.mu.Unlock()

	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base.Add(10*time.Second), 0x10, 0x08, 0xB5, 0x24))
	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base.Add(20*time.Second), 0x10, 0x08, 0xB5, 0x24))
	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base.Add(30*time.Second), 0x15, 0x26, 0xB5, 0x16))

	items := store.PeriodicitySnapshot()
	if len(items) != 1 {
		t.Fatalf("PeriodicitySnapshot length = %d; want 1", len(items))
	}
	if items[0].SourceBucket != "0x15" || items[0].TargetBucket != "0x26" {
		t.Fatalf("PeriodicitySnapshot[0] = %+v; want surviving tuple 0x15->0x26", items[0])
	}

	metrics := store.RenderPrometheus()
	if !strings.Contains(metrics, "ebus_periodicity_tuple_budget_overflow_total 1") {
		t.Fatalf("RenderPrometheus missing periodicity overflow counter:\n%s", metrics)
	}
}

func TestBusObservabilityStoreSuppressesBusyMetricsOnEbusdTCP(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TransportConfig.Protocol = TransportEbusdTCP
	store := NewBusObservabilityStore(cfg)

	metrics := store.RenderPrometheus()
	if strings.Contains(metrics, "ebus_bus_busy_seconds_total") {
		t.Fatalf("RenderPrometheus unexpectedly exposed busy metrics for ebusd-tcp:\n%s", metrics)
	}
	if !strings.Contains(metrics, `ebus_observability_transport_info{scope="passive",timing_quality="unavailable",transport_class="ebusd-tcp"} 1`) {
		t.Fatalf("RenderPrometheus missing passive transport metadata:\n%s", metrics)
	}
	if !strings.Contains(metrics, `ebus_passive_warmup_state{state="unavailable"} 1`) {
		t.Fatalf("RenderPrometheus missing unavailable passive warmup state:\n%s", metrics)
	}
}

func TestBusObservabilityStoreExportsBusyMetricsWhenPassiveAvailable(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BroadcastListen = true
	store := NewBusObservabilityStore(cfg)
	base := time.Now().UTC()
	store.now = func() time.Time { return base }

	store.mu.Lock()
	store.passive.state = "available"
	store.passive.startupWindowClosed = true
	store.mu.Unlock()

	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(base, 0x10, 0x08, 0xB5, 0x24))

	metrics := store.RenderPrometheus()
	if !strings.Contains(metrics, "ebus_bus_busy_seconds_total 0.05") {
		t.Fatalf("RenderPrometheus missing busy total for available passive state:\n%s", metrics)
	}
	if !strings.Contains(metrics, `ebus_bus_busy_ratio{window="1m"} 0.0008333333333333334`) {
		t.Fatalf("RenderPrometheus missing 1m busy ratio:\n%s", metrics)
	}
}

func observabilityPassiveTransactionEvent(observedAt time.Time, source, target, primary, secondary byte) PassiveClassifiedEvent {
	return PassiveClassifiedEvent{
		Kind:      PassiveClassifiedEventTransaction,
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
		Timing: PassiveTimingMarkers{
			RequestStart:  observedAt.Add(-50 * time.Millisecond),
			RequestEnd:    observedAt.Add(-25 * time.Millisecond),
			ResponseStart: observedAt.Add(-20 * time.Millisecond),
			ResponseEnd:   observedAt.Add(-5 * time.Millisecond),
			Terminal:      observedAt,
		},
		ObservedAt: observedAt,
	}
}
