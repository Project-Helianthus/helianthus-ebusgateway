package ebusgateway

import (
	"strings"
	"sync"
	"testing"
)

func TestTransportRuntimeMetricsRenderDetachedFiniteSnapshot(t *testing.T) {
	store := NewBusObservabilityStore(DefaultConfig())
	store.SetTransportRuntimeStatus(TransportRuntimeStatus{
		Protocol: TransportRuntimeProtocolModbusTCP,
		State:    TransportRuntimeStateReady,
		Reason:   TransportRuntimeReasonNone,
		Outcome:  TransportRuntimeOutcomeAvailable,
	})
	store.SetTransportRuntimeStatus(TransportRuntimeStatus{
		Protocol: TransportRuntimeProtocolEEBus,
		State:    TransportRuntimeStateDegraded,
		Reason:   TransportRuntimeReasonStartupFailed,
		Outcome:  TransportRuntimeOutcomeUnavailable,
	})

	metrics := store.RenderPrometheus()
	for _, want := range []string{
		`helianthus_transport_runtime_status{outcome="available",protocol="modbus_tcp",reason="none",state="ready"} 1`,
		`helianthus_transport_runtime_status{outcome="unavailable",protocol="eebus",reason="startup_failed",state="degraded"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("missing detached runtime snapshot %q:\n%s", want, metrics)
		}
	}
	if got := strings.Count(metrics, "\nhelianthus_transport_runtime_status{"); got != 2 {
		t.Fatalf("runtime metric samples = %d; want fixed two-protocol budget", got)
	}
}

func TestTransportRuntimeMetricsRejectUnboundedLabels(t *testing.T) {
	store := NewBusObservabilityStore(DefaultConfig())
	store.SetTransportRuntimeStatus(TransportRuntimeStatus{
		Protocol: TransportRuntimeProtocolModbusTCP,
		State:    TransportRuntimeState("peer-192.0.2.1"),
		Reason:   "dial tcp://private.example:1502",
		Outcome:  TransportRuntimeOutcomeAvailable,
	})

	metrics := store.RenderPrometheus()
	if strings.Contains(metrics, "192.0.2.1") || strings.Contains(metrics, "private.example") {
		t.Fatalf("runtime metric leaked unbounded input:\n%s", metrics)
	}
	want := `helianthus_transport_runtime_status{outcome="unknown",protocol="modbus_tcp",reason="unknown",state="unknown"} 1`
	if !strings.Contains(metrics, want) {
		t.Fatalf("invalid runtime status did not fail closed to unknown:\n%s", metrics)
	}
}

func TestTransportRuntimeMetricsScrapeIsStableDuringLifecycleReplacement(t *testing.T) {
	store := NewBusObservabilityStore(DefaultConfig())
	statuses := []TransportRuntimeStatus{
		{Protocol: TransportRuntimeProtocolEEBus, State: TransportRuntimeStateStarting, Reason: TransportRuntimeReasonNone, Outcome: TransportRuntimeOutcomePending},
		{Protocol: TransportRuntimeProtocolEEBus, State: TransportRuntimeStateReady, Reason: TransportRuntimeReasonNone, Outcome: TransportRuntimeOutcomeAvailable},
		{Protocol: TransportRuntimeProtocolEEBus, State: TransportRuntimeStateRetired, Reason: TransportRuntimeReasonShutdown, Outcome: TransportRuntimeOutcomeUnavailable},
	}
	var writers sync.WaitGroup
	writers.Add(1)
	go func() {
		defer writers.Done()
		for index := 0; index < 1000; index++ {
			store.SetTransportRuntimeStatus(statuses[index%len(statuses)])
		}
	}()
	for index := 0; index < 1000; index++ {
		metrics := store.RenderPrometheus()
		if got := strings.Count(metrics, "\nhelianthus_transport_runtime_status{"); got != 2 {
			t.Fatalf("scrape %d emitted %d runtime samples; want exactly two", index, got)
		}
	}
	writers.Wait()
}

func TestTransportRuntimeMetricsRejectsStaleEEBusLifecycleRevision(t *testing.T) {
	store := NewBusObservabilityStore(DefaultConfig())
	store.SetTransportRuntimeStatus(TransportRuntimeStatus{Protocol: TransportRuntimeProtocolEEBus, State: TransportRuntimeStateReady, Reason: TransportRuntimeReasonNone, Outcome: TransportRuntimeOutcomeAvailable, Revision: 2})
	store.SetTransportRuntimeStatus(TransportRuntimeStatus{Protocol: TransportRuntimeProtocolEEBus, State: TransportRuntimeStateDegraded, Reason: TransportRuntimeReasonStartupFailed, Outcome: TransportRuntimeOutcomeUnavailable, Revision: 1})
	metrics := store.RenderPrometheus()
	if !strings.Contains(metrics, `protocol="eebus",reason="none",state="ready"`) || strings.Contains(metrics, `protocol="eebus",reason="startup_failed",state="degraded"`) {
		t.Fatalf("stale eeBUS revision overwrote ready snapshot:\n%s", metrics)
	}
}

func TestTransportRuntimeMetricsAcceptsNewerInvalidEEBusRevisionAsUnknown(t *testing.T) {
	store := NewBusObservabilityStore(DefaultConfig())
	store.SetTransportRuntimeStatus(TransportRuntimeStatus{Protocol: TransportRuntimeProtocolEEBus, State: TransportRuntimeStateReady, Reason: TransportRuntimeReasonNone, Outcome: TransportRuntimeOutcomeAvailable, Revision: 1})
	store.SetTransportRuntimeStatus(TransportRuntimeStatus{Protocol: TransportRuntimeProtocolEEBus, State: TransportRuntimeState("invalid"), Reason: "invalid", Outcome: TransportRuntimeOutcomeAvailable, Revision: 2})
	metrics := store.RenderPrometheus()
	if !strings.Contains(metrics, `protocol="eebus",reason="unknown",state="unknown"`) || strings.Contains(metrics, `protocol="eebus",reason="none",state="ready"`) {
		t.Fatalf("newer invalid eeBUS status did not replace old ready state:\n%s", metrics)
	}
}
