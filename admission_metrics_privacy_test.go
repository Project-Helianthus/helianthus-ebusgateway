package ebusgateway

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

const k02SecretMarker = "K02_SYNTHETIC_SECRET"

// k02ExpositionShape compares series identities and metadata, ignoring values.
// It inspects the real renderer's output rather than recreating its logic.
func k02ExpositionShape(t *testing.T, output string) map[string]bool {
	t.Helper()
	if strings.Contains(output, k02SecretMarker) {
		t.Fatal("synthetic secret leaked into Prometheus exposition")
	}
	shape := make(map[string]bool)
	for _, line := range strings.Split(output, "\n") {
		if line == "" {
			continue
		}
		key := line
		if !strings.HasPrefix(line, "#") {
			index := strings.LastIndexByte(line, ' ')
			if index < 0 {
				t.Fatalf("sample without value: %q", line)
			}
			key = line[:index]
		}
		if shape[key] {
			t.Fatalf("duplicate exposition entry: %q", key)
		}
		shape[key] = true
	}
	return shape
}

func TestK02AdmissionErrorIdentityAmplification(t *testing.T) {
	store := NewBusObservabilityStore(DefaultConfig())
	kinds := []protocol.BusEventKind{
		protocol.BusEventTimeout, protocol.BusEventNACK,
		protocol.BusEventCRCMismatch, protocol.BusEventEchoMismatch,
	}
	var baseline map[string]bool
	for i := 0; i < 512; i++ {
		identity := fmt.Sprintf("%s_identity_%d", k02SecretMarker, i)
		store.RecordBusAdmissionTransition("degraded", byte(i), byte(i+1), identity)
		for _, kind := range kinds {
			if err := store.OnBusEvent(protocol.BusEvent{
				Kind: kind, Initiator: byte(i),
			}); err != nil {
				t.Fatalf("record synthetic error: %v", err)
			}
		}
		output := store.RenderPrometheus()
		shape := k02ExpositionShape(t, output)
		if i == 0 {
			baseline = shape
		} else if !reflect.DeepEqual(shape, baseline) {
			t.Fatalf("input %d changed metric names, label sets, series count, or HELP/TYPE text", i)
		}
		// Existing active error vocabulary has one class/phase tuple per kind.
		for _, labels := range []string{
			`class="timeout",phase="terminal",scope="active"`,
			`class="nack",phase="ack",scope="active"`,
			`class="crc_mismatch",phase="response",scope="active"`,
			`class="echo_mismatch",phase="request",scope="active"`,
		} {
			want := fmt.Sprintf("ebus_errors_total{%s} %d\n", labels, i+1)
			if !strings.Contains(output, want) {
				t.Fatalf("missing cumulative error sample %q", want)
			}
		}
		if count := strings.Count(output, "\nebus_errors_total{"); count != len(kinds) {
			t.Fatalf("error series count = %d; want %d fixed tuples", count, len(kinds))
		}
	}
}

func TestK02RuntimeLabelDimensions(t *testing.T) {
	// Fixed dimensions documented in docs/transport-prometheus-runtime-v1.md.
	protocols := []string{"modbus_tcp", "eebus"}
	states := []string{"disabled", "starting", "ready", "degraded", "retired", "unknown"}
	reasons := []string{"not_configured", "none", "startup_failed", "shutdown", "unknown"}
	outcomes := []string{"unavailable", "pending", "available", "unknown"}
	store := NewBusObservabilityStore(DefaultConfig())
	const metric = "helianthus_transport_runtime_status"
	seen := make(map[string]bool)
	for _, p := range protocols {
		for _, state := range states {
			for _, reason := range reasons {
				for _, outcome := range outcomes {
					store.SetTransportRuntimeStatus(TransportRuntimeStatus{
						Protocol: TransportRuntimeProtocol(p), State: TransportRuntimeState(state),
						Reason: reason, Outcome: TransportRuntimeOutcome(outcome),
					})
					output := store.RenderPrometheus()
					shape := k02ExpositionShape(t, output)
					key := fmt.Sprintf(`%s{outcome=%q,protocol=%q,reason=%q,state=%q}`, metric, outcome, p, reason, state)
					if !shape[key] || !strings.Contains(output, key+" 1\n") {
						t.Fatalf("documented runtime tuple missing: %s", key)
					}
					if count := strings.Count(output, "\n"+metric+"{"); count != len(protocols) {
						t.Fatalf("runtime series per scrape = %d; want %d", count, len(protocols))
					}
					seen[key] = true
				}
			}
		}
	}
	if want := len(protocols) * len(states) * len(reasons) * len(outcomes); len(seen) != want {
		t.Fatalf("documented runtime dimension product = %d; want %d", len(seen), want)
	}

	for _, p := range protocols {
		for _, field := range []string{"state", "reason", "outcome", "protocol"} {
			t.Run(p+"/"+field, func(t *testing.T) {
				store := NewBusObservabilityStore(DefaultConfig())
				store.SetTransportRuntimeStatus(TransportRuntimeStatus{
					Protocol: TransportRuntimeProtocol(p), State: TransportRuntimeStateReady,
					Reason: TransportRuntimeReasonNone, Outcome: TransportRuntimeOutcomeAvailable,
				})
				before := k02ExpositionShape(t, store.RenderPrometheus())
				var baseline map[string]bool
				for i := 0; i < 512; i++ {
					identity := fmt.Sprintf("%s_%s_%d", k02SecretMarker, field, i)
					status := TransportRuntimeStatus{
						Protocol: TransportRuntimeProtocol(p), State: TransportRuntimeStateReady,
						Reason: TransportRuntimeReasonNone, Outcome: TransportRuntimeOutcomeAvailable,
						Revision: uint64(i + 1),
					}
					switch field {
					case "state":
						status.State = TransportRuntimeState(identity)
					case "reason":
						status.Reason = identity
					case "outcome":
						status.Outcome = TransportRuntimeOutcome(identity)
					case "protocol":
						status.Protocol = TransportRuntimeProtocol(identity)
					}
					store.SetTransportRuntimeStatus(status)
					shape := k02ExpositionShape(t, store.RenderPrometheus())
					if field == "protocol" && !reflect.DeepEqual(shape, before) {
						t.Fatal("unknown protocol changed the supported series")
					}
					if i == 0 {
						baseline = shape
					} else if !reflect.DeepEqual(shape, baseline) {
						t.Fatalf("input %d amplified label sets or changed metric metadata", i)
					}
					if field != "protocol" {
						key := fmt.Sprintf(`%s{outcome="unknown",protocol=%q,reason="unknown",state="unknown"}`, metric, p)
						if !shape[key] {
							t.Fatal("invalid runtime label did not fail closed to the unknown tuple")
						}
					}
				}
			})
		}
	}
}

func TestM54AbsoluteMetricSeriesMatrix(t *testing.T) {
	store := NewBusObservabilityStore(DefaultConfig())
	errors := []struct {
		kind   protocol.BusEventKind
		labels string
	}{
		{protocol.BusEventTimeout, `class="timeout",phase="terminal",scope="active"`},
		{protocol.BusEventNACK, `class="nack",phase="ack",scope="active"`},
		{protocol.BusEventCRCMismatch, `class="crc_mismatch",phase="response",scope="active"`},
		{protocol.BusEventEchoMismatch, `class="echo_mismatch",phase="request",scope="active"`},
	}
	// Public emitted subclasses, independent of the classifier under test.
	// The bit_flip fallback has no reachable byte in the current address table.
	subclasses := []struct {
		name     string
		perSweep int
	}{
		{"pre_echo_syn_raw", 1},
		{"pre_echo_syn_escaped_data", 1},
		{"post_grant_collision_initiator", 1},
		{"post_grant_collision_target", 2},
		{"post_grant_ack", 1},
		{"post_grant_nack", 1},
		{"post_grant_reserved", 1},
	}
	echoes := []struct {
		value   byte
		escaped bool
	}{
		{protocol.SymbolSyn, false}, {protocol.SymbolSyn, true},
		{0xF1, false}, {0x15, false}, {protocol.AddressBroadcast, false},
		{protocol.SymbolAck, false}, {protocol.SymbolNack, false},
		{protocol.SymbolEscape, false},
	}
	// Count only the K02 contract families so unrelated semantic collectors
	// cannot change this absolute bound. Runtime exposes one row per protocol,
	// rather than the Cartesian product of its state/reason/outcome vocabulary.
	const runtimeMetric = "helianthus_transport_runtime_status"
	protocols := []string{"modbus_tcp", "eebus"}
	wantSeries := len(errors) + len(subclasses) + len(protocols)
	var baseline map[string]bool
	for i := 0; i < 512; i++ {
		identity := fmt.Sprintf("%s_request_%d", k02SecretMarker, i)
		store.RecordBusAdmissionTransition("degraded", byte(i), byte(i+1), identity)
		request := protocol.Frame{
			Source: byte(i), Target: byte(i + 1),
			Primary: byte(i + 2), Secondary: byte(i + 3), Data: []byte(identity),
		}
		// Every request sees every echo case; event.Byte is never derived
		// from the independently varied request or admission identity.
		for _, echo := range echoes {
			for _, item := range errors {
				if err := store.OnBusEvent(protocol.BusEvent{
					Kind: item.kind, Byte: echo.value, EchoWasEscaped: echo.escaped,
					Initiator: byte(i), Request: request, HasRequest: i%2 == 0,
				}); err != nil {
					t.Fatalf("record synthetic event: %v", err)
				}
			}
		}
		for _, p := range protocols {
			store.SetTransportRuntimeStatus(TransportRuntimeStatus{
				Protocol: TransportRuntimeProtocol(p), State: TransportRuntimeState(identity),
				Reason: identity, Outcome: TransportRuntimeOutcome(identity), Revision: uint64(i + 1),
			})
		}
		output := store.RenderPrometheus()
		shape := make(map[string]bool)
		for key := range k02ExpositionShape(t, output) {
			if strings.HasPrefix(key, "ebus_errors_total{") ||
				strings.HasPrefix(key, "ebus_active_echo_mismatch_subclass_total{") ||
				strings.HasPrefix(key, runtimeMetric+"{") {
				shape[key] = true
			}
		}
		if len(shape) != wantSeries {
			t.Fatalf("input %d: contract series = %d; want %d error + %d subclass + %d runtime = %d",
				i, len(shape), len(errors), len(subclasses), len(protocols), wantSeries)
		}
		if i == 0 {
			baseline = shape
		} else if !reflect.DeepEqual(shape, baseline) {
			t.Fatalf("input %d changed bounded public label tuples", i)
		}
		for _, item := range errors {
			want := fmt.Sprintf("ebus_errors_total{%s} %d\n", item.labels, (i+1)*len(echoes))
			if !strings.Contains(output, want) {
				t.Fatalf("missing cumulative error sample %q", want)
			}
		}
		for _, subclass := range subclasses {
			want := fmt.Sprintf("ebus_active_echo_mismatch_subclass_total{subclass=%q} %d\n",
				subclass.name, (i+1)*subclass.perSweep)
			if !strings.Contains(output, want) {
				t.Fatalf("missing emitted subclass sample %q", want)
			}
		}
		for _, p := range protocols {
			want := fmt.Sprintf(`%s{outcome="unknown",protocol=%q,reason="unknown",state="unknown"} 1`+"\n", runtimeMetric, p)
			if !strings.Contains(output, want) {
				t.Fatalf("missing fail-closed runtime sample %q", want)
			}
		}
	}
}
