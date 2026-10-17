package ebusgateway

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgo/transport"
)

// Embedding the unimplemented I/O interface makes any accidental device access
// fail immediately. Only the transport-owned snapshot is available to callers.
type m37CounterTransport struct {
	transport.RawTransport
	snapshot transport.BaudObservabilitySnapshot
}

func (source *m37CounterTransport) BaudObservabilitySnapshot() transport.BaudObservabilitySnapshot {
	return source.snapshot
}

var _ transport.BaudObservability = (*m37CounterTransport)(nil)

func m37AssertUnavailable(t *testing.T, store *BusObservabilityStore) string {
	t.Helper()
	metrics := store.RenderPrometheus()
	var baudLines []string
	for _, line := range strings.Split(metrics, "\n") {
		if strings.Contains(line, "ebus_baud_") {
			baudLines = append(baudLines, line)
		}
	}
	want := "# HELP ebus_baud_observation_available Whether a current qualified native baud timing observation is available.\n" +
		"# TYPE ebus_baud_observation_available gauge\n" +
		"ebus_baud_observation_available 0"
	if got := strings.Join(baudLines, "\n"); got != want {
		t.Fatalf("baud exposition =\n%s\nwant exactly:\n%s", got, want)
	}
	return metrics
}

func TestM37BaudObservationUnavailableWithoutTimingOwner(t *testing.T) {
	t.Run("absent counters", func(t *testing.T) {
		m37AssertUnavailable(t, NewBusObservabilityStore(DefaultConfig()))
	})
	t.Run("unsupported counters", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Transport = &struct{ transport.RawTransport }{}
		m37AssertUnavailable(t, NewBusObservabilityStore(cfg))
	})
	t.Run("absent collector", func(t *testing.T) {
		var store *BusObservabilityStore
		if got := store.RenderPrometheus(); got != "" {
			t.Fatalf("absent collector rendered %q", got)
		}
	})
}

func TestM37TransportCounterFieldsCannotEstablishMeasuredBaud(t *testing.T) {
	cases := []struct {
		name     string
		snapshot transport.BaudObservabilitySnapshot
	}{
		{"zero counters", transport.BaudObservabilitySnapshot{}},
		{"ConfiguredBaud", transport.BaudObservabilitySnapshot{ConfiguredBaud: 1234}},
		{"ConfiguredBaudKnown", transport.BaudObservabilitySnapshot{ConfiguredBaudKnown: true}},
		{"configured baud with known flag", transport.BaudObservabilitySnapshot{ConfiguredBaud: 4321, ConfiguredBaudKnown: true}},
		{"ReceivedSymbolsTotal", transport.BaudObservabilitySnapshot{ReceivedSymbolsTotal: 101}},
		{"ENHFramingErrorsTotal", transport.BaudObservabilitySnapshot{ENHFramingErrorsTotal: 103}},
		{"ENHParserResetsTotal", transport.BaudObservabilitySnapshot{ENHParserResetsTotal: 107}},
		{"AdapterResetResponsesTotal", transport.BaudObservabilitySnapshot{AdapterResetResponsesTotal: 109}},
		{"EscapeDecodeFaultsTotal", transport.BaudObservabilitySnapshot{EscapeDecodeFaultsTotal: 113}},
		{"EscapeRecoveriesTotal", transport.BaudObservabilitySnapshot{EscapeRecoveriesTotal: 127}},
		{"EscapeBudgetExhaustedTotal", transport.BaudObservabilitySnapshot{EscapeBudgetExhaustedTotal: 131}},
		{"EscapePendingTimeoutsTotal", transport.BaudObservabilitySnapshot{EscapePendingTimeoutsTotal: 137}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := &m37CounterTransport{snapshot: tc.snapshot}
			cfg := DefaultConfig()
			cfg.Transport = source
			store := NewBusObservabilityStore(cfg)
			for range 2 {
				m37AssertUnavailable(t, store)
			}
			if got := source.BaudObservabilitySnapshot(); got != tc.snapshot {
				t.Fatalf("scrapes mutated transport counters: got %+v, want %+v", got, tc.snapshot)
			}
		})
	}
}

func TestM37BaudObservationReconnectResetAndTraffic(t *testing.T) {
	source := &m37CounterTransport{snapshot: transport.BaudObservabilitySnapshot{
		ConfiguredBaud: 4321, ConfiguredBaudKnown: true,
		ReceivedSymbolsTotal: 101, ENHFramingErrorsTotal: 103,
		ENHParserResetsTotal: 107, AdapterResetResponsesTotal: 109,
		EscapeDecodeFaultsTotal: 113, EscapeRecoveriesTotal: 127,
		EscapeBudgetExhaustedTotal: 131, EscapePendingTimeoutsTotal: 137,
	}}
	cfg := DefaultConfig()
	cfg.Transport = source
	store := NewBusObservabilityStore(cfg)
	now := time.Unix(0, 0).UTC()
	store.now = func() time.Time { return now }
	reconstructor := newPassiveTransactionReconstructorCore(cfg)
	reconstructor.tap = &PassiveBusTap{transport: source}
	store.reconstructor = reconstructor
	initial := source.BaudObservabilitySnapshot()

	stages := []struct {
		name   string
		status PassiveTapStatus
	}{
		{"startup", PassiveTapStatus{}},
		{"connected", PassiveTapStatus{Connected: true, ConnectAttemptCount: 1, ConnectCount: 1}},
		{"disconnected", PassiveTapStatus{ConnectAttemptCount: 3, ConnectCount: 1, ConnectFailureCount: 2, DisconnectCount: 1}},
		{"reconnected", PassiveTapStatus{Connected: true, ConnectAttemptCount: 4, ConnectCount: 2, ConnectFailureCount: 2, DisconnectCount: 1}},
		{"adapter reset", PassiveTapStatus{Connected: true, ConnectAttemptCount: 4, ConnectCount: 2, ConnectFailureCount: 2, DisconnectCount: 1, ResetCount: 1}},
	}
	for _, stage := range stages {
		t.Run(stage.name, func(t *testing.T) {
			reconstructor.tap.status = stage.status
			for range 2 {
				metrics := m37AssertUnavailable(t, store)
				for name, value := range map[string]uint64{
					"ebus_passive_tap_reconnect_attempts_total":  stage.status.ConnectAttemptCount,
					"ebus_passive_tap_reconnect_successes_total": stage.status.ConnectCount,
					"ebus_passive_tap_reconnect_failures_total":  stage.status.ConnectFailureCount,
				} {
					want := fmt.Sprintf("\n%s %d\n", name, value)
					if !strings.Contains(metrics, want) {
						t.Fatalf("missing exact lifecycle sample %q", want)
					}
				}
			}
			if got := source.BaudObservabilitySnapshot(); got != initial {
				t.Fatalf("lifecycle/scrape changed cumulative transport counters: got %+v, want %+v", got, initial)
			}
		})
	}

	// Application frames and retained busy timing cannot qualify physical baud.
	store.OnPassiveClassifiedEvent(observabilityPassiveTransactionEvent(now, 0x10, 0x08, 0xB5, 0x24))
	now = now.Add(time.Second)
	source.snapshot.ReceivedSymbolsTotal += 29
	source.snapshot.ENHParserResetsTotal++
	source.snapshot.AdapterResetResponsesTotal++
	afterTraffic := source.BaudObservabilitySnapshot()
	first := m37AssertUnavailable(t, store)
	second := m37AssertUnavailable(t, store)
	if first != second {
		t.Fatal("repeated scrapes changed metrics without a source update")
	}
	if got := source.BaudObservabilitySnapshot(); got != afterTraffic {
		t.Fatalf("application traffic/scrapes double counted events: got %+v, want %+v", got, afterTraffic)
	}
	// A fresh transport/store starts a new counter epoch, not a baud estimate.
	cfg.Transport = &m37CounterTransport{}
	m37AssertUnavailable(t, NewBusObservabilityStore(cfg))
}
