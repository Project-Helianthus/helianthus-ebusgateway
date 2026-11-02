package ebusgateway

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// This matrix deliberately includes every byte value, even addresses that a
// physical bus would reject. It bounds the metric ingestion API, not admission.
func TestQ04FrameBytePairDomain(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ObserveFirstSeriesBudget = 1
	store := NewBusObservabilityStore(cfg)
	const wantSeries = 256 * 256
	for repetition := 1; repetition <= 3; repetition++ {
		for src := 0; src < 256; src++ {
			for dst := 0; dst < 256; dst++ {
				if err := store.OnBusEvent(protocol.BusEvent{
					Kind: protocol.BusEventAttemptComplete, HasRequest: true,
					Request: protocol.Frame{Source: byte(src), Target: byte(dst), Primary: 0xB5, Secondary: 0x24,
						Data: []byte{byte(repetition), byte(src), byte(dst)}},
				}); err != nil {
					t.Fatal(err)
				}
			}
		}
		metrics := store.RenderPrometheus()
		// The exact label set and closed vocabularies exclude payload labels.
		row := regexp.MustCompile(`^ebus_frames_observed_total\{dst="(0x[0-9a-f]{2})",family="B524",frame_type="(broadcast|initiator_initiator|initiator_target|local_participant_inbound|abandoned_partial)",scope="active",src="(0x[0-9a-f]{2})"\} ([0-9]+)$`)
		pairs := make(map[string]bool, wantSeries)
		for _, line := range strings.Split(metrics, "\n") {
			if !strings.HasPrefix(line, "ebus_frames_observed_total{") {
				continue
			}
			match := row.FindStringSubmatch(line)
			if match == nil {
				t.Fatalf("unexpected frame labels: %s", line)
			}
			pair := match[3] + "/" + match[1]
			if pairs[pair] {
				t.Fatalf("duplicate byte pair %s", pair)
			}
			pairs[pair] = true
			if match[4] != strconv.Itoa(repetition) {
				t.Fatalf("pair %s count = %s; want %d", pair, match[4], repetition)
			}
		}
		if len(pairs) != wantSeries || len(store.frames) != wantSeries {
			t.Fatalf("rendered/stored series = %d/%d; want %d each", len(pairs), len(store.frames), wantSeries)
		}
		for src := 0; src < 256; src++ {
			for dst := 0; dst < 256; dst++ {
				if !pairs[fmt.Sprintf("0x%02x/0x%02x", src, dst)] {
					t.Fatalf("missing pair %02x/%02x", src, dst)
				}
			}
		}
		// Only new frame identities cross this budget; repeats must not overflow.
		q04WantOverflow(t, metrics, wantSeries-1)
	}
}

func TestQ04FrameDiagnosticThresholds(t *testing.T) {
	for _, tc := range []struct {
		name           string
		budget, before int
	}{
		{"configured_budget", 1, 1},
		// One event retains both a frame and a frameBytes series, so the
		// next frame meets the configured budget exactly before insertion.
		{"configured_budget_equality", 2, 1},
		{"named_hard_cap", 1000000, 2048},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.ObserveFirstSeriesBudget = tc.budget
			store := NewBusObservabilityStore(cfg)
			for i := 0; i <= tc.before; i++ {
				if i == tc.before {
					q04WantOverflow(t, store.RenderPrometheus(), 0)
				}
				if err := store.OnBusEvent(protocol.BusEvent{
					Kind: protocol.BusEventAttemptComplete, HasRequest: true,
					Request: protocol.Frame{Source: byte(i / 256), Target: byte(i % 256), Primary: 0xB5, Secondary: 0x24},
				}); err != nil {
					t.Fatal(err)
				}
			}
			if len(store.frames) != tc.before+1 {
				t.Fatalf("threshold truncated map: got %d; want %d", len(store.frames), tc.before+1)
			}
			q04WantOverflow(t, store.RenderPrometheus(), 1)
		})
	}
}

func q04WantOverflow(t *testing.T, metrics string, want int) {
	t.Helper()
	line := fmt.Sprintf("ebus_observability_series_budget_overflow_total %d", want)
	if !strings.Contains("\n"+metrics, "\n"+line+"\n") {
		t.Fatalf("missing exact overflow sample %q", line)
	}
}

type q04LocalSnapshotter struct{}

func (q04LocalSnapshotter) LocalAddressSnapshot() LocalAddressSnapshot {
	return LocalAddressSnapshot{Known: true, Address: 0x10}
}

func TestQ04FrameOtherLabelClasses(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LocalAddressSnapshotter = q04LocalSnapshotter{}
	store := NewBusObservabilityStore(cfg)
	for _, tc := range []struct {
		primary, secondary, target byte
		frameType                  string
	}{
		{0xB5, 0x09, 0xFE, "broadcast"},
		{0xB5, 0x16, 0x03, "initiator_initiator"},
		{0xB5, 0x55, 0x08, "initiator_target"},
		{0x00, 0x00, 0x10, "local_participant_inbound"},
		{0xFF, 0xFF, 0x08, "initiator_target"},
	} {
		for repetition := 1; repetition <= 2; repetition++ {
			request := protocol.Frame{Source: 0x31, Target: tc.target, Primary: tc.primary, Secondary: tc.secondary, Data: []byte{byte(repetition)}}
			if err := store.OnBusEvent(protocol.BusEvent{Kind: protocol.BusEventAttemptComplete, HasRequest: true, Request: request}); err != nil {
				t.Fatal(err)
			}
			store.OnPassiveClassifiedEvent(PassiveClassifiedEvent{Kind: PassiveClassifiedEventTransaction, HasRequest: true, Request: request})
		}
		for _, scope := range []string{"active", "passive"} {
			want := fmt.Sprintf(`ebus_frames_observed_total{dst="0x%02x",family="%02X%02X",frame_type="%s",scope="%s",src="0x31"} 2`, tc.target, tc.primary, tc.secondary, tc.frameType, scope)
			if !strings.Contains(store.RenderPrometheus(), want+"\n") {
				t.Fatalf("missing representative sample %s", want)
			}
		}
	}
	for _, reason := range []PassiveAbandonReason{PassiveAbandonReasonNoProgress, PassiveAbandonReasonScanTimeout} {
		store.OnPassiveClassifiedEvent(PassiveClassifiedEvent{Kind: PassiveClassifiedEventAbandonedTransaction, HasRequest: true,
			Request: protocol.Frame{Source: 0x31, Target: 0x08, Primary: 0xB5, Secondary: 0x24}, AbandonReason: reason})
	}
	for _, dst := range []string{"0x08", "scan"} {
		want := fmt.Sprintf(`ebus_frames_observed_total{dst="%s",family="B524",frame_type="abandoned_partial",scope="passive",src="0x31"} 1`, dst)
		if !strings.Contains(store.RenderPrometheus(), want+"\n") {
			t.Fatalf("missing abandoned class %s", want)
		}
	}
	if len(store.frames) != 12 {
		t.Fatalf("representative series = %d; want 12", len(store.frames))
	}
	// The four special cases are aliases of their command bytes, not extra
	// families. Exhaust this cheap classifier domain without retaining series.
	for command := 0; command < 256*256; command++ {
		frame := protocol.Frame{Primary: byte(command / 256), Secondary: byte(command % 256)}
		if got, want := classifyFamily(frame), fmt.Sprintf("%04X", command); got != want {
			t.Fatalf("family = %q; want %q", got, want)
		}
	}
}
