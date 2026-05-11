package ebusgateway

import (
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgo/protocol"
)

// TestActiveError_WithRequest_BumpsFramesObservedAndErrorsTotal pins the
// Option A alignment: an active-path failure that carries a request now
// increments BOTH `ebus_frames_observed_total{scope="active",…}` AND
// `ebus_errors_total{scope="active",…}`, matching the long-standing
// passive-path semantic in recordPassiveAbandonedLocked.
//
// Before Option A: active frames_observed only counted successes, so
// `rate(frames_observed{scope="active"})` silently underreported total
// attempts and `1 - errors/observed` was not a valid success ratio.
// After Option A: both scopes share the contract
//
//	frames_observed = attempts (success + failure)
//	errors_total    = subset of failures, by class
//	success ratio   = 1 - errors/observed
func TestActiveError_WithRequest_BumpsFramesObservedAndErrorsTotal(t *testing.T) {
	t.Parallel()

	store := NewBusObservabilityStore(DefaultConfig())

	request := protocol.Frame{
		Source:    0x10,
		Target:    0x08,
		Primary:   0xB5,
		Secondary: 0x24,
		Data:      []byte{0x01, 0x02, 0x03},
	}

	if err := store.OnBusEvent(protocol.BusEvent{
		Kind:       protocol.BusEventTimeout,
		Outcome:    protocol.BusOutcomeTimeout,
		Request:    request,
		HasRequest: true,
	}); err != nil {
		t.Fatalf("OnBusEvent error = %v", err)
	}

	metrics := store.RenderPrometheus()

	wantError := `ebus_errors_total{class="timeout",phase="terminal",scope="active"} 1`
	if !strings.Contains(metrics, wantError) {
		t.Errorf("missing error counter line %q\nmetrics:\n%s", wantError, metrics)
	}

	wantFrame := `ebus_frames_observed_total{dst="0x08",family="B524",frame_type="initiator_target",scope="active",src="0x10"} 1`
	if !strings.Contains(metrics, wantFrame) {
		t.Errorf("missing aligned frame counter line %q (Option A regression — active error must bump frames_observed)\nmetrics:\n%s", wantFrame, metrics)
	}

	// Request bytes must also book into frame_bytes for the active
	// scope so byte-rate dashboards stay coherent. Request wire len
	// for the frame above is request bytes through CRC; we only
	// assert the line exists with a non-zero value rather than a
	// magic number, since wire-length math lives in frameWireLen
	// and is exercised by its own tests.
	if !strings.Contains(metrics, `ebus_frame_bytes_total{family="B524",frame_type="initiator_target",part="request",scope="active"}`) {
		t.Errorf("missing frame_bytes line for active-error request bytes\nmetrics:\n%s", metrics)
	}
}

// TestActiveError_WithoutRequest_NoFrameIncrement pins the safety
// boundary: a failure event with no request context (e.g. an orphan
// echo_mismatch where the gateway didn't have an in-flight active
// transaction) MUST still pump errors_total but MUST NOT create a
// frames_observed bucket. We don't have src/dst/family/frame_type
// labels to populate, and inventing an "unknown" bucket would inflate
// cardinality without operator value.
func TestActiveError_WithoutRequest_NoFrameIncrement(t *testing.T) {
	t.Parallel()

	store := NewBusObservabilityStore(DefaultConfig())

	if err := store.OnBusEvent(protocol.BusEvent{
		Kind:    protocol.BusEventTimeout,
		Outcome: protocol.BusOutcomeTimeout,
		// HasRequest deliberately false / zero.
	}); err != nil {
		t.Fatalf("OnBusEvent error = %v", err)
	}

	metrics := store.RenderPrometheus()

	wantError := `ebus_errors_total{class="timeout",phase="terminal",scope="active"} 1`
	if !strings.Contains(metrics, wantError) {
		t.Errorf("missing error counter line %q\nmetrics:\n%s", wantError, metrics)
	}

	for _, line := range strings.Split(metrics, "\n") {
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}
		if strings.HasPrefix(line, "ebus_frames_observed_total") {
			t.Errorf("frames_observed bumped for request-less active error — should be skipped\nline: %q\nfull:\n%s", line, metrics)
		}
		if strings.HasPrefix(line, "ebus_frame_bytes_total") {
			t.Errorf("frame_bytes bumped for request-less active error — should be skipped\nline: %q\nfull:\n%s", line, metrics)
		}
	}
}

// TestActiveError_EachClassBumpsFramesObserved exercises every active
// error class — timeout, nack, crc_mismatch, echo_mismatch — and
// verifies the aligned counting fires uniformly. Locks the invariant
// that the frame-increment block doesn't accidentally regress for a
// specific class (e.g. via early-return on echo_mismatch).
func TestActiveError_EachClassBumpsFramesObserved(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		kind      protocol.BusEventKind
		outcome   protocol.BusOutcomeClass
		wantClass string
	}{
		{"timeout", protocol.BusEventTimeout, protocol.BusOutcomeTimeout, "timeout"},
		{"nack", protocol.BusEventNACK, protocol.BusOutcomeNACK, "nack"},
		{"crc_mismatch", protocol.BusEventCRCMismatch, protocol.BusOutcomeCRCMismatch, "crc_mismatch"},
		{"echo_mismatch", protocol.BusEventEchoMismatch, protocol.BusOutcomeEchoMismatch, "echo_mismatch"},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			store := NewBusObservabilityStore(DefaultConfig())

			request := protocol.Frame{
				Source:    0x10,
				Target:    0x08,
				Primary:   0xB5,
				Secondary: 0x24,
				Data:      []byte{0x01},
			}

			if err := store.OnBusEvent(protocol.BusEvent{
				Kind:       c.kind,
				Outcome:    c.outcome,
				Request:    request,
				HasRequest: true,
				Byte:       0xAA, // arbitrary byte for echo_mismatch path; ignored elsewhere
			}); err != nil {
				t.Fatalf("OnBusEvent(%s) error = %v", c.name, err)
			}

			metrics := store.RenderPrometheus()

			if !strings.Contains(metrics, `class="`+c.wantClass+`"`) || !strings.Contains(metrics, `scope="active"`) {
				t.Errorf("missing errors_total{class=%q,scope=active} 1 for kind %s\nmetrics:\n%s", c.wantClass, c.name, metrics)
			}

			wantFrame := `ebus_frames_observed_total{dst="0x08",family="B524",frame_type="initiator_target",scope="active",src="0x10"} 1`
			if !strings.Contains(metrics, wantFrame) {
				t.Errorf("class %s: missing aligned frame counter %q\nmetrics:\n%s", c.name, wantFrame, metrics)
			}
		})
	}
}
