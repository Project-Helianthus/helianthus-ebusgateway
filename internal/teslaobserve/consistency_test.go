package teslaobserve

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// Attempt positions in the prior fixture.
const (
	attemptCaptured = iota
	attemptSegmentBound
	attemptDeadline
	attemptReadError
	attemptOpenError
)

// priorFixture builds a genuine evidence file with one attempt of every
// termination a run can record, plus a failed open, by appending synthetic
// runs. Every append also proves that the unedited prior is accepted.
func priorFixture(t *testing.T) []byte {
	t.Helper()
	oneByte := make([]string, 9)
	offsets := make([]int, 9)
	for index := range oneByte {
		oneByte[index], offsets[index] = string("TESLA\x00PAS"[index]), index+1
	}
	steps := []struct {
		kind   string
		script []scriptedByte
		open   func() (ByteSource, error)
	}{
		{CaseInitial, segmentsAt(syntheticFragments, syntheticOffsets, 5), nil},
		{CaseRecovery, segmentsAt(oneByte, offsets, 0), nil},
		{CaseIdle, segmentsAt([]string{"TESLA\x00"}, []int{1}, 0), nil},
		{CaseInitial, nil, func() (ByteSource, error) { return failingSource{}, nil }},
		{CaseIdle, nil, func() (ByteSource, error) { return nil, errors.New("synthetic open failure") }},
	}
	var evidence *Evidence
	for _, step := range steps {
		harness := &runHarness{clock: &syntheticClock{base: time.Unix(0, 0)}}
		if step.script != nil {
			harness.scripts = [][]scriptedByte{step.script}
		}
		config := newRunConfig(harness, step.kind, 1)
		if step.open != nil {
			config.Open = step.open
		}
		got, err := Run(context.Background(), config, evidence)
		if err != nil && !errors.Is(err, ErrAllAttemptsFailed) {
			// a step whose every attempt fails by design still records its evidence
			t.Fatalf("fixture step %d: %v", len(got.Attempts), err)
		}
		evidence = &got
	}
	want := []Termination{TerminationCaptured, TerminationSegmentBound, TerminationDeadline, TerminationReadError, ""}
	if len(evidence.Attempts) != len(want) {
		t.Fatalf("fixture has %d attempts", len(evidence.Attempts))
	}
	for index, termination := range want {
		attempt := evidence.Attempts[index]
		switch {
		case termination == "" && (attempt.Observation != nil || attempt.OpenError == ""):
			t.Fatalf("fixture attempt %d is not a failed open: %+v", index, attempt)
		case termination != "" && (attempt.Observation == nil || attempt.Observation.Termination != termination):
			t.Fatalf("fixture attempt %d termination: %+v", index, attempt.Observation)
		}
	}
	data, err := EncodeEvidence(*evidence)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestTeslaObservePriorFixtureIsAccepted(t *testing.T) {
	prior, err := DecodeEvidence(priorFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkPrior(prior, "synthetic-e"); err != nil {
		t.Fatalf("genuine prior refused: %v", err)
	}
}

// Each row edits one persisted field of a genuine file, leaving the bytes in
// place, to a value that the observer cannot have emitted. The row names the
// attempt it edits and a fragment of the refusal it expects, so a row cannot
// pass by tripping an unrelated check.
func TestTeslaObserveRunRefusesInconsistentPriorMetadataBeforeOpening(t *testing.T) {
	long := strings.Repeat("x", maxCauseBytes+1)
	rows := []struct {
		name    string
		attempt int
		mutate  func(*Attempt)
		want    string
	}{
		// Segment direction: the observer sees only the receive side.
		{"first segment transmit", attemptCaptured, func(a *Attempt) { a.Observation.Segments[0].Direction = "transmit" }, "direction"},
		{"last segment transmit", attemptCaptured, func(a *Attempt) { a.Observation.Segments[3].Direction = "transmit" }, "direction"},
		{"direction send", attemptDeadline, func(a *Attempt) { a.Observation.Segments[0].Direction = "send" }, "direction"},
		{"direction empty", attemptSegmentBound, func(a *Attempt) { a.Observation.Segments[2].Direction = "" }, "direction"},
		{"direction other case", attemptCaptured, func(a *Attempt) { a.Observation.Segments[1].Direction = "Receive" }, "direction"},

		// Counts against the slices.
		{"captured count high", attemptCaptured, func(a *Attempt) { a.Observation.Coverage.CapturedBytes++ }, "captured byte count"},
		{"captured count low", attemptCaptured, func(a *Attempt) { a.Observation.Coverage.CapturedBytes-- }, "captured byte count"},
		{"captured above requested", attemptCaptured, func(a *Attempt) { a.Observation.Coverage.RequestedBytes = 10 }, "captured byte count"},
		{"segment count high", attemptCaptured, func(a *Attempt) { a.Observation.Coverage.SegmentCount++ }, "segment count"},
		{"segment count low", attemptCaptured, func(a *Attempt) { a.Observation.Coverage.SegmentCount-- }, "segment count"},
		{"segment count above cap", attemptCaptured, func(a *Attempt) { a.Observation.Coverage.SegmentCap = 3 }, "segment count"},
		{"requested zero", attemptCaptured, func(a *Attempt) { a.Observation.Coverage.RequestedBytes = 0 }, "requested byte count"},
		{"requested above bound", attemptCaptured, func(a *Attempt) { a.Observation.Coverage.RequestedBytes = MaxRequestedBytes + 1 }, "requested byte count"},
		{"segment cap zero", attemptCaptured, func(a *Attempt) { a.Observation.Coverage.SegmentCap = 0 }, "segment cap"},
		{"segment cap above bound", attemptCaptured, func(a *Attempt) { a.Observation.Coverage.SegmentCap = MaxSegmentCap + 1 }, "segment cap"},
		{"segment gap zero", attemptCaptured, func(a *Attempt) { a.Observation.Coverage.SegmentGap = 0 }, "segment gap"},
		{"segment gap negative", attemptCaptured, func(a *Attempt) { a.Observation.Coverage.SegmentGap = -1 }, "segment gap"},
		{"segment gap above bound", attemptCaptured, func(a *Attempt) { a.Observation.Coverage.SegmentGap = MaxObservationWindow + 1 }, "segment gap"},

		// Overflow only follows an exhausted segment cap after a gap.
		{"two overflow bytes", attemptSegmentBound, func(a *Attempt) {
			a.Observation.Coverage.Overflow = []byte{1, 2}
			a.Observation.Coverage.OverflowOffsets = []time.Duration{ms(9), ms(9)}
		}, "more than one overflow byte"},
		{"overflow with spare segments", attemptCaptured, func(a *Attempt) {
			a.Observation.Coverage.Overflow = []byte{1}
			a.Observation.Coverage.OverflowOffsets = []time.Duration{ms(5)}
		}, "overflow byte without"},
		{"overflow in a partial window", attemptDeadline, func(a *Attempt) {
			a.Observation.Coverage.Overflow = []byte{1}
			a.Observation.Coverage.OverflowOffsets = []time.Duration{ms(1)}
		}, "overflow byte without"},
		{"overflow within the gap", attemptSegmentBound, func(a *Attempt) {
			a.Observation.Coverage.OverflowOffsets[0] = ms(8) + a.Observation.Coverage.SegmentGap
		}, "overflow byte offset"},
		{"overflow before the last byte", attemptSegmentBound, func(a *Attempt) {
			a.Observation.Coverage.OverflowOffsets[0] = ms(7)
		}, "overflow byte offset"},
		{"overflow after the return", attemptSegmentBound, func(a *Attempt) {
			a.Observation.Coverage.OverflowOffsets[0] = a.Observation.Return + 1
		}, "overflow byte offset"},
		{"overflow removed from segment_bound", attemptSegmentBound, func(a *Attempt) {
			a.Observation.Coverage.Overflow, a.Observation.Coverage.OverflowOffsets = nil, nil
		}, "segment_bound without"},

		// Receipt offsets against the slices and the segment gap.
		{"negative offset", attemptCaptured, func(a *Attempt) { a.Observation.Segments[0].Offsets[0] = -1 }, "outside 0..return"},
		{"offset after the return", attemptCaptured, func(a *Attempt) { a.Observation.Segments[2].Offsets[1] = ms(60) }, "outside 0..return"},
		{"offset before the previous byte", attemptCaptured, func(a *Attempt) { a.Observation.Segments[1].Offsets[0] = 0 }, "before the previous byte"},
		{"segments reordered", attemptCaptured, func(a *Attempt) {
			segments := a.Observation.Segments
			segments[0], segments[1] = segments[1], segments[0]
		}, "before the previous byte"},
		{"segments merged across a gap", attemptCaptured, func(a *Attempt) {
			a.Observation.Segments[1].Offsets[0] = a.Observation.Segments[0].Offsets[1] + a.Observation.Coverage.SegmentGap
		}, "starts within the segment gap"},
		{"segment wider than the gap", attemptCaptured, func(a *Attempt) { a.Observation.Segments[0].Offsets[1] = ms(2) }, "wider than the segment gap"},

		// Termination and its cause.
		{"unknown termination", attemptCaptured, func(a *Attempt) { a.Observation.Termination = "bogus" }, "termination"},
		{"empty termination", attemptCaptured, func(a *Attempt) { a.Observation.Termination = "" }, "termination"},
		{"closed termination", attemptCaptured, func(a *Attempt) { a.Observation.Termination = TerminationClosed }, "termination"},
		{"generation_lost termination", attemptDeadline, func(a *Attempt) { a.Observation.Termination = TerminationGenerationLost }, "termination"},
		{"partial window called captured", attemptDeadline, func(a *Attempt) { a.Observation.Termination = TerminationCaptured }, "short of the requested"},
		{"segment_bound window called captured", attemptSegmentBound, func(a *Attempt) { a.Observation.Termination = TerminationCaptured }, "short of the requested"},
		{"captured with overflow", attemptCaptured, func(a *Attempt) {
			a.Observation.Coverage.Overflow, a.Observation.Coverage.OverflowOffsets = []byte{1}, []time.Duration{ms(9)}
			a.Observation.Coverage.CapturedBytes = a.Observation.Coverage.RequestedBytes
		}, "overflow byte without"},
		{"captured at its deadline", attemptCaptured, func(a *Attempt) { a.Observation.Return = a.Observation.Deadline }, "at or after its deadline"},
		{"full window called deadline", attemptCaptured, func(a *Attempt) { a.Observation.Termination = TerminationDeadline }, "no read that could have ended"},
		{"segment_bound called deadline", attemptSegmentBound, func(a *Attempt) { a.Observation.Termination = TerminationDeadline }, "no read that could have ended"},
		{"full window called segment_bound", attemptCaptured, func(a *Attempt) { a.Observation.Termination = TerminationSegmentBound }, "segment_bound without"},
		{"segment_bound at its deadline", attemptSegmentBound, func(a *Attempt) { a.Observation.Return = a.Observation.Deadline }, "at or after its deadline"},
		{"full window called read_error", attemptCaptured, func(a *Attempt) {
			a.Observation.Termination, a.Observation.Cause = TerminationReadError, "synthetic"
		}, "read could not have reached"},
		{"read_error without a cause", attemptReadError, func(a *Attempt) { a.Observation.Cause = "" }, "read_error without a cause"},
		{"read_error with a long cause", attemptReadError, func(a *Attempt) { a.Observation.Cause = long }, "cause is longer"},
		{"read_error at its deadline", attemptReadError, func(a *Attempt) { a.Observation.Return = a.Observation.Deadline }, "at or after its deadline"},
		{"cause on a captured window", attemptCaptured, func(a *Attempt) { a.Observation.Cause = "synthetic" }, "cause beside a termination"},
		{"cause on a deadline", attemptDeadline, func(a *Attempt) { a.Observation.Cause = "synthetic" }, "cause beside a termination"},
		{"read_error called captured", attemptReadError, func(a *Attempt) { a.Observation.Termination, a.Observation.Cause = TerminationCaptured, "" }, "short of the requested"},

		// Identity and enumerated fields.
		{"instance of another attempt", attemptCaptured, func(a *Attempt) { a.Observation.Instance = 2 }, "attempt number"},
		{"instance zero", attemptDeadline, func(a *Attempt) { a.Observation.Instance = 0 }, "attempt number"},
		{"generation", attemptCaptured, func(a *Attempt) { a.Observation.Generation = 2 }, "generation"},
		{"observation id", attemptCaptured, func(a *Attempt) { a.Observation.ID = 2 }, "observation id"},
		{"clock epoch of another attempt", attemptCaptured, func(a *Attempt) { a.Observation.ClockEpoch = clockEpochPrefix + "a02" }, "clock epoch"},
		{"clock epoch free text", attemptCaptured, func(a *Attempt) { a.Observation.ClockEpoch = "synthetic-c" }, "clock epoch"},
		{"deadline zero", attemptCaptured, func(a *Attempt) { a.Observation.Deadline = 0 }, "deadline offset"},
		{"deadline above bound", attemptCaptured, func(a *Attempt) { a.Observation.Deadline = MaxObservationWindow + 1 }, "deadline offset"},
		{"deadline negative", attemptCaptured, func(a *Attempt) { a.Observation.Deadline = -ms(1) }, "deadline offset"},
		{"return negative", attemptCaptured, func(a *Attempt) { a.Observation.Return = -1 }, "return offset"},
		{"timing resolution", attemptCaptured, func(a *Attempt) { a.Observation.Coverage.TimingResolution = "wire" }, "timing resolution"},
		{"framing result", attemptCaptured, func(a *Attempt) { a.Observation.Coverage.Framing = "rtu_crc_valid" }, "framing result"},
		{"limitations removed", attemptCaptured, func(a *Attempt) { a.Observation.Coverage.Limitations = nil }, "limitations"},
		{"limitation added", attemptCaptured, func(a *Attempt) {
			a.Observation.Coverage.Limitations = append(a.Observation.Coverage.Limitations, "sender attribution is Proven")
		}, "limitations"},
		{"limitation reworded", attemptCaptured, func(a *Attempt) { a.Observation.Coverage.Limitations[0] = "sender is the device" }, "limitations"},

		// Attempt shape and lifecycle replay.
		{"case", attemptCaptured, func(a *Attempt) { a.Case = "reset" }, "case"},
		{"case empty", attemptDeadline, func(a *Attempt) { a.Case = "" }, "case"},
		{"observation removed", attemptCaptured, func(a *Attempt) { a.Observation = nil }, "lifecycle replay without an observation"},
		{"observation and lifecycle removed", attemptCaptured, func(a *Attempt) { a.Observation, a.Lifecycle = nil, nil }, "neither an observation"},
		{"open error beside an observation", attemptCaptured, func(a *Attempt) { a.OpenError = "synthetic" }, "open error beside"},
		{"lifecycle removed", attemptCaptured, func(a *Attempt) { a.Lifecycle = nil }, "without a lifecycle replay"},
		{"open error removed", attemptOpenError, func(a *Attempt) { a.OpenError = "" }, "neither an observation"},
		{"open error with a replay", attemptOpenError, func(a *Attempt) { a.Lifecycle = &LifecycleReplay{Disposition: LifecycleReplayDisposition} }, "lifecycle replay without an observation"},
		{"open error long", attemptOpenError, func(a *Attempt) { a.OpenError = long }, "open error is longer"},
		{"replay disposition", attemptCaptured, func(a *Attempt) { a.Lifecycle.Disposition = "activation evidence" }, "disposition"},
		{"replay state unknown", attemptCaptured, func(a *Attempt) { a.Lifecycle.State = "bogus" }, "lifecycle state"},
		{"replay state empty", attemptCaptured, func(a *Attempt) { a.Lifecycle.State = "" }, "lifecycle state"},
		{"replay readiness flipped", attemptCaptured, func(a *Attempt) { a.Lifecycle.Ready = !a.Lifecycle.Ready }, "readiness disagrees"},
		{"replay ready with a refused acquisition", attemptDeadline, func(a *Attempt) {
			a.Lifecycle.Ready, a.Lifecycle.State = true, "ready"
		}, "ready yet records a refused acquisition"},
		{"replay ready for a partial window", attemptDeadline, func(a *Attempt) {
			a.Lifecycle.Ready, a.Lifecycle.State, a.Lifecycle.AcquireRefused = true, "ready", false
		}, "ready for an incomplete window"},
		{"replay not constructed without an error", attemptCaptured, func(a *Attempt) {
			a.Lifecycle.State, a.Lifecycle.Ready, a.Lifecycle.AcquireRefused, a.Lifecycle.Error = lifecycleNotConstructed, false, false, ""
		}, "never constructed"},
		{"replay frames negative", attemptCaptured, func(a *Attempt) { a.Lifecycle.FramesSent = -1 }, "frame count"},
		{"replay error long", attemptCaptured, func(a *Attempt) { a.Lifecycle.Error = long }, "replay error is longer"},
	}
	encoded := priorFixture(t)
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			tampered, err := DecodeEvidence(encoded)
			if err != nil {
				t.Fatal(err)
			}
			row.mutate(&tampered.Attempts[row.attempt])
			harness := &runHarness{clock: &syntheticClock{}}
			_, err = Run(context.Background(), newRunConfig(harness, CaseIdle, 1), &tampered)
			if !errors.Is(err, ErrInvalidRequest) || harness.opens != 0 {
				t.Fatalf("inconsistent prior accepted: err=%v opens=%d", err, harness.opens)
			}
			if !strings.Contains(err.Error(), row.want) {
				t.Fatalf("refusal %q lacks %q", err, row.want)
			}
		})
	}
}

// A transmit direction is refused, and the refusal names the attempt and the
// kind of inconsistency without echoing the persisted value.
func TestTeslaObservePriorRefusalNamesTheAttemptAndOmitsTheValue(t *testing.T) {
	tampered, err := DecodeEvidence(priorFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	tampered.Attempts[attemptSegmentBound].Observation.Segments[3].Direction = "transmit"
	err = checkPrior(tampered, "synthetic-e")
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("transmit direction accepted: %v", err)
	}
	for _, want := range []string{"attempt 2", "inconsistent observation metadata", "segment 4"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal %q lacks %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "transmit") {
		t.Fatalf("refusal echoes the persisted value: %q", err)
	}
}

// Metadata the observer can emit stays accepted: a late or canceled window,
// a read that ends on the deadline or fails, and a recorded lifecycle error.
// The captured attempt of the fixture replayed to readiness, so a variant that
// makes its window incomplete also makes its replay not ready, as a real run
// would have recorded.
func TestTeslaObservePriorKeepsAcceptingEmittableVariants(t *testing.T) {
	incomplete := func(a *Attempt) {
		a.Lifecycle.Ready, a.Lifecycle.State, a.Lifecycle.AcquireRefused = false, "unavailable", true
	}
	accepted := []struct {
		name    string
		attempt int
		mutate  func(*Attempt)
	}{
		{"captured window canceled", attemptCaptured, func(a *Attempt) { a.Observation.Termination = TerminationCanceled; incomplete(a) }},
		{"partial window canceled", attemptDeadline, func(a *Attempt) { a.Observation.Termination = TerminationCanceled }},
		{"full window late at the deadline", attemptCaptured, func(a *Attempt) {
			a.Observation.Termination, a.Observation.Return = TerminationDeadline, a.Observation.Deadline
			incomplete(a)
		}},
		{"segment_bound window late at the deadline", attemptSegmentBound, func(a *Attempt) {
			a.Observation.Termination, a.Observation.Return = TerminationDeadline, a.Observation.Deadline
		}},
		{"read ended on the deadline", attemptReadError, func(a *Attempt) { a.Observation.Termination, a.Observation.Cause = TerminationDeadline, "" }},
		{"failed read keeps a partial window", attemptDeadline, func(a *Attempt) {
			a.Observation.Termination, a.Observation.Cause = TerminationReadError, "synthetic read failure"
		}},
		{"lifecycle error recorded", attemptCaptured, func(a *Attempt) { a.Lifecycle.Error = "synthetic lifecycle refusal" }},
		{"lifecycle frames counted", attemptCaptured, func(a *Attempt) { a.Lifecycle.FramesSent = 3 }},
	}
	encoded := priorFixture(t)
	for _, tc := range accepted {
		t.Run(tc.name, func(t *testing.T) {
			prior, err := DecodeEvidence(encoded)
			if err != nil {
				t.Fatal(err)
			}
			tc.mutate(&prior.Attempts[tc.attempt])
			if err := checkPrior(prior, "synthetic-e"); err != nil {
				t.Fatalf("emittable metadata refused: %v", err)
			}
		})
	}
}

func TestTeslaObserveBoundedCauseIsNeverEmpty(t *testing.T) {
	if got := boundedCause(errors.New("")); got == "" {
		t.Fatal("empty cause")
	}
	if got := boundedCause(errors.New(strings.Repeat("x", 3*maxCauseBytes))); len(got) != maxCauseBytes {
		t.Fatalf("cause length %d", len(got))
	}
}
