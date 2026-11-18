package teslaobserve

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	modbus "github.com/Project-Helianthus/helianthus-modbus"
	"github.com/Project-Helianthus/helianthus-modbusreg/tesla"
)

func TestTeslaObserveOfflineControlsPassAndSendNothing(t *testing.T) {
	results := RunControls(context.Background())
	want := map[string]struct {
		row   string
		term  Termination
		ready bool
	}{
		"synthetic_fragmented_baseline": {ControlRowPositive, TerminationCaptured, true},
		"missing_sequence":              {ControlRowToken, TerminationDeadline, false},
		"partial_sequence":              {ControlRowToken, TerminationDeadline, false},
		"reordered_segments":            {ControlRowToken, TerminationCaptured, false},
		"wrong_direction":               {ControlRowToken, TerminationCaptured, false},
		"leading_unrelated_byte":        {ControlRowToken, TerminationCaptured, false},
		"segment_exhaustion":            {ControlRowToken, TerminationSegmentBound, false},
		"late_return_at_deadline":       {ControlRowWitness, TerminationDeadline, false},
		"canceled_before_commit":        {ControlRowWitness, TerminationCaptured, false},
		"prior_generation":              {ControlRowWitness, TerminationCaptured, false},
		"close_before_commit":           {ControlRowWitness, TerminationCaptured, false},
		"expired_activation_window":     {ControlRowWitness, TerminationCaptured, false},
		"reused_observation":            {ControlRowWitness, TerminationCaptured, false},
	}
	if len(results) != len(want) {
		t.Fatalf("%d controls, want %d", len(results), len(want))
	}
	for _, result := range results {
		expected, ok := want[result.Name]
		if !ok {
			t.Fatalf("unexpected control %s", result.Name)
		}
		if !result.Pass || result.Row != expected.row || result.Termination != expected.term ||
			result.Ready != expected.ready || result.FramesSent != 0 {
			t.Fatalf("control %+v", result)
		}
		if !result.Ready && (!result.AcquireRefused || result.LifecycleState == string(tesla.Gen3ReadLifecycleReady)) {
			t.Fatalf("negative control reached readiness or acquisition: %+v", result)
		}
	}
}

// The negative controls only discriminate if the same configuration can
// reach readiness: the positive control proves the rejections are caused by
// the varied input, not by an unqualified offline configuration.
func TestTeslaObserveControlRowNeedsThePositiveControl(t *testing.T) {
	results := RunControls(context.Background())
	if row := controlRow(results, ControlRowToken); row.Grade != GradeProven {
		t.Fatalf("token row = %+v", row)
	}
	for index := range results {
		if results[index].Row == ControlRowPositive {
			results[index].Pass = false
		}
	}
	if row := controlRow(results, ControlRowToken); row.Grade != GradeUnknown {
		t.Fatalf("token row without a positive control = %+v", row)
	}
	results = RunControls(context.Background())
	results[len(results)-1].Pass = false
	if row := controlRow(results, ControlRowWitness); row.Grade != GradeUnknown {
		t.Fatalf("witness row with a failed control = %+v", row)
	}
}

func TestTeslaObservePassiveBaselineIsUnreadyAndScriptedExchangeIsReady(t *testing.T) {
	harness := newHookHarness(t, segmentsAt(syntheticFragments, syntheticOffsets, 5))
	observation := harness.observe(t)
	if observation.Termination != TerminationCaptured || string(observation.Window()) != "TESLA\x00PASS\x00" {
		t.Fatalf("baseline observation = %s window %q", observation.Termination, observation.Window())
	}
	passive := ReplayLifecycle(context.Background(), harness.endpoint, observation, nil)
	if passive.Ready || !passive.AcquireRefused || passive.FramesSent != 0 ||
		passive.State != string(tesla.Gen3ReadLifecycleUnavailable) {
		t.Fatalf("passive baseline replay = %+v", passive)
	}
	fresh := newHookHarness(t, segmentsAt(syntheticFragments, syntheticOffsets, 5))
	freshObservation := fresh.observe(t)
	scripted := replayLifecycle(context.Background(), scriptedActivation{observationReport{endpoint: fresh.endpoint, observation: freshObservation}}, freshObservation.Endpoint, nil)
	if !scripted.Ready || scripted.FramesSent != 0 || scripted.AcquireRefused ||
		scripted.State != string(tesla.Gen3ReadLifecycleReady) {
		t.Fatalf("scripted exchange replay = %+v", scripted)
	}
}

// On the scripted pass, where the unvaried baseline reaches readiness, each
// negative control must be refused by the property it names rather than by
// the passive driver's unconditional refusal.
func TestTeslaObserveNegativeControlsAreRefusedByTheirVariation(t *testing.T) {
	cause := map[string]string{
		"missing_sequence":          "incomplete: deadline",
		"partial_sequence":          "incomplete: deadline",
		"reordered_segments":        "unexpected activation report",
		"wrong_direction":           "non-receive segment",
		"leading_unrelated_byte":    "unexpected activation report",
		"segment_exhaustion":        "incomplete: segment_bound",
		"late_return_at_deadline":   "incomplete: deadline",
		"canceled_before_commit":    "context canceled",
		"prior_generation":          ErrNotCurrent.Error(),
		"close_before_commit":       ErrClosed.Error(),
		"expired_activation_window": "deadline exceeded",
		"reused_observation":        ErrNotCurrent.Error(),
	}
	scripted := func(report observationReport) tesla.HSCActivationDriver { return scriptedActivation{report} }
	for index, control := range controlCases() {
		replay, _, ok := replayControl(context.Background(), uint64(index+1), control, scripted)
		if !ok {
			t.Fatalf("%s: no observation", control.name)
		}
		if control.row == ControlRowPositive {
			if !replay.Ready || replay.FramesSent != 0 {
				t.Fatalf("%s: scripted pass = %+v", control.name, replay)
			}
			continue
		}
		want, ok := cause[control.name]
		if !ok {
			t.Fatalf("unexpected control %s", control.name)
		}
		if replay.Ready || !replay.AcquireRefused || replay.FramesSent != 0 ||
			!strings.Contains(replay.Error, want) || strings.Contains(replay.Error, errPassiveNoHostEcho.Error()) {
			t.Fatalf("%s: scripted pass = %+v, want cause %q", control.name, replay, want)
		}
	}
}

// A negative control whose variation is removed reaches readiness on the
// scripted pass and no longer passes, so a pass depends on the variation.
func TestTeslaObserveControlWithoutItsVariationFails(t *testing.T) {
	baseline := segmentsAt(syntheticFragments, syntheticOffsets, 5)
	for index, control := range controlCases() {
		if control.row == ControlRowPositive {
			continue
		}
		control.script, control.termination = baseline, TerminationCaptured
		control.mutate, control.before, control.lifecycleNow, control.reuse = nil, nil, nil, false
		result := runControl(context.Background(), uint64(index+1), control)
		if result.Pass || !result.Ready || result.FramesSent != 0 {
			t.Fatalf("%s without its variation = %+v", control.name, result)
		}
	}
}

func TestTeslaObserveLifecycleReplayOfNonMatchingWindowRefusesAcquisition(t *testing.T) {
	harness := newHookHarness(t, segmentsAt([]string{"\x01\x02\x03"}, []int{1}, 2))
	request := harness.request()
	request.RequestedBytes = 3
	observation, err := harness.endpoint.Observe(context.Background(), request)
	if err != nil || observation.Termination != TerminationCaptured {
		t.Fatalf("err=%v observation=%+v", err, observation)
	}
	replay := ReplayLifecycle(context.Background(), harness.endpoint, observation, nil)
	if replay.Ready || !replay.AcquireRefused || replay.FramesSent != 0 || replay.Disposition != LifecycleReplayDisposition ||
		replay.State != string(tesla.Gen3ReadLifecycleUnavailable) {
		t.Fatalf("replay = %+v", replay)
	}
}

func TestTeslaObserveControlEndpointNeverTransmits(t *testing.T) {
	var exchanges atomic.Int64
	config, err := controlAcquisitionConfig("synthetic-e", observationReport{}, nil, &exchanges)
	if err != nil {
		t.Fatal(err)
	}
	if config.LocalPath != "offline-control" || config.MaxAttempts != 1 || len(config.Operations) != len(ObservationOperations) {
		t.Fatalf("control configuration = %+v", config)
	}
	endpoint, err := config.Open(modbus.RTUProductionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	responses, evidence, err := endpoint.ExchangePrivateFunction(context.Background(), 1, modbus.PrivateFunctionRequest{}, modbus.PrivateFunctionResponsePolicy{}, nil)
	if err == nil || responses != nil || evidence.RequestID != 0 || len(evidence.RequestADU) != 0 || exchanges.Load() != 1 {
		t.Fatalf("control exchange responses=%v evidence=%+v err=%v", responses, evidence, err)
	}
	if endpoint.Recover(context.Background()) == nil {
		t.Fatal("control endpoint recovered")
	}
}
