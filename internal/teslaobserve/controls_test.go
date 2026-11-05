package teslaobserve

import (
	"context"
	"sync/atomic"
	"testing"

	modbus "github.com/Project-Helianthus/helianthus-modbus"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
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
		if !result.Ready && (!result.AcquireRefused || result.LifecycleState == string(modbusreg.TeslaGen3ReadLifecycleReady)) {
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
		replay.State != string(modbusreg.TeslaGen3ReadLifecycleUnavailable) {
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
