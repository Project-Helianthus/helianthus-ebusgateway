package semhub

import (
	"context"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// maxCaptureClockCoordinates bounds the clock domains one exact capture accepts.
const maxCaptureClockCoordinates = 32

// CaptureClockSample is the explicit evaluation input of one exact capture:
// one wall point shared by every asset and one monotonic coordinate per clock
// epoch. It is a detached value for a single call, not a clock registry.
type CaptureClockSample struct {
	Wall      semreg.TimePoint
	Monotonic map[semreg.ClockEpochID]semreg.MonotonicPoint
}

// CaptureExact applies Capture's selection and admission rules, then evaluates
// each admitted snapshot with the sampled wall point and the coordinate for the
// snapshot's own clock epoch. No coordinate is derived from wall time, another
// epoch or a default. sample runs once under the hub read lock; like Capture's
// clock it must not block or reenter the hub.
func (h *Hub) CaptureExact(ctx context.Context, ids []semreg.AssetID, sample func() (CaptureClockSample, error)) (CoherentCapture, error) {
	return h.capture(ctx, ids, func() (semreg.TimePoint, captureContext, error) {
		if sample == nil {
			return semreg.TimePoint{}, nil, &semreg.Error{ID: semreg.InvalidTime, Detail: "missing capture clock sample"}
		}
		clock, err := sample()
		if err != nil {
			return semreg.TimePoint{}, nil, err
		}
		wall, coordinates, err := detachClockSample(clock)
		if err != nil {
			return semreg.TimePoint{}, nil, err
		}
		return wall, func(snapshot semreg.Snapshot) (semreg.EvaluationContext, error) {
			point, ok := coordinates[snapshot.EvaluateMonotonic.ClockEpochID]
			if !ok {
				return semreg.EvaluationContext{}, &semreg.Error{ID: semreg.InvalidTime, Detail: "missing clock epoch coordinate"}
			}
			return semreg.EvaluationContext{EvaluatedAt: wall, EvaluateMonotonic: point}, nil
		}, nil
	})
}

// detachClockSample validates the whole sample and copies its coordinates, so
// later caller changes to the map cannot reach this read.
func detachClockSample(clock CaptureClockSample) (semreg.TimePoint, map[semreg.ClockEpochID]semreg.MonotonicPoint, error) {
	if clock.Wall.Validate() != nil {
		return semreg.TimePoint{}, nil, &semreg.Error{ID: semreg.InvalidTime, Detail: "capture wall point"}
	}
	if len(clock.Monotonic) > maxCaptureClockCoordinates {
		return semreg.TimePoint{}, nil, &semreg.Error{ID: semreg.InvalidTime, Detail: "capture clock coordinates"}
	}
	coordinates := make(map[semreg.ClockEpochID]semreg.MonotonicPoint, len(clock.Monotonic))
	for epoch, point := range clock.Monotonic {
		if point.ClockEpochID != epoch || point.Validate() != nil {
			return semreg.TimePoint{}, nil, &semreg.Error{ID: semreg.InvalidTime, Detail: "capture clock coordinate"}
		}
		coordinates[epoch] = point
	}
	return clock.Wall, coordinates, nil
}
