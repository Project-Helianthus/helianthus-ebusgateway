package teslaobserve

import (
	"fmt"
	"slices"
	"time"
	"unicode/utf8"

	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

// An appended run copies the prior attempts into the rebuilt worksheet rows,
// and evidenceRows renders every segment with the fixed receive wording. A
// file that edits observation metadata without changing the bytes would
// therefore leave the persisted attempt and the derived rows in conflict.
// The checks below accept only metadata that runAttempt and Endpoint.Observe
// can have emitted; checkPrior runs them before any port is opened.

// priorTerminations are the terminations a persisted observation can carry.
// closed and generation_lost need a Close or Fence on the attempt's endpoint
// while it observes. runAttempt owns that endpoint privately and issues
// neither, so a file this tool wrote never contains them.
var priorTerminations = map[Termination]bool{
	TerminationCaptured:     true,
	TerminationDeadline:     true,
	TerminationCanceled:     true,
	TerminationReadError:    true,
	TerminationSegmentBound: true,
}

// lifecycleNotConstructed is the replay state when the pinned lifecycle could
// not be built from the offline control configuration.
const lifecycleNotConstructed = "not_constructed"

var lifecycleStates = map[string]bool{
	lifecycleNotConstructed:                             true,
	string(modbusreg.TeslaGen3ReadLifecycleStopped):     true,
	string(modbusreg.TeslaGen3ReadLifecycleActivating):  true,
	string(modbusreg.TeslaGen3ReadLifecycleReady):       true,
	string(modbusreg.TeslaGen3ReadLifecycleRecovering):  true,
	string(modbusreg.TeslaGen3ReadLifecycleUnavailable): true,
	string(modbusreg.TeslaGen3ReadLifecycleClosed):      true,
}

// metadataFault names why the attempt's derived metadata cannot have been
// emitted by runAttempt as attempt number (1-based), or returns "" when it
// is consistent. The label, the operator record and the segment shapes have
// been checked by the caller.
func (attempt Attempt) metadataFault(number int) string {
	if attempt.Case != CaseInitial && attempt.Case != CaseRecovery && attempt.Case != CaseIdle {
		return "case is not initial, recovery or idle"
	}
	observation := attempt.Observation
	if observation == nil {
		switch {
		case attempt.Lifecycle != nil:
			return "lifecycle replay without an observation"
		case attempt.OpenError == "":
			return "neither an observation nor an open error"
		case utf8.RuneCountInString(attempt.OpenError) > maxCauseBytes:
			return "open error is longer than a bounded cause"
		}
		return ""
	}
	switch {
	case attempt.OpenError != "":
		return "open error beside an observation"
	case attempt.Lifecycle == nil:
		return "observation without a lifecycle replay"
	}
	if fault := observation.metadataFault(attempt.Label, uint64(number)); fault != "" {
		return fault
	}
	return attempt.Lifecycle.metadataFault(observation.Termination)
}

// metadataFault checks an observation whose segment shapes already passed
// shapeValid. instance is the attempt number, which runAttempt passes to
// NewEndpoint.
func (observation Observation) metadataFault(label string, instance uint64) string {
	coverage := observation.Coverage
	switch {
	case observation.Instance != instance:
		return fmt.Sprintf("instance %d is not the attempt number %d", observation.Instance, instance)
	case observation.Generation != 1 || observation.ID != 1:
		return "generation and observation id are not those of a fresh single-use endpoint"
	case observation.ClockEpoch != clockEpochPrefix+label:
		return "clock epoch is not the attempt's host-monotonic epoch"
	case observation.Deadline <= 0 || observation.Deadline > MaxObservationWindow:
		return "deadline offset is outside the observation window bound"
	case observation.Return < 0:
		return "return offset is negative"
	case !priorTerminations[observation.Termination]:
		return "termination is not one this tool records"
	case coverage.RequestedBytes < 1 || coverage.RequestedBytes > MaxRequestedBytes:
		return "requested byte count is outside its bound"
	case coverage.SegmentCap < 1 || coverage.SegmentCap > MaxSegmentCap:
		return "segment cap is outside its bound"
	case coverage.SegmentGap <= 0 || coverage.SegmentGap > MaxObservationWindow:
		return "segment gap is outside its bound"
	case coverage.TimingResolution != TimingResolutionHostRead:
		return "timing resolution is not host_read_return"
	case coverage.Framing != FramingUnknown:
		return "framing result is not unknown"
	case !slices.Equal(coverage.Limitations, observationLimitations):
		return "limitations are not the fixed observer limitations"
	}
	if fault := observation.segmentFault(); fault != "" {
		return fault
	}
	return observation.terminationFault()
}

// segmentFault checks directions, counts, receipt offsets and the segment
// boundaries against the segment gap, which is how Observe splits segments.
func (observation Observation) segmentFault() string {
	coverage := observation.Coverage
	if len(observation.Segments) != coverage.SegmentCount || coverage.SegmentCount > coverage.SegmentCap {
		return "segment count disagrees with the segments or exceeds the segment cap"
	}
	var (
		captured int
		last     time.Duration
		seen     bool
	)
	for index, segment := range observation.Segments {
		if segment.Direction != DirectionReceive {
			return fmt.Sprintf("segment %d direction is not %q; the observer sees only the receive side", index+1, DirectionReceive)
		}
		captured += len(segment.Bytes)
		for position, offset := range segment.Offsets {
			switch {
			case offset < 0 || offset > observation.Return:
				return fmt.Sprintf("segment %d has a receipt offset outside 0..return", index+1)
			case !seen:
			case offset < last:
				return fmt.Sprintf("segment %d has a receipt offset before the previous byte", index+1)
			case position == 0 && offset-last <= coverage.SegmentGap:
				return fmt.Sprintf("segment %d starts within the segment gap of the previous byte", index+1)
			case position > 0 && offset-last > coverage.SegmentGap:
				return fmt.Sprintf("segment %d spans a receipt gap wider than the segment gap", index+1)
			}
			seen, last = true, offset
		}
	}
	if captured != coverage.CapturedBytes || captured > coverage.RequestedBytes {
		return "captured byte count disagrees with the segments or exceeds the requested count"
	}
	switch len(coverage.Overflow) {
	case 0:
	case 1:
		if len(coverage.OverflowOffsets) != 1 || coverage.SegmentCount != coverage.SegmentCap || captured >= coverage.RequestedBytes {
			return "overflow byte without an exhausted segment cap and an open window"
		}
		if at := coverage.OverflowOffsets[0]; at > observation.Return || at < last || at-last <= coverage.SegmentGap {
			return "overflow byte offset does not follow a segment gap after the last byte"
		}
	default:
		return "more than one overflow byte"
	}
	return ""
}

// terminationFault checks the termination against the counts and the cause
// that Observe derives it from.
func (observation Observation) terminationFault() string {
	coverage := observation.Coverage
	overflow := len(coverage.Overflow)
	// Observe tests the deadline before every termination except canceled, so
	// captured, segment_bound and read_error all returned before it.
	early := observation.Return < observation.Deadline
	switch observation.Termination {
	case TerminationCaptured:
		switch {
		case coverage.CapturedBytes != coverage.RequestedBytes:
			return "captured window is short of the requested byte count"
		case overflow != 0:
			return "captured window carries an overflow byte"
		case !early:
			return "captured window returned at or after its deadline"
		}
	case TerminationSegmentBound:
		switch {
		case overflow != 1:
			return "segment_bound without the consumed overflow byte"
		case !early:
			return "segment_bound window returned at or after its deadline"
		}
	case TerminationReadError:
		switch {
		case observation.Cause == "":
			return "read_error without a cause"
		case overflow != 0 || coverage.CapturedBytes >= coverage.RequestedBytes:
			return "read_error beside a bound the read could not have reached"
		case !early:
			return "read_error window returned at or after its deadline"
		}
	case TerminationDeadline:
		// A deadline before the return offset reaches the deadline needs a
		// read that ended on the deadline, so the window cannot be full.
		if early && (overflow != 0 || coverage.CapturedBytes >= coverage.RequestedBytes) {
			return "deadline termination before the deadline, with no read that could have ended on it"
		}
	}
	switch {
	case observation.Termination != TerminationReadError && observation.Cause != "":
		return "cause beside a termination that records none"
	case utf8.RuneCountInString(observation.Cause) > maxCauseBytes:
		return "cause is longer than a bounded cause"
	}
	return ""
}

// metadataFault checks a replay against the termination of the observation it
// replayed. The pinned lifecycle only becomes ready on a captured window.
func (replay LifecycleReplay) metadataFault(termination Termination) string {
	switch {
	case replay.Disposition != LifecycleReplayDisposition:
		return "lifecycle replay disposition is not the offline comparison wording"
	case !lifecycleStates[replay.State]:
		return "lifecycle state is not a pinned lifecycle state"
	case replay.Ready != (replay.State == string(modbusreg.TeslaGen3ReadLifecycleReady)):
		return "lifecycle readiness disagrees with the lifecycle state"
	case replay.Ready && replay.AcquireRefused:
		return "lifecycle replay is ready yet records a refused acquisition"
	case replay.Ready && termination != TerminationCaptured:
		return "lifecycle replay is ready for an incomplete window"
	case replay.State == lifecycleNotConstructed && (replay.Error == "" || replay.AcquireRefused):
		return "lifecycle replay that was never constructed carries a state it cannot have"
	case replay.FramesSent < 0:
		return "lifecycle replay frame count is negative"
	case utf8.RuneCountInString(replay.Error) > maxCauseBytes:
		return "lifecycle replay error is longer than a bounded cause"
	}
	return ""
}
