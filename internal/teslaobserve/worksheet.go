package teslaobserve

import (
	"fmt"
	"strings"
	"time"
)

// Evidence grades. The run assigns Proven only to facts about its own tool
// bounds and offline software policy; observation rows stay Unknown until
// the owner reviews them.
const (
	GradeProven     = "Proven"
	GradeHypothesis = "Hypothesis"
	GradeUnknown    = "Unknown"
)

// Row is one worksheet result row. Field is the worksheet's own wording.
type Row struct {
	Field  string `json:"field"`
	Result string `json:"result"`
	Grade  string `json:"grade"`
	Basis  string `json:"basis"`
}

// EvidenceRow is one row of the worksheet's ordered native evidence section.
type EvidenceRow struct {
	SegmentOrder string `json:"segment_order"`
	Direction    string `json:"direction_and_attribution_basis"`
	NativeBytes  string `json:"complete_bounded_native_bytes_and_completeness"`
	Offset       string `json:"relative_monotonic_offset_gap_and_uncertainty"`
	Framing      string `json:"framing_integrity_result"`
	Context      string `json:"context_and_outcome"`
}

// Worksheet field wording, in worksheet order.
var (
	preconditionFields = []string{
		"Synthetic case label and independently reviewed public evidence reference",
		"Family and applicable protocol/operation version; basis for version applicability without identity data",
		"Observation method, directional visibility, clock resolution, and byte-loss limitations",
		"Explicitly configured serial format; evidence it was in effect",
		"Prior state: fresh session, already active, recovery, reset, or unavailable; basis for that classification",
		"Other initiators present or excluded; basis for attribution of each direction",
		"Start/end conditions, byte/frame count cap, duration cap, and stop conditions",
		"Read-only allowlist, retry bound, and whole-observation deadline for any separately admitted read acquisition; no discovery or implicit retry",
		"Whether observation itself could change state; separate action-time authorization satisfied, without names or credentials",
	}
	repeatFields = []string{
		"Independent initial-session observations: count, direction/order consistency, byte/framing differences, and failures",
		"Independent recovery/reset observations, when separately authorized: whether the sequence reappears and prior readiness is lost",
		"Idle or no-sequence baseline: bounded observation and any independently known readiness state",
		"Offline missing, partial, reordered, or wrong-direction token cases: rejection without readiness or send authority",
		"Offline late, cancelled, expired, or prior-generation witness cases: no successor admission",
		"Incomplete visibility or competing-initiator case: attribution remains Unknown, no inferred activation",
		"Contradictory or unsuccessful independent observations, including alternative explanations",
	}
	reviewFields = []string{
		"Supported facts and publishable evidence links, each with applicability and grade",
		"Hypotheses, counterexamples, missing evidence, and projection loss",
		"Registry qualification decision, independently from observation grade",
		"Remaining acquisition prerequisites and no-send disposition",
		"#158 clarification proposal and #38 observation gaps; parent acceptance remaining",
	}
)

const (
	reviewPendingBasis   = "owner review pending; observation summary only, the run assigns no evidence grade"
	offlineControlsBasis = "synthetic offline controls against the pinned read-acquisition lifecycle; fail-closed software policy only, not firmware behavior"
	directionText        = "receive at the observing port; sender Unknown: no independent attribution"
	framingText          = "Unknown: no framing or integrity diagnostic applied; not assumed to be RTU"
)

func buildWorksheet(evidence *Evidence) {
	evidence.Preconditions = preconditionRows(evidence)
	evidence.OrderedEvidence = evidenceRows(evidence.Attempts)
	evidence.RepeatsAndControls = repeatRows(evidence)
	evidence.Review = make([]Row, len(reviewFields))
	for index, field := range reviewFields {
		evidence.Review[index] = Row{Field: field, Result: GradeUnknown, Grade: GradeUnknown,
			Basis: "owner review pending; the run does not fill review fields"}
	}
}

func preconditionRows(evidence *Evidence) []Row {
	attempts := evidence.Attempts
	labels := perAttempt(attempts, func(attempt Attempt) string { return attempt.Case })
	operations := make([]string, len(ObservationOperations))
	for index, operation := range ObservationOperations {
		operations[index] = string(operation)
	}
	bounds := perAttempt(attempts, func(attempt Attempt) string {
		if attempt.Observation == nil {
			return "no window (port did not open)"
		}
		coverage := attempt.Observation.Coverage
		return fmt.Sprintf("requested %d bytes, segment cap %d, segment gap %s, deadline %s",
			coverage.RequestedBytes, coverage.SegmentCap, formatOffset(coverage.SegmentGap), formatOffset(attempt.Observation.Deadline))
	})
	prior := perAttempt(attempts, func(attempt Attempt) string {
		return operatorValue(attempt.Operator.PriorState, attempt.Operator.PriorStateBasis)
	})
	initiators := perAttempt(attempts, func(attempt Attempt) string {
		return operatorValue(attempt.Operator.OtherInitiators, attempt.Operator.OtherInitiatorsBasis)
	})
	authorization := perAttempt(attempts, func(attempt Attempt) string {
		return valueOrUnknown(attempt.Operator.ActionTimeAuthorization)
	})
	sendGrade := GradeProven
	if evidence.FramesSent != 0 {
		sendGrade = GradeUnknown
	}
	results := []Row{
		{Result: GradeUnknown, Grade: GradeUnknown,
			Basis: "case labels: " + labels + "; no independently reviewed public evidence reference exists before owner review"},
		{Result: "family: " + Family + ", as selected by the operator; protocol/operation version: Unknown", Grade: GradeUnknown,
			Basis: "the run sends no request and reads no version, so no version applicability evidence exists"},
		{Result: "passive receive-only observation of one local serial port; directional visibility: receive side of that port only; clock: host monotonic clock sampled when each byte read returns; byte loss: no loss detector, and bytes buffered before the port opened have Unknown provenance",
			Grade: GradeProven, Basis: "fixed by the observation tool, whose byte source has no transmit method; device-side facts stay Unknown"},
		{Result: "115200 baud, 8 data bits, no parity, 1 stop bit configured on the local port; evidence it was in effect at the device: Unknown",
			Grade: GradeUnknown, Basis: "the local port accepted the configuration; nothing confirms the device side"},
		{Result: prior, Grade: operatorGrade(attempts, func(attempt Attempt) string { return attempt.Operator.PriorState }),
			Basis: "operator record per attempt, not independently supported"},
		{Result: initiators + "; attribution of each direction: only the receive direction is visible and its sender is Unknown",
			Grade: operatorGrade(attempts, func(attempt Attempt) string { return attempt.Operator.OtherInitiators }),
			Basis: "operator record per attempt, not independently supported"},
		{Result: "start: after the port opens, one window per attempt on a fresh port instance; end: the first of the requested byte count, the deadline, the segment cap, cancellation, close or a read error; bounds: " + bounds + "; stop conditions: captured, deadline, canceled, closed, generation_lost, read_error, segment_bound",
			Grade: GradeProven, Basis: "tool bounds, validated before any byte is read"},
		{Result: fmt.Sprintf("none admitted: NO_SEND mode runs no read acquisition; allowlist named for a later separately admitted acquisition: %s; retry bound 0; frames sent %d; whole-observation deadline: the per-attempt window; no discovery or implicit retry",
			strings.Join(operations, ", "), evidence.FramesSent),
			Grade: sendGrade, Basis: "the byte source has no transmit method; offline lifecycle replays use a control endpoint with no transport"},
		{Result: "the run transmits nothing; whether opening the port or the line state can change device state is Unknown; separate action-time authorization: " + authorization,
			Grade: GradeUnknown, Basis: "operator record without names or credentials"},
	}
	for index := range results {
		results[index].Field = preconditionFields[index]
	}
	return results
}

func evidenceRows(attempts []Attempt) []EvidenceRow {
	var rows []EvidenceRow
	for _, attempt := range attempts {
		context := fmt.Sprintf("case %s; prior state %s; condition: %s", attempt.Case,
			valueOrUnknown(attempt.Operator.PriorState), valueOrUnknown(attempt.Operator.Condition))
		observation := attempt.Observation
		if observation == nil {
			rows = append(rows, EvidenceRow{SegmentOrder: attempt.Label, Direction: directionText,
				NativeBytes: "none: the port did not open", Offset: GradeUnknown, Framing: framingText,
				Context: context + "; outcome: open failed (" + attempt.OpenError + ")"})
			continue
		}
		outcome := context + "; window termination " + string(observation.Termination)
		completeness := "window complete"
		if observation.Termination != TerminationCaptured {
			completeness = "window incomplete (" + string(observation.Termination) + ")"
		}
		if len(observation.Segments) == 0 {
			rows = append(rows, EvidenceRow{SegmentOrder: attempt.Label, Direction: directionText,
				NativeBytes: "none observed in the window; absence of an observation is not an observed absence",
				Offset:      "return " + formatOffset(observation.Return) + " after observation start",
				Framing:     framingText, Context: outcome})
		}
		var previous time.Duration
		for index, segment := range observation.Segments {
			first, last := segment.Offsets[0], segment.Offsets[len(segment.Offsets)-1]
			gap := "first segment"
			if index > 0 {
				gap = "gap " + formatOffset(first-previous) + " after the previous segment"
			}
			previous = last
			rows = append(rows, EvidenceRow{
				SegmentOrder: fmt.Sprintf("%s.s%d", attempt.Label, index+1),
				Direction:    directionText,
				NativeBytes:  fmt.Sprintf("%s; %d bytes; %s", hexBytes(segment.Bytes), len(segment.Bytes), completeness),
				Offset: fmt.Sprintf("first byte %s, last byte %s after observation start; %s; uncertainty: host read-return resolution, on-wire timing Unknown",
					formatOffset(first), formatOffset(last), gap),
				Framing: framingText, Context: outcome,
			})
		}
		if len(observation.Coverage.Overflow) != 0 {
			rows = append(rows, EvidenceRow{
				SegmentOrder: attempt.Label + ".overflow", Direction: directionText,
				NativeBytes: fmt.Sprintf("%s; consumed after the segment cap, retained outside the window", hexBytes(observation.Coverage.Overflow)),
				Offset:      "at " + formatOffset(observation.Coverage.OverflowOffsets[0]) + " after observation start",
				Framing:     framingText, Context: outcome,
			})
		}
	}
	return rows
}

func repeatRows(evidence *Evidence) []Row {
	rows := []Row{
		caseRow(evidence.Attempts, CaseInitial, ""),
		caseRow(evidence.Attempts, CaseRecovery, "; prior readiness loss: Unknown, no independent readiness criterion"),
		caseRow(evidence.Attempts, CaseIdle, "; independently known readiness state: Unknown"),
		controlRow(evidence.OfflineControls, ControlRowToken),
		controlRow(evidence.OfflineControls, ControlRowWitness),
		visibilityRow(evidence.Attempts),
		contradictionRow(evidence.Attempts),
	}
	for index := range rows {
		rows[index].Field = repeatFields[index]
	}
	return rows
}

func caseRow(attempts []Attempt, kind, suffix string) Row {
	var parts []string
	windows := map[string]bool{}
	failures := 0
	for _, attempt := range attempts {
		if attempt.Case != kind {
			continue
		}
		if attempt.Observation == nil {
			parts = append(parts, attempt.Label+" open failed")
			failures++
			continue
		}
		observation := attempt.Observation
		parts = append(parts, fmt.Sprintf("%s %s %d bytes in %d segments", attempt.Label, observation.Termination,
			observation.Coverage.CapturedBytes, observation.Coverage.SegmentCount))
		windows[hexBytes(observation.Window())] = true
		if observation.Termination != TerminationCaptured {
			failures++
		}
	}
	if len(parts) == 0 {
		return Row{Result: "Unknown: no " + kind + " attempt recorded", Grade: GradeUnknown, Basis: reviewPendingBasis}
	}
	return Row{
		Result: fmt.Sprintf("%d attempts (%s); %d distinct byte windows; direction/order: receive side only, receipt order; framing: Unknown; incomplete or failed: %d%s",
			len(parts), strings.Join(parts, ", "), len(windows), failures, suffix),
		Grade: GradeUnknown, Basis: reviewPendingBasis,
	}
}

func controlRow(controls []ControlResult, row string) Row {
	var parts []string
	passed, total, positive := 0, 0, false
	for _, control := range controls {
		if control.Row == ControlRowPositive {
			positive = control.Pass
			continue
		}
		if control.Row != row {
			continue
		}
		total++
		verdict := "fail"
		if control.Pass {
			passed++
			verdict = "pass"
		}
		parts = append(parts, fmt.Sprintf("%s %s (termination %s, offline ready %t, frames sent %d)",
			control.Name, verdict, control.Termination, control.Ready, control.FramesSent))
	}
	grade := GradeUnknown
	if total > 0 && passed == total && positive {
		grade = GradeProven
	}
	return Row{
		Result: fmt.Sprintf("%d of %d passed: %s; positive synthetic control reached offline readiness: %t",
			passed, total, strings.Join(parts, "; "), positive),
		Grade: grade, Basis: offlineControlsBasis,
	}
}

func visibilityRow(attempts []Attempt) Row {
	var incomplete []string
	for _, attempt := range attempts {
		if attempt.Observation == nil || attempt.Observation.Termination != TerminationCaptured {
			incomplete = append(incomplete, attempt.Label)
		}
	}
	initiators := perAttempt(attempts, func(attempt Attempt) string { return valueOrUnknown(attempt.Operator.OtherInitiators) })
	return Row{
		Result: "attribution remains Unknown for every ordered evidence row: only the receive side of one port is visible; incomplete windows: " +
			listOrNone(incomplete) + "; other initiators per attempt: " + initiators + "; no activation is inferred",
		Grade: GradeUnknown, Basis: reviewPendingBasis,
	}
}

func contradictionRow(attempts []Attempt) Row {
	first := map[string]string{}
	var differing, unsuccessful []string
	for _, attempt := range attempts {
		if attempt.Observation == nil || attempt.Observation.Termination != TerminationCaptured {
			unsuccessful = append(unsuccessful, attempt.Label)
		}
		if attempt.Observation == nil {
			continue
		}
		window := hexBytes(attempt.Observation.Window())
		if reference, ok := first[attempt.Case]; !ok {
			first[attempt.Case] = window
		} else if reference != window {
			differing = append(differing, attempt.Label)
		}
	}
	return Row{
		Result: "windows differing from the first attempt of the same case: " + listOrNone(differing) +
			"; unsuccessful attempts: " + listOrNone(unsuccessful) + "; alternative explanations: Unknown, pending review",
		Grade: GradeUnknown, Basis: reviewPendingBasis,
	}
}

func perAttempt(attempts []Attempt, value func(Attempt) string) string {
	if len(attempts) == 0 {
		return "no attempt recorded"
	}
	parts := make([]string, len(attempts))
	for index, attempt := range attempts {
		parts[index] = attempt.Label + " " + value(attempt)
	}
	return strings.Join(parts, "; ")
}

func operatorGrade(attempts []Attempt, value func(Attempt) string) string {
	for _, attempt := range attempts {
		if v := value(attempt); v != "" && v != "unknown" {
			return GradeHypothesis
		}
	}
	return GradeUnknown
}

func operatorValue(value, basis string) string {
	text := valueOrUnknown(value)
	if basis != "" {
		text += " (basis: " + basis + ")"
	}
	return text
}

func valueOrUnknown(value string) string {
	if value == "" {
		return "unknown"
	}
	return value
}

func listOrNone(values []string) string {
	if len(values) == 0 {
		return "none"
	}
	return strings.Join(values, ", ")
}

func hexBytes(values []byte) string {
	if len(values) == 0 {
		return "(none)"
	}
	parts := make([]string, len(values))
	for index, value := range values {
		parts[index] = fmt.Sprintf("%02X", value)
	}
	return strings.Join(parts, " ")
}

func formatOffset(value time.Duration) string {
	return fmt.Sprintf("%.3f ms", float64(value)/float64(time.Millisecond))
}

// RenderMarkdown renders the worksheet sections for owner review. The JSON
// evidence file remains the exact native record.
func RenderMarkdown(evidence Evidence) string {
	var out strings.Builder
	out.WriteString("# Tesla Gen3 activation evidence worksheet: observation run\n\n")
	fmt.Fprintf(&out, "Mode: %s. Family: %s. Frames sent: %d.\n\n%s\n\n", evidence.Mode, evidence.Family, evidence.FramesSent, evidence.Disposition)
	writeRows(&out, "Preconditions and observation bounds", "Required field", evidence.Preconditions)
	out.WriteString("## Ordered native evidence\n\n")
	out.WriteString("| Segment order | Direction and attribution basis | Complete bounded native bytes and completeness | Relative monotonic offset/gap and uncertainty | Framing/integrity result | Context and outcome |\n")
	out.WriteString("| --- | --- | --- | --- | --- | --- |\n")
	if len(evidence.OrderedEvidence) == 0 {
		out.WriteString("| Unknown | Unknown | Unknown | Unknown | Unknown | Unknown |\n")
	}
	for _, row := range evidence.OrderedEvidence {
		fmt.Fprintf(&out, "| %s | %s | %s | %s | %s | %s |\n", cell(row.SegmentOrder), cell(row.Direction),
			cell(row.NativeBytes), cell(row.Offset), cell(row.Framing), cell(row.Context))
	}
	out.WriteString("\n")
	writeRows(&out, "Independent repeats and negative controls", "Evidence requirement", evidence.RepeatsAndControls)
	writeRows(&out, "Review disposition", "Required review field", evidence.Review)
	return out.String()
}

func writeRows(out *strings.Builder, heading, column string, rows []Row) {
	fmt.Fprintf(out, "## %s\n\n| %s | Result | Grade | Basis |\n| --- | --- | --- | --- |\n", heading, column)
	for _, row := range rows {
		fmt.Fprintf(out, "| %s | %s | %s | %s |\n", cell(row.Field), cell(row.Result), cell(row.Grade), cell(row.Basis))
	}
	out.WriteString("\n")
}

func cell(value string) string {
	value = strings.ReplaceAll(value, "|", "\\|")
	return strings.Join(strings.Fields(value), " ")
}
