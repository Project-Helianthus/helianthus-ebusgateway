package adversarial

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"time"
)

var (
	ErrInvalidFixture      = errors.New("invalid adversarial fixture v1")
	ErrInvalidReport       = errors.New("invalid adversarial runtime report v1")
	ErrInvalidSeamEvidence = errors.New("invalid adversarial fixture seam evidence")
	uuidV4                 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	hex40                  = regexp.MustCompile(`^[0-9a-f]{40}$`)
	hex64                  = regexp.MustCompile(`^[0-9a-f]{64}$`)
	caseIDPattern          = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)
)

const safeInteger = int64(9007199254740991)

func validInt(v int64) bool { return v >= 0 && v <= safeInteger }
func validStamp(s string) (time.Time, bool) {
	t, e := time.Parse("2006-01-02T15:04:05.000Z", s)
	return t, e == nil && t.UTC().Format("2006-01-02T15:04:05.000Z") == s
}
func oneOf(v string, values ...string) bool {
	for _, x := range values {
		if v == x {
			return true
		}
	}
	return false
}

func validateFixture(f FixtureV1) string {
	if f.Schema != FixtureSchemaURL || f.SchemaVersion != 1 || f.Suite.ID != SuiteID || f.Suite.Version != 1 {
		return "contract_identity"
	}
	if !caseIDPattern.MatchString(f.FixtureCaseID) || len(f.FixtureCaseID) > 48 || !uuidV4.MatchString(f.RunID) || len(f.Scenarios) != 4 {
		return "fixture_identity"
	}
	for i, s := range f.Scenarios {
		d := catalogV1[i]
		if s.ScenarioID != d.ScenarioID || s.TriggerKind != d.TriggerKind {
			return "scenario_sequence"
		}
		if s.Precondition.Available == (s.Precondition.UnavailableReason != nil) {
			return "precondition_shape"
		}
		if s.Precondition.UnavailableReason != nil && !validInfraReasonFor(d.ScenarioID, *s.Precondition.UnavailableReason) {
			return "precondition_reason"
		}
		if s.Events == nil || s.ResourceArtifactIDs == nil {
			return "required_array"
		}
		if len(s.ResourceArtifactIDs) > 1 {
			return "resource_cardinality"
		}
		for _, id := range s.ResourceArtifactIDs {
			if len(id) > 64 || !caseIDPattern.MatchString(id) {
				return "resource_identifier"
			}
		}
		if !s.Precondition.Available {
			if len(s.Events) != 0 || s.Observations.Baseline != nil || s.Observations.End != nil || s.TerminalError != nil || len(s.ResourceArtifactIDs) != 0 {
				return "precondition_precedence"
			}
			continue
		}
		if (i < 3 && len(s.ResourceArtifactIDs) != 0) || (i == 3 && len(s.ResourceArtifactIDs) != 1) {
			return "resource_scenario"
		}
		if len(s.Events) == 0 {
			return "event_sequence"
		}
		for j, e := range s.Events {
			if j >= len(d.ExpectedEvents) || e.Kind != d.ExpectedEvents[j] || !validInt(e.OffsetMS) || e.OffsetMS > d.DurationLimitMS || e.ErrorBoundMS < 0 || e.ErrorBoundMS > 1000 || (j > 0 && e.OffsetMS < s.Events[j-1].OffsetMS) {
				return "event_sequence"
			}
		}
		if s.Events[0].OffsetMS+s.Events[0].ErrorBoundMS > 1000 || (len(s.Events) > 1 && s.Events[1].OffsetMS+s.Events[1].ErrorBoundMS > 1000) {
			return "activation_window"
		}
		if len(s.Events) != len(d.ExpectedEvents) {
			return "event_sequence"
		}
		if s.ScenarioID == "ADV-03" {
			a, c := s.Events[1], s.Events[2]
			if abs64(c.OffsetMS-a.OffsetMS-60000)+a.ErrorBoundMS+c.ErrorBoundMS > 1000 {
				return "action_duration"
			}
		}
		if (s.Observations.Baseline != nil && !validFixtureSnapshot(*s.Observations.Baseline)) || (s.Observations.End != nil && !validFixtureSnapshot(*s.Observations.End)) {
			return "observation_value"
		}
		if s.TerminalError != nil {
			if !validTerminal(*s.TerminalError) {
				return "terminal_error"
			}
			continue
		}
		if s.Observations.Baseline == nil || s.Observations.End == nil {
			return "observation_shape"
		}
	}
	return ""
}

func validFixtureSnapshot(s FixtureSnapshot) bool {
	return uuidV4.MatchString(s.CounterEpoch) && validInt(s.OffsetMS) && s.OffsetMS <= 180000 && oneOf(s.SemanticStartupCurrentPhase, "BOOT_INIT", "CACHE_LOADED_STALE", "LIVE_WARMUP", "LIVE_READY", "DEGRADED") && validInt(s.SemanticLiveEpoch) && validInt(s.SemanticBusCollisionsTotal) && s.SemanticZoneCount >= 0 && s.SemanticZoneCount <= 20
}
func validInfraReasonFor(id, s string) bool {
	if s == "observer_unavailable" {
		return true
	}
	switch id {
	case "ADV-01":
		return s == "ha_harness_unavailable"
	case "ADV-02":
		return s == "adapter_control_unavailable"
	case "ADV-03":
		return s == "network_fault_injector_unavailable"
	case "ADV-04":
		return s == "isolated_cache_sandbox_unavailable"
	}
	return false
}
func validTerminal(e TerminalError) bool {
	return oneOf(e.Phase, "precondition", "trigger", "observer", "evaluation", "artifact") && oneOf(e.Code, "precondition_unavailable", "trigger_rejected", "trigger_timeout", "trigger_failed", "observer_timeout", "observer_failed", "counter_epoch_changed", "negative_counter_delta", "action_duration_out_of_bounds", "evidence_incomplete")
}

func ValidateRuntimeReportV1(r ReportV1) error {
	if rule := validateRuntime(r); rule != "" {
		return fmt.Errorf("%w: %s", ErrInvalidReport, rule)
	}
	return nil
}

func validateRuntime(r ReportV1) string {
	if r.Schema != ReportSchemaURL || r.SchemaVersion != 1 || r.Suite.ID != SuiteID || r.Suite.Version != 1 {
		return "contract_identity"
	}
	if r.Execution.Mode != "offline-fixture" || !uuidV4.MatchString(r.Execution.RunID) {
		return "execution_identity"
	}
	start, ok := validStamp(r.Execution.StartedAt)
	if !ok {
		return "execution_time"
	}
	complete, ok := validStamp(r.Execution.CompletedAt)
	if !ok || !complete.Equal(start.Add(12*time.Minute)) {
		return "execution_time"
	}
	if !validProvenance(r.Provenance) {
		return "provenance"
	}
	if len(r.Scenarios) != 4 {
		return "scenario_sequence"
	}
	counts := Summary{Total: 4}
	for i := range r.Scenarios {
		expectedStart := start.Add(time.Duration(i) * 3 * time.Minute)
		if rule := validateScenario(r.Scenarios[i], catalogV1[i], expectedStart); rule != "" {
			return rule
		}
		switch r.Scenarios[i].Outcome {
		case "pass":
			counts.Passed++
		case "fail":
			counts.Failed++
		case "blocked-infra":
			counts.Blocked++
		default:
			return "outcome"
		}
	}
	counts.Verdict = "pass"
	if counts.Failed > 0 {
		counts.Verdict = "fail"
	} else if counts.Blocked > 0 {
		counts.Verdict = "blocked-infra"
	}
	if !reflect.DeepEqual(r.Summary, counts) {
		return "summary"
	}
	if r.Provenance.Subject.SourceTree == "dirty" && counts.Passed > 0 {
		return "dirty_provenance"
	}
	return ""
}

func validProvenance(p Provenance) bool {
	if p.Subject.Repository != subjectRepository || p.Subject.Commit != subjectCommit || !oneOf(p.Subject.SourceTree, "clean", "dirty") || p.Subject.ArtifactKind != "gateway-fixture-set" || !hex64.MatchString(p.Subject.ArtifactSHA256) || p.Subject.ArtifactSHA256 != p.FixtureSetSHA256 || p.FixtureSetSHA256 != fixtureSetDigest {
		return false
	}
	if !oneOf(p.FixtureCaseID, "evaluated-fail", "execution-error", "infrastructure-block", "offline-all-pass") {
		return false
	}
	q := p.Producer
	if q.Repository != subjectRepository || !hex40.MatchString(q.Commit) || !hex64.MatchString(q.BuildSHA256) {
		return false
	}
	return q.Component == "internal/adversarial" && q.BuildKind == "go-test-binary" && q.InputGatewayReportSHA256 == nil
}

func publicDefinition(d Definition) Definition {
	d.ExpectedEvents = nil
	d.RecoveryAnchor = ""
	d.RecoveryEvent = ""
	d.BaselinePhase = ""
	d.EndPhase = ""
	return d
}
func validateScenario(s ScenarioResult, d Definition, start time.Time) string {
	if !reflect.DeepEqual(s.Definition, publicDefinition(d)) {
		return "definition"
	}
	ss, ok := validStamp(s.Timing.ScenarioStartedAt)
	if !ok || !ss.Equal(start) {
		return "serial_timing"
	}
	se, ok := validStamp(s.Timing.ScenarioEndedAt)
	if !ok || !se.Equal(start.Add(3*time.Minute)) || s.Timing.ElapsedMS != 180000 || s.Timing.ErrorBoundMS < 0 || s.Timing.ErrorBoundMS > 1000 {
		return "serial_timing"
	}
	if s.Action.Events == nil || s.Errors == nil {
		return "required_array"
	}
	for i, e := range s.Action.Events {
		if i >= len(d.ExpectedEvents) || e.Kind != d.ExpectedEvents[i] || e.Source != "fixture" || e.OffsetMS < 0 || e.OffsetMS > 180000 || e.ErrorBoundMS < 0 || e.ErrorBoundMS > 1000 || (i > 0 && e.OffsetMS < s.Action.Events[i-1].OffsetMS) {
			return "action_evidence"
		}
		at, ok := validStamp(e.At)
		if !ok || !at.Equal(start.Add(time.Duration(e.OffsetMS)*time.Millisecond)) {
			return "action_timestamp"
		}
	}
	if len(s.Action.Events) > 0 && s.Action.Events[0].OffsetMS+s.Action.Events[0].ErrorBoundMS > 1000 {
		return "activation_window"
	}
	if len(s.Action.Events) > 1 && s.Action.Events[1].OffsetMS+s.Action.Events[1].ErrorBoundMS > 1000 {
		return "activation_window"
	}
	switch s.ResultKind {
	case "infrastructure-block":
		return validateInfra(s)
	case "execution-error":
		return validateExecutionError(s, d)
	case "evaluated":
		return validateEvaluated(s, d)
	default:
		return "result_kind"
	}
}

func validateInfra(s ScenarioResult) string {
	if s.Outcome != "blocked-infra" || len(s.Action.Events) != 0 || s.Evaluation != nil || s.Metrics.Baseline != nil || s.Metrics.End != nil || s.Metrics.Delta != nil || s.InfrastructureReason == nil || !validInfraReasonFor(s.Definition.ScenarioID, *s.InfrastructureReason) || len(s.Errors) != 1 || s.Errors[0] != (ScenarioError{"precondition", "precondition_unavailable"}) || !allRecoveryNil(s.Timing) || s.Timing.ErrorBoundMS != 0 {
		return "infrastructure_precedence"
	}
	return ""
}

func validateExecutionError(s ScenarioResult, d Definition) string {
	if s.Outcome != "fail" || s.Evaluation != nil || s.InfrastructureReason != nil || len(s.Errors) != 1 || !validTerminal(TerminalError(s.Errors[0])) {
		return "execution_error_shape"
	}
	e := s.Errors[0]
	switch e.Code {
	case "trigger_rejected", "trigger_timeout", "trigger_failed":
		if e.Phase != "trigger" || len(s.Action.Events) != 1 || !allRecoveryNil(s.Timing) || s.Timing.ErrorBoundMS != bounds(s.Action.Events) || s.Metrics.Baseline != nil || s.Metrics.End != nil || s.Metrics.Delta != nil {
			return "trigger_error_evidence"
		}
	case "observer_timeout", "observer_failed":
		if e.Phase != "observer" || len(s.Action.Events) != len(d.ExpectedEvents) || !validRecovery(s, d) || s.Metrics.Baseline != nil || s.Metrics.End != nil || s.Metrics.Delta != nil {
			return "observer_error_evidence"
		}
	case "counter_epoch_changed", "negative_counter_delta":
		if e.Phase != "evaluation" || len(s.Action.Events) != len(d.ExpectedEvents) || !validRecovery(s, d) || s.Metrics.Baseline == nil || s.Metrics.End == nil || s.Metrics.Delta != nil {
			return "continuity_error_evidence"
		}
		if !validSnapshots(s, d) {
			return "continuity_error_evidence"
		}
		b, n := s.Metrics.Baseline, s.Metrics.End
		if e.Code == "counter_epoch_changed" && b.CounterEpoch == n.CounterEpoch {
			return "continuity_error_evidence"
		}
		if e.Code == "negative_counter_delta" && (b.CounterEpoch != n.CounterEpoch || (n.SemanticLiveEpoch >= b.SemanticLiveEpoch && n.SemanticBusCollisionsTotal >= b.SemanticBusCollisionsTotal)) {
			return "continuity_error_evidence"
		}
	case "action_duration_out_of_bounds":
		if e.Phase != "evaluation" || d.ScenarioID != "ADV-03" || len(s.Action.Events) != len(d.ExpectedEvents) || !validRecovery(s, d) || s.Metrics.Baseline != nil || s.Metrics.End != nil || s.Metrics.Delta != nil {
			return "action_duration_error"
		}
		a, c := s.Action.Events[1], s.Action.Events[2]
		if abs64(c.OffsetMS-a.OffsetMS-60000)+a.ErrorBoundMS+c.ErrorBoundMS <= 1000 {
			return "action_duration_error"
		}
	case "evidence_incomplete":
		if !oneOf(e.Phase, "evaluation", "artifact") || len(s.Action.Events) != len(d.ExpectedEvents) || s.Metrics.Delta != nil || !validRecovery(s, d) {
			return "evidence_error"
		}
		if !validPresentSnapshots(s) {
			return "evidence_snapshot"
		}
		if validSnapshots(s, d) {
			return "evidence_error"
		}
	default:
		return "execution_error_code"
	}
	return ""
}

func validateEvaluated(s ScenarioResult, d Definition) string {
	if s.Outcome != "pass" && s.Outcome != "fail" {
		return "evaluated_outcome"
	}
	if len(s.Action.Events) != len(d.ExpectedEvents) || s.Evaluation == nil || s.InfrastructureReason != nil || len(s.Errors) != 0 || s.Metrics.Baseline == nil || s.Metrics.End == nil || s.Metrics.Delta == nil {
		return "evaluated_shape"
	}
	if d.ScenarioID == "ADV-03" {
		a, c := s.Action.Events[1], s.Action.Events[2]
		if abs64(c.OffsetMS-a.OffsetMS-60000)+a.ErrorBoundMS+c.ErrorBoundMS > 1000 {
			return "action_duration"
		}
	}
	if !validRecovery(s, d) || !validSnapshots(s, d) {
		return "evaluated_evidence"
	}
	b, n := s.Metrics.Baseline, s.Metrics.End
	if b.CounterEpoch != n.CounterEpoch || n.SemanticLiveEpoch < b.SemanticLiveEpoch || n.SemanticBusCollisionsTotal < b.SemanticBusCollisionsTotal {
		return "continuity_precedence"
	}
	live := n.SemanticLiveEpoch - b.SemanticLiveEpoch
	coll := n.SemanticBusCollisionsTotal - b.SemanticBusCollisionsTotal
	if s.Metrics.Delta.SemanticLiveEpoch != live || s.Metrics.Delta.SemanticBusCollisionsTotal != coll {
		return "delta"
	}
	maxBound := int64(0)
	for _, e := range s.Action.Events {
		if e.ErrorBoundMS > maxBound {
			maxBound = e.ErrorBoundMS
		}
	}
	anchor, recovery := eventByKind(s.Action.Events, d.RecoveryAnchor), eventByKind(s.Action.Events, d.RecoveryEvent)
	recoveryMS := recovery.OffsetMS - anchor.OffsetMS
	recoveryBound := anchor.ErrorBoundMS + recovery.ErrorBoundMS
	zones := n.SemanticZoneCount > 0
	dhw := n.SemanticDHWPresent
	expected := Evaluation{Duration: DurationDecision{180000, 180000, maxBound, 180000+maxBound <= 181000}, Action: ActionDecision{d.TriggerKind, d.TriggerKind, true}, Recovery: RecoveryDecision{d.MaximumRecoveryMS, recoveryMS, recoveryBound, recoveryMS+recoveryBound <= d.MaximumRecoveryMS}, LiveEpoch: MinimumDecision{2, live, live >= 2}, Zones: RequiredDecision{d.ZonesRequired, zones, !d.ZonesRequired || zones}, DHW: RequiredDecision{d.DHWRequired, dhw, !d.DHWRequired || dhw}, Collisions: MaximumDecision{d.MaximumCollisionsDelta, coll, coll <= d.MaximumCollisionsDelta}}
	if !reflect.DeepEqual(*s.Evaluation, expected) {
		return "evaluation"
	}
	pass := expected.Duration.Passed && expected.Action.Passed && expected.Recovery.Passed && expected.LiveEpoch.Passed && expected.Zones.Passed && expected.DHW.Passed && expected.Collisions.Passed
	if (s.Outcome == "pass") != pass {
		return "evaluated_outcome"
	}
	return ""
}

func validSnapshots(s ScenarioResult, d Definition) bool {
	b, n := s.Metrics.Baseline, s.Metrics.End
	if b == nil || n == nil {
		return false
	}
	if !validSnapshot(*b) || !validSnapshot(*n) || b.SemanticStartupCurrentPhase != d.BaselinePhase || n.SemanticStartupCurrentPhase != d.EndPhase {
		return false
	}
	if b.OffsetMS-bounds(s.Action.Events) > 0 || n.OffsetMS+bounds(s.Action.Events) < s.Timing.ElapsedMS {
		return false
	}
	bs, _ := validStamp(b.CapturedAt)
	ns, _ := validStamp(n.CapturedAt)
	ss, _ := validStamp(s.Timing.ScenarioStartedAt)
	return bs.Equal(ss.Add(time.Duration(b.OffsetMS)*time.Millisecond)) && ns.Equal(ss.Add(time.Duration(n.OffsetMS)*time.Millisecond))
}
func validPresentSnapshots(s ScenarioResult) bool {
	start, ok := validStamp(s.Timing.ScenarioStartedAt)
	if !ok {
		return false
	}
	for _, snapshot := range []*Snapshot{s.Metrics.Baseline, s.Metrics.End} {
		if snapshot == nil {
			continue
		}
		if !validSnapshot(*snapshot) {
			return false
		}
		captured, ok := validStamp(snapshot.CapturedAt)
		if !ok || !captured.Equal(start.Add(time.Duration(snapshot.OffsetMS)*time.Millisecond)) {
			return false
		}
	}
	return true
}
func validSnapshot(s Snapshot) bool {
	return uuidV4.MatchString(s.CounterEpoch) && validInt(s.OffsetMS) && s.OffsetMS <= 180000 && oneOf(s.SemanticStartupCurrentPhase, "BOOT_INIT", "CACHE_LOADED_STALE", "LIVE_WARMUP", "LIVE_READY", "DEGRADED") && validInt(s.SemanticLiveEpoch) && validInt(s.SemanticBusCollisionsTotal) && s.SemanticZoneCount >= 0 && s.SemanticZoneCount <= 20
}
func bounds(e []ActionEvent) int64 {
	m := int64(0)
	for _, x := range e {
		if x.ErrorBoundMS > m {
			m = x.ErrorBoundMS
		}
	}
	return m
}
func eventByKind(es []ActionEvent, k string) *ActionEvent {
	for i := range es {
		if es[i].Kind == k {
			return &es[i]
		}
	}
	return nil
}
func allRecoveryNil(t Timing) bool {
	return t.RecoveryAnchor == nil && t.RecoveryObserved == nil && t.RecoveryMS == nil
}
func validRecovery(s ScenarioResult, d Definition) bool {
	a, r := eventByKind(s.Action.Events, d.RecoveryAnchor), eventByKind(s.Action.Events, d.RecoveryEvent)
	if a == nil || r == nil || s.Timing.RecoveryAnchor == nil || s.Timing.RecoveryObserved == nil || s.Timing.RecoveryMS == nil {
		return false
	}
	return *s.Timing.RecoveryAnchor == d.RecoveryAnchor && *s.Timing.RecoveryObserved == d.RecoveryEvent && *s.Timing.RecoveryMS == r.OffsetMS-a.OffsetMS && s.Timing.ErrorBoundMS == bounds(s.Action.Events)
}
func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}
