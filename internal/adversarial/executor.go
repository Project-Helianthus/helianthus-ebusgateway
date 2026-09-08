package adversarial

import (
	"fmt"
	"time"
)

type ScenarioSpec struct {
	ScenarioID     string
	TriggerKind    string
	ExpectedEvents []string
	DurationMS     int64
}

type SeamFailure struct{ Code string }
type ActionResult struct {
	Events  []FixtureEvent
	Failure *SeamFailure
}
type ObservationResult struct {
	Baseline *FixtureSnapshot
	End      *FixtureSnapshot
	Failure  *SeamFailure
}
type FixtureAction interface {
	Execute(FixtureContext, ScenarioSpec) ActionResult
}
type FixtureObserver interface {
	Observe(FixtureContext, ScenarioSpec, []FixtureEvent) ObservationResult
}

type Executor struct {
	StartedAt time.Time
	Action    FixtureAction
	Observer  FixtureObserver
	producer  producerIdentity
	subject   string
}

func (e Executor) Run(rawDriver []byte) (ReportV1, error) {
	bound, err := BindFixtureDriverV1(rawDriver)
	if err != nil {
		return ReportV1{}, err
	}
	return e.runBound(bound)
}

func (e Executor) runBound(bound BoundFixture) (ReportV1, error) {
	if e.StartedAt.IsZero() || !e.StartedAt.Equal(e.StartedAt.UTC()) || e.StartedAt.Nanosecond()%int(time.Millisecond) != 0 {
		return ReportV1{}, fmt.Errorf("%w: execution_time", ErrInvalidFixture)
	}
	producer := e.producer.wire()
	p := Provenance{Subject: Subject{Repository: subjectRepository, Commit: e.subject, SourceTree: "clean", ArtifactKind: "gateway-fixture-set", ArtifactSHA256: fixtureSetDigest}, Producer: producer, FixtureSetSHA256: fixtureSetDigest, FixtureCaseID: bound.CaseID()}
	if !validProvenance(p) {
		return ReportV1{}, fmt.Errorf("%w: producer_identity", ErrInvalidFixture)
	}
	driver := bound.Driver()
	results := make([]ScenarioResult, 0, 4)
	counts := Summary{Total: 4}
	ctx := FixtureContext{bound: bound}
	for i := range driver.Scenarios {
		d := catalogV1[i]
		f := driver.Scenarios[i]
		start := e.StartedAt.Add(time.Duration(i) * 3 * time.Minute)
		r, err := e.runScenario(ctx, d, f, start)
		if err != nil {
			return ReportV1{}, err
		}
		results = append(results, r)
		switch r.Outcome {
		case "pass":
			counts.Passed++
		case "fail":
			counts.Failed++
		case "blocked-infra":
			counts.Blocked++
		}
	}
	counts.Verdict = "pass"
	if counts.Failed > 0 {
		counts.Verdict = "fail"
	} else if counts.Blocked > 0 {
		counts.Verdict = "blocked-infra"
	}
	r := ReportV1{Schema: ReportSchemaURL, SchemaVersion: 1, Suite: Suite{SuiteID, 1}, Execution: Execution{Mode: "offline-fixture", RunID: driver.RunID, StartedAt: stamp(e.StartedAt), CompletedAt: stamp(e.StartedAt.Add(12 * time.Minute))}, Provenance: p, Scenarios: results, Summary: counts}
	if err := ValidateRuntimeReportV1(r); err != nil {
		return ReportV1{}, fmt.Errorf("%w: projected_report", ErrInvalidSeamEvidence)
	}
	return r, nil
}

func (e Executor) runScenario(ctx FixtureContext, d Definition, f FixtureScenario, start time.Time) (ScenarioResult, error) {
	r := baseScenario(d, start)
	if !f.Precondition.Available {
		reason := *f.Precondition.UnavailableReason
		r.InfrastructureReason = &reason
		r.ResultKind = "infrastructure-block"
		r.Outcome = "blocked-infra"
		r.Errors = []ScenarioError{{"precondition", "precondition_unavailable"}}
		return r, nil
	}
	spec := ScenarioSpec{ScenarioID: d.ScenarioID, TriggerKind: d.TriggerKind, ExpectedEvents: append([]string(nil), d.ExpectedEvents...), DurationMS: d.DurationLimitMS}
	action := e.Action
	if action == nil {
		action = driverAction{scenario: f}
	}
	ar := action.Execute(ctx, spec)
	if ar.Failure != nil {
		if !oneOf(ar.Failure.Code, "trigger_rejected", "trigger_timeout", "trigger_failed") {
			return ScenarioResult{}, fmt.Errorf("%w: action_failure", ErrInvalidSeamEvidence)
		}
		if len(ar.Events) != 1 {
			return ScenarioResult{}, fmt.Errorf("%w: trigger_failure_progress", ErrInvalidSeamEvidence)
		}
		first := ar.Events[0]
		if first.Kind != d.ExpectedEvents[0] || first.OffsetMS < 0 || first.OffsetMS > d.DurationLimitMS || first.ErrorBoundMS < 0 || first.ErrorBoundMS > 1000 || first.OffsetMS+first.ErrorBoundMS > 1000 {
			return ScenarioResult{}, fmt.Errorf("%w: trigger_failure_progress", ErrInvalidSeamEvidence)
		}
		r.Action.Events = []ActionEvent{reportEvent(first, start)}
		r.Timing.ErrorBoundMS = first.ErrorBoundMS
		r.ResultKind = "execution-error"
		r.Outcome = "fail"
		r.Errors = []ScenarioError{{"trigger", ar.Failure.Code}}
		return r, nil
	}
	if err := validSeamEvents(ar.Events, d); err != nil {
		return ScenarioResult{}, err
	}
	r.Action.Events = make([]ActionEvent, len(ar.Events))
	for i, v := range ar.Events {
		r.Action.Events[i] = reportEvent(v, start)
	}
	setRecovery(&r, d)
	if d.ScenarioID == "ADV-03" {
		a, c := ar.Events[1], ar.Events[2]
		if abs64(c.OffsetMS-a.OffsetMS-60000)+a.ErrorBoundMS+c.ErrorBoundMS > 1000 {
			r.ResultKind = "execution-error"
			r.Outcome = "fail"
			r.Errors = []ScenarioError{{"evaluation", "action_duration_out_of_bounds"}}
			return r, nil
		}
	}
	if f.TerminalError != nil && f.TerminalError.Phase == "artifact" {
		r.ResultKind = "execution-error"
		r.Outcome = "fail"
		r.Errors = []ScenarioError{{f.TerminalError.Phase, f.TerminalError.Code}}
		return r, nil
	}
	observer := e.Observer
	if observer == nil {
		observer = driverObserver{scenario: f}
	}
	or := observer.Observe(ctx, spec, append([]FixtureEvent(nil), ar.Events...))
	if !validObservationResult(or, d, bounds(r.Action.Events)) {
		return ScenarioResult{}, fmt.Errorf("%w: observer_snapshot", ErrInvalidSeamEvidence)
	}
	if or.Failure != nil {
		if !oneOf(or.Failure.Code, "observer_timeout", "observer_failed") {
			return ScenarioResult{}, fmt.Errorf("%w: observer_failure", ErrInvalidSeamEvidence)
		}
		r.ResultKind = "execution-error"
		r.Outcome = "fail"
		r.Errors = []ScenarioError{{"observer", or.Failure.Code}}
		return r, nil
	}
	if or.Baseline == nil || or.End == nil {
		r.ResultKind = "execution-error"
		r.Outcome = "fail"
		r.Errors = []ScenarioError{{"evaluation", "evidence_incomplete"}}
		return r, nil
	}
	b, n := reportSnapshot(*or.Baseline, start), reportSnapshot(*or.End, start)
	r.Metrics.Baseline = &b
	r.Metrics.End = &n
	if b.CounterEpoch != n.CounterEpoch {
		r.ResultKind = "execution-error"
		r.Outcome = "fail"
		r.Errors = []ScenarioError{{"evaluation", "counter_epoch_changed"}}
		return r, nil
	}
	live := n.SemanticLiveEpoch - b.SemanticLiveEpoch
	coll := n.SemanticBusCollisionsTotal - b.SemanticBusCollisionsTotal
	if live < 0 || coll < 0 {
		r.ResultKind = "execution-error"
		r.Outcome = "fail"
		r.Errors = []ScenarioError{{"evaluation", "negative_counter_delta"}}
		return r, nil
	}
	r.Metrics.Delta = &Delta{live, coll}
	anchor, recovery := eventByKind(r.Action.Events, d.RecoveryAnchor), eventByKind(r.Action.Events, d.RecoveryEvent)
	recoveryMS := recovery.OffsetMS - anchor.OffsetMS
	recoveryBound := anchor.ErrorBoundMS + recovery.ErrorBoundMS
	mb := bounds(r.Action.Events)
	zones := n.SemanticZoneCount > 0
	dhw := n.SemanticDHWPresent
	ev := Evaluation{Duration: DurationDecision{180000, 180000, mb, 180000+mb <= 181000}, Action: ActionDecision{d.TriggerKind, d.TriggerKind, true}, Recovery: RecoveryDecision{d.MaximumRecoveryMS, recoveryMS, recoveryBound, recoveryMS+recoveryBound <= d.MaximumRecoveryMS}, LiveEpoch: MinimumDecision{2, live, live >= 2}, Zones: RequiredDecision{d.ZonesRequired, zones, !d.ZonesRequired || zones}, DHW: RequiredDecision{d.DHWRequired, dhw, !d.DHWRequired || dhw}, Collisions: MaximumDecision{d.MaximumCollisionsDelta, coll, coll <= d.MaximumCollisionsDelta}}
	r.Evaluation = &ev
	r.ResultKind = "evaluated"
	r.Outcome = "fail"
	if ev.Duration.Passed && ev.Action.Passed && ev.Recovery.Passed && ev.LiveEpoch.Passed && ev.Zones.Passed && ev.DHW.Passed && ev.Collisions.Passed {
		r.Outcome = "pass"
	}
	return r, nil
}

func validObservationResult(result ObservationResult, d Definition, bound int64) bool {
	return validFixtureSnapshotRole(result.Baseline, d.BaselinePhase, true, bound, d.DurationLimitMS) && validFixtureSnapshotRole(result.End, d.EndPhase, false, bound, d.DurationLimitMS)
}

func baseScenario(d Definition, start time.Time) ScenarioResult {
	return ScenarioResult{Definition: publicDefinition(d), Action: Action{Events: []ActionEvent{}}, Timing: Timing{ScenarioStartedAt: stamp(start), ScenarioEndedAt: stamp(start.Add(3 * time.Minute)), ElapsedMS: 180000, ErrorBoundMS: 0}, Metrics: Metrics{}, Errors: []ScenarioError{}}
}
func setRecovery(r *ScenarioResult, d Definition) {
	a, b := eventByKind(r.Action.Events, d.RecoveryAnchor), eventByKind(r.Action.Events, d.RecoveryEvent)
	if a == nil || b == nil {
		return
	}
	aa, bb := a.Kind, b.Kind
	ms := b.OffsetMS - a.OffsetMS
	r.Timing.RecoveryAnchor = &aa
	r.Timing.RecoveryObserved = &bb
	r.Timing.RecoveryMS = &ms
	r.Timing.ErrorBoundMS = bounds(r.Action.Events)
}
func validSeamEvents(es []FixtureEvent, d Definition) error {
	if len(es) != len(d.ExpectedEvents) {
		return fmt.Errorf("%w: event_count", ErrInvalidSeamEvidence)
	}
	for i, v := range es {
		if v.Kind != d.ExpectedEvents[i] || v.OffsetMS < 0 || v.OffsetMS > 180000 || v.ErrorBoundMS < 0 || v.ErrorBoundMS > 1000 || (i > 0 && v.OffsetMS < es[i-1].OffsetMS) {
			return fmt.Errorf("%w: event_sequence", ErrInvalidSeamEvidence)
		}
	}
	if es[0].OffsetMS+es[0].ErrorBoundMS > 1000 || es[1].OffsetMS+es[1].ErrorBoundMS > 1000 {
		return fmt.Errorf("%w: activation_window", ErrInvalidSeamEvidence)
	}
	return nil
}
func reportEvent(v FixtureEvent, start time.Time) ActionEvent {
	return ActionEvent{Kind: v.Kind, Source: "fixture", At: stamp(start.Add(time.Duration(v.OffsetMS) * time.Millisecond)), OffsetMS: v.OffsetMS, ErrorBoundMS: v.ErrorBoundMS}
}
func reportSnapshot(v FixtureSnapshot, start time.Time) Snapshot {
	return Snapshot{CounterEpoch: v.CounterEpoch, CapturedAt: stamp(start.Add(time.Duration(v.OffsetMS) * time.Millisecond)), OffsetMS: v.OffsetMS, SemanticStartupCurrentPhase: v.SemanticStartupCurrentPhase, SemanticLiveEpoch: v.SemanticLiveEpoch, SemanticBusCollisionsTotal: v.SemanticBusCollisionsTotal, SemanticZoneCount: v.SemanticZoneCount, SemanticDHWPresent: v.SemanticDHWPresent}
}
func stamp(v time.Time) string { return v.UTC().Format("2006-01-02T15:04:05.000Z") }

type driverAction struct{ scenario FixtureScenario }

func (d driverAction) Execute(_ FixtureContext, _ ScenarioSpec) ActionResult {
	if d.scenario.TerminalError != nil && d.scenario.TerminalError.Phase == "trigger" {
		return ActionResult{Events: append([]FixtureEvent(nil), d.scenario.Events[:1]...), Failure: &SeamFailure{d.scenario.TerminalError.Code}}
	}
	return ActionResult{Events: append([]FixtureEvent(nil), d.scenario.Events...)}
}

type driverObserver struct{ scenario FixtureScenario }

func (d driverObserver) Observe(_ FixtureContext, _ ScenarioSpec, _ []FixtureEvent) ObservationResult {
	if d.scenario.TerminalError != nil && d.scenario.TerminalError.Phase == "observer" {
		return ObservationResult{Failure: &SeamFailure{d.scenario.TerminalError.Code}}
	}
	var b, n *FixtureSnapshot
	if d.scenario.Observations.Baseline != nil {
		x := *d.scenario.Observations.Baseline
		b = &x
	}
	if d.scenario.Observations.End != nil {
		x := *d.scenario.Observations.End
		n = &x
	}
	return ObservationResult{Baseline: b, End: n}
}
