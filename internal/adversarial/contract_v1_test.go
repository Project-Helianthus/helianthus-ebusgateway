package adversarial

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"
)

var fixtureCases = []string{"evaluated-fail", "execution-error", "infrastructure-block", "offline-all-pass"}

const (
	testProducerCommit = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testProducerSHA256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func fixtureBytes(t *testing.T, kind, name string) []byte {
	t.Helper()
	b, e := fixtureFiles.ReadFile("fixtures/v1/" + kind + "/" + name + ".json")
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func canonicalPublisher() Publisher {
	publisher, err := newPublisher(func() (*debug.BuildInfo, bool) {
		return &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: testProducerCommit}, {Key: "vcs.modified", Value: "false"}}}, true
	}, func() (string, error) { return testProducerSHA256, nil })
	if err != nil {
		panic(err)
	}
	return publisher
}

func canonicalRunner() Executor {
	return canonicalPublisher().NewExecutor(time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC), nil, nil)
}

func TestFixtureCorpusParsesBindsExecutesAndProjects(t *testing.T) {
	for _, name := range fixtureCases {
		t.Run(name, func(t *testing.T) {
			driver := fixtureBytes(t, "inputs", name)
			f, e := ParseFixtureV1(driver)
			if e != nil {
				t.Fatal(e)
			}
			if f.FixtureCaseID != name {
				t.Fatal(f.FixtureCaseID)
			}
			bound, e := BindFixtureDriverV1(driver)
			if e != nil {
				t.Fatal(e)
			}
			if _, ok := bound.resources[bound.caseInfo.ResourceArtifactIDs[0]]; !ok {
				t.Fatal("resource not bound")
			}
			actual, e := canonicalRunner().Run(driver)
			if e != nil {
				t.Fatal(e)
			}
			if e := ValidateFixtureProjectionV1(actual, bound); e != nil {
				t.Fatal(e)
			}
		})
	}
}

func TestRuntimeReportAllowsDynamicTimestamp(t *testing.T) {
	r := canonicalRunner()
	r.StartedAt = r.StartedAt.Add(24 * time.Hour)
	report, e := r.Run(fixtureBytes(t, "inputs", "offline-all-pass"))
	if e != nil {
		t.Fatal(e)
	}
	if e = ValidateReport(report); e != nil {
		t.Fatal(e)
	}
	raw, _ := json.Marshal(report)
	if e = ValidateReportBytes(raw); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "nested", "report.json")
	if e = canonicalPublisher().WriteReport(report, path); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(path); e != nil {
		t.Fatal(e)
	}
	bound, _ := BindFixtureDriverV1(fixtureBytes(t, "inputs", "offline-all-pass"))
	if e = ValidateFixtureProjectionV1(report, bound); e != nil {
		t.Fatalf("dynamic deterministic projection rejected: %v", e)
	}
}

type actionFailure struct {
	code          string
	offset, bound int64
}

func (a actionFailure) Execute(_ FixtureContext, spec ScenarioSpec) ActionResult {
	return ActionResult{Events: []FixtureEvent{{Kind: spec.ExpectedEvents[0], OffsetMS: a.offset, ErrorBoundMS: a.bound}}, Failure: &SeamFailure{a.code}}
}

type observerFailure struct{ code string }

func (a observerFailure) Observe(FixtureContext, ScenarioSpec, []FixtureEvent) ObservationResult {
	return ObservationResult{Failure: &SeamFailure{a.code}}
}

func TestSeamFailuresAreWritable(t *testing.T) {
	for _, code := range []string{"trigger_rejected", "trigger_timeout", "trigger_failed"} {
		for _, uncertainty := range []struct{ offset, bound int64 }{{0, 0}, {900, 100}} {
			r := canonicalRunner()
			r.Action = actionFailure{code: code, offset: uncertainty.offset, bound: uncertainty.bound}
			report, e := r.Run(fixtureBytes(t, "inputs", "offline-all-pass"))
			if e != nil {
				t.Fatal(e)
			}
			for _, s := range report.Scenarios {
				if len(s.Action.Events) != 1 || s.Errors[0].Code != code || s.Timing.ErrorBoundMS != uncertainty.bound {
					t.Fatalf("%s/%d: %#v", code, uncertainty.bound, s)
				}
			}
			if e = canonicalPublisher().WriteReport(report, filepath.Join(t.TempDir(), fmt.Sprintf("%s-%d.json", code, uncertainty.bound))); e != nil {
				t.Fatal(e)
			}
		}
	}
	for _, code := range []string{"observer_timeout", "observer_failed"} {
		r := canonicalRunner()
		r.Observer = observerFailure{code}
		report, e := r.Run(fixtureBytes(t, "inputs", "offline-all-pass"))
		if e != nil {
			t.Fatal(e)
		}
		for i, s := range report.Scenarios {
			if len(s.Action.Events) != len(catalogV1[i].ExpectedEvents) || s.Timing.RecoveryMS == nil || s.Errors[0].Code != code {
				t.Fatalf("%s: %#v", code, s)
			}
		}
		if e = canonicalPublisher().WriteReport(report, filepath.Join(t.TempDir(), code+".json")); e != nil {
			t.Fatal(e)
		}
	}
}

func TestADV03ObserverErrorsRequireValidPartitionDuration(t *testing.T) {
	for _, code := range []string{"observer_timeout", "observer_failed"} {
		t.Run(code, func(t *testing.T) {
			runner := canonicalRunner()
			runner.Observer = observerFailure{code}
			base, err := runner.Run(fixtureBytes(t, "inputs", "offline-all-pass"))
			if err != nil {
				t.Fatal(err)
			}

			inclusive := cloneReport(base)
			setADV03PartitionClearedOffset(t, &inclusive, 59000)
			if err := ValidateReport(inclusive); err != nil {
				t.Fatalf("inclusive 1000 ms bound rejected: %v", err)
			}

			invalid := cloneReport(base)
			setADV03PartitionClearedOffset(t, &invalid, 58000)
			if err := ValidateReport(invalid); !errors.Is(err, ErrInvalidReport) {
				t.Fatalf("ValidateReport accepted 58-second partition: %v", err)
			}
			raw, _ := json.Marshal(invalid)
			if err := ValidateReportBytes(raw); !errors.Is(err, ErrInvalidReport) {
				t.Fatalf("ValidateReportBytes accepted 58-second partition: %v", err)
			}
			path := filepath.Join(t.TempDir(), code+"-invalid-duration.json")
			if err := canonicalPublisher().WriteReport(invalid, path); !errors.Is(err, ErrInvalidReport) {
				t.Fatalf("WriteReport accepted 58-second partition: %v", err)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("destination created: %v", err)
			}

			durationError := cloneReport(invalid)
			s := &durationError.Scenarios[2]
			s.Errors = []ScenarioError{{Phase: "evaluation", Code: "action_duration_out_of_bounds"}}
			if err := ValidateReport(durationError); err != nil {
				t.Fatalf("duration error rejected: %v", err)
			}
		})
	}
}

func setADV03PartitionClearedOffset(t *testing.T, report *ReportV1, offset int64) {
	t.Helper()
	s := &report.Scenarios[2]
	start, ok := validStamp(s.Timing.ScenarioStartedAt)
	if !ok {
		t.Fatal("invalid scenario start")
	}
	s.Action.Events[2].OffsetMS = offset
	s.Action.Events[2].At = stamp(start.Add(time.Duration(offset) * time.Millisecond))
	recovery := s.Action.Events[3].OffsetMS - offset
	s.Timing.RecoveryMS = &recovery
}

type invalidTriggerFailure struct{ mode string }

func (a invalidTriggerFailure) Execute(_ FixtureContext, s ScenarioSpec) ActionResult {
	events := []FixtureEvent{}
	switch a.mode {
	case "wrong":
		events = []FixtureEvent{{Kind: "not_the_request", OffsetMS: 0, ErrorBoundMS: 0}}
	case "multiple":
		events = []FixtureEvent{{Kind: s.ExpectedEvents[0], OffsetMS: 0, ErrorBoundMS: 0}, {Kind: s.ExpectedEvents[1], OffsetMS: 1, ErrorBoundMS: 0}}
	}
	return ActionResult{Events: events, Failure: &SeamFailure{Code: "trigger_failed"}}
}

func TestTriggerFailureRequiresSeamSuppliedProgress(t *testing.T) {
	for _, mode := range []string{"none", "wrong", "multiple"} {
		r := canonicalRunner()
		r.Action = invalidTriggerFailure{mode}
		report, err := r.Run(fixtureBytes(t, "inputs", "offline-all-pass"))
		if !errors.Is(err, ErrInvalidSeamEvidence) || !reflect.DeepEqual(report, ReportV1{}) {
			t.Fatalf("%s: report=%#v err=%v", mode, report, err)
		}
	}
}

type mutateObserver struct {
	driver FixtureV1
	mode   string
}

func (m mutateObserver) Observe(_ FixtureContext, s ScenarioSpec, _ []FixtureEvent) ObservationResult {
	var f FixtureScenario
	for _, x := range m.driver.Scenarios {
		if x.ScenarioID == s.ScenarioID {
			f = x
		}
	}
	b, n := *f.Observations.Baseline, *f.Observations.End
	if s.ScenarioID != "ADV-01" {
		return ObservationResult{Baseline: &b, End: &n}
	}
	switch m.mode {
	case "epoch":
		n.CounterEpoch = "00000000-0000-4000-8000-000000000099"
	case "live":
		n.SemanticLiveEpoch = b.SemanticLiveEpoch - 1
	case "collision":
		n.SemanticBusCollisionsTotal = b.SemanticBusCollisionsTotal - 1
	}
	return ObservationResult{Baseline: &b, End: &n}
}

type scenarioObserver struct {
	driver     FixtureV1
	scenarioID string
	result     ObservationResult
}

func (o scenarioObserver) Observe(_ FixtureContext, spec ScenarioSpec, _ []FixtureEvent) ObservationResult {
	if spec.ScenarioID == o.scenarioID {
		result := o.result
		if result.Baseline != nil {
			baseline := *result.Baseline
			result.Baseline = &baseline
		}
		if result.End != nil {
			end := *result.End
			result.End = &end
		}
		if result.Failure != nil {
			failure := *result.Failure
			result.Failure = &failure
		}
		return result
	}
	for _, scenario := range o.driver.Scenarios {
		if scenario.ScenarioID == spec.ScenarioID {
			baseline, end := *scenario.Observations.Baseline, *scenario.Observations.End
			return ObservationResult{Baseline: &baseline, End: &end}
		}
	}
	return ObservationResult{}
}

func TestObserverSeamRejectsMalformedSnapshotsBeforeCompleteness(t *testing.T) {
	driver := fixtureBytes(t, "inputs", "offline-all-pass")
	fixture, err := ParseFixtureV1(driver)
	if err != nil {
		t.Fatal(err)
	}
	baseline := *fixture.Scenarios[0].Observations.Baseline
	end := *fixture.Scenarios[0].Observations.End
	cases := []struct {
		name    string
		failure bool
		mutate  func(*FixtureSnapshot, *FixtureSnapshot)
	}{
		{"baseline_uuid", false, func(b, _ *FixtureSnapshot) { b.CounterEpoch = "not-a-uuid" }},
		{"end_uuid", false, func(_, n *FixtureSnapshot) { n.CounterEpoch = "not-a-uuid" }},
		{"negative_offset", false, func(b, _ *FixtureSnapshot) { b.OffsetMS = -1 }},
		{"offset_above_scenario", false, func(_, n *FixtureSnapshot) { n.OffsetMS = 180001 }},
		{"baseline_offset_role", false, func(b, _ *FixtureSnapshot) { b.OffsetMS = 1 }},
		{"end_offset_role", false, func(_, n *FixtureSnapshot) { n.OffsetMS = 179999 }},
		{"unknown_phase", false, func(b, _ *FixtureSnapshot) { b.SemanticStartupCurrentPhase = "UNKNOWN" }},
		{"wrong_scenario_phase", false, func(b, _ *FixtureSnapshot) { b.SemanticStartupCurrentPhase = "BOOT_INIT" }},
		{"negative_live_counter", false, func(b, _ *FixtureSnapshot) { b.SemanticLiveEpoch = -1 }},
		{"unsafe_collision_counter", false, func(_, n *FixtureSnapshot) { n.SemanticBusCollisionsTotal = safeInteger + 1 }},
		{"negative_zone_count", false, func(_, n *FixtureSnapshot) { n.SemanticZoneCount = -1 }},
		{"malformed_with_observer_failure", true, func(b, _ *FixtureSnapshot) { b.CounterEpoch = "not-a-uuid" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, n := baseline, end
			tc.mutate(&b, &n)
			result := ObservationResult{Baseline: &b, End: &n}
			if tc.failure {
				result.Failure = &SeamFailure{Code: "observer_failed"}
			}
			runner := canonicalRunner()
			runner.Observer = scenarioObserver{driver: fixture, scenarioID: "ADV-01", result: result}
			report, err := runner.Run(driver)
			if !errors.Is(err, ErrInvalidSeamEvidence) || !reflect.DeepEqual(report, ReportV1{}) {
				t.Fatalf("report=%#v err=%v", report, err)
			}
			path := filepath.Join(t.TempDir(), "malformed-observer.json")
			if err := canonicalPublisher().WriteReport(report, path); !errors.Is(err, ErrInvalidReport) {
				t.Fatalf("zero report write: %v", err)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("destination created: %v", err)
			}
		})
	}
}

func TestObserverSeamValidPartialAndAbsentRemainAllNullErrors(t *testing.T) {
	driver := fixtureBytes(t, "inputs", "offline-all-pass")
	fixture, err := ParseFixtureV1(driver)
	if err != nil {
		t.Fatal(err)
	}
	baseline := *fixture.Scenarios[0].Observations.Baseline
	end := *fixture.Scenarios[0].Observations.End
	cases := []struct {
		name   string
		result ObservationResult
		code   string
	}{
		{"baseline_only", ObservationResult{Baseline: &baseline}, "evidence_incomplete"},
		{"end_only", ObservationResult{End: &end}, "evidence_incomplete"},
		{"absent", ObservationResult{}, "evidence_incomplete"},
		{"failure_with_valid_baseline", ObservationResult{Baseline: &baseline, Failure: &SeamFailure{Code: "observer_failed"}}, "observer_failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runner := canonicalRunner()
			runner.Observer = scenarioObserver{driver: fixture, scenarioID: "ADV-01", result: tc.result}
			report, err := runner.Run(driver)
			if err != nil {
				t.Fatal(err)
			}
			scenario := report.Scenarios[0]
			if scenario.ResultKind != "execution-error" || len(scenario.Errors) != 1 || scenario.Errors[0].Code != tc.code || scenario.Metrics.Baseline != nil || scenario.Metrics.End != nil || scenario.Metrics.Delta != nil {
				t.Fatalf("unexpected projection: %#v", scenario)
			}
			if err := canonicalPublisher().WriteReport(report, filepath.Join(t.TempDir(), tc.name+".json")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestContinuityFailuresAreWritable(t *testing.T) {
	driver := fixtureBytes(t, "inputs", "offline-all-pass")
	f, _ := ParseFixtureV1(driver)
	for _, mode := range []string{"epoch", "live", "collision"} {
		r := canonicalRunner()
		r.Observer = mutateObserver{f, mode}
		report, e := r.Run(driver)
		if e != nil {
			t.Fatal(e)
		}
		want := "negative_counter_delta"
		if mode == "epoch" {
			want = "counter_epoch_changed"
		}
		s := report.Scenarios[0]
		if len(s.Errors) != 1 || s.Errors[0].Code != want || s.Metrics.Baseline == nil || s.Metrics.Delta != nil {
			t.Fatalf("%s: %#v", mode, s)
		}
		if e = canonicalPublisher().WriteReport(report, filepath.Join(t.TempDir(), mode+".json")); e != nil {
			t.Fatal(e)
		}
	}
}

type countingAction struct{ calls *int }

func (a countingAction) Execute(FixtureContext, ScenarioSpec) ActionResult {
	*a.calls++
	return ActionResult{}
}

type invalidAction struct{ events []FixtureEvent }

func (a invalidAction) Execute(FixtureContext, ScenarioSpec) ActionResult {
	return ActionResult{Events: append([]FixtureEvent(nil), a.events...)}
}

type resourceMutator struct {
	id   string
	seen *bool
}

func (a resourceMutator) Execute(c FixtureContext, s ScenarioSpec) ActionResult {
	if s.ScenarioID == "ADV-04" {
		one, ok := c.Resource(a.id)
		if !ok {
			return ActionResult{Failure: &SeamFailure{"trigger_failed"}}
		}
		one[0] ^= 0xff
		two, _ := c.Resource(a.id)
		*a.seen = one[0] != two[0]
	}
	for _, f := range c.bound.driver.Scenarios {
		if f.ScenarioID == s.ScenarioID {
			return ActionResult{Events: append([]FixtureEvent(nil), f.Events...)}
		}
	}
	return ActionResult{}
}

func TestTypedSeamEvidenceAndResourceImmutability(t *testing.T) {
	driver := fixtureBytes(t, "inputs", "offline-all-pass")
	f, _ := ParseFixtureV1(driver)
	bad := [][]FixtureEvent{f.Scenarios[0].Events[:3], append([]FixtureEvent(nil), f.Scenarios[0].Events...), append([]FixtureEvent(nil), f.Scenarios[0].Events...)}
	bad[1][1].Kind = "consumer_started"
	bad[2][1].ErrorBoundMS = -1
	for i, events := range bad {
		r := canonicalRunner()
		r.Action = invalidAction{events}
		if _, e := r.Run(driver); !errors.Is(e, ErrInvalidSeamEvidence) {
			t.Fatalf("case %d: %v", i, e)
		}
	}
	seen := false
	r := canonicalRunner()
	r.Action = resourceMutator{"cache-offline-all-pass-adv04", &seen}
	if _, e := r.Run(driver); e != nil || !seen {
		t.Fatalf("resource copy: %v %v", seen, e)
	}
}

func TestProducerIdentityIsRequired(t *testing.T) {
	r := Executor{StartedAt: time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)}
	if _, e := r.Run(fixtureBytes(t, "inputs", "offline-all-pass")); !errors.Is(e, ErrInvalidFixture) {
		t.Fatal(e)
	}
}

func TestGatewayValidatorsRejectHAProducerClaims(t *testing.T) {
	base, _ := canonicalRunner().Run(fixtureBytes(t, "inputs", "offline-all-pass"))
	for _, inputDigest := range []string{strings.Repeat("0", 64), strings.Repeat("1", 64)} {
		r := cloneReport(base)
		r.Provenance.Producer = Producer{Repository: "Project-Helianthus/helianthus-ha-integration", Commit: strings.Repeat("a", 40), Component: "ha-adversarial-harness", BuildKind: "ha-harness", BuildSHA256: strings.Repeat("b", 64), InputGatewayReportSHA256: &inputDigest}
		if err := ValidateReport(r); !errors.Is(err, ErrInvalidReport) {
			t.Fatalf("ValidateReport accepted HA producer: %v", err)
		}
		raw, _ := json.Marshal(r)
		if err := ValidateReportBytes(raw); !errors.Is(err, ErrInvalidReport) {
			t.Fatalf("ValidateReportBytes accepted HA producer: %v", err)
		}
		path := filepath.Join(t.TempDir(), "ha-report.json")
		if err := canonicalPublisher().WriteReport(r, path); !errors.Is(err, ErrInvalidReport) {
			t.Fatalf("WriteReport accepted HA producer: %v", err)
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("destination created: %v", err)
		}
	}
}
func TestRawAdmissionPrecedesSeams(t *testing.T) {
	raw := fixtureBytes(t, "inputs", "offline-all-pass")
	mutations := [][]byte{append(append([]byte(nil), raw...), byte(' ')), bytes.Replace(raw, []byte(`"fixture_case_id": "offline-all-pass"`), []byte(`"fixture_case_id": "forged-case"`), 1), bytes.Replace(raw, []byte(`"kind": "restart_requested"`), []byte(`"kind": 7`), 1), bytes.Replace(raw, []byte(`"error_bound_ms": 0`), []byte(`"error_bound_ms": -1`), 1)}
	mutations = append(mutations,
		bytes.Replace(raw, []byte("\"fixture_case_id\": \"offline-all-pass\""), []byte("\"undeclared\": true, \"fixture_case_id\": \"offline-all-pass\""), 1),
		bytes.Replace(raw, []byte("\"schema_version\": 1"), []byte("\"$schema\": \"forged\", \"schema_version\": 1"), 1),
		bytes.Replace(raw, []byte("\"fixture_case_id\": \"offline-all-pass\""), []byte("\"fixture_case_id\": \"forged-case\""), 1),
		bytes.Replace(raw, []byte("\"kind\": \"restart_requested\""), []byte("\"kind\": 7"), 1),
		bytes.Replace(raw, []byte("\"semantic_startup_current_phase\": \"LIVE_READY\""), []byte("\"semantic_startup_current_phase\": \"BOOT_INIT\""), 1),
		bytes.Replace(raw, []byte("\"offset_ms\": 60000"), []byte("\"offset_ms\": 59000"), 1),
		bytes.Replace(raw, []byte("\"error_bound_ms\": 0"), []byte("\"error_bound_ms\": -1"), 1),
		bytes.Replace(raw, []byte("\"offset_ms\": 180000"), []byte("\"offset_ms\": 0"), 1),
	)
	for i, b := range mutations {
		calls := 0
		r := canonicalRunner()
		r.Action = countingAction{&calls}
		_, e := r.Run(b)
		if !errors.Is(e, ErrInvalidFixture) || calls != 0 {
			t.Fatalf("mutation %d err=%v calls=%d", i, e, calls)
		}
	}
}

func TestStrictJSONScanner(t *testing.T) {
	good := fixtureBytes(t, "inputs", "offline-all-pass")
	cases := [][]byte{{0xff}, []byte(`{"a":1,"a":2}`), []byte(`{} {}`), []byte(`{"a":1.0}`), []byte(`{"a":1e2}`), []byte(`{"a":12345678901234567}`), bytes.Repeat([]byte(" "), maximumJSONBytes+1)}
	deep := append(bytes.Repeat([]byte("["), 65), bytes.Repeat([]byte("]"), 65)...)
	cases = append(cases, deep)
	for i, b := range cases {
		if _, e := ParseFixtureV1(b); !errors.Is(e, ErrInvalidFixture) {
			t.Fatalf("case %d: %v", i, e)
		}
	}
	if _, e := ParseFixtureV1(good); e != nil {
		t.Fatal(e)
	}
}

func TestReportMutationsFailClosed(t *testing.T) {
	report, _ := canonicalRunner().Run(fixtureBytes(t, "inputs", "offline-all-pass"))
	mutations := []func(*ReportV1){func(r *ReportV1) { r.Summary.Verdict = "fail" }, func(r *ReportV1) { r.Scenarios[0].Definition.MaximumRecoveryMS++ }, func(r *ReportV1) { r.Scenarios[0].Action.Events[0].ErrorBoundMS = -1 }, func(r *ReportV1) { r.Scenarios[0].Timing.ScenarioStartedAt = r.Execution.CompletedAt }, func(r *ReportV1) { r.Scenarios[0].Metrics.End.SemanticStartupCurrentPhase = "BOOT_INIT" }, func(r *ReportV1) { r.Scenarios[0].Evaluation.Recovery.Passed = false }, func(r *ReportV1) { r.Provenance.Producer.BuildSHA256 = "1111" }}
	for i, fn := range mutations {
		x := cloneReport(report)
		fn(&x)
		if e := ValidateReport(x); !errors.Is(e, ErrInvalidReport) {
			t.Fatalf("mutation %d: %v", i, e)
		}
	}
	raw, _ := json.Marshal(report)
	for _, bad := range [][]byte{bytes.Replace(raw, []byte(`"summary":`), []byte(`"extra":1,"summary":`), 1), bytes.Replace(raw, []byte(`"schema_version":1`), []byte(`"schema_version":1,"schema_version":1`), 1)} {
		if e := ValidateReportBytes(bad); !errors.Is(e, ErrInvalidReport) {
			t.Fatal(e)
		}
	}
}

func TestIncompleteEvidenceRejectsMalformedPresentSnapshotBeforeWrite(t *testing.T) {
	report, _ := canonicalRunner().Run(fixtureBytes(t, "inputs", "offline-all-pass"))
	s := &report.Scenarios[0]
	s.ResultKind = "execution-error"
	s.Outcome = "fail"
	s.Evaluation = nil
	s.Metrics.Delta = nil
	s.Metrics.End = nil
	s.Metrics.Baseline.CounterEpoch = "not-a-uuid"
	s.Errors = []ScenarioError{{Phase: "evaluation", Code: "evidence_incomplete"}}
	report.Summary = Summary{Total: 4, Passed: 3, Failed: 1, Verdict: "fail"}
	if err := ValidateReport(report); !errors.Is(err, ErrInvalidReport) {
		t.Fatalf("ValidateReport accepted malformed partial snapshot: %v", err)
	}
	raw, _ := json.Marshal(report)
	if err := ValidateReportBytes(raw); !errors.Is(err, ErrInvalidReport) {
		t.Fatalf("ValidateReportBytes accepted malformed partial snapshot: %v", err)
	}
	path := filepath.Join(t.TempDir(), "malformed-incomplete.json")
	if err := canonicalPublisher().WriteReport(report, path); !errors.Is(err, ErrInvalidReport) {
		t.Fatalf("WriteReport accepted malformed partial snapshot: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination created: %v", err)
	}
}

func TestIncompleteEvidenceRequiresAllMetricsAbsent(t *testing.T) {
	base, _ := canonicalRunner().Run(fixtureBytes(t, "inputs", "offline-all-pass"))
	report := cloneReport(base)
	s := &report.Scenarios[0]
	s.ResultKind = "execution-error"
	s.Outcome = "fail"
	s.Evaluation = nil
	s.Metrics.Delta = nil
	s.Metrics.End = nil
	s.Errors = []ScenarioError{{Phase: "evaluation", Code: "evidence_incomplete"}}
	report.Summary = Summary{Total: 4, Passed: 3, Failed: 1, Verdict: "fail"}

	if err := ValidateReport(report); !errors.Is(err, ErrInvalidReport) {
		t.Fatalf("ValidateReport accepted structurally valid partial baseline: %v", err)
	}
	raw, _ := json.Marshal(report)
	if err := ValidateReportBytes(raw); !errors.Is(err, ErrInvalidReport) {
		t.Fatalf("ValidateReportBytes accepted structurally valid partial baseline: %v", err)
	}
	path := filepath.Join(t.TempDir(), "partial-incomplete.json")
	if err := canonicalPublisher().WriteReport(report, path); !errors.Is(err, ErrInvalidReport) {
		t.Fatalf("WriteReport accepted structurally valid partial baseline: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination created: %v", err)
	}

	s.Metrics.Baseline = nil
	if err := ValidateReport(report); err != nil {
		t.Fatalf("all-null evidence_incomplete rejected: %v", err)
	}
	if raw, err := json.Marshal(report); err != nil {
		t.Fatal(err)
	} else if err := ValidateReportBytes(raw); err != nil {
		t.Fatalf("all-null evidence_incomplete bytes rejected: %v", err)
	}
}

func TestReportSemanticZoneCountUsesSafeIntegerRange(t *testing.T) {
	base, _ := canonicalRunner().Run(fixtureBytes(t, "inputs", "offline-all-pass"))

	zone21 := cloneReport(base)
	zone21.Scenarios[0].Metrics.Baseline.SemanticZoneCount = 21
	zone21.Scenarios[0].Metrics.End.SemanticZoneCount = 21
	if err := ValidateReport(zone21); err != nil {
		t.Fatalf("zone count 21 rejected: %v", err)
	}
	if raw, err := json.Marshal(zone21); err != nil {
		t.Fatal(err)
	} else if err := ValidateReportBytes(raw); err != nil {
		t.Fatalf("zone count 21 bytes rejected: %v", err)
	}

	for _, tc := range []struct {
		name  string
		value int64
	}{{"negative", -1}, {"above_safe_integer", safeInteger + 1}} {
		t.Run(tc.name, func(t *testing.T) {
			report := cloneReport(base)
			report.Scenarios[0].Metrics.End.SemanticZoneCount = tc.value
			if err := ValidateReport(report); !errors.Is(err, ErrInvalidReport) {
				t.Fatalf("ValidateReport accepted %d: %v", tc.value, err)
			}
			raw, _ := json.Marshal(report)
			if err := ValidateReportBytes(raw); !errors.Is(err, ErrInvalidReport) {
				t.Fatalf("ValidateReportBytes accepted %d: %v", tc.value, err)
			}
		})
	}
}

func TestReportFixtureCaseRunIdentityBinding(t *testing.T) {
	type fixtureIdentity struct {
		caseID string
		runID  string
		report ReportV1
	}
	identities := make([]fixtureIdentity, 0, len(fixtureCases))
	for _, name := range fixtureCases {
		report, err := canonicalRunner().Run(fixtureBytes(t, "inputs", name))
		if err != nil {
			t.Fatalf("%s execute: %v", name, err)
		}
		if err := ValidateReport(report); err != nil {
			t.Fatalf("%s ValidateReport: %v", name, err)
		}
		raw, err := json.Marshal(report)
		if err != nil {
			t.Fatalf("%s marshal: %v", name, err)
		}
		if err := ValidateReportBytes(raw); err != nil {
			t.Fatalf("%s ValidateReportBytes: %v", name, err)
		}
		if err := canonicalPublisher().WriteReport(report, filepath.Join(t.TempDir(), name+".json")); err != nil {
			t.Fatalf("%s WriteReport: %v", name, err)
		}
		identities = append(identities, fixtureIdentity{name, report.Execution.RunID, report})
	}

	assertRejected := func(t *testing.T, report ReportV1) {
		t.Helper()
		if err := ValidateReport(report); !errors.Is(err, ErrInvalidReport) {
			t.Fatalf("ValidateReport accepted identity mismatch: %v", err)
		}
		raw, _ := json.Marshal(report)
		if err := ValidateReportBytes(raw); !errors.Is(err, ErrInvalidReport) {
			t.Fatalf("ValidateReportBytes accepted identity mismatch: %v", err)
		}
		path := filepath.Join(t.TempDir(), "identity-mismatch.json")
		if err := canonicalPublisher().WriteReport(report, path); !errors.Is(err, ErrInvalidReport) {
			t.Fatalf("WriteReport accepted identity mismatch: %v", err)
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("destination created: %v", err)
		}
	}

	for i, identity := range identities {
		for j, other := range identities {
			if i == j {
				continue
			}
			t.Run(identity.caseID+"_relabeled_"+other.caseID, func(t *testing.T) {
				report := cloneReport(identity.report)
				report.Provenance.FixtureCaseID = other.caseID
				assertRejected(t, report)
			})
		}
		t.Run(identity.caseID+"_known_run_mismatch", func(t *testing.T) {
			report := cloneReport(identity.report)
			report.Execution.RunID = identities[(i+1)%len(identities)].runID
			assertRejected(t, report)
		})
		t.Run(identity.caseID+"_unknown_run_mismatch", func(t *testing.T) {
			report := cloneReport(identity.report)
			report.Execution.RunID = "00000000-0000-4000-8000-000000000099"
			assertRejected(t, report)
		})
	}
}

func TestPublishedNegativeCorpusFailsClosed(t *testing.T) {
	raw, err := fixtureFiles.ReadFile("fixtures/v1/negative-cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		BaseFixtureCaseID string `json:"base_fixture_case_id"`
		Cases             []struct {
			ID        string `json:"id"`
			Mutations []struct {
				Op    string `json:"op"`
				Path  string `json:"path"`
				Value any    `json:"value"`
			} `json:"mutations"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	baseReport, err := canonicalRunner().Run(fixtureBytes(t, "inputs", corpus.BaseFixtureCaseID))
	if err != nil {
		t.Fatal(err)
	}
	base, err := json.Marshal(baseReport)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range corpus.Cases {
		t.Run(tc.ID, func(t *testing.T) {
			var doc any
			d := json.NewDecoder(bytes.NewReader(base))
			d.UseNumber()
			if err := d.Decode(&doc); err != nil {
				t.Fatal(err)
			}
			for _, m := range tc.Mutations {
				if err := mutateJSON(&doc, m.Op, m.Path, m.Value); err != nil {
					t.Fatal(err)
				}
			}
			changed, _ := json.Marshal(doc)
			if err := ValidateReportBytes(changed); !errors.Is(err, ErrInvalidReport) {
				t.Fatalf("accepted: %v", err)
			}
		})
	}
}

func mutateJSON(root *any, op, p string, value any) error {
	parts := strings.Split(strings.TrimPrefix(p, "/"), "/")
	cur := *root
	for _, part := range parts[:len(parts)-1] {
		switch x := cur.(type) {
		case map[string]any:
			cur = x[part]
		case []any:
			var i int
			if _, e := fmt.Sscanf(part, "%d", &i); e != nil {
				return e
			}
			cur = x[i]
		default:
			return errors.New("bad pointer")
		}
	}
	last := parts[len(parts)-1]
	switch x := cur.(type) {
	case map[string]any:
		if op == "remove" {
			delete(x, last)
		} else {
			x[last] = value
		}
	case []any:
		if last == "-" {
			x = append(x, value)
			return setJSONArray(root, parts[:len(parts)-1], x)
		}
		var i int
		if _, e := fmt.Sscanf(last, "%d", &i); e != nil {
			return e
		}
		if op == "remove" {
			x = append(x[:i], x[i+1:]...)
			return setJSONArray(root, parts[:len(parts)-1], x)
		}
		x[i] = value
	default:
		return errors.New("bad target")
	}
	return nil
}
func setJSONArray(root *any, parts []string, v []any) error {
	if len(parts) == 0 {
		*root = v
		return nil
	}
	cur := *root
	for _, part := range parts[:len(parts)-1] {
		switch x := cur.(type) {
		case map[string]any:
			cur = x[part]
		case []any:
			var i int
			_, _ = fmt.Sscanf(part, "%d", &i)
			cur = x[i]
		}
	}
	last := parts[len(parts)-1]
	switch x := cur.(type) {
	case map[string]any:
		x[last] = v
	case []any:
		var i int
		_, _ = fmt.Sscanf(last, "%d", &i)
		x[i] = v
	}
	return nil
}

func TestConcurrentAtomicWrite(t *testing.T) {
	a, _ := canonicalRunner().Run(fixtureBytes(t, "inputs", "offline-all-pass"))
	b, _ := canonicalRunner().Run(fixtureBytes(t, "inputs", "evaluated-fail"))
	path := filepath.Join(t.TempDir(), "report.json")
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := a
			if i%2 == 1 {
				r = b
			}
			errs <- canonicalPublisher().WriteReport(r, path)
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	raw, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if e = ValidateReportBytes(raw); e != nil {
		t.Fatal(e)
	}
	left, _ := filepath.Glob(filepath.Join(filepath.Dir(path), ".adversarial-report-*"))
	if len(left) != 0 {
		t.Fatal(left)
	}
}

func cloneReport(r ReportV1) ReportV1 {
	raw, _ := json.Marshal(r)
	var out ReportV1
	_ = json.Unmarshal(raw, &out)
	return out
}

func FuzzParseFixtureV1(f *testing.F) {
	for _, name := range fixtureCases {
		b, _ := fixtureFiles.ReadFile("fixtures/v1/inputs/" + name + ".json")
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) { v, _ := ParseFixtureV1(b); _ = validateFixture(v) })
}
func FuzzParseReportV1(f *testing.F) {
	for _, name := range fixtureCases {
		report, _ := canonicalRunner().Run(fixtureBytesForFuzz("inputs", name))
		b, _ := json.Marshal(report)
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) { v, _ := ParseReportV1(b); _ = ValidateRuntimeReportV1(v) })
}

func TestCompiledPublisherHelper(t *testing.T) {
	separator := -1
	for i, arg := range os.Args {
		if arg == "--" {
			separator = i
			break
		}
	}
	if separator < 0 {
		t.Skip("compiled publication helper")
	}
	args := os.Args[separator+1:]
	if len(args) != 2 {
		t.Fatalf("helper arguments = %d; want 2", len(args))
	}
	raw, err := os.ReadFile(args[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PublishReport(raw, time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC), args[1]); err != nil {
		t.Fatal(err)
	}
}

func fixtureBytesForFuzz(kind, name string) []byte {
	b, _ := fixtureFiles.ReadFile("fixtures/v1/" + kind + "/" + name + ".json")
	return b
}

var _ = strings.Builder{}
