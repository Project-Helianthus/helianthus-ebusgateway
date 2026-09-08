package adversarial

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

var fixtureCases = []string{"evaluated-fail", "execution-error", "infrastructure-block", "offline-all-pass"}

func fixtureBytes(t *testing.T, kind, name string) []byte {
	t.Helper()
	b, e := fixtureFiles.ReadFile("fixtures/v1/" + kind + "/" + name + ".json")
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func canonicalRunner() Executor {
	return Executor{StartedAt: time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC), Producer: PublishedSyntheticProducerIdentity()}
}

func TestPublishedCorpusParsesBindsExecutesAndProjects(t *testing.T) {
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
			expected, e := ParseReportV1(fixtureBytes(t, "positive", name))
			if e != nil {
				t.Fatal(e)
			}
			actual, e := canonicalRunner().Run(driver)
			if e != nil {
				t.Fatal(e)
			}
			if !reflect.DeepEqual(actual, expected) {
				a, _ := json.Marshal(actual)
				b, _ := json.Marshal(expected)
				t.Fatalf("projection mismatch\n%s\n%s", a, b)
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
	if e = WriteReport(report, path); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(path); e != nil {
		t.Fatal(e)
	}
	bound, _ := BindFixtureDriverV1(fixtureBytes(t, "inputs", "offline-all-pass"))
	if e = ValidateFixtureProjectionV1(report, bound); e == nil {
		t.Fatal("dynamic report accepted as exact fixture projection")
	}
}

type actionFailure struct{ code string }

func (a actionFailure) Execute(_ FixtureContext, spec ScenarioSpec) ActionResult {
	return ActionResult{Events: []FixtureEvent{{Kind: spec.ExpectedEvents[0], OffsetMS: 0, ErrorBoundMS: 0}}, Failure: &SeamFailure{a.code}}
}

type observerFailure struct{ code string }

func (a observerFailure) Observe(FixtureContext, ScenarioSpec, []FixtureEvent) ObservationResult {
	return ObservationResult{Failure: &SeamFailure{a.code}}
}

func TestSeamFailuresAreWritable(t *testing.T) {
	for _, code := range []string{"trigger_rejected", "trigger_timeout", "trigger_failed"} {
		r := canonicalRunner()
		r.Action = actionFailure{code}
		report, e := r.Run(fixtureBytes(t, "inputs", "offline-all-pass"))
		if e != nil {
			t.Fatal(e)
		}
		for _, s := range report.Scenarios {
			if len(s.Action.Events) != 1 || s.Errors[0].Code != code {
				t.Fatalf("%s: %#v", code, s)
			}
		}
		if e = WriteReport(report, filepath.Join(t.TempDir(), code+".json")); e != nil {
			t.Fatal(e)
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
		if e = WriteReport(report, filepath.Join(t.TempDir(), code+".json")); e != nil {
			t.Fatal(e)
		}
	}
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
		if e = WriteReport(report, filepath.Join(t.TempDir(), mode+".json")); e != nil {
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
	r := canonicalRunner()
	r.Producer = ProducerIdentity{}
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
		if err := WriteReport(r, path); !errors.Is(err, ErrInvalidReport) {
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

func TestPublishedNegativeCorpusFailsClosed(t *testing.T) {
	raw, err := fixtureFiles.ReadFile("fixtures/v1/negative-cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Base  string `json:"base"`
		Cases []struct {
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
	base := fixtureBytes(t, "positive", strings.TrimSuffix(filepath.Base(corpus.Base), ".json"))
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
			errs <- WriteReport(r, path)
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
		b, _ := fixtureFiles.ReadFile("fixtures/v1/positive/" + name + ".json")
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) { v, _ := ParseReportV1(b); _ = ValidateRuntimeReportV1(v) })
}

var _ = strings.Builder{}
