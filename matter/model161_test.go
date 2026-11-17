package matter

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

func aa03Candidate(phase string) semreg.FactCandidate {
	c := matterCandidate(semreg.CandidateID("candidate:synthetic:"+phase), phase, "32", -1)
	digest := sha256.Sum256([]byte("public synthetic AA03 current evidence"))
	evidence := semreg.EvidenceRef{Owner: "owner.test", Kind: "evidence.test", Digest: semreg.Digest(fmt.Sprintf("sha256:%x", digest)), Contract: "test.evidence/v1", Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone}
	c.Evidence = []semreg.EvidenceRef{evidence}
	c.Origin.Evidence = []semreg.EvidenceRef{evidence}
	return c
}

func aa03Fixture(t *testing.T) matterFixture {
	t.Helper()
	return newMatterFixture(t, []semreg.FactCandidate{aa03Candidate("l1")})
}

func aa03Positive(t *testing.T, f matterFixture) Result161 {
	t.Helper()
	got, err := Project161(f.snapshot, f.evaluation)
	if err != nil {
		t.Fatal(err)
	}
	want := []TargetAttribute{{Element: TargetElement{DeviceType: 0x0510, Cluster: 0x0090, Kind: ElementAttribute, ElementID: 5}, Value: AttributeValue{Milliampere: "3200"}}}
	if !reflect.DeepEqual(got.Document.Attributes, want) {
		t.Fatalf("attributes=%#v", got.Document.Attributes)
	}
	return got
}

func TestAA03TargetAndFullLedger(t *testing.T) {
	f := aa03Fixture(t)
	before, _ := json.Marshal([]any{f.snapshot, f.evaluation})
	got := aa03Positive(t, f)
	if got.Document.TargetVersion != "1.6.1" || got.Document.MappingRevision != "1" || got.Document.Model != (Model161{1, 1, 3}) {
		t.Fatalf("document=%#v", got.Document)
	}
	wantManifest := projection.ProjectionManifest{TargetID: "matter.data-model", TargetVersion: "1.6.1", KernelVersion: semreg.ContractKernelV1, MappingRevision: "1", PackVersions: []semreg.PackRef{
		{ID: "helianthus.pack.evse", Version: "1.0.0"}, {ID: "helianthus.pack.infrastructure", Version: "1.0.0"}, {ID: "helianthus.pack.pv", Version: "1.0.0"}, {ID: "helianthus.pack.storage", Version: "1.1.0"}, {ID: "helianthus.pack.thermal", Version: "1.0.0"},
	}}
	if !reflect.DeepEqual(got.Report.Manifest, wantManifest) {
		t.Fatalf("manifest=%#v", got.Report.Manifest)
	}
	requested, dispositions := Ledger()
	if !reflect.DeepEqual(got.Report.Requested, requested) || len(got.Report.Dispositions) != len(dispositions) {
		t.Fatal("incomplete ledger")
	}
	positives := 0
	for i, row := range got.Report.Dispositions {
		if row.Kind == projection.ItemFact && row.ItemID == "evse.ac.current" {
			positives++
			if row.Outcome != projection.ProjectionTransformed || row.Reason == nil || *row.Reason != "matter.phase_endpoint_identity_loss.v1" || len(row.SourceKeys) != 1 || !reflect.DeepEqual(row.SourceKeys[0], f.snapshot.Facts[0].Key) || len(row.Loss) != 1 || row.Loss[0].Reversible {
				t.Fatalf("current loss=%#v", row)
			}
		} else if !reflect.DeepEqual(row, dispositions[i]) {
			t.Fatalf("unaffected ledger row %d changed", i)
		}
	}
	if positives != 1 {
		t.Fatalf("positive rows=%d", positives)
	}
	a, err := CanonicalJSON161(got)
	if err != nil {
		t.Fatal(err)
	}
	b, err := CanonicalJSON161(aa03Positive(t, f))
	if err != nil || !bytes.Equal(a, b) {
		t.Fatal("nondeterministic output")
	}
	after, _ := json.Marshal([]any{f.snapshot, f.evaluation})
	if !bytes.Equal(before, after) {
		t.Fatal("mutated inputs")
	}
	got.Document.Attributes[0].Value.Milliampere = "0"
	aa03Positive(t, f)
	legacy, err := Project(f.snapshot, f.evaluation)
	if err != nil || legacy.Document.TargetVersion != TargetVersion || legacy.Report.Manifest.TargetVersion != TargetVersion {
		t.Fatal("legacy changed")
	}
}

func TestAA03TargetAgreement(t *testing.T) {
	cases := map[string]func(*Result161){
		"document contract":    func(r *Result161) { r.Document.Contract = "other.contract/v1" },
		"document target":      func(r *Result161) { r.Document.TargetID = "other.target" },
		"document version":     func(r *Result161) { r.Document.TargetVersion = TargetVersion },
		"document mapping":     func(r *Result161) { r.Document.MappingRevision = "2" },
		"sensor revision":      func(r *Result161) { r.Document.Model.ElectricalSensorRevision = 2 },
		"topology revision":    func(r *Result161) { r.Document.Model.PowerTopologyRevision = 2 },
		"measurement revision": func(r *Result161) { r.Document.Model.ElectricalPowerMeasurementRevision = 4 },
		"manifest target":      func(r *Result161) { r.Report.Manifest.TargetID = "other.target" },
		"manifest version":     func(r *Result161) { r.Report.Manifest.TargetVersion = TargetVersion },
		"manifest mapping":     func(r *Result161) { r.Report.Manifest.MappingRevision = "2" },
		"manifest kernel":      func(r *Result161) { r.Report.Manifest.KernelVersion = "other.kernel/v1" },
		"device":               func(r *Result161) { r.Document.Attributes[0].Element.DeviceType++ },
		"cluster":              func(r *Result161) { r.Document.Attributes[0].Element.Cluster++ },
		"kind":                 func(r *Result161) { r.Document.Attributes[0].Element.Kind = "command" },
		"attribute":            func(r *Result161) { r.Document.Attributes[0].Element.ElementID++ },
	}
	for i := 0; i < 5; i++ {
		i := i
		cases[fmt.Sprintf("pack id %d", i)] = func(r *Result161) { r.Report.Manifest.PackVersions[i].ID = "other.pack" }
		cases[fmt.Sprintf("pack version %d", i)] = func(r *Result161) { r.Report.Manifest.PackVersions[i].Version = "9.0.0" }
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := aa03Positive(t, aa03Fixture(t))
			if _, err := CanonicalJSON161(r); err != nil {
				t.Fatal(err)
			}
			mutate(&r)
			if raw, err := CanonicalJSON161(r); err == nil || raw != nil {
				t.Fatal("accepted drift")
			}
		})
	}
}

func TestAA03CanonicalValueAndReportAgreement(t *testing.T) {
	for _, value := range []string{"not-a-number", "", "1.5", "1e3", " 1", "+1", "01", "-0", "4611686018427387905", "-4611686018427387905"} {
		t.Run("invalid value "+value, func(t *testing.T) {
			r := aa03Positive(t, aa03Fixture(t))
			r.Document.Attributes[0].Value.Milliampere = value
			if raw, err := CanonicalJSON161(r); err == nil || raw != nil {
				t.Fatalf("accepted invalid milliamperes %q", value)
			}
		})
	}
	for _, value := range []string{"-4611686018427387904", "4611686018427387904", "0", "-1"} {
		t.Run("valid value "+value, func(t *testing.T) {
			r := aa03Positive(t, aa03Fixture(t))
			r.Document.Attributes[0].Value.Milliampere = value
			if _, err := CanonicalJSON161(r); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("transformed row without attribute", func(t *testing.T) {
		r := aa03Positive(t, aa03Fixture(t))
		r.Document.Attributes = []TargetAttribute{}
		if err := r.Report.Validate(); err != nil {
			t.Fatal(err)
		}
		if raw, err := CanonicalJSON161(r); err == nil || raw != nil {
			t.Fatal("accepted transformed row without attribute")
		}
	})
	t.Run("attribute without transformed row", func(t *testing.T) {
		positive := aa03Positive(t, aa03Fixture(t))
		f := newMatterFixture(t, []semreg.FactCandidate{})
		r, err := Project161(f.snapshot, f.evaluation)
		if err != nil {
			t.Fatal(err)
		}
		r.Document.Attributes = positive.Document.Attributes
		if err := r.Report.Validate(); err != nil {
			t.Fatal(err)
		}
		if raw, err := CanonicalJSON161(r); err == nil || raw != nil {
			t.Fatal("accepted attribute without transformed row")
		}
	})
	t.Run("unmapped row claimed exact", func(t *testing.T) {
		r := aa03Positive(t, aa03Fixture(t))
		changed := false
		for i := range r.Report.Dispositions {
			row := &r.Report.Dispositions[i]
			if row.Kind != projection.ItemFact || row.ItemID == "evse.ac.current" || row.Outcome != projection.ProjectionUnknown {
				continue
			}
			for _, cur := range r.Report.Dispositions {
				if cur.ItemID == "evse.ac.current" {
					row.SourceKeys = cur.SourceKeys
				}
			}
			row.Outcome, row.Loss, row.Reason, changed = projection.ProjectionExact, []projection.LossDetail{}, nil, true
			break
		}
		if !changed {
			t.Fatal("no unmapped row")
		}
		if err := r.Report.Validate(); err != nil {
			t.Fatalf("variant must stay structurally valid: %v", err)
		}
		if raw, err := CanonicalJSON161(r); err == nil || raw != nil {
			t.Fatal("accepted positive outcome for unmapped row")
		}
	})
	for name, keepAttribute := range map[string]bool{"exact row without attribute": false, "exact row with attribute": true} {
		t.Run(name, func(t *testing.T) {
			r := aa03Positive(t, aa03Fixture(t))
			found := false
			for i := range r.Report.Dispositions {
				row := &r.Report.Dispositions[i]
				if row.Kind == projection.ItemFact && row.ItemID == "evse.ac.current" {
					row.Outcome, row.Loss, row.Reason, found = projection.ProjectionExact, []projection.LossDetail{}, nil, true
				}
			}
			if !found {
				t.Fatal("current disposition missing")
			}
			if !keepAttribute {
				r.Document.Attributes = []TargetAttribute{}
			}
			if err := r.Report.Validate(); err != nil {
				t.Fatalf("mutated report must stay structurally valid: %v", err)
			}
			if raw, err := CanonicalJSON161(r); err == nil || raw != nil {
				t.Fatalf("accepted %s", name)
			}
		})
	}
}

func TestAA03EvaluationJoins(t *testing.T) {
	cases := map[string]func(*semreg.EvaluationView){
		"snapshot": func(v *semreg.EvaluationView) {
			v.SnapshotID = semreg.SnapshotID(fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("other snapshot"))))
		},
		"semantic revision":     func(v *semreg.EvaluationView) { v.Revisions.Semantic = "2" },
		"identity revision":     func(v *semreg.EvaluationView) { v.Revisions.Identity = "2" },
		"facts revision":        func(v *semreg.EvaluationView) { v.Revisions.Facts = "2" },
		"services revision":     func(v *semreg.EvaluationView) { v.Revisions.Services = "2" },
		"capabilities revision": func(v *semreg.EvaluationView) { v.Revisions.Capabilities = "2" },
		"candidate identity":    func(v *semreg.EvaluationView) { v.Facts[0].CandidateID = "candidate:other" },
		"candidate revision":    func(v *semreg.EvaluationView) { v.Facts[0].CandidateRevision = "2" },
		"candidate omission":    func(v *semreg.EvaluationView) { v.Facts = []semreg.EvaluatedFact{} },
		"freshness":             func(v *semreg.EvaluationView) { v.Facts[0].Freshness = semreg.FreshnessStale },
		"availability":          func(v *semreg.EvaluationView) { v.Facts[0].EffectiveAvailability = semreg.AvailabilityUnavailable },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := aa03Fixture(t)
			aa03Positive(t, f)
			mutate(&f.evaluation)
			sealEvaluation(t, &f.evaluation)
			aa03Rejected(t, f)
		})
	}
}

// Reseal without prevalidating: structural source-join refusals must be tested
// through Project161, with the outer snapshot/evaluation join still matching.
func aa03Reseal(t *testing.T, f *matterFixture) {
	t.Helper()
	raw, err := json.Marshal(f.snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err = json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	delete(value, "snapshot_id")
	raw, err = json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	f.snapshot.SnapshotID = semreg.SnapshotID(fmt.Sprintf("sha256:%x", sha256.Sum256(raw)))
	f.evaluation.SnapshotID = f.snapshot.SnapshotID
	sealEvaluation(t, &f.evaluation)
}

func aa03Rejected(t *testing.T, f matterFixture) {
	t.Helper()
	got, err := Project161(f.snapshot, f.evaluation)
	if err == nil || !reflect.DeepEqual(got, Result161{}) {
		t.Fatalf("accepted invalid input: result=%#v err=%v", got, err)
	}
}

func TestAA03SourceJoins(t *testing.T) {
	cases := map[string]func(*semreg.FactCandidate){
		"binding":             func(c *semreg.FactCandidate) { v := semreg.NativeBindingID("binding:other"); c.BindingID = &v },
		"source epoch":        func(c *semreg.FactCandidate) { v := semreg.SourceEpochID("epoch:other"); c.SourceEpochID = &v },
		"driver generation":   func(c *semreg.FactCandidate) { v := semreg.Uint64("2"); c.DriverGeneration = &v },
		"origin source":       func(c *semreg.FactCandidate) { v := semreg.SourceID("source:other"); c.Origin.SourceID = &v },
		"origin binding":      func(c *semreg.FactCandidate) { v := semreg.NativeBindingID("binding:other"); c.Origin.BindingID = &v },
		"origin epoch":        func(c *semreg.FactCandidate) { v := semreg.SourceEpochID("epoch:other"); c.Origin.SourceEpochID = &v },
		"key pack id":         func(c *semreg.FactCandidate) { c.Key.PackID = "other.pack" },
		"key pack version":    func(c *semreg.FactCandidate) { c.Key.PackVersion = "9.0.0" },
		"key fact":            func(c *semreg.FactCandidate) { c.Key.FactID = "evse.ac.active_power" },
		"key dimension id":    func(c *semreg.FactCandidate) { c.Key.Dimensions[0].ID = "evse.dimension.connector" },
		"key dimension value": func(c *semreg.FactCandidate) { v := "l2"; c.Key.Dimensions[0].Value.Text = &v },
		"key dimension kind": func(c *semreg.FactCandidate) {
			v := true
			c.Key.Dimensions[0].Value = semreg.Value{Kind: semreg.ValueBoolean, Boolean: &v}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := aa03Fixture(t)
			aa03Positive(t, f)
			mutate(&f.snapshot.Facts[0].Candidates[0])
			aa03Reseal(t, &f)
			aa03Rejected(t, f)
		})
	}
}

func TestAA03QualificationAndPrecision(t *testing.T) {
	cases := map[string]func(*semreg.FactCandidate){
		"assertion":     func(c *semreg.FactCandidate) { c.Quality.Assertion = semreg.AssertionInferred },
		"qualification": func(c *semreg.FactCandidate) { c.Quality.Qualification = semreg.QualificationCandidate },
		"promotion":     func(c *semreg.FactCandidate) { c.Quality.Promotion = semreg.PromotionUnpromoted },
		"validity":      func(c *semreg.FactCandidate) { c.Quality.Validity = semreg.ValidityBad },
		"availability":  func(c *semreg.FactCandidate) { c.Quality.Availability = semreg.AvailabilityUnavailable },
		"freshness":     func(c *semreg.FactCandidate) { c.Quality.Freshness = semreg.FreshnessStale },
		"remainder": func(c *semreg.FactCandidate) {
			c.Value.Quantity.Number = semreg.Decimal{Coefficient: "1", Exponent10: -4}
		},
		"unit":          func(c *semreg.FactCandidate) { c.Value.Quantity.Unit = "unit.volt" },
		"source bounds": func(c *semreg.FactCandidate) { c.Value.Quantity.Number = semreg.Decimal{Coefficient: "1001"} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := aa03Fixture(t)
			aa03Positive(t, f)
			mutate(&f.snapshot.Facts[0].Candidates[0])
			aa03Reseal(t, &f)
			aa03Rejected(t, f)
		})
	}
	for _, phase := range []string{"l1", "l2"} {
		t.Run("ambiguous "+phase, func(t *testing.T) {
			f := newMatterFixture(t, []semreg.FactCandidate{aa03Candidate("l1"), matterCandidate("candidate:synthetic:second", phase, "33", -1)})
			aa03Rejected(t, f)
		})
	}
}

func TestAA03EmptyAndPhaseControls(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		f := newMatterFixture(t, []semreg.FactCandidate{})
		got, err := Project161(f.snapshot, f.evaluation)
		if err != nil {
			t.Fatal(err)
		}
		requested, dispositions := Ledger()
		if got.Document.Attributes == nil || len(got.Document.Attributes) != 0 || !reflect.DeepEqual(got.Report.Requested, requested) || !reflect.DeepEqual(got.Report.Dispositions, dispositions) {
			t.Fatal("empty snapshot lost non-positive accounting")
		}
		a, err := CanonicalJSON161(got)
		if err != nil {
			t.Fatal(err)
		}
		again, err := Project161(f.snapshot, f.evaluation)
		if err != nil {
			t.Fatal(err)
		}
		b, err := CanonicalJSON161(again)
		if err != nil || !bytes.Equal(a, b) {
			t.Fatal("empty ledger is nondeterministic")
		}
	})
	for _, phase := range []string{"l2", "l3"} {
		t.Run(phase, func(t *testing.T) {
			f := newMatterFixture(t, []semreg.FactCandidate{aa03Candidate(phase)})
			got := aa03Positive(t, f)
			for _, row := range got.Report.Dispositions {
				if row.ItemID == "evse.ac.current" && !reflect.DeepEqual(row.SourceKeys, []semreg.FactKey{f.snapshot.Facts[0].Key}) {
					t.Fatal("phase source key lost")
				}
			}
		})
	}
}

func TestAA03ReportAccountingAndCurrentMetadata(t *testing.T) {
	current := func(r *Result161) *projection.ProjectionDisposition {
		for i := range r.Report.Dispositions {
			if r.Report.Dispositions[i].ItemID == "evse.ac.current" && r.Report.Dispositions[i].Kind == projection.ItemFact {
				return &r.Report.Dispositions[i]
			}
		}
		t.Fatal("current disposition missing")
		return nil
	}
	cases := map[string]func(*Result161){
		"paired omission": func(r *Result161) {
			last := len(r.Report.Requested) - 1
			r.Report.Requested = r.Report.Requested[:last]
			r.Report.Dispositions = r.Report.Dispositions[:last]
		},
		"empty source keys": func(r *Result161) { current(r).SourceKeys = []semreg.FactKey{} },
		"no reason":         func(r *Result161) { current(r).Reason = nil },
		"reversible loss":   func(r *Result161) { current(r).Loss[0].Reversible = true },
		"wrong reason": func(r *Result161) {
			reason := semreg.DefinitionID("matter.unmapped.v1")
			current(r).Reason = &reason
		},
		"foreign source": func(r *Result161) { current(r).SourceKeys[0].FactID = "evse.ac.voltage" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := aa03Positive(t, aa03Fixture(t))
			mutate(&r)
			if raw, err := CanonicalJSON161(r); err == nil || raw != nil {
				t.Fatalf("accepted %s", name)
			}
		})
	}
	if _, err := CanonicalJSON161(aa03Positive(t, aa03Fixture(t))); err != nil {
		t.Fatal(err)
	}
}

// Each variant stays structurally valid, so only the pack's dimension rule for
// evse.ac.current can refuse it, matching what Project161 does on the source fact.
func TestAA03CurrentRowSourceDimensions(t *testing.T) {
	phaseText, otherText := "l1", "l2"
	flag := true
	phase := semreg.Dimension{ID: "evse.dimension.phase", Value: semreg.Value{Kind: semreg.ValueText, Text: &phaseText}}
	connector := semreg.Dimension{ID: "evse.dimension.connector", Value: semreg.Value{Kind: semreg.ValueText, Text: &otherText}}
	withDimensions := func(t *testing.T, dimensions []semreg.Dimension) Result161 {
		t.Helper()
		r := aa03Positive(t, aa03Fixture(t))
		for i := range r.Report.Dispositions {
			if row := &r.Report.Dispositions[i]; row.Kind == projection.ItemFact && row.ItemID == "evse.ac.current" {
				row.SourceKeys[0].Dimensions = dimensions
				return r
			}
		}
		t.Fatal("current disposition missing")
		return r
	}
	t.Run("other phase control", func(t *testing.T) {
		r := withDimensions(t, []semreg.Dimension{{ID: phase.ID, Value: semreg.Value{Kind: semreg.ValueText, Text: &otherText}}})
		if _, err := CanonicalJSON161(r); err != nil {
			t.Fatal(err)
		}
	})
	cases := map[string][]semreg.Dimension{
		"wrong id":   {{ID: connector.ID, Value: phase.Value}},
		"wrong kind": {{ID: phase.ID, Value: semreg.Value{Kind: semreg.ValueBoolean, Boolean: &flag}}},
		"missing":    {},
		"extra":      {connector, phase},
	}
	for name, dimensions := range cases {
		t.Run(name, func(t *testing.T) {
			r := withDimensions(t, dimensions)
			if err := r.Report.Validate(); err != nil {
				t.Fatalf("variant must stay structurally valid: %v", err)
			}
			if raw, err := CanonicalJSON161(r); err == nil || raw != nil {
				t.Fatalf("accepted %s", name)
			}
		})
	}
}

func TestAA03EmptyProjectionCurrentRowMustBeLedgerRow(t *testing.T) {
	empty := func() Result161 {
		f := newMatterFixture(t, []semreg.FactCandidate{})
		r, err := Project161(f.snapshot, f.evaluation)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	current := func(r *Result161) *projection.ProjectionDisposition {
		for i := range r.Report.Dispositions {
			if r.Report.Dispositions[i].ItemID == "evse.ac.current" && r.Report.Dispositions[i].Kind == projection.ItemFact {
				return &r.Report.Dispositions[i]
			}
		}
		t.Fatal("current disposition missing")
		return nil
	}
	if _, err := CanonicalJSON161(empty()); err != nil {
		t.Fatalf("empty projection control: %v", err)
	}
	cases := map[string]func(*projection.ProjectionDisposition){
		"unsupported instead of unknown": func(d *projection.ProjectionDisposition) { d.Outcome = projection.ProjectionUnsupported },
		"unknown with altered reason": func(d *projection.ProjectionDisposition) {
			reason := semreg.DefinitionID("matter.altered.v1")
			d.Reason = &reason
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := empty()
			mutate(current(&r))
			if err := r.Report.Validate(); err != nil {
				t.Fatalf("variant must stay structurally valid: %v", err)
			}
			if raw, err := CanonicalJSON161(r); err == nil || raw != nil {
				t.Fatalf("accepted %s", name)
			}
		})
	}
}

// docsSemanticRootEnv names an immutable docs-semantic checkout holding the
// adopted roster. Without it the test reads the repository-local staged
// reference; when neither is present the pin cannot be compared here and the
// public mapping gates remain the authority.
const docsSemanticRootEnv = "HELIANTHUS_DOCS_SEMANTIC_ROOT"

func stagedDocsSemanticRoot(t *testing.T) string {
	t.Helper()
	if root := os.Getenv(docsSemanticRootEnv); root != "" {
		if info, err := os.Stat(root); err != nil || !info.IsDir() {
			t.Fatalf("%s does not name a directory: %v", docsSemanticRootEnv, err)
		}
		return root
	}
	root := filepath.Join("..", ".tmp", "references", "docs-semantic")
	if info, err := os.Stat(root); err != nil || !info.IsDir() {
		t.Skipf("docs-semantic roster is not staged; set %s", docsSemanticRootEnv)
	}
	return root
}

type roster161Ref struct {
	Pack struct {
		ID      string `json:"id"`
		Version string `json:"version"`
	} `json:"pack"`
	ID string `json:"id"`
}

type roster161Row struct {
	Ref         roster161Ref `json:"ref"`
	Kind        string       `json:"kind"`
	Disposition string       `json:"disposition"`
	Reason      string       `json:"reason"`
	Target      *struct {
		DeviceTypeID uint32 `json:"device_type_id"`
		ClusterID    uint32 `json:"cluster_id"`
		AttributeID  uint32 `json:"attribute_id"`
	} `json:"target"`
}

type roster161 struct {
	Contract        string      `json:"contract"`
	MappingRevision json.Number `json:"mapping_revision"`
	Target          struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"target"`
	Pins struct {
		Packs map[string]string `json:"packs"`
	} `json:"pins"`
	Rows []roster161Row `json:"rows"`
}

// readRoster161 reads the roster at path below root and refuses bytes whose
// SHA-256 differs from digest before decoding them.
func readRoster161(root, path, digest string) (roster161, error) {
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
	if err != nil {
		return roster161{}, err
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(raw)); got != digest {
		return roster161{}, fmt.Errorf("roster digest %s, pinned %s", got, digest)
	}
	var roster roster161
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&roster); err != nil {
		return roster161{}, err
	}
	return roster, nil
}

// checkRoster161 compares the adopted roster with the 1.6.1 manifest and the
// shared ledger. Fact, capability and operation rows are the projected kinds and
// must equal the ledger; service and relation rows are composed by the endpoint
// work of the Matter binding and are counted but not projected here. Any other
// row kind is refused.
func checkRoster161(roster roster161) (map[string]int, error) {
	manifest := Manifest161()
	if roster.Contract != Contract || roster.Target.Name != string(TargetID) || roster.Target.Version != string(TargetVersion161) || roster.MappingRevision.String() != string(MappingRevision161) {
		return nil, errors.New("roster identity differs from the 1.6.1 target")
	}
	packs := make(map[string]string, len(roster.Pins.Packs))
	for name, version := range roster.Pins.Packs {
		packs["helianthus.pack."+name] = version
	}
	if len(packs) != len(manifest.PackVersions) {
		return nil, errors.New("roster pins another pack set")
	}
	for _, pack := range manifest.PackVersions {
		if packs[string(pack.ID)] != string(pack.Version) {
			return nil, fmt.Errorf("roster pins %s at %q", pack.ID, packs[string(pack.ID)])
		}
	}
	ignored := map[string]int{}
	requested := make([]projection.RequestedItem, 0, len(roster.Rows))
	for _, row := range roster.Rows {
		if version, ok := packs[row.Ref.Pack.ID]; !ok || version != row.Ref.Pack.Version {
			return nil, fmt.Errorf("row %s names an unpinned pack", row.Ref.ID)
		}
		kind := projection.ItemKind(row.Kind)
		switch kind {
		case projection.ItemFact, projection.ItemCapability, projection.ItemOperation:
			requested = append(requested, projection.RequestedItem{Kind: kind, ItemID: semreg.DefinitionID(row.Ref.ID)})
			if kind == projection.ItemFact && row.Ref.ID == "evse.ac.current" {
				if row.Disposition != string(projection.ProjectionTransformed) || row.Target == nil || row.Target.DeviceTypeID != 0x0510 || row.Target.ClusterID != 0x0090 || row.Target.AttributeID != 0x0005 {
					return nil, errors.New("current row is not the transformed ActiveCurrent mapping")
				}
				continue
			}
			if row.Disposition != string(projection.ProjectionUnknown) || row.Reason != "matter.unmapped.v1" || row.Target != nil {
				return nil, fmt.Errorf("row %s is not an unmapped ledger row", row.Ref.ID)
			}
		case "service", projection.ItemRelation:
			ignored[row.Kind]++
		default:
			return nil, fmt.Errorf("row %s has unknown kind %q", row.Ref.ID, row.Kind)
		}
	}
	sort.Slice(requested, func(i, j int) bool {
		if requested[i].Kind != requested[j].Kind {
			return requested[i].Kind < requested[j].Kind
		}
		return requested[i].ItemID < requested[j].ItemID
	})
	if want, _ := Ledger(); !reflect.DeepEqual(requested, want) {
		return nil, errors.New("projected roster rows differ from the ledger")
	}
	return ignored, nil
}

func TestAA03RosterPin(t *testing.T) {
	root := stagedDocsSemanticRoot(t)
	roster, err := readRoster161(root, DocsSemanticContractPath161, DocsSemanticContractSHA256161)
	if err != nil {
		t.Fatal(err)
	}
	ignored, err := checkRoster161(roster)
	if err != nil {
		t.Fatal(err)
	}
	if ignored["service"] == 0 || ignored[string(projection.ItemRelation)] == 0 {
		t.Fatalf("roster derived from the element catalog lacks service or relation rows: %v", ignored)
	}
}

func TestAA03RosterPinRejectsDrift(t *testing.T) {
	root := stagedDocsSemanticRoot(t)
	t.Run("path", func(t *testing.T) {
		if _, err := readRoster161(root, DocsSemanticContractPath, DocsSemanticContractSHA256161); err == nil {
			t.Fatal("accepted the historical contract under the 1.6.1 digest")
		}
		if _, err := readRoster161(root, "api/v1/targets/matter-absent-v1.json", DocsSemanticContractSHA256161); err == nil {
			t.Fatal("accepted an absent roster")
		}
	})
	t.Run("digest", func(t *testing.T) {
		other := fmt.Sprintf("%x", sha256.Sum256([]byte("synthetic other roster")))
		if _, err := readRoster161(root, DocsSemanticContractPath161, other); err == nil {
			t.Fatal("accepted a roster under another digest")
		}
	})
	firstRow := func(r *roster161, kind string) *roster161Row {
		for i := range r.Rows {
			if r.Rows[i].Kind == kind {
				return &r.Rows[i]
			}
		}
		t.Fatalf("roster has no %s row", kind)
		return nil
	}
	cases := map[string]func(*roster161){
		"contract":         func(r *roster161) { r.Contract = "other.contract/v1" },
		"target name":      func(r *roster161) { r.Target.Name = "other.target" },
		"target version":   func(r *roster161) { r.Target.Version = string(TargetVersion) },
		"mapping revision": func(r *roster161) { r.MappingRevision = "2" },
		"pack version":     func(r *roster161) { r.Pins.Packs["storage"] = "9.0.0" },
		"pack id": func(r *roster161) {
			r.Pins.Packs["other"] = r.Pins.Packs["thermal"]
			delete(r.Pins.Packs, "thermal")
		},
		"unknown row kind":      func(r *roster161) { firstRow(r, "service").Kind = "dimension" },
		"service as fact":       func(r *roster161) { firstRow(r, "service").Kind = string(projection.ItemFact) },
		"dropped projected row": func(r *roster161) { firstRow(r, string(projection.ItemCapability)).Kind = "service" },
		"relation as operation": func(r *roster161) {
			firstRow(r, string(projection.ItemRelation)).Kind = string(projection.ItemOperation)
		},
		"unmapped row disposed":  func(r *roster161) { firstRow(r, string(projection.ItemOperation)).Disposition = "unsupported" },
		"unmapped row reason":    func(r *roster161) { firstRow(r, string(projection.ItemOperation)).Reason = "matter.altered.v1" },
		"row from unpinned pack": func(r *roster161) { firstRow(r, string(projection.ItemRelation)).Ref.Pack.Version = "9.0.0" },
		"current attribute": func(r *roster161) {
			for i := range r.Rows {
				if r.Rows[i].Ref.ID == "evse.ac.current" && r.Rows[i].Target != nil {
					r.Rows[i].Target.AttributeID++
				}
			}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			roster, err := readRoster161(root, DocsSemanticContractPath161, DocsSemanticContractSHA256161)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := checkRoster161(roster); err != nil {
				t.Fatalf("control: %v", err)
			}
			mutate(&roster)
			if _, err := checkRoster161(roster); err == nil {
				t.Fatalf("accepted %s drift", name)
			}
		})
	}
}
