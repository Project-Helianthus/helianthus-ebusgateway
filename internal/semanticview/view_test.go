package semanticview

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/evse"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/infrastructure"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/pv"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/storage"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
	"reflect"
	"sort"
	"testing"
)

func m08JSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func m08Base(t *testing.T) (semreg.Snapshot, projection.ProjectionReport) {
	t.Helper()
	// Entirely synthetic: one source, binding, candidate and referenced fact.
	digest := sha256.Sum256([]byte("synthetic projection revision axes evidence"))
	evidence := semreg.EvidenceRef{
		Owner: "owner.test", Kind: "evidence.test", Digest: semreg.Digest(fmt.Sprintf("sha256:%x", digest)),
		Contract: "test.contract/v1", Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone,
	}
	source := semreg.SourceID("source:test")
	epoch := semreg.SourceEpochID("epoch:test")
	binding := semreg.NativeBindingID("binding:test")
	generation := semreg.Uint64("1")
	point := semreg.TimePoint{UnixNanoseconds: "1", ClockID: "clock.test", UncertaintyNS: "0"}
	monotonic := semreg.MonotonicPoint{ClockEpochID: "clock-epoch:test", Nanoseconds: "1"}
	key := semreg.FactKey{PackID: "pack.test", PackVersion: "1.0.0", FactID: "fact.test", Dimensions: []semreg.Dimension{}}
	value := true
	snapshot := semreg.Snapshot{
		Contract: semreg.ContractKernelV1, AssetID: "asset:test",
		Revisions:   semreg.RevisionVector{Semantic: "2", Identity: "3", Facts: "4", Services: "5", Capabilities: "6"},
		EvaluatedAt: point, EvaluateMonotonic: monotonic,
		Sources: []semreg.SourceDescriptor{{
			SourceID: source, SourceEpochID: epoch, ProtocolID: "protocol.test", ProfileID: "profile.test", ProfileVersion: "test-1",
			RegistryEvidence: evidence, StartedAt: point, State: semreg.SourceCurrent, Revision: "1",
		}},
		Bindings: []semreg.NativeBinding{{
			BindingID: binding, AssetID: "asset:test", SourceID: source, SourceEpochID: epoch, DriverGeneration: generation,
			NativeResource: evidence, State: semreg.BindingCurrent, Revision: "1",
		}},
		IdentityLinks: []semreg.IdentityLink{},
		Facts: []semreg.FactEnvelope{{
			AssetID: "asset:test", Key: key, Revision: "1", Conflicts: []semreg.Conflict{},
			Candidates: []semreg.FactCandidate{{
				CandidateID: "candidate:test", Key: key, Value: &semreg.Value{Kind: semreg.ValueBoolean, Boolean: &value},
				Quality: semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified,
					Promotion: semreg.PromotionPromoted, Validity: semreg.ValidityGood, Availability: semreg.AvailabilityAvailable,
					Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}},
				Times:           semreg.Times{ReceivedAt: point, ReceiptMonotonic: monotonic, EvaluatedAt: point, EvaluateMonotonic: monotonic},
				FreshnessPolicy: semreg.FreshnessPolicy{PolicyID: "policy:test", Version: "1.0.0", FreshForNS: "1", RetainForNS: "2", MaxWallUncertaintyNS: "0"},
				BindingID:       &binding, SourceEpochID: &epoch, DriverGeneration: &generation,
				Origin: semreg.OriginRef{OriginID: "origin:test", Kind: semreg.OriginNativeObservation,
					SourceID: &source, SourceEpochID: &epoch, BindingID: &binding, Evidence: []semreg.EvidenceRef{evidence}},
				Evidence: []semreg.EvidenceRef{evidence}, Revision: "1",
			}},
		}},
		Services: []semreg.ServiceInstance{}, Capabilities: []semreg.CapabilityInstance{}, Fences: []semreg.GenerationFence{},
		Cursors: []semreg.PublicationCursor{{SourceID: source, SourceEpochID: epoch, DriverGeneration: generation,
			LastSequence: "1", LastBatchDigest: evidence.Digest}},
	}
	var document map[string]any
	if err := json.Unmarshal(m08JSON(t, snapshot), &document); err != nil {
		t.Fatal(err)
	}
	delete(document, "snapshot_id")
	snapshotDigest := sha256.Sum256(m08JSON(t, document))
	snapshot.SnapshotID = semreg.SnapshotID(fmt.Sprintf("sha256:%x", snapshotDigest))
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("synthetic snapshot: %v", err)
	}
	report := projection.ProjectionReport{
		Contract: projection.ContractProjectionV1, SnapshotID: snapshot.SnapshotID, Revisions: snapshot.Revisions,
		Manifest: projection.ProjectionManifest{TargetID: "target:test", TargetVersion: "test-1", KernelVersion: semreg.ContractKernelV1,
			PackVersions: []semreg.PackRef{{ID: "pack.test", Version: "1.0.0"}}, MappingRevision: "1"},
		Requested: []projection.RequestedItem{{Kind: projection.ItemFact, ItemID: "fact.test"}},
		Dispositions: []projection.ProjectionDisposition{{Kind: projection.ItemFact, ItemID: "fact.test", Outcome: projection.ProjectionExact,
			SourceKeys: []semreg.FactKey{key}, Loss: []projection.LossDetail{}}},
	}
	return snapshot, report
}

func m08Validators() []semreg.PackValidator {
	return []semreg.PackValidator{evse.New(), infrastructure.New(), pv.New(), storage.New(), thermal.New()}
}

func m08Evaluate(t *testing.T, s semreg.Snapshot) semreg.EvaluationView {
	t.Helper()
	v, err := semreg.EvaluateSnapshot(s, semreg.EvaluationContext{EvaluatedAt: s.EvaluatedAt, EvaluateMonotonic: s.EvaluateMonotonic})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func m08Fixture(t *testing.T) semreg.Snapshot {
	t.Helper()
	s, _ := m08Base(t)
	candidate := s.Facts[0].Candidates[0]
	s.Facts = []semreg.FactEnvelope{}
	s.Services = []semreg.ServiceInstance{}
	s.Capabilities = []semreg.CapabilityInstance{}
	for _, validator := range m08Validators() {
		metadata := validator.(semreg.PackMetadataProvider).Metadata()
		var field semreg.FieldMetadata
		for _, f := range metadata.Fields {
			if f.CanonicalUnit != nil {
				field = f
				break
			}
		}
		label := "synthetic"
		key := semreg.FactKey{PackID: metadata.Pack.ID, PackVersion: metadata.Pack.Version, FactID: field.Ref.ID, Dimensions: []semreg.Dimension{{ID: field.Dimension.ID, Value: semreg.Value{Kind: semreg.ValueText, Text: &label}}}}
		c := candidate
		c.CandidateID = semreg.CandidateID("candidate:" + string(metadata.Pack.ID))
		c.Key = key
		c.Value = &semreg.Value{Kind: semreg.ValueQuantity, Quantity: &semreg.Quantity{Number: semreg.Decimal{Coefficient: "0"}, Unit: field.CanonicalUnit.ID}}
		c.Quality.Reasons = []semreg.DefinitionID{"reason.synthetic"}
		s.Facts = append(s.Facts, semreg.FactEnvelope{AssetID: s.AssetID, Key: key, Candidates: []semreg.FactCandidate{c}, Conflicts: []semreg.Conflict{}, Revision: "1"})
		var cap semreg.CapabilityMetadata
		for _, item := range metadata.Capabilities {
			if bytes.Contains([]byte(item.Ref.ID), []byte(".read.")) {
				cap = item
				break
			}
		}
		id := string(metadata.Pack.ID)
		service := semreg.ServiceInstance{InstanceID: semreg.ServiceInstanceID("service:" + id), AssetID: s.AssetID, Definition: cap.Service, BindingID: s.Bindings[0].BindingID, SourceEpochID: s.Bindings[0].SourceEpochID, DriverGeneration: s.Bindings[0].DriverGeneration, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Revision: "1"}
		s.Services = append(s.Services, service)
		s.Capabilities = append(s.Capabilities, semreg.CapabilityInstance{InstanceID: semreg.CapabilityInstanceID("capability:" + id), AssetID: s.AssetID, ServiceInstance: service.InstanceID, Definition: cap.Ref, BindingID: service.BindingID, SourceEpochID: service.SourceEpochID, DriverGeneration: service.DriverGeneration, Qualification: service.Qualification, Availability: service.Availability, Constraints: []semreg.TypedField{}, ActivationEvidence: []semreg.EvidenceRef{s.Bindings[0].NativeResource}, Revision: "1"})
	}
	sort.Slice(s.Facts, func(i, j int) bool {
		a, _ := semreg.CanonicalJSON(s.Facts[i].Key)
		b, _ := semreg.CanonicalJSON(s.Facts[j].Key)
		return bytes.Compare(a, b) < 0
	})
	m08Seal(t, &s)
	return s
}

func m08Registry(t *testing.T) *semreg.PackMetadataRegistry {
	t.Helper()
	r, err := packs.NewMetadataRegistry()
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func m08Seal(t *testing.T, snapshot *semreg.Snapshot) {
	t.Helper()
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	delete(document, "snapshot_id")
	raw, err = json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	snapshot.SnapshotID = semreg.SnapshotID(fmt.Sprintf("sha256:%x", digest))
	if err := snapshot.Validate(); err != nil {
		t.Fatalf("expanded projection snapshot: %v", err)
	}
}

func m08Input(t *testing.T) Input {
	t.Helper()
	s := m08Fixture(t)
	e := m08Evaluate(t, s)
	return Input{AssetID: s.AssetID, Snapshot: &s, Evaluation: &e}
}
func m08Build(t *testing.T, in Input) View {
	t.Helper()
	v, err := Build(m08Registry(t), []Input{in})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestM08FivePacks(t *testing.T) {
	for _, state := range []string{"fresh", "stale", "expired", "unknown", "unqualified", "absent_value", "withdrawn"} {
		t.Run(state, func(t *testing.T) {
			in := m08Input(t)
			switch state {
			case "unqualified":
				for i := range in.Snapshot.Capabilities {
					in.Snapshot.Capabilities[i].Qualification = semreg.QualificationCandidate
				}
			case "absent_value":
				for i := range in.Snapshot.Facts {
					in.Snapshot.Facts[i].Candidates[0].Value = nil
					in.Snapshot.Facts[i].Candidates[0].Quality.Qualification = semreg.QualificationUnsupported
					in.Snapshot.Facts[i].Candidates[0].Quality.Promotion = semreg.PromotionUnpromoted
				}
			case "withdrawn":
				for i := range in.Snapshot.Capabilities {
					in.Snapshot.Capabilities[i].Availability = semreg.AvailabilityWithdrawn
				}
			}
			m08Seal(t, in.Snapshot)
			context := in.Evaluation.Context
			switch state {
			case "stale":
				context.EvaluateMonotonic.Nanoseconds = "2"
			case "expired":
				context.EvaluateMonotonic.Nanoseconds = "3"
			case "unknown":
				context.EvaluateMonotonic.ClockEpochID = "clock-epoch:other"
			}
			e, err := semreg.EvaluateSnapshot(*in.Snapshot, context)
			if err != nil {
				t.Fatal(err)
			}
			in.Evaluation = &e
			before := m08JSON(t, in)
			v := m08Build(t, in)
			a := v.Assets[0]
			if !bytes.Equal(m08JSON(t, *a.Snapshot), m08JSON(t, *in.Snapshot)) || !bytes.Equal(m08JSON(t, *a.Evaluation), m08JSON(t, e)) {
				t.Fatalf("source changed snapshot=%v evaluation=%v", bytes.Equal(m08JSON(t, *a.Snapshot), m08JSON(t, *in.Snapshot)), bytes.Equal(m08JSON(t, *a.Evaluation), m08JSON(t, e)))
			}
			if len(a.Availability.Packs) != 5 {
				t.Fatal("missing pack")
			}
			for _, pack := range a.Availability.Packs {
				if len(pack.Facts) != 1 || len(pack.Capabilities) != 1 {
					t.Fatal("lost pack records")
				}
				if !reflect.DeepEqual(pack.Facts[0].Envelope.Candidates[0].Value, in.Snapshot.Facts[indexOfFact(in.Snapshot.Facts, pack.Facts[0].Envelope.Key.FactID)].Candidates[0].Value) {
					t.Fatal("exact value lost")
				}
			}
			if a.Projection.SnapshotID != in.Snapshot.SnapshotID || a.Projection.Revisions != in.Snapshot.Revisions {
				t.Fatal("projection identity changed")
			}
			missing := 0
			for _, d := range a.Projection.Dispositions {
				if len(d.Loss) != 0 {
					t.Fatal("invented transformation")
				}
				if len(d.SourceKeys) == 0 && d.Kind == projection.ItemFact {
					missing++
					if d.Outcome != projection.ProjectionUnknown || d.Reason == nil {
						t.Fatal("absent item erased")
					}
				}
				if d.Kind == projection.ItemCapability && hasCapability(in.Snapshot.Capabilities, d.ItemID) && (state == "unqualified" || state == "withdrawn") && d.Outcome != projection.ProjectionWithheld {
					t.Fatal("unsafe capability admitted")
				}
				if d.Kind == projection.ItemFact && len(d.SourceKeys) > 0 {
					want := projection.ProjectionExact
					if state == "expired" {
						want = projection.ProjectionWithheld
					}
					if state == "absent_value" {
						want = projection.ProjectionUnsupported
					}
					if d.Outcome != want {
						t.Fatalf("fact disposition %s want %s", d.Outcome, want)
					}
				}
			}
			if missing == 0 {
				t.Fatal("fixture must exercise absent definitions")
			}
			if !bytes.Equal(before, m08JSON(t, in)) {
				t.Fatal("input changed")
			}
		})
	}
}
func indexOfFact(facts []semreg.FactEnvelope, id semreg.DefinitionID) int {
	for i, f := range facts {
		if f.Key.FactID == id {
			return i
		}
	}
	return -1
}
func hasCapability(caps []semreg.CapabilityInstance, id semreg.DefinitionID) bool {
	for _, c := range caps {
		if c.Definition.ID == id {
			return true
		}
	}
	return false
}

func TestM08Detached(t *testing.T) {
	in := m08Input(t)
	a := m08Build(t, in)
	b := m08Build(t, in)
	baseline := m08JSON(t, b)
	in.Snapshot.Facts[0].Candidates[0].Value.Quantity.Number.Coefficient = "99"
	in.Snapshot.Facts[0].Candidates[0].Quality.Reasons[0] = "reason.changed"
	*in.Snapshot.Facts[0].Key.Dimensions[0].Value.Text = "changed"
	in.Evaluation.Facts[0].Freshness = semreg.FreshnessExpired
	if !bytes.Equal(baseline, m08JSON(t, a)) {
		t.Fatal("caller mutation reached capture")
	}
	a.Assets[0].Snapshot.Facts[0].Candidates[0].Value.Quantity.Number.Coefficient = "88"
	a.Assets[0].Evaluation.Facts[0].Freshness = semreg.FreshnessUnknown
	*a.Assets[0].Projection.Dispositions[len(a.Assets[0].Projection.Dispositions)-1].Reason = "reason.changed"
	a.Assets[0].Availability.Packs[0].Facts[0].Envelope.Candidates[0].Value.Quantity.Number.Coefficient = "77"
	if !bytes.Equal(baseline, m08JSON(t, b)) {
		t.Fatal("responses alias")
	}
	if b.Assets[0].Availability.Packs[0].Facts[0].Envelope.Candidates[0].Value.Quantity.Number.Coefficient != "0" {
		t.Fatal("availability aliases response")
	}
	if a.Assets[0].Snapshot.Facts[0].Candidates[0].Value.Quantity.Number.Coefficient != "88" {
		t.Fatal("availability aliases source snapshot")
	}
}

func TestM08IdentityRejection(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Input)
	}{
		{"asset_id", func(i *Input) { i.AssetID = "asset:other" }},
		{"snapshot_id", func(i *Input) { i.Evaluation.SnapshotID = "snapshot:other" }},
		{"semantic", func(i *Input) { i.Evaluation.Revisions.Semantic = "9" }},
		{"identity", func(i *Input) { i.Evaluation.Revisions.Identity = "9" }},
		{"facts", func(i *Input) { i.Evaluation.Revisions.Facts = "9" }},
		{"services", func(i *Input) { i.Evaluation.Revisions.Services = "9" }},
		{"capabilities", func(i *Input) { i.Evaluation.Revisions.Capabilities = "9" }},
		{"candidate_id", func(i *Input) { i.Evaluation.Facts[0].CandidateID = "candidate:aaa-other" }},
		{"candidate_revision", func(i *Input) { i.Evaluation.Facts[0].CandidateRevision = "9" }},
		{"missing_snapshot", func(i *Input) { i.Snapshot = nil }},
		{"missing_evaluation", func(i *Input) { i.Evaluation = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := m08Input(t)
			tt.edit(&in)
			if in.Evaluation != nil {
				d, err := in.Evaluation.EvaluationDigestValue()
				if err != nil {
					t.Fatal(err)
				}
				in.Evaluation.EvaluationDigest = d
			}
			out, err := Build(m08Registry(t), []Input{in})
			if err == nil || len(err.Error()) > 256 || !reflect.DeepEqual(out, View{}) {
				t.Fatalf("invalid input accepted: %v", err)
			}
		})
	}
}

func TestM08OrderingAndUnavailable(t *testing.T) {
	in := m08Input(t)
	other := m08Input(t)
	other.AssetID = "asset:other"
	s := other.Snapshot
	s.AssetID = other.AssetID
	for i := range s.Bindings {
		s.Bindings[i].AssetID = s.AssetID
	}
	for i := range s.Facts {
		s.Facts[i].AssetID = s.AssetID
	}
	for i := range s.Services {
		s.Services[i].AssetID = s.AssetID
	}
	for i := range s.Capabilities {
		s.Capabilities[i].AssetID = s.AssetID
	}
	s.Revisions.Identity = "8"
	m08Seal(t, s)
	e := m08Evaluate(t, *s)
	other.Evaluation = &e
	missing := Input{AssetID: "asset:missing", UnavailableReason: reasonPrefix + "asset_unknown"}
	a, err := Build(m08Registry(t), []Input{in, missing, other})
	if err != nil {
		t.Fatal(err)
	}
	b, err := Build(m08Registry(t), []Input{other, in, missing})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(m08JSON(t, a), m08JSON(t, b)) {
		t.Fatal("input order changed output")
	}
	if a.Assets[0].UnavailableReason == "" || a.Assets[0].Snapshot != nil || len(a.Assets[0].Selections) != 0 {
		t.Fatal("missing asset erased")
	}
	for _, inputs := range [][]Input{nil, {in, in}, {{AssetID: in.AssetID}}, {{AssetID: "asset:missing", UnavailableReason: "raw error"}}} {
		out, err := Build(m08Registry(t), inputs)
		if err == nil || !reflect.DeepEqual(out, View{}) {
			t.Fatal("invalid selection succeeded")
		}
	}
	if _, err := Build(nil, []Input{in}); err == nil {
		t.Fatal("unwired provider succeeded")
	}
}

func TestM08FactKeyFields(t *testing.T) {
	for _, tt := range []struct {
		name              string
		edit              func(*semreg.FactKey)
		invalid           bool
		availabilityFacts int
	}{
		{"pack_id", func(k *semreg.FactKey) { k.PackID = "pack.synthetic.other" }, false, 1},
		{"pack_version", func(k *semreg.FactKey) { k.PackVersion = "9.0.0" }, false, 1},
		{"fact_id", func(k *semreg.FactKey) { k.FactID = "fact.synthetic.other" }, true, 0},
		{"dimension_id", func(k *semreg.FactKey) { k.Dimensions[0].ID = "dimension.synthetic.other" }, true, 0},
		{"dimension_value", func(k *semreg.FactKey) { *k.Dimensions[0].Value.Text = "another" }, false, 2},
		{"dimension_value_kind", func(k *semreg.FactKey) {
			value := false
			k.Dimensions[0].Value = semreg.Value{Kind: semreg.ValueBoolean, Boolean: &value}
		}, true, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			in := m08Input(t)
			original := in.Snapshot.Facts[0]
			extra, err := clone(original)
			if err != nil {
				t.Fatal(err)
			}
			tt.edit(&extra.Key)
			extra.Candidates[0].Key, err = clone(extra.Key)
			if err != nil {
				t.Fatal(err)
			}
			extra.Candidates[0].CandidateID = "candidate:zz-extra"
			in.Snapshot.Facts = append(in.Snapshot.Facts, extra)
			sort.Slice(in.Snapshot.Facts, func(i, j int) bool {
				a, _ := semreg.CanonicalJSON(in.Snapshot.Facts[i].Key)
				b, _ := semreg.CanonicalJSON(in.Snapshot.Facts[j].Key)
				return bytes.Compare(a, b) < 0
			})
			m08Seal(t, in.Snapshot)
			e := m08Evaluate(t, *in.Snapshot)
			in.Evaluation = &e
			out, err := Build(m08Registry(t), []Input{in})
			if tt.invalid {
				if err == nil || !reflect.DeepEqual(out, View{}) {
					t.Fatal("invalid pack key admitted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(out.Assets[0].Availability.Packs[0].Facts) != tt.availabilityFacts {
				t.Fatal("inexact pack match or collapsed dimensions")
			}
			for _, d := range out.Assets[0].Projection.Dispositions {
				if d.Kind == projection.ItemFact && d.ItemID == original.Key.FactID {
					if len(d.SourceKeys) != 2 {
						t.Fatal("projection collapsed a full key")
					}
					foundOriginal, foundExtra := false, false
					for _, key := range d.SourceKeys {
						foundOriginal = foundOriginal || reflect.DeepEqual(key, original.Key)
						foundExtra = foundExtra || reflect.DeepEqual(key, extra.Key)
					}
					if !foundOriginal || !foundExtra {
						t.Fatal("projection key changed")
					}
				}
			}
		})
	}
}

func TestM08STD01AndExactNumbers(t *testing.T) {
	in := m08Input(t)
	for i := range in.Snapshot.Facts {
		c := &in.Snapshot.Facts[i].Candidates[0]
		c.Value.Quantity.Number = semreg.Decimal{Coefficient: fmt.Sprint(i + 1), Exponent10: -9}
		c.Quality.Reasons = []semreg.DefinitionID{"unknown_pending_std_01"}
		c.Quality.Qualification = semreg.QualificationCandidate
		c.Quality.Promotion = semreg.PromotionUnpromoted
	}
	m08Seal(t, in.Snapshot)
	e := m08Evaluate(t, *in.Snapshot)
	in.Evaluation = &e
	out := m08Build(t, in)
	for _, p := range out.Assets[0].Availability.Packs {
		fact := p.Facts[0].Envelope
		original := in.Snapshot.Facts[indexOfFact(in.Snapshot.Facts, fact.Key.FactID)]
		if !reflect.DeepEqual(fact, original) {
			t.Fatal("decimal, qualification or source evidence changed")
		}
	}
	for _, d := range out.Assets[0].Projection.Dispositions {
		if len(d.SourceKeys) > 0 && (d.Outcome != projection.ProjectionUnknown || d.Reason == nil || *d.Reason != "unknown_pending_std_01") {
			t.Fatal("STD-01 support inferred")
		}
	}
}

func TestM08EvaluationContextIdentity(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*semreg.EvaluationContext)
	}{
		{"wall_point", func(c *semreg.EvaluationContext) { c.EvaluatedAt.UnixNanoseconds = "2" }},
		{"wall_clock", func(c *semreg.EvaluationContext) { c.EvaluatedAt.ClockID = "clock.other" }},
		{"wall_uncertainty", func(c *semreg.EvaluationContext) { c.EvaluatedAt.UncertaintyNS = "1" }},
		{"monotonic_epoch", func(c *semreg.EvaluationContext) { c.EvaluateMonotonic.ClockEpochID = "clock-epoch:other" }},
		{"monotonic_point", func(c *semreg.EvaluationContext) { c.EvaluateMonotonic.Nanoseconds = "2" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			in := m08Input(t)
			original := in.Evaluation.EvaluationDigest
			tt.edit(&in.Evaluation.Context)
			if out, err := Build(m08Registry(t), []Input{in}); err == nil || !reflect.DeepEqual(out, View{}) {
				t.Fatal("context tampering accepted")
			}
			e, err := semreg.EvaluateSnapshot(*in.Snapshot, in.Evaluation.Context)
			if err != nil {
				t.Fatal(err)
			}
			in.Evaluation = &e
			out := m08Build(t, in)
			if !reflect.DeepEqual(out.Assets[0].Evaluation.Context, e.Context) || out.Assets[0].Availability.EvaluationDigest == original {
				t.Fatal("context identity collapsed")
			}
		})
	}
}

// m08Selection builds a valid presentation selection for the fact at index.
func m08Selection(in Input, index int, policy semreg.PolicyID, version semreg.SemanticVersion) semreg.Selection {
	candidate := in.Snapshot.Facts[index].Candidates[0]
	return semreg.Selection{
		Contract: semreg.ContractSelectionV1, SnapshotID: in.Snapshot.SnapshotID, Revisions: in.Snapshot.Revisions,
		EvaluationDigest: in.Evaluation.EvaluationDigest, Context: in.Evaluation.Context,
		Key: in.Snapshot.Facts[index].Key, PolicyID: policy, PolicyVersion: version,
		SelectedCandidate: candidate.CandidateID, CandidateRevision: candidate.Revision, PresentationOnly: true,
	}
}

func TestM08ContextMismatchAcrossAssets(t *testing.T) {
	first := m08Input(t)
	second := m08Input(t)
	second.AssetID = "asset:other"
	s := second.Snapshot
	s.AssetID = second.AssetID
	for i := range s.Bindings {
		s.Bindings[i].AssetID = s.AssetID
	}
	for i := range s.Facts {
		s.Facts[i].AssetID = s.AssetID
	}
	for i := range s.Services {
		s.Services[i].AssetID = s.AssetID
	}
	for i := range s.Capabilities {
		s.Capabilities[i].AssetID = s.AssetID
	}
	m08Seal(t, s)
	later := semreg.EvaluationContext{EvaluatedAt: s.EvaluatedAt, EvaluateMonotonic: s.EvaluateMonotonic}
	later.EvaluatedAt.UnixNanoseconds = "2"
	later.EvaluateMonotonic.Nanoseconds = "2"
	e, err := semreg.EvaluateSnapshot(*s, later)
	if err != nil {
		t.Fatal(err)
	}
	second.Evaluation = &e
	if _, err := Build(m08Registry(t), []Input{second}); err != nil {
		t.Fatalf("second asset alone must be valid: %v", err)
	}
	for _, inputs := range [][]Input{{first, second}, {second, first}} {
		out, err := Build(m08Registry(t), inputs)
		if err == nil || !reflect.DeepEqual(out, View{}) {
			t.Fatalf("differing evaluation contexts accepted: %v", err)
		}
	}
}

func TestM08SelectionsDetached(t *testing.T) {
	in := m08Input(t)
	in.Selections = []semreg.Selection{m08Selection(in, 0, "policy:a", "1.0.0")}
	view := m08Build(t, in)
	other := m08Build(t, in)
	baseline := m08JSON(t, view)
	if len(view.Assets[0].Selections) != 1 {
		t.Fatal("selection not published")
	}
	in.Selections[0].PolicyID = "policy:changed"
	in.Selections[0].Key.Dimensions[0].ID = "dimension.changed"
	*in.Selections[0].Key.Dimensions[0].Value.Text = "changed"
	in.Selections[0].Key.Dimensions = append(in.Selections[0].Key.Dimensions[:0], semreg.Dimension{})
	if !bytes.Equal(baseline, m08JSON(t, view)) {
		t.Fatal("input selection mutation reached the view")
	}
	other.Assets[0].Selections[0].Key.Dimensions[0].Value.Text = nil
	other.Assets[0].Selections[0].PolicyID = "policy:mutated"
	if !bytes.Equal(baseline, m08JSON(t, view)) {
		t.Fatal("views alias")
	}
}

func m08SelectionInput(t *testing.T) Input {
	in := m08Input(t)
	in.Selections = []semreg.Selection{m08Selection(in, 0, "policy:a", "1.0.0")}
	return in
}

func TestM08SelectionRevisionMismatchRejected(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(*semreg.Selection)
	}{
		{"semantic_revision", func(s *semreg.Selection) { s.Revisions.Semantic = "9" }},
		{"facts_revision", func(s *semreg.Selection) { s.Revisions.Facts = "9" }},
		{"candidate_revision", func(s *semreg.Selection) { s.CandidateRevision = "9" }},
		{"snapshot_id", func(s *semreg.Selection) {
			s.SnapshotID = semreg.SnapshotID("sha256:" + string(bytes.Repeat([]byte("0"), 64)))
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			in := m08SelectionInput(t)
			tt.edit(&in.Selections[0])
			out, err := Build(m08Registry(t), []Input{in})
			if err == nil || !reflect.DeepEqual(out, View{}) {
				t.Fatalf("mismatched selection accepted: %v", err)
			}
		})
	}
}

func TestM08SelectionDuplicateKeyRejected(t *testing.T) {
	in := m08SelectionInput(t)
	in.Selections = append(in.Selections, in.Selections[0])
	out, err := Build(m08Registry(t), []Input{in})
	if err == nil || !reflect.DeepEqual(out, View{}) {
		t.Fatalf("duplicate selection accepted: %v", err)
	}
	// Same fact key under a different policy is a distinct selection.
	in.Selections[1].PolicyID = "policy:b"
	if _, err := Build(m08Registry(t), []Input{in}); err != nil {
		t.Fatalf("distinct policy rejected: %v", err)
	}
}

func TestM08SelectionOrdering(t *testing.T) {
	in := m08Input(t)
	ordered := []semreg.Selection{
		m08Selection(in, 0, "policy:a", "1.0.0"),
		m08Selection(in, 0, "policy:a", "1.0.1"),
		m08Selection(in, 0, "policy:b", "1.0.0"),
		m08Selection(in, 1, "policy:a", "1.0.0"),
		m08Selection(in, 2, "policy:a", "1.0.0"),
	}
	sort.Slice(ordered, func(i, j int) bool { return selectionKey(ordered[i]) < selectionKey(ordered[j]) })
	var want [][]byte
	for _, s := range ordered {
		want = append(want, m08JSON(t, s))
	}
	// Feed in reverse of the expected order and in a rotated order.
	reversed := make([]semreg.Selection, len(ordered))
	for i := range ordered {
		reversed[len(ordered)-1-i] = ordered[i]
	}
	rotated := append(append([]semreg.Selection{}, ordered[2:]...), ordered[:2]...)
	for _, input := range [][]semreg.Selection{reversed, rotated} {
		in.Selections = input
		got := m08Build(t, in).Assets[0].Selections
		if len(got) != len(want) {
			t.Fatalf("selection count %d", len(got))
		}
		for i := range got {
			if !bytes.Equal(m08JSON(t, got[i]), want[i]) {
				t.Fatalf("selection %d out of order", i)
			}
		}
	}
	// The order is by key first, then policy id, then policy version.
	for i := 1; i < len(ordered); i++ {
		a, b := ordered[i-1], ordered[i]
		ka, _ := semreg.CanonicalJSON(a.Key)
		kb, _ := semreg.CanonicalJSON(b.Key)
		if c := bytes.Compare(ka, kb); c > 0 || (c == 0 && (a.PolicyID > b.PolicyID || (a.PolicyID == b.PolicyID && a.PolicyVersion > b.PolicyVersion))) {
			t.Fatalf("expected order violates key, policy id, version at %d", i)
		}
	}
}

func TestM08IdentityFieldMutations(t *testing.T) {
	generation := semreg.Uint64("2")
	for _, tt := range []struct {
		name string
		edit func(*semreg.Snapshot)
	}{
		{"driver_generation", func(s *semreg.Snapshot) {
			for i := range s.Bindings {
				s.Bindings[i].DriverGeneration = generation
			}
			for i := range s.Facts {
				for j := range s.Facts[i].Candidates {
					s.Facts[i].Candidates[j].DriverGeneration = &generation
				}
			}
			for i := range s.Cursors {
				s.Cursors[i].DriverGeneration = generation
			}
			for i := range s.Services {
				s.Services[i].DriverGeneration = generation
			}
			for i := range s.Capabilities {
				s.Capabilities[i].DriverGeneration = generation
			}
		}},
		{"source_epoch_id", func(s *semreg.Snapshot) {
			epoch := semreg.SourceEpochID("epoch:other")
			for i := range s.Sources {
				s.Sources[i].SourceEpochID = epoch
			}
			for i := range s.Bindings {
				s.Bindings[i].SourceEpochID = epoch
			}
			for i := range s.Cursors {
				s.Cursors[i].SourceEpochID = epoch
			}
			for i := range s.Services {
				s.Services[i].SourceEpochID = epoch
			}
			for i := range s.Capabilities {
				s.Capabilities[i].SourceEpochID = epoch
			}
			for i := range s.Facts {
				for j := range s.Facts[i].Candidates {
					s.Facts[i].Candidates[j].SourceEpochID = &epoch
					s.Facts[i].Candidates[j].Origin.SourceEpochID = &epoch
				}
			}
		}},
		{"binding_id", func(s *semreg.Snapshot) {
			binding := semreg.NativeBindingID("binding:other")
			for i := range s.Bindings {
				s.Bindings[i].BindingID = binding
			}
			for i := range s.Services {
				s.Services[i].BindingID = binding
			}
			for i := range s.Capabilities {
				s.Capabilities[i].BindingID = binding
			}
			for i := range s.Facts {
				for j := range s.Facts[i].Candidates {
					s.Facts[i].Candidates[j].BindingID = &binding
					s.Facts[i].Candidates[j].Origin.BindingID = &binding
				}
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			baseline := m08JSON(t, m08Build(t, m08Input(t)))
			// Editing identity without resealing must be rejected.
			raw := m08Input(t)
			tt.edit(raw.Snapshot)
			if out, err := Build(m08Registry(t), []Input{raw}); err == nil || !reflect.DeepEqual(out, View{}) {
				t.Fatalf("unsealed identity edit accepted: %v", err)
			}
			// A coherent edit is a different capture and must change the view.
			in := m08Input(t)
			tt.edit(in.Snapshot)
			m08Seal(t, in.Snapshot)
			e := m08Evaluate(t, *in.Snapshot)
			in.Evaluation = &e
			out, err := Build(m08Registry(t), []Input{in})
			if err != nil {
				t.Fatalf("coherent identity edit rejected: %v", err)
			}
			if bytes.Equal(baseline, m08JSON(t, out)) {
				t.Fatal("identity field did not change the view")
			}
		})
	}
}
