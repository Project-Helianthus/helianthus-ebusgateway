package graphql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/evse"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/infrastructure"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/pv"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/storage"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
	graphqlgo "github.com/graphql-go/graphql"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
)

func m09JSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func m09Base(t *testing.T) (semreg.Snapshot, projection.ProjectionReport) {
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
	if err := json.Unmarshal(m09JSON(t, snapshot), &document); err != nil {
		t.Fatal(err)
	}
	delete(document, "snapshot_id")
	snapshotDigest := sha256.Sum256(m09JSON(t, document))
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

func m09Validators() []semreg.PackValidator {
	return []semreg.PackValidator{evse.New(), infrastructure.New(), pv.New(), storage.New(), thermal.New()}
}

func m09Evaluate(t *testing.T, s semreg.Snapshot) semreg.EvaluationView {
	t.Helper()
	v, err := semreg.EvaluateSnapshot(s, semreg.EvaluationContext{EvaluatedAt: s.EvaluatedAt, EvaluateMonotonic: s.EvaluateMonotonic})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func m09Fixture(t *testing.T) semreg.Snapshot {
	t.Helper()
	s, _ := m09Base(t)
	candidate := s.Facts[0].Candidates[0]
	s.Facts = []semreg.FactEnvelope{}
	s.Services = []semreg.ServiceInstance{}
	s.Capabilities = []semreg.CapabilityInstance{}
	for _, validator := range m09Validators() {
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
	m09Seal(t, &s)
	return s
}

func m09Registry(t *testing.T) *semreg.PackMetadataRegistry {
	t.Helper()
	r, err := packs.NewMetadataRegistry()
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func m09Seal(t *testing.T, snapshot *semreg.Snapshot) {
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

func m09Input(t *testing.T) semanticview.Input {
	t.Helper()
	s := m09Fixture(t)
	e := m09Evaluate(t, s)
	return semanticview.Input{AssetID: s.AssetID, Snapshot: &s, Evaluation: &e}
}

type m09Provider struct {
	calls  int
	ids    []semreg.AssetID
	inputs map[semreg.AssetID]semanticview.Input
}

func (p *m09Provider) Capture(_ context.Context, ids []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
	p.calls++
	p.ids = append([]semreg.AssetID{}, ids...)
	out := []semanticview.Input{}
	for _, id := range ids {
		if in, ok := p.inputs[id]; ok {
			out = append(out, in)
		} else {
			out = append(out, semanticview.Input{AssetID: id, UnavailableReason: "semantic.read.reason.asset_unknown"})
		}
	}
	registry, err := packs.NewMetadataRegistry()
	return registry, out, err
}
func m09Request(t *testing.T, b *Builder, query string, variables map[string]any, invoke bool) map[string]any {
	t.Helper()
	h, err := NewHandler(b)
	if invoke {
		h, err = NewInvokeHandler(b, nil, nil)
	}
	if err != nil {
		t.Fatal(err)
	}
	body := m09JSON(t, map[string]any{"query": query, "variables": variables})
	request := httptest.NewRequest("POST", "/graphql", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	h.ServeHTTP(response, request)
	var result map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode %s: %v", response.Body.String(), err)
	}
	return result
}
func m09Success(t *testing.T, result map[string]any) map[string]any {
	t.Helper()
	if result["errors"] != nil {
		t.Fatalf("GraphQL errors: %v", result)
	}
	return result["data"].(map[string]any)
}
func m09Error(t *testing.T, result map[string]any, code string) {
	t.Helper()
	errors, ok := result["errors"].([]any)
	if !ok || len(errors) == 0 {
		t.Fatalf("missing error: %v", result)
	}
	for _, e := range errors {
		if e.(map[string]any)["extensions"].(map[string]any)["code"] != code {
			t.Fatalf("error code: %v", result)
		}
	}
}

type m09NoNativeRegistry struct{}

func (m09NoNativeRegistry) IterateSnapshots(func(registry.DeviceEntrySnapshot) bool) {
	panic("semantic snapshot must not read the native registry")
}

func TestM09FivePacksHandler(t *testing.T) {
	in := m09Input(t)
	p := &m09Provider{inputs: map[semreg.AssetID]semanticview.Input{in.AssetID: in}}
	b := NewBuilder(m09NoNativeRegistry{}, nil)
	b.SetSemanticSnapshotProvider(p)
	// Selecting every typed record field exercises the actual HTTP schema mapping,
	// including every revision component, evidence reference and empty array.
	schema, err := NewQuerySchema(b)
	if err != nil {
		t.Fatal(err)
	}
	field := schema.QueryType().Fields()["semanticSnapshot"]
	selection := m09Selection(field.Type, 0)
	q := `{ semanticSnapshot(assetIds:["asset:test"]) ` + selection + ` }`
	want, err := semanticview.Build(m09Registry(t), []semanticview.Input{in})
	if err != nil {
		t.Fatal(err)
	}
	for _, invoke := range []bool{false, true} {
		result := m09Success(t, m09Request(t, b, q, nil, invoke))["semanticSnapshot"]
		// GraphQL represents omitted optional members as null. Compare every source
		// member recursively, permitting those additional nulls only.
		m09Contains(t, result, semanticCamelValue(m09Decode(t, want)))
	}
	if p.calls != 2 {
		t.Fatalf("captures %d want 2", p.calls)
	}
	if len(want.Assets[0].Availability.Packs) != 5 {
		t.Fatal("fixture lacks five packs")
	}
}
func m09Decode(t *testing.T, v any) any {
	t.Helper()
	var out any
	if err := json.Unmarshal(m09JSON(t, v), &out); err != nil {
		t.Fatal(err)
	}
	return out
}
func m09Contains(t *testing.T, got, want any) {
	t.Helper()
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			t.Fatalf("object %v want %v", got, w)
		}
		for k, v := range w {
			m09Contains(t, g[k], v)
		}
		for k, v := range g {
			if _, ok := w[k]; !ok && v != nil {
				t.Fatalf("extra non-null member %s: %v", k, v)
			}
		}
	case []any:
		g, ok := got.([]any)
		if !ok || len(g) != len(w) {
			t.Fatalf("array %v want %v", got, w)
		}
		for i := range w {
			m09Contains(t, g[i], w[i])
		}
	default:
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("value %v want %v", got, want)
		}
	}
}
func m09Selection(output graphqlgo.Output, depth int) string {
	if depth > 30 {
		panic("recursive record")
	}
	switch typ := output.(type) {
	case *graphqlgo.NonNull:
		return m09Selection(typ.OfType, depth+1)
	case *graphqlgo.List:
		return m09Selection(typ.OfType, depth+1)
	case *graphqlgo.Object:
		names := []string{}
		for name := range typ.Fields() {
			names = append(names, name)
		}
		sort.Strings(names)
		fields := []string{}
		for _, name := range names {
			fields = append(fields, name+" "+m09Selection(typ.Fields()[name].Type, depth+1))
		}
		return "{ " + strings.Join(fields, " ") + " }"
	default:
		return ""
	}
}
func TestM09AliasUnionAndRequestIsolation(t *testing.T) {
	p := &m09Provider{}
	b := NewBuilder(nil, nil)
	b.SetSemanticSnapshotProvider(p)
	q := `query($ids:[ID!]!,$skip:Boolean!){ first:semanticSnapshot(assetIds:$ids){assets{assetId unavailableReason}} ...More ignored:semanticSnapshot(assetIds:[] ) @skip(if:$skip){assets{assetId}} } fragment More on Query { second:semanticSnapshot(assetIds:["asset:b"]){assets{assetId}} }`
	for i := 0; i < 2; i++ {
		d := m09Success(t, m09Request(t, b, q, map[string]any{"ids": []string{"asset:a"}, "skip": true}, false))
		if p.calls != i+1 || strings.Join([]string{string(p.ids[0]), string(p.ids[1])}, ",") != "asset:a,asset:b" {
			t.Fatalf("capture union: %+v", p)
		}
		if len(d["first"].(map[string]any)["assets"].([]any)) != 1 || len(d["second"].(map[string]any)["assets"].([]any)) != 1 {
			t.Fatalf("alias filtering: %v", d)
		}
	}
}
func TestM09InvalidSelectionAndUnavailable(t *testing.T) {
	b := NewBuilder(nil, nil)
	m09Error(t, m09Request(t, b, `{semanticSnapshot(assetIds:["asset:a"]){assets{assetId}}}`, nil, false), "UNAVAILABLE")
	p := &m09Provider{}
	b.SetSemanticSnapshotProvider(p)
	invalid := []any{[]string{}, []string{"asset:a", "asset:a"}, []string{""}, []string{"*"}, []string{"native://resource"}, []string{strings.Repeat("x", 257)}}
	for _, ids := range invalid {
		m09Error(t, m09Request(t, b, `query($ids:[ID!]!){semanticSnapshot(assetIds:$ids){assets{assetId}}}`, map[string]any{"ids": ids}, false), "INVALID_ARGUMENT")
	}
	a, c := []string{}, []string{}
	for i := 0; i < 17; i++ {
		a = append(a, fmt.Sprintf("asset:a%d", i))
		c = append(c, fmt.Sprintf("asset:b%d", i))
	}
	m09Error(t, m09Request(t, b, `query($a:[ID!]!,$b:[ID!]!){a:semanticSnapshot(assetIds:$a){assets{assetId}} b:semanticSnapshot(assetIds:$b){assets{assetId}}}`, map[string]any{"a": a, "b": c}, false), "INVALID_ARGUMENT")
	if p.calls != 0 {
		t.Fatalf("invalid selection captured %d times", p.calls)
	}
}
func TestM09UnavailableCapabilityAndConflict(t *testing.T) {
	in := m09Input(t)
	for i := range in.Snapshot.Capabilities {
		in.Snapshot.Capabilities[i].Availability = semreg.AvailabilityUnavailable
	}
	m09Seal(t, in.Snapshot)
	e := m09Evaluate(t, *in.Snapshot)
	in.Evaluation = &e
	p := &m09Provider{inputs: map[semreg.AssetID]semanticview.Input{in.AssetID: in}}
	b := NewBuilder(nil, nil)
	b.SetSemanticSnapshotProvider(p)
	q := `{semanticSnapshot(assetIds:["asset:test","asset:missing"]){assets{assetId unavailableReason snapshot{snapshotId} projection{dispositions{kind outcome reason}}}}}`
	data := m09Success(t, m09Request(t, b, q, nil, false))
	assets := data["semanticSnapshot"].(map[string]any)["assets"].([]any)
	if assets[0].(map[string]any)["unavailableReason"] != "semantic.read.reason.asset_unknown" {
		t.Fatalf("unknown sibling: %v", assets)
	}
	dispositions := assets[1].(map[string]any)["projection"].(map[string]any)["dispositions"].([]any)
	count := 0
	for _, raw := range dispositions {
		d := raw.(map[string]any)
		if d["kind"] == "capability" && d["outcome"] == "withheld" && d["reason"] == "semantic.read.reason.capability_unavailable" {
			count++
		}
	}
	if count != 5 {
		t.Fatalf("unavailable capabilities %d", count)
	}
	in.Evaluation.Revisions.Facts = "99"
	m09Error(t, m09Request(t, b, q, nil, false), "CONFLICT")
}

func TestM09EveryRevisionComponent(t *testing.T) {
	for _, axis := range []string{"semantic", "identity", "facts", "services", "capabilities"} {
		t.Run(axis, func(t *testing.T) {
			in := m09Input(t)
			switch axis {
			case "semantic":
				in.Snapshot.Revisions.Semantic = "19"
			case "identity":
				in.Snapshot.Revisions.Identity = "19"
			case "facts":
				in.Snapshot.Revisions.Facts = "19"
			case "services":
				in.Snapshot.Revisions.Services = "19"
			case "capabilities":
				in.Snapshot.Revisions.Capabilities = "19"
			}
			m09Seal(t, in.Snapshot)
			e := m09Evaluate(t, *in.Snapshot)
			in.Evaluation = &e
			b := NewBuilder(nil, nil)
			b.SetSemanticSnapshotProvider(&m09Provider{inputs: map[semreg.AssetID]semanticview.Input{in.AssetID: in}})
			revisions := `snapshotId revisions{semantic identity facts services capabilities}`
			q := `{semanticSnapshot(assetIds:["asset:test"]){assets{snapshot{` + revisions + `} evaluation{` + revisions + `} projection{` + revisions + `}}}}`
			data := m09Success(t, m09Request(t, b, q, nil, false))
			asset := data["semanticSnapshot"].(map[string]any)["assets"].([]any)[0].(map[string]any)
			for _, record := range []string{"snapshot", "evaluation", "projection"} {
				v := asset[record].(map[string]any)
				if v["snapshotId"] != string(in.Snapshot.SnapshotID) {
					t.Fatalf("%s snapshot identity changed", record)
				}
				m09Contains(t, v["revisions"], m09Decode(t, in.Snapshot.Revisions))
			}
		})
	}
}

func TestM09PackQueries(t *testing.T) {
	for _, validator := range m09Validators() {
		pack := validator.(semreg.PackMetadataProvider).Metadata().Pack
		t.Run(string(pack.ID), func(t *testing.T) {
			in := m09Input(t)
			s := in.Snapshot
			facts := []semreg.FactEnvelope{}
			services := []semreg.ServiceInstance{}
			caps := []semreg.CapabilityInstance{}
			for _, f := range s.Facts {
				if f.Key.PackID == pack.ID {
					facts = append(facts, f)
				}
			}
			for _, v := range s.Services {
				if v.Definition.Pack == pack {
					services = append(services, v)
				}
			}
			for _, v := range s.Capabilities {
				if v.Definition.Pack == pack {
					caps = append(caps, v)
				}
			}
			s.Facts, s.Services, s.Capabilities = facts, services, caps
			m09Seal(t, s)
			e := m09Evaluate(t, *s)
			in.Evaluation = &e
			p := &m09Provider{inputs: map[semreg.AssetID]semanticview.Input{in.AssetID: in}}
			b := NewBuilder(m09NoNativeRegistry{}, nil)
			b.SetSemanticSnapshotProvider(p)
			q := `{semanticSnapshot(assetIds:["asset:test"]){assets{snapshot{snapshotId revisions{semantic identity facts services capabilities} facts{key{packId}}} projection{manifest{packVersions{id version}}}}}}`
			d := m09Success(t, m09Request(t, b, q, nil, false))
			asset := d["semanticSnapshot"].(map[string]any)["assets"].([]any)[0].(map[string]any)
			snapshot := asset["snapshot"].(map[string]any)
			if snapshot["snapshotId"] != string(s.SnapshotID) {
				t.Fatal("snapshot identity changed")
			}
			m09Contains(t, snapshot["revisions"], m09Decode(t, s.Revisions))
			gotPacks := asset["projection"].(map[string]any)["manifest"].(map[string]any)["packVersions"]
			m09Contains(t, gotPacks, m09Decode(t, []semreg.PackRef{pack}))
			if len(snapshot["facts"].([]any)) != 1 || p.calls != 1 {
				t.Fatal("pack query did not resolve exactly once")
			}
		})
	}
}
