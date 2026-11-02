package mcp_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gql "github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
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
)

func m10JSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func m10Base(t *testing.T) (semreg.Snapshot, projection.ProjectionReport) {
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
	if err := json.Unmarshal(m10JSON(t, snapshot), &document); err != nil {
		t.Fatal(err)
	}
	delete(document, "snapshot_id")
	snapshotDigest := sha256.Sum256(m10JSON(t, document))
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

func m10Validators() []semreg.PackValidator {
	return []semreg.PackValidator{evse.New(), infrastructure.New(), pv.New(), storage.New(), thermal.New()}
}

func m10Evaluate(t *testing.T, s semreg.Snapshot) semreg.EvaluationView {
	t.Helper()
	v, err := semreg.EvaluateSnapshot(s, semreg.EvaluationContext{EvaluatedAt: s.EvaluatedAt, EvaluateMonotonic: s.EvaluateMonotonic})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func m10Fixture(t *testing.T) semreg.Snapshot {
	t.Helper()
	s, _ := m10Base(t)
	candidate := s.Facts[0].Candidates[0]
	s.Facts = []semreg.FactEnvelope{}
	s.Services = []semreg.ServiceInstance{}
	s.Capabilities = []semreg.CapabilityInstance{}
	for _, validator := range m10Validators() {
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
	m10Seal(t, &s)
	return s
}

func m10Registry(t *testing.T) *semreg.PackMetadataRegistry {
	t.Helper()
	r, err := packs.NewMetadataRegistry()
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func m10Seal(t *testing.T, snapshot *semreg.Snapshot) {
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

func m10Input(t *testing.T) semanticview.Input {
	t.Helper()
	s := m10Fixture(t)
	e := m10Evaluate(t, s)
	return semanticview.Input{AssetID: s.AssetID, Snapshot: &s, Evaluation: &e}
}

func m10Selection(output graphqlgo.Output, depth int) string {
	if depth > 30 {
		panic("recursive record")
	}
	switch typ := output.(type) {
	case *graphqlgo.NonNull:
		return m10Selection(typ.OfType, depth+1)
	case *graphqlgo.List:
		return m10Selection(typ.OfType, depth+1)
	case *graphqlgo.Object:
		names := []string{}
		for name := range typ.Fields() {
			names = append(names, name)
		}
		sort.Strings(names)
		fields := []string{}
		for _, name := range names {
			fields = append(fields, name+" "+m10Selection(typ.Fields()[name].Type, depth+1))
		}
		return "{ " + strings.Join(fields, " ") + " }"
	default:
		return ""
	}
}
func m10Decode(t *testing.T, v any) any {
	t.Helper()
	var out any
	if err := json.Unmarshal(m10JSON(t, v), &out); err != nil {
		t.Fatal(err)
	}
	return out
}
func m10Contains(t *testing.T, got, want any) {
	t.Helper()
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			t.Fatalf("object %v want %v", got, w)
		}
		for k, v := range w {
			m10Contains(t, g[k], v)
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
			m10Contains(t, g[i], w[i])
		}
	default:
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("value %v want %v", got, want)
		}
	}
}

type m10Provider func(context.Context, []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error)

func (p m10Provider) Capture(ctx context.Context, ids []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
	return p(ctx, ids)
}

func m10Server(t *testing.T, p mcp.SemanticSnapshotProvider) *mcp.Server {
	t.Helper()
	s, err := mcp.NewServer(registry.NewDeviceRegistry(nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = mcp.RegisterSemanticSnapshotTool(s, p); err != nil {
		t.Fatal(err)
	}
	return s
}
func m10RPC(t *testing.T, ctx context.Context, s *mcp.Server, method, params string) map[string]any {
	t.Helper()
	r := httptest.NewRequest("POST", "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"`+method+`","params":`+params+`}`)).WithContext(ctx)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	var response map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err, w.Body.String())
	}
	if response["error"] != nil {
		t.Fatal(response)
	}
	return response["result"].(map[string]any)
}
func m10Call(t *testing.T, ctx context.Context, s *mcp.Server, args string) map[string]any {
	t.Helper()
	result := m10RPC(t, ctx, s, "tools/call", `{"name":"semantic.v1.snapshot.get","arguments":`+args+`}`)
	var env map[string]any
	if err := json.Unmarshal([]byte(result["content"].([]any)[0].(map[string]any)["text"].(string)), &env); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(m10JSON(t, env["data"]))
	meta := env["meta"].(map[string]any)
	if meta["data_hash"] != fmt.Sprintf("%x", sum) {
		t.Fatal("wire hash mismatch")
	}
	if _, err := time.Parse(time.RFC3339Nano, meta["data_timestamp"].(string)); err != nil {
		t.Fatal(err)
	}
	if (env["error"] != nil) != (result["isError"] == true) {
		t.Fatal("isError mismatch")
	}
	return env
}
func m10Error(t *testing.T, env map[string]any, code string) {
	t.Helper()
	e, ok := env["error"].(map[string]any)
	if !ok || e["code"] != code || env["data"] != nil || e["source_layer"] != "gateway" {
		t.Fatalf("want %s: %v", code, env)
	}
	if e["retriable"] != (code == "UNAVAILABLE" || code == "DEADLINE_EXCEEDED") {
		t.Fatal("retry policy")
	}
}
func TestM10DiscoveryGolden(t *testing.T) {
	s := m10Server(t, nil)
	listed := m10RPC(t, context.Background(), s, "tools/list", `{}`)
	var tool any
	for _, item := range listed["tools"].([]any) {
		if item.(map[string]any)["name"] == mcp.SemanticSnapshotTool {
			if tool != nil {
				t.Fatal("duplicate")
			}
			tool = item
		}
	}
	if tool == nil {
		t.Fatal("tool missing")
	}
	env := m10Call(t, context.Background(), s, `{"asset_ids":["asset:test"]}`)
	m10Error(t, env, "UNAVAILABLE")
	meta := env["meta"].(map[string]any)
	meta["data_timestamp"] = "<evaluation-time>"
	meta["data_hash"] = "<computed-data-hash>"
	p := m10Provider(func(context.Context, []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
		return m10Registry(t), []semanticview.Input{
			{AssetID: "asset:withheld", UnavailableReason: "semantic.read.reason.publication_withheld"},
			{AssetID: "asset:missing", UnavailableReason: "semantic.read.reason.asset_unknown"},
		}, nil
	})
	success := m10Call(t, context.Background(), m10Server(t, p), `{"asset_ids":["asset:withheld","asset:missing"]}`)
	successMeta := success["meta"].(map[string]any)
	successMeta["data_timestamp"] = "<evaluation-time>"
	successMeta["data_hash"] = "<computed-data-hash>"
	got := map[string]any{"tool": tool, "unavailable": env, "success": success}
	raw, err := os.ReadFile("testdata/semantic_snapshot.json")
	if err != nil {
		t.Fatal(err)
	}
	var want any
	if err = json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m10Decode(t, got), want) {
		t.Fatalf("local golden mismatch: %s", m10JSON(t, got))
	}
	if mcp.RegisterSemanticSnapshotTool(s, nil) == nil || mcp.RegisterSemanticSnapshotTool(nil, nil) == nil {
		t.Fatal("registration accepted")
	}
}
func TestM10InvalidSelection(t *testing.T) {
	var calls atomic.Int32
	p := m10Provider(func(context.Context, []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
		calls.Add(1)
		return nil, nil, errors.New("must not call")
	})
	s := m10Server(t, p)
	for _, args := range []string{`{}`, `null`, `[]`, `{"asset_ids":null}`, `{"asset_ids":[]}`, `{"asset_ids":[null]}`, `{"asset_ids":[1]}`, `{"asset_ids":["*"]}`, `{"asset_ids":["asset:a","asset:a"]}`, `{"asset_ids":["https://invalid"]}`, `{"asset_ids":["asset:a"],"extra":true}`, `{"asset_ids":["asset:a"],"asset_ids":["asset:b"]}`, string(m10JSON(t, map[string]any{"asset_ids": []string{strings.Repeat("a", 257)}})), string(m10JSON(t, map[string]any{"asset_ids": make([]string, 33)}))} {
		t.Run(args, func(t *testing.T) { m10Error(t, m10Call(t, context.Background(), s, args), "INVALID_ARGUMENT") })
	}
	if calls.Load() != 0 {
		t.Fatal("invalid input captured")
	}
}
func TestM10FivePackM09Parity(t *testing.T) {
	for _, validator := range m10Validators() {
		pack := validator.(semreg.PackMetadataProvider).Metadata().Pack
		t.Run(string(pack.ID), func(t *testing.T) {
			in := m10Input(t)
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
			m10Seal(t, s)
			e := m10Evaluate(t, *s)
			in.Evaluation = &e
			var calls atomic.Int32
			p := m10Provider(func(_ context.Context, ids []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
				call := calls.Add(1)
				wantIDs := []semreg.AssetID{"asset:test", "asset:missing"}
				if call == 2 {
					wantIDs = []semreg.AssetID{"asset:missing", "asset:test"}
				}
				if !reflect.DeepEqual(ids, wantIDs) {
					t.Error("opaque selection changed")
				}
				return m10Registry(t), []semanticview.Input{{AssetID: "asset:missing", UnavailableReason: "semantic.read.reason.asset_unknown"}, in}, nil
			})
			server := m10Server(t, p)
			env := m10Call(t, context.Background(), server, `{"asset_ids":["asset:test","asset:missing"]}`)
			if env["error"] != nil || calls.Load() != 1 {
				t.Fatal(env, calls.Load())
			}
			want, err := semanticview.Build(m10Registry(t), []semanticview.Input{in, {AssetID: "asset:missing", UnavailableReason: "semantic.read.reason.asset_unknown"}})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(env["data"], m10Decode(t, want)) {
				t.Fatal("M08 shared view differs")
			}
			// Exercise the actual M09 HTTP adapter over the identical capture, selecting
			// every typed field, including loss records and all revision components.
			b := gql.NewBuilder(registry.NewDeviceRegistry(nil), nil)
			b.SetSemanticSnapshotProvider(p)
			schema, err := gql.NewQuerySchema(b)
			if err != nil {
				t.Fatal(err)
			}
			q := `{semanticSnapshot(assetIds:["asset:test","asset:missing"]) ` + m10Selection(schema.QueryType().Fields()["semanticSnapshot"].Type, 0) + `}`
			h, err := gql.NewHandler(b)
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest("POST", "/graphql", bytes.NewReader(m10JSON(t, map[string]any{"query": q})))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			var result map[string]any
			if err = json.Unmarshal(w.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if result["errors"] != nil {
				t.Fatal(result)
			}
			m10Contains(t, result["data"].(map[string]any)["semanticSnapshot"], m10Camel(env["data"]))
		})
	}
}
func m10Camel(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, v := range x {
			parts := strings.Split(k, "_")
			for i := 1; i < len(parts); i++ {
				parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
			}
			out[strings.Join(parts, "")] = m10Camel(v)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, v := range x {
			out[i] = m10Camel(v)
		}
		return out
	default:
		return v
	}
}
func TestM10ProviderFailuresAndDeadline(t *testing.T) {
	for _, mode := range []string{"error", "omit", "substitute", "duplicate", "metadata", "revision", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			in := m10Input(t)
			p := m10Provider(func(ctx context.Context, _ []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
				switch mode {
				case "error":
					return nil, nil, errors.New("private provider error")
				case "omit":
					return m10Registry(t), nil, nil
				case "substitute":
					return m10Registry(t), []semanticview.Input{{AssetID: "asset:other", UnavailableReason: "semantic.read.reason.asset_unknown"}}, nil
				case "duplicate":
					return m10Registry(t), []semanticview.Input{in, in}, nil
				case "metadata":
					return nil, []semanticview.Input{in}, nil
				case "revision":
					in.Evaluation.Revisions.Facts = "99"
				case "deadline":
					<-ctx.Done()
					return nil, nil, ctx.Err()
				}
				return m10Registry(t), []semanticview.Input{in}, nil
			})
			ctx := context.Background()
			if mode == "deadline" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 10*time.Millisecond)
				defer cancel()
			}
			code := "CONFLICT"
			if mode == "error" || mode == "metadata" {
				code = "UNAVAILABLE"
			}
			if mode == "deadline" {
				code = "DEADLINE_EXCEEDED"
			}
			env := m10Call(t, ctx, m10Server(t, p), `{"asset_ids":["asset:test"]}`)
			m10Error(t, env, code)
			if strings.Contains(string(m10JSON(t, env)), "private provider error") {
				t.Fatal("provider error leaked")
			}
		})
	}
}

func TestM10EveryRevisionComponent(t *testing.T) {
	// Each field is varied independently: removing its equality check in the
	// shared snapshot/evaluation join must turn this rejection into a test failure.
	for _, field := range []string{"Semantic", "Identity", "Facts", "Services", "Capabilities"} {
		t.Run(field, func(t *testing.T) {
			in := m10Input(t)
			reflect.ValueOf(&in.Evaluation.Revisions).Elem().FieldByName(field).SetString("99")
			p := m10Provider(func(context.Context, []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
				return m10Registry(t), []semanticview.Input{in}, nil
			})
			m10Error(t, m10Call(t, context.Background(), m10Server(t, p), `{"asset_ids":["asset:test"]}`), "CONFLICT")
		})
	}
}

func TestM10OpaqueSelectionBoundsAndUnavailableSiblings(t *testing.T) {
	ids := []semreg.AssetID{"asset:Case", "asset:case", semreg.AssetID("asset:" + strings.Repeat("a", 250))}
	for len(ids) < 32 {
		ids = append(ids, semreg.AssetID(fmt.Sprintf("asset:synthetic-%02d", len(ids))))
	}
	p := m10Provider(func(_ context.Context, got []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
		if !reflect.DeepEqual(got, ids) {
			t.Error("request identities changed")
		}
		inputs := make([]semanticview.Input, len(got))
		reasons := []string{"asset_unknown", "snapshot_unavailable", "source_unavailable", "publication_withheld"}
		for i, id := range got {
			inputs[len(got)-1-i] = semanticview.Input{AssetID: id, UnavailableReason: "semantic.read.reason." + reasons[i%4]}
		}
		return m10Registry(t), inputs, nil
	})
	env := m10Call(t, context.Background(), m10Server(t, p), string(m10JSON(t, map[string]any{"asset_ids": ids})))
	if env["error"] != nil {
		t.Fatal(env)
	}
	assets := env["data"].(map[string]any)["assets"].([]any)
	if len(assets) != 32 {
		t.Fatal("asset bound truncated")
	}
	last := ""
	for _, a := range assets {
		asset := a.(map[string]any)
		id := asset["asset_id"].(string)
		if id <= last || len(asset) != 3 || len(asset["selections"].([]any)) != 0 {
			t.Fatal("unavailable record changed", asset)
		}
		last = id
	}
}
