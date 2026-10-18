package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

var edgeNilCaptures atomic.Int32

type edgeSnapshotProvider struct {
	capture func(context.Context, []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error)
}

func (p *edgeSnapshotProvider) Capture(ctx context.Context, ids []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
	if p == nil {
		edgeNilCaptures.Add(1)
		panic("typed-nil snapshot provider was invoked")
	}
	return p.capture(ctx, ids)
}

const edgeSnapshotQuery = `query($ids:[ID!]!){semanticSnapshot(assetIds:$ids){assets{assetId unavailableReason}}}`

func edgeUnknown(id semreg.AssetID) semanticview.Input {
	return semanticview.Input{AssetID: id, UnavailableReason: "semantic.read.reason.asset_unknown"}
}

func edgeBuilder(p SemanticSnapshotProvider) *Builder {
	b := NewBuilder(m09NoNativeRegistry{}, nil)
	b.SetSemanticSnapshotProvider(p)
	return b
}

func edgeFailure(t *testing.T, result map[string]any, code string) {
	t.Helper()
	errs, ok := result["errors"].([]any)
	if !ok || len(errs) != 1 {
		t.Fatalf("want one %s error, got %v", code, result)
	}
	e, _ := errs[0].(map[string]any)
	ext, _ := e["extensions"].(map[string]any)
	data, _ := result["data"].(map[string]any)
	if ext["code"] != code || data == nil || data["semanticSnapshot"] != nil {
		t.Fatalf("want %s with null root, got %v", code, result)
	}
}

func edgeAssets(t *testing.T, result map[string]any) []any {
	t.Helper()
	return m09Success(t, result)["semanticSnapshot"].(map[string]any)["assets"].([]any)
}

func TestSemanticSnapshotProviderEdgesCanonicalOrder(t *testing.T) {
	registry := m09Registry(t)
	calls := 0
	p := &edgeSnapshotProvider{capture: func(_ context.Context, ids []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
		calls++
		if !reflect.DeepEqual(ids, []semreg.AssetID{"asset:a", "asset:b", "asset:c"}) {
			t.Errorf("capture order: %v", ids)
		}
		// Deliberately independent of the resolver's request union order.
		return registry, []semanticview.Input{edgeUnknown("asset:c"), edgeUnknown("asset:b"), edgeUnknown("asset:a")}, nil
	}}
	b := edgeBuilder(p)
	for _, ids := range [][]string{{"asset:a", "asset:b", "asset:c"}, {"asset:c", "asset:b", "asset:a"}} {
		assets := edgeAssets(t, m09Request(t, b, edgeSnapshotQuery, map[string]any{"ids": ids}, false))
		if len(assets) != 3 {
			t.Fatalf("assets: %v", assets)
		}
		for i, want := range []string{"asset:a", "asset:b", "asset:c"} {
			if assets[i].(map[string]any)["assetId"] != want {
				t.Fatalf("canonical order: %v", assets)
			}
		}
	}
	if calls != 2 {
		t.Fatalf("captures=%d want 2", calls)
	}
}

func TestSemanticSnapshotProviderEdgesFailures(t *testing.T) {
	registry := m09Registry(t)
	for _, tc := range []struct {
		name, code      string
		inputs          []semanticview.Input
		err             error
		missingMetadata bool
	}{
		{name: "provider_error", code: "UNAVAILABLE", inputs: []semanticview.Input{edgeUnknown("asset:a"), edgeUnknown("asset:b")}, err: errors.New("synthetic provider failure")},
		{name: "duplicate", code: "CONFLICT", inputs: []semanticview.Input{edgeUnknown("asset:a"), edgeUnknown("asset:a")}},
		// Missing metadata alone is UNAVAILABLE. A duplicate must instead be
		// rejected as CONFLICT before Build, whose own duplicate check would
		// otherwise mask removal of the resolver's selected-ID deletion.
		{name: "missing_metadata", code: "UNAVAILABLE", inputs: []semanticview.Input{edgeUnknown("asset:a"), edgeUnknown("asset:b")}, missingMetadata: true},
		{name: "duplicate_before_missing_metadata", code: "CONFLICT", inputs: []semanticview.Input{edgeUnknown("asset:a"), edgeUnknown("asset:a")}, missingMetadata: true},
		{name: "substituted", code: "CONFLICT", inputs: []semanticview.Input{edgeUnknown("asset:a"), edgeUnknown("asset:c")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A fresh provider and slice keep the valid control detached from the fault.
			control := &edgeSnapshotProvider{capture: func(context.Context, []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
				return registry, []semanticview.Input{edgeUnknown("asset:a"), edgeUnknown("asset:b")}, nil
			}}
			vars := map[string]any{"ids": []string{"asset:a", "asset:b"}}
			if assets := edgeAssets(t, m09Request(t, edgeBuilder(control), edgeSnapshotQuery, vars, false)); len(assets) != 2 {
				t.Fatalf("control: %v", assets)
			}
			calls := 0
			broken := &edgeSnapshotProvider{capture: func(context.Context, []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
				calls++
				metadata := registry
				if tc.missingMetadata {
					metadata = nil
				}
				return metadata, tc.inputs, tc.err
			}}
			edgeFailure(t, m09Request(t, edgeBuilder(broken), edgeSnapshotQuery, vars, false), tc.code)
			if calls != 1 {
				t.Fatalf("captures=%d want 1", calls)
			}
		})
	}
}

func TestSemanticSnapshotProviderEdgesSerializedOverflow(t *testing.T) {
	registry := m09Registry(t)
	in := m09Input(t)
	// Legal, synthetic dimension text increases serialized selections without
	// exceeding a kernel collection or per-value bound.
	text := strings.Repeat("x", 4096)
	in.Snapshot.Facts[0].Key.Dimensions[0].Value.Text = &text
	in.Snapshot.Facts[0].Candidates[0].Key = in.Snapshot.Facts[0].Key
	m09Seal(t, in.Snapshot)
	evaluation := m09Evaluate(t, *in.Snapshot)
	in.Evaluation = &evaluation
	base, err := semanticview.Build(registry, []semanticview.Input{in})
	if err != nil {
		t.Fatal(err)
	}
	candidate := in.Snapshot.Facts[0].Candidates[0]
	selections := make([]semreg.Selection, 1024)
	for i := range selections {
		selections[i] = semreg.Selection{Contract: semreg.ContractSelectionV1, SnapshotID: in.Snapshot.SnapshotID, Revisions: in.Snapshot.Revisions,
			EvaluationDigest: in.Evaluation.EvaluationDigest, Context: in.Evaluation.Context, Key: in.Snapshot.Facts[0].Key,
			PolicyID: semreg.PolicyID(fmt.Sprintf("policy:synthetic-%04d", i)), PolicyVersion: "1.0.0",
			SelectedCandidate: candidate.CandidateID, CandidateRevision: candidate.Revision, PresentationOnly: true}
		if err := semreg.ValidateSelection(*in.Snapshot, *in.Evaluation, selections[i]); err != nil {
			t.Fatalf("selection %d: %v", i, err)
		}
	}
	// Measure the complete detached view independently of Build's size guard.
	expected := semanticview.View{Assets: append([]semanticview.Asset(nil), base.Assets...)}
	expected.Assets[0].Selections = selections
	raw, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) <= 4*1024*1024 {
		t.Fatalf("fixture only %d bytes", len(raw))
	}
	t.Logf("serialized shared view: %d bytes (bound %d)", len(raw), 4*1024*1024)
	control := in
	control.Selections = append([]semreg.Selection(nil), selections[:1]...)
	p := func(input semanticview.Input) *edgeSnapshotProvider {
		return &edgeSnapshotProvider{capture: func(context.Context, []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
			return registry, []semanticview.Input{input}, nil
		}}
	}
	vars := map[string]any{"ids": []string{"asset:test"}}
	if assets := edgeAssets(t, m09Request(t, edgeBuilder(p(control)), edgeSnapshotQuery, vars, false)); len(assets) != 1 {
		t.Fatalf("control: %v", assets)
	}
	in.Selections = selections
	edgeFailure(t, m09Request(t, edgeBuilder(p(in)), edgeSnapshotQuery, vars, false), "RESOURCE_EXHAUSTED")
}

func TestSemanticSnapshotProviderEdgesCancellation(t *testing.T) {
	registry := m09Registry(t)
	for _, cancelRequest := range []bool{false, true} {
		name := "release_control"
		if cancelRequest {
			name = "cancel"
		}
		t.Run(name, func(t *testing.T) {
			entered := make(chan context.Context, 1)
			observed := make(chan error, 1)
			release := make(chan struct{})
			defer close(release)
			p := &edgeSnapshotProvider{capture: func(ctx context.Context, _ []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
				entered <- ctx
				select {
				case <-ctx.Done():
					observed <- ctx.Err()
					return nil, nil, ctx.Err()
				case <-release:
					return registry, []semanticview.Input{edgeUnknown("asset:a")}, nil
				}
			}}
			h, err := NewHandler(edgeBuilder(p))
			if err != nil {
				t.Fatal(err)
			}
			type contextKey struct{}
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "synthetic-context"))
			defer cancel()
			body := m09JSON(t, map[string]any{"query": edgeSnapshotQuery, "variables": map[string]any{"ids": []string{"asset:a"}}})
			req := httptest.NewRequest("POST", "/graphql", bytes.NewReader(body)).WithContext(ctx)
			req.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			done := make(chan struct{})
			go func() { defer close(done); h.ServeHTTP(response, req) }()
			select {
			case captured := <-entered:
				if captured.Value(contextKey{}) != "synthetic-context" {
					t.Error("Capture lost request context")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Capture did not enter")
			}
			if cancelRequest {
				cancel()
				select {
				case err := <-observed:
					if !errors.Is(err, context.Canceled) {
						t.Errorf("Capture cancellation: %v", err)
					}
				case <-time.After(2 * time.Second):
					t.Error("Capture did not observe request cancellation")
					return
				}
			} else {
				release <- struct{}{}
			}
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("handler did not finish")
			}
			var result map[string]any
			if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if cancelRequest {
				if result["errors"] == nil {
					t.Fatalf("cancellation succeeded: %v", result)
				}
				if data, _ := result["data"].(map[string]any); data != nil && data["semanticSnapshot"] != nil {
					t.Fatalf("cancellation exposed data: %v", result)
				}
			} else if assets := edgeAssets(t, result); len(assets) != 1 {
				t.Fatalf("control: %v", assets)
			}
		})
	}
}

func TestSemanticSnapshotProviderEdgesTypedNilRegistration(t *testing.T) {
	registry := m09Registry(t)
	valid := &edgeSnapshotProvider{capture: func(context.Context, []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
		return registry, []semanticview.Input{edgeUnknown("asset:a")}, nil
	}}
	b := edgeBuilder(valid)
	vars := map[string]any{"ids": []string{"asset:a"}}
	if assets := edgeAssets(t, m09Request(t, b, edgeSnapshotQuery, vars, false)); len(assets) != 1 {
		t.Fatalf("control: %v", assets)
	}
	b.SetSemanticSnapshotProvider(nil)
	edgeFailure(t, m09Request(t, b, edgeSnapshotQuery, vars, false), "UNAVAILABLE")
	b.SetSemanticSnapshotProvider(valid)
	var typedNil *edgeSnapshotProvider
	before := edgeNilCaptures.Load()
	b.SetSemanticSnapshotProvider(typedNil)
	// A typed nil must be treated as an unwired provider, before Capture can panic.
	result := m09Request(t, b, edgeSnapshotQuery, vars, false)
	if edgeNilCaptures.Load() != before {
		t.Error("typed-nil registration invoked Capture and panicked")
	}
	edgeFailure(t, result, "UNAVAILABLE")
}
