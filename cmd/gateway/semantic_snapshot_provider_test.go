package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

type m11CaptureSource struct {
	hub    *semhub.Hub
	calls  atomic.Int64
	after  func()
	mutate func(*semhub.CoherentCapture)
	err    error
}

func (s *m11CaptureSource) Capture(ctx context.Context, ids []semreg.AssetID, at time.Time) (semhub.CoherentCapture, error) {
	s.calls.Add(1)
	if s.err != nil {
		return semhub.CoherentCapture{}, s.err
	}
	c, err := s.hub.Capture(ctx, ids, at)
	if err == nil && s.mutate != nil {
		s.mutate(&c)
		for i := range c.Assets {
			if e := c.Assets[i].Evaluation; e != nil {
				digest, digestErr := e.EvaluationDigestValue()
				if digestErr != nil {
					return semhub.CoherentCapture{}, digestErr
				}
				e.EvaluationDigest = digest
			}
		}
	}
	if err == nil && s.after != nil {
		s.after()
	}
	return c, err
}

func m11Handlers(t *testing.T, p *hubSemanticSnapshotProvider) (http.Handler, http.Handler) {
	t.Helper()
	gateway := &ebusgateway.Gateway{Registry: registry.NewDeviceRegistry(nil)}
	query, _, _, server, err := newHTTPControlPlaneSemanticHandlers(gateway, graphql.NewBuilder(gateway.Registry, nil), graphql.NewBroadcastHub(nil), p)
	if err != nil {
		t.Fatal(err)
	}
	return query, server.Handler()
}
func m11GraphQL(t *testing.T, handler http.Handler, query string) map[string]any {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"query": query})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(raw))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var result map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode GraphQL: %v: %s", err, response.Body.String())
	}
	return result
}
func m11MCP(t *testing.T, handler http.Handler) map[string]any {
	t.Helper()
	response := mcpRPC(t, handler, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"semantic.v1.snapshot.get","arguments":{"asset_ids":["asset:synthetic-boiler"]}}}`)
	if response.Error != nil {
		t.Fatalf("MCP error: %+v", response.Error)
	}
	content := response.Result["content"].([]any)[0].(map[string]any)["text"].(string)
	var envelope map[string]any
	if err := json.Unmarshal([]byte(content), &envelope); err != nil {
		t.Fatal(err)
	}
	return envelope
}
func m11Camel(s string) string {
	parts := strings.Split(s, "_")
	for i := 1; i < len(parts); i++ {
		if parts[i] != "" {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}
	return strings.Join(parts, "")
}
func m11Normalize(v any) any {
	switch x := v.(type) {
	case map[string]any:
		result := map[string]any{}
		for k, value := range x {
			if value != nil {
				result[m11Camel(k)] = m11Normalize(value)
			}
		}
		return result
	case []any:
		result := make([]any, len(x))
		for i, value := range x {
			result[i] = m11Normalize(value)
		}
		return result
	}
	return v
}
func m11Selection(typ reflect.Type) string {
	for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return ""
	}
	fields := []string{}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := strings.Split(field.Tag.Get("json"), ",")[0]
		if tag == "" || tag == "-" {
			continue
		}
		fields = append(fields, m11Camel(tag)+m11Selection(field.Type))
	}
	return " { " + strings.Join(fields, " ") + " }"
}
func m11Query() string {
	selection := m11Selection(reflect.TypeOf(semanticview.View{}))
	return `{ a: semanticSnapshot(assetIds:["asset:synthetic-boiler"])` + selection + ` b: semanticSnapshot(assetIds:["asset:synthetic-boiler"])` + selection + ` }`
}

func m11Publish(hub *semhub.Hub, handle *semhub.Handle, id semreg.AssetID) error {
	tx, err := hub.Begin(handle, id)
	if err != nil {
		return err
	}
	defer tx.Abort()
	snapshot, _ := tx.Current()
	candidate := snapshot.Facts[0].Candidates[0]
	candidate.Value.Quantity.Number = semreg.Decimal{Coefficient: "51"}
	candidate.Revision = "2"
	batch := semreg.PublicationBatch{Contract: semreg.ContractKernelV1, BatchID: "batch:synthetic-update", AssetID: id, SourceID: handle.Source(), SourceEpochID: handle.Epoch(), DriverGeneration: handle.GenerationID(), Sequence: tx.NextSequence(), ExpectedSemanticRevision: snapshot.Revisions.Semantic, ObservedAt: snapshot.EvaluatedAt, FactUpserts: []semreg.FactCandidate{candidate},
		SourceUpserts: []semreg.SourceDescriptor{}, SourceRetirements: []semreg.SourceEpochID{}, BindingUpserts: []semreg.NativeBinding{}, IdentityLinkUpserts: []semreg.IdentityLink{}, FactWithdrawals: []semreg.CandidateID{}, ServiceUpserts: []semreg.ServiceInstance{}, ServiceWithdrawals: []semreg.ServiceInstanceID{}, CapabilityUpserts: []semreg.CapabilityInstance{}, CapabilityWithdrawals: []semreg.CapabilityInstanceID{}, GenerationFences: []semreg.GenerationFence{}}
	batch.BatchDigest, err = batch.ComputedDigest()
	if err != nil {
		return err
	}
	if _, _, err := tx.Apply(batch, snapshot.EvaluateMonotonic); err != nil {
		return err
	}
	_, err = tx.Commit(nil)
	return err
}
func m11AssertParity(t *testing.T, gql, envelope map[string]any) map[string]any {
	t.Helper()
	if gql["errors"] != nil || envelope["error"] != nil {
		t.Fatalf("read failed: GraphQL=%v MCP=%v", gql["errors"], envelope["error"])
	}
	data := gql["data"].(map[string]any)
	if !reflect.DeepEqual(m11Normalize(data["a"]), m11Normalize(data["b"])) || !reflect.DeepEqual(m11Normalize(data["a"]), m11Normalize(envelope["data"])) {
		t.Fatalf("GraphQL/MCP snapshot, revision, value or loss differs:\n%v\n%v", data, envelope["data"])
	}
	asset := envelope["data"].(map[string]any)["assets"].([]any)[0].(map[string]any)
	snapshot := asset["snapshot"].(map[string]any)
	projection := asset["projection"].(map[string]any)
	evaluation := asset["evaluation"].(map[string]any)
	if snapshot["snapshot_id"] != projection["snapshot_id"] || snapshot["snapshot_id"] != evaluation["snapshot_id"] || !reflect.DeepEqual(snapshot["revisions"], projection["revisions"]) || !reflect.DeepEqual(snapshot["revisions"], evaluation["revisions"]) {
		t.Fatal("mixed tuple inside response")
	}
	if len(snapshot["facts"].([]any)) == 0 || len(projection["dispositions"].([]any)) == 0 {
		t.Fatal("empty parity fixture")
	}
	return snapshot
}

func TestM11HandlersShareHubAndAllowPublishBetweenRequests(t *testing.T) {
	publication, handle, _ := m15Fixture(t)
	if publication.scalar(publication.binding.Identity.Target) == nil {
		t.Fatal(publication.lastError)
	}
	control := &gatewayRuntimeControl{bridge: &ebusControlRuntime{hub: publication.hub}}
	provider := newGatewaySemanticSnapshotProvider(control)
	if provider.hub != semanticHubCapture(publication.hub) {
		t.Fatal("provider did not bind the composed hub")
	}
	provider.now = func() time.Time { return time.Unix(0, 1800000000000000020) }
	source := &m11CaptureSource{hub: publication.hub}
	provider.hub = source
	gql, rpc := m11Handlers(t, provider)
	before := m11AssertParity(t, m11GraphQL(t, gql, m11Query()), m11MCP(t, rpc))
	if source.calls.Load() != 2 {
		t.Fatalf("capture calls=%d; want one per request", source.calls.Load())
	}
	if err := m11Publish(publication.hub, handle, publication.binding.Identity.Asset); err != nil {
		t.Fatal(err)
	}
	after := m11AssertParity(t, m11GraphQL(t, gql, m11Query()), m11MCP(t, rpc))
	if before["snapshot_id"] == after["snapshot_id"] || reflect.DeepEqual(before["facts"], after["facts"]) {
		t.Fatal("new publication not visible")
	}
}

func TestM11PublishDuringRequestKeepsCapturedRevision(t *testing.T) {
	for _, surface := range []string{"GraphQL", "MCP"} {
		t.Run(surface, func(t *testing.T) {
			publication, handle, _ := m15Fixture(t)
			if publication.scalar(publication.binding.Identity.Target) == nil {
				t.Fatal(publication.lastError)
			}
			prior, _ := publication.hub.Inspect(publication.binding.Identity.Asset)
			source := &m11CaptureSource{hub: publication.hub}
			source.after = func() {
				if err := m11Publish(publication.hub, handle, publication.binding.Identity.Asset); err != nil {
					source.err = err
				}
			}
			p := &hubSemanticSnapshotProvider{hub: source, now: func() time.Time { return time.Unix(0, 1800000000000000020) }}
			gql, rpc := m11Handlers(t, p)
			var data any
			if surface == "GraphQL" {
				response := m11GraphQL(t, gql, m11Query())
				if response["errors"] != nil {
					t.Fatal(response["errors"])
				}
				aliases := response["data"].(map[string]any)
				if !reflect.DeepEqual(aliases["a"], aliases["b"]) {
					t.Fatal("aliases mixed revisions")
				}
				data = aliases["a"]
			} else {
				response := m11MCP(t, rpc)
				if response["error"] != nil {
					t.Fatal(response["error"])
				}
				data = response["data"]
			}
			asset := m11Normalize(data).(map[string]any)["assets"].([]any)[0].(map[string]any)
			if asset["snapshot"].(map[string]any)["snapshotId"] != string(prior.Snapshot.SnapshotID) || source.calls.Load() != 1 {
				t.Fatal("request recaptured a newer revision")
			}
			next, _ := publication.hub.Inspect(publication.binding.Identity.Asset)
			if source.err != nil {
				t.Fatal(source.err)
			}
			if next.Snapshot.SnapshotID == prior.Snapshot.SnapshotID {
				t.Fatal("fixture did not publish during request")
			}
		})
	}
}

func TestM11MixedRevisionRejectedByBothHandlers(t *testing.T) {
	mutations := map[string]func(*semreg.EvaluationView){
		"snapshot": func(e *semreg.EvaluationView) {
			digest := sha256.Sum256([]byte("synthetic different publication"))
			e.SnapshotID = semreg.SnapshotID(fmt.Sprintf("sha256:%x", digest))
		},
		"semantic":     func(e *semreg.EvaluationView) { e.Revisions.Semantic = "999" },
		"identity":     func(e *semreg.EvaluationView) { e.Revisions.Identity = "999" },
		"facts":        func(e *semreg.EvaluationView) { e.Revisions.Facts = "999" },
		"services":     func(e *semreg.EvaluationView) { e.Revisions.Services = "999" },
		"capabilities": func(e *semreg.EvaluationView) { e.Revisions.Capabilities = "999" },
	}
	for field, mutate := range mutations {
		t.Run(field, func(t *testing.T) {
			publication, _, _ := m15Fixture(t)
			if publication.scalar(publication.binding.Identity.Target) == nil {
				t.Fatal(publication.lastError)
			}
			source := &m11CaptureSource{hub: publication.hub, mutate: func(c *semhub.CoherentCapture) { mutate(c.Assets[0].Evaluation) }}
			gql, rpc := m11Handlers(t, &hubSemanticSnapshotProvider{hub: source, now: func() time.Time { return time.Unix(0, 1800000000000000020) }})
			response := m11GraphQL(t, gql, m11Query())
			if response["errors"] == nil {
				t.Fatal("GraphQL admitted mixed revision")
			}
			for _, raw := range response["errors"].([]any) {
				if raw.(map[string]any)["extensions"].(map[string]any)["code"] != "CONFLICT" {
					t.Fatal(raw)
				}
			}
			if got := m11MCP(t, rpc)["error"].(map[string]any)["code"]; got != "CONFLICT" {
				t.Fatalf("MCP error=%v", got)
			}
		})
	}
}

func TestM11UnavailableProviderKeepsShellResponsive(t *testing.T) {
	for name, provider := range map[string]*hubSemanticSnapshotProvider{
		"nil control":    newGatewaySemanticSnapshotProvider(nil),
		"failed capture": {hub: &m11CaptureSource{err: errors.New("synthetic failed driver")}, now: time.Now},
	} {
		t.Run(name, func(t *testing.T) {
			gql, rpc := m11Handlers(t, provider)
			if m11GraphQL(t, gql, m11Query())["errors"] == nil {
				t.Fatal("missing provider appeared available")
			}
			if got := m11MCP(t, rpc)["error"].(map[string]any)["code"]; got != "UNAVAILABLE" {
				t.Fatalf("error=%v", got)
			}
			result := mcpRPC(t, rpc, `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`)
			if result.Error != nil {
				t.Fatal(result.Error)
			}
			found := false
			for _, tool := range result.Result["tools"].([]any) {
				if tool.(map[string]any)["name"] == mcp.SemanticSnapshotTool {
					found = true
				}
			}
			if !found {
				t.Fatal("unavailable tool disappeared")
			}
			if got := m11GraphQL(t, gql, `{ __typename }`); got["errors"] != nil || got["data"] == nil {
				t.Fatal("shell GraphQL unavailable")
			}
		})
	}
}

func TestM11FencedDriverUnavailableAndRetainsNativeEvidence(t *testing.T) {
	publication, handle, _ := m15Fixture(t)
	if publication.scalar(publication.binding.Identity.Target) == nil {
		t.Fatal(publication.lastError)
	}
	id := publication.binding.Identity.Asset
	before, err := publication.hub.Inspect(id)
	if err != nil {
		t.Fatal(err)
	}
	provider := newGatewaySemanticSnapshotProvider(&gatewayRuntimeControl{bridge: &ebusControlRuntime{hub: publication.hub}})
	provider.now = func() time.Time { return time.Unix(0, 1800000000000000020) }
	gql, rpc := m11Handlers(t, provider)
	if _, err := publication.hub.Fence(semhub.FenceRequest{Source: handle.Source(), Epoch: handle.Epoch(), Generation: handle.Generation(), Reason: "lifecycle.driver_failed", Evidence: []semreg.EvidenceRef{before.Snapshot.Sources[0].RegistryEvidence}, ObservedAt: before.Snapshot.EvaluatedAt, Monotonic: before.Snapshot.EvaluateMonotonic}); err != nil {
		t.Fatal(err)
	}
	response, envelope := m11GraphQL(t, gql, m11Query()), m11MCP(t, rpc)
	if response["errors"] != nil || envelope["error"] != nil {
		t.Fatalf("shell unavailable: %v %v", response, envelope)
	}
	for _, value := range []any{response["data"].(map[string]any)["a"], envelope["data"]} {
		asset := m11Normalize(value).(map[string]any)["assets"].([]any)[0].(map[string]any)
		if asset["unavailableReason"] != "semantic.read.reason.source_unavailable" || asset["snapshot"] != nil {
			t.Fatalf("fenced publication exposed: %v", asset)
		}
	}
	after, err := publication.hub.Inspect(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Snapshot.Retained) != 1 || !reflect.DeepEqual(after.Snapshot.Retained[0].Candidate.Evidence, before.Snapshot.Facts[0].Candidates[0].Evidence) {
		t.Fatal("native evidence was lost")
	}
}
