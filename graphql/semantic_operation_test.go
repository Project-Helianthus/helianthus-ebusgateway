package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations/semoptest"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/operation"
	graphqlgo "github.com/graphql-go/graphql"
)

const m56Mutation = `mutation SemanticOperation($input: SemanticOperationInput!) {
  semanticOperation(input: $input) { status kernelOutcome delivery acknowledgement readbackRelation reason }
}`

type m56Registry struct{}

func (m56Registry) Lookup(byte) (registry.DeviceEntry, bool) { return nil, false }

// m56Invoker proves the semantic route never reaches the eBUS invoker.
type m56Invoker struct{ t *testing.T }

func (i m56Invoker) Invoke(context.Context, router.Plane, string, map[string]any) (any, error) {
	i.t.Error("semantic operation reached the eBUS invoker")
	return nil, errors.New("unexpected invoke")
}

type m56Harness struct {
	f        *semoptest.Fixture
	owner    *semoptest.Owner
	lookup   *semoptest.Lookup
	selected *semoptest.Executor
	builder  *Builder
	handler  http.Handler
}

func newM56Harness(t *testing.T, outcome semoptest.Outcome) m56Harness {
	t.Helper()
	f := semoptest.New(t)
	h := m56Harness{f: f, owner: f.Owner(), lookup: &semoptest.Lookup{}, selected: f.Executor(outcome), builder: NewBuilder(nil, nil)}
	h.lookup.Register(f.Route(), semoptest.Operation, h.selected)
	h.builder.SetSemanticOperationRouter(semanticoperations.NewRouter(semanticoperations.Config{Snapshots: f, Kernel: f.Kernel, Intents: h.owner, Executors: h.lookup}))
	handler, err := NewInvokeHandler(h.builder, m56Registry{}, m56Invoker{t: t})
	if err != nil {
		t.Fatalf("NewInvokeHandler() error = %v", err)
	}
	h.handler = handler
	return h
}

func (h m56Harness) untouched(t *testing.T) {
	t.Helper()
	if h.owner.Calls() != 0 || h.f.Captures() != 0 || h.selected.Calls() != 0 {
		t.Fatalf("refused request reached owner=%d capture=%d executor=%d", h.owner.Calls(), h.f.Captures(), h.selected.Calls())
	}
}

func m56Camel(t *testing.T, value any) any {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	return semanticCamelValue(decoded)
}

func m56Input(t *testing.T, request semanticoperations.Request) map[string]any {
	return map[string]any{
		"assetId": string(request.AssetID), "bindingId": string(request.BindingID), "capabilityInstanceId": string(request.CapabilityInstanceID),
		"operation": m56Camel(t, request.Operation), "arguments": m56Camel(t, request.Arguments),
		"expectedSemanticRevision": string(request.ExpectedSemanticRevision), "expectedCapabilityRevision": string(request.ExpectedCapabilityRevision),
		"expectedCapabilityInstanceRevision": string(request.ExpectedCapabilityInstanceRevision), "expectedSourceEpochId": string(request.ExpectedSourceEpochID),
		"expectedDriverGeneration": string(request.ExpectedDriverGeneration), "preconditions": m56Camel(t, request.Preconditions),
		"timeoutMs": request.Timeout.Milliseconds(),
	}
}

func m56Body(t *testing.T, query string, input any) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{"query": query, "variables": map[string]any{"input": input}})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

type m56Response struct {
	Data   map[string]any `json:"data"`
	Errors []struct {
		Message    string         `json:"message"`
		Extensions map[string]any `json:"extensions"`
	} `json:"errors"`
}

func m56Serve(t *testing.T, handler http.Handler, request *http.Request) m56Response {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	var response m56Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("response %q: %v", recorder.Body.String(), err)
	}
	return response
}

func m56Post(t *testing.T, handler http.Handler, contentType string, body []byte) m56Response {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(body))
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	return m56Serve(t, handler, request)
}

func m56Result(t *testing.T, response m56Response) map[string]any {
	t.Helper()
	if len(response.Errors) != 0 {
		t.Fatalf("errors = %+v", response.Errors)
	}
	result, ok := response.Data["semanticOperation"].(map[string]any)
	if !ok {
		t.Fatalf("data = %+v", response.Data)
	}
	return result
}

func m56RequireCode(t *testing.T, response m56Response, code semanticoperations.Code) {
	t.Helper()
	if len(response.Errors) != 1 || response.Errors[0].Extensions["code"] != string(code) {
		t.Fatalf("errors = %+v; want one %s", response.Errors, code)
	}
	if response.Data != nil && response.Data["semanticOperation"] != nil {
		t.Fatalf("data = %+v; want a null mutation result", response.Data)
	}
	if strings.Contains(response.Errors[0].Message, "transport") {
		t.Fatalf("message leaks detail: %q", response.Errors[0].Message)
	}
}

func m56Projection(status string, fields ...any) map[string]any {
	projection := map[string]any{"status": status, "kernelOutcome": nil, "delivery": nil, "acknowledgement": nil, "readbackRelation": nil, "reason": nil}
	for index := 0; index+1 < len(fields); index += 2 {
		projection[fields[index].(string)] = fields[index+1]
	}
	return projection
}

func m56Refused(reason string) map[string]any {
	return m56Projection("failed", "kernelOutcome", "rejected", "delivery", "not_sent", "reason", reason)
}

func m56Equal(t *testing.T, got, want map[string]any) {
	t.Helper()
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(want)
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("semanticOperation = %s; want %s", gotJSON, wantJSON)
	}
}

func TestM56GraphQLMutationReachesExactlyOneSelectedExecutor(t *testing.T) {
	sent := operation.DeliverySent
	tests := []struct {
		name    string
		outcome semoptest.Outcome
		fence   bool
		want    map[string]any
	}{
		{"protocol ACK", semoptest.Outcome{Kind: operation.OutcomeAcknowledgedUnverified, Delivery: sent, Ack: operation.AckAccepted},
			false, m56Projection("protocol_ack", "kernelOutcome", "acknowledged_unverified", "delivery", "sent", "acknowledgement", "accepted")},
		{"confirming readback", semoptest.Outcome{Kind: operation.OutcomeApplied, Delivery: sent, Readback: operation.ReadbackConfirms},
			false, m56Projection("readback_confirmed", "kernelOutcome", "applied", "delivery", "sent", "readbackRelation", "confirms")},
		{"admission only", semoptest.Outcome{}, false, m56Projection("accepted")},
		{"guard change before invocation", semoptest.Outcome{Kind: operation.OutcomeApplied, Delivery: sent, Readback: operation.ReadbackConfirms},
			true, m56Projection("failed", "kernelOutcome", "failed_no_contact", "delivery", "not_sent")},
		{"indeterminate delivery", semoptest.Outcome{Kind: operation.OutcomeIndeterminate, Delivery: operation.DeliveryUnknown},
			false, m56Projection("unknown", "kernelOutcome", "indeterminate", "delivery", "unknown")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newM56Harness(t, test.outcome)
			if test.fence {
				h.selected.Before = h.f.Fence
			}
			otherGeneration := h.f.Route()
			otherGeneration.DriverGeneration = "5"
			otherOperation := semoptest.Operation
			otherOperation.ID = "fixture.operation.other"
			decoys := []*semoptest.Executor{h.f.Executor(semoptest.Outcome{}), h.f.Executor(semoptest.Outcome{})}
			h.lookup.Register(otherGeneration, semoptest.Operation, decoys[0])
			h.lookup.Register(h.f.Route(), otherOperation, decoys[1])

			response := m56Post(t, h.handler, "application/json", m56Body(t, m56Mutation, m56Input(t, h.f.Request())))
			m56Equal(t, m56Result(t, response), test.want)
			if h.selected.Calls() != 1 || decoys[0].Calls() != 0 || decoys[1].Calls() != 0 {
				t.Fatalf("selected=%d decoys=%d,%d; want exactly the selected executor once", h.selected.Calls(), decoys[0].Calls(), decoys[1].Calls())
			}
			if test.fence && h.selected.Sends() != 0 {
				t.Fatal("guard change still sent")
			}
		})
	}
}

// Each identity and revision member travels through the GraphQL decoder alone,
// so swapped or dropped decoding is refused before any send.
func TestM56GraphQLRefusalsProduceNoSend(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*semanticoperations.Request)
		reason string
	}{
		{"asset", func(r *semanticoperations.Request) { r.AssetID = "asset:other" }, "semantic.read.reason.asset_unknown"},
		{"binding", func(r *semanticoperations.Request) { r.BindingID = "binding:other" }, semanticoperations.ReasonBindingMismatch},
		{"unsupported capability instance", func(r *semanticoperations.Request) { r.CapabilityInstanceID = semoptest.ObserveInstanceID }, string(semreg.AmbiguousRoute)},
		{"unsupported operation", func(r *semanticoperations.Request) { r.Operation.ID = "fixture.operation.other" }, string(semreg.DefinitionOwnerMissing)},
		{"operation version", func(r *semanticoperations.Request) { r.Operation.Version = "1.0.1" }, string(semreg.DefinitionOwnerMissing)},
		{"operation pack", func(r *semanticoperations.Request) { r.Operation.Pack.ID = "fixture.pack.other" }, string(semreg.DefinitionOwnerMissing)},
		{"operation pack version", func(r *semanticoperations.Request) { r.Operation.Pack.Version = "1.0.1" }, string(semreg.DefinitionOwnerMissing)},
		{"semantic revision", func(r *semanticoperations.Request) { r.ExpectedSemanticRevision = r.ExpectedCapabilityRevision }, string(semreg.RevisionConflict)},
		{"capability collection revision", func(r *semanticoperations.Request) {
			r.ExpectedCapabilityRevision = r.ExpectedCapabilityInstanceRevision
		}, string(semreg.RevisionConflict)},
		{"capability instance revision", func(r *semanticoperations.Request) {
			r.ExpectedCapabilityInstanceRevision = r.ExpectedCapabilityRevision
		}, string(semreg.RevisionConflict)},
		{"stale epoch", func(r *semanticoperations.Request) { r.ExpectedSourceEpochID = "epoch:retired" }, string(semreg.StaleSourceEpoch)},
		{"stale generation", func(r *semanticoperations.Request) { r.ExpectedDriverGeneration = "3" }, string(semreg.StaleDriverGeneration)},
		{"stale precondition", func(r *semanticoperations.Request) { r.Preconditions[0].CandidateRevision = "1" }, string(semreg.PreconditionFailed)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newM56Harness(t, semoptest.Outcome{Kind: operation.OutcomeAcknowledgedUnverified, Delivery: operation.DeliverySent, Ack: operation.AckAccepted})
			request := h.f.Request()
			test.mutate(&request)
			response := m56Post(t, h.handler, "application/json", m56Body(t, m56Mutation, m56Input(t, request)))
			m56Equal(t, m56Result(t, response), m56Refused(test.reason))
			if h.selected.Calls() != 0 {
				t.Fatalf("executor invoked %d times", h.selected.Calls())
			}
		})
	}
	t.Run("absent qualified executor", func(t *testing.T) {
		f := semoptest.New(t)
		builder := NewBuilder(nil, nil)
		builder.SetSemanticOperationRouter(semanticoperations.NewRouter(semanticoperations.Config{Snapshots: f, Kernel: f.Kernel, Intents: f.Owner(), Executors: semanticoperations.NoExecutors()}))
		handler, err := NewInvokeHandler(builder, m56Registry{}, m56Invoker{t: t})
		if err != nil {
			t.Fatal(err)
		}
		response := m56Post(t, handler, "application/json", m56Body(t, m56Mutation, m56Input(t, f.Request())))
		m56Equal(t, m56Result(t, response), m56Refused(semanticoperations.ReasonExecutorMissing))
	})
}

func TestM56GraphQLStructuralRefusalsNeverDispatch(t *testing.T) {
	type mutation func(t *testing.T, h m56Harness, input map[string]any) (contentType string, body []byte)
	withInput := func(edit func(map[string]any)) mutation {
		return func(t *testing.T, h m56Harness, input map[string]any) (string, []byte) {
			edit(input)
			return "application/json", m56Body(t, m56Mutation, input)
		}
	}
	precondition := func(input map[string]any) map[string]any {
		return input["preconditions"].([]any)[0].(map[string]any)
	}
	raw := func(contentType, body string) mutation {
		return func(*testing.T, m56Harness, map[string]any) (string, []byte) { return contentType, []byte(body) }
	}
	withQuery := func(query string) mutation {
		return func(t *testing.T, h m56Harness, input map[string]any) (string, []byte) {
			return "application/json", m56Body(t, query, input)
		}
	}
	long := "a" + strings.Repeat("b", semanticoperations.MaxOpaqueIDBytes)
	invalid, exhausted := semanticoperations.CodeInvalidArgument, semanticoperations.CodeResourceExhausted
	// Each fragment spreads the next one twice: 2^30 selections if expanded.
	fanOut := `mutation($input: SemanticOperationInput!) { semanticOperation(input: $input) { status } ...F0 }`
	for level := 0; level < 30; level++ {
		next := fmt.Sprintf("F%d", level+1)
		fanOut += fmt.Sprintf(" fragment F%d on Mutation { ...%s ...%s }", level, next, next)
	}
	fanOut += " fragment F30 on Mutation { __typename }"
	validInline := `assetId: "asset:synthetic", bindingId: "binding:synthetic", capabilityInstanceId: "capability:synthetic", operation: {pack: {id: "fixture.pack.synthetic", version: "1.0.0"}, id: "fixture.operation.synthetic", version: "1.0.0"}, arguments: [{id: "fixture.field.level", value: {kind: "boolean", boolean: true}}], expectedSemanticRevision: "3", expectedCapabilityRevision: "2", expectedCapabilityInstanceRevision: "1", expectedSourceEpochId: "epoch:synthetic", expectedDriverGeneration: "4", preconditions: [], timeoutMs: 1000`
	tests := []struct {
		name  string
		build mutation
		code  semanticoperations.Code
	}{
		{"numeric opaque identifier", withInput(func(in map[string]any) { in["assetId"] = 7 }), invalid},
		{"numeric revision", withInput(func(in map[string]any) { in["expectedSemanticRevision"] = 3 }), invalid},
		{"numeric nested revision", withInput(func(in map[string]any) { precondition(in)["candidateRevision"] = 2 }), invalid},
		{"numeric nested identifier", withInput(func(in map[string]any) { precondition(in)["candidateId"] = 1 }), invalid},
		{"unknown member", withInput(func(in map[string]any) { in["route"] = "binding:synthetic" }), invalid},
		{"snake_case nested member", withInput(func(in map[string]any) {
			p := precondition(in)
			p["candidate_id"] = p["candidateId"]
			delete(p, "candidateId")
		}), invalid},
		{"missing member", withInput(func(in map[string]any) { delete(in, "expectedDriverGeneration") }), invalid},
		{"null member", withInput(func(in map[string]any) { in["bindingId"] = nil }), invalid},
		{"zero timeout", withInput(func(in map[string]any) { in["timeoutMs"] = 0 }), invalid},
		{"timeout above bound", withInput(func(in map[string]any) { in["timeoutMs"] = 5001 }), invalid},
		{"fractional timeout", withInput(func(in map[string]any) { in["timeoutMs"] = 1.5 }), invalid},
		{"string timeout", withInput(func(in map[string]any) { in["timeoutMs"] = "1000" }), invalid},
		{"singleton list coercion", withInput(func(in map[string]any) { in["arguments"] = in["arguments"].([]any)[0] }), invalid},
		{"zero revision", withInput(func(in map[string]any) { in["expectedCapabilityRevision"] = "0" }), invalid},
		{"noncanonical revision", withInput(func(in map[string]any) { in["expectedDriverGeneration"] = "04" }), invalid},
		{"mismatched value arm", withInput(func(in map[string]any) {
			in["arguments"].([]any)[0].(map[string]any)["value"] = map[string]any{"kind": "boolean", "text": "true"}
		}), invalid},
		{"unordered arguments", withInput(func(in map[string]any) {
			first := in["arguments"].([]any)[0]
			in["arguments"] = []any{first, map[string]any{"id": "fixture.field.alpha", "value": map[string]any{"kind": "boolean", "boolean": true}}}
		}), invalid},
		{"duplicate preconditions", withInput(func(in map[string]any) {
			in["preconditions"] = append(in["preconditions"].([]any), in["preconditions"].([]any)[0])
		}), invalid},
		{"argument overflow", withInput(func(in map[string]any) {
			arguments := make([]any, semanticoperations.MaxArguments+1)
			for index := range arguments {
				arguments[index] = in["arguments"].([]any)[0]
			}
			in["arguments"] = arguments
		}), exhausted},
		{"overlong identifier", withInput(func(in map[string]any) { in["assetId"] = long }), exhausted},
		{"overlong nested identifier", withInput(func(in map[string]any) { precondition(in)["candidateId"] = long }), exhausted},
		{"overlong source epoch", withInput(func(in map[string]any) { in["expectedSourceEpochId"] = long }), exhausted},
		{"two fields through an alias", withQuery(`mutation($input: SemanticOperationInput!) { a: semanticOperation(input: $input) { status } b: semanticOperation(input: $input) { status } }`), invalid},
		{"two fields through fragments", withQuery(`mutation($input: SemanticOperationInput!) { ...Op ...Op } fragment Op on Mutation { semanticOperation(input: $input) { status } }`), invalid},
		{"exponential fragment fan-out", withQuery(fanOut), invalid},
		{"skipped second field still counts", withQuery(`mutation($input: SemanticOperationInput!) { semanticOperation(input: $input) { status } ... on Mutation @skip(if: true) { again: semanticOperation(input: $input) { status } } }`), invalid},
		{"query operation", withQuery(`query($input: SemanticOperationInput!) { semanticOperation(input: $input) { status } }`), invalid},
		{"missing variable", withQuery(`mutation($other: SemanticOperationInput!) { semanticOperation(input: $other) { status } }`), invalid},
		{"extra argument", withQuery(`mutation($input: SemanticOperationInput!) { semanticOperation(input: $input, dryRun: true) { status } }`), invalid},
		{"inline numeric identifier", withQuery(`mutation { semanticOperation(input: {` + strings.Replace(validInline, `assetId: "asset:synthetic"`, `assetId: 7`, 1) + `}) { status } }`), invalid},
		{"invalid document", withQuery(`mutation { semanticOperation(input: { }`), invalid},
		{"duplicate input member", raw("application/json", `{"query":"mutation($input: SemanticOperationInput!) { semanticOperation(input: $input) { status } }","variables":{"input":{"assetId":"asset:synthetic","assetId":"asset:other"}}}`), invalid},
		{"duplicate escaped envelope member", raw("application/json", `{"query":"mutation { semanticOperation(input: {}) { status } }","query":"mutation { semanticOperation(input: {}) { status } }"}`), invalid},
		{"invalid UTF-8", raw("application/json", "{\"query\":\"mutation { semanticOperation(input: {}) { status } } \xff\"}"), invalid},
		{"batched request", raw("application/json", `[{"query":"mutation { semanticOperation(input: {}) { status } }"}]`), invalid},
		{"trailing JSON value", raw("application/json", `{"query":"mutation { semanticOperation(input: {}) { status } }"} {}`), invalid},
		{"case-variant query member", raw("application/json", `{"Query":"mutation { semanticOperation(input: {}) { status } }"}`), invalid},
		{"unknown envelope member", raw("application/json", `{"query":"mutation { semanticOperation(input: {}) { status } }","id":1}`), invalid},
		{"string variables", raw("application/json", `{"query":"mutation($input: SemanticOperationInput!) { semanticOperation(input: $input) { status } }","variables":"{}"}`), invalid},
		{"non-JSON media type", func(t *testing.T, h m56Harness, input map[string]any) (string, []byte) {
			return "text/plain", m56Body(t, m56Mutation, input)
		}, invalid},
		{"non-UTF-8 charset", func(t *testing.T, h m56Harness, input map[string]any) (string, []byte) {
			return "application/json; charset=latin1", m56Body(t, m56Mutation, input)
		}, invalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newM56Harness(t, semoptest.Outcome{Kind: operation.OutcomeAcknowledgedUnverified, Delivery: operation.DeliverySent, Ack: operation.AckAccepted})
			contentType, body := test.build(t, h, m56Input(t, h.f.Request()))
			m56RequireCode(t, m56Post(t, h.handler, contentType, body), test.code)
			h.untouched(t)
		})
	}
	t.Run("encoded body", func(t *testing.T) {
		h := newM56Harness(t, semoptest.Outcome{})
		request := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(m56Body(t, m56Mutation, m56Input(t, h.f.Request()))))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Content-Encoding", "gzip")
		m56RequireCode(t, m56Serve(t, h.handler, request), invalid)
		h.untouched(t)
	})
	t.Run("non-JSON transports", func(t *testing.T) {
		h := newM56Harness(t, semoptest.Outcome{})
		inline := `mutation { semanticOperation(input: {` + validInline + `}) { status } }`
		requests := []*http.Request{
			httptest.NewRequest(http.MethodGet, "/graphql?query="+url.QueryEscape(inline), nil),
			httptest.NewRequest(http.MethodPost, "/graphql?query="+url.QueryEscape(inline), strings.NewReader(`{}`)),
		}
		form := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(url.Values{"query": {inline}}.Encode()))
		form.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		document := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(inline))
		document.Header.Set("Content-Type", "application/graphql")
		for _, request := range append(requests, form, document) {
			m56RequireCode(t, m56Serve(t, h.handler, request), invalid)
		}
		h.untouched(t)
	})
	t.Run("body over the byte bound", func(t *testing.T) {
		h := newM56Harness(t, semoptest.Outcome{})
		body := m56Body(t, m56Mutation, m56Input(t, h.f.Request()))
		padded := append(body[:len(body)-1:len(body)-1], append(bytes.Repeat([]byte(" "), semanticoperations.MaxRequestBytes-len(body)+1), '}')...)
		if len(padded) != semanticoperations.MaxRequestBytes+1 {
			t.Fatalf("padded body = %d bytes", len(padded))
		}
		m56RequireCode(t, m56Post(t, h.handler, "application/json", padded), exhausted)
		h.untouched(t)
	})
	t.Run("padded body past the byte bound never reaches graphql", func(t *testing.T) {
		h := newM56Harness(t, semoptest.Outcome{})
		body := m56Body(t, m56Mutation, m56Input(t, h.f.Request()))
		pad := bytes.Repeat([]byte(" "), 4*semanticoperations.MaxRequestBytes)
		padded := append(body[:len(body)-1:len(body)-1], append(pad, '}')...)
		m56RequireCode(t, m56Post(t, h.handler, "application/json", padded), exhausted)
		hugePad := bytes.Repeat([]byte(" "), 2<<20)
		hugeSemantic := append(body[:len(body)-1:len(body)-1], append(hugePad, '}')...)
		m56RequireCode(t, m56Post(t, h.handler, "application/json", hugeSemantic), exhausted)
		malformed := append([]byte(`{"query":"mutation { semanticOperation(`), pad...)
		if got := m56Post(t, h.handler, "application/json", malformed); len(got.Errors) == 0 || got.Data != nil {
			t.Fatalf("malformed oversized body = %+v", got)
		}
		h.untouched(t)
	})
	t.Run("oversized non-semantic body keeps its handling", func(t *testing.T) {
		h := newM56Harness(t, semoptest.Outcome{})
		huge := append([]byte(`{"query":"{ __typename }"`), append(bytes.Repeat([]byte(" "), 2<<20), '}')...)
		got := m56Post(t, h.handler, "application/json", huge)
		if len(got.Errors) != 0 || got.Data["__typename"] == nil {
			t.Fatalf("response = %+v", got)
		}
		h.untouched(t)
	})
	t.Run("body at the byte bound", func(t *testing.T) {
		h := newM56Harness(t, semoptest.Outcome{Kind: operation.OutcomeAcknowledgedUnverified, Delivery: operation.DeliverySent, Ack: operation.AckAccepted})
		body := m56Body(t, m56Mutation, m56Input(t, h.f.Request()))
		padded := append(body[:len(body)-1:len(body)-1], append(bytes.Repeat([]byte(" "), semanticoperations.MaxRequestBytes-len(body)), '}')...)
		if len(padded) != semanticoperations.MaxRequestBytes {
			t.Fatalf("padded body = %d bytes", len(padded))
		}
		if got := m56Result(t, m56Post(t, h.handler, "application/json", padded)); got["status"] != "protocol_ack" || h.selected.Calls() != 1 {
			t.Fatalf("result = %+v calls = %d", got, h.selected.Calls())
		}
	})
}

func TestM56GraphQLAcceptsInlineLiteralsAndDefaults(t *testing.T) {
	h := newM56Harness(t, semoptest.Outcome{Kind: operation.OutcomeAcknowledgedUnverified, Delivery: operation.DeliverySent, Ack: operation.AckAccepted})
	request := h.f.Request()
	query := `mutation($asset: String!) { semanticOperation(input: {assetId: $asset, bindingId: "binding:synthetic", capabilityInstanceId: "capability:synthetic",
	  operation: {pack: {id: "fixture.pack.synthetic", version: "1.0.0"}, id: "fixture.operation.synthetic", version: "1.0.0"},
	  arguments: [{id: "fixture.field.level", value: {kind: "boolean", boolean: true}}],
	  expectedSemanticRevision: "` + string(request.ExpectedSemanticRevision) + `", expectedCapabilityRevision: "` + string(request.ExpectedCapabilityRevision) + `",
	  expectedCapabilityInstanceRevision: "1", expectedSourceEpochId: "epoch:synthetic", expectedDriverGeneration: "4",
	  preconditions: [{fact: {packId: "fixture.pack.synthetic", packVersion: "1.0.0", factId: "fixture.fact.interlock", dimensions: []}, candidateId: "candidate:interlock", candidateRevision: "2", operator: "equal", expected: {kind: "boolean", boolean: true}}],
	  timeoutMs: 1000}) { status } }`
	body, err := json.Marshal(map[string]any{"query": query, "variables": map[string]any{"asset": "asset:synthetic"}, "operationName": nil})
	if err != nil {
		t.Fatal(err)
	}
	if got := m56Result(t, m56Post(t, h.handler, "application/json; charset=UTF-8", body)); got["status"] != "protocol_ack" || h.selected.Calls() != 1 {
		t.Fatalf("result = %+v calls = %d", got, h.selected.Calls())
	}
}

func TestM56GraphQLUnwiredRouteIsUnavailable(t *testing.T) {
	f := semoptest.New(t)
	for name, router := range map[string]SemanticOperationRouter{
		"no router":       nil,
		"typed nil":       (*semanticoperations.Router)(nil),
		"no intent owner": semanticoperations.NewRouter(semanticoperations.Config{Snapshots: f, Kernel: f.Kernel, Executors: semanticoperations.NoExecutors()}),
	} {
		t.Run(name, func(t *testing.T) {
			builder := NewBuilder(nil, nil)
			builder.SetSemanticOperationRouter(router)
			handler, err := NewInvokeHandler(builder, m56Registry{}, m56Invoker{t: t})
			if err != nil {
				t.Fatal(err)
			}
			before := f.Captures()
			m56RequireCode(t, m56Post(t, handler, "application/json", m56Body(t, m56Mutation, m56Input(t, f.Request()))), semanticoperations.CodeUnavailable)
			if f.Captures() != before {
				t.Fatal("unwired route captured a snapshot")
			}
		})
	}
}

// Schemas executed outside the guarded HTTP body path, such as the WebSocket
// subscription transport, fail closed instead of decoding coerced input.
func TestM56GraphQLUnguardedExecutionFailsClosed(t *testing.T) {
	h := newM56Harness(t, semoptest.Outcome{})
	schema, err := NewSchema(h.builder, m56Registry{}, m56Invoker{t: t}, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := graphqlgo.Do(graphqlgo.Params{Schema: schema, Context: context.Background(), RequestString: m56Mutation,
		VariableValues: map[string]any{"input": m56Input(t, h.f.Request())}})
	if len(result.Errors) != 1 || result.Errors[0].Extensions["code"] != string(semanticoperations.CodeInvalidArgument) {
		t.Fatalf("errors = %+v", result.Errors)
	}
	h.untouched(t)
}

func TestM56GraphQLGuardPreservesOtherRequests(t *testing.T) {
	h := newM56Harness(t, semoptest.Outcome{})
	typename := func(response m56Response) {
		t.Helper()
		if len(response.Errors) != 0 || response.Data["__typename"] != "Query" {
			t.Fatalf("response = %+v", response)
		}
	}
	typename(m56Post(t, h.handler, "application/json", []byte(`{"query":"{ __typename } # semanticOperation is only named here"}`)))
	typename(m56Post(t, h.handler, "application/json", []byte(`{"query":"{ semanticSnapshot }","query":"{ __typename }"}`)))
	typename(m56Post(t, h.handler, "text/plain", []byte(`{"query":"{ __typename }"}`)))
	large := append([]byte(`{"query":"{ __typename }"`), append(bytes.Repeat([]byte(" "), semanticoperations.MaxRequestBytes), '}')...)
	typename(m56Post(t, h.handler, "application/json", large))
	typename(m56Serve(t, h.handler, httptest.NewRequest(http.MethodGet, "/graphql?query="+url.QueryEscape("{ __typename }"), nil)))
	h.untouched(t)
}

func TestM56GraphQLSchemaDeclaresClosedRoute(t *testing.T) {
	h := newM56Harness(t, semoptest.Outcome{})
	query := `{
	  mutation: __schema { mutationType { fields { name type { kind name } args { name type { kind ofType { name } } } } } }
	  input: __type(name: "SemanticOperationInput") { inputFields { name type { ...T } } }
	  result: __type(name: "SemanticOperationResult") { fields { name type { ...T } } }
	  precondition: __type(name: "SemanticPreconditionInput") { inputFields { name } }
	  value: __type(name: "SemanticValueInput") { inputFields { name type { ...T } } }
	}
	fragment T on __Type { kind name ofType { kind name ofType { kind name ofType { kind name } } } }`
	body, _ := json.Marshal(map[string]string{"query": query})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	h.handler.ServeHTTP(recorder, request)
	var response struct {
		Data struct {
			Mutation struct {
				MutationType struct {
					Fields []struct {
						Name string
						Type struct{ Kind, Name string }
						Args []struct {
							Name string
							Type struct {
								Kind   string
								OfType struct{ Name string }
							}
						}
					}
				}
			}
			Input        struct{ InputFields []m56TypeRef }
			Result       struct{ Fields []m56TypeRef }
			Precondition struct{ InputFields []struct{ Name string } }
			Value        struct{ InputFields []m56TypeRef }
		}
		Errors []any
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil || len(response.Errors) != 0 {
		t.Fatalf("introspection = %s: %v", recorder.Body.String(), err)
	}
	found := false
	for _, field := range response.Data.Mutation.MutationType.Fields {
		if field.Name != "semanticOperation" {
			continue
		}
		found = true
		if field.Type.Kind != "OBJECT" || field.Type.Name != "SemanticOperationResult" || len(field.Args) != 1 ||
			field.Args[0].Name != "input" || field.Args[0].Type.Kind != "NON_NULL" || field.Args[0].Type.OfType.Name != "SemanticOperationInput" {
			t.Fatalf("semanticOperation field = %+v", field)
		}
	}
	if !found {
		t.Fatal("semanticOperation mutation missing")
	}
	wantInput := map[string]string{
		"assetId": "String!", "bindingId": "String!", "capabilityInstanceId": "String!", "operation": "SemanticDefinitionRefInput!",
		"arguments": "[SemanticTypedFieldInput!]!", "expectedSemanticRevision": "String!", "expectedCapabilityRevision": "String!",
		"expectedCapabilityInstanceRevision": "String!", "expectedSourceEpochId": "String!", "expectedDriverGeneration": "String!",
		"preconditions": "[SemanticPreconditionInput!]!", "timeoutMs": "Int!",
	}
	m56RequireTypes(t, "SemanticOperationInput", response.Data.Input.InputFields, wantInput)
	m56RequireTypes(t, "SemanticOperationResult", response.Data.Result.Fields, map[string]string{
		"status": "String!", "kernelOutcome": "String", "delivery": "String", "acknowledgement": "String", "readbackRelation": "String", "reason": "String",
	})
	names := []string{}
	for _, field := range response.Data.Precondition.InputFields {
		names = append(names, field.Name)
	}
	sort.Strings(names)
	if strings.Join(names, ",") != "candidateId,candidateRevision,expected,fact,operator" {
		t.Fatalf("SemanticPreconditionInput fields = %v", names)
	}
	m56RequireTypes(t, "SemanticValueInput", response.Data.Value.InputFields, map[string]string{
		"kind": "String!", "quantity": "SemanticQuantityInput", "boolean": "Boolean", "text": "String",
		"symbol": "SemanticSymbolInput", "symbols": "[SemanticSymbolInput!]", "time": "SemanticTimePointInput",
	})
}

type m56TypeRef struct {
	Name string
	Type m56TypeNode
}

type m56TypeNode struct {
	Kind   string
	Name   string
	OfType *m56TypeNode
}

func (node m56TypeNode) String() string {
	switch node.Kind {
	case "NON_NULL":
		return node.OfType.String() + "!"
	case "LIST":
		return "[" + node.OfType.String() + "]"
	}
	return node.Name
}

func m56RequireTypes(t *testing.T, owner string, fields []m56TypeRef, want map[string]string) {
	t.Helper()
	got := map[string]string{}
	for _, field := range fields {
		got[field.Name] = field.Type.String()
	}
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(want)
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("%s fields = %s; want %s", owner, gotJSON, wantJSON)
	}
}

func TestM56GuardOversizedSemanticBodyNeverReachesDownstream(t *testing.T) {
	called := 0
	guarded := guardSemanticOperationRequests(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called++ }))
	h := newM56Harness(t, semoptest.Outcome{})
	body := m56Body(t, m56Mutation, m56Input(t, h.f.Request()))
	padded := append(body[:len(body)-1:len(body)-1], append(bytes.Repeat([]byte(" "), 2<<20), '}')...)
	m56RequireCode(t, m56Post(t, guarded, "application/json", padded), semanticoperations.CodeResourceExhausted)
	if called != 0 {
		t.Fatalf("downstream handler called %d times", called)
	}
	h.untouched(t)
}

func TestM56GuardPrefixPaddedSemanticBodyNeverReachesDownstream(t *testing.T) {
	called := 0
	guarded := guardSemanticOperationRequests(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called++ }))
	h := newM56Harness(t, semoptest.Outcome{})
	body := m56Body(t, m56Mutation, m56Input(t, h.f.Request()))
	for _, size := range []int{2 * semanticoperations.MaxRequestBytes, 2 * semanticOversizeHardCap} {
		pad := bytes.Repeat([]byte(" "), size)
		prefixed := append([]byte("{"), append(pad, body[1:]...)...)
		m56RequireCode(t, m56Post(t, guarded, "application/json", prefixed), semanticoperations.CodeResourceExhausted)
	}
	if called != 0 {
		t.Fatalf("downstream handler called %d times", called)
	}
	h.untouched(t)
}
