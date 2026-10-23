package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticoperations/semoptest"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semhub"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

type m56CountingHub struct{ captures atomic.Int32 }

func (h *m56CountingHub) Capture(context.Context, []semreg.AssetID, func() time.Time) (semhub.CoherentCapture, error) {
	h.captures.Add(1)
	return semhub.CoherentCapture{}, semhub.ErrCaptureUnavailable
}

func m56SemanticOperationBody(t *testing.T, request semanticoperations.Request) []byte {
	t.Helper()
	input := fmt.Sprintf(`{"assetId":%q,"bindingId":%q,"capabilityInstanceId":%q,
	  "operation":{"pack":{"id":%q,"version":%q},"id":%q,"version":%q},
	  "arguments":[{"id":"fixture.field.level","value":{"kind":"boolean","boolean":true}}],
	  "expectedSemanticRevision":%q,"expectedCapabilityRevision":%q,"expectedCapabilityInstanceRevision":%q,
	  "expectedSourceEpochId":%q,"expectedDriverGeneration":%q,"preconditions":[],"timeoutMs":1000}`,
		request.AssetID, request.BindingID, request.CapabilityInstanceID,
		request.Operation.Pack.ID, request.Operation.Pack.Version, request.Operation.ID, request.Operation.Version,
		request.ExpectedSemanticRevision, request.ExpectedCapabilityRevision, request.ExpectedCapabilityInstanceRevision,
		request.ExpectedSourceEpochID, request.ExpectedDriverGeneration)
	body, err := json.Marshal(map[string]any{
		"query":     `mutation($input: SemanticOperationInput!) { semanticOperation(input: $input) { status kernelOutcome delivery reason } }`,
		"variables": map[string]json.RawMessage{"input": json.RawMessage(input)},
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

type m56GatewayResponse struct {
	Data   map[string]any `json:"data"`
	Errors []struct {
		Extensions map[string]any `json:"extensions"`
	} `json:"errors"`
}

func m56GatewayPost(t *testing.T, builder *graphql.Builder, body []byte) m56GatewayResponse {
	t.Helper()
	handler, err := graphql.NewInvokeHandler(builder, emptyLateInvokeRegistry{}, emptyLateInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	var response m56GatewayResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("response %q: %v", recorder.Body.String(), err)
	}
	return response
}

// The production composition installs the route with real SemReg admission and
// the default executor lookup, but no runtime intent owner: it answers
// UNAVAILABLE before any capture, admission or send.
func TestM56GatewayCompositionInstallsUnwiredRoute(t *testing.T) {
	hub := &m56CountingHub{}
	builder := graphql.NewBuilder(nil, nil)
	if err := installGatewaySemanticOperationRoute(builder, &hubSemanticSnapshotProvider{hub: hub, now: time.Now}); err != nil {
		t.Fatalf("installGatewaySemanticOperationRoute() = %v", err)
	}
	response := m56GatewayPost(t, builder, m56SemanticOperationBody(t, semoptest.New(t).Request()))
	if len(response.Errors) != 1 || response.Errors[0].Extensions["code"] != string(semanticoperations.CodeUnavailable) || response.Data["semanticOperation"] != nil {
		t.Fatalf("response = %+v; want UNAVAILABLE with a null result", response)
	}
	if hub.captures.Load() != 0 {
		t.Fatal("unwired route captured a snapshot")
	}
	if err := installGatewaySemanticOperationRoute(nil, nil); err == nil {
		t.Fatal("a missing builder must fail composition")
	}
}

// With an intent owner injected through the same composition helper, admission
// is the built-in SemReg kernel: an operation no built-in pack defines is
// refused before executor lookup and never sent.
func TestM56GatewayCompositionAdmitsThroughBuiltinKernel(t *testing.T) {
	f := semoptest.New(t)
	executor := f.Executor(semoptest.Outcome{})
	lookup := &semoptest.Lookup{}
	lookup.Register(f.Route(), semoptest.Operation, executor)
	builder := graphql.NewBuilder(nil, nil)
	if err := composeGatewaySemanticOperationRoute(builder, f, f.Owner(), lookup); err != nil {
		t.Fatal(err)
	}
	response := m56GatewayPost(t, builder, m56SemanticOperationBody(t, f.Request()))
	result, _ := response.Data["semanticOperation"].(map[string]any)
	if len(response.Errors) != 0 || result["status"] != "failed" || result["kernelOutcome"] != "rejected" || result["delivery"] != "not_sent" ||
		result["reason"] != string(semreg.DefinitionOwnerMissing) {
		t.Fatalf("response = %+v", response)
	}
	if executor.Calls() != 0 || lookup.Calls() != 0 {
		t.Fatalf("executor calls=%d lookups=%d; want none", executor.Calls(), lookup.Calls())
	}
}

// startHTTPServer installs the route on the shared provider before the GraphQL
// handlers exist, and the installed lookup is the default one.
func TestM56GatewayHTTPServerInstallsRouteBeforeHandlers(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "gateway_http_server.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	functions := map[string]*ast.FuncDecl{}
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok {
			functions[function.Name.Name] = function
		}
	}
	callsIn := func(function string) []*ast.CallExpr {
		var calls []*ast.CallExpr
		ast.Inspect(functions[function], func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				calls = append(calls, call)
			}
			return true
		})
		return calls
	}
	render := types.ExprString
	install, handlers := -1, -1
	for index, call := range callsIn("startHTTPServer") {
		name := render(call.Fun)
		if name == "installGatewaySemanticOperationRoute" && install < 0 {
			install = index
			if len(call.Args) != 2 || render(call.Args[0]) != "builder" || render(call.Args[1]) != "sharedSemanticProvider" {
				t.Fatalf("installGatewaySemanticOperationRoute arguments = %d", len(call.Args))
			}
		}
		if name == "newHTTPControlPlaneSemanticHandlers" && handlers < 0 {
			handlers = index
		}
	}
	if install < 0 || handlers < 0 || install > handlers {
		t.Fatalf("install call %d, handler construction %d; want install first", install, handlers)
	}
	composed := false
	for _, call := range callsIn("installGatewaySemanticOperationRoute") {
		if render(call.Fun) == "composeGatewaySemanticOperationRoute" {
			composed = len(call.Args) == 4 && render(call.Args[2]) == "nil" && render(call.Args[3]) == "semanticoperations.NoExecutors()"
		}
	}
	if !composed {
		t.Fatal("production composition must pass no intent owner and the default executor lookup")
	}
}
