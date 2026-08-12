package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

type issue788StateProvider struct{}

func (issue788StateProvider) M8SourceState(_ context.Context, inputID string) (json.RawMessage, error) {
	return issue788RawJSONNoTest(map[string]any{"source": inputID, "direct": true}), nil
}

func issue788RawJSONNoTest(value any) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return raw
}

func issue788RPCBody(t *testing.T, request rpcRequest) *bytes.Reader {
	t.Helper()
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.NewReader(raw)
}

func issue788RawJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func issue788DecodeResponse(t *testing.T, raw []byte) rpcResponse {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var response rpcResponse
	if err := decoder.Decode(&response); err != nil {
		t.Fatal(err)
	}
	return response
}

var m8SourceToolInventoryV1 = []string{
	"ebus.v1.registry.devices.list",
	"ebus.v1.semantic.snapshot.get",
	"eebus.v1.runtime.status.get",
	"eebus.v1.services.list",
	"eebus.v1.services.get",
	"eebus.v1.sessions.list",
	"eebus.v1.sessions.get",
	"eebus.v1.topology.get",
	"eebus.v1.snapshot.capture",
	"eebus.v1.snapshot.drop",
	"eebus.v1.pairing.status.get",
}

func TestIssue788M8SourceScopeListsOnlyFrozenReadOnlyInventory(t *testing.T) {
	server, _ := issue743Server(t)
	handler := server.eebusV1OperatorHandler()

	request := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	request.Header.Set(m8SourceScopeHeader, m8SourceScopeV1)
	request = request.WithContext(request.Context())
	recorder := httptest.NewRecorder()

	// Use the ordinary JSON-RPC helper body after adding the evidence-scope
	// header so this exercises the production operator handler boundary.
	request = httptest.NewRequest(http.MethodPost, "/mcp", issue788RPCBody(t, rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/list",
	}))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(m8SourceScopeHeader, m8SourceScopeV1)
	handler.ServeHTTP(recorder, request)

	response := issue788DecodeResponse(t, recorder.Body.Bytes())
	if response.Error != nil {
		t.Fatalf("tools/list error = %+v", response.Error)
	}
	result, ok := response.Result.(map[string]any)
	if !ok {
		t.Fatalf("tools/list result type = %T", response.Result)
	}
	items, ok := result["tools"].([]any)
	if !ok {
		t.Fatalf("tools type = %T", result["tools"])
	}
	names := make([]string, 0, len(items))
	for _, raw := range items {
		item, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("tool type = %T", raw)
		}
		name, ok := item["name"].(string)
		if !ok {
			t.Fatalf("tool name type = %T", item["name"])
		}
		names = append(names, name)
	}
	if !reflect.DeepEqual(names, m8SourceToolInventoryV1) {
		t.Fatalf("M8 inventory = %v, want %v", names, m8SourceToolInventoryV1)
	}
}

func TestIssue788M8SourceScopeRejectsToolsOutsideFrozenInventory(t *testing.T) {
	server, _ := issue743Server(t)
	handler := server.eebusV1OperatorHandler()

	for _, name := range []string{
		"ebus.v1.runtime.status.get",
		"eebus.v1.semantic.system.set_config",
		"eebus.v1.features.data.get",
		"helianthus.experimental.leaf_promotion.capture",
	} {
		request := httptest.NewRequest(http.MethodPost, "/mcp", issue788RPCBody(t, rpcRequest{
			JSONRPC: "2.0", ID: 1, Method: "tools/call",
			Params: issue788RawJSON(t, map[string]any{"name": name, "arguments": map[string]any{}}),
		}))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(m8SourceScopeHeader, m8SourceScopeV1)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		response := issue788DecodeResponse(t, recorder.Body.Bytes())
		if response.Error == nil || response.Error.Message != `unknown tool "`+name+`"` {
			t.Fatalf("%s error = %+v", name, response.Error)
		}
	}
}

func TestIssue788DirectSourceStateIsOperatorOnlyAndExcludedFromM8Inventory(t *testing.T) {
	server, _ := issue743Server(t)
	if err := server.RegisterM8SourceStateProvider(issue788StateProvider{}); err != nil {
		t.Fatal(err)
	}

	operator := issue743OperatorHandler(t, server)
	for _, inputID := range m8SourceStateInputIDs {
		result := msp06Call(t, operator, m8SourceStateToolName, map[string]any{"input_id": inputID})
		if result.isError {
			t.Fatalf("%s returned error: %s", inputID, result.raw)
		}
		data := msp06Map(t, result.envelope["data"], inputID)
		if data["source"] != inputID || data["direct"] != true {
			t.Fatalf("%s data = %#v", inputID, data)
		}
	}

	params := issue788RawJSON(t, map[string]any{
		"name": m8SourceStateToolName, "arguments": map[string]any{"input_id": "ebus.debug"},
	})
	public := doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/call", Params: params})
	if public.Error == nil || public.Error.Message != `unknown tool "`+m8SourceStateToolName+`"` {
		t.Fatalf("public error = %+v", public.Error)
	}

	request := httptest.NewRequest(http.MethodPost, "/mcp", issue788RPCBody(t, rpcRequest{
		JSONRPC: "2.0", ID: 2, Method: "tools/list",
	}))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(m8SourceScopeHeader, m8SourceScopeV1)
	recorder := httptest.NewRecorder()
	operator.ServeHTTP(recorder, request)
	if bytes.Contains(recorder.Body.Bytes(), []byte(m8SourceStateToolName)) {
		t.Fatalf("M8 scoped inventory leaked experimental source-state tool: %s", recorder.Body.Bytes())
	}
}
