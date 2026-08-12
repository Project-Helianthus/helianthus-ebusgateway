package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/m8sourcestate"
)

type issue788ConfigWriter struct {
	routing m8sourcestate.CommandRoutingFragment
}

func (*issue788ConfigWriter) SetSystemConfig(context.Context, string, string) ConfigSetResult {
	return ConfigSetResult{Success: true}
}

func (*issue788ConfigWriter) SetBoilerConfig(context.Context, string, string) ConfigSetResult {
	return ConfigSetResult{Success: true}
}

func (writer *issue788ConfigWriter) M8CommandRoutingState() (m8sourcestate.CommandRoutingFragment, error) {
	return writer.routing, nil
}

type issue788ScheduleWriter struct {
	routing m8sourcestate.CommandRoutingFragment
}

func (issue788ScheduleWriter) SetZoneTimeProgram(context.Context, int, int, []TimeProgramSlot) (*TimeProgramWriteResult, error) {
	return &TimeProgramWriteResult{Success: true}, nil
}

func (issue788ScheduleWriter) SetDhwTimeProgram(context.Context, int, []TimeProgramSlot) (*TimeProgramWriteResult, error) {
	return &TimeProgramWriteResult{Success: true}, nil
}

func (writer issue788ScheduleWriter) M8CommandRoutingState() (m8sourcestate.CommandRoutingFragment, error) {
	return writer.routing, nil
}

type issue788DebugOwner struct {
	state m8sourcestate.DebugState
}

func (*issue788DebugOwner) Snapshot() BusObservabilitySnapshot { return BusObservabilitySnapshot{} }

func (*issue788DebugOwner) ProtocolSpecimens(string) []BusProtocolSpecimen { return nil }

func (owner *issue788DebugOwner) M8DebugSourceState() m8sourcestate.DebugState {
	return owner.state
}

type issue788SemanticOwner struct {
	testSemanticProvider
	registry m8sourcestate.SemanticRegistry
}

func (owner *issue788SemanticOwner) M8SemanticRegistryState() (m8sourcestate.SemanticRegistry, error) {
	return owner.registry, nil
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

	request := httptest.NewRequest(http.MethodPost, "/mcp", issue788RPCBody(t, rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/list",
	}))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(m8SourceScopeHeader, m8SourceScopeV1)
	recorder := httptest.NewRecorder()
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

func TestIssue788PublicHTTPCannotActivateM8SourceScope(t *testing.T) {
	server, _ := issue743Server(t)
	request := httptest.NewRequest(http.MethodPost, "/mcp", issue788RPCBody(t, rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/list",
	}))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(m8SourceScopeHeader, m8SourceScopeV1)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)

	response := issue788DecodeResponse(t, recorder.Body.Bytes())
	result, ok := response.Result.(map[string]any)
	if response.Error != nil || !ok {
		t.Fatalf("public tools/list = result:%T error:%+v", response.Result, response.Error)
	}
	items, ok := result["tools"].([]any)
	if !ok || len(items) <= len(m8SourceToolInventoryV1) {
		t.Fatalf("public header activated scoped inventory: %#v", result["tools"])
	}
	for _, raw := range items {
		item, ok := raw.(map[string]any)
		if ok && item["name"] == m8SourceStateToolName {
			t.Fatalf("public inventory exposed %s", m8SourceStateToolName)
		}
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

func TestIssue788M8SourceScopeRejectsSnapshotLifecycleBeforeStoreAccess(t *testing.T) {
	server, _ := issue743Server(t)
	operator := server.eebusV1OperatorHandler()
	scopedHeaders := map[string]string{m8SourceScopeHeader: m8SourceScopeV1}

	beforeCapture := len(server.eebusV1.store.activeRoots)
	for _, tool := range []string{msp06SnapshotCapture, msp06SnapshotDrop} {
		params := map[string]any{}
		if tool == msp06SnapshotDrop {
			params["snapshot_ref"] = "not-a-valid-reference"
		}
		response := issue788ScopedToolCall(t, operator, tool, params, scopedHeaders)
		want := `tool "` + tool + `" is not callable in read-only evidence scope`
		if response.Error == nil || response.Error.Message != want {
			t.Fatalf("%s error = %+v, want %q", tool, response.Error, want)
		}
	}
	if got := len(server.eebusV1.store.activeRoots); got != beforeCapture {
		t.Fatalf("scoped lifecycle calls changed active root count: got %d, want %d", got, beforeCapture)
	}

	duplicateParams := json.RawMessage(`{"name":"eebus.v1.snapshot.capture","arguments":{"nested":{"key":1,"key":2}}}`)
	request := httptest.NewRequest(http.MethodPost, "/mcp", issue788RPCBody(t, rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call", Params: duplicateParams,
	}))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(m8SourceScopeHeader, m8SourceScopeV1)
	recorder := httptest.NewRecorder()
	operator.ServeHTTP(recorder, request)
	response := issue788DecodeResponse(t, recorder.Body.Bytes())
	want := `tool "` + msp06SnapshotCapture + `" is not callable in read-only evidence scope`
	if response.Error == nil || response.Error.Message != want {
		t.Fatalf("duplicate-key snapshot.capture error = %+v, want %q", response.Error, want)
	}
	if got := len(server.eebusV1.store.activeRoots); got != beforeCapture {
		t.Fatalf("duplicate-key lifecycle call changed active root count: got %d, want %d", got, beforeCapture)
	}

	capture := msp06Call(t, operator, msp06SnapshotCapture, map[string]any{})
	root := msp06Map(t, capture.envelope["data"], "capture data")
	token, ok := root["snapshot_ref"].(string)
	if !ok || token == "" {
		t.Fatalf("snapshot_ref = %#v", root["snapshot_ref"])
	}
	beforeDropRoots := len(server.eebusV1.store.activeRoots)
	beforeDropTokens := len(server.eebusV1.store.activeTokens)
	response = issue788ScopedToolCall(t, operator, msp06SnapshotDrop, map[string]any{"snapshot_ref": token}, scopedHeaders)
	want = `tool "` + msp06SnapshotDrop + `" is not callable in read-only evidence scope`
	if response.Error == nil || response.Error.Message != want {
		t.Fatalf("snapshot.drop error = %+v, want %q", response.Error, want)
	}
	if got := len(server.eebusV1.store.activeRoots); got != beforeDropRoots {
		t.Fatalf("scoped snapshot.drop changed active root count: got %d, want %d", got, beforeDropRoots)
	}
	if got := len(server.eebusV1.store.activeTokens); got != beforeDropTokens {
		t.Fatalf("scoped snapshot.drop changed active token count: got %d, want %d", got, beforeDropTokens)
	}

	drop := msp06Call(t, operator, msp06SnapshotDrop, map[string]any{"snapshot_ref": token})
	dropData := msp06Map(t, drop.envelope["data"], "drop data")
	if drop.isError || dropData["status"] != "dropped" {
		t.Fatalf("operator cleanup drop = %s", drop.raw)
	}
}

func issue788ScopedToolCall(t *testing.T, handler http.Handler, tool string, arguments map[string]any, headers map[string]string) rpcResponse {
	t.Helper()
	params := issue788RawJSON(t, map[string]any{"name": tool, "arguments": arguments})
	request := httptest.NewRequest(http.MethodPost, "/mcp", issue788RPCBody(t, rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call", Params: params,
	}))
	request.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return issue788DecodeResponse(t, recorder.Body.Bytes())
}

func TestIssue788DirectSourceStateIsOperatorOnlyAndExcludedFromM8Inventory(t *testing.T) {
	server, _ := issue743Server(t)
	debugOwner := &issue788DebugOwner{state: m8sourcestate.DebugState{
		Status: m8sourcestate.DebugStatus{TransportClass: "enh"},
	}}
	semanticOwner := &issue788SemanticOwner{registry: m8sourcestate.SemanticRegistry{
		Authority: "ebus.promoted",
		Leaves: []m8sourcestate.SemanticLeaf{
			{Path: "/zones/0", PromotionState: "PROMOTED", Source: "ebus"},
		},
	}}
	configOwner := &issue788ConfigWriter{routing: m8sourcestate.CommandRoutingFragment{Routes: []m8sourcestate.CommandRoute{
		{SemanticPath: "/mcp/ebus.v1.semantic.system.set_config", Source: "ebus", Available: true},
		{SemanticPath: "/mcp/ebus.v1.semantic.boiler_status.set_config", Source: "ebus", Available: true},
	}}}
	scheduleOwner := issue788ScheduleWriter{routing: m8sourcestate.CommandRoutingFragment{Routes: []m8sourcestate.CommandRoute{
		{SemanticPath: "/mcp/ebus.v1.semantic.schedules.set_zone_time_program", Source: "ebus", Available: true},
		{SemanticPath: "/mcp/ebus.v1.semantic.schedules.set_dhw_time_program", Source: "ebus", Available: true},
	}}}
	server.SetBusObservabilityProvider(debugOwner)
	server.SetSemanticProvider(semanticOwner)
	server.SetConfigWriter(configOwner)
	server.SetScheduleWriter(scheduleOwner)

	operator := issue743OperatorHandler(t, server)
	for _, inputID := range m8SourceStateInputIDs {
		result := msp06Call(t, operator, m8SourceStateToolName, map[string]any{"input_id": inputID})
		if result.isError {
			t.Fatalf("%s returned error: %s", inputID, result.raw)
		}
		data := msp06Map(t, result.envelope["data"], inputID)
		switch inputID {
		case "ebus.debug":
			status := msp06Map(t, data["status"], "debug status")
			if status["transport_class"] != "enh" {
				t.Fatalf("%s data = %#v", inputID, data)
			}
		case "command.routing":
			routes, ok := data["routes"].([]any)
			if !ok || len(routes) != 4 {
				t.Fatalf("%s routes = %#v", inputID, data["routes"])
			}
		case "semantic.registry":
			leaves, ok := data["leaves"].([]any)
			if data["authority"] != "ebus.promoted" || !ok || len(leaves) != 1 {
				t.Fatalf("%s data = %#v", inputID, data)
			}
		}
	}

	debugOwner.state.Status.TransportClass = "ens"
	debugChanged := msp06Call(t, operator, m8SourceStateToolName, map[string]any{"input_id": "ebus.debug"})
	if !bytes.Contains([]byte(debugChanged.raw), []byte(`"transport_class":"ens"`)) {
		t.Fatalf("debug owner mutation was not captured: %s", debugChanged.raw)
	}
	configOwner.routing.Routes[0].Source = "candidate"
	routingChanged := msp06Call(t, operator, m8SourceStateToolName, map[string]any{"input_id": "command.routing"})
	if !bytes.Contains([]byte(routingChanged.raw), []byte(`"source":"candidate"`)) {
		t.Fatalf("routing owner mutation was not captured: %s", routingChanged.raw)
	}
	semanticOwner.registry.Leaves[0].PromotionState = "WITHHELD"
	semanticChanged := msp06Call(t, operator, m8SourceStateToolName, map[string]any{"input_id": "semantic.registry"})
	if !bytes.Contains([]byte(semanticChanged.raw), []byte(`"promotion_state":"WITHHELD"`)) {
		t.Fatalf("semantic owner mutation was not captured: %s", semanticChanged.raw)
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

func TestIssue788DirectSourceStateFailsClosedWithoutOwningSnapshot(t *testing.T) {
	tests := []struct {
		name    string
		inputID string
		install func(*Server)
	}{
		{
			name: "debug", inputID: "ebus.debug",
			install: func(server *Server) { server.SetBusObservabilityProvider(&testBusObservabilityProvider{}) },
		},
		{
			name: "routing", inputID: "command.routing",
			install: func(server *Server) { server.SetScheduleWriter(&testScheduleWriter{}) },
		},
		{
			name: "semantic", inputID: "semantic.registry",
			install: func(server *Server) { server.SetSemanticProvider(testSemanticProvider{}) },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, _ := issue743Server(t)
			test.install(server)
			result := msp06Call(t, issue743OperatorHandler(t, server), m8SourceStateToolName, map[string]any{"input_id": test.inputID})
			if !result.isError || !bytes.Contains([]byte(result.raw), []byte(`"category":"ACQUISITION_FAILED"`)) {
				t.Fatalf("result = %s; want fail-closed acquisition error", result.raw)
			}
		})
	}
}
