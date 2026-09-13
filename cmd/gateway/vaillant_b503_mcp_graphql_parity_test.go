package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/vaillant/b503session"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	graphqlgo "github.com/graphql-go/graphql"
)

type b503ParityDispatcher struct {
	failIndex *byte
}

func (dispatcher *b503ParityDispatcher) Invoke(_ context.Context, _ byte, payload []byte) ([]byte, error) {
	if len(payload) < 2 {
		return nil, errors.New("short request")
	}
	switch {
	case payload[0] == 0x00 && payload[1] == 0x01:
		return []byte{0x19, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, nil
	case payload[0] == 0x01 && payload[1] == 0x01 && len(payload) == 3:
		index := payload[2]
		if dispatcher.failIndex != nil && index == *dispatcher.failIndex {
			return nil, errors.New("history transport failure")
		}
		value := uint16(0x0119) + uint16(index)
		return []byte{index, byte(value), byte(value >> 8), 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, nil
	default:
		return nil, errors.New("unsupported request")
	}
}

func newB503ParityRuntime(t *testing.T, dispatcher mcp.RPCDispatcher) (*b503Runtime, *b503GraphQLProvider) {
	t.Helper()
	server, err := mcp.NewServer(emptyMCPRegistry{}, nil)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	manager := b503session.New(
		b503session.TransportKey{AdapterInstanceID: "parity", TransportEpoch: 1},
		30*time.Second,
		nil,
	)
	mcp.RegisterVaillantB503Tools(server, mcp.VaillantB503Options{
		Dispatcher: dispatcher, SessionManager: manager, DefaultTarget: defaultVaillantTarget,
	})
	runtime := &b503Runtime{mcpServer: server, manager: manager, dispatcher: dispatcher}
	return runtime, newB503GraphQLProvider(runtime)
}

func b503GraphQLResult(t *testing.T, provider *b503GraphQLProvider, query string) *graphqlgo.Result {
	t.Helper()
	builder := graphql.NewBuilder(nil, nil)
	builder.SetVaillantB503Provider(provider)
	schema, err := graphql.NewQuerySchema(builder)
	if err != nil {
		t.Fatalf("NewQuerySchema: %v", err)
	}
	return graphqlgo.Do(graphqlgo.Params{Schema: schema, RequestString: query})
}

func TestIssue552VaillantB503PromotedMCPGraphQLParity(t *testing.T) {
	runtime, provider := newB503ParityRuntime(t, &b503ParityDispatcher{})
	mcpHistory := mcpCallToolEnvelope(t, runtime.mcpServer.Handler(), "ebus.v1.vaillant.errors.history.list", `{"target_address":21,"limit":2}`)
	graphResult := b503GraphQLResult(t, provider, `{ vaillantErrorsHistory(targetAddress:21, limit:2) { index firstActiveError slots } }`)
	if len(graphResult.Errors) != 0 {
		t.Fatalf("GraphQL history errors: %+v", graphResult.Errors)
	}

	mcpRows := mcpHistory["data"].([]any)
	graphRows := graphResult.Data.(map[string]any)["vaillantErrorsHistory"].([]any)
	if len(mcpRows) != len(graphRows) {
		t.Fatalf("history lengths MCP=%d GraphQL=%d", len(mcpRows), len(graphRows))
	}
	for index := range mcpRows {
		mcpRow := mcpRows[index].(map[string]any)
		graphRow := graphRows[index].(map[string]any)
		if int(mcpRow["index"].(float64)) != graphRow["index"] || int(mcpRow["first_active_error"].(float64)) != graphRow["firstActiveError"] {
			t.Fatalf("history row %d drift: MCP=%#v GraphQL=%#v", index, mcpRow, graphRow)
		}
		mcpSlots := mcpRow["slots"].([]any)
		graphSlots := graphRow["slots"].([]any)
		if len(mcpSlots) != len(graphSlots) {
			t.Fatalf("history row %d slot length drift", index)
		}
		for slot := range mcpSlots {
			if mcpSlots[slot] == nil && graphSlots[slot] == nil {
				continue
			}
			if mcpSlots[slot] == nil || graphSlots[slot] == nil || int(mcpSlots[slot].(float64)) != graphSlots[slot] {
				t.Fatalf("history row %d slot %d drift: MCP=%#v GraphQL=%#v", index, slot, mcpSlots[slot], graphSlots[slot])
			}
		}
	}

	if _, err := runtime.manager.Enable(context.Background()); err != nil {
		t.Fatalf("Enable session: %v", err)
	}
	mcpSession := mcpCallToolEnvelope(t, runtime.mcpServer.Handler(), "ebus.v1.vaillant.live_monitor.session.get", `{"target_address":21}`)
	graphSession := b503GraphQLResult(t, provider, `{ vaillantLiveMonitorSession(targetAddress:21) { state owned } }`)
	if len(graphSession.Errors) != 0 {
		t.Fatalf("GraphQL session errors: %+v", graphSession.Errors)
	}
	mcpState := mcpSession["data"].(map[string]any)
	graphState := graphSession.Data.(map[string]any)["vaillantLiveMonitorSession"].(map[string]any)
	if mcpState["state"] != graphState["state"] || mcpState["owned"] != graphState["owned"] {
		t.Fatalf("session drift: MCP=%#v GraphQL=%#v", mcpState, graphState)
	}
}

func TestIssue552VaillantB503HistoryMCPGraphQLFailureParity(t *testing.T) {
	failIndex := byte(1)
	runtime, provider := newB503ParityRuntime(t, &b503ParityDispatcher{failIndex: &failIndex})
	body := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"ebus.v1.vaillant.errors.history.list","arguments":{"limit":2}}}`
	response := mcpRPC(t, runtime.mcpServer.Handler(), body)
	if response.Error != nil {
		t.Fatalf("MCP RPC error: %+v", response.Error)
	}
	content := response.Result["content"].([]any)[0].(map[string]any)["text"].(string)
	if !strings.Contains(content, `"code":"UPSTREAM_RPC_FAILED"`) || !strings.Contains(content, `"data":null`) {
		t.Fatalf("MCP aggregate did not fail closed: %s", content)
	}

	graphResult := b503GraphQLResult(t, provider, `{
		vaillantErrorsHistory(limit:2) { index }
		vaillantCapabilities { vaillantB503 { reason available } }
	}`)
	if len(graphResult.Errors) != 1 || !strings.Contains(graphResult.Errors[0].Message, "UPSTREAM_RPC_FAILED") {
		t.Fatalf("GraphQL aggregate error drift: %+v", graphResult.Errors)
	}
	if data, ok := graphResult.Data.(map[string]any); ok {
		encoded, err := json.Marshal(data)
		if err != nil || !strings.Contains(string(encoded), `"vaillantErrorsHistory":null`) {
			t.Fatalf("GraphQL returned non-null aggregate data: %s (%v)", encoded, err)
		}
		capability := data["vaillantCapabilities"].(map[string]any)["vaillantB503"].(map[string]any)
		if capability["reason"] != "AVAILABLE" || capability["available"] != true {
			t.Fatalf("GraphQL lost sibling capability after aggregate failure: %#v", capability)
		}
	} else {
		t.Fatalf("GraphQL aggregate failure discarded all root data: %#v", graphResult.Data)
	}
}
