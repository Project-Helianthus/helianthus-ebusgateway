package main

import (
	"context"
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

type b503GraphQLErrorDispatcher struct{ err error }

func (dispatcher b503GraphQLErrorDispatcher) Invoke(_ context.Context, _ byte, _ []byte) ([]byte, error) {
	return nil, dispatcher.err
}

func (dispatcher b503GraphQLErrorDispatcher) InvokeB503Outcome(_ context.Context, _ byte, _ []byte) mcp.B503DispatchOutcome {
	return mcp.B503DispatchOutcome{Err: dispatcher.err}
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
	return newB503ParityRuntimeWithManager(t, dispatcher, b503session.New(
		b503session.TransportKey{AdapterInstanceID: "parity", TransportEpoch: 1},
		30*time.Second,
		nil,
	))
}

func newB503ParityRuntimeWithManager(t *testing.T, dispatcher mcp.RPCDispatcher, manager *b503session.Manager) (*b503Runtime, *b503GraphQLProvider) {
	t.Helper()
	server, err := mcp.NewServer(emptyMCPRegistry{}, nil)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	mcp.RegisterVaillantB503Tools(server, mcp.VaillantB503Options{
		Dispatcher: dispatcher, SessionManager: manager, DefaultTarget: defaultVaillantTarget,
	})
	runtime := &b503Runtime{mcpServer: server, manager: manager, dispatcher: dispatcher}
	return runtime, newB503GraphQLProvider(runtime)
}

func TestIssue552VaillantB503RefreshingSessionMCPGraphQLParity(t *testing.T) {
	refreshStarted := make(chan struct{})
	allowRefresh := make(chan struct{})
	manager := b503session.New(
		b503session.TransportKey{AdapterInstanceID: "parity", TransportEpoch: 1},
		time.Minute,
		func(context.Context) (b503session.TransportKey, error) {
			close(refreshStarted)
			<-allowRefresh
			return b503session.TransportKey{AdapterInstanceID: "parity", TransportEpoch: 2}, nil
		},
	)
	runtime, provider := newB503ParityRuntimeWithManager(t, &b503ParityDispatcher{}, manager)
	if _, err := manager.Enable(context.Background()); err != nil {
		t.Fatalf("Enable session: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		manager.OnEpochAdvance(context.Background(), 2)
		_, err := manager.ReadOperation(context.Background(), 0, func(context.Context, byte) b503session.DispatchOutcome {
			return b503session.DispatchOutcome{Emitted: true, Native: b503session.NativeACK}
		})
		done <- err
	}()
	<-refreshStarted

	mcpSession := mcpCallToolEnvelope(t, runtime.mcpServer.Handler(), "ebus.v1.vaillant.live_monitor.session.get", `{"target_address":21}`)
	graphSession := b503GraphQLResult(t, provider, `{ vaillantLiveMonitorSession(targetAddress:21) { state owned } }`)
	if len(graphSession.Errors) != 0 {
		t.Fatalf("GraphQL session errors: %+v", graphSession.Errors)
	}
	mcpState := mcpSession["data"].(map[string]any)
	graphState := graphSession.Data.(map[string]any)["vaillantLiveMonitorSession"].(map[string]any)
	if mcpState["state"] != "Refreshing" || mcpState["owned"] != true {
		t.Fatalf("MCP session=%#v; want Refreshing with ownership held", mcpState)
	}
	if mcpState["state"] != graphState["state"] || mcpState["owned"] != graphState["owned"] {
		t.Fatalf("session drift: MCP=%#v GraphQL=%#v", mcpState, graphState)
	}
	close(allowRefresh)
	if err := <-done; err != nil {
		t.Fatalf("triggering read: %v", err)
	}
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

func TestIssue552B503GraphQLPreservesDispatcherErrorCodes(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code string
	}{
		{name: "context cancellation before turnaround", err: errRawFrameUpstreamTimeout, code: "UPSTREAM_RPC_FAILED"},
		{name: "nak crc or protocol failure", err: errRawFrameUpstreamRPCFailed, code: "UPSTREAM_RPC_FAILED"},
		{name: "stale epoch completion", err: errRawFrameStaleEpoch, code: "UPSTREAM_RPC_FAILED"},
		{name: "cleanup pending", err: b503session.ErrCleanupPending, code: "UNKNOWN"},
		{name: "transport down", err: b503session.ErrTransportDown, code: "TRANSPORT_DOWN"},
		{name: "session contention", err: b503session.ErrSessionBusy, code: "SESSION_BUSY"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, provider := newB503ParityRuntime(t, b503GraphQLErrorDispatcher{err: tc.err})
			result := b503GraphQLResult(t, provider, `{ vaillantErrors(targetAddress:21) { firstActiveError } }`)
			if len(result.Errors) != 1 {
				t.Fatalf("GraphQL errors=%+v; want one field-local error", result.Errors)
			}
			if !strings.Contains(result.Errors[0].Message, tc.code) {
				t.Fatalf("GraphQL error=%q; want public code %q", result.Errors[0].Message, tc.code)
			}
		})
	}
}

func TestIssue552B503DispatcherTimeoutMCPGraphQLParity(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mcpTool    string
		mcpArgs    string
		graphQuery string
	}{
		{
			name:       "errors get",
			mcpTool:    "ebus.v1.vaillant.errors.get",
			mcpArgs:    `{"target_address":21}`,
			graphQuery: `{ vaillantErrors(targetAddress:21) { firstActiveError } }`,
		},
		{
			name:       "live enable",
			mcpTool:    "ebus.v1.vaillant.live_monitor.get",
			mcpArgs:    `{"action":"enable","target_address":21}`,
			graphQuery: `{ vaillantLiveMonitor(action:"enable", targetAddress:21) { issuerToken } }`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime, provider := newB503ParityRuntime(t, b503GraphQLErrorDispatcher{err: errRawFrameUpstreamTimeout})
			body := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"` + tc.mcpTool + `","arguments":` + tc.mcpArgs + `}}`
			response := mcpRPC(t, runtime.mcpServer.Handler(), body)
			if response.Error != nil || response.Result == nil {
				t.Fatalf("MCP response error=%+v result=%#v", response.Error, response.Result)
			}
			content := response.Result["content"].([]any)[0].(map[string]any)["text"].(string)
			if !strings.Contains(content, `"code":"UPSTREAM_RPC_FAILED"`) || strings.Contains(content, `"code":"UPSTREAM_TIMEOUT"`) {
				t.Fatalf("MCP timeout classification drift: %s", content)
			}

			result := b503GraphQLResult(t, provider, tc.graphQuery)
			if len(result.Errors) != 1 {
				t.Fatalf("GraphQL errors=%+v; want one field-local timeout", result.Errors)
			}
			message := result.Errors[0].Message
			if !strings.Contains(message, "UPSTREAM_RPC_FAILED") || strings.Contains(message, "UPSTREAM_TIMEOUT") {
				t.Fatalf("GraphQL timeout classification %q does not match MCP", message)
			}
		})
	}
}

func TestIssue552VaillantB503PromotedMCPGraphQLParity(t *testing.T) {
	runtime, provider := newB503ParityRuntime(t, &b503ParityDispatcher{})
	mcpHistory := mcpCallToolEnvelope(t, runtime.mcpServer.Handler(), "ebus.v1.vaillant.errors.history.list", `{"target_address":21,"limit":2}`)
	graphResult := b503GraphQLResult(t, provider, `{ vaillantErrorsHistory(targetAddress:21, limit:2) { records { index firstActiveError slots } failure { index code message } } }`)
	if len(graphResult.Errors) != 0 {
		t.Fatalf("GraphQL history errors: %+v", graphResult.Errors)
	}

	mcpData := mcpHistory["data"].(map[string]any)
	mcpRows := mcpData["records"].([]any)
	graphHistory := graphResult.Data.(map[string]any)["vaillantErrorsHistory"].(map[string]any)
	graphRows := graphHistory["records"].([]any)
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
	if !strings.Contains(content, `"records":[{`) || !strings.Contains(content, `"failure":{"index":1,"code":"UPSTREAM_RPC_FAILED"`) {
		t.Fatalf("MCP partial aggregate contract drift: %s", content)
	}

	graphResult := b503GraphQLResult(t, provider, `{
		vaillantErrorsHistory(limit:2) { records { index } failure { index code message } }
		vaillantCapabilities { vaillantB503 { reason available } }
	}`)
	if len(graphResult.Errors) != 0 {
		t.Fatalf("GraphQL partial aggregate must remain typed data: %+v", graphResult.Errors)
	}
	if data, ok := graphResult.Data.(map[string]any); ok {
		history := data["vaillantErrorsHistory"].(map[string]any)
		records := history["records"].([]any)
		failure := history["failure"].(map[string]any)
		if len(records) != 1 || records[0].(map[string]any)["index"] != 0 || failure["index"] != 1 || failure["code"] != "UPSTREAM_RPC_FAILED" {
			t.Fatalf("GraphQL partial history=%#v; want prefix and structured failure", history)
		}
		capability := data["vaillantCapabilities"].(map[string]any)["vaillantB503"].(map[string]any)
		if capability["reason"] != "AVAILABLE" || capability["available"] != true {
			t.Fatalf("GraphQL lost sibling capability after aggregate failure: %#v", capability)
		}
	} else {
		t.Fatalf("GraphQL aggregate failure discarded all root data: %#v", graphResult.Data)
	}
}
