package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type p91StatusProvider struct {
	staticStatusProvider
	calls int
}

func (p *p91StatusProvider) DaemonStatus() ServiceStatus {
	p.calls++
	return ServiceStatus{Status: "synthetic"}
}

type p91SemanticProvider struct {
	staticSemanticProvider
	calls int
}

func (p *p91SemanticProvider) Zones() []Zone {
	p.calls++
	return []Zone{}
}

func p91Server(t *testing.T) (*Server, *p91StatusProvider, *p91SemanticProvider) {
	t.Helper()
	server, err := NewServer(&testRegistry{}, &testInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	status := &p91StatusProvider{}
	semantic := &p91SemanticProvider{}
	server.SetStatusProvider(status)
	server.SetSemanticProvider(semantic)
	return server, status, semantic
}

func p91Request(t *testing.T, server *Server, body string) rpcResponse {
	t.Helper()
	recorder := httptest.NewRecorder()
	// ServeHTTP directly: no listener or background goroutine is needed.
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("HTTP status = %d; want %d: %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var response rpcResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return response
}

func TestP91DuplicateEnvelopeMembersRejectBeforeTools(t *testing.T) {
	for _, tool := range []string{toolRuntimeStatusGetName, toolSemanticZonesGetName} {
		t.Run(tool, func(t *testing.T) {
			params := fmt.Sprintf(`{"name":%q,"arguments":{}}`, tool)
			otherTool := toolRuntimeStatusGetName
			if tool == otherTool {
				otherTool = toolSemanticZonesGetName
			}
			members := []struct {
				name  string
				extra string
			}{
				{"method", `"method":"tools/call"`},
				{"params", `"params":` + params},
				{"id", `"id":"request-a"`},
				{"jsonrpc", `"jsonrpc":"2.0"`},
				{"method_different", `"method":"tools/list"`},
				{"params_different", fmt.Sprintf(`"params":{"name":%q,"arguments":{}}`, otherTool)},
				{"id_different", `"id":"request-b"`},
				{"jsonrpc_equivalent_encoding", `"jsonrpc":"2\u002e0"`},
				{"escaped_method", `"meth\u006fd":"tools/call"`},
				{"unknown", `"extension":1,"extension":2`},
			}
			for _, member := range members {
				t.Run(member.name, func(t *testing.T) {
					server, status, semantic := p91Server(t)
					if !server.hasToolNamed(tool) {
						t.Fatalf("tool %q is not registered", tool)
					}
					body := fmt.Sprintf(`{"jsonrpc":"2.0","id":"request-a","method":"tools/call","params":%s,%s}`, params, member.extra)
					response := p91Request(t, server, body)
					if response.JSONRPC != "2.0" || response.Error == nil || response.Error.Code != -32600 || response.Result != nil || response.ID != nil {
						t.Fatalf("response = %+v; want invalid request without an ambiguous ID or result", response)
					}
					if status.calls != 0 || semantic.calls != 0 {
						t.Fatalf("tool providers ran: status=%d zones=%d", status.calls, semantic.calls)
					}
				})
			}
		})
	}
}

func TestP91ValidEnvelopePreservesToolResponses(t *testing.T) {
	for _, tool := range []string{toolRuntimeStatusGetName, toolSemanticZonesGetName} {
		for _, reordered := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reordered=%t", tool, reordered), func(t *testing.T) {
				server, status, semantic := p91Server(t)
				params := fmt.Sprintf(`{"name":%q,"arguments":{}}`, tool)
				// The pre-existing dispatch path is the response compatibility oracle.
				result, rpcErr := server.dispatch(context.Background(), "tools/call", json.RawMessage(params))
				if rpcErr != nil {
					t.Fatalf("positive dispatch failed: %+v", rpcErr)
				}
				status.calls, semantic.calls = 0, 0
				body := fmt.Sprintf(`{"jsonrpc":"2.0","id":"request-a","method":"tools/call","params":%s}`, params)
				if reordered {
					body = fmt.Sprintf(`{"params":%s,"method":"tools/call","id":"request-a","jsonrpc":"2.0"}`, params)
				}
				response := p91Request(t, server, body)
				if response.JSONRPC != "2.0" || response.ID != "request-a" || response.Error != nil || response.Result == nil {
					t.Fatalf("positive response = %+v", response)
				}
				wantStatus, wantZones := 0, 0
				if tool == toolRuntimeStatusGetName {
					wantStatus = 1
				} else {
					wantZones = 1
				}
				if status.calls != wantStatus || semantic.calls != wantZones {
					t.Fatalf("provider calls status=%d zones=%d; want %d, %d", status.calls, semantic.calls, wantStatus, wantZones)
				}
				if got, want := p91NormalizedResult(t, response.Result), p91NormalizedResult(t, result); !bytes.Equal(got, want) {
					t.Fatalf("response changed:\ngot  %s\nwant %s", got, want)
				}
			})
		}
	}
}

func p91NormalizedResult(t *testing.T, result any) []byte {
	t.Helper()
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil || len(decoded.Content) != 1 || decoded.IsError {
		t.Fatalf("invalid successful tool result: %s (%v)", encoded, err)
	}
	envelope := parseEnvelope(t, decoded.Content[0].Text)
	meta, ok := envelope["meta"].(map[string]any)
	if !ok {
		t.Fatalf("missing meta: %s", encoded)
	}
	timestamp, ok := meta["data_timestamp"].(string)
	if !ok {
		t.Fatalf("missing data_timestamp: %s", encoded)
	}
	if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil {
		t.Fatalf("invalid data_timestamp: %v", err)
	}
	// Only the wall-clock field varies between otherwise identical tool calls.
	delete(meta, "data_timestamp")
	text, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	var original map[string]any
	if err := json.Unmarshal(encoded, &original); err != nil {
		t.Fatal(err)
	}
	original["content"].([]any)[0].(map[string]any)["text"] = string(text)
	normalized, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	return normalized
}

func TestP91EnvelopeValidationDoesNotRecurseIntoParams(t *testing.T) {
	server, status, semantic := p91Server(t)
	response := p91Request(t, server, `{"jsonrpc":"2.0","id":"request-a","method":"ping","params":{"nested":{"value":1,"value":2}}}`)
	if response.Error != nil || response.Result == nil {
		t.Fatalf("nested members affected envelope validation: %+v", response)
	}
	response = p91Request(t, server, `{"jsonrpc":"2.0","id":"request-a","method":"tools/call","params":{"name":"ebus.v1.semantic.zones.get","arguments":{"value":1,"value":2}}}`)
	if response.Error == nil || response.Error.Code != -32602 {
		t.Fatalf("params duplicate behavior changed: %+v", response)
	}
	if status.calls != 0 || semantic.calls != 0 {
		t.Fatal("invalid params invoked a provider")
	}
}
