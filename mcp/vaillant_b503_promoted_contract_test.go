package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type promotedB503Dispatcher struct {
	failIndex *byte
}

func (dispatcher *promotedB503Dispatcher) Invoke(_ context.Context, _ byte, payload []byte) ([]byte, error) {
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

func TestVaillantB503PromotedToolsGolden(t *testing.T) {
	dispatcher := &promotedB503Dispatcher{}
	server := newB503Server(t, &stubB503Dispatcher{
		respByPrefix: map[string][]byte{
			"\x00\x01": {0x19, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
			"\x01\x01": {0x00, 0x19, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		},
	}, newDefaultMgr())
	// Replace only the dispatcher in the registered state so indexed responses
	// are deterministic and distinct while retaining the shared test server.
	state, ok := b503StateFor(server)
	if !ok {
		t.Fatal("B503 state unavailable")
	}
	state.opts.Dispatcher = dispatcher

	history := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolVaillantB503ErrorsHistoryListName + `","arguments":{"target_address":21,"limit":2}}`),
	}))
	session := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 2, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolVaillantB503LiveSessionGetName + `","arguments":{"target_address":21}}`),
	}))
	for name, envelope := range map[string]map[string]any{
		"vaillant_b503_errors_history_list.golden.json":      history,
		"vaillant_b503_live_monitor_session_get.golden.json": session,
	} {
		meta := envelope["meta"].(map[string]any)
		if meta["data_timestamp"] == "" {
			t.Fatalf("%s data timestamp is empty", name)
		}
		meta["data_timestamp"] = "<runtime>"
		assertB503Golden(t, name, envelope)
	}

	list := doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 3, Method: "tools/list"})
	tools := list.Result.(map[string]any)["tools"].([]any)
	selected := make([]any, 0, 2)
	for _, raw := range tools {
		tool, _ := raw.(map[string]any)
		name, _ := tool["name"].(string)
		if name == toolVaillantB503ErrorsHistoryListName || name == toolVaillantB503LiveSessionGetName {
			selected = append(selected, tool)
		}
	}
	if len(selected) != 2 {
		t.Fatalf("promoted tool schema count = %d; want 2", len(selected))
	}
	assertB503Golden(t, "vaillant_b503_promoted_tools.golden.json", selected)
}

func TestVaillantB503ErrorsHistoryListFailsWithoutPartialData(t *testing.T) {
	failIndex := byte(1)
	server := newB503Server(t, &stubB503Dispatcher{}, newDefaultMgr())
	state, _ := b503StateFor(server)
	state.opts.Dispatcher = &promotedB503Dispatcher{failIndex: &failIndex}

	envelope := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolVaillantB503ErrorsHistoryListName + `","arguments":{"limit":2}}`),
	}))
	if envelope["data"] != nil {
		t.Fatalf("partial aggregate data = %#v; want nil", envelope["data"])
	}
	errorObject := envelope["error"].(map[string]any)
	if errorObject["code"] != "UPSTREAM_RPC_FAILED" {
		t.Fatalf("error code = %#v; want UPSTREAM_RPC_FAILED", errorObject["code"])
	}
}

func TestVaillantB503ErrorsHistoryListRejectsEchoedIndexDrift(t *testing.T) {
	server := newB503Server(t, &stubB503Dispatcher{
		respByPrefix: map[string][]byte{
			"\x00\x01": {0x19, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
			"\x01\x01": {0x05, 0x19, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		},
	}, newDefaultMgr())
	envelope := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"` + toolVaillantB503ErrorsHistoryListName + `","arguments":{"limit":1}}`),
	}))
	if envelope["data"] != nil {
		t.Fatalf("drifted aggregate data = %#v; want nil", envelope["data"])
	}
	errorObject := envelope["error"].(map[string]any)
	if errorObject["code"] != "DECODE_FAILED" {
		t.Fatalf("error code = %#v; want DECODE_FAILED", errorObject["code"])
	}
}

func assertB503Golden(t *testing.T, name string, value any) {
	t.Helper()
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatalf("marshal %s: %v", name, err)
	}
	path := filepath.Join("testdata", name)
	if os.Getenv("UPDATE") == "1" {
		if err := os.WriteFile(path, append(encoded, '\n'), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	if string(want) != string(encoded)+"\n" {
		t.Fatalf("%s mismatch\nwant:\n%s\ngot:\n%s", name, want, encoded)
	}
}
