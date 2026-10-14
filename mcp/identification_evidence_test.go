package mcp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type testIdentificationEvidenceProvider struct {
	snapshot IdentificationEvidenceSnapshot
}

func (p testIdentificationEvidenceProvider) IdentificationEvidence() IdentificationEvidenceSnapshot {
	return p.snapshot
}

func identificationEvidenceFixture() IdentificationEvidenceSnapshot {
	at := time.Date(2026, 10, 14, 8, 0, 0, 0, time.UTC)
	return IdentificationEvidenceSnapshot{
		SessionID: "observer-20261014T080000.000000000Z",
		Exchanges: []IdentificationEvidenceExchange{
			{Address: 0x08, Sequence: 1, SessionID: "observer-20261014T080000.000000000Z", CapturedAt: at,
				Request: []byte{0x71, 0x08, 0x07, 0x04, 0x00}, Response: []byte{0x0A, 0xB5, 'B', 'A', 'I', '0', '0', 0x01, 0x02, 0x03, 0x04}},
			{Address: 0x15, Sequence: 2, SessionID: "observer-20261014T080000.000000000Z", CapturedAt: at.Add(time.Second),
				Request: []byte{0x71, 0x15, 0x07, 0x04, 0x00}, Response: []byte{0x0A, 0xB5, 'B', 'A', 'S', 'V', '2', 0x05, 0x06, 0x07, 0x08}},
		},
	}
}

func newIdentificationEvidenceServer(t *testing.T, withProvider bool) *Server {
	t.Helper()
	server, err := NewServer(registry.NewDeviceRegistry(nil), &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	if withProvider {
		server.SetIdentificationEvidenceProvider(testIdentificationEvidenceProvider{snapshot: identificationEvidenceFixture()})
	}
	return server
}

func TestIdentificationEvidenceToolRegisteredOnlyWithProvider(t *testing.T) {
	if newIdentificationEvidenceServer(t, false).hasToolNamed(toolBusIdentificationEvidenceListName) {
		t.Fatalf("tool registered without provider")
	}
	server := newIdentificationEvidenceServer(t, true)
	if !server.hasToolNamed(toolBusIdentificationEvidenceListName) {
		t.Fatalf("tool not registered with provider")
	}
	server.SetIdentificationEvidenceProvider(testIdentificationEvidenceProvider{})
	count := 0
	for _, tool := range server.tools {
		if tool.Name == toolBusIdentificationEvidenceListName {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("tool registered %d times", count)
	}
}

func TestIdentificationEvidenceGoldenEnvelope(t *testing.T) {
	server := newIdentificationEvidenceServer(t, true)
	call := func(args string) map[string]any {
		return envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0", ID: 1, Method: "tools/call",
			Params: json.RawMessage(`{"name":"ebus.v1.bus.identification_evidence.list","arguments":` + args + `}`),
		}))
	}
	first, second := call(`{}`), call(`{}`)
	meta := first["meta"].(map[string]any)
	if meta["data_hash"] != second["meta"].(map[string]any)["data_hash"] {
		t.Fatalf("data_hash differs across identical calls")
	}
	meta["data_timestamp"] = "2026-10-14T09:00:00Z"

	actual, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	path := filepath.Join("testdata", "bus_identification_evidence.golden.json")
	if os.Getenv("UPDATE") == "1" {
		if err := os.WriteFile(path, append(actual, '\n'), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden: %v (run with UPDATE=1 to create)", err)
	}
	if string(actual) != strings.TrimSpace(string(want)) {
		t.Fatalf("identification evidence golden mismatch\nwant: %s\ngot:  %s", strings.TrimSpace(string(want)), actual)
	}

	data := first["data"].(map[string]any)
	if data["evidence_class"] != "raw" || data["decoded"] != false || data["qualification"] != "none" {
		t.Fatalf("raw-evidence markers missing: %v", data)
	}
}

func TestIdentificationEvidenceLimitKeepsNewest(t *testing.T) {
	server := newIdentificationEvidenceServer(t, true)
	env := envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0", ID: 1, Method: "tools/call",
		Params: json.RawMessage(`{"name":"ebus.v1.bus.identification_evidence.list","arguments":{"limit":1}}`),
	}))
	items := env["data"].(map[string]any)["items"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["address"].(float64) != 0x15 {
		t.Fatalf("items = %v", items)
	}
}

// TestIdentificationEvidenceToolSchemaSignature pins the stable input schema of
// the provider-enabled tool (the generic inventory signature test builds a
// server without this provider).
func TestIdentificationEvidenceToolSchemaSignature(t *testing.T) {
	server := newIdentificationEvidenceServer(t, true)
	res := doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list"})
	tools := res.Result.(map[string]any)["tools"].([]any)
	var schema any
	for _, raw := range tools {
		tool := raw.(map[string]any)
		if tool["name"] == toolBusIdentificationEvidenceListName {
			schema = tool["inputSchema"]
		}
	}
	if schema == nil {
		t.Fatalf("tool missing from tools/list")
	}
	blob, err := json.Marshal(schema)
	if err != nil {
		t.Fatalf("marshal schema: %v", err)
	}
	const wantSchema = `{"additionalProperties":false,"properties":{"limit":{"minimum":1,"type":"integer"}},"type":"object"}`
	if string(blob) != wantSchema {
		t.Fatalf("input schema drift\nwant: %s\ngot:  %s", wantSchema, blob)
	}
	const wantHash = "52e5deee5c167d9fbfa6aad8df5384e5ac1fe7d9ff67fefebe5b120452a47a5f"
	if got := hashData(schema); got != wantHash {
		t.Fatalf("input schema hash = %s, want %s", got, wantHash)
	}
}

func TestIdentificationEvidenceRejectsUnknownArguments(t *testing.T) {
	server := newIdentificationEvidenceServer(t, true)
	for _, args := range []string{`{"unexpected":true}`, `{"limit":1,"unexpected":true}`, `{"Limit":1}`} {
		res := doRPC(t, server.Handler(), rpcRequest{
			JSONRPC: "2.0", ID: 1, Method: "tools/call",
			Params: json.RawMessage(`{"name":"ebus.v1.bus.identification_evidence.list","arguments":` + args + `}`),
		})
		env := envelopeFromResult(t, res)
		errObj, _ := env["error"].(map[string]any)
		if errObj == nil || env["data"] != nil {
			t.Fatalf("args %s accepted: %v", args, env)
		}
		if msg, _ := errObj["message"].(string); !strings.Contains(msg, "unknown argument") {
			t.Fatalf("args %s error message = %q", args, msg)
		}
	}
}
