package mcp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

// TestSemanticSystemDHWBivalenceBoundGoldenEnvelope pins the
// ebus.v1.semantic.system.get output (including data_hash) for the two shapes
// of the active dhwBivalencePointC bound: a matched controller profile and the
// conservative unmatched policy. Regenerate with
// TZ=UTC UPDATE=1 go test ./mcp -run TestSemanticSystemDHWBivalenceBoundGoldenEnvelope.
func TestSemanticSystemDHWBivalenceBoundGoldenEnvelope(t *testing.T) {
	value := 8.0
	cases := []struct {
		name  string
		bound *ConfigBound
	}{
		{"profile", &ConfigBound{MinC: -20, MaxC: 20, Source: "controller_profile", NarrowedOnly: true, Profile: "BASV0/SW0217/HW8803/API1.1"}},
		{"unmatched", &ConfigBound{MinC: -20, MaxC: 50, Source: "unmatched_policy", Reason: "unknown_profile"}},
	}
	out := map[string]any{}
	for _, tc := range cases {
		server, err := NewServer(registry.NewDeviceRegistry(nil), &testInvoker{})
		if err != nil {
			t.Fatalf("NewServer error = %v", err)
		}
		server.SetSemanticProvider(testSemanticProvider{system: &SystemStatus{
			Config: &SystemConfig{DhwBivalencePoint: &value, DhwBivalenceBound: tc.bound},
		}})
		call := func() map[string]any {
			return envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
				JSONRPC: "2.0",
				ID:      1,
				Method:  "tools/call",
				Params:  json.RawMessage(`{"name":"ebus.v1.semantic.system.get","arguments":{}}`),
			}))
		}
		first, second := call(), call()
		firstMeta := first["meta"].(map[string]any)
		if firstMeta["data_hash"] != second["meta"].(map[string]any)["data_hash"] {
			t.Fatalf("%s: data_hash differs across identical calls", tc.name)
		}
		firstMeta["data_timestamp"] = "2026-10-14T09:00:00Z"
		out[tc.name] = first
	}
	actual, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal envelopes: %v", err)
	}
	path := filepath.Join("testdata", "semantic_system_dhw_bivalence_bound.golden.json")
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
		t.Fatalf("system dhw bivalence bound envelope golden mismatch\nwant: %s\ngot:  %s", strings.TrimSpace(string(want)), actual)
	}
}

// TestSemanticSnapshotDHWBivalenceBoundGoldenEnvelope pins both explicit
// system selection and the default planes, including the deterministic hash.
// Regenerate only this golden with TZ=UTC UPDATE=1 go test ./mcp
// -run '^TestSemanticSnapshotDHWBivalenceBoundGoldenEnvelope$'.
func TestSemanticSnapshotDHWBivalenceBoundGoldenEnvelope(t *testing.T) {
	value := 8.0
	cases := []struct {
		name  string
		bound *ConfigBound
	}{
		{"profile", &ConfigBound{MinC: -20, MaxC: 20, Source: "controller_profile", NarrowedOnly: true, Profile: "BASV0/SW0217/HW8803/API1.1"}},
		{"unmatched", &ConfigBound{MinC: -20, MaxC: 50, Source: "unmatched_policy", Reason: "unknown_profile"}},
	}
	out := map[string]any{}
	for _, tc := range cases {
		server, err := NewServer(registry.NewDeviceRegistry(nil), &testInvoker{})
		if err != nil {
			t.Fatalf("NewServer error = %v", err)
		}
		server.SetSemanticProvider(testSemanticProvider{system: &SystemStatus{
			Config: &SystemConfig{DhwBivalencePoint: &value, DhwBivalenceBound: tc.bound},
		}})
		for _, selection := range []struct{ name, arguments string }{
			{"system", `{"planes":["system"]}`},
			{"default", `{}`},
		} {
			call := func() map[string]any {
				return envelopeFromResult(t, doRPC(t, server.Handler(), rpcRequest{
					JSONRPC: "2.0",
					ID:      1,
					Method:  "tools/call",
					Params:  json.RawMessage(`{"name":"ebus.v1.semantic.snapshot.get","arguments":` + selection.arguments + `}`),
				}))
			}
			first, second := call(), call()
			firstMeta := first["meta"].(map[string]any)
			if firstMeta["data_hash"] != second["meta"].(map[string]any)["data_hash"] {
				t.Fatalf("%s: data_hash differs across identical calls", tc.name)
			}
			firstMeta["data_timestamp"] = "2026-10-14T09:00:00Z"
			out[tc.name+"/"+selection.name] = first
		}
	}
	actual, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal envelopes: %v", err)
	}
	path := filepath.Join("testdata", "semantic_snapshot_dhw_bivalence_bound.golden.json")
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
		t.Fatalf("snapshot dhw bivalence bound envelope golden mismatch\nwant: %s\ngot:  %s", strings.TrimSpace(string(want)), actual)
	}
}
