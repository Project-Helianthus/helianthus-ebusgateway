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

func TestDeviceRetainedOriginGoldenEnvelopes(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	observedAt := time.Date(2026, time.September, 6, 14, 0, 0, 0, time.UTC)

	static := registry.DeviceInfo{
		Address: 0x30, Manufacturer: "Vaillant", DeviceID: "retained-static",
		SerialNumber: "retained-static-30",
	}
	reg.RegisterStaticSeed(static, registry.SlotRoleSlave, observedAt)
	reg.Register(static)

	passive := registry.DeviceInfo{
		Address: 0x31, Manufacturer: "Vaillant", DeviceID: "retained-passive",
		SerialNumber: "retained-passive-31",
	}
	reg.RegisterPassiveObserved(passive, registry.SlotRoleSlave, observedAt.Add(time.Second))
	reg.Register(passive)

	server, err := NewServer(reg, nil)
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}

	for _, tc := range []struct {
		name      string
		tool      string
		arguments string
		golden    string
		address   int
	}{
		{
			name: "static get", tool: toolDeviceGetV1Name,
			arguments: `{"address":48}`, golden: "device_retained_static_confirmed.golden.json", address: 48,
		},
		{
			name: "passive get", tool: toolDeviceGetV1Name,
			arguments: `{"address":49}`, golden: "device_retained_passive_confirmed.golden.json", address: 49,
		},
		{
			name: "combined list", tool: toolDevicesV1Name,
			arguments: `{}`, golden: "device_retained_list_confirmed.golden.json",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first := retainedOriginEnvelope(t, server, tc.tool, tc.arguments)
			second := retainedOriginEnvelope(t, server, tc.tool, tc.arguments)
			firstMeta := first["meta"].(map[string]any)
			secondMeta := second["meta"].(map[string]any)
			if firstMeta["data_hash"] != secondMeta["data_hash"] {
				t.Fatalf("data_hash differs across identical calls: %v != %v", firstMeta["data_hash"], secondMeta["data_hash"])
			}
			var sourceData any
			if tc.address == 0 {
				sourceData = server.listDevices(nil)
			} else {
				sourceData, err = server.getDevice(map[string]any{"address": tc.address}, nil)
				if err != nil {
					t.Fatalf("get source device: %v", err)
				}
			}
			if got, want := firstMeta["data_hash"], hashData(sourceData); got != want {
				t.Fatalf("data_hash = %v; want deterministic hash %s", got, want)
			}

			firstMeta["data_timestamp"] = "2026-09-06T14:00:00Z"
			actual, err := json.Marshal(first)
			if err != nil {
				t.Fatalf("marshal envelope: %v", err)
			}
			path := filepath.Join("testdata", tc.golden)
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
				t.Fatalf("retained-origin envelope golden mismatch\nwant: %s\ngot:  %s", strings.TrimSpace(string(want)), actual)
			}
		})
	}
}

func retainedOriginEnvelope(t *testing.T, server *Server, tool, arguments string) map[string]any {
	t.Helper()
	result := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":` + mustJSON(tool) + `,"arguments":` + arguments + `}`),
	})
	envelope := envelopeFromResult(t, result)
	if envelope["error"] != nil {
		t.Fatalf("envelope error = %#v", envelope["error"])
	}
	meta, ok := envelope["meta"].(map[string]any)
	if !ok {
		t.Fatalf("meta type = %T; want map", envelope["meta"])
	}
	if _, err := time.Parse(time.RFC3339Nano, meta["data_timestamp"].(string)); err != nil {
		t.Fatalf("data_timestamp = %q: %v", meta["data_timestamp"], err)
	}
	return envelope
}
