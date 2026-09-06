package mcp

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

func TestDeviceGetZeroAliasGoldenEnvelope(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	identity := registry.DeviceInfo{Manufacturer: "Vaillant", DeviceID: "alias-device", SerialNumber: "zero-alias-golden"}
	primary := identity
	primary.Address = 0x15
	reg.Register(primary)
	alias := identity
	alias.Address = 0x00
	reg.RegisterStaticSeed(alias, registry.SlotRoleMaster, time.Date(2026, time.September, 6, 9, 0, 0, 0, time.UTC))

	server, err := NewServer(reg, nil)
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	result := doRPC(t, server.Handler(), rpcRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name":"ebus.v1.registry.devices.get","arguments":{"address":0}}`),
	})
	envelope := envelopeFromResult(t, result)
	meta := envelope["meta"].(map[string]any)
	timestamp := meta["data_timestamp"].(string)
	if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil {
		t.Fatalf("data_timestamp = %q: %v", timestamp, err)
	}
	meta["data_timestamp"] = "2026-09-06T09:00:00Z"

	actual, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	want, err := os.ReadFile("testdata/device_zero_alias.golden.json")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if string(actual) != strings.TrimSpace(string(want)) {
		t.Fatalf("zero-alias envelope golden mismatch\nwant: %s\ngot:  %s", strings.TrimSpace(string(want)), actual)
	}
}
