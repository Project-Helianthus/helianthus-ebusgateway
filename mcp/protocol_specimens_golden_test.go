package mcp

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type protocolSpecimensGoldenProvider struct {
	testBusObservabilityProvider
}

func (*protocolSpecimensGoldenProvider) ProtocolSpecimens(family string) []BusProtocolSpecimen {
	if family != "" && family != "B510" {
		return nil
	}
	// Invented fixture values exercise serialization without retaining captures
	// or asserting protocol meaning for the synthetic bytes.
	return []BusProtocolSpecimen{{
		Family: "B510", Source: "synthetic-source", Target: "synthetic-target",
		FrameType: "synthetic", RequestHex: "00", ResponseHex: "01",
		RequestLen: 1, ResponseLen: 1, Outcome: "Unknown",
		FirstSeenAt: "2026-10-15T00:00:00Z", LastSeenAt: "2026-10-15T00:00:00Z", Count: 2,
	}}
}

func TestProtocolSpecimensStableGoldens(t *testing.T) {
	server, err := NewServer(registry.NewDeviceRegistry(nil), &testInvoker{})
	if err != nil {
		t.Fatalf("NewServer error = %v", err)
	}
	server.SetBusObservabilityProvider(&protocolSpecimensGoldenProvider{})
	res := doRPC(t, server.Handler(), rpcRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list"})
	if res.Error != nil {
		t.Fatalf("tools/list error = %+v", res.Error)
	}
	var schema any
	for _, raw := range res.Result.(map[string]any)["tools"].([]any) {
		tool := raw.(map[string]any)
		if tool["name"] == toolBusProtocolSpecimensListName {
			schema = tool["inputSchema"]
		}
	}
	if schema == nil {
		t.Fatal("provider-enabled specimen tool missing from tools/list")
	}
	compareRPCInvokeGolden(t, "bus_protocol_specimens_schema.golden.json", schema)

	for _, tc := range []struct {
		name string
		args string
	}{
		{"populated", `{}`},
		{"filtered", `{"family":"B510","limit":1}`},
		{"empty", `{"family":"synthetic-absent-family"}`},
		{"invalid_family", `{"family":123}`},
		{"invalid_limit", `{"limit":0}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			call := func() map[string]any {
				res := doRPC(t, server.Handler(), rpcRequest{
					JSONRPC: "2.0", ID: 2, Method: "tools/call",
					Params: json.RawMessage(`{"name":"` + toolBusProtocolSpecimensListName + `","arguments":` + tc.args + `}`),
				})
				envelope := envelopeFromResult(t, res)
				// Only the wall-clock timestamp varies across identical calls.
				// Keep the computed data hash and every other envelope field.
				meta := envelope["meta"].(map[string]any)
				if _, ok := meta["data_timestamp"].(string); !ok {
					t.Fatal("missing data_timestamp")
				}
				meta["data_timestamp"] = "2026-10-15T00:00:00Z"
				return map[string]any{
					"is_error": res.Result.(map[string]any)["isError"],
					"envelope": envelope,
				}
			}
			first, second := call(), call()
			if !reflect.DeepEqual(first, second) {
				t.Fatal("identical specimen calls produced different envelopes")
			}
			compareRPCInvokeGolden(t, "bus_protocol_specimens_"+tc.name+".golden.json", first)
		})
	}
}
