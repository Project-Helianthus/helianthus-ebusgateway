package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	graphqlgo "github.com/graphql-go/graphql"
)

func executeDeviceProvenanceQuery(t *testing.T, reg *registry.DeviceRegistry, query string) *graphqlgo.Result {
	t.Helper()
	builder := NewBuilder(reg, nil)
	if err := builder.Start(context.Background()); err != nil {
		t.Fatalf("builder.Start() error = %v", err)
	}
	schema, err := NewQuerySchema(builder)
	if err != nil {
		t.Fatalf("NewQuerySchema() error = %v", err)
	}
	result := graphqlgo.Do(graphqlgo.Params{Schema: schema, RequestString: query})
	if len(result.Errors) != 0 {
		t.Fatalf("GraphQL errors = %v", result.Errors)
	}
	return result
}

func queryMCPDeviceTool(t *testing.T, reg *registry.DeviceRegistry, name string, arguments map[string]any) any {
	t.Helper()
	server, err := mcp.NewServer(reg, nil)
	if err != nil {
		t.Fatalf("mcp.NewServer() error = %v", err)
	}
	payload := map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "tools/call",
		"params": map[string]any{"name": name, "arguments": arguments},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(raw))
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("MCP response JSON error = %v: %s", err, recorder.Body.String())
	}
	result := response["result"].(map[string]any)
	content := result["content"].([]any)
	text := content[0].(map[string]any)["text"].(string)
	var envelope map[string]any
	if err := json.Unmarshal([]byte(text), &envelope); err != nil {
		t.Fatalf("MCP envelope JSON error = %v: %s", err, text)
	}
	return envelope["data"]
}

func queryMCPDeviceProvenance(t *testing.T, reg *registry.DeviceRegistry, address int) map[string]any {
	t.Helper()
	data, ok := queryMCPDeviceTool(t, reg, "ebus.v1.registry.devices.get", map[string]any{"address": address}).(map[string]any)
	if !ok {
		t.Fatalf("MCP device get data has unexpected type")
	}
	return data
}

func queryMCPDeviceListProvenance(t *testing.T, reg *registry.DeviceRegistry) []any {
	t.Helper()
	data, ok := queryMCPDeviceTool(t, reg, "ebus.v1.registry.devices.list", map[string]any{}).([]any)
	if !ok {
		t.Fatalf("MCP device list data has unexpected type")
	}
	return data
}

func TestDeviceProvenance_DevicesExposeCandidatePassiveAndConfirmedStates(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	now := time.Date(2026, time.September, 6, 9, 0, 0, 0, time.UTC)
	reg.RegisterStaticSeed(registry.DeviceInfo{Address: 0x04, Manufacturer: "seed", DeviceID: "candidate"}, registry.SlotRoleMaster, now)
	reg.RegisterPassiveObserved(registry.DeviceInfo{Address: 0xF1, Manufacturer: "passive", DeviceID: "observed"}, registry.SlotRoleMaster, now)
	reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "active", DeviceID: "confirmed", SerialNumber: "active-confirmed-15"})

	result := executeDeviceProvenanceQuery(t, reg, `{ devices { address discoverySource verificationState } }`)
	data := result.Data.(map[string]any)
	devices := data["devices"].([]any)
	want := map[int][2]string{
		0x04: {"static_seed", "candidate"},
		0xF1: {"passive_observed", "corroborated_pending"},
		0x15: {"active_confirmed", "identity_confirmed"},
	}
	candidates := 0
	for _, raw := range devices {
		device := raw.(map[string]any)
		address := device["address"].(int)
		labels, ok := want[address]
		if !ok {
			t.Fatalf("unexpected device address = 0x%02X", address)
		}
		if device["discoverySource"] != labels[0] || device["verificationState"] != labels[1] {
			t.Fatalf("device 0x%02X provenance = (%v,%v); want (%s,%s)", address, device["discoverySource"], device["verificationState"], labels[0], labels[1])
		}
		if device["verificationState"] == "candidate" {
			candidates++
		}
	}
	if candidates != 1 {
		t.Fatalf("consumer candidate filter matched %d devices; want 1", candidates)
	}
}

func TestDeviceProvenance_DeviceAddressArgumentSelectsAliasFaceWithoutChangingIdentity(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	now := time.Date(2026, time.September, 6, 9, 0, 0, 0, time.UTC)
	identity := registry.DeviceInfo{Manufacturer: "Vaillant", DeviceID: "same-device", SerialNumber: "serial-1"}
	primary := identity
	primary.Address = 0x03
	reg.RegisterStaticSeed(primary, registry.SlotRoleMaster, now)
	alias := identity
	alias.Address = 0x08
	reg.RegisterPassiveObserved(alias, registry.SlotRoleSlave, now.Add(time.Second))

	result := executeDeviceProvenanceQuery(t, reg, `{ primary: device(address: 3) { address addresses deviceId discoverySource verificationState } alias: device(address: 8) { address addresses deviceId discoverySource verificationState } missing: device(address: 153) { address discoverySource verificationState } }`)
	data := result.Data.(map[string]any)
	primaryResult := data["primary"].(map[string]any)
	aliasResult := data["alias"].(map[string]any)
	for name, device := range map[string]map[string]any{"primary": primaryResult, "alias": aliasResult} {
		if device["address"] != 3 || device["deviceId"] != "same-device" {
			t.Fatalf("%s stable identity = %+v; want canonical address 3 and same-device", name, device)
		}
	}
	if primaryResult["discoverySource"] != "static_seed" || primaryResult["verificationState"] != "candidate" {
		t.Fatalf("primary provenance = %+v; want static_seed/candidate", primaryResult)
	}
	if aliasResult["discoverySource"] != "passive_observed" || aliasResult["verificationState"] != "corroborated_pending" {
		t.Fatalf("alias provenance = %+v; want passive_observed/corroborated_pending", aliasResult)
	}
	if data["missing"] != nil {
		t.Fatalf("missing arbitrary address = %+v; want null", data["missing"])
	}
}

func TestDeviceProvenance_AddressZeroParityWithMCP(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	now := time.Date(2026, time.September, 6, 9, 0, 0, 0, time.UTC)
	identity := registry.DeviceInfo{Manufacturer: "Vaillant", DeviceID: "same-device", SerialNumber: "serial-zero"}
	primary := identity
	primary.Address = 0x15
	reg.Register(primary)
	zeroAlias := identity
	zeroAlias.Address = 0x00
	reg.RegisterStaticSeed(zeroAlias, registry.SlotRoleMaster, now)

	graphqlResult := executeDeviceProvenanceQuery(t, reg, `{ device(address: 0) { address discoverySource verificationState } }`)
	graphqlDevice := graphqlResult.Data.(map[string]any)["device"].(map[string]any)
	mcpDevice := queryMCPDeviceProvenance(t, reg, 0)

	if graphqlDevice["address"].(int) != int(mcpDevice["address"].(float64)) {
		t.Fatalf("canonical address parity: GraphQL=%v MCP=%v", graphqlDevice["address"], mcpDevice["address"])
	}
	if graphqlDevice["discoverySource"] != mcpDevice["discovery_source"] || graphqlDevice["verificationState"] != mcpDevice["verification_state"] {
		t.Fatalf("zero-alias provenance parity: GraphQL=(%v,%v) MCP=(%v,%v)", graphqlDevice["discoverySource"], graphqlDevice["verificationState"], mcpDevice["discovery_source"], mcpDevice["verification_state"])
	}
	if graphqlDevice["discoverySource"] != "static_seed" || graphqlDevice["verificationState"] != "candidate" {
		t.Fatalf("zero-alias provenance = %+v; want static_seed/candidate", graphqlDevice)
	}
}

func TestDeviceProvenance_CrossSurfaceParityForListGetAliasesAndAllStates(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	now := time.Date(2026, time.September, 6, 9, 0, 0, 0, time.UTC)
	reg.RegisterStaticSeed(registry.DeviceInfo{Address: 0x04, Manufacturer: "seed", DeviceID: "candidate"}, registry.SlotRoleMaster, now)
	reg.RegisterPassiveObserved(registry.DeviceInfo{Address: 0xF1, Manufacturer: "passive", DeviceID: "observed"}, registry.SlotRoleMaster, now)
	reg.Register(registry.DeviceInfo{Address: 0x15, Manufacturer: "active", DeviceID: "confirmed", SerialNumber: "active-confirmed-15"})

	aliasIdentity := registry.DeviceInfo{Manufacturer: "Vaillant", DeviceID: "alias-device", SerialNumber: "alias-parity"}
	aliasPrimary := aliasIdentity
	aliasPrimary.Address = 0x21
	reg.Register(aliasPrimary)
	zeroAlias := aliasIdentity
	zeroAlias.Address = 0x00
	reg.RegisterStaticSeed(zeroAlias, registry.SlotRoleMaster, now.Add(time.Second))

	want := map[int][2]string{
		0x04: {"static_seed", "candidate"},
		0xF1: {"passive_observed", "corroborated_pending"},
		0x15: {"active_confirmed", "identity_confirmed"},
		0x21: {"active_confirmed", "identity_confirmed"},
	}
	graphqlList := executeDeviceProvenanceQuery(t, reg, `{ devices { address discoverySource verificationState } }`).Data.(map[string]any)["devices"].([]any)
	mcpList := queryMCPDeviceListProvenance(t, reg)
	mcpByAddress := make(map[int]map[string]any, len(mcpList))
	for _, raw := range mcpList {
		device := raw.(map[string]any)
		mcpByAddress[int(device["address"].(float64))] = device
	}
	if len(graphqlList) != len(mcpList) {
		t.Fatalf("device list length parity: GraphQL=%d MCP=%d", len(graphqlList), len(mcpList))
	}
	for _, raw := range graphqlList {
		graphqlDevice := raw.(map[string]any)
		address := graphqlDevice["address"].(int)
		expected, ok := want[address]
		if !ok {
			t.Fatalf("unexpected GraphQL list address = 0x%02X", address)
		}
		mcpDevice, ok := mcpByAddress[address]
		if !ok {
			t.Fatalf("MCP list missing GraphQL address 0x%02X", address)
		}
		if graphqlDevice["discoverySource"] != mcpDevice["discovery_source"] || graphqlDevice["verificationState"] != mcpDevice["verification_state"] {
			t.Fatalf("list provenance parity at 0x%02X: GraphQL=(%v,%v) MCP=(%v,%v)", address, graphqlDevice["discoverySource"], graphqlDevice["verificationState"], mcpDevice["discovery_source"], mcpDevice["verification_state"])
		}
		if graphqlDevice["discoverySource"] != expected[0] || graphqlDevice["verificationState"] != expected[1] {
			t.Fatalf("list provenance at 0x%02X = (%v,%v); want (%s,%s)", address, graphqlDevice["discoverySource"], graphqlDevice["verificationState"], expected[0], expected[1])
		}
	}

	getCases := []struct {
		address   int
		canonical int
		want      [2]string
	}{
		{address: 0x04, canonical: 0x04, want: want[0x04]},
		{address: 0xF1, canonical: 0xF1, want: want[0xF1]},
		{address: 0x15, canonical: 0x15, want: want[0x15]},
		{address: 0x21, canonical: 0x21, want: want[0x21]},
		{address: 0x00, canonical: 0x21, want: [2]string{"static_seed", "candidate"}},
	}
	for _, tc := range getCases {
		query := fmt.Sprintf(`{ device(address: %d) { address discoverySource verificationState } }`, tc.address)
		graphqlDevice := executeDeviceProvenanceQuery(t, reg, query).Data.(map[string]any)["device"].(map[string]any)
		mcpDevice := queryMCPDeviceProvenance(t, reg, tc.address)
		if graphqlDevice["address"] != tc.canonical || int(mcpDevice["address"].(float64)) != tc.canonical {
			t.Fatalf("get canonical address parity for 0x%02X: GraphQL=%v MCP=%v want=%d", tc.address, graphqlDevice["address"], mcpDevice["address"], tc.canonical)
		}
		if graphqlDevice["discoverySource"] != mcpDevice["discovery_source"] || graphqlDevice["verificationState"] != mcpDevice["verification_state"] {
			t.Fatalf("get provenance parity for 0x%02X: GraphQL=(%v,%v) MCP=(%v,%v)", tc.address, graphqlDevice["discoverySource"], graphqlDevice["verificationState"], mcpDevice["discovery_source"], mcpDevice["verification_state"])
		}
		if graphqlDevice["discoverySource"] != tc.want[0] || graphqlDevice["verificationState"] != tc.want[1] {
			t.Fatalf("get provenance for 0x%02X = (%v,%v); want (%s,%s)", tc.address, graphqlDevice["discoverySource"], graphqlDevice["verificationState"], tc.want[0], tc.want[1])
		}
	}
}

func TestDeviceProvenance_RetainsOriginalSourceAfterIdentityConfirmation(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		address    byte
		admit      func(*registry.DeviceRegistry, registry.DeviceInfo, time.Time)
		wantSource string
	}{
		{
			name: "static seed", address: 0x30, wantSource: "static_seed",
			admit: func(reg *registry.DeviceRegistry, info registry.DeviceInfo, observedAt time.Time) {
				reg.RegisterStaticSeed(info, registry.SlotRoleSlave, observedAt)
			},
		},
		{
			name: "passive observation", address: 0x31, wantSource: "passive_observed",
			admit: func(reg *registry.DeviceRegistry, info registry.DeviceInfo, observedAt time.Time) {
				reg.RegisterPassiveObserved(info, registry.SlotRoleSlave, observedAt)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := registry.NewDeviceRegistry(nil)
			info := registry.DeviceInfo{
				Address: tc.address, Manufacturer: "Vaillant", DeviceID: "source-retention",
				SerialNumber: fmt.Sprintf("source-retention-%02x", tc.address),
			}
			tc.admit(reg, info, time.Date(2026, time.September, 6, 14, 0, 0, 0, time.UTC))
			reg.Register(info)

			query := fmt.Sprintf(`{ device(address: %d) { address discoverySource verificationState } }`, tc.address)
			graphqlDevice := executeDeviceProvenanceQuery(t, reg, query).Data.(map[string]any)["device"].(map[string]any)
			mcpDevice := queryMCPDeviceProvenance(t, reg, int(tc.address))

			if graphqlDevice["discoverySource"] != tc.wantSource || graphqlDevice["verificationState"] != "identity_confirmed" {
				t.Fatalf("GraphQL provenance = (%v,%v); want (%s,identity_confirmed)", graphqlDevice["discoverySource"], graphqlDevice["verificationState"], tc.wantSource)
			}
			if mcpDevice["discovery_source"] != tc.wantSource || mcpDevice["verification_state"] != "identity_confirmed" {
				t.Fatalf("MCP provenance = (%v,%v); want (%s,identity_confirmed)", mcpDevice["discovery_source"], mcpDevice["verification_state"], tc.wantSource)
			}
		})
	}
}

func TestDeviceProvenance_SlotlessEntryReturnsNullableAbsence(t *testing.T) {
	reg := mockRegistry{entries: []registry.DeviceEntry{mockEntry{
		info: registry.DeviceInfo{
			Address:         0x10,
			Manufacturer:    "fixture",
			DeviceID:        "slotless",
			SoftwareVersion: "1.0",
			HardwareVersion: "1.0",
		},
	}}}
	builder := NewBuilder(reg, nil)
	if err := builder.Start(context.Background()); err != nil {
		t.Fatalf("builder.Start() error = %v", err)
	}
	schema, err := NewQuerySchema(builder)
	if err != nil {
		t.Fatalf("NewQuerySchema() error = %v", err)
	}
	result := graphqlgo.Do(graphqlgo.Params{
		Schema:        schema,
		RequestString: `{ devices { address discoverySource verificationState } device(address: 16) { address discoverySource verificationState } }`,
	})
	if len(result.Errors) != 0 {
		t.Fatalf("GraphQL errors = %v", result.Errors)
	}
	data := result.Data.(map[string]any)
	listDevice := data["devices"].([]any)[0].(map[string]any)
	selectedDevice := data["device"].(map[string]any)
	for name, device := range map[string]map[string]any{"devices": listDevice, "device": selectedDevice} {
		if device["address"] != 16 {
			t.Fatalf("%s address = %v; want 16", name, device["address"])
		}
		if device["discoverySource"] != nil || device["verificationState"] != nil {
			t.Fatalf("%s slotless provenance = (%v,%v); want (null,null)", name, device["discoverySource"], device["verificationState"])
		}
	}
}
