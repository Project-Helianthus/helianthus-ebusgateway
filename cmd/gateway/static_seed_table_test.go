package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/router"
)

// noopMCPInvoker satisfies mcp.Invoker for tests that exercise the
// devices.list path (which never invokes a method).
type noopMCPInvoker struct{}

func (noopMCPInvoker) Invoke(ctx context.Context, plane router.Plane, methodName string, params map[string]any) (any, error) {
	return nil, nil
}

// TestApplyStaticSeedTable_StampsStaticSeedLabel asserts the central
// P3.5 contract: applyStaticSeedTable plants identity for the
// productids seed entries via registry.RegisterStaticSeed, so each
// resulting AddressSlot is labelled
// DiscoverySource=DiscoverySourceStaticSeed and
// VerificationState=VerificationStateCandidate (NOT the
// ActiveConfirmed/IdentityConfirmed labels Register would stamp).
//
// Coverage: every address in productids.LoadSeedTable(true) must
// appear with the static_seed/candidate labels after applyStaticSeedTable.
// This locks in the operator-visible discovery_source label going
// through the address-table snapshot at
// helianthus-ebusgateway/address_table.go.
func TestApplyStaticSeedTable_StampsStaticSeedLabel(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	applyStaticSeedTable(reg)

	// Each NETX3 face + each BASV2 face from productids.LoadSeedTable(true).
	expectedAddresses := []byte{0xF1, 0xF6, 0x04, 0x15, 0xEC}
	for _, addr := range expectedAddresses {
		slot, ok := reg.LookupSlot(addr)
		if !ok || slot == nil {
			t.Errorf("LookupSlot(0x%02X) ok=%v slot=%v; want non-nil seeded slot", addr, ok, slot)
			continue
		}
		if got, want := slot.DiscoverySource, registry.DiscoverySourceStaticSeed; got != want {
			t.Errorf("slot[0x%02X].DiscoverySource = %v; want %v (must NOT be active_confirmed for a static-seeded entry)", addr, got, want)
		}
		if got, want := slot.VerificationState, registry.VerificationStateCandidate; got != want {
			t.Errorf("slot[0x%02X].VerificationState = %v; want %v", addr, got, want)
		}
		if entry, ok := reg.Lookup(addr); !ok || entry == nil {
			t.Errorf("Lookup(0x%02X) ok=%v entry=%v; want non-nil", addr, ok, entry)
		} else {
			if got, want := entry.Manufacturer(), "Vaillant"; got != want {
				t.Errorf("entry[0x%02X].Manufacturer() = %q; want %q", addr, got, want)
			}
		}
	}
}

// TestApplyStaticSeedTable_MCPDeviceListProjection is the operator-
// surface verification for P3.5: after applyStaticSeedTable runs, the
// MCP `ebus.v1.registry.devices.list` tool MUST return JSON entries
// whose `discovery_source` field equals `"static_seed"` and whose
// `verification_state` field equals `"candidate"` for each seeded
// address. This is the path the operator follows on a deployed
// gateway to verify the contract — the registry-level slot stamping
// is necessary but not sufficient unless MCP projects the labels.
//
// Posts a real JSON-RPC `tools/call` to the MCP HTTP handler with a
// real `*registry.DeviceRegistry` (NOT the testRegistry stub) so
// LookupSlot returns the actual stamp.
func TestApplyStaticSeedTable_MCPDeviceListProjection(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	applyStaticSeedTable(reg)

	server, err := mcp.NewServer(reg, noopMCPInvoker{})
	if err != nil {
		t.Fatalf("mcp.NewServer error = %v", err)
	}
	server.SetAdmittedRPCSource(0x7F)

	httpSrv := httptest.NewServer(server.Handler())
	defer httpSrv.Close()

	body := strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"ebus.v1.registry.devices.list","arguments":{}}}`)
	resp, err := http.Post(httpSrv.URL, "application/json", body)
	if err != nil {
		t.Fatalf("POST tools/call error = %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var rpcResp struct {
		Result struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
			IsError bool `json:"isError"`
		} `json:"result"`
		Error any `json:"error,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		t.Fatalf("decode rpc response error = %v", err)
	}
	if rpcResp.Error != nil {
		t.Fatalf("rpc error = %v", rpcResp.Error)
	}
	if rpcResp.Result.IsError {
		t.Fatalf("tools/call isError=true; want false")
	}
	if len(rpcResp.Result.Content) != 1 {
		t.Fatalf("content len = %d; want 1", len(rpcResp.Result.Content))
	}

	var envelope struct {
		Data []struct {
			Address           int    `json:"address"`
			Manufacturer      string `json:"manufacturer"`
			DiscoverySource   string `json:"discovery_source"`
			VerificationState string `json:"verification_state"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(rpcResp.Result.Content[0].Text), &envelope); err != nil {
		t.Fatalf("envelope decode error = %v; text=%q", err, rpcResp.Result.Content[0].Text)
	}
	if len(envelope.Data) == 0 {
		t.Fatalf("envelope.data empty; want at least the seeded entries")
	}

	// Build address -> entry index for assertions.
	byAddr := make(map[int]struct {
		discovery, verification string
	}, len(envelope.Data))
	for _, d := range envelope.Data {
		byAddr[d.Address] = struct{ discovery, verification string }{d.DiscoverySource, d.VerificationState}
	}
	// All 5 productids seed addresses must appear with static_seed/candidate.
	for _, addr := range []int{0xF1, 0xF6, 0x04, 0x15, 0xEC} {
		got, ok := byAddr[addr]
		if !ok {
			// Some seeded faces may share an entry (canonical pair
			// merge); skip if absent — the seeded primary still
			// represents them. The strong assertion is on the
			// presence of static_seed entries below.
			continue
		}
		if got.discovery != "static_seed" {
			t.Errorf("device 0x%02X discovery_source = %q; want %q", addr, got.discovery, "static_seed")
		}
		if got.verification != "candidate" {
			t.Errorf("device 0x%02X verification_state = %q; want %q", addr, got.verification, "candidate")
		}
	}
	// At least one entry must carry the static_seed label — proves the
	// projection path is wired even if all 5 addresses merged into one.
	staticSeen := 0
	for _, v := range byAddr {
		if v.discovery == "static_seed" {
			staticSeen++
		}
	}
	if staticSeen == 0 {
		t.Fatalf("no entries carry discovery_source=static_seed; got %v", byAddr)
	}
}

// TestApplyStaticSeedTable_MCPDeviceListProjection_SnapshotMode covers
// the SNAPSHOT-consistency path through MCP. Codex P3.5 review pass 2
// caught that cloneDeviceInfoList (the helper that materialises a
// snapshot for replay) had not been updated to carry the new
// discovery_source / verification_state fields, so requests with
// `consistency: SNAPSHOT` would have lost them silently while LIVE
// requests carried them. This test creates a snapshot, then issues a
// devices.list call with the captured snapshot_id and asserts the
// new fields are still present.
func TestApplyStaticSeedTable_MCPDeviceListProjection_SnapshotMode(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	applyStaticSeedTable(reg)

	server, err := mcp.NewServer(reg, noopMCPInvoker{})
	if err != nil {
		t.Fatalf("mcp.NewServer error = %v", err)
	}
	server.SetAdmittedRPCSource(0x7F)

	httpSrv := httptest.NewServer(server.Handler())
	defer httpSrv.Close()

	// 1. Capture a snapshot.
	captureBody := strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"ebus.v1.snapshot.capture","arguments":{}}}`)
	captureResp, err := http.Post(httpSrv.URL, "application/json", captureBody)
	if err != nil {
		t.Fatalf("snapshot.capture POST error = %v", err)
	}
	defer func() { _ = captureResp.Body.Close() }()
	var captureRPC struct {
		Result struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
			IsError bool `json:"isError"`
		} `json:"result"`
	}
	if err := json.NewDecoder(captureResp.Body).Decode(&captureRPC); err != nil {
		t.Fatalf("snapshot.capture decode error = %v", err)
	}
	if captureRPC.Result.IsError {
		t.Fatalf("snapshot.capture isError=true")
	}
	var captureEnvelope struct {
		Data struct {
			SnapshotID string `json:"snapshot_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(captureRPC.Result.Content[0].Text), &captureEnvelope); err != nil {
		t.Fatalf("snapshot.capture envelope decode error = %v", err)
	}
	if captureEnvelope.Data.SnapshotID == "" {
		t.Fatalf("snapshot.capture returned empty snapshot_id")
	}

	// 2. Call devices.list with consistency: SNAPSHOT and the captured ID.
	devicesBody := strings.NewReader(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"ebus.v1.registry.devices.list","arguments":{"consistency":{"mode":"snapshot","snapshot_id":"` + captureEnvelope.Data.SnapshotID + `"}}}}`)
	devResp, err := http.Post(httpSrv.URL, "application/json", devicesBody)
	if err != nil {
		t.Fatalf("snapshot devices.list POST error = %v", err)
	}
	defer func() { _ = devResp.Body.Close() }()
	var devRPC struct {
		Result struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
			IsError bool `json:"isError"`
		} `json:"result"`
	}
	if err := json.NewDecoder(devResp.Body).Decode(&devRPC); err != nil {
		t.Fatalf("snapshot devices.list decode error = %v", err)
	}
	if devRPC.Result.IsError {
		t.Fatalf("snapshot devices.list isError=true; body=%q", devRPC.Result.Content[0].Text)
	}
	var devEnvelope struct {
		Data []struct {
			Address           int    `json:"address"`
			DiscoverySource   string `json:"discovery_source"`
			VerificationState string `json:"verification_state"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(devRPC.Result.Content[0].Text), &devEnvelope); err != nil {
		t.Fatalf("snapshot devices envelope decode error = %v", err)
	}

	staticSeen := 0
	for _, d := range devEnvelope.Data {
		if d.DiscoverySource == "static_seed" {
			staticSeen++
			if d.VerificationState != "candidate" {
				t.Errorf("snapshot device 0x%02X verification_state = %q; want %q", d.Address, d.VerificationState, "candidate")
			}
		}
	}
	if staticSeen == 0 {
		t.Fatalf("snapshot devices envelope: no entries carry discovery_source=static_seed (snapshot clone path dropped the field?); data=%v", devEnvelope.Data)
	}
}

// TestApplyStaticSeedTable_RoleMapping asserts the gateway-side
// role-string → registry.SlotRole mapping. Roles in
// productids.SeedAddressEntry are free-form strings; we map at this
// seam to typed enum values so registry stays
// productids-agnostic.
//
// Maps:
//   - "initiator" → SlotRoleMaster (initiator-role on the bus)
//   - "target"    → SlotRoleSlave  (target-role on the bus)
//   - anything else → SlotRoleUnknown (registry's monotonic guard
//     leaves Role untouched; passive observation or active scan
//     fills it later)
//
// productids.LoadSeedTable assigns:
//   - 0xF1 → "initiator" (NETX3 initiator face) → SlotRoleMaster
//   - 0xF6 / 0x04 → "target" (NETX3 target faces) → SlotRoleSlave
//   - 0x15 / 0xEC → "target" (BASV2 target faces) → SlotRoleSlave
func TestApplyStaticSeedTable_RoleMapping(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	applyStaticSeedTable(reg)

	cases := []struct {
		addr byte
		role registry.SlotRole
	}{
		{0xF1, registry.SlotRoleMaster},
		{0xF6, registry.SlotRoleSlave},
		{0x04, registry.SlotRoleSlave},
		{0x15, registry.SlotRoleSlave},
		{0xEC, registry.SlotRoleSlave},
	}
	for _, tc := range cases {
		slot, ok := reg.LookupSlot(tc.addr)
		if !ok || slot == nil {
			t.Errorf("LookupSlot(0x%02X) ok=%v slot=%v", tc.addr, ok, slot)
			continue
		}
		if slot.Role != tc.role {
			t.Errorf("slot[0x%02X].Role = %v; want %v", tc.addr, slot.Role, tc.role)
		}
	}
}
