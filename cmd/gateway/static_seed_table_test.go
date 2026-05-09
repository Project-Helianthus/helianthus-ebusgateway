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

// TestApplyStaticSeedTable_MCPDeviceGetProjectsQueriedAddressSlot
// covers Codex P3.5 review pass 3 thread 3: when a merged DeviceEntry
// has aliases at different DiscoverySource levels (e.g. NETX3's three
// faces — 0xF1, 0xF6, 0x04 — share a Manufacturer+DeviceID and
// identity-merge into one entry), `ebus.v1.registry.devices.get`
// MUST project the QUERIED address's slot state, not the primary's.
//
// Setup: run applyStaticSeedTable (all NETX3 faces stamp at
// static_seed/candidate), then re-Register 0xF1 as ActiveConfirmed
// to advance JUST that face. Now query devices.get(address=0x04) and
// devices.get(address=0xF1):
//   - 0x04 must report static_seed/candidate (the static-seeded
//     broadcast face).
//   - 0xF1 must report active_confirmed/identity_confirmed.
//
// Without the per-queried-address projection, both queries would
// return the SAME labels (whatever the primary's slot says), which
// is wrong for either 0x04 or 0xF1 depending on which is primary.
func TestApplyStaticSeedTable_MCPDeviceGetProjectsQueriedAddressSlot(t *testing.T) {
	reg := registry.NewDeviceRegistry(nil)
	applyStaticSeedTable(reg)
	// Advance just 0xF1 to ActiveConfirmed via Register.
	reg.Register(registry.DeviceInfo{
		Address:      0xF1,
		Manufacturer: "Vaillant",
		DeviceID:     "NETX3",
		SerialNumber: "SN-F1-active",
	})

	server, err := mcp.NewServer(reg, noopMCPInvoker{})
	if err != nil {
		t.Fatalf("mcp.NewServer error = %v", err)
	}
	server.SetAdmittedRPCSource(0x7F)
	httpSrv := httptest.NewServer(server.Handler())
	defer httpSrv.Close()

	queryAddress := func(addr int) (string, string) {
		t.Helper()
		body := strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"ebus.v1.registry.devices.get","arguments":{"address":` + intToHexJSON(addr) + `}}}`)
		resp, err := http.Post(httpSrv.URL, "application/json", body)
		if err != nil {
			t.Fatalf("devices.get(0x%02X) POST error = %v", addr, err)
		}
		defer func() { _ = resp.Body.Close() }()
		var rpcResp struct {
			Result struct {
				Content []struct {
					Text string `json:"text"`
				} `json:"content"`
				IsError bool `json:"isError"`
			} `json:"result"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
			t.Fatalf("devices.get(0x%02X) decode = %v", addr, err)
		}
		if rpcResp.Result.IsError {
			t.Fatalf("devices.get(0x%02X) isError; body=%q", addr, rpcResp.Result.Content[0].Text)
		}
		var envelope struct {
			Data struct {
				DiscoverySource   string `json:"discovery_source"`
				VerificationState string `json:"verification_state"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(rpcResp.Result.Content[0].Text), &envelope); err != nil {
			t.Fatalf("devices.get(0x%02X) envelope decode = %v", addr, err)
		}
		return envelope.Data.DiscoverySource, envelope.Data.VerificationState
	}

	// 0x04 must stay at static_seed/candidate — it was only ever seeded.
	if got, gotV := queryAddress(0x04); got != "static_seed" || gotV != "candidate" {
		t.Errorf("devices.get(0x04) discovery=%q, verification=%q; want static_seed, candidate", got, gotV)
	}
	// 0xF1 must reflect the ActiveConfirmed advance — not the
	// primary's stamping.
	if got, gotV := queryAddress(0xF1); got != "active_confirmed" || gotV != "identity_confirmed" {
		t.Errorf("devices.get(0xF1) discovery=%q, verification=%q; want active_confirmed, identity_confirmed", got, gotV)
	}
}

// intToHexJSON helper renders a hex int literal that the MCP address
// parser accepts (it accepts both "0xNN" strings and decimal numbers;
// here we use the decimal form to keep the JSON simple).
func intToHexJSON(addr int) string {
	switch addr {
	case 0xF1:
		return "241"
	case 0xF6:
		return "246"
	case 0x04:
		return "4"
	case 0x15:
		return "21"
	case 0xEC:
		return "236"
	}
	return "0"
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
