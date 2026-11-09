package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
)

type gatewayInventoryRow struct {
	Name    string `json:"name"`
	Surface string `json:"surface"`
	Profile string `json:"registration_profile"`
}

func gatewayInventoryProfiles(t *testing.T) map[string][]string {
	t.Helper()
	raw, err := os.ReadFile("../../docs/mcp-tool-inventory.md")
	if err != nil {
		t.Fatal(err)
	}
	start := bytes.Index(raw, []byte("```json\n"))
	if start < 0 {
		t.Fatal("missing inventory")
	}
	raw = raw[start+8:]
	end := bytes.Index(raw, []byte("\n```"))
	if end < 0 {
		t.Fatal("unterminated inventory")
	}
	var doc struct {
		SchemaVersion int                   `json:"schema_version"`
		Rows          []gatewayInventoryRow `json:"tools"`
	}
	if err := json.Unmarshal(raw[:end], &doc); err != nil {
		t.Fatal(err)
	}
	if doc.SchemaVersion != 1 {
		t.Fatal("unsupported schema")
	}
	profiles := map[string][]string{}
	for _, row := range doc.Rows {
		if row.Surface == "ordinary" {
			profiles[row.Profile] = append(profiles[row.Profile], row.Name)
		}
	}
	for _, names := range profiles {
		sort.Strings(names)
	}
	return profiles
}

func gatewayInventoryNames(t *testing.T, s *mcp.Server) []string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	var result struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
		Error any `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || result.Error != nil {
		t.Fatal("tools/list failed")
	}
	names := []string{}
	seen := map[string]bool{}
	for _, tool := range result.Result.Tools {
		if seen[tool.Name] {
			t.Fatal("ambiguous duplicate", tool.Name)
		}
		seen[tool.Name] = true
		names = append(names, tool.Name)
	}
	sort.Strings(names)
	if len(names) == 0 {
		t.Fatal("empty tools/list")
	}
	return names
}

func gatewayInventoryEqual(got, want []string) error {
	if len(want) == 0 || !reflect.DeepEqual(got, want) {
		return fmt.Errorf("registration matrix mismatch: got %v, want %v", got, want)
	}
	return nil
}

func TestMCPInventoryRegistrationCoverage(t *testing.T) {
	profiles := gatewayInventoryProfiles(t)
	// Exercise every optional availability branch with all owner combinations.
	expected := map[modbusProviderProfileID]modbusProviderState{
		modbusProviderSunSpec: modbusProviderPresent, modbusProviderGrowattBMS: modbusProviderPresent, modbusProviderTeslaRetained: modbusProviderPresent,
		modbusProviderSmartLogger: modbusProviderWithheld, modbusProviderEMMA: modbusProviderWithheld, modbusProviderSDongle: modbusProviderWithheld, modbusProviderGrowattProtocolII: modbusProviderWithheld, modbusProviderOutBack: modbusProviderWithheld,
	}
	entries := modbusProviderEntries()
	if len(entries) != len(expected) {
		t.Fatal("provider branch escaped matrix")
	}
	baseNames := map[string]bool{}
	for _, name := range profiles["constructor"] {
		baseNames[name] = true
	}
	optionalNames := map[modbusProviderProfileID][]string{}
	for id, profile := range map[modbusProviderProfileID]string{modbusProviderGrowattBMS: "modbus_production_2", modbusProviderTeslaRetained: "modbus_production_4"} {
		for _, name := range profiles[profile] {
			if !baseNames[name] {
				optionalNames[id] = append(optionalNames[id], name)
			}
		}
	}
	seen := map[modbusProviderProfileID]bool{}
	for _, entry := range entries {
		state, ok := expected[entry.id]
		if !ok || seen[entry.id] || entry.state != state {
			t.Fatal("unreviewed provider branch", entry.id)
		}
		seen[entry.id] = true
		tools := append([]string{}, entry.tools...)
		sort.Strings(tools)
		wantTools := append([]string{}, optionalNames[entry.id]...)
		sort.Strings(wantTools)
		if !reflect.DeepEqual(tools, wantTools) {
			t.Fatal("optional tool branch escaped inventory", entry.id)
		}
	}
	for mask := 0; mask < 8; mask++ {
		t.Run(fmt.Sprintf("owners_%d", mask), func(t *testing.T) {
			owners := modbusProviderOwners{}
			if mask&1 != 0 {
				owners.tcp = &modbusadapter.Adapter{}
			}
			if mask&2 != 0 {
				owners.growatt = &growattBMSRS485ProductionProvider{}
			}
			if mask&4 != 0 {
				owners.tesla = &teslaHSCRetainedOwner{}
			}
			for _, entry := range entries {
				for _, name := range entry.tools {
					want := entry.id == modbusProviderGrowattBMS && mask&2 != 0 || entry.id == modbusProviderTeslaRetained && mask&4 != 0
					if modbusProviderOptionalToolAvailable(name, owners) != want {
						t.Fatal("availability branch differs", entry.id, name)
					}
				}
			}
			if modbusProviderOptionalToolAvailable("unknown", owners) {
				t.Fatal("unknown optional tool admitted")
			}
			provider := newGatewayModbusMCPProviderWithRuntimes(owners.tcp, owners.growatt, owners.tesla)
			server, err := mcp.NewServer(registry.NewDeviceRegistry(nil), nil)
			if err != nil {
				t.Fatal(err)
			}
			mcp.RegisterModbusV1Tools(server, provider)
			want := profiles[fmt.Sprintf("modbus_production_%d", mask)]
			if err := gatewayInventoryEqual(gatewayInventoryNames(t, server), want); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("ordinary_composition_without_eebus", func(t *testing.T) {
		gw := &ebusgateway.Gateway{Registry: registry.NewDeviceRegistry(nil)}
		_, _, _, server, err := newHTTPControlPlaneSemanticHandlers(gw, graphql.NewBuilder(gw.Registry, nil), graphql.NewBroadcastHub(nil), newGatewaySemanticSnapshotProvider(nil))
		if err != nil {
			t.Fatal(err)
		}
		before := gatewayInventoryNames(t, server)
		if err := gatewayInventoryEqual(before, profiles["semantic_disabled"]); err != nil {
			t.Fatal(err)
		}
		if err := registerRuntimeControlMCPTools(server, nil); err != nil {
			t.Fatal(err)
		}
		if installVaillantB503(server, nil, nil, nil) == nil {
			t.Fatal("B503 composition absent")
		}
		names := gatewayInventoryNames(t, server)
		wantSet := map[string]bool{}
		for _, profile := range []string{"semantic_disabled", "runtime_disabled", "b503_enabled"} {
			for _, name := range profiles[profile] {
				wantSet[name] = true
			}
		}
		want := []string{}
		for name := range wantSet {
			want = append(want, name)
		}
		sort.Strings(want)
		if err := gatewayInventoryEqual(names, want); err != nil {
			t.Fatal(err)
		}
		for _, tool := range []string{mcp.RuntimeControlListTool, mcp.RuntimeControlGetTool, mcp.RuntimeControlStartTool, mcp.RuntimeControlStopTool, mcp.RuntimeControlRestartTool} {
			if !wantSet[tool] {
				t.Fatal("runtime tool omitted", tool)
			}
		}
		// Omission of the unconditional runtime registration must fail the same comparator.
		withoutRuntime := []string{}
		for _, name := range names {
			if !strings.HasPrefix(name, "helianthus.runtime.") {
				withoutRuntime = append(withoutRuntime, name)
			}
		}
		if gatewayInventoryEqual(withoutRuntime, want) == nil {
			t.Fatal("missing runtime registration accepted")
		}
	})
	inventoryRegistrationCallCoverage(t)
}

// Compare registration-bearing calls in each production construction point.
// A newly introduced registration cannot silently sit outside this matrix.
func inventoryRegistrationCallCoverage(t *testing.T) {
	t.Helper()
	matrix := map[string]map[string][]string{
		"gateway_http_server.go": {
			"startHTTPServer":                {"newHTTPControlPlaneSemanticHandlers", "registerRuntimeControlMCPTools", "RegisterEEBusV1Provider", "RegisterEEBusV1CommandRouter", "RegisterModbusV1Tools", "RegisterLeafPromotionCapture", "RegisterSynchronizedEvidenceCapture", "SetBusObservabilityProvider", "SetWatchSummaryProvider", "installVaillantB503"},
			"registerRuntimeControlMCPTools": {"RegisterRuntimeControlTools"},
		},
		"vaillant_b503_wiring.go":          {"installVaillantB503": {"RegisterVaillantB503Tools"}},
		"semantic_snapshot_provider.go":    {"newHTTPControlPlaneSemanticHandlers": {"NewServer", "RegisterSemanticSnapshotTool"}},
		"../../mcp/server_construction.go": {"NewServer": {"defaultServerTools", "RegisterEbusStandardTools"}},
		"../../mcp/modbus_v1.go":           {"RegisterModbusV1Tools": {"registerTeslaFC100SummaryV1Tool", "registerTeslaHSCV1Tool", "registerTeslaWCVitalsV1Tool", "registerGrowattProtocolIIV1Tool", "registerOutBackAXSV1Tool", "registerFroniusSunSpecV1Tool", "registerHuaweiEMMAV1Tool", "registerHuaweiSmartLoggerV1Tool", "registerHuaweiSDongleV1Tool", "registerTeslaGen3EVSECurrentLimitV1Tool", "registerGrowattBMSRS485V202Tool", "registerGrowattStorageSemanticTool", "registerTeslaGen3EVSESemanticTool"}},
	}
	for file, functions := range matrix {
		tree, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		found := map[string]bool{}
		for _, decl := range tree.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			want, ok := functions[fn.Name.Name]
			if !ok {
				continue
			}
			found[fn.Name.Name] = true
			got := []string{}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				name := ""
				switch fun := call.Fun.(type) {
				case *ast.Ident:
					name = fun.Name
				case *ast.SelectorExpr:
					name = fun.Sel.Name
				}
				selected := strings.HasPrefix(name, "Register") || strings.HasPrefix(name, "register") && strings.HasSuffix(name, "Tool") || strings.HasSuffix(name, "Tools") || name == "NewServer" || name == "newHTTPControlPlaneSemanticHandlers" || name == "installVaillantB503" || name == "SetBusObservabilityProvider" || name == "SetWatchSummaryProvider"
				if selected {
					got = append(got, name)
				}
				return true
			})
			sort.Strings(got)
			sort.Strings(want)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("registration coverage %s %s: got %v want %v", file, fn.Name.Name, got, want)
			}
		}
		if len(found) != len(functions) {
			t.Fatal("construction point missing", file)
		}
	}
}
