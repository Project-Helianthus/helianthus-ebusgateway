package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/semanticview"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

type inventoryRow struct {
	Name      string `json:"name"`
	Surface   string `json:"surface"`
	Profile   string `json:"registration_profile"`
	Condition string `json:"availability_condition"`
	Source    string `json:"implementation_source"`
}

type inventoryDocument struct {
	SchemaVersion int            `json:"schema_version"`
	Rows          []inventoryRow `json:"tools"`
}

// These wrappers expose structural interfaces without calling their providers.
// The production profile independently checks the same owner combinations.
type inventoryModbus struct {
	*teslaHSCV1FixtureProvider
	mask int
}

func (p *inventoryModbus) ModbusV1CoreAvailable() bool { return p.mask&1 != 0 }
func (p *inventoryModbus) ModbusV1OptionalToolAvailable(name string) bool {
	switch name {
	case GrowattBMSRS485V202StatusGetTool, SemanticV1GrowattStorageCurrentGetTool:
		return p.mask&2 != 0
	case TeslaGen3EVSECurrentLimitV1GetTool, SemanticV1EVSECurrentGetTool:
		return p.mask&4 != 0
	default:
		return false
	}
}
func (*inventoryModbus) GrowattBMSRS485V202(context.Context) (GrowattBMSRS485V202Observation, error) {
	panic("inventory acquired native data")
}
func (*inventoryModbus) GrowattStorageSemanticCurrent(context.Context) (any, error) {
	panic("inventory acquired semantic data")
}
func (*inventoryModbus) TeslaGen3EVSECurrentLimitV1(context.Context) (TeslaGen3EVSECurrentLimitV1Source, error) {
	panic("inventory acquired native data")
}
func (*inventoryModbus) TeslaGen3EVSESemanticCurrent(context.Context) (any, error) {
	panic("inventory acquired semantic data")
}

type inventoryBus struct {
	*testBusObservabilityProvider
	testIdentificationEvidenceProvider
}

type inventorySemantic struct{}

func (*inventorySemantic) Capture(context.Context, []semreg.AssetID) (*semreg.PackMetadataRegistry, []semanticview.Input, error) {
	panic("inventory acquired semantic data")
}

// Discovery must not acquire a publication from an enabled provider.

type inventoryProfile struct {
	name, condition, source string
	setup                   func(*testing.T, *Server)
	operator                bool
	sourceScope             bool
}

func inventoryProfiles() []inventoryProfile {
	profiles := []inventoryProfile{
		{name: "operator_commands_only", condition: "command router present; eeBUS provider absent", operator: true, setup: func(t *testing.T, s *Server) {
			inventoryMust(t, s.RegisterEEBusV1CommandRouter(&issue749CommandRuntime{}))
		}},
		{name: "operator_promotion_only", condition: "leaf promotion capture provider present; eeBUS provider absent", operator: true, setup: func(t *testing.T, s *Server) {
			inventoryMust(t, s.RegisterLeafPromotionCapture(&issue784CaptureProvider{}))
		}},
		{name: "operator_capture_only", condition: "synchronized capture provider present; eeBUS provider absent", operator: true, setup: func(t *testing.T, s *Server) {
			inventoryMust(t, s.RegisterSynchronizedEvidenceCapture(&issue764CaptureProvider{}))
		}},
		{name: "operator_eebus_only", condition: "eeBUS provider present; command and capture providers absent", operator: true, setup: func(t *testing.T, s *Server) { inventoryMust(t, s.RegisterEEBusV1Provider(&msp06Provider{})) }},
		{name: "ordinary_composed", condition: "all composed ordinary optional providers present", setup: inventoryComposed},
		{name: "operator_composed", condition: "all composed ordinary and operator providers present", operator: true, setup: inventoryComposed},
		{name: "semantic_enabled", condition: "shared semantic provider present", source: "mcp/semantic_snapshot.go", setup: func(t *testing.T, s *Server) { inventoryMust(t, RegisterSemanticSnapshotTool(s, &inventorySemantic{})) }},
		{name: "constructor", condition: "NewServer; optional providers absent"},
		{name: "runtime_disabled", condition: "runtime service absent; eeBUS absent", source: "mcp/runtime_control.go", setup: func(t *testing.T, s *Server) { inventoryMust(t, RegisterRuntimeControlTools(s, nil)) }},
		{name: "runtime_enabled", condition: "runtime service present; eeBUS absent", source: "mcp/runtime_control.go", setup: func(t *testing.T, s *Server) { inventoryMust(t, RegisterRuntimeControlTools(s, &rcFake{})) }},
		{name: "semantic_disabled", condition: "shared semantic provider absent; discovery remains configured", source: "mcp/semantic_snapshot.go", setup: func(t *testing.T, s *Server) { inventoryMust(t, RegisterSemanticSnapshotTool(s, nil)) }},
		{name: "b503_disabled", condition: "invalid B503 options; registration withheld", source: "mcp/vaillant_b503.go", setup: func(_ *testing.T, s *Server) { RegisterVaillantB503Tools(s, VaillantB503Options{}) }},
		{name: "b503_enabled", condition: "B503 dispatcher and session manager present", source: "mcp/vaillant_b503.go", setup: func(_ *testing.T, s *Server) {
			RegisterVaillantB503Tools(s, VaillantB503Options{Dispatcher: &stubB503Dispatcher{}, SessionManager: newDefaultMgr()})
		}},
		{name: "bus_enabled", condition: "bus observability provider present", source: "mcp/server.go", setup: func(_ *testing.T, s *Server) { s.SetBusObservabilityProvider(&testBusObservabilityProvider{}) }},
		{name: "identification_enabled", condition: "bus provider retains identification evidence", source: "mcp/identification_evidence.go", setup: func(_ *testing.T, s *Server) {
			s.SetBusObservabilityProvider(&inventoryBus{testBusObservabilityProvider: &testBusObservabilityProvider{}})
		}},
		{name: "watch_enabled", condition: "watch summary provider present", source: "mcp/server.go", setup: func(_ *testing.T, s *Server) { s.SetWatchSummaryProvider(&testWatchSummaryProvider{}) }},
		{name: "eebus_enabled", condition: "eeBUS provider present", source: "mcp/eebus_v1.go", setup: func(t *testing.T, s *Server) { inventoryMust(t, s.RegisterEEBusV1Provider(&msp06Provider{})) }},
		{name: "operator_disabled", condition: "operator handler; optional providers absent", operator: true},
		{name: "operator_enabled", condition: "eeBUS provider, command router and capture providers present", operator: true, setup: inventoryOperator},
		{name: "operator_source_disabled", condition: "operator source scope; eeBUS absent", operator: true, sourceScope: true},
		{name: "operator_source_enabled", condition: "operator source scope; eeBUS provider present", operator: true, sourceScope: true, setup: inventoryOperator},
	}
	core := &modbusV1FixtureProvider{}
	targets := []struct {
		name, source string
		p            ModbusV1Provider
	}{
		{"modbus_core", "mcp/modbus_v1.go", core},
		{"tesla_summary", "mcp/tesla_fc100_summary_v1.go", &teslaFC100SummaryV1FixtureProvider{modbusV1FixtureProvider: core}},
		{"tesla_hsc", "mcp/tesla_hsc_v1.go", &teslaHSCV1FixtureProvider{core}},
		{"tesla_vitals", "mcp/tesla_wcapi_get_vitals_v1.go", &teslaWCVitalsV1FixtureProvider{modbusV1FixtureProvider: core}},
		{"growatt_protocol", "mcp/growatt_protocol_ii_v1.go", &growattProtocolIIV1FixtureProvider{modbusV1FixtureProvider: core}},
		{"outback", "mcp/outback_axs_v1.go", &outBackAXSV1FixtureProvider{core}},
		{"fronius", "mcp/fronius_sunspec_v1.go", &froniusSunSpecFixture{core}},
		{"huawei_emma", "mcp/huawei_emma_v1.go", &emmaFixture{core}},
		{"huawei_logger", "mcp/huawei_smartlogger_v1.go", &smartLoggerFixture{core}},
		{"huawei_dongle", "mcp/huawei_sdongle_v1.go", &sdongleFixture{core}},
		{"huawei_instances", "mcp/huawei_smartlogger_v1.go", &h10InstanceFixture{modbusV1FixtureProvider: core}},
		{"huawei_instances_coreless", "mcp/huawei_smartlogger_v1.go", &h10CorelessInstanceFixture{&h10InstanceFixture{modbusV1FixtureProvider: core}}},
	}
	for _, target := range targets {
		target := target
		profiles = append(profiles, inventoryProfile{name: "target_" + target.name, condition: inventoryTargetCondition(target.name), source: target.source, setup: func(_ *testing.T, s *Server) { RegisterModbusV1Tools(s, target.p) }})
	}
	for mask := 0; mask < 8; mask++ {
		mask := mask
		profiles = append(profiles, inventoryProfile{name: fmt.Sprintf("modbus_production_%d", mask), condition: fmt.Sprintf("owner mask %d: SunSpec=1, Growatt storage=2, Tesla retained=4", mask), setup: func(_ *testing.T, s *Server) {
			if mask == 0 {
				RegisterModbusV1Tools(s, nil)
			}
			if mask != 0 {
				RegisterModbusV1Tools(s, &inventoryModbus{teslaHSCV1FixtureProvider: &teslaHSCV1FixtureProvider{&modbusV1FixtureProvider{}}, mask: mask})
			}
		}})
	}
	return profiles
}

func inventoryMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func inventoryComposed(t *testing.T, s *Server) {
	inventoryMust(t, RegisterRuntimeControlTools(s, &rcFake{}))
	inventoryMust(t, RegisterSemanticSnapshotTool(s, &inventorySemantic{}))
	RegisterVaillantB503Tools(s, VaillantB503Options{Dispatcher: &stubB503Dispatcher{}, SessionManager: newDefaultMgr()})
	s.SetBusObservabilityProvider(&inventoryBus{testBusObservabilityProvider: &testBusObservabilityProvider{}})
	s.SetWatchSummaryProvider(&testWatchSummaryProvider{})
	RegisterModbusV1Tools(s, &inventoryModbus{teslaHSCV1FixtureProvider: &teslaHSCV1FixtureProvider{&modbusV1FixtureProvider{}}, mask: 7})
	inventoryOperator(t, s)
}

func inventoryOperator(t *testing.T, s *Server) {
	inventoryMust(t, s.RegisterEEBusV1Provider(&msp06Provider{}))
	inventoryMust(t, s.RegisterEEBusV1CommandRouter(&issue749CommandRuntime{}))
	inventoryMust(t, s.RegisterLeafPromotionCapture(&issue784CaptureProvider{}))
	inventoryMust(t, s.RegisterSynchronizedEvidenceCapture(&issue764CaptureProvider{}))
}

func inventoryTools(t *testing.T, s *Server, p inventoryProfile) []Tool {
	t.Helper()
	h := s.Handler()
	if p.operator {
		h = s.eebusV1OperatorHandler()
	}
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	req.Header.Set("Content-Type", "application/json")
	if p.sourceScope {
		req.Header.Set(m8SourceScopeHeader, m8SourceScopeV1)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var result struct {
		Result struct {
			Tools []Tool `json:"tools"`
		} `json:"result"`
		Error any `json:"error"`
	}
	inventoryMust(t, json.Unmarshal(rec.Body.Bytes(), &result))
	if rec.Code != http.StatusOK || result.Error != nil || len(result.Result.Tools) == 0 {
		t.Fatalf("tools/list failed: %d %s", rec.Code, rec.Body.String())
	}
	return result.Result.Tools
}

func inventoryTargetCondition(name string) string {
	if name == "modbus_core" || name == "tesla_hsc" {
		return "structural helper profile; production owner availability checked separately"
	}
	return "target/uncomposed structural provider; not a production enablement claim"
}

func inventorySource(name string, p inventoryProfile, base map[string]string) string {
	if src := base[name]; src != "" {
		return src
	}
	if strings.HasPrefix(name, "helianthus.runtime.") {
		return "mcp/runtime_control.go"
	}
	if strings.HasPrefix(name, "ebus.v1.vaillant.") {
		return "mcp/vaillant_b503.go"
	}
	if name == SemanticSnapshotTool {
		return "mcp/semantic_snapshot.go"
	}
	for _, tool := range identificationEvidenceTools() {
		if tool.Name == name {
			return "mcp/identification_evidence.go"
		}
	}
	for _, tool := range busObservabilityTools() {
		if tool.Name == name {
			return "mcp/server.go"
		}
	}
	for _, tool := range watchSummaryTools() {
		if tool.Name == name {
			return "mcp/server.go"
		}
	}
	switch name {
	case ModbusV1RawReadTool, ModbusV1ProfileObservationGetTool, SemanticV1PVCurrentGetTool:
		return "mcp/modbus_v1.go"
	case TeslaHSCV1StatusGetTool:
		return "mcp/tesla_hsc_v1.go"
	case GrowattBMSRS485V202StatusGetTool:
		return "mcp/growatt_bms_rs485_v202.go"
	case SemanticV1GrowattStorageCurrentGetTool:
		return "mcp/growatt_storage_semantic.go"
	case TeslaGen3EVSECurrentLimitV1GetTool:
		return "mcp/tesla_gen3_evse_current_limit_v1.go"
	case SemanticV1EVSECurrentGetTool:
		return "mcp/tesla_gen3_evse_semantic.go"
	case leafPromotionCaptureToolName:
		return "mcp/leaf_promotion_capture.go"
	case synchronizedEvidenceCaptureToolName:
		return "mcp/synchronized_evidence.go"
	case m8SourceStateToolName:
		return "mcp/m8_source_state.go"
	}
	if strings.HasPrefix(name, "eebus.v1.") {
		for _, tool := range eebusV1Tools() {
			if tool.Name == name {
				return "mcp/eebus_v1.go"
			}
		}
		return "mcp/eebus_v1_commands.go"
	}
	// Instance schemas are emitted by the shared instance helper.
	if p.name == "target_huawei_instances" || p.name == "target_huawei_instances_coreless" {
		return "mcp/huawei_smartlogger_v1.go"
	}
	if p.name == "identification_enabled" && name != toolBusIdentificationEvidenceListName {
		return "mcp/server.go"
	}
	return p.source
}

func inventoryValidate(got, want inventoryDocument) error {
	if got.SchemaVersion != 1 || want.SchemaVersion != 1 {
		return fmt.Errorf("unsupported schema")
	}
	seen := map[string]bool{}
	for _, r := range got.Rows {
		key := r.Surface + "/" + r.Profile + "/" + r.Name
		if seen[key] {
			return fmt.Errorf("duplicate %s", key)
		}
		seen[key] = true
		if r.Name == "" || r.Source == "" || r.Condition == "" {
			return fmt.Errorf("incomplete row")
		}
	}
	if !reflect.DeepEqual(got, want) {
		return fmt.Errorf("inventory differs from tools/list matrix")
	}
	return nil
}

func inventoryRead(t *testing.T) inventoryDocument {
	t.Helper()
	data, err := os.ReadFile("../docs/mcp-tool-inventory.md")
	inventoryMust(t, err)
	start := bytes.Index(data, []byte("```json\n"))
	if start < 0 {
		t.Fatal("missing committed inventory")
	}
	data = data[start+8:]
	end := bytes.Index(data, []byte("\n```"))
	if end < 0 {
		t.Fatal("unterminated inventory")
	}
	var d inventoryDocument
	inventoryMust(t, json.Unmarshal(data[:end], &d))
	return d
}

func inventoryOutput(t *testing.T, d inventoryDocument) {
	t.Helper()
	output := os.Getenv("HELIANTHUS_MCP_INVENTORY_OUT")
	if output == "" {
		return
	}
	root, err := filepath.Abs("..")
	inventoryMust(t, err)
	root, err = filepath.EvalSymlinks(root)
	inventoryMust(t, err)
	scratch, err := filepath.EvalSymlinks(filepath.Join(root, ".tmp"))
	inventoryMust(t, err)
	if scratch != filepath.Join(root, ".tmp") {
		t.Fatal("scratch directory must be local, without symlink redirection")
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(output))
	inventoryMust(t, err)
	rel, err := filepath.Rel(scratch, parent)
	inventoryMust(t, err)
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.Ext(output) != ".json" {
		t.Fatal("inventory output must be a worktree-local .tmp JSON file")
	}
	if info, err := os.Lstat(output); err == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
		t.Fatal("inventory output must be a regular file")
	}
	data, err := json.MarshalIndent(d, "", "  ")
	inventoryMust(t, err)
	inventoryMust(t, os.WriteFile(output, append(data, '\n'), 0600))
}

func TestMCPToolInventory(t *testing.T) {
	constructor, err := NewServer(&testRegistry{}, &testInvoker{})
	inventoryMust(t, err)
	base := map[string]string{}
	for _, tool := range defaultServerTools() {
		base[tool.Name] = "mcp/server_tools.go"
	}
	for _, tool := range inventoryTools(t, constructor, inventoryProfile{}) {
		if base[tool.Name] == "" {
			base[tool.Name] = "mcp/ebus_standard_wiring.go"
		}
	}
	doc := inventoryDocument{SchemaVersion: 1}
	for _, p := range inventoryProfiles() {
		t.Run(p.name, func(t *testing.T) {
			s, err := NewServer(&testRegistry{}, &testInvoker{})
			inventoryMust(t, err)
			if p.setup != nil {
				p.setup(t, s)
			}
			surface := "ordinary"
			if p.operator {
				surface = "operator"
			}
			if p.sourceScope {
				surface = "operator_source"
			}
			for _, tool := range inventoryTools(t, s, p) {
				condition := p.condition
				if base[tool.Name] != "" {
					condition = "always: NewServer"
				}
				doc.Rows = append(doc.Rows, inventoryRow{tool.Name, surface, p.name, condition, inventorySource(tool.Name, p, base)})
			}
		})
	}
	sort.Slice(doc.Rows, func(i, j int) bool {
		a, b := doc.Rows[i], doc.Rows[j]
		return a.Surface+"/"+a.Profile+"/"+a.Name < b.Surface+"/"+b.Profile+"/"+b.Name
	})
	inventoryMust(t, inventoryValidate(doc, doc))
	inventoryOutput(t, doc)
	inventoryMust(t, inventoryValidate(inventoryRead(t), doc))
	t.Logf("enumerated %d rows across %d profiles", len(doc.Rows), len(inventoryProfiles()))
	for _, mutation := range []string{"remove_profile", "remove_tool", "duplicate", "omit_runtime", "name", "surface", "profile", "condition", "source"} {
		t.Run("negative_"+mutation, func(t *testing.T) {
			changed := inventoryDocument{SchemaVersion: 1, Rows: append([]inventoryRow(nil), doc.Rows...)}
			switch mutation {
			case "remove_profile":
				filtered := changed.Rows[:0]
				for _, r := range doc.Rows {
					if r.Profile != "constructor" {
						filtered = append(filtered, r)
					}
				}
				changed.Rows = filtered
			case "remove_tool":
				changed.Rows = changed.Rows[1:]
			case "duplicate":
				changed.Rows = append(changed.Rows, changed.Rows[0])
			case "omit_runtime":
				filtered := changed.Rows[:0]
				for _, r := range doc.Rows {
					if !strings.HasPrefix(r.Name, "helianthus.runtime.") {
						filtered = append(filtered, r)
					}
				}
				changed.Rows = filtered
			case "name":
				changed.Rows[0].Name += ".unexpected"
			case "surface":
				changed.Rows[0].Surface = "unexpected"
			case "profile":
				changed.Rows[0].Profile = "unexpected"
			case "condition":
				changed.Rows[0].Condition = "unexpected"
			case "source":
				changed.Rows[0].Source = "mcp/unexpected.go"
			}
			if inventoryValidate(changed, doc) == nil {
				t.Fatal("completeness accepted mutation")
			}
		})
	}
}
