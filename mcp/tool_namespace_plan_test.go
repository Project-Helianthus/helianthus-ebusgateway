package mcp

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	mcpstandard "github.com/Project-Helianthus/helianthus-ebusgateway/mcp/ebusstandard"
)

// toolNamespacePlanPrefixes are the namespaces admitted by
// docs/architecture/mcp-namespaces.md, one per owner.
var toolNamespacePlanPrefixes = []string{
	"ebus_v1_",
	"eebus_v1_",
	"modbus_v1_",
	"semantic_v1_",
	"helianthus_v1_",
	"helianthus_runtime_",
}

const toolNamespacePlanDeferredTarget = "moves with the retrieval work of #1540"

type toolNamespacePlanRow struct {
	name, prefix, target string
}

// toolNamespacePlanRegistered enumerates the same profiles as the inventory
// test and returns every tool name any of them registers.
func toolNamespacePlanRegistered(t *testing.T) map[string]bool {
	t.Helper()
	names := map[string]bool{}
	for _, p := range inventoryProfiles() {
		s, err := NewServer(&testRegistry{}, &testInvoker{})
		inventoryMust(t, err)
		if p.setup != nil {
			p.setup(t, s)
		}
		for _, tool := range inventoryTools(t, s, p) {
			names[tool.Name] = true
		}
	}
	return names
}

// toolNamespacePlanDispatched lists the tool names the gateway compares a
// caller's tool name against before dispatch.
func toolNamespacePlanDispatched() []string {
	names := []string{
		synchronizedEvidenceCaptureToolName,
		leafPromotionCaptureToolName,
		m8SourceStateToolName,
		SemanticSnapshotTool,
		RuntimeControlListTool,
		RuntimeControlGetTool,
		RuntimeControlStartTool,
		RuntimeControlStopTool,
		RuntimeControlRestartTool,
		toolRuntimeStatusGetName,
		toolBusSummaryGetName,
		toolBusMessagesListName,
		toolBusIdentificationEvidenceListName,
		toolBusPeriodicityListName,
		toolBusProtocolSpecimensListName,
		toolWatchSummaryGetName,
		toolSemanticZonesGetName,
		toolSemanticCircuitsGetName,
		toolSemanticRadioGetName,
		toolSemanticFM5ModeGetName,
		toolSemanticFM5InterpretationGetName,
		toolSemanticSolarGetName,
		toolSemanticCylindersGetName,
		toolSemanticDHWGetName,
		toolSemanticEnergyGetName,
		toolSemanticBoilerGetName,
		toolSemanticSystemGetName,
		toolSemanticAdapterInfoGetName,
		toolSemanticSchedulesGetName,
		toolSemanticSystemSetConfigName,
		toolSemanticBoilerSetConfigName,
		toolSemanticSchedulesSetZoneName,
		toolSemanticSchedulesSetDhwName,
		toolSemanticSnapshotName,
		toolSnapshotCaptureName,
		toolSnapshotDropName,
		toolDevicesV1Name,
		toolDeviceGetV1Name,
		toolPlanesListV1Name,
		toolMethodsListV1Name,
		toolInvokeV1Name,
		mcpstandard.ToolServicesList,
		mcpstandard.ToolCommandsList,
		mcpstandard.ToolCommandGet,
		mcpstandard.ToolDecode,
		toolVaillantB503ErrorsGetName,
		toolVaillantB503ErrorsHistoryGetName,
		toolVaillantB503ErrorsHistoryListName,
		toolVaillantB503ServiceCurrentGetName,
		toolVaillantB503ServiceHistoryGetName,
		toolVaillantB503LiveMonitorName,
		toolVaillantB503LiveSessionGetName,
		ModbusV1RawReadTool,
		ModbusV1ProfileObservationGetTool,
		SemanticV1PVCurrentGetTool,
		eebusV1RuntimeStatusTool,
		eebusV1ServicesListTool,
		eebusV1ServicesGetTool,
		eebusV1SessionsListTool,
		eebusV1SessionsGetTool,
		eebusV1TopologyGetTool,
		eebusV1PairingStatusTool,
	}
	for _, spec := range eebusV1CommandSpecs {
		names = append(names, strings.ReplaceAll(string(spec.tool), ".", "_"))
	}
	return append(names, m8SourceToolNamesV1...)
}

func toolNamespacePlanReadDoc(t *testing.T) []toolNamespacePlanRow {
	t.Helper()
	data, err := os.ReadFile("../docs/architecture/mcp-namespaces.md")
	inventoryMust(t, err)
	text := string(data)
	const heading = "## Registered tools\n"
	start := strings.Index(text, heading)
	if start < 0 {
		t.Fatal("namespace plan has no registered tools table")
	}
	var rows []toolNamespacePlanRow
	for _, line := range strings.Split(text[start+len(heading):], "\n") {
		if !strings.HasPrefix(line, "| `") {
			continue
		}
		cells := strings.Split(strings.Trim(line, "|"), "|")
		if len(cells) != 3 {
			t.Fatalf("namespace plan row has %d cells: %q", len(cells), line)
		}
		for i := range cells {
			cells[i] = strings.TrimSpace(cells[i])
		}
		rows = append(rows, toolNamespacePlanRow{
			name:   strings.Trim(cells[0], "`"),
			prefix: strings.Trim(cells[1], "`"),
			target: cells[2],
		})
	}
	return rows
}

func toolNamespacePlanPrefix(name string) (string, bool) {
	for _, prefix := range toolNamespacePlanPrefixes {
		if strings.HasPrefix(name, prefix) && len(name) > len(prefix) {
			return prefix, true
		}
	}
	return "", false
}

func toolNamespacePlanCheck(registered map[string]bool, dispatched []string, rows []toolNamespacePlanRow) error {
	names := make([]string, 0, len(registered))
	for name := range registered {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if _, ok := toolNamespacePlanPrefix(name); !ok {
			return fmt.Errorf("tool %q is outside the namespace plan", name)
		}
		if strings.Contains(name, "experimental") {
			return fmt.Errorf("tool %q carries an experimental name", name)
		}
	}
	for _, name := range dispatched {
		if !registered[name] {
			return fmt.Errorf("dispatched tool name %q is not a registered tool", name)
		}
	}
	documented := map[string]bool{}
	for _, row := range rows {
		if documented[row.name] {
			return fmt.Errorf("namespace plan lists %q twice", row.name)
		}
		documented[row.name] = true
		if !registered[row.name] {
			return fmt.Errorf("namespace plan lists unregistered tool %q", row.name)
		}
		prefix, _ := toolNamespacePlanPrefix(row.name)
		if row.prefix != prefix {
			return fmt.Errorf("namespace plan puts %q in %q, want %q", row.name, row.prefix, prefix)
		}
		want := "`" + row.name + "`"
		if strings.HasPrefix(row.name, "ebus_v1_semantic_") && strings.HasSuffix(row.name, "_get") {
			want = toolNamespacePlanDeferredTarget
		}
		if row.target != want {
			return fmt.Errorf("namespace plan target for %q is %q, want %q", row.name, row.target, want)
		}
	}
	for _, name := range names {
		if !documented[name] {
			return fmt.Errorf("namespace plan omits registered tool %q", name)
		}
	}
	return nil
}

func TestToolNamespacePlanCoversEveryRegisteredTool(t *testing.T) {
	registered := toolNamespacePlanRegistered(t)
	dispatched := toolNamespacePlanDispatched()
	rows := toolNamespacePlanReadDoc(t)
	if len(registered) == 0 || len(rows) == 0 {
		t.Fatal("namespace plan check enumerated nothing")
	}
	inventoryMust(t, toolNamespacePlanCheck(registered, dispatched, rows))
	for _, name := range []string{leafPromotionCaptureToolName, m8SourceStateToolName} {
		if !strings.HasPrefix(name, "helianthus_v1_") {
			t.Fatalf("host tool %q is outside the host namespace", name)
		}
	}
	t.Logf("checked %d registered tools, %d dispatched names, %d plan rows", len(registered), len(dispatched), len(rows))
}

func TestToolNamespacePlanRejectsDeviations(t *testing.T) {
	registered := toolNamespacePlanRegistered(t)
	dispatched := toolNamespacePlanDispatched()
	rows := toolNamespacePlanReadDoc(t)
	cloneRegistered := func() map[string]bool {
		out := make(map[string]bool, len(registered))
		for name := range registered {
			out[name] = true
		}
		return out
	}
	cloneRows := func() []toolNamespacePlanRow { return append([]toolNamespacePlanRow(nil), rows...) }
	rowIndex := func(name string) int {
		for i, row := range rows {
			if row.name == name {
				return i
			}
		}
		t.Fatalf("plan row %q missing", name)
		return -1
	}
	cases := map[string]func() (map[string]bool, []string, []toolNamespacePlanRow){
		"registered_outside_plan": func() (map[string]bool, []string, []toolNamespacePlanRow) {
			r := cloneRegistered()
			r["helianthus_tool_get"] = true
			return r, dispatched, rows
		},
		"registered_bare_prefix": func() (map[string]bool, []string, []toolNamespacePlanRow) {
			r := cloneRegistered()
			r["semantic_v1_"] = true
			return r, dispatched, rows
		},
		"registered_experimental": func() (map[string]bool, []string, []toolNamespacePlanRow) {
			r := cloneRegistered()
			r["helianthus_v1_experimental_capture"] = true
			return r, dispatched, rows
		},
		"dispatched_unregistered": func() (map[string]bool, []string, []toolNamespacePlanRow) {
			return registered, append(append([]string(nil), dispatched...), "eebus.v1.features.data.get"), rows
		},
		"dispatched_constant_unregistered": func() (map[string]bool, []string, []toolNamespacePlanRow) {
			r := cloneRegistered()
			delete(r, m8SourceStateToolName)
			return r, dispatched, rows
		},
		"dispatched_helper_constant_unregistered": func() (map[string]bool, []string, []toolNamespacePlanRow) {
			r := cloneRegistered()
			delete(r, ModbusV1RawReadTool)
			return r, dispatched, rows
		},
		"row_missing": func() (map[string]bool, []string, []toolNamespacePlanRow) {
			out := cloneRows()
			i := rowIndex(leafPromotionCaptureToolName)
			return registered, dispatched, append(out[:i], out[i+1:]...)
		},
		"row_duplicate": func() (map[string]bool, []string, []toolNamespacePlanRow) {
			return registered, dispatched, append(cloneRows(), rows[0])
		},
		"row_name_unregistered": func() (map[string]bool, []string, []toolNamespacePlanRow) {
			out := cloneRows()
			out[rowIndex(m8SourceStateToolName)].name = "helianthus_v1_unregistered_get"
			return registered, dispatched, out
		},
		"row_prefix": func() (map[string]bool, []string, []toolNamespacePlanRow) {
			out := cloneRows()
			out[rowIndex(RuntimeControlListTool)].prefix = "helianthus_v1_"
			return registered, dispatched, out
		},
		"row_target_renamed": func() (map[string]bool, []string, []toolNamespacePlanRow) {
			out := cloneRows()
			out[rowIndex(SemanticSnapshotTool)].target = "`semantic_v1_other_get`"
			return registered, dispatched, out
		},
		"row_target_deferred_dropped": func() (map[string]bool, []string, []toolNamespacePlanRow) {
			out := cloneRows()
			out[rowIndex(toolSemanticZonesGetName)].target = "`" + toolSemanticZonesGetName + "`"
			return registered, dispatched, out
		},
		"row_write_marked_deferred": func() (map[string]bool, []string, []toolNamespacePlanRow) {
			out := cloneRows()
			out[rowIndex(toolSemanticSystemSetConfigName)].target = toolNamespacePlanDeferredTarget
			return registered, dispatched, out
		},
	}
	names := make([]string, 0, len(cases))
	for name := range cases {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			r, d, rows := cases[name]()
			if err := toolNamespacePlanCheck(r, d, rows); err == nil {
				t.Fatal("deviation accepted")
			}
		})
	}
}
