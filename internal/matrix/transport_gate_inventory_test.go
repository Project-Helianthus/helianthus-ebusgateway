package matrix

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The transport gate (scripts/transport_gate.sh) declares the adaptermux-v2
// inventory it checks a report against. This test renders that declaration
// from the generator, so adding, removing or recategorising a cell without
// updating the gate, or the other way round, fails here.
func TestTransportGateDeclaresTheAdapterMuxV2Inventory(t *testing.T) {
	t.Parallel()

	script, err := os.ReadFile(filepath.Join("..", "..", "scripts", "transport_gate.sh"))
	if err != nil {
		t.Fatalf("read transport gate: %v", err)
	}
	text := string(script)
	block := renderGateInventoryDeclaration(GenerateAdapterMuxV2Cases(""))
	if count := strings.Count(text, block); count != 1 {
		t.Fatalf("transport gate declaration differs from the generated inventory (found %d copies); want exactly:\n%s", count, block)
	}
	for _, retired := range []string{"len(cases) == 88", "len(cases) != 88", "expected 88 cases", "HELIANTHUS_AD_GATE_SKIP", "T01..T88"} {
		if strings.Contains(text, retired) {
			t.Errorf("transport gate still carries the retired report form: %q", retired)
		}
	}
}

func TestTransportGateDeclarationRenderingCoversEveryCellField(t *testing.T) {
	t.Parallel()

	base := GenerateAdapterMuxV2Cases("")
	want := renderGateInventoryDeclaration(base)
	mutations := map[string]func(cases []TopologyCase){
		"case id":    func(cases []TopologyCase) { cases[0].ID = "ENS-AD13" },
		"row":        func(cases []TopologyCase) { cases[len(cases)-1].AdapterMux.Row = "ENS" },
		"southbound": func(cases []TopologyCase) { cases[0].AdapterMux.Southbound = TransportENH },
		"invariant":  func(cases []TopologyCase) { cases[0].AdapterMux.Invariant = "AD02" },
		"category":   func(cases []TopologyCase) { category := CategoryRespec; cases[0].AdapterMux.Reserved = &category },
		"executable": func(cases []TopologyCase) {
			cases[len(cases)-1].AdapterMux.Reserved = nil
			cases[4].AdapterMux.Reserved = nil
		},
		"cell count":  func(cases []TopologyCase) {},
		"xf reason":   func(cases []TopologyCase) {},
		"xf case":     func(cases []TopologyCase) {},
		"bi02 reason": func(cases []TopologyCase) {},
	}
	for name, mutate := range mutations {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cases := GenerateAdapterMuxV2Cases("")
			for index := range cases {
				cell := *cases[index].AdapterMux
				cases[index].AdapterMux = &cell
			}
			mutate(cases)
			var got string
			switch name {
			case "cell count":
				got = renderGateInventoryDeclaration(cases[:len(cases)-1])
			case "xf reason":
				got = renderGateDeclaration(cases, CategoryBI02.Reason, []ExpectedFailure{{ID: "XF-01", CaseID: "TCP-CX01", Reason: "other"}})
			case "xf case":
				got = renderGateDeclaration(cases, CategoryBI02.Reason, []ExpectedFailure{{ID: "XF-01", CaseID: "UDP-CX01", Reason: AdapterMuxV2ExpectedFailures[0].Reason}})
			case "bi02 reason":
				got = renderGateDeclaration(cases, "other", AdapterMuxV2ExpectedFailures)
			default:
				got = renderGateInventoryDeclaration(cases)
			}
			if got == want {
				t.Fatalf("mutating the %s does not change the rendered declaration", name)
			}
		})
	}
}

func renderGateInventoryDeclaration(cases []TopologyCase) string {
	return renderGateDeclaration(cases, CategoryBI02.Reason, AdapterMuxV2ExpectedFailures)
}

func renderGateDeclaration(cases []TopologyCase, bi02Reason string, expectedFailures []ExpectedFailure) string {
	var builder strings.Builder
	builder.WriteString("# BEGIN adaptermux-v2 inventory declaration\n")
	fmt.Fprintf(&builder, "INVENTORY_VERSION = %q\n", AdapterMuxV2InventoryVersion)
	fmt.Fprintf(&builder, "SUITE = %q\n", SuiteAdapterMuxV2)
	builder.WriteString("# Accepted categories: the outcome a cell of the category carries and the\n")
	builder.WriteString("# reason string it must name. BI-01 applies to executable cells only.\n")
	builder.WriteString("CATEGORIES = {\n")
	fmt.Fprintf(&builder, "    %q: (%q, %q),\n", "BI-01", caseOutcomeBlocked, infraReasonAdapterNoSignal)
	bi02 := CategoryBI02
	bi02.Reason = bi02Reason
	for _, category := range []ReservedCategory{bi02, CategoryPL01, CategoryPL02, CategoryPL03, CategoryPL04} {
		if !category.GateAccepts {
			continue
		}
		fmt.Fprintf(&builder, "    %q: (%q, %q),\n", category.ID, category.Outcome, category.Reason)
	}
	builder.WriteString("}\n")
	builder.WriteString("EXPECTED_FAILURES = {\n")
	for _, entry := range expectedFailures {
		fmt.Fprintf(&builder, "    %q: (%q, %q),\n", entry.CaseID, entry.ID, entry.Reason)
	}
	builder.WriteString("}\n")
	builder.WriteString("# case_id, row whose hardware setup runs it, southbound transport, invariant,\n")
	builder.WriteString("# reserved category (\"-\" for an executable cell).\n")
	builder.WriteString("CELLS = \"\"\"\n")
	for _, testCase := range cases {
		cell := testCase.AdapterMux
		category := "-"
		if cell.Reserved != nil {
			category = cell.Reserved.ID
		}
		fmt.Fprintf(&builder, "%s %s %s %s %s\n", testCase.ID, cell.Row, cell.Southbound, cell.Invariant, category)
	}
	builder.WriteString("\"\"\"\n")
	builder.WriteString("# END adaptermux-v2 inventory declaration\n")
	return builder.String()
}
