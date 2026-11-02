package matrix

import (
	"strings"
	"testing"
)

// The expected inventory is spelled out independently of the generator.
var (
	wantRows       = []string{"ENS", "ENH", "UDP", "TCP", "UARTENS", "UARTENH", "UARTRAW"}
	wantInvariants = []string{
		"AD01", "AD02", "AD03", "AD04", "AD05", "AD06", "AD07", "AD08", "AD09", "AD10", "AD12",
		"PX01", "PX02", "PX03", "PX04", "PX05", "PX06", "PX07", "PX08", "PX09", "PX10", "PX11", "PX12",
		"CX01",
	}
	wantCompat     = []string{"ENS-DS01", "ENH-DS01", "UDP-DS01", "TCP-DS01", "EBUSD-ES01"}
	wantSouthbound = map[string]Transport{
		"ENS": "ens", "ENH": "enh", "UDP": "udp", "TCP": "tcp",
		"UARTENS": "uart-ens", "UARTENH": "uart-enh", "UARTRAW": "uart-raw",
	}
)

func adapterMuxCaseMap(t *testing.T) map[string]TopologyCase {
	t.Helper()
	cases := GenerateAdapterMuxV2Cases("")
	byID := make(map[string]TopologyCase, len(cases))
	for _, testCase := range cases {
		if _, dup := byID[testCase.ID]; dup {
			t.Fatalf("duplicate case %s", testCase.ID)
		}
		byID[testCase.ID] = testCase
	}
	return byID
}

func TestAdapterMuxV2InventoryExactIDsOrderAndCount(t *testing.T) {
	t.Parallel()

	want := make([]string, 0, 173)
	for _, row := range wantRows {
		for _, invariant := range wantInvariants {
			want = append(want, row+"-"+invariant)
		}
	}
	want = append(want, wantCompat...)

	cases, err := CasesForSuite(SuiteAdapterMuxV2)
	if err != nil {
		t.Fatalf("CasesForSuite error = %v", err)
	}
	if len(cases) != 173 || len(want) != 173 {
		t.Fatalf("len(cases) = %d, len(want) = %d; want 173", len(cases), len(want))
	}
	for index, testCase := range cases {
		if testCase.ID != want[index] {
			t.Fatalf("cases[%d] = %s; want %s", index, testCase.ID, want[index])
		}
		if testCase.AdapterMux == nil {
			t.Fatalf("%s has no adaptermux cell", testCase.ID)
		}
	}
	if got := AdapterMuxV2Transports(); strings.Join(got, ",") != strings.Join(wantRows, ",") {
		t.Fatalf("transports = %v; want %v", got, wantRows)
	}
}

func TestAdapterMuxV2CellIdentityFields(t *testing.T) {
	t.Parallel()

	byID := adapterMuxCaseMap(t)
	for _, row := range wantRows {
		for _, invariant := range wantInvariants {
			testCase := byID[row+"-"+invariant]
			cell := testCase.AdapterMux
			if cell.Row != row || cell.Southbound != wantSouthbound[row] || cell.Invariant != invariant {
				t.Fatalf("%s identity = %s/%s/%s", testCase.ID, cell.Row, cell.Southbound, cell.Invariant)
			}
			if cell.Composition != CompositionMux {
				t.Fatalf("%s composition = %q; want mux", testCase.ID, cell.Composition)
			}
			wantKind, wantFamily := TopologyMuxInvariant, invariant[:2]
			if invariant == "CX01" {
				wantKind = TopologyMuxCoexistence
			}
			if testCase.Kind != wantKind || cell.Family != wantFamily {
				t.Fatalf("%s kind/family = %s/%s; want %s/%s", testCase.ID, testCase.Kind, cell.Family, wantKind, wantFamily)
			}
		}
	}

	compat := []struct {
		id          string
		row         string
		southbound  Transport
		kind        TopologyKind
		composition string
		invariant   string
	}{
		{"ENS-DS01", "ENS", "ens", TopologyDirectCompat, CompositionDirect, "DS01"},
		{"ENH-DS01", "ENH", "enh", TopologyDirectCompat, CompositionDirect, "DS01"},
		{"UDP-DS01", "UDP", "udp", TopologyDirectCompat, CompositionDirect, "DS01"},
		{"TCP-DS01", "TCP", "tcp", TopologyDirectCompat, CompositionDirect, "DS01"},
		{"EBUSD-ES01", "ENH", "ebusd-tcp", TopologyEbusdTCPCompat, CompositionEbusdTCP, "ES01"},
	}
	for _, want := range compat {
		testCase := byID[want.id]
		cell := testCase.AdapterMux
		if cell.Row != want.row || cell.Southbound != want.southbound || testCase.Kind != want.kind ||
			cell.Composition != want.composition || cell.Invariant != want.invariant || cell.Family != FamilyCompat {
			t.Fatalf("%s = %+v kind %s", want.id, *cell, testCase.Kind)
		}
		if !cell.Executable() {
			t.Fatalf("%s must be executable", want.id)
		}
	}
	if !byID["EBUSD-ES01"].UsesEbusd || byID["ENH-DS01"].UsesEbusd {
		t.Fatalf("only EBUSD-ES01 of the compatibility block uses ebusd")
	}
}

func TestAdapterMuxV2CategoryPrecedence(t *testing.T) {
	t.Parallel()

	byID := adapterMuxCaseMap(t)
	cases := []struct {
		id   string
		want string // category ID, "" for executable
	}{
		{"UARTENS-PX09", "BI-02"}, // BI-02 over PL-02
		{"UARTENH-AD05", "BI-02"}, // BI-02 over PL-03
		{"UARTRAW-PX05", "BI-02"}, // BI-02 over re-specification
		{"UARTRAW-AD01", "BI-02"},
		{"UDP-PX10", "PL-01"}, // PL-01 over PL-02
		{"TCP-AD08", "PL-01"}, // PL-01 over PL-03
		{"TCP-AD07", "PL-01"}, // PL-01 over re-specification
		{"UDP-CX01", "PL-01"},
		{"ENS-PX11", "PL-02"},
		{"ENH-AD12", "PL-03"},
		{"ENS-AD05", "PL-03"},
		{"ENH-PX05", "RS"},
		{"ENS-AD04", "RS"},
		{"ENH-AD01", ""},
		{"ENS-CX01", ""},
	}
	for _, tc := range cases {
		cell := byID[tc.id].AdapterMux
		got := ""
		if cell.Reserved != nil {
			got = cell.Reserved.ID
		}
		if got != tc.want {
			t.Fatalf("%s category = %q; want %q", tc.id, got, tc.want)
		}
	}

	// No inventory cell carries both PL-02 and PL-03 or re-specification
	// flags, so the lower part of the order is checked on a synthetic spec.
	all := InvariantSpec{localResponder: true, rawForwarding: true, respec: true}
	order := []struct {
		row  adapterMuxRow
		spec InvariantSpec
		want ReservedCategory
	}{
		{adapterMuxRow{uart: true, plain: true}, all, CategoryBI02},
		{adapterMuxRow{plain: true}, all, CategoryPL01},
		{adapterMuxRow{}, all, CategoryPL02},
		{adapterMuxRow{}, InvariantSpec{rawForwarding: true, respec: true}, CategoryPL03},
		{adapterMuxRow{}, InvariantSpec{respec: true}, CategoryRespec},
	}
	for index, tc := range order {
		got := reservedCategoryFor(tc.row, tc.spec)
		if got == nil || *got != tc.want {
			t.Fatalf("order[%d] = %+v; want %+v", index, got, tc.want)
		}
	}
	if got := reservedCategoryFor(adapterMuxRow{}, InvariantSpec{}); got != nil {
		t.Fatalf("plain spec reserved as %+v", got)
	}
}

func TestAdapterMuxV2CategoryCountsAndReasons(t *testing.T) {
	t.Parallel()

	reasons := map[string]string{
		"BI-02": "uart_direct_hardware_absent",
		"PL-01": "mux_southbound_plain_not_implemented",
		"PL-02": "mux_local_responder_not_implemented",
		"PL-03": "mux_raw_symbol_forwarding_not_implemented",
		"RS":    "cell_awaiting_respecification",
	}
	counts := map[string]int{}
	executable := make([]string, 0, 32)
	for _, testCase := range GenerateAdapterMuxV2Cases("") {
		cell := testCase.AdapterMux
		if cell.Reserved == nil {
			executable = append(executable, testCase.ID)
			if cell.Workload == "" || cell.PassCriterion == "" {
				t.Fatalf("%s is executable without a workload and pass criterion", testCase.ID)
			}
			continue
		}
		counts[cell.Reserved.ID]++
		if cell.Reserved.Reason != reasons[cell.Reserved.ID] {
			t.Fatalf("%s reason = %q", testCase.ID, cell.Reserved.Reason)
		}
		if cell.Reserved.GateAccepts != (cell.Reserved.ID != "RS") {
			t.Fatalf("%s gate acceptance = %v", testCase.ID, cell.Reserved.GateAccepts)
		}
	}
	want := map[string]int{"BI-02": 72, "PL-01": 48, "PL-02": 6, "PL-03": 6, "RS": 14}
	for id, count := range want {
		if counts[id] != count {
			t.Fatalf("%s count = %d; want %d", id, counts[id], count)
		}
	}
	wantExecutable := []string{}
	for _, row := range []string{"ENS", "ENH"} {
		for _, invariant := range []string{"AD01", "AD02", "AD03", "AD06", "AD10", "PX01", "PX02", "PX03", "PX04", "PX12", "CX01"} {
			wantExecutable = append(wantExecutable, row+"-"+invariant)
		}
	}
	wantExecutable = append(wantExecutable, wantCompat...)
	if strings.Join(executable, ",") != strings.Join(wantExecutable, ",") {
		t.Fatalf("executable cells = %v; want %v", executable, wantExecutable)
	}
}

func TestAdapterMuxV2ExpectedFailureInventoryIsXF01Only(t *testing.T) {
	t.Parallel()

	if len(AdapterMuxV2ExpectedFailures) != 1 {
		t.Fatalf("expected-failure inventory = %+v; want XF-01 only", AdapterMuxV2ExpectedFailures)
	}
	entry := AdapterMuxV2ExpectedFailures[0]
	if entry.ID != "XF-01" || entry.CaseID != "TCP-CX01" ||
		entry.Reason != "southbound plain tcp under dual-client load reports no signal (carried from proxy dual-client)" {
		t.Fatalf("XF-01 = %+v", entry)
	}
	for _, testCase := range GenerateAdapterMuxV2Cases("") {
		hasXF := testCase.AdapterMux.ExpectedFailure != nil
		if hasXF != (testCase.ID == "TCP-CX01") {
			t.Fatalf("%s expected failure = %v", testCase.ID, hasXF)
		}
	}
}

func TestAdapterMuxV2Classification(t *testing.T) {
	t.Parallel()

	byID := adapterMuxCaseMap(t)
	want := map[CellClass][]string{
		CellClassPassive:       {"AD02", "AD03", "AD05"},
		CellClassQuery:         {"AD01", "AD06", "AD07", "AD08", "AD10", "AD12", "PX01", "PX02", "PX03", "PX04", "PX05", "PX06", "PX07", "PX08", "PX12", "CX01"},
		CellClassAdapterAction: {"AD04", "AD09"},
		CellClassNotExecuted:   {"PX09", "PX10", "PX11"},
	}
	for class, invariants := range want {
		for _, row := range []string{"ENS", "ENH"} {
			for _, invariant := range invariants {
				if got := byID[row+"-"+invariant].AdapterMux.Class; got != class {
					t.Fatalf("%s-%s class = %s; want %s", row, invariant, got, class)
				}
			}
		}
	}
	for _, testCase := range byID {
		cell := testCase.AdapterMux
		switch {
		case strings.HasPrefix(cell.Row, "UART") && cell.Class != CellClassNotExecuted:
			t.Fatalf("%s class = %s; UART cells are never executed", testCase.ID, cell.Class)
		case (cell.Row == "UDP" || cell.Row == "TCP") && cell.Composition == CompositionMux &&
			cell.Class != CellClassAdapterAction && cell.Class != CellClassNotExecuted:
			t.Fatalf("%s class = %s; plain rows are W once they leave PL-01", testCase.ID, cell.Class)
		}
	}
	for id, class := range map[string]CellClass{
		"ENS-DS01": CellClassQuery, "ENH-DS01": CellClassQuery, "EBUSD-ES01": CellClassQuery,
		"UDP-DS01": CellClassAdapterAction, "TCP-DS01": CellClassAdapterAction,
	} {
		if got := byID[id].AdapterMux.Class; got != class {
			t.Fatalf("%s class = %s; want %s", id, got, class)
		}
	}
}

func TestAdapterMuxV2NorthboundClients(t *testing.T) {
	t.Parallel()

	byID := adapterMuxCaseMap(t)
	if got := byID["ENH-CX01"].AdapterMux.NorthboundClient; got != NorthboundEbusd {
		t.Fatalf("default CX01 client = %q; want ebusd", got)
	}
	explorer := GenerateAdapterMuxV2Cases(NorthboundVRCExplorer)
	for _, testCase := range explorer {
		cell := testCase.AdapterMux
		if cell.Invariant == "CX01" && cell.NorthboundClient != NorthboundVRCExplorer {
			t.Fatalf("%s client = %q; want vrc-explorer", testCase.ID, cell.NorthboundClient)
		}
		if cell.Invariant != "CX01" && cell.NorthboundClient == NorthboundVRCExplorer {
			t.Fatalf("%s uses vrc-explorer outside CX01", testCase.ID)
		}
		if cell.Invariant == "CX01" && testCase.UsesEbusd != cell.Executable() {
			t.Fatalf("%s uses_ebusd = %v; the explorer relay needs ebusd", testCase.ID, testCase.UsesEbusd)
		}
	}
	for _, invariant := range []string{"PX01", "PX02", "PX03", "PX04", "PX12"} {
		if got := byID["ENS-"+invariant].AdapterMux.NorthboundClient; got != NorthboundSyntheticENH {
			t.Fatalf("ENS-%s client = %q; want synthetic-enh", invariant, got)
		}
	}
	if got := byID["ENH-AD10"].AdapterMux.PairedCases; strings.Join(got, ",") != "ENH-DS01,EBUSD-ES01" {
		t.Fatalf("ENH-AD10 paired = %v", got)
	}
	if got := byID["ENS-AD10"].AdapterMux.PairedCases; strings.Join(got, ",") != "ENS-DS01" {
		t.Fatalf("ENS-AD10 paired = %v", got)
	}
}

func TestAdapterMuxV2TransportSelection(t *testing.T) {
	t.Parallel()

	all := GenerateAdapterMuxV2Cases("")
	selected, err := selectAdapterMuxCases(all, []string{"ENH"}, nil)
	if err != nil {
		t.Fatalf("select ENH: %v", err)
	}
	if len(selected) != 26 || selected[24].ID != "ENH-DS01" || selected[25].ID != "EBUSD-ES01" {
		t.Fatalf("ENH selection = %d cases, tail %s %s", len(selected), selected[len(selected)-2].ID, selected[len(selected)-1].ID)
	}
	if _, err := selectAdapterMuxCases(all, []string{"ENS"}, []string{"ENH-AD01"}); err == nil {
		t.Fatalf("a case outside the selected transports must be rejected")
	}
	if _, err := selectAdapterMuxCases(all, nil, []string{"T01"}); err == nil {
		t.Fatalf("an unknown case must be rejected")
	}
	got, err := selectAdapterMuxCases(all, nil, []string{"enh-ad01", "ENS-AD01"})
	if err != nil || len(got) != 2 || got[0].ID != "ENS-AD01" || got[1].ID != "ENH-AD01" {
		t.Fatalf("case filter = %v, %v; want inventory order", got, err)
	}

	for _, bad := range [][]string{{"EBUSD"}, {"uart"}, {" ", ""}} {
		if _, err := normalizeTransportTokens(bad); err == nil {
			t.Fatalf("transports %q must be rejected", bad)
		}
	}
	tokens, err := normalizeTransportTokens([]string{"ens", " ENH ", "ens"})
	if err != nil || strings.Join(tokens, ",") != "ENS,ENH" {
		t.Fatalf("normalize = %v, %v", tokens, err)
	}
}
