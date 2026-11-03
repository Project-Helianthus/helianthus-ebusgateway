package matrix

import (
	"fmt"
	"strings"
)

// The adaptermux-v2 suite is the two-axis AdapterMux conformance matrix of
// matrix/M7-adaptermux-conformance-matrix.md: 7 southbound transports × 24
// northbound invariants, plus 5 compatibility cells.

const (
	SuiteAdapterMuxV2 = "adaptermux-v2"

	// AdapterMuxV2InventoryVersion changes whenever a cell is added, removed
	// or renamed, or a cell's assertion, category or workload changes.
	AdapterMuxV2InventoryVersion = "1"
)

const (
	TopologyMuxInvariant   TopologyKind = "mux-invariant"
	TopologyMuxCoexistence TopologyKind = "mux-coexistence"
	TopologyDirectCompat   TopologyKind = "direct-compat"
	TopologyEbusdTCPCompat TopologyKind = "ebusd-tcp-compat"
)

const (
	TransportUARTENS Transport = "uart-ens"
	TransportUARTENH Transport = "uart-enh"
	TransportUARTRaw Transport = "uart-raw"
)

const (
	CompositionMux      = "mux"
	CompositionDirect   = "direct"
	CompositionEbusdTCP = "ebusd-tcp"

	FamilyAD     = "AD"
	FamilyPX     = "PX"
	FamilyCX     = "CX"
	FamilyCompat = "COMPAT"

	NorthboundNone         = "none"
	NorthboundEbusd        = "ebusd"
	NorthboundVRCExplorer  = "vrc-explorer"
	NorthboundSyntheticENH = "synthetic-enh"
)

// CellClass is the read-only versus bus-writing classification of a cell.
type CellClass string

const (
	// CellClassPassive listens only.
	CellClassPassive CellClass = "R"
	// CellClassQuery transmits identification or read requests only.
	CellClassQuery CellClass = "Q"
	// CellClassAdapterAction performs an adapter-side action and needs the
	// operator present and confirming at action time.
	CellClassAdapterAction CellClass = "W"
	// CellClassNotExecuted is never executed.
	CellClassNotExecuted CellClass = "-"
)

// ReservedCategory is a category under which a cell is written without
// executing any command.
type ReservedCategory struct {
	ID          string `json:"id"`
	Outcome     string `json:"outcome"`
	Reason      string `json:"reason"`
	GateAccepts bool   `json:"gate_accepts"`
}

var (
	CategoryBI02 = ReservedCategory{ID: "BI-02", Outcome: caseOutcomeBlocked, Reason: "uart_direct_hardware_absent", GateAccepts: true}
	CategoryPL01 = ReservedCategory{ID: "PL-01", Outcome: caseOutcomePlanned, Reason: "mux_southbound_plain_not_implemented", GateAccepts: true}
	CategoryPL02 = ReservedCategory{ID: "PL-02", Outcome: caseOutcomePlanned, Reason: "mux_local_responder_not_implemented", GateAccepts: true}
	CategoryPL03 = ReservedCategory{ID: "PL-03", Outcome: caseOutcomePlanned, Reason: "mux_raw_symbol_forwarding_not_implemented", GateAccepts: true}
	// CategoryRespec holds cells whose assertion or procedure is being
	// re-specified on #1378 (arbitration replaced by #1410; adapter-side
	// procedures not yet written). It is deliberately not a gate category:
	// a report carrying it cannot pass the gate until the cell is
	// re-specified and the inventory version is bumped.
	CategoryRespec = ReservedCategory{ID: "RS", Outcome: caseOutcomePlanned, Reason: "cell_awaiting_respecification", GateAccepts: false}
)

// ExpectedFailure is one entry of the suite's explicit expected-failure
// inventory.
type ExpectedFailure struct {
	ID     string `json:"id"`
	CaseID string `json:"case_id"`
	Reason string `json:"reason"`
}

// AdapterMuxV2ExpectedFailures is the complete expected-failure inventory of
// the suite (XF-01 only).
var AdapterMuxV2ExpectedFailures = []ExpectedFailure{
	{
		ID:     "XF-01",
		CaseID: "TCP-CX01",
		Reason: "southbound plain tcp under dual-client load reports no signal (carried from proxy dual-client)",
	},
}

type adapterMuxRow struct {
	token      string
	southbound Transport
	uart       bool
	plain      bool
}

// adapterMuxRows is axis 1 in matrix order.
var adapterMuxRows = []adapterMuxRow{
	{token: "ENS", southbound: TransportENS},
	{token: "ENH", southbound: TransportENH},
	{token: "UDP", southbound: TransportUDPPlain, plain: true},
	{token: "TCP", southbound: TransportTCPPlain, plain: true},
	{token: "UARTENS", southbound: TransportUARTENS, uart: true},
	{token: "UARTENH", southbound: TransportUARTENH, uart: true},
	{token: "UARTRAW", southbound: TransportUARTRaw, uart: true},
}

// AdapterMuxV2Transports lists the axis-1 tokens accepted by --transports.
func AdapterMuxV2Transports() []string {
	tokens := make([]string, 0, len(adapterMuxRows))
	for _, row := range adapterMuxRows {
		tokens = append(tokens, row.token)
	}
	return tokens
}

// InvariantSpec defines one axis-2 invariant: what is asserted, which client
// drives the workload, and what makes the cell pass.
type InvariantSpec struct {
	ID            string    `json:"id"`
	Family        string    `json:"family"`
	Assertion     string    `json:"assertion"`
	Class         CellClass `json:"class"`
	Northbound    string    `json:"northbound_client"`
	Workload      string    `json:"workload"`
	PassCriterion string    `json:"pass_criterion"`
	// localResponder marks PX09–PX11 (PL-02).
	localResponder bool
	// rawForwarding marks the cells that wait for raw-symbol forwarding (PL-03).
	rawForwarding bool
	// respec marks the cells awaiting re-specification.
	respec bool
}

const (
	smokeCriterion        = "the smoke command exits 0 with MATRIX_INVARIANT set to this invariant"
	externalWorkloadEbusd = "guarded ebusd command workload: state, identification scan of the workload target, 07 04 identification as a hex telegram"
	syntheticWorkload     = "guarded synthetic ENH contender: INIT, then identification telegrams (07 04) to the workload target, each through START/SEND with per-symbol echo verification"
)

// adapterMuxInvariants is axis 2 in matrix order: AD01–AD10, AD12, PX01–PX12, CX01.
var adapterMuxInvariants = []InvariantSpec{
	{ID: "AD01", Family: FamilyAD, Class: CellClassQuery, Northbound: NorthboundNone,
		Assertion:     "Active path sends/receives transactions",
		Workload:      "gateway session through the mux; the smoke command runs its active identification and read transactions",
		PassCriterion: smokeCriterion},
	{ID: "AD02", Family: FamilyAD, Class: CellClassPassive, Northbound: NorthboundNone,
		Assertion:     "Passive path zero self-echo",
		Workload:      "passive consumer of the mux stream while the gateway transacts; listens only",
		PassCriterion: smokeCriterion + "; the passive path reports zero self-echo frames"},
	{ID: "AD03", Family: FamilyAD, Class: CellClassPassive, Northbound: NorthboundNone,
		Assertion:     "Passive path reconstructs third-party traffic",
		Workload:      "passive consumer of the mux stream; listens only",
		PassCriterion: smokeCriterion + "; third-party frames are reconstructed"},
	{ID: "AD04", Family: FamilyAD, Class: CellClassAdapterAction, Northbound: NorthboundNone, respec: true,
		Assertion: "RESETTED propagates to all consumers"},
	{ID: "AD05", Family: FamilyAD, Class: CellClassPassive, Northbound: NorthboundEbusd, rawForwarding: true,
		Assertion: "ebusd connects, receives broadcasts"},
	{ID: "AD06", Family: FamilyAD, Class: CellClassQuery, Northbound: NorthboundEbusd,
		Assertion:     "ebusd sends START/SEND successfully",
		Workload:      externalWorkloadEbusd,
		PassCriterion: smokeCriterion + "; every guarded ebusd command answers without ERR"},
	{ID: "AD07", Family: FamilyAD, Class: CellClassQuery, Northbound: NorthboundEbusd, respec: true,
		Assertion: "Gateway and ebusd compete fairly at SYN"},
	{ID: "AD08", Family: FamilyAD, Class: CellClassQuery, Northbound: NorthboundEbusd, rawForwarding: true,
		Assertion: "Per-session echo suppression"},
	{ID: "AD09", Family: FamilyAD, Class: CellClassAdapterAction, Northbound: NorthboundNone, respec: true,
		Assertion: "Adapter disconnect triggers reconnection recovery"},
	{ID: "AD10", Family: FamilyAD, Class: CellClassQuery, Northbound: NorthboundNone,
		Assertion:     "Migration preserves semantics",
		Workload:      "AD01 workload on the mux, compared with the replaced compositions named in MATRIX_AD10_PAIRED_CASES of the same output directory",
		PassCriterion: smokeCriterion + "; values, presence, types, units, lineage and MCP/GraphQL parity are equivalent per pair"},
	{ID: "AD12", Family: FamilyAD, Class: CellClassQuery, Northbound: NorthboundNone, rawForwarding: true,
		Assertion: "No 0xA9/0xAA escape corruption"},
	{ID: "PX01", Family: FamilyPX, Class: CellClassQuery, Northbound: NorthboundSyntheticENH,
		Assertion:     "stale STARTED absorb succeeds when matching result arrives in absorb window",
		Workload:      syntheticWorkload,
		PassCriterion: smokeCriterion + "; every contender telegram completes with verified echoes"},
	{ID: "PX02", Family: FamilyPX, Class: CellClassQuery, Northbound: NorthboundSyntheticENH,
		Assertion:     "stale STARTED absorb expires via bounded fail path",
		Workload:      syntheticWorkload,
		PassCriterion: smokeCriterion + "; every contender telegram completes with verified echoes"},
	{ID: "PX03", Family: FamilyPX, Class: CellClassQuery, Northbound: NorthboundSyntheticENH,
		Assertion:     "SYN while waiting for command ACK reopens arbitration immediately",
		Workload:      syntheticWorkload,
		PassCriterion: smokeCriterion + "; every contender telegram completes with verified echoes"},
	{ID: "PX04", Family: FamilyPX, Class: CellClassQuery, Northbound: NorthboundSyntheticENH,
		Assertion:     "SYN while waiting for target response bytes reopens arbitration immediately",
		Workload:      syntheticWorkload,
		PassCriterion: smokeCriterion + "; every contender telegram completes with verified echoes"},
	{ID: "PX05", Family: FamilyPX, Class: CellClassQuery, Northbound: NorthboundSyntheticENH, respec: true,
		Assertion: "same-boundary competition is resolved by FIFO registration order, not initiator priority"},
	{ID: "PX06", Family: FamilyPX, Class: CellClassQuery, Northbound: NorthboundSyntheticENH, respec: true,
		Assertion: "queued higher initiator keeps its FIFO turn when a lower initiator arrives before the next round closes"},
	{ID: "PX07", Family: FamilyPX, Class: CellClassQuery, Northbound: NorthboundSyntheticENH, respec: true,
		Assertion: "requeue-after-timeout receives a new FIFO position and cannot steal priority from an older contender"},
	{ID: "PX08", Family: FamilyPX, Class: CellClassQuery, Northbound: NorthboundSyntheticENH, respec: true,
		Assertion: "equal-initiator FIFO ordering is preserved"},
	{ID: "PX09", Family: FamilyPX, Class: CellClassNotExecuted, Northbound: NorthboundNone, localResponder: true,
		Assertion: "local target observes request only from echoed RECEIVED path, never from owner SEND intent"},
	{ID: "PX10", Family: FamilyPX, Class: CellClassNotExecuted, Northbound: NorthboundNone, localResponder: true,
		Assertion: "local emulated target response inside responder window remains coherent"},
	{ID: "PX11", Family: FamilyPX, Class: CellClassNotExecuted, Northbound: NorthboundNone, localResponder: true,
		Assertion: "late responder bytes are rejected and counted"},
	{ID: "PX12", Family: FamilyPX, Class: CellClassQuery, Northbound: NorthboundSyntheticENH,
		Assertion:     "non-owner/non-responder sends are rejected during active transaction",
		Workload:      syntheticWorkload,
		PassCriterion: smokeCriterion + "; every contender telegram completes with verified echoes"},
	{ID: "CX01", Family: FamilyCX, Class: CellClassQuery, Northbound: "",
		Assertion:     "Gateway and an external ENH client (ebusd or VRC Explorer) simultaneously through the mux",
		Workload:      "smoke command for the gateway while the external client selected by --external-client runs its guarded workload through the mux (ebusd command workload, or a VRC Explorer scan through the guarded ebusd command relay)",
		PassCriterion: smokeCriterion + "; the external client workload completes with no guard refusal and no ERR answer"},
}

type compatSpec struct {
	id         string
	row        string
	southbound Transport
	kind       TopologyKind
	class      CellClass
	usesEbusd  bool
}

// adapterMuxCompatCells is the compatibility block in matrix order. Each
// runs the smoke command on the hardware setup of its row; EBUSD-ES01 runs on
// the ENH setup.
var adapterMuxCompatCells = []compatSpec{
	{id: "ENS-DS01", row: "ENS", southbound: TransportENS, kind: TopologyDirectCompat, class: CellClassQuery},
	{id: "ENH-DS01", row: "ENH", southbound: TransportENH, kind: TopologyDirectCompat, class: CellClassQuery},
	{id: "UDP-DS01", row: "UDP", southbound: TransportUDPPlain, kind: TopologyDirectCompat, class: CellClassAdapterAction},
	{id: "TCP-DS01", row: "TCP", southbound: TransportTCPPlain, kind: TopologyDirectCompat, class: CellClassAdapterAction},
	{id: "EBUSD-ES01", row: "ENH", southbound: TransportEbusdTCP, kind: TopologyEbusdTCPCompat, class: CellClassQuery, usesEbusd: true},
}

// AdapterMuxCell carries the adaptermux-v2 identity of a topology case.
type AdapterMuxCell struct {
	// Row is the axis-1 token whose hardware setup runs the cell.
	Row              string            `json:"row"`
	Southbound       Transport         `json:"southbound_transport"`
	Invariant        string            `json:"invariant"`
	Family           string            `json:"invariant_family"`
	Composition      string            `json:"composition"`
	NorthboundClient string            `json:"northbound_client"`
	Class            CellClass         `json:"class"`
	Assertion        string            `json:"assertion"`
	Workload         string            `json:"workload,omitempty"`
	PassCriterion    string            `json:"pass_criterion,omitempty"`
	Reserved         *ReservedCategory `json:"reserved,omitempty"`
	ExpectedFailure  *ExpectedFailure  `json:"expected_failure,omitempty"`
	PairedCases      []string          `json:"paired_cases,omitempty"`
}

// Executable reports whether the runner executes commands for the cell.
func (cell AdapterMuxCell) Executable() bool {
	return cell.Reserved == nil
}

// reservedCategoryFor applies the category precedence BI-02, PL-01, PL-02,
// PL-03, then re-specification.
func reservedCategoryFor(row adapterMuxRow, spec InvariantSpec) *ReservedCategory {
	var category ReservedCategory
	switch {
	case row.uart:
		category = CategoryBI02
	case row.plain:
		category = CategoryPL01
	case spec.localResponder:
		category = CategoryPL02
	case spec.rawForwarding:
		category = CategoryPL03
	case spec.respec:
		category = CategoryRespec
	default:
		return nil
	}
	return &category
}

func expectedFailureFor(caseID string) *ExpectedFailure {
	for _, entry := range AdapterMuxV2ExpectedFailures {
		if entry.CaseID == caseID {
			found := entry
			return &found
		}
	}
	return nil
}

// GenerateAdapterMuxV2Cases returns the full adaptermux-v2 inventory in
// matrix order. externalClient selects the CX01 client (ebusd when empty).
func GenerateAdapterMuxV2Cases(externalClient string) []TopologyCase {
	if externalClient == "" {
		externalClient = NorthboundEbusd
	}
	cases := make([]TopologyCase, 0, len(adapterMuxRows)*len(adapterMuxInvariants)+len(adapterMuxCompatCells))
	for _, row := range adapterMuxRows {
		for _, spec := range adapterMuxInvariants {
			caseID := row.token + "-" + spec.ID
			northbound := spec.Northbound
			kind := TopologyMuxInvariant
			if spec.Family == FamilyCX {
				kind = TopologyMuxCoexistence
				northbound = externalClient
			}
			cell := &AdapterMuxCell{
				Row:              row.token,
				Southbound:       row.southbound,
				Invariant:        spec.ID,
				Family:           spec.Family,
				Composition:      CompositionMux,
				NorthboundClient: northbound,
				Class:            cellClass(row, spec),
				Assertion:        spec.Assertion,
				Workload:         spec.Workload,
				PassCriterion:    spec.PassCriterion,
				Reserved:         reservedCategoryFor(row, spec),
				ExpectedFailure:  expectedFailureFor(caseID),
			}
			if cell.Reserved != nil {
				cell.Workload = ""
				cell.PassCriterion = ""
			}
			if spec.ID == "AD10" {
				cell.PairedCases = ad10PairedCases(row.token)
			}
			cases = append(cases, TopologyCase{
				ID:               caseID,
				Kind:             kind,
				GatewayTransport: row.southbound,
				UsesEbusd:        cell.Executable() && usesEbusd(northbound),
				AdapterMux:       cell,
			})
		}
	}
	for _, compat := range adapterMuxCompatCells {
		composition := CompositionDirect
		invariant := "DS01"
		assertion := "gateway direct transport without the mux; the gateway is the sole owner"
		if compat.kind == TopologyEbusdTCPCompat {
			composition = CompositionEbusdTCP
			invariant = "ES01"
			assertion = "gateway over ebusd-tcp; ebusd reaches the adapter over ENH"
		}
		cell := &AdapterMuxCell{
			Row:              compat.row,
			Southbound:       compat.southbound,
			Invariant:        invariant,
			Family:           FamilyCompat,
			Composition:      composition,
			NorthboundClient: NorthboundNone,
			Class:            compat.class,
			Assertion:        assertion,
			Workload:         "the smoke command, as the former direct and ebusd-tcp cases",
			PassCriterion:    "the smoke command exits 0",
			ExpectedFailure:  expectedFailureFor(compat.id),
		}
		cases = append(cases, TopologyCase{
			ID:               compat.id,
			Kind:             compat.kind,
			GatewayTransport: compat.southbound,
			EbusdTransport:   ebusdTransportFor(compat),
			UsesEbusd:        compat.usesEbusd,
			AdapterMux:       cell,
		})
	}
	return cases
}

// usesEbusd reports whether a northbound client needs ebusd running: the
// ebusd workload talks to it, and the VRC Explorer scan reaches the mux
// through ebusd behind the guarded command relay.
func usesEbusd(northbound string) bool {
	return northbound == NorthboundEbusd || northbound == NorthboundVRCExplorer
}

func ebusdTransportFor(compat compatSpec) Transport {
	if compat.usesEbusd {
		return TransportENH
	}
	return ""
}

// cellClass returns the classification a cell carries: UART rows and the
// local-responder cells are never executed; the plain rows need an adapter
// protocol change once they leave PL-01.
func cellClass(row adapterMuxRow, spec InvariantSpec) CellClass {
	switch {
	case row.uart, spec.Class == CellClassNotExecuted:
		return CellClassNotExecuted
	case row.plain:
		return CellClassAdapterAction
	default:
		return spec.Class
	}
}

// ad10PairedCases names the replaced compositions an AD10 cell compares
// with: S-DS01 on every row that has one, and EBUSD-ES01 for ENH.
func ad10PairedCases(row string) []string {
	switch row {
	case "ENS", "UDP", "TCP":
		return []string{row + "-DS01"}
	case "ENH":
		return []string{"ENH-DS01", "EBUSD-ES01"}
	default:
		return nil
	}
}

// normalizeTransportTokens upper-cases and validates --transports tokens.
func normalizeTransportTokens(tokens []string) ([]string, error) {
	if len(tokens) == 0 {
		return nil, nil
	}
	known := make(map[string]struct{}, len(adapterMuxRows))
	for _, row := range adapterMuxRows {
		known[row.token] = struct{}{}
	}
	seen := make(map[string]struct{}, len(tokens))
	normalized := make([]string, 0, len(tokens))
	for _, token := range tokens {
		value := strings.ToUpper(strings.TrimSpace(token))
		if value == "" {
			continue
		}
		if _, ok := known[value]; !ok {
			return nil, fmt.Errorf("unknown transport %q (allowed: %s; EBUSD-ES01 runs with ENH)", token, strings.Join(AdapterMuxV2Transports(), ", "))
		}
		if _, dup := seen[value]; dup {
			continue
		}
		seen[value] = struct{}{}
		normalized = append(normalized, value)
	}
	if len(normalized) == 0 {
		return nil, fmt.Errorf("--transports names no transport")
	}
	return normalized, nil
}

// selectAdapterMuxCases filters the inventory by transport rows and case IDs.
// An unknown case ID, or one outside the selected rows, is an error.
func selectAdapterMuxCases(all []TopologyCase, transports []string, includeIDs []string) ([]TopologyCase, error) {
	rows := make(map[string]struct{}, len(transports))
	for _, token := range transports {
		rows[token] = struct{}{}
	}
	inRows := func(testCase TopologyCase) bool {
		if len(rows) == 0 {
			return true
		}
		_, ok := rows[testCase.AdapterMux.Row]
		return ok
	}

	if len(includeIDs) == 0 {
		selected := make([]TopologyCase, 0, len(all))
		for _, testCase := range all {
			if inRows(testCase) {
				selected = append(selected, testCase)
			}
		}
		return selected, nil
	}

	byID := make(map[string]TopologyCase, len(all))
	for _, testCase := range all {
		byID[testCase.ID] = testCase
	}
	wanted := make(map[string]struct{}, len(includeIDs))
	for _, id := range includeIDs {
		caseID := strings.ToUpper(strings.TrimSpace(id))
		testCase, ok := byID[caseID]
		if !ok {
			return nil, fmt.Errorf("unknown %s case %q", SuiteAdapterMuxV2, id)
		}
		if !inRows(testCase) {
			return nil, fmt.Errorf("case %s does not belong to the selected transports %s", caseID, strings.Join(transports, ","))
		}
		wanted[caseID] = struct{}{}
	}
	selected := make([]TopologyCase, 0, len(wanted))
	for _, testCase := range all {
		if _, ok := wanted[testCase.ID]; ok {
			selected = append(selected, testCase)
		}
	}
	return selected, nil
}
