package main

// semanticFamily names one composed device family on the semantic
// publication hub. A family groups every publisher whose declarations switch
// graph enforcement on together, whichever driver publishes them.
type semanticFamily string

// The composed families.
const (
	// semanticFamilyEVSE is the EVSE asset of the configured charger.
	semanticFamilyEVSE semanticFamily = "evse"
	// semanticFamilyPV is every PV asset: the SunSpec PV class and the
	// logger-attached PV class.
	semanticFamilyPV semanticFamily = "pv"
	// semanticFamilyStorage is the configured battery storage asset.
	semanticFamilyStorage semanticFamily = "storage"
	// semanticFamilyThermal is the eBUS boiler asset.
	semanticFamilyThermal semanticFamily = "thermal"
)

// semanticFamilyGraphEntry tells whether one family's declarations ask the
// hub for a graph-enforced kernel.
type semanticFamilyGraphEntry struct {
	family semanticFamily
	graph  bool
}

// semanticFamilyGraphTable is the one place that switches graph enforcement
// on for a family, in bytewise family order. Switching a family on changes
// what it may publish: its declarations then build their kernels with the
// element catalog, and every pack they register must be covered by it. A
// family that is off is reported not enforced, so its assets stay ungraphed.
// Every family is off until its publishers migrate.
var semanticFamilyGraphTable = [...]semanticFamilyGraphEntry{
	{family: semanticFamilyEVSE, graph: false},
	{family: semanticFamilyPV, graph: false},
	{family: semanticFamilyStorage, graph: false},
	{family: semanticFamilyThermal, graph: false},
}

// semanticFamilyGraph is the value a family's declarations read for their
// AssetSpec.Graph or ClassSpec.Graph. A family the table does not list is not
// graph-enforced.
func semanticFamilyGraph(family semanticFamily) bool {
	for _, entry := range semanticFamilyGraphTable {
		if entry.family == family {
			return entry.graph
		}
	}
	return false
}
