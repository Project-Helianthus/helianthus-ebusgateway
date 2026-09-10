package main

import (
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
)

// semanticPrometheusDomains reads only detached accepted publications. In
// particular it never calls the Growatt observation path (which owns native
// reads and publication) and has no Portal, GraphQL, or MCP dependency.
func semanticPrometheusDomains(adapter *modbusadapter.Adapter, growatt *growattBMSRS485ProductionProvider, storageAsset string, at time.Time) []ebusgateway.SemanticMetricsDomain {
	// One instant is captured before either detached view is read. The domains
	// may be different revisions, but every individual tuple is coherent.
	domains := []ebusgateway.SemanticMetricsDomain{{Name: "pv"}, {Name: "storage"}}
	if adapter != nil {
		if current, ok := adapter.SemanticPVCurrentSingleAt(at); ok {
			domains[0] = ebusgateway.SemanticMetricsDomain{Name: "pv", Snapshot: current.Snapshot, Evaluation: current.Evaluation, Projection: current.Projection, Available: true}
		}
	}
	if growatt != nil && growatt.storage != nil {
		if current, ok := growatt.storage.CurrentAt(storageAsset, at); ok {
			domains[1] = ebusgateway.SemanticMetricsDomain{Name: "storage", Snapshot: current.Snapshot, Evaluation: current.Evaluation, Projection: current.Projection, Available: true}
		}
	}
	return domains
}
