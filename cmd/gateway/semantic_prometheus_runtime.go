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
	domains := make([]ebusgateway.SemanticMetricsDomain, 0, 2)
	if adapter != nil {
		domain := ebusgateway.SemanticMetricsDomain{Name: "pv"}
		if current, ok := adapter.SemanticPVCurrentSingleAt(at); ok {
			domain = ebusgateway.SemanticMetricsDomain{Name: "pv", Snapshot: current.Snapshot, Evaluation: current.Evaluation, Projection: current.Projection, Available: true}
		}
		domains = append(domains, domain)
	}
	if growatt != nil && growatt.storage != nil {
		domain := ebusgateway.SemanticMetricsDomain{Name: "storage"}
		if current, ok := growatt.storage.CurrentAt(storageAsset, at); ok {
			domain = ebusgateway.SemanticMetricsDomain{Name: "storage", Snapshot: current.Snapshot, Evaluation: current.Evaluation, Projection: current.Projection, Available: true}
		}
		domains = append(domains, domain)
	}
	return domains
}
