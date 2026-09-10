package main

import (
	"time"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
)

// semanticPrometheusDomains reads only detached accepted publications. In
// particular it never calls the Growatt observation path (which owns native
// reads and publication) or a Portal, GraphQL, MCP, or native operation.
func semanticPrometheusDomains(adapter *modbusadapter.Adapter, growatt *growattBMSRS485ProductionProvider, evse mcp.SemanticEVSEPrometheusProvider, pvEnabled, storageEnabled, evseEnabled bool, storageAsset string, at time.Time) []ebusgateway.SemanticMetricsDomain {
	// One instant is captured before either detached view is read. The domains
	// may be different revisions, but every individual tuple is coherent.
	domains := make([]ebusgateway.SemanticMetricsDomain, 0, 3)
	if pvEnabled {
		domain := ebusgateway.SemanticMetricsDomain{Name: "pv"}
		if adapter != nil {
			if current, ok := adapter.SemanticPVCurrentSingleAt(at); ok {
				domain = ebusgateway.SemanticMetricsDomain{Name: "pv", Snapshot: current.Snapshot, Evaluation: current.Evaluation, Projection: current.Projection, Available: true}
			}
		}
		domains = append(domains, domain)
	}
	if storageEnabled {
		domain := ebusgateway.SemanticMetricsDomain{Name: "storage"}
		if growatt != nil && growatt.storage != nil {
			if current, ok := growatt.storage.CurrentAt(storageAsset, at); ok {
				domain = ebusgateway.SemanticMetricsDomain{Name: "storage", Snapshot: current.Snapshot, Evaluation: current.Evaluation, Projection: current.Projection, Available: true}
			}
		}
		domains = append(domains, domain)
	}
	if evseEnabled {
		domain := ebusgateway.SemanticMetricsDomain{Name: "evse"}
		if evse != nil {
			if current, ok := evse.SemanticEVSECurrentAt(at); ok {
				domain = ebusgateway.SemanticMetricsDomain{Name: "evse", Snapshot: current.Snapshot, Evaluation: current.Evaluation, Projection: current.Projection, Available: true}
			}
		}
		domains = append(domains, domain)
	}
	return domains
}
