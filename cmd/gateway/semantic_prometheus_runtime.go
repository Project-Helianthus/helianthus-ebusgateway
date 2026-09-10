package main

import (
	"encoding/json"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/modbusadapter"
	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

// semanticPrometheusDomains reads only detached accepted publications. In
// particular it never calls the Growatt observation path (which owns native
// reads and publication) and has no Portal, GraphQL, or MCP dependency.
func semanticPrometheusDomains(adapter *modbusadapter.Adapter, growatt *growattBMSRS485ProductionProvider, storageAsset string) []ebusgateway.SemanticMetricsDomain {
	domains := []ebusgateway.SemanticMetricsDomain{{Name: "pv"}, {Name: "storage"}}
	if adapter != nil {
		if current, ok := adapter.SemanticPVCurrentSingle(); ok {
			domains[0] = ebusgateway.SemanticMetricsDomain{Name: "pv", Snapshot: current.Snapshot, Evaluation: current.Evaluation, Projection: current.Projection, Available: true}
		}
	}
	if growatt != nil && growatt.storage != nil {
		if raw, ok := growatt.storage.Current(storageAsset); ok {
			var current struct {
				Snapshot   semreg.Snapshot             `json:"snapshot"`
				Evaluation semreg.EvaluationView       `json:"evaluation"`
				Projection projection.ProjectionReport `json:"projection"`
			}
			if json.Unmarshal(raw, &current) == nil {
				domains[1] = ebusgateway.SemanticMetricsDomain{Name: "storage", Snapshot: current.Snapshot, Evaluation: current.Evaluation, Projection: current.Projection, Available: true}
			}
		}
	}
	return domains
}
