package main

import (
	"encoding/json"
	"sort"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

type thermalMaintenanceSelector struct {
	PB, SB, Opcode, OperationType, Group, Instance byte
	Register                                       uint16
	Direction                                      string
}

type thermalMaintenanceIdentity struct {
	Asset                                semreg.AssetID
	Service                              semreg.ServiceInstanceID
	Path                                 semreg.SourcePathRef
	BindingRevision, AssociationRevision semreg.Uint64
	Selector                             thermalMaintenanceSelector
}

// These records are supplied by the native owner, including retained values and
// unsuccessful acquisitions. The mapper neither acquires nor refreshes them.
type thermalMaintenanceObservation struct {
	Identity                                           thermalMaintenanceIdentity
	Value                                              *bool
	RawValue, Request, Reply                           []byte
	Validity                                           semreg.Validity
	Freshness                                          semreg.Freshness
	Qualification                                      semreg.Qualification
	EvidenceStatus                                     string // Proven, Hypothesis, or Unknown; separate from qualification.
	Evidence                                           []semreg.EvidenceRef
	EvidenceRevision, DecoderRevision, MappingRevision string
	ReceivedAt                                         semreg.TimePoint
	ReceiptMonotonic                                   semreg.MonotonicPoint
}

type thermalMaintenanceInput struct {
	Bindings          []thermalMaintenanceIdentity
	Observations      []thermalMaintenanceObservation
	Requested         []projection.RequestedItem
	Independent       []projection.ProjectionDisposition
	EvaluatedAt       semreg.TimePoint
	EvaluateMonotonic semreg.MonotonicPoint
}

type thermalMaintenanceNative struct {
	Observation    thermalMaintenanceObservation
	CurrentBinding bool
	Conflict       bool
	Loss           string
}

type thermalMaintenanceResult struct {
	Native            []thermalMaintenanceNative
	Dispositions      []projection.ProjectionDisposition
	Facts             []semreg.FactEnvelope
	EvaluatedAt       semreg.TimePoint
	EvaluateMonotonic semreg.MonotonicPoint
}

// mapThermalMaintenance implements the native retention/withholding contract:
// https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/maintenance-system-state-cutover.md
// Bindings are caller-owned current identities, not qualification proof. No
// combination of these Boolean observations establishes a canonical fact.
func mapThermalMaintenance(input thermalMaintenanceInput) (thermalMaintenanceResult, error) {
	// Detach nested SemReg values as well as native buffers in both directions.
	encoded, err := json.Marshal(input)
	if err != nil {
		return thermalMaintenanceResult{}, err
	}
	var in thermalMaintenanceInput
	if err := json.Unmarshal(encoded, &in); err != nil {
		return thermalMaintenanceResult{}, err
	}
	out := thermalMaintenanceResult{
		Native:       make([]thermalMaintenanceNative, 0, len(in.Observations)),
		Dispositions: []projection.ProjectionDisposition{}, Facts: []semreg.FactEnvelope{},
		EvaluatedAt: in.EvaluatedAt, EvaluateMonotonic: in.EvaluateMonotonic,
	}
	for _, observation := range in.Observations {
		native := thermalMaintenanceNative{Observation: observation, Loss: "unresolved_native_meaning"}
		for _, binding := range in.Bindings {
			if binding == observation.Identity {
				native.CurrentBinding = true
			}
		}
		s := observation.Identity.Selector
		supported := s.PB == 0xb5 && s.SB == 0x24 && s.Opcode == 0x02 &&
			s.OperationType == 0 && s.Group == 0 && s.Instance == 0 &&
			(s.Register == 0x0007 || s.Register == 0x0096) && s.Direction == "target_to_initiator"
		switch {
		case !native.CurrentBinding:
			native.Loss = "different_binding"
		case !supported:
			native.Loss = "unsupported_selector"
		case observation.Value == nil:
			native.Loss = "missing_decoded_value"
		case observation.Validity != semreg.ValidityGood:
			native.Loss = "invalid_native_value"
		case observation.Freshness != semreg.FreshnessFresh:
			native.Loss = "freshness_withheld"
		case observation.Qualification != semreg.QualificationQualified:
			native.Loss = "qualification_withheld"
		}
		for _, other := range in.Observations {
			if observation.Identity == other.Identity && observation.Value != nil && other.Value != nil &&
				observation.Validity == semreg.ValidityGood && other.Validity == semreg.ValidityGood && *observation.Value != *other.Value {
				native.Conflict = true
			}
		}
		out.Native = append(out.Native, native)
	}
	known := map[semreg.DefinitionID]bool{}
	for _, field := range thermal.Metadata().Fields {
		known[field.Ref.ID] = true
	}
	seen := map[projection.RequestedItem]bool{}
	for _, request := range in.Requested {
		if seen[request] {
			continue
		}
		seen[request] = true
		d := projection.ProjectionDisposition{Kind: request.Kind, ItemID: request.ItemID, Outcome: projection.ProjectionUnsupported}
		if request.Kind == projection.ItemFact && known[request.ItemID] {
			d.Outcome = projection.ProjectionWithheld
		}
		// Independent dispositions already account for the shared definition item.
		// Source-level native loss must never replace that accounting.
		for _, independent := range in.Independent {
			if independent.Kind == request.Kind && independent.ItemID == request.ItemID {
				d = independent
				break
			}
		}
		out.Dispositions = append(out.Dispositions, d)
	}
	sort.Slice(out.Dispositions, func(i, j int) bool {
		a, b := out.Dispositions[i], out.Dispositions[j]
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return a.ItemID < b.ItemID
	})
	return out, nil
}
