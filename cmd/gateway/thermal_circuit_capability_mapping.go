package main

import (
	"encoding/json"
	"sort"
	"strconv"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

// These adapter records are caller-supplied evidence, not discovery or topology.
// Contract: https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/circuit-capability-semreg-cutover.md
type thermalCircuitIdentity struct {
	Asset               semreg.AssetID
	Service             semreg.ServiceInstanceID
	Capability          semreg.CapabilityInstanceID
	Source              semreg.SourceID
	Epoch               semreg.SourceEpochID
	Generation          semreg.Uint64
	Binding             semreg.NativeBindingID
	BindingRevision     semreg.Uint64
	AssociationRevision semreg.Uint64
	ControllerContext   string
	Family              string
}

type thermalCircuitSelector struct {
	PB, SB, Opcode, OperationType, Group, Instance byte
	Register                                       uint16
	Initiator, Destination                         byte
	RequestDirection, ValueDirection               string
}

type thermalCircuitObservation struct {
	Identity       thermalCircuitIdentity
	Selector       thermalCircuitSelector
	Raw            []byte
	Decoded        uint16
	Validity       semreg.Validity
	Qualification  semreg.Qualification
	EvidenceStatus string // Proven, Hypothesis or Unknown; never qualification.
	Evidence       semreg.EvidenceRef
	ReceivedAt     semreg.TimePoint
	Receipt        semreg.MonotonicPoint
	Present        bool
	Historical     bool
}

type thermalCircuitRole struct {
	Identity                                             thermalCircuitIdentity
	Selector                                             thermalCircuitSelector
	Role                                                 string // ordinary or virtual_water; never inferred from type or II.
	Qualification                                        semreg.Qualification
	Presence, ReadBinding, FamilyApplicable, Unambiguous bool
	QualificationRef, RoleEvidenceRef                    string
	ReceivedAt                                           semreg.TimePoint
	Receipt                                              semreg.MonotonicPoint
}

type thermalCircuitCandidate struct {
	Identity     thermalCircuitIdentity
	TypeSelector thermalCircuitSelector
	Role         thermalCircuitRole
}

type thermalCircuitMappingInput struct {
	Candidates        []thermalCircuitCandidate
	Observations      []thermalCircuitObservation
	Requested         []projection.RequestedItem
	EvaluatedAt       semreg.TimePoint
	EvaluateMonotonic semreg.MonotonicPoint
	FreshForNS        semreg.Uint64
}

type thermalCircuitSourceOutcome struct {
	Candidate             thermalCircuitCandidate
	Service               *semreg.ServiceInstance
	Capability            *semreg.CapabilityInstance
	Loss                  string
	TypeLoss, CoolingLoss string
}

type thermalCircuitMappingResult struct {
	Sources      []thermalCircuitSourceOutcome
	Native       []thermalCircuitObservation
	Dispositions []projection.ProjectionDisposition
	EvaluatedAt  semreg.TimePoint
}

// mapThermalCircuitCapabilities is a detached, offline candidate mapper. It does
// not publish, acquire, qualify, or replace native last-known-good observations.
// Native field loss is deliberately separate from requested-item accounting.
func mapThermalCircuitCapabilities(in thermalCircuitMappingInput) thermalCircuitMappingResult {
	out := thermalCircuitMappingResult{EvaluatedAt: in.EvaluatedAt}
	key := func(v any) string { b, _ := json.Marshal(v); return string(b) }
	fresh := func(receipt semreg.MonotonicPoint) bool {
		r, er := strconv.ParseUint(string(receipt.Nanoseconds), 10, 64)
		e, ee := strconv.ParseUint(string(in.EvaluateMonotonic.Nanoseconds), 10, 64)
		limit, el := strconv.ParseUint(string(in.FreshForNS), 10, 64)
		return er == nil && ee == nil && el == nil && limit > 0 && receipt.ClockEpochID != "" &&
			receipt.ClockEpochID == in.EvaluateMonotonic.ClockEpochID && e >= r && e-r <= limit
	}
	for _, o := range in.Observations {
		o.Raw = append([]byte(nil), o.Raw...)
		currentIdentity := false
		for _, c := range in.Candidates {
			if c.Identity == o.Identity {
				currentIdentity = true
			}
		}
		if !currentIdentity {
			o.Historical = true
		}
		out.Native = append(out.Native, o)
	}
	sort.Slice(out.Native, func(i, j int) bool { return key(out.Native[i]) < key(out.Native[j]) })
	// No fabricated definitions: resolve the existing read vocabulary from its owner.
	metadata := thermal.Metadata()
	for _, c := range in.Candidates {
		s := thermalCircuitSourceOutcome{Candidate: c}
		read := func(selector thermalCircuitSelector) (*thermalCircuitObservation, string) {
			var matches []thermalCircuitObservation
			for _, o := range out.Native {
				if o.Identity == c.Identity && o.Selector == selector && !o.Historical && o.Present {
					matches = append(matches, o)
				}
			}
			if len(matches) == 0 {
				return nil, "missing"
			}
			if len(matches) != 1 {
				return nil, "conflict"
			}
			o := matches[0]
			if o.Validity != semreg.ValidityGood {
				return nil, "invalid"
			}
			if o.Qualification != semreg.QualificationQualified {
				return nil, "unqualified"
			}
			if !fresh(o.Receipt) {
				return nil, "stale"
			}
			return &o, ""
		}
		t, typeLoss := read(c.TypeSelector)
		coolSelector := c.TypeSelector
		coolSelector.Register = 6
		cool, coolingLoss := read(coolSelector)
		if t != nil && (len(t.Raw) != 2 || uint16(t.Raw[0])|uint16(t.Raw[1])<<8 != t.Decoded) {
			t, typeLoss = nil, "invalid"
		}
		if cool != nil && (len(cool.Raw) != 1 || uint16(cool.Raw[0]) != cool.Decoded || cool.Decoded > 1) {
			cool, coolingLoss = nil, "invalid"
		}
		if typeLoss == "" {
			typeLoss = "native_only"
			if t.Decoded > 4 {
				typeLoss = "unknown_code"
			}
		}
		if coolingLoss == "" {
			coolingLoss = "native_only"
		}
		s.TypeLoss, s.CoolingLoss = typeLoss, coolingLoss
		id, sel, role := c.Identity, c.TypeSelector, c.Role
		switch {
		case id.Asset == "" || id.Service == "" || id.Capability == "" || id.Source == "" || id.Epoch == "" || id.Generation == "" || id.Binding == "" || id.BindingRevision == "" || id.AssociationRevision == "" || id.ControllerContext == "" || id.Family == "":
			s.Loss = "identity"
		case sel.PB != 0xb5 || sel.SB != 0x24 || sel.Opcode != 2 || sel.OperationType != 0 || sel.Group != 2 || sel.Register != 2 || sel.RequestDirection != "initiator_to_target" || sel.ValueDirection != "target_to_initiator":
			s.Loss = "selector"
		case role.Identity != id || role.Selector != sel:
			s.Loss = "role_provenance"
		case role.Qualification != semreg.QualificationQualified || !role.Presence || !role.ReadBinding || !role.FamilyApplicable || !role.Unambiguous || role.QualificationRef == "" || role.RoleEvidenceRef == "":
			s.Loss = "unqualified_role"
		case !fresh(role.Receipt):
			s.Loss = "stale_role"
		case role.Role != "ordinary" && role.Role != "virtual_water":
			s.Loss = "unknown_role"
		case role.Role == "ordinary" && sel.Instance == 9:
			s.Loss = "virtual_not_physical"
		case t == nil:
			s.Loss = typeLoss
		case role.Role == "ordinary" && t.Decoded == 0:
			s.Loss = "inactive"
		}
		if s.Loss == "" {
			serviceID, capID := semreg.DefinitionID("thermal.service.circuit"), semreg.DefinitionID("thermal.capability.read.circuit")
			if role.Role == "virtual_water" {
				serviceID, capID = "thermal.service.dhw", "thermal.capability.read.dhw"
			}
			var serviceRef, capRef semreg.DefinitionRef
			for _, def := range metadata.Services {
				if def.Ref.ID == serviceID {
					serviceRef = def.Ref
				}
			}
			for _, def := range metadata.Capabilities {
				if def.Ref.ID == capID && def.Service.ID == serviceID {
					capRef = def.Ref
				}
			}
			if serviceRef.ID == "" || capRef.ID == "" {
				s.Loss = "unsupported_definition"
			} else {
				s.Service = &semreg.ServiceInstance{InstanceID: id.Service, AssetID: id.Asset, Definition: serviceRef, BindingID: id.Binding, SourceEpochID: id.Epoch, DriverGeneration: id.Generation, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Revision: id.BindingRevision}
				s.Capability = &semreg.CapabilityInstance{InstanceID: id.Capability, AssetID: id.Asset, ServiceInstance: id.Service, Definition: capRef, BindingID: id.Binding, SourceEpochID: id.Epoch, DriverGeneration: id.Generation, Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Revision: id.BindingRevision}
			}
		}
		out.Sources = append(out.Sources, s)
	}
	// Competing admitted owners of the same opaque service are unresolved. A
	// merely absent source is not a competing owner and cannot erase a sibling.
	conflicts := make([]bool, len(out.Sources))
	for i, a := range out.Sources {
		for j, b := range out.Sources {
			if i != j && a.Service != nil && b.Service != nil && a.Service.AssetID == b.Service.AssetID && a.Service.InstanceID == b.Service.InstanceID {
				conflicts[i] = true
			}
		}
	}
	for i := range out.Sources {
		if conflicts[i] {
			out.Sources[i].Service, out.Sources[i].Capability, out.Sources[i].Loss = nil, nil, "conflict"
		}
	}
	sort.Slice(out.Sources, func(i, j int) bool { return key(out.Sources[i]) < key(out.Sources[j]) })
	requested := append([]projection.RequestedItem(nil), in.Requested...)
	sort.Slice(requested, func(i, j int) bool {
		if requested[i].Kind != requested[j].Kind {
			return requested[i].Kind < requested[j].Kind
		}
		return requested[i].ItemID < requested[j].ItemID
	})
	for i, r := range requested {
		if i > 0 && r == requested[i-1] {
			continue
		}
		d := projection.ProjectionDisposition{Kind: r.Kind, ItemID: r.ItemID, Outcome: projection.ProjectionUnsupported}
		if r.Kind == projection.ItemCapability && (r.ItemID == "thermal.capability.read.circuit" || r.ItemID == "thermal.capability.read.dhw") {
			d.Outcome = projection.ProjectionWithheld
			for _, s := range out.Sources {
				if s.Capability != nil && s.Capability.Definition.ID == r.ItemID {
					d.Outcome = projection.ProjectionExact
				}
			}
		}
		out.Dispositions = append(out.Dispositions, d)
	}
	return out
}
