package main

import (
	"strconv"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
)

// All types in this adapter are local to the zone-mode field group. Qualification
// is supplied by the native owner; this mapper never establishes a binding.
type thermalZoneModeIdentity struct {
	Asset               semreg.AssetID
	Service             semreg.ServiceInstanceID
	Source              semreg.SourceID
	Epoch               semreg.SourceEpochID
	Generation          semreg.Uint64
	Binding             semreg.NativeBindingID
	BindingRevision     semreg.Uint64
	AssociationRevision semreg.Uint64
	SemanticRevision    semreg.Uint64
}

type thermalZoneModeSelector struct {
	PB, SB, Operation, Group, Instance byte
	Opcode                             uint16
	Register                           uint16
	Direction                          string
}

type thermalZoneModeObservation struct {
	Identity         thermalZoneModeIdentity
	Selector         thermalZoneModeSelector
	Number           *uint16
	Boolean          *bool
	Qualification    semreg.Qualification
	Validity         semreg.Validity
	Freshness        semreg.Freshness
	ReceivedAt       semreg.TimePoint
	ReceiptMonotonic semreg.MonotonicPoint
	Request, Reply   []byte
	Evidence         []semreg.EvidenceRef
}

type thermalZoneModeContext struct {
	Identity                      thermalZoneModeIdentity
	Zone                          string
	ZoneInstance, CircuitInstance byte
	// These independently supplied decisions cover native association and the
	// heating-versus-whole-zone equivalence, including override scope.
	AssociationQualified, EquivalenceQualified bool
	Evaluation                                 semreg.EvaluationContext
	FreshForNS                                 uint64
}

type thermalZoneModeInput struct {
	Context thermalZoneModeContext
	// O, S, A, T, C in that order; A is the native circuit assignment.
	Observations [5]*thermalZoneModeObservation
}

type thermalZoneModeLoss struct{ Component, Detail string }

type thermalZoneModeResult struct {
	Key     semreg.FactKey
	Value   *semreg.Value
	Context thermalZoneModeContext
	Native  [5]*thermalZoneModeObservation
	// Source losses are not requested-definition dispositions. The caller must
	// aggregate eligible candidates before accounting for a shared definition.
	Losses []thermalZoneModeLoss
}

// mapThermalZoneMode implements the conditional auto/off rows of
// https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/zone-mode-semreg-cutover.md
// It is a pure candidate mapping, with no publication, retention or admission.
func mapThermalZoneMode(in thermalZoneModeInput) thermalZoneModeResult {
	c := in.Context
	r := thermalZoneModeResult{Context: c}
	loss := func(component, detail string) { r.Losses = append(r.Losses, thermalZoneModeLoss{component, detail}) }
	loss("preset", "preset, schedule, manual, override and allowed-mode semantics are unrepresented")
	id := c.Identity
	ready := id.Asset != "" && id.Service != "" && id.Source != "" && id.Epoch != "" && id.Generation != "" && id.Binding != "" && id.BindingRevision != "" && id.AssociationRevision != "" && id.SemanticRevision != "" && c.Zone != "" && c.AssociationQualified && c.EquivalenceQualified
	if !ready {
		loss("context", "missing identity or independently qualified association/equivalence")
	}
	selectors := [5]thermalZoneModeSelector{
		{0xB5, 0x24, 2, 3, c.ZoneInstance, 0xB524, 6, "target_to_initiator"},
		{0xB5, 0x24, 2, 3, c.ZoneInstance, 0xB524, 0xE, "target_to_initiator"},
		{0xB5, 0x24, 2, 3, c.ZoneInstance, 0xB524, 0x1C, "target_to_initiator"},
		{0xB5, 0x24, 2, 2, c.CircuitInstance, 0xB524, 2, "target_to_initiator"},
		{0xB5, 0x24, 2, 2, c.CircuitInstance, 0xB524, 6, "target_to_initiator"},
	}
	names := [5]string{"O", "S", "A", "T", "C"}
	for i, observation := range in.Observations {
		if observation == nil {
			ready = false
			loss(names[i], "missing component")
			continue
		}
		o := *observation
		o.Request = append([]byte(nil), o.Request...)
		o.Reply = append([]byte(nil), o.Reply...)
		o.Evidence = append([]semreg.EvidenceRef(nil), o.Evidence...)
		if o.Number != nil {
			v := *o.Number
			o.Number = &v
		}
		if o.Boolean != nil {
			v := *o.Boolean
			o.Boolean = &v
		}
		r.Native[i] = &o
		received, e1 := strconv.ParseUint(string(o.ReceiptMonotonic.Nanoseconds), 10, 64)
		evaluated, e2 := strconv.ParseUint(string(c.Evaluation.EvaluateMonotonic.Nanoseconds), 10, 64)
		fresh := e1 == nil && e2 == nil && c.FreshForNS > 0 && o.ReceiptMonotonic.ClockEpochID != "" && o.ReceiptMonotonic.ClockEpochID == c.Evaluation.EvaluateMonotonic.ClockEpochID && evaluated >= received && evaluated-received <= c.FreshForNS
		decoded := o.Number != nil && o.Boolean == nil
		if i == 4 {
			decoded = o.Boolean != nil && o.Number == nil
		}
		if o.Identity != id || o.Selector != selectors[i] || o.Qualification != semreg.QualificationQualified || o.Validity != semreg.ValidityGood || o.Freshness != semreg.FreshnessFresh || !fresh || !decoded || len(o.Request) == 0 || len(o.Reply) == 0 || len(o.Evidence) == 0 {
			ready = false
			loss(names[i], "incoherent, invalid, stale, unqualified or incomplete native evidence")
		}
	}
	if !ready {
		return r
	}
	o, s, a, t := *r.Native[0].Number, *r.Native[1].Number, *r.Native[2].Number, *r.Native[3].Number
	if a != uint16(c.CircuitInstance) {
		loss("A", "assignment differs from qualified circuit")
		return r
	}
	if s != 0 {
		loss("S", "override or unresolved special function; no preset equivalence")
		return r
	}
	if t != 1 {
		loss("T", "inactive, fixed-value or unresolved circuit scope; no cooling inference")
		return r
	}
	token := ""
	switch o {
	case 0:
		token = "off"
	case 1:
		token = "auto"
	default:
		loss("O", "manual or unresolved mode is unrepresentable")
		return r
	}
	zone := c.Zone
	pack := thermal.New().Pack()
	key := semreg.FactKey{PackID: pack.ID, PackVersion: pack.Version, FactID: "thermal.mode.zone", Dimensions: []semreg.Dimension{{ID: "thermal.dimension.zone", Value: semreg.Value{Kind: semreg.ValueText, Text: &zone}}}}
	value := &semreg.Value{Kind: semreg.ValueSymbol, Symbol: &semreg.Symbol{Namespace: "thermal.mode.zone", Token: token, Known: true}}
	if err := thermal.New().ValidateFact(key, value); err != nil {
		loss("mode", "existing thermal definition rejected candidate")
		return r
	}
	r.Key, r.Value = key, value
	return r
}
