package main

import (
	"strconv"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// These adapter records retain native candidates, not Thermal pack facts. The
// accepted contract has no solar pump, runtime-counter or starts definition:
// https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/solar-pump-semreg-cutover.md
type thermalSolarPumpIdentity struct {
	Asset               semreg.AssetID
	Service             semreg.ServiceInstanceID
	Owner               string
	Source              semreg.SourceID
	SourceEpoch         semreg.SourceEpochID
	DriverGeneration    semreg.Uint64
	Binding             semreg.NativeBindingID
	BindingRevision     semreg.Uint64
	AssociationRevision semreg.Uint64
}

type thermalSolarPumpSelector struct {
	PB, SB, Opcode  byte
	OperationType   string
	Group, Instance byte
	Register        uint16
	Direction       string
}

// Decoding and selector-correlated extraction belong to the caller. Raw evidence
// and the original receipt survive invalid candidates and partial acquisition.
type thermalSolarPumpObservation struct {
	Identity         thermalSolarPumpIdentity
	Selector         thermalSolarPumpSelector
	Status           *uint8
	Hours            *uint32
	Request, Reply   []byte
	Evidence         []semreg.EvidenceRef
	Family, Firmware string
	ReceivedAt       semreg.TimePoint
	ReceiptMonotonic semreg.MonotonicPoint
	Policy           semreg.FreshnessPolicy
	Validity         semreg.Validity
	Qualification    semreg.Qualification
	Acquisition      semreg.Availability
}

type thermalSolarPumpContext struct {
	Identity      thermalSolarPumpIdentity
	Qualification semreg.Qualification
	// Available means coherent presence; degraded retains the prior coherent
	// fields; withdrawn means coherent structural absence/non-interpretability.
	FM5        semreg.Availability
	Evaluation semreg.EvaluationContext
}

type thermalSolarPumpState struct {
	Status, Hours *thermalSolarPumpObservation
}

type thermalSolarPumpField struct {
	Retained  *thermalSolarPumpObservation
	Freshness semreg.Freshness
	// Usable concerns only the retained native compatibility candidate. It
	// never admits a SemReg fact or establishes physical qualification.
	Usable  bool
	Problem string
}

// Source loss is deliberately outside requested-item accounting. In particular,
// missing solar evidence cannot overwrite another source's projected operation
// or action disposition. The labels below are not registry definition IDs.
type thermalSolarPumpLoss struct {
	Source, MeaningGrade, Reason string
}

type thermalSolarPumpMapping struct {
	Status, Hours thermalSolarPumpField
	Availability  semreg.Availability
	Evaluation    semreg.EvaluationContext
	Attempted     []thermalSolarPumpObservation
	History       []thermalSolarPumpObservation
	Loss          []thermalSolarPumpLoss
	Facts         []semreg.FactCandidate
}

// mapThermalSolarPump is a detached, read-only adapter seam. It neither acquires
// data nor publishes/withdraws facts, services, dispositions or compatibility
// envelopes. Freshness here requires comparable monotonic epochs; unknown
// clocks fail closed without manufacturing a new receipt from evaluation.
func mapThermalSolarPump(ctx thermalSolarPumpContext, previous thermalSolarPumpState, status, hours *thermalSolarPumpObservation) thermalSolarPumpMapping {
	clone := func(in *thermalSolarPumpObservation) *thermalSolarPumpObservation {
		if in == nil {
			return nil
		}
		out := *in
		out.Request = append([]byte(nil), in.Request...)
		out.Reply = append([]byte(nil), in.Reply...)
		out.Evidence = append([]semreg.EvidenceRef(nil), in.Evidence...)
		if in.Status != nil {
			v := *in.Status
			out.Status = &v
		}
		if in.Hours != nil {
			v := *in.Hours
			out.Hours = &v
		}
		return &out
	}
	identityValid := ctx.Identity.Asset.Validate() == nil && ctx.Identity.Service.Validate() == nil &&
		ctx.Identity.Owner != "" && ctx.Identity.Source.Validate() == nil && ctx.Identity.SourceEpoch.Validate() == nil &&
		ctx.Identity.Binding.Validate() == nil
	for _, revision := range []semreg.Uint64{ctx.Identity.DriverGeneration, ctx.Identity.BindingRevision, ctx.Identity.AssociationRevision} {
		n, err := strconv.ParseUint(string(revision), 10, 64)
		identityValid = identityValid && err == nil && n > 0
	}
	freshness := func(o *thermalSolarPumpObservation) semreg.Freshness {
		if o == nil || o.Policy.Validate() != nil || ctx.Evaluation.Validate() != nil {
			return semreg.FreshnessUnknown
		}
		times := semreg.Times{ReceivedAt: o.ReceivedAt, ReceiptMonotonic: o.ReceiptMonotonic,
			EvaluatedAt: ctx.Evaluation.EvaluatedAt, EvaluateMonotonic: ctx.Evaluation.EvaluateMonotonic}
		elapsed, err := times.ElapsedMonotonicNS()
		if err != nil {
			return semreg.FreshnessUnknown
		}
		age, _ := strconv.ParseUint(string(elapsed), 10, 64)
		fresh, _ := strconv.ParseUint(string(o.Policy.FreshForNS), 10, 64)
		retain, _ := strconv.ParseUint(string(o.Policy.RetainForNS), 10, 64)
		if age < fresh {
			return semreg.FreshnessFresh
		}
		if age < retain {
			return semreg.FreshnessStale
		}
		return semreg.FreshnessExpired
	}
	result := thermalSolarPumpMapping{Availability: ctx.FM5, Evaluation: ctx.Evaluation,
		Attempted: []thermalSolarPumpObservation{}, History: []thermalSolarPumpObservation{}, Facts: []semreg.FactCandidate{},
		Loss: []thermalSolarPumpLoss{
			{Source: "pump-active", MeaningGrade: "Hypothesis", Reason: "no_thermal_pack_definition"},
			{Source: "operating-hours", MeaningGrade: "Hypothesis", Reason: "no_thermal_pack_definition_absolute_hours_only"},
			{Source: "starts", MeaningGrade: "Unknown", Reason: "no_native_selector_or_thermal_pack_definition"},
		}}
	mapField := func(old, incoming *thermalSolarPumpObservation, register uint16) thermalSolarPumpField {
		expected := thermalSolarPumpSelector{PB: 0xB5, SB: 0x24, Opcode: 0x02, OperationType: "read", Group: 0x04, Instance: 0, Register: register, Direction: "target_to_initiator"}
		valid := func(o *thermalSolarPumpObservation) bool {
			if o == nil || !identityValid || o.Identity != ctx.Identity || o.Selector != expected ||
				o.Validity != semreg.ValidityGood || o.Acquisition != semreg.AvailabilityAvailable ||
				o.ReceivedAt.Validate() != nil || o.ReceiptMonotonic.Validate() != nil {
				return false
			}
			if register == 0x0008 {
				return o.Status != nil && *o.Status <= 1 && o.Hours == nil
			}
			return o.Hours != nil && *o.Hours != ^uint32(0) && o.Status == nil
		}
		field := thermalSolarPumpField{Freshness: semreg.FreshnessUnknown}
		if valid(old) {
			field.Retained = clone(old)
		} else if old != nil {
			result.History = append(result.History, *clone(old))
		}
		if incoming == nil {
			field.Problem = "missing_observation"
		} else {
			result.Attempted = append(result.Attempted, *clone(incoming))
			switch {
			case !valid(incoming):
				field.Problem = "invalid_or_unbound_observation"
			case ctx.FM5 != semreg.AvailabilityAvailable:
				field.Problem = "fm5_not_coherent_present"
			case freshness(incoming) == semreg.FreshnessUnknown:
				field.Problem = "incomparable_receipt_or_policy"
			default:
				// Arrival order and equal values cannot renew old evidence. A new
				// clock epoch needs a new source lineage, not an implicit unwrap.
				newer := true
				if field.Retained != nil {
					before, _ := strconv.ParseUint(string(field.Retained.ReceiptMonotonic.Nanoseconds), 10, 64)
					after, _ := strconv.ParseUint(string(incoming.ReceiptMonotonic.Nanoseconds), 10, 64)
					newer = incoming.ReceiptMonotonic.ClockEpochID == field.Retained.ReceiptMonotonic.ClockEpochID && after > before
				}
				if newer {
					if field.Retained != nil {
						result.History = append(result.History, *clone(field.Retained))
					}
					field.Retained = clone(incoming)
				} else {
					field.Problem = "nonadvancing_or_incomparable_receipt"
				}
			}
		}
		field.Freshness = freshness(field.Retained)
		field.Usable = field.Retained != nil && field.Freshness == semreg.FreshnessFresh &&
			field.Retained.Qualification == semreg.QualificationQualified && ctx.Qualification == semreg.QualificationQualified &&
			(ctx.FM5 == semreg.AvailabilityAvailable || ctx.FM5 == semreg.AvailabilityDegraded)
		return field
	}
	result.Status = mapField(previous.Status, status, 0x0008)
	result.Hours = mapField(previous.Hours, hours, 0x000B)
	return result
}
