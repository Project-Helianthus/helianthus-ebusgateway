package main

import (
	"strconv"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

// These adapter records retain native observations, not universal pump facts.
// Contract: https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/circuit-pump-semreg-cutover.md
type thermalCircuitPumpSelector struct {
	Source, Target                   byte
	PB, SB, Opcode, OperationType    byte
	Group, Instance                  byte
	Register                         uint16
	RequestDirection, ValueDirection string
}

type thermalCircuitPumpIdentity struct {
	Asset               semreg.AssetID
	Service             semreg.ServiceInstanceID
	Source              semreg.SourceID
	SourceEpoch         semreg.SourceEpochID
	DriverGeneration    semreg.Uint64
	Binding             semreg.NativeBindingID
	BindingRevision     semreg.Uint64
	AssociationRevision semreg.Uint64
	SemanticRevision    semreg.Uint64
	Selector            thermalCircuitPumpSelector
}

type thermalCircuitPumpObservation struct {
	Identity         thermalCircuitPumpIdentity
	Decoded          *uint32
	Raw              []byte
	WireType, Unit   string
	Evidence         []semreg.EvidenceRef
	EvidenceStatus   string // Proven/Hypothesis/Unknown is independent of qualification.
	Applicability    string // Family applicability, or Unknown; never inferred here.
	Qualification    semreg.Qualification
	Validity         semreg.Validity
	Acquisition      string // Caller records wire/cache origin without renewing receipt.
	ReceivedAt       semreg.TimePoint
	ReceiptMonotonic semreg.MonotonicPoint
	FreshForNS       semreg.Uint64
	Failure          string
}

type thermalCircuitPumpInput struct {
	Identity thermalCircuitPumpIdentity
	Previous *thermalCircuitPumpObservation
	Attempts []thermalCircuitPumpObservation
}

type thermalCircuitPumpMember struct {
	Identity   thermalCircuitPumpIdentity
	Current    *thermalCircuitPumpObservation
	Historical []thermalCircuitPumpObservation
	Attempts   []thermalCircuitPumpObservation
	Freshness  semreg.Freshness
	Retained   bool
	Reason     string
	// A decrease has Unknown continuity. No delta/rate or reset cause is derived.
	Continuity string
	Meaning    string
}

type thermalCircuitPumpSourceLoss struct {
	Identity thermalCircuitPumpIdentity
	Outcome  projection.ProjectionOutcome
	Detail   projection.LossDetail
}

type thermalCircuitPumpMapping struct {
	ServiceDefinition, CircuitDimension semreg.DefinitionRef
	Evaluation                          semreg.EvaluationContext
	Members                             []thermalCircuitPumpMember
	SourceLoss                          []thermalCircuitPumpSourceLoss
	Facts                               []semreg.FactCandidate
}

// mapThermalCircuitPump is pure: callers supply decoded evidence and evaluation
// context. It performs no acquisition, registration, publication or projection
// accounting. In particular, source loss cannot replace another source's single
// disposition for a requested shared item. No pump FactKey is allocated.
func mapThermalCircuitPump(inputs []thermalCircuitPumpInput, evaluation semreg.EvaluationContext) thermalCircuitPumpMapping {
	out := thermalCircuitPumpMapping{Evaluation: evaluation, Facts: []semreg.FactCandidate{}, Members: []thermalCircuitPumpMember{}, SourceLoss: []thermalCircuitPumpSourceLoss{}}
	for _, service := range thermal.Metadata().Services {
		if service.Ref.ID == "thermal.service.circuit" {
			out.ServiceDefinition, out.CircuitDimension = service.Ref, service.FactKeyDimension
		}
	}
	for _, input := range inputs {
		member := thermalCircuitPumpMember{Identity: input.Identity, Freshness: semreg.FreshnessUnknown, Continuity: "Unknown", Meaning: "Unknown", Reason: "missing"}
		var prior *thermalCircuitPumpObservation
		if input.Previous != nil {
			previous := thermalCircuitPumpClone(*input.Previous)
			if previous.Identity == input.Identity && thermalCircuitPumpGood(previous) {
				prior = &previous
			} else {
				member.Historical = append(member.Historical, previous)
			}
		}
		var candidates []thermalCircuitPumpObservation
		for _, attempt := range input.Attempts {
			attempt = thermalCircuitPumpClone(attempt)
			member.Attempts = append(member.Attempts, attempt)
			if attempt.Identity != input.Identity {
				member.Reason = "identity_mismatch"
				continue
			}
			if !thermalCircuitPumpGood(attempt) {
				member.Reason = "invalid_or_failed"
				continue
			}
			// Reject delayed/cache replay as a new acquisition. Preserve its bytes
			// in Attempts and the prior's original age in Current.
			if prior != nil && !thermalCircuitPumpLater(attempt.ReceiptMonotonic, prior.ReceiptMonotonic) {
				member.Reason = "receipt_not_newer"
				continue
			}
			candidates = append(candidates, attempt)
		}
		switch len(candidates) {
		case 0:
			member.Current, member.Retained = prior, prior != nil
		case 1:
			current := thermalCircuitPumpClone(candidates[0])
			member.Current, member.Reason = &current, "observed"
			if prior != nil {
				member.Historical = append(member.Historical, thermalCircuitPumpClone(*prior))
				if input.Identity.Selector.Register != 0x001b && *current.Decoded < *prior.Decoded {
					member.Reason = "counter_decreased_continuity_unknown"
				}
			}
		default:
			// Arrival order and value agreement cannot resolve competing candidates.
			member.Current, member.Retained, member.Reason = prior, prior != nil, "conflict"
		}
		if !thermalCircuitPumpSupported(input.Identity) {
			if member.Current != nil {
				member.Historical = append(member.Historical, thermalCircuitPumpClone(*member.Current))
			}
			member.Current, member.Retained, member.Reason = nil, false, "unsupported_identity"
		}
		if member.Current != nil {
			member.Freshness = thermalCircuitPumpFreshness(*member.Current, evaluation)
		}
		out.Members = append(out.Members, member)
		description := "no Thermal pump status definition; native codebook and active meaning remain Unknown"
		switch input.Identity.Selector.Register {
		case 0x0024:
			description = "no Thermal pump operating-hours definition; native total retained without delta or rate"
		case 0x0025:
			description = "no Thermal pump starts definition; native total retained without delta or rate"
		}
		out.SourceLoss = append(out.SourceLoss, thermalCircuitPumpSourceLoss{Identity: input.Identity, Outcome: projection.ProjectionWithheld, Detail: projection.LossDetail{Kind: projection.LossPolicy, Description: description}})
	}
	return out
}

func thermalCircuitPumpSupported(id thermalCircuitPumpIdentity) bool {
	s := id.Selector
	return id.Asset != "" && id.Service != "" && id.Source != "" && id.SourceEpoch != "" &&
		id.DriverGeneration != "" && id.DriverGeneration != "0" && id.Binding != "" &&
		id.BindingRevision != "" && id.AssociationRevision != "" && id.SemanticRevision != "" &&
		s.PB == 0xb5 && s.SB == 0x24 && s.Opcode == 0x02 && s.OperationType == 0 && s.Group == 0x02 &&
		(s.Register == 0x001b || s.Register == 0x0024 || s.Register == 0x0025) &&
		s.RequestDirection == "initiator_to_target" && s.ValueDirection == "target_to_initiator"
}

func thermalCircuitPumpGood(o thermalCircuitPumpObservation) bool {
	if o.Decoded == nil || o.Validity != semreg.ValidityGood || o.Failure != "" {
		return false
	}
	if o.Identity.Selector.Register == 0x001b {
		return *o.Decoded <= 0xffff && o.WireType == "u16" && o.Unit == "raw"
	}
	// Sentinel rejection is the existing reader's implementation rule, not a
	// claim about universal reset, rollover or cross-family semantics.
	unit := "hours"
	if o.Identity.Selector.Register == 0x0025 {
		unit = "count"
	}
	return *o.Decoded != ^uint32(0) && o.WireType == "u32" && o.Unit == unit
}

func thermalCircuitPumpLater(a, b semreg.MonotonicPoint) bool {
	x, xe := strconv.ParseUint(string(a.Nanoseconds), 10, 64)
	y, ye := strconv.ParseUint(string(b.Nanoseconds), 10, 64)
	return a.ClockEpochID != "" && a.ClockEpochID == b.ClockEpochID && xe == nil && ye == nil && x > y
}

func thermalCircuitPumpFreshness(o thermalCircuitPumpObservation, e semreg.EvaluationContext) semreg.Freshness {
	r, re := strconv.ParseUint(string(o.ReceiptMonotonic.Nanoseconds), 10, 64)
	n, ne := strconv.ParseUint(string(e.EvaluateMonotonic.Nanoseconds), 10, 64)
	f, fe := strconv.ParseUint(string(o.FreshForNS), 10, 64)
	if re != nil || ne != nil || fe != nil || o.ReceiptMonotonic.ClockEpochID == "" || o.ReceiptMonotonic.ClockEpochID != e.EvaluateMonotonic.ClockEpochID || n < r {
		return semreg.FreshnessUnknown
	}
	if n-r > f {
		return semreg.FreshnessStale
	}
	return semreg.FreshnessFresh
}

func thermalCircuitPumpClone(o thermalCircuitPumpObservation) thermalCircuitPumpObservation {
	if o.Decoded != nil {
		value := *o.Decoded
		o.Decoded = &value
	}
	o.Raw = append([]byte(nil), o.Raw...)
	o.Evidence = append([]semreg.EvidenceRef(nil), o.Evidence...)
	return o
}
