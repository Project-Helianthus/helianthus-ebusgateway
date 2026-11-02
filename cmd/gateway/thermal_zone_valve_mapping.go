package main

import (
	"encoding/binary"
	"strconv"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
)

const (
	thermalZoneValveStatus = iota
	thermalZoneValveAssociation
	thermalZoneValveCircuitType
	thermalZoneValveComponentCount
)

type thermalZoneValveSelector struct {
	PB, SB, Opcode, OperationType, Group, Instance byte
	Register                                       uint16
	RequestDirection, ValueDirection               string
	Initiator, Target                              string
}

type thermalZoneValveIdentity struct {
	Asset                                semreg.AssetID
	Service                              semreg.ServiceInstanceID
	Source                               semreg.SourceID
	SourceEpoch                          semreg.SourceEpochID
	DriverGeneration                     semreg.Uint64
	BindingRevision, AssociationRevision string
	Selector                             thermalZoneValveSelector
}

// Observations are caller-decoded evidence, not a new acquisition or decoder API.
// EvidenceGrade is independent of qualification, validity and freshness.
type thermalZoneValveObservation struct {
	Identity                                                         thermalZoneValveIdentity
	Raw                                                              []byte
	Value                                                            *uint16
	WireType, Origin, ControllerFamily, FirmwareScope, EvidenceGrade string
	ReceivedAt                                                       semreg.TimePoint
	ReceiptMonotonic                                                 semreg.MonotonicPoint
	Qualification                                                    semreg.Qualification
	Validity                                                         semreg.Validity
	Freshness                                                        semreg.Freshness
}

// The caller qualifies the association and supplies mutually consistent expected
// identities across roles; this mapper checks each observation against its role.
type thermalZoneValveContext struct {
	Expected             [thermalZoneValveComponentCount]thermalZoneValveIdentity
	EvaluatedAt          semreg.TimePoint
	EvaluateMonotonic    semreg.MonotonicPoint
	FreshForNS           uint64
	AssociationQualified bool
}

type thermalZoneValveInput struct {
	Context      thermalZoneValveContext
	Observations [thermalZoneValveComponentCount]*thermalZoneValveObservation
	Previous     [thermalZoneValveComponentCount]*thermalZoneValveObservation
}

type thermalZoneValveComponent struct {
	Observation *thermalZoneValveObservation
	// Retained is diagnostic history only; it never fills a missing current input.
	Retained *thermalZoneValveObservation
	// Current describes this evidence component, never semantic representability.
	Current bool
	Reason  string
}

type thermalZoneValveLoss struct {
	Member, Reason string
	// Nil means the pack has no representation; never invent a definition ID.
	Definition *semreg.DefinitionRef
}

type thermalZoneValveResult struct {
	Components        [thermalZoneValveComponentCount]thermalZoneValveComponent
	EvaluatedAt       semreg.TimePoint
	EvaluateMonotonic semreg.MonotonicPoint
	NativeStatus      string
	// Legacy outputs reproduce presentation/helper behavior only, including the
	// open-without-context fallback. Only admitted components supply these outputs;
	// cooling additionally requires admitted, qualified association context.
	// They are never measured travel or delivery.
	LegacyPercentage *float64
	LegacyAction     string
	// This reports component currency, not permission to admit a zone action.
	LegacyInputsCurrent bool
	CircuitTypeConflict bool
	Losses              []thermalZoneValveLoss
}

// mapThermalZoneValve retains detached source evidence and explicit source losses.
// It deliberately returns no facts, service bindings or requested-item dispositions:
// the current thermal pack cannot represent these zone members. In particular,
// source absence cannot overwrite another source's projected system action.
// Contract: https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/zone-valve-action-cutover.md
func mapThermalZoneValve(input thermalZoneValveInput) thermalZoneValveResult {
	result := thermalZoneValveResult{
		EvaluatedAt:       input.Context.EvaluatedAt,
		EvaluateMonotonic: input.Context.EvaluateMonotonic,
		NativeStatus:      "unknown",
		Losses: []thermalZoneValveLoss{
			{Member: "raw_valve", Reason: "no_zone_valve_field"},
			{Member: "legacy_percentage", Reason: "no_travel_field_or_qualified_scale"},
			{Member: "legacy_action", Reason: "system_action_is_not_zone_delivery"},
		},
	}
	for _, field := range thermal.Metadata().Fields {
		if field.Ref.ID == "thermal.action.state" {
			ref := field.Ref
			result.Losses[2].Definition = &ref
		}
	}
	for role, observation := range input.Observations {
		component := &result.Components[role]
		component.Observation = thermalZoneValveClone(observation)
		component.Reason = thermalZoneValveAdmission(observation, input.Context.Expected[role], role, input.Context)
		component.Current = component.Reason == ""
		if !component.Current {
			component.Retained = thermalZoneValveClone(input.Previous[role])
		}
	}
	var valve, circuit *uint16
	if result.Components[thermalZoneValveStatus].Current {
		valve = result.Components[thermalZoneValveStatus].Observation.Value
	}
	if input.Context.AssociationQualified && result.Components[thermalZoneValveAssociation].Current && result.Components[thermalZoneValveCircuitType].Current {
		circuit = result.Components[thermalZoneValveCircuitType].Observation.Value
	}
	if valve != nil {
		percentage := float64(*valve) / 655.35
		result.LegacyPercentage = &percentage
		switch *valve {
		case 0:
			result.NativeStatus, result.LegacyAction = "closed", "idle"
		case 1:
			result.NativeStatus, result.LegacyAction = "open", "heating"
			if circuit != nil && *circuit == 2 {
				result.LegacyAction = "cooling"
				result.CircuitTypeConflict = true
			}
		}
	}
	result.LegacyInputsCurrent = input.Context.AssociationQualified
	for _, component := range result.Components {
		result.LegacyInputsCurrent = result.LegacyInputsCurrent && component.Current
	}
	return result
}

func thermalZoneValveClone(observation *thermalZoneValveObservation) *thermalZoneValveObservation {
	if observation == nil {
		return nil
	}
	copy := *observation
	copy.Raw = append([]byte(nil), observation.Raw...)
	if observation.Value != nil {
		value := *observation.Value
		copy.Value = &value
	}
	return &copy
}

func thermalZoneValveDecoded(observation *thermalZoneValveObservation) *uint16 {
	if observation == nil || observation.Validity != semreg.ValidityGood || observation.WireType != "u16_le" || len(observation.Raw) != 2 || observation.Value == nil {
		return nil
	}
	if *observation.Value != binary.LittleEndian.Uint16(observation.Raw) {
		return nil
	}
	return observation.Value
}

func thermalZoneValveAdmission(observation *thermalZoneValveObservation, expected thermalZoneValveIdentity, role int, context thermalZoneValveContext) string {
	if observation == nil {
		return "missing"
	}
	if observation.Identity != expected {
		return "identity_mismatch"
	}
	s := expected.Selector
	group, register := byte(3), uint16(0x12)
	switch role {
	case thermalZoneValveAssociation:
		register = 0x1c
	case thermalZoneValveCircuitType:
		group, register = 2, 2
	}
	if expected.Asset == "" || expected.Service == "" || expected.Source == "" || expected.SourceEpoch == "" || expected.DriverGeneration == "" || expected.BindingRevision == "" || expected.AssociationRevision == "" ||
		s.PB != 0xb5 || s.SB != 0x24 || s.Opcode != 2 || s.OperationType != 0 || s.Group != group || s.Register != register ||
		s.RequestDirection != "initiator_to_target" || s.ValueDirection != "target_to_initiator" || s.Initiator == "" || s.Target == "" {
		return "unsupported_identity"
	}
	if thermalZoneValveDecoded(observation) == nil {
		return "invalid"
	}
	if observation.Qualification != semreg.QualificationQualified {
		return "unqualified"
	}
	if observation.Freshness != semreg.FreshnessFresh {
		return "not_fresh"
	}
	receipt, receiptErr := strconv.ParseUint(string(observation.ReceiptMonotonic.Nanoseconds), 10, 64)
	evaluation, evaluationErr := strconv.ParseUint(string(context.EvaluateMonotonic.Nanoseconds), 10, 64)
	if receiptErr != nil || evaluationErr != nil || observation.ReceiptMonotonic.ClockEpochID == "" || observation.ReceiptMonotonic.ClockEpochID != context.EvaluateMonotonic.ClockEpochID ||
		context.FreshForNS == 0 || evaluation < receipt || evaluation-receipt > context.FreshForNS {
		return "age_unknown_or_stale"
	}
	return ""
}
