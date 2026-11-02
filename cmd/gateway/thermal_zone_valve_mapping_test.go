package main

import (
	"reflect"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
)

// All qualification-positive inputs below are synthetic controls, not device evidence.
func thermalZoneValveFixture() thermalZoneValveInput {
	input := thermalZoneValveInput{Context: thermalZoneValveContext{
		EvaluatedAt:       semreg.TimePoint{UnixNanoseconds: "200", ClockID: "clock.synthetic"},
		EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: "synthetic-clock", Nanoseconds: "20"},
		FreshForNS:        15, AssociationQualified: true,
	}}
	for role := range input.Observations {
		selector := thermalZoneValveSelector{
			PB: 0xb5, SB: 0x24, Opcode: 2, OperationType: 0, Group: 3, Instance: 4, Register: 0x12,
			RequestDirection: "initiator_to_target", ValueDirection: "target_to_initiator",
			Initiator: "synthetic-initiator", Target: "synthetic-target",
		}
		value := uint16(1)
		if role == thermalZoneValveAssociation {
			selector.Register, value = 0x1c, 8
		}
		if role == thermalZoneValveCircuitType {
			selector.Group, selector.Instance, selector.Register = 2, 6, 2
		}
		identity := thermalZoneValveIdentity{
			Asset: "synthetic-zone", Service: "synthetic-service", Source: "synthetic-source",
			SourceEpoch: "synthetic-epoch", DriverGeneration: "3", BindingRevision: "binding-a", AssociationRevision: "association-a", Selector: selector,
		}
		input.Context.Expected[role] = identity
		input.Observations[role] = &thermalZoneValveObservation{
			Identity: identity, Value: &value, Raw: []byte{byte(value), byte(value >> 8)}, WireType: "u16_le",
			Origin: "synthetic-import", ControllerFamily: "synthetic-family", FirmwareScope: "synthetic-scope", EvidenceGrade: "Unknown",
			ReceivedAt:       semreg.TimePoint{UnixNanoseconds: "100", ClockID: "clock.synthetic"},
			ReceiptMonotonic: semreg.MonotonicPoint{ClockEpochID: "synthetic-clock", Nanoseconds: "10"},
			Qualification:    semreg.QualificationQualified, Validity: semreg.ValidityGood, Freshness: semreg.FreshnessFresh,
		}
	}
	return input
}

func thermalZoneValveSetValue(observation *thermalZoneValveObservation, value uint16) {
	observation.Value = &value
	observation.Raw = []byte{byte(value), byte(value >> 8)}
}

func TestThermalZoneValveMappingCases(t *testing.T) {
	circuitOne := uint16(1)
	for _, test := range []struct {
		name           string
		valve          uint16
		circuit        *uint16
		status, action string
		percentage     float64
		conflict       bool
	}{
		{name: "closed", valve: 0, circuit: &circuitOne, status: "closed", action: "idle", percentage: 0},
		{name: "open_without_context", valve: 1, status: "open", action: "heating", percentage: 0.0015259021896696422},
		{name: "unknown_enum", valve: 7, status: "unknown", percentage: 0.010681315327687496},
		{name: "not_qualified_travel", valve: 65535, circuit: &circuitOne, status: "unknown", percentage: 100},
		{name: "type2_conflict", valve: 1, circuit: func() *uint16 { v := uint16(2); return &v }(), status: "open", action: "cooling", percentage: 0.0015259021896696422, conflict: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := thermalZoneValveFixture()
			thermalZoneValveSetValue(input.Observations[0], test.valve)
			if test.circuit == nil {
				input.Observations[2] = nil
			} else {
				thermalZoneValveSetValue(input.Observations[2], *test.circuit)
			}
			result := mapThermalZoneValve(input)
			if result.NativeStatus != test.status || result.LegacyAction != test.action || result.CircuitTypeConflict != test.conflict || result.LegacyPercentage == nil || *result.LegacyPercentage != test.percentage {
				t.Fatalf("unexpected raw/legacy evidence: %+v", result)
			}
			if result.LegacyInputsCurrent != (test.circuit != nil) {
				t.Fatal("missing context became current")
			}
			wantLosses := []thermalZoneValveLoss{
				{Member: "raw_valve", Reason: "no_zone_valve_field"},
				{Member: "legacy_percentage", Reason: "no_travel_field_or_qualified_scale"},
				{Member: "legacy_action", Reason: "system_action_is_not_zone_delivery", Definition: &semreg.DefinitionRef{
					Pack: semreg.PackRef{ID: "helianthus.pack.thermal", Version: "1.0.0"}, ID: "thermal.action.state", Version: "1.0.0",
				}},
			}
			if !reflect.DeepEqual(result.Losses, wantLosses) {
				t.Fatalf("losses = %+v; want %+v", result.Losses, wantLosses)
			}
		})
	}
}

func TestThermalZoneValveMappingIdentity(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*thermalZoneValveIdentity)
	}{
		{"asset", func(i *thermalZoneValveIdentity) { i.Asset = "other-asset" }},
		{"service", func(i *thermalZoneValveIdentity) { i.Service = "other-service" }},
		{"source", func(i *thermalZoneValveIdentity) { i.Source = "other-source" }},
		{"source_epoch", func(i *thermalZoneValveIdentity) { i.SourceEpoch = "other-epoch" }},
		{"driver_generation", func(i *thermalZoneValveIdentity) { i.DriverGeneration = "4" }},
		{"binding_revision", func(i *thermalZoneValveIdentity) { i.BindingRevision = "binding-b" }},
		{"association_revision", func(i *thermalZoneValveIdentity) { i.AssociationRevision = "association-b" }},
		{"pb", func(i *thermalZoneValveIdentity) { i.Selector.PB++ }},
		{"sb", func(i *thermalZoneValveIdentity) { i.Selector.SB++ }},
		{"opcode", func(i *thermalZoneValveIdentity) { i.Selector.Opcode = 6 }},
		{"operation_type", func(i *thermalZoneValveIdentity) { i.Selector.OperationType++ }},
		{"group", func(i *thermalZoneValveIdentity) { i.Selector.Group++ }},
		{"instance", func(i *thermalZoneValveIdentity) { i.Selector.Instance++ }},
		{"register", func(i *thermalZoneValveIdentity) { i.Selector.Register++ }},
		{"request_direction", func(i *thermalZoneValveIdentity) { i.Selector.RequestDirection = "other" }},
		{"value_direction", func(i *thermalZoneValveIdentity) { i.Selector.ValueDirection = "other" }},
		{"initiator", func(i *thermalZoneValveIdentity) { i.Selector.Initiator = "other-initiator" }},
		{"target", func(i *thermalZoneValveIdentity) { i.Selector.Target = "other-target" }},
	}
	for role, roleName := range []string{"valve", "association", "circuit"} {
		for _, mutation := range mutations {
			t.Run(roleName+"/"+mutation.name, func(t *testing.T) {
				input := thermalZoneValveFixture()
				thermalZoneValveSetValue(input.Observations[thermalZoneValveCircuitType], 2)
				baseline := mapThermalZoneValve(input)
				if !baseline.LegacyInputsCurrent {
					t.Fatal("synthetic positive control failed")
				}
				mutation.mutate(&input.Observations[role].Identity)
				result := mapThermalZoneValve(input)
				thermalZoneValveAssertRejectedOutputs(t, result, role)
				if result.Components[role].Current || result.Components[role].Reason != "identity_mismatch" || result.LegacyInputsCurrent {
					t.Fatal("mismatched identity reused")
				}
				for sibling := range result.Components {
					if sibling != role && !reflect.DeepEqual(result.Components[sibling], baseline.Components[sibling]) {
						t.Fatalf("unaffected sibling %d changed", sibling)
					}
				}
			})
		}
	}
}

func TestThermalZoneValveMappingIndependentAxes(t *testing.T) {
	mutations := []struct {
		name, reason string
		mutate       func(*thermalZoneValveObservation)
	}{
		{"freshness", "not_fresh", func(o *thermalZoneValveObservation) { o.Freshness = semreg.FreshnessStale }},
		{"validity", "invalid", func(o *thermalZoneValveObservation) { o.Validity = semreg.ValidityBad }},
		{"qualification", "unqualified", func(o *thermalZoneValveObservation) { o.Qualification = semreg.QualificationCandidate }},
		{"width", "invalid", func(o *thermalZoneValveObservation) { o.Raw = append(o.Raw, 0) }},
		{"wire_type", "invalid", func(o *thermalZoneValveObservation) { o.WireType = "u8" }},
		{"decode_mismatch", "invalid", func(o *thermalZoneValveObservation) { value := uint16(0); o.Value = &value }},
		{"raw_low_byte_mismatch", "invalid", func(o *thermalZoneValveObservation) { o.Raw[0] ^= 1 }},
		{"raw_high_byte_mismatch", "invalid", func(o *thermalZoneValveObservation) { o.Raw[1] ^= 1 }},
		{"missing_decode", "invalid", func(o *thermalZoneValveObservation) { o.Value = nil }},
		{"elapsed_age", "age_unknown_or_stale", func(o *thermalZoneValveObservation) { o.ReceiptMonotonic.Nanoseconds = "1" }},
		{"future_receipt", "age_unknown_or_stale", func(o *thermalZoneValveObservation) { o.ReceiptMonotonic.Nanoseconds = "21" }},
		{"clock_epoch", "age_unknown_or_stale", func(o *thermalZoneValveObservation) { o.ReceiptMonotonic.ClockEpochID = "other-clock" }},
		{"bad_receipt", "age_unknown_or_stale", func(o *thermalZoneValveObservation) { o.ReceiptMonotonic.Nanoseconds = "invalid" }},
	}
	for role, roleName := range []string{"valve", "association", "circuit"} {
		for _, mutation := range mutations {
			t.Run(roleName+"/"+mutation.name, func(t *testing.T) {
				input := thermalZoneValveFixture()
				thermalZoneValveSetValue(input.Observations[thermalZoneValveCircuitType], 2)
				baseline := mapThermalZoneValve(input)
				before := *input.Observations[role]
				mutation.mutate(input.Observations[role])
				result := mapThermalZoneValve(input)
				component := result.Components[role]
				thermalZoneValveAssertRejectedOutputs(t, result, role)
				if component.Current || component.Reason != mutation.reason || result.LegacyInputsCurrent {
					t.Fatalf("axis did not fail closed: %+v", component)
				}
				if component.Observation.ReceivedAt != before.ReceivedAt || result.EvaluatedAt != input.Context.EvaluatedAt {
					t.Fatal("evaluation replaced original receipt")
				}
				for sibling := range result.Components {
					if sibling != role && !reflect.DeepEqual(result.Components[sibling], baseline.Components[sibling]) {
						t.Fatal("unaffected sibling changed")
					}
				}
			})
		}
	}
}

func TestThermalZoneValveMappingLossAndHistory(t *testing.T) {
	input := thermalZoneValveFixture()
	for role := range input.Observations {
		input.Previous[role] = thermalZoneValveClone(input.Observations[role])
	}
	input.Observations[0] = nil
	input.Observations[2].Identity.AssociationRevision = "old-association"
	result := mapThermalZoneValve(input)
	if result.Components[0].Reason != "missing" || result.LegacyAction != "" || result.LegacyPercentage != nil || result.NativeStatus != "unknown" || result.LegacyInputsCurrent {
		t.Fatal("history manufactured a current valve/action")
	}
	if !reflect.DeepEqual(result.Components[0].Retained, input.Previous[0]) || !result.Components[1].Current || result.Components[2].Current {
		t.Fatal("partial loss erased history/sibling or reused reassociated circuit")
	}
	if result.Components[0].Retained.ReceivedAt.UnixNanoseconds != "100" {
		t.Fatal("history was refreshed")
	}
	// The source-only return type has no requested-item/accounting or projected-fact
	// member: absent zone evidence cannot produce a replacement system disposition.
	input.Context.AssociationQualified = false
	input.Observations = thermalZoneValveFixture().Observations
	thermalZoneValveSetValue(input.Observations[thermalZoneValveCircuitType], 2)
	unqualified := mapThermalZoneValve(input)
	if unqualified.LegacyInputsCurrent {
		t.Fatal("association qualification ignored")
	}
	thermalZoneValveAssertRejectedOutputs(t, unqualified, thermalZoneValveAssociation)
}

func TestThermalZoneValveMappingDetached(t *testing.T) {
	input := thermalZoneValveFixture()
	input.Previous[0] = thermalZoneValveClone(input.Observations[0])
	input.Observations[0].Freshness = semreg.FreshnessStale
	result := mapThermalZoneValve(input)
	input.Observations[0].Raw[0], *input.Observations[0].Value = 7, 7
	input.Previous[0].Raw[0], *input.Previous[0].Value = 9, 9
	if result.Components[0].Observation.Raw[0] != 1 || *result.Components[0].Observation.Value != 1 || result.Components[0].Retained.Raw[0] != 1 || *result.Components[0].Retained.Value != 1 {
		t.Fatal("input aliases output")
	}
	result.Components[0].Observation.Raw[0], *result.Components[0].Observation.Value = 3, 3
	result.Components[0].Retained.Raw[0], *result.Components[0].Retained.Value = 4, 4
	if result.LegacyPercentage != nil {
		t.Fatal("stale valve produced a percentage")
	}
	percentageResult := mapThermalZoneValve(thermalZoneValveFixture())
	*percentageResult.LegacyPercentage = 50
	result.Losses[2].Definition.ID = "changed"
	if input.Observations[0].Raw[0] != 7 || *input.Observations[0].Value != 7 || input.Previous[0].Raw[0] != 9 || *input.Previous[0].Value != 9 {
		t.Fatal("output aliases input")
	}
	again := mapThermalZoneValve(thermalZoneValveFixture())
	if again.Losses[2].Definition.ID != "thermal.action.state" || *again.LegacyPercentage != 0.0015259021896696422 {
		t.Fatal("outputs alias subsequent results")
	}
}

func TestThermalZoneValveMappingContext(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*thermalZoneValveInput)
	}{
		{"zero_window", func(i *thermalZoneValveInput) { i.Context.FreshForNS = 0 }},
		{"invalid_evaluation", func(i *thermalZoneValveInput) { i.Context.EvaluateMonotonic.Nanoseconds = "invalid" }},
		{"evaluation_clock", func(i *thermalZoneValveInput) { i.Context.EvaluateMonotonic.ClockEpochID = "other-clock" }},
		{"absent_identity", func(i *thermalZoneValveInput) {
			i.Context.Expected[0].Asset = ""
			i.Observations[0].Identity.Asset = ""
		}},
		{"matched_but_unsupported_selector", func(i *thermalZoneValveInput) {
			i.Context.Expected[0].Selector.Opcode = 6
			i.Observations[0].Identity.Selector.Opcode = 6
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := thermalZoneValveFixture()
			test.mutate(&input)
			result := mapThermalZoneValve(input)
			thermalZoneValveAssertRejectedOutputs(t, result, thermalZoneValveStatus)
			if result.Components[0].Current || result.LegacyInputsCurrent {
				t.Fatal("unsupported context accepted")
			}
		})
	}
	input := thermalZoneValveFixture()
	baseline := mapThermalZoneValve(input)
	if baseline.LegacyAction != "heating" || !baseline.LegacyInputsCurrent || *baseline.Components[1].Observation.Value != 8 || baseline.Components[1].Observation.Identity.Selector.Register != 0x1c {
		t.Fatal("native association was normalized or synthetic control failed")
	}
	for _, grade := range []string{"Proven", "Hypothesis", "Unknown"} {
		input.Observations[0].EvidenceGrade = grade
		result := mapThermalZoneValve(input)
		if !result.Components[0].Current || result.Components[0].Observation.EvidenceGrade != grade {
			t.Fatal("grade changed qualification")
		}
		input.Observations[0].Qualification = semreg.QualificationUnknown
		if mapThermalZoneValve(input).Components[0].Current {
			t.Fatal("grade qualified evidence")
		}
		input.Observations[0].Qualification = semreg.QualificationQualified
	}
	input.Observations = [thermalZoneValveComponentCount]*thermalZoneValveObservation{}
	absent := mapThermalZoneValve(input)
	if !reflect.DeepEqual(absent.Losses, baseline.Losses) {
		t.Fatal("source loss changed requested-definition meaning")
	}
	for _, component := range absent.Components {
		if component.Current || component.Reason != "missing" || component.Observation != nil {
			t.Fatal("absence became a measurement")
		}
	}
}

// A rejected valve supplies no derived outputs. Rejected association/circuit
// context preserves the admitted valve's documented open-without-context fallback.
func thermalZoneValveAssertRejectedOutputs(t *testing.T, result thermalZoneValveResult, role int) {
	t.Helper()
	if role == thermalZoneValveStatus {
		if result.NativeStatus != "unknown" || result.LegacyAction != "" || result.LegacyPercentage != nil || result.CircuitTypeConflict {
			t.Fatalf("rejected valve supplied derived outputs: %+v", result)
		}
		return
	}
	if result.NativeStatus != "open" || result.LegacyAction != "heating" || result.LegacyPercentage == nil || *result.LegacyPercentage != 0.0015259021896696422 || result.CircuitTypeConflict {
		t.Fatalf("rejected context supplied cooling/conflict or erased admitted valve: %+v", result)
	}
}
