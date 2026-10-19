package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

func thermalDhwTestQuantity(integer int) *semreg.Quantity {
	coefficient, exponent := strconv.Itoa(integer), int32(0)
	for integer != 0 && strings.HasSuffix(coefficient, "0") {
		coefficient, exponent = strings.TrimSuffix(coefficient, "0"), exponent+1
	}
	return &semreg.Quantity{Number: semreg.Decimal{Coefficient: coefficient, Exponent10: exponent}, Unit: "unit.celsius"}
}

func thermalDhwTestFixture(service string, integer int) (thermalDhwObservation, thermalDhwBinding, semreg.EvaluationContext) {
	digest := sha256.Sum256([]byte("synthetic DHW qualification control " + service))
	evidence := []semreg.EvidenceRef{{Owner: "test.dhw", Kind: "test.synthetic", Digest: semreg.Digest("sha256:" + hex.EncodeToString(digest[:])),
		Contract: "test.synthetic/v1", Access: semreg.EvidenceAccessPublic, Redaction: semreg.RedactionNone}}
	id := thermalDhwIdentity{Asset: "asset:synthetic", Service: semreg.ServiceInstanceID("dhw:" + service),
		Source: semreg.SourceID("source:" + service), SourceEpoch: semreg.SourceEpochID("epoch:" + service), DriverGeneration: "2",
		Binding: semreg.NativeBindingID("binding:" + service), BindingRevision: "7", AssociationRevision: "9",
		Selector: thermalDhwSelector{PB: 0xb5, SB: 0x24, Opcode: 2, OperationType: 0, Group: 1, Instance: 0, Register: 4,
			Direction: "controller_to_initiator", Initiator: "initiator:synthetic", Controller: "controller:" + service, Codec: "f32:qualified", Profile: "profile:synthetic"}}
	context := semreg.EvaluationContext{EvaluatedAt: semreg.TimePoint{UnixNanoseconds: "200", ClockID: "clock.synthetic", UncertaintyNS: "0"},
		EvaluateMonotonic: semreg.MonotonicPoint{ClockEpochID: "clock:synthetic", Nanoseconds: "200"}}
	raw := make([]byte, 4)
	binary.LittleEndian.PutUint32(raw, math.Float32bits(float32(integer)))
	observation := thermalDhwObservation{Identity: id, Presentation: "controller", Value: &semreg.Value{Kind: semreg.ValueQuantity, Quantity: thermalDhwTestQuantity(integer)},
		Quality: semreg.Quality{Assertion: semreg.AssertionObserved, Qualification: semreg.QualificationQualified, Validity: semreg.ValidityGood,
			Availability: semreg.AvailabilityAvailable, Freshness: semreg.FreshnessFresh, Reasons: []semreg.DefinitionID{}},
		Times: semreg.Times{ReceivedAt: semreg.TimePoint{UnixNanoseconds: "100", ClockID: "clock.synthetic", UncertaintyNS: "0"},
			ReceiptMonotonic: semreg.MonotonicPoint{ClockEpochID: "clock:synthetic", Nanoseconds: "100"}},
		RawReply: raw, Evidence: evidence}
	binding := thermalDhwBinding{Identity: id, Qualification: semreg.QualificationQualified, Evidence: evidence,
		Service: semreg.ServiceInstance{InstanceID: id.Service, AssetID: id.Asset, BindingID: id.Binding, SourceEpochID: id.SourceEpoch, DriverGeneration: id.DriverGeneration,
			Definition:    semreg.DefinitionRef{Pack: semreg.PackRef{ID: "helianthus.pack.thermal", Version: "1.0.0"}, ID: "thermal.service.dhw", Version: "1.0.0"},
			Qualification: semreg.QualificationQualified, Availability: semreg.AvailabilityAvailable, Revision: "3"},
		Domain: thermalDhwDomain{Minimum: thermalDhwTestQuantity(35), Maximum: thermalDhwTestQuantity(70), Qualification: semreg.QualificationQualified, Evidence: evidence}}
	return observation, binding, context
}

func thermalDhwTestLoss(source thermalDhwSourceResult, description string) bool {
	for _, loss := range source.Loss {
		if loss.Description == description {
			return true
		}
	}
	return false
}

func thermalDhwTestSource(t *testing.T, result thermalDhwMapping, identity thermalDhwIdentity) thermalDhwSourceResult {
	t.Helper()
	for _, source := range result.Sources {
		if source.Observation != nil && source.Identity == identity {
			return source
		}
	}
	t.Fatal("observation evidence lost")
	return thermalDhwSourceResult{}
}

func TestThermalDhwSetpointMappingDomains(t *testing.T) {
	for _, test := range []struct {
		name                    string
		value, minimum, maximum int
		eligible                bool
		loss                    string
	}{
		{"target50", 50, 35, 70, true, ""},
		{"nativeMinimum", 35, 35, 70, true, ""},
		{"nativeMaximum", 70, 35, 70, true, ""},
		{"zeroPermitted", 0, 0, 70, true, ""},
		{"zeroNativeInvalid", 0, 35, 70, false, "value outside or missing qualified native domain"},
		{"nativeInvalid80", 80, 35, 70, false, "value outside or missing qualified native domain"},
		{"packInvalid160", 160, 0, 200, false, "value outside thermal pack representation"},
		{"packInvalidBelow", -51, -100, 200, false, "value outside thermal pack representation"},
		{"packMinimum", -50, -100, 200, true, ""},
		{"packMaximum", 150, -100, 200, true, ""},
		{"reversedDomain", 50, 70, 35, false, "value outside or missing qualified native domain"},
	} {
		t.Run(test.name, func(t *testing.T) {
			observation, binding, evaluation := thermalDhwTestFixture("alpha", test.value)
			binding.Domain.Minimum, binding.Domain.Maximum = thermalDhwTestQuantity(test.minimum), thermalDhwTestQuantity(test.maximum)
			result := mapThermalDhwSetpoints([]thermalDhwObservation{observation}, []thermalDhwBinding{binding}, evaluation)
			source := thermalDhwTestSource(t, result, observation.Identity)
			if source.Eligible != test.eligible || (test.loss != "" && !thermalDhwTestLoss(source, test.loss)) {
				t.Fatalf("source checks: %+v", source)
			}
			if !reflect.DeepEqual(source.Observation.Value, observation.Value) || !reflect.DeepEqual(source.Binding.Domain, binding.Domain) {
				t.Fatal("value clamped or context changed")
			}
			if result.Disposition.ItemID != "thermal.setpoint.dhw_temperature" || result.Disposition.Kind != projection.ItemFact || result.Disposition.Outcome != projection.ProjectionWithheld || len(result.Disposition.SourceKeys) != 0 || result.Disposition.Loss[0].Kind != projection.LossIdentity {
				t.Fatalf("service identity blocker lost: %+v", result.Disposition)
			}
		})
	}
}

func TestThermalDhwSetpointMappingIdentity(t *testing.T) {
	mutations := []struct {
		name   string
		change func(*thermalDhwIdentity)
	}{
		{"asset", func(id *thermalDhwIdentity) { id.Asset = "asset:replacement" }},
		{"service", func(id *thermalDhwIdentity) { id.Service = "dhw:replacement" }},
		{"source", func(id *thermalDhwIdentity) { id.Source = "source:replacement" }},
		{"epoch", func(id *thermalDhwIdentity) { id.SourceEpoch = "epoch:replacement" }},
		{"generation", func(id *thermalDhwIdentity) { id.DriverGeneration = "3" }},
		{"binding", func(id *thermalDhwIdentity) { id.Binding = "binding:replacement" }},
		{"bindingRevision", func(id *thermalDhwIdentity) { id.BindingRevision = "8" }},
		{"associationRevision", func(id *thermalDhwIdentity) { id.AssociationRevision = "10" }},
		{"pb", func(id *thermalDhwIdentity) { id.Selector.PB = 0xb6 }},
		{"sb", func(id *thermalDhwIdentity) { id.Selector.SB = 0x09 }},
		{"opcode", func(id *thermalDhwIdentity) { id.Selector.Opcode = 0x06 }},
		{"operationType", func(id *thermalDhwIdentity) { id.Selector.OperationType = 1 }},
		{"group", func(id *thermalDhwIdentity) { id.Selector.Group = 0 }},
		{"instance", func(id *thermalDhwIdentity) { id.Selector.Instance = 1 }},
		{"register", func(id *thermalDhwIdentity) { id.Selector.Register = 0x46 }},
		{"direction", func(id *thermalDhwIdentity) { id.Selector.Direction = "initiator_to_controller" }},
		{"initiator", func(id *thermalDhwIdentity) { id.Selector.Initiator = "initiator:other" }},
		{"controller", func(id *thermalDhwIdentity) { id.Selector.Controller = "controller:other" }},
		{"codec", func(id *thermalDhwIdentity) { id.Selector.Codec = "codec:other" }},
		{"profile", func(id *thermalDhwIdentity) { id.Selector.Profile = "profile:other" }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			alpha, alphaBinding, evaluation := thermalDhwTestFixture("alpha", 50)
			beta, betaBinding, _ := thermalDhwTestFixture("beta", 55)
			mutation.change(&alpha.Identity)
			result := mapThermalDhwSetpoints([]thermalDhwObservation{alpha, beta}, []thermalDhwBinding{alphaBinding, betaBinding}, evaluation)
			changed := thermalDhwTestSource(t, result, alpha.Identity)
			if changed.Eligible || changed.Binding != nil || !thermalDhwTestLoss(changed, "missing or ambiguous independently qualified binding") {
				t.Fatalf("foreign identity borrowed original qualification: %+v", changed)
			}
			if !thermalDhwTestSource(t, result, beta.Identity).Eligible || len(result.Sources) != 3 {
				t.Fatal("identity mismatch affected sibling or erased original absence")
			}
		})
	}
}

func TestThermalDhwSetpointMappingIndependentAxes(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*thermalDhwObservation, *thermalDhwBinding)
		loss   string
	}{
		{"freshness", func(o *thermalDhwObservation, _ *thermalDhwBinding) { o.Quality.Freshness = semreg.FreshnessStale }, "original observation is not fresh"},
		{"missingReceipt", func(o *thermalDhwObservation, _ *thermalDhwBinding) { o.Times.ReceivedAt = semreg.TimePoint{} }, "original observation is not fresh"},
		{"receiptClockEpoch", func(o *thermalDhwObservation, _ *thermalDhwBinding) {
			o.Times.ReceiptMonotonic.ClockEpochID = "clock:other"
		}, "original observation is not fresh"},
		{"validity", func(o *thermalDhwObservation, _ *thermalDhwBinding) { o.Quality.Validity = semreg.ValidityBad }, "missing or invalid native observation"},
		{"availability", func(o *thermalDhwObservation, _ *thermalDhwBinding) {
			o.Quality.Availability = semreg.AvailabilityUnavailable
		}, "missing or invalid native observation"},
		{"observationQualification", func(o *thermalDhwObservation, _ *thermalDhwBinding) {
			o.Quality.Qualification = semreg.QualificationCandidate
		}, "source, service and native domain require independent qualification"},
		{"bindingQualification", func(_ *thermalDhwObservation, b *thermalDhwBinding) { b.Qualification = semreg.QualificationUnknown }, "source, service and native domain require independent qualification"},
		{"serviceQualification", func(_ *thermalDhwObservation, b *thermalDhwBinding) {
			b.Service.Qualification = semreg.QualificationCandidate
		}, "source, service and native domain require independent qualification"},
		{"domainQualification", func(_ *thermalDhwObservation, b *thermalDhwBinding) {
			b.Domain.Qualification = semreg.QualificationUnknown
		}, "source, service and native domain require independent qualification"},
		{"serviceUnavailable", func(_ *thermalDhwObservation, b *thermalDhwBinding) {
			b.Service.Availability = semreg.AvailabilityUnavailable
		}, "source, service and native domain require independent qualification"},
		{"serviceAsset", func(_ *thermalDhwObservation, b *thermalDhwBinding) { b.Service.AssetID = "asset:other" }, "service does not match source ownership"},
		{"serviceInstance", func(_ *thermalDhwObservation, b *thermalDhwBinding) { b.Service.InstanceID = "dhw:other" }, "service does not match source ownership"},
		{"serviceBinding", func(_ *thermalDhwObservation, b *thermalDhwBinding) { b.Service.BindingID = "binding:other" }, "service does not match source ownership"},
		{"serviceEpoch", func(_ *thermalDhwObservation, b *thermalDhwBinding) { b.Service.SourceEpochID = "epoch:other" }, "service does not match source ownership"},
		{"serviceGeneration", func(_ *thermalDhwObservation, b *thermalDhwBinding) { b.Service.DriverGeneration = "3" }, "service does not match source ownership"},
		{"serviceDefinition", func(_ *thermalDhwObservation, b *thermalDhwBinding) { b.Service.Definition.ID = "thermal.service.zone" }, "service does not match source ownership"},
		{"missingMinimum", func(_ *thermalDhwObservation, b *thermalDhwBinding) { b.Domain.Minimum = nil }, "value outside or missing qualified native domain"},
		{"missingMaximum", func(_ *thermalDhwObservation, b *thermalDhwBinding) { b.Domain.Maximum = nil }, "value outside or missing qualified native domain"},
		{"wrongMinimumUnit", func(_ *thermalDhwObservation, b *thermalDhwBinding) { b.Domain.Minimum.Unit = "unit.watt" }, "value outside or missing qualified native domain"},
		{"wrongMaximumUnit", func(_ *thermalDhwObservation, b *thermalDhwBinding) { b.Domain.Maximum.Unit = "unit.watt" }, "value outside or missing qualified native domain"},
		{"missingTargetMax65", func(o *thermalDhwObservation, b *thermalDhwBinding) {
			o.Value = nil
			b.Domain.Maximum = thermalDhwTestQuantity(65)
		}, "requested temperature requires an exact Celsius quantity"},
		{"booleanFalse", func(o *thermalDhwObservation, _ *thermalDhwBinding) {
			value := false
			o.Value = &semreg.Value{Kind: semreg.ValueBoolean, Boolean: &value}
		}, "requested temperature requires an exact Celsius quantity"},
		{"wrongUnit", func(o *thermalDhwObservation, _ *thermalDhwBinding) { o.Value.Quantity.Unit = "unit.watt" }, "requested temperature requires an exact Celsius quantity"},
		{"noncanonicalDecimal", func(o *thermalDhwObservation, _ *thermalDhwBinding) { o.Value.Quantity.Number.Coefficient = "50" }, "requested temperature requires an exact Celsius quantity"},
		{"nativeEvidence", func(o *thermalDhwObservation, _ *thermalDhwBinding) { o.Evidence = nil }, "missing or invalid native observation"},
		{"rawReply", func(o *thermalDhwObservation, _ *thermalDhwBinding) { o.RawReply = nil }, "missing or invalid native observation"},
		{"bindingEvidence", func(_ *thermalDhwObservation, b *thermalDhwBinding) { b.Evidence = nil }, "source, service and native domain require independent qualification"},
		{"domainEvidence", func(_ *thermalDhwObservation, b *thermalDhwBinding) { b.Domain.Evidence = nil }, "source, service and native domain require independent qualification"},
	} {
		t.Run(test.name, func(t *testing.T) {
			alpha, binding, evaluation := thermalDhwTestFixture("alpha", 50)
			beta, betaBinding, _ := thermalDhwTestFixture("beta", 55)
			test.change(&alpha, &binding)
			result := mapThermalDhwSetpoints([]thermalDhwObservation{alpha, beta}, []thermalDhwBinding{binding, betaBinding}, evaluation)
			source := thermalDhwTestSource(t, result, alpha.Identity)
			if source.Eligible || !thermalDhwTestLoss(source, test.loss) || !thermalDhwTestSource(t, result, beta.Identity).Eligible {
				t.Fatalf("independent source check failed: %+v", source)
			}
			if !reflect.DeepEqual(source.Observation.Quality, alpha.Quality) || source.Observation.Times.ReceivedAt != alpha.Times.ReceivedAt || source.Observation.Times.ReceiptMonotonic != alpha.Times.ReceiptMonotonic || source.Observation.Times.EvaluatedAt != evaluation.EvaluatedAt {
				t.Fatal("qualification, validity, freshness or receipt relabelled")
			}
		})
	}
}

func TestThermalDhwSetpointMappingRoles(t *testing.T) {
	for _, role := range []string{"controller", "controller_mirror", "boiler_direct", "context", "boiler_mirror"} {
		t.Run(role, func(t *testing.T) {
			observation, binding, evaluation := thermalDhwTestFixture("alpha", 50)
			observation.Presentation = role
			result := mapThermalDhwSetpoints([]thermalDhwObservation{observation}, []thermalDhwBinding{binding}, evaluation)
			source := thermalDhwTestSource(t, result, observation.Identity)
			if source.Eligible != (role == "controller" || role == "controller_mirror") || source.Observation.Identity != observation.Identity || !reflect.DeepEqual(source.Observation.Evidence, observation.Evidence) || source.Observation.Times.ReceivedAt != observation.Times.ReceivedAt {
				t.Fatal("presentation changed owner, receipt or source admission")
			}
		})
	}
	for _, test := range []struct {
		name                string
		sb, group, instance byte
		register            uint16
	}{
		{"boilerEA03", 9, 1, 0, 0xea03}, {"flowCeiling", 0x24, 0, 0, 0x46},
		{"sameGroupNonTarget", 0x24, 1, 0, 0x46},
		{"heatPumpCeiling", 0x24, 0, 0, 0xfc}, {"cylinderMaximum", 0x24, 5, 0, 1}, {"inferredInstance", 0x24, 1, 1, 4},
	} {
		t.Run(test.name, func(t *testing.T) {
			observation, binding, evaluation := thermalDhwTestFixture("alpha", 50)
			observation.Identity.Selector.SB, observation.Identity.Selector.Group, observation.Identity.Selector.Instance, observation.Identity.Selector.Register = test.sb, test.group, test.instance, test.register
			binding.Identity = observation.Identity
			source := thermalDhwTestSource(t, mapThermalDhwSetpoints([]thermalDhwObservation{observation}, []thermalDhwBinding{binding}, evaluation), observation.Identity)
			if source.Eligible || !thermalDhwTestLoss(source, "not the controller requested-target source; preserve as native context") {
				t.Fatal("context substituted for requested target")
			}
		})
	}
}

func TestThermalDhwSetpointMappingPartialAndReplacement(t *testing.T) {
	alpha, a, evaluation := thermalDhwTestFixture("alpha", 50)
	beta, b, _ := thermalDhwTestFixture("beta", 55)
	both := mapThermalDhwSetpoints([]thermalDhwObservation{alpha, beta}, []thermalDhwBinding{a, b}, evaluation)
	if len(both.Sources) != 2 || !thermalDhwTestSource(t, both, alpha.Identity).Eligible || !thermalDhwTestSource(t, both, beta.Identity).Eligible {
		t.Fatal("two services collapsed")
	}
	partial := mapThermalDhwSetpoints([]thermalDhwObservation{alpha}, []thermalDhwBinding{a, b}, evaluation)
	if len(partial.Sources) != 2 || !thermalDhwTestSource(t, partial, alpha.Identity).Eligible || !reflect.DeepEqual(partial.Disposition, both.Disposition) {
		t.Fatal("absence changed shared disposition or sibling")
	}
	for _, source := range partial.Sources {
		if source.Identity == beta.Identity && (source.Observation != nil || source.Eligible || source.Binding.Domain.Maximum.Number != thermalDhwTestQuantity(70).Number) {
			t.Fatal("absence synthesized target or lost context")
		}
	}
	b.Identity.SourceEpoch, beta.Identity.SourceEpoch = "epoch:replacement", "epoch:replacement"
	b.Service.SourceEpochID = "epoch:replacement"
	b.Qualification = semreg.QualificationUnknown
	replacement := mapThermalDhwSetpoints([]thermalDhwObservation{alpha, beta}, []thermalDhwBinding{a, b}, evaluation)
	if thermalDhwTestSource(t, replacement, beta.Identity).Eligible || !thermalDhwTestSource(t, replacement, alpha.Identity).Eligible {
		t.Fatal("replacement copied qualification or affected sibling")
	}
	beta, b, _ = thermalDhwTestFixture("beta", 55)
	if !reflect.DeepEqual(both, mapThermalDhwSetpoints([]thermalDhwObservation{beta, alpha}, []thermalDhwBinding{b, a}, evaluation)) {
		t.Fatal("nondeterministic ordering")
	}
}

func TestThermalDhwSetpointMappingExactDecimal(t *testing.T) {
	observation, binding, evaluation := thermalDhwTestFixture("alpha", 50)
	observation.Value.Quantity.Number = semreg.Decimal{Coefficient: "50125", Exponent10: -3}
	binary.LittleEndian.PutUint32(observation.RawReply, math.Float32bits(50.125))
	source := thermalDhwTestSource(t, mapThermalDhwSetpoints([]thermalDhwObservation{observation}, []thermalDhwBinding{binding}, evaluation), observation.Identity)
	if !source.Eligible || source.Observation.Value.Quantity.Number != (semreg.Decimal{Coefficient: "50125", Exponent10: -3}) {
		t.Fatal("exact decoded quantity rounded")
	}
}

func TestThermalDhwSetpointMappingStaleMirrorAndEvaluation(t *testing.T) {
	controller, binding, evaluation := thermalDhwTestFixture("alpha", 50)
	mirror, _, _ := thermalDhwTestFixture("alpha", 55)
	controller.Times.SourceAt = &semreg.TimePoint{UnixNanoseconds: "90", ClockID: "clock.synthetic", UncertaintyNS: "1"}
	controller.Times.PhenomenonAt = &semreg.TimePoint{UnixNanoseconds: "80", ClockID: "clock.synthetic", UncertaintyNS: "2"}
	mirror.Times.SourceAt = &semreg.TimePoint{UnixNanoseconds: "85", ClockID: "clock.synthetic", UncertaintyNS: "3"}
	mirror.Times.PhenomenonAt = &semreg.TimePoint{UnixNanoseconds: "75", ClockID: "clock.synthetic", UncertaintyNS: "4"}
	mirror.Presentation, mirror.Quality.Freshness = "controller_mirror", semreg.FreshnessStale
	later := evaluation
	later.EvaluatedAt.UnixNanoseconds, later.EvaluateMonotonic.Nanoseconds = "300", "300"
	for _, context := range []semreg.EvaluationContext{evaluation, later} {
		result := mapThermalDhwSetpoints([]thermalDhwObservation{controller, mirror}, []thermalDhwBinding{binding}, context)
		if len(result.Sources) != 2 {
			t.Fatal("mirror evidence lost")
		}
		for _, source := range result.Sources {
			original := controller
			if source.Observation.Presentation == "controller_mirror" {
				original = mirror
			}
			if !reflect.DeepEqual(source.Observation.Times.SourceAt, original.Times.SourceAt) || !reflect.DeepEqual(source.Observation.Times.PhenomenonAt, original.Times.PhenomenonAt) {
				t.Fatal("retained source or phenomenon timestamp refreshed")
			}
			if source.Eligible != (source.Observation.Presentation == "controller") || source.Observation.Times.ReceivedAt.UnixNanoseconds != "100" || source.Observation.Times.ReceiptMonotonic.Nanoseconds != "100" || source.Observation.Times.EvaluatedAt != context.EvaluatedAt || source.Observation.Times.EvaluateMonotonic != context.EvaluateMonotonic {
				t.Fatal("mirror renewed receipt or bypassed freshness")
			}
		}
	}
}

func TestThermalDhwSetpointMappingAmbiguityAndAbsence(t *testing.T) {
	observation, binding, evaluation := thermalDhwTestFixture("alpha", 50)
	_, other, _ := thermalDhwTestFixture("alpha", 50)
	other.Evidence = []semreg.EvidenceRef{other.Evidence[0]}
	other.Domain.Minimum = thermalDhwTestQuantity(40)
	other.Domain.Maximum = thermalDhwTestQuantity(65)
	other.Evidence[0].Kind = "test.other.binding"
	other.Domain.Evidence[0].Kind = "test.other.domain"
	ambiguous := mapThermalDhwSetpoints([]thermalDhwObservation{observation}, []thermalDhwBinding{binding, other}, evaluation)
	source := thermalDhwTestSource(t, ambiguous, observation.Identity)
	if source.Eligible || !thermalDhwTestLoss(source, "missing or ambiguous independently qualified binding") {
		t.Fatal("ambiguous binding admitted")
	}
	if len(ambiguous.Sources) != 3 || source.Binding != nil || ambiguous.Disposition.Outcome != projection.ProjectionWithheld {
		t.Fatal("ambiguous records collapsed or admitted")
	}
	for _, expected := range []thermalDhwBinding{binding, other} {
		count := 0
		for _, retained := range ambiguous.Sources {
			if reflect.DeepEqual(retained.Binding, &expected) {
				count++
				if retained.Observation != nil || retained.Eligible || retained.Identity != expected.Identity {
					t.Fatal("ambiguous binding synthesized an observation or admission")
				}
			}
		}
		if count != 1 {
			t.Fatalf("ambiguous binding context retained %d times, want once: %+v", count, expected)
		}
	}
	binding.Domain.Maximum = thermalDhwTestQuantity(65)
	absent := mapThermalDhwSetpoints(nil, []thermalDhwBinding{binding}, evaluation)
	if len(absent.Sources) != 1 || absent.Sources[0].Observation != nil || absent.Sources[0].Eligible || absent.Sources[0].Binding.Domain.Maximum.Number != (semreg.Decimal{Coefficient: "65", Exponent10: 0}) {
		t.Fatal("absent target lost maximum or synthesized fact")
	}
}

func TestThermalDhwSetpointMappingDetached(t *testing.T) {
	alpha, binding, evaluation := thermalDhwTestFixture("alpha", 50)
	alpha.Times.SourceAt = &semreg.TimePoint{UnixNanoseconds: "90", ClockID: "clock.synthetic", UncertaintyNS: "0"}
	inputs, bindings := []thermalDhwObservation{alpha, alpha}, []thermalDhwBinding{binding}
	inputs[1].Presentation = "controller_mirror"
	beforeInputs, _ := json.Marshal(inputs)
	beforeBindings, _ := json.Marshal(bindings)
	result := mapThermalDhwSetpoints(inputs, bindings, evaluation)
	afterInputs, _ := json.Marshal(inputs)
	afterBindings, _ := json.Marshal(bindings)
	if string(beforeInputs) != string(afterInputs) || string(beforeBindings) != string(afterBindings) {
		t.Fatal("mapper mutated input")
	}
	snapshot, _ := json.Marshal(result)
	inputs[0].Value.Quantity.Number = semreg.Decimal{Coefficient: "6", Exponent10: 1}
	inputs[0].RawReply[0] = 99
	inputs[0].Evidence[0].Kind = "test.changed"
	inputs[0].Times.SourceAt.UnixNanoseconds = "999"
	bindings[0].Domain.Maximum.Number = semreg.Decimal{Coefficient: "8", Exponent10: 1}
	bindings[0].Domain.Evidence[0].Kind = "test.changed"
	afterMutation, _ := json.Marshal(result)
	if string(snapshot) != string(afterMutation) {
		t.Fatal("input aliases output")
	}
	otherBefore, _ := json.Marshal(result.Sources[1])
	result.Sources[0].Observation.Value.Quantity.Number.Coefficient = "9"
	result.Sources[0].Observation.RawReply[0] = 77
	result.Sources[0].Observation.Times.SourceAt.UnixNanoseconds = "777"
	result.Sources[0].Binding.Domain.Maximum.Number.Coefficient = "9"
	result.Sources[0].Binding.Evidence[0].Kind = "test.output"
	otherAfter, _ := json.Marshal(result.Sources[1])
	if string(otherBefore) != string(otherAfter) || inputs[0].RawReply[0] != 99 || bindings[0].Domain.Maximum.Number.Coefficient != "8" {
		t.Fatal("output aliases input or sibling")
	}
}

func TestThermalDhwSetpointMappingPackIdentityBlocker(t *testing.T) {
	temperature, service := "temperature:dhw_requested", "dhw:alpha"
	key := semreg.FactKey{PackID: "helianthus.pack.thermal", PackVersion: "1.0.0", FactID: "thermal.setpoint.dhw_temperature",
		Dimensions: []semreg.Dimension{{ID: "thermal.dimension.temperature", Value: semreg.Value{Kind: semreg.ValueText, Text: &temperature}}}}
	value := &semreg.Value{Kind: semreg.ValueQuantity, Quantity: thermalDhwTestQuantity(50)}
	if err := thermal.New().ValidateFact(key, value); err != nil {
		t.Fatalf("representation control: %v", err)
	}
	key.Dimensions = append(key.Dimensions, semreg.Dimension{ID: "thermal.dimension.dhw", Value: semreg.Value{Kind: semreg.ValueText, Text: &service}})
	if thermal.New().ValidateFact(key, value) == nil {
		t.Fatal("pack changed; source-bound cutover requires review")
	}
}

func TestI40EvidenceReferences(t *testing.T) {
	for _, location := range []string{"binding", "domain", "observation"} {
		for _, field := range []string{"zero", "owner", "kind", "digest", "contract", "access", "redaction"} {
			for _, position := range []string{"first", "last"} {
				t.Run(location+"/"+field+"/"+position, func(t *testing.T) {
					o, b, evaluation := thermalDhwTestFixture("alpha", 50)
					if o.Evidence[0].Validate() != nil {
						t.Fatal("valid reference control rejected")
					}
					invalid := o.Evidence[0]
					switch field {
					case "zero":
						invalid = semreg.EvidenceRef{}
					case "owner":
						invalid.Owner = "\n"
					case "kind":
						invalid.Kind = "\n"
					case "digest":
						invalid.Digest = "invalid"
					case "contract":
						invalid.Contract = "invalid"
					case "access":
						invalid.Access = "invalid"
					case "redaction":
						invalid.Redaction = "invalid"
					}
					if invalid.Validate() == nil {
						t.Fatal("invalid evidence control unexpectedly valid")
					}
					refs := []semreg.EvidenceRef{invalid, o.Evidence[0]}
					if position == "last" {
						refs[0], refs[1] = refs[1], refs[0]
					}
					switch location {
					case "binding":
						b.Evidence = refs
					case "domain":
						b.Domain.Evidence = refs
					case "observation":
						o.Evidence = refs
					}
					source := thermalDhwTestSource(t, mapThermalDhwSetpoints([]thermalDhwObservation{o}, []thermalDhwBinding{b}, evaluation), o.Identity)
					if source.Eligible || !reflect.DeepEqual(source.Binding, &b) || !reflect.DeepEqual(source.Observation.Evidence, o.Evidence) {
						t.Fatal("invalid reference admitted or evidence discarded")
					}
				})
			}
		}
	}
}

func TestI40ObservedAssertion(t *testing.T) {
	for _, assertion := range []semreg.AssertionKind{semreg.AssertionObserved, semreg.AssertionInferred, semreg.AssertionKind("unknown"), semreg.AssertionKind("")} {
		t.Run(string(assertion), func(t *testing.T) {
			o, b, evaluation := thermalDhwTestFixture("alpha", 50)
			o.Quality.Assertion = assertion
			source := thermalDhwTestSource(t, mapThermalDhwSetpoints([]thermalDhwObservation{o}, []thermalDhwBinding{b}, evaluation), o.Identity)
			if source.Eligible != (assertion == semreg.AssertionObserved) || source.Observation.Quality.Assertion != assertion {
				t.Fatal("non-observed assertion admitted or relabelled")
			}
		})
	}
}

func TestI40IdenticalMalformedIdentity(t *testing.T) {
	for _, field := range []string{"Asset", "Service", "Source", "SourceEpoch", "DriverGeneration", "Binding", "BindingRevision", "AssociationRevision"} {
		for _, malformed := range []string{"", "\n", "abc\n"} {
			t.Run(field+"/"+strconv.Quote(malformed), func(t *testing.T) {
				o, b, evaluation := thermalDhwTestFixture("alpha", 50)
				if !thermalDhwValidIdentity(o.Identity) {
					t.Fatal("valid identity control rejected")
				}
				reflect.ValueOf(&o.Identity).Elem().FieldByName(field).SetString(malformed)
				b.Identity = o.Identity
				b.Service.AssetID, b.Service.InstanceID = o.Identity.Asset, o.Identity.Service
				b.Service.SourceEpochID, b.Service.BindingID = o.Identity.SourceEpoch, o.Identity.Binding
				b.Service.DriverGeneration = o.Identity.DriverGeneration
				// Exercise each typed guard independently of overlapping service validation.
				if thermalDhwValidIdentity(o.Identity) {
					t.Fatal("malformed identity accepted by typed guard")
				}
				source := thermalDhwTestSource(t, mapThermalDhwSetpoints([]thermalDhwObservation{o}, []thermalDhwBinding{b}, evaluation), o.Identity)
				if source.Eligible || source.Binding == nil || !thermalDhwTestLoss(source, "service does not match source ownership") {
					t.Fatal("identical malformed ownership admitted or lost")
				}
			})
		}
	}
	for _, field := range []string{"DriverGeneration", "BindingRevision", "AssociationRevision"} {
		t.Run(field+"/nonnumeric", func(t *testing.T) {
			o, b, evaluation := thermalDhwTestFixture("alpha", 50)
			reflect.ValueOf(&o.Identity).Elem().FieldByName(field).SetString("abc")
			b.Identity = o.Identity
			b.Service.DriverGeneration = o.Identity.DriverGeneration
			if thermalDhwValidIdentity(o.Identity) || thermalDhwTestSource(t, mapThermalDhwSetpoints([]thermalDhwObservation{o}, []thermalDhwBinding{b}, evaluation), o.Identity).Eligible {
				t.Fatal("identical nonnumeric revision admitted")
			}
		})
	}
}

func TestI40FullyPopulatedDetach(t *testing.T) {
	flag, text := false, "synthetic text"
	symbol := semreg.Symbol{Namespace: "test.symbol", Token: "synthetic", Known: true}
	point := semreg.TimePoint{UnixNanoseconds: "80", ClockID: "clock.synthetic", UncertaintyNS: "1"}
	values := []semreg.Value{
		{Kind: semreg.ValueQuantity, Quantity: thermalDhwTestQuantity(50)},
		{Kind: semreg.ValueBoolean, Boolean: &flag},
		{Kind: semreg.ValueText, Text: &text},
		{Kind: semreg.ValueSymbol, Symbol: &symbol},
		{Kind: semreg.ValueSymbols, Symbols: []semreg.Symbol{symbol}},
		{Kind: semreg.ValueTime, Time: &point},
	}
	for _, value := range values {
		t.Run(string(value.Kind), func(t *testing.T) {
			o, b, evaluation := thermalDhwTestFixture("alpha", 50)
			o.Value = &value
			o.Quality.Promotion = semreg.PromotionPromoted
			o.Quality.Reasons = []semreg.DefinitionID{"test.reason"}
			o.Times.PhenomenonAt = &semreg.TimePoint{UnixNanoseconds: "70", ClockID: "clock.synthetic", UncertaintyNS: "2"}
			o.Times.SourceAt = &point
			o.Times.EvaluatedAt, o.Times.EvaluateMonotonic = evaluation.EvaluatedAt, evaluation.EvaluateMonotonic
			source := thermalDhwSourceResult{Identity: o.Identity, Observation: &o, Binding: &b, Eligible: true,
				Loss: []projection.LossDetail{{Kind: projection.LossIdentity, SourceItems: []semreg.DefinitionID{thermalDhwTarget}, Description: "synthetic diagnostic", Reversible: true}}}
			detached := thermalDhwDetached(source)
			if !reflect.DeepEqual(detached, source) {
				t.Fatalf("fully populated source lost fields: got %+v, want %+v", detached, source)
			}
			thermalDhwTestNoAliases(t, reflect.ValueOf(source), reflect.ValueOf(detached), "source")
		})
	}
}

func thermalDhwTestNoAliases(t *testing.T, input, output reflect.Value, path string) {
	t.Helper()
	switch input.Kind() {
	case reflect.Pointer, reflect.Slice:
		if input.IsNil() {
			return
		}
		if input.Pointer() == output.Pointer() && (input.Kind() == reflect.Pointer || input.Len() > 0) {
			t.Fatalf("detached nested storage aliases input at %s", path)
		}
		if input.Kind() == reflect.Pointer {
			thermalDhwTestNoAliases(t, input.Elem(), output.Elem(), path+"*")
		} else {
			for i := 0; i < input.Len(); i++ {
				thermalDhwTestNoAliases(t, input.Index(i), output.Index(i), path+"["+strconv.Itoa(i)+"]")
			}
		}
	case reflect.Struct:
		for i := 0; i < input.NumField(); i++ {
			thermalDhwTestNoAliases(t, input.Field(i), output.Field(i), path+"."+input.Type().Field(i).Name)
		}
	}
}
