package main

import (
	"encoding/json"
	"math/big"
	"sort"

	semreg "github.com/Project-Helianthus/helianthus-semreg/semreg/v1"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/packs/thermal"
	"github.com/Project-Helianthus/helianthus-semreg/semreg/v1/projection"
)

const thermalDhwTarget semreg.DefinitionID = "thermal.setpoint.dhw_temperature"

// These adapter records describe caller-decoded evidence, never acquisition or
// qualification discovery. Native roles follow the public DHW cutover contract:
// https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/dhw-setpoint-semreg-cutover.md
type thermalDhwSelector struct {
	PB, SB, Opcode, OperationType, Group, Instance   byte
	Register                                         uint16
	Direction, Initiator, Controller, Codec, Profile string
}

type thermalDhwIdentity struct {
	Asset               semreg.AssetID
	Service             semreg.ServiceInstanceID
	Source              semreg.SourceID
	SourceEpoch         semreg.SourceEpochID
	DriverGeneration    semreg.Uint64
	Binding             semreg.NativeBindingID
	BindingRevision     semreg.Uint64
	AssociationRevision semreg.Uint64
	Selector            thermalDhwSelector
}

type thermalDhwDomain struct {
	Minimum, Maximum *semreg.Quantity
	Qualification    semreg.Qualification
	Evidence         []semreg.EvidenceRef
}

type thermalDhwBinding struct {
	Identity      thermalDhwIdentity
	Service       semreg.ServiceInstance
	Qualification semreg.Qualification
	Evidence      []semreg.EvidenceRef
	Domain        thermalDhwDomain
}

type thermalDhwObservation struct {
	Identity thermalDhwIdentity
	// A mirror carries the original controller identity, receipt and evidence.
	Presentation string // controller, controller_mirror, boiler_direct, or context
	Value        *semreg.Value
	Quality      semreg.Quality
	Times        semreg.Times
	RawReply     []byte
	Evidence     []semreg.EvidenceRef
}

type thermalDhwSourceResult struct {
	Identity    thermalDhwIdentity
	Observation *thermalDhwObservation
	Binding     *thermalDhwBinding
	// Eligible means source and value checks passed, NOT consumer admission.
	Eligible bool
	Loss     []projection.LossDetail
}

type thermalDhwMapping struct {
	Sources     []thermalDhwSourceResult
	Disposition projection.ProjectionDisposition
}

// mapThermalDhwSetpoints is a detached, read-only proposal. It neither updates
// last-known-good state nor emits withdrawals. Source absence belongs in Sources,
// not in a second disposition for the shared target definition. The current pack
// cannot preserve the required DHW service dimension, so even eligible values
// remain withheld. Registration and persistence require a separate cutover.
func mapThermalDhwSetpoints(observations []thermalDhwObservation, bindings []thermalDhwBinding, evaluation semreg.EvaluationContext) thermalDhwMapping {
	observations = thermalDhwDetached(observations)
	bindings = thermalDhwDetached(bindings)
	result := thermalDhwMapping{Sources: []thermalDhwSourceResult{}, Disposition: projection.ProjectionDisposition{
		Kind: projection.ItemFact, ItemID: thermalDhwTarget, Outcome: projection.ProjectionWithheld,
		SourceKeys: []semreg.FactKey{}, Loss: []projection.LossDetail{{Kind: projection.LossIdentity,
			SourceItems: []semreg.DefinitionID{thermalDhwTarget}, Description: "current thermal fact cannot preserve DHW service identity"}},
	}}
	seen := make([]bool, len(bindings))
	validator := thermal.New()
	for _, observation := range observations {
		observation.Times.EvaluatedAt = evaluation.EvaluatedAt
		observation.Times.EvaluateMonotonic = evaluation.EvaluateMonotonic
		source := thermalDhwSourceResult{Identity: observation.Identity, Observation: &observation, Loss: []projection.LossDetail{}}
		index, matches := -1, 0
		for i := range bindings {
			if observation.Identity == bindings[i].Identity {
				index, matches = i, matches+1
				seen[i] = true
			}
		}
		if matches != 1 {
			thermalDhwLoss(&source, projection.LossIdentity, "missing or ambiguous independently qualified binding")
			result.Sources = append(result.Sources, source)
			continue
		}
		binding := bindings[index]
		source.Binding = &binding
		id, service, selector := binding.Identity, binding.Service, observation.Identity.Selector
		if id.Asset == "" || id.Service == "" || id.Source == "" || id.SourceEpoch == "" || id.DriverGeneration == "" || id.Binding == "" || id.BindingRevision == "" || id.AssociationRevision == "" ||
			service.AssetID != id.Asset || service.InstanceID != id.Service || service.BindingID != id.Binding || service.SourceEpochID != id.SourceEpoch || service.DriverGeneration != id.DriverGeneration ||
			service.Definition.ID != "thermal.service.dhw" || validator.ValidateService(service) != nil {
			thermalDhwLoss(&source, projection.LossIdentity, "service does not match source ownership")
		}
		if selector.PB != 0xb5 || selector.SB != 0x24 || selector.Opcode != 0x02 || selector.OperationType != 0x00 || selector.Group != 0x01 || selector.Instance != 0x00 || selector.Register != 0x0004 ||
			selector.Direction != "controller_to_initiator" || selector.Initiator == "" || selector.Controller == "" || selector.Codec == "" || selector.Profile == "" ||
			(observation.Presentation != "controller" && observation.Presentation != "controller_mirror") {
			thermalDhwLoss(&source, projection.LossProvenance, "not the controller requested-target source; preserve as native context")
		}
		if binding.Qualification != semreg.QualificationQualified || service.Qualification != semreg.QualificationQualified ||
			binding.Domain.Qualification != semreg.QualificationQualified || len(binding.Evidence) == 0 || len(binding.Domain.Evidence) == 0 ||
			observation.Quality.Qualification != semreg.QualificationQualified || service.Availability != semreg.AvailabilityAvailable {
			thermalDhwLoss(&source, projection.LossPolicy, "source, service and native domain require independent qualification")
		}
		if observation.Quality.Validity != semreg.ValidityGood || observation.Quality.Availability != semreg.AvailabilityAvailable || len(observation.RawReply) == 0 || len(observation.Evidence) == 0 {
			thermalDhwLoss(&source, projection.LossProvenance, "missing or invalid native observation")
		}
		if _, err := observation.Times.ElapsedMonotonicNS(); err != nil || observation.Quality.Freshness != semreg.FreshnessFresh {
			thermalDhwLoss(&source, projection.LossTime, "original observation is not fresh")
		}
		value := observation.Value
		if value == nil || value.Validate() != nil || value.Kind != semreg.ValueQuantity || value.Quantity == nil || value.Quantity.Unit != "unit.celsius" {
			thermalDhwLoss(&source, projection.LossUnit, "requested temperature requires an exact Celsius quantity")
		} else {
			minimum, maximum := binding.Domain.Minimum, binding.Domain.Maximum
			if !thermalDhwBound(minimum) || !thermalDhwBound(maximum) || thermalDhwCompare(minimum.Number, maximum.Number) > 0 ||
				thermalDhwCompare(value.Quantity.Number, minimum.Number) < 0 || thermalDhwCompare(value.Quantity.Number, maximum.Number) > 0 {
				thermalDhwLoss(&source, projection.LossRange, "value outside or missing qualified native domain")
			}
			temperature := "temperature:dhw_requested"
			pack := validator.Pack()
			key := semreg.FactKey{PackID: pack.ID, PackVersion: pack.Version, FactID: thermalDhwTarget,
				Dimensions: []semreg.Dimension{{ID: "thermal.dimension.temperature", Value: semreg.Value{Kind: semreg.ValueText, Text: &temperature}}}}
			// Validate representation separately, before the known service-identity
			// blocker. Dropping the service dimension never authorizes projection.
			if validator.ValidateFact(key, value) != nil {
				thermalDhwLoss(&source, projection.LossRange, "value outside thermal pack representation")
			}
		}
		source.Eligible = len(source.Loss) == 0
		result.Sources = append(result.Sources, source)
	}
	for i, binding := range bindings {
		if !seen[i] {
			source := thermalDhwSourceResult{Identity: binding.Identity, Binding: &binding, Loss: []projection.LossDetail{}}
			thermalDhwLoss(&source, projection.LossProvenance, "target absent; retain prior same-owner history separately without renewing receipt")
			result.Sources = append(result.Sources, source)
		}
	}
	sort.Slice(result.Sources, func(i, j int) bool {
		a, _ := json.Marshal(result.Sources[i])
		b, _ := json.Marshal(result.Sources[j])
		return string(a) < string(b)
	})
	// Repeated observations and mirrors must not alias each other either.
	return thermalDhwDetached(result)
}

func thermalDhwLoss(source *thermalDhwSourceResult, kind projection.LossKind, description string) {
	source.Loss = append(source.Loss, projection.LossDetail{Kind: kind, SourceItems: []semreg.DefinitionID{thermalDhwTarget}, Description: description})
}

func thermalDhwBound(bound *semreg.Quantity) bool {
	return bound != nil && bound.Unit == "unit.celsius" && bound.Number.Validate() == nil
}

func thermalDhwCompare(a, b semreg.Decimal) int {
	rat := func(decimal semreg.Decimal) *big.Rat {
		number, _ := new(big.Int).SetString(decimal.Coefficient, 10)
		exponent := int64(decimal.Exponent10)
		if exponent < 0 {
			return new(big.Rat).SetFrac(number, new(big.Int).Exp(big.NewInt(10), big.NewInt(-exponent), nil))
		}
		return new(big.Rat).SetInt(new(big.Int).Mul(number, new(big.Int).Exp(big.NewInt(10), big.NewInt(exponent), nil)))
	}
	return rat(a).Cmp(rat(b))
}

// All adapter fields are JSON-safe SemReg values or bytes; no external objects
// or arbitrary interfaces can reach this clone boundary.
func thermalDhwDetached[T any](value T) T {
	encoded, _ := json.Marshal(value)
	var detached T
	_ = json.Unmarshal(encoded, &detached)
	return detached
}
