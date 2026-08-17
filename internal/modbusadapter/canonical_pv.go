package modbusadapter

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	pv "github.com/Project-Helianthus/helianthus-ebusreg/pv"
	modbusreg "github.com/Project-Helianthus/helianthus-modbusreg"
)

const canonicalPVSourceRegistryRef pv.Digest = "sha256:e21d5d4914fba2249c68cc147243c22f89cc9e1f2be71e4565a3950f31e94750"

var canonicalPVSourceIdentity = pv.SourceIdentity{
	Protocol: "sunspec_modbus", ProfileID: modbusreg.SunSpecThreePhaseMonitoringCapabilityID,
	ProfileVersion: "1.0.0", Validity: pv.SourceTerminalVerified,
}

type canonicalPVResolver struct{}

func (canonicalPVResolver) ResolveSource(identity pv.SourceIdentity) (pv.Digest, bool) {
	return canonicalPVSourceRegistryRef, identity == canonicalPVSourceIdentity
}

type CanonicalPVMapper struct {
	registry *pv.Registry
	decoder  modbusreg.SunSpecDecoderRegistry
}

func NewCanonicalPVMapper() (*CanonicalPVMapper, error) {
	decoder, err := modbusreg.NewStandardSunSpecDecoderRegistry(modbusreg.SunSpecModelsRevisionV1)
	if err != nil {
		return nil, err
	}
	return &CanonicalPVMapper{registry: pv.NewRegistry(canonicalPVResolver{}), decoder: decoder}, nil
}

func (mapper *CanonicalPVMapper) Map(
	observation modbusreg.SunSpecQualificationObservation,
	encoded []byte,
	evaluated pv.MonotonicNanos,
) (pv.Snapshot, error) {
	if mapper == nil || mapper.registry == nil || len(encoded) == 0 || evaluated < 0 {
		return pv.Snapshot{}, errors.New("canonical PV mapping input is incomplete")
	}
	capability := observation.Capability()
	if !capability.Admitted() || capability.Reason() != modbusreg.SunSpecCapabilityReasonAdmitted ||
		capability.ProfileID() != canonicalPVSourceIdentity.ProfileID {
		return pv.Snapshot{}, errors.New("canonical PV source capability is not admitted")
	}
	assetRef, err := mapper.assetRef(observation)
	if err != nil {
		return pv.Snapshot{}, err
	}
	observationRef := pv.Digest(domainDigest("canonical-pv-observation-v1", encoded))
	provenance := pv.Provenance{
		SourceIdentity:       canonicalPVSourceIdentity,
		SourceRegistryRef:    canonicalPVSourceRegistryRef,
		SourceObservationRef: observationRef,
		SourceShadowRef:      pv.Digest(domainDigest("canonical-pv-shadow-v1", encoded)),
		EvidenceRef:          pv.Digest(domainDigest("canonical-pv-evidence-v1", encoded)),
	}
	update := pv.Update{
		AssetRef: assetRef, Evaluated: evaluated, SourceTimeState: pv.SourceTimeUnavailable,
		Source: provenance, Capability: pv.Capability{ID: pv.CapabilityThreePhaseTelemetryV1, Outcome: pv.CapabilitySatisfied},
	}
	for _, fact := range capability.Facts() {
		requested := pv.RequestedOutput{
			SourceRef:          observationRef,
			RequestedOutputRef: pv.Digest(domainDigest("canonical-pv-request-v1", []byte(fact.FieldID()))),
		}
		update.RequestedOutputs = append(update.RequestedOutputs, requested)
		input, mapped, err := mapSunSpecCapabilityFact(fact, evaluated)
		if err != nil {
			return pv.Snapshot{}, err
		}
		projection := pv.Projection{SourceRef: requested.SourceRef, RequestedOutputRef: requested.RequestedOutputRef}
		if mapped {
			dimensions := input.Candidate.Dimensions
			projection.FactID, projection.Dimensions, projection.Outcome = input.Candidate.ID, &dimensions, pv.ProjectionMapped
			update.Facts = append(update.Facts, input)
		} else {
			projection.Outcome = pv.ProjectionWithheld
		}
		update.ProjectionReport = append(update.ProjectionReport, projection)
	}
	return mapper.registry.Apply(update)
}

func (mapper *CanonicalPVMapper) assetRef(observation modbusreg.SunSpecQualificationObservation) (string, error) {
	var identity []string
	for _, occurrence := range observation.Occurrences() {
		if occurrence.ModelID() != 1 {
			continue
		}
		common, err := mapper.decoder.DecodeOccurrence(occurrence)
		if err != nil {
			return "", err
		}
		for _, fieldID := range []string{"device.manufacturer", "device.model", "device.serial"} {
			fact, ok := common.Fact(fieldID)
			if !ok {
				return "", errors.New("SunSpec Common identity is incomplete")
			}
			text, ok := fact.Value.Text()
			if !ok || text == "" {
				return "", errors.New("SunSpec Common identity is invalid")
			}
			identity = append(identity, text)
		}
		break
	}
	if len(identity) != 3 {
		return "", errors.New("SunSpec Common identity is absent")
	}
	sum := sha256.Sum256([]byte(strings.Join(identity, "\x00")))
	return "pv-asset-" + hex.EncodeToString(sum[:16]), nil
}

func mapSunSpecCapabilityFact(fact modbusreg.SunSpecCapabilityFact, receipt pv.MonotonicNanos) (pv.FactInput, bool, error) {
	type mapping struct {
		id         pv.FactID
		dimensions pv.Dimensions
		unit       pv.Unit
		policy     pv.PolicyID
	}
	mappings := map[string]mapping{
		"inverter.ac.current.phase_a":  {pv.FactACCurrent, pv.Dimensions{Phase: pv.PhaseL1}, pv.UnitAmpere, pv.PolicyTelemetryFastV1},
		"inverter.ac.current.phase_b":  {pv.FactACCurrent, pv.Dimensions{Phase: pv.PhaseL2}, pv.UnitAmpere, pv.PolicyTelemetryFastV1},
		"inverter.ac.current.phase_c":  {pv.FactACCurrent, pv.Dimensions{Phase: pv.PhaseL3}, pv.UnitAmpere, pv.PolicyTelemetryFastV1},
		"inverter.ac.voltage.phase_a":  {pv.FactACVoltageLineToNeutral, pv.Dimensions{Phase: pv.PhaseL1}, pv.UnitVolt, pv.PolicyTelemetryFastV1},
		"inverter.ac.voltage.phase_b":  {pv.FactACVoltageLineToNeutral, pv.Dimensions{Phase: pv.PhaseL2}, pv.UnitVolt, pv.PolicyTelemetryFastV1},
		"inverter.ac.voltage.phase_c":  {pv.FactACVoltageLineToNeutral, pv.Dimensions{Phase: pv.PhaseL3}, pv.UnitVolt, pv.PolicyTelemetryFastV1},
		"inverter.ac.power.active":     {pv.FactACActivePower, pv.Dimensions{Scope: pv.ScopeTotal}, pv.UnitWatt, pv.PolicyTelemetryFastV1},
		"inverter.ac.frequency":        {pv.FactACFrequency, pv.Dimensions{Scope: pv.ScopeTotal}, pv.UnitHertz, pv.PolicyTelemetryFastV1},
		"inverter.ac.energy_lifetime":  {pv.FactEnergyActiveExportTotal, pv.Dimensions{Scope: pv.ScopeTotal}, pv.UnitWattHour, pv.PolicyAccumulatorV1},
		"inverter.temperature.cabinet": {pv.FactTemperature, pv.Dimensions{SensorID: "cabinet"}, pv.UnitCelsius, pv.PolicyTelemetryFastV1},
		"inverter.operating_state":     {pv.FactOperatingState, pv.Dimensions{Scope: pv.ScopeTotal}, pv.UnitOne, pv.PolicyStatusV1},
	}
	target, ok := mappings[fact.FieldID()]
	if !ok {
		return pv.FactInput{}, false, nil
	}
	value := fact.Value()
	var canonical pv.FactValue
	switch target.id {
	case pv.FactOperatingState:
		_, symbol, ok := value.Enum()
		if !ok {
			return pv.FactInput{}, false, errors.New("SunSpec operating state is invalid")
		}
		mapped, ok := mapOperatingState(symbol)
		if !ok {
			return pv.FactInput{}, false, fmt.Errorf("SunSpec operating state %q is not representable", symbol)
		}
		canonical = pv.EnumFactValue(mapped)
	default:
		number, ok := value.Number()
		if !ok {
			return pv.FactInput{}, false, fmt.Errorf("SunSpec field %q is not numeric", fact.FieldID())
		}
		decimal, err := exactPVDecimal(number)
		if err != nil {
			return pv.FactInput{}, false, err
		}
		canonical = pv.DecimalFactValue(decimal)
	}
	return pv.FactInput{
		Candidate: pv.FactCandidate{ID: target.id, Dimensions: target.dimensions, Value: canonical, Unit: target.unit},
		Quality:   pv.QualityGood, Receipt: receipt, Policy: target.policy,
	}, true, nil
}

func exactPVDecimal(value string) (pv.Decimal, error) {
	negative := strings.HasPrefix(value, "-")
	unsigned := strings.TrimPrefix(value, "-")
	parts := strings.Split(unsigned, ".")
	if len(parts) > 2 || len(parts) == 0 || parts[0] == "" {
		return pv.Decimal{}, pv.ErrInvalidDecimal
	}
	coefficient, scale := parts[0], 0
	if len(parts) == 2 {
		if parts[1] == "" {
			return pv.Decimal{}, pv.ErrInvalidDecimal
		}
		coefficient += parts[1]
		scale = -len(parts[1])
	}
	coefficient = strings.TrimLeft(coefficient, "0")
	if coefficient == "" {
		coefficient, negative = "0", false
	}
	if negative {
		coefficient = "-" + coefficient
	}
	return pv.NewDecimal(coefficient, scale)
}

func mapOperatingState(symbol string) (string, bool) {
	states := map[string]string{
		"OFF": pv.OperatingStateOff, "SLEEPING": pv.OperatingStateStandby, "STARTING": pv.OperatingStateStarting,
		"MPPT": pv.OperatingStateOperating, "THROTTLED": pv.OperatingStateDerated, "SHUTTING_DOWN": pv.OperatingStateShuttingDown,
		"FAULT": pv.OperatingStateFault, "STANDBY": pv.OperatingStateStandby,
	}
	value, ok := states[strings.TrimPrefix(symbol, "gg")]
	return value, ok
}

func domainDigest(domain string, payload []byte) string {
	input := append(append([]byte(domain), 0), payload...)
	sum := sha256.Sum256(input)
	return "sha256:" + hex.EncodeToString(sum[:])
}
