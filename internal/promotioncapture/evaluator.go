package promotioncapture

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"time"
)

var timestampPattern = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?Z$`)

func (registry *Registry) EvaluateWindow(candidateID string, input WindowAssessmentInput) (WindowEvaluation, error) {
	if registry == nil {
		return WindowEvaluation{}, fmt.Errorf("%w: nil registry", ErrInvalidEvidence)
	}
	index, ok := registry.byID[candidateID]
	if !ok {
		return WindowEvaluation{}, fmt.Errorf("%w: %s", ErrUnknownCandidate, candidateID)
	}
	candidate := registry.candidates[index]
	if outcome, fixed := candidate.FixedOutcome(); fixed {
		return WindowEvaluation{CandidateID: candidateID, Outcome: outcome, Fixed: true}, nil
	}
	if err := validateWindow(input.Window); err != nil {
		return WindowEvaluation{}, err
	}
	comparator, err := comparatorFor(candidate)
	if err != nil {
		return WindowEvaluation{}, err
	}
	assessment := Assessment{
		WindowID:                  input.Window.WindowID,
		EBusSample:                cloneSamplePointer(input.EBusSample),
		EEBusSample:               cloneSamplePointer(input.EEBusSample),
		ObservedEBusIdentityHash:  cloneStringPointer(input.ObservedEBusIdentityHash),
		ObservedEEBusIdentityHash: cloneStringPointer(input.ObservedEEBusIdentityHash),
		ConflictSamples:           cloneSamples(input.ConflictSamples),
		MaxSkewNS:                 registry.captureLimits.MaxSkewNS,
		MaxAgeNS:                  registry.captureLimits.MaxAgeNS,
		Comparator:                comparator,
	}

	if input.EBusSample == nil || input.EEBusSample == nil {
		if err := validateMissingEvidence(candidate, input); err != nil {
			return WindowEvaluation{}, err
		}
		assessment.Comparator.Outcome = OutcomeMissing
		return assessedEvaluation(candidateID, assessment), nil
	}
	if input.ObservedEBusIdentityHash == nil || input.ObservedEEBusIdentityHash == nil {
		return WindowEvaluation{}, fmt.Errorf("%w: present sample lacks observed identity", ErrInvalidEvidence)
	}
	if !digestPattern.MatchString(input.ExpectedEBusIdentityHash) || !digestPattern.MatchString(input.ExpectedEEBusIdentityHash) ||
		!digestPattern.MatchString(*input.ObservedEBusIdentityHash) || !digestPattern.MatchString(*input.ObservedEEBusIdentityHash) {
		return WindowEvaluation{}, fmt.Errorf("%w: malformed identity hash", ErrInvalidEvidence)
	}
	if err := validateSampleStructure(input.EBusSample, SourceEBus, input.Window, true); err != nil {
		return WindowEvaluation{}, err
	}
	if err := validateSampleStructure(input.EEBusSample, SourceEEBus, input.Window, true); err != nil {
		return WindowEvaluation{}, err
	}

	ebusObserved, _ := parseTimestamp(input.EBusSample.ObservedAt)
	eebusObserved, _ := parseTimestamp(input.EEBusSample.ObservedAt)
	windowEnd, _ := parseTimestamp(input.Window.EndedAt)
	skew := absoluteDurationNS(ebusObserved.Sub(eebusObserved))
	if skew > registry.captureLimits.MaxSkewNS {
		return WindowEvaluation{}, fmt.Errorf("%w: sample skew %d exceeds %d", ErrInvalidEvidence, skew, registry.captureLimits.MaxSkewNS)
	}
	age := maxInt64(windowEnd.Sub(ebusObserved).Nanoseconds(), windowEnd.Sub(eebusObserved).Nanoseconds())
	assessment.SkewNS = int64PointerValue(skew)
	assessment.AgeNS = int64PointerValue(age)

	outcome := OutcomeMatch
	var numericComparison *NumericComparison
	switch {
	case *input.ObservedEBusIdentityHash != input.ExpectedEBusIdentityHash ||
		*input.ObservedEEBusIdentityHash != input.ExpectedEEBusIdentityHash:
		outcome = OutcomeIdentityMismatch
	case !sampleGenerationMatches(*input.EBusSample, input.Window) || !sampleGenerationMatches(*input.EEBusSample, input.Window):
		outcome = OutcomeGenerationChanged
	default:
		valid, comparison, err := validComparablePair(candidate, *input.EBusSample, *input.EEBusSample)
		if err != nil {
			return WindowEvaluation{}, err
		}
		numericComparison = comparison
		if !input.EBusSample.Valid || !input.EEBusSample.Valid || !valid {
			outcome = OutcomeInvalid
		} else if age > registry.captureLimits.MaxAgeNS {
			outcome = OutcomeStale
		} else {
			conflict, err := registry.hasConflict(candidate, input)
			if err != nil {
				return WindowEvaluation{}, err
			}
			if conflict {
				outcome = OutcomeConflict
			} else if candidate.ComparatorClass == ComparatorNumeric {
				if numericComparison == nil {
					return WindowEvaluation{}, fmt.Errorf("%w: numeric comparison missing", ErrInvalidEvidence)
				}
				if !numericComparison.Match {
					outcome = OutcomeMismatch
				}
			} else if !typedSemanticEqual(input.EBusSample.Value, input.EEBusSample.Value) {
				outcome = OutcomeMismatch
			}
		}
	}

	assessment.Comparator.Outcome = outcome
	if candidate.ComparatorClass == ComparatorNumeric && numericComparison != nil &&
		(outcome == OutcomeMatch || outcome == OutcomeMismatch) {
		delta := numericComparison.Delta
		assessment.Comparator.Delta = &delta
	}
	return assessedEvaluation(candidateID, assessment), nil
}

func assessedEvaluation(candidateID string, assessment Assessment) WindowEvaluation {
	return WindowEvaluation{
		CandidateID: candidateID,
		Outcome:     assessment.Comparator.Outcome,
		Assessment:  &assessment,
	}
}

func comparatorFor(candidate CandidateDefinition) (Comparator, error) {
	comparator := Comparator{Class: candidate.ComparatorClass, Outcome: OutcomeNotEvaluated}
	source := candidate.EEBusSource
	switch candidate.ComparatorClass {
	case ComparatorNumeric:
		if source == nil || source.DeclaredConstraints == nil || source.Conversion == nil {
			return Comparator{}, fmt.Errorf("%w: numeric catalog entry incomplete", ErrInvalidEvidence)
		}
		step := source.DeclaredConstraints.Step
		conversion := *source.Conversion
		comparator.DeclaredSpineStep = &step
		comparator.Conversion = &conversion
	case ComparatorEnum, ComparatorBoolean:
		if source == nil || source.MappingProfile == nil {
			return Comparator{}, fmt.Errorf("%w: mapping catalog entry incomplete", ErrInvalidEvidence)
		}
		hash, err := HashMapping(*source.MappingProfile)
		if err != nil {
			return Comparator{}, err
		}
		comparator.MappingHash = &hash
	default:
		return Comparator{}, fmt.Errorf("%w: ineligible comparator %q", ErrInvalidEvidence, candidate.ComparatorClass)
	}
	return comparator, nil
}

func validateWindow(window Window) error {
	if window.WindowID == "" || window.CaptureGeneration == "" || window.EBusPollGeneration == "" ||
		(window.Phase != PhasePreRestart && window.Phase != PhasePostRestart) ||
		window.AdmittedSource < 1 || window.AdmittedSource > 255 || window.EEBusRuntimeEpoch < 0 ||
		window.ConnectionGeneration < 0 || !window.M8NoDrift || !window.RollbackExact {
		return fmt.Errorf("%w: malformed capture window", ErrInvalidEvidence)
	}
	started, err := parseTimestamp(window.StartedAt)
	if err != nil {
		return err
	}
	ended, err := parseTimestamp(window.EndedAt)
	if err != nil {
		return err
	}
	if !started.Before(ended) {
		return fmt.Errorf("%w: capture window is not increasing", ErrInvalidEvidence)
	}
	return nil
}

func validateMissingEvidence(candidate CandidateDefinition, input WindowAssessmentInput) error {
	if (input.EBusSample == nil) != (input.ObservedEBusIdentityHash == nil) ||
		(input.EEBusSample == nil) != (input.ObservedEEBusIdentityHash == nil) || len(input.ConflictSamples) != 0 {
		return fmt.Errorf("%w: missing sample identity/conflict binding", ErrInvalidEvidence)
	}
	for _, pair := range []struct {
		sample   *Sample
		source   Source
		expected string
		observed *string
	}{
		{input.EBusSample, SourceEBus, input.ExpectedEBusIdentityHash, input.ObservedEBusIdentityHash},
		{input.EEBusSample, SourceEEBus, input.ExpectedEEBusIdentityHash, input.ObservedEEBusIdentityHash},
	} {
		if pair.sample == nil {
			continue
		}
		if pair.observed == nil || *pair.observed != pair.expected || !digestPattern.MatchString(pair.expected) {
			return fmt.Errorf("%w: present side of missing pair has wrong identity", ErrInvalidEvidence)
		}
		if err := validateSampleStructure(pair.sample, pair.source, input.Window, false); err != nil {
			return err
		}
		if !pair.sample.Valid || !sampleGenerationMatches(*pair.sample, input.Window) || !catalogSampleValid(candidate, *pair.sample) {
			return fmt.Errorf("%w: present side of missing pair is not admissible", ErrInvalidEvidence)
		}
	}
	return nil
}

func validateSampleStructure(sample *Sample, source Source, window Window, allowStale bool) error {
	if sample == nil || sample.Source != source {
		return fmt.Errorf("%w: sample source mismatch", ErrInvalidEvidence)
	}
	if err := sample.RawValue.Validate(); err != nil {
		return err
	}
	if err := sample.Value.Validate(); err != nil {
		return err
	}
	expectedRawHash, err := HashRawValue(sample.RawValue)
	if err != nil || sample.RawHash != expectedRawHash {
		return fmt.Errorf("%w: raw value hash mismatch", ErrInvalidEvidence)
	}
	if sample.Value.Kind == ValueNumeric {
		if sample.RawValue.Kind != ValueNumeric || !decimalPointersEqual(sample.RawValue.Decimal, sample.Value.Decimal) {
			return fmt.Errorf("%w: numeric raw/decoded values differ", ErrInvalidEvidence)
		}
	}
	observed, err := parseTimestamp(sample.ObservedAt)
	if err != nil {
		return err
	}
	started, _ := parseTimestamp(window.StartedAt)
	ended, _ := parseTimestamp(window.EndedAt)
	if observed.After(ended) || (!allowStale && observed.Before(started)) {
		return fmt.Errorf("%w: sample timestamp outside window", ErrInvalidEvidence)
	}
	if source == SourceEBus {
		if sample.PollID == nil || *sample.PollID == "" || sample.PollGeneration == nil || *sample.PollGeneration == "" ||
			sample.RuntimeEpoch != nil || sample.ConnectionGeneration != nil {
			return fmt.Errorf("%w: malformed eBUS generation fields", ErrInvalidEvidence)
		}
	} else if sample.PollID != nil || sample.PollGeneration != nil || sample.RuntimeEpoch == nil ||
		sample.ConnectionGeneration == nil || *sample.RuntimeEpoch < 0 || *sample.ConnectionGeneration < 0 {
		return fmt.Errorf("%w: malformed eeBUS generation fields", ErrInvalidEvidence)
	}
	return nil
}

func validComparablePair(candidate CandidateDefinition, ebus, eebus Sample) (bool, *NumericComparison, error) {
	if !catalogSampleValid(candidate, ebus) || !catalogSampleValid(candidate, eebus) {
		return false, nil, nil
	}
	if candidate.ComparatorClass == ComparatorNumeric {
		comparison, err := CompareNumeric(*ebus.Value.Decimal, *eebus.Value.Decimal, *candidate.EEBusSource.DeclaredConstraints, *candidate.EEBusSource.Conversion)
		if err != nil {
			if err == ErrOutOfRange {
				return false, nil, nil
			}
			return false, nil, err
		}
		return true, &comparison, nil
	}
	if typedSemanticEqual(ebus.Value, eebus.Value) && !crossProtocolMappingMatches(candidate, ebus, eebus) {
		return false, nil, nil
	}
	return true, nil, nil
}

func catalogSampleValid(candidate CandidateDefinition, sample Sample) bool {
	source := candidate.EEBusSource
	if source == nil {
		return false
	}
	switch candidate.ComparatorClass {
	case ComparatorNumeric:
		if sample.Value.Kind != ValueNumeric || source.Conversion == nil {
			return false
		}
		expectedUnit := source.Conversion.TargetUnit
		if sample.Source == SourceEBus {
			expectedUnit = source.Conversion.SourceUnit
		}
		return stringPointersEqual(sample.Unit, &expectedUnit)
	case ComparatorEnum:
		if sample.Value.Kind != ValueEnum || sample.Unit != nil {
			return false
		}
	case ComparatorBoolean:
		if sample.Value.Kind != ValueBoolean || sample.Unit != nil {
			return false
		}
	default:
		return false
	}
	if sample.Source == SourceEEBus {
		return protocolMappingMatches(*source.ExactMapping, sample)
	}
	return eBusMappingMatches(*source.MappingProfile, sample)
}

func protocolMappingMatches(mapping ProtocolMapping, sample Sample) bool {
	raw, ok := protocolRawJSON(sample.RawValue)
	if !ok {
		return false
	}
	normalized, ok := normalizedJSON(sample.Value)
	if !ok {
		return false
	}
	for _, pair := range mapping.Pairs {
		if rawJSONEqual(pair.Raw, raw) && rawJSONEqual(pair.Normalized, normalized) {
			return true
		}
	}
	return false
}

func eBusMappingMatches(mapping MappingProfile, sample Sample) bool {
	if sample.RawValue.Kind != ValueNumeric || sample.RawValue.Decimal == nil {
		return false
	}
	normalized, ok := normalizedJSON(sample.Value)
	if !ok {
		return false
	}
	for _, pair := range mapping.Pairs {
		if pair.EBusRaw == *sample.RawValue.Decimal && rawJSONEqual(pair.Normalized, normalized) {
			return true
		}
	}
	return false
}

func crossProtocolMappingMatches(candidate CandidateDefinition, ebus, eebus Sample) bool {
	profile := candidate.EEBusSource.MappingProfile
	if profile == nil || ebus.RawValue.Kind != ValueNumeric || ebus.RawValue.Decimal == nil {
		return false
	}
	eebusRaw, ok := profileRawJSON(eebus.RawValue)
	if !ok {
		return false
	}
	normalized, ok := normalizedJSON(ebus.Value)
	if !ok {
		return false
	}
	for _, pair := range profile.Pairs {
		if pair.EBusRaw == *ebus.RawValue.Decimal && rawJSONEqual(pair.EEBusRaw, eebusRaw) && rawJSONEqual(pair.Normalized, normalized) {
			return true
		}
	}
	return false
}

func (registry *Registry) hasConflict(candidate CandidateDefinition, input WindowAssessmentInput) (bool, error) {
	if len(input.ConflictSamples) == 0 {
		return false, nil
	}
	if len(input.ConflictSamples) != 2 || input.ConflictSamples[0].Source != input.ConflictSamples[1].Source {
		return false, fmt.Errorf("%w: conflict requires two same-source samples", ErrInvalidEvidence)
	}
	source := input.ConflictSamples[0].Source
	if source != SourceEBus && source != SourceEEBus {
		return false, fmt.Errorf("%w: unknown conflict source", ErrInvalidEvidence)
	}
	for index := range input.ConflictSamples {
		sample := &input.ConflictSamples[index]
		if err := validateSampleStructure(sample, source, input.Window, false); err != nil {
			return false, err
		}
		if !sample.Valid || !sampleGenerationMatches(*sample, input.Window) || !catalogSampleValid(candidate, *sample) {
			return false, fmt.Errorf("%w: inadmissible conflict sample", ErrInvalidEvidence)
		}
		observed, _ := parseTimestamp(sample.ObservedAt)
		ended, _ := parseTimestamp(input.Window.EndedAt)
		if ended.Sub(observed).Nanoseconds() > registry.captureLimits.MaxAgeNS {
			return false, fmt.Errorf("%w: stale conflict sample", ErrInvalidEvidence)
		}
		if candidate.ComparatorClass == ComparatorNumeric {
			value := *sample.Value.Decimal
			if source == SourceEBus {
				converted, err := convertExact(value, *candidate.EEBusSource.Conversion)
				if err != nil {
					return false, err
				}
				value, err = decimalFromExact(converted)
				if err != nil {
					return false, err
				}
			}
			constraints := candidate.EEBusSource.DeclaredConstraints
			comparison, err := CompareNumeric(value, value, *constraints, Conversion{
				Mode: ConversionIdentity, SourceUnit: "normalized", TargetUnit: "normalized",
				Scale: Decimal{Number: 1}, Offset: Decimal{},
			})
			if err != nil || !comparison.Match {
				return false, fmt.Errorf("%w: out-of-range conflict sample", ErrInvalidEvidence)
			}
		}
	}
	firstObserved, _ := parseTimestamp(input.ConflictSamples[0].ObservedAt)
	secondObserved, _ := parseTimestamp(input.ConflictSamples[1].ObservedAt)
	if absoluteDurationNS(firstObserved.Sub(secondObserved)) > registry.captureLimits.MaxSkewNS {
		return false, fmt.Errorf("%w: conflict sample skew exceeded", ErrInvalidEvidence)
	}
	equal, err := conflictValuesEqual(candidate, input.ConflictSamples[0], input.ConflictSamples[1])
	if err != nil {
		return false, err
	}
	if equal {
		return false, fmt.Errorf("%w: conflict samples have equal semantic values", ErrInvalidEvidence)
	}
	return true, nil
}

func conflictValuesEqual(candidate CandidateDefinition, first, second Sample) (bool, error) {
	if candidate.ComparatorClass != ComparatorNumeric {
		return typedSemanticEqual(first.Value, second.Value), nil
	}
	left, err := exactFromDecimal(*first.Value.Decimal)
	if err != nil {
		return false, err
	}
	right, err := exactFromDecimal(*second.Value.Decimal)
	if err != nil {
		return false, err
	}
	if first.Source == SourceEBus {
		left, err = convertExact(*first.Value.Decimal, *candidate.EEBusSource.Conversion)
		if err != nil {
			return false, err
		}
		right, err = convertExact(*second.Value.Decimal, *candidate.EEBusSource.Conversion)
		if err != nil {
			return false, err
		}
	}
	return compareExact(left, right) == 0, nil
}

func sampleGenerationMatches(sample Sample, window Window) bool {
	if sample.CaptureGeneration != window.CaptureGeneration {
		return false
	}
	if sample.Source == SourceEBus {
		return sample.PollGeneration != nil && *sample.PollGeneration == window.EBusPollGeneration
	}
	return sample.RuntimeEpoch != nil && sample.ConnectionGeneration != nil &&
		*sample.RuntimeEpoch == window.EEBusRuntimeEpoch && *sample.ConnectionGeneration == window.ConnectionGeneration
}

func typedSemanticEqual(left, right TypedValue) bool {
	if left.Kind != right.Kind {
		return false
	}
	switch left.Kind {
	case ValueNumeric:
		return decimalPointersEqual(left.Decimal, right.Decimal)
	case ValueEnum:
		return left.Enum != nil && right.Enum != nil && *left.Enum == *right.Enum
	case ValueBoolean:
		return left.Boolean != nil && right.Boolean != nil && *left.Boolean == *right.Boolean
	default:
		return false
	}
}

func decimalPointersEqual(left, right *Decimal) bool {
	if left == nil || right == nil {
		return left == right
	}
	comparison, err := left.Compare(*right)
	return err == nil && comparison == 0
}

func protocolRawJSON(value TypedValue) ([]byte, bool) {
	switch value.Kind {
	case ValueNumeric:
		if value.Decimal == nil || value.Decimal.Scale != 0 {
			return nil, false
		}
		raw, err := json.Marshal(value.Decimal.Number)
		return raw, err == nil
	case ValueBoolean:
		if value.Boolean == nil {
			return nil, false
		}
		raw, err := json.Marshal(*value.Boolean)
		return raw, err == nil
	default:
		return nil, false
	}
}

func profileRawJSON(value TypedValue) ([]byte, bool) {
	switch value.Kind {
	case ValueNumeric:
		if value.Decimal == nil {
			return nil, false
		}
		raw, err := json.Marshal(value.Decimal)
		return raw, err == nil
	case ValueBoolean:
		if value.Boolean == nil {
			return nil, false
		}
		raw, err := json.Marshal(*value.Boolean)
		return raw, err == nil
	default:
		return nil, false
	}
}

func normalizedJSON(value TypedValue) ([]byte, bool) {
	switch value.Kind {
	case ValueEnum:
		if value.Enum == nil {
			return nil, false
		}
		raw, err := json.Marshal(*value.Enum)
		return raw, err == nil
	case ValueBoolean:
		if value.Boolean == nil {
			return nil, false
		}
		raw, err := json.Marshal(*value.Boolean)
		return raw, err == nil
	default:
		return nil, false
	}
}

func rawJSONEqual(left, right []byte) bool {
	leftCanonical, leftErr := CanonicalJSON(json.RawMessage(left))
	rightCanonical, rightErr := CanonicalJSON(json.RawMessage(right))
	return leftErr == nil && rightErr == nil && bytes.Equal(leftCanonical, rightCanonical)
}

func parseTimestamp(value string) (time.Time, error) {
	if !timestampPattern.MatchString(value) {
		return time.Time{}, fmt.Errorf("%w: invalid timestamp %q", ErrInvalidEvidence, value)
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid timestamp %q", ErrInvalidEvidence, value)
	}
	return parsed, nil
}

func absoluteDurationNS(duration time.Duration) int64 {
	value := duration.Nanoseconds()
	if value < 0 {
		return -value
	}
	return value
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}

func stringPointersEqual(left, right *string) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func cloneStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func int64PointerValue(value int64) *int64 {
	return &value
}

func cloneSamplePointer(sample *Sample) *Sample {
	if sample == nil {
		return nil
	}
	clone := cloneSample(*sample)
	return &clone
}

func cloneSamples(samples []Sample) []Sample {
	result := make([]Sample, len(samples))
	for index, sample := range samples {
		result[index] = cloneSample(sample)
	}
	return result
}

func cloneSample(sample Sample) Sample {
	clone := sample
	clone.PollID = cloneStringPointer(sample.PollID)
	clone.PollGeneration = cloneStringPointer(sample.PollGeneration)
	if sample.RuntimeEpoch != nil {
		value := *sample.RuntimeEpoch
		clone.RuntimeEpoch = &value
	}
	if sample.ConnectionGeneration != nil {
		value := *sample.ConnectionGeneration
		clone.ConnectionGeneration = &value
	}
	clone.Unit = cloneStringPointer(sample.Unit)
	clone.RawValue = cloneTypedValue(sample.RawValue)
	clone.Value = cloneTypedValue(sample.Value)
	return clone
}

func cloneTypedValue(value TypedValue) TypedValue {
	clone := value
	if value.Decimal != nil {
		decimal := *value.Decimal
		clone.Decimal = &decimal
	}
	clone.Enum = cloneStringPointer(value.Enum)
	if value.Boolean != nil {
		boolean := *value.Boolean
		clone.Boolean = &boolean
	}
	return clone
}
