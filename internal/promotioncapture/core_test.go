package promotioncapture

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

const (
	testEBusIdentityHash  = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	testEEBusIdentityHash = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
)

func TestEmbeddedRegistryIsPinnedAndClosed(t *testing.T) {
	raw := EmbeddedRegistryBytes()
	digest := fmt.Sprintf("sha256:%x", sha256.Sum256(raw))
	if digest != RegistrySHA256 {
		t.Fatalf("embedded registry digest = %s, want %s", digest, RegistrySHA256)
	}
	if RegistrySHA256 != "sha256:00ceefc05439e9aec5830b640661cdc6be2b503f9365eed437e3dbffdf6d0678" {
		t.Fatalf("unexpected pinned digest %q", RegistrySHA256)
	}

	registry := mustRegistry(t)
	candidates := registry.Candidates()
	if len(candidates) != 22 {
		t.Fatalf("catalog length = %d, want 22", len(candidates))
	}

	wantEligibility := []ProtocolEligibility{
		ProtocolTerminal, ProtocolTerminal, ProtocolTerminal, ProtocolTerminal,
		ProtocolCrossProtocol, ProtocolCrossProtocol, ProtocolCrossProtocol, ProtocolEEBusNative,
		ProtocolCrossProtocol, ProtocolCrossProtocol, ProtocolCrossProtocol, ProtocolCrossProtocol,
		ProtocolEEBusNative, ProtocolCrossProtocol, ProtocolCrossProtocol, ProtocolCrossProtocol,
		ProtocolEEBusNative, ProtocolCrossProtocol, ProtocolEEBusNative, ProtocolEEBusNative,
		ProtocolEEBusNative, ProtocolEEBusNative,
	}
	wantClasses := []ComparatorClass{
		ComparatorNone, ComparatorNone, ComparatorNone, ComparatorNone,
		ComparatorNumeric, ComparatorNumeric, ComparatorEnum, ComparatorBoolean,
		ComparatorBoolean, ComparatorNumeric, ComparatorNumeric, ComparatorEnum,
		ComparatorBoolean, ComparatorNumeric, ComparatorNumeric, ComparatorEnum,
		ComparatorBoolean, ComparatorNumeric, ComparatorString, ComparatorString,
		ComparatorString, ComparatorString,
	}
	wantFixed := []Outcome{
		OutcomeCloudOnly, OutcomeNotTested, OutcomeNotTested, OutcomeNotTested,
		"", "", "", "", "", "", "", "",
		"", "", "", "", "", "", "", "",
		"", "",
	}

	for i, candidate := range candidates {
		wantID := fmt.Sprintf("m7-candidate-%04d", i+1)
		if candidate.CandidateID != wantID {
			t.Errorf("catalog[%d].candidate_id = %q, want %q", i, candidate.CandidateID, wantID)
		}
		if candidate.ProtocolEligibility != wantEligibility[i] {
			t.Errorf("%s eligibility = %q, want %q", wantID, candidate.ProtocolEligibility, wantEligibility[i])
		}
		if candidate.ComparatorClass != wantClasses[i] {
			t.Errorf("%s comparator = %q, want %q", wantID, candidate.ComparatorClass, wantClasses[i])
		}
		outcome, fixed := candidate.FixedOutcome()
		if fixed != (wantFixed[i] != "") || outcome != wantFixed[i] {
			t.Errorf("%s fixed outcome = (%q, %t), want (%q, %t)", wantID, outcome, fixed, wantFixed[i], wantFixed[i] != "")
		}
	}

	// Callers cannot mutate the embedded bytes or registry's catalog order.
	raw[0] = 'x'
	if EmbeddedRegistryBytes()[0] != '{' {
		t.Fatal("EmbeddedRegistryBytes returned mutable package storage")
	}
	candidates[0].CandidateID = "changed"
	first, ok := registry.Candidate("m7-candidate-0001")
	if !ok || first.CandidateID != "m7-candidate-0001" {
		t.Fatal("Candidates returned mutable registry storage")
	}
	if _, ok := registry.Candidate("m7-candidate-0023"); ok {
		t.Fatal("unregistered candidate was accepted")
	}
}

func TestAllCatalogRowsProduceTheirContractOutcome(t *testing.T) {
	registry := mustRegistry(t)
	for _, candidate := range registry.Candidates() {
		candidate := candidate
		t.Run(candidate.CandidateID, func(t *testing.T) {
			if fixed, ok := candidate.FixedOutcome(); ok {
				result, err := registry.EvaluateWindow(candidate.CandidateID, WindowAssessmentInput{})
				if err != nil {
					t.Fatalf("EvaluateWindow fixed row: %v", err)
				}
				if !result.Fixed || result.Outcome != fixed || result.Assessment != nil {
					t.Fatalf("fixed result = %#v, want %q without assessment", result, fixed)
				}
				return
			}

			input := validInput(t, candidate, Decimal{Number: 20, Scale: 0}, Decimal{Number: 20, Scale: 0})
			result, err := registry.EvaluateWindow(candidate.CandidateID, input)
			if err != nil {
				t.Fatalf("EvaluateWindow: %v", err)
			}
			want := OutcomeMatch
			if candidate.ProtocolEligibility == ProtocolEEBusNative {
				want = OutcomeNativeValid
			}
			if result.Fixed || result.Outcome != want || result.Assessment == nil {
				t.Fatalf("real-leaf result = %#v, want %s assessment", result, want)
			}
		})
	}
}

func TestDecimalArithmeticIsExact(t *testing.T) {
	equivalent := []Decimal{
		{Number: 1, Scale: 0},
		{Number: 10, Scale: -1},
		{Number: 1000000, Scale: -6},
	}
	for _, value := range equivalent[1:] {
		comparison, err := equivalent[0].Compare(value)
		if err != nil || comparison != 0 {
			t.Fatalf("1 and %v compare = %d, %v", value, comparison, err)
		}
	}
	comparison, err := (Decimal{Number: -1, Scale: -1}).Compare(Decimal{Number: 0, Scale: -12})
	if err != nil || comparison >= 0 {
		t.Fatalf("negative comparison = %d, %v", comparison, err)
	}
	if _, err := (Decimal{Number: 1, Scale: 13}).Compare(Decimal{}); !errors.Is(err, ErrInvalidDecimal) {
		t.Fatalf("invalid scale error = %v, want ErrInvalidDecimal", err)
	}
}

func TestCompareNumericEnforcesRangeStepAndInclusiveThreshold(t *testing.T) {
	constraints := DeclaredConstraints{
		Minimum: Decimal{Number: 0, Scale: 0},
		Maximum: Decimal{Number: 60, Scale: 0},
		Step:    Decimal{Number: 5, Scale: -1},
	}
	conversion := identityConversion("degC")

	for name, test := range map[string]struct {
		ebus, eebus Decimal
		match       bool
	}{
		"minimum equality":  {Decimal{Number: 0, Scale: -6}, Decimal{Number: 0, Scale: 0}, true},
		"maximum equality":  {Decimal{Number: 6, Scale: 1}, Decimal{Number: 60, Scale: 0}, true},
		"below threshold":   {Decimal{Number: 20, Scale: 0}, Decimal{Number: 204, Scale: -1}, true},
		"boundary equality": {Decimal{Number: 20, Scale: 0}, Decimal{Number: 205, Scale: -1}, true},
		"above threshold":   {Decimal{Number: 20, Scale: 0}, Decimal{Number: 205000001, Scale: -7}, false},
	} {
		t.Run(name, func(t *testing.T) {
			comparison, err := CompareNumeric(test.ebus, test.eebus, constraints, conversion)
			if err != nil {
				t.Fatalf("CompareNumeric: %v", err)
			}
			if comparison.Match != test.match {
				t.Fatalf("match = %t, want %t (delta %v)", comparison.Match, test.match, comparison.Delta)
			}
		})
	}

	for _, invalidStep := range []Decimal{{Number: 0, Scale: 0}, {Number: -1, Scale: 0}, {Number: 1, Scale: 13}} {
		invalid := constraints
		invalid.Step = invalidStep
		if _, err := CompareNumeric(Decimal{}, Decimal{}, invalid, conversion); !errors.Is(err, ErrInvalidStep) {
			t.Errorf("step %v error = %v, want ErrInvalidStep", invalidStep, err)
		}
	}
	for _, values := range [][2]Decimal{
		{{Number: -1, Scale: -1}, {Number: 0, Scale: 0}},
		{{Number: 0, Scale: 0}, {Number: 601, Scale: -1}},
	} {
		if _, err := CompareNumeric(values[0], values[1], constraints, conversion); !errors.Is(err, ErrOutOfRange) {
			t.Errorf("range %v error = %v, want ErrOutOfRange", values, err)
		}
	}
}

func TestNumericWindowBoundariesAndInvalidSentinel(t *testing.T) {
	registry := mustRegistry(t)
	candidate := mustCandidate(t, registry, "m7-candidate-0010")

	for name, test := range map[string]struct {
		ebus, eebus Decimal
		want        Outcome
	}{
		"boundary equality": {Decimal{Number: 20, Scale: 0}, Decimal{Number: 205, Scale: -1}, OutcomeMatch},
		"below threshold":   {Decimal{Number: 20, Scale: 0}, Decimal{Number: 204, Scale: -1}, OutcomeMatch},
		"above threshold":   {Decimal{Number: 20, Scale: 0}, Decimal{Number: 205000001, Scale: -7}, OutcomeMismatch},
		"below range":       {Decimal{Number: -1, Scale: -1}, Decimal{Number: 0, Scale: 0}, OutcomeInvalid},
		"above range":       {Decimal{Number: 60, Scale: 0}, Decimal{Number: 601, Scale: -1}, OutcomeInvalid},
	} {
		t.Run(name, func(t *testing.T) {
			input := numericInput(t, candidate, test.ebus, test.eebus)
			result := mustEvaluate(t, registry, candidate.CandidateID, input)
			if result.Outcome != test.want {
				t.Fatalf("outcome = %q, want %q", result.Outcome, test.want)
			}
		})
	}

	input := validInput(t, candidate, Decimal{Number: 32767, Scale: 0}, Decimal{Number: 20, Scale: 0})
	input.EBusSample.Valid = false
	result := mustEvaluate(t, registry, candidate.CandidateID, input)
	if result.Outcome != OutcomeInvalid {
		t.Fatalf("invalid sentinel outcome = %q, want INVALID", result.Outcome)
	}
}

func TestOutcomePrecedence(t *testing.T) {
	registry := mustRegistry(t)
	candidate := mustCandidate(t, registry, "m7-candidate-0010")

	t.Run("missing", func(t *testing.T) {
		input := validInput(t, candidate, Decimal{Number: 20, Scale: 0}, Decimal{Number: 21, Scale: 0})
		input.EEBusSample = nil
		input.ObservedEEBusIdentityHash = nil
		if got := mustEvaluate(t, registry, candidate.CandidateID, input).Outcome; got != OutcomeMissing {
			t.Fatalf("outcome = %q, want MISSING", got)
		}
	})

	t.Run("identity before generation and invalid", func(t *testing.T) {
		input := validInput(t, candidate, Decimal{Number: 20, Scale: 0}, Decimal{Number: 21, Scale: 0})
		input.ObservedEBusIdentityHash = stringPointer("sha256:" + strings.Repeat("f", 64))
		input.EBusSample.CaptureGeneration = "changed"
		input.EBusSample.Valid = false
		if got := mustEvaluate(t, registry, candidate.CandidateID, input).Outcome; got != OutcomeIdentityMismatch {
			t.Fatalf("outcome = %q, want IDENTITY_MISMATCH", got)
		}
	})

	t.Run("generation before invalid", func(t *testing.T) {
		input := validInput(t, candidate, Decimal{Number: 20, Scale: 0}, Decimal{Number: 21, Scale: 0})
		input.EBusSample.CaptureGeneration = "changed"
		input.EBusSample.Valid = false
		if got := mustEvaluate(t, registry, candidate.CandidateID, input).Outcome; got != OutcomeGenerationChanged {
			t.Fatalf("outcome = %q, want GENERATION_CHANGED", got)
		}
	})

	t.Run("invalid before stale", func(t *testing.T) {
		input := validInput(t, candidate, Decimal{Number: 20, Scale: 0}, Decimal{Number: 21, Scale: 0})
		input.EBusSample.Valid = false
		input.EBusSample.ObservedAt = "2026-08-11T10:00:10Z"
		input.EEBusSample.ObservedAt = "2026-08-11T10:00:10Z"
		if got := mustEvaluate(t, registry, candidate.CandidateID, input).Outcome; got != OutcomeInvalid {
			t.Fatalf("outcome = %q, want INVALID", got)
		}
	})

	t.Run("stale before conflict", func(t *testing.T) {
		input := validInput(t, candidate, Decimal{Number: 20, Scale: 0}, Decimal{Number: 21, Scale: 0})
		input.EBusSample.ObservedAt = "2026-08-11T10:00:10Z"
		input.EEBusSample.ObservedAt = "2026-08-11T10:00:10Z"
		input.ConflictSamples = conflictSamples(t, input.Window, candidate, "2026-08-11T10:00:28Z", "2026-08-11T10:00:29Z")
		if got := mustEvaluate(t, registry, candidate.CandidateID, input).Outcome; got != OutcomeStale {
			t.Fatalf("outcome = %q, want STALE", got)
		}
	})

	t.Run("conflict before mismatch", func(t *testing.T) {
		input := validInput(t, candidate, Decimal{Number: 20, Scale: 0}, Decimal{Number: 21, Scale: 0})
		input.ConflictSamples = conflictSamples(t, input.Window, candidate, "2026-08-11T10:00:28Z", "2026-08-11T10:00:29Z")
		if got := mustEvaluate(t, registry, candidate.CandidateID, input).Outcome; got != OutcomeConflict {
			t.Fatalf("outcome = %q, want CONFLICT", got)
		}
	})
}

func TestEnumAndBooleanRequireExactRawPairs(t *testing.T) {
	registry := mustRegistry(t)

	t.Run("enum off and auto", func(t *testing.T) {
		candidate := mustCandidate(t, registry, "m7-candidate-0007")
		for name, values := range map[string]struct {
			ebusRaw, eebusRaw Decimal
			normalized        string
		}{
			"off":  {Decimal{Number: 0, Scale: 0}, Decimal{Number: 2, Scale: 0}, "off"},
			"auto": {Decimal{Number: 1, Scale: 0}, Decimal{Number: 0, Scale: 0}, "auto"},
		} {
			t.Run(name, func(t *testing.T) {
				input := mappedInput(t, candidate, NumericValue(values.ebusRaw), EnumValue(values.normalized), NumericValue(values.eebusRaw), EnumValue(values.normalized))
				if got := mustEvaluate(t, registry, candidate.CandidateID, input).Outcome; got != OutcomeMatch {
					t.Fatalf("outcome = %q, want MATCH", got)
				}
			})
		}
	})

	t.Run("enum mismatch is comparable", func(t *testing.T) {
		candidate := mustCandidate(t, registry, "m7-candidate-0012")
		input := mappedInput(t, candidate,
			NumericValue(Decimal{Number: 0, Scale: 0}), EnumValue("off"),
			NumericValue(Decimal{Number: 0, Scale: 0}), EnumValue("auto"),
		)
		if got := mustEvaluate(t, registry, candidate.CandidateID, input).Outcome; got != OutcomeMismatch {
			t.Fatalf("outcome = %q, want MISMATCH", got)
		}
	})

	t.Run("enum substituted raw is invalid", func(t *testing.T) {
		candidate := mustCandidate(t, registry, "m7-candidate-0016")
		input := mappedInput(t, candidate,
			NumericValue(Decimal{Number: 2, Scale: 0}), EnumValue("off"),
			NumericValue(Decimal{Number: 2, Scale: 0}), EnumValue("off"),
		)
		if got := mustEvaluate(t, registry, candidate.CandidateID, input).Outcome; got != OutcomeInvalid {
			t.Fatalf("outcome = %q, want INVALID", got)
		}
	})

	t.Run("boolean false and true", func(t *testing.T) {
		candidate := mustCandidate(t, registry, "m7-candidate-0009")
		for name, values := range map[string]struct {
			ebusRaw int64
			value   bool
		}{"false": {0, false}, "true": {6, true}} {
			t.Run(name, func(t *testing.T) {
				input := mappedInput(t, candidate,
					NumericValue(Decimal{Number: values.ebusRaw, Scale: 0}), BooleanValue(values.value),
					BooleanValue(values.value), BooleanValue(values.value),
				)
				if got := mustEvaluate(t, registry, candidate.CandidateID, input).Outcome; got != OutcomeMatch {
					t.Fatalf("outcome = %q, want MATCH", got)
				}
			})
		}
	})

	t.Run("boolean mismatch is comparable", func(t *testing.T) {
		candidate := mustCandidate(t, registry, "m7-candidate-0009")
		input := mappedInput(t, candidate,
			NumericValue(Decimal{Number: 0, Scale: 0}), BooleanValue(false),
			BooleanValue(true), BooleanValue(true),
		)
		if got := mustEvaluate(t, registry, candidate.CandidateID, input).Outcome; got != OutcomeMismatch {
			t.Fatalf("outcome = %q, want MISMATCH", got)
		}
	})
}

func TestCanonicalHashesAreDeterministic(t *testing.T) {
	value := NumericValue(Decimal{Number: 20, Scale: 0})
	digest, err := HashRawValue(value)
	if err != nil {
		t.Fatalf("HashRawValue: %v", err)
	}
	if digest != "sha256:0621fc5f21a643eb033664301d46635d2b23f4961795dd36d8ed46959dbf8c8f" {
		t.Fatalf("raw hash = %q", digest)
	}
	registry := mustRegistry(t)
	enumCandidate := mustCandidate(t, registry, "m7-candidate-0007")
	mappingHash, err := HashMapping(*enumCandidate.MappingProfile)
	if err != nil {
		t.Fatalf("HashMapping: %v", err)
	}
	if mappingHash != "sha256:e8270252df3a1e58afd0c5c8223f145152bc3fed08bd46f06b81bc4985036afb" {
		t.Fatalf("mapping hash = %q", mappingHash)
	}

	left := map[string]any{"z": int64(2), "a": map[string]any{"y": true, "x": "value"}}
	right := map[string]any{"a": map[string]any{"x": "value", "y": true}, "z": int64(2)}
	leftHash, err := CanonicalDigest("TEST-DOMAIN\x00", left)
	if err != nil {
		t.Fatalf("CanonicalDigest(left): %v", err)
	}
	rightHash, err := CanonicalDigest("TEST-DOMAIN\x00", right)
	if err != nil {
		t.Fatalf("CanonicalDigest(right): %v", err)
	}
	if leftHash != rightHash {
		t.Fatalf("canonical hashes differ: %s != %s", leftHash, rightHash)
	}

	window := testWindow()
	first, err := HashWindow(window)
	if err != nil {
		t.Fatalf("HashWindow(first): %v", err)
	}
	second, err := HashWindow(window)
	if err != nil || first != second {
		t.Fatalf("window hashes = %q, %q, %v", first, second, err)
	}
}

func mustRegistry(t *testing.T) *Registry {
	t.Helper()
	registry, err := DefaultRegistry()
	if err != nil {
		t.Fatalf("DefaultRegistry: %v", err)
	}
	return registry
}

func mustCandidate(t *testing.T, registry *Registry, id string) CandidateDefinition {
	t.Helper()
	candidate, ok := registry.Candidate(id)
	if !ok {
		t.Fatalf("candidate %s not found", id)
	}
	return candidate
}

func mustEvaluate(t *testing.T, registry *Registry, id string, input WindowAssessmentInput) WindowEvaluation {
	t.Helper()
	result, err := registry.EvaluateWindow(id, input)
	if err != nil {
		t.Fatalf("EvaluateWindow(%s): %v", id, err)
	}
	return result
}

func testWindow() Window {
	return Window{
		WindowID:             "window-pre",
		Phase:                PhasePreRestart,
		StartedAt:            "2026-08-11T10:00:00Z",
		EndedAt:              "2026-08-11T10:00:30Z",
		CaptureGeneration:    "capture-1",
		ProcessInstanceHash:  "sha256:" + strings.Repeat("3", 64),
		LocalIdentityHash:    "sha256:" + strings.Repeat("4", 64),
		TrustStateHash:       "sha256:" + strings.Repeat("5", 64),
		PeerBindingHash:      "sha256:" + strings.Repeat("6", 64),
		AdmittedSource:       0xf7,
		EEBusRuntimeEpoch:    7,
		ConnectionGeneration: 11,
		EBusPollGeneration:   "poll-generation-1",
		M8NoDrift:            true,
		RollbackExact:        true,
	}
}

func validInput(t *testing.T, candidate CandidateDefinition, ebusValue, eebusValue Decimal) WindowAssessmentInput {
	t.Helper()
	if candidate.ProtocolEligibility == ProtocolEEBusNative {
		value := BooleanValue(false)
		if candidate.ComparatorClass == ComparatorString {
			value = StringValue("stable-native-value")
		}
		return nativeInput(t, candidate, testWindow(), value, nil)
	}
	if candidate.ComparatorClass == ComparatorEnum {
		return mappedInput(t, candidate,
			NumericValue(Decimal{Number: 0, Scale: 0}), EnumValue("off"),
			NumericValue(Decimal{Number: 2, Scale: 0}), EnumValue("off"),
		)
	}
	if candidate.ComparatorClass == ComparatorBoolean {
		return mappedInput(t, candidate,
			NumericValue(Decimal{Number: 0, Scale: 0}), BooleanValue(false),
			BooleanValue(false), BooleanValue(false),
		)
	}

	constraints := candidate.EEBusSource.DeclaredConstraints
	if constraints == nil {
		t.Fatalf("numeric candidate %s has no constraints", candidate.CandidateID)
	}
	if outside, _ := ebusValue.Compare(constraints.Minimum); outside < 0 {
		ebusValue = constraints.Minimum
	}
	if outside, _ := ebusValue.Compare(constraints.Maximum); outside > 0 {
		ebusValue = constraints.Minimum
	}
	if outside, _ := eebusValue.Compare(constraints.Minimum); outside < 0 {
		eebusValue = constraints.Minimum
	}
	if outside, _ := eebusValue.Compare(constraints.Maximum); outside > 0 {
		eebusValue = constraints.Minimum
	}
	return numericInput(t, candidate, ebusValue, eebusValue)
}

func numericInput(t *testing.T, candidate CandidateDefinition, ebusValue, eebusValue Decimal) WindowAssessmentInput {
	t.Helper()
	unit := candidate.EEBusSource.Unit
	return mappedInput(t, candidate, NumericValue(ebusValue), NumericValue(ebusValue), NumericValue(eebusValue), NumericValue(eebusValue), unit)
}

func mappedInput(t *testing.T, candidate CandidateDefinition, ebusRaw, ebusValue, eebusRaw, eebusValue TypedValue, optionalUnit ...*string) WindowAssessmentInput {
	t.Helper()
	window := testWindow()
	var ebusUnit, eebusUnit *string
	if len(optionalUnit) == 1 {
		ebusUnit = optionalUnit[0]
		eebusUnit = optionalUnit[0]
	}
	ebus := sample(t, SourceEBus, window, "2026-08-11T10:00:29Z", ebusRaw, ebusValue, ebusUnit)
	eebus := sample(t, SourceEEBus, window, "2026-08-11T10:00:29.5Z", eebusRaw, eebusValue, eebusUnit)
	return WindowAssessmentInput{
		Window:                    window,
		ExpectedEBusIdentityHash:  testEBusIdentityHash,
		ExpectedEEBusIdentityHash: testEEBusIdentityHash,
		ObservedEBusIdentityHash:  stringPointer(testEBusIdentityHash),
		ObservedEEBusIdentityHash: stringPointer(testEEBusIdentityHash),
		EBusSample:                &ebus,
		EEBusSample:               &eebus,
		ConflictSamples:           nil,
	}
}

func sample(t *testing.T, source Source, window Window, observedAt string, raw, value TypedValue, unit *string) Sample {
	t.Helper()
	sample := Sample{
		Source:            source,
		ObservedAt:        observedAt,
		Valid:             true,
		CaptureGeneration: window.CaptureGeneration,
		RawValue:          raw,
		Value:             value,
		Unit:              unit,
	}
	if source == SourceEBus {
		sample.PollID = stringPointer("poll-1")
		sample.PollGeneration = stringPointer(window.EBusPollGeneration)
	} else {
		sample.RuntimeEpoch = int64Pointer(window.EEBusRuntimeEpoch)
		sample.ConnectionGeneration = int64Pointer(window.ConnectionGeneration)
	}
	if err := sample.BindRawHash(); err != nil {
		t.Fatalf("BindRawHash: %v", err)
	}
	return sample
}

func conflictSamples(t *testing.T, window Window, candidate CandidateDefinition, firstAt, secondAt string) []Sample {
	t.Helper()
	unit := candidate.EEBusSource.Unit
	first := sample(t, SourceEBus, window, firstAt,
		NumericValue(Decimal{Number: 20, Scale: 0}), NumericValue(Decimal{Number: 20, Scale: 0}), unit)
	second := sample(t, SourceEBus, window, secondAt,
		NumericValue(Decimal{Number: 21, Scale: 0}), NumericValue(Decimal{Number: 21, Scale: 0}), unit)
	return []Sample{first, second}
}

func identityConversion(unit string) Conversion {
	return Conversion{
		Mode:       ConversionIdentity,
		SourceUnit: unit,
		TargetUnit: unit,
		Scale:      Decimal{Number: 1, Scale: 0},
		Offset:     Decimal{Number: 0, Scale: 0},
	}
}

func stringPointer(value string) *string { return &value }
func int64Pointer(value int64) *int64    { return &value }

func TestTypedValueConstructorsAreExclusive(t *testing.T) {
	tests := []struct {
		value TypedValue
		kind  ValueKind
	}{
		{NumericValue(Decimal{Number: 1, Scale: 0}), ValueNumeric},
		{EnumValue("auto"), ValueEnum},
		{BooleanValue(true), ValueBoolean},
		{StringValue("label"), ValueString},
	}
	for _, test := range tests {
		if test.value.Kind != test.kind {
			t.Errorf("kind = %q, want %q", test.value.Kind, test.kind)
		}
		if err := test.value.Validate(); err != nil {
			t.Errorf("Validate(%#v): %v", test.value, err)
		}
	}
	invalid := NumericValue(Decimal{Number: 1, Scale: 0})
	invalid.Enum = stringPointer("also-populated")
	if err := invalid.Validate(); err == nil {
		t.Fatal("multi-valued TypedValue was accepted")
	}
}

func TestCandidateCopiesDoNotAliasNestedRegistryData(t *testing.T) {
	registry := mustRegistry(t)
	first := mustCandidate(t, registry, "m7-candidate-0007")
	second := mustCandidate(t, registry, "m7-candidate-0007")
	if first.EEBusSource == nil || second.EEBusSource == nil || first.MappingProfile == nil {
		t.Fatal("enum candidate mapping profile missing")
	}
	first.MappingProfile.Pairs[0].Normalized = []byte(`"changed"`)
	if reflect.DeepEqual(first.MappingProfile.Pairs, second.MappingProfile.Pairs) {
		t.Fatal("Candidate returned nested mutable registry storage")
	}
}
