package graphql

import "testing"

func TestLiveSemanticProvider_FM5InterpretationIsAtomic(t *testing.T) {
	provider := NewLiveSemanticProvider()
	want := Fm5Interpretation{
		Mode:             Fm5SemanticModeGPIOOnly,
		DegradedReason:   Fm5SemanticDegradedReasonControllerUnreachable,
		EvidenceRevision: "acq-9",
	}
	provider.SetFM5Interpretation(want)

	if got := provider.FM5Interpretation(); got != want {
		t.Fatalf("FM5Interpretation() = %#v; want %#v", got, want)
	}
	if got := provider.FM5SemanticMode(); got != want.Mode {
		t.Fatalf("legacy FM5SemanticMode() = %s; want %s", got, want.Mode)
	}
}

func TestFM5InterpretationValidationRejectsUnexplainedGPIOOnly(t *testing.T) {
	tests := []struct {
		name  string
		value Fm5Interpretation
		valid bool
	}{
		{"interpreted", Fm5Interpretation{Mode: Fm5SemanticModeInterpreted, EvidenceRevision: "acq-1"}, true},
		{"absent", Fm5Interpretation{Mode: Fm5SemanticModeAbsent, EvidenceRevision: "acq-2"}, true},
		{"explained gpio", Fm5Interpretation{Mode: Fm5SemanticModeGPIOOnly, DegradedReason: Fm5SemanticDegradedReasonEvidenceStale, EvidenceRevision: "acq-3"}, true},
		{"unexplained gpio", Fm5Interpretation{Mode: Fm5SemanticModeGPIOOnly, EvidenceRevision: "acq-4"}, false},
		{"reason on interpreted", Fm5Interpretation{Mode: Fm5SemanticModeInterpreted, DegradedReason: Fm5SemanticDegradedReasonSolarAcquisitionFailed, EvidenceRevision: "acq-5"}, false},
		{"missing revision", Fm5Interpretation{Mode: Fm5SemanticModeAbsent}, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.value.Validate(); (err == nil) != test.valid {
				t.Fatalf("Validate() error = %v; valid=%v", err, test.valid)
			}
		})
	}
}

func TestLiveSemanticProvider_LegacyGPIOOnlyCannotBecomeUnexplained(t *testing.T) {
	provider := NewLiveSemanticProvider()
	provider.SetFM5SemanticMode(Fm5SemanticModeGPIOOnly)

	verdict := provider.FM5Interpretation()
	if err := verdict.Validate(); err != nil {
		t.Fatalf("legacy GPIO_ONLY verdict is invalid: %v", err)
	}
	if verdict.DegradedReason != Fm5SemanticDegradedReasonIncoherentAcquisition {
		t.Fatalf("legacy GPIO_ONLY reason = %q; want %q", verdict.DegradedReason, Fm5SemanticDegradedReasonIncoherentAcquisition)
	}
}
