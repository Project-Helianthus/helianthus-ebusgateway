package ebusgateway

import "testing"

func TestDefaultObserveFirstFeatureFlagsAreConservative(t *testing.T) {
	flags := DefaultObserveFirstFeatureFlags()

	if flags.ObserveFirstEnabled() {
		t.Fatal("ObserveFirstEnabled() = true; want false")
	}
	if flags.PassiveStateDirectApply() {
		t.Fatal("PassiveStateDirectApply() = true; want false")
	}
	if flags.PassiveConfigDirectApply() {
		t.Fatal("PassiveConfigDirectApply() = true; want false")
	}
	if flags.ExternalWritePolicy() != ObserveFirstExternalWritePolicyRecordOnly {
		t.Fatalf("ExternalWritePolicy() = %q; want %q", flags.ExternalWritePolicy(), ObserveFirstExternalWritePolicyRecordOnly)
	}
}

func TestNormalizeObserveFirstFeatureFlags_MasterOffClampsUnsafeSubFlags(t *testing.T) {
	flags := NormalizeObserveFirstFeatureFlags(true, true, true, ObserveFirstExternalWritePolicyRecordAndInvalidate)
	if !flags.ObserveFirstEnabled() {
		t.Fatal("precondition failed: normalized flags unexpectedly disabled")
	}

	flags = NormalizeObserveFirstFeatureFlags(false, true, true, ObserveFirstExternalWritePolicyInvalidateOnly)

	if flags.ObserveFirstEnabled() {
		t.Fatal("ObserveFirstEnabled() = true; want false")
	}
	if flags.PassiveStateDirectApply() {
		t.Fatal("PassiveStateDirectApply() = true; want false")
	}
	if flags.PassiveConfigDirectApply() {
		t.Fatal("PassiveConfigDirectApply() = true; want false")
	}
	if flags.ExternalWritePolicy() != ObserveFirstExternalWritePolicyRecordOnly {
		t.Fatalf("ExternalWritePolicy() = %q; want record_only", flags.ExternalWritePolicy())
	}
	assertNormalizationReasons(t, flags, ObserveFirstFeatureFlagNormalizationReasonMasterOffClamp)
}

func TestNormalizeObserveFirstFeatureFlags_StateDisabledForcesConfigOffOnly(t *testing.T) {
	flags := NormalizeObserveFirstFeatureFlags(true, false, true, ObserveFirstExternalWritePolicyInvalidateOnly)

	if !flags.ObserveFirstEnabled() {
		t.Fatal("ObserveFirstEnabled() = false; want true")
	}
	if flags.PassiveStateDirectApply() {
		t.Fatal("PassiveStateDirectApply() = true; want false")
	}
	if flags.PassiveConfigDirectApply() {
		t.Fatal("PassiveConfigDirectApply() = true; want false")
	}
	if flags.ExternalWritePolicy() != ObserveFirstExternalWritePolicyInvalidateOnly {
		t.Fatalf("ExternalWritePolicy() = %q; want invalidate_only", flags.ExternalWritePolicy())
	}
	assertNormalizationReasons(t, flags,
		ObserveFirstFeatureFlagNormalizationReasonConfigRequiresState,
	)
}

func TestNormalizeObserveFirstFeatureFlags_StateDisabledKeepsValidExternalWritePolicy(t *testing.T) {
	for _, policy := range []ObserveFirstExternalWritePolicy{
		ObserveFirstExternalWritePolicyInvalidateOnly,
		ObserveFirstExternalWritePolicyRecordAndInvalidate,
	} {
		t.Run(string(policy), func(t *testing.T) {
			flags := NormalizeObserveFirstFeatureFlags(true, false, false, policy)

			if !flags.ObserveFirstEnabled() {
				t.Fatal("ObserveFirstEnabled() = false; want true")
			}
			if flags.PassiveStateDirectApply() {
				t.Fatal("PassiveStateDirectApply() = true; want false")
			}
			if flags.PassiveConfigDirectApply() {
				t.Fatal("PassiveConfigDirectApply() = true; want false")
			}
			if flags.ExternalWritePolicy() != policy {
				t.Fatalf("ExternalWritePolicy() = %q; want %q", flags.ExternalWritePolicy(), policy)
			}
			assertNormalizationReasons(t, flags)
		})
	}
}

func TestNormalizeObserveFirstFeatureFlags_ConfigDirectRequiresInvalidatingPolicy(t *testing.T) {
	flags := NormalizeObserveFirstFeatureFlags(true, true, true, ObserveFirstExternalWritePolicyRecordOnly)

	if !flags.PassiveConfigDirectApply() {
		t.Fatal("PassiveConfigDirectApply() = false; want true")
	}
	if flags.ExternalWritePolicy() != ObserveFirstExternalWritePolicyRecordAndInvalidate {
		t.Fatalf("ExternalWritePolicy() = %q; want record_and_invalidate", flags.ExternalWritePolicy())
	}
	assertNormalizationReasons(t, flags, ObserveFirstFeatureFlagNormalizationReasonConfigRequiresInvalidation)
}

func TestApplyDefaultsNormalizesObserveFirstFlagsIntoConfig(t *testing.T) {
	cfg := applyDefaults(Config{
		ObserveFirstEnabled:      true,
		PassiveStateDirectApply:  false,
		PassiveConfigDirectApply: true,
		ExternalWritePolicy:      ObserveFirstExternalWritePolicyInvalidateOnly,
	})

	if cfg.PassiveConfigDirectApply {
		t.Fatal("PassiveConfigDirectApply = true; want false after normalization")
	}
	if cfg.ExternalWritePolicy != ObserveFirstExternalWritePolicyInvalidateOnly {
		t.Fatalf("ExternalWritePolicy = %q; want invalidate_only", cfg.ExternalWritePolicy)
	}
	if cfg.ObserveFirstFlags.PassiveConfigDirectApply() {
		t.Fatal("ObserveFirstFlags.PassiveConfigDirectApply() = true; want false")
	}
}

func TestParseObserveFirstExternalWritePolicyAcceptsBoundedValues(t *testing.T) {
	tests := []struct {
		input string
		want  ObserveFirstExternalWritePolicy
	}{
		{input: "invalidate_only", want: ObserveFirstExternalWritePolicyInvalidateOnly},
		{input: "record_only", want: ObserveFirstExternalWritePolicyRecordOnly},
		{input: "record_and_invalidate", want: ObserveFirstExternalWritePolicyRecordAndInvalidate},
		{input: "record-and-invalidate", want: ObserveFirstExternalWritePolicyRecordAndInvalidate},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := ParseObserveFirstExternalWritePolicy(tc.input)
			if err != nil {
				t.Fatalf("ParseObserveFirstExternalWritePolicy(%q) error = %v", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("ParseObserveFirstExternalWritePolicy(%q) = %q; want %q", tc.input, got, tc.want)
			}
		})
	}

	if _, err := ParseObserveFirstExternalWritePolicy("unsafe"); err == nil {
		t.Fatal("ParseObserveFirstExternalWritePolicy(\"unsafe\") error = nil; want error")
	}
}

func assertNormalizationReasons(t *testing.T, flags ObserveFirstFeatureFlags, want ...ObserveFirstFeatureFlagNormalizationReason) {
	t.Helper()

	got := flags.NormalizationReasons()
	if len(got) != len(want) {
		t.Fatalf("NormalizationReasons() = %v; want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("NormalizationReasons()[%d] = %q; want %q", index, got[index], want[index])
		}
	}
}
