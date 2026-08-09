package coexistence

import "testing"

func TestIdentityKeyV1ClassifiesViaDeviceKeysNarrowly(t *testing.T) {
	for _, test := range []struct {
		key  string
		want bool
	}{
		{key: "via_device", want: true},
		{key: "via_parent_device", want: true},
		{key: "via", want: false},
		{key: "via_prose", want: false},
		{key: "via_device_note", want: false},
	} {
		t.Run(test.key, func(t *testing.T) {
			if got := identityKeyV1(keyTokensV1(test.key)); got != test.want {
				t.Fatalf("identityKeyV1(%q) = %t; want %t", test.key, got, test.want)
			}
		})
	}
}

func TestCandidateLeakKeyV1ClassifiesIdentifierTokensNarrowly(t *testing.T) {
	for _, test := range []struct {
		key  string
		want bool
	}{
		{key: "candidate_status_v1", want: true},
		{key: "candidateStatusV1", want: true},
		{key: "v1_candidate_status", want: true},
		{key: "conflict_state", want: true},
		{key: "raw_only_reason", want: true},
		{key: "rawOnlyReason", want: true},
		{key: "withheld_count_v1", want: true},
		{key: "candidately_explained", want: false},
		{key: "conflictingly_explained", want: false},
		{key: "rawness_only", want: false},
		{key: "unwithheld_count", want: false},
	} {
		t.Run(test.key, func(t *testing.T) {
			if got := candidateLeakKeyV1(test.key); got != test.want {
				t.Fatalf("candidateLeakKeyV1(%q) = %t; want %t", test.key, got, test.want)
			}
		})
	}
}

func TestCandidateLeakV1AllowsExplanatoryStrings(t *testing.T) {
	payload := map[string]any{
		"note": "candidate_status_v1 is an internal field name and is not emitted here",
	}
	if containsCandidateLeakV1(payload, nil, nil) {
		t.Fatal("explanatory string must not be treated as a structured candidate leak")
	}
}
