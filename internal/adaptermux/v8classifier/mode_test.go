package v8classifier

import "testing"

// Phase 3 Step B3.2: tests for the mode-parsing surface. The
// behavioral methods (Observe, OnAdminEvent) are tested in
// classifier_test.go.

func TestMode_String(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		m    Mode
		want string
	}{
		{ModeOff, "off"},
		{ModeShadow, "shadow"},
		{ModeEnforce, "enforce"},
		{Mode(99), "unknown"},
	} {
		if got := tc.m.String(); got != tc.want {
			t.Errorf("Mode(%d).String() = %q; want %q", tc.m, got, tc.want)
		}
	}
}

func TestParseMode_KnownLabels(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		in   string
		want Mode
	}{
		// Canonical lowercase.
		{"off", ModeOff},
		{"shadow", ModeShadow},
		{"enforce", ModeEnforce},
		// Mixed case.
		{"OFF", ModeOff},
		{"Shadow", ModeShadow},
		{"ENFORCE", ModeEnforce},
		// Surrounding whitespace.
		{"  shadow  ", ModeShadow},
		{"\toff\n", ModeOff},
		// "off" synonyms (sanity — operator might use any of these).
		{"disabled", ModeOff},
		{"0", ModeOff},
		{"false", ModeOff},
		{"no", ModeOff},
		// Empty string is the safe default.
		{"", ModeOff},
		{"   ", ModeOff},
	} {
		got, err := ParseMode(tc.in)
		if err != nil {
			t.Errorf("ParseMode(%q) err=%v; want nil", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("ParseMode(%q) = %v; want %v", tc.in, got, tc.want)
		}
	}
}

func TestParseMode_UnknownLabels(t *testing.T) {
	t.Parallel()
	// Each of these is a plausible operator typo or wrong guess.
	// All must return ModeOff + non-nil err so the gateway startup
	// glue can fail loudly rather than silently disable v8.
	for _, in := range []string{
		"enfource", // typo
		"shadows",
		"on",        // ambiguous — must be off|shadow|enforce
		"1",         // arabic-numeral truthy — could be confused with "on"
		"yes",       // truthy
		"true",      // truthy
		"force",     // half-word
		"shadow!",   // suffix
		"v8",        // shorthand
	} {
		got, err := ParseMode(in)
		if err == nil {
			t.Errorf("ParseMode(%q) err=nil; want non-nil (unrecognized label)", in)
		}
		if got != ModeOff {
			t.Errorf("ParseMode(%q) = %v; want ModeOff on error (safe fallback)", in, got)
		}
	}
}

func TestEnvVarName_IsStable(t *testing.T) {
	t.Parallel()
	// Pin the env var name. A rename here would silently disable
	// the v8 classifier on every running gateway after a rolling
	// upgrade, so the rename is a coordinated config change.
	const want = "HELIANTHUS_V8_CLASSIFIER_MODE"
	if EnvVarName != want {
		t.Errorf("EnvVarName = %q; want %q (must NOT rename without operator coordination)", EnvVarName, want)
	}
}
