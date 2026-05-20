package main

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/adaptermux/v8classifier"
)

// TestV8ClassifierMode_EnvParse pins the production wiring contract
// surfaced by Codex round-1 MAJOR on promotion PR #650: the
// HELIANTHUS_V8_CLASSIFIER_MODE env var MUST be parseable by
// v8classifier.ParseMode in the way cmd/gateway/main.go consumes
// it (cmd/gateway/main.go reads os.Getenv(v8classifier.EnvVarName)
// and feeds the value to ParseMode).
//
// This is a unit-level guard: if a future refactor renames the
// env var, changes the ParseMode contract, or removes the
// production wiring branch in main.go, this test won't directly
// catch it — but it WILL flag breaking changes to the env-parse
// surface that the production code relies on.
//
// The full integration (env var → muxCfg.V8ClassifierMode →
// Mux.New constructs a non-nil classifier) is best validated by
// a smoke test on the live deployment; this unit test pins the
// API stability the production code depends on.
func TestV8ClassifierMode_EnvParse(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		input   string
		want    v8classifier.Mode
		wantErr bool
	}{
		{"empty defaults to Off", "", v8classifier.ModeOff, false},
		{"off literal", "off", v8classifier.ModeOff, false},
		{"shadow literal", "shadow", v8classifier.ModeShadow, false},
		{"enforce literal", "enforce", v8classifier.ModeEnforce, false},
		{"case-insensitive Shadow", "Shadow", v8classifier.ModeShadow, false},
		{"case-insensitive ENFORCE", "ENFORCE", v8classifier.ModeEnforce, false},
		{"whitespace-trimmed", "  shadow  ", v8classifier.ModeShadow, false},
		{"synonym disabled", "disabled", v8classifier.ModeOff, false},
		{"synonym false", "false", v8classifier.ModeOff, false},
		{"unknown errors", "enfource", v8classifier.ModeOff, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := v8classifier.ParseMode(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("ParseMode(%q) err=nil; want non-nil", tc.input)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseMode(%q) err=%v; want nil", tc.input, err)
				return
			}
			if got != tc.want {
				t.Errorf("ParseMode(%q) = %v; want %v", tc.input, got, tc.want)
			}
		})
	}

	// EnvVarName is what cmd/gateway/main.go reads — pin the
	// exact string so a rename surfaces here.
	if got := v8classifier.EnvVarName; got != "HELIANTHUS_V8_CLASSIFIER_MODE" {
		t.Errorf("v8classifier.EnvVarName = %q; want \"HELIANTHUS_V8_CLASSIFIER_MODE\"", got)
	}
}
