package ebus_standard_test

// Forward-compat test (decision doc §4.5 + M4B §7.3).
//
// A synthetic v1.1 envelope that carries:
//   - An unknown active.scope literal ("future_unknown_scope").
//   - An unknown transports[].state literal ("future_unknown_state").
//   - An unknown transports[].reason literal ("future_unknown_reason").
//
// MUST parse via encoding/json without error — consumers that don't know
// these values apply fail-closed behavior per §4.3. This test pins the
// open-enum contract at the shape level (no strict enum validator in the
// wire parser).

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestForwardCompat_UnknownEnumsParse(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "forward_compat_synthetic_v1_1.golden.json"))
	if err != nil {
		t.Fatalf("read synthetic golden: %v", err)
	}
	var env map[string]any
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("unmarshal failed: %v — forward-compat contract broken", err)
	}
	meta, ok := env["meta"].(map[string]any)
	if !ok {
		t.Fatal("meta missing in synthetic envelope")
	}
	contract, _ := meta["contract"].(map[string]any)
	if m, _ := contract["minor"].(float64); int(m) != 1 {
		t.Fatalf("contract.minor=%v in synthetic, want 1", contract["minor"])
	}
	caps, _ := meta["capabilities"].(map[string]any)
	resp, _ := caps["responder"].(map[string]any)
	if resp == nil {
		t.Fatal("capabilities.responder missing")
	}
	// Confirm unknown active.scope passed through.
	active, _ := resp["active"].(map[string]any)
	if s, _ := active["scope"].(string); s != "future_unknown_scope" {
		t.Fatalf("unknown active.scope dropped/rewritten: %q", s)
	}
	// Confirm unknown state + reason passed through.
	rows, _ := resp["transports"].([]any)
	var sawUnknownState, sawUnknownReason bool
	for _, r := range rows {
		row, _ := r.(map[string]any)
		if st, _ := row["state"].(string); st == "future_unknown_state" {
			sawUnknownState = true
		}
		if rs, _ := row["reason"].(string); rs == "future_unknown_reason" {
			sawUnknownReason = true
		}
	}
	if !sawUnknownState {
		t.Fatal("unknown transports[].state dropped")
	}
	if !sawUnknownReason {
		t.Fatal("unknown transports[].reason dropped")
	}
}
