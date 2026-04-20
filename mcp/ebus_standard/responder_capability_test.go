package ebus_standard_test

// M4c2 envelope tests — meta.capabilities.responder.
//
// These tests lock decision doc
// (helianthus-execution-plans@567a6798) §4.2 shape + §4.4 invariants I1–I8
// + §4.3 forward-compat rule 1 (absence ⇒ fail-closed) into the gateway
// envelope composer.
//
// Test-time DI: the package-level provider setter
// SetResponderCapabilityProvider is how tests inject fixed capabilities
// without widening the NewEnvelope signature. Each test resets the
// provider via t.Cleanup so parallel packages see a pristine state.

import (
	"encoding/json"
	"testing"
	"time"

	estd "github.com/Project-Helianthus/helianthus-ebusgateway/mcp/ebus_standard"
)

// withProvider installs p for the duration of the test and restores the
// prior provider (nil by default) on cleanup.
func withProvider(t *testing.T, p estd.ResponderCapabilityProvider) {
	t.Helper()
	estd.SetResponderCapabilityProvider(p)
	t.Cleanup(func() { estd.SetResponderCapabilityProvider(nil) })
}

// fixedCapability constructs a canonical ENH-active v1.1 capability.
func fixedCapability() estd.ResponderCapability {
	return estd.ResponderCapability{
		Version: "v1",
		Active: estd.ActiveResponder{
			Transport: "ENH",
			Scope:     "partial",
			Surfaces:  []string{"FF_03", "FF_04", "FF_05", "FF_06"},
			Refusal:   nil,
		},
		Transports: []estd.TransportRow{
			{Transport: "ENH", State: "supported", Scope: "partial", Surfaces: []string{"FF_03", "FF_04", "FF_05", "FF_06"}, Reason: ""},
			{Transport: "ENS", State: "supported", Scope: "partial", Surfaces: []string{"FF_03", "FF_04", "FF_05", "FF_06"}, Reason: ""},
			{Transport: "ebusd-tcp", State: "blocked", Scope: "none", Surfaces: []string{}, Reason: "command_bridge_no_companion_listen"},
		},
	}
}

// getCapabilitySubtree navigates meta.capabilities.responder; returns nil on
// any missing intermediate key.
func getCapabilitySubtree(env map[string]any) map[string]any {
	meta, ok := env["meta"].(map[string]any)
	if !ok {
		return nil
	}
	caps, ok := meta["capabilities"].(map[string]any)
	if !ok {
		return nil
	}
	resp, _ := caps["responder"].(map[string]any)
	return resp
}

// TestCapability_EmittedWhenProviderSet asserts that when a provider is
// registered, meta.capabilities.responder appears in every envelope.
func TestCapability_EmittedWhenProviderSet(t *testing.T) {
	withProvider(t, func() estd.ResponderCapability { return fixedCapability() })
	env := estd.NewEnvelope(nil, nil, time.Unix(0, 0).UTC())
	resp := getCapabilitySubtree(env)
	if resp == nil {
		t.Fatal("meta.capabilities.responder absent when provider is registered")
	}
}

// TestCapability_OmittedWhenProviderNil asserts that when no provider is
// registered (nil), the capabilities.responder key is omitted entirely.
// Per §4.3 rule 1, consumers treat absence as active.scope = none
// (fail-closed) — so absence is the correct "no signal" representation and
// MUST NOT be emitted as an empty object or null value.
func TestCapability_OmittedWhenProviderNil(t *testing.T) {
	estd.SetResponderCapabilityProvider(nil) // explicit reset for clarity
	env := estd.NewEnvelope(nil, nil, time.Unix(0, 0).UTC())
	meta, _ := env["meta"].(map[string]any)
	if caps, ok := meta["capabilities"]; ok {
		t.Fatalf("meta.capabilities key must be absent when no provider set (got %v)", caps)
	}
}

// TestCapability_ContractMinorBumped locks the open-enum forward-compat
// rule at the envelope level: emitting the capability key requires
// contract.minor >= 1.
func TestCapability_ContractMinorBumped(t *testing.T) {
	if estd.EnvelopeContractMajor != 1 {
		t.Fatalf("major drift: got %d, want 1", estd.EnvelopeContractMajor)
	}
	if estd.EnvelopeContractMinor != 1 {
		t.Fatalf("M4c2 minor bump missing: got %d, want 1", estd.EnvelopeContractMinor)
	}
}

// TestCapability_I1_ThreeTransportRows (decision §4.4 I1).
func TestCapability_I1_ThreeTransportRows(t *testing.T) {
	withProvider(t, func() estd.ResponderCapability { return fixedCapability() })
	env := estd.NewEnvelope(nil, nil, time.Unix(0, 0).UTC())
	resp := getCapabilitySubtree(env)
	if resp == nil {
		t.Fatal("responder subtree absent")
	}
	rows, _ := resp["transports"].([]any)
	if len(rows) != 3 {
		t.Fatalf("I1: transports[] len=%d, want 3 (ENH, ENS, ebusd-tcp)", len(rows))
	}
}

// TestCapability_I2_ActiveInTransports (decision §4.4 I2).
func TestCapability_I2_ActiveInTransports(t *testing.T) {
	withProvider(t, func() estd.ResponderCapability { return fixedCapability() })
	env := estd.NewEnvelope(nil, nil, time.Unix(0, 0).UTC())
	resp := getCapabilitySubtree(env)
	active, _ := resp["active"].(map[string]any)
	activeT, _ := active["transport"].(string)
	rows, _ := resp["transports"].([]any)
	found := false
	for _, r := range rows {
		row, _ := r.(map[string]any)
		if t2, _ := row["transport"].(string); t2 == activeT {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("I2: active.transport %q not present in transports[]", activeT)
	}
}

// TestCapability_I3_ActiveScopeMatchesRow (decision §4.4 I3).
func TestCapability_I3_ActiveScopeMatchesRow(t *testing.T) {
	withProvider(t, func() estd.ResponderCapability { return fixedCapability() })
	env := estd.NewEnvelope(nil, nil, time.Unix(0, 0).UTC())
	resp := getCapabilitySubtree(env)
	active, _ := resp["active"].(map[string]any)
	activeT, _ := active["transport"].(string)
	activeS, _ := active["scope"].(string)
	rows, _ := resp["transports"].([]any)
	for _, r := range rows {
		row, _ := r.(map[string]any)
		if t2, _ := row["transport"].(string); t2 == activeT {
			if rs, _ := row["scope"].(string); rs != activeS {
				t.Fatalf("I3: active.scope=%q != transports[%s].scope=%q", activeS, activeT, rs)
			}
			return
		}
	}
	t.Fatal("I3: active row not located")
}

// TestCapability_I4_ScopeNoneIffBlocked (decision §4.4 I4/I5/I6
// combined via the ebusd-tcp row).
func TestCapability_I4_I5_I6_BlockedRowShape(t *testing.T) {
	withProvider(t, func() estd.ResponderCapability { return fixedCapability() })
	env := estd.NewEnvelope(nil, nil, time.Unix(0, 0).UTC())
	resp := getCapabilitySubtree(env)
	rows, _ := resp["transports"].([]any)
	var blocked map[string]any
	for _, r := range rows {
		row, _ := r.(map[string]any)
		if state, _ := row["state"].(string); state == "blocked" {
			blocked = row
			break
		}
	}
	if blocked == nil {
		t.Fatal("I5: no blocked row found for ebusd-tcp")
	}
	if s, _ := blocked["scope"].(string); s != "none" {
		t.Fatalf("I5: blocked row scope=%q, want none", s)
	}
	if r, _ := blocked["reason"].(string); r == "" {
		t.Fatal("I5: blocked row reason must be non-null/non-empty")
	}
	// I6: supported rows MUST have reason null/empty.
	for _, r := range rows {
		row, _ := r.(map[string]any)
		if state, _ := row["state"].(string); state == "supported" {
			if rr := row["reason"]; rr != nil && rr != "" {
				t.Fatalf("I6: supported row has non-null reason %v", rr)
			}
		}
	}
}

// TestCapability_I7_NoDuplicateTransports (decision §4.4 I7).
func TestCapability_I7_NoDuplicateTransports(t *testing.T) {
	withProvider(t, func() estd.ResponderCapability { return fixedCapability() })
	env := estd.NewEnvelope(nil, nil, time.Unix(0, 0).UTC())
	resp := getCapabilitySubtree(env)
	rows, _ := resp["transports"].([]any)
	seen := map[string]bool{}
	for _, r := range rows {
		row, _ := r.(map[string]any)
		tn, _ := row["transport"].(string)
		if seen[tn] {
			t.Fatalf("I7: duplicate transport row %q", tn)
		}
		seen[tn] = true
	}
}

// TestCapability_I8_UnknownRefusalFailsClosed asserts the envelope composer
// passes through an unknown active.refusal.code verbatim — consumers apply
// fail-closed per §4.3 rule 6. This test confirms the gateway does not
// try to sanitise/drop unknown refusal codes (forward-compat contract).
func TestCapability_I8_UnknownRefusalFailsClosed(t *testing.T) {
	cap := fixedCapability()
	cap.Active.Scope = "none"
	cap.Active.Surfaces = []string{}
	cap.Active.Refusal = &estd.ActiveRefusal{Code: "synthetic_future_code", Reason: "forward-compat probe"}
	withProvider(t, func() estd.ResponderCapability { return cap })
	env := estd.NewEnvelope(nil, nil, time.Unix(0, 0).UTC())
	resp := getCapabilitySubtree(env)
	active, _ := resp["active"].(map[string]any)
	refusal, _ := active["refusal"].(map[string]any)
	if refusal == nil {
		t.Fatal("I8: refusal must be preserved verbatim for unknown-code forward-compat")
	}
	if c, _ := refusal["code"].(string); c != "synthetic_future_code" {
		t.Fatalf("I8: refusal.code altered: got %q", c)
	}
}

// TestCapability_VersionFieldIsV1 (decision §4.2).
func TestCapability_VersionFieldIsV1(t *testing.T) {
	withProvider(t, func() estd.ResponderCapability { return fixedCapability() })
	env := estd.NewEnvelope(nil, nil, time.Unix(0, 0).UTC())
	resp := getCapabilitySubtree(env)
	if v, _ := resp["version"].(string); v != "v1" {
		t.Fatalf("version=%q, want v1", v)
	}
}

// TestCapability_MarshalsToStableJSON asserts the emitted subtree marshals
// cleanly (no unmarshallable types leak from the provider struct).
func TestCapability_MarshalsToStableJSON(t *testing.T) {
	withProvider(t, func() estd.ResponderCapability { return fixedCapability() })
	env := estd.NewEnvelope(nil, nil, time.Unix(0, 0).UTC())
	if _, err := json.Marshal(env); err != nil {
		t.Fatalf("envelope marshals: %v", err)
	}
}
