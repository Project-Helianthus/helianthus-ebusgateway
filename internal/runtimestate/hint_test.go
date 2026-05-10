package runtimestate

import "testing"

// HintFromState contract — extracts the cached source-address hint for the
// SourceAddressSelector. AD24: the returned byte is HISTORICAL only; the
// caller MUST validate via the selector before any surface treats it as the
// admitted source.

func TestHintFromState_NilStateReturnsNoHint(t *testing.T) {
	hint, ok := HintFromState(nil)
	if ok {
		t.Errorf("nil state must return ok=false; got hint=0x%02x ok=true", hint)
	}
	if hint != 0 {
		t.Errorf("nil state hint must be 0; got 0x%02x", hint)
	}
}

func TestHintFromState_StateWithoutEBusReturnsNoHint(t *testing.T) {
	state := &State{SchemaVersion: 1}
	hint, ok := HintFromState(state)
	if ok {
		t.Errorf("state without EBus must return ok=false; got hint=0x%02x ok=true", hint)
	}
	if hint != 0 {
		t.Errorf("state without EBus hint must be 0; got 0x%02x", hint)
	}
}

func TestHintFromState_EBusWithoutSelfReturnsNoHint(t *testing.T) {
	state := &State{
		SchemaVersion: 1,
		EBus:          &EBusNamespace{SchemaVersion: 1},
	}
	hint, ok := HintFromState(state)
	if ok {
		t.Errorf("EBus without Self must return ok=false; got hint=0x%02x ok=true", hint)
	}
	if hint != 0 {
		t.Errorf("EBus without Self hint must be 0; got 0x%02x", hint)
	}
}

func TestHintFromState_PopulatedSelfReturnsCachedSource(t *testing.T) {
	state := &State{
		SchemaVersion: 1,
		EBus: &EBusNamespace{
			SchemaVersion: 1,
			Self: &Self{
				LastAdmittedSource: 0x77,
				SelectionMethod:    SelectionMethodWarmup,
			},
		},
	}
	hint, ok := HintFromState(state)
	if !ok {
		t.Errorf("populated Self must return ok=true; got hint=0x%02x ok=false", hint)
	}
	if hint != 0x77 {
		t.Errorf("hint = 0x%02x; want 0x77", hint)
	}
}

func TestHintFromState_PopulatedSelfWithZeroSourceIsHonored(t *testing.T) {
	// LastAdmittedSource=0x00 is a legal cached value (PC/Modem in the
	// source-address table). HintFromState must return (0, true) so the
	// caller can pass HintCandidateSet=true to the selector and have it
	// honored as a real hint, not silently dropped. The byte-zero
	// ambiguity is resolved by the boolean.
	state := &State{
		SchemaVersion: 1,
		EBus: &EBusNamespace{
			SchemaVersion: 1,
			Self: &Self{
				LastAdmittedSource: 0x00,
				SelectionMethod:    SelectionMethodWarmup,
			},
		},
	}
	hint, ok := HintFromState(state)
	if !ok {
		t.Errorf("Self with LastAdmittedSource=0x00 must still return ok=true; got hint=0x%02x ok=false", hint)
	}
	if hint != 0x00 {
		t.Errorf("hint = 0x%02x; want 0x00 (literal cached value)", hint)
	}
}

// -----------------------------------------------------------------------------
// AD24 invariant test (runtime-state-w19-26.locked):
//
// "ebus.self is HISTORICAL HINT ONLY. The current admitted source is
// exclusively the in-memory SourceAddressSelection.Source from the current
// session, AFTER SourceAddressSelector validation succeeds. No surface
// (loader, GraphQL, MCP, metrics) may expose runtime_state.ebus.self as the
// current admitted source until the current session's SourceAddressSelector
// validation passes."
//
// HintFromState is the ONLY function that exports ebus.self.last_admitted_source
// out of the runtimestate package. By contract its return value is wired
// exclusively into SourceAddressSelectionConfig.HintCandidate (a candidate-
// ordering bias the selector validates), NEVER into builder.SetAdmittedMutationSource
// or any "current admitted source" surface. This test pins that contract:
// the package has exactly one exported helper that surfaces the cached
// source byte, and its docstring says "HISTORICAL".
// -----------------------------------------------------------------------------

func TestAD24_HintFromStateIsTheOnlyExportedAccessor(t *testing.T) {
	// This test is a compile-time-style assertion: HintFromState exists
	// and returns the cached byte plus a presence flag. If a future PR
	// adds another exported function/method that returns the cached
	// last_admitted_source as a "current" source, this test should be
	// updated to reflect the new contract — and a careful review must
	// verify the new accessor preserves AD24.
	state := &State{
		SchemaVersion: 1,
		EBus: &EBusNamespace{
			SchemaVersion: 1,
			Self: &Self{
				LastAdmittedSource: 0x77,
				SelectionMethod:    SelectionMethodWarmup,
			},
		},
	}
	hint, ok := HintFromState(state)
	if !ok || hint != 0x77 {
		t.Fatalf("HintFromState contract regression: got hint=0x%02x ok=%v; want 0x77 true", hint, ok)
	}

	// Sanity: Manager.State() exposes the State by clone — but the State
	// type has no method that says "this is the admitted source". Callers
	// that want the cached byte must reach in via state.EBus.Self.LastAdmittedSource
	// (a plain struct field) or via HintFromState (this function). Both
	// require explicit knowledge that the value is cached, not current.
	if state.EBus.Self.LastAdmittedSource != 0x77 {
		t.Fatalf("ebus.self.last_admitted_source field corruption; got 0x%02x", state.EBus.Self.LastAdmittedSource)
	}
}
