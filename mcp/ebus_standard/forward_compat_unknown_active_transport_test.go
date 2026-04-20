package ebus_standard_test

// M6a follow-up fold-in — forward-compat §4 rule 7 (M4D ch.13 §10.3).
//
// Scope.
//   - Existing fixture forward_compat_synthetic_v1_1.golden.json pairs
//     active.transport = "ENH" (a KNOWN value at v1.1) with an unknown
//     active.scope. That covers §4 rule 6 (unknown scope ⇒ fail-closed)
//     but does NOT cover rule 7 (unknown TRANSPORT paired with a KNOWN
//     non-`none` scope such as "partial"). Rule 7 exists to enforce the
//     unknown-transport scope-override precedence: a later contract.minor
//     producer MAY emit a new transport enum value paired with a known
//     non-`none` scope, and rule-6-only consumers would incorrectly infer
//     capability from scope=="partial" alone.
//
// This file:
//   1. Loads a NEW producer-emitted golden that pairs
//      active.transport = "future_transport" (NOT in the v1.1 enum) with
//      active.scope = "partial" (a KNOWN non-`none` value). This is the
//      exact shape flagged in M4D ch.13 §10.3 as "a follow-up obligation
//      on the producer side".
//   2. Asserts the producer actually emits this combination via the
//      canonical envelope composer + SetResponderCapabilityProvider DI
//      (forward-compat in action: producer at minor=1 MUST be able to
//      emit a future transport literal without breaking shape).
//   3. Asserts meta.data_hash is deterministic over the fixture (canonical
//      JSON + SHA-256; re-hashing the decoded envelope yields the same
//      hash the golden records).
//   4. Runs an inline consumer-simulation helper implementing rule 7:
//      unknown active.transport ⇒ treat scope as none, MUST NOT invoke
//      responder regardless of active.scope. This mirrors the M5_PORTAL
//      and M5b_HA_NOOP_COMPAT consumer obligation.
//
// Deliberately NOT duplicating the existing forward_compat_synthetic_v1_1
// coverage (unknown scope / state / reason) — that golden stays locked.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	estd "github.com/Project-Helianthus/helianthus-ebusgateway/mcp/ebus_standard"
)

// knownV11Transports is the v1.1 enum set per M4D ch.13 §5. A consumer at
// contract.minor = 1 only recognises these literals; anything else is an
// unknown-transport (rule 7) trigger.
var knownV11Transports = map[string]struct{}{
	"ENH":       {},
	"ENS":       {},
	"ebusd-tcp": {},
}

// consumerRule7Decision is the fail-closed outcome a canonical consumer
// MUST reach per M4D §4 rule 7. It isolates the scope-override precedence
// so the test can assert it independently of the rest of the consumer
// state machine.
type consumerRule7Decision struct {
	invokeResponder bool
	effectiveScope  string
	logUnknown      string
}

// applyRule7 is a minimal canonical-consumer reduction over the capability
// subtree. It implements rule 7 only: unknown active.transport ⇒
// effective scope = "none", responder MUST NOT be invoked, even when the
// wire active.scope is a known non-none value. All other rules are out of
// scope for this test.
func applyRule7(resp map[string]any) consumerRule7Decision {
	active, _ := resp["active"].(map[string]any)
	transport, _ := active["transport"].(string)
	scope, _ := active["scope"].(string)
	if _, known := knownV11Transports[transport]; !known {
		return consumerRule7Decision{
			invokeResponder: false,
			effectiveScope:  "none",
			logUnknown:      transport,
		}
	}
	// rule 7 path not triggered; leave scope as-is for higher-order rules.
	return consumerRule7Decision{
		invokeResponder: scope == "full" || scope == "partial",
		effectiveScope:  scope,
	}
}

// futureTransportCapability constructs a producer capability that exercises
// rule 7: a future-minor transport literal in active.transport paired with a
// known "partial" scope. The transports[] list still contains the three
// v1.1-enumerated rows plus the future one, per I1 (active MUST reference a
// row in transports[]).
func futureTransportCapability() estd.ResponderCapability {
	return estd.ResponderCapability{
		Version: "v1",
		Active: estd.ActiveResponder{
			Transport: "future_transport",
			Scope:     "partial",
			Surfaces:  []string{"FF_03", "FF_04"},
			Refusal:   nil,
		},
		Transports: []estd.TransportRow{
			{Transport: "ENH", State: "supported", Scope: "partial", Surfaces: []string{"FF_03", "FF_04", "FF_05", "FF_06"}, Reason: ""},
			{Transport: "ENS", State: "supported", Scope: "partial", Surfaces: []string{"FF_03", "FF_04", "FF_05", "FF_06"}, Reason: ""},
			{Transport: "ebusd-tcp", State: "blocked", Scope: "none", Surfaces: []string{}, Reason: "command_bridge_no_companion_listen"},
			{Transport: "future_transport", State: "supported", Scope: "partial", Surfaces: []string{"FF_03", "FF_04"}, Reason: ""},
		},
	}
}

// TestForwardCompat_UnknownActiveTransport_GoldenParses pins the new golden
// fixture shape: unknown active.transport + known non-none active.scope
// (rule 7 trigger).
func TestForwardCompat_UnknownActiveTransport_GoldenParses(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "forward_compat_unknown_active_transport_v1_1.golden.json"))
	if err != nil {
		t.Fatalf("read rule-7 golden: %v", err)
	}
	var env map[string]any
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("unmarshal rule-7 golden: %v — forward-compat contract broken", err)
	}
	meta, _ := env["meta"].(map[string]any)
	if meta == nil {
		t.Fatal("meta missing")
	}
	contract, _ := meta["contract"].(map[string]any)
	if m, _ := contract["minor"].(float64); int(m) != 1 {
		t.Fatalf("contract.minor=%v, want 1", contract["minor"])
	}
	caps, _ := meta["capabilities"].(map[string]any)
	resp, _ := caps["responder"].(map[string]any)
	if resp == nil {
		t.Fatal("capabilities.responder missing in rule-7 golden")
	}
	active, _ := resp["active"].(map[string]any)
	if got, _ := active["transport"].(string); got != "future_transport" {
		t.Fatalf("active.transport=%q, want %q", got, "future_transport")
	}
	if got, _ := active["scope"].(string); got != "partial" {
		t.Fatalf("active.scope=%q, want %q (rule 7 requires known non-none scope)", got, "partial")
	}
}

// TestForwardCompat_UnknownActiveTransport_ProducerEmits asserts the gateway
// envelope composer, when driven by a provider emitting the future-transport
// capability, produces the rule-7 shape. Forward-compat at the producer
// side: at contract.minor = 1 we MUST be able to emit an active.transport
// literal outside the v1.1 enum (e.g. a future minor's additive value).
func TestForwardCompat_UnknownActiveTransport_ProducerEmits(t *testing.T) {
	estd.SetResponderCapabilityProvider(func() estd.ResponderCapability {
		return futureTransportCapability()
	})
	t.Cleanup(func() { estd.SetResponderCapabilityProvider(nil) })

	env := estd.NewEnvelope(nil, nil, time.Unix(0, 0).UTC())
	meta, _ := env["meta"].(map[string]any)
	caps, _ := meta["capabilities"].(map[string]any)
	resp, _ := caps["responder"].(map[string]any)
	if resp == nil {
		t.Fatal("producer dropped meta.capabilities.responder when active.transport was unknown — forward-compat broken")
	}
	active, _ := resp["active"].(map[string]any)
	if got, _ := active["transport"].(string); got != "future_transport" {
		t.Fatalf("producer rewrote/dropped future active.transport: got %q", got)
	}
	if got, _ := active["scope"].(string); got != "partial" {
		t.Fatalf("producer rewrote known active.scope: got %q want partial", got)
	}
}

// TestForwardCompat_UnknownActiveTransport_HashStable asserts canonical JSON
// + SHA-256 over the decoded envelope yields the same data_hash the golden
// records. Guards M4B §7.3 determinism for the rule-7 shape.
func TestForwardCompat_UnknownActiveTransport_HashStable(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "forward_compat_unknown_active_transport_v1_1.golden.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var env map[string]any
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	meta, _ := env["meta"].(map[string]any)
	recorded, _ := meta["data_hash"].(string)
	if recorded == "" {
		t.Fatal("golden missing meta.data_hash")
	}
	// Recompute over the envelope's data field (data-hash scope per
	// envelope.go DataHash contract — only the `data` payload is hashed,
	// not meta).
	recomputed := estd.DataHash(env["data"])
	if recomputed != recorded {
		t.Fatalf("data_hash drift: recomputed=%s recorded=%s", recomputed, recorded)
	}
}

// TestForwardCompat_UnknownActiveTransport_ConsumerFailsClosed is the
// load-bearing assertion: any canonical consumer applying §4 rule 7 MUST
// NOT invoke responder when active.transport is unknown, regardless of
// active.scope. This test guards the scope-override precedence that
// distinguishes rule 7 from rule 6.
func TestForwardCompat_UnknownActiveTransport_ConsumerFailsClosed(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "forward_compat_unknown_active_transport_v1_1.golden.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var env map[string]any
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	meta, _ := env["meta"].(map[string]any)
	caps, _ := meta["capabilities"].(map[string]any)
	resp, _ := caps["responder"].(map[string]any)
	if resp == nil {
		t.Fatal("capabilities.responder missing in golden")
	}
	decision := applyRule7(resp)
	if decision.invokeResponder {
		t.Fatal("consumer invoked responder despite unknown active.transport — rule 7 violated")
	}
	if decision.effectiveScope != "none" {
		t.Fatalf("consumer effective scope=%q, want none (rule 7 override)", decision.effectiveScope)
	}
	if decision.logUnknown != "future_transport" {
		t.Fatalf("consumer did not capture unknown transport for diagnostics: got %q", decision.logUnknown)
	}
}
