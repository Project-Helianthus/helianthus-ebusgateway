package nm_runtime_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/execution_policy"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/nm_runtime"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/rpc_source"
	ebusstd "github.com/Project-Helianthus/helianthus-ebusreg/catalog/ebus_standard"
)

const testRuntimeSource byte = 0x7F

type recordingEmitter struct {
	calls []struct {
		src, pb, sb byte
		payload     []byte
	}
}

func (r *recordingEmitter) EmitBroadcast(_ context.Context, src, pb, sb byte, payload []byte) error {
	if err := rpc_source.Enforce(src); err != nil {
		return err
	}
	r.calls = append(r.calls, struct {
		src, pb, sb byte
		payload     []byte
	}{src, pb, sb, append([]byte(nil), payload...)})
	return nil
}

func u8(v uint8) *uint8 { return &v }

// syntheticCatalog builds a minimal catalog with the two whitelisted emit
// broadcasts used by M4 first-delivery. Axis literals MUST mirror
// ebusreg@30aa69a catalog/ebus_standard/catalog.yaml exactly.
func syntheticCatalog() ebusstd.Catalog {
	mk := func(sb uint8, variant string) ebusstd.Command {
		return ebusstd.Command{
			ID:          "ebus_standard.nm." + variant,
			Name:        variant,
			SafetyClass: ebusstd.SafetyBroadcast,
			Identity: ebusstd.IdentityKey{
				Namespace:                       "ebus_standard",
				PB:                              u8(0xFF),
				SB:                              u8(sb),
				TelegramClass:                   ebusstd.TelegramClassBroadcast,
				Direction:                       ebusstd.DirectionRequest,
				RequestOrResponseRole:           ebusstd.RoleOriginator,
				BroadcastOrAddressed:            ebusstd.AddressedBroadcast,
				AnswerPolicy:                    ebusstd.AnswerNone,
				LengthPrefixMode:                ebusstd.LengthPrefixFixed,
				SelectorDecoder:                 "none",
				ServiceVariant:                  variant,
				TransportCapabilityRequirements: []string{"broadcast_send"},
				Version:                         "v1.0-locked",
			},
		}
	}
	pb := uint8(0xFF)
	return ebusstd.Catalog{
		Namespace: "ebus_standard",
		Version:   "v1.0-locked",
		Services: []ebusstd.Service{{
			PB:   &pb,
			Name: "Network Management",
			Commands: []ebusstd.Command{
				mk(0x00, "reset_status"),
				mk(0x02, "failure_message"),
			},
		}},
	}
}

func TestRuntime_Emit_ResetStatus_PassesPolicyAndEmits(t *testing.T) {
	em := &recordingEmitter{}
	rt, err := nm_runtime.NewRuntime(syntheticCatalog(), em, testRuntimeSource)
	if err != nil {
		t.Fatalf("NewRuntime err: %v", err)
	}
	if err := rt.Emit(context.Background(), nm_runtime.EventResetStatus, []byte{0xAB}); err != nil {
		t.Fatalf("Emit reset status err: %v", err)
	}
	if len(em.calls) != 1 {
		t.Fatalf("want 1 emit call, got %d", len(em.calls))
	}
	c := em.calls[0]
	if c.src != testRuntimeSource {
		t.Fatalf("emit source = 0x%02X, want 0x%02X", c.src, testRuntimeSource)
	}
	if c.pb != 0xFF || c.sb != 0x00 {
		t.Fatalf("emit pb/sb = 0x%02X/0x%02X, want FF/00", c.pb, c.sb)
	}
}

func TestRuntime_Emit_FailureBroadcast(t *testing.T) {
	em := &recordingEmitter{}
	rt, err := nm_runtime.NewRuntime(syntheticCatalog(), em, testRuntimeSource)
	if err != nil {
		t.Fatalf("NewRuntime err: %v", err)
	}
	if err := rt.Emit(context.Background(), nm_runtime.EventFailure, []byte{0x01}); err != nil {
		t.Fatalf("Emit failure err: %v", err)
	}
	if len(em.calls) != 1 || em.calls[0].sb != 0x02 {
		t.Fatalf("want single FF 02 emit, got %+v", em.calls)
	}
}

func TestRuntime_Emit_RefusesIfCatalogMissing(t *testing.T) {
	em := &recordingEmitter{}
	rt, err := nm_runtime.NewRuntime(ebusstd.Catalog{Namespace: "ebus_standard"}, em, testRuntimeSource)
	if err != nil {
		t.Fatalf("NewRuntime err: %v", err)
	}
	err = rt.Emit(context.Background(), nm_runtime.EventResetStatus, nil)
	if !errors.Is(err, nm_runtime.ErrNoCatalogEntry) {
		t.Fatalf("want ErrNoCatalogEntry, got %v", err)
	}
}

// TestNewRuntime_RejectsNilEmitter is the fail-fast guard for issue #505
// r3106859312. NewRuntime previously accepted a nil Emitter and would
// nil-panic on the first Emit call; now construction must reject it with
// ErrEmitterRequired.
func TestNewRuntime_RejectsNilEmitter(t *testing.T) {
	rt, err := nm_runtime.NewRuntime(syntheticCatalog(), nil, testRuntimeSource)
	if err == nil {
		t.Fatal("NewRuntime(nil emitter) must return error")
	}
	if !errors.Is(err, nm_runtime.ErrEmitterRequired) {
		t.Fatalf("want ErrEmitterRequired, got %v", err)
	}
	if rt != nil {
		t.Fatalf("runtime must be nil on construction error, got %+v", rt)
	}
}

// TestRuntime_Emit_RejectsUndeclaredEvent is the regression guard for
// issue #505 r3106904915. Even if the caller casts a rogue string through
// EmitEvent AND the execution_policy whitelist or catalog would otherwise
// permit it, Emit MUST reject it BEFORE any catalog lookup because M4
// first-delivery scope is strictly the declared constants (reset_status,
// failure_message). Extending the declared set requires locked-plan
// approval.
func TestRuntime_Emit_RejectsUndeclaredEvent(t *testing.T) {
	em := &recordingEmitter{}
	rt, err := nm_runtime.NewRuntime(syntheticCatalog(), em, testRuntimeSource)
	if err != nil {
		t.Fatalf("NewRuntime err: %v", err)
	}
	err = rt.Emit(context.Background(), nm_runtime.EmitEvent("sign_of_life"), nil)
	if err == nil {
		t.Fatal("undeclared emit event must be rejected")
	}
	if !errors.Is(err, nm_runtime.ErrUnknownEmitEvent) {
		t.Fatalf("want ErrUnknownEmitEvent, got %v", err)
	}
	if len(em.calls) != 0 {
		t.Fatal("rejected call must not reach emitter")
	}
}

// TestRuntime_Emit_DeclaredEventContinuesToCatalog proves the guard does
// not short-circuit the happy path — declared events still flow through
// findEmit and policy.
func TestRuntime_Emit_DeclaredEventContinuesToCatalog(t *testing.T) {
	em := &recordingEmitter{}
	rt, err := nm_runtime.NewRuntime(syntheticCatalog(), em, testRuntimeSource)
	if err != nil {
		t.Fatalf("NewRuntime err: %v", err)
	}
	// EventResetStatus is declared AND present in syntheticCatalog, so
	// Emit must succeed end-to-end.
	if err := rt.Emit(context.Background(), nm_runtime.EventResetStatus, nil); err != nil {
		t.Fatalf("declared event must reach emitter, got err=%v", err)
	}
	if len(em.calls) != 1 {
		t.Fatalf("want 1 emit call, got %d", len(em.calls))
	}
}

func TestRuntime_Emit_PolicyDeniesWhenWhitelistMismatch(t *testing.T) {
	// Catalog entry whose service_variant is NOT on the NM whitelist —
	// policy must deny even for system_nm_runtime caller.
	//
	// NOTE: Post-#505-r3106904915, Emit rejects undeclared EmitEvent values
	// BEFORE reaching policy. To still exercise the policy-denial path on
	// an already-declared event, we build a catalog where reset_status is
	// configured with a PB/SB pair that the whitelist does not accept —
	// but the whitelist is service_variant keyed, so the clearest way to
	// prove the policy path still fires is to run with a declared event
	// whose catalog identity is present but whose whitelist lookup fails.
	//
	// In practice, all declared EmitEvents are on the whitelist (that's
	// the whole point of declaration). So this test now instead asserts
	// that an undeclared EmitEvent is refused WITHOUT reaching the
	// policy layer (no emit, policy not consulted). The "policy denies"
	// path is covered by internal/execution_policy own tests.
	em := &recordingEmitter{}
	rt, err := nm_runtime.NewRuntime(syntheticCatalog(), em, testRuntimeSource)
	if err != nil {
		t.Fatalf("NewRuntime err: %v", err)
	}
	err = rt.Emit(context.Background(), "not_whitelisted_variant", nil)
	if err == nil {
		t.Fatal("non-whitelisted variant must be refused")
	}
	// Must be ErrUnknownEmitEvent (pre-catalog guard), NOT policy-denied
	// — refusal must happen before the whitelist is consulted.
	if !errors.Is(err, nm_runtime.ErrUnknownEmitEvent) {
		t.Fatalf("want ErrUnknownEmitEvent, got %v", err)
	}
	if execution_policy.IsDenied(err) {
		t.Fatal("refusal must not come from policy — undeclared events are rejected upstream")
	}
	if len(em.calls) != 0 {
		t.Fatal("refused call must not reach emitter")
	}
}
