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
	rt := nm_runtime.NewRuntime(syntheticCatalog(), em)
	if err := rt.Emit(context.Background(), nm_runtime.EventResetStatus, []byte{0xAB}); err != nil {
		t.Fatalf("Emit reset status err: %v", err)
	}
	if len(em.calls) != 1 {
		t.Fatalf("want 1 emit call, got %d", len(em.calls))
	}
	c := em.calls[0]
	if c.src != rpc_source.Gateway {
		t.Fatalf("emit source = 0x%02X, want 0x71", c.src)
	}
	if c.pb != 0xFF || c.sb != 0x00 {
		t.Fatalf("emit pb/sb = 0x%02X/0x%02X, want FF/00", c.pb, c.sb)
	}
}

func TestRuntime_Emit_FailureBroadcast(t *testing.T) {
	em := &recordingEmitter{}
	rt := nm_runtime.NewRuntime(syntheticCatalog(), em)
	if err := rt.Emit(context.Background(), nm_runtime.EventFailure, []byte{0x01}); err != nil {
		t.Fatalf("Emit failure err: %v", err)
	}
	if len(em.calls) != 1 || em.calls[0].sb != 0x02 {
		t.Fatalf("want single FF 02 emit, got %+v", em.calls)
	}
}

func TestRuntime_Emit_RefusesIfCatalogMissing(t *testing.T) {
	em := &recordingEmitter{}
	rt := nm_runtime.NewRuntime(ebusstd.Catalog{Namespace: "ebus_standard"}, em)
	err := rt.Emit(context.Background(), nm_runtime.EventResetStatus, nil)
	if !errors.Is(err, nm_runtime.ErrNoCatalogEntry) {
		t.Fatalf("want ErrNoCatalogEntry, got %v", err)
	}
}

func TestRuntime_Emit_PolicyDeniesWhenWhitelistMismatch(t *testing.T) {
	// Catalog entry whose service_variant is NOT on the NM whitelist —
	// policy must deny even for system_nm_runtime caller.
	pb := uint8(0xFF)
	cat := ebusstd.Catalog{
		Namespace: "ebus_standard",
		Services: []ebusstd.Service{{
			PB:   &pb,
			Name: "NM",
			Commands: []ebusstd.Command{{
				ID:          "x",
				SafetyClass: ebusstd.SafetyBroadcast,
				Identity: ebusstd.IdentityKey{
					Namespace:             "ebus_standard",
					PB:                    u8(0xFF),
					SB:                    u8(0x00),
					ServiceVariant:        "not_whitelisted_variant",
					Version:               "v1.0-locked",
					TelegramClass:         ebusstd.TelegramClassBroadcast,
					Direction:             ebusstd.DirectionRequest,
					RequestOrResponseRole: ebusstd.RoleOriginator,
					BroadcastOrAddressed:  ebusstd.AddressedBroadcast,
				},
			}},
		}},
	}
	em := &recordingEmitter{}
	rt := nm_runtime.NewRuntime(cat, em)
	err := rt.Emit(context.Background(), "not_whitelisted_variant", nil)
	if err == nil {
		t.Fatal("non-whitelisted variant must be denied")
	}
	if !execution_policy.IsDenied(err) {
		t.Fatalf("want IsDenied(err), got %v", err)
	}
	if len(em.calls) != 0 {
		t.Fatal("denied call must not reach emitter")
	}
}
