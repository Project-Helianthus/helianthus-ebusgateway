package execution_policy_test

import (
	"errors"
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/execution_policy"
	ebusstd "github.com/Project-Helianthus/helianthus-ebusreg/catalog/ebus_standard"
)

func u8(v uint8) *uint8 { return &v }

func cmd(id string, cls ebusstd.SafetyClass, id14 ebusstd.IdentityKey) ebusstd.Command {
	return ebusstd.Command{ID: id, SafetyClass: cls, Identity: id14}
}

func TestCheck_AllowsReadOnlySafe(t *testing.T) {
	c := cmd("x.read", ebusstd.SafetyReadOnlySafe, ebusstd.IdentityKey{})
	if err := execution_policy.Check(c, execution_policy.CallerUserFacing); err != nil {
		t.Fatalf("read_only_safe must be allowed for user_facing, got %v", err)
	}
}

func TestCheck_AllowsReadOnlyBusLoad(t *testing.T) {
	c := cmd("x.busread", ebusstd.SafetyReadOnlyBusLoad, ebusstd.IdentityKey{})
	if err := execution_policy.Check(c, execution_policy.CallerUserFacing); err != nil {
		t.Fatalf("read_only_bus_load must be allowed for user_facing, got %v", err)
	}
}

func TestCheck_DeniesMutatingForUserFacing(t *testing.T) {
	c := cmd("x.mut", ebusstd.SafetyMutating, ebusstd.IdentityKey{})
	err := execution_policy.Check(c, execution_policy.CallerUserFacing)
	if err == nil {
		t.Fatal("mutating must be denied for user_facing")
	}
	if !errors.Is(err, execution_policy.ErrSafetyClassDenied) {
		t.Fatalf("want errors.Is(err, ErrSafetyClassDenied), got %v", err)
	}
	// The wrapped error MUST also satisfy the provider-level sentinel per
	// 05-execution-safety.md parity requirement.
	if !errors.Is(err, ebusstd.ErrSafetyClassDenied) {
		t.Fatalf("want errors.Is(err, ebusstd.ErrSafetyClassDenied) for parity, got %v", err)
	}
}

func TestCheck_DeniesDestructiveBroadcastMemoryWrite(t *testing.T) {
	for _, cls := range []ebusstd.SafetyClass{
		ebusstd.SafetyDestructive,
		ebusstd.SafetyBroadcast,
		ebusstd.SafetyMemoryWrite,
	} {
		c := cmd("x", cls, ebusstd.IdentityKey{})
		if err := execution_policy.Check(c, execution_policy.CallerUserFacing); err == nil {
			t.Fatalf("class=%s must be denied for user_facing", cls)
		}
	}
}

func TestCheck_SystemNMRuntime_WhitelistsFF00Broadcast(t *testing.T) {
	c := cmd("nm.reset", ebusstd.SafetyBroadcast, ebusstd.IdentityKey{
		Namespace:             "ebus_standard",
		PB:                    u8(0xFF),
		SB:                    u8(0x00),
		TelegramClass:         ebusstd.TelegramClassBroadcast,
		Direction:             ebusstd.DirectionRequest,
		RequestOrResponseRole: "initiator_broadcast_emit",
		BroadcastOrAddressed:  ebusstd.AddressedBroadcast,
		AnswerPolicy:          "no-answer",
		LengthPrefixMode:      ebusstd.LengthPrefixNone,
		SelectorDecoder:       "none",
		ServiceVariant:        "nm_reset_status_broadcast",
		Version:               "v1.0-locked",
	})
	if err := execution_policy.Check(c, execution_policy.CallerSystemNMRuntime); err != nil {
		t.Fatalf("FF 00 nm_reset_status_broadcast must be allowed for system_nm_runtime, got %v", err)
	}
	// Same command under user_facing MUST be denied — whitelist does not widen.
	if err := execution_policy.Check(c, execution_policy.CallerUserFacing); err == nil {
		t.Fatal("user_facing must NOT inherit nm_runtime whitelist")
	}
}

func TestCheck_SystemNMRuntime_AdjacentVariantsDenied(t *testing.T) {
	// Wrong service_variant — must be denied even with matching PB/SB.
	c := cmd("nm.reset.typo", ebusstd.SafetyBroadcast, ebusstd.IdentityKey{
		PB:                    u8(0xFF),
		SB:                    u8(0x00),
		TelegramClass:         ebusstd.TelegramClassBroadcast,
		Direction:             ebusstd.DirectionRequest,
		RequestOrResponseRole: "initiator_broadcast_emit",
		BroadcastOrAddressed:  ebusstd.AddressedBroadcast,
		AnswerPolicy:          "no-answer",
		ServiceVariant:        "some_other_variant",
		Version:               "v1.0-locked",
	})
	if err := execution_policy.Check(c, execution_policy.CallerSystemNMRuntime); err == nil {
		t.Fatal("PB/SB-matching variant with different service_variant must be denied")
	}
}

func TestWhitelistSize_MatchesPlan(t *testing.T) {
	// 05-execution-safety.md lists exactly 7 entries in first-delivery whitelist.
	if got := execution_policy.NMWhitelistSize(); got != 7 {
		t.Fatalf("whitelist size = %d, want 7", got)
	}
}

func TestIsDenied_ClassifiesViaErrorsIs(t *testing.T) {
	c := cmd("x", ebusstd.SafetyMutating, ebusstd.IdentityKey{})
	err := execution_policy.Check(c, execution_policy.CallerUserFacing)
	if !execution_policy.IsDenied(err) {
		t.Fatal("IsDenied must be true for denied call")
	}
}
