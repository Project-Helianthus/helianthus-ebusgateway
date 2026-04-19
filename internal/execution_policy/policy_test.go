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

// ff00Reset is the exact full-14-tuple identity of the first whitelist
// entry. It is the fixture reused across positive-allow and
// adjacent-variant-denied regression tests. Any axis flipped from this
// reference MUST be denied (AD09 invariant).
//
// Axis literals match ebusreg@30aa69a catalog/ebus_standard/catalog.yaml
// for command ebus_standard.nm.reset_status.
func ff00Reset() ebusstd.IdentityKey {
	return ebusstd.IdentityKey{
		Namespace:                       "ebus_standard",
		PB:                              u8(0xFF),
		SB:                              u8(0x00),
		SelectorPath:                    "",
		TelegramClass:                   ebusstd.TelegramClassBroadcast,
		Direction:                       ebusstd.DirectionRequest,
		RequestOrResponseRole:           ebusstd.RoleOriginator,
		BroadcastOrAddressed:            ebusstd.AddressedBroadcast,
		AnswerPolicy:                    ebusstd.AnswerNone,
		LengthPrefixMode:                ebusstd.LengthPrefixFixed,
		SelectorDecoder:                 "none",
		ServiceVariant:                  "reset_status",
		TransportCapabilityRequirements: []string{"broadcast_send"},
		Version:                         "v1.0-locked",
	}
}

func TestCheck_SystemNMRuntime_WhitelistsFF00Broadcast(t *testing.T) {
	c := cmd("nm.reset", ebusstd.SafetyBroadcast, ff00Reset())
	if err := execution_policy.Check(c, execution_policy.CallerSystemNMRuntime); err != nil {
		t.Fatalf("FF 00 reset_status must be allowed for system_nm_runtime, got %v", err)
	}
	// Same command under user_facing MUST be denied — whitelist does not widen.
	if err := execution_policy.Check(c, execution_policy.CallerUserFacing); err == nil {
		t.Fatal("user_facing must NOT inherit nm_runtime whitelist")
	}
}

func TestCheck_SystemNMRuntime_AdjacentVariantsDenied(t *testing.T) {
	// Wrong service_variant — must be denied even with matching PB/SB.
	base := ff00Reset()
	base.ServiceVariant = "some_other_variant"
	c := cmd("nm.reset.typo", ebusstd.SafetyBroadcast, base)
	if err := execution_policy.Check(c, execution_policy.CallerSystemNMRuntime); err == nil {
		t.Fatal("PB/SB-matching variant with different service_variant must be denied")
	}
}

// TestCheck_SystemNMRuntime_AllAxesEnforced is the AD09-invariant regression
// test. For the FF 00 whitelist entry it constructs one adjacent variant
// per axis (each axis flipped away from the canonical full-match identity)
// and asserts Check() denies every variant. Complements the positive-match
// TestCheck_SystemNMRuntime_WhitelistsFF00Broadcast case.
func TestCheck_SystemNMRuntime_AllAxesEnforced(t *testing.T) {
	// Baseline positive allow must hold before we mutate axes.
	if err := execution_policy.Check(
		cmd("nm.reset", ebusstd.SafetyBroadcast, ff00Reset()),
		execution_policy.CallerSystemNMRuntime,
	); err != nil {
		t.Fatalf("baseline full-match must be allowed, got %v", err)
	}

	cases := []struct {
		axis   string
		mutate func(*ebusstd.IdentityKey)
	}{
		{"namespace", func(k *ebusstd.IdentityKey) { k.Namespace = "ebus_vendor" }},
		{"pb", func(k *ebusstd.IdentityKey) { k.PB = u8(0xFE) }},
		{"sb", func(k *ebusstd.IdentityKey) { k.SB = u8(0x01) }},
		{"selector_path", func(k *ebusstd.IdentityKey) { k.SelectorPath = "x" }},
		{"telegram_class", func(k *ebusstd.IdentityKey) { k.TelegramClass = ebusstd.TelegramClassAddressed }},
		{"direction", func(k *ebusstd.IdentityKey) { k.Direction = ebusstd.DirectionResponse }},
		{"request_or_response_role", func(k *ebusstd.IdentityKey) {
			k.RequestOrResponseRole = ebusstd.RoleResponder
		}},
		{"broadcast_or_addressed", func(k *ebusstd.IdentityKey) {
			k.BroadcastOrAddressed = ebusstd.AddressedDirect
		}},
		{"answer_policy", func(k *ebusstd.IdentityKey) { k.AnswerPolicy = ebusstd.AnswerRequired }},
		{"length_prefix_mode", func(k *ebusstd.IdentityKey) {
			k.LengthPrefixMode = ebusstd.LengthPrefixByte
		}},
		{"selector_decoder", func(k *ebusstd.IdentityKey) { k.SelectorDecoder = "b5_group" }},
		{"service_variant", func(k *ebusstd.IdentityKey) { k.ServiceVariant = "failure_message" }},
		{"transport_capability_requirements", func(k *ebusstd.IdentityKey) {
			k.TransportCapabilityRequirements = []string{"responder"}
		}},
		{"version", func(k *ebusstd.IdentityKey) { k.Version = "v2.0-locked" }},
	}

	for _, tc := range cases {
		t.Run(tc.axis, func(t *testing.T) {
			id := ff00Reset()
			tc.mutate(&id)
			err := execution_policy.Check(
				cmd("nm.reset.adj", ebusstd.SafetyBroadcast, id),
				execution_policy.CallerSystemNMRuntime,
			)
			if err == nil {
				t.Fatalf("AD09 violation: variant flipped on axis %q was allowed; must be denied",
					tc.axis)
			}
			if !errors.Is(err, execution_policy.ErrSafetyClassDenied) {
				t.Fatalf("axis %q: want ErrSafetyClassDenied, got %v", tc.axis, err)
			}
		})
	}
}

// TestCheck_SystemNMRuntime_EveryWhitelistEntryAllowed asserts every one of
// the 7 whitelist entries has a constructible full-identity command that
// passes Check(). This is the positive-coverage half of the AD09
// invariant — without it, a typo in a whitelist axis could silently make
// one of the 7 entries unreachable.
//
// All 7 fixtures are sourced from ebusreg@30aa69a
// catalog/ebus_standard/catalog.yaml.
func TestCheck_SystemNMRuntime_EveryWhitelistEntryAllowed(t *testing.T) {
	ids := allWhitelistIdentities()
	if got, want := len(ids), execution_policy.NMWhitelistSize(); got != want {
		t.Fatalf("fixture entries = %d, want %d (match whitelist)", got, want)
	}
	for _, id := range ids {
		id := id
		t.Run(id.ServiceVariant, func(t *testing.T) {
			if err := execution_policy.Check(
				cmd("nm."+id.ServiceVariant, ebusstd.SafetyBroadcast, id),
				execution_policy.CallerSystemNMRuntime,
			); err != nil {
				t.Fatalf("whitelist entry %q must be allowed, got %v", id.ServiceVariant, err)
			}
		})
	}
}

// allWhitelistIdentities returns the canonical full-14-tuple identities for
// each of the 7 whitelist entries, in whitelist order. Keep synchronized
// with internal/execution_policy/whitelist.go nmWhitelist AND with
// ebusreg@30aa69a catalog/ebus_standard/catalog.yaml.
func allWhitelistIdentities() []ebusstd.IdentityKey {
	originatorBroadcast := func(sb uint8, variant string, lpm ebusstd.LengthPrefixMode) ebusstd.IdentityKey {
		return ebusstd.IdentityKey{
			Namespace:                       "ebus_standard",
			PB:                              u8(0xFF),
			SB:                              u8(sb),
			SelectorPath:                    "",
			TelegramClass:                   ebusstd.TelegramClassBroadcast,
			Direction:                       ebusstd.DirectionRequest,
			RequestOrResponseRole:           ebusstd.RoleOriginator,
			BroadcastOrAddressed:            ebusstd.AddressedBroadcast,
			AnswerPolicy:                    ebusstd.AnswerNone,
			LengthPrefixMode:                lpm,
			SelectorDecoder:                 "none",
			ServiceVariant:                  variant,
			TransportCapabilityRequirements: []string{"broadcast_send"},
			Version:                         "v1.0-locked",
		}
	}
	responderReply := func(sb uint8, variant string, lpm ebusstd.LengthPrefixMode) ebusstd.IdentityKey {
		return ebusstd.IdentityKey{
			Namespace:                       "ebus_standard",
			PB:                              u8(0xFF),
			SB:                              u8(sb),
			SelectorPath:                    "",
			TelegramClass:                   ebusstd.TelegramClassAddressed,
			Direction:                       ebusstd.DirectionResponse,
			RequestOrResponseRole:           ebusstd.RoleResponder,
			BroadcastOrAddressed:            ebusstd.AddressedDirect,
			AnswerPolicy:                    ebusstd.AnswerRequired,
			LengthPrefixMode:                lpm,
			SelectorDecoder:                 "none",
			ServiceVariant:                  variant,
			TransportCapabilityRequirements: []string{"responder"},
			Version:                         "v1.0-locked",
		}
	}
	signOfLife := ebusstd.IdentityKey{
		Namespace:                       "ebus_standard",
		PB:                              u8(0x07),
		SB:                              u8(0xFF),
		SelectorPath:                    "",
		TelegramClass:                   ebusstd.TelegramClassBroadcast,
		Direction:                       ebusstd.DirectionRequest,
		RequestOrResponseRole:           ebusstd.RoleOriginator,
		BroadcastOrAddressed:            ebusstd.AddressedBroadcast,
		AnswerPolicy:                    ebusstd.AnswerNone,
		LengthPrefixMode:                ebusstd.LengthPrefixNone,
		SelectorDecoder:                 "none",
		ServiceVariant:                  "sign_of_life",
		TransportCapabilityRequirements: []string{"broadcast_send"},
		Version:                         "v1.0-locked",
	}
	return []ebusstd.IdentityKey{
		originatorBroadcast(0x00, "reset_status", ebusstd.LengthPrefixFixed),
		originatorBroadcast(0x02, "failure_message", ebusstd.LengthPrefixFixed),
		responderReply(0x03, "net_status_query", ebusstd.LengthPrefixFixed),
		responderReply(0x04, "monitored_participants_query", ebusstd.LengthPrefixByte),
		responderReply(0x05, "failed_nodes_query", ebusstd.LengthPrefixByte),
		responderReply(0x06, "required_services_query", ebusstd.LengthPrefixByte),
		signOfLife,
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
