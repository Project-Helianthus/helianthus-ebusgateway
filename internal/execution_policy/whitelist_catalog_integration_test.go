//go:build !tinygo
// +build !tinygo

package execution_policy_test

import (
	"testing"

	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/execution_policy"
	ebusstd "github.com/Project-Helianthus/helianthus-ebusreg/catalog/ebus_standard"
)

// TestWhitelist_AgainstEmbeddedCatalog is the catalog-grounded regression
// gate for issue #505 r3106832678. It loads the REAL embedded ebusreg
// catalog (no fixtures, no synthetic identities) and asserts that for every
// one of the 7 whitelist tuples there exists a catalog command whose full
// 14-axis identity matches AND that policy.Check allows it under
// caller_context=system_nm_runtime.
//
// Drift: any future change in catalog enum values for the 7 NM/sign-of-life
// commands will fail this test, forcing the whitelist to be re-aligned in
// the same PR as the catalog change.
func TestWhitelist_AgainstEmbeddedCatalog(t *testing.T) {
	cat := ebusstd.MustEmbeddedCatalog()

	type ref struct {
		pb, sb              uint8
		serviceVariant      string
		direction           ebusstd.Direction
		role                ebusstd.RequestOrResponseRole
		broadcastAddressing ebusstd.BroadcastOrAddressed
	}
	// Reference set — the 7 commands the M4 whitelist authorizes.
	// Each entry's tuple values mirror the canonical-plan whitelist.
	refs := []ref{
		{0xFF, 0x00, "reset_status", ebusstd.DirectionRequest, ebusstd.RoleOriginator, ebusstd.AddressedBroadcast},
		{0xFF, 0x02, "failure_message", ebusstd.DirectionRequest, ebusstd.RoleOriginator, ebusstd.AddressedBroadcast},
		{0xFF, 0x03, "net_status_query", ebusstd.DirectionResponse, ebusstd.RoleResponder, ebusstd.AddressedDirect},
		{0xFF, 0x04, "monitored_participants_query", ebusstd.DirectionResponse, ebusstd.RoleResponder, ebusstd.AddressedDirect},
		{0xFF, 0x05, "failed_nodes_query", ebusstd.DirectionResponse, ebusstd.RoleResponder, ebusstd.AddressedDirect},
		{0xFF, 0x06, "required_services_query", ebusstd.DirectionResponse, ebusstd.RoleResponder, ebusstd.AddressedDirect},
		{0x07, 0xFF, "sign_of_life", ebusstd.DirectionRequest, ebusstd.RoleOriginator, ebusstd.AddressedBroadcast},
	}

	if got, want := execution_policy.NMWhitelistSize(), len(refs); got != want {
		t.Fatalf("whitelist size = %d, want %d (catalog reference set)", got, want)
	}

	for _, r := range refs {
		r := r
		t.Run(r.serviceVariant, func(t *testing.T) {
			cmd, ok := findCatalogCommand(cat, r.pb, r.sb, r.serviceVariant, r.direction, r.role, r.broadcastAddressing)
			if !ok {
				t.Fatalf("catalog has no command pb=0x%02X sb=0x%02X variant=%q direction=%s role=%s addressing=%s",
					r.pb, r.sb, r.serviceVariant, r.direction, r.role, r.broadcastAddressing)
			}
			if err := execution_policy.Check(cmd, execution_policy.CallerSystemNMRuntime); err != nil {
				t.Fatalf("whitelist must allow catalog command %q (pb=0x%02X sb=0x%02X variant=%q) for system_nm_runtime, got %v",
					cmd.ID, r.pb, r.sb, r.serviceVariant, err)
			}
			// For non-read-only commands the whitelist branch is the only path
			// to allow; assert user_facing is denied. Read-only commands are
			// universally allowed by design and skip this check.
			if cmd.SafetyClass != ebusstd.SafetyReadOnlySafe && cmd.SafetyClass != ebusstd.SafetyReadOnlyBusLoad {
				if err := execution_policy.Check(cmd, execution_policy.CallerUserFacing); err == nil {
					t.Fatalf("user_facing must NOT allow whitelisted catalog command %q (class=%s)", cmd.ID, cmd.SafetyClass)
				}
			}
		})
	}
}

func findCatalogCommand(cat ebusstd.Catalog, pb, sb uint8, variant string,
	direction ebusstd.Direction, role ebusstd.RequestOrResponseRole,
	addressing ebusstd.BroadcastOrAddressed) (ebusstd.Command, bool) {
	for _, svc := range cat.Services {
		for _, cmd := range svc.Commands {
			id := cmd.Identity
			if id.PB == nil || id.SB == nil {
				continue
			}
			if *id.PB != pb || *id.SB != sb {
				continue
			}
			if id.ServiceVariant != variant {
				continue
			}
			if id.Direction != direction {
				continue
			}
			if id.RequestOrResponseRole != role {
				continue
			}
			if id.BroadcastOrAddressed != addressing {
				continue
			}
			return cmd, true
		}
	}
	return ebusstd.Command{}, false
}
