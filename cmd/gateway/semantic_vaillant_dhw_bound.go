package main

import (
	"strings"

	"github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	vaillantsystem "github.com/Project-Helianthus/helianthus-ebusreg/vaillant/system"
)

// registryControllerIdentity returns the EID/SW/HW the registry holds for the
// B524 controller address, without the API qualifier.
func (p *vaillantSemanticPoller) registryControllerIdentity(controller byte) vaillantsystem.B524ControllerIdentity {
	if p == nil || p.reg == nil || controller == 0 {
		return vaillantsystem.B524ControllerIdentity{}
	}
	var identity vaillantsystem.B524ControllerIdentity
	p.reg.IterateSnapshots(func(snap registry.DeviceEntrySnapshot) bool {
		if !ebusgateway.SnapshotContainsAddress(snap, controller) {
			return true
		}
		identity = vaillantsystem.B524ControllerIdentity{
			EID:         strings.ToUpper(strings.TrimSpace(snap.DeviceID)),
			SoftwareRaw: strings.TrimSpace(snap.SoftwareVersion),
			HardwareRaw: strings.TrimSpace(snap.HardwareVersion),
		}
		return false
	})
	return identity
}

// controllerB524Identity uses only the registry snapshot from discovery or
// imported scans. Existing B524 polling does not acquire the system-information
// values 0006h (api_version) / 0007h (api_revision), so API fields stay absent
// and profiles requiring them remain unmatched. Bound resolution never acquires
// or refreshes identity on the bus.
//
// A controller swap at the same address retains the old registry identity until
// registry metadata changes; validation must not try to detect or refresh it.
func (p *vaillantSemanticPoller) controllerB524Identity() vaillantsystem.B524ControllerIdentity {
	if p == nil {
		return vaillantsystem.B524ControllerIdentity{}
	}
	p.mu.Lock()
	controller := p.controller
	p.mu.Unlock()
	return p.registryControllerIdentity(controller)
}

func configBoundEquals(a, b *graphql.ConfigBound) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func mcpConfigBound(bound *graphql.ConfigBound) *mcp.ConfigBound {
	if bound == nil {
		return nil
	}
	return &mcp.ConfigBound{
		NarrowedOnly: bound.NarrowedOnly,
		MinC:         bound.MinC,
		MaxC:         bound.MaxC,
		Source:       bound.Source,
		Reason:       bound.Reason,
		Profile:      bound.Profile,
	}
}
