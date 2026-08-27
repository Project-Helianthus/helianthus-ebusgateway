package main

import (
	"log"
	"time"

	"github.com/Project-Helianthus/helianthus-ebusreg/registry"
	"github.com/Project-Helianthus/helianthus-ebusreg/vaillant/productids"
)

func applyStaticSeedTable(reg *registry.DeviceRegistry) {
	seeds := productids.LoadSeedTable(true)
	if len(seeds) == 0 {
		return
	}
	now := time.Now()
	count := 0
	for _, seed := range seeds {
		for _, addr := range seed.Addresses {
			role := registry.SlotRoleUnknown
			switch addr.Role {
			case "initiator":
				role = registry.SlotRoleMaster
			case "target":
				role = registry.SlotRoleSlave
			}
			info := registry.DeviceInfo{
				Address:      addr.Addr,
				Manufacturer: seed.Manufacturer,
				DeviceID:     seed.DeviceID,
			}
			reg.RegisterStaticSeed(info, role, now)
			count++
		}
	}
	log.Printf("static seed table: planted %d address(es) across %d device(s) at startup (source=productids.LoadSeedTable, label=static_seed/candidate)", count, len(seeds))
}

// initRuntimeStateManager constructs and starts the runtime-state Manager,
// wiring AD08 eager-persist for the instance-guid CLI flag (when present)
// and exposing the loaded state for hint extraction.
//
// On Manager.Load failure (missing / corrupt file), Manager.Load returns an
// empty state and the gateway continues without a hint — the cache is a
// best-effort optimisation, not a startup requirement (AD11 + M2 spec).
