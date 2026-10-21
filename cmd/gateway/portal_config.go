package main

import (
	"context"
	"log"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/portalconfig"
	"github.com/Project-Helianthus/helianthus-ebusgateway/internal/runtimecontrol"
	"github.com/Project-Helianthus/helianthus-ebusgateway/portal"
)

func openGatewayPortalConfig(cfg ebusgateway.Config, registry *runtimecontrol.Registry) *portalconfig.Store {
	initial := portalconfig.Record{Format: portalconfig.Format, Revision: "0", Drivers: []portalconfig.Driver{}}
	plan := newHTTPControlPlaneRoutePlan(cfg, false)
	constraints := portalconfig.Constraints{
		GraphQL: plan.graphqlPath != "", MCP: plan.mcpPath != "",
		AdapterProxy: cfg.ProxyListenAddr != "", Matter: !cfg.M2MGraphQL.Disabled(),
	}
	if registry != nil {
		for _, driver := range registry.List().Drivers {
			id := string(driver.DriverID)
			initial.Drivers = append(initial.Drivers, portalconfig.Driver{ID: id, Enabled: driver.Disposition == runtimecontrol.DispositionEnabled})
			constraints.Drivers = append(constraints.Drivers, id)
		}
	}
	// Open returns a usable error view even on cold-load failure. Keep that
	// view mounted and never replace corrupt or unavailable storage with defaults.
	store, err := portalconfig.Open(cfg.PortalConfigRoot, initial, constraints)
	if err != nil {
		log.Printf("portal configuration unavailable: %v", err)
	}
	return store
}

func gatewayPortalConfigCallbacks(store *portalconfig.Store) *portal.ConfigCallbacks {
	return &portal.ConfigCallbacks{
		Read: func(context.Context) (portalconfig.Record, error) { return store.Read() },
		Update: func(_ context.Context, record portalconfig.Record) (portalconfig.Record, error) {
			return store.Update(record)
		},
	}
}
