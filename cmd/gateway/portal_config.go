package main

import (
	"context"
	"log"
	"net"
	"net/netip"
	"strconv"

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

func gatewayPortalConfigCallbacks(store *portalconfig.Store, cfg ebusgateway.Config) *portal.ConfigCallbacks {
	// Snapshot provisioned endpoints, not socket readiness or desired selections.
	plan := newHTTPControlPlaneRoutePlan(cfg, false)
	httpEndpoint := cfg.HTTPAddr
	host, port, err := net.SplitHostPort(httpEndpoint)
	explicitAddress := err == nil && host != ""
	if err == nil && host == "" {
		// Omitted HTTP hosts plan loopback; broader exposure requires an explicit IP.
		httpEndpoint = net.JoinHostPort("127.0.0.1", port)
	}
	httpBinding := gatewayPortalListenerBinding("http", httpEndpoint)
	bindings := portalconfig.ListenerAvailability{Local: httpBinding, LocalAddressExplicit: explicitAddress}
	if plan.graphqlPath != "" {
		bindings.GraphQL = httpBinding
	}
	if plan.mcpPath != "" {
		bindings.MCP = httpBinding
	}
	bindings.AdapterProxy = gatewayPortalListenerBinding("adapter_proxy", cfg.ProxyListenAddr)
	return &portal.ConfigCallbacks{
		Bindings: func(context.Context) portalconfig.ListenerAvailability { return bindings },
		Read:     func(context.Context) (portalconfig.Record, error) { return store.Read() },
		Update: func(_ context.Context, record portalconfig.Record) (portalconfig.Record, error) {
			return store.Update(record)
		},
	}
}

// gatewayPortalListenerBinding deliberately performs no DNS resolution or socket
// probing. Hostnames and unassigned ephemeral ports lack a concrete endpoint.
func gatewayPortalListenerBinding(owner, endpoint string) portalconfig.ListenerBinding {
	host, portText, err := net.SplitHostPort(endpoint)
	if err != nil {
		return portalconfig.ListenerBinding{}
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return portalconfig.ListenerBinding{}
	}
	address := netip.IPv6Unspecified() // Empty host may bind both address families.
	if host != "" {
		address, err = netip.ParseAddr(host)
		if err != nil {
			return portalconfig.ListenerBinding{}
		}
	}
	return portalconfig.ListenerBinding{Owner: owner, Address: address, Port: uint16(port), Available: true}
}
