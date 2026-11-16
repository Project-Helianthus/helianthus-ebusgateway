package main

import (
	"fmt"
	"log"

	ebusgateway "github.com/Project-Helianthus/helianthus-ebusgateway"
	"github.com/Project-Helianthus/helianthus-ebusgateway/graphql"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp"
	"github.com/Project-Helianthus/helianthus-ebusgateway/mcp/ebusstandard"
)

// gatewayHTTPMCPSurface holds the collaborators of the MCP surface that the
// HTTP server installs after provider composition and before any route is
// mounted. server, gateway, builder and cfg are required; the source
// provider, the eBUS driver controller and both writers are optional.
type gatewayHTTPMCPSurface struct {
	server         *mcp.Server
	gateway        *ebusgateway.Gateway
	builder        *graphql.Builder
	cfg            *ebusgateway.Config
	sourceProvider func() (byte, bool)
	ebusDriver     *ebusDriverController
	scheduleWriter mcp.ScheduleWriter
	configWriter   mcp.ConfigWriter
}

func (surface gatewayHTTPMCPSurface) validate() error {
	switch {
	case surface.server == nil:
		return fmt.Errorf("mcp server missing for http mcp surface")
	case surface.gateway == nil:
		return fmt.Errorf("gateway missing for http mcp surface")
	case surface.builder == nil:
		return fmt.Errorf("graphql builder missing for http mcp surface")
	case surface.cfg == nil:
		return fmt.Errorf("configuration missing for http mcp surface")
	}
	return nil
}

// installGatewayHTTPMCPSurface installs, in this order: the Vaillant B503
// tools with their driver lifecycle observer and the GraphQL provider that
// shares their session manager, the responder capability, then the schedule
// and configuration writers. Nothing is installed when a required input is
// missing.
func installGatewayHTTPMCPSurface(surface gatewayHTTPMCPSurface) error {
	if err := surface.validate(); err != nil {
		return err
	}
	// The B503 tools are listed by tools/list and their forbidden-surface
	// guards apply to the production build. MCP and GraphQL operate on one
	// session manager, so a live-monitor session has a single owner across
	// both surfaces.
	b503rt := installVaillantB503(surface.server, surface.gateway, surface.cfg, surface.sourceProvider)
	if b503rt != nil && surface.ebusDriver != nil {
		surface.ebusDriver.SetLifecycleObserver(b503rt)
	}
	if b503rt != nil {
		surface.builder.SetVaillantB503Provider(newB503GraphQLProvider(b503rt))
	}
	installGatewayResponderCapability(*surface.cfg, surface.gateway)
	if surface.scheduleWriter != nil {
		surface.server.SetScheduleWriter(surface.scheduleWriter)
	}
	if surface.configWriter != nil {
		surface.server.SetConfigWriter(surface.configWriter)
	}
	return nil
}

// installGatewayResponderCapability publishes the package-level responder
// capability before any MCP surface serves its first envelope; consumers treat
// its absence as fail-closed. The live transport instance decides whether the
// bus path can emit responder bytes, so a configured protocol whose active path
// cannot respond is downgraded. A transport protocol that maps to none of the
// published rows omits the capability entirely rather than widening the schema.
func installGatewayResponderCapability(cfg ebusgateway.Config, gateway *ebusgateway.Gateway) {
	if provider := buildResponderCapabilityProvider(cfg, gateway.Transport); provider != nil {
		ebusstandard.SetResponderCapabilityProvider(provider)
	} else {
		log.Printf("warning: meta.capabilities.responder omitted: transport protocol %q does not canonicalize to ENH/ENS/ebusd-tcp", cfg.TransportConfig.Protocol)
	}
}
