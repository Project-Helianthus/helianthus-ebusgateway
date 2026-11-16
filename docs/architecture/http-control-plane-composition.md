# HTTP control-plane composition

`startHTTPServer` in `cmd/gateway/gateway_http_server.go` is the server-lifecycle
orchestrator. In order, it validates configuration, builds the Portal clients,
composes the providers, installs the MCP surface, registers the core routes,
builds and mounts the Portal surface, and starts the listener. It calls
`newHTTPControlPlaneProviders` once after validation and returns any
composition error before mounting routes or starting a listener. Its signature
is unchanged.

Each surface is built by named functions in its own file:

| Surface | Owner | Required inputs | Builds |
| --- | --- | --- | --- |
| MCP | `installGatewayHTTPMCPSurface` in `cmd/gateway/gateway_http_mcp_surface.go` | MCP server, gateway, GraphQL builder, configuration | Vaillant B503 tools with the eBUS driver lifecycle observer and the GraphQL provider sharing their session manager, then the responder capability, then the schedule and configuration writers |
| Portal | `newGatewayHTTPPortalOptions` in `cmd/gateway/gateway_http_portal_surface.go` | gateway, GraphQL builder, MCP server, Portal configuration runtime | `portal.Options` from `gatewayPortalRegistryDevices`, `gatewayPortalSemanticSnapshot`, `gatewayPortalProjectionDevices`, `gatewayPortalProjectionGraph`, `gatewayPortalBusObservability` and `logGatewayPortalRawModbusAudit` |

A surface with a missing required input returns an error and installs or builds
nothing; `startHTTPServer` returns that error before it starts a listener. When
the Portal surface is rejected, the Portal configuration writer is released as
the listener would release it on a startup failure. `startHTTPServer` keeps the
Portal configuration runtime selection, the shared semantic read binding, the
Portal and catalog mounts, and the listener. Route paths, handlers, metric names,
log texts and error texts are unchanged.

The private `httpControlPlaneProviders` owner in
`cmd/gateway/gateway_http_providers.go` creates the shared semantic snapshot
provider, installs the semantic operation route before constructing handlers,
and creates the semantic metric renderer. It registers the MCP server identity,
unconditional runtime-control tools, optional eeBUS provider and command router,
Modbus tools, optional leaf and synchronized evidence captures, and the status,
bus-observability, watch and semantic adapters in their existing order. It does
not mount routes, start services, open listeners or introduce configuration.

The returned handlers and MCP server use the same snapshot provider as the
returned metric renderer. The caller passes that provider to Portal. An absent
semantic hub continues to report unavailable. Optional native providers remain
optional; evidence capture requires its existing enablement and collaborators.
Registration performs no native acquisition and does not confer qualification.

The caller retains the route plan and listener lifecycle and delegates the
family-specific wiring, responder capability and schedule/config writers to the
MCP surface above.

In-memory characterization tests cover tool names and schemas, route availability,
optional registration combinations, unavailable providers, early failure,
duplicate registration and shared-provider identity. Source guards inspect each
owner separately and verify the caller delegates before mounting handlers.
`TestGatewayHTTPSurfaces...` tests reject each missing required input of both
surfaces and of `startHTTPServer`, and compare the Portal options built by the
named functions with the former inline composition for two fixed configurations.
This structural change affects runtime artifacts; offline characterization does
not establish binary neutrality, physical qualification or deployed acceptance.
Release, security and hardware evidence must be renewed for affected artifacts.
