# HTTP control-plane composition

`startHTTPServer` in `cmd/gateway/gateway_http_server.go` owns configuration
validation, Portal clients, route registration, listener startup and shutdown.
It calls `newHTTPControlPlaneProviders` once after validation and returns any
composition error before mounting routes or starting a listener.

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

The caller retains the later family-specific wiring, responder capability,
schedule/config writers, route plan and listener lifecycle. Lifecycle
decomposition remains separate work.

In-memory characterization tests cover tool names and schemas, route availability,
optional registration combinations, unavailable providers, early failure,
duplicate registration and shared-provider identity. Source guards inspect each
owner separately and verify the caller delegates before mounting handlers.
This structural change affects runtime artifacts; offline characterization does
not establish binary neutrality, physical qualification or deployed acceptance.
Release, security and hardware evidence must be renewed for affected artifacts.
