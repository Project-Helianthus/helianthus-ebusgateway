# Gateway composition owners

`runGatewayLifecycle` in
[gateway_run_lifecycle.go](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/gateway_run_lifecycle.go) is the
entry of the gateway process. It composes four owners, each with its own
contract: the process shell, the API composition, the retained eBUS consumers
and the eBUS generation owners. `startHTTPServer` in
[gateway_http_server.go](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/gateway_http_server.go) is the
server-lifecycle entry over two surface builders; its contract is stated in
[HTTP control-plane composition](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/docs/architecture/http-control-plane-composition.md).

This page states each owner's contract: required collaborators, defaults,
startup validation, failure isolation and shutdown order, followed by the
complete shutdown sequence. The decomposition is structural: the order of side
effects, log texts, metric and expvar names, configuration and command-line
flags are the ones of the former single function.

## Entry sequence

`runGatewayLifecycle` runs these steps in order and returns the first error:

1. Create the process shell and defer its shutdown sequence, which produces
   the process result.
2. Acquire the shell resources (`acquireResources`).
3. Build the Portal catalog source (`newGatewayPortalCatalogSource`) and the
   M2M runtime (`newProfileM2MRuntime`) as two top-level statements, and
   register the M2M release when an M2M runtime exists.
4. Acquire the eeBUS surfaces of the shell (`acquireEEBusSurfaces`).
5. Create the API composition (`newGatewayAPIComposition`) and compose the API
   (`compose`).
6. Create the retained eBUS consumers from the collaborators the API
   composition holds and the leaf capture state root
   (`newRetainedEBusConsumers`).
7. Serve the HTTP control plane and register the consumer shutdown (`serve`).
8. Open the semantic barrier on process exit.
9. Create the eBUS generation owners and their task runner, register the
   generation-task release and bind the tasks to the runtime control
   (`BindTasks`).
10. Wait for the process context to end and return; the deferred shutdown
    sequence then runs.

The HTTP control plane serves through stable fail-closed bindings before any
eBUS generation task is bound. Every post-start eBUS step (current-session
source validation and admission, the startup discovery scan, revalidation,
background scanning and passive promotion) runs as a task of the admitted
generation: once per admitted generation, including a later START from STOPPED
and a manager-driven replacement, cancelled and joined before any successor.

## Process shell

Owner: `gatewayProcessShell` in
[gateway_process_shell.go](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/gateway_process_shell.go).

- Collaborators: the process context and the gateway configuration, which the
  shell updates in place while it prepares the run configuration and the eBUS
  driver.
- Startup validation: `newGatewayProcessShell` rejects a missing context or
  configuration before any side effect and registers the endpoint redaction as
  the first release.
- Startup: `acquireResources` acquires, in order, the run configuration, the
  semantic hub preflight, the release checker, the synchronized evidence
  sidecar, the Portal contribution registry, the runtime state, the Portal
  configuration, the stale eeBUS operator socket clean-up, the managed
  sidecars, the eBUS driver, the runtime control and the retained EVSE owner.
  `acquireEEBusSurfaces` then creates the eeBUS adapter and admin handler and
  configures the synchronized evidence sidecar.
- Defaults: an absent synchronized evidence sidecar or retained EVSE owner
  registers no release; an unavailable eBUS control service or a driver that is
  not running is logged and is not a process failure.
- Failure isolation: the first failed acquisition stops the startup with its
  existing error text; the releases registered until then still run. A release
  that fails or panics does not prevent the earlier releases from running.
- Shutdown: every release joins one named sequence in acquisition order and
  runs last in, first out. A release either joins its error into the process
  result or logs it. The endpoint redaction runs last, over the complete
  result.

## API composition

Owner: `gatewayAPIComposition` in
[gateway_api_composition.go](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/gateway_api_composition.go).

- Collaborators: the process context, the process shell after
  `acquireResources` and `acquireEEBusSurfaces` (runtime state, semantic
  publication hub, release checker, managed sidecar bindings, eBUS driver,
  runtime control, eeBUS adapter and admin handler), the run configuration the
  shell prepared, and the Portal catalog source. `serve` also requires the
  retained eBUS consumers.
- Startup validation: `newGatewayAPIComposition` rejects each missing
  collaborator, and a run configuration other than the shell's, with its own
  error before any step runs; `serve` rejects missing retained consumers before
  it starts anything.
- Startup: `compose` builds, in order, the startup source selection with its
  delayed artifact emission; the observe-first observers with the transport
  watch and the semantic metrics and adapter multiplexer providers; the gateway
  (created, then started) with its v8 rollout observability; the static seed;
  the GraphQL builder with the live admitted source; the address-table inserter
  with its runtime-state observer; the broadcast hub on the gateway router
  plane; the semantic runtime with the eeBUS promoted semantic provider and the
  status provider; and the late-bound writers and watch providers, after which
  the builder starts. `serve` registers the control-plane context, starts the
  HTTP control plane through `startHTTPServerFn` and registers the consumer
  shutdown. Neither waits for an eBUS generation.
- Defaults: without bus observability no transport watch is registered and the
  observability providers stay unset; the static seed is applied only when it
  is enabled; the mutable eBUS surfaces (schedule, configuration and GraphQL
  writers, watch providers) are late-bound and fail closed until a generation
  admits a source; an empty HTTP address starts no listener, and the consumer
  shutdown then has no HTTP drain.
- Failure isolation: the first failing step returns its error unchanged and the
  releases registered until then still run. A gateway close failure is logged
  and leaves the process result unchanged. The address-table inserter reads the
  admitted source live from the builder, so a source admitted later is never
  mistaken for a third-party initiator.
- Shutdown: the releases join the shutdown sequence in the order final
  source-selection artifact, transport watch, gateway, control-plane context,
  consumers. They run in reverse after the generation tasks have joined.

## Retained eBUS consumers

Owner: `retainedEBusConsumers` in
[gateway_retained_consumers.go](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/gateway_retained_consumers.go);
the full contract is in
[gateway consumer shutdown](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/docs/architecture/gateway-consumer-shutdown.md#retained-ebus-consumers).

- Collaborators: the gateway, GraphQL builder, semantic runtime, semantic
  publication hub, broadcast hub, runtime control, address-table inserter,
  eeBUS runtime adapter, live admitted-source authority and the five late-bound
  writers and watch providers are required; bus observability and the
  deduplicator are optional.
- Startup validation: `newRetainedEBusConsumers` rejects a missing collaborator
  before the HTTP control plane starts. It starts nothing.
- Defaults: every consumer runs on the process context and is created once, by
  the first admitted generation, then survives stopped periods with truthful
  unavailability.
- Failure isolation: the process surface records readiness only after every
  fallible step succeeded; a failed step is retried by the next admitted
  generation. A generation publishes its reconstructor and admitted source only
  through the generation fence.
- Shutdown: the owner registers no release of its own. The consumer shutdown
  reads the listener and reconstructor through the owner after the generation
  tasks have joined and closes them.

## eBUS generation owners

Owner: `ebusGenerationOwners` in
[gateway_ebus_generation_owners.go](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/gateway_ebus_generation_owners.go).

- Collaborators: the process context, the runtime-state manager, the gateway,
  the GraphQL builder, the runtime control, the retained consumers and the
  source-selection metrics and artifact builder are required; bus
  observability, the adapter classifier and the loaded runtime state are
  optional.
- Startup validation: `newEBusGenerationOwners` rejects a missing collaborator;
  `newEBusGenerationTaskRunner` rejects an invalid task graph. Neither starts a
  task.
- Defaults: the four generation tasks (source admission, semantic surface,
  startup scan and passive observe-first) run once per admitted generation;
  only the newest admitted generation keeps its startup state.
- Failure isolation: every effect a task installs goes through the generation
  scope, every goroutine a task starts is joined with the generation, and a
  withdrawn generation installs nothing.
- Shutdown: `runGatewayLifecycle` registers the generation-task release last,
  so it runs first: it closes the runtime-control admission, which fences and
  joins the generation tasks, before any process consumer is torn down.

## Shutdown sequence

Releases are registered in this order; the sequence runs them in the reverse
order. Optional releases are registered only when their resource exists.

| Registration | Release | Registered by | Effect |
| --- | --- | --- | --- |
| 1 | 14 | process shell | endpoint redaction of the process result |
| 2 | 13 | process shell | close the synchronized evidence sidecar (optional) |
| 3 | 12 | process shell | stop the runtime state |
| 4 | 11 | process shell | close the Portal configuration |
| 5 | 10 | process shell | shut down the eBUS driver, joining the stop errors of the managed sidecars |
| 6 | 9 | process shell | close the runtime-control admission and native resources |
| 7 | 8 | process shell | close the retained EVSE owner (optional) |
| 8 | 7 | `runGatewayLifecycle` | close the M2M sidecar (optional) |
| 9 | 6 | API composition | write the final source-selection artifact |
| 10 | 5 | API composition | stop the transport watch (with bus observability) |
| 11 | 4 | API composition | close the gateway |
| 12 | 3 | API composition | cancel the control-plane context |
| 13 | 2 | API composition | consumer shutdown: transport watch, retirement, HTTP drain, listener, deduplicator, reconstructor, bus observability, mDNS |
| 14 | 1 | `runGatewayLifecycle` | close the runtime-control admission and join the generation tasks |

The consumer shutdown steps are detailed in
[gateway consumer shutdown](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/docs/architecture/gateway-consumer-shutdown.md).

## Size budget and tests

`TestGatewayCompositionSizeBudget` parses the two entry files and every owner
file of this decomposition (`gateway_process_shell.go`,
`gateway_api_composition.go`, `gateway_lifecycle_observability.go`,
`gateway_retained_consumers.go`, `gateway_ebus_generation_owners.go`,
`gateway_http_mcp_surface.go`, `gateway_http_portal_surface.go`) and fails on a
function over 150 lines or a file over 1,000 lines or 60 functions.

`TestGatewayProcessShellShutdownSequenceThroughLifecycle` asserts the complete
trace of shell acquisitions, API composition steps, registrations and releases
for a full process; `TestGatewayProcessShellReleasesOnlyComposedAPIOnFailure`
asserts that a failed API composition step runs only the releases registered
before it; `TestGatewayAPICompositionRejectsMissingWiring` rejects each missing
collaborator. `TestGatewayConsumerShutdownLifecycleWiring` and
`TestMainStartsControlPlaneBeforeWarmupAndRetiresItInLIFOOrder` assert that the
HTTP control plane starts before the generation tasks are bound and is retired
after they have joined.

These tests prove offline ordering. They do not establish binary neutrality,
physical qualification or deployed acceptance; the gateway binary changes, so
release, security and hardware evidence must be renewed for the affected
artifacts.
