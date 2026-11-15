# Gateway consumer shutdown

The private `gatewayConsumerShutdown` in
[the shutdown owner](../../cmd/gateway/gateway_consumer_shutdown.go) owns the
adjacent HTTP and retained native-consumer teardown previously held by
[the lifecycle shell](../../cmd/gateway/gateway_run_lifecycle.go). It is a bounded
part of the lifecycle decomposition; generation owners, startup, admission,
source selection and driver registration remain with their existing owners.

The shell registers the consumer shutdown after successful HTTP acquisition.
Its later generation-admission defer closes admission and joins generation
tasks first. The shutdown delegate then performs these steps synchronously:

1. Stop and join the transport watch.
2. Publish managed transport retirement.
3. Stop HTTP admission and drain admitted requests using the existing HTTP
   shutdown helper and its unchanged timeout and close fallback.
4. Snapshot the current listener and reconstructor through the retained
   owner's `surface` method, under its surface lock.
5. Close the broadcast listener.
6. Close the deduplicator.
7. Close the passive reconstructor.
8. Close bus observability.
9. Close the mDNS advertiser.

The shell cancels the control-plane context after the delegate returns. The
control plane remains independent of parent cancellation until then. Other
resource defers retain their acquisition positions. Listener and reconstructor
are resolved during cleanup, so installation by a later generation is retained.
Optional absent resources are ignored. Each HTTP or consumer close error keeps
its existing log label; failures do not prevent subsequent closes and do not
join the lifecycle result. The shell calls the delegate once.

[Fake-only characterization tests](../../cmd/gateway/gateway_consumer_shutdown_test.go)
assert exact callback order and counts, each close failure, nil resources,
late installation, canceled parent behavior and drain completion before consumer
release. Lifecycle source assertions also guard the production delegate wiring
and the generation-join defer position. These checks prove offline ordering,
not physical qualification, deployed acceptance or binary neutrality.

This structural change affects the gateway runtime and binaries that contain
it. Release, security and hardware evidence must be renewed for those artifacts;
full repository CI, applicable transport/conformance gates and independent
review remain shipping requirements. Protocol contracts, native evidence,
last-known-good fields, fail-closed admission and MCP/GraphQL contracts are
unchanged by this ownership extraction.

## Retained eBUS consumers

The consumers that live across eBUS generations have one owner, the private
`retainedEBusConsumers` in
[the retained-consumer owner](../../cmd/gateway/gateway_retained_consumers.go):
the semantic poller and its late-bound writers and watch providers, the
passive reconstructor, the broadcast listener, the passive discovery promoter,
the semantic startup barrier, the address-table inserter subscription and the
process-level admitted source.

- Collaborators: the gateway, GraphQL builder, semantic runtime, semantic
  publication hub, broadcast hub, runtime control, address-table inserter,
  eeBUS runtime adapter, live admitted-source authority and the five
  late-bound writers and watch providers are required. The constructor
  rejects a missing one before the HTTP control plane starts. Bus
  observability and the deduplicator are optional.
- Context: every retained consumer runs on the process context, never on a
  generation context, so a stopped interval does not tear it down.
- Generation access: the generation tasks of the lifecycle shell reach the
  consumers only through the owner's methods. A reconstructor created by a
  generation and the admitted source are published through the generation
  fence; a fenced generation publishes nothing and closes the reconstructor it
  created. Every admitted generation rebinds the retained poller to its own
  initiator and companion (`rebindPollerSource`).
- Failure isolation: the process surface is created in ordered steps (rebind
  the poller source, start the poller, bind the GraphQL writers, bind the watch
  observer, install the capture, attach the passive shadow lane, create the
  promoter, bind enrichment and the MCP writers, record readiness). Readiness
  is recorded only after every fallible step succeeded; a failed step is
  retried by the next admitted generation and completed steps are not
  repeated.
- The semantic barrier closes at most once: on the admission outcome that
  opens it, or on process exit.
- Teardown: the owner does not tear down a published consumer. This consumer
  shutdown reads the listener and reconstructor through the owner after the
  generation tasks joined, and closes them in the order above.

[Unit tests](../../cmd/gateway/gateway_retained_consumers_test.go) cover the
rejected wiring, the once-only inserter subscription after admission, the
once-only barrier close and the order of the process-surface steps.
