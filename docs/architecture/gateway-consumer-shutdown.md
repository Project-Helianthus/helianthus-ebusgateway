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
4. Snapshot the current listener and reconstructor under `surfaceMu`.
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
