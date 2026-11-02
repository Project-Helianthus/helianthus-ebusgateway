# Thermal and Infrastructure semantic metrics on the scrape route

Status: gateway export binding for issue
[#1339](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1339).
Series names, units, label domains and the missing-value rules are owned by the
[bounded transport and semantic Prometheus coverage contract](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/deployment/prometheus-semantic-coverage.md);
this page records only how the gateway serves them. All MCP namespaces remain
pre-stable and nothing here is a stability promise.

## Route and ownership

The existing metrics route (`MetricsPath`, default `/metrics`) keeps its
transport exposition, status and content type unchanged. The bus observability
store still renders every transport and PV/Storage/EVSE family, and the
Thermal/Infrastructure families of `internal/semanticmetrics` follow after it.
No route, listener, collector or acquisition is added, and a scrape never
probes a device or publishes.

## One capture per scrape

The renderer reads the same `hubSemanticSnapshotProvider` instance that HTTP
composition gives GraphQL `semanticSnapshot`, MCP `semantic.v1.snapshot.get`
and the Portal shared-view callback. A scrape captures once: one hub read lock
over the distinct assets of the configured, non-retired bindings, in bytewise
order. A registry admits at most 16 slots per domain, so the selection stays
within the capture bound of 32 assets. Every value and availability sample of
that scrape comes from this capture and its view. The transport store renders
and releases its lock before the capture starts, so the sections do not wait on
each other.

## Binding registry

Only configured bindings produce series. Labels are limited to `slot` (0..15)
on value series and `slot`, `domain` and `field` on
`helianthus_semantic_field_available`; asset, source, binding, service and
dimension identifiers never become labels. A malformed registry fails gateway
startup instead of silently dropping series.

The contract requires slot assignments to be durably persisted before
publication, with retired slots reserved for the scrape target. No such store
exists yet, so production composition configures no binding: the route
declares the availability family without samples and performs no capture.
Exported Thermal and Infrastructure values therefore remain pending a
persisted registry and an exact candidate.

## Unavailable values

A missing process hub, a restoring hub, a failed capture, an unbuildable view,
an unvalidated registry or a retired slot withholds the affected values:
configured fields report availability `0` with no value sample. A failure
never reuses an earlier scrape or another read path. The gateway logs a
failure when it begins or changes, not on every scrape; an abandoned scrape is
not logged.

## Evidence

**Proven** by the `TestM38` tests in
[`cmd/gateway/semantic_metrics_runtime_test.go`](../cmd/gateway/semantic_metrics_runtime_test.go)
against a real hub and synthetic publications: the route registered by the
control plane serves the unchanged transport exposition followed by the full
32-field inventory; each scrape captures once, and a publication committed
after the capture appears only in the next scrape; withheld, stale, retired,
mixed-revision and missing-hub cases report availability `0` without values;
many declared hub assets do not widen the selection or the labels; the
production registry performs no capture. Production wiring of the shared
provider is checked from the `startHTTPServer` source.

**Unknown**: behavior with live devices, qualified bindings or a deployment
candidate. Those remain separate acceptance work.
