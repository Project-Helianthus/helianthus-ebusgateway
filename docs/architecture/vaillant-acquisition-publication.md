# Vaillant acquisition and publication owners

The Vaillant-family semantic planes have two owners in `cmd/gateway`. The
acquisition owner talks to the bus and keeps native state; the publication
owner maps native values to the GraphQL provider and broadcast hub. Neither
owner changes a published name, value, read order, interval, log text or
expvar name.

## Acquisition owner

`vaillantSemanticPoller`, declared in
[`semantic_vaillant.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/semantic_vaillant.go), reads,
refreshes, discovers and writes over the bus in the `semantic_vaillant_*.go`
files. It keeps the native zone, hot water, circuit, boiler, system, radio
device, FM5, solar and cylinder snapshots, the native qualification state and
the partial last-known-good values. Its configuration writers return the
native `vaillantConfigWriteResult` and its time-program writers the native
`vaillantTimeProgramWriteResult`.

It reaches the publication owner only through `vaillantSemanticPublisher`,
declared in
[`semantic_vaillant_publisher.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/semantic_vaillant_publisher.go).
Every method of that interface speaks native values: views and detached
snapshots built under the snapshot lock, the controller capability, native
schedule programs and adapter info, the energy register values, and native
readiness flags and counts of the published planes. A poller without a
publication owner, such as the registry-only classifier of the startup scan,
publishes nothing.

| Lock | Guards |
| --- | --- |
| `pollMu` | Serializes acquisition passes: every task of the poll schedule and every direct acquisition caller hold it for the whole pass, and a source rebind holds it so that a pass in flight never continues with the successor's source. It is the outermost lock and guards no field by itself. |
| `readMu` | Serializes the semantic bus exchanges that take it: the B509 and B524 reads, the B524 register probe, the B509 write and the B555 reads and writes. It is held only around building and sending one frame with its timeout context. |
| `mu` | Guards the native snapshot and qualification state and the selected source and companion. Views for the publication owner are built under it and handed over after it is released. |
| `b524WidthMismatchMu` | Guards the per-selector time of the last logged B524 width mismatch. |

## Publication owner

`vaillantSemanticPublication`, declared in
[`vaillant_semantic_publication.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/vaillant_semantic_publication.go),
holds the GraphQL provider, the broadcast hub and the semantic cache persister,
and no bus access. It maps the native views in the
`vaillant_semantic_publication_*.go` files and builds the poller with
`newVaillantSemanticPoller` and `startVaillantSemanticPolling`.

The writer adapters in
[`vaillant_semantic_publication_writes.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/vaillant_semantic_publication_writes.go)
gate the poller's writers behind the admitted source and map the native write
results to the GraphQL and MCP results with the same field names.

| Lock | Guards |
| --- | --- |
| `boilerPublishMu` | Orders boiler-status publication against the thermal withdrawal of the flow temperature. Each path holds it across its change of the native boiler snapshot and the provider's boiler status; it is taken before the poller's `mu` and guards no other state. |

## Guard

[`vaillant_acquisition_ownership_test.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/vaillant_acquisition_ownership_test.go)
parses the non-test sources of the package and fails when:

- a file that declares a `vaillantSemanticPoller` method, or any
  `semantic_vaillant*.go` file, imports the GraphQL, the MCP or the Portal
  package; a new acquisition file is covered without a list to update;
- a poller field has a type of those packages or a type declared in a
  publication file;
- `vaillantSemanticPublisher` is declared in a publication file or uses such a
  type in its methods;
- a `vaillant_semantic_publication*.go` file imports the ebusgo protocol or
  transport package.

The same test runs each check on synthetic sources that break one rule at a
time, so a check that stops looking at its input fails.
