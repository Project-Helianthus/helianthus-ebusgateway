# eBUS raw-evidence ownership inventory

Status: current-code inventory and later migration outline for
[issue #1097](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1097).
Refs [parent #1021](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1021);
this partial slice does not close the parent or implement G5.

## Evidence boundary and entry points

The [runtime/driver contract](runtime-driver-provider-contract-v1.md) assigns
native evidence to the driver. That is the intended destination, not a claim
that current construction already follows it. The supplied G5 operator decisions
D1–D5 and J01–J17 are planned lifecycle work. This inventory adds no lifecycle
API, recovery design, protocol interpretation, or namespace. In particular,
J12/J14/J15 require process-scoped retained S7 evidence with capture paused
while unadmitted; G5 itself does not relocate or clear these stores. The later
#1021 move must preserve that bridge contract.

Current entry is [main.go](../../cmd/gateway/main.go): `main` calls `run`, which
delegates to `runGatewayLifecycle` in
[gateway_run_lifecycle.go](../../cmd/gateway/gateway_run_lifecycle.go).
The read-only current-main reference still contains the orchestration and
observer factory in `main.go`; in this checkout those bodies are respectively
in `gateway_run_lifecycle.go` and
[gateway_admission.go](../../cmd/gateway/gateway_admission.go).
Use these local equivalents for implementation planning.

`runGatewayLifecycle` prepares/starts the managed eBUS transport, then calls
`wireObserveFirstObservers`, constructs the gateway bus, installs retained-view
providers, and starts the HTTP control plane before waiting for the first
RUNNING generation when startup scanning is enabled. Driver unavailability can
therefore leave the shell available. This does not establish the proposed G5
control-state boot/recovery contract. The native `ebusDriverRuntime` in
[ebus_driver_runtime.go](../../cmd/gateway/ebus_driver_runtime.go) owns transport
generations and withdrawal fencing, not the following store constructors.

## Current stores (pre-G5 ownership)

Links identify implementation files; symbols name the constructor, mutation,
and read paths in those files. Retention below describes current code, not a
new STOP contract. In-memory data has no implied process-restart durability.

| Store / current owner and constructor | Mutation entry points | Retention / lifetime | Readers and public projection |
| --- | --- | --- | --- |
| Raw identification exchanges: process observer chain; `wireObserveFirstObservers` constructs `NewIdentificationEvidenceStore` in [identification_evidence.go](../../identification_evidence.go). [identification_evidence_wiring.go](../../cmd/gateway/identification_evidence_wiring.go) publishes an atomic process pointer. | `OnBusEvent` admits only successful complete directed identification exchanges; failed/response-less attempts leave retained exchanges intact. It copies request and response bytes. First event fixes observer session; each admitted exchange advances sequence. | Eight exchanges per target, at most 256 targets; oldest exchange replaced per target. Session and sequence belong to this store instance, not DriverManager generation. No TTL, disk persistence, clear, or lifecycle STOP hook. | `Snapshot` deep-copies in address/sequence order. The wiring adapters' `IdentificationEvidence` feeds [MCP](../../mcp/identification_evidence.go) and [GraphQL](../../graphql/identification_evidence.go); raw/undecoded/qualification-none. CRC/ACK are absent from observer input, not recoverable evidence. |
| Recent message ring: `BusObservabilityStore`, constructed by `NewBusObservabilityStore` in [bus_observability_store.go](../../bus_observability_store.go), owned by process composition. | `OnBusEvent` and `OnPassiveClassifiedEvent` reach active/passive recorders and `pushRecentLocked`. | Configured `ObserveFirstRecentMessageCapacity`; overwrite oldest. Metadata/length records, not a complete byte archive. `Close` cancels subscription worker and joins; it does not empty the ring. | `RecentMessages` and [Snapshot](../../bus_observability_api.go); [bus_observability_provider.go](../../cmd/gateway/bus_observability_provider.go) maps retained messages to [MCP bus tools](../../mcp/bus.go) and [GraphQL bus fields](../../graphql/bus_observability.go). |
| Protocol specimen rings: same `BusObservabilityStore` constructor/owner. | `OnPassiveClassifiedEvent` → `pushSpecimenLocked`; equal payload/outcome keys increment count and update last seen. Payload bytes copied on insertion. | 32 specimens per family, 64 families maximum; per-family overwrite oldest unique specimen, refuse additional families at cap. First/last-seen and count retained; no TTL or disk persistence. | `ProtocolSpecimens` in [bus_observability_api.go](../../bus_observability_api.go), through MCP adapter `ProtocolSpecimens`. No equivalent GraphQL specimen field in the current parity mapping. |
| Periodicity and observability aggregates: same `BusObservabilityStore`; includes frame/error/byte series, address buckets, busy window, warmup and watch-efficiency state. These are derived observations, not raw payload archives. | Active/passive recorders; `recordPeriodicityLocked`, `recordBusyLocked`, `ObserveWatchRead`, `ObserveWatchDirectApply`; composition also supplies admission and other transport status. | Periodicity uses configured `ObserveFirstPeriodicityCapacity`, stale TTL and LRU eviction. Series/address budgets bound labels; busy segments are windowed. Counters otherwise accumulate for store lifetime. `Snapshot` and `RenderPrometheus` can refresh derived state/expire periodicity even without new capture. | [Snapshot and summary](../../bus_observability_api.go), MCP/GraphQL adapters above, `MetricsHandler`/`RenderPrometheus`. Not all aggregates are in the existing parity mapping. |
| Discovery `EvidenceBuffer`: nested in `BusObservabilityStore`, constructed with `NewEvidenceBuffer` in [evidence_buffer.go](../../evidence_buffer.go). | `recordEvidenceFromEventLocked` records passive address evidence; [PassiveDiscoveryPromoter](../../passive_discovery_promoter.go) reads `PromotedAddresses` and can `Demote` after failed confirmation. | Runtime construction uses 128 address entries with baseline-protected LRU; records retain last-observed and observation counts, not frame bytes. No TTL/persistence; `Demote` clears promotion/counts, not the address. | `EvidenceBuffer` accessor passed by process composition to `NewPassiveDiscoveryPromoter`; directed discovery/registry enrichment, not direct raw-byte export. Promotion here is discovery candidacy, not semantic qualification or a Proven protocol claim. |
| Passive reconstruction state and fanout queues: process composition starts `StartPassiveTransactionReconstructorWithTransport` in [passive_transaction_reconstructor.go](../../passive_transaction_reconstructor.go). | `OnPassiveTapEvent` assembles/classifies; watchdog, discontinuity/reset and shutdown paths retire partial transactions. | Partial request/response state plus bounded subscriber queues; no historical frame archive. `Close` cancels tap, publishes shutdown events and closes subscribers once. | Observability store, deduplicator, admission/discovery consumers; aggregate `Snapshot` reaches bus summary. These queues must not be confused with retained specimen/exchange rings. |
| Active/passive fingerprint and pending-event windows: `NewActivePassiveDeduplicator` in [active_passive_deduplicator.go](../../active_passive_deduplicator.go), constructed by observer wiring when broadcast listening is enabled. | `OnBusEvent`, `OnPassiveClassifiedEvent`, expiry and discontinuity handling. | Configured active/pending budgets, dedup windows and grace expiry. Discontinuity advances its own epoch and flushes windows; that epoch is not a G5 source epoch. `Close` cancels/joins and closes subscribers. | Watch observer fallback and adjudicated passive consumers; feeds shadow/semantic paths. Current windows are transient admission machinery, not durable raw-evidence history. |
| Raw watch-value `ShadowCache`: semantic poller owns `NewShadowCache`, constructed by `newVaillantSemanticPoller` in [semantic_vaillant_snapshots.go](../../cmd/gateway/semantic_vaillant_snapshots.go). | `Write`/`Invalidate` in [shadow_cache.go](../../shadow_cache.go); active confirmed writes from [semantic_read_scheduler.go](../../semantic_read_scheduler.go), passive writes/invalidation from [semantic_vaillant_passive.go](../../cmd/gateway/semantic_vaillant_passive.go), operator invalidation in [semantic_vaillant.go](../../cmd/gateway/semantic_vaillant.go). | Bounded entries/pinned budgets; per-key generations, observed/expiry times, source and confidence. Stale/conflicting/generation-obsolete writes rejected. Tombstone retention/compaction differs from capture retention. `Close` concerns compactor shutdown; no proof here of a G5-managed compactor lifecycle. | Scheduler `Lookup`/eligibility and poller consumption; detached watch summaries through [watch_summary_provider.go](../../cmd/gateway/watch_summary_provider.go). No permission to serve stale values as current or to move semantic ownership into this cache. |
| Synchronized evidence `FileStore`: gateway recorder, opened by `openSynchronizedEvidenceRuntime` in [synchronized_evidence_runtime.go](../../cmd/gateway/synchronized_evidence_runtime.go), implementation [store.go](../../internal/syncevidence/store.go). | `Capture` reserves quota, recorder acquires, reservation `Publish` persists; store also exposes `Drop`/`EnforceRetention` and open-time recovery. | Explicit config retention/quota, durable bundles with journals/locking; recovery applies retention. `Close` releases resources, not deletion of bundles. Generic recorder is disabled by default; current eBUS source registrations use `unavailableEBusEvidenceReader`, not a live bridge to the above rings. | Generic runtime returns bundle bytes/ID to its caller; no generic public capture call is wired by `runGatewayLifecycle`. The separate [one-shot runtime](../../cmd/gateway/synchronized_evidence_runtime.go) and [executor](../../internal/syncevidence/oneshot.go) open the same storage implementation per operation with their own fixed quota/retention; [MCP synchronized evidence](../../mcp/synchronized_evidence.go), registered in [gateway_http_server.go](../../cmd/gateway/gateway_http_server.go), is a separate control surface. This mixed-source facility is not wholly an eBUS-driver store. |

## STOP, retained age and provenance

Current `ebusDriverRuntime.Stop` fences withdrawal, clears its current transport
generation pointer and stops the transport runtime. `Replace` fences before
replacement. Neither method constructs, clears, persists or swaps these stores.
Process teardown separately closes listener, deduplicator, reconstructor and
observability worker after HTTP admission/drain. That teardown is not evidence
of an implemented public G5 STOP/START transaction. In particular, observability
`Close` leaves records but retains an attachment context: treating a closed store
as automatically restartable would be incorrect.

Pause means no further admitted capture, not destruction. Existing observation
times, specimen first/last seen, exchange session/sequence and raw bytes must
remain historical facts; a STOP timestamp or a later read cannot replace capture
time or imply qualification. Periodicity may age out on a read, and availability
may degrade as time passes. Thus a retained view need not be byte-identical in
all derived status fields while stopped. Never reset age by serving a snapshot.

| G5 decision (planned) | Constraint on later ownership migration |
| --- | --- |
| D1 | RESTART and START-after-STOP allocate a successor epoch after retirement/close proof. Retained old observer session IDs and per-key/cache epochs are not evidence of fresh source admission. Resolve their explicit mapping before enabling resumed writers. |
| D2 | One-time RUNNING seed only when control state is absent; disabled configuration wins. Corrupt/unreadable/unsupported control state keeps shell up and eBUS FAILED pending explicit repair. Retained raw data or a runtime-state hint cannot reopen acquisition. |
| D3 | START/STOP are idempotent by desired state; RESTART executes. Accepted desired state/revision is separate from observed completion. No lifecycle receipts, idempotency keys or completed-operation ledger; this does not delete native evidence or D5 recovery material. |
| D4 | No G5 principal/authority checks before 0.9; ordinary MCP control is planned. Preserve unrelated native-write/invoke/session safeguards. Raw evidence ownership conveys no write authority. |
| D5 | Gateway service writes ahead and uses gateway-owned semhub export/restore hooks for service-routed state. This does not make raw rings durable, invent a lifecycle-only eBUS asset, or redesign SemReg recovery. Restored history cannot authorize current capture; unresolved recovery/admission fails closed. |

Evidence grades Proven/Hypothesis/Unknown describe support for a claim;
qualification/admission is a separate axis. Raw capture and discovery promotion
alone establish neither. Public native contracts remain authoritative, including
[runtime-state hint versus current admission](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/runtime-state.md).
The public [native identification contract](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/ebus_standard/03-identification.md)
requires preservation of raw bytes and diagnostics. Byte layouts in the table
describe current observer storage, not new wire or decoding requirements.

## Intended post-G5 destination and unresolved edges

The eBUS driver should own creation and mutation of its identification,
message/specimen, periodicity, discovery and passive-window state; gateway shell
consumers retain detached read access across unavailability. The shadow's native
acquisition/value ownership must be split from semantic publication by its
existing owner before relocation. Mixed-source synchronized storage and durable
runtime-state persistence remain gateway facilities; only an accepted eBUS
acquisition adapter would move behind the driver. This is an outline, not an
accepted descriptor evidence hook (J07 reserves/rejects such hooks).

Unresolved edges blocking implementation:

- Retained-store lifetime across explicit epoch succession versus transport
  retry; observer sessions currently identify store instances, not generations.
  Define fencing for queued/late callbacks without relabeling old captures.
- The process-global identification pointer needs a driver-owned provider seam;
  simply publishing a new empty store would hide old evidence. Decide retention
  across successors and empty/unavailable projections with the lifecycle owner.
- Observability mixes eBUS data with gateway-owned Modbus/eeBUS lifecycle and
  semantic metric callbacks. Moving the whole object would lose shell diagnostics
  for other protocols; split those responsibilities before moving construction.
- Reconstructor/dedup subscription teardown and retained-store worker restart
  need separate ownership. Old queues must not feed new-generation shadow state.
- Shadow compactor start/close wiring and cache-generation/source-epoch mapping
  need a dedicated lifecycle trace; current constructor ownership is established,
  but G5 coverage is Unknown. Preserve partial last-known-good fields under
  existing identity/age rules rather than replacing a valid snapshot wholesale.
- B503 has a separate session owner and request-time history reads in
  [vaillant_b503_wiring.go](../../cmd/gateway/vaillant_b503_wiring.go) and
  [mcp/vaillant_b503.go](../../mcp/vaillant_b503.go); it is not an additional
  generic retained raw ring. Its existing activation/withdrawal fence must remain
  intact. Native registry evidence, semantic caches and source-selection/runtime
  state are adjacent owners, not authorization to fold them into this migration.

## Minimal later migration outline and checks

| Later file group | Minimal responsibility / required evidence |
| --- | --- |
| `gateway_admission.go`, `ebus_driver_runtime.go`, `gateway_run_lifecycle.go` (linked above) | Move eBUS construction/writer lifetime under driver, retain shell provider handles; keep native generation fencing and current failure isolation. Do not perform this move before the G5 J01–J17 contracts and relevant implementation are accepted. |
| `identification_evidence_wiring.go`, `bus_observability_provider.go` | Replace process publication/direct concrete-store coupling with driver-owned retained snapshots; prove absent/stopped/failed reads perform zero bus I/O and preserve historical provenance. |
| `identification_evidence.go`, `bus_observability_store.go`, `bus_observability_api.go`, `evidence_buffer.go` | Preserve bounded retention/copy ownership, deterministic ordering and age; separate gateway diagnostic fields; distinguish pause, worker close, eviction and explicit destruction. |
| `passive_transaction_reconstructor.go`, `active_passive_deduplicator.go`, `shadow_cache.go` and poller/scheduler files above | Coordinate subscription withdrawal, stale callback fencing, new-generation admission and source-aware last-known-good behavior with their owners; do not silently change retry, dedup, qualification or semantic policy. |
| MCP/GraphQL evidence and bus files above | Preserve envelopes, hashes, raw/qualification meaning and compatibility; document any intentional payload change with updated goldens. All namespaces remain pre-stable until 1.0; no new major or experimental namespace. |

Existing [buffer tests](../../evidence_buffer_test.go),
[identification tests](../../identification_evidence_test.go),
[observability tests](../../bus_observability_store_test.go), and
[bus parity tests](../../cmd/gateway/bus_graphql_parity_test.go) are starting
regressions, not proof of the planned migration. The parity file explicitly
excludes specimens, envelope/summary aggregates and much status; cover those
separately before changing their owner. Add synthetic STOP/START/RESTART tests
for capture quiescence, retained reads/age, failed acquisition preserving data,
old-generation rejection, bounded close and shell availability. Run the real
[scripts/ci_local.sh](../../scripts/ci_local.sh), which discovers Go tests with
`go test -race -count=1 ./...`, and final transport/passive classifiers.

This documentation slice changes no production transport/protocol code and
claims no hardware, Pi, release or completed migration result. A later production
eBUS change remains HELD for its applicable T01..T88/AD01..AD12 and passive smoke
gates; D6 is not a blanket waiver. Offline Modbus RTU conformance remains required
whenever the final classifier triggers it. No override is supplied here.
