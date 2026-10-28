# eBUS raw-evidence ownership inventory

Status: current-code reconciliation after the W12/W13/W14 storage extractions.
The [original inventory #1097](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1097)
is historical context.
Refs [parent #1021](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1021);
this partial slice does not close the parent or complete driver-owned writers.

## Evidence boundary and entry points

The [runtime/driver contract](runtime-driver-provider-contract-v1.md) assigns
native evidence to the driver. Storage mechanics now live under
`internal/ebus/evidence`, but root facades and process composition still own
construction and observer wiring. Package extraction is not completed driver
lifecycle ownership. The gateway hosts drivers; eBUS is a southbound driver.

The delivered public contract from docs #639 is
[runtime-driver-lifecycle.md, S7: retained eBUS evidence and deferred store move](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/runtime-driver-lifecycle.md#s7-retained-ebus-evidence-and-deferred-store-move).
It requires process-scoped retained evidence, capture paused while unadmitted,
and original identity, provenance and age across STOP/START/RESTART. S7's
reserved `evidence` hook clause says the hook remains unimplemented and declaring
it fails registration. This is not an available descriptor evidence hook.
The later #1021 move must preserve that contract. This inventory adds no
lifecycle API, recovery design, protocol interpretation, retention policy or
namespace.

Current entry is [main.go](../../cmd/gateway/main.go): `main` calls `run`, which
delegates to `runGatewayLifecycle` in
[gateway_run_lifecycle.go](../../cmd/gateway/gateway_run_lifecycle.go).
The orchestration is in `gateway_run_lifecycle.go`; the observer factory
`wireObserveFirstObservers` is in
[gateway_admission.go](../../cmd/gateway/gateway_admission.go).

`runGatewayLifecycle` composes managed runtime/control and the process observer
chain, constructs the gateway bus, installs retained-view providers and starts
the HTTP control plane. Post-start eBUS tasks run under an admitted generation;
process consumers are created once by the first admitted generation and survive
stopped periods. Driver unavailability can leave the shell available. The native
`ebusDriverRuntime` in
[ebus_driver_runtime.go](../../cmd/gateway/ebus_driver_runtime.go) owns transport
generations and withdrawal fencing, not the retained-store constructors.
This inventory does not independently establish complete capture pause/resume
or old-callback fencing for the remaining process-owned writers.

## Current stores: extracted mechanics and process-owned facades

Links identify implementation files; symbols name the constructor, mutation,
and read paths in those files. Retention below describes current code, not a
new STOP contract. In-memory data has no implied process-restart durability.

| Store / current owner and constructor | Mutation entry points | Retention / lifetime | Readers and public projection |
| --- | --- | --- | --- |
| Raw identification exchanges: process observer chain; `wireObserveFirstObservers` constructs `NewIdentificationEvidenceStore` in [identification_evidence.go](../../identification_evidence.go), whose wrapper delegates to `identification.NewStore` in [the extracted store](../../internal/ebus/evidence/identification/store.go) (W13). [identification_evidence_wiring.go](../../cmd/gateway/identification_evidence_wiring.go) publishes an atomic process pointer. | The root `OnBusEvent` delegates to native `Store.OnBusEvent`, which admits only successful complete directed identification exchanges; failed/response-less attempts leave retained exchanges intact. It copies request and response bytes. First event fixes observer session; each admitted exchange advances sequence. | Eight exchanges per target, at most 256 targets; oldest exchange replaced per target. Session and sequence belong to this store instance, not DriverManager generation. No TTL, disk persistence, clear, or lifecycle STOP hook. | The root `Snapshot` delegates to native `Store.Snapshot`, which deep-copies in address/sequence order. The wiring adapters' `IdentificationEvidence` feeds [MCP](../../mcp/identification_evidence.go) and [GraphQL](../../graphql/identification_evidence.go); raw/undecoded/qualification-none. CRC/ACK are absent from observer input, not recoverable evidence. |
| Recent message ring: `BusObservabilityStore`, constructed by `NewBusObservabilityStore` in [bus_observability_store.go](../../bus_observability_store.go), owned by process composition. | `OnBusEvent` and `OnPassiveClassifiedEvent` reach active/passive recorders and `pushRecentLocked`. | Configured `ObserveFirstRecentMessageCapacity`; overwrite oldest. Metadata/length records, not a complete byte archive. `Close` cancels subscription worker and joins; it does not empty the ring. | `RecentMessages` and [Snapshot](../../bus_observability_api.go); [bus_observability_provider.go](../../cmd/gateway/bus_observability_provider.go) maps retained messages to [MCP bus tools](../../mcp/bus.go) and [GraphQL bus fields](../../graphql/bus_observability.go). |
| Protocol specimen rings: `BusObservabilityStore` embeds a zero-value-ready `specimens.Store` from [the extracted store](../../internal/ebus/evidence/specimens/store.go) (W14); process construction still owns that instance. | `OnPassiveClassifiedEvent` → `pushSpecimenLocked` only for successful or abandoned events with a request and `!isImplementedFamily(family)`, excluding B509, B516, B524 and B555. The root classifies and admits events; `pushSpecimenLocked` maps them to native `Insert`. The native key includes family, source, target, frame type, request bytes, outcome and response presence/bytes; a match increments count and updates last seen. Bytes are copied on insertion. | 32 specimens per family, 64 families maximum; per-family overwrite oldest unique specimen, refuse additional families at cap. First/last-seen and count retained; no TTL or disk persistence. | Native `Snapshot` returns detached entries in per-family insertion order, with cross-family order unspecified; `ProtocolSpecimens` in [bus_observability_api.go](../../bus_observability_api.go) converts to hex and sorts deterministically for MCP adapter `ProtocolSpecimens`. Native `Counts` supplies summary counts. No equivalent GraphQL specimen field in the current parity mapping. |
| Periodicity and observability aggregates: same `BusObservabilityStore`; includes frame/error/byte series, address buckets, busy window, warmup and watch-efficiency state. These are derived observations, not raw payload archives. | Active/passive recorders; `recordPeriodicityLocked`, `recordBusyLocked`, `ObserveWatchRead`, `ObserveWatchDirectApply`; composition also supplies admission and other transport status. | Periodicity uses configured `ObserveFirstPeriodicityCapacity`, stale TTL and LRU eviction. Frame/error series budget and hard-cap checks increment overflow accounting but still insert new keys; no dropping or eviction is enforced by those checks. Address buckets have no configured budget; the byte-sized protocol address space limits them to at most 256 addresses. Busy segments are windowed. Counters otherwise accumulate for store lifetime. `Snapshot` and `RenderPrometheus` can refresh derived state/expire periodicity even without new capture. | [Snapshot and summary](../../bus_observability_api.go), MCP/GraphQL adapters above, `MetricsHandler`/`RenderPrometheus`. Not all aggregates are in the existing parity mapping. |
| Discovery `EvidenceBuffer`: nested in `BusObservabilityStore`, constructed with root `NewEvidenceBuffer` in [evidence_buffer.go](../../evidence_buffer.go), which delegates to `discovery.NewEvidenceBuffer` in [the extracted buffer](../../internal/ebus/evidence/discovery/buffer.go) (W12). Root evidence types are aliases; baseline/probe-target configuration remains at root. | Root `recordEvidenceFromEventLocked` classifies/filters passive addresses and calls native `Record`; [PassiveDiscoveryPromoter](../../passive_discovery_promoter.go) reads `PromotedAddresses` and can `Demote` after failed confirmation. | Runtime construction uses 128 address entries with baseline-protected LRU; records retain last-observed and observation counts, not frame bytes. No TTL/persistence; `Demote` clears promotion/counts, not the address. | `EvidenceBuffer` accessor passed by process composition to `NewPassiveDiscoveryPromoter`; directed discovery/registry enrichment, not direct raw-byte export. Promotion here is discovery candidacy, not semantic qualification or a Proven protocol claim. |
| Passive reconstruction state and fanout queues: process composition calls `startPassiveTransactionReconstructor` in [gateway_run_lifecycle.go](../../cmd/gateway/gateway_run_lifecycle.go). Its [binding](../../cmd/gateway/gateway_v8_admin_http.go) delegates to `StartPassiveTransactionReconstructor`, which calls `StartPassiveTransactionReconstructorWithTransport` in [passive_transaction_reconstructor.go](../../passive_transaction_reconstructor.go). | `OnPassiveTapEvent` assembles/classifies; watchdog, discontinuity/reset and shutdown paths retire partial transactions. | Partial request/response state plus bounded subscriber queues; no historical frame archive. `Close` cancels tap, publishes shutdown events and closes subscribers once. | Observability store, deduplicator, admission/discovery consumers; aggregate `Snapshot` reaches bus summary. These queues must not be confused with retained specimen/exchange rings. |
| Active/passive fingerprint and pending-event windows: `NewActivePassiveDeduplicator` in [active_passive_deduplicator.go](../../active_passive_deduplicator.go), constructed by observer wiring when broadcast listening is enabled. | `OnBusEvent`, `OnPassiveClassifiedEvent`, expiry and discontinuity handling. | Configured active/pending budgets, dedup windows and grace expiry. Discontinuity advances its own epoch and flushes windows; that epoch is not a G5 source epoch. `Close` cancels/joins and closes subscribers. | Watch observer fallback and adjudicated passive consumers; feeds shadow/semantic paths. Current windows are transient admission machinery, not durable raw-evidence history. |
| Raw watch-value `ShadowCache`: semantic poller owns `NewShadowCache`, constructed by `newVaillantSemanticPoller` in [semantic_vaillant_snapshots.go](../../cmd/gateway/semantic_vaillant_snapshots.go). | `Write`/`Invalidate` in [shadow_cache.go](../../shadow_cache.go); active confirmed writes from [semantic_read_scheduler.go](../../semantic_read_scheduler.go), passive writes/invalidation from [semantic_vaillant_passive.go](../../cmd/gateway/semantic_vaillant_passive.go), operator invalidation in [semantic_vaillant.go](../../cmd/gateway/semantic_vaillant.go). | Bounded entries/pinned budgets; per-key generations, observed/expiry times, source and confidence. Stale/conflicting/generation-obsolete writes rejected. Tombstone retention/compaction differs from capture retention. `StartCompactor` and `Close` have no production caller; `Close` only concerns compactor shutdown. Construction does not start the compactor. | Scheduler `Lookup`/eligibility and poller consumption; detached watch summaries through [watch_summary_provider.go](../../cmd/gateway/watch_summary_provider.go). No permission to serve stale values as current or to move semantic ownership into this cache. |
| Synchronized evidence `FileStore`: gateway recorder, opened by `openSynchronizedEvidenceRuntime` in [synchronized_evidence_runtime.go](../../cmd/gateway/synchronized_evidence_runtime.go), implementation [store.go](../../internal/syncevidence/store.go). | `Capture` reserves quota, recorder acquires, reservation `Publish` persists; store also exposes `Drop`/`EnforceRetention` and open-time recovery. | Explicit config retention/quota, durable bundles with journals/locking; recovery applies retention. `Close` releases resources, not deletion of bundles. Generic recorder is disabled by default; current eBUS source registrations use `unavailableEBusEvidenceReader`, not a live bridge to the above rings. | Generic runtime returns bundle bytes/ID to its caller; no generic public capture call is wired by `runGatewayLifecycle`. The separate [one-shot runtime](../../cmd/gateway/synchronized_evidence_runtime.go) and [executor](../../internal/syncevidence/oneshot.go) open the same storage implementation per operation with their own fixed quota/retention; [MCP synchronized evidence](../../mcp/synchronized_evidence.go), registered in [gateway_http_server.go](../../cmd/gateway/gateway_http_server.go), is a separate control surface. This mixed-source facility is not wholly an eBUS-driver store. |

## STOP, retained age and provenance

Current `ebusDriverRuntime.Stop` fences withdrawal, clears its current transport
generation pointer and stops the transport runtime. `Replace` fences before
replacement. Neither method constructs, clears, persists or swaps these stores.
The extracted discovery, identification and specimen stores have no STOP, pause,
resume or close hook of their own; their retained lifetime follows their process
facade instances. Process teardown separately closes listener, deduplicator,
reconstructor and observability worker after HTTP admission/drain. That teardown
does not prove generation-owned evidence writers or complete capture pause/resume
coverage. In particular, observability
`Close` leaves records but retains an attachment context: treating a closed store
as automatically restartable would be incorrect.

Pause means no further admitted capture, not destruction. Existing observation
times, specimen first/last seen, exchange session/sequence and raw bytes must
remain historical facts; a STOP timestamp or a later read cannot replace capture
time or imply qualification. Periodicity may age out on a read, and availability
may degrade as time passes. Thus a retained view need not be byte-identical in
all derived status fields while stopped. Never reset age by serving a snapshot.

| Delivered lifecycle decision | Constraint on remaining ownership migration |
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

## Remaining driver-owned writer boundary and unresolved edges

The eBUS driver should own creation and mutation of its identification,
message/specimen, periodicity, discovery and passive-window state; gateway shell
consumers retain detached read access across unavailability. The shadow's native
acquisition/value ownership must be split from semantic publication by its
existing owner before relocation. Mixed-source synchronized storage and durable
runtime-state persistence remain gateway facilities; only an accepted eBUS
acquisition adapter would move behind the driver. The accepted [S7 evidence-hook clause](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/runtime-driver-lifecycle.md#s7-retained-ebus-evidence-and-deferred-store-move)
reserves `evidence` and rejects its declaration; it does not provide a usable
read-view or capture hook. K04/M49/W17 remain historical slices, not work
reimplemented by this reconciliation.

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
- Shadow compactor `StartCompactor`/`Close` have no production caller. Future
  lifecycle wiring and cache-generation/source-epoch mapping remain unresolved;
  current constructor ownership does not establish G5 lifecycle coverage. Preserve
  partial last-known-good fields under existing identity/age rules rather than
  replacing a valid snapshot wholesale.
- B503 has a separate session owner and request-time history reads in
  [vaillant_b503_wiring.go](../../cmd/gateway/vaillant_b503_wiring.go) and
  [mcp/vaillant_b503.go](../../mcp/vaillant_b503.go); it is not an additional
  generic retained raw ring. Its existing activation/withdrawal fence must remain
  intact. Native registry evidence, semantic caches and source-selection/runtime
  state are adjacent owners, not authorization to fold them into this migration.

## Remaining migration responsibilities and checks

| Later file group | Minimal responsibility / required evidence |
| --- | --- |
| `gateway_admission.go`, `ebus_driver_runtime.go`, `gateway_run_lifecycle.go` (linked above) | Move eBUS construction/writer lifetime under driver, retain shell provider handles; keep native generation fencing and current failure isolation. Follow the delivered [S7 contract](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/runtime-driver-lifecycle.md#s7-retained-ebus-evidence-and-deferred-store-move) and managed eBUS admission boundary; accepted lifecycle work is not an outstanding pre-G5 hold or proof that evidence writers have moved. |
| `identification_evidence_wiring.go`, `bus_observability_provider.go` | Replace process publication/direct concrete-store coupling with driver-owned retained snapshots; prove absent/stopped/failed reads perform zero bus I/O and preserve historical provenance. |
| `internal/ebus/evidence/{discovery,identification,specimens}`, root `identification_evidence.go`, `evidence_buffer.go`, `bus_observability_store.go`, `bus_observability_api.go` | Preserve the extracted mechanics and facade contracts, bounded retention/copy ownership, deterministic public ordering and age; separate gateway diagnostic fields; distinguish pause, worker close, eviction and explicit destruction. |
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
old-generation rejection, bounded close and shell availability. Full repository
CI, committed-change review and final transport/passive classification belong to the subsequent integration gate; this reconciliation
records only focused offline source/manual evidence.

This documentation slice changes no production transport/protocol code and
claims no hardware, Pi, release or completed migration result. A later production
eBUS change requires its applicable T01..T88/AD01..AD12 and passive smoke
evidence; D6 is not a blanket waiver. Offline Modbus RTU conformance remains
required whenever the final classifier triggers it. No override is supplied here.
