# Retained eBUS evidence across driver generations

Status: bounded design contract for issue #1141, a partial slice of parent
#1021. The lifetime, writer-binding and detached-reader seams below are
**proposed and unimplemented**. This page does not establish a public lifecycle
API or prove runtime behavior. Nothing is stable before Helianthus 1.0.

## Scope and current evidence

The [ownership inventory](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/docs/architecture/ebus-raw-evidence-owner-map.md)
remains the current-code inventory. This contract resolves its retained-access
design questions without editing its editorial residual or the reserved
[runtime/driver contract](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/docs/architecture/runtime-driver-provider-contract-v1.md).
It defines storage lifetime constraints, not protocol decoding or wire facts.
Public native documentation remains authoritative for protocol interpretation;
retention supplies no additional protocol meaning.

Current implementation anchors:

| Source | Observed behavior; limit of the claim |
| --- | --- |
| [identification_evidence.go](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/identification_evidence.go) | `IdentificationEvidenceStore` fixes an observer session on its first event, advances sequence only for accepted exchanges, bounds history to eight exchanges per target, and deep-copies snapshots ordered by address/sequence. Session identifies the store run, not a driver generation. No source epoch/generation is stored on exchanges. |
| [evidence_buffer.go](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/evidence_buffer.go) | `EvidenceBuffer` retains address observations/counts with baseline-protected LRU. `Demote` resets promotion/counts but retains the address. Discovery promotion is candidacy, not semantic qualification or a Proven claim. |
| [bus_observability_store.go](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/bus_observability_store.go) | Recent messages/specimens are bounded; periodicity has TTL/LRU. `Close` cancels/joins the passive worker without clearing rings or resetting its attachment context. Direct mutation methods have no general closed-store guard. Close alone is not a capture fence or a restart API. |
| [bus_observability_api.go](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/bus_observability_api.go) | `Snapshot` calls a startup provider, takes the store lock, reads reconstructor status, refreshes passive state and expires periodicity. Existing read-time aging is distinct from new capture. This is not the proposed detached retained-reader seam. |
| [gateway_admission.go](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/gateway_admission.go) and [identification_evidence_wiring.go](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/identification_evidence_wiring.go) | Process composition constructs observers and publishes the identification store through an atomic pointer; adapters copy its snapshot. Swapping that pointer for a fresh empty store would hide history. |
| [ebus_driver_runtime.go](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/ebus_driver_runtime.go) and [gateway_run_lifecycle.go](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/gateway_run_lifecycle.go) | Native Stop/Replace fence withdrawal before retiring transport. Final process cleanup separately drains HTTP then closes listener, deduplicator, reconstructor and observability worker. These paths do not implement the proposed retained-writer binding. |

## Responsibilities and lifetimes (proposed, unimplemented)

Process-scoped means bounded in-memory history survives driver withdrawal and
replacement within the same gateway process. It does not promise unbounded
retention, disk persistence or restoration after process exit. A stable shell
read handle must not be replaced by a new empty history on each generation.

| Responsibility | Owner / lifetime | Required boundary |
| --- | --- | --- |
| Native retained history: identification, recent messages/specimens and discovery observations | eBUS-native storage owner, held for process lifetime | Driver ownership of capture/interpretation is compatible with retaining the storage object while no driver generation is available. Keep current bounds and native bytes/diagnostics. |
| Active/passive capture and transient reconstructor/dedup queues | Generation-scoped native writer attachment | Borrow the existing lifecycle correlation/fence; bind identity before enqueue and recheck at append. Never grant a new writer merely because history exists. |
| Shell read handle | Process composition retains a read-only handle to eBUS-owned snapshots | Return detached copies with zero acquisition I/O while absent, stopped, failed or closing. No dial, probe, reconnect, scan, device read/write, subscription creation or generation activation on read. |
| Derived freshness/availability | Existing projection owner | Evaluate age/status separately from capture. Preserve current periodicity expiry semantics; a changed derived view must not rewrite stored capture facts. |
| Mixed-protocol metrics and semantic metric callbacks | Gateway | Keep Modbus/eeBUS diagnostics and detached semantic metrics available independently of eBUS. Split native storage from this facade before relocating constructors; do not move the entire observability object wholesale. |
| Synchronized evidence `FileStore` | Gateway recorder | Keep mixed-source storage, quota, retention and recovery gateway-owned. A later native acquisition adapter does not transfer FileStore ownership or create a live bridge. |
| Semantic values, native registry admission and shadow state | Their existing owners | Retained raw evidence does not requalify devices, select a source, restore admission or make stale values current. Preserve partial last-known-good fields under their existing identity/age rules. |

The FileStore boundary follows the inventory and
[synchronized evidence runtime](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/synchronized_evidence_runtime.go).
No new lifecycle manager, descriptor evidence hook, receipt, idempotency ledger,
principal boundary or recovery subsystem is introduced. G5 D1–D5/O8 and their
implementation remain J01–J17-owned. There are no new authorization gates before
0.9; existing native admission/trust and invoke safeguards remain in force.

## Provenance and ordering invariants

The following fields are conceptual requirements for a later internal attachment
mapping, **not new public payload fields or implemented storage schemas**. An
unavailable mapping is Unknown; never infer it from timestamps or retrofit it
from whichever driver is current at read time.

| Field / distinction | Invariant | Falsifying trace |
| --- | --- | --- |
| Native source identity versus frame source/target | Bind writer to the lifecycle owner's source identity. Preserve observed frame endpoints independently; equal addresses do not establish equal source ownership. | History from source A is returned as source B solely because both observed the same target. |
| Source epoch | Preserve the capture attachment's epoch if known; it is not observer session, dedup discontinuity epoch, cache epoch or a wall-clock label. | An old record acquires successor epoch E2 when START admits E2. |
| Driver generation / exact correlation | Only the currently admitted attachment may append; compare both `Generation` and `Operation` in the existing correlation within the bound source/epoch, not a numeric generation alone. Do not allocate lifecycle identifiers in storage. | A queued callback from retired G1 appends after G2 opens, or a superseded operation with the same generation passes. |
| Observer session | Preserve the original store session. Keeping the store across generations keeps its session; generation change is recorded by attachment mapping, not by rewriting historical sessions. | RESTART changes session S on old exchanges to S2. |
| Capture sequence | Preserve original ordering within its observer session. Continued use of the same store continues its counter; eviction leaves gaps. No global ordering across independent sessions is implied. | STOP resets sequence in the retained session, or compaction renumbers retained entries. |
| Capture time and first/last seen | Keep the original stored times. A newly admitted matching observation may update an aggregate's last-seen/count under existing rules; a read, resume or rejected callback may not. | Reading old history at logical time r replaces its capture time t with r. |
| Age / evaluation time | Age derives from original capture and evaluation time; unknown/clock anomalies do not become fresh evidence. Derived expiry/status may change without capture. | A later snapshot has age zero despite no accepted observation. |
| Bytes, outcome and native diagnostics | Copy ownership and existing admission filters remain intact. Failed/partial capture does not erase previous successful evidence or fill unknown bytes. | A response-less failure replaces a complete retained exchange with an empty record. |
| Evidence grade and qualification | Proven/Hypothesis/Unknown grade support for a claim; qualification, acquisition availability and admission are separate axes. | Retained raw bytes or discovery promotion changes qualification to qualified or opens acquisition. |

For legacy records without an epoch/generation mapping, preserve their existing
session/sequence/time exactly and expose no invented attribution. Mapping
representation and public exposure need a separate accepted implementation;
this contract does not expand MCP or GraphQL schemas.
The current [manager correlation](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/drivermanager/manager.go)
contains `Generation` and `Operation`; the proposed source/epoch attachment
context does not imply those are already fields of that type.

## STOP / START / RESTART and failures (proposed, unimplemented)

These are retained-storage obligations within the lifecycle owner's operations,
not a replacement lifecycle state machine. Successor epoch/generation allocation,
desired-state idempotence and admission decisions stay with that owner. Transport
retry alone does not establish a new observer session or authorize a writer.

| Transition / condition | Capture and writer obligation | Retained read / source-epoch-session-sequence-time obligation | Falsifying trace |
| --- | --- | --- | --- |
| Initial absent/unadmitted source | No writer is eligible. | Shell can return the existing empty projection; absence is not proof of eviction or qualification. No invented epoch/session/sequence/time. | Empty read starts a discovery probe. |
| STOP from admitted generation | Invalidate its writer at the existing withdrawal fence before asynchronous cleanup; prohibit later appends. | Keep source/epoch attribution, observer session, sequence and capture times; age continues. Status describes paused acquisition. | STOP clears rings or an old callback increments sequence after fence. |
| Repeated STOP | Remain ineligible; do not reopen/reattach workers. | No recapture, timestamp refresh or sequence reset. | A second STOP refreshes last-seen. |
| START after STOP | Bind a successor only after lifecycle retirement/close and admission requirements are satisfied. Reuse process history, not the closed worker attachment. | Historical records retain their source/epoch/session/sequence/time; new records use the new exact attachment and existing store sequence. | START swaps in an empty history or relabels E1 records as E2. |
| START while already admitted | Follow existing lifecycle idempotence; do not add a second capture subscription. | Same history/provenance and sequence domain. | One observation produces two new exchanges due to duplicate wiring. |
| RESTART | Fence predecessor, prove required close/retirement, then bind the accepted successor. No writer overlap. | Reads remain available throughout; old source/epoch/session/sequence/time survive, successor capture is distinguishable internally. | G2 becomes writable while G1 close is unconfirmed. |
| Acquisition/activation failure | No writer admitted for the failed attachment; preserve native failure isolation. | Keep prior successful records and their original attribution/time; availability may be failed with nonempty history. | Failed activation replaces last-known-good history with empty current data. |
| Late/queued callback or cancellation race | Recheck original attachment at mutation; reject stale work even if transport is already closed. | Rejection changes no native record, count, last-seen, session or sequence. Do not replay through successor. | Dequeue code substitutes current correlation for the queued one. |
| Close timeout/error / uncertain quiescence | Remain fenced; no successor writer until the lifecycle owner supplies its required proof. Report uncertainty, not success. | Detached reads keep historical provenance; closed transport is not inferred from a timeout. | A timeout is treated as close proof and G2 starts writing. |
| Capacity / TTL eviction | Apply existing store-specific bounds, independently of STOP. | Removed entries are evicted; remaining records keep identity/order/time. Empty output alone cannot prove why history is absent. | A read reports evicted history as never observed or renumbers survivors. |
| Process exit / new process | Memory history has no durability promise; native writers terminate with process ownership. | Do not claim continuity/restoration or infer new epoch admission from durable mixed-source bundles. | A FileStore bundle reopens live capture after process start. |

“Paused” describes capture eligibility while retained history may exist.
“Closed” describes an attachment/worker's completed shutdown, not deletion or
restartability of storage. “Evicted” describes record removal by an existing
retention policy, not source availability. These are independent dimensions,
not mutually exclusive API enum values; no new public status fields are added.

## Callback linearization, teardown and lock order

The following discipline is **unimplemented for retained writer bindings**:

1. Capture the existing exact lifecycle correlation when attaching a writer;
   queued work retains that identity. Do not read the current generation later
   to label an old event. Transient dedup/reconstruction state cannot be carried
   into a successor as fresh input.
2. Serialize attachment invalidation with append eligibility. A short local
   gate must cover both the final identity/eligibility check and storage mutation,
   so a callback either commits before withdrawal or is rejected after it. A
   check followed by an unlocked append leaves a stale-write race. Admission at
   enqueue alone is insufficient. Do not add this storage gate to transport I/O.
3. The existing withdrawal hook runs under manager locking and must stay bounded
   and manager-non-reentrant. It may invalidate a local token; it must not wait
   for workers, call the manager, perform I/O, or run shell/provider callbacks.
   A future local gate may nest a native store lock only in that direction;
   store mutation must never call back into lifecycle/manager locking.
4. Release lifecycle and storage locks before canceling/closing subscriptions,
   joining workers or waiting for native close. Preserve the existing native
   teardown dependency order: listener, deduplicator, reconstructor,
   observability worker. Their shutdown emissions are cleanup, not permission
   for a fenced writer to append. A drain must not feed successor queues.
5. Snapshot only retained memory and already-detached status. Release native
   locks before formatting or calling consumers; no arbitrary provider callback
   under a retained-store lock. Do not acquire manager locks from a snapshot.
   Current observability snapshots consult reconstructor/status providers, so
   storage extraction alone does not satisfy this future detached-reader rule.

Lock-order acceptance must include the append/fence race, callback-triggered
withdrawal, a reader concurrent with close, and a blocked worker join. A bounded
close result is separate from retained-read success. Existing process shutdown
drains HTTP before native-consumer cleanup; keeping shell reads through a driver
STOP does not remove final process HTTP drain or prolong reads past process exit.

## Synthetic falsification review and delivery boundary

These traces use symbolic sources and logical event order only. They are
reasoned acceptance examples, **not executed tests or runtime proof**:

| Minimal trace | Required result / mutation that would falsify it |
| --- | --- |
| A/E1/G1 captures session S sequence 1 at t; STOP; read at r later than t | Same bytes/S/1/t, older age and unavailable acquisition. Clearing on STOP or stamping r falsifies the contract. |
| Queue A/E1/G1 event; fence; admit A/E2/G2; dequeue old event | No append/count/last-seen/sequence change. Removing the final fence check or rebinding queued identity falsifies it. |
| Start with eligible source A / epoch E1 / correlation (Generation G1, Operation O1); vary source alone, epoch alone, Generation alone, then Operation alone | Each mismatch rejects before mutation. Omitting source or epoch scope, omitting Generation, or omitting Operation respectively falsifies that field's check. This is an implementation review obligation, not a claim that raw records currently carry these fields. |
| Hold a callback at the append boundary; race withdrawal | Commit entirely before fence or reject entirely after it. Splitting check from mutation falsifies linearization. |
| Retain one complete exchange; fail successor activation or receive incomplete capture | Original record remains, acquisition fails closed. Whole-snapshot empty replacement falsifies preservation. |
| Close worker while a reader copies history; block worker completion | Reader needs no worker/manager/transport action; close remains unconfirmed until proof. Joining under the storage lock falsifies lock order. |
| Fill an existing bounded ring; append one eligible record | Only policy-selected oldest history is evicted; survivor sequences/times unchanged. Renumbering or extending bounds falsifies preservation. |

W12–W14 storage-only extraction must preserve existing behavior, constructors'
effective lifetimes, retention and public projections. It does not depend on
this proposed lifetime design and must not silently implement it. Later G5
constructor/attachment moves need separate accepted lifecycle integration and
executable race/retention/zero-I/O coverage. No unmerged implementation or open
PR is a prerequisite for this documentation slice.

Remaining parent acceptance includes moving both stores together with the native
deduplicator boundary, resolving mixed metric extraction, replacing global
publication safely, coordinating generation queues, and proving compatibility
and applicable composition/transport gates. Preserve MCP `meta`/`data`/`error`,
deterministic ordering/hashes, raw evidence meaning, goldens and existing GraphQL
parity. Historical `v1` naming requires no new major or experimental namespace.
No MCP-first rule is extended to other protocols. This page claims no runtime,
transport, passive smoke, hardware, release or parent-closure success.
