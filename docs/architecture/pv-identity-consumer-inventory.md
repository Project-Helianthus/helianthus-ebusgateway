# PV identity consumer inventory

Read-only evidence for [issue #1038](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1038),
a partial slice of [#1030](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1030).
The parent remains open for a separately approved migration. This inventory
selects no migration policy and establishes no compatibility verdict.

## Evidence boundary and revisions

**G** means the supplied `helianthus-ebusgateway` checkout at the starting
`issue/1038-pv-identity-consumers` HEAD, equal to locally cached `origin/main`.
The worktree was clean before this document was added. Local `main` is older;
it was not used as source evidence. No remote freshness was checked.
Exact commit values were read with `git rev-parse HEAD`, `git rev-parse main`
and `git merge-base HEAD refs/remotes/origin/main` in the worker command evidence.
The dispatch publication rule prohibits commit hashes in documents and reports;
therefore the revision column uses G rather than reproducing an exact SHA.
This is a limitation of the requested per-row exact-SHA evidence, not an
immutable substitute for it. Revalidate citations at the orchestrator's committed
HEAD before review. All source links below use public GitHub URLs on `main`.

**H** (`helianthus-ha-integration`) and **M** (`helianthus-matter-binding`)
were named but no snapshot locations or exact revisions were supplied in the
issue/companion bundle. Neither was inspected; no sibling checkout was substituted.
No active-PR or In Progress snapshot was supplied either. The exact owned path
was absent in G, and existing characterization tests cover consumer output,
not this persisted-identity inventory. Remote overlap remains unverified;
the orchestrator must reconcile it before publishing a duplicate result.

**Proven** means directly supported by inspected implementation, **Hypothesis**
means a possible consumer consequence requiring evidence, and **Unknown** means
missing or uninspected evidence. These are evidence labels, independent of
runtime qualification/admission. This document describes ID plumbing only;
it asserts no new native protocol, register, decoding or qualification facts.

## Consumer trace

Each G citation gives the exact inspected file and starting line. “No persistence
in this path” is scoped to that implementation, never to an entire deployment.
No persistent PV identity key is proven in the supplied consumer snapshots.

| Consumer / evidence | Repo / revision; file:line | Key and later consumer | Lifetime / restart behavior | Compatibility impact and missing evidence |
| --- | --- | --- | --- | --- |
| Producer asset — Proven | Gateway / G; [publication core:241](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/modbusadapter/semreg_pv_publication_core.go#L241), [identity inputs:330](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/modbusadapter/semreg_pv_publication_core.go#L330) | `AssetID` hashes the decoded Common identity tuple, then enters snapshot, catalog source, M2M selection and feed resource. No raw tuple values are reproduced here. | Formula excludes epoch; identical input bytes produce identical ID. Runtime maps are created in [core:534](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/modbusadapter/semreg_pv_publication_core.go#L534); no disk write in this construction. Input continuity across real restarts is unverified. | Any future asset-ID change would reach allowlists and configured asset references. No asset migration or identity equivalence is approved. Missing downstream persisted-key inventory. |
| Producer source — Proven | Gateway / G; [publication core:243](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/modbusadapter/semreg_pv_publication_core.go#L243), [native tuple:361](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/modbusadapter/semreg_pv_publication_core.go#L361) | `SourceID` hashes native resource identity; referenced by binding, source descriptor, candidates and hub handles. Embedded snapshot provenance reaches Portal and M2M. | Conditional stability for identical native tuple; no epoch in this formula. Core handles are process memory. No general promise across reconfiguration. | A changed native tuple can change source ID independently of process restart. Persistence outside inspected paths is unknown. |
| Producer epoch and binding — Proven | Gateway / G; [adapter:171](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/modbusadapter/adapter.go#L171), [epoch minter:222](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/semhub/ids.go#L222), [binding:245](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/modbusadapter/semreg_pv_publication_core.go#L245), [legacy admission:554](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/modbusadapter/semreg_pv_publication_core.go#L554), [legacy minter:151](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/semhub/ids.go#L151) | Epoch depends on endpoint and runtime start sample. `BindingID` depends on asset, source, epoch and generation; enters identity link, facts, services, capabilities and consumer fences. | Restart-dependent when the start sample changes; generation replacement also changes binding input. No persisted incarnation recovery in these constructors. | Replacing the legacy minter changes observable bytes and freshness context. Missing approved restart contract and cross-consumer migration evidence; no migration performed. |
| Producer service/capability instances — Proven | Gateway / G; [publication core:428](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/modbusadapter/semreg_pv_publication_core.go#L428), [legacy service:173](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/semhub/ids.go#L173), [legacy capability:187](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/semhub/ids.go#L187) | Service `InstanceID` and capability `InstanceID` hash asset + binding + definition. Capability `ServiceInstance` links the instances. These differ from `Definition.ID`. | Follow binding changes even when definition strings are unchanged. Snapshot/core memory in inspected path. | Consumers of instances may observe replacement. A stable definition does not prove a stable instance or entity key. Missing HA and Matter binding references. |
| Portal catalog — Proven | Gateway / G; [catalog:194](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/portal_catalog_v1.go#L194), [matching:213](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/portal_catalog_v1.go#L213), [return:261](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/portal_catalog_v1.go#L261) | Resource `service_id` / `capability_id` are **definition IDs**. Source includes asset, binding, epoch, generation and complete snapshot (including source/instance IDs). Matching checks instance relationships and current binding/source before exposure. | Rebuilt from current snapshot per capture; [composer:37](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/portal/catalogv1/compose.go#L37), [digest:158](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/portal/catalogv1/compose.go#L158). No durable catalog identity store in this path. | Changed binding reaches catalog context/digests even if definition IDs stay unchanged. Missing external catalog-client persistence; display is not a compatibility guarantee. |
| Portal browser keys and display — Proven | Gateway / G; [resource key:62](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/portal/web/src/app.js#L62), [maps:1741](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/portal/web/src/app.js#L1741), [provenance:430](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/portal/web/src/app.js#L430), [snapshot details:634](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/portal/web/src/app.js#L634) | Resource grouping key = contribution driver/manifest/version + resource ID. Asset, source, binding, service and capability instance IDs are rendered in snapshot/provenance details. Selection uses an in-memory resource index. | Inspected app has no PV-ID browser-storage write; its `localStorage` calls are [theme only:814](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/portal/web/src/app.js#L814). Catalog maps/selection are rebuilt in browser memory. | Displayed IDs may change on replacement/restart. This establishes display and in-memory use, not absence of persistence in uninspected consumers or browser extensions. |
| Portal forwarding configuration — Proven | Gateway / G; [Portal client:20](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/portal_pv_client.go#L20), [M2M client:25](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/m2mgraphql/client.go#L25), [request:88](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/m2mgraphql/client.go#L88) | Configured `AssetRef` is copied into M2M client state and every query. Binding/source/service/capability instances pass through response snapshots. | Asset reference lives for configured client lifetime. No configuration-file write in these functions; external configuration persistence uninspected. | Asset-ID changes could invalidate configured selectors. Binding-only changes do not alter the shown request key, but response handling still requires owner review. |
| M2M GraphQL provider / server — Proven | Gateway / G; [runtime:104](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/m2m_graphql_runtime.go#L104), [PV response:117](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/m2m_graphql_runtime.go#L117), [handler:162](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/m2mgraphql/handler.go#L162) | `assetRef` must match configured allowed assets; full snapshot/evaluation/selections/projection is returned. All five ID classes can reach external clients in the snapshot. | Request-time lookup and response; no durable response store in inspected provider/handler path. Allowlists are configured inputs, not proven persisted records. | Asset selectors and snapshot consumers have distinct impact. External M2M client databases, caches, saved queries and automation references are Unknown; no compatibility claim. |
| M2M supplied client — Proven | Gateway / G; [client state:30](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/m2mgraphql/client.go#L30), [query:84](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/m2mgraphql/client.go#L84) | In-memory configured asset key; response JSON consumed by caller. No ID rewrite in inspected request path. | Client lifetime; downstream persistence after return uninspected. | Missing consumer applications and storage schemas. Hypothesis: callers may persist full snapshots or instance IDs; no such persistence is demonstrated. |
| Matter gateway feed conversion — Proven | Gateway / G; [feed map:100](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/matter_binding_feed_v1.go#L100), [join:175](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/matter_binding_feed_v1.go#L175), [projection:179](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/matter_binding_feed_v1.go#L179) | Raw catalog join uses contribution identity + resource + service/capability **definitions**. Outgoing resource key = asset + resource; outgoing context includes binding/epoch/generation. Complete snapshot/evaluation is passed to `matter.Project`; projector internals in the dependency are uninspected here. | Per-capture maps and current projected result; no durable endpoint allocation in this conversion. | Binding changes propagate to feed context; source/service/capability instances enter projection input, but their exact projection treatment needs SemReg projector inspection. No endpoint-stability verdict. |
| Matter feed replay and claims — Proven | Gateway / G; [server state:43](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/matter/feedv1/server.go#L43), [capture:162](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/matter/feedv1/server.go#L162), [resync:190](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/matter/feedv1/server.go#L190), [claims:148](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/matter_binding_feed_v1.go#L148), [claim checks:256](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/matter/feedv1/contract.go#L256) | Current snapshots/history carry asset/resource and binding context; operation claims compare capability definition and binding/epoch/generation. Cursor uses feed instance + sequence, not a PV instance ID. | Bounded in-memory history; [runtime:135](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/m2m_graphql_runtime.go#L135) creates a fresh random feed instance. Foreign cursor requires resync. No disk replay persistence in server. | Restart resync is observable; it does not prove downstream endpoint remapping is safe. Production action admission is separately constrained at [feed:215](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/matter_binding_feed_v1.go#L215). Missing binding-worker state evidence. |
| Matter binding durable endpoint mapping — Unknown | `helianthus-matter-binding` / M unavailable; file:line and exact revision unverified | Whether asset/source/binding/service/capability IDs participate in persisted endpoint, fabric or resource mapping is uninspected. | Persistence keys, storage schema and restart/rejoin behavior unknown. | Snapshot provisioning required. Owner must trace feed ingestion through endpoint allocation, durable storage and reconciliation before migration. Absence of supplied code is not absence of persistence. |
| HA entity unique IDs — Unknown | `helianthus-ha-integration` / H unavailable; file:line and exact revision unverified | Formula and use of asset/source/binding/service/capability IDs in PV `unique_id`, device identifiers and discovery keys uninspected. | Unique-ID lifetime and restart behavior unknown. | Provision exact source snapshot; trace coordinator payload through entity constructors and device info. Cannot infer entity continuity from gateway asset stability. |
| HA entity-registry persistence — Unknown | `helianthus-ha-integration` / H unavailable; file:line and exact revision unverified | Persisted entity/config-entry keys, registry lookup/update/removal, restore state and any saved instance references uninspected. | No demonstrated persisted key or rename/reconciliation behavior. HA core registry behavior is also uninspected. | Integration owner must supply source and synthetic registry/restart fixtures; release-note and registry migration needs remain undecided. No private registry export required. |

## Existing tests and remaining evidence

[PV generation/epoch characterization:12](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/modbusadapter/semreg_pv_generation_characterization_test.go#L12)
records generation refresh/replacement and epoch replacement. It is not a
two-process restart-persistence proof. [Consumer characterization:738](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/portal_catalog_v1_test.go#L738)
and [M2M characterization:494](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/m2m_graphql_runtime_red_test.go#L494)
pin gateway consumer output, not HA registry or Matter endpoint continuity.
No goldens or production files are changed by this inventory.

The trace ends at inspected gateway display/response/replay paths and explicit
Unknown boundaries. The SemReg projector, external M2M applications, HA and
Matter binding storage are uninspected, not proven persistence-free. A complete
consumer-impact verdict requires those snapshots and downstream owner evidence.

## Migration alternatives for owner review only

| Alternative, not selected | Owners affected | Evidence needed before deciding |
| --- | --- | --- |
| One-time rename/rekey of persisted identities | Gateway producer, HA integration/entity registry, Matter binding endpoint store, external M2M operators | Actual key schemas, collision rules, atomicity, rollback, entity/endpoint continuity and restart fixtures. |
| Compatibility mapping from old to new identifiers | Gateway and SemReg contract owners, all consumers that resolve aliases | Alias scope/lifetime, ownership, ambiguity handling and retirement rules; mappings must not admit a stale binding or bypass epoch/generation fences. |
| No compatibility mapping; explicit replacement/rediscovery | HA, Matter binding, Portal, M2M consumers and release owners | Demonstrated impact on saved entities/endpoints/selectors, operator recovery and observable release notes. This is an alternative, not a claim that impact is acceptable. |
| Defer migration while evidence is incomplete | Gateway and parent-issue owner, companion evidence owners | Missing snapshots, overlap reconciliation and a separately approved target identity/restart contract. Existing restart dependence remains. |

The parent wording must still resolve which identities should survive restart:
[hub-native binding minter:89](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/semhub/ids.go#L89) takes epoch and
generation as inputs too. A minter switch alone does not demonstrate restart
stability. Gateway/semantic owners must specify the intended observable contract;
this inventory does not choose one. HA release documentation and public semantic
identity documentation need owner review with the eventual migration. No native
protocol documentation or qualification meaning is changed by this read-only slice.

## Logger-attached PV children as resources

Added for [issue #1539](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1539).
It records what the resource publication of the logger-attached PV family
changes for the consumers traced above. It is evidence for consumer owners, not
a migration decision.

**What changes (Proven, gateway).** Each admitted inverter child behind a
SmartLogger or an S-Dongle keeps its home asset and now publishes, in that
asset, its PV system (a function on an operator slot), its inverter (equipment
on an operator slot) and a DC input component at each native string index whose
voltage and current pair shows a present input. The gateway allocates every
resource identifier, and the value of every fact's resource dimension is the
identifier of the fact's subject: the inverter for the power and status facts,
the DC input for the string voltage and current, and the PV system for the
lifetime energy counter. The system groups the child's inverter and the
inverter contains each published input, both by the gate's rule. No phase is
allocated, because no admitted profile reads a per-phase word. A dimension
value is no longer built from the asset identifier or from the string index.

**Identity events (Proven, gateway tests).** A driver restart into a new source
epoch and a changed endpoint address keep every identifier. When the native
owner withholds a child because another unit answered at it and later admits it
again, the inverter and its inputs get new identifiers, the replaced inverter
and inputs are retired once nothing in the asset names them, and the PV system
keeps its identifier. The anchor table is kept beside the runtime state file
when the composition opens it there, so a replacement also survives a process
restart; with the in-memory table it survives driver restarts within one
process only.

**Consumers.**

| Consumer | Effect | Evidence class |
| --- | --- | --- |
| Semantic MCP read and GraphQL `semanticSnapshot` of a logger child | Fact keys carry resource identifiers; the snapshot carries resources, relations and identity links that name a resource. | Proven |
| MCP and Portal characterization goldens | Unchanged: they are produced by the SunSpec PV, storage and EVSE families, whose publishers are not part of this change. | Proven |
| Prometheus semantic target | Unchanged text: a graphed snapshot's label is derived from the subject resource's kind and, for a phase, its slot, and a snapshot without resources is read as before; the logger family does not feed this target. | Proven |
| Home Assistant entity identifiers built from logger-child dimension values | Change with this publication; the integration follows in its own change. | Hypothesis until the integration's source is traced |
| Portal instance contribution | Unchanged: it carries native instance diagnostics and asset identifiers only; the published assets render through the generic semantic surfaces. | Proven |
| Matter feed conversion and binding endpoints | The inspected feed conversion names no logger-family source; whether a downstream binding reads these assets is not traced. | Unknown |

### Logger, relation check and withheld children

**Relation check (Proven, gateway tests).** Before a batch is offered, every
relation instance of the family is checked against the three relation rows of
the family's PV gate: `relation.relays` with basis class `native_observation`,
`pv.relation.system_inverter` with `gate_rule` and `pv.relation.inverter_input`
with `gate_rule`. An instance whose definition, basis class or end kinds the
gate does not state, or whose basis is an index, a label or an operator origin,
is refused as a false join. An instance with an end the publication has not
published is refused as a dangling endpoint. A refused instance refuses its
whole batch. The basis reference of each instance names the gate row and the
scope of its rule. One unit reached through two configured labels stays two
inverters with no relation between them.

**Logger (Proven, gateway tests).** The logger or dongle of each configured
instance is published as `resource.equipment` on the operator slot of the
instance label. It sits in its own asset, declared by the family's class and
keyed by the instance label alone, with one binding of the gateway path and a
qualified identity link that names it. Nothing is published for the logger
before a child batch has committed. The instance status lists the logger asset
among the published asset identifiers once it is published. A logger asset
holds no fact.

**Relays withheld (Proven, gateway tests).** The `relation.relays` instance from
the logger to each published inverter is built and checked against the gate,
then withheld: the pinned SemReg lets a relation record name a definition only
through a pack reference, so the core `relays` definition cannot be named in a
record yet. The instances are published once the kernel resolves that
definition.

**Withheld children (Proven, gateway tests).** When the native owner withholds
a child, or a complete listing no longer names it, the child's system, inverter
and input records stay in its asset as `withdrawn` under the same identifiers,
and its gate-rule relation instances are withdrawn. A withdrawn record says the
resource is known and not observed now. It is not an absence statement: the
gate states none, so the publisher publishes none and refuses one. Re-admission
makes the same identifiers current again. A replaced inverter withdrawn this
way is retired.

**Operator declarations and epochs (Proven, gateway tests).** A child or
logger batch that observes a binding carries a pending qualified operator
identity declaration when the declaration names that resource. A withdrawal
of a child carries the pending withdrawals of installed declarations and
leaves a qualified declaration for a binding it does not observe pending, so
the withdrawn records commit and that declaration stays pending. A successor
epoch carries a declaration that is still pending. Retiring an epoch removes
every gate-rule instance in the same snapshot and keeps the operator-origin
resources current.

**Consumers.** A semantic snapshot read of a logger child also shows its
withdrawn records while the child is withheld. The logger asset is a new asset
for every consumer that enumerates published assets; it carries a resource,
a binding and an identity link, and no fact. Prometheus, MCP and Portal
characterization goldens are unchanged.

**Not yet published.** The logger's `relation.relays` instances wait for the
SemReg change that names a core relation definition in a record. An absence
statement for a withheld child waits for a gate decision, because the PV gate
states that a withheld child is not absent. The SunSpec PV family's resources
follow in a later change, and the SunSpec PV class keeps its own graph table
entry off until then.
