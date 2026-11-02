# Semantic metric slot store

This internal gateway API implements durable assignment mechanics for the
[Thermal and Infrastructure renderer](../semantic-prometheus-thermal-infrastructure-v1.md)
and the accepted
[coverage contract](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/deployment/prometheus-semantic-coverage.md).
It adds no public metric schema or native protocol meaning. Nothing here is a
stability promise before Helianthus 1.0.

## Private API

`internal/semanticmetrics.O21Persistence` injects `Load(target)` and
`Commit(target, document)`. The target is an opaque local configuration identity,
never a metric label or a network address. One adapter/store owns one target
exclusively: the adapter must fence competing writers, including other processes
and store instances. It must bound reads, return detached bytes, and neither
retain nor mutate committed bytes. A successful commit means the whole document
has been atomically replaced and durably completed, including any required
filesystem metadata durability. Partial writes, uncertain acknowledgements and
lost writer fencing must return errors. These guarantees are adapter obligations;
this slice supplies no concrete filesystem implementation or path.

`O21Create` explicitly provisions a definitely absent target. Only
`fs.ErrNotExist` permits creation; corruption and other read errors never reset
reservations. Creation itself must complete durably. Missing state on restart
must be investigated by composition, never automatically recreated.

`O21Open` reads the document with version, exact target and complete `Binding`
identities, including retired entries. It rejects documents above 256 KiB,
unknown properties, trailing data, invalid version/target, unsupported domains,
out-of-range or duplicate slots, duplicate tuples, invalid identity members,
invalid dimensions and invalid, duplicate or cross-domain selected fields. It
uses the existing renderer configuration validator and admits at most 16 slots
in each of the thermal and infrastructure domains. The target is nonempty and
at most 256 bytes. It never imports a saved validation flag.

`Assign(target, binding)` reserves a caller-selected slot; it does not allocate
on scrape. Assignments are immutable. A changed identity uses a new unused slot.
`Retire(target, binding)` requires the complete original non-retired binding and
persists its tombstone. Neither an active nor a retired slot can be reused for
that target. Tombstones have no expiry or deletion API. Each domain eventually
exhausts after 16 reservations, independently of the other domain.

`Export()` returns a detached `Registry`. `Assign` and `Retire` also return
detached registries only after durable completion, with `Validated=false`.
Before invoking persistence, a transition clears validation; concurrent exports
retain the last accepted assignments but withhold all values. Successful
persistence installs the proposed document. Any persistence error leaves the
last accepted assignments intact, latches uncertainty and prohibits further
writes or revalidation in that instance. Reopen through the adapter is required
to discover what actually persisted; corrupt or missing state still fails
closed. Previously returned registries are detached snapshots, so composition
must not cache an earlier validated registry across transitions.

`Revalidate(target, bindings)` is an explicit current-owner attestation of the
entire exact ordered binding list, including tombstones, slot, domain, asset,
source, source epoch, native binding, driver generation, service instance,
dimensions and selected fields. The caller must obtain current owner evidence
before calling it. Exact equality deliberately rejects reorderings. A mismatch
clears validation without changing assignments or writing persistence. An
uncertain instance cannot be revalidated. This API does not itself consult a
native owner or turn stored identities into qualified observations. The renderer
still requires current admitted, qualified, fresh and valid semantic evidence
for each field and preserves its existing independent missing-value behavior.

## Composition still required

`gatewaySemanticMetricRegistry` remains empty. A later slice must supply durable
storage, exclusive ownership, explicit target lifecycle and missing-store policy,
current-owner revalidation, and registry refresh/invalidation at the renderer
boundary. It must serialize transitions with consumption of exported registry
snapshots. No configuration, metrics route, automatic repair, acquisition,
filesystem path, deployment wiring, dependency or public API is added here.
MCP compatibility, GraphQL parity, native evidence and per-field last-known-good
state remain unchanged.

## Evidence classification

**Proven** by focused synthetic `TestO21` fixtures when run successfully:
durable-before-return behavior, withholding during transitions and uncertainty,
full identity comparisons, detached output, permanent retirement, domain
capacity, corrupt-load refusal, unvalidated restart and a zero-valued sample
only after explicit synthetic owner revalidation.

**Hypothesis**: a future adapter can satisfy atomic durable replacement and
exclusive-writer requirements on the selected deployment filesystem. No concrete
adapter is qualified by these tests.

**Unknown**: production composition, crash/filesystem fault behavior, live owner
reattachment, device behavior and physical acceptance. Proven/Hypothesis/Unknown
are evidence grades, separate from qualification, freshness and physical
acceptance. Full repository CI and committed-change review remain separate gates.
