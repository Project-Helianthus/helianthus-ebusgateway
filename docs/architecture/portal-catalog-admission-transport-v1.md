# Portal catalog and action admission transport v1

`helianthus.gateway.portal-catalog/v1` is a Gateway-owned, immutable and
detached Portal read model. It is served only by `POST /graphql/portal/v1` with
the fixed operation names `PortalCatalogV1` and `PortalActionInvokeV1`.

Catalog reads use committed PV, Storage and EVSE snapshots only. Thermal/HVAC
and Infrastructure remain source-absent until an accepted detached source is
registered. They do not call a transport,
native acquisition path, MCP, the existing GraphQL endpoint, M2M GraphQL,
Prometheus, a publisher, or an operation path. The catalog always includes the
five registered semantic packs. A source-absent domain is `ABSENT`, has no
invented resource, field, or enabled action, and does not turn missing values
into zero.

The host computes contribution digests from canonical validated descriptors.
Two descriptors with equal `(driver_id, manifest_id, manifest_version)` and a
different computed digest are quarantined together and neither is rendered.
Gateway retains those accepted descriptors in a long-lived generation-fenced
contribution registry. A detached source owner publishes its complete generation
when it starts and withdraws that exact generation before it stops; a stale
shutdown cannot remove a successor. Each catalog read takes one detached
registry snapshot, so a driver addition, replacement, or withdrawal cannot
produce a partial contribution set. A new valid driver uses this registry
publication boundary and does not require a Portal handler or bootstrap switch
edit.

An asset may hold several sources: the owner, whose driver published the
descriptor, and declared contributors. The catalog groups the service instances
of the descriptor's service by the source of their binding and applies the
single-source rules to each source: one service instance, one capability
instance on it, and a current binding and source epoch that agree with both. It
keeps the owner's path when the owner carries the capability, otherwise the path
of the one contributor whose binding carries it, and attributes the resource and
its actions to the driver of that binding. A contributor's path is kept only
under that driver's own admitted descriptor for the same pack, resource context,
service and capability, so the action, its claim and its binding, source epoch
and generation all name the contributor. The driver of a source is resolved
through the publication hub's source records when the composition supplies
them; without that resolution only an asset with one current source is
attributed, to the descriptor's driver, which keeps single-source output
unchanged. A resource that is not kept is never dropped silently: the catalog
source reports its domain, asset, resource context, descriptor and one reason
(`descriptor_unusable`, `binding_unresolved`, `capability_unbound`,
`capability_ambiguous`, `contributor_driver_unresolved`,
`contributor_unadmitted` or `source_record_incomplete`) as a capture
diagnostic. The catalog wire contract is unchanged.
The action-stability catalog revision binds accepted contributions and
quarantines, all resource and field bytes, action eligibility, source
snapshot/revision/binding/epoch/generation vectors, lifecycle fence, pack refs,
authorization scope, and every nonvolatile evaluation fact. It deliberately
excludes the top-level `evaluation_instant`, each source evaluation
`evaluated_at` and `evaluate_monotonic` clock coordinate, and the derived
`evaluation_digest`; equivalent detached reads therefore remain invocable while
their source evaluation clocks advance. Freshness, expiry, source revisions and
all other evaluation facts remain bound and are revalidated at invocation. The
catalog digest additionally binds all caller-visible bytes and evaluation time.

Actions are caller-scoped previews. A hidden action is omitted. Invocation
requires a fresh request-bound caller and rechecks the catalog claim,
contribution identity/digest, resource/capability, snapshot/revision, binding,
source epoch, generation, deadline and idempotency key. A gateway adapter must
then admit the exact SemReg operation and exact DriverManager/native binding
before one native invocation. This increment installs no trusted Portal caller
adapter, so the runtime default exposes no actions and rejects invocation.

This contract does not add a renderer, legacy GraphQL compatibility path,
native source acquisition, production authentication, or live control.
