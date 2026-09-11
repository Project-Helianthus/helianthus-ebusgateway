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
The catalog revision binds accepted contributions and quarantines, all resource
and field bytes, action eligibility, source snapshot/revision/evaluation/binding/
epoch/generation vectors, lifecycle fence, pack refs, authorization scope and
evaluation instant. The
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
