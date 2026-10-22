# Read-only Portal contributions and path-prefix embedding

Status: integration contract for issue [#1129](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1129).
This is a partial slice of [#999](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/999)
(Project #2 INT-10); it does not close the parent. The descriptor and bootstrap
transport is implemented by [#1172](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1172).
Handler prefix support and the shared semantic read provider wiring are
implemented; production/Ingress forwarding remains separate integration work.
This is not an available universal lifecycle/configuration API or a stability promise.

## Existing seams and remaining work

Reuse the [driver descriptor](portal-driver-contribution-contract-v1.md) and
[catalog transport](portal-catalog-admission-transport-v1.md), rather than
introducing another manifest language. Their schemas, validation, canonical
ordering, full contribution identity and quarantine rules remain authoritative.
This read-only slice adds no action admission or invocation path.

The implementation comparison is:

| Existing source | Current behavior | Integration requirement |
| --- | --- | --- |
| `portal/handler.go`, `Options` | Validated endpoint paths, trusted external prefix, host catalog availability and Portal base path; optional detached shared-view callback and legacy semantic callbacks | Keep existing options compatible; production sets the shared-view callback and advertises the mounted snapshot operation together. |
| `cmd/gateway/gateway_http_server.go` | Mounts the Portal with `StripPrefix`; redirects the bare mount to its trailing slash; mounts catalog separately at `/graphql/portal/v1` | Preserve route ownership; external prefix rewriting must cover both mounts. |
| `portal/static/index.html` | Relative `assets/app.css` and `assets/app.js` | Resolve from the canonical Portal directory. |
| `portal/handler.go`, bootstrap | Host-composed endpoints using the external prefix and Portal base path | Preserve handler composition; production/Ingress forwarding must cover advertised routes. |
| `portal/web/src/app.js` | Relative health/bootstrap fetches and SSE URL, bootstrap catalog endpoint, capability-based legacy sections | Contribution cards use one generic renderer; advertised endpoints become authoritative without root-path fallbacks. |
| `cmd/gateway/portal_catalog_contributions.go` | Generation-fenced descriptor publication and withdrawal | Reuse the registry; no per-family handler/bootstrap switch. |

The existing sixth-contribution example is in
`portal/web/test/portal-contribution-v1.test.mjs`; it proves prototype navigation,
not production M07 transport integration. The current catalog frontend also
validates the fixed five-domain catalog; it does not yet accept an arbitrary
sixth production domain. Generic shared-read card wiring and its executable
production coverage remain separate implementation work. This page supplies the missing shared
read binding and external URL rules. No existing predecessor is replaced.

## Small read-only interface

The host composes three inputs: an admitted descriptor registry snapshot,
public-safe detached semantic reads, and trusted mount configuration. A card
registration consists only of an existing descriptor identity
`(driver_id, manifest_id, manifest_version)`, its groups/fields/views, and a
host-owned binding from resource/capability to opaque asset IDs. The binding is
registration data, not a new member of the closed descriptor schema. Asset IDs
and semantic definition references are never parsed as native addresses.

The conceptual adapter has just two read operations:

- `describe()` returns the detached accepted registry snapshot and its revision.
- `read(asset_ids)` returns the shared `assets` view defined by
  [M07: shared SemReg snapshot read](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/api/semantic-snapshot-read.md).

These are interface responsibilities, not exported Go APIs. Providers perform
no discovery, acquisition, publication, decoding or operation dispatch. Capture
registry/bindings and the selected publication pointers coherently; reject a
changed generation instead of combining mismatched descriptor and data state.
No provider can supply executable components, routes, query text or decoding.
The host owns rendering, navigation, accessibility and transport policy.

### Catalog and read coherence

The browser issues two independent requests: `PortalCatalogV1` at the catalog
endpoint, then `semanticSnapshot` at the ordinary GraphQL endpoint. Coherence
between them is never assumed. The shared read is the sole authority for field
values; catalog `fields[].value` is descriptive only and is never displayed as a
second value.

Coherence uses a clock-independent source identity, not the evaluation digest.
Each request reevaluates the same stored snapshot at its own instant, so its
evaluation clocks (`evaluated_at`, `evaluate_monotonic`) and the evaluation
digest over them differ between any two requests even when the underlying data
is unchanged. An exact digest comparison across independent requests would
therefore never converge and would hide unchanged data. **Proven** from the
current code: the Storage current-read reevaluates at each supplied instant
without changing the stored snapshot or its revisions, and the catalog digest
covers the whole evaluation including those clocks.

Participating fields, compared exactly, from the catalog resource `source` and
the shared read's per-asset entry: asset ID, snapshot ID, the complete snapshot
revision vector, driver generation, source epoch and binding ID. Excluded
fields: the evaluation digest, `evaluated_at`, `evaluate_monotonic` and any other
evaluation clock coordinate. No digest is compared: the catalog's
`source.evaluation_digest` is a catalog-local cache key over the catalog's own
marshaled evaluation, not an identity shared with the read (whose SemReg
`evaluation_digest` carries a `sha256:` prefix and is a different value). Render freshness and disposition solely from the
shared read's captured evaluation, selections and projection, never from the
catalog's evaluation or eligibility. Validate the shared tuple's internal
snapshot/revision and evaluation references under M07; cross-request evaluation
equality is not required. Before rendering any value,
the client requires the participating fields to be equal. Sequential reads of
unchanged data then converge to equal identities. A new publication or driver
generation changes the identity, which is the intended mismatch.

Every 64-bit identity member in the compared set, notably the driver generation,
is a decimal string on both sides and is compared as a string, never as a number:
a browser parses JSON numbers as doubles and rounds beyond 2^53, while the shared
record keeps the SemReg decimal string. **Proven** from the current code: the
catalog contract and browser validator use a canonical unsigned decimal string
for `driver_generation`. Numeric JSON generations, empty strings and leading
zeros are rejected by the browser. The Matter feed's embedded Portal source
schema uses the same string representation; its native feed generation fields
retain their existing representation.

On mismatch the client refetches both requests, at most two times. While
refetching it shows an inconsistent/refreshing state and retains previous valid
cards explicitly as historical, withholding the mismatched response's values.
Only three confirmed mismatched pairs enter the generation-conflict state below;
an inconclusive HTTP, timeout or malformed-response failure during a retry keeps
the historical cards with a refresh error. It never
presents rows from one response under the identity of the other. Identities are
compared exactly and never recomputed from the selected GraphQL response. The
shared GraphQL read is exposed through the shared provider; the catalog side
already carries every participating field. Cross-request identity comparison
remains the consumer contract described here.

Implementation acceptance must exercise sequential catalog/shared reads:

| Sequence | Required result |
| --- | --- |
| Unchanged Storage publication; evaluation clocks and digests advance on every request | Publication/lifecycle identities match; render on the first pair without refetching. Repeated pairs remain readable. |
| Same publication; shared evaluation crosses a freshness boundary after catalog capture | Render the shared read's updated freshness/disposition, including withholding values when required by M07; do not retain catalog freshness or refetch solely for evaluation drift. |
| Publication, binding, source epoch or driver generation changes between requests | Withhold mismatched values and refetch both within the bound, retaining previous valid cards as historical; render once a pair matches. Three confirmed mismatches become a generation conflict; an inconclusive retry failure retains historical cards. |

These are required tests for the future shared-read implementation, not claims
of current production coverage.

Use M07's 1–32 distinct, byte-exact opaque asset IDs, coherent request capture,
4 MiB whole-view limit and conflict/error semantics. Each readable entry carries
`asset_id`, complete `snapshot`, `evaluation`, `projection`, and `selections`.
Each asset has its own `snapshot_id` and complete revision vector; there is no
global SemReg revision. Preserve exact candidate identity/revision and all
FactKey dimensions when joining fields, evaluation and projection. A catalog
revision is not a substitute for those source identities.

An unavailable entry keeps `asset_id`, `selections: []`, and the M07
`unavailable_reason`, with no fabricated snapshot/evaluation/projection.
Withhold an entire unsafe asset as M07 requires while preserving readable
siblings. Do not hide a failed asset by returning a shorter success array.
Copy quality axes independently, including qualification and promotion;
**Proven**, **Hypothesis** and **Unknown** are documentary evidence labels,
not replacements for machine quality. Preserve projection losses, reasons,
alternatives, conflicts, retained observations, evidence access/redaction and
lifecycle fences. Native evidence remains with its owner; an evidence reference
is not permission to retrieve its body. Native interpretation requires its
public native documentation first; this interface defines no native facts.

Contribution cards plan the complete expansion before formatting rows or building
HTML, with a shared budget of 512 DOM nodes across all resources. Card headings,
view labels, fact and audit rows, projections, diagnostics and unavailable or
historical notices all consume that budget, including repeated references.
After node planning, a shared 4 MiB UTF-8 HTML budget counts markup, headings,
row text and HTML escaping across all resources, including repeated audit records.
Each row is checked before allocating its escaped HTML; overflow stops further
formatting and discards the partial result. An oversized expansion fails closed
through the unavailable-read path; a prior valid view remains explicitly historical.

Contribution cards restrict view fields to the resource's domain, service and
capability: the field, service and capability pack IDs must match the domain,
and the service and capability IDs must match the resource. View membership
alone does not authorize rendering another context's fields.

Contribution cards first filter candidates by the resource's exact binding ID,
source epoch and decimal-string driver generation, for both default choices and
explicit selections. A selection outside that source tuple withholds the value
without falling back to a matching sibling. Distinct selection policy IDs or
versions for the same fact key coalesce only when every selection agrees on the
candidate identity and revision. Conflicting choices or duplicate policy tuples
withhold the value.
Contribution cards check the selected candidate itself: qualification must be
`qualified`, promotion `promoted`, and validity `good` or `suspect`, in addition
to the shared evaluation and projection checks. A presentation selection cannot
borrow eligibility from another candidate that makes the fact projection
`exact`. Cards keep the selected candidate's source binding, epoch, generation,
origin, causal context, times, derivation and evidence references, including
access/redaction metadata, alongside the fact's conflict records and competing
candidate identities. These details remain visible when the value is withheld;
all are escaped text and no evidence body is fetched.

Render decimal coefficient/exponent and canonical units without floating-point
conversion; preserve string revisions/time coordinates. Missing values stay
missing, while zero, false and empty text remain present. When a requested fact
is absent, show its value as unknown and retain matching shared projection
outcomes, reasons and losses. Match the fact item ID and, when source keys are
present, the requested pack ID, pack version and fact ID; source-less dispositions
remain visible for that item. Evaluate freshness
from the captured evaluation, not browser receipt time. Valid partial
publications retain omitted last-known fields and their original aging rules;
rejected publications replace nothing. Retained or stale values are visibly
historical, never current availability or capability authority.

### Synthetic sixth contribution

Start with the existing five-domain synthetic fixture. Register an additional
valid descriptor using the identity `fixture.sixth / portal.fixture.sixth / 1.0.0`,
an existing admitted pack's field/service/capability/unit references, a `summary`
view, and no actions. Add a resource binding to `asset:fixture:sixth` and a
public-safe M07 asset entry with matching snapshot/evaluation/projection
identities. Reusing an existing pack exercises contribution extension without
pretending to define a sixth semantic pack.

The generic pipeline validates the descriptor, resolves the binding, reads the
selected asset and renders the registered summary. Only registration/data
changes: no family switch, new capability boolean, section ID, endpoint, query
per family or renderer branch. Sort by the existing descriptor `(order,id)`
rules and M07 canonical data orders. Reject conflicting full identities; never
select by manifest ID alone. Unsupported view types reject that contribution
without removing valid siblings. Withdrawal removes that generation's cards;
a late withdrawal cannot remove its successor. If the selected resource goes
away, choose an available registered resource deterministically and announce
the change; cancel its pending reads/stream listeners and ignore late replies.

## Coherent capture and shared provider

**Proven** from `internal/semhub/coherent_capture.go`: `Hub.Capture` takes one
hub read lock across the selected assets. It reads the current admitted epoch
and returns detached copies of selected publications with their lifecycle,
read-admission and public-exposure decisions, matching evaluation context and
catalog metadata. Evaluation uses the supplied time. The capture is read-only;
`Inspect` remains an independent ungated audit surface, and the existing
admission predicates are unchanged. This internal API introduces no public
schema change.

A missing hub or restore in progress returns `ErrCaptureUnavailable`, without a
view. Cancellation, invalid or duplicate asset IDs, selections outside the
1–32 asset bound, metadata construction and evaluation failures return errors.
Within a successful capture, an unavailable asset has no snapshot or evaluation
and carries one of these reasons:

- `semantic.read.reason.asset_unknown`: the asset is not declared.
- `semantic.read.reason.source_unavailable`: its source is not currently admitted
  and live, or is fenced or retired.
- `semantic.read.reason.snapshot_unavailable`: no current publication exists.
- `semantic.read.reason.publication_withheld`: restore/read admission is closed,
  the publication does not belong to the current source epoch and generation,
  or its evidence is neither public nor metadata-only.

**Proven** from `cmd/gateway/semantic_snapshot_provider.go` and
`cmd/gateway/gateway_http_server.go`: HTTP composition injects the same
`hubSemanticSnapshotProvider` instance into GraphQL `semanticSnapshot`, MCP
`semantic.v1.snapshot.get` and the Portal shared-view callback. The provider
currently obtains its hub from the composed eBUS runtime bridge. Without that
hub it returns `ErrCaptureUnavailable`; no replacement publication is fabricated.
Each read captures once, then calls `semanticview.Build` with the returned
metadata and inputs. GraphQL and MCP accept an explicit bounded asset selection.
Portal uses `Hub.AssetIDs`, a detached, bytewise-ordered list of declared assets;
selection itself grants no admission. The callback preserves the capture and
builder errors, including bound failures, rather than truncating the selection.
Portal bootstrap checks the mounted-operation flag and callback together and
does not capture data. The shared read performs no acquisition, publication or
hub mutation.

**Proven** by `TestM11PortalProductionOptionsSharedView` and
`TestM11PortalProductionOptionsUnavailable`: the production options helper feeds
the mounted Portal handler, bootstrap advertises the shared operation, an
admitted publication produces the same view as the shared provider for the same
assets, and a provider without a hub returns the unavailable error and no view.
The tests also check one capture per callback and unchanged stored publication.
Production/Ingress forwarding and live-device behavior remain **Unknown** from
these local fixtures.

## Bootstrap transport

Keep `GET <portal-base>api/v1/bootstrap` as the bootstrap discovery entry. An
additive `portal_read` block is emitted as host-produced transport metadata:

```json
{
  "portal_read": {
    "base_path": "/console/portal/",
    "snapshot_operation": "semanticSnapshot",
    "stream_endpoint": null,
    "unavailable_operations": ["semanticSnapshot"]
  }
}
```

The handler emits this block. `base_path` combines the trusted configured
external prefix and Portal directory mount. Handler tests cover prefixed shell,
assets, bootstrap and endpoint construction; this does not establish
production/Ingress forwarding or shared semantic read provider availability.
The existing bootstrap
`endpoints.portal_catalog` and `endpoints.graphql` members remain the authoritative
catalog and shared-read URLs; no separate endpoint fields are introduced.
`PortalCatalogV1` emits `descriptors`, an array keyed by full contribution
identity. Each entry carries `descriptor_version`, groups, fields, labels,
views, diagnostics and the admitted canonical digest so consumers can cache by
identity. The catalog schema includes this member. Every admitted diagnostic
preserves `id`, `group`, `label`, `member_id`, `kind` and `order`, allowing each
view's `diagnostic_ids` to resolve within the same descriptor entry;
`diagnostics: []` remains present when empty. Diagnostic definitions describe
metadata and do not expose actions or enable invocation. Descriptors and resources
share one catalog capture instead of requiring a separate describe operation.
The in-process read adapter consumes the admitted contribution registry and a
shared detached semantic view when supplied; production serves its catalog on
the outer gateway mux. The host explicitly declares that mounted catalog
available in bootstrap. Production also supplies `SharedSemanticView` and
sets `SemanticSnapshotAvailable` through `configureGatewayPortalSemanticRead`,
so bootstrap no longer lists the mounted `semanticSnapshot` operation as
unavailable. This advertisement describes wiring, not a promise that every
capture or asset is available.

Obtain the shared read through ordinary GraphQL `semanticSnapshot(assetIds:)` on `endpoints.graphql`
only once M07 exposure and applicable parity gates are accepted. Ask for the
complete metadata needed above; recursively restore SemReg member names before
treating GraphQL data as a kernel record. Never recompute source IDs/digests
from a selected GraphQL response. MCP is not a browser fallback.

Shared endpoints stay advertised whenever any operation on them is usable.
`endpoints.graphql` also serves the existing Portal GraphQL operations and
`endpoints.portal_catalog` has independent consumers, so an unwired snapshot or
catalog provider never removes them.
`portal_read.unavailable_operations` is emitted as an operation-level list
naming unavailable `semanticSnapshot` or `PortalCatalogV1` operations, which
consumers consult before issuing them. A configured endpoint alone does not
establish availability; a host-mounted catalog or the in-process registry/index
adapter establishes catalog availability. When an operation is unavailable, do not fabricate an empty domain or use the legacy
eBUS semantic snapshot. Omit an endpoint only when none of its operations is
usable. Existing legacy bootstrap members retain their meaning. The catalog
endpoint does not acquire a new operation implicitly.
All MCP namespaces remain pre-stable until 1.0; existing eBUS envelopes,
classification, determinism/goldens and MCP-to-GraphQL graduation/parity rules
remain in force. This page grants no graduation or namespace change.

## Exact external URL construction

Let `P` be a trusted, configured external prefix: empty or an absolute path
with no trailing slash. Let `M` be the internal Portal mount with exactly one
leading slash and no trailing slash (normally `/portal`); `Options.BasePath`
represents that directory with its trailing slash (normally `/portal/`).
The root mount uses `M = ""` and `Options.BasePath = "/"`. The public Portal
directory is `B = P + M + "/"`. Configuration rejects schemes, authorities,
queries, fragments, backslashes, repeated slashes, dot segments and percent
escapes in `P`/`M`; do not repair ambiguous input. Neither request headers nor
contribution data choose these paths. Validation errors identify only the invalid
`Options` field from the closed list `ExternalPrefix`, `BasePath`, `GraphQLPath`,
`PortalCatalogPath`, `SnapshotPath`, `SubscriptionPath`, `MCPPath` and
`EEBusAdminPath`; they never include the supplied path value. Invalid
configuration is rejected rather than normalized. The HTTP failure response
remains generic; these field diagnostics belong to path validation.

For fixed relative Portal suffix `s` (no leading slash), construct `B + s`.
For a configured gateway endpoint `E` (an absolute internal path, validated by
the same rules), construct `P + E` exactly once. Do not prepend `B` to a
GraphQL endpoint or prepend `P` again to an already external bootstrap URL.
The host must apply this rule to every advertised endpoint, including existing
options-derived paths. Empty/disabled endpoints remain absent, never `P` alone.

| Resource | `P = ""`, `M = "/portal"` | `P = "/console"`, `M = "/portal"` |
| --- | --- | --- |
| Bare mount redirect target | `/portal/` | `/console/portal/` |
| Static module | `/portal/assets/app.js` | `/console/portal/assets/app.js` |
| Static stylesheet | `/portal/assets/app.css` | `/console/portal/assets/app.css` |
| Health | `/portal/api/v1/health` | `/console/portal/api/v1/health` |
| Bootstrap | `/portal/api/v1/bootstrap` | `/console/portal/api/v1/bootstrap` |
| Ordinary GraphQL | `/graphql` | `/console/graphql` |
| Portal catalog | `/graphql/portal/v1` | `/console/graphql/portal/v1` |
| Legacy SSE | `/portal/api/v1/stream?max_events_per_second=2&interval_ms=1000` | `/console/portal/api/v1/stream?max_events_per_second=2&interval_ms=1000` |

Redirect the bare mount before loading relative assets. Support only the
canonical directory and `index.html` shell entry; card navigation is in-page
state, not an invented deep server route. Construct local fragments with
encoded opaque IDs and leave the directory unchanged. Use URLSearchParams for
query values; never concatenate unescaped asset IDs into paths or queries.
Resolve shell-relative assets/initial health/bootstrap against `B`, even when
entering via `index.html`. After bootstrap, use its host-composed external
endpoints verbatim, validated as same-origin paths within configured mounts.
Reject protocol-relative, cross-origin or escaping URLs rather than falling
back to `/graphql`, `/portal`, or a native endpoint.

An embedding proxy strips `P` once before forwarding, preserves the Portal
mount for the gateway's existing `StripPrefix(M, ...)`, and routes sibling
GraphQL endpoints under the same external prefix. It must also preserve the
external redirect target. The handler already composes prefixed bootstrap paths;
configuring it alone does not establish production/Ingress forwarding. That
integration still requires its own implementation and acceptance.
The host retains its listener, proxy, TLS, origin and trust configuration.
Prefix handling neither relaxes those controls nor introduces a new trust source.

The table above gives examples, not the complete allowlist. The rule is: a
capability advertised by the embedded bootstrap implies every route it uses is
forwarded. The shell (`portal/web/src/app.js`) fetches many relative `api/v1/`
routes (registry, semantic, observability, timeline, provenance, projection,
snapshots, sessions, search, ebus-standard, explorer and others), so the
forwarded set must not be a hand-maintained short list. The implementation slice
derives the allowlist from the shell's route table as one generated list, and
adds a test that every relative `api/v1/` route the shell fetches is either
forwarded or belongs to a capability the embedded bootstrap disables. Routes that
mutate state or reach native endpoints (explorer scans and raw reads, for
example) belong to capabilities the read-only embedded bootstrap disables and
omits, not to the forwarded set. This is **Unknown** in current code: no such
list or test exists today.

Production/Ingress integration must omit endpoints the entry point does not
forward, for example `mcp` and `eebus_admin`, rather than advertise them under the
prefix. This omission remains separate entry-point work; the current handler
prefixes configured non-empty endpoints and does not establish forwarding.
This route allowlist requirement adds no authorization gate. It avoids
accidental administrative-route exposure ([#999](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/999))
and keeps native protocol endpoints off Ingress
([ha-addon #239](https://github.com/Project-Helianthus/helianthus-ha-addon/issues/239)).

## Stream and error states

The existing SSE route remains a legacy Portal stream; its presence does not
prove shared SemReg snapshot invalidation coverage. Until such coverage exists,
`portal_read.stream_endpoint` is null. Render read snapshots and offer explicit
refresh without opening an invented stream or probing native data.

When a shared-read stream is separately implemented, bootstrap may advertise
its exact external URL using the same construction rules. Stream messages are
invalidation hints; refresh the detached read, never patch authoritative fields
from arbitrary event payloads. Bound/coalesce refreshes, close the old stream on
navigation/disconnect, and fence late responses by lifecycle and request order.
Reconnect only to the same advertised URL, preserving prefix and query policy.

| Condition | Required visible behavior |
| --- | --- |
| Initial read pending | Loading state; no fabricated values. |
| Bootstrap/URL validation failure | Bootstrap unavailable; no endpoint fallback or stream start. |
| Catalog/read HTTP failure, malformed response or GraphQL errors | Read unavailable; retain previous valid cards as last-known with an explicit refresh error, never merge an inconsistent result. |
| M07 per-asset unavailable reason | Show the reason for that asset; valid siblings remain readable. |
| Stream disabled | Live updates unavailable; explicit read refresh remains usable. |
| Stream disconnect/parse failure | Mark disconnected/degraded, retain last-known data with source freshness; never mark it fresh on reconnect. |
| Generation conflict (catalog/read identity mismatch persisting after the bounded refetch, or a changed registry/publication generation) or whole-read limit exceeded | Display the inconsistent/contract error and no values; do not splice/truncate a successful view. |

Component reattachment preserves previous valid semantic cards as historical while
the new bootstrap and refresh run. A refresh failure retains those cards; a
successful replacement or a conflict/limit result replaces them. Every pending
refresh keeps retained cards explicitly marked historical, including when their
source freshness is fresh. A new lifecycle clears shared-read authorization;
Refresh issues no shared reads until its bootstrap supplies that authorization.

Structured GraphQL errors with `extensions.code` of `CONFLICT` or
`RESOURCE_EXHAUSTED` use the no-values conflict/limit state, including after a
previous successful read.

## Already trusted external caller context

A host adapter may attach an opaque, immutable, request-scoped context value
representing an already trusted external caller. This is an in-process value,
not a browser identity object, bootstrap field, manifest field, principal, role
or permission list. Absent context means no external context; it does not deny
these read-only cards. Do not serialize, log, persist or reuse it across callers.
Only the existing host trust boundary can populate it; arbitrary identity,
forwarding or prefix headers never become trusted merely by reaching Portal.

Do not add users, principals, roles, login redirects or an authorization gate
before the 0.11 authentication and authorization frameworks. Preserve existing host trust configuration and native qualification,
read/write safeguards and fail-closed action admission. Existing catalog action
interfaces are separate: the catalog has no Authorizer hook or caller
`authorization_scope` field. This read-only adapter does not turn caller presence
into permission, expose actions or enable invocation.
The current runtime default with no trusted action adapter continues to expose
no actions and reject invocation.
