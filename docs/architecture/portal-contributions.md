# Read-only Portal contributions and path-prefix embedding

Status: proposed integration contract for issue [#1129](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1129).
This is a partial slice of [#999](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/999)
(Project #2 INT-10); it does not close the parent. It specifies future wiring,
not an available universal lifecycle/configuration API or a stability promise.

## Existing seams and remaining work

Reuse the [driver descriptor](portal-driver-contribution-contract-v1.md) and
[catalog transport](portal-catalog-admission-transport-v1.md), rather than
introducing another manifest language. Their schemas, validation, canonical
ordering, full contribution identity and quarantine rules remain authoritative.
This read-only slice adds no action admission or invocation path.

The implementation comparison is:

| Existing source | Current behavior | Integration requirement |
| --- | --- | --- |
| `portal/handler.go`, `Options` | Configured GraphQL, catalog, snapshot, subscription, MCP and administration endpoint paths; legacy semantic callbacks | Keep existing options compatible; do not claim a shared snapshot callback already exists. |
| `cmd/gateway/gateway_http_server.go` | Mounts the Portal with `StripPrefix`; redirects the bare mount to its trailing slash; mounts catalog separately at `/graphql/portal/v1` | Preserve route ownership; external prefix rewriting must cover both mounts. |
| `portal/static/index.html` | Relative `assets/app.css` and `assets/app.js` | Resolve from the canonical Portal directory. |
| `portal/handler.go`, bootstrap | Options-derived endpoints plus fixed `/portal/api/v1/...` endpoints | Replace fixed external paths with host-composed paths when implementing embedding. |
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

Render decimal coefficient/exponent and canonical units without floating-point
conversion; preserve string revisions/time coordinates. Missing values stay
missing, while zero, false and empty text remain present. Evaluate freshness
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

## Bootstrap transport

Keep `GET <portal-base>api/v1/bootstrap` as the bootstrap discovery entry. An
additive target `portal_read` block is host-produced transport metadata:

```json
{
  "portal_read": {
    "base_path": "/console/portal/",
    "catalog_endpoint": "/console/graphql/portal/v1",
    "snapshot_endpoint": "/console/graphql",
    "snapshot_operation": "semanticSnapshot",
    "stream_endpoint": null
  }
}
```

This block is proposed, not emitted by the current handler. Fetch descriptors
through the existing fixed `PortalCatalogV1` operation on the catalog endpoint;
obtain the shared read through ordinary GraphQL `semanticSnapshot(assetIds:)`
only once M07 exposure and applicable parity gates are accepted. Ask for the
complete metadata needed above; recursively restore SemReg member names before
treating GraphQL data as a kernel record. Never recompute source IDs/digests
from a selected GraphQL response. MCP is not a browser fallback.

If either provider is unwired, advertise that read capability as unavailable
and omit its usable endpoint; do not fabricate an empty domain or use the
legacy eBUS semantic snapshot. Existing legacy bootstrap members retain their
meaning. The catalog endpoint does not acquire a new operation implicitly.
All MCP namespaces remain pre-stable until 1.0; existing eBUS envelopes,
classification, determinism/goldens and MCP-to-GraphQL graduation/parity rules
remain in force. This page grants no graduation or namespace change.

## Exact external URL construction

Let `P` be a trusted, configured external prefix: empty or an absolute path
with no trailing slash. Let `M` be the configured Portal mount with exactly one
leading slash and no trailing slash (normally `/portal`). The public Portal
directory is `B = P + M + "/"`. Configuration rejects schemes, authorities,
queries, fragments, backslashes, repeated slashes, dot segments and percent
escapes in `P`/`M`; do not repair ambiguous input. Neither request headers nor
contribution data choose these paths.

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
external redirect target. Configuring `M` alone does not fix today's fixed
bootstrap paths; production prefix support needs a separate implementation.
The host retains its listener, proxy, TLS, origin and trust configuration.
Prefix handling neither relaxes those controls nor introduces a new trust source.

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
| Generation conflict or whole-read limit exceeded | Display the contract error; do not splice/truncate a successful view. |

## Already trusted external caller context

A host adapter may attach an opaque, immutable, request-scoped context value
representing an already trusted external caller. This is an in-process value,
not a browser identity object, bootstrap field, manifest field, principal, role
or permission list. Absent context means no external context; it does not deny
these read-only cards. Do not serialize, log, persist or reuse it across callers.
Only the existing host trust boundary can populate it; arbitrary identity,
forwarding or prefix headers never become trusted merely by reaching Portal.

Do not add users, principals, roles, login redirects or an authorization gate
before 0.9. Preserve existing host trust configuration and native qualification,
read/write safeguards and fail-closed action admission. Existing catalog action
interfaces are separate: this read-only adapter does not implement an authorizer,
turn caller presence into permission, expose actions or enable invocation.
The current runtime default with no trusted action adapter continues to expose
no actions and reject invocation.
