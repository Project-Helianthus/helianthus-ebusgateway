# Portal configuration persistence and sharing state

Status: application design contract for [#1299](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1299),
a partial slice of [#1000](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1000),
[#1001](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1001)
and [#1002](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1002).
This defines subsequent implementation requirements. It adds no implemented
configuration endpoint, persistence reader, authorization system or plugin loader.

## Owners and precedence

[M01: managed sidecar composition](managed-sidecar-composition.md) owns compiled
driver registration, configured disablement, lifecycle reconciliation and shell
survival. [M21: Portal contributions](portal-contributions.md) owns read-only
contributions, coherent reads, host-composed paths and embedding. This record
neither replaces those contracts nor makes reserved driver hooks available.
The host retains listener, TLS, origin, path and native trust configuration.
Portal renders supplied state; it does not acquire evidence or define semantics.

**Proven**: the source anchors below exist. **Hypothesis**: the bounded record
and its application behavior specified here, until implemented and accepted.
**Unknown**: effective operation without current owner readback, managed
attachment or accepted native evidence. These evidence labels are independent
of candidate, qualified, promoted, unsupported and unknown native states.
Protocol facts require the public native documentation contract first; this
page introduces none. [SemReg](https://github.com/Project-Helianthus/helianthus-semreg)
continues to own protocol-neutral evaluation and publication.

| Record option | Current configuration/runtime owner | M01/M21 constraint on implementation |
| --- | --- | --- |
| `drivers[].enabled` | Compiled `Descriptor.Disposition` in [runtimecontrol types](../../internal/runtimecontrol/types.go), `Registry.Register` in [registry](../../internal/runtimecontrol/registry.go), `gatewayRuntimeControlOptions.Disabled` and `composeEBusPrimary` in [composition](../../cmd/gateway/gateway_run_setup.go); family settings remain in [Config](../../config.go) | M01 validates disabled descriptors too; disabled wins over durable RUNNING without rewriting lifecycle intent. Construct only enabled, supported, configured bindings through the shared manager/service. A sidecar not yet managed is unavailable for editing. |
| `sharing.graphql` | `Config.HTTPAddr`, `GraphQLPath` and handlers in [HTTP composition](../../cmd/gateway/gateway_http_server.go) | Proposed local data-sharing selection within host-provisioned routes. M21 bootstrap reflects operation availability; selection cannot create a listener, change a path, expose native routes or remove shell status. |
| `sharing.mcp` | `Config.MCPPath`, MCP server/tool registration in [HTTP composition](../../cmd/gateway/gateway_http_server.go) | Proposed local data-sharing selection; preserve M01 shell lifecycle/status tools even with failed drivers. M21 supplies no browser MCP fallback. |
| `adapter_proxy` | `Config.ProxyListenAddr`, generation-owned proxy readiness in [eBUS runtime](../../cmd/gateway/ebus_driver_runtime.go) | Independent native adapter exposure, never implied by GraphQL/MCP selection. M01 generation admission, withdrawal and close proof still apply. |
| `output_bindings.matter` | Dedicated `Config.M2MGraphQL` validation, [M2M runtime](../../cmd/gateway/m2m_graphql_runtime.go), [Matter feed composition](../../cmd/gateway/matter_binding_feed_v1.go) and [feed contract](matter-binding-feed-v1.md) | Local selection of an existing binding only. M01 current generation and evidence admission remain prerequisites; M21 catalog presence supplies neither native qualification nor effective output publication. |

These are application field names proposed here, not current CLI flags or new
public GraphQL/MCP members. An enablement edit is configured eligibility, not
START/STOP/RESTART; the existing service alone owns durable desired lifecycle
state and revision. M01 D1/D2/D3/D5, expected desired revision, explicit repair,
retry ownership and restore-before-open remain unchanged. If a configured
enablement cannot be applied dynamically, report restart required and leave its
effective state truthful; never invoke another manager or force RESTART.

Passive observation and cache are always-on application facilities: there are
no switches for either in this record. Turning sharing off changes exposure,
not observation, cache retention or native evidence. Driver disablement still
obeys M01 and opens nothing; always-on does not mean observing an unavailable
source, constructing a disabled driver or bypassing native trust. Existing
observations retain provenance and age; partial failures preserve unaffected
last-known-good fields. Cache contents never become fresh or qualified because
a sharing selection changed. Existing observe-first policy and cache settings
remain owned by [Config](../../config.go).

## Bounded desired record

The host owns one local application record, separate from lifecycle control
and recovery material. It stores only format, application revision, configured
driver IDs with boolean enablement, and the four sharing selections above.
There is no user-supplied factory, executable, route, protocol configuration,
asset selector, generic option map or extension payload. No secrets, endpoints,
credentials, paths, private observations or native identity belong here.

Synthetic desired record (a design example, not a supported import API):

```json
{
  "format": "helianthus.portal-config/v1",
  "revision": "1",
  "drivers": [{"driver_id": "ebus.primary", "enabled": false}],
  "sharing": {"graphql": false, "mcp": false},
  "adapter_proxy": false,
  "output_bindings": {"matter": true}
}
```

The closed shape requires every listed member, exact booleans and no nulls or
unknown members. Reject duplicate JSON object keys, duplicate driver IDs,
trailing content and unsupported formats. Revision is a canonical unsigned
decimal string, without leading zeros except `0`, bounded to unsigned 64-bit;
reject overflow instead of rounding or wrapping. IDs must match the existing
compiled, host-configured registry exactly; they never select a model or infer
an endpoint. Require an entry for every editable driver and no others. Sort
entries bytewise by ID when saving. A disabled entry still needs a valid
descriptor and native configuration under M01.

Limit the UTF-8 document to 64 KiB and 64 driver entries. Validate the complete
record, host constraints and supported selections before replacing anything;
do not silently discard an unknown field or partially apply a malformed batch.
An absent file is distinct from a failed read: use a host-supplied initial
record with all sharing/proxy/output selections false and only explicitly
provisioned driver enablement. Do not silently import legacy listener settings
as consent to broader sharing. Existing installations require explicit local
adoption; this contract does not change today's defaults by itself.

## Desired, effective and error views

Persist desired choices only. Derive effective state and errors from current
owners after load/apply; never trust effective flags recovered from disk.
Application revision is not a lifecycle desired revision, source epoch, driver
generation, SemReg revision or evidence identity. A successful save means
durable desired selection, never completed driver start or publication.

For every selection, show desired value (or unavailable when no valid record
loaded), effective boolean, status and sanitized reason. Status vocabulary is
`disabled`, `pending`, `effective`, `withheld`, `error`, or `restart_required`.
Effective true requires current positive readback from that option's owner;
unknown or absent readback yields false. After deselection, fence new use
immediately; report pending withdrawal and any still-active owner readback
until withdrawal is confirmed. Never label an unconfirmed native close disabled.

Synthetic detached status fragment; this is not persisted:

```json
{
  "revision": "1",
  "output_bindings": {
    "matter": {
      "desired": true,
      "effective": false,
      "status": "withheld",
      "reason": "evidence_unavailable"
    }
  }
}
```

GraphQL and MCP are ordinary application sharing, not an authorization policy
or native proxy. A selection cannot broaden the host's existing mounts, TLS,
origin policy, M2M asset filter or embedding allowlist. A host-disabled route
stays unavailable. An unavailable data operation must not remove the shell's
health, status, configuration-error display or M01 lifecycle tools. Keep M21's
operation-level unavailable metadata and host-composed URLs; invent no fallback.

Proxy enablement separately requires provisioned native configuration and an
admitted generation with positive listener readiness. An ordinary sharing
toggle neither binds a proxy nor grants raw reads or writes. Output binding
selection separately requires existing host configuration and target support,
current admitted source/binding identities, native qualification, and the
matching SemReg evaluation/projection with its freshness, availability and loss
rules. A running listener, feed response, catalog card or selected checkbox
does not prove effective publication. Pure projection is not a commissioned
output runtime. If the publisher is unimplemented or evidence is withheld,
effective publication is false with an actionable reason. Show per-resource
withholding; aggregate effective requires at least one confirmed current
publication and must never imply all resources are published. Evidence loss,
withdrawal or a generation conflict fences publication immediately, even when
desired remains true. Retained native history stays historical and cannot
reactivate a binding.

All MCP namespaces remain pre-stable before 1.0. Preserve eBUS envelopes,
deterministic ordering/hashes and golden snapshots, explicit invoke intent,
dangerous-operation and replay safeguards, and executable MCP-to-GraphQL
graduation/parity. Add no namespace or authorization gate before 0.9. Native
trust, qualification, read admission and write safeguards remain mandatory.

## Atomic persistence and recovery

One host-owned writer serializes complete-record replacements. Edits carry the
expected application revision; stale edits fail before persistence or runtime
effects, including for no-op requests. The server alone owns revision allocation;
the host-supplied initial record starts at `0`. Under the same writer lock,
validate the complete edit and compare desired selections with the accepted
record. A no-op returns the current accepted revision without writing, advancing
the revision or reapplying runtime effects. For a changing edit, allocate exactly
the accepted revision plus one and persist it with the complete replacement;
publish that revision in memory and return it only after durable acceptance.
Never accept a caller-selected successor or reuse a revision for different
accepted selections. At the unsigned 64-bit maximum, reject changing edits with
`revision_exhausted` before writing or applying; never wrap or reset the counter.
No-ops at the maximum still return that revision. Write a restrictive-permission
temporary file in the same directory, flush its contents, atomically rename over the destination
and flush the parent directory. Do not truncate the destination in place, copy
across filesystems as a fallback, follow an untrusted symlink or report success
before durable completion. Use a host-controlled storage location, never one
supplied by a descriptor, browser or this record. Existing
[runtime state persistence](../../internal/runtimestate/runtimestate.go) is a
source reference for storage mechanics, not the storage format or load policy
for this record. Preserve the separate lifecycle and recovery stores.

Apply only after durable acceptance. Runtime application is reconciled and
generation-fenced, not falsely atomic across independent native drivers. On a
failed write/flush/rename keep the last accepted in-memory desired record and
report persistence failure. If rename succeeded but directory flush failed,
durability is uncertain: do not apply new exposure, do not claim the old disk
record survived, and block further edits and new record-controlled admission.
Before resuming, the serialized writer must validate the destination and
successfully flush its parent directory, or complete an explicit repair through
the full durable replacement sequence, including a successful parent-directory
flush. Reread alone is insufficient. Reconcile the recovered durable record and
its revision before reopening admission; report failure while durability remains
uncertain and never reuse an uncertain successor for different selections.
On restart, ignore incomplete temporary files and validate the destination
before reconciliation. Recovery must never seed over an unreadable, corrupt,
unsupported or locked record.

At cold-start load failure, keep shell/error/status available; withhold the
record-controlled drivers and data/proxy/output exposure. Do not fall back to
permissive defaults or interpret missing effective state as enabled. During a
failed edit, keep the previous valid desired state and its already-admitted
effects, subject to current native evidence; never install any part of the
rejected edit. An external invalid replacement is not an accepted edit: keep
the last accepted desired view visibly historical and fence record-controlled
exposure until explicit repair. Unrelated shell state, caches and evidence
remain intact. No automatic rewrite, migration or retry may erase the bad
record; offer explicit local repair and revalidation without showing its body.

Errors contain a closed code, allowlisted field location and safe next step:
`invalid_record` (repair structure), `unsupported_format` (use supported format),
`unsupported_selection` (choose a provisioned option), `revision_conflict`
(reload), `persistence_failed` (check local storage), or `evidence_unavailable`
(inspect the owning driver's sanitized status); `revision_exhausted` requires a
future supported format migration, not a counter reset. Never echo supplied
values,
unknown key text, parser snippets, paths or underlying errors. Diagnostic
exports are built from an allowlist of format, safe state booleans, statuses
and closed codes; omit driver IDs, record bytes, revisions, raw errors, native
evidence, endpoints, secrets, private runtime data and caller context. Do not
export this record or a generic runtime/configuration dump as a diagnostic.

## Subsequent implementation acceptance

Offline tests must cover valid load/save, server-owned successor persistence and
returned revision, concurrent stale-edit conflict, no-op revision preservation
and revision exhaustion without wrap/reset;
duplicate keys/IDs, wrong types, unknown fields/format, size limits and overflow;
cold-start corrupt/unreadable/locked data with shell survival and no expanded
exposure; each persistence failure point and restart after incomplete writes,
including refusal to resume after rename until parent-directory sync succeeds;
disabled-driver precedence without rewriting lifecycle intent; independent
GraphQL/MCP/proxy/output selections; withheld and later revoked evidence without
effective publication; stale generation callbacks; and diagnostic redaction
using synthetic secret/endpoint/unknown-key markers. Preserve valid siblings
and retained evidence through partial runtime failures. These are future runtime
acceptance cases, not tests established by this documentation change.

Focused documentation validation checks JSON examples, repository links and
the option-to-owner trace above. Public docs acceptance, full committed-head
CI, independent review and any applicable transport/offline RTU gates remain
separate requirements. This page claims no physical smoke, qualification,
effective installation, parent closure or release readiness.
