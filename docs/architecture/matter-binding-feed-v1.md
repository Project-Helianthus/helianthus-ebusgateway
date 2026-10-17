# Matter binding feed v1

Issue #997 defines the Gateway half of the public Matter output binding. The
Gateway owns semantic composition, driver lifecycle, request admission and exact
native dispatch. A separately built public Matter binding consumes this feed and
owns the Matter SDK, node, endpoints, fabric state, commissioning, reporting and
controller fixture.

The wire contract is `helianthus.gateway.matter-binding-feed/v1`. It is exposed
only on the existing dedicated M2M listener, over server-authenticated TLS:

- `GET /matter-binding/v1/snapshot`
- `GET /matter-binding/v1/changes?cursor=<instance>:<sequence>`
- `POST /matter-binding/v1/invoke`

The listener remains disabled unless its server TLS configuration (listen
address, server name, server certificate and key) is complete. The feed is not
mounted on the general Gateway HTTP listener.

## Transport and request budget

The listener requires TLS 1.3 and presents the configured server certificate.
The client CA is optional. When a client CA is configured, a certificate that a
client presents is verified against it and against the denied-fingerprint list,
and a failing certificate ends the handshake; a client that presents no
certificate is accepted. Without a client CA no client certificate is requested.
A client certificate never becomes a principal: the Gateway derives no identity
from it, and a caller cannot provide a principal or native route in JSON.

There is no authentication or authorization gate on this listener in this
release. Authentication and authorization frameworks arrive in a later release;
until then the feed, like the GraphQL route on the same listener, is exposed to
whatever network the operator chooses to bind the listener to. The M2M asset
allowlist filters what is published; it does not identify or authorize a caller.

Every request on the listener, to the GraphQL route and to the three feed
routes alike, first spends one request budget keyed by the remote host (the
source port is ignored). The budget is admitted before the request body is read
or parsed, and released on every exit path:

| Property | Value |
| --- | --- |
| Concurrent requests per host | 1 in flight, burst of 2 tokens |
| Refill | 1 token per second, up to the burst |
| Table bound | 4096 host entries |
| Idle eviction | entries untouched for more than 60 s |
| Pressure eviction | least recently used entry without a request in flight |
| In-flight entries | never evicted; the table exceeds 4096 only by in-flight entries |

A refused request is answered before any feed capture, cursor parse or intent
decode. On the GraphQL route it receives the GraphQL error envelope with code
`REQUEST_LIMIT_EXCEEDED`, an empty path and HTTP 200. On the three feed routes
it receives the feed's own schema-defined error, `{"contract": ..., "error":
"unavailable"}`, with HTTP 503, so the response stays inside the feed schema. A
new host is never refused because the table is full; pressure can only evict an idle or
released entry, which returns with a full budget.

| Vector | Expected outcome |
| --- | --- |
| Client without a certificate, client CA configured | handshake accepted; feed served |
| Certificate that fails the configured client CA or is denied | handshake rejected |
| Second concurrent feed request from one host | feed error `unavailable`, HTTP 503, before capture |
| Feed request from a host whose GraphQL request is in flight | feed error `unavailable`, HTTP 503 (shared budget) |
| Concurrent feed request from a different host | admitted; waits for the serialized capture |
| Malformed body from a refused host | refused without reading the body |
| Third GraphQL request from one host within one second | `REQUEST_LIMIT_EXCEEDED` |

## Target and ownership pins

Every snapshot pins the pure projection contract
`helianthus.gateway.matter-projection/v1`, target `matter.data-model`, version
`1.7-draft-ballot-0.9+29b4768a513cf566011ab8cd60df1bc495204953`, and mapping
revision `1`. The machine-readable target is pinned to
`AryaHassanli/connectedhomeip@29b4768a513cf566011ab8cd60df1bc495204953`
with specification source `214e40c9d51cfe89050eae68ca5b76238fcfa332`.

The accepted public mapping contract is
`Project-Helianthus/helianthus-docs-semantic@30f5e5c79ac6da3a7c7c10c990599906d1dfd0cb`,
path `api/v1/targets/matter-1.7-ballot-0.9-v1.json`, SHA-256
`5ae81d5e0971d25ead08f982fdf31caf47ada4838d0ee6f2b3e719d11a6df39c`.
SemReg remains the canonical semantic owner. Portal's detached catalog capture
is reused as a Gateway composition primitive; Portal does not own the Matter
contract or target semantics.

## Snapshot and change contract

A feed snapshot contains the exact static five-pack ledger once. All 127 fields,
capabilities and operations are present with the accepted mapped, transformed or
explicit loss disposition. Each current resource then carries:

- stable asset and resource identity;
- SemReg snapshot ID and complete revision vector;
- native binding, source epoch and driver generation fences;
- lifecycle state;
- the complete detached Matter document and snapshot-bound SemReg projection
  report, including provenance and loss.

The Gateway captures only detached, contribution-fenced current sources. A
changed contribution generation during capture rejects the capture. Malformed,
stale, unavailable, unqualified, conflicting or revision-mismatched SemReg input
fails in the accepted pure projector; no partial Matter resource is published.
Absent domains remain represented by the static ledger and do not acquire a
fabricated value, resource, endpoint or capability.

Each process generates a fresh opaque feed instance. Sequence numbers increase
only when canonical snapshot bytes change. The server retains a bounded history
of 64 full changed snapshots. A cursor from another process, ahead of the current
sequence or older than the retained window receives HTTP 409 with
`resync_required`; the consumer must fetch a full snapshot. Removal of a current
resource or operation is therefore atomic and cannot retain its prior projected
value in the next canonical snapshot.

This is a capture-on-request feed, so the history records canonical states
observed by snapshot/change requests. Multiple source transitions between two
captures coalesce into the next atomic snapshot; it is not a lifecycle event log
and never claims that every intermediate transition is replayable.

## Admission boundary

An admitted operation contains the discoverable/enabled action and immutable
catalog, contribution, resource, capability, snapshot, revision, binding, source
epoch and driver-generation fences observed in the snapshot. It never contains a
native route. The client returns that exact claim with a bounded idempotency key,
deadline, opaque arguments and a valid SemReg causal context. The server
re-captures before invocation and accepts only an exact current operation.

The Gateway provider remains the final authority. It must revalidate the
capability, operation, snapshot/revision, lifecycle, source epoch, driver
generation, route ownership, preconditions, deadline, idempotency and causal
path immediately before native I/O. No caller principal or authorization
decision is part of that check in this release. ACK, readback and
outcome are separate response members. An indeterminate mutation is never retried
or redirected.

No production Matter operation is admitted at this revision because the current
Gateway contribution set contains read-only descriptors and has no accepted
generic native-operation bridge. The production invoke provider therefore fails
closed. The public contract tests use an offline fake owner to prove exact
admission, one dispatch, zero-I/O rejection, causal rejection and the distinct
execution record without claiming an enabled device operation.

## Schema and non-claims

The normative JSON shape is
[`docs/schemas/matter-binding-feed-v1.schema.json`](../schemas/matter-binding-feed-v1.schema.json).
Unknown JSON members, oversized requests, malformed cursors, expired deadlines,
non-current claims and injected route fields fail closed. A request without a
client certificate or principal is not rejected for that reason.
The same M2M asset allowlist used by the listener's GraphQL surface filters feed
resources and operations before projection and serialization.

This feed does not add or vendor a Matter SDK, create a Matter node or endpoint,
allocate or persist a fabric, commission a controller, publish a Matter report or
subscription, implement Matter transport, or establish interoperability,
conformance, certification, deployment, live-device, hardware commissioning or
physical-validation evidence. T01..T88 and P01..P06 remain unchanged and apply
where already declared for the final 0.7 candidate.
