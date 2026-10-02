# Matter binding feed v1

Issue #997 defines the Gateway half of the public Matter output binding. The
Gateway owns semantic composition, driver lifecycle, authorization and exact
native dispatch. A separately built public Matter binding consumes this feed and
owns the Matter SDK, node, endpoints, fabric state, commissioning, reporting and
controller fixture.

The wire contract is `helianthus.gateway.matter-binding-feed/v1`. It is exposed
only on the existing dedicated M2M listener after mutual-TLS client
authentication:

- `GET /matter-binding/v1/snapshot`
- `GET /matter-binding/v1/changes?cursor=<instance>:<sequence>`
- `POST /matter-binding/v1/invoke`

The listener remains disabled unless its existing M2M TLS configuration is
complete. The feed is not mounted on the general Gateway HTTP listener. Client
certificate fingerprints become request-context principals after certificate
chain validation; a caller cannot provide a principal or native route in JSON.

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

The Gateway provider remains the final authority. It must revalidate the mTLS
principal, authorization, capability, operation, snapshot/revision, lifecycle,
source epoch, driver generation, route ownership, preconditions, deadline,
idempotency and causal path immediately before native I/O. ACK, readback and
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
Unknown JSON members, oversized requests, malformed cursors, missing principals,
expired deadlines, non-current claims and injected route fields fail closed.
The same M2M asset allowlist used by the listener's GraphQL surface filters feed
resources and operations before projection and serialization.

This feed does not add or vendor a Matter SDK, create a Matter node or endpoint,
allocate or persist a fabric, commission a controller, publish a Matter report or
subscription, implement Matter transport, or establish interoperability,
conformance, certification, deployment, live-device, hardware commissioning or
physical-validation evidence. T01..T88 and P01..P06 remain unchanged and apply
where already declared for the final 0.7 candidate.
