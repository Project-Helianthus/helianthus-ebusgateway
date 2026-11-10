# Semantic snapshot field merging

The Vaillant-family poller acquires native observations in
[`semantic_vaillant.go`](../../cmd/gateway/semantic_vaillant.go) and its native
read and refresh helpers. Snapshot declarations, cache hydration, cloning and
freshness seeding remain in
[`semantic_vaillant_snapshots.go`](../../cmd/gateway/semantic_vaillant_snapshots.go).

[`semantic_snapshot_merge.go`](../../cmd/gateway/semantic_snapshot_merge.go)
owns field freshness assignment and zone and domestic-hot-water field merging.
It receives typed observations, their source and the set of attempted fields.
It performs no bus access or publication. The existing poller publication methods
in `semantic_vaillant.go` compose the retained snapshots into the GraphQL
provider and broadcast hub. This extraction is one part of the acquisition and
publication separation; the complete separation remains open in
[the parent issue](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1480).

## Retention and provenance

An unattempted field retains its value and freshness. A present attempted value
replaces the retained value and records the incoming source as fresh. An absent
attempted value retains the last-known value and marks existing provenance
stale; it does not invent provenance for an unknown field. A successful merge
records its supplied source, including a cache source following a live source;
source admission and observation precedence remain the callers' responsibility.

Strings containing only whitespace and slices with no elements are absent.
Nonblank strings retain their original whitespace. Nonempty slices preserve
ordering and detach their storage from the incoming slice. Nil numeric pointers
are absent; nonnil pointers containing zero are present and copied into separate
storage. Independent field failures retain their own last-known values while
successful fields can advance. Nil snapshots are ignored. Snapshot identity and
presence are outside these field merges.

These rules preserve software provenance and freshness. They add no native
protocol meaning, qualification, wire fields or public API. Native evidence and
admission remain with their existing owners. The eBUS MCP compatibility and
GraphQL parity obligations continue to apply.

## Validation and evidence boundary

[`semantic_snapshot_merge_test.go`](../../cmd/gateway/semantic_snapshot_merge_test.go)
pins every merged field to its freshness key and varies incoming and retained
sources, attempted status, existing provenance and stale status. Fixtures cover
missing and present zero values, empty strings and slices, ordered detached
slices and pointers, and simultaneous field successes and failures.

This is an artifact-affecting structural change (evidence class S). Gateway
runtime binaries and the release artifacts that contain them require fresh
release, security and hardware evidence under the release process. Offline
characterization demonstrates fixture behavior; it establishes neither binary
neutrality, physical qualification nor deployed acceptance. Full repository CI,
independent review, and applicable transport and conformance gates remain
shipping requirements.
