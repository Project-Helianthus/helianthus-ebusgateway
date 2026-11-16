# Semantic snapshot field merging

The Vaillant-family poller type, its start and scheduling hooks and its expvar
counters are declared in
[`semantic_vaillant.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/semantic_vaillant.go). The poller
acquires native observations through read, refresh, discovery and write methods
kept in thematic files of the same package: register constants in
[`semantic_vaillant_registers.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/semantic_vaillant_registers.go),
discovery, zones, hot water (with cylinders and solar), circuits, boiler, boiler
configuration, system, radio devices, FM5, energy and schedules in the
`semantic_vaillant_<theme>.go` files, B524 access in
[`semantic_vaillant_b524_access.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/semantic_vaillant_b524_access.go),
B509 and B555 access with the shared read-watch runtime in
[`semantic_vaillant_b509_access.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/semantic_vaillant_b509_access.go)
and value decoding in
[`semantic_vaillant_codec.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/semantic_vaillant_codec.go).
The committed [`move_go_declarations.py`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/scripts/move_go_declarations.py)
moved these declarations unchanged as
[`semantic_vaillant_split.json`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/scripts/semantic_vaillant_split.json)
lists them. Snapshot declarations, cache hydration, cloning and freshness
seeding remain in
[`semantic_vaillant_snapshots.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/semantic_vaillant_snapshots.go).

[`semantic_snapshot_merge.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/semantic_snapshot_merge.go)
owns field freshness assignment and zone and domestic-hot-water field merging.
It receives typed observations, their source and the set of attempted fields.
It performs no bus access or publication. The poller no longer holds the GraphQL
provider, the broadcast hub, the semantic cache persister or the boiler
publication lock as fields: they belong to the `vaillantSemanticPublication`
owner in
[`vaillant_semantic_publication.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/vaillant_semantic_publication.go),
which the poller reaches through nil-safe accessors. A poller without that owner,
such as the registry-only classifier of the startup scan, sees nil collaborators
and a no-op publish lock. The lock orders GraphQL boiler-status publication
against the thermal withdrawal of the boiler flow temperature. The poller
publication methods in the same file compose the retained snapshots into the
provider and hub through the owner; the committed
[`relocate_poller_publication_state.py`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/scripts/relocate_poller_publication_state.py)
rewrote the direct field uses, and its `--check` mode lists any that remain. This extraction is one part of the acquisition and
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

[`semantic_snapshot_merge_test.go`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/semantic_snapshot_merge_test.go)
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
