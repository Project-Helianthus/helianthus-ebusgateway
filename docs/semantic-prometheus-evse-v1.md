# EVSE SemReg metrics v1

`helianthus-ebusgateway` can append a bounded EVSE view to the existing
`/metrics` output when `--semantic-prometheus-evse-enabled` is set. The option
enables only consumption of an accepted detached SemReg publication. It does
not create a Tesla endpoint, native acquisition, MCP or GraphQL request,
Portal surface, control capability, fallback, second publication, or EVSE
runtime. If the option is set without an injected detached runtime, the target
emits `helianthus_semantic_projection_available{domain="evse"} 0`.

The binding reads one `SemanticEVSECurrent` snapshot/evaluation/projection
tuple for the captured scrape instant. It reevaluates the immutable snapshot
with monotonic elapsed time, clamps a backward wall clock to the sealed
evaluation coordinate, and makes at most one snapshot-floor evaluation when a
publication wins after the scrape instant. A scrape does not open a serial
endpoint, make a request, retry, publish, advance a revision, or change EVSE
lifecycle state.

The accepted contract is `helianthus.pack.evse@1.0.0` from the public mapping
at https://github.com/Project-Helianthus/helianthus-docs-semantic/tree/88a422896e1dc8c45a6bf629f08b8bff6115c009.
Only these exact quantity schemas are rendered:

| Projection item | Fact | Dimension role | Unit |
| --- | --- | --- | --- |
| `evse.limit.configured_current` | `evse.limit.configured_current` | `evse.dimension.evse` | `unit.ampere` |
| `evse.limit.allocated_current` | `evse.limit.allocated_current` | `evse.dimension.connector` | `unit.ampere` |

Configured current is independent from allocation. Missing, malformed,
inhibited, expired, or withheld allocation therefore records the requested
outcome and loss while a valid configured-current value remains observable.
Values are emitted only for qualified, promoted, good, available, fresh,
conflict-free facts.

The target never exports asset, EVSE, connector, source, binding, serial,
endpoint, policy, evidence, timestamp, digest, certificate, or raw text. For
allocated current it maps at most eight accepted connectors in a detached
snapshot to deterministic `connector_1` through `connector_8` labels. The raw
semantic connector identifier orders that temporary snapshot only; it is not
rendered. Any ninth connector, unknown pack/version/fact/item/dimension/value
shape/unit, invalid state token, duplicate series, or budget excess is omitted
and increments `helianthus_semantic_render_overflow`.

Prometheus remains a lossy observation surface. The SemReg kernel and
projection contracts remain authoritative. An alert must treat an absent
`helianthus_semantic_fact_value` series as unavailable or withheld rather than
as zero.
