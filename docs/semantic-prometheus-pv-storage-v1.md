# PV and Storage SemReg metrics v1

`helianthus-ebusgateway` owns the current `/metrics` output binding for the
accepted PV and Storage SemReg projections. It appends this bounded semantic
view to the existing eBUS exposition; it does not add an endpoint, collector,
database, or scrape-triggered acquisition.

The target consumes one detached `snapshot`, `evaluation`, and `projection`
tuple per domain. The renderer exposes availability, requested projection
outcomes, quality/effective availability/freshness, evidence age when the wall
clock is comparable, open conflicts, and projection loss kinds. Numeric and
boolean values appear only for selected qualified, promoted, valid, available,
fresh, conflict-free facts. Missing, withheld, stale, expired, rejected,
unknown, malformed, or conflicting values are absent rather than zero.

Metric labels are bounded semantic vocabulary only: domain, pack, fact ID, unit,
outcome, and loss kind. The target contains no asset, source, binding, serial,
network address, evidence, timestamp, digest, error text, or raw locator label.
Its fixed renderer budget fails closed and reports an overflow gauge.

Prometheus is a lossy observation target, not a semantic authority. The
versioned SemReg kernel and projection contracts remain authoritative:
https://github.com/Project-Helianthus/helianthus-semreg/tree/f3f761bc67e10d6a65eba6c13cb4dc51002d6955/semreg/v1
and the accepted public mapping inputs remain in
https://github.com/Project-Helianthus/helianthus-docs-semantic/tree/88a422896e1dc8c45a6bf629f08b8bff6115c009.

An alert can notify on `helianthus_semantic_projection_available{domain="pv"}
== 0` or a non-zero `helianthus_semantic_open_conflicts`; it must not infer a
numeric value from an absent fact-value series. This increment covers only PV
and Storage. EVSE, Thermal, Infrastructure, Matter, eeBUS, HA, Portal, release,
and physical acceptance remain separate work.
