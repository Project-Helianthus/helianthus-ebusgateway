# Boiler pump evidence

**Proven — software source:** in the Vaillant-family implementation,
`refreshBoilerStatusB509` calls `readB509OnOff` for
`boiler_b509_central_heating_pump` during the fast boiler refresh. A non-nil
result supplies `CentralHeatingPumpActive` in the incoming boiler snapshot.
`readB509OnOff` requires both a successful `readB509Value` and successful
`decodeOnOff`; otherwise it returns nil. This identifies the implemented direct
boiler on/off source, without establishing a new native wire interpretation.
See [the boiler refresh and read helpers](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/semantic_vaillant.go).

The fast B524 path uses `mergeBoilerMirrorFieldsFromSnapshots`, which leaves the
boiler pump field absent rather than deriving it from circuit 0's
`PumpStatusRaw`. That circuit observation does not establish boiler pump state;
the native value remains in
[`vaillantCircuitSnapshot`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/semantic_vaillant_snapshots.go)
and is published only as the circuit's own pump field
(`circuits[].state.pumpActive` in GraphQL, `pump_active` in MCP and Portal).
The boiler status register table no longer defines any circuit-group entry, so
neither circuit 0's pump status (RR=0x001B) nor its circuit state (RR=0x001E) has
a boiler-side decoder. Other supported mirror fields can still refresh
independently.

**Proven — software source; Unknown — boiler heating status:** the same rule
applies to `diagnostics.heatingStatusRaw`. Circuit 0 is managed by the
functional module on the proven topology and can report heating, cooling or DHW
independently of the boiler, so its circuit state remains on the circuit
surface (`circuits[].state.circuitState`) and is not copied into the boiler
diagnostic. No boiler-qualified source is currently implemented, so the
diagnostic stays unset (null in GraphQL, absent in MCP and Portal). The cache
conversions drop it in both directions, like the pump, so a legacy
circuit-derived value is neither restored nor persisted. See
`TestM62BoilerUnavailableLeavesBoilerFieldsUnknownAndPublishesCircuit`,
`TestM62BoilerPumpComesOnlyFromB509` and
`TestM62CachedHeatingStatusRawWithoutBoilerSourceIsDiscarded` in
[the circuit-zero isolation regression](../cmd/gateway/boiler_circuit_zero_isolation_test.go).

**Proven — load-time discard; Unknown — cached pump provenance:**
`cacheBoilerToGraphQL` omits the cached pump boolean while restoring other
supported boiler fields. The pump is therefore Unknown (nil) before cache
preload publishes it and before poller hydration; an old circuit-derived value
does not remain published awaiting a successful read. `graphQLBoilerToCache`
also omits the pump field, so pump state is not persisted. Independent evidence
is required again after a restart. See [both cache conversions](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/semantic_cache.go)
and `TestK01CachedBoilerPumpWithoutProvenanceIsDiscarded` in
[the existing provenance regression](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/boiler_pump_provenance_test.go).

**Proven — software recovery path:** the next successful independent boiler
pump read through `refreshBoilerStatusB509` supplies a new boolean, including
false. `refreshBoilerStatusTier` merges that incoming snapshot and publishes
the result. A successful read of another field alone does not recover the
pump value. The cache regression demonstrates recovery by injecting independent
synthetic boiler evidence; it does not execute a successful B509 wire read.

**Proven — partial-update retention:** once independent pump evidence exists,
`mergeBoilerSnapshotNonDestructive` replaces it only when the incoming pump
pointer is non-nil. Omission or a failed pump read leaves the last-known value
intact while other successful fields can update. This applies to both true and
false, independently of circuit pump changes. See the merge implementation in
[the semantic poller](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/semantic_vaillant.go)
and `TestK01PartialUpdatesPreserveIndependentBoilerPump` in the linked regression.
This live retention is separate from the load-time discard of unproven cache
state.

**Hypothesis — native interpretation:** any broader claim that a circuit pump
observation identifies the boiler pump remains a hypothesis, not an admitted
source. **Unknown — physical applicability:** software source and synthetic
assembly fixtures do not establish hardware behavior or qualification across
device families. Proven/Hypothesis/Unknown describe evidence here independently
of qualification. This explanation adds no native protocol fact, register
qualification, device support, or stability promise; native interpretation
requires the public native documentation contract first.

Tracking: [direct-source and cache wording](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1116).
Refs: [follow-up scope](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1109),
[parent scope](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1012).
