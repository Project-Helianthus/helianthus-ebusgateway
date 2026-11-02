# Boiler temperature cache provenance

**Proven:** On restart, `cacheBoilerToGraphQL` in
`cmd/gateway/semantic_cache.go` withholds restored boiler flow temperature from
the public provider and the SemReg flow fact. Cached flow has no native
provenance and cannot seed a retained qualified observation. The flow fact
remains withheld until the running process receives a fresh qualified B509
observation. Last-known-good retention then applies only to observations
qualified by the running process, under the M14 freshness rules.

**Proven:** The legacy boiler return field keeps its cache behaviour and stays
outside this flow-publication slice. Cache conversion restores that field, and
`hydrateFromCache` in `cmd/gateway/semantic_vaillant_snapshots.go` preserves it
through hydration. A fresh qualified flow observation does not change the legacy
return. A restored return value is visible only until the first boiler merge:
`mergeBoilerSnapshotNonDestructive` in `cmd/gateway/semantic_vaillant.go` clears
it, including when the merge contains only an unrelated successful read. This
slice does not change that pre-existing behaviour. No boiler return fact is
emitted.

**Unknown:** Native provenance for cached flow is unavailable; the cached value
does not establish a qualified B509 observation.

**Proven:** `TestM15RestartWithholdsCachedFlowKeepsLegacyReturnUntilFirstMerge` in
`cmd/gateway/thermal_boiler_publication_test.go` checks flow withholding before
polling, absence of a cache-derived flow fact, preservation of the cached legacy
return and unrelated fields through hydration, and flow publication after the
first fresh qualified observation without changing the legacy return. It then
checks that the first boiler merge clears the restored return while the
qualified flow remains published and no return fact is emitted.

See the [cache conversion](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/semantic_cache.go)
and [boiler publication tests](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/thermal_boiler_publication_test.go).
