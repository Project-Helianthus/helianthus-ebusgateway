# Boiler temperature cache provenance

**Proven:** `cacheBoilerToGraphQL` omits cached boiler flow and return
temperatures before `preloadSemanticCache` loads the public provider. Hydration
therefore cannot restore either temperature from the persisted cache.

**Unknown:** cached temperatures carry no native provenance and cannot seed
a SemReg flow fact or retained qualified observation. Flow remains withheld
until the running process receives a fresh qualified B509 observation. Retention
then follows the M14 freshness rules for that observation. No boiler return
candidate exists.

`TestM15RestartWithholdsCachedTemperaturesUntilFreshObservation` checks the
provider before polling, absence of a cache-derived flow fact, preservation of
unrelated cached fields, and publication after the first fresh observation.

See the [cache conversion](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/semantic_cache.go)
and [boiler publication tests](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/cmd/gateway/thermal_boiler_publication_test.go).
