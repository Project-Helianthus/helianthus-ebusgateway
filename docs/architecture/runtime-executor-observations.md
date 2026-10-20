# Executor observations: publication correlation

The post-RUNNING executor-observation method requested by [issue #1239](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1239)
is **not implemented**. Its publication seam needs a coordinated service change
before an internal caller can safely submit observations. No new public API,
MCP tool, status schema, lifecycle authority or qualification is established.
Nothing here establishes stability before Helianthus 1.0.

## Publication boundary

`stageObservation` changes the shared pending observation under `viewMu`.
`commitObserved` then advances the durable catalog and publishes, without
returning a persistence/publication result. The publication filter
`observationShown` compares state, reason and quarantine against the built view
and compares epoch and generation against the staged observation under the same
`viewMu` lock.

A publisher paused after staging can resume after STOP and replacement. Even
when the replacement has the same categorical status, the old event is coalesced
away if either epoch or generation differs. The focused regression oracle
independently varies epoch and generation and exercises this interleaving through
the existing staging/publication functions, with positive controls for current
events. It does not establish end-to-end lifecycle or executor-observation
admission coverage.

Holding `driverState.mu` from correlation validation through staging closes
the local check-to-stage window, but extending that lock across
`commitObserved` is unsafe. Store decisions hold store serialization while
their planner acquires the driver lock; publication performs store writes and
can run deferred decision releases that acquire that same driver lock.
An added wrapper cannot safely serialize those existing paths by holding the
driver lock through publication.

The remaining service work needs a bounded result for failed or ambiguous
persistence in
[the existing service](../../internal/runtimecontrol/service.go), preserving
durable catalog ordering, event coherence and decision lock ordering. The
follow-up must test actual STOP/replacement admission, all correlation fields,
closed/fenced refusal, categorical sanitization and persistence failures.
Status-only reports must never discharge native-close obligations or clear
quarantine. No standalone observation method is offered by this publication-filter fix.

## Carried work

SourceID is declared in `LifecycleCorrelation`, but `corr` does not populate it.
End-to-end source attribution and durable driver-to-native generation mapping
remain separate carried items requiring service/store/adapter coordination and
schema review. Shell/status wiring and the canonical provider-contract update
remain after J17. Native evidence and partial last-known-good state retain their
existing owners; these observations cannot confer native acceptance.

Proven, Hypothesis and Unknown describe evidence independently of qualification,
freshness, runtime acceptance and physical acceptance. No protocol fact, native
qualification or hardware result is claimed by this service-seam analysis.
