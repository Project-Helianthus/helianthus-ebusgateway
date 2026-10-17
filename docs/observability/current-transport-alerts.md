# Current transport alert examples

These examples cover the existing eBUS observability metric families. They are
local tuning suggestions, not final-candidate coverage or a stability promise.
**Proven** means visible in the current source renderer; **Hypothesis** means an
operator interpretation to investigate; **Unknown** means evidence is missing.
These terms do not establish codec or profile qualification.

## 1. Active transport-error event ratio

```promql
(
  sum by (scope) (rate(ebus_errors_total{scope="active"}[5m]))
  /
  sum by (scope) (rate(ebus_frames_observed_total{scope="active"}[5m]))
  > 0.05
)
and on (scope)
(
  sum by (scope) (rate(ebus_frames_observed_total{scope="active"}[5m])) > 0
)
```

**Proven:** `BusObservabilityStore.RenderPrometheus` emits the counter families
[`ebus_errors_total` and `ebus_frames_observed_total`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/bus_observability_store.go).
Errors have `scope`, `class`, `phase`; frames have `scope`, `src`, `dst`,
`family`, `frame_type`. Both sides retain only `scope`, select `active`, and use
the same window. This deliberately aggregates across all scraped series in the
selected population, including any scrape-added labels. It is a population
ratio, not a per-gateway alert; restrict both selectors identically when narrowing
the population. A per-family or per-address ratio is unsupported for this
example because errors have neither dimension.

The numerator counts error events, not unique failed transactions.
[`OnBusEvent`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/bus_observability_store.go)
also records collision retries; [`recordActiveErrorLocked`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/bus_observability_store.go)
records errors before checking for a request, but adds a frame only with a
request. The ratio can exceed one and is not a failure probability.
**Hypothesis:** a value above 0.05 suggests investigating transport errors;
traffic mix and retries affect the useful threshold.

## 2. Series-budget-overflow activity

```promql
increase(ebus_observability_series_budget_overflow_total[5m]) > 0
```

**Proven:** `RenderPrometheus` emits
[`ebus_observability_series_budget_overflow_total`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/bus_observability_store.go)
as a counter with no renderer labels. This expression preserves any scrape-added
dimensions; there is no numerator/denominator pairing. The
[`incrementFrameLocked` and `incrementErrorLocked`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/bus_observability_store.go)
symbols increment it when inserting a new frame/error series at or above the
configured budget or corresponding hard cap. They still insert the series.
It is not a count of dropped frames, current occupancy, or distinct affected
device families. **Hypothesis:** any recent increase merits a cardinality/budget
review. Adjust the window and persistence locally.

## 3. Passive observation unavailable or warming up

```promql
ebus_passive_warmup_state{state=~"unavailable|warming_up"} == 1
```

**Proven:** the suitable current-state signal is a gauge, not a counter.
`RenderPrometheus` emits
[`ebus_passive_warmup_state`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/bus_observability_store.go)
with only `state`, using the
[`passiveWarmupStates`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/bus_observability_store.go)
values `unavailable`, `warming_up`, `available`. No denominator applies, and the
expression preserves the state distinction and scrape-added dimensions.
`unavailable` signals passive observation unavailability; `warming_up` signals
incomplete warmup, which may be expected during startup or reconnection.
**Hypothesis:** a sustained match merits investigation; choose any alert `for`
duration against local warmup expectations rather than paging on every startup.

**Proven:** the store starts in `unavailable` and stays there when broadcast
listening is disabled or the active transport has no passive support. On such
installations this expression matches permanently, so enable the alert only
where passive observation is expected, or add a selector that excludes those
gateways.

For diagnosis, the same renderer exposes the gauge
[`ebus_passive_capability_unavailable_reason`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/bus_observability_store.go)
with only `reason`, drawn from `passiveUnavailableReasons`; its matching reason
is one only while unavailable. All-zero reasons during warmup do not mean
available. **Unknown:** these signals do not establish active-path failure or
the physical root cause. A counter-only current-state interpretation is
unsupported for this example. Do not infer readiness from a flat counter or
from missing busy-time metrics: the
[`ebus_bus_busy_seconds_total` / `ebus_bus_busy_ratio` block](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/bus_observability_store.go)
is conditional on available passive state and usable timing.

## Reading empty results and resets

- Apply `rate`/`increase` to each counter series before aggregation so each
  series' observed resets are handled independently. Do not apply them to the
  passive-state gauge. A window needs at least two usable counter samples;
  missed resets or gaps limit the evidence, and extrapolation can yield
  fractional increases.
- Synthetic zero-traffic check: existing frame samples remain 100 and error
  samples remain 2 throughout the window. Both rates are zero; division gives
  `0/0` (NaN), which fails `> 0.05`, and the explicit denominator `> 0` filter
  also removes the result. With errors increasing but frames flat, division
  could give infinity; the same filter removes it. Neither case proves health.
- Synthetic reset check: equally spaced overflow samples `8, 10, 1, 3` have
  reset-corrected increments `2 + 1 + 2 = 5`, rather than the raw endpoint
  difference `3 - 8 = -5`. `increase` accounts for the observed reset and may
  extrapolate at the range boundaries; the activity comparison remains true.
- Absent series are **Unknown**, not measured zero. Frame/error maps are
  rendered only for present entries; a never-created error series can leave
  example 1 empty even with traffic. A zero counter rate/increase with sufficient
  samples means no measured increase; gauge zero means that state is not selected. Missing
  scrapes, missing state series, or insufficient samples need separate
  monitoring. These examples do not synthesize missing metrics with zero.
