# Frame metric domain and diagnostic budget

For this design follow-up, preserve the current finite byte-domain behavior of
`ebus_frames_observed_total`, including public byte source and destination
labels. This is a bound on series identities, not a claim that retaining every
possible identity is operationally affordable. The metric remains subject to
the existing pre-stable public contract.

## Finite-domain upper bound

The storage identity is
[`frameSeriesKey`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/bus_observability_store.go#L308).
Its complete label domains are:

| Label | Domain | Size |
| --- | --- | ---: |
| `scope` | `active`, `passive` | 2 |
| `family` | Four uppercase hexadecimal digits from the primary and secondary bytes | 65,536 |
| `frame_type` | `broadcast`, `initiator_initiator`, `initiator_target`, `local_participant_inbound`, `abandoned_partial` | 5 |
| `src` | `0x00` through `0xff`, lowercase | 256 |
| `dst` (active) | `0x00` through `0xff`, lowercase | 256 |
| `dst` (passive) | The same byte labels plus `scan` | 257 |

[`classifyFamily`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/bus_observability_store.go#L2375)
formats all command-byte pairs. The explicit B509, B516, B524 and B555 branches
return the same spelling as that formatting, so they add no extra families.
[`classifyActiveFrameType`, `classifyPassiveFrameType` and `classifyFrameType`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/bus_observability_store.go#L2459)
close the frame-type vocabulary, including local-participant and abandoned
classes.
[`normalizeAddressLocked`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/bus_observability_store.go#L2134)
preserves each byte label, including the broadcast byte. Only
[`recordPassiveAbandonedLocked`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/bus_observability_store.go#L1926)
substitutes the extra destination `scan` for scan-related abandonment.

A conservative scope-aware Cartesian upper bound is therefore:

```text
65,536 families × 5 frame types × 256 sources × (256 active + 257 passive destinations)
= 43,033,559,040 frame series
```

This is an absolute finite upper bound for the current ingestion paths, not an
attainable exact maximum: destination-based frame classification and the
passive-only scan path constrain combinations. Counting all five frame types
in each scope intentionally overestimates reachable combinations. It includes
all reachable labels and remains valid across changes in the local-address
snapshot. The 256 × 256 byte-pair count alone applies only to a fixed scope and
family with one classification per pair, not the whole metric domain. Payload
bytes do not enter the key. No assumption about physical bus admission or the
number of installed participants is needed for this implementation bound.

## Diagnostic thresholds and enforced caps

[`Config.ObserveFirstSeriesBudget`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/config.go#L398)
has a default of 1,024. In
[`incrementFrameLocked`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/bus_observability_store.go#L2096),
a new frame identity checks the pre-insertion sum of `frames`, `errors` and
`frameBytes` map lengths against that configured budget. It also checks the
frame-map length against `observeFirstFramesHardCap`, currently 2,048.
Despite that symbol's name, neither check truncates the frame map: crossing
either threshold increments `ebus_observability_series_budget_overflow_total`
and the new frame identity is still inserted with count one. Crossing both
thresholds in one insertion increments the diagnostic once. Existing frame
identities increment their counters and return before the threshold checks.
The diagnostic is shared with new error-series accounting; it is neither a
rejected-series count nor a precise measure of all exporter series.

A real enforced cap would bound admission or retention of new identities and
specify what happens to excess observations and existing counters. No such
policy is supplied here. A configurable tighter hard cap remains a separately
owned future policy change requiring reconciliation with M35's broader metric
contract and M37's renderer ownership. This finite-domain design conclusion
does not close INT-11 or the parent follow-up.

## Verification scope

[`TestQ04FrameBytePairDomain`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/issue/1110-frame-metric-domain-bound/frame_metric_domain_test.go)
feeds the production `OnBusEvent` entrypoint all 65,536 synthetic byte pairs for
one active family, then repeats the sweep twice with different request payloads.
Each rendered pair must retain one identity and advance from one to three.
Exact rendered labels reject added payload dimensions. The test also requires
65,535 overflow diagnostics under a deliberately small budget of one, with no
additional overflow on repeated identities.

The companion Q04 tests exercise both diagnostic thresholds independently,
representative active/passive families and frame classes, the scan destination,
and every family-classifier byte pair. They render Prometheus text directly;
no exporter server, socket, capture or device is involved. These are synthetic
implementation observations, not protocol qualification evidence. They make no
new protocol claims or changes to native admission, MCP or GraphQL contracts.

Refs: [gateway follow-up #1108](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1108)
and [this partial slice #1110](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1110).
