# Proposed CAN transport metric observation boundary

This is a future host/driver contract, not an implemented API or an exporter.
It advances part of [gateway issue #1005](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1005)
through [issue #1161](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1161).
The parent remains open. Nothing is stable before Helianthus 1.0.

## Current evidence and scope

The current [runtime metric implementation][runtime-source] admits only
`modbus_tcp` and `eebus`. Its [runtime contract][runtime-doc] describes a
detached lifecycle snapshot, bounded labels and side-effect-free scrapes;
CAN is `not_composed`, with no emitted CAN series. This page does not extend
that implementation, its protocol allowlist or its lifecycle vocabulary.

The [transport and semantic coverage contract][coverage] separates native
acquisition from semantic projection, omits unavailable measurements, and
keeps native provenance outside labels. It explicitly leaves CAN acquisition
pending. These are **Proven source observations**, not proof of deployed CAN
behavior. The design below is a **Hypothesis / proposed contract**. Existing
CAN provider APIs, supported counters, timing accuracy and runtime behavior
are **Unknown**: no canbusreg source establishes them here. These evidence
conclusions are separate from device qualification and runtime admission.

The host owns configuration selection, observation admission, receipt clocks,
detached storage and eventual exposition. A future driver owns transport
acquisition, counter definitions and native evidence. This is an architecture
boundary outside protocol/type contracts. It defines no CAN wire semantics,
battery/HVAC decoding, device qualification, credentials or permission checks.
No new namespace, MCP behavior or GraphQL contract is introduced; existing
eBUS deterministic envelopes, goldens and MCP-to-GraphQL parity remain intact.
The eeBUS output binding remains owned by its driver repository.

## Proposed typed values

Names in this table describe values, not exported Go interfaces. Integers
are unsigned with checked arithmetic; optionals are explicit, never zero
sentinels. One host-selected logical CAN transport is allowed per collector.
Multiple candidates without an explicit unique selection fail closed; there
is no implicit aggregation or selection by discovery order.

| Value | Proposed type / closed domain | Owner and rule |
| --- | --- | --- |
| Configured presence | `Absent` or `Configured` | Host configuration, independent of provider registration. Unknown/invalid selection cannot be treated as absence. |
| Selection result | `Selected`, `Missing`, `Ambiguous`, `Invalid` | Host; only `Selected` permits observations. |
| Driver generation | opaque host-issued token | New token on provider attachment/restart; never reused during a host lifetime. Not a label or public identity. |
| Source generation | opaque host-issued token | New token when the selected acquisition source changes, including reconnect when continuity cannot be established. Not a label. |
| Observation sequence | `uint64`, strictly increasing per generation pair | Driver delivery ordering; duplicates and older sequences are rejected before changing any record or receipt. Exhaustion requires a new generation. |
| Counter kind | `rx_frames`, `tx_frames`, `rx_errors`, `tx_errors` | Candidate transport categories only. Each requires documented provider counting semantics before support can be asserted. No vendor fault interpretation. |
| Counter record | map from counter kind to `Unsupported`, `Unavailable(reason)` or `Observed(epoch, total)` | Detached driver result; `epoch` is an opaque continuity token and `total` is a monotonic `uint64` within that epoch. Missing kinds mean unavailable, not zero or unsupported. |
| Counter epoch | opaque token, scoped to generation pair and counter kind | Driver changes it on an intentional counter reset; repeated/reused old epochs cannot be admitted. No assumption that reconnect resets counters. |
| Host receipt | optional monotonic instant | Host stamps only a newly accepted record for that kind; scrape, replay and reevaluation cannot refresh it. Wall-clock timestamps and device clocks cannot establish freshness. |
| Freshness policy | positive finite maximum receipt age | Host policy fixed before admission; its value and acquisition/queue-delay guarantees require downstream evidence. Age outside the limit or a clock discontinuity makes that kind unavailable. |
| Counter result | optional host cumulative total plus `Available` or `Unavailable(reason)` | Available only with a current supported record and valid baseline; missing numeric value is not zero. Internal retained totals/provenance are not current measurements. |
| Reset reason | `none`, `provider_restart`, `source_change`, `counter_reset`, `host_restart`, `unknown` | Host-normalized continuity diagnostic, separate from availability and loss. |
| Loss reason | `none`, `sequence_gap`, `delivery_loss`, `clock_discontinuity`, `unknown` | Observation continuity diagnostic; never a guessed number of lost CAN frames. |

An input contains its driver/source generation pair, sequence and per-kind
records. The result contains the selected configured state, per-kind
availability, optional totals, original receipts and normalized reset/loss
reasons. Native evidence references and native diagnostic details stay in the
driver's private provenance store; only detached bounded values cross into
the exposition snapshot. No raw payload, identifier or error text becomes a
label. No claim is made that such a store or interface currently exists.

## Proposed admission and continuity

1. Validate configuration and the active generation pair before processing
   sequence or values. A callback from a retired pair is discarded wholesale;
   it cannot change status, totals, baseline, sequence or receipt time. An
   invalid structural envelope is rejected, with no values admitted.
2. Validate sequence once, then validate each counter independently. Reject
   unknown kinds and invalid per-kind records without replacing valid sibling
   records. Missing, invalid, unsupported or explicitly unavailable kinds lose
   current availability while retaining their last-known-good provenance and
   totals internally. No failed field clears valid siblings.
3. On first valid observation in an epoch, record its absolute total as a
   baseline. Initialize a new host cumulative total to zero; this zero means
   zero counted increments since admission, not a zero native total. Baseline
   admission can make the kind available. Never add pre-admission traffic.
4. For subsequent fresh observations in the same generation pair and epoch,
   add only the checked nonnegative difference from the last valid baseline
   to the host cumulative total. Identical totals at a newer sequence are
   valid zero traffic increments. A decreasing total without a new epoch is
   invalid: withhold that kind, retain its provenance and require an explicit
   fresh epoch before resuming. Do not infer wraparound or reset semantics.
5. Provider restart, source change or declared counter reset fences the old
   baseline. Availability is zero until a new accepted baseline arrives.
   Preserve the host cumulative total and add no cross-boundary difference.
   Host restart recreates host totals; eventual Prometheus counters may reset
   only at this owner lifetime boundary. Overflow withholds the affected kind
   until a new host lifetime; never wrap a public total.
6. A sequence gap does not count dropped CAN frames. Within an explicitly
   continuous epoch, a documented cumulative provider total can bridge missing
   observations. Otherwise withhold the affected kinds and require a fresh
   epoch/baseline. Staleness, delivery loss or clock discontinuity similarly
   requires revalidation of continuity before adding another delta. Retained
   receipt time never becomes fresh merely because a scrape occurs.

The host must serialize configuration/generation changes and snapshot updates.
A scrape only copies detached values and evaluates receipt age against a host
clock; it performs no provider callback, polling, decoding, retries or device
I/O. Host receipt age alone cannot prove wire freshness: a future provider
must establish bounded delivery delay and fence queued pre-restart evidence.
Without that evidence, timing-dependent availability remains unavailable.

These rules follow the current sources' detached-snapshot and missing-value
boundaries; the CAN types, generation scheme and accumulation algorithm are
new proposals, not behavior established by those sources.

## Proposed omission, availability and public bounds

No CAN metrics are emitted today. The following are rules for a future
implemented exporter, not reserved metric names or modifications to existing
eBUS/semantic series:

| Situation | Proposed exporter result |
| --- | --- |
| CAN absent from configuration | Omit every CAN value and availability series. |
| CAN collector/host owner not implemented | Omit every CAN series, even if another configuration layer mentions CAN. Expected-collector inventory must detect the missing owner separately. |
| Owner implemented, CAN configured, provider absent or selection unsupported/ambiguous | Emit unavailable for each of the four counter kinds; omit all numeric counters. Do not claim connection failure or unsupported native semantics. |
| Provider present, individual counter unsupported | That configured kind has availability zero and no numeric sample; valid siblings continue. |
| Counter unavailable, stale or invalid | Availability zero, no numeric sample; retain last-known-good data internally with its original receipt. |
| Valid supported counter with current receipt and admitted baseline | Availability one and host cumulative counter sample, including legitimate zero. |

The proposed per-kind availability gauge has only `kind` and `reason` labels.
`kind` is exactly the four values above. `reason` is exactly `none`,
`no_provider`, `selection_unavailable`, `unsupported`, `no_observation`,
`stale`, `reset_pending`, `observation_loss`, `invalid_observation`, or
`unknown`. Available uses `none`; unavailable uses one of the other values.
Choose a single reason deterministically in this order: selection unavailable,
no provider, unsupported, invalid observation, reset pending, observation
loss, no observation, stale, unknown. At most one availability sample per
kind exists at a scrape; old reason tuples are not retained by the exporter.
Unknown input maps to `unknown`, never to arbitrary text.

The proposed cumulative-counter family has only `kind`; no protocol label is
needed for a CAN-specific family. There are at most four counter samples and
four availability samples at a scrape. Across retained reason combinations,
the upper bound is four counter series plus forty availability series per
target, or `44 × T` for `T` retained deployment target-label combinations.
Deployment labels must themselves be bounded and non-identifying. Generations,
sequences, epochs, timestamps, reset/loss diagnostics and native evidence have
no public label or standalone metric in this proposal. No interface names,
addresses, raw CAN IDs, source IDs, hashes, vendor codes or free-form errors
are permitted labels. All public domains are closed; expanding them requires
a reviewed contract change.

Unavailable is an export state, not a zero measurement. Historical numeric
samples can remain in Prometheus lookback: downstream queries must join
current per-kind availability before presenting a current count/rate. Use
counter-aware `rate`/`increase` only after implementation validation; outages
and rebaselines can omit increments, so do not promise complete wire totals
or continuous rates across them. Never zero-fill an absent measurement.

## Synthetic transition review cases

All numbers below are invented counts or relative ages, not capture data.
Each row is a proposed expectation, manually reviewed against the current
sources' constraints; none is an executed runtime test.

| Case | Input / transition | Required proposed result |
| --- | --- | --- |
| Absent CAN | Configuration absent | No CAN series, no provider invocation. |
| Configured, no provider | Implemented owner, configured selection, no attachment | All four kinds unavailable with `no_provider`; no counters, no fabricated zero. |
| First baseline / no traffic | Fresh `rx_frames` baseline 20, then newer total 20 | Host counter 0 remains available; two actual accepted receipts, no invented traffic. |
| Normal increment | Same epoch, fresh total 20 then 27 | Host count increases by 7 only. |
| Provider restart | Host count 7; new driver generation with baseline 3 | `reset_pending` until baseline; host count remains 7, no cross-generation delta. |
| Source replacement | New selected source generation, baseline 40 | Fence prior source, rebaseline without adding 40 or mixing native evidence. |
| Declared counter reset | Same driver/source, new counter epoch with total 2 | Rebaseline; retained host count unchanged; `counter_reset` diagnostic. |
| Unannounced decrease | Same epoch total 27 then 2 | Affected kind unavailable/invalid; no negative delta; require new epoch. |
| Stale receipt | Receipt age exceeds policy, no accepted new observation | Availability zero/`stale`; omit count, keep original receipt/provenance. |
| Unsupported counter | `tx_errors=Unsupported`, fresh valid `rx_frames` | Omit transmit-error count, availability zero/`unsupported`; receive count continues. |
| Late old generation | New pair active; old pair delivers larger total and newer sequence | Discard callback completely, including receipt and availability changes. |
| Gap / loss | Sequence jumps; continuity cannot be established | `observation_loss`, no guessed lost-frame count; fresh epoch required. |
| Partial invalid update | Valid receive increment plus invalid transmit record | Accept receive increment; withhold transmit kind; retain its previous valid provenance. |
| Duplicate / clock discontinuity | Replayed sequence, or host monotonic clock discontinuity | Replay cannot refresh receipt; clock discontinuity withholds affected observations pending revalidation. |

## Downstream evidence still required

The current runtime implementation/contract support the detached host snapshot,
finite domains and unchanged non-CAN scope. The coverage contract supports the
absence/unavailability split, preservation of native evidence and valid sibling
fields, and separation from semantic measurements. Every proposed type,
transition and synthetic case above is a future design decision constrained by
those sources; they provide no CAN acquisition implementation evidence.

Before implementation acceptance, the provider owner must supply public native
transport documentation and inspected source establishing counting points,
receive/transmit direction, error-event definitions, unsupported capabilities,
counter widths, reset/overflow behavior, continuity and delivery guarantees.
An error counter is not a decoded battery fault or HVAC fault. Battery/HVAC
facts require their own public native mapping, separate qualification and
accepted semantic owners; transport observations cannot establish them.

The host/provider implementation still needs reviewed typed APIs, unique source
selection and generation ownership, per-kind atomicity, queue fencing, freshness
policy evidence and executable tests for every transition above. Exporter work
must choose metric names, validate counter arithmetic and precision, cardinality,
lookback/rate behavior, deterministic output and scrape side-effect freedom,
with corresponding public documentation and applicable gates. There are no new
pins, imports, credentials, emitted CAN series or claims of profile admission.

This page leaves existing eBUS/semantic exports and M35/M36/M38/W08 scope
unchanged. It consumes no other unmerged specification. Readability and manual
source/link/table review establish only a document deliverable, not semantic,
runtime, full-CI, transport, hardware or parent-issue acceptance. Full candidate
gates, independent review and any separately required physical acceptance remain
downstream work; this slice does not close the parent.

[runtime-source]: https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/runtime_transport_metrics.go
[runtime-doc]: https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/docs/transport-prometheus-runtime-v1.md
[coverage]: https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/deployment/prometheus-semantic-coverage.md
