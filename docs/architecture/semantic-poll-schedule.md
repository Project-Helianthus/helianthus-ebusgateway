# Semantic polling schedule ownership

The private `semanticPollSchedule` in
[the scheduling implementation](../../cmd/gateway/semantic_poll_schedule.go)
owns startup gating, recurring tick submission and acquisition serialization.
It receives typed task descriptors containing a key, priority, interval and
refresh callback. It imports only standard library packages and has no native
protocol, GraphQL or MCP dependency.

[The poller](../../cmd/gateway/semantic_vaillant.go) binds existing refresh
methods to descriptors. Its existing entry points delegate directly to the
schedule owner. Acquisition, native evidence, qualification, partial
last-known-good state and publication remain in the existing refresh paths.
This boundary does not complete the acquisition/publication separation tracked
by [the parent issue](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1480).

## Submission contract

The task worker starts before the startup barrier. When a barrier exists,
refresh loops and adapter information wait for its release; cancellation can
end that wait. Discovery is initially submitted at high priority, followed by
schedules at low priority. These are submissions, not a guarantee of completion
order. Discovery retains responsibility for downstream startup priming.

| Recurring task | Interval source | Priority |
| --- | --- | --- |
| Regulator capability | Regulator recheck | Low |
| Discovery | Discovery | Low |
| Configuration | Configuration | Medium |
| State | State | High |
| Circuits | Configuration | Low |
| System | Configuration | Low |
| Radio devices | Configuration | Low |
| Energy | Energy | Medium |
| Schedules | Schedules | Low |
| Boiler fast tier | Boiler fast | High |
| Boiler medium tier | Boiler medium | Medium |
| Boiler slow tier | Boiler slow | Low |

Nonpositive intervals disable their loops. Each enabled loop waits for its
first tick before submitting. Adapter information retains its separate existing
loop and cadence. Cancellation stops tickers; as before, a simultaneously ready
tick or barrier and cancellation follow Go select semantics.

The existing queue coalesces pending tasks by key and raises their priority
without replacing the first pending callback. A running task can have one
coalesced successor. Overload drops the new submission and logs it. A stopped
queue rejects submissions. Queued refresh callbacks and direct locked callers
share the poller's `pollMu`; the owner holds that lock across the callback.
With no queue, callbacks run synchronously with a background context, preserving
the existing fallback that does not acquire `pollMu`.

## Evidence boundary

[The scheduling tests](../../cmd/gateway/semantic_poll_schedule_test.go) use
synthetic callbacks and controlled ticks to check startup, cancellation,
disabled intervals, task identity, callback binding, initial submission order,
coalescing, overload and serialization. These are proven offline scheduling
properties, separate from native qualification. Physical qualification and
deployed acceptance remain unknown from these fixtures.

This is an artifact-affecting structural change. Gateway executable and packaged
runtime artifacts must be rebuilt. Release, security and hardware evidence for
affected artifacts requires renewal; binary neutrality is not claimed. Full
repository CI, independent review and applicable transport/conformance gates
remain required before shipping. No timer cadence, native register contract,
public API or publication contract is changed here.
