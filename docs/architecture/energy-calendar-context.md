# Private energy calendar context

The internal `energycalendar` package implements the immutable startup-context
prerequisite of the accepted [native calendar policy](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/energy-calendar-policy.md).
It is a private building block for [the calendar follow-up](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1022),
not a completed consumer integration or a public configuration contract.

`New` distinguishes omitted configuration (`nil`) from an explicitly supplied
empty name. An explicit IANA location is loaded once using available zone rules.
For omitted configuration, an injected resolver must supply the process-local
IANA name once. `UTC` is a valid explicit choice. Empty names, unknown names,
`Local`, abbreviations and fixed-offset expressions are rejected; no failure
falls back to UTC. The `Etc/GMT` offset family is also rejected. Missing rules
are an error. Callers keep the previously accepted context when construction
of a candidate fails. There is no location setter or hot-replacement operation.

Every construction has an opaque, comparable process-local generation that
binds the loaded rules and clock dependency. Even two constructions with the
same location name have different generations. The identity is neither a digest
of zone rules nor a persistent or public evidence identifier. Changing process
`TZ`, `time.Local`, or the caller's configuration string after construction
cannot change the accepted location. Applying a different startup context and
invalidating its derived caches remain lifecycle integration responsibilities.

`Capture` samples the injected clock once, preserves that exact `time.Time`
value, and derives the civil date in the selected location. A reference supplies
its date, current/previous civil month, current/previous year, original instant
and context generation. Previous month uses civil subtraction, including the
January-to-December year change; it never subtracts an elapsed duration or rolls
back from the last day of a long month. Returned references and civil components
are values; callers cannot mutate context state through them.

`BeginAcquisition` captures one reference. Its single-owner guard must be checked
before accepting each reply, also after a failed read. A different generation
or any change to the complete `(year, month, day)` date aborts the acquisition,
including same-month midnight and rollback. Abort is permanent for that guard,
even if a later clock reading returns to the original date. Already accepted
points are untouched; the next poll creates a new guard. DST offset changes and
wall-clock rollback within the same civil date do not alone invalidate it.
The clock dependency must meet its caller's concurrency requirements; guards
must not be shared across concurrent acquisitions.

Evidence instants, monotonic observation markers, source ordering, freshness
and partial last-known-good values remain owned by their existing consumers.
The context accepts none of those records and never renews their age. A captured
reference instant is an arithmetic reference, not a new energy observation.
No decoder meaning, admission rule, retention policy, MCP envelope or GraphQL
shape changes here. Nothing in this internal prerequisite promises stability
before Helianthus 1.0.

## Evidence and remaining boundary

The synthetic offline tests establish the reference arithmetic and guard
behavior as Proven only within this private unit scope. Controller-calendar
agreement and physical qualification remain Unknown and separate from freshness
or admission. No Hypothesis about controller behavior is promoted by these tests.

Ingest, acquisition and live-projection wiring, a reported configuration/default,
and the native policy's implementation-status update remain one later coordinated
slice. Existing characterization is not duplicated as consumer acceptance.
Monotonic freshness across wall-clock corrections, persistence, restored evidence,
and cache invalidation across real process lifecycles are not implemented here.
Full committed-change review, repository CI and physical acceptance remain
unverified by this prerequisite's focused tests.
