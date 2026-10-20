# Zone setpoint retention and aggregate accounting

This pure mapper hardening advances [gateway #1220](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1220)
and partially advances [gateway #952](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/952).
The [native zone setpoint cutover contract](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/zone-setpoint-semreg-cutover.md)
remains the source for roles, qualification and representability; its
[native catalog](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/protocols/vaillant/ebus-vaillant-B524-register-map.md)
precedes protocol claims. This change establishes no new native mapping,
family qualification, publication path, recovery store or runtime integration.
Nothing here promises stability before Helianthus 1.0.

## Retained selection and latest failure

`Previous` is caller-owned detached history. A record marked `Current` is
re-evaluated against the current mappings and evaluation context unless a new
observation with the same complete native identity and role passes admission
against exactly one current mapping. Identity includes asset, service, source,
source epoch, generation, binding, binding revision, association revision and
all selector members (PB, SB, opcode, operation type, group, instance, register
and direction). Neither a display label nor equal temperature establishes
identity or role equivalence.

A missing, invalid, stale or otherwise rejected update is recorded separately
with its own diagnostic reason. It does not withdraw or refresh a previously
admitted value. The prior value remains selectable only while its original
receipt, freshness interval, clock epoch, qualification, mapping and any
applicable activity interval still pass admission. A changed binding or
semantic revision cannot borrow old admission. Competing admitted candidates
still fail closed; retention does not choose between them by receipt time.

For example, an invented qualified heating-manual observation with receipt 100
and freshness interval 200 remains selected at evaluation 200 after a missing
or invalid same-key update at receipt 150. The failure stays in history; the
selected value still has receipt 100. At evaluation 300 that prior value is
expired and is withheld. A valid admitted replacement instead supplies its own
value and receipt. These numbers are synthetic monotonic offsets, not captures.

Before retaining at most 64 prior diagnostic records, the mapper orders
`Current` records first, then compares serialized full records. Full identity,
role, receipt (epoch and nanoseconds), received wall-clock context and remaining
evidence provide deterministic tie-breaks. Identical normalized diagnostic
records are deduplicated. Every prior `Current` observation is considered for
re-evaluation even beyond the diagnostic cap; newly evaluated records are
additional to that cap. Final collections are sorted deterministically. This
bounded diagnostic window is not a chronological event log or durable storage.

## One disposition per requested item

All matching `OtherDispositions` contribute their source keys and losses;
matching uses both item kind and definition ID. Each source is independently
normalized and validated before contributing to the aggregate. An Exact source
with loss or a Transformed source without loss returns an error, even when
another source supplies valid loss. Successful Exact or Transformed
sources and local selections establish projection. Source keys are deduplicated
and ordered by SemReg canonical JSON. Loss source items are deduplicated and
sorted; losses use the SemReg identity of kind, source items and description.
Duplicate losses combine `Reversible` with logical AND, preserving irreversible
loss conservatively. Loss identity and ordering follow the
[SemReg projection validator](https://github.com/Project-Helianthus/helianthus-semreg/blob/main/semreg/v1/projection/validation.go).
A transformed source cannot become Exact by changing input order. A projected
aggregate carrying any loss is Transformed. Malformed aggregate output returns
an error rather than dropping keys or losses to fit collection bounds.

For example, an Exact source contributing key A and a Transformed source
contributing key B with precision loss produce one Transformed disposition
containing A, B and that loss in either source order. Adding a local selection
does not erase the loss. Matching nonprojected source keys and losses also
survive, including in a Withheld aggregate. Those keys identify contributing
source evidence; their presence does not establish projection. Only an Exact or
Transformed source or a local selection establishes projection. Source-specific
native admission failures remain in `History`, outside shared
requested-item accounting. Unrequested items are not manufactured.

The mapper no longer invents a reason definition. If the aggregate is withheld,
the caller must supply `WithheldReason`: a definition reference plus an explicit
`RegistryValidated` assertion that its registry resolves that reference and its
meaning applies to the withheld items in this call. Reference validation checks
syntax; it does not discover registry membership. Missing, unresolved or
malformed context returns an error. The synthetic tests supply an invented
caller-owned reason catalog assertion; they prove neither public registration
nor native qualification. Successful aggregates need no withholding context.

## Copy and acceptance boundary

Input and output JSON copies are all-or-error. An input or final output copy
failure returns an error and no usable result. The mapper never mutates the
caller-owned input, including `Previous`, and does not delete the caller's last
successful state. Callers retain that state themselves on error; this mapper
creates no recovery store and makes no claim that a failed call returns partial
output. Other validation failures follow the same return boundary.

The supported concrete input graph has no injectable marshal-failure member.
Tests exercise populated supported-type round-trip equality, detached outputs,
and unchanged prior input on validation failure. They do not claim a forced
mapper copy-failure test.

Proven here means source inspection and synthetic focused-test evidence only.
Native family semantics remain Hypothesis or Unknown where the native contract
says so; qualification, freshness, runtime acceptance and physical acceptance
are separate. MCP envelopes, namespace compatibility and GraphQL exposure are
unchanged by this pure mapping slice; parity graduation, full CI, independent
review and remaining parent cutover acceptance are separate gates. No transport,
passive acquisition, live-device smoke or hardware acceptance is claimed.

Documentation acceptance requires manual source/claim, transition, loss,
example and public link-path review, independently of prose readability.
