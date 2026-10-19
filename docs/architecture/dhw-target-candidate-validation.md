# DHW target candidate validation

This read-only mapper hardening addresses [#1210](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1210)
and advances a bounded part of [#952](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/952).
It adds no acquisition, registration, persistence, write operation or consumer
admission. Nothing here establishes stability before Helianthus 1.0.

The prerequisite native contract is the public
[B524 register catalog](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/protocols/vaillant/ebus-vaillant-B524-register-map.md).
The public [DHW cutover contract](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/dhw-setpoint-semreg-cutover.md)
defines requested-target roles, source-specific bounds and the unresolved DHW
service dimension. This slice changes validation of caller-supplied candidates;
it establishes no additional native selector, unit, bound or family mapping.

Eligibility requires every binding, domain and observation evidence reference
to pass SemReg `EvidenceRef.Validate`, and each collection must be nonempty. A usable
reference does not establish that its contents qualify a source. Binding, service,
domain and observation qualification remain separate requirements. An eligible
native observation must assert `observed`; inferred, unknown and missing
assertions remain ineligible with their original quality preserved.

The complete binding identity must match the observation, including asset,
service, source, source epoch, driver generation, binding, binding revision,
association revision and all native selector fields. Asset, service, source,
source epoch and binding IDs also pass their SemReg typed validators; generation
and both revisions pass `Uint64.Validate`. Identical malformed values cannot
establish ownership. The service's own identity, definition, qualification and
availability checks remain in force. Both minimum and maximum must be exact
Celsius quantities from the independently qualified native domain. No default
bounds are installed and values are never clamped.

If multiple bindings match an observation, the observation remains ineligible
with explicit identity loss and no selected binding. Each binding is retained
as a separate ineligible source record, including its complete domain, service,
qualification and evidence. The diagnostic for a binding without an unambiguous
observation states absence or ambiguity; it does not synthesize a target. For
example, two synthetic bindings for `dhw:alpha`, with domains 35..70 and 40..65
Celsius, produce three records: the ambiguous observation and both binding
contexts. These are synthetic test domains, never family-wide operating limits.

Detachment must preserve every populated source field, including all value
variants, raw bytes, nested evidence, bounds, quality reasons, loss details and
time metadata, without sharing mutable storage with input or sibling records.
The equality and nested-storage checks exercise the current JSON-safe SemReg
records; they do not claim arbitrary future custom JSON types cannot fail.
Mapping updates evaluation time only. Retained receipt, source and phenomenon
timestamps never refresh. Missing or ambiguous candidates do not modify prior
last-known-good state.

`Eligible` remains a local validation result, **not consumer admission**. The
aggregate disposition remains `ProjectionWithheld` because the current thermal
fact cannot preserve the required DHW service identity. No MCP envelope,
namespace, GraphQL parity contract or runtime integration changes here.

**Proven** is limited to the public contract and executable synthetic validation
behavior. **Hypothesis** remains the proposed canonical cutover. Native evidence
not supplied by a caller remains **Unknown**. These labels do not confer
qualification, freshness, admission, runtime acceptance or physical acceptance.
Documentation acceptance requires manual source, claim, transition, loss,
example and link review. Focused offline tests supply no transport, passive
capture, runtime, device or hardware acceptance.
