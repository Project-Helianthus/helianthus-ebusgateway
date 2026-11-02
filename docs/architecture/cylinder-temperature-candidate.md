# Detached cylinder temperature candidates

This pure adapter slice advances [#1234](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1234)
and [#952](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/952)
without completing thermal cutover. Nothing here is stable before Helianthus
1.0. No acquisition, registration, consumer change or publication is enabled.

The public [cylinder cutover contract](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/cylinder-temperature-cutover.md)
and its first native authority, the [B524 register catalog](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/protocols/vaillant/ebus-vaillant-B524-register-map.md),
bound the implementation. The catalog's candidate cylinder water temperature
meaning and Celsius unit are **Hypothesis** from value matching. Profile and
per-instance applicability remain **Unknown** without native qualification.
**Proven** documentation contents and synthetic mapper behavior do not qualify
a native sensor. Evidence grade, qualification, freshness, runtime acceptance
and physical acceptance remain separate.

## Input and ownership

`mapCylinderTemperatureCandidates` consumes supplied, correlated native value
bytes, qualified codec metadata, FM5 context and optional detached prior records.
It has no production provider port. Its sole measurement selector is PB/SB
B5/24, opcode B524, operation 02, group 05, register 0004. Opcode identifies the
B524 command; operation identifies its OP02 plane. Instance and direction remain
part of identity. Instances of two or above remain caller-qualified, just like
other instances; the acquisition loop's two-slot implementation bound proves
neither a device count nor a universal selector rejection. The mapper never
substitutes system cylinder top/bottom, DHW outlet, solar-loop values or
configuration companions.

The opaque `Service` string is a caller's physical association fence, **not** a
Thermal service or FactKey dimension. Identity also includes asset, source, epoch,
generation, binding and its revision, association/mapping revisions,
family/profile and profile/registry/codec revisions, request source/destination,
all selector fields and the complete evidence reference (owner, kind, contract,
digest, access, redaction). Every field participates in observation, codec and
retention matching. Missing binding metadata, invalid evidence, or an evidence
grade outside `Proven`, `Hypothesis` and `Unknown` withholds the quantity.
Empty or arbitrary
grades cannot become current or last-good records; diagnostic history remains
inspectable. All three valid grades still require independent codec
qualification. A changed binding cannot inherit old qualification; foreign prior
records survive only as diagnostic history. No family is qualified by default.

The caller removes validated framing/status/selector bytes and supplies successful
read correlation and independent per-instance presence. The mapper cannot prove
those assertions from a value-only response. Raw request/reply and value bytes
remain inspectable. Explicit qualified metadata must name f32 little- or
big-endian encoding, Celsius and finite ordered validation bounds. Exactly four
value bytes and a finite value within those supplied bounds are required. The
catalog's historical range is never used as policy. There is no clamping or
decimal conversion, including for binary fractions. Exact zero survives.

## Transitions and loss

FM5 mode/reason/revision is supplied using the existing interpretation vocabulary,
validated and returned unchanged. Structural FM5 freshness is checked separately
from measurement freshness. A coherent fresh ABSENT or GPIO_ONLY with
CONFIGURATION_NOT_INTERPRETABLE withdraws the structural candidate. Its old
record remains in `History`, with no last-good structural ghost. Transient
reasons preserve the supplied last coherent mode and same-binding last-good
record, while withholding a current measurement. The mapper does not derive
mode from an empty instance set or calculate a new FM5 verdict.

Each result separates `Attempt`, diagnostic `History`, `LastGood`, `Structural`,
`Current` and `Withdrawn`. A missing/invalid/stale attempt retains an eligible
same-binding last-good record with its original receipt and bytes; it never
renews receipt or marks that retained value current. With no eligible prior
measurement, configuration alone creates no instance. Partial requests preserve
healthy siblings independently. Callers must supply the prior accepted record,
not reinterpret historical withdrawn records as active selections.

Original typed wall receipt and monotonic receipt remain separate from the
explicit evaluation point. Canonical unsigned monotonic times and a positive
caller-supplied `FreshForNS` are required. The clock epoch must match, receipt
must not be in the future, and age must be strictly less than the window. No TTL
is invented. Recovery requires a valid fresh same-binding sample with a strictly
newer receipt in the same clock epoch; replay cannot revive current status.
A fresh sample in a new clock epoch remains withheld while the old binding
retains its last-good receipt. The owner must supply a new binding/history
boundary for recovery; the mapper never creates that boundary. A changed binding
keeps the foreign old record only as diagnostic history.

| Synthetic input | Candidate result | Loss / preservation |
| --- | --- | --- |
| Qualified current four-byte zero | Native zero, current structural candidate | Complete Thermal projection withheld |
| Unknown unit, existence, codec or bounds | No quantity | Original native attempt preserved |
| Missing, short/extended, non-finite or out-of-bounds value after valid receipt | No current quantity | Same-binding last-good value and original receipt preserved |
| Expired or malformed receipt/window | No current quantity | Stale history never refreshed |
| Fresh coherent withdrawal after a valid value | Withdrawn, no structural candidate | Prior native evidence retained as history |
| Transient FM5 degradation | Retained structure, no current quantity | Provider mode/reason/revision unchanged |
| Same asset with distinct native instances | Independent candidates | Neither instance silently overwrites the other |
| Duplicate complete identity in one batch | Ambiguous, no current quantity | Every diagnostic record retained |

Outputs and nested byte slices are detached from inputs and each other. Results
sort by complete diagnostic content, preserving evidence differences and making
input permutation irrelevant. Diagnostic losses are local text, not invented
SemReg reason DefinitionIDs.

## Withheld projection and acceptance

Every result explicitly withholds the complete Thermal binding and decimal
policy. The result type emits no fact, semantic service binding, dimension value,
TTL or admitted SemReg quantity and invents no definition ID. Native `float64` carries the exact widened
f32 value only; it does not establish decimal representability. The
[Thermal vocabulary](https://github.com/Project-Helianthus/helianthus-semreg/blob/main/semreg/v1/packs/thermal/definitions.go)
does not resolve the cylinder service or a collision-free per-instance key.
Even a qualified, current native candidate therefore emits no SemReg fact.

Focused `TestP17` vectors reject malformed grades independently of qualification,
compare every prior-codec field and complete codec identity directly, and pin
clock-epoch withholding plus owner-supplied binding recovery. Encoding and bound
changes also discriminate retention end to end with independently native-valid
prior records. Qualification, known-bounds and unit mismatches have redundant
native guards, so direct helper checks distinguish those equality comparisons.
Selector identity helper checks do not establish full native-selector guard
mutation coverage. Ordering-key marshal error handling remains deferred under
[#1248](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1248).

Focused `TestI34` vectors cover widths, finite values, qualified bounds,
independent identity fences, evidence provenance, malformed typed time,
freshness, recovery, FM5 withdrawal/degradation, permutation, collisions and
detached history. Manual documentation review checks source authority, claims,
transitions, loss, examples and public links. These are synthetic adapter checks,
not native qualification, transport conformance, passive/live smoke or hardware
acceptance. Independent review, committed-head CI, final binding/rounding policy,
shared provider integration and MCP/GraphQL parity acceptance remain later work.
Existing eBUS MCP envelopes, namespaces and GraphQL behavior are unchanged.
