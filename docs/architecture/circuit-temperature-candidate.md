# Detached circuit temperature candidates

This partial implementation advances [#1233](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1233)
and [#952](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/952).
It implements the native-only seam of the public
[circuit temperature cutover contract](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/circuit-temperature-cutover.md).
The parent remains open. This candidate seam is pre-stable.

The native authority is the [B524 circuit register catalog](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/protocols/vaillant/ebus-vaillant-B524-register-map.md#gg0x02--heating-circuits-multi-instance),
including its profile-dependent float byte order and RR0020 correction.
The [Thermal definitions](https://github.com/Project-Helianthus/helianthus-semreg/blob/main/semreg/v1/packs/thermal/definitions.go)
provide semantic vocabulary and the inclusive water-temperature bounds of
-100..200 Celsius; they do not qualify native observations.

## Contract and scope

[The mapper](../../cmd/gateway/thermal_circuit_temperature_candidate.go) accepts
supplied observations, explicit qualified codec metadata, detached previous
records and a typed evaluation context. It performs no acquisition, registration,
publication or legacy consumer changes within the mapper. This is a local candidate port, not the
common production Provider interface or live gateway integration.

For PB=B5, SB=24, OP=02, operation=00, group=02, the supported ordinary circuit
candidate interval is instance 00..08. That interval never proves presence.
The caller must independently qualify the family/profile and active physical
circuit; virtual native-water instance 09 and instance 0A are excluded. Unsupported
selectors receive the unsupported role and loss even when no observations exist.

| Register | Native disposition | Semantic loss / withholding |
| --- | --- | --- |
| RR0008 | Measured flow; exactly four bytes decoded with the supplied qualified byte order; finite, within supplied profile bounds when known, and within -100..200 inclusive | Candidate native number only; exact temperature-role FactKey, service binding and decimal policy remain unresolved |
| RR0007 | Requested flow retained separately; finite exact-width float with any supplied native bounds | Never substitutes for measured flow, receives no invented measured-temperature bounds, and grants no write capability |
| RR0020 | Raw evidence only; one byte is retained in diagnostic attempts/history | Not a temperature; even four plausible float bytes are a layout conflict. Status meaning remains Unknown |

**Proven:** the native catalog distinguishes these selectors and explicitly
rejects the RR0020 temperature interpretation within its stated family scope.
**Hypothesis:** RR0008 could later map to the pack's water-temperature field.
**Unknown:** the final temperature-role FactKey, exact service binding,
decimal rounding policy and broader family applicability. These evidence labels
remain independent of qualification, freshness, runtime admission and physical
acceptance. The mapper preserves the supplied evidence status without upgrading
it. Only Proven, Hypothesis and Unknown are accepted as evidence status; other
values remain diagnostic and cannot become last-good. All results contain explicit
local loss/withholding text and an empty facts collection. No definition, reason DefinitionID, TTL, capability or dimension
value is invented.

## Identity, retention and recovery

Every observation must match the requested full identity: asset, service,
source, source epoch, driver generation, binding and binding/association/profile/
decoder/semantic revisions, family, profile, available firmware context, and
selector PB/SB/opcode/operation/group/instance/register, initiator/responder,
direction and request/reply association. Empty firmware context is incomplete.
Firmware may explicitly remain Unknown; its value still participates in isolation. A caller-supplied correlation decision
is required; this seam does not parse or authenticate frames. Codec metadata
must match the requested metadata exactly and be explicitly qualified for an
active circuit. No address, name, temperature or pack match establishes that.

Attempts and previous history are copied, including raw request/reply, value
bytes, original wall and monotonic receipt, clock epoch, freshness budget and
all evidence owner/kind/contract/digest/access/redaction fields. Foreign or
invalid records remain diagnostic evidence and cannot update this binding.
Different instance IDs remain separate even for the same asset and semantic
field. The result preserves all supplied history; callers must bound their
input history outside this pure function.

The newest valid same-clock native observation becomes last-good. Older
observations cannot replace it. Equal-receipt records must agree in their entire
record, including evidence, otherwise no new winner is selected and an independently valid prior last-good
record survives. Different monotonic
clock epochs are incomparable and withhold a new selection. An independently
valid prior history survives; while old-epoch history is retained, a new-epoch
attempt cannot replace it. The caller must supply a separate history after a
clock-epoch transition. Canonical diagnostic
ordering makes attempt/history permutation independent. A failed attempt cannot
replace a retained value with zero, refresh its receipt or alter its evidence.
A new valid receipt recovers only that selector; requested and raw sibling
records have independent inputs and histories.

Freshness uses original monotonic receipt and the explicitly supplied FreshForNS.
Malformed typed points or budgets, missing/zero budget, future receipt or epoch
mismatch give Unknown. Age equal to the budget is fresh; age greater than the
budget is stale. The comparison uses an inclusive caller-supplied budget; the mapper supplies
no duration. A stale last-good number remains historical native evidence and
is still returned in NativeValue with Freshness=Stale. Unknown freshness also
retains the native number. Neither supplies a current projection. Even fresh
native candidates emit no fact in this slice.

For example, measured 40 at monotonic receipt 10 survives an invalid attempt
at 12 with receipt 10 intact. A valid measured 45 at 12 then replaces only that
field. Exact zero is present. Requested 45 cannot repair missing measured flow;
a plausible four-byte RR0020 cannot become calculated temperature. Two
conflicting values or evidence references at receipt 12 retain the independently
valid prior 40 at receipt 10. Without such a prior record, no value is selected.

## Acceptance and remaining cutover

[Focused synthetic tests](../../cmd/gateway/thermal_circuit_temperature_candidate_test.go)
cover independent identity axes, evidence axes, bounds, invalid widths/non-finite
values, typed time validation, staleness, recovery, conflicts, permutation and
copy isolation. They are offline mapping evidence, not qualification or runtime
acceptance. Documentation acceptance requires manual source, claim, transition,
loss, example and link review; no keyword test establishes correctness.

The existing eBUS MCP envelope, namespace and circuit field shape remain in place.
The [legacy reader](../../cmd/gateway/semantic_vaillant.go) now follows the native
RR0020 correction: ordinary circuit refresh and pseudo-circuit evidence no longer
acquire RR0020 as a float. The float reader rejects OP02/GG02/RR0020 before live
acquisition or cache lookup, independently of instance and payload width. Neither
one raw byte nor four plausible float bytes supplies a calculated temperature.
Snapshot cloning discards retained calculated-flow values, and merging ignores
incoming calculated-flow values, including cache-source publication. The existing
calculated-flow field therefore remains unavailable; no new raw status field or
status meaning is introduced. Measured RR0008 and requested RR0007 remain
independent, including their per-field last-known-good retention.

[Focused reader regressions](../../cmd/gateway/o18_circuit_rr0020_test.go) exercise
synthetic selector neighbors, acquisition, ordinary and pseudo-circuit refresh,
partial merges and cache-source publication. This repair is a partial advance of
[#1253](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1253),
not a complete thermal cutover. **Proven:** the repaired reader withholds the
contradictory temperature interpretation. **Unknown:** raw status semantics and
physical acceptance; neither follows from these offline checks.

The older calculated-flow f32 row in the public
[legacy semantic mapping](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/b524-semantic-mapping.md)
remains conflicting historical text. Correcting it is documentation debt for its
docs owner; the controlling native register catalog and circuit cutover contract
already reject that interpretation. This reader repair changes neither contract.

Later integration must supply native qualification,
approved FactKey/service and rounding/freshness policies, receipt continuity,
and a reviewed legacy compatibility decision with executable MCP/GraphQL parity.
HA behavior, transport, runtime composition, smoke, physical acceptance,
independent review and committed-head gates are unverified here. The eeBUS output
binding remains driver-owned. No sibling implementation is a dependency.
