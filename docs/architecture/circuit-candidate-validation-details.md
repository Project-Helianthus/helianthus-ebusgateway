# Circuit candidate validation details

This detached, offline mapper implements a bounded hardening slice of
[issue #1249](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1249),
advancing [parent #952](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/952).
It acquires no observations and supplies no installation qualification. Nothing
here is stable before Helianthus 1.0.

## Sources and claims

The public native contract comes first: the
[B524 register catalog](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/protocols/vaillant/ebus-vaillant-B524-register-map.md)
sections on circuit discovery, circuit registers and `mctype` bound native
interpretation. The
[circuit cutover contract](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/circuit-capability-semreg-cutover.md)
bounds the service mapping, sparse loss and conflicting-source transitions.
[SemReg validation](https://github.com/Project-Helianthus/helianthus-semreg/blob/main/semreg/v1/validation.go)
owns typed monotonic points, unsigned durations and evidence reference syntax.
The [mapper](../../cmd/gateway/thermal_circuit_capability_mapping.go) and its
[synthetic tests](../../cmd/gateway/thermal_circuit_capability_mapping_test.go)
provide source behavior evidence only.

**Proven:** the mapper validates supplied typed freshness provenance before
comparison and retains native records during withholding. **Hypothesis:** these
bounded candidates can support a later accepted integration. **Unknown:** actual
installation presence and role qualification. These evidence labels are separate
from qualification, freshness, runtime conformance and physical acceptance.

## Freshness and conservative type admission

Validate both the receipt and evaluation `MonotonicPoint` and `FreshForNS` before
comparing their values. Canonical unsigned nanoseconds and valid clock epoch
identifiers are required. Epochs must match, evaluation cannot precede receipt,
and elapsed time must be no greater than a positive duration. This applies to
role qualification and each native observation independently. Invalid provenance
uses the existing `stale_role` or field `stale` loss; it cannot admit a service.
Evaluation does not rewrite receipt metadata or renew last-known-good evidence.

For example, a synthetic receipt of `10`, evaluation of `20` and duration of
`10` in the same valid epoch reaches the inclusive freshness boundary. `010`,
a shared malformed epoch identifier, or duration `010` is withheld even when
numeric parsing would otherwise succeed.

Missing type remains unresolved for every role, including independently qualified
`virtual_water`. Preserve the candidate and native cooling evidence, record
`TypeLoss=missing`, and withhold the DHW service. A later valid fresh type read
can restore admission under the other prerequisites. A valid zero type with an
independently qualified virtual-water role remains eligible for DHW; ordinary
zero type remains inactive. Neither zero nor an available cooling flag supplies
missing role qualification. Cooling loss stays independent and cannot erase an
otherwise admitted service. These are conservative adapter admission rules, not
new native presence predicates or inferred physical topology.

## Evidence identity and conflicts

Deterministic activation evidence identity contains owner, kind, contract,
digest, access and redaction, in that order. Exact duplicates collapse. References
that differ only in access or redaction both survive, in the same order regardless
of input order. No policy is silently replaced by a less restrictive reference.
Each reference must independently satisfy its owner's validation. Returned
activation evidence and native raw bytes are detached from caller storage.

The currently pinned SemReg evidence-set validator compares only owner, kind,
contract and digest. It therefore rejects a retained policy-distinct pair as a
duplicate. The mapper validates each constructed service and capability before
admission. If either fails, both are withheld with `invalid_result`; the requested
capability disposition is withheld unless an independent sibling is admitted.
The merged `ActivationEvidence` remains on the source outcome for diagnostics,
alongside the unchanged candidate and native observations, including access and
redaction policy. Every emitted service and capability must validate. Resolving
the owner contract is outside this slice; no dependency pin changes or runtime
publication integration are included.

Two otherwise admitted candidates conflict when they reuse a service or
capability instance ID, or share an asset and service definition even with
different instance IDs. The existing service definition supplies its dimension
association; this mapper invents no dimension value. Callers must supply a distinct
asset per circuit; distinct circuit instance IDs do not distinguish this association.
For example, two qualified
ordinary read candidates for the same asset and circuit definition are both
withheld, even when their service IDs, capability IDs and sources differ and both
callers set `Unambiguous=true`. Preserve both source records and all native
observations. Neither arrival order nor a caller assertion resolves the observed
conflict. Distinct assets and distinct circuit/DHW definitions remain independent;
a merely absent candidate cannot erase an admitted sibling.

## Acceptance boundary

Synthetic checks cover typed validation, all six evidence identity fields, both
input orders, duplicate collapse, semantic conflicts, missing virtual-water type,
positive boundary cases and detached evidence. Manual documentation review compares
source claims, transitions, losses, examples and public links against the supplied
contracts. No type or cooling field, write support, namespace, public lifecycle
API, production call site or topology qualification is added. MCP compatibility
and GraphQL parity obligations remain unchanged; downstream parity, full CI,
runtime integration, native qualification and physical acceptance remain parent
work. G5 D1–D5/O8 are unchanged; eeBUS output remains driver-owned.
