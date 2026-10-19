# Maintenance mapping dispositions

This bounded read-only mapper follows the public native
[maintenance and system-off contract](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/maintenance-system-state-cutover.md)
and its [B524 register map](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/protocols/vaillant/ebus-vaillant-B524-register-map.md).
The controller-family selectors retain their native ownership. The mapper adds
no call-site registration, acquisition, write route or canonical fault inference.
Nothing is stable before Helianthus 1.0.

Each distinct requested `(kind, item_id)` receives one disposition. Known thermal
facts are withheld; other requests are unsupported. Generated rows have empty,
non-nil `source_keys`: no canonical FactKey is justified. Their non-nil `loss`
contains a provenance detail whose description retains each native identity and
its source loss as JSON. `source_items` is empty and non-nil because native
registers are not invented semantic definition IDs. Loss is irreversible for a
canonical-only consumer: the native switch and maintenance indicators have no
canonical equivalent here. Native observations remain available separately.

The reason is the existing gateway read-projection definition
`semantic.read.reason.value_unknown`, owned by
[`internal/semanticview.project`](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/internal/semanticview/view.go).
That owner initializes it for non-exact rows without a known canonical value.
Reusing this reason describes the unresolved canonical value; it does not
establish native equivalence or add a thermal-pack reason. The thermal pack
[definitions](https://github.com/Project-Helianthus/helianthus-semreg/blob/main/semreg/v1/packs/thermal/definitions.go)
define representable fields, not a maintenance meaning. The mapper validates
returned dispositions using the existing
[projection validator](https://github.com/Project-Helianthus/helianthus-semreg/blob/main/semreg/v1/projection/validation.go),
including independent rows; invalid rows return an error. Identifier syntax
validation alone is not proof of reason ownership.

For example, RR=0x0007 retains unresolved switch polarity and scope; RR=0x0096
retains a source-coded maintenance indicator whose due/active interpretation
remains unresolved. Neither true nor false establishes fault.present,
fault.clear, operation, action or a system mode. A stale maintenance candidate
retains `freshness_withheld` alongside its full identity. An independent valid
projection for the same requested item is preserved, with native losses still
retained separately; missing maintenance cannot erase that source's result.

`EvidenceStatus` is retained native provenance text. `Proven`, `Hypothesis` and
`Unknown` are recognized only for the separate `EvidenceInterpretation` field.
Any other text, including an empty value or a typo such as `Provne`, remains
unchanged in the observation and is flagged as `Unknown` for interpretation.
This classification never promotes a fact, validates a payload, refreshes a
receipt or upgrades qualification. Proven source inspection, Hypothesis about
meaning and Unknown interpretation remain separate from admission.

`Conflict` is a diagnostic for differing good decoded Boolean candidates under
the same complete identity. It is not a fresh-qualified winner selection rule.
A stale candidate and a fresh candidate with that identity both remain retained
and both carry the diagnostic. Their freshness, validity, qualification, raw
bytes and original receipt remain unchanged. Asset, service, binding, source,
source epoch, driver generation, binding revision, association revision, PB,
SB, opcode, operation type, group, instance, register and direction each isolate
conflict matching. Missing and invalid candidates do not erase prior good data.

Focused synthetic tests cover envelope validation and required-member removal,
source loss retention, invalid evidence vocabulary, stale/fresh diagnostics and
each identity component independently. Manual documentation review covers the
source contracts, claims, transitions, loss examples and public links. This is
not native qualification, runtime integration, MCP/GraphQL parity execution,
transport conformance or physical acceptance. Existing MCP/GraphQL surfaces,
G5 D1-D5/O8 and driver-owned eeBUS output remain unchanged. The parent cutover
requires separate registration, review and applicable gates.
