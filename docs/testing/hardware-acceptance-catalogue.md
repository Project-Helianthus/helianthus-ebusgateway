# Transitional hardware acceptance catalogue

Refs [gateway #1345](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1345).
This publication supplies offline inventory and result validation. It changes no
active gate and reports no candidate, physical result, release closure or authority
to access equipment. Nothing is stable before Helianthus 1.0.

The versioned artifacts are [catalogue v1](hardware-acceptance-catalogue-v1.json)
and [result schema v1](hardware-acceptance-result-v1.schema.json). The
[standard-library reader](../../internal/acceptancecatalog/catalog.go) accepts
only a version/signature pair in a separately trusted ledger. Consumers must
validate the catalogue and schema before using the catalogue-aware result validator;
JSON Schema alone checks shape, not inventory equality or authenticity.

## Source and transition

The [current handoff](consolidated-candidate-handoff.md) has 65 result rows:
64 current gates and one historical prior RTU applicability row. Its original
52 gates have gained twelve Huawei family rows. Five current gates are ranges:
T01–T04, T05–T08, T09–T24, T25–T88 and PX01–PX12. Their case lists are explicit.
The other 59 gates each require their own row record. Historical RTU evidence is
retained as a non-gate; it cannot substitute for a candidate result or a triggered
offline RTU gate. Rows remain bound to their owning native contracts; this
catalogue defines no protocol interpretation or semantic admission.

The [v2 matrix](../../matrix/M7-adaptermux-conformance-matrix.md),
[cell map](../../matrix/M7-adaptermux-conformance-cell-map.md) and
[proposed policy](transport-gate-policy-v2.md) define the planned second form.
It has 173 cells: seven transports × 24 invariants, plus five compatibility
cells. AD11 is retired only in that form. The catalogue includes every old
T01–T88 transition mapping, its disposition, reason code, keeper cells and
expected-failure disposition; AD10 additionally uses T13–T16 as proxy baselines
and ENH-AD10 uses EBUSD-ES01. UART and PX cells have no T predecessor.
The v2 acceptance form replaces the five range and twelve AD summary gates
with 173 cells, retaining the other 47 handoff gates (220 gates in total).
Both forms belong to the consolidated transition session; publishing the v2
form does not activate it. Current full88 and AD01–AD12 remain active until the
operator session and separate switch change. Passive P01–P06 remain unchanged;
proxy-dependent passive decisions remain with their owners.

Category membership and exact reasons are machine readable. BI-02 precedes
PL-01, then PL-02, then PL-03. The projected v2 inventory has 41 executable,
60 planned and 72 UART infrastructure-blocked cells. BI-01 applies only to
executable cells. XF-01 names TCP-CX01, but PL-01 currently takes precedence;
UDP-CX01 has no expected-failure allowance. Planned is valid only in PL-01,
PL-02 or PL-03, never as generic missing evidence. Allowed non-pass categories
are not passing physical acceptance. Fail, timeout, not-run, omitted-checks and
xpass remain visible and prevent completion.

## Result identity and evidence

A report names its catalogue version, form, opaque M44 candidate association,
session and target. The caller supplies the expected current identity from its
private verification process. Every gate/case pair occurs exactly once, with
no extras, and carries that same identity, its own unique record ID, scope,
procedure, expected and actual observation, public-safe evidence reference,
omissions, evidence grade, qualification and admission. Range summaries never
replace individual cases. Each case also has a unique review ID, bound to its
candidate, session, target, gate, case and record ID, marked current and
independently reviewed. A single family review cannot stand in for row reviews.

These are opaque public-safe associations, not source coordinates, contributor
identities or an authentication mechanism. The caller verifies actual review
independence, freshness, provenance, M44 equality and evidence authenticity;
non-empty strings do not prove them. Do not publish private captures, serials,
network coordinates, capture times or source revisions. Native raw evidence is
preserved separately under its owning retention rules. Missing, stale or
contradictory evidence withholds admission while bounded per-field last-known-good
state retains its lineage; catalogue validation changes no runtime state.

Evidence is Proven / Hypothesis / Unknown, separately from qualification,
freshness and admission. `pass` requires Proven and no omissions; it does not
qualify a family or grant control. `synthetic` reports may exercise the shape and
validator but always return incomplete, including when a synthetic outcome is
`pass`. Only a complete set of current independently reviewed live pass records
can return complete; that is a bounded offline consistency verdict, not hardware
authentication. This catalogue supplies no live report or candidate revision.

## Version history signature ledger

Canonical identity is compact `encoding/json.Marshal` of `O22Catalogue`: struct
field order, map keys sorted by encoding/json, array order significant, UTF-8,
no trailing newline. Signature is SHA-256 encoded as standard base64. It covers
the version, both complete inventories, mappings, category rules, vocabulary and
the companion schema signature. Schema identity is compact encoding/json of its
parsed object (sorted object keys). These are content signatures, not source
revision identifiers. Whitespace in either input is immaterial.

| Version | Canonical catalogue signature (SHA-256 base64) |
|---|---|
| 1 | dgQZfJpvcTWqfdroLGAAKl8T+/Th28VeKtZKOyPC/70= |

Any added, removed or renamed gate/case, changed transition, category, result
vocabulary or schema requires a new catalogue version and a new ledger entry;
retain earlier entries unchanged. Consumers reject unknown versions. Do not
replace a signature under an existing version to admit drift. Tests compare the
ledger with canonical identity and independently compare the handoff and matrix
inventories. If an owning handoff row changes, regenerate and review the owned
catalogue files; never patch the handoff to make this check pass.

Add-on consumption and removal of its local lists are a separate future change
under [add-on #273](https://github.com/Project-Helianthus/helianthus-ha-addon/issues/273).
The offline index builder, gate scripts, runner, dependency pins and workflows
are unchanged. Full committed-change CI/review and physical acceptance remain
unverified by this publication. Existing eBUS envelopes, invoke safety and
MCP/GraphQL parity remain unchanged.
