# Detached public selection for investigation bundles

This implements the pure selection slice of
[issue #1235](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1235),
advancing [#1002](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1002).
The [bundle contract](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/docs/architecture/portal-investigation-bundle.md)
remains the governing public-selection proposal. Correlation and selection-loss
clarifications follow [issue #1255](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1255),
deferred from [#1254](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1254). Nothing here is stable before
Helianthus 1.0.

## Input and selection

[SelectInvestigationPublic](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/portal/investigation_selection.go)
accepts an existing export serialized as JSON in a caller-supplied memory buffer.
The root and `evidence` must be objects. The input limit is 256 KiB, checked
before JSON parsing. This interface deliberately avoids traversing or serializing
an arbitrary caller-owned object graph to discover its size. The caller must
bound any earlier serialization independently. Invalid JSON, an invalid root,
an unknown requested diagnostic, more than 32 diagnostic selectors, or invalid
output limits return a fixed error and no selection. Errors never interpolate
input text. Malformed individual components are isolated; independent valid
components remain selectable.

All component switches default to false. `Snapshots`, `Timeline`, and
`Provenance` explicitly select those components. Provenance additionally requires
a corresponding retained timeline event. `Diagnostics` selects a set of exact
paths; its order and repeated members do not change the result. The seven paths
are:

| Paths | Accepted JSON type |
| --- | --- |
| `semantic.zones_count` | Nonnegative integer within signed 64-bit range |
| `semantic.has_dhw`, `semantic.has_energy`, `semantic.energy_series` | Boolean |
| `registry.device_count`, `projection.device_count`, `projection.projection_count` | Nonnegative integer within signed 64-bit range |

Only these direct leaves of timeline payload groups are inspected for copying.
Unknown nested copies never become another source of diagnostics. Snapshot
payloads, including their nested timeline/provenance copies, remain entirely
withheld. `energy_series` preserves the existing stream's energy-object presence
meaning; it does not establish a complete series. No protocol fact, measurement,
family identification, or qualification is inferred. Further protocol facts need
the native owner's public contract first.

False and zero survive selection. Absent leaves stay absent. Strings, null,
arrays, nested objects, negative numbers, fractions, and overflowing counters
are not coerced. `IncludeDropped` and `IncludeInterval` independently opt in to
existing nonnegative integer `dropped` and `interval_ms` fields in selected
source/provenance records. Missing or invalid counters remain absent. They do
not imply zero drops, complete coverage, freshness, or collection success.

## Categories, aliases and loss

Selected timeline records can retain only the exact `snapshot` type and
`composite` layer. Sources retain only `poll:composite`. Decode paths retain
only the existing `source:poll:composite`, `layer:composite`,
`gateway.portal.stream`, and `gateway.portal.timeline` literals. Unrecognized
categories are absent. No user narrative or Markdown is copied, including
Markdown's duplicate JSON evidence. A future renderer must construct its own
fixed narrative from these categories and loss counts; this result is not an
export envelope or a new bundle schema.

Snapshot aliases use their supplied record positions (`snapshot-1`, etc.).
Timeline aliases use their positions (`event-1`, etc.). They are local references,
reused across independent selections and never evidence of cross-bundle
continuity. No private value is hashed. Snapshot and event aliases have distinct
roles; equal original strings alone do not establish a relationship. Original
snapshot identifiers never survive. A nonempty event correlation is aliased
only when unique among all supplied timeline events. A provenance record must
match that correlation exactly. Ill-formed UTF-8 and lone JSON surrogates are
ineligible for correlation matching or aliasing. Valid escaped and literal
representations of the same Unicode string match; distinct code points are not
normalized and identifiers are not trimmed. Valid replacement characters remain
distinct from malformed input; ambiguous, missing, unmatched, or omitted events
have no retained provenance relationship. `payload_keys` is rebuilt from the
corresponding selected payload's top-level groups and sorted. It never uses
original key strings. No source generation, receipt, evaluation, replacement,
or historical classification is fabricated.

`Components` accounts for snapshots, timeline, provenance, and registry devices:
`present` distinguishes absent data from an empty supplied array; `invalid`
marks a supplied value that is not an array; `supplied`, `retained`, and `omitted`
count whole records. Malformed records count as supplied and omitted. Registry
records are always omitted. Counts say nothing about acquisition success or
native device totals. A malformed component's record count is unknown, indicated
by `invalid`, rather than an asserted empty source. `Diagnostics` reports
missing and invalid selected leaves in timeline records examined before byte
budget removal, including records later removed. Events beyond the twenty-record
selection cap are not examined for these leaf counts. These are selection-loss
counts, not a tally of every private nested field. With `Timeline=false`, selected
paths are unexamined and their diagnostic entries are omitted, rather than
reporting zero missing and zero invalid. With `Timeline=true`, entries start at
zero and count examined records only. An absent payload group or leaf counts as
missing; a present group that is not an object (including null) counts as invalid,
as does a present leaf with the wrong type.

`Notes` contains only fixed policy categories. They describe the loss rules
below, not assertions that withheld material exists. They never echo an unknown
field name, raw error, or value. An absent component does not establish failed,
unavailable, or not-collected acquisition; acquisition remains unknown.

| Fixed reason | Affected class and diagnostic limit |
| --- | --- |
| `withheld_time` | All absolute clocks omitted; `Freshness` is always `Unknown` |
| `withheld_identity` | Identifiers replaced or omitted; no persistent fingerprints or continuity claim |
| `withheld_coordinate` | Registry/native coordinates and topology omitted; no topology inference |
| `withheld_secret` | User text and arbitrary values omitted; private reproduction is unavailable |
| `unsafe_raw_evidence` | Snapshot/native/registry contents omitted; summaries cannot prove wire behavior |
| `unreviewed_field` | Unknown keys, categories, errors, original payload keys and confidence omitted |
| `lineage_unknown` | No authoritative source/receipt/evaluation chain supplied by this interface |
| `collection_unknown` | Array shape and selection counts cannot establish collection outcome |

Proven/Hypothesis/Unknown describe claim support separately from qualification,
freshness, runtime admission and physical acceptance. Retained structural
observations do not qualify or promote a device or authorize traffic. Filtering
never edits native evidence or caller-owned buffers. Partial failures preserve
independent supplied observations; this is not a runtime state store.

## Bounds and adoption

At most three snapshots, twenty timeline events, and twenty corresponding
provenance records are retained in supplied order. `MaxBytes` defaults to 256 KiB
and can only lower that ceiling. It bounds serialized renderer input, including
loss accounting. When necessary the filter removes whole records from the tail,
provenance first, then snapshots, then timeline, recounting after each removal.
This order prevents dangling provenance references. Per-component `size_limit`
counts byte-budget removals; other omissions include unselected, invalid,
unmatched and record-cap losses. If the minimal explanation cannot fit, no
selection is returned. The renderer must independently enforce the bundle's
256 KiB bound including duplicated evidence and its 16 KiB narrative bound;
this component cannot certify future rendering overhead.

For example, selecting only `semantic.has_dhw` from an existing event containing
false and an unselected zone count retains `{"semantic":{"has_dhw":false}}`.
Selecting that same leaf from an empty payload retains `{}` and increments its
missing count. Both have freshness Unknown. Neither case asserts zero zones,
missing native equipment, or source failure.

The
[focused tests](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/portal/investigation_selection_test.go)
exercise synthetic adversarial payloads without HTTP listeners. Manual review
covers source claims, transitions, omission classes, examples, and repository
link destinations; remote availability is not checked offline. The
[handler](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/portal/handler.go)
and [Explorer](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/portal/explorer.go)
remain unchanged. Adoption by the reserved integration owner, archive writing,
filesystem intake, endpoint registration, renderer privacy/byte accounting,
independent review and committed-head gates remain outside this slice. No MCP
namespace, deterministic envelope, GraphQL parity, transport, admission, or
native ownership contract changes here. Parent Portal acceptance remains open.
