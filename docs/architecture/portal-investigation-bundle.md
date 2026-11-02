# Portal investigation bundle contents and lineage

This is a partial contract slice for
[issue #1160](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1160),
advancing [#1002](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1002).
It defines proposed public evidence selection and the loss that selection must
disclose. It does not implement an exporter. Nothing here is stable before
Helianthus 1.0.

## Current source contract

The source anchors are the public
[Portal handler](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/portal/handler.go),
[Explorer](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/portal/explorer.go),
and [handler tests](https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/portal/handler_test.go).
The inventory below describes those sources, not a privacy guarantee.

`handleIssueExport` calls `buildIssueDraft`, then emits the following envelope.
`IssueDraft` itself contains only `title`, `markdown`, and `evidence`.

| Current export field | Actual source/value |
| --- | --- |
| `format_version` | `helianthus-issue-bundle/v1` |
| `generated_at` | Export wall clock, formatted as UTC RFC3339Nano |
| `title` | Trimmed request title or a default semantic mapping candidate title |
| `markdown` | Context, observation, reproduction, evidence, hypothesis, impact, proposal, acceptance criteria |
| `evidence` | Result of `collectIssueEvidence` |
| `filename_hint` | Sanitized title plus `.json`; sanitizing a filename does not redact its meaning |

The current Markdown context includes `gateway_version`, `build_id`, `controller`,
and `device`. Other request text is copied into its corresponding section. The
evidence object is also serialized into that Markdown. Removing a JSON field
alone therefore leaves a second copy to inspect.

| Current evidence path | Actual selection and nested field names |
| --- | --- |
| `evidence.captured_at` | Collection wall clock; always created |
| `evidence.snapshots` | Up to three stored snapshots, newest first, if the store exists; each has `id`, optional `label`, `captured_at`, `payload` |
| `evidence.timeline` | Up to twenty buffered events if the buffer exists; each has `at`, `type`, `layer`, `correlation_id`, `payload`, `provenance` |
| `timeline[].provenance` | `source`, `dropped`, `interval_ms` |
| `evidence.provenance` | Derived from those same events: `correlation_id`, `layer`, `at`, `source`, `dropped`, `interval_ms`, `decode_path`, `payload_keys`, `confidence` |
| `evidence.registry_devices` | Callback result if present; device fields are `address`, optional `addresses`, `manufacturer`, `device_id`, optional `display_name`, optional `role`, optional `serial_number`, `software_version`, `hardware_version`, optional `planes`; plane fields are `name`, optional `methods` |

`buildSnapshotPayload` adds `captured_at`; optional `registry` and `projection`
objects each contain `count` and `items`; `semantic` contains a
`SemanticSnapshot`; `timeline` and `provenance` each contain `count` and `items`
when the timeline buffer exists. These nested copies need the same selection
rules. Semantic snapshots include optional adapter and radio-device details,
names, configuration, and timestamps; they are not inherently public-safe.

`snapshotStreamEvents` currently builds composite summaries. Its payload keys
are `registry.device_count`, `semantic.zones_count`, `semantic.has_dhw`,
`semantic.captured_utc`, `semantic.has_energy`, `semantic.energy_series`,
`projection.device_count`, and `projection.projection_count`, according to
available callbacks. Missing callbacks omit their section. Availability is not
freshness. `toProvenanceRecord` sorts payload keys, constructs a decode path from
the stream source/layer and Portal stages, and sets `confidence` to a fixed 0.7.
This is Portal derivation metadata, not a measured protocol confidence or a
source/receipt/evaluation chain.

Export gathers existing state; it does not invoke snapshot capture or Explorer
scans. Separate callback reads and collection clocks do not establish an atomic
snapshot. The present count limits do not bound payload bytes or text lengths.
`TestIssueDraftAndExportEndpoints` checks successful draft/export responses,
draft context/evidence, format discriminator, and a nonempty filename hint. It
does not verify redaction, freshness, failure isolation, or lineage.

## Proposed public selection within the existing envelope

Every rule in this section is **proposed future behavior**, including changed
omissions. Keep the existing format discriminator and field vocabulary; do not
introduce another bundle schema or namespace. Human-readable status, opaque
references, lineage, and omission explanations belong in `markdown` for this
slice. No new machine-readable status or lineage fields are specified. A future
implementation must review compatible output changes, documentation, and tests;
the examples below are not outputs promised by today's handler.

The public artifact is an explicitly reviewed, finite selection of existing
read-only state for issue reporting. It must not start traffic, refresh a source,
capture new native evidence, write configuration, or export automatically.
Retain the current maximum of three snapshots and twenty timeline events and
their corresponding provenance records. Proposed additional bounds are 256 KiB
of UTF-8 serialized JSON, including Markdown's duplicate evidence, and 16 KiB
for sanitized narrative text before evidence embedding. If a complete safe
artifact cannot fit, withhold whole evidence records, record `size_limit` in
Markdown, and recount retained items. Never byte-truncate JSON, split a record,
or fabricate a complete history. If even the minimal explanation cannot fit,
produce no public artifact. These bounds are proposed, not current limits.

| Field or class | Proposed allowed public content | Proposed omitted/changed selection and loss |
| --- | --- | --- |
| `format_version` | Existing literal discriminator | No alternate format or stability promise |
| `generated_at`, every `captured_at`, `at`, `captured_utc`; Explorer `started_utc`, `finished_utc` | No absolute times in this public selection | Omit rather than invent a clock; disclose `withheld_time` and report freshness as unknown, never fresh or stale without a disclosed reference instant. This changes today's output, including mandatory timestamp fields; consumer compatibility remains to be tested |
| `title`, `filename_hint` | Generic issue description and corresponding generic filename | Remove identities, installation labels, build identifiers and user-supplied private text; filename character filtering alone is insufficient |
| `markdown` context and all free text | Reviewed observation, family-level context, separate evidence/hypothesis/unknown statements and loss notes | Withhold build identifiers, contributor identities, coordinates, secrets, private reproduction detail, original controller/device identifiers and capture times; inspect code fences and duplicated evidence too |
| `evidence.snapshots[].id`, timeline/provenance `correlation_id` | Bundle-local opaque aliases, consistently remapped where a relationship is known | Do not export original identifiers, time-derived identifiers, source hashes or stable cross-export fingerprints; aliases prove neither identity nor continuity |
| Snapshot `label` | Generic reviewed label, or omit | Withhold original free text |
| Snapshot `payload.registry`, `payload.projection`, and `registry_devices` | No native item arrays in the initial public allowlist | Withhold device fields, addresses, software/hardware fingerprints, plane/method inventories, graph IDs, paths and topology. Family-level context may be written in reviewed Markdown; omission does not mean zero devices |
| Snapshot `payload.semantic` | None initially; summarize only allowlisted existing stream diagnostics below | Withhold full semantic snapshots, measurements, schedules, names, configuration, adapter/radio identity and telemetry. New semantic fields require explicit review; no recursive pass-through |
| Timeline `type`, `layer` | Reviewed known category literals such as `snapshot`, `composite` | Unknown or free-form categories withheld; preserve uncertainty in Markdown |
| Timeline `payload` | Only the exact summary paths listed below, when safe for the case | All other keys/values withheld, including nested copies and absolute times; count values can reveal installation structure and may also be withheld |
| Timeline `provenance.source`; provenance `source`, `decode_path` | Reviewed category/stage literals only, e.g. `poll:composite`, `gateway.portal.stream` | Withhold unknown/free-form paths or sources, never turn a coordinate into a source label |
| `dropped`, `interval_ms` | Existing nonnegative diagnostic counters/intervals if reviewed safe | Do not infer zero when unavailable; a drop count is not proof of complete native coverage |
| Provenance `layer`, `correlation_id` | Reviewed category and consistent alias | No invented correlation to an omitted source or receipt |
| Provenance `payload_keys` | Recomputed keys of the retained corresponding payload, sorted | Original key names may leak identity and must not survive redaction |
| Provenance `confidence` | Omit; explain fixed Portal value in narrative if needed | No conversion of 0.7 into Proven status, device qualification or admission |
| Explorer `raw_hex`, `raw_len`, `default_float`, `groups`, `results`; ScanID `chunks`, `serial`, `target`, `source` | None | Withhold raw responses, derived native values, bus coordinates and identity-bearing results; even partial chunks or digests may fingerprint a device |
| Errors, unknown extension keys, logs, sessions | Reviewed generic failure category in Markdown only | Withhold raw error strings, request parameters, session state, arbitrary logs and unreviewed fields; no recursive export of local state |

The initial semantic diagnostic allowlist is exactly the existing composite
stream's `semantic.zones_count`, `semantic.has_dhw`, `semantic.has_energy`, and
`semantic.energy_series`. Optional structural diagnostics are
`registry.device_count`, `projection.device_count`, and
`projection.projection_count`. Preserve their existing meaning and type; for
example, `energy_series` currently expresses energy-object presence, not a
complete or validated series. Only existing observations may be selected. A
missing value stays unknown, never zero or false. No measurement or protocol
decoding is inferred here. Further protocol facts require the public native
owner's contract first; Portal does not supply one.

## Proposed component and lineage notes

Use Markdown to account for each expected evidence component: snapshots,
timeline, provenance and registry, plus any specifically requested native
evidence. Record collection outcome separately from freshness and disclosure.
Outcomes are collected, failed, unavailable, or unknown; freshness is fresh,
stale, or unknown; disclosure is included, partial, or withheld. These are
proposed narrative terms, not new JSON enum fields. An empty current array or
absent callback does not by itself establish any of them. Freshness needs an
owner-provided criterion, trustworthy observation and a disclosed reference
instant for that evaluation; otherwise write unknown. Since this public
selection withholds all absolute times, freshness must be unknown throughout
the bundle; do not label any component fresh or stale.
No global freshness label may conceal mixed components.

For every included artifact, identify it by an opaque alias such as
`artifact-a`, meaningful only inside this bundle. Give separate bundle-local
source, receipt and evaluation aliases only when supported by evidence. Source
means the origin of the observation, receipt means its acquisition, evaluation
means the derivation that consumed that receipt. A Portal collection clock is
not a receipt clock; a correlation ID is not an evaluation ID. Unknown links
must say unknown, with a reason. The current exporter supplies no authoritative
receipt/evaluation identities, source generation, replacement event or freshness
contract. Do not manufacture them from snapshot sequence, time, confidence or
payload equality. Do not hash private identifiers to obtain aliases. Stable
global artifact identity and persistence are outside this slice.

Where independently available, preserve lineage relationships through consistent
aliases after redaction. On source replacement, use a different source alias;
retain the old source's last-known artifact separately as historical only
when that classification is supported, with freshness unknown when its reference
instant is withheld. Never rebind an old receipt or evaluation
to the new source, merge generations, or label old values current because a new
source is present. If replacement cannot be established, report source continuity
unknown. A failed component must not erase valid last-known fields from other
components. This is an export selection rule, not an implemented runtime state
transition or a change to admission.

For each omission, state the affected component/path class, reason, retained
summary (if any), and resulting diagnostic limit. Suggested narrative reasons
are `withheld_identity`, `withheld_secret`, `withheld_coordinate`,
`withheld_time`, `unsafe_raw_evidence`, `not_collected`, `source_unavailable`,
`collection_failed`, `lineage_unknown`, `unreviewed_field`, and `size_limit`.
Withheld means evidence is intentionally absent; unavailable means it could not
be obtained; unknown means its existence or meaning is not established. Do not
claim possession merely by writing withheld. Truncation and buffer drops must
also be disclosed; exported counts describe the selected subset only.

Proven/Hypothesis/Unknown describe support for a stated claim. They are separate
from candidate/qualified/promoted/unsupported/unknown qualification states and
from runtime admission. In particular, a redacted bundle cannot prove omitted
wire evidence, qualify a device, promote a mapping, or authorize traffic. It can
support only the limited observation that its retained diagnostics describe.

## Synthetic proposed bundle examples

These four specimens use only existing field names. They intentionally omit
absolute timestamp fields under the **proposed** rule, so they are not exact
current exports. All aliases, outcomes and replacement histories are synthetic
illustrations, not claims that current providers supply them. Status and
lineage remain prose in `markdown`; no new schema is implied. No specimen
contains private native evidence, a digest, or a capture time.

### Mixed components with freshness withheld

```json
{
  "format_version": "helianthus-issue-bundle/v1",
  "title": "Mixed component observations",
  "markdown": "Synthetic. Timeline artifact-a: collected/unknown/partial. Snapshot artifact-b: collected/unknown/partial. Freshness unknown for both because the evaluation reference instant is withheld_time, even if a producer classified them differently locally. Source, receipt and evaluation links unknown: lineage_unknown. Registry: unknown/unknown/withheld. Absolute times withheld_time; snapshot payload unsafe_raw_evidence. Proven: retained summary reports energy-object presence. Hypothesis: components may have diverged. Unknown: native values, qualification and admission. This selection cannot prove wire behavior.",
  "evidence": {
    "snapshots": [{"id": "artifact-b", "payload": {}}],
    "timeline": [{"type": "snapshot", "layer": "composite", "correlation_id": "artifact-a", "payload": {"semantic": {"has_energy": true, "energy_series": true}}, "provenance": {"source": "poll:composite", "dropped": 0, "interval_ms": 1000}}],
    "provenance": [{"correlation_id": "artifact-a", "layer": "composite", "source": "poll:composite", "dropped": 0, "interval_ms": 1000, "decode_path": ["source:poll:composite", "layer:composite", "gateway.portal.stream", "gateway.portal.timeline"], "payload_keys": ["semantic"]}]
  },
  "filename_hint": "mixed-component-observations.json"
}
```

Provenance is collected/unknown/partial: its time and fixed confidence are
omitted, and its payload keys describe only the retained payload. Registry
contents are withheld as `withheld_identity`, with existence otherwise unknown.
The retained stream source is a category, not a lineage identity. A synthetic
zero drop count does not imply that omitted snapshots or native frames were
observed.

### One failed component with unaffected evidence retained

```json
{
  "format_version": "helianthus-issue-bundle/v1",
  "title": "Partial investigation",
  "markdown": "Synthetic. Registry: failed/unknown/withheld, collection_failed; raw error withheld. Timeline artifact-a: collected/unknown/partial. Snapshots: unknown/unknown/withheld, not_collected in this scenario. Provenance: collected/unknown/withheld, unreviewed_field. Source, receipt, evaluation unknown: lineage_unknown. Times withheld_time. Proven: the retained summary reports presence only. Unknown: registry contents and freshness. Registry failure does not invalidate the independent retained timeline observation or qualify a device.",
  "evidence": {
    "timeline": [{"type": "snapshot", "layer": "composite", "correlation_id": "artifact-a", "payload": {"semantic": {"has_dhw": true}}, "provenance": {"source": "poll:composite", "dropped": 1, "interval_ms": 1000}}]
  },
  "filename_hint": "partial-investigation.json"
}
```

The drop count limits continuity. The current registry callback returns a slice,
not a structured error; this synthetic failure outcome needs future producer
evidence and must not be inferred from an absent or empty `registry_devices`.

### Source replacement without rebinding old evidence

```json
{
  "format_version": "helianthus-issue-bundle/v1",
  "title": "Source continuity investigation",
  "markdown": "Synthetic assumed external lineage: artifact-a belongs to source-a, receipt-a and evaluation-a; source-b replaced source-a. Snapshot artifact-a: collected/unknown/partial, historical; freshness unknown because the evaluation reference instant is withheld_time. New source-b receipt and evaluation: unknown, lineage_unknown; no new artifact asserted. Timeline and provenance: unknown/unknown/withheld, not_collected in this scenario. Registry: unknown/unknown/withheld. Native payload unsafe_raw_evidence; times withheld_time. Hypothesis: replacement may explain discontinuity. Unknown: current values, qualification and admission. Source-a evidence must not be attributed to source-b.",
  "evidence": {"snapshots": [{"id": "artifact-a", "payload": {}}]},
  "filename_hint": "source-continuity-investigation.json"
}
```

The current handler cannot establish this external lineage or replacement. With
only its existing fields, replace that assumed history with source continuity
unknown; snapshot IDs alone are insufficient.

### Unavailable evidence distinguished from withheld evidence

```json
{
  "format_version": "helianthus-issue-bundle/v1",
  "title": "Unavailable native evidence",
  "markdown": "Synthetic. Snapshots, timeline, provenance and registry: unavailable/unknown/withheld, source_unavailable in this scenario; no retained observations. Requested native evidence: unavailable, not_collected. No claim that raw evidence exists. Times withheld_time. Source, receipt and evaluation unknown: lineage_unknown. Unknown: behavior, freshness, device qualification and admission. No wire conclusion can be drawn.",
  "evidence": {},
  "filename_hint": "unavailable-native-evidence.json"
}
```

An empty current evidence collection cannot itself prove this explanation.
Unavailable and failed scenarios require evidence beyond the current exporter.

## Local view, review boundaries and remaining evidence

Explorer locally exposes native read results and ScanID identity material;
`ExplorerRegisterResult`, `ExplorerScanState`, and `ExplorerScanIDResult` are
not an issue-reporting allowlist. A locally useful raw view must not become a
public attachment by default. Preserve native evidence at its driver/native
owner; withholding it from the public selection does not delete, rewrite or
replace that evidence with a semantic summary. This page defines no protocol
decoding facts and no eeBUS driver output binding.

Manual source review covers envelope and nested inventory against
`handleIssueExport`, `buildIssueDraft`, `collectIssueEvidence`,
`buildSnapshotPayload`, the serialized structs and Explorer result types. The
selection table and four cases are proposals reviewed against those limits:
mixed components have no disclosed freshness reference; component failure has no
structured callback outcome; replacement has no generation lineage; unavailable
evidence has no authoritative reason field. Opaque aliases, allowlisting,
redaction of both evidence copies, byte limits and omission narratives all need
future implementation and executable compatibility/privacy tests. No current
runtime transition or new callable interface is claimed.

Public source links use `blob/main` file destinations corresponding to the
reviewed source files. Offline destination/path review does not verify remote
availability or that main has not moved. Document readability is not semantic
validation, executed test evidence, MCP/GraphQL parity evidence, or runtime
success. Existing deterministic MCP envelopes and GraphQL parity remain required
and unchanged; export clocks are not a deterministic snapshot hash.

This slice does not define persistence, contribution registration or prefix
shape (M21), configuration/sharing persistence (M25), routing, lifecycle APIs,
or automatic export. It adds no credential, principal, authentication,
authorization or permission check; those remain outside this contract and in
their separately scheduled scope. No unmerged specification or parallel
implementation is required. Parent Portal/extensibility acceptance remains open,
including implementation, safe export verification and independent review.
No full-CI, transport, hardware, passive/live smoke or profile-admission result
is established by this document.
