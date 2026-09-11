# Portal driver contribution contract v1

`helianthus.gateway.portal-contribution/v1` is a closed, declarative driver
manifest. It gives the gateway Portal a versioned label, group, field, view,
action and typed-native-diagnostic contribution without transferring semantic,
native, rendering, navigation, state or authorization ownership to a driver.
The matching machine schema is
[`portal-driver-contribution-v1.schema.json`](../schemas/portal-driver-contribution-v1.schema.json).

The later host-composed read model is `helianthus.gateway.portal-catalog/v1`.
It binds a validated descriptor to one catalog revision, SemReg snapshot and
evaluation digest, driver source epoch/generation, installation/resource/
service/capability context, per-field semantic state, and caller-scoped action
presentation. Catalog composition does no native I/O.

## Closed descriptors

Every manifest has contract `helianthus.gateway.portal-contribution/v1`, a
stable manifest id/version, a driver id plus typed native contract, the exact
`helianthus.semantic.kernel/v1` requirement and exact SemReg PackRefs. v1 only
accepts major contract 1 and does not select a newer pack version.

Groups are resource-scoped labels with unique ids/orders. Fields carry exact
field, service, capability and canonical-unit DefinitionRefs. The SemReg index
must resolve each reference, confirm the exact field/service/capability
ownership relationship and confirm the field's exact canonical unit. There is no scale, offset, formula or
alternate display unit.

Views select only `summary`, `field_table`, `relationship_graph`, `state_strip`,
`timeline`, `evidence_table`, or `session_controls`, and only the host slots
`lens` and `native_diagnostics`. Actions carry presentation metadata and exact
operation, capability, service, argument and effect refs. Diagnostics identify a
typed native-contract field/action member. They never describe decoding.

The host owns localization, installation/resource/capability navigation,
Registry/Plane/Lens/Compare/Provenance/History/Native perspectives, rendering,
accessibility, current state, navigation cleanup, authorization and executable
code. A manifest cannot contain JavaScript, HTML, CSS, web components, URLs,
GraphQL/query text, JSONPath, formulas, register offsets, opcodes, decoders,
routes, retry policy, authority or precondition results. There is no arbitrary
component escape hatch.

The JSON size limit is 256 KiB. Limits are 64 groups, 64 views, 512 fields, 64
actions and 128 diagnostics. IDs are manifest-scoped Unicode strings of at most
128 code points; this is the JSON Schema `maxLength` and Go validator limit.
IDs and orders are unique within each collection. A host canonicalizes
validated arrays by `(order,id)`, serializes the canonical value and derives its
trusted SHA-256 digest. Publisher-supplied digest metadata is never trusted.
Equal driver/manifest/version records with different derived digests are a
conflict and neither is rendered; the identity/version is quarantined for that
catalog generation. A rejected
manifest is isolated to that contribution; Portal Core and other valid drivers
remain available.

Every record order is a signed 32-bit integer from `-2147483648` through
`2147483647`. The JSON Schema, raw wire decoder and typed Go validation use this
same portable range, including Linux 32-bit builds.
Schema-valid integral decimal and exponent spellings, such as `1.0` and
`1e0`, are accepted without float narrowing and normalize to that typed order;
fractional, non-finite and out-of-range forms reject.

The wire decoder rejects invalid UTF-8 before JSON parsing, duplicate object
keys, non-object top-level values, unknown members, and every missing member
required by the schema; deliberately empty arrays and a present `order: 0` are
preserved as distinct valid wire values.
Every required wire member is also type-checked and non-null before typed
decode. Direct typed admission requires each schema-required array to be a
non-nil slice, while explicitly empty arrays remain valid where the schema
permits them. Native diagnostic member IDs follow the same non-empty,
128-code-point identifier rule as every other public ID.
Every raw object is checked against its exact, case-sensitive schema member
set before typed decoding. Case-folded aliases and duplicate semantic targets
with distinct wire spellings are unknown members, never alternate fields.
It also rejects escaped unpaired UTF-16 high or low surrogates before JSON
normalization; a valid high/low surrogate pair remains a valid JSON string.
Canonical order is `(order,id)` for descriptor records, `(id,version)` for
required PackRefs, and lexical identifier order for view field/diagnostic
reference lists. Direct typed admission applies the same 256 KiB bound to the
trusted canonical JSON encoding. Registry and fixture-index lookups use typed
tuples, never delimiter-concatenated identity strings.
Canonicalization keeps every schema-required explicit empty collection as a
non-nil empty slice, so its canonical JSON remains `[]`, never `null`.
`StaticIndex` keeps those tuple keys private and exposes `NewStaticIndex` plus
typed add methods for each pack, definition, unit, service-capability, field,
operation and native-member relation; its zero value initializes safely on the
first add.

## Truth and actions

Semantic availability, freshness, validity, qualification, promotion, conflict,
selection and projection loss remain SemReg facts. Driver lifecycle remains
DriverManager state. A view independently reports `complete`, `partial` or
`empty`; partial retains valid siblings. `unavailable`, `stale`, `conflict`,
`unknown`, `unsupported`, `withdrawn` and `withheld` stay distinguishable. A
missing value is never rendered as zero.

Actions are caller-scoped. Without discovery permission they are hidden; with
discovery but no invoke permission they are visible and disabled. A discoverable
invokable action is enabled only if the exact current capability, binding,
generation, semantic revisions, typed preconditions, route and deadline are
admitted. Invocation repeats every check. A visible descriptor or enabled button
does not grant authority.

Hidden means absent: a caller without discovery permission receives no action
DOM node, label, identifier, count, tooltip, description or other explanatory
text. Test-only state evidence may record that condition, but is never rendered
in that caller's action surface.

## Fixture and #552 boundary

`portal/contributionv1/testdata/five-domain-catalog.json` pins Thermal/HVAC
1.0.0, PV 1.0.0, Storage/BMS 1.1.0, EVSE 1.0.0 and Infrastructure 1.0.0. The
fixture-driven INT-09 static prototype covers installation/resources,
capabilities, all host perspectives, HVAC/PV-BMS/EVSE, unavailable
Infrastructure and retained negative states. The fixture carries resource and
capability state, contribution-state binding, perspective entries and default
navigation. The prototype reads those records generically, so a changed state or
valid sixth fixture contribution requires no manifest-id/product branch. Fixture
tests validate the presentation references before the prototype uses them. It
has no production handler, network request or operation invocation.

The state fixture records the open gateway #552 reconciliation: B503 target
context is resource-scoped; its five native availability states are retained;
Plane/Lens, host session strip/nav-away cleanup and typed history are host
primitives; AD02 remains an install-write omission banner; transport stays
GraphQL-only; M8-TGT-01..04 and F1-F8 are INT-10 implementation acceptance.
This document and fixture do not close #552.

INT-10 must bind this descriptor to the accepted catalog/admission transport and
then prove production integration. This v1 package does not alter Portal
handlers, drivers, SemReg, MCP, GraphQL, Prometheus, Home Assistant, generated
assets or device behavior.
