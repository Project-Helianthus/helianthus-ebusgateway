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
must resolve each reference, confirm service/capability ownership and confirm
the field's exact canonical unit. There is no scale, offset, formula or
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
actions and 128 diagnostics. IDs are manifest-scoped UTF-8 strings of at most
128 bytes. IDs and orders are unique within each collection. A host canonicalizes
validated arrays by `(order,id)`; equal driver/manifest/version records with
different `sha256:` digests are a conflict and neither is rendered. A rejected
manifest is isolated to that contribution; Portal Core and other valid drivers
remain available.

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

## Fixture and #552 boundary

`portal/contributionv1/testdata/five-domain-catalog.json` pins Thermal/HVAC
1.0.0, PV 1.0.0, Storage/BMS 1.1.0, EVSE 1.0.0 and Infrastructure 1.0.0. The
fixture-driven INT-09 static prototype covers installation/resources,
capabilities, all host perspectives, HVAC/PV-BMS/EVSE, unavailable
Infrastructure and retained negative states. It has no production handler,
network request or operation invocation.

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
