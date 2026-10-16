# Driver-owned configuration-bound injection

Status: application contract for issue
[#1101](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1101),
a partial slice of
[#1033](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1033)
following review of
[#1088](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1088).
This specifies the seam; it does not claim that every existing mutation path
implements it or expose a public lifecycle/configuration service.

## Native contract and scope

The public native contracts remain authoritative:

- [B524 protocol](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/protocols/vaillant/ebus-vaillant-B524.md)
- [B524 register map](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/protocols/vaillant/ebus-vaillant-B524-register-map.md)
- [B524 namespace invariants](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/b524-namespace-invariants.md)

This application seam adds no protocol interpretation, register, public bound
value, or native write authority. The Vaillant driver owns identity, native
bound resolution, encoding, qualification and admission. GraphQL projects the
resolved result. Protocol-neutral promotion remains owned by SemReg.

## Resolution and application inputs

The J18 native seam is
`internal/vaillant/systemconfig.ResolveDHWBivalenceBound(ControllerIdentity)`.
Its identity input contains `EID`, `SoftwareRaw`, `HardwareRaw`, `APIVersion`
and `Revision`. The driver obtains identity from already-held atomic registry
snapshots. `ControllerIdentityFromSnapshot` trims the raw software/hardware
fields and trims and uppercases the EID; it does not supply API qualifiers.
The existing poller likewise leaves API qualifiers absent. Resolution must
never acquire or refresh identity on the bus, guess missing qualifiers, or
substitute another controller family's profile.

The application composition passes the driver's resolved `ConfigBound` in
`SystemStatus.Config.DhwBivalenceBound` through the semantic provider:

1. `vaillantSemanticPoller.publishSystem` resolves from
   `controllerB524Identity()` and assembles the system snapshot.
2. `LiveSemanticProvider.SetSystem` (or `SetSystemFromCache`) receives that
   snapshot. Its config retains `MinC`, `MaxC`, `Source`, `Reason`, `Profile`
   and `NarrowedOnly`; cloning preserves the optional bound by value.
3. `Builder.SetSemanticProvider` installs the provider. The `system` query
   consumes `SemanticProvider.System()`. Bound field resolvers only project
   the supplied value; they do not call a native resolver or inspect registry
   identity.

The existing provider input is a complete `SystemStatus` snapshot with an
optional bound, not a raw identity or an instruction to resolve one. This
contract introduces no additional exported provider method. Without an
installed semantic provider, the builder uses `staticSemanticProvider`, whose
`System()` is nil. With a system snapshot but no bound, both bound aliases
return null. Absence must not be replaced by a zero-valued bound or interpreted
as unrestricted writes. A missing provider is distinct from a driver-resolved
unmatched-controller policy bound.

Mutation composition supplies the driver-owned `SystemConfigWriter` using
`Builder.SetSystemConfigWriter`. Its inputs remain context, field name and raw
value. The writer resolves against its current retained identity and validates
before native I/O; a previously displayed bound is not write authorization.
Any retained registry/invoker compatibility path must receive a driver-resolved
bound for bound validation, rather than resolving native identity in GraphQL.
If that input is unavailable, the path must fail before invocation. This is a
requirement for completing the seam, not a claim about today's fallback.

## Sole regulator-address owner

`internal/vaillant/systemconfig` is the sole application owner of the canonical
regulator target priority used for identity selection and config routing.
The current priority is `0x15`, `0x35`, `0x75`, `0xF5`, `0x76`, `0xF6`.
Consumers must obtain this policy from that owner without a second literal
list, independent reordering, or identifier-prefix scan. The owner must not
expose mutable shared storage. Address selection does not qualify a controller
or permit cross-domain register access.

`ControllerIdentityFromRegistry` consumes an object implementing
`LookupEntrySnapshot(byte) (registry.DeviceEntrySnapshot, bool)` and takes the
first snapshot in this priority order. No snapshot interface or no matching
entry yields an empty identity and the native unmatched policy. This identity
fallback must not be confused with the missing resolved-bound input described
above. Native routing and admission checks still apply independently.

## Explicit ValueType conversion

The application adapter must map each supported native `ValueType` explicitly.
Matching integer ordinals are not a contract; neither conversion direction may
use an unchecked cast.

| Native `systemconfig.ValueType` | GraphQL internal representation |
| --- | --- |
| `ValueFloat32` | `configValueFloat32` |
| `ValueUint16` | `configValueUint16` |
| `ValueBoolU8` | `configValueBoolU8` |
| `ValueEnumU16` | `configValueEnumU16` |
| `ValueCString` | `configValueCString` |
| `ValueDateHDA3` | `configValueDateHDA3` |

Every other value returns an unsupported-type error before encoding, readback
comparison or invocation. Do not coerce it to the zero enum member or silently
drop the affected spec. Conversion preserves group, register address, min/max,
maximum length and enum entries. An adapter failure must not erase unrelated
last-known-good fields. These enum representations are internal; this mapping
does not add a GraphQL schema enum.

## Decision-D and compatibility invariants

The released unmatched-controller policy remains the inclusive `[-20, 50]`
window for `dhwBivalencePointC`. Only a found, qualified, finite, ordered native
profile bound may narrow that window through intersection. An empty
intersection, invalid or unqualified bound, unknown register or unmatched
identity retains the conservative policy with its existing reason. A profile
match alone never qualifies an installation. `NarrowedOnly` remains true for
accepted profile evidence even when intersection leaves the endpoints unchanged.
Other fields retain their existing bounds and codecs.

Preserve the G03 projection aliases and their null behavior:

| Public fields | Shared supplied value |
| --- | --- |
| `dhwBivalencePointBound`, `dhw_bivalence_point_bound` | `DhwBivalenceBound` |
| `minC`, `min_c` | `MinC` |
| `maxC`, `max_c` | `MaxC` |
| `narrowedOnly`, `narrowed_only` | `NarrowedOnly` |
| `source`, `reason`, `profile` | corresponding bound metadata |
| `dhwBivalencePoint`, `dhw_bivalence_point` | existing measured/configured value |

Empty reason/profile strings continue to project as null. MCP conversion must
preserve the same bound fields and existing executable MCP/GraphQL parity.
All MCP namespaces remain pre-stable; this document grants no stability promise
or new namespace.

Native source admission, qualification, domain isolation, payload validation,
write/readback verification and replay protections remain unchanged. Retain
native evidence alongside projected fields. Partial failures must preserve
unaffected last-known-good state. Evidence labels Proven, Hypothesis and Unknown
describe evidence confidence separately from qualification; a projected bound
is neither physical evidence nor an admission decision.

## Provisioned implementation boundary

The inspected source contains the native resolver and the projection aliases.
The query publication path already carries a resolved bound. The following
remaining differences prevent claiming complete runtime conformance:

- `graphql/mutations.go:applySystemFieldMutation` still obtains registry
  identity and calls the native resolver through `graphql/system_bounds.go`.
  `setSystemConfigResolve` uses that fallback when no writer is installed.
- GraphQL and `systemconfig` still contain separate regulator-address lists.
- `adaptSystemConfigSpec` and `nativeConfigSpec` still use ordinal casts.

Those implementation changes are outside this documentation slice. Their
acceptance requires focused coverage for absent resolved-bound inputs, one
address-policy owner, every supported conversion and unknown enum rejection,
plus unchanged Decision-D and parity checks. Existing resolver/alias tests
cannot prove those missing properties. Dependency merge identity and any newer
companion snapshot must be reconciled by the orchestrator before acceptance.
