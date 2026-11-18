# Northbound output hook

Status: implemented in the gateway registry for
[issue #1125](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/1125).
This page is a partial slice of that issue. It follows the public
[northbound output hook](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/northbound-output-hook.md)
contract and restates it for this host.

## Authority

The public
[runtime driver lifecycle](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/runtime-driver-lifecycle.md)
owns registration, start and stop. Health is the snapshot in the G5 runtime
lifecycle MCP contract. **Proven** means a fact already stated by those public
contracts: an empty hook list is a valid registration, the reserved names fail
as unimplemented, start and stop belong to the driver manager, and health is
that manager's snapshot. **Hypothesis** names nothing on this page.
**Unknown** names device publication and the lens rules, which stay with their
owners. Qualification of a device family stays a separate record.

The runtime driver lifecycle states that the first host requires an empty hook
set and that all hook declarations fail. The public output hook contract
narrows that rule for one name only: a northbound descriptor may declare the
hook spelling `output`. For every other hook name and every other descriptor
direction, that rule stands unchanged.

## Hook admission

A northbound descriptor may register the hook spelling `output`. The only
accepted non-empty hook list on that descriptor is the single name `output`.
An empty hook list remains a valid registration on every direction.

The `output` declaration carries hook version `1`, the only supported
version. An `output` declaration with no version or with any other version
fails as an unsupported version.

The hook spelling `output` is accepted only on a northbound descriptor. On this
host a northbound descriptor is one whose direction is `northbound` or `both`,
because `both` declares the northbound role together with the southbound one.
The southbound eeBUS driver registers with an empty hook list, and that
registration stays valid. Declaring `output` on a `southbound` descriptor is
refused.

The host refuses every hook name other than `output`. The reserved names
`mcp`, `setup`, `portal`, `semantic` and `evidence` fail as unimplemented.
Any name that is neither `output` nor one of those reserved names fails as
unknown. A refused hook list admits nothing and leaves the previous registry
and its last-known-good state unchanged.

Registering that descriptor does not by itself publish a fact.

## Lifecycle and health

Start and stop are the existing driver-manager lifecycle. A disabled
descriptor does not start.

Health is read from the existing driver-manager snapshot. Its health
indicators are observed state, reason and the safety-quarantine flag. They are
members of the closed driver object in the MCP read data, which keeps every
member that contract lists; this page defines no reduced object. That snapshot
remains the health read on the existing MCP driver object. There is no
lifecycle GraphQL API and no second health API.

## Gateway registry

`internal/runtimecontrol` admits the hook during descriptor validation, before
any adapter runs or any registry state changes:

| Declared hook list | Result |
| --- | --- |
| Empty | Registers on `southbound`, `northbound` and `both` |
| One `output` at version `1` on `northbound` or `both` | Registers; the frozen entry lists the declaration |
| Any reserved name, alone or beside `output` | `ErrHookUnimplemented` |
| Any other name, alone or beside `output` | `ErrUnknownHook` |
| `output` on `southbound` | `ErrHookDirection` |
| `output` with no version or another version | `ErrUnsupportedHookVersion` |
| `output` declared more than once | `ErrDuplicateHook` |

The checks run in that order, so a reserved or unknown name fails the same way
it did before this hook existed. The registry entry keeps a detached copy of
the admitted declaration; list and get return copies. No health type is added:
the driver-manager snapshot is the health read. Hook resolution is unchanged:
the host resolves no hook implementation, and `ResolveHook` keeps its existing
classification for every name.

## Boundary

This page does not restate the lens rules and does not choose which devices
are published. The eeBUS output binding lives in its driver repository, not in
the gateway.

## Tests

`internal/runtimecontrol/output_hook_test.go` and the refusal table in
`internal/runtimecontrol/registry_test.go` use synthetic descriptors and
synthetic runtimes only. They cover registration on `northbound` and `both`,
an empty hook list on every direction, every refusal in the table above with
the registry unchanged and no adapter call, the detached frozen entry, start
and stop through the driver manager, and health as observed state, reason and
the safety-quarantine flag read from the manager snapshot, including a
disabled descriptor and a safety quarantine.
