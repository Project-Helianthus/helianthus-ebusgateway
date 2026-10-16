# AGENTS

## Repository scope

`helianthus-ebusgateway` is the current Helianthus gateway runtime. It owns
runtime composition, driver lifecycle, protocol-native integration, MCP and
GraphQL surfaces, Portal behavior, and the HTTP/operator edge.

The repository keeps this name until a separately approved gateway rewrite.
Do not rename it as part of unrelated work.

This repository is not the universal semantic owner. The separate public
[`helianthus-semreg`](https://github.com/Project-Helianthus/helianthus-semreg)
repository is the current canonical owner of promoted protocol-neutral types,
publication, evaluation, projection, and capability-pack contracts. Keep
protocol-specific identity, qualification, decoding, behavior, and raw evidence
inside their driver, adapter, or native registry boundaries; the Gateway composes
accepted native and SemReg surfaces without redefining either owner.

The current binary has scoped runtime and contribution seams. Its universal
public lifecycle/configuration service remains in progress; do not document or
consume it as an available public API before that contract is implemented and
accepted.

## Workflow

1. Create one scoped English GitHub issue.
2. Create `issue/<number>-<slug>` from the repository's current `main`.
3. Keep the change within the issue acceptance criteria and add focused tests.
4. Run `./scripts/ci_local.sh` before push. Install golangci-lint v2.11.4, the
   version pinned in `.github/workflows/ci.yml` and expected by the script
   (it warns when the installed major version differs).
5. Open one linked PR containing commands, results, applicable gates, and
   residual risk.
6. Resolve every valid P0-P2 finding and rerun validation after fixes.
7. Obtain a fresh exact-HEAD `NO_BLOCKING_FINDINGS` review with all applicable
   checks green.
8. Squash merge, verify remote `main`, and stop at the requested boundary.

Use public GitHub URLs in tracked documentation. These instructions must work in
a standalone clone and must not require a parent workspace, sibling checkout,
private network, Home Assistant instance, or physical bus.

## Architecture boundaries

- Drivers own protocol-native connection, discovery, qualification, decoding,
  and raw evidence.
- Shared runtime code must not import vendor-specific or protocol-specific
  implementation details merely for convenience.
- Preserve native evidence such as frames, registers, SPINE data, Modbus
  observations, or CAN messages alongside promoted views.
- Keep candidate, qualified, promoted, unsupported, and unknown states distinct.
- Partial failures must not wholesale replace valid last-known fields.
- Consumers use stable public contracts; Portal and Home Assistant do not define
  upstream protocol or semantic meaning.
- Existing eBUS MCP namespaces are compatibility surfaces, not a universal
  delivery rule for other protocol families.

### eBUS MCP compatibility (pre-1.0) and invoke safety

Until 1.0, `ebus.v1.*` and every other MCP namespace are pre-stable; the
version in the name is historical, not a stability promise. This namespace
compatibility status is separate from per-tool maturity classifications such as
`core_stable` and `experimental`. A `core_stable` classification supports
GraphQL graduation but does not promise namespace stability before 1.0:

- Every tool returns the top-level `meta`, `data`, and `error` envelope. `meta`
  contains `contract`, `consistency`, `data_timestamp`, and deterministic
  `data_hash`; `error` is null or a structured error.
- Keep list ordering and snapshot hashes deterministic for identical inputs and
  state. Every tool schema and output has a golden snapshot. Update and review
  those goldens whenever an intentional compatible output change occurs.
- An intentional schema or behavior change before 1.0 updates its goldens and
  its documentation companion and states the change in the pull request. It
  needs no new namespace.
- The major-namespace and experimental-namespace rules are decided after 1.0.

`ebus.v1.rpc.invoke` is an eBUS compatibility and safety surface:

- Every call requires explicit `intent` (`READ_ONLY` or `MUTATE`) and an
  `allow_dangerous` boolean.
- `READ_ONLY` is allowed only for a known method classified as read-only;
  mutating or unknown methods fail closed.
- `MUTATE` requires `allow_dangerous=true` and a non-empty
  `idempotency_key`.
- Missing or unknown intent and missing safety fields fail closed. Preserve
  replay/idempotency behavior when changing invoke request normalization.

### eBUS MCP to GraphQL graduation and parity

For eBUS capabilities, MCP remains the discovery and stabilization surface. A
capability may graduate to GraphQL only when:

1. it exists as an `ebus.v1.*` MCP capability classified as `core_stable`;
2. its determinism, contract, classification, and golden-snapshot tests are
   green; and
3. executable MCP-to-GraphQL parity coverage for that capability is green.

Parity drift between an existing eBUS MCP contract and its GraphQL exposure is a
CI failure. Home Assistant, Portal, and other consumers adopt the capability only
after the graduation checks above are green. Graduation before 1.0 does not
imply a stability promise. These are eBUS compatibility rules for the current
gateway; they do not impose MCP-first delivery on other protocol families.

### eBUS heat-source modeling boundary

- Treat `scan` as cross-device discovery, not as a class-specific heat-source
  plane.
- Keep `HEAT_SOURCE` and `REGULATOR` semantic domains isolated. Cross-domain
  register access must be denied explicitly.
- Mirror heat-source architecture changes in `helianthus-docs-ebus` and keep the
  design reusable for equivalent VWZ-class implementations.

Public eBUS and B524 contracts are documented at:

- https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/protocols/vaillant/ebus-vaillant-B524.md
- https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/protocols/vaillant/ebus-vaillant-B524-register-map.md
- https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/b524-namespace-invariants.md

## Gates and validation

- Architecture, public API, protocol, semantic behavior, state-machine, or
  reverse-engineering changes require the corresponding public docs gate.
- eBUS transport, topology, or protocol-code changes require the applicable
  T01..T88 result with no unexpected fail or xpass unless the operator records
  a scope-specific override. Modbus RTU production-composition changes require
  the declared offline Modbus RTU transport gate: gateway composition fixtures
  plus the pinned `helianthus-modbus` `RTUProductionEndpoint` conformance
  tests. Both gates fail closed on an unexpected test failure.
- A transport-gate override requires both
  `TRANSPORT_GATE_OWNER_OVERRIDE=OVERRIDE_TRANSPORT_GATE_BY_OWNER` and a
  non-empty `TRANSPORT_GATE_OWNER_REASON`. The reason must identify the exact
  scope and residual risk. An override never implies permission for live writes
  and never creates standing or permanent merge authorization; every use remains
  bound to its recorded scope and current merge gates.
  The override covers only the physical requirements (the eBUS
  `TRANSPORT_MATRIX_REPORT` T01..T88 check and the AD01..AD12 block). It never
  skips the offline Modbus RTU gate: when that gate is triggered it still runs
  under the override, and its failure still fails the gate.
- Prefer deterministic fixtures, replay, mocks, and local integration tests.
- A real Home Assistant or live-device smoke is supplemental unless the issue
  explicitly requires it; it is not a prerequisite for ordinary contribution.
- Obtain explicit operator confirmation immediately before credentials,
  installation writes, live-device mutation, destructive actions, or
  safety-relevant control.
- Never commit private addresses, serial numbers, credentials, device
  fingerprints, account data, or private captures.
