# Gateway issue #607 — verification-state public projection

## Source and scope

- Repository: `Project-Helianthus/helianthus-ebusgateway`
- Issue: [#607](https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/607)
- Branch: `issue/607-verification-state-corroborated`
- Remote base: `34c8a5d8a5444a7f5a8d6350c7b1258af665bb0a`
- Remote base tree: `8e101f29ccbc86910185237eb1a99ba7e7a0ef84`
- Implementation commit: `9cd76f59b28aab27805f31298db47517bf164d3c`
- Implementation tree: `706408208e141de163a0848d4c7b11435a479628`

The implementation replaces the retired public verification-state spelling
with the normative `corroborated` value in the address-table, GraphQL, and MCP
projections. It retains native `VerificationStateCorroborated`, discovery
provenance, lifecycle transitions, and the unrelated runtime-confidence
`corroborated` state. It contains no Portal/B503, transport script, issue #938,
issue #916, or issue #917 implementation changes.

GraphQL covers `Device.verificationState`; MCP covers
`ebus.v1.registry.devices.list`, `ebus.v1.registry.devices.get`, and the
existing `ebus.devices` alias. Each applicable wire field emits one exact
normative value. There is no alternate emitted spelling, fallback projection,
or compatibility-only namespace.

GitHub code search on 2026-09-15 found no Home Assistant reference to the
retired spelling, `verification_state`, or `verificationState` in
`Project-Helianthus/helianthus-home-assistant`.

## Documentation gate

The docs gate is required because this changes a public semantic value. It is
satisfied in this repository by the public contract section added to
`docs/architecture/runtime-driver-provider-contract-v1.md`. That section links
the accepted normative ATR source, the
[Address Table Model](https://github.com/Project-Helianthus/helianthus-docs-ebus/blob/main/architecture/atr/01-address-table-model.md),
which defines `nil -> candidate -> corroborated -> identity_confirmed`.

Issue #607's Board comment selects this work as part of the complete SemReg
public-surface cutover and excludes dual emission, fallback, retained legacy
semantic projection, and a parallel contract migration. No separate docs issue
was created.

## RED/GREEN evidence

- RED: after characterization expectations were changed and before production
  code changed, `CGO_ENABLED=0 GOWORK=off go test . ./graphql ./mcp -run
  'Test(DeviceProvenance|LookupDiscoveryLabels_UsesSnapshotPath|AddressTableInserter|AddressTableInsertion)' -count=1`
  failed in GraphQL and MCP: the observed wire value was still the retired
  spelling while the exact expectation was `corroborated`.
- GREEN: `CGO_ENABLED=0 GOWORK=off go test . ./graphql ./mcp -run
  'Test(ATRInserter_P8|DeviceProvenance|LookupDiscoveryLabels_UsesSnapshotPath)' -count=1`
  passed for the address-table, GraphQL, MCP v1 list/get, and legacy alias
  characterization.
- `CGO_ENABLED=0 GOWORK=off go vet . ./graphql ./mcp` passed.
- `git diff --check` passed.
- A repository-wide exact-literal scan returned no matches for the retired
  spelling after the implementation commit.

## Applicable gates and current blocker

- Transport gate: triggered by the exact lifecycle comment update. `CGO_ENABLED=0
  GOWORK=off TRANSPORT_GATE_BASE_REF=origin/main ./scripts/transport_gate.sh`
  passed, including the gateway composition checks and pinned Modbus RTU
  production endpoint conformance.
- Passive smoke gate: `PASSIVE_SMOKE_GATE_BASE_REF=origin/main
  ./scripts/passive_smoke_gate.sh` reported `not triggered`; this change does
  not alter passive capture behavior.
- Local CI: `GOWORK=off ./scripts/ci_local.sh` passed the terminology,
  source-selection, portal test (101 passing), portal asset, and Go vet stages.
  It stopped at `go build ./...` because the local macOS SDK `.tbd` files use
  architecture declarations rejected by the installed linker. The same failure
  occurred with Homebrew clang and prevents the required complete CI/race suite.

No push or PR was created because repository workflow requires complete local
CI before push. The committed local checkpoint is ready to validate again after
the macOS command-line-tools/SDK mismatch is repaired.

## Runtime routing record

The assignment requested Terra/high. This task's accessible runtime interfaces
do not expose an applied model/effort value to independently verify, so the
report records the requested profile and this metadata limitation rather than
claiming a verified applied setting.
