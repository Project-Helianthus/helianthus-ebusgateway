# Gateway #951 SemReg PV cutover author report

## Scope and source state

- Repository: `Project-Helianthus/helianthus-ebusgateway`
- Issue: https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/951
- Branch: `issue/951-semreg-pv-cutover`
- Base: `3d170c44008ba8e6e4b8495b83347b8f24ef55cf` (`origin/main`)
- SemReg pin: `v0.0.0-20260909085241-f3f761bc67e1`
  (`f3f761bc67e10d6a65eba6c13cb4dc51002d6955`, tree
  `b55c25052e74615023c7b1ebf2e1329c0b2f1357`)

The preserved `-951`, `-951-v3`, and failed-attempt worktrees were not
modified. No deployment, credential, live device, hardware, Modbus I/O, or
write action occurred. Home Assistant issue #256 remains downstream of this
gateway contract.

## Implemented cutover

The gateway now builds one stable per-asset SemReg publication from a qualified
SunSpec observation using `helianthus.pack.pv@1.0.0`. It records stable source,
binding, source-start, source epoch, driver generation, sequence/revision, and
native evidence coordinates. The retained-observation lifecycle covers a
generation fence followed by source retirement using the accepted upstream
SemReg fix.

The mapping accounts for all 14 requested native items. It retains partial
field validity, makes invalid/missing and unavailable symbols field-local,
transforms generated lifetime energy from Wh to kWh, and declares the stable
`counter_continuity_unavailable` loss while keeping native baseline/reset/
rollover/delta evidence native.

Enabled public consumers share one immutable evaluated projection:

- MCP exposes `semantic.v1.pv.current.get`; the former PV MCP tool is absent.
- Dedicated M2M GraphQL accepts only `SemanticPVCurrent` with
  `PUBLIC_GRAPHQL_SEMANTIC_PV_V1` and returns `semanticPVCurrent`.
- Portal forwards that same M2M response.

The legacy canonical mapper, old GraphQL query/contracts, known-assets selector,
shadow/comparator integration module, and compatibility-only publication paths
were removed. The MCP tool-list golden was regenerated from the final tool set.
Native Modbus profile qualification, FC03/FC04 transport, provenance,
reconnect, and raw-observation paths remain owned by the adapter; this change
adds no write authority or reads.

## Validation

Focused normal and race coverage passed for SemReg mapping, 14-row disposition
accounting, invalid-field retention, exact energy transformation, rejected
sequence/revision non-advance, fence-to-retirement retention, MCP/GraphQL/Portal
consumer behavior, and legacy-public-surface absence.

```text
GOWORK=off go test ./cmd/gateway -run 'TestM2MGraphQLRuntime|TestM2MGraphQLFlags|TestNewGatewayModbusMCPProvider' -count=1
PASS (2.370s)

GOWORK=off go test ./internal/modbusadapter -run 'TestPVPublication|TestAdapterPublishes|TestSunSpecProducerQualifiesExactObserved' -count=1
PASS (1.184s)

GOWORK=off go test -race ./internal/modbusadapter ./m2mgraphql ./mcp ./portal -run 'TestPVPublication|TestAdapterPublishes|TestSemanticPV|TestPortalPV|TestGrowattProtocolIIV1Golden' -count=1
PASS (internal/modbusadapter 8.975s; m2mgraphql 1.368s; mcp 1.543s; portal 1.860s)

UPDATE=1 GOWORK=off go test ./mcp -run '^TestGrowattProtocolIIV1GoldenToolsListAndCallEnvelope$' -count=1
PASS (0.517s)
```

Final configured CI passed:

```text
GOWORK=off ./scripts/ci_local.sh
PASS
```

It includes terminology/gofmt, Portal Node `93/93`, Portal asset build, vet,
native and Linux cross-builds, `go test -race -count=1 ./...`, source-selection
schema coverage, all Python script suites, and `golangci-lint` (`0 issues`).
The final CI log SHA-256 is
`476debddeac14a17cd63a080c975d3abea3501c03237bb1009dcf35135769537`.

The transport and passive-smoke gates are `not triggered`: the only changed
generic config entries remove M2M SemReg PV compatibility state and do not
change transport, topology, passive acquisition, or runtime admission. Their
classifiers are covered by their repository Python tests. Documentation is
updated in `docs/architecture/runtime-driver-provider-contract-v1.md`.

## Review boundary

The implementation commit is `a2bab01ae713caf2225625bf1d150f20755c5a37`;
the current branch includes this report and is open as
https://github.com/Project-Helianthus/helianthus-ebusgateway/pull/956. No merge
was attempted. Residual risk is limited to ordinary integration review of the
broad deletion of legacy PV compatibility code and of the SemReg public
projection contract; no live smoke claim is made.
