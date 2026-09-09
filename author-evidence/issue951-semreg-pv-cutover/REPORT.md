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

## Review remediation at PR #956 head `56cee40`

The independent review identified four blockers and one orphaned golden. This
follow-up patch corrects each without restoring a compatibility path:

- Hosted CI replaces the deleted `canonicalpvshadow` directory step with the
  race-tested SemReg public-integration set.
- A stale retained field is preserved in the evaluated view and has no
  presentation selection; other fields from the same qualified refresh commit.
  The regression proves a stale invalid frequency is withheld while aggregate
  active power advances to candidate revision 2.
- The fixed GraphQL operation and response now use the same exact four fields:
  `snapshot`, `evaluation`, `selections`, and `projection`. The handler rejects
  an object with undeclared fields, and an exact response golden covers the
  accepted shape.
- MCP converts SemReg evaluated nanoseconds to RFC3339Nano before assigning
  `meta.data_timestamp`; success parsing and existing error precedence are
  covered.
- `mcp/testdata/modbus_v1_canonical_pv.golden.json` was unreferenced and
  compatibility-only, so it is removed and its absence is covered by the
  legacy-tool/public-contract searches and MCP replacement test.

Focused normal and race regressions passed:

```text
GOWORK=off go test ./internal/modbusadapter ./m2mgraphql ./mcp ./portal ./cmd/gateway -run 'TestPVPublication|TestAdapterPublishesOneSemRegPVProjection|TestSemanticPV|TestPortalPV|TestM2MGraphQLRuntime|TestGatewaySemanticPVProvider' -count=1
PASS

GOWORK=off go test -race ./internal/modbusadapter ./m2mgraphql ./mcp ./portal ./cmd/gateway -run 'TestPVPublication|TestAdapterPublishesOneSemRegPVProjection|TestSemanticPV|TestPortalPV|TestM2MGraphQLRuntime|TestGatewaySemanticPVProvider' -count=1
PASS

GOWORK=off ./scripts/ci_local.sh
PASS
```

The remediation CI log SHA-256 is
`6a25722cd88b5f84aa6fd23170311af906d9387654d3f20bbec8267ce5bf2cd4`.

## Read-time freshness and clock remediation

The subsequent review found that the immutable snapshot was being served with
its ingestion-time evaluation. Public `SemanticPVCurrent` reads now construct a
trusted read-time wall/monotonic evaluation context, run SemReg
`EvaluateSnapshot` again, and rebuild presentation selections without mutating
the snapshot bytes, ID, revisions, candidates, or projection report. MCP and
M2M GraphQL both obtain this re-evaluated view through the same adapter getter.

The adapter retains a private monotonic-bearing process start separately from
the UTC wall start. Publication and read contexts use elapsed monotonic time;
wall time is converted to a UTC `TimePoint` separately and is not allowed to
precede the recorded source start. The wall-adjustment regression proves a
backward wall correction cannot invalidate a monotonic-valid publication.

Deterministic adapter coverage proves a single immutable PV snapshot is
observed as fresh, then stale, then expired at successive public reads, and
that stale/expired candidates have no presentation selection. It also proves
the canonical bytes and SnapshotID remain unchanged. Focused normal/race tests
and final configured CI passed; the final log SHA-256 is
`779a356df4fdc85fb884c89a3fa19acfe691a390dd2c9b2262897880804f9145`.

The implementation commit is `a2bab01ae713caf2225625bf1d150f20755c5a37`;
the current branch includes this report and is open as
https://github.com/Project-Helianthus/helianthus-ebusgateway/pull/956. No merge
was attempted. Residual risk is limited to ordinary integration review of the
broad deletion of legacy PV compatibility code and of the SemReg public
projection contract; no live smoke claim is made.

## Current-asset refresh-evidence retention remediation

The bounded refresh-evidence store now determines referenced qualification
observation digests from every current SemReg PV asset snapshot while holding
one publication-core read lock. Preflight eviction rejects the update before
publication when all 32 retained records remain referenced; post-commit pruning
removes only records that no current asset references. The observation is staged
before publication, rolled back on a failed publication, and copied at both
storage and lookup boundaries.

The regression qualifies an initial identity, then refreshes distinct serial
identities A and B so both A and B observations are owned by the bounded refresh
store. It verifies both current public asset views retain matching immutable,
replayable MCP evidence. The test would fail with the former incoming-asset-only
reference set because B publication would prune A's refresh record. A separate
partial-refresh regression verifies that accumulator evidence remains retrievable
across more than 32 later withheld-energy refreshes until it is replaced.

Focused race coverage passed:

```text
GOWORK=off go test -race ./internal/modbusadapter -run 'TestSunSpecProducer(RetainsEvidenceForEveryCurrentIdentity|RetainsReferencedAccumulatorEvidenceAcrossPartialRefreshes|RefreshRetainsCurrentSemRegEvidenceWithBoundedEviction)' -count=1 -v
PASS (3 tests)
```

Focused log SHA-256:
`080490a4a5d10b8e18ac758028f36c5731923fe099ef8531e935fe3522e541f7`.

The full configured `GOWORK=off ./scripts/ci_local.sh` passed: Portal Node
`93/93`; all Go race packages; Python `168 + 6 + 11 + 8 + 6 + 2`; and
`golangci-lint` with `0 issues`. Transport and passive-smoke gates were not
triggered. Final CI log SHA-256:
`766b7fdc4f6b9ab1d6195a725bd328587f0ed3ee970207f3a99456399b7415b3`.
