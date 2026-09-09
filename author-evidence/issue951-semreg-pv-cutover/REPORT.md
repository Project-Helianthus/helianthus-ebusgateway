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

## Prospective global evidence-capacity remediation

Refresh evidence now stages its exact detached observation under `profileMu`
before the SemReg candidate is built. While the publication core holds its
write lock, it forks, applies, and evaluates that candidate, substitutes the
candidate snapshot for the target asset in the all-current-asset reference set,
and validates the resulting references before assigning `kernel` or `current`.
The validator fails closed when any referenced observation is absent or more
than 32 refresh records remain referenced. A failed validation rolls back the
provisional record without a semantic revision; successful publication prunes
only globally unreferenced refresh records.

Capability activation evidence is refreshed with each exact observation. Thus a
complete replacement does not keep its predecessor alive only through an old
capability activation reference, while retained partial fields still preserve
their referenced observation normally.

The boundary regression creates 32 distinct, protected refresh identities,
successfully replaces one identity through a temporary 33rd provisional record,
and verifies the new public digest is immutable and replayable while the
superseded record is pruned and the stored set remains at most 32. It then
attempts a distinct 33rd protected identity and proves it stops without a new
semantic asset, semantic revision, or retained evidence.

Focused normal and race coverage passed for the bounded eviction, retained
accumulator, all-current-identity, and prospective-capacity cases. Race log
SHA-256: `64af18cca9838f05a20912a9ed4222a42bde0b0efc44bc8f52ea8269356bf63c`.

The final configured `GOWORK=off ./scripts/ci_local.sh` passed: Portal Node
`93/93`; all Go race packages; Python `168 + 6 + 11 + 8 + 6 + 2`; and
`golangci-lint` with `0 issues`. Transport and passive-smoke gates were not
triggered. CI log SHA-256:
`12c5831c5d842283feb7d3953359c58d6abd04d04b85325c04ef976543bf7f4e`.

## Public-read and qualification-publication ordering remediation

`SemanticPVCurrentByAsset` now detaches the exact current snapshot while the
publication core read lock is held, then captures wall and monotonic context,
and evaluates only that detached snapshot. A deterministic interleaving test
blocks context capture, commits a later receipt, and proves the read returns
the already-detached healthy snapshot instead of transiently becoming
unavailable because a pre-captured context precedes the later receipt.

Initial qualification observations are now staged as detached records under
`profileMu` before their SemReg publication. The prospective evidence validator
observes that staged record before assigning the publication-core state; any
build or publication error removes it. The regression proves a blocked
publication writes neither a qualification record nor a semantic asset, then
races a public reader with a successful qualification and verifies every
observed public snapshot has its exact replayable evidence available.

Focused normal and race coverage passed for read ordering, qualification
rollback/evidence following, bounded evidence retention, retained accumulator,
all-current identities, and prospective capacity. Race log SHA-256:
`7ee7b72ea702c01b6c6e30503049fc9d415be7b7a478b7a2420366dc6c738172`.

The final configured `GOWORK=off ./scripts/ci_local.sh` passed: Portal Node
`93/93`; all Go race packages; Python `168 + 6 + 11 + 8 + 6 + 2`; and
`golangci-lint` with `0 issues`. Transport and passive-smoke gates were not
triggered. CI log SHA-256:
`9d1802e466f70f3c53448391b2efdeee8412874efa394d8a13378f8cb00c729b`.

## Mandatory semantic MCP provider remediation

The migrated `ModbusV1Provider` contract now requires
`SemanticPVCurrent(context.Context, profileID, sampleID)`. Registration cannot
compose a provider that lacks the enabled SemReg PV capability, and the
optional type assertion plus `semantic PV provider unavailable` compatibility
branch are removed. The advertised `semantic.v1.pv.current.get` tool invokes
the mandatory provider method directly; normal no-current and asset-not-found
errors remain provider-owned terminal results.

Every real and fixture composition now implements the required method. The MCP
regression verifies that a raw/profile-only implementation fails the provider
interface, while any composed provider advertises and successfully serves the
semantic PV tool. Existing tool-list goldens already describe the unchanged
final enabled tool set and require no regenerated bytes.

Focused normal and race MCP, gateway, and Portal coverage passed. Race log
SHA-256: `69cd257311013e14d0ee05a1d38a05d707f0a3935907a6f797877d652a3686d6`.

The final configured `GOWORK=off ./scripts/ci_local.sh` passed: Portal Node
`93/93`; all Go race packages; Python `168 + 6 + 11 + 8 + 6 + 2`; and
`golangci-lint` with `0 issues`. Transport and passive-smoke gates were not
triggered. CI log SHA-256:
`1ee04b2f02d50c24c0a8890c96af49322091c4d77884bac60ee08f45a320db05`.

## Per-view wall-floor remediation

Each committed PV publication view now retains a detached, nondecreasing wall
floor derived from its lifecycle receipt/evaluation coordinate and the prior
current view. Public reads still detach a single immutable snapshot before
capturing current wall/monotonic context. The read then clamps only its wall
coordinate to that selected view's same-clock floor; monotonic elapsed remains
the current value and retains its invalid-rollback rejection. Canonical snapshot
bytes and selection ownership are never mutated.

Deterministic coverage publishes at T1, rolls the wall clock back to T0, and
proves the original snapshot remains available and byte-identical with an
evaluation wall at T1. A refresh while the wall remains at T0 carries the T1
floor forward, and a later forward wall is used normally. Concurrent refresh
and public-read coverage proves every read remains available under `-race`.

Focused normal and race coverage passed for wall rollback, concurrent
refresh/read, detach-before-context, qualification staging, and bounded
evidence cases. Race log SHA-256:
`39e7a5daf957336b685d34be7173eb4d768768ebd00f96a03756d9e4dd8777b7`.

The final configured `GOWORK=off ./scripts/ci_local.sh` passed: Portal Node
`93/93`; all Go race packages; Python `168 + 6 + 11 + 8 + 6 + 2`; and
`golangci-lint` with `0 issues`. Transport and passive-smoke gates were not
triggered. CI log SHA-256:
`8bd31ef1feddcdb2462de94058a62942c46df5a43f003fd502e1a1efbab51b48`.
