# Gateway #961 Tesla Gen3 EVSE SemReg cutover author report

## Scope and source state

- Repository: `Project-Helianthus/helianthus-ebusgateway`
- Issue: https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/961
- PR: https://github.com/Project-Helianthus/helianthus-ebusgateway/pull/962
- Branch: `issue/961-evse-semreg-cutover`
- Base: `b2651d73639efb7ba690bc1464d9b8b04df51e4a`
- Implementation HEAD: `ccc3bccbaef22315fcb4e929f96097f699534f82`
- Implementation tree: `dc63f43a7525128a34a8cb266cd641c83ce03758`
- Lifecycle remediation implementation HEAD: `8f39cc90c5becace4d27a2b0ad7bf05dbafac900`
- Lifecycle remediation implementation tree: `58e0abe4ee3536fb26255fc4e4b0efe0b003d39c`
- Delayed-ingestion remediation implementation HEAD: `ff83f755c6d0f98891301f0b2fdf697e389adf02`
- Delayed-ingestion remediation implementation tree: `a728abfd3acf428769ca0feff0b1ba89f935fc4f`
- Rollback remediation HEAD: `7f23eeeb428b5512bdb7e9516e1dc5b6815796bf`
- Rollback remediation tree: `cf6b976b19fee40140d31b9f4491d1a2cf834e0c`
- Portal scope-correction source HEAD: `f5bf99fd4623c6cc4ce4ae21923e29e61dbba904`
- Portal scope-correction source tree: `857cd1bcb1029a69eaba56a487a52da0f836f006`
- Capability-activation remediation source HEAD:
  `a358005e8c6d839eef5eaf501d05cd6790380579`
- Capability-activation remediation source tree:
  `94ed05b2c77f06885b6bb1b30ebe16bc289a4ec9`
- Evidence and monotonic-clock remediation source HEAD:
  `a050bce32e92b0db1ca7540baa34558b67c75e44`
- Evidence and monotonic-clock remediation source tree:
  `743144167a802c99e4ce233f7fb3f845defcd82d`
- Accepted documentation mapping: docs-semantic main
  `88a422896e1dc8c45a6bf629f08b8bff6115c009`, reviewed source
  `c0f105cf83229f58ef71664f9cfd30d24c1b02ac`, tree
  `8ac4aa3786298907b6bf3d71cd70d7c95a89764f`
- SemReg runtime: `f3f761bc67e10d6a65eba6c13cb4dc51002d6955`
  (`v0.0.0-20260909085241-f3f761bc67e1`)
- Native input: gateway issue #931, closed at
  https://github.com/Project-Helianthus/helianthus-ebusgateway/issues/931;
  registry `helianthus-modbusreg v0.6.7`, peeled commit
  `57f7eb84f7d4e1173621711bf64624726c71bc75`.

The base, branch, remote head, and implementation tree were reconciled before
this report. The implementation worktree was clean and its remote branch pointed
to the same implementation HEAD. No transport connection, acquisition, request
construction, credential, deployment, device operation, live I/O, or physical
test occurred.

## Implemented semantic publication

`TeslaGen3EVSESemanticPublication` is a stateful per-asset
`helianthus.pack.evse@1.0.0` `PublicationKernel` owner. Its explicit configured
identity includes asset, source, EVSE, connector, source epoch, clock epoch, and
driver generation. Each commit binds immutable native-record evidence, source,
binding, qualified identity link, source epoch/generation, sequence and expected
semantic revision, two read services, and two read capabilities. It declares no
EVSE operation or set-current capability.

The accepted `wc3_24_44_3` mapping is field-local:

- valid persistent t7-to-t8 terminal evidence maps exactly to
  `evse.limit.configured_current`;
- provisional t25-to-t26 plus correlated t27-to-t28 maps to
  `evse.limit.allocated_current` only with the accepted operation version,
  all four nonempty native payloads, timeout `1..86399`, no inhibit, and an
  evaluation before expiry;
- missing, malformed/correlation-mismatched, zero-timeout, inhibited,
  out-of-range, or expired provisional evidence withholds only allocated current
  and records an explicit projection loss. It cannot withdraw a valid configured
  current.

Invalid persistent evidence and invalid lifecycle metadata fail before the
kernel fork can be committed. A replay or sequence collision is rejected while
the existing detached public snapshot remains unchanged.

## P1 lifecycle remediation

The two accepted P1 review findings are corrected by
`8f39cc90c5becace4d27a2b0ad7bf05dbafac900`. Stable Tesla candidate IDs now take
their next revision from the candidate in the current committed SemReg snapshot;
the first candidate is revision `1` and a subsequent accepted publication is
revision `2`. The native evidence sequence remains the caller-supplied collision
fence, while kernel fork/apply remains atomic.

Every MCP and GraphQL read now evaluates the retained immutable SemReg snapshot
at an injected, testable clock. The read derives a later monotonic point from the
sealed publication point and calls `EvaluateSnapshot`; it neither calls a native
provider nor emits a time-only lifecycle batch or fabricated replacement snapshot.
Configured current therefore remains independently exact. A valid provisional
allocation has `FreshFor=timeout` and `RetainFor=timeout+1ns`: just before expiry
it is fresh and exact; at expiry its projection is explicitly withheld with
`withheld_provisional_expired`; after expiry SemReg evaluates it as expired and
the same projection remains withheld. This is an output disposition over the
unchanged snapshot, consistent with the accepted SemReg lifecycle/projection
contract.

## P2 delayed-ingestion remediation

`ff83f755c6d0f98891301f0b2fdf697e389adf02` keeps the native receipt
coordinate (`ObservedAt`, `MonotonicNS`) distinct from the later semantic
evaluation coordinate. It derives `EvaluateMonotonic` by adding the validated
observation-to-evaluation interval to `ReceiptMonotonic`, records both pairs in
every fact candidate, and uses the evaluation coordinate as the committed and
subsequent read baseline. Thus a 30-second ingestion delay contributes to the
SemReg freshness age: a 60-second allocation is fresh at 59 seconds and stale
at its wall-clock expiry, exactly when its public projection becomes
`withheld_provisional_expired`.

This remains an evaluation of the immutable snapshot. It makes no provider
call, native observation, or time-only lifecycle batch; MCP and GraphQL retain
their parity for the delayed-ingestion boundary.

## Portal scope correction and rollback remediation

The independent by-design opinion at
`wave12/review/gateway962-portal-by-design-opinion/REVIEW.md`
(SHA-256 `d8fe64571733b62e15a86b48c353e5adbfece81c6726758883bcfcad43615725`)
confirmed the canonical #961 acceptance excludes an EVSE Portal surface.
`f5bf99fd4623c6cc4ce4ae21923e29e61dbba904` therefore removes the temporary
Portal EVSE configuration, validation, setup and HTTP wiring, GraphQL client
branch, bootstrap capability and endpoint, handler, tests, and classifier
allowance. No Portal route, capability, BFF, second publication, native call,
fallback, alias, acquisition, operation, or runtime owner remains.

Read evaluation now holds an exclusive lifecycle lock and persists the greatest
returned wall and monotonic evaluation coordinates. A post-expiry read followed
by a wall-clock rollback to before expiry is clamped to the prior evaluation and
remains expired and withheld on both MCP and GraphQL.

## P2 capability-activation remediation

`a358005e8c6d839eef5eaf501d05cd6790380579` removes source-identity-derived
activation proof. Each qualified, available read capability now carries the
immutable persistent current-limit record as activation evidence and includes
the provisional record only when it passes the accepted version, correlation,
timeout, inhibition, and expiry checks. The capability instance ID remains
stable while a new accepted native record refreshes its evidence. Invalid
persistent evidence is rejected before a kernel commit, so it cannot create or
advance a qualified available capability; rejection preserves the prior
snapshot. Configured-current publication remains independent of a withheld
provisional allocation, and no operation, Portal, production composition, or
native call is added.

## P2 evidence and monotonic-clock remediation

`a050bce32e92b0db1ca7540baa34558b67c75e44` replaces pointer-based evidence
marshalling with detached serializable DTOs. Persistent evidence contains the
operation version, decoded current, request and terminal payloads, and captured
lifecycle coordinates. Provisional evidence additionally contains the decoded
limit, timeout, inhibit state, and all set/ACK/readback payloads. The fact,
identity, and capability digests therefore change when an accepted native value
or one payload byte changes; later source-pointer mutation cannot alter the
committed snapshot.

Publication captures independent receipt and evaluation monotonic coordinates.
Each read captures a separate injected monotonic clock and advances evaluation
from that clock alone. Wall time remains in the public reporting envelope and is
clamped only to keep reported evaluations non-regressing. Provisional expiry
compares the monotonic receipt coordinate plus timeout with the read evaluation,
so a forward wall jump cannot expire allocation early and a backward jump cannot
resurrect it. Read-clock rollback or error fails closed before state changes or
provider access.

## Public contracts and boundary

- MCP: `semantic.v1.evse.current.get` returns the SemReg
  `snapshot`, `evaluation`, `selections`, and `projection` envelope. It has no
  arguments and derives its timestamp from the SemReg evaluation.
- GraphQL: fixed `SemanticEVSECurrent` with
  `PUBLIC_GRAPHQL_SEMANTIC_EVSE_V1` returns the same four detached members via
  the existing mTLS-authenticated GraphQL handler. It rejects a different query
  shape, contract, or unauthorised asset.
- Native evidence: `modbus.v1.tesla.gen3.evse.current_limit.get` remains the
  only native Tesla record surface. No legacy semantic adapter, alias, fallback,
  comparator, shadow, or dual publication exists.
- Operations: unavailable. ACK/readback evidence proves only the bounded native
  record correlation and never grants a sender, write, charging, or control path.

The deliberately honest limitation is that the production gateway constructor
does not compose an EVSE injected-record owner. The new boundary classifier and
gateway test fail if this seam reaches production config, lifecycle, MCP provider,
or GraphQL runtime paths. Consequently the public contract is implementable and
tested through explicit injected records, while production exposure remains
unavailable until a separately scoped, qualified retained-record injection owner
is accepted. This report makes no production or live-device reachability claim.

## RED/GREEN and validation

RED-first tests define these rejection vectors before their corresponding
implementation path is accepted: malformed persistent evidence; missing
provisional evidence; zero timeout; inhibit state; expiry; replay/collision;
stable sequential candidate revisions; just-before/equal/after-expiry lifecycle
boundaries; concurrent read stability; MCP/GraphQL parity; and production
composition. The P2 regression additionally proves that a shared SourceID does
not synthesize activation, different accepted persistent evidence refreshes the
stable capability identity's activation evidence, and invalid evidence neither
creates nor advances capabilities. The focused race suite passed:

```text
GOWORK=off go test -race ./mcp ./m2mgraphql ./portal ./cmd/gateway \
  -run 'TeslaGen3EVSESemantic|SemanticEVSE' -count=1
PASS
```

The committed-head focused race log is
`/tmp/helianthus-ebusgateway-961-portal-removal-focused-race-committed.log`
with SHA-256
`7b8201cf9db8b4dba07ab3619eb508435b8f73281c7c21cbcb2b59c65dc5e095`.

The first pre-commit scope-correction full-CI invocation reached all compiled,
race, Python, and lint phases but correctly stopped at the transport gate:
historical committed Portal additions and unstaged removals were jointly
classified as a transport change. It is recorded as a failed pre-commit run,
not green:

```text
GOWORK=off ./scripts/ci_local.sh
transport gate: TRANSPORT_MATRIX_REPORT is required for eBUS transport/protocol changes.
```

Its log is `/tmp/helianthus-ebusgateway-961-portal-removal-ci.log`, SHA-256
`c9156bb069b8684cc4c5f351462b57117bcfd2b8e18952be7dd8d93bdbfc70ac`.
After committing the complete removal, the final full local gate passed without
an override or borrowed matrix report:

```text
GOWORK=off ./scripts/ci_local.sh
PASS
```

It covered Portal Node `93/93`, `go test -race -count=1 ./...`, source-selection
schema coverage, Python script suites `168 + 6 + 24 + 9 + 6 + 2 = 215`,
`golangci-lint` with `0 issues`, the Modbus RTU production transport gate, and
the existing Growatt Storage SemReg mapping gate. The passive-smoke classifier
reported `not triggered`. The committed-head final log is
`/tmp/helianthus-ebusgateway-961-portal-removal-ci-committed.log`, SHA-256
`090aa0c08f2dfe3c702affe8d328f3471efebffd24cd533c4bd184012139f9f8`.

The capability-activation focused race passed with
`/tmp/helianthus-ebusgateway-961-capability-activation-focused-race.log`,
SHA-256
`1f7aa93cb038898b5f98b67f9b2a781d1c7aa1338c725b7edb85346102001157`.
Complete local CI passed on the capability source head with
`/tmp/helianthus-ebusgateway-961-capability-activation-ci.log`, SHA-256
`dd8ccc60d7203c5f853345035d950daac606220ac19a818bbe261396defb308d`.

The evidence/monotonic focused race passed with
`/tmp/helianthus-ebusgateway-961-evidence-monotonic-focused-race.log`,
SHA-256
`1f4094d1f01366cc5a6d8553ae66a5a0f6e78702318588eb43c8be4e17fd6dcf`.
Complete local CI passed on the evidence/monotonic source head with
`/tmp/helianthus-ebusgateway-961-evidence-monotonic-ci.log`, SHA-256
`af5595b6101fa06a845465a51046340c8aad2982605e789e1f346b7a1cb5b43d`.

## Gate classification

- Documentation: satisfied by the accepted docs-semantic EVSE mapping and the
  gateway runtime-provider contract update; the correction restores its explicit
  absence of a Portal EVSE view.
- Semantic/lifecycle: applicable and passed through the focused race tests and
  full repository race suite.
- Transport: source-selected existing Modbus RTU production conformance passed;
  no Tesla transport implementation changed.
- Smoke: not applicable and not triggered because the work adds no runtime
  composition, acquisition, or live route.
- Hosted CI: the final report head will trigger a new hosted run after push. The
  earlier run `34448798371` is historical evidence only, against
  `ccc3bccbaef22315fcb4e929f96097f699534f82`; it is not claimed for the
  scope-correction head.

Issue #961 was restored exactly to the canonical acceptance body and received a
concise correction-evidence comment. PR #962 now retains `Closes #961` and
describes the MCP plus authenticated mTLS GraphQL-only contract. The Portal
finding `discussion_r3977269676` received a by-design reply without being
resolved. No independent review or merge was requested or performed.

## P2 successor withdrawal and monotonic-floor remediation

Source `a85d4ff644d278668c6937d2155bc8b3248bf267` uses SemReg
`FactWithdrawals` to retire an earlier allocated-current candidate whenever an
accepted successor withholds allocation. Configured current remains current.
Missing, malformed, inhibited, zero-timeout, and expired successor vectors prove
snapshot/evaluation/projection MCP and GraphQL parity. Later same-epoch receipt
or evaluation coordinates below either retained publication/read floor are
rejected before fork without changing snapshot, revision, sequence, or read
state. Focused race SHA-256:
`8b8e590340dcc494fdcce4f476a7e033fb209485870f2a5a55b5a7712d6ad756`.
Complete CI SHA-256:
`526745d2a4316b7e67fd9c70b0084f11bc6e5203aa459936852bf03421ab6eb4`.

## P2 retry idempotence and candidate-history remediation

Source `6d5caec1bc042ac8453bdd3033d22ab75819c05e` retains the immutable
accepted input digest with its caller generation and sequence. An identical
same-generation, same-sequence, same-input-digest retry returns without reading
the injected clock, forking the SemReg kernel, changing any snapshot bytes,
candidate or semantic revision, sequence, or public output. A same-sequence
input with a different digest remains a collision and is rejected before any
mutation.

The stateful publication also retains each stable candidate's accepted revision
high-water independently of its current snapshot membership. Therefore an
accepted `FactWithdrawal` removes a superseded allocated-current candidate from
the kernel snapshot, while a later valid allocation with the same stable
candidate identity reactivates at revision `2` rather than reusing `1` or
colliding with kernel history. Configured current remains independently current.
The regression suite covers exact retry/no-op, distinct-digest conflict,
withdrawal then reactivation, and MCP/mTLS GraphQL parity for both retained and
reactivated states. It makes no native-provider call, time-only lifecycle batch,
Portal surface, production composition, fallback, alias, or operation path.

Focused semantic race evidence passed:

```text
GOWORK=off go test -race ./mcp -run \
  'TestTeslaGen3EVSESemanticPublication(RetriesIdenticalInputWithoutMutation|ReactivatesWithdrawnAllocatedCurrentAboveHighWater|RejectsReplayAndPreservesLastKnownGood|WithdrawsSupersededAllocatedCurrent)$' -count=1
PASS
```

Log: `/tmp/helianthus-ebusgateway-961-retry-highwater-focused-race.log`;
SHA-256 `075f22f84959a3cd94c3b05b3b3010c7cabf139142b47f9202ae87a68a1101d7`.

The full committed-head local gate also passed:

```text
GOWORK=off ./scripts/ci_local.sh
PASS
```

It covered terminology/source-selection, gofmt, Portal Node `93/93`, go vet,
native and Linux cross-builds, the full Go race suite, source-selection schema
coverage, Python suites `168 + 6 + 24 + 9 + 6 + 2 = 215`, golangci-lint with
`0 issues`, Modbus RTU transport conformance, Growatt Storage SemReg mapping,
and a passive-smoke classifier result of `not triggered`. Log:
`/tmp/helianthus-ebusgateway-961-retry-highwater-ci.log`; SHA-256
`92a4dd82619b61440017060a116fa8b613c57469d7ce66469297e4b9f9545c06`.

## P2 contiguous-sequence remediation

Source `00e71d9a8ed7d5033ae844c700946afdbd901fb9` requires the first accepted
EVSE publication sequence to be `1`; every distinct later input must be exactly
the retained sequence plus one. A first-sequence violation or a later gap is
rejected before an injected-clock read or SemReg kernel fork, preserving the
snapshot, semantic revision, stable candidate revisions, retained input digest,
and sequence. The accepted same-sequence identical input retry remains a no-op,
while a different same-sequence input remains a collision. After a rejected gap,
the missing next sequence resumes normal publication without a resynchronization
or provider call.

The focused race suite proves invalid first sequence, gap rejection, exact retry,
contiguous resume, unchanged-state invariants, and MCP/authenticated GraphQL
parity:

```text
GOWORK=off go test -race ./mcp -run 'TestTeslaGen3EVSESemanticPublication' -count=1
PASS
```

Log: `/tmp/helianthus-ebusgateway-961-contiguous-sequence-focused-race.log`;
SHA-256 `dd7f95b8acc8800782bb41956c5499227a2132d13f0234bf464bcec303716b1a`.

Complete local CI passed on the same committed source:

```text
GOWORK=off ./scripts/ci_local.sh
PASS
```

It covered terminology/source-selection, gofmt, Portal Node `93/93`, full Go
race, schema coverage, Python `215`, golangci-lint `0 issues`, the Modbus RTU
transport gate, the Growatt Storage SemReg mapping gate, and passive smoke `not
triggered`. Log: `/tmp/helianthus-ebusgateway-961-contiguous-sequence-ci.log`;
SHA-256 `818d0d23350faf70f3704c1c238e01c8ef139cb6660d52f2b94970f195d8b1f6`.

## P1 docs gate and P2 delayed-coordinate remediation

Source `8e7b7c4d92f16c15ec2a8582537fadbdbd5520de` adds the declared Tesla EVSE
SemReg mapping gate. It fetches the public immutable docs-semantic commit
`88a422896e1dc8c45a6bf629f08b8bff6115c009`, proves that exact commit was
fetched, runs its Tesla Gen3 WC3 24.44.3 mapping validator, verifies the pinned
SemReg runtime `f3f761bc67e10d6a65eba6c13cb4dc51002d6955`, and runs the EVSE
pack test. `ci_local.sh` invokes this gate after the existing Growatt mapping
gate; the production-boundary classifier remains separate and unchanged.

Delayed injected evidence with unequal observed/evaluated wall coordinates now
requires an explicit non-regressing evaluation monotonic coordinate. Equal-time
evidence may use the receipt coordinate, including zero. Regression coverage
proves equal zero-coordinate acceptance, delayed missing-coordinate rejection
before clock/kernel mutation, explicit delayed promotion, timeout-boundary
withholding, unchanged public state, and MCP/authenticated GraphQL parity.

Focused race: `GOWORK=off go test -race ./mcp -run
'TestTeslaGen3EVSESemanticPublication' -count=1`; log
`/tmp/helianthus-ebusgateway-961-docs-delay-focused-race.log`, SHA-256
`29441a831eb6ab5bfa2203a319311472d797f320dd6a24f0c4181f58eb871eae`.

Complete CI passed with the new mapping gate: `GOWORK=off
./scripts/ci_local.sh`; log
`/tmp/helianthus-ebusgateway-961-docs-delay-ci.log`, SHA-256
`d1c0ae3f66161dd7af7ca3629336ed384e1135888df5681e8a95a6a835dd9d42`.
